import pandas as pd
import numpy as np
from datetime import datetime
from dateutil.relativedelta import relativedelta
import math
from sqlalchemy import text

def calculate_unsettled_pv(
    loss_amount: float, 
    claim_pattern: list, 
    discount_rates: dict, 
    accident_month: str, 
    evaluation_month: str,
    use_current_rate_curve: bool
) -> (float, list):
    """
    计算未决赔款的现值。

    Args:
        loss_amount (float): 未决赔款（或费用）的总金额。
        claim_pattern (list): 该风险对应的赔付模式进展因子数组 (例如 [0.05, 0.1, ...])。
        discount_rates (dict): 利率曲线, 格式为 {月份差异: 利率}, e.g., {1: 0.002, 2: 0.0021, ...}。
        accident_month (str): 事故年月 (格式 'YYYYMM')。
        evaluation_month (str): 评估年月 (格式 'YYYYMM')。
        use_current_rate_curve (bool): 是否使用当前评估时点的利率曲线 (True for PV1, False for PV3)。

    Returns:
        tuple: (总现值, 详细计算过程的列表)
    """
    if loss_amount == 0 or not claim_pattern or not discount_rates:
        return 0.0, []

    # 1. 计算已过月数
    try:
        acc_date = datetime.strptime(accident_month, '%Y%m')
        eval_date = datetime.strptime(evaluation_month, '%Y%m')
        # +1 因为我们计算的是到评估月月末的完整月数
        months_passed = (eval_date.year - acc_date.year) * 12 + (eval_date.month - acc_date.month) + 1
    except (ValueError, TypeError):
        return 0.0, []

    # 2. 获取剩余赔付进展因子
    if months_passed > len(claim_pattern):
        remaining_pattern = []
    else:
        remaining_pattern = claim_pattern[months_passed:]
    
    sum_remaining_pattern = sum(remaining_pattern)
    if sum_remaining_pattern == 0:
        return 0.0, []

    total_pv = 0.0
    calculation_log = []
    
    # 3. 逐期计算现金流并折现
    for i, ratio in enumerate(remaining_pattern):
        period = i + 1
        
        # a. 计算当期现金流
        cash_flow = loss_amount * (ratio / sum_remaining_pattern)
        
        # b. 计算折现因子
        discount_factor = 1.0
        rate_log = []

        # PV1: 利率从第1期开始 (相对于评估月)
        # PV3: 利率从评估月之后的第一期开始 (相对于事故月利率曲线)
        # e.g. 事故月2411, 评估月2412, 已过2个月(months_passed=2). 
        # 第一个未来现金流(2501末)折现到2412末, 使用的是第2期的利率(rate_2), 
        # 因为rate_1用于2411末->2412末的折现.
        start_rate_term = 1 if use_current_rate_curve else months_passed
        
        # 累积折现因子
        for j in range(period):
            term = start_rate_term + j
            rate = discount_rates.get(term, 0)
            if rate is None: rate = 0
            discount_factor *= (1 + rate)
            rate_log.append(f"(1+{rate:.6f})")

        # c. 计算当期现值
        pv_of_period = cash_flow / discount_factor if discount_factor != 0 else 0
        total_pv += pv_of_period
        
        calculation_log.append({
            "期数": period,
            "未来月份": (eval_date + relativedelta(months=period)).strftime('%Y%m'),
            "赔付进展因子": ratio,
            "现金流": cash_flow,
            "利率期限": f"第{start_rate_term}到{start_rate_term + period - 1}月",
            "累积折现因子计算": " * ".join(rate_log) + f" = {discount_factor:.6f}",
            "累积折现因子": discount_factor,
            "当期现值": pv_of_period
        })
        
    return total_pv, calculation_log

def get_last_period_results(_engine, unit_id: str, last_eval_month: str) -> dict:
    """
    从数据库获取指定计量单元的上一个评估月份的结果。
    """
    default_results = {
        'pv_last_case_current': 0.0, 'pv_last_ibnr_current': 0.0, 'pv_last_ulae_current': 0.0,
        'pv_last_case_accident': 0.0, 'pv_last_ibnr_accident': 0.0, 'pv_last_ulae_accident': 0.0,
        'pv_last_case_amt': 0.0, 'pv_last_ibnr_amt': 0.0, 'pv_last_ulae_amt': 0.0
    }
    
    if not unit_id or not last_eval_month:
        return default_results

    query = text("""
        SELECT 
            pv_case_current AS pv_last_case_current,
            pv_ibnr_current AS pv_last_ibnr_current,
            pv_ulae_current AS pv_last_ulae_current,
            pv_case_accident AS pv_last_case_accident,
            pv_ibnr_accident AS pv_last_ibnr_accident,
            pv_ulae_accident AS pv_last_ulae_accident,
            pv_last_case_amt,
            pv_last_ibnr_amt,
            pv_last_ulae_amt
        FROM measure_platform.measure_cx_unsettled
        WHERE "val_month" = :val_month AND "unit_id" = :unit_id
    """)
    
    with _engine.connect() as connection:
        df = pd.read_sql(query, connection, params={"val_month": last_eval_month, "unit_id": unit_id})
    
    if df.empty:
        return default_results
    
    # FIX: Convert index to lower case for robust matching
    last_results_series = df.iloc[0]
    last_results_series.index = last_results_series.index.str.lower()
    return last_results_series.to_dict()


def calculate_direct_unsettled_measure(
    unsettled_data: pd.DataFrame,
    assumptions: pd.DataFrame,
    patterns: pd.DataFrame,
    rates: pd.DataFrame,
    evaluation_month: str,
    db_engine 
) -> (dict, list):
    """
    直保未决计量的主计算函数。
    """
    
    logs = []
    final_results = {}
    
    if unsettled_data.empty:
        return {}, []

    record = unsettled_data.iloc[0]

    class_code = record.get('class_code') # Directly use class_code from the new table
    accident_month = record.get('accident_month')
    
    if not class_code or not accident_month:
        logs.append({"title": "数据错误", "log": f"记录缺少 class_code ({class_code}) 或 accident_month ({accident_month})", "summary": {}})
        return {}, logs

    # 获取赔付模式
    claim_pattern_list = patterns[patterns['class_code'] == class_code].sort_values('month_id')['paid_ratio'].tolist()
    
    # 获取利率曲线
    current_rates_dict = dict(rates[rates['val_month'] == evaluation_month][['term_month', 'forward_disrate_value']].values)
    accident_rates_dict = dict(rates[rates['val_month'] == accident_month][['term_month', 'forward_disrate_value']].values)
    
    # 获取精算假设 (风险调整率RA)
    ra_value = 0.0
    if not assumptions.empty:
        assumption_row = assumptions[assumptions['class_code'] == class_code]
        if not assumption_row.empty:
            ra_value = assumption_row.iloc[0].get('lic_ra', 0.0)
            
    # --- 标题翻译映射 ---
    title_map = {
        'case_amt': '已报案赔案',
        'ibnr_amt': 'IBNR',
        'ulae_amt': '理赔费用'
    }

    # --- 循环计算所有金额类型 ---
    amount_types = ['case_amt', 'ibnr_amt', 'ulae_amt']
    for amount_type in amount_types:
        amount = record.get(amount_type, 0.0)
        if amount is None or pd.isna(amount) or amount == 0:
            continue

        # Correct field name prefix by removing '_amt' for pv fields
        pv_prefix = amount_type.replace('_amt', '')
        cn_title = title_map.get(amount_type, amount_type)

        # -- PV1 (BEL & RA) --
        pv1_bel, pv1_log = calculate_unsettled_pv(amount, claim_pattern_list, current_rates_dict, accident_month, evaluation_month, True)
        final_results[f'pv_{pv_prefix}_current'] = pv1_bel
        final_results[f'pv_{pv_prefix}_current_ra'] = pv1_bel * ra_value
        
        pv1_summary = {
            "总现值(BEL)": pv1_bel,
            "利率曲线": f"使用评估月份 {evaluation_month} 的利率曲线"
        }
        logs.append({"title": f"{cn_title} - PV1 (当期利率) 计算过程", "log": pv1_log, "summary": pv1_summary})
        
        # -- PV3 (BEL & RA) --
        pv3_bel, pv3_log = calculate_unsettled_pv(amount, claim_pattern_list, accident_rates_dict, accident_month, evaluation_month, False)
        final_results[f'pv_{pv_prefix}_accident'] = pv3_bel
        final_results[f'pv_{pv_prefix}_accident_ra'] = pv3_bel * ra_value
        
        pv3_summary = {
            "总现值(BEL)": pv3_bel,
            "利率曲线": f"使用事故月份 {accident_month} 的利率曲线"
        }
        logs.append({"title": f"{cn_title} - PV3 (事故时点利率) 计算过程", "log": pv3_log, "summary": pv3_summary})

        # -- 计算下一期末现值 (用于财务费用) --
        try:
            acc_date = datetime.strptime(accident_month, '%Y%m')
            eval_date = datetime.strptime(evaluation_month, '%Y%m')
            months_passed = (eval_date.year - acc_date.year) * 12 + (eval_date.month - acc_date.month) + 1
            interest_rate = accident_rates_dict.get(months_passed, 0)
            pv_next_period = pv3_bel * (1 + interest_rate if interest_rate else 1)
            final_results[f'{pv_prefix}_amt_ifie_accident'] = pv_next_period
        except Exception:
            final_results[f'{pv_prefix}_amt_ifie_accident'] = pv3_bel


    # --- 获取上期结果 ---
    last_eval_month = (datetime.strptime(evaluation_month, '%Y%m') - relativedelta(months=1)).strftime('%Y%m')
    unit_id = record.get('unit_id')
    last_results = get_last_period_results(db_engine, unit_id, last_eval_month)
    final_results.update(last_results)

    # --- 计算会计分录 ---
    # 汇总各部分金额 (BEL + RA)
    current_pv1 = sum(final_results.get(f'pv_{t.replace("_amt", "")}_current', 0) + final_results.get(f'pv_{t.replace("_amt", "")}_current_ra', 0) for t in amount_types)
    last_pv1 = sum(last_results.get(f'pv_last_{t.replace("_amt", "")}_current', 0) for t in amount_types) # 上期结果已包含RA
    current_pv3 = sum(final_results.get(f'pv_{t.replace("_amt", "")}_accident', 0) + final_results.get(f'pv_{t.replace("_amt", "")}_accident_ra', 0) for t in amount_types)
    last_pv3 = sum(last_results.get(f'pv_last_{t.replace("_amt", "")}_accident', 0) for t in amount_types) # 上期结果已包含RA
    
    # PV5的计算: 上期的 `*_ifie_accident` 字段本身就是基于PV3(BEL+RA)计息而来的
    pv5_total = sum(last_results.get(f'pv_last_{t.replace("_amt","")}_amt', 0) for t in amount_types)

    final_results['paid_claim_change'] = current_pv1 - last_pv1
    final_results['service_fee_change'] = current_pv3 - pv5_total
    final_results['paid_claim_ifie'] = pv5_total - last_pv3
    final_results['oci_change'] = (current_pv1 - last_pv1) - (current_pv3 - last_pv3)

    return final_results, logs
