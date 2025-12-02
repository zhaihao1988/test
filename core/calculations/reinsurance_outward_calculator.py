"""
再保分出计量计算器
Reinsurance Outward Calculator for LRC measurement

计算逻辑：
- 非亏损部分：逐月净额法计算（类似再保分入，但无经纪费和非跟单获取费用）
- 亏损部分：不计算，引用对应的直保或再保分入的亏损金额
"""

import pandas as pd
from decimal import Decimal, ROUND_HALF_UP
from datetime import datetime
from dateutil.relativedelta import relativedelta
from calendar import monthrange
from sqlalchemy.engine import Engine
from typing import List, Dict, Any, Tuple
from io import StringIO

from core.data_fetcher.reinsurance_outward_data import (
    get_reinsurance_outward_source_data,
    get_reinsurance_outward_measure_prep_data,
    get_all_reinsurance_outward_measure_records,
    get_underlying_loss_amount,
    get_invest_prop
)
from core.data_fetcher.reinsurance_input_data import (
    get_reinsurance_outward_assumptions,
    get_reinsurance_discount_rates,
    get_reinsurance_claim_models,
)
from core.data_fetcher.comparison_data import get_db_reinsurance_outward_measure_result


SCALE = 10  # Decimal precision

def _calculate_one_month_outward(
    engine: Engine,
    val_month: str,
    static_data: dict,
    cash_flows_for_month: dict,
    prev_result: dict,
    non_onerous_rate_curve: dict,
    original_ini_confirm_month: str,
    term_month: int,
    rein_type: str,
    loss_data: dict
) -> Tuple[Dict[str, Any], Dict[str, Decimal], str]:
    """
    Calculates all metrics for a single evaluation month for reinsurance outward.
    再保分出的逐月计算（净额法，无经纪费和非跟单费用）
    """
    logs = StringIO()
    D = Decimal
    
    logs.write(f"\n--- 开始计算评估月: {val_month} ---\n")
    logs.write("步骤 1: 初始化数据\n")
    
    # --- FIX: 使用 .get() 安全地从 Series 中提取值 ---
    # 使用 .get(key, 0) or 0 的模式确保即使数据库返回None也能安全处理
    current_premium_cash_flow = D(cash_flows_for_month.get('re_premium_cny_cash_flow', 0) or 0)
    current_commission_cash_flow = D(cash_flows_for_month.get('re_commission_cny_cash_flow', 0) or 0)
    current_net_premium_cash_flow = current_premium_cash_flow - current_commission_cash_flow
    
    logs.write(f"  - 当期分出保费现金流: {current_premium_cash_flow:.4f}\n")
    logs.write(f"  - 当期分出佣金现金流: {current_commission_cash_flow:.4f}\n")
    logs.write(f"  - 当期净现金流 (保费 - 佣金): {current_net_premium_cash_flow:.4f}\n")

    # 从上一轮结果中获取累计值
    prev_lrc_no_loss = D(prev_result.get('lrc_no_loss_amt', 0))
    prev_acc_insurance_revenue = D(prev_result.get('acc_insurance_revenue', 0))
    prev_acc_investment_amortization = D(prev_result.get('acc_investment_amortization', 0))
    prev_acc_income_before_splitting = D(prev_result.get('acc_income_before_splitting', 0))
    prev_acc_ifie = D(prev_result.get('acc_ifie', 0))

    logs.write(f" -> 期初非亏损余额: {prev_lrc_no_loss:.4f}\n")
    logs.write(f" -> 上期累计确认收入: {prev_acc_insurance_revenue:.4f}\n")
    logs.write(f" -> 上期累计投资成分摊销: {prev_acc_investment_amortization:.4f}\n")
    logs.write(f" -> 上期累计IFIE: {prev_acc_ifie:.4f}\n")

    # --- 2. Amortization Calculation ---
    logs.write("\n【计算累计服务比例】:\n")
    val_date = datetime.strptime(val_month, '%Y%m').date()
    val_date = val_date.replace(day=monthrange(val_date.year, val_date.month)[1])

    # --- FIX: Handle both Timestamp and date objects gracefully ---
    pi_start_date_val = static_data.get('pi_start_date')
    pi_end_date_val = static_data.get('pi_end_date')

    pi_start_date_obj = pi_start_date_val.date() if hasattr(pi_start_date_val, 'date') else pi_start_date_val
    pi_end_date_obj = pi_end_date_val.date() if hasattr(pi_end_date_val, 'date') else pi_end_date_val

    total_days = D((pi_end_date_obj - pi_start_date_obj).days + 1)
    elapsed_days = D(0)
    if val_date >= pi_start_date_obj:
        elapsed_days = D((min(val_date, pi_end_date_obj) - pi_start_date_obj).days + 1)
    
    amortized_ratio = elapsed_days / total_days if total_days > 0 else D(0)
    logs.write(f"  累计服务天数: {elapsed_days} / {total_days}\n")
    logs.write(f"  -> 累计服务比例: {amortized_ratio:.10f}\n")

    # --- 3. Investment Component Amortization ---
    logs.write("\n【计算当期投资成分摊销】:\n")
    acc_investment_amortization = (D(static_data.get('total_investment_component', 0)) * amortized_ratio).quantize(D(f'1e-{SCALE}'))
    current_investment_amortization = (acc_investment_amortization - prev_acc_investment_amortization).quantize(D(f'1e-{SCALE}'))
    logs.write(f"  公式: (总投资成分 * 累计服务比例) - 上期累计摊销\n")
    logs.write(f"  = ({D(static_data.get('total_investment_component', 0)):.4f} * {amortized_ratio:.10f}) - {prev_acc_investment_amortization:.4f} = {current_investment_amortization:.4f}\n")
    logs.write(f"  -> 累计投资成分摊销更新为: {acc_investment_amortization:.4f}\n")

    # --- 4. IFIE (Interest) Calculation ---
    logs.write("\n【计算当期IFIE (未到期利息)】:\n")
    
    # This is the rate for non-onerous calculation (IFIE)
    non_onerous_rate_float = non_onerous_rate_curve.get(term_month)
    if non_onerous_rate_float is None:
        logs.write(f"警告: 在锁定的初始确认月 '{original_ini_confirm_month}' 利率曲线中未找到第 {term_month} 期的利率, 将使用 0。\n")
        non_onerous_rate_float = 0.0
    non_onerous_rate_decimal = D(str(non_onerous_rate_float))
    monthly_rate = non_onerous_rate_decimal
    ifie_from_opening_balance = prev_lrc_no_loss * monthly_rate
    ifie_from_net_premium = current_net_premium_cash_flow * monthly_rate * D('0.5')
    current_ifie = (ifie_from_opening_balance + ifie_from_net_premium).quantize(D(f'1e-{SCALE}'))
    acc_ifie = (prev_acc_ifie + current_ifie).quantize(D(f'1e-{SCALE}'))
    logs.write(f"  公式: 上月余额利息 + 本月净现金流利息\n")
    logs.write(f"  = ({prev_lrc_no_loss:.4f} * {monthly_rate:.12f}) + ({current_net_premium_cash_flow:.4f} * {monthly_rate:.12f} * 0.5) = {current_ifie:.4f}\n")
    logs.write(f"  -> 累计IFIE更新为: {acc_ifie:.4f}\n")

    # --- 5. Insurance Revenue (for outward, this is negative as it's ceded) ---
    logs.write("\n【计算当期确认收入】:\n")
    total_net_premium = D(static_data.get('net_premium', 0) or 0)

    # 5.1 Calculate income before splitting investment component
    logs.write("  步骤 5.1: 计算总分摊（分解投资成分前）\n")
    acc_income_before_splitting = ((total_net_premium + acc_ifie) * amortized_ratio).quantize(D(f'1e-{SCALE}'))
    current_income_before_splitting = (acc_income_before_splitting - prev_acc_income_before_splitting).quantize(D(f'1e-{SCALE}'))
    logs.write(f"  - 公式: (总净保费 + 累计IFIE) * 累计服务比例\n")
    logs.write(f"  - 累计总分摊(分解前): (({total_net_premium:.4f} + {acc_ifie:.4f}) * {amortized_ratio:.10f}) = {acc_income_before_splitting:.4f}\n")

    # 5.2 Calculate final insurance revenue
    logs.write("  步骤 5.2: 分解投资成分，计算实际保险服务收入\n")
    acc_insurance_revenue = (acc_income_before_splitting - acc_investment_amortization).quantize(D(f'1e-{SCALE}'))
    current_insurance_revenue = (acc_insurance_revenue - prev_acc_insurance_revenue).quantize(D(f'1e-{SCALE}'))
    logs.write(f"  - 公式: 累计总分摊(分解前) - 累计投资成分摊销\n")
    logs.write(f"  - 累计确认收入: {acc_income_before_splitting:.4f} - {acc_investment_amortization:.4f} = {acc_insurance_revenue:.4f}\n")
    logs.write(f"  - 当期确认收入: {acc_insurance_revenue:.4f} - {prev_acc_insurance_revenue:.4f} = {current_insurance_revenue:.4f}\n")

    # --- 6. Non-Onerous LRC Calculation ---
    logs.write("\n【计算期末非亏损余额】:\n")
    logs.write(f"  公式: 期初余额 + 净现金流 + 当期IFIE - 当期确认收入 - 当期投资成分摊销\n")
    lrc_no_loss_amt = (
        prev_lrc_no_loss + 
        current_net_premium_cash_flow + 
        current_ifie - 
        current_insurance_revenue -
        current_investment_amortization
    ).quantize(D(f'1e-{SCALE}'))
    logs.write(f"  = {prev_lrc_no_loss:.4f} + {current_net_premium_cash_flow:.4f} + {current_ifie:.4f} - {current_insurance_revenue:.4f} - {current_investment_amortization:.4f} = {lrc_no_loss_amt:.4f}\n")
    logs.write(f"  -> 期末非亏损余额 (closing_balance) = {lrc_no_loss_amt:.4f}\n")

    # --- 7. Final Results ---
    result = {
        'val_month': val_month,
        'closing_balance': float(lrc_no_loss_amt),
        'lrc_no_loss_amt': float(lrc_no_loss_amt),
        'current_insurance_revenue': float(current_insurance_revenue),
        'current_net_cash_flow': float(current_net_premium_cash_flow),
        'acc_insurance_revenue': float(acc_insurance_revenue),
        'acc_income_before_splitting': float(acc_income_before_splitting),
        'current_income_before_splitting': float(current_income_before_splitting),
        'acc_investment_amortization': float(acc_investment_amortization),
        'current_investment_amortization': float(current_investment_amortization),
        'acc_ifie': float(acc_ifie),
        'amortized_ratio': float(amortized_ratio),
        'current_ifie': float(current_ifie),
    }

    # For next iteration
    internal_result_for_next_loop = {
        'lrc_no_loss_amt': lrc_no_loss_amt,
        'acc_insurance_revenue': acc_insurance_revenue,
        'acc_investment_amortization': acc_investment_amortization,
        'acc_income_before_splitting': acc_income_before_splitting,
        'acc_ifie': acc_ifie,
    }
    
    return result, internal_result_for_next_loop, logs.getvalue()

def build_reinsurance_outward_cost_timeline(
    measure_prep_record: pd.Series,
    initial_booking_month: str
) -> pd.DataFrame:
    """
    Builds the cash flow timeline based on the single, latest measure prep record.
    All cash flows are booked to the initial_booking_month.
    """
    timeline = {}
    D = Decimal

    # All cash flows (premium and commission) from the single record are booked to the initial month.
    premium_flow = D(measure_prep_record.get('premium', 0) or 0)
    commission_flow = D(measure_prep_record.get('commission', 0) or 0)

    if premium_flow != 0 or commission_flow != 0:
        timeline[initial_booking_month] = {
            'premium': premium_flow,
            'commission': commission_flow
        }

    if not timeline:
        return pd.DataFrame(columns=['month', 'premium', 'commission'])

    timeline_df = pd.DataFrame.from_dict(timeline, orient='index').reset_index()
    timeline_df = timeline_df.rename(columns={'index': 'month'})
    
    return timeline_df.sort_values(by='month').reset_index(drop=True)


def calculate_reinsurance_outward_unexpired_measure(
    engine: Engine,
    measure_val_month: str,
    policy_no: str,
    certi_no: str,
    contract_id: str
) -> Tuple[List[Dict[str, Any]], pd.DataFrame, pd.DataFrame, Dict[str, Any]]:
    """
    Orchestrates the new month-by-month calculation for reinsurance LRA for a specific contract_id.
    
    Returns:
        - calculation_logs: 逐月计算日志
        - final_result_df: 最终计量结果
        - cashflow_df: 费用时间线
        - loss_info: 亏损信息字典
    """
    D = Decimal
    main_logs = StringIO()
    loss_info = {} # Initialize to prevent UnboundLocalError
    calculation_logs = []
    final_result_df = pd.DataFrame()
    rein_type = '1' # Default value
    discount_rates_map = {} # Initialize to prevent UnboundLocalError
    months_to_calculate = [] # Initialize to prevent UnboundLocalError

    # --- 1. Data Fetching ---
    main_logs.write("步骤 1: 获取计量所需数据...\n")
    try:
        # Step 1.1: Fetch the single, latest measure prep record. This is the source of truth.
        measure_prep_df = get_reinsurance_outward_measure_prep_data(engine, policy_no, certi_no, contract_id)
        if measure_prep_df.empty:
            raise ValueError(f"在计量准备表 'public.int_t_pp_re_mon_arr_new' 中未找到合约 '{contract_id}' 的记录。")
        
        # This single record is our static data for the entire calculation
        static_data = measure_prep_df.iloc[0].copy()
        main_logs.write("  - 成功获取最新的计量准备数据。\n")

        # --- FIX: Manually calculate and add 'net_premium' to static_data ---
        # The downstream calculator expects 'net_premium' for the amortization base.
        static_data['net_premium'] = D(static_data.get('premium', 0) or 0) - D(static_data.get('commission', 0) or 0)
        main_logs.write(f"  - 计算总净保费 (摊销基数): {static_data['net_premium']:.4f}\n")

        # --- NEW: Fetch invest_prop and calculate total_investment_component ---
        invest_prop = get_invest_prop(engine, policy_no, certi_no, contract_id)
        total_premium = D(static_data.get('premium', 0) or 0)
        static_data['total_investment_component'] = (total_premium * invest_prop).quantize(D(f'1e-{SCALE}'))
        main_logs.write(f"  - 获取投资成分比例: {invest_prop:.4f}\n")
        main_logs.write(f"  - 计算总投资成分: {static_data['total_investment_component']:.4f}\n")

        # Get rein_type to determine where to fetch loss from
        rein_type = static_data.get('rein_type', '1')

        # Step 1.2: Fetch all historical results for lookups (e.g., previous balances)
        all_measure_records_df = get_all_reinsurance_outward_measure_records(
            engine, policy_no, certi_no, contract_id
        )
        main_logs.write(f"  - 成功获取所有历史计算结果 ({len(all_measure_records_df)} 条记录)。\n")

    except Exception as e:
        main_logs.write(f"数据获取失败: {e}\n")
        calculation_logs.append({'month': 'N/A', 'result_df': None, 'logs': [main_logs.getvalue()]})
        return calculation_logs, final_result_df, pd.DataFrame(), {}

    # --- 2. Generate Timeline ---
    main_logs.write("步骤 2: 生成计算时间轴...\n")
    try:
        # Get all required discount rates once
        discount_rates_map = get_reinsurance_discount_rates(engine)

        # --- NEW LOGIC: Determine the effective start month ---
        ini_confirm = static_data.get('ini_confirm')
        if pd.isna(ini_confirm):
            raise ValueError("'ini_confirm' is missing and cannot determine start month.")
        
        # 获取签单日期：原保单用 under_write_date，批单用 certi_write_date
        # 优先从 static_data 获取，如果没有则从历史记录中获取
        under_write_date = static_data.get('under_write_date')
        certi_write_date = static_data.get('certi_write_date')
        
        # 如果 static_data 中没有，尝试从历史记录中获取
        if (pd.isna(under_write_date) or under_write_date is None or under_write_date == '') and not all_measure_records_df.empty:
            if 'under_write_date' in all_measure_records_df.columns:
                under_write_date = all_measure_records_df['under_write_date'].iloc[0]
        if (pd.isna(certi_write_date) or certi_write_date is None or certi_write_date == '') and not all_measure_records_df.empty:
            if 'certi_write_date' in all_measure_records_df.columns:
                certi_write_date = all_measure_records_df['certi_write_date'].iloc[0]
        
        # 根据 certi_no 判断使用哪个签单日期
        # certi_no 为空或 NULL 表示原保单，使用 under_write_date；否则使用 certi_write_date
        is_certi_no_empty = (certi_no is None or 
                            certi_no == '' or 
                            str(certi_no).strip().upper() in ['NA', 'N/A', 'NULL', 'NONE'])
        
        if is_certi_no_empty:
            # 原保单：使用签单日期 (under_write_date)
            sign_date = under_write_date
            sign_date_type = "签单日期 (under_write_date)"
        else:
            # 批单：使用批单签单日期 (certi_write_date)
            sign_date = certi_write_date
            sign_date_type = "批单签单日期 (certi_write_date)"
        
        if pd.isna(sign_date) or sign_date is None or sign_date == '':
            raise ValueError(f"无法获取签单日期：{sign_date_type} 为空。")
        
        # 处理日期格式：如果是字符串格式 'YYYYMMDD'，需要转换为日期对象
        # 初始计量日期取签单日期与 ini_confirm 的孰晚值
        if isinstance(sign_date, str) and len(sign_date) == 8 and sign_date.isdigit():
            # 格式为 'YYYYMMDD'，需要转换
            sign_date_parsed = pd.to_datetime(sign_date, format='%Y%m%d')
        else:
            sign_date_parsed = pd.to_datetime(sign_date)
        ini_confirm_parsed = pd.to_datetime(ini_confirm)
        effective_start_date = max(sign_date_parsed, ini_confirm_parsed)
        initial_booking_month = effective_start_date.strftime('%Y%m')
        
        main_logs.write(f"  - 批单号 (certi_no): {certi_no if not is_certi_no_empty else '(空-原保单)'}\n")
        main_logs.write(f"  - {sign_date_type}: {sign_date_parsed.strftime('%Y-%m-%d')}\n")
        main_logs.write(f"  - 初始确认日 (ini_confirm): {ini_confirm_parsed.strftime('%Y-%m-%d')}\n")
        main_logs.write(f"  - 初始计量日期 (取孰晚): {effective_start_date.strftime('%Y-%m-%d')}\n")
        main_logs.write(f"  - 初始计量月: {initial_booking_month}\n")

        # The calculation timeline starts from the initial booking month.
        start_month_dt = effective_start_date.to_pydatetime().date().replace(day=1)
        end_month_dt = datetime.strptime(measure_val_month, '%Y%m').date().replace(day=1)
        
        if start_month_dt > end_month_dt:
            raise ValueError(f"起始月份 {start_month_dt.strftime('%Y%m')} 不能晚于评估月份 {measure_val_month}。")

        # Generate list of months to calculate
        months_to_calculate = []
        current_month_dt = start_month_dt
        while current_month_dt <= end_month_dt:
            months_to_calculate.append(current_month_dt.strftime('%Y%m'))
            current_month_dt += relativedelta(months=1)
        
        main_logs.write(f"  - 计算期间: 从 {months_to_calculate[0]} 到 {months_to_calculate[-1]}\n")

        # --- NEW LOGIC: Lock in rate curve based on original ini_confirm month for IFIE ---
        # 利率曲线仍使用原始 ini_confirm 的月份，而不是孰晚后的日期
        original_ini_confirm_month = ini_confirm_parsed.strftime('%Y%m')
        locked_in_rate_curve = discount_rates_map.get(original_ini_confirm_month)
        if not locked_in_rate_curve:
            raise ValueError(f"在利率表中未找到用于锁定IFIE计算的初始确认月 '{original_ini_confirm_month}' 的利率曲线。")
        main_logs.write(f"  - 利率曲线 (用于IFIE) 锁定于初始确认月 (使用原始ini_confirm): {original_ini_confirm_month}\n")

        # --- Get all discount rates once ---
        try:
            all_discount_rates = get_reinsurance_discount_rates(engine)
            locked_in_rate_curve = all_discount_rates.get(original_ini_confirm_month)

            # --- DIAGNOSTIC LOG ---
            if locked_in_rate_curve:
                # Format the first 5 rates for logging to see what we actually got
                rates_preview = {k: v for k, v in list(locked_in_rate_curve.items())[:5]}
                main_logs.write(f"  - [DIAGNOSIS] 实际锁定的 '{original_ini_confirm_month}' 利率曲线内容 (前5期): {rates_preview}\n")
            else:
                main_logs.write(f"  - [DIAGNOSIS] 警告: 未能加载 '{original_ini_confirm_month}' 的锁定利率曲线。\n")
            # --- END DIAGNOSTIC LOG ---

            if not locked_in_rate_curve:
                raise ValueError(f"在利率表中未找到初始确认月 '{original_ini_confirm_month}' 的利率曲线。")
        except Exception as e:
            main_logs.write(f"锁定利率曲线失败: {e}\n")
            calculation_logs.append({'month': 'N/A', 'result_df': None, 'logs': [main_logs.getvalue()]})
            return calculation_logs, final_result_df, pd.DataFrame(), {}

        original_ini_confirm_dt = pd.to_datetime(static_data.get('ini_confirm')).to_pydatetime().date().replace(day=1)

        # Build cash flow timeline based on the single measure_prep record
        cashflow_df = build_reinsurance_outward_cost_timeline(static_data, initial_booking_month)
        
        # Create cash flow map by month
        cash_flows_map = {}
        for _, row in cashflow_df.iterrows():
            month = row['month']
            cash_flows_map[month] = {
                're_premium_cny_cash_flow': Decimal(str(row['premium'])),
                're_commission_cny_cash_flow': Decimal(str(row['commission']))
            }

    except Exception as e:
        main_logs.write(f"时间轴生成失败: {e}\n")
        calculation_logs.append({'month': 'N/A', 'result_df': None, 'logs': [main_logs.getvalue()]})
        return calculation_logs, final_result_df, pd.DataFrame(), {}

    # --- Get Direct Insurance Loss Component ---
    from core.data_fetcher.reinsurance_input_data import get_direct_insurance_loss_map
    try:
        main_logs.write(f"  - 开始获取底层直保业务的亏损金额...\n")
        direct_insurance_loss_map = get_direct_insurance_loss_map(engine, policy_no, certi_no)
        loss_info['direct_insurance_loss_map'] = {k: str(v) for k, v in direct_insurance_loss_map.items()} # for logging
        main_logs.write(f"  - 成功获取直保亏损金额。\n")
    except Exception as e:
        main_logs.write(f"获取直保亏损金额失败: {e}\n")
        # Continue with empty map
        direct_insurance_loss_map = {}


    # --- 3. Month-by-Month Calculation ---
    main_logs.write("步骤 3: 开始逐月计算...\n")
    previous_month_result_internal = {}
    
    # 1. 一次性获取所有需要的数据
    assumptions_map = get_reinsurance_outward_assumptions(engine)
    claim_model_map = get_reinsurance_claim_models(engine)
    
    all_monthly_results = []
    loss_info = {} # Initialize to store the last loss_info for the return value

    for i, current_month_str in enumerate(months_to_calculate):
        
        # Get cash flows for the current month
        cash_flows_for_month = cash_flows_map.get(current_month_str, {})

        current_val_dt = datetime.strptime(current_month_str, '%Y%m').date().replace(day=1)
        term_month = (current_val_dt.year - original_ini_confirm_dt.year) * 12 + (current_val_dt.month - original_ini_confirm_dt.month) + 1

        # Get loss data for the current month from direct insurance results
        loss_data = direct_insurance_loss_map.get(current_month_str)

        result_for_df, next_prev_result, month_logs_str = _calculate_one_month_outward(
            engine,
            current_month_str,
            static_data,
            cash_flows_for_month,
            previous_month_result_internal,
            locked_in_rate_curve,
            original_ini_confirm_month,
            term_month,
            rein_type,
            loss_data
        )
        
        # --- Get Loss Component for the CURRENT month ---
        try:
            lookup_certi_no = certi_no if certi_no and certi_no.strip() else 'NA'
            
            loss_info = get_underlying_loss_amount(
                engine=engine,
                policy_no=policy_no,
                certi_no=lookup_certi_no,
                rein_type=rein_type,
                val_month=current_month_str # Match on the same evaluation month
            )
            
            if loss_info['loss_amount'] not in ['未找到', '查询失败']:
                underlying_loss = float(loss_info['loss_amount'])
                share_rate = float(static_data.get('share_rate', 1.0))
                loss_amount = underlying_loss * share_rate
            else:
                loss_amount = 0
        except Exception as e:
            loss_amount = 0
            loss_info = {'loss_amount': f'查询失败: {e}', 'rein_type': rein_type}
        
        result_for_df['loss_component'] = loss_amount
        result_for_df['lrc_debt'] = result_for_df['closing_balance'] + loss_amount

        previous_month_result_internal = next_prev_result
        all_monthly_results.append(result_for_df)

        # --- Update Logs for the current month ---
        full_log_list = [f"--- 开始计算评估月: {current_month_str} ---", month_logs_str]
        log_loss_str = StringIO()
        log_loss_str.write("\n【获取亏损部分】:\n")
        if loss_info.get('loss_amount') not in ['未找到', '查询失败'] and '查询失败' not in str(loss_info.get('loss_amount')):
             log_loss_str.write(f"  - 从底层业务 ({'直保' if rein_type=='1' else '再保分入'}) 获取亏损: {underlying_loss:.4f}\n")
             log_loss_str.write(f"  - 应用分出比例: {share_rate:.4f}\n")
             log_loss_str.write(f"  -> 本月分出亏损 (loss_component): {loss_amount:.4f}\n")
        else:
             log_loss_str.write(f"  - 未找到或查询失败 ({loss_info.get('loss_amount')})，使用 0\n")
        log_loss_str.write(f"  -> 未到期责任资产 (lrc_debt): {result_for_df['closing_balance']:.4f} + {loss_amount:.4f} = {result_for_df['lrc_debt']:.4f}\n")
        
        full_log_list.append(log_loss_str.getvalue())
        
        calculation_logs.append({
            'month': current_month_str,
            'result_df': pd.DataFrame([result_for_df]),
            'logs': full_log_list
        })

    # --- 4. Finalize ---
    main_logs.write("步骤 4: 计量完成。\n")
    if all_monthly_results:
        final_result_df = pd.DataFrame(all_monthly_results)

    # Add main logs to the first entry
    if calculation_logs:
        calculation_logs[0]['logs'].insert(0, main_logs.getvalue())

    return calculation_logs, final_result_df, cashflow_df, loss_info


