import pandas as pd
from sqlalchemy.engine import Engine
from decimal import Decimal, ROUND_HALF_UP
from typing import Tuple, List, Dict
import datetime

from core.data_fetcher import measure_input_data as fetcher

# --- 常量定义 ---
# 设置Decimal的精度，确保与Java的BigDecimal行为一致
from decimal import getcontext
getcontext().prec = 38 # 设置全局精度
TEN_DIGITS = Decimal('1E-10') # 用于四舍五入的精度

INVESTMENT_RATIO = Decimal(0)
IFIE_RATIO = Decimal('0.5')

def calculate_unexpired_measure(engine: Engine, policy_no: str, certi_no: str, val_month: str) -> Tuple[pd.DataFrame, List[Dict]]:
    """
    根据给定的保单、批单和评估月，复刻Java逻辑计算未到期计量的所有结果，并返回详细的计算日志。
    """
    # 1. 获取所有计算所需的输入数据
    contract_data = fetcher.get_measure_source_data(engine, val_month, policy_no, certi_no)
    
    ini_confirm_month = pd.to_datetime(contract_data['ini_confirm']).strftime('%Y%m')
    # 确保 val_month 也在查询范围内
    end_period = pd.to_datetime(val_month, format='%Y%m')
    start_period = pd.to_datetime(ini_confirm_month, format='%Y%m')
    required_months = list(pd.date_range(start=start_period, end=end_period, freq='MS').strftime('%Y%m'))
    if val_month not in required_months:
        required_months.append(val_month)

    assumptions_map = fetcher.get_actuarial_assumptions_map(engine, required_months)
    discount_rates_map = fetcher.get_discount_rates_map(engine, required_months)
    claim_model_map = fetcher.get_claim_model_map(engine)
    
    # --- 修复: 移除 stat_date 参数，匹配新的函数签名 ---
    paid_premiums_map = fetcher.get_paid_premiums_map(engine, policy_no, certi_no)
    
    iacf_fol_map = fetcher.get_iacf_fol_map(engine, policy_no, certi_no)
    iacf_unfol_map = fetcher.get_iacf_unfol_map(engine, policy_no, certi_no)

    # 2. 从 ini_confirm 开始，按月滚动累计计算到 val_month
    rolling_results, monthly_logs = _perform_monthly_rolling(
        contract_data,
        val_month,
        assumptions_map,
        discount_rates_map,
        paid_premiums_map,
        iacf_fol_map,
        iacf_unfol_map
    )

    # 3. 计算未来现金流并进行亏损测试
    final_results, loss_test_logs = _perform_loss_test(
        contract_data,
        val_month,
        rolling_results,
        assumptions_map,
        discount_rates_map,
        claim_model_map
    )
    
    # 组合日志
    all_logs = monthly_logs + [loss_test_logs]

    # 6. 组装并返回最终结果
    result_df = pd.DataFrame([final_results])
    return result_df, all_logs


def _calculate_effective_days_in_period(ini_confirm: str, start_date: str, val_month: str, end_date: str) -> Dict[str, int]:
    """
    复刻Java的calculatePayments逻辑。
    计算从初始确认日到评估月（或保单止期，取早）之间，每个月的有效服务天数。
    有效服务天数从保险责任起期开始计算。
    """
    ini_confirm_date = pd.to_datetime(ini_confirm).date()
    start_date_dt = pd.to_datetime(start_date).date()
    val_month_end = (pd.to_datetime(val_month, format='%Y%m') + pd.offsets.MonthEnd(0)).date()
    end_date_dt = pd.to_datetime(end_date).date()

    effective_end_date = min(val_month_end, end_date_dt)
    
    period_map = {}
    current_month_start = pd.to_datetime(ini_confirm_date).replace(day=1).date()

    while current_month_start <= effective_end_date:
        month_key = current_month_start.strftime('%Y%m')
        current_month_end = (pd.to_datetime(current_month_start) + pd.offsets.MonthEnd(0)).date()
        
        # 计算当月在 [start_date, effective_end_date] 区间内的有效天数
        overlap_start = max(current_month_start, start_date_dt)
        overlap_end = min(current_month_end, effective_end_date)
        
        days = 0
        if overlap_start <= overlap_end:
            days = (overlap_end - overlap_start).days + 1
        
        period_map[month_key] = days
        current_month_start = (current_month_start + datetime.timedelta(days=32)).replace(day=1)
        
    return period_map

def _perform_monthly_rolling(contract_data, val_month, assumptions_map, discount_rates_map, paid_premiums_map, iacf_fol_map, iacf_unfol_map):
    """
    执行从初始确认日到评估月的逐月滚动计算。
    """
    monthly_logs = []

    # --- 1. 初始化变量 ---
    # 从 contract_data 获取基础信息 (并安全转换为Decimal)
    premium_cny = Decimal(str(contract_data.get('premium_cny', 0) or 0))
    ini_confirm_str = contract_data['ini_confirm']
    ini_confirm_date = pd.to_datetime(ini_confirm_str).date()
    start_date = contract_data['start_date']
    end_date = contract_data['end_date']
    class_code = contract_data['class_code']
    term_days = Decimal(str(contract_data.get('term', 0) or 0)) # 总保障天数
    
    # 获取初始确认日的精算假设
    ini_confirm_month_str = pd.to_datetime(ini_confirm_str).strftime('%Y%m')
    try:
        ini_confirm_assumption = assumptions_map[ini_confirm_month_str][class_code]
    except KeyError:
         raise ValueError(f"未在 assumptions_map 中找到 {ini_confirm_month_str}/{class_code} 的精算假设")

    # --- 2. 准备费用数据 ---
    total_iacf_amt = Decimal(0) # 用于摊销的总费用基础
    is_old_policy = ini_confirm_date < datetime.date(2024, 1, 1)
    actuarial_iacf = Decimal(0)
    
    actuarial_iacf_log = ""
    if is_old_policy:
        try:
            ini_confirm_assumption = assumptions_map[ini_confirm_month_str][class_code]
            acquisition_expense_ratio = Decimal(str(ini_confirm_assumption.get('acquisition_expense_ratio', 0) or 0))
            actuarial_iacf = (premium_cny * acquisition_expense_ratio).quantize(TEN_DIGITS, rounding=ROUND_HALF_UP)
            total_iacf_amt += actuarial_iacf
            actuarial_iacf_log = f"老单-精算假设费用: {premium_cny:.2f} * {acquisition_expense_ratio} = {actuarial_iacf:.2f}"
        except KeyError:
            raise ValueError(f"未在 assumptions_map 中找到老单 {ini_confirm_month_str}/{class_code} 的精算假设")

    # 按 202412 切分实际费用
    historical_iacf = Decimal(0)
    future_iacf_fol_by_month = {}  # 跟单费用按月份
    future_iacf_unfol_by_month = {}  # 非跟单费用按月份（累计值）
    
    # 分离处理跟单费用和非跟单费用
    # 跟单费用：按月份直接存储
    for month, amt in iacf_fol_map.items():
        amt_decimal = Decimal(str(amt or 0))
        if month <= '202412':
            historical_iacf += amt_decimal
        else:
            future_iacf_fol_by_month[month] = amt_decimal
    
    # 非跟单费用：按月份存储累计值
    for month, amt in iacf_unfol_map.items():
        amt_decimal = Decimal(str(amt or 0))
        if month <= '202412':
            historical_iacf += amt_decimal
        else:
            future_iacf_unfol_by_month[month] = amt_decimal

    # 将历史费用（<=202412）累加到摊销总额中
    total_iacf_amt += historical_iacf

    # --- 3. 初始化滚动变量 ---
    cumulative_received_premiums = Decimal(0)
    cumulative_premiums = Decimal(0)
    cumulative_iacf = Decimal(0)
    cumulative_ifie = Decimal(0)
    opening_balance = Decimal(0)
    served_days = 0

    period_map = _calculate_effective_days_in_period(ini_confirm_str, start_date, val_month, end_date)

    # --- 4. 开始逐月滚动循环 ---
    month_counter = 0
    sorted_months = sorted(period_map.keys())

    for month in sorted_months:
        days_in_month = period_map[month]
        month_counter += 1
        
        month_log = { "month": month, "logs": [f"--- 开始计算月份: {month} ---", f"本月有效服务天数: {days_in_month}"] }
        if month_counter == 1:
            if actuarial_iacf_log:
                month_log["logs"].append(actuarial_iacf_log)
            month_log["logs"].append(f"所有期间实际跟单费用总额: {sum(Decimal(str(v or 0)) for v in iacf_fol_map.values()):.2f}")
            month_log["logs"].append(f"所有期间实际非跟单费用总额: {sum(Decimal(str(v or 0)) for v in iacf_unfol_map.values()):.2f}")
            month_log["logs"].append(f" -> 初始摊销基数(历史费用<=202412): {total_iacf_amt:.2f}")
            month_log["logs"].append(f" -> 说明: 对于202501及以后，摊销基数将动态累加新增的非跟单费用")

        init_month_rate_map = discount_rates_map.get(ini_confirm_month_str, {})
        dis_rate = Decimal(str(init_month_rate_map.get(month_counter, 0) or 0))
        month_log["logs"].append(f"获取初始确认月({ini_confirm_month_str})的第 {month_counter} 个月远期利率: {dis_rate:.10f}")
        
        # --- 最终的、按月分配的现金流逻辑 (根据 202412 切分) ---
        iacf_cashflow_current = Decimal(0)
        
        # 规则1: 如果是首月，则计入所有历史费用 (<=202412) 和老单精算费用
        if month_counter == 1:
            iacf_cashflow_current += historical_iacf
            month_log["logs"].append(f"规则1(首月): 计入202412及之前的实际费用现金流: {historical_iacf:.2f}")

            # 如果是老单，再额外计入精算假设费用
            if is_old_policy:
                iacf_cashflow_current += actuarial_iacf
                month_log["logs"].append(f"规则1.1(老单首月): 额外计入精算假设费用现金流: {actuarial_iacf:.2f}")

        # 规则2: 如果当前月份晚于202412，则计入当月的实际费用
        if month > '202412':
            # 2.1 处理跟单费用（直接使用当月值）
            current_month_fol_iacf = future_iacf_fol_by_month.get(month, Decimal(0))
            iacf_cashflow_current += current_month_fol_iacf
            if current_month_fol_iacf > 0:
                month_log["logs"].append(f"规则2.1(>=202501): 计入当期({month})的跟单费用现金流: {current_month_fol_iacf:.2f}")
            
            # 2.2 处理非跟单费用（需要特殊处理：202501直接使用累计值，202502+使用差值）
            current_month_unfol_accumulated = future_iacf_unfol_by_month.get(month, Decimal(0))
            current_month_unfol_new = Decimal(0)
            
            if month == '202501':
                # 202501月份：直接使用累计值作为新增
                current_month_unfol_new = current_month_unfol_accumulated
                if current_month_unfol_new > 0:
                    month_log["logs"].append(f"规则2.2(202501): 计入当期({month})的非跟单费用累计值作为新增: {current_month_unfol_new:.2f}")
                    # 更新摊销基数：202412的total_iacf + 202501新增值
                    total_iacf_amt += current_month_unfol_new
                    month_log["logs"].append(f"规则2.2.1(202501): 摊销基数更新，新增非跟单费用 {current_month_unfol_new:.2f}，当前摊销基数: {total_iacf_amt:.2f}")
                    
            elif month >= '202502':
                # 202502及以后：使用差值（当月累计 - 上月累计）
                prev_month = (pd.to_datetime(month, format='%Y%m') - pd.DateOffset(months=1)).strftime('%Y%m')
                prev_month_unfol_accumulated = future_iacf_unfol_by_month.get(prev_month, Decimal(0))
                current_month_unfol_new = current_month_unfol_accumulated - prev_month_unfol_accumulated
                
                if current_month_unfol_new > 0:
                    month_log["logs"].append(f"规则2.2(>=202502): 计算当期({month})的非跟单费用新增 = {current_month_unfol_accumulated:.2f} - {prev_month_unfol_accumulated:.2f} = {current_month_unfol_new:.2f}")
                    # 更新摊销基数：上期摊销基数 + 当月新增值
                    total_iacf_amt += current_month_unfol_new
                    month_log["logs"].append(f"规则2.2.1(>=202502): 摊销基数更新，新增非跟单费用 {current_month_unfol_new:.2f}，当前摊销基数: {total_iacf_amt:.2f}")
                elif current_month_unfol_new < 0:
                    month_log["logs"].append(f"规则2.2(>=202502): 警告 - 当期({month})的非跟单费用累计值小于上月，差值: {current_month_unfol_new:.2f}，按0处理")
                    current_month_unfol_new = Decimal(0)
            
            iacf_cashflow_current += current_month_unfol_new

        month_log["logs"].append(f" -> 当期最终获取费用现金流: {iacf_cashflow_current:.2f}")

        premium_cashflow = Decimal(str(paid_premiums_map.get(month, 0) or 0))
        if month_counter == 1:
            historical_premium = sum(Decimal(str(v)) for k, v in paid_premiums_map.items() if k <= month)
            premium_cashflow = historical_premium
        cumulative_received_premiums += premium_cashflow
        month_log["logs"].append(f"当期实收保费现金流: {premium_cashflow:.2f}")
        month_log["logs"].append(f" -> 累计实收保费更新为: {cumulative_received_premiums:.2f}")

        served_days += days_in_month
        cumulative_proportion = (Decimal(served_days) / term_days).quantize(TEN_DIGITS, rounding=ROUND_HALF_UP) if term_days > 0 else Decimal(0)
        month_log["logs"].append(f"累计服务天数: {served_days} / {term_days}")
        month_log["logs"].append(f"计算累计服务比例: {cumulative_proportion:.10f}")

        opening_balance_ifie = (opening_balance * dis_rate).quantize(TEN_DIGITS, rounding=ROUND_HALF_UP)
        premium_cashflow_ifie = (premium_cashflow * dis_rate * IFIE_RATIO).quantize(TEN_DIGITS, rounding=ROUND_HALF_UP)
        iacf_cashflow_ifie = (iacf_cashflow_current * dis_rate * IFIE_RATIO).quantize(TEN_DIGITS, rounding=ROUND_HALF_UP) 
        current_ifie = opening_balance_ifie + premium_cashflow_ifie - iacf_cashflow_ifie
        cumulative_ifie += current_ifie
        month_log["logs"].append(f"【计算当期利息】:")
        month_log["logs"].append(f"  公式: 期初余额利息 + 实收保费利息 - 获取费用利息")
        month_log["logs"].append(f"  = ({opening_balance:.2f} * {dis_rate:.6f}) + ({premium_cashflow:.2f} * {dis_rate:.6f} * {IFIE_RATIO}) - ({iacf_cashflow_current:.2f} * {dis_rate:.6f} * {IFIE_RATIO}) = {current_ifie:.10f}")
        month_log["logs"].append(f" -> 累计利息更新为: {cumulative_ifie:.10f}")
        
        current_premiums = ((premium_cny + cumulative_ifie) * cumulative_proportion - cumulative_premiums).quantize(TEN_DIGITS, rounding=ROUND_HALF_UP)
        cumulative_premiums += current_premiums
        month_log["logs"].append(f"【计算当期确认保费】:")
        month_log["logs"].append(f"  公式: (总保费 + 累计利息) * 累计服务比例 - 上期累计确认保费")
        month_log["logs"].append(f"  = ({premium_cny:.2f} + {cumulative_ifie:.10f}) * {cumulative_proportion:.10f} - {cumulative_premiums - current_premiums:.10f} = {current_premiums:.10f}")
        month_log["logs"].append(f" -> 累计确认保费更新为: {cumulative_premiums:.10f}")

        # --- e. 计算当期确认获取费用 (摊销) ---
        current_iacf = (total_iacf_amt * cumulative_proportion - cumulative_iacf).quantize(TEN_DIGITS, rounding=ROUND_HALF_UP)
        cumulative_iacf += current_iacf
        month_log["logs"].append(f"【计算当期确认获取费用】:")
        month_log["logs"].append(f"  公式: 总获取费用 * 累计服务比例 - 上期累计确认获取费用")
        month_log["logs"].append(f"  = {total_iacf_amt:.2f} * {cumulative_proportion:.10f} - {cumulative_iacf - current_iacf:.10f} = {current_iacf:.10f}")
        month_log["logs"].append(f" -> 累计确认获取费用更新为: {cumulative_iacf:.10f}")

        closing_balance = opening_balance + premium_cashflow - iacf_cashflow_current + current_ifie - current_premiums + current_iacf
        month_log["logs"].append(f"【计算期末非亏损余额】:")
        month_log["logs"].append(f"  公式: 期初余额 + 实收保费 - 获取费用现金流 + 当期利息 - 当期确认保费 + 当期确认获取费用")
        month_log["logs"].append(f"  = {opening_balance:.4f} + {premium_cashflow:.2f} - {iacf_cashflow_current:.2f} + {current_ifie:.10f} - {current_premiums:.10f} + {current_iacf:.10f} = {closing_balance:.10f}")
        
        opening_balance = closing_balance
        monthly_logs.append(month_log)

    rolling_results = {
        "cumulative_premiums": cumulative_premiums, "cumulative_iacf": cumulative_iacf,
        "cumulative_ifie": cumulative_ifie, "cumulative_received_premiums": cumulative_received_premiums,
        "closing_balance": opening_balance, "total_iacf_amt": total_iacf_amt,
        "served_days": served_days, "term_days": term_days, "month_counter": month_counter,
    }
    return rolling_results, monthly_logs

def _get_pv_maintenance(amt: Decimal, n: int, months_rate_map: Dict[int, object], val_month: str) -> Tuple[Decimal, pd.DataFrame]:
    if amt == Decimal(0) or n <= 0:
        return Decimal(0), pd.DataFrame()

    details = []
    monthly_amt = (amt / Decimal(n)).quantize(TEN_DIGITS, rounding=ROUND_HALF_UP)
    product = Decimal(1)
    pv_maintenance = Decimal(0)
    start_period = pd.to_datetime(val_month, format='%Y%m')

    for i in range(1, n + 1):
        current_month = (start_period + pd.DateOffset(months=i)).strftime('%Y%m')
        rate = Decimal(str(months_rate_map.get(i, 0) or 0))
        product *= (Decimal(1) + rate)
        cumulative_discount_factor = (Decimal(1) / product)
        discounted_amt = (monthly_amt * cumulative_discount_factor).quantize(TEN_DIGITS, rounding=ROUND_HALF_UP)
        pv_maintenance += discounted_amt
        details.append({
            '年月': current_month,
            '现金流': monthly_amt,
            '当期远期利率': rate,
            '累计折现系数': cumulative_discount_factor,
            '折现值': discounted_amt
        })
    details_df = pd.DataFrame(details).set_index('年月').T
    return pv_maintenance, details_df


def _get_pv_loss(amt: Decimal, n: int, claim_factor_arr: list, months_rate_map: Dict[int, object], val_month: str) -> Tuple[Decimal, pd.DataFrame]:
    # This function is called by _perform_loss_test, which has a `loss_test_logs` object.
    # It does not have a `logs` parameter to write to. I cannot add logs here without larger refactoring.
    # The user wants to see logs. I will add print statements to stderr.
    
    import sys
    print("\n--- 进入【直保专用】_get_pv_loss 函数 (版本: FINAL_DEBUG_20251111_V2) ---", file=sys.stderr)
    print(f"  - 输入 amt (总额): {amt:.4f}", file=sys.stderr)
    print(f"  - 输入 n (剩余月份): {n}", file=sys.stderr)
    # Truncate for readability
    print(f"  - 输入 claim_factor_arr (赔付模式, 前5个): {claim_factor_arr[:5]}", file=sys.stderr)

    if amt == Decimal(0) or n <= 0:
        return Decimal(0), pd.DataFrame()
        
    claim_factor = [Decimal(str(d)) for d in claim_factor_arr]
    avg_amt = (amt / Decimal(n)).quantize(TEN_DIGITS, rounding=ROUND_HALF_UP)
    print(f"  - 计算 avg_amt (月均赔付): {avg_amt:.4f}", file=sys.stderr)

    claim_factor_applied = [(avg_amt * factor).quantize(TEN_DIGITS, rounding=ROUND_HALF_UP) for factor in claim_factor]
    if claim_factor_applied:
        print(f"  - 计算 claim_factor_applied[0] (首月现金流基数): {claim_factor_applied[0]:.4f}", file=sys.stderr)

    k, result_length = n - 1, len(claim_factor_applied) + n - 1
    result = [Decimal(0)] * result_length
    prefix = [Decimal(0)] * (len(claim_factor_applied) + 1)
    for i in range(len(claim_factor_applied)): prefix[i + 1] = prefix[i] + claim_factor_applied[i]
    for j in range(result_length):
        start, end = max(0, j - k), min(j, len(claim_factor_applied) - 1)
        result[j] = prefix[end + 1] - prefix[start]
    
    details = []
    product = Decimal(1)
    pv_loss = Decimal(0)
    start_period = pd.to_datetime(val_month, format='%Y%m')

    for i in range(len(result)):
        current_month = (start_period + pd.DateOffset(months=i + 1)).strftime('%Y%m')
        cash_flow = result[i]
        rate = Decimal(str(months_rate_map.get(i + 1, 0) or 0))
        product *= (Decimal(1) + rate)
        cumulative_discount_factor = (Decimal(1) / product)
        discounted_amt = (cash_flow * cumulative_discount_factor).quantize(TEN_DIGITS, rounding=ROUND_HALF_UP)
        pv_loss += discounted_amt
        details.append({
            '年月': current_month,
            '现金流': cash_flow,
            '当期远期利率': rate,
            '累计折现系数': cumulative_discount_factor,
            '折现值': discounted_amt
        })
    details_df = pd.DataFrame(details).set_index('年月').T
    return pv_loss, details_df

def _perform_loss_test(contract_data, val_month, rolling_results, assumptions_map, discount_rates_map, claim_model_map):
    loss_test_logs = { "month": f"{val_month} 亏损测试", "logs": [f"--- 开始亏损测试 ---"] }
    premium_cny = Decimal(str(contract_data.get('premium_cny', 0) or 0))
    class_code = contract_data['class_code']
    closing_balance, cumulative_received_premiums = rolling_results['closing_balance'], rolling_results['cumulative_received_premiums']
    
    # --- 新增逻辑: 精算假设回退机制 ---
    current_assumption = assumptions_map.get(val_month, {}).get(class_code)
    # 检查关键假设是否有效（例如，赔付率不应为0）
    is_invalid = not current_assumption or Decimal(str(current_assumption.get('loss_ratio', 0) or 0)) == 0

    if is_invalid:
        prev_month_dt = pd.to_datetime(val_month, format='%Y%m') - pd.DateOffset(months=1)
        prev_month_str = prev_month_dt.strftime('%Y%m')
        loss_test_logs['logs'].append(f"警告: 未找到或无效的 {val_month} 精算假设，尝试使用 {prev_month_str} 的数据。")
        
        current_assumption = assumptions_map.get(prev_month_str, {}).get(class_code)

        if not current_assumption or Decimal(str(current_assumption.get('loss_ratio', 0) or 0)) == 0:
            raise ValueError(f"在 {val_month} 和 {prev_month_str} 都未找到有效的精算假设 for class_code {class_code}")

    loss_ratio = Decimal(str(current_assumption.get('loss_ratio', 0) or 0))
    indirect_claims_expense_ratio = Decimal(str(current_assumption.get('indirect_claims_expense_ratio', 0) or 0))
    maintenance_expense_ratio = Decimal(str(current_assumption.get('maintenance_expense_ratio', 0) or 0))
    ra_ratio = Decimal(str(current_assumption.get('ra', 0) or 0))
    
    future_proportion = Decimal(1) - (Decimal(rolling_results['served_days']) / rolling_results['term_days']) if rolling_results['term_days'] > 0 else Decimal(0)
    unexpired_premium = (premium_cny * future_proportion).quantize(TEN_DIGITS, rounding=ROUND_HALF_UP)
    future_receivable_premiums = premium_cny - cumulative_received_premiums
    future_loss = (unexpired_premium * loss_ratio * (Decimal(1) + indirect_claims_expense_ratio)).quantize(TEN_DIGITS, rounding=ROUND_HALF_UP)
    future_maintenance = (unexpired_premium * maintenance_expense_ratio).quantize(TEN_DIGITS, rounding=ROUND_HALF_UP)

    ini_confirm_dt, end_date_dt = pd.to_datetime(contract_data['ini_confirm']), pd.to_datetime(contract_data['end_date'])
    total_months = (end_date_dt.year - ini_confirm_dt.year) * 12 + (end_date_dt.month - ini_confirm_dt.month) + 1
    remaining_months = total_months - rolling_results['month_counter']
    val_month_rates, claim_factors = discount_rates_map.get(val_month, {}), claim_model_map.get(class_code, [])
    
    pv_future_loss, loss_pv_details_df = _get_pv_loss(future_loss, remaining_months, claim_factors, val_month_rates, val_month)
    pv_future_maintenance, maintenance_pv_details_df = _get_pv_maintenance(future_maintenance, remaining_months, val_month_rates, val_month)
    
    risk_adjustment = ((pv_future_maintenance + pv_future_loss) * ra_ratio).quantize(TEN_DIGITS, rounding=ROUND_HALF_UP)
    future_cash_flow = pv_future_loss + pv_future_maintenance + risk_adjustment - future_receivable_premiums
    net_future_cash_flow = future_cash_flow - closing_balance
    lrc_loss_amt = max(Decimal(0), net_future_cash_flow) if premium_cny >= 0 else min(Decimal(0), net_future_cash_flow)

    loss_test_logs['logs'].extend([
        f"【获取 {val_month} 精算假设】:",
        f"  赔付率: {loss_ratio}, 间接理赔费用率: {indirect_claims_expense_ratio}, 维持费用率: {maintenance_expense_ratio}, 风险调整率: {ra_ratio}",
        "---------------------------------",
        f"【计算未来现金流】:",
        f"  1. 未来服务比例 = 1 - ({rolling_results['served_days']} / {rolling_results['term_days']}) = {future_proportion:.10f}",
        f"  2. 未到期保费 = 总保费 * 未来服务比例 = {premium_cny:.2f} * {future_proportion:.10f} = {unexpired_premium:.10f}",
        f"  3. 未来应收保费 = 总保费 - 累计已收保费 = {premium_cny:.2f} - {cumulative_received_premiums:.2f} = {future_receivable_premiums:.2f}",
        f"  4. 未来赔付成本 = 未到期保费 * 赔付率 * (1 + 间接理赔费用率) = {unexpired_premium:.2f} * {loss_ratio} * (1 + {indirect_claims_expense_ratio}) = {future_loss:.10f}",
        f"  5. 未来维持费用 = 未到期保费 * 维持费用率 = {unexpired_premium:.2f} * {maintenance_expense_ratio} = {future_maintenance:.10f}",
        f"  6. 剩余服务月数 = {remaining_months}",
        f"  7. 未来赔付成本折现PV = {pv_future_loss:.10f}",
        f"  8. 未来维持费用折现PV = {pv_future_maintenance:.10f}",
        f"  9. 风险调整 = (未来赔付成本PV + 未来维持费用PV) * 风险调整率 = ({pv_future_loss:.2f} + {pv_future_maintenance:.2f}) * {ra_ratio} = {risk_adjustment:.10f}",
        f"  10. 未来净现金流 = 未来赔付PV + 未来维持PV + 风险调整 - 未来应收保费 = {pv_future_loss:.2f} + {pv_future_maintenance:.2f} + {risk_adjustment:.2f} - {future_receivable_premiums:.2f} = {future_cash_flow:.10f}",
        "---------------------------------",
        f"【计算亏损金额】:",
        f"  期末非亏损余额: {closing_balance:.10f}",
        f"  亏损测试余额 = 未来净现金流 - 期末非亏损余额 = {future_cash_flow:.2f} - {closing_balance:.2f} = {net_future_cash_flow:.10f}",
        f"  最终亏损合同负债 (LRC Loss) = max(0, 亏损测试余额) = {lrc_loss_amt:.10f}"
    ])

    final_results = {
        **rolling_results, "lrc_no_loss_amt": closing_balance, "lrc_loss_amt": lrc_loss_amt,
        "lrc_debt": closing_balance + lrc_loss_amt, "unexpired_premium": unexpired_premium,
        "future_receivable_premiums": future_receivable_premiums, "pv_future_compensation": pv_future_loss,
        "pv_future_maintenance": pv_future_maintenance, "risk_adjustment": risk_adjustment,
        "future_cash_flow": future_cash_flow,
        "loss_pv_details_df": loss_pv_details_df,
        "maintenance_pv_details_df": maintenance_pv_details_df,
    }
    return final_results, loss_test_logs
