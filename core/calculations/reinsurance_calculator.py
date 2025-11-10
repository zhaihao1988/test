import pandas as pd
from decimal import Decimal, getcontext
from datetime import datetime
from dateutil.relativedelta import relativedelta
from calendar import monthrange
from io import StringIO
from sqlalchemy import Engine
from typing import List, Dict, Any, Tuple

from test.core.data_fetcher.reinsurance_data import get_reinsurance_inward_data, get_reinsurance_measure_prep_data
from test.core.data_fetcher.reinsurance_input_data import get_reinsurance_assumptions, get_reinsurance_discount_rates

# --- Global Settings ---
getcontext().prec = 38
getcontext().rounding = 'ROUND_HALF_UP'
SCALE = 10

def _calculate_one_month(
    val_month: str,
    static_data: Dict[str, Any],
    cash_flows: Dict[str, Decimal],
    prev_result: Dict[str, Decimal],
    assumptions: Dict[str, Any],
    discount_rates: Dict[int, float]
) -> Tuple[Dict[str, Any], str]:
    """Calculates all metrics for a single evaluation month based on the flowchart logic."""
    logs = StringIO()
    result = {}
    D = Decimal

    # --- 1. Initial Data and Parameters ---
    logs.write("步骤 1: 初始化数据\n")
    pi_start_date = static_data['pi_start_date']
    pi_end_date = static_data['pi_end_date']
    total_premium = D(static_data.get('premium', 0))
    total_commission = D(static_data.get('commission', 0))
    total_brokerage = D(static_data.get('brokerage', 0))
    total_iacf_unfol = D(static_data.get('iacf_unfol', 0)) # This is the total from the latest measure prep

    # Total acquisition costs for amortization
    total_acquisition_cost = total_commission + total_brokerage + total_iacf_unfol

    # Cash flows for the current month
    current_premium_cash_flow = cash_flows.get('premium', D(0))
    current_commission_cash_flow = cash_flows.get('commission', D(0))
    current_brokerage_cash_flow = cash_flows.get('brokerage', D(0))
    current_iacf_unfol_cash_flow = cash_flows.get('iacf_unfol', D(0))
    current_net_premium_cash_flow = current_premium_cash_flow - current_commission_cash_flow - current_brokerage_cash_flow
    
    logs.write(f"  - 当期保费现金流: {current_premium_cash_flow:.4f}\n")
    logs.write(f"  - 当期净保费现金流 (保费 - 佣金 - 经纪费): {current_net_premium_cash_flow:.4f}\n")
    logs.write(f"  - 当期非跟单获取费用现金流: {current_iacf_unfol_cash_flow:.4f}\n")

    # Previous month's results
    prev_lrc_no_loss = prev_result.get('lrc_no_loss_amt', D(0))
    prev_acc_insurance_revenue = prev_result.get('acc_insurance_revenue', D(0))
    prev_acc_iacf_unfol_amortization = prev_result.get('acc_iacf_unfol_amortization', D(0))
    prev_acc_ifie = prev_result.get('acc_ifie', D(0))

    logs.write(f" -> 期初非亏损余额: {prev_lrc_no_loss:.4f}\n")
    logs.write(f" -> 上期累计确认保费: {prev_acc_insurance_revenue:.4f}\n")
    logs.write(f" -> 上期累计非跟单费用摊销: {prev_acc_iacf_unfol_amortization:.4f}\n")
    logs.write(f" -> 上期累计IFIE: {prev_acc_ifie:.4f}\n")

    # --- 2. Amortization Calculation ---
    logs.write("\n【计算累计服务比例】:\n")
    val_date = datetime.strptime(val_month, '%Y%m').date()
    val_date = val_date.replace(day=monthrange(val_date.year, val_date.month)[1]) # End of month

    total_days = D((pi_end_date - pi_start_date).days + 1)
    elapsed_days = D(0)
    if val_date >= pi_start_date:
        elapsed_days = D((min(val_date, pi_end_date) - pi_start_date).days + 1)
    
    amortized_ratio = elapsed_days / total_days if total_days > 0 else D(0)
    logs.write(f"  累计服务天数: {elapsed_days} / {total_days}\n")
    logs.write(f"  -> 累计服务比例: {amortized_ratio:.10f}\n")

    # --- 3. IFIE (Interest) Calculation ---
    logs.write("\n【计算当期IFIE (未到期利息)】:\n")
    # For simplicity, using a placeholder monthly rate. A real implementation would fetch this.
    monthly_rate = D(discount_rates.get(1, 0.0012)) # Approximation
    ifie_from_opening_balance = prev_lrc_no_loss * monthly_rate
    ifie_from_net_premium = current_net_premium_cash_flow * monthly_rate * D('0.5')
    ifie_from_iacf_unfol = current_iacf_unfol_cash_flow * monthly_rate * D('0.5')
    current_ifie = (ifie_from_opening_balance + ifie_from_net_premium - ifie_from_iacf_unfol).quantize(D(f'1e-{SCALE}'))
    acc_ifie = (prev_acc_ifie + current_ifie).quantize(D(f'1e-{SCALE}'))
    logs.write(f"  公式: 上月余额利息 + 本月净保费利息 - 本月非跟单费用利息\n")
    logs.write(f"  = ({prev_lrc_no_loss:.4f} * {monthly_rate:.12f}) + ({current_net_premium_cash_flow:.4f} * {monthly_rate:.12f} * 0.5) - ({current_iacf_unfol_cash_flow:.4f} * {monthly_rate:.12f} * 0.5) = {current_ifie:.4f}\n")
    logs.write(f"  -> 累计IFIE更新为: {acc_ifie:.4f}\n")

    # --- 4. Insurance Revenue ---
    logs.write("\n【计算当期确认保费】:\n")
    logs.write(f"  公式: ((总净保费 + 累计IFIE) * 累计服务比例) - 上期累计确认保费\n")
    total_net_premium = total_premium - total_commission - total_brokerage
    acc_insurance_revenue = ((total_net_premium + acc_ifie) * amortized_ratio).quantize(D(f'1e-{SCALE}'))
    current_insurance_revenue = (acc_insurance_revenue - prev_acc_insurance_revenue).quantize(D(f'1e-{SCALE}'))
    logs.write(f"  = (({total_net_premium:.4f} + {acc_ifie:.4f}) * {amortized_ratio:.10f}) - {prev_acc_insurance_revenue:.4f} = {current_insurance_revenue:.4f}\n")
    logs.write(f"  -> 累计确认保费更新为: {acc_insurance_revenue:.4f}\n")

    # --- 5. Acquisition Costs Amortization (iacf_unfol only) ---
    logs.write("\n【计算当期非跟单获取费用摊销】:\n")
    logs.write(f"  公式: 总非跟单获取费用 * 累计服务比例 - 上期累计摊销\n")
    total_iacf_unfol = static_data.get('total_iacf_unfol', D(0))
    acc_iacf_unfol_amortization = (total_iacf_unfol * amortized_ratio).quantize(D(f'1e-{SCALE}'))
    current_iacf_unfol_amortization = (acc_iacf_unfol_amortization - prev_acc_iacf_unfol_amortization).quantize(D(f'1e-{SCALE}'))
    logs.write(f"  = {total_iacf_unfol:.4f} * {amortized_ratio:.10f} - {prev_acc_iacf_unfol_amortization:.4f} = {current_iacf_unfol_amortization:.4f}\n")
    logs.write(f"  -> 累计非跟单费用摊销更新为: {acc_iacf_unfol_amortization:.4f}\n")
    
    # --- 6. Investment Component ---
    logs.write("\n【当期投资成分确认】: 0.0 (暂未实现)\n")

    # --- 7. Non-Onerous LRC Calculation ---
    logs.write("\n【计算期末非亏损余额】:\n")
    logs.write(f"  公式: 期初余额 + 净保费现金流 - 非跟单费用现金流 + 当期IFIE - 当期确认收入 + 当期非跟单费用摊销\n")
    lrc_no_loss_amt = (
        prev_lrc_no_loss + 
        current_net_premium_cash_flow - 
        current_iacf_unfol_cash_flow + 
        current_ifie - 
        current_insurance_revenue + 
        current_iacf_unfol_amortization
    ).quantize(D(f'1e-{SCALE}'))
    logs.write(f"  = {prev_lrc_no_loss:.4f} + {current_net_premium_cash_flow:.4f} - {current_iacf_unfol_cash_flow:.4f} + {current_ifie:.4f} - {current_insurance_revenue:.4f} + {current_iacf_unfol_amortization:.4f} = {lrc_no_loss_amt:.4f}\n")
    logs.write(f"  -> 期末非亏损余额 (LRC_no_loss_amt) = {lrc_no_loss_amt:.4f}\n")

    # --- Onerous Test (placeholder, as it uses gross amounts) ---
    lrc_loss_amt, loss_test_logs, pv_loss_df, pv_maintenance_df = _perform_onerous_test(
        val_month,
        static_data,
        lrc_no_loss_amt,
        amortized_ratio,
        assumptions, # Pass assumptions
        discount_rates # Pass discount rates
    )
    logs.write(loss_test_logs)
    
    # --- 8. Final Results ---
    result = {
        'val_month': val_month,
        'lrc_no_loss_amt': float(lrc_no_loss_amt),
        'lrc_loss_amt': float(lrc_loss_amt),
        'current_insurance_revenue': float(current_insurance_revenue),
        'current_acquisition_cost': float(current_iacf_unfol_amortization), # Changed from current_acquisition_cost
        'current_net_cash_flow': float(current_net_premium_cash_flow), # Changed from current_net_cash_flow
        'acc_insurance_revenue': float(acc_insurance_revenue),
        'acc_acquisition_cost': float(acc_iacf_unfol_amortization), # Changed from acc_acquisition_cost
        'acc_ifie': float(acc_ifie), # Added acc_ifie
        'amortized_ratio': float(amortized_ratio),
        'loss_pv_details_df': pv_loss_df,  # Add detailed PV DataFrame for loss
        'maintenance_pv_details_df': pv_maintenance_df,  # Add detailed PV DataFrame for maintenance
    }

    # For next iteration
    internal_result_for_next_loop = {
        'lrc_no_loss_amt': lrc_no_loss_amt,
        'acc_insurance_revenue': acc_insurance_revenue,
        'acc_iacf_unfol_amortization': acc_iacf_unfol_amortization,
        'acc_ifie': acc_ifie,
    }
    
    return result, internal_result_for_next_loop, logs.getvalue()

def calculate_reinsurance_unexpired_measure(
    engine: Engine,
    measure_val_month: str,
    contract_id: str,
    policy_no: str,
    certi_no: str
) -> Tuple[List[Dict[str, Any]], pd.DataFrame, pd.DataFrame]:
    """Orchestrates the new month-by-month calculation for reinsurance LRC."""
    main_logs = StringIO()
    calculation_logs = []
    final_result_df = pd.DataFrame()

    # --- 1. Data Fetching ---
    main_logs.write("步骤 1: 获取计量所需数据...\n")
    try:
        original_df = get_reinsurance_inward_data(engine, contract_id)
        measure_prep_df = get_reinsurance_measure_prep_data(engine, contract_id)

        if original_df.empty:
            raise ValueError(f"在 'bi_to_cas25.ri_pp_re_mon_arr_in' 中未找到合约 '{contract_id}' 的原始记录。")
        if measure_prep_df.empty:
            raise ValueError(f"在 'public.int_t_pp_re_mon_arr_in_new' 中未找到合约 '{contract_id}' 的计量准备数据。")

        original_record = original_df.iloc[0]
        measure_prep_record = measure_prep_df.iloc[0]
        main_logs.write("  - 成功获取原始记录和计量准备数据。\n")
        
        # Combine into a single static data object
        static_data = {**original_record.to_dict(), **measure_prep_record.to_dict()}

    except Exception as e:
        main_logs.write(f"数据获取失败: {e}\n")
        calculation_logs.append({'month': 'N/A', 'result_df': None, 'logs': [main_logs.getvalue()]})
        return calculation_logs, final_result_df, pd.DataFrame()

    # --- 2. Generate Timeline ---
    main_logs.write("步骤 2: 生成计算时间轴...\n")
    try:
        confirm_date_val = static_data.get('confirm_date')
        if not confirm_date_val or pd.isna(confirm_date_val):
            raise ValueError("'confirm_date' 在原始记录中为空，无法确定起始计算月份。")

        # Ensure we are comparing date objects, not datetime objects
        start_month_dt = pd.to_datetime(confirm_date_val).date().replace(day=1)
        end_month_dt = datetime.strptime(measure_val_month, '%Y%m').date().replace(day=1)
        
        if start_month_dt > end_month_dt:
            raise ValueError(f"起始月份 {start_month_dt.strftime('%Y%m')} 不能晚于评估月份 {measure_val_month}。")

        months_to_calculate = []
        current_month_dt = start_month_dt
        while current_month_dt <= end_month_dt:
            months_to_calculate.append(current_month_dt.strftime('%Y%m'))
            current_month_dt += relativedelta(months=1)
        
        main_logs.write(f"  - 计算期间: 从 {months_to_calculate[0]} 到 {months_to_calculate[-1]}\n")

    except Exception as e:
        main_logs.write(f"时间轴生成失败: {e}\n")
        calculation_logs.append({'month': 'N/A', 'result_df': None, 'logs': [main_logs.getvalue()]})
        return calculation_logs, final_result_df, pd.DataFrame()

    # --- 3. Prepare Cash Flows ---
    cost_timeline_df = build_reinsurance_cost_timeline(original_record, measure_prep_record)
    cash_flows_map = cost_timeline_df.set_index('month').to_dict(orient='index')

    # --- 4. Loop Through Months ---
    main_logs.write("步骤 3: 开始逐月计算...\n")
    previous_month_result_internal = {}
    
    # Fetch all assumptions and rates at once before the loop
    try:
        # Get val_method from measure prep data, with '11' as a fallback
        val_method = static_data.get('val_method', '11')
        if not val_method:
            val_method = '11'
            main_logs.write("  - 警告: 'val_method' 在计量准备数据中为空, 使用默认值 '11'。\n")

        all_assumptions = get_reinsurance_assumptions(engine, val_method)
        # The discount rates function does not require val_method
        all_discount_rates = get_reinsurance_discount_rates(engine)
        main_logs.write(f"  - 成功获取所有精算假设 (评估方法 '{val_method}') 和折现率。\n")
    except Exception as e:
        main_logs.write(f"获取精算假设或折现率失败: {e}\n")
        calculation_logs.append({'month': 'N/A', 'result_df': None, 'logs': [main_logs.getvalue()]})
        return calculation_logs, final_result_df, pd.DataFrame()

    all_monthly_results = []

    for val_month in months_to_calculate:
        month_cash_flows = cash_flows_map.get(val_month, {})
        
        # Get the specific assumptions and rates for the current month
        class_code = static_data.get('class_code', 'default') # Use a default if not present
        current_assumptions = all_assumptions.get(val_month, {}).get(class_code, {})
        current_discount_rates = all_discount_rates.get(val_month, {})

        if not current_assumptions:
            main_logs.write(f"警告: 在评估月 {val_month} 未找到险类 '{class_code}' 的精算假设。\n")
        if not current_discount_rates:
            main_logs.write(f"警告: 在评估月 {val_month} 未找到折现率。\n")
            
        result_for_df, next_prev_result, month_logs_str = _calculate_one_month(
            val_month,
            static_data,
            month_cash_flows,
            previous_month_result_internal,
            current_assumptions,
            current_discount_rates
        )
        
        previous_month_result_internal = next_prev_result
        all_monthly_results.append(result_for_df)

        full_log_list = [f"--- 开始计算评估月: {val_month} ---", month_logs_str]
        calculation_logs.append({
            'month': val_month,
            'result_df': pd.DataFrame([result_for_df]),
            'logs': full_log_list
        })

    # --- 5. Finalize ---
    main_logs.write("步骤 4: 计量完成。\n")
    if all_monthly_results:
        final_result_df = pd.DataFrame(all_monthly_results)

    if calculation_logs:
        calculation_logs[0]['logs'].insert(0, main_logs.getvalue())

    return calculation_logs, final_result_df, cost_timeline_df

def build_reinsurance_cost_timeline(original_record: pd.Series, measure_prep_record: pd.Series) -> pd.DataFrame:
    """
    Builds a timeline of costs for a reinsurance contract.
    - Timing for one-off costs comes from the original record (confirm_date).
    - All financial amounts should come from the measure_prep_record, as it is the source of truth for calculations.
    """
    timeline = {}
    D = Decimal

    # 1. Handle one-time costs (Premium, Commission, Brokerage)
    # Timing is based on the original transaction's confirm_date
    confirm_date = original_record.get('confirm_date')
    if pd.notna(confirm_date):
        confirm_month = confirm_date.strftime('%Y%m')
        if confirm_month not in timeline:
            timeline[confirm_month] = {'premium': D(0), 'commission': D(0), 'brokerage': D(0), 'iacf_unfol': D(0)}
        
        # Amounts are from the definitive measure prep record
        timeline[confirm_month]['premium'] += D(measure_prep_record.get('premium', 0) or 0)
        timeline[confirm_month]['commission'] += D(measure_prep_record.get('commission', 0) or 0)
        timeline[confirm_month]['brokerage'] += D(measure_prep_record.get('brokerage', 0) or 0)

    # 2. Handle non-proportional acquisition costs
    # Timing and amount are from the measure prep record
    val_month = measure_prep_record.get('val_month')
    if val_month:
        if val_month not in timeline:
            timeline[val_month] = {'premium': D(0), 'commission': D(0), 'brokerage': D(0), 'iacf_unfol': D(0)}
        timeline[val_month]['iacf_unfol'] += D(measure_prep_record.get('iacf_unfol', 0) or 0)

    if not timeline:
        return pd.DataFrame(columns=['month', 'premium', 'commission', 'brokerage', 'iacf_unfol'])

    # Convert to DataFrame
    timeline_df = pd.DataFrame.from_dict(timeline, orient='index').reset_index()
    timeline_df = timeline_df.rename(columns={'index': 'month'})
    
    # Ensure all columns are present even if no costs occurred
    for col in ['premium', 'commission', 'brokerage', 'iacf_unfol']:
        if col not in timeline_df.columns:
            timeline_df[col] = D(0)
            
    return timeline_df.sort_values(by='month').reset_index(drop=True)

def _perform_onerous_test(
    val_month: str,
    static_data: Dict[str, Any],
    lrc_no_loss_amt: Decimal,
    amortized_ratio: Decimal,
    assumptions: Dict[str, Any],
    discount_rates: Dict[int, float]
) -> Tuple[Decimal, str]:
    """Performs the onerous contract test and returns the loss amount and detailed logs."""
    logs = StringIO()
    D = Decimal

    logs.write("\n--- 开始亏损测试 ---\n")

    # --- 1. Get Assumptions ---
    logs.write(f"【获取 {val_month} 精算假设】:\n")
    loss_ratio = D(assumptions.get('loss_ratio', 0))
    indirect_claims_expense_ratio = D(assumptions.get('indirect_claims_expense_ratio', 0))
    maintenance_expense_ratio = D(assumptions.get('maintenance_expense_ratio', 0))
    ra_ratio = D(assumptions.get('ra', 0))
    logs.write(f"  赔付率: {loss_ratio:.4f}, 间接理赔费用率: {indirect_claims_expense_ratio:.4f}, "
               f"维持费用率: {maintenance_expense_ratio:.4f}, 风险调整率: {ra_ratio:.4f}\n")

    # --- 2. Calculate Future Proportions ---
    logs.write("---------------------------------\n")
    logs.write("【计算未来现金流】:\n")
    
    total_premium = D(static_data.get('premium', 0))
    pi_start_date = static_data['pi_start_date']
    pi_end_date = static_data['pi_end_date']
    val_date = datetime.strptime(val_month, '%Y%m').date().replace(day=monthrange(int(val_month[:4]), int(val_month[4:]))[1])

    future_ratio = D(1) - amortized_ratio
    unexpired_premium = (total_premium * future_ratio).quantize(D(f'1e-{SCALE}'))
    
    future_loss_cost = (unexpired_premium * loss_ratio * (D(1) + indirect_claims_expense_ratio)).quantize(D(f'1e-{SCALE}'))
    future_maintenance_cost = (unexpired_premium * maintenance_expense_ratio).quantize(D(f'1e-{SCALE}'))

    logs.write(f"  1. 未来服务比例 = 1 - {amortized_ratio:.10f} = {future_ratio:.10f}\n")
    logs.write(f"  2. 未到期保费 = {total_premium:.4f} * {future_ratio:.10f} = {unexpired_premium:.4f}\n")
    logs.write(f"  3. 未来赔付成本总额 = {unexpired_premium:.4f} * {loss_ratio:.4f} * (1 + {indirect_claims_expense_ratio:.4f}) = {future_loss_cost:.4f}\n")
    logs.write(f"  4. 未来维持费用总额 = {unexpired_premium:.4f} * {maintenance_expense_ratio:.4f} = {future_maintenance_cost:.4f}\n")

    # --- 3. Discounting with Monthly Breakdown ---
    # Calculate remaining months for cash flow projection
    remaining_months = (
        (pi_end_date.year - val_date.year) * 12 + pi_end_date.month - val_date.month
    )
    if remaining_months <= 0:
        pv_future_loss, pv_future_maintenance = D(0), D(0)
        loss_pv_details, maintenance_pv_details = [], []
        logs.write("  - 剩余服务期为0，未来现金流折现为0。\n")
    else:
        avg_monthly_loss = (future_loss_cost / D(remaining_months)).quantize(D(f'1e-{SCALE}'))
        avg_monthly_maintenance = (future_maintenance_cost / D(remaining_months)).quantize(D(f'1e-{SCALE}'))
        logs.write(f"  - 剩余服务月数: {remaining_months}, 平均每月赔付: {avg_monthly_loss:.4f}, 平均每月维持: {avg_monthly_maintenance:.4f}\n")

        # Perform discounting month by month
        pv_future_loss, loss_pv_details = _discount_cash_flows(avg_monthly_loss, remaining_months, discount_rates, val_month)
        
        pv_future_maintenance, maintenance_pv_details = _discount_cash_flows(avg_monthly_maintenance, remaining_months, discount_rates, val_month)

    pv_future_loss = pv_future_loss.quantize(D(f'1e-{SCALE}'))
    pv_future_maintenance = pv_future_maintenance.quantize(D(f'1e-{SCALE}'))
    logs.write(f"  5. 未来赔付成本折现PV = {pv_future_loss:.4f}\n")
    logs.write(f"  6. 未来维持费用折现PV = {pv_future_maintenance:.4f}\n")

    # --- 4. Risk Adjustment and Net Future CF ---
    risk_adjustment = ((pv_future_loss + pv_future_maintenance) * ra_ratio).quantize(D(f'1e-{SCALE}'))
    net_future_cash_flow = (pv_future_loss + pv_future_maintenance + risk_adjustment).quantize(D(f'1e-{SCALE}'))
    
    logs.write(f"  7. 风险调整 = ({pv_future_loss:.4f} + {pv_future_maintenance:.4f}) * {ra_ratio:.4f} = {risk_adjustment:.4f}\n")
    logs.write(f"  8. 未来净现金流 = {pv_future_loss:.4f} + {pv_future_maintenance:.4f} + {risk_adjustment:.4f} = {net_future_cash_flow:.4f}\n")
    logs.write("---------------------------------\n")

    # --- 5. Calculate Loss Amount ---
    logs.write("【计算亏损金额】:\n")
    loss_test_balance = (net_future_cash_flow - lrc_no_loss_amt).quantize(D(f'1e-{SCALE}'))
    final_loss_amt = max(D(0), loss_test_balance)
    
    logs.write(f"  期末非亏损余额: {lrc_no_loss_amt:.4f}\n")
    logs.write(f"  亏损测试余额 = 未来净现金流 - 期末非亏损余额 = {net_future_cash_flow:.4f} - {lrc_no_loss_amt:.4f} = {loss_test_balance:.4f}\n")
    logs.write(f"  最终亏损合同负债 (LRC Loss) = max(0, {loss_test_balance:.4f}) = {final_loss_amt:.4f}\n")

    # Create DataFrames for detailed PV view
    loss_pv_df = pd.DataFrame(loss_pv_details)
    maintenance_pv_df = pd.DataFrame(maintenance_pv_details)

    return final_loss_amt, logs.getvalue(), loss_pv_df, maintenance_pv_df

def _discount_cash_flows(
    monthly_cash_flow: Decimal,
    remaining_months: int,
    discount_rates: Dict[int, float],
    val_month: str
) -> Tuple[Decimal, List[Dict[str, Any]]]:
    """
    Discounts a series of monthly cash flows using the provided discount rates.
    Returns the present value and a list of details for the DataFrame.
    """
    D = Decimal  # Define D as Decimal for this function
    pv_total = D(0)
    details = []
    cumulative_discount = D(1)
    
    # Parse val_month to get the starting date
    val_date = datetime.strptime(val_month, '%Y%m')

    for i in range(1, remaining_months + 1):
        rate = D(discount_rates.get(i, 0.0))
        cumulative_discount /= (D(1) + rate)
        pv_total += monthly_cash_flow * cumulative_discount
        
        # Calculate the actual month for this period
        future_date = val_date + relativedelta(months=i)
        month_label = future_date.strftime('%Y-%m')
        
        details.append({
            'month': month_label,
            'cash_flow': float(monthly_cash_flow),
            'discount_rate': float(rate),
            'cumulative_discount': float(cumulative_discount),
            'present_value': float(monthly_cash_flow * cumulative_discount)
        })
    return pv_total, details
