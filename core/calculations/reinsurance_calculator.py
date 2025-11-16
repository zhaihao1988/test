import pandas as pd
from decimal import Decimal, getcontext, ROUND_HALF_UP
from datetime import datetime
from dateutil.relativedelta import relativedelta
from calendar import monthrange
from io import StringIO
from sqlalchemy import Engine
from typing import List, Dict, Any, Tuple

from core.data_fetcher.reinsurance_data import get_reinsurance_inward_data, get_reinsurance_measure_prep_data, get_all_reinsurance_measure_records
from core.data_fetcher.reinsurance_input_data import get_reinsurance_inward_assumptions, get_reinsurance_discount_rates, get_reinsurance_claim_models

# --- Global Settings ---
getcontext().prec = 38
getcontext().rounding = 'ROUND_HALF_UP'
SCALE = 10
TEN_DIGITS = Decimal(f'1e-{SCALE}')

def _calculate_one_month(
    val_month: str,
    static_data: Dict[str, Any],
    cash_flows: Dict[str, Decimal],
    prev_result: Dict[str, Decimal],
    assumptions: Dict[str, Any],
    non_onerous_rate: Decimal,
    onerous_rate_curve: Dict[int, float],
    claim_model_map: Dict[str, List[float]],
    month_counter: int,
    rate_info_log: str
) -> Tuple[Dict[str, Any], Dict[str, Decimal], str]:
    """Calculates all metrics for a single evaluation month based on the flowchart logic."""
    logs = StringIO()
    result = {}
    D = Decimal

    # --- 1. Initial Data and Parameters ---
    logs.write("步骤 1: 初始化数据\n")
    
    # --- FIX: 使用 .get() 安全地从 static_data 中提取关键值 ---
    pi_start_date_str = static_data.get('pi_start_date')
    pi_end_date_str = static_data.get('pi_end_date')
    if not pi_start_date_str or not pi_end_date_str:
        raise ValueError("pi_start_date 或 pi_end_date 在 static_data 中缺失，无法继续计算。")
    pi_start_date = datetime.strptime(str(pi_start_date_str), '%Y-%m-%d').date()
    pi_end_date = datetime.strptime(str(pi_end_date_str), '%Y-%m-%d').date()

    total_premium = D(static_data.get('premium', 0) or 0)
    total_commission = D(static_data.get('commission', 0) or 0)
    total_brokerage = D(static_data.get('brokerage', 0) or 0)
    total_iacf_unfol = D(static_data.get('iacf_unfol', 0) or 0) # This is the dynamic total for amortization
    class_code = static_data.get('class_code', 'default')

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

    logs.write(f"\n{rate_info_log}\n") # Log the rate info

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
    monthly_rate = non_onerous_rate # Use the locked-in rate for IFIE
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
    current_iacf_unfol_amortization = (total_iacf_unfol * amortized_ratio).quantize(D(f'1e-{SCALE}'))
    current_iacf_unfol_amortization = (current_iacf_unfol_amortization - prev_acc_iacf_unfol_amortization).quantize(D(f'1e-{SCALE}'))
    logs.write(f"  公式: 总非跟单获取费用 * 累计服务比例 - 上期累计摊销\n")
    logs.write(f"  = {total_iacf_unfol:.4f} * {amortized_ratio:.10f} - {prev_acc_iacf_unfol_amortization:.4f} = {current_iacf_unfol_amortization:.4f}\n")
    acc_iacf_unfol_amortization = (prev_acc_iacf_unfol_amortization + current_iacf_unfol_amortization).quantize(D(f'1e-{SCALE}'))
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

    # --- Onerous Test (Now performed once at the end) ---
    # logs.write(loss_test_logs)
    
    # --- 8. Final Results ---
    result = {
        'val_month': val_month,
        'lrc_no_loss_amt': float(lrc_no_loss_amt),
        'lrc_loss_amt': 0.0, # Default to 0, will be calculated at the end
        'current_insurance_revenue': float(current_insurance_revenue),
        'current_acquisition_cost': float(current_iacf_unfol_amortization),
        'current_net_cash_flow': float(current_net_premium_cash_flow),
        'acc_insurance_revenue': float(acc_insurance_revenue),
        'acc_acquisition_cost': float(acc_iacf_unfol_amortization),
        'acc_ifie': float(acc_ifie),
        'amortized_ratio': float(amortized_ratio),
    }

    # For next iteration
    internal_result_for_next_loop = {
        'lrc_no_loss_amt': lrc_no_loss_amt,
        'acc_insurance_revenue': acc_insurance_revenue,
        'acc_iacf_unfol_amortization': acc_iacf_unfol_amortization,
        'acc_ifie': acc_ifie,
        'amortized_ratio': amortized_ratio, # Pass this for the final loss test
    }
    
    return result, internal_result_for_next_loop, logs.getvalue()

def calculate_reinsurance_unexpired_measure(
    engine: Engine,
    measure_val_month: str,
    contract_id: str,
    policy_no: str, # This might be part of the composite key but not used in all fetchers
    certi_no: str,
    confirm_date: str,
    pi_start_date: str
) -> Tuple[List[Dict[str, Any]], pd.DataFrame, pd.DataFrame]:
    """Orchestrates the new month-by-month calculation for reinsurance LRC."""
    D = Decimal
    main_logs = StringIO()
    calculation_logs = []
    final_result_df = pd.DataFrame()

    # --- 1. Data Fetching ---
    main_logs.write("步骤 1: 获取计量所需数据...\n")
    try:
        # Step 1.1: Fetch all necessary source dataframes using the composite key
        original_df = get_reinsurance_inward_data(engine, contract_id, confirm_date, pi_start_date)
        measure_prep_df = get_reinsurance_measure_prep_data(engine, contract_id, confirm_date, pi_start_date)
        all_measure_records_df = get_all_reinsurance_measure_records(engine, contract_id, confirm_date, pi_start_date)

        # --- Specific error checking ---
        if original_df.empty:
            raise ValueError(f"在源表 'bi_to_cas25.ri_pp_re_mon_arr_in' 中未找到合约 '{contract_id}' (confirm_date={confirm_date}, pi_start_date={pi_start_date}) 的记录。")
        if measure_prep_df.empty:
            raise ValueError(f"在准备表 'public.int_t_pp_re_mon_arr_in_new' 中未找到合约 '{contract_id}' (confirm_date={confirm_date}, pi_start_date={pi_start_date}) 的记录。")
        if all_measure_records_df.empty:
            raise ValueError(f"在结果表 'measure_platform.int_measure_cx_unexpired_rein' 中未找到合约 '{contract_id}' (confirm_date={confirm_date}, pi_start_date={pi_start_date}) 的任何计量记录。")
        
        main_logs.write(f"  - 成功获取所有源数据 ({len(all_measure_records_df)} 条计量记录)。\n")

        # Step 1.2: Prepare static data from the latest records
        original_record = original_df.iloc[0]
        measure_prep_record = measure_prep_df.iloc[0]
        static_data = {**original_record.to_dict(), **measure_prep_record.to_dict()}
        # Note: 'iacf_unfol' in static_data will be dynamically calculated for amortization inside the loop
        # but the value from measure_prep_record might be used elsewhere if needed. For clarity, we rely on the loop's dynamic value.

    except Exception as e:
        main_logs.write(f"数据获取失败: {e}\n")
        calculation_logs.append({'month': 'N/A', 'result_df': None, 'logs': [main_logs.getvalue()]})
        return calculation_logs, final_result_df, pd.DataFrame()

    # --- 2. Generate Timeline ---
    main_logs.write("步骤 2: 生成计算时间轴...\n")
    try:
        first_ini_confirm_date = all_measure_records_df['ini_confirm'].iloc[0]
        if pd.isna(first_ini_confirm_date):
            raise ValueError("'ini_confirm' date is missing from the measurement records.")
        ini_confirm_month = first_ini_confirm_date.strftime('%Y%m')

        start_month_dt = first_ini_confirm_date.to_pydatetime().date().replace(day=1)
        end_month_dt = datetime.strptime(measure_val_month, '%Y%m').date().replace(day=1)
        
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
    cost_timeline_df = build_reinsurance_cost_timeline(
        measure_prep_record,    # For premium, commission, brokerage
        all_measure_records_df, # For iacf_unfol flows
        ini_confirm_month
    )
    cash_flows_map = cost_timeline_df.set_index('month').to_dict(orient='index')

    val_method = static_data.get('val_method', '11')
    if not val_method:
        val_method = '11'
        main_logs.write("  - 警告: 'val_method' 在计量准备数据中为空, 使用默认值 '11'。\n")

    # --- 3. 准备计算所需的数据 ---
    main_logs.write("步骤 3: 开始逐月计算...\n")

    try:
        assumptions_map = get_reinsurance_inward_assumptions(engine)
        discount_rates_map = get_reinsurance_discount_rates(engine)
        claim_model_map = get_reinsurance_claim_models(engine)
        
        main_logs.write(f"  - 成功获取所有精算假设 (评估方法 '{val_method}') 和折现率。\n")
    except Exception as e:
        main_logs.write(f"获取精算假设或折现率失败: {e}\n")
        calculation_logs.append({'month': 'N/A', 'result_df': None, 'logs': [main_logs.getvalue()]})
        return calculation_logs, final_result_df, pd.DataFrame()

    # --- 4. Loop Through Months ---
    previous_month_result_internal = {}
    month_counter = 0
    all_monthly_results = []

    # --- Lock in the discount rate curve for non-onerous calculation ---
    try:
        locked_in_rate_curve = discount_rates_map.get(ini_confirm_month)
        if not locked_in_rate_curve:
            raise ValueError(f"在利率表中未找到初始确认月 '{ini_confirm_month}' 的利率曲线。")
    except Exception as e:
        main_logs.write(f"锁定用于IFIE计算的利率曲线失败: {e}\n")
        calculation_logs.append({'month': 'N/A', 'result_df': None, 'logs': [main_logs.getvalue()]})
        return calculation_logs, final_result_df, pd.DataFrame()

    for val_month in months_to_calculate:
        month_counter += 1
        current_month_cash_flows = cash_flows_map.get(val_month, {})
        
        # --- CORRECT LOGIC: Calculate CUMULATIVE amortization base dynamically ---
        # Base is the '202412' value.
        base_record = all_measure_records_df[all_measure_records_df['val_month'] == '202412']
        amort_base = D(base_record['no_iacf_cash_flow'].iloc[0]) if not base_record.empty else D(0)
        
        # Add cumulative values from 2025 onwards up to the current month.
        future_records_for_amort = all_measure_records_df[
            (all_measure_records_df['val_month'] > '202412') &
            (all_measure_records_df['val_month'] <= val_month)
        ]
        cumulative_iacf_unfol_for_amort = amort_base + D(future_records_for_amort['no_iacf_cash_flow'].sum())

        # Use a copy of static_data to pass the dynamic amortization base
        static_data_for_month = static_data.copy()
        static_data_for_month['iacf_unfol'] = cumulative_iacf_unfol_for_amort
        # --- END CORRECT LOGIC ---

        class_code = static_data.get('class_code', 'default')
        
        # --- Rate Selection Logic ---
        # For non-onerous (IFIE), use the locked-in rate curve based on term month
        term_month = month_counter
        non_onerous_rate_float = locked_in_rate_curve.get(term_month)
        if non_onerous_rate_float is None:
            main_logs.write(f"警告: 在初始确认月 '{ini_confirm_month}' 的利率曲线中未找到第 {term_month} 期的利率 (用于IFIE)，将使用0。\n")
            non_onerous_rate_float = 0.0
        non_onerous_rate_decimal = D(str(non_onerous_rate_float))
        rate_info_log = f"利率信息 (用于IFIE): 使用初始确认月 [{ini_confirm_month}] 的第 [{term_month}] 期利率, 值为 [{non_onerous_rate_float:.10f}]"

        # For onerous test (PV of future CFs), use the current evaluation month's curve
        onerous_rate_curve = discount_rates_map.get(val_month, {})
        if not onerous_rate_curve:
            main_logs.write(f"警告: 在评估月 {val_month} 未找到用于亏损测试的利率曲线。\n")

        current_assumptions = assumptions_map.get(val_month, {}).get(class_code, {})
        if not current_assumptions:
            main_logs.write(f"警告: 在评估月 {val_month} 未找到险类 '{class_code}' 的精算假设。\n")
            
        result_for_df, next_prev_result, month_logs_str = _calculate_one_month(
            val_month,
            static_data_for_month, # Pass the dynamically updated static data
            current_month_cash_flows,
            previous_month_result_internal,
            current_assumptions,
            non_onerous_rate_decimal,
            onerous_rate_curve,
            claim_model_map,
            month_counter,
            rate_info_log
        )
        
        previous_month_result_internal = next_prev_result
        all_monthly_results.append(result_for_df)

        full_log_list = [f"--- 开始计算评估月: {val_month} ---", month_logs_str]
        calculation_logs.append({
            'month': val_month,
            'result_df': pd.DataFrame([result_for_df]),
            'logs': full_log_list
        })

    # --- 5. Onerous Test (performed once at the end) ---
    main_logs.write("步骤 4: 期末亏损测试...\n")
    final_logs = StringIO()
    try:
        # Get data for the final month
        lrc_no_loss_amt = previous_month_result_internal['lrc_no_loss_amt']
        amortized_ratio = previous_month_result_internal['amortized_ratio']
        class_code = static_data.get('class_code', 'default')
        total_premium = D(static_data.get('premium', 0) or 0)
        pi_start_date = datetime.strptime(str(static_data['pi_start_date']), '%Y-%m-%d').date()
        pi_end_date = datetime.strptime(str(static_data['pi_end_date']), '%Y-%m-%d').date()

        # Get assumptions and rates for the final evaluation month
        onerous_rate_curve = discount_rates_map.get(measure_val_month, {})
        final_assumptions = assumptions_map.get(measure_val_month, {}).get(class_code, {})
        if not onerous_rate_curve:
            final_logs.write(f"警告: 在评估月 {measure_val_month} 未找到用于亏损测试的利率曲线。\n")
        if not final_assumptions:
            final_logs.write(f"警告: 在评估月 {measure_val_month} 未找到险类 '{class_code}' 的精算假设。\n")

        lrc_loss_amt, loss_test_logs, pv_loss_df, pv_maintenance_df = _perform_onerous_test(
            measure_val_month,
            pi_start_date,
            pi_end_date,
            lrc_no_loss_amt,
            amortized_ratio,
            total_premium,
            class_code,
            final_assumptions,
            onerous_rate_curve,
            claim_model_map,
            month_counter
        )
        final_logs.write(loss_test_logs)

        # Update the last month's result with the loss info
        if all_monthly_results:
            all_monthly_results[-1]['lrc_loss_amt'] = float(lrc_loss_amt)
            all_monthly_results[-1]['loss_pv_details_df'] = pv_loss_df
            all_monthly_results[-1]['maintenance_pv_details_df'] = pv_maintenance_df
    
    except Exception as e:
        final_logs.write(f"期末亏损测试失败: {e}\n")

    # Add the final loss test logs to the last month's calculation log
    if calculation_logs:
        calculation_logs[-1]['logs'].append(final_logs.getvalue())


    # --- 6. Finalize ---
    main_logs.write("步骤 5: 计量完成。\n")
    if all_monthly_results:
        final_result_df = pd.DataFrame(all_monthly_results)

    if calculation_logs:
        calculation_logs[0]['logs'].insert(0, main_logs.getvalue())

    return calculation_logs, final_result_df, cost_timeline_df

def build_reinsurance_cost_timeline(
    measure_prep_record: pd.Series,
    all_records_df: pd.DataFrame,
    initial_confirm_month: str
) -> pd.DataFrame:
    """
    Builds the cash flow timeline based on the final, specific rules.
    """
    timeline = {}
    D = Decimal
    cutoff_month = '202412'

    def get_or_create_month_entry(month_str: str):
        if month_str not in timeline:
            timeline[month_str] = {'premium': D(0), 'commission': D(0), 'brokerage': D(0), 'iacf_unfol': D(0)}

    # 1. Book Premium, Commission, Brokerage to the initial confirm month
    get_or_create_month_entry(initial_confirm_month)
    premium = D(measure_prep_record.get('premium', 0) or 0)
    commission = D(measure_prep_record.get('commission', 0) or 0)
    brokerage = D(measure_prep_record.get('brokerage', 0) or 0)
    
    timeline[initial_confirm_month]['premium'] += premium
    timeline[initial_confirm_month]['commission'] += commission
    timeline[initial_confirm_month]['brokerage'] += brokerage

    # 2. Book 'no_iacf_cash_flow' based on the cutoff rule
    for index, row in all_records_df.iterrows():
        current_month = row['val_month']
        no_iacf_flow = D(row.get('no_iacf_cash_flow', 0) or 0)

        if no_iacf_flow == 0:
            continue

        if current_month == cutoff_month:
            # Book the 202412 value to the initial month
            timeline[initial_confirm_month]['iacf_unfol'] += no_iacf_flow
        elif current_month > cutoff_month:
            # Book future flows to their own month
            get_or_create_month_entry(current_month)
            timeline[current_month]['iacf_unfol'] += no_iacf_flow
        # Flows before 202412 are ignored for cash flow purposes, but included in amortization base logic.

    if not timeline:
        return pd.DataFrame(columns=['month', 'premium', 'commission', 'brokerage', 'iacf_unfol'])

    timeline_df = pd.DataFrame.from_dict(timeline, orient='index').reset_index()
    timeline_df = timeline_df.rename(columns={'index': 'month'})
    
    for col in ['premium', 'commission', 'brokerage', 'iacf_unfol']:
        if col not in timeline_df.columns:
            timeline_df[col] = D(0)
            
    return timeline_df.sort_values(by='month').reset_index(drop=True)

def _perform_onerous_test(
    val_month: str,
    pi_start_date: datetime.date, # Receive parsed date
    pi_end_date: datetime.date,   # Receive parsed date
    lrc_no_loss_amt: Decimal,
    amortized_ratio: Decimal,
    total_premium: Decimal,
    class_code: str,
    assumptions: Dict[str, Any],
    onerous_rate_curve: Dict[int, float],
    claim_model_map: Dict[str, List[float]],
    month_counter: int
) -> Tuple[Decimal, str, pd.DataFrame, pd.DataFrame]:
    """Performs the onerous contract test and returns the loss amount and detailed logs."""
    logs = StringIO()
    D = Decimal

    logs.write("\n--- 开始亏损测试 ---\n")

    logs.write(f"【获取 {val_month} 精算假设】:\n")
    loss_ratio = D(assumptions.get('loss_ratio', 0))
    indirect_claims_expense_ratio = D(assumptions.get('indirect_claims_expense_ratio', 0))
    maintenance_expense_ratio = D(assumptions.get('maintenance_expense_ratio', 0))
    ra_ratio = D(assumptions.get('risk_adjustment_ratio', 0))
    
    logs.write(f"  赔付率: {loss_ratio:.4f}, 间接理赔费用率: {indirect_claims_expense_ratio:.4f}, "
               f"维持费用率: {maintenance_expense_ratio:.4f}, 风险调整率: {ra_ratio:.4f}\n")

    logs.write("---------------------------------\n")
    logs.write("【计算未来现金流】:\n")
    
    future_proportion = D(1) - amortized_ratio
    unexpired_premium = (total_premium * future_proportion).quantize(D(f'1e-{SCALE}'))
    
    future_loss = (unexpired_premium * loss_ratio * (D(1) + indirect_claims_expense_ratio)).quantize(D(f'1e-{SCALE}'))
    future_maintenance = (unexpired_premium * maintenance_expense_ratio).quantize(D(f'1e-{SCALE}'))
    
    # --- FIX: 直接根据评估月和止期计算剩余服务月数 ---
    val_month_dt = datetime.strptime(val_month, '%Y%m').date()
    # 计算从评估月之后到合同止期之间的完整月数
    remaining_months = (pi_end_date.year - val_month_dt.year) * 12 + (pi_end_date.month - val_month_dt.month)
    remaining_months = max(0, remaining_months)
    
    logs.write(f"  - (调试) 累计摊销比例: {amortized_ratio:.10f}\n")
    logs.write(f"  1. 未来服务比例 = 1 - {amortized_ratio:.10f} = {future_proportion:.10f}\n")
    logs.write(f"  2. 未到期保费 = {total_premium:.4f} * {future_proportion:.10f} = {unexpired_premium:.4f}\n")
    logs.write(f"  3. 未来赔付成本总额 = {unexpired_premium:.4f} * {loss_ratio:.4f} * (1 + {indirect_claims_expense_ratio:.4f}) = {future_loss:.4f}\n")
    logs.write(f"  4. 未来维持费用总额 = {unexpired_premium:.4f} * {maintenance_expense_ratio:.4f} = {future_maintenance:.4f}\n")
    logs.write(f"  - 剩余服务月数: {remaining_months}\n")

    claim_factors = claim_model_map.get(class_code, [])
    if not claim_factors:
         logs.write(f"警告: 未找到险类 '{class_code}' 的赔付模式。未来现金流将无法按模式分配。\n")

    # --- FIX: Pass the correct 'onerous_rate_curve' to the PV functions ---
    pv_future_loss, loss_pv_details_df = _get_pv_loss(future_loss, remaining_months, claim_factors, onerous_rate_curve, val_month, logs)
    pv_future_maintenance, maintenance_pv_details_df = _get_pv_maintenance(future_maintenance, remaining_months, onerous_rate_curve, val_month, logs)

    logs.write(f"  5. 未来赔付成本折现PV = {pv_future_loss:.4f}\n")
    logs.write(f"  6. 未来维持费用折现PV = {pv_future_maintenance:.4f}\n")

    risk_adjustment = ((pv_future_maintenance + pv_future_loss) * ra_ratio).quantize(D(f'1e-{SCALE}'))
    net_future_cash_flow = (pv_future_loss + pv_future_maintenance + risk_adjustment).quantize(D(f'1e-{SCALE}'))
    
    logs.write(f"  7. 风险调整 = ({pv_future_maintenance:.4f} + {pv_future_loss:.4f}) * {ra_ratio:.4f} = {risk_adjustment:.4f}\n")
    logs.write(f"  8. 未来净现金流 = {pv_future_loss:.4f} + {pv_future_maintenance:.4f} + {risk_adjustment:.4f} = {net_future_cash_flow:.4f}\n")
    logs.write("---------------------------------\n")

    logs.write("【计算亏损金额】:\n")
    loss_test_balance = (net_future_cash_flow - lrc_no_loss_amt).quantize(D(f'1e-{SCALE}'))
    final_loss_amt = max(D(0), loss_test_balance)
    
    logs.write(f"  期末非亏损余额: {lrc_no_loss_amt:.4f}\n")
    logs.write(f"  亏损测试余额 = 未来净现金流 - 期末非亏损余额 = {net_future_cash_flow:.4f} - {lrc_no_loss_amt:.4f} = {loss_test_balance:.4f}\n")
    logs.write(f"  最终亏损合同负债 (LRC Loss) = max(0, {loss_test_balance:.4f}) = {final_loss_amt:.4f}\n")

    return final_loss_amt, logs.getvalue(), loss_pv_details_df, maintenance_pv_details_df

def _get_pv_maintenance(amt: Decimal, n: int, months_rate_map: Dict[int, object], val_month: str, logs: StringIO) -> Tuple[Decimal, pd.DataFrame]:
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

def _get_pv_loss(amt: Decimal, n: int, claim_factor_arr: list, months_rate_map: Dict[int, object], val_month: str, logs: StringIO) -> Tuple[Decimal, pd.DataFrame]:
    """
    计算未来赔付成本的折现值。
    此函数逻辑已与直保模块 (measure_unexpired_calculator.py) 的 `_get_pv_loss` 完全对齐。
    """
    logs.write("\n--- 进入【亏损计算】_get_pv_loss 函数 (逻辑与直保对齐) ---\n")
    logs.write(f"  - 输入 amt (总额): {amt:.4f}\n")
    logs.write(f"  - 输入 n (剩余月份): {n}\n")
    logs.write(f"  - 输入 claim_factor_arr (赔付模式, 前5个): {claim_factor_arr[:5]}\n")

    if amt == Decimal(0) or n <= 0:
        return Decimal(0), pd.DataFrame()
        
    claim_factor = [Decimal(str(d)) for d in claim_factor_arr]
    avg_amt = (amt / Decimal(n)).quantize(TEN_DIGITS, rounding=ROUND_HALF_UP)
    logs.write(f"  - 计算 avg_amt (月均赔付): {avg_amt:.4f}\n")

    claim_factor_applied = [(avg_amt * factor).quantize(TEN_DIGITS, rounding=ROUND_HALF_UP) for factor in claim_factor]
    if claim_factor_applied:
        logs.write(f"  - 计算 claim_factor_applied[0] (首月现金流基数): {claim_factor_applied[0]:.4f}\n")

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