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

from test.core.data_fetcher.reinsurance_outward_data import (
    get_reinsurance_outward_data,
    get_reinsurance_outward_measure_prep_data,
    get_underlying_loss_amount
)
from test.core.data_fetcher.reinsurance_input_data import (
    get_reinsurance_assumptions,
    get_reinsurance_discount_rates
)

SCALE = 10  # Decimal precision

def _calculate_one_month_outward(
    val_month: str,
    static_data: Dict[str, Any],
    cash_flows: Dict[str, Decimal],
    prev_result: Dict[str, Decimal],
    assumptions: Dict[str, Any],
    discount_rates: Dict[int, float]
) -> Tuple[Dict[str, Any], Dict[str, Decimal], str]:
    """
    Calculates all metrics for a single evaluation month for reinsurance outward.
    再保分出的逐月计算（净额法，无经纪费和非跟单费用）
    """
    logs = StringIO()
    D = Decimal
    
    logs.write(f"\n--- 开始计算评估月: {val_month} ---\n")
    logs.write("步骤 1: 初始化数据\n")
    
    # Extract static data
    total_premium = D(static_data.get('premium', 0))
    total_commission = D(static_data.get('commission', 0))
    pi_start_date = static_data['pi_start_date']
    pi_end_date = static_data['pi_end_date']
    
    # Cash flows for the current month (分出：保费和佣金都是支出，符号为负)
    current_premium_cash_flow = cash_flows.get('premium', D(0))
    current_commission_cash_flow = cash_flows.get('commission', D(0))
    # 净保费 = 保费 - 佣金（对于分出，这是净支出）
    current_net_premium_cash_flow = current_premium_cash_flow - current_commission_cash_flow
    
    logs.write(f"  - 当期分出保费现金流: {current_premium_cash_flow:.4f}\n")
    logs.write(f"  - 当期分出佣金现金流: {current_commission_cash_flow:.4f}\n")
    logs.write(f"  - 当期净现金流 (保费 - 佣金): {current_net_premium_cash_flow:.4f}\n")

    # Previous month's results
    prev_lrc_no_loss = prev_result.get('lrc_no_loss_amt', D(0))
    prev_acc_insurance_revenue = prev_result.get('acc_insurance_revenue', D(0))
    prev_acc_ifie = prev_result.get('acc_ifie', D(0))

    logs.write(f" -> 期初非亏损余额: {prev_lrc_no_loss:.4f}\n")
    logs.write(f" -> 上期累计确认收入: {prev_acc_insurance_revenue:.4f}\n")
    logs.write(f" -> 上期累计IFIE: {prev_acc_ifie:.4f}\n")

    # --- 2. Amortization Calculation ---
    logs.write("\n【计算累计服务比例】:\n")
    val_date = datetime.strptime(val_month, '%Y%m').date()
    val_date = val_date.replace(day=monthrange(val_date.year, val_date.month)[1])

    total_days = D((pi_end_date - pi_start_date).days + 1)
    elapsed_days = D(0)
    if val_date >= pi_start_date:
        elapsed_days = D((min(val_date, pi_end_date) - pi_start_date).days + 1)
    
    amortized_ratio = elapsed_days / total_days if total_days > 0 else D(0)
    logs.write(f"  累计服务天数: {elapsed_days} / {total_days}\n")
    logs.write(f"  -> 累计服务比例: {amortized_ratio:.10f}\n")

    # --- 3. IFIE (Interest) Calculation ---
    logs.write("\n【计算当期IFIE (未到期利息)】:\n")
    monthly_rate = D(discount_rates.get(1, 0.0012))
    ifie_from_opening_balance = prev_lrc_no_loss * monthly_rate
    ifie_from_net_premium = current_net_premium_cash_flow * monthly_rate * D('0.5')
    current_ifie = (ifie_from_opening_balance + ifie_from_net_premium).quantize(D(f'1e-{SCALE}'))
    acc_ifie = (prev_acc_ifie + current_ifie).quantize(D(f'1e-{SCALE}'))
    logs.write(f"  公式: 上月余额利息 + 本月净现金流利息\n")
    logs.write(f"  = ({prev_lrc_no_loss:.4f} * {monthly_rate:.12f}) + ({current_net_premium_cash_flow:.4f} * {monthly_rate:.12f} * 0.5) = {current_ifie:.4f}\n")
    logs.write(f"  -> 累计IFIE更新为: {acc_ifie:.4f}\n")

    # --- 4. Insurance Revenue (for outward, this is negative as it's ceded) ---
    logs.write("\n【计算当期确认收入】:\n")
    logs.write(f"  公式: ((总净保费 + 累计IFIE) * 累计服务比例) - 上期累计确认收入\n")
    total_net_premium = total_premium - total_commission
    acc_insurance_revenue = ((total_net_premium + acc_ifie) * amortized_ratio).quantize(D(f'1e-{SCALE}'))
    current_insurance_revenue = (acc_insurance_revenue - prev_acc_insurance_revenue).quantize(D(f'1e-{SCALE}'))
    logs.write(f"  = (({total_net_premium:.4f} + {acc_ifie:.4f}) * {amortized_ratio:.10f}) - {prev_acc_insurance_revenue:.4f} = {current_insurance_revenue:.4f}\n")
    logs.write(f"  -> 累计确认收入更新为: {acc_insurance_revenue:.4f}\n")

    # --- 5. Non-Onerous LRC Calculation ---
    logs.write("\n【计算期末非亏损余额】:\n")
    logs.write(f"  公式: 期初余额 + 净现金流 + 当期IFIE - 当期确认收入\n")
    lrc_no_loss_amt = (
        prev_lrc_no_loss + 
        current_net_premium_cash_flow + 
        current_ifie - 
        current_insurance_revenue
    ).quantize(D(f'1e-{SCALE}'))
    logs.write(f"  = {prev_lrc_no_loss:.4f} + {current_net_premium_cash_flow:.4f} + {current_ifie:.4f} - {current_insurance_revenue:.4f} = {lrc_no_loss_amt:.4f}\n")
    logs.write(f"  -> 期末非亏损余额 (closing_balance) = {lrc_no_loss_amt:.4f}\n")

    # --- 6. Final Results ---
    result = {
        'val_month': val_month,
        'closing_balance': float(lrc_no_loss_amt),
        'lrc_no_loss_amt': float(lrc_no_loss_amt),
        'current_insurance_revenue': float(current_insurance_revenue),
        'current_net_cash_flow': float(current_net_premium_cash_flow),
        'acc_insurance_revenue': float(acc_insurance_revenue),
        'acc_ifie': float(acc_ifie),
        'amortized_ratio': float(amortized_ratio),
        'current_ifie': float(current_ifie),
    }

    # For next iteration
    internal_result_for_next_loop = {
        'lrc_no_loss_amt': lrc_no_loss_amt,
        'acc_insurance_revenue': acc_insurance_revenue,
        'acc_ifie': acc_ifie,
    }
    
    return result, internal_result_for_next_loop, logs.getvalue()

def build_reinsurance_outward_cost_timeline(original_record: pd.Series, measure_prep_record: pd.Series) -> pd.DataFrame:
    """
    Builds a timeline of costs for a reinsurance outward contract.
    构建再保分出的费用时间线（只有保费和佣金）
    """
    timeline = {}
    D = Decimal

    # Use ini_confirm from measure prep data
    ini_confirm = measure_prep_record.get('ini_confirm')
    if pd.notna(ini_confirm):
        if isinstance(ini_confirm, str):
            ini_confirm_month = ini_confirm[:6]  # YYYYMM format
        else:
            ini_confirm_month = ini_confirm.strftime('%Y%m')
            
        if ini_confirm_month not in timeline:
            timeline[ini_confirm_month] = {'premium': D(0), 'commission': D(0)}
        
        # Amounts from measure prep record
        timeline[ini_confirm_month]['premium'] += D(measure_prep_record.get('premium', 0) or 0)
        timeline[ini_confirm_month]['commission'] += D(measure_prep_record.get('commission', 0) or 0)

    if not timeline:
        return pd.DataFrame(columns=['month', 'premium', 'commission'])

    # Convert to DataFrame
    timeline_df = pd.DataFrame.from_dict(timeline, orient='index').reset_index()
    timeline_df = timeline_df.rename(columns={'index': 'month'})
    
    # Ensure all columns are present
    for col in ['premium', 'commission']:
        if col not in timeline_df.columns:
            timeline_df[col] = D(0)
            
    return timeline_df.sort_values(by='month').reset_index(drop=True)

def calculate_reinsurance_outward_unexpired_measure(
    engine: Engine,
    measure_val_month: str,
    policy_no: str,
    certi_no: str
) -> Tuple[List[Dict[str, Any]], pd.DataFrame, pd.DataFrame, Dict[str, Any]]:
    """
    Orchestrates the month-by-month calculation for reinsurance outward LRC.
    
    Returns:
        - calculation_logs: 逐月计算日志
        - final_result_df: 最终计量结果
        - cashflow_df: 费用时间线
        - loss_info: 亏损信息字典
    """
    main_logs = StringIO()
    calculation_logs = []
    final_result_df = pd.DataFrame()

    # --- 1. Data Fetching ---
    main_logs.write("步骤 1: 获取计量所需数据...\n")
    try:
        original_df = get_reinsurance_outward_data(engine, policy_no, certi_no)
        measure_prep_df = get_reinsurance_outward_measure_prep_data(engine, policy_no, certi_no)

        if original_df.empty:
            raise ValueError(f"在 'bi_to_cas25.ri_pp_re_mon_arr' 中未找到保单 '{policy_no}' 批单 '{certi_no}' 的原始记录。")
        if measure_prep_df.empty:
            raise ValueError(f"在 'public.int_t_pp_re_mon_arr_new' 中未找到保单 '{policy_no}' 批单 '{certi_no}' 的计量准备数据。")

        original_record = original_df.iloc[0]
        measure_prep_record = measure_prep_df.iloc[0]
        main_logs.write("  - 成功获取原始记录和计量准备数据。\n")
        
        # Combine into a single static data object
        static_data = {**original_record.to_dict(), **measure_prep_record.to_dict()}
        
        # Get rein_type to determine where to fetch loss from
        rein_type = static_data.get('rein_type', '1')

    except Exception as e:
        main_logs.write(f"数据获取失败: {e}\n")
        calculation_logs.append({'month': 'N/A', 'result_df': None, 'logs': [main_logs.getvalue()]})
        return calculation_logs, final_result_df, pd.DataFrame(), {}

    # --- 2. Generate Timeline ---
    main_logs.write("步骤 2: 生成计算时间轴...\n")
    try:
        ini_confirm = static_data.get('ini_confirm')
        if not ini_confirm or pd.isna(ini_confirm):
            raise ValueError("'ini_confirm' 在计量准备数据中为空，无法确定起始计算月份。")

        # Parse ini_confirm
        if isinstance(ini_confirm, str):
            if len(ini_confirm) >= 6:
                start_month_str = ini_confirm[:6]
            else:
                start_month_str = ini_confirm
            start_month_dt = datetime.strptime(start_month_str, '%Y%m').date().replace(day=1)
        else:
            start_month_dt = ini_confirm.replace(day=1)
        
        end_month_dt = datetime.strptime(measure_val_month, '%Y%m').date().replace(day=1)
        
        if start_month_dt > end_month_dt:
            raise ValueError(f"起始月份 {start_month_dt.strftime('%Y%m')} 不能晚于评估月份 {measure_val_month}。")

        # Generate list of months to calculate
        months_to_calculate = []
        current_dt = start_month_dt
        while current_dt <= end_month_dt:
            months_to_calculate.append(current_dt.strftime('%Y%m'))
            current_dt += relativedelta(months=1)

        main_logs.write(f"  - 计算期间: 从 {months_to_calculate[0]} 到 {months_to_calculate[-1]}\n")

        # Build cash flow timeline
        cashflow_df = build_reinsurance_outward_cost_timeline(original_record, measure_prep_record)
        
        # Create cash flow map by month
        cash_flows_map = {}
        for _, row in cashflow_df.iterrows():
            month = row['month']
            cash_flows_map[month] = {
                'premium': Decimal(str(row['premium'])),
                'commission': Decimal(str(row['commission']))
            }

    except Exception as e:
        main_logs.write(f"时间轴生成失败: {e}\n")
        calculation_logs.append({'month': 'N/A', 'result_df': None, 'logs': [main_logs.getvalue()]})
        return calculation_logs, final_result_df, pd.DataFrame(), {}

    # --- 3. Month-by-Month Calculation ---
    main_logs.write("步骤 3: 开始逐月计算...\n")
    previous_month_result_internal = {}
    
    # Fetch all assumptions and rates at once before the loop
    try:
        val_method = static_data.get('val_method', '10')
        if not val_method:
            val_method = '10'
            main_logs.write("  - 警告: 'val_method' 在计量准备数据中为空, 使用默认值 '10'。\n")

        all_assumptions = get_reinsurance_assumptions(engine, val_method)
        all_discount_rates = get_reinsurance_discount_rates(engine)
        main_logs.write(f"  - 成功获取所有精算假设 (评估方法 '{val_method}') 和折现率。\n")
    except Exception as e:
        main_logs.write(f"获取精算假设或折现率失败: {e}\n")
        calculation_logs.append({'month': 'N/A', 'result_df': None, 'logs': [main_logs.getvalue()]})
        return calculation_logs, final_result_df, pd.DataFrame(), {}

    all_monthly_results = []
    loss_info = {} # Initialize to store the last loss_info for the return value

    for val_month in months_to_calculate:
        month_cash_flows = cash_flows_map.get(val_month, {})
        
        # Get the specific assumptions and rates for the current month
        class_code = static_data.get('class_code', 'default')
        current_assumptions = all_assumptions.get(val_month, {}).get(class_code, {})
        current_discount_rates = all_discount_rates.get(val_month, {})

        if not current_assumptions:
            main_logs.write(f"警告: 在评估月 {val_month} 未找到险类 '{class_code}' 的精算假设。\n")
        if not current_discount_rates:
            main_logs.write(f"警告: 在评估月 {val_month} 未找到折现率。\n")
            
        result_for_df, next_prev_result, month_logs_str = _calculate_one_month_outward(
            val_month,
            static_data,
            month_cash_flows,
            previous_month_result_internal,
            current_assumptions,
            current_discount_rates
        )
        
        # --- Get Loss Component for the CURRENT month ---
        try:
            lookup_certi_no = certi_no if certi_no and certi_no.strip() else 'NA'
            
            loss_info = get_underlying_loss_amount(
                engine=engine,
                policy_no=policy_no,
                certi_no=lookup_certi_no,
                rein_type=rein_type,
                val_month=val_month # Match on the same evaluation month
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
        full_log_list = [f"--- 开始计算评估月: {val_month} ---", month_logs_str]
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
            'month': val_month,
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


