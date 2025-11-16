import pandas as pd
from sqlalchemy.engine import Engine
from sqlalchemy import text
from typing import Dict, Any, Tuple, List

def get_reinsurance_inward_assumptions(engine: Engine) -> Dict[str, Dict[str, Any]]:
    """加载所有评估月份的再保分入精算假设数据 (val_method = '11')。"""
    query = text("""
        SELECT val_month, class_code, loss_ratio, maintenance_expense_ratio, 
               indirect_claims_expense_ratio, ra AS risk_adjustment_ratio
        FROM measure_platform.conf_measure_actuarial_assumption
        WHERE val_method = '11'
    """)
    try:
        with engine.connect() as connection:
            df = pd.read_sql(query, connection)
        
        assumptions_map = df.groupby('val_month').apply(
            lambda x: x.set_index('class_code').to_dict('index')
        ).to_dict()
        return assumptions_map
    except Exception as e:
        print(f"Error fetching reinsurance inward assumptions: {e}")
        raise

def get_reinsurance_outward_assumptions(engine: Engine) -> Dict[str, Dict[str, Any]]:
    """加载所有评估月份的再保分出精算假设数据 (val_method = '10')。"""
    query = text("""
        SELECT val_month, class_code, loss_ratio, maintenance_expense_ratio, 
               indirect_claims_expense_ratio, ra AS risk_adjustment_ratio
        FROM measure_platform.conf_measure_actuarial_assumption
        WHERE val_method = '10'
    """)
    try:
        with engine.connect() as connection:
            df = pd.read_sql(query, connection)
        
        assumptions_map = df.groupby('val_month').apply(
            lambda x: x.set_index('class_code').to_dict('index')
        ).to_dict()
        return assumptions_map
    except Exception as e:
        print(f"Error fetching reinsurance outward assumptions: {e}")
        raise

def get_reinsurance_claim_models(engine: Engine) -> Dict[str, List[float]]:
    """
    从数据库加载所有险类的赔付模式, 并按 class_code 进行聚合。
    使用新表 conf_measure_claim_model_new。
    """
    query = text("""
        SELECT class_code, month_id, paid_ratio
        FROM measure_platform.conf_measure_claim_model_new
        ORDER BY class_code, month_id
    """)
    
    with engine.connect() as connection:
        df = pd.read_sql_query(query, connection)
        
    if df.empty:
        return {}

    # 确保 paid_ratio 是浮点数
    df['paid_ratio'] = pd.to_numeric(df['paid_ratio'], errors='coerce').fillna(0.0)

    # 按 class_code 分组, 并将每个组的 paid_ratio 聚合为一个列表
    claim_model_map = df.groupby('class_code')['paid_ratio'].apply(list).to_dict()
    
    return claim_model_map


def get_direct_insurance_loss_map(engine: Engine, policy_no: str, certi_no: str) -> Dict[str, float]:
    """
    获取指定直保保单/批单在所有评估月的亏损部分(lrc_loss_amt)。
    这些数据将作为再保分出计算的基础。
    """
    certi_no_filter_sql = f"certi_no = '{certi_no}'" if certi_no and certi_no != 'NA' else "(certi_no IS NULL OR certi_no = 'NA')"
    sql = f"""
    SELECT 
        val_month,
        lrc_loss_amt
    FROM measure_platform.measure_cx_unexpired
    WHERE policy_no = '{policy_no}'
      AND {certi_no_filter_sql}
      AND val_method = '8' -- 确保只获取直保的正式计量结果
    ORDER BY val_month;
    """
    df = pd.read_sql(text(sql), engine)
    if df.empty:
        return {}
    
    # 将 lrc_loss_amt 转换为 float 类型以避免 Decimal 序列化问题
    df['lrc_loss_amt'] = df['lrc_loss_amt'].astype(float)
    
    return df.set_index('val_month')['lrc_loss_amt'].to_dict()

def get_reinsurance_discount_rates(engine: Engine) -> Dict[str, Dict[int, float]]:
    """
    加载所有评估月份的月度远期利率。
    """
    query = text("""
        SELECT val_month, term_month, forward_disrate_value
        FROM measure_platform.conf_measure_month_disrate
    """)
    try:
        with engine.connect() as connection:
            df = pd.read_sql(query, connection)
            df['forward_disrate_value'] = df['forward_disrate_value'].astype(float)

        rates_map = df.groupby('val_month').apply(
            lambda x: x.set_index('term_month')['forward_disrate_value'].to_dict()
        ).to_dict()
        return rates_map
    except Exception as e:
        print(f"Error fetching reinsurance discount rates: {e}")
        raise

def get_reinsurance_calculation_basis(
    engine: Engine, 
    contract_id: str, 
    policy_no: str,
    certi_no: str, # certi_no is now consistently None for empty values
    val_month: str
) -> pd.DataFrame:
    """
    获取指定合约在评估月的计量基础数据，并关联上一个月的计量结果作为期初。
    """
    current_month_dt = pd.to_datetime(val_month, format='%Y%m')
    prev_month_dt = current_month_dt - pd.DateOffset(months=1)
    prev_val_month = prev_month_dt.strftime('%Y%m')

    # Base query parts
    base_where_clause = """
        WHERE contract_id = :contract_id
          AND policy_no = :policy_no
          AND val_month = :val_month
    """
    prev_where_clause = """
        WHERE contract_id = :contract_id
          AND policy_no = :policy_no
          AND val_month = :prev_val_month
    """

    # Dynamically add certi_no condition
    params = {
        'contract_id': contract_id,
        'policy_no': policy_no,
        'val_month': val_month,
        'prev_val_month': prev_val_month
    }
    
    if certi_no is not None:
        certi_no_condition = "AND certi_no = :certi_no"
        params['certi_no'] = certi_no
    else:
        certi_no_condition = "AND (certi_no IS NULL OR certi_no = '')"

    # Combine query parts
    query = text(f"""
        WITH current_data AS (
            SELECT *
            FROM public.int_t_pp_re_mon_arr_in_new
            {base_where_clause} {certi_no_condition}
        ),
        prev_month_result AS (
            SELECT
                closing_balance AS prev_closing_balance,
                cumulative_ifie_amt AS prev_cumulative_ifie_amt,
                cumulative_no_iacf AS prev_cumulative_no_iacf,
                net_premium_amortization AS prev_net_premium_amortization,
                cumulative_ifie_amt_amortization AS prev_cumulative_ifie_amt_amortization,
                base_investment_amortization AS prev_base_investment_amortization,
                cumulative_no_iacf_amortization AS prev_cumulative_no_iacf_amortization,
                month_count AS prev_month_count
            FROM measure_platform.int_measure_cx_unexpired_rein
            {prev_where_clause} {certi_no_condition}
            LIMIT 1 
        )
        SELECT c.*, p.*
        FROM current_data c
        CROSS JOIN prev_month_result p;
    """)

    try:
        with engine.connect() as connection:
            df = pd.read_sql(query, connection, params=params)

            if df.empty:
                simple_query = text(f"""
                    SELECT *
                    FROM public.int_t_pp_re_mon_arr_in_new
                    {base_where_clause} {certi_no_condition}
                """)
                df = pd.read_sql(simple_query, connection, params=params)

        return df
    except Exception as e:
        print(f"Error fetching reinsurance calculation basis: {e}")
        raise

def get_reinsurance_initial_data(
    engine: Engine, 
    contract_id: str, 
    policy_no: str,
    certi_no: str
) -> pd.DataFrame:
    """
    Fetches the initial contract data for reinsurance inward calculations.
    It now groups by the contract identifiers and sums up the key financial figures
    to get the total amounts for the contract's lifetime.
    """
    if certi_no:
        certi_no_condition = "certi_no = :certi_no"
    else:
        certi_no_condition = "(certi_no IS NULL OR certi_no = '')"

    query = f"""
    SELECT 
        -- Grouping Keys and Static Info (taking the first non-null value)
        MAX(contract_id) as contract_id,
        MAX(policy_no) as policy_no,
        MAX(certi_no) as certi_no,
        MIN(ini_confirm_date) as ini_confirm_date,
        MIN(start_date) as start_date,
        MAX(end_date) as end_date,
        MAX(class_code) as class_code,
        MAX(currency) as currency,
        
        -- Summed Financial Figures
        SUM(premium) as total_premium,
        SUM(commission) as total_commission,
        SUM(brokerage) as total_brokerage,
        
        -- Proportions (taking the average, assuming they are constant for the contract)
        AVG(iacf_follow_prop) as iacf_follow_prop,
        AVG(iacf_unfol_prop) as iacf_unfol_prop,
        AVG(brokerage_prop) as brokerage_prop,
        AVG(commission_prop) as commission_prop

    FROM 
        public.int_t_pp_re_mon_arr_in_new
    WHERE 
        contract_id = :contract_id
        AND policy_no = :policy_no
        AND {certi_no_condition}
    GROUP BY
        contract_id, policy_no, certi_no
    LIMIT 1
    """
    
    params = {'contract_id': contract_id, 'policy_no': policy_no}
    if certi_no:
        params['certi_no'] = certi_no

    with engine.connect() as connection:
        df = pd.read_sql(query, connection, params=params)
    return df
