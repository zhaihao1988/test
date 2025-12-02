"""
此模块用于从数据库获取已存在的计量结果，用于与Python脚本的计算结果进行比对。
"""
import pandas as pd
from sqlalchemy.engine import Engine
from sqlalchemy import text
from typing import Dict, Any, Tuple

NULL_EQUIVALENTS = ['', 'NA', 'N/A', 'NONE', 'NULL']

def _get_certi_no_condition(certi_no: str, params: Dict) -> str:
    """
    Generates a standardized SQL condition for certi_no, treating various null-like strings as equivalent.
    """
    # Check if certi_no is a meaningful value or a null equivalent
    if certi_no and certi_no.strip().upper() not in NULL_EQUIVALENTS:
        condition = "AND certi_no = :certi_no"
        params['certi_no'] = certi_no
    else:
        # If it's a null equivalent, match against all possible null/empty/'NA' values in the DB
        condition = "AND (certi_no IS NULL OR certi_no = '' OR UPPER(certi_no) = 'NA')"
    return condition

def get_db_measure_result(engine: Engine, val_month: str, policy_no: str, certi_no: str) -> Dict[str, Any]:
    """
    从 measure_platform.measure_cx_unexpired 表中获取指定保单在评估月的已存计量结果。
    """
    params = {'val_month': val_month, 'policy_no': policy_no}
    certi_no_condition = _get_certi_no_condition(certi_no, params)

    sql = f"""
    SELECT 
        lrc_no_loss_amt,
        lrc_loss_amt,
        lrc_loss_cost_policy
    FROM measure_platform.measure_cx_unexpired
    WHERE val_month = :val_month
      AND policy_no = :policy_no
      {certi_no_condition}
    ORDER BY update_time DESC
    LIMIT 1;
    """
    try:
        with engine.connect() as connection:
            df = pd.read_sql(text(sql), connection, params=params)

        if df.empty:
            return {'lrc_no_loss_amt': '数据库中无当期评估结果', 'lrc_loss_amt': '数据库中无当期评估结果', 'lrc_loss_cost_policy': '数据库中无当期评估结果'}
            
        return df.iloc[0].to_dict()
    except Exception as e:
        print(f"Error fetching DB measure result: {e}")
        return {'lrc_no_loss_amt': '数据库中无当期评估结果', 'lrc_loss_amt': '数据库中无当期评估结果', 'lrc_loss_cost_policy': '数据库中无当期评估结果'}


def get_db_reinsurance_measure_result(
    engine: Engine, 
    val_month: str, 
    contract_id: str, 
    confirm_date: str,
    pi_start_date: str
) -> Dict[str, Any]:
    """
    Fetches the measure result from the database for a specific reinsurance contract,
    using the full composite key for precise matching.
    """
    # Reformat dates from YYYY-MM-DD to YYYYMMDD to match varchar(8)
    params = {
        'val_month': val_month, 
        'contract_id': contract_id,
        'confirm_date': confirm_date.replace('-', ''),
        'pi_start_date': pi_start_date.replace('-', '')
    }

    # The original function used policy_no and certi_no, which might be needed for臨分 contracts
    # but for 合約, the main keys are contract_id, confirm_date, pi_start_date.
    # The query below is a robust version that should work for both.
    
    query = f"""
    SELECT 
        closing_balance AS lrc_no_loss_amt,
        loss_component AS lrc_loss_amt
    FROM measure_platform.int_measure_cx_unexpired_rein
    WHERE 
        val_month = :val_month
        AND contract_id = :contract_id
        AND confirm_date = :confirm_date
        AND pi_start_date = :pi_start_date
        AND val_method = '11'
    ORDER BY update_time DESC
    LIMIT 1
    """
    
    try:
        with engine.connect() as connection:
            df = pd.read_sql(text(query), connection, params=params)
        
        if df.empty:
            return {'lrc_no_loss_amt': '数据库中无当期评估结果', 'lrc_loss_amt': '数据库中无当期评估结果'}
            
        return df.iloc[0].to_dict()
    except Exception as e:
        print(f"Error fetching DB reinsurance measure result with composite key: {e}")
        return {'lrc_no_loss_amt': '数据库中无当期评估结果', 'lrc_loss_amt': '数据库中无当期评估结果'}

def get_db_reinsurance_outward_measure_result(engine: Engine, val_month: str, policy_no: str, certi_no: str, contract_id: str) -> Dict:
    """
    获取指定评估月、保批单和合约号的再保分出计量结果。
    """
    certi_no_filter_sql = f"AND certi_no = '{certi_no}'" if certi_no and certi_no != 'NA' else "AND (certi_no IS NULL OR certi_no = 'NA')"
    
    sql = f"""
    SELECT 
        closing_balance,
        loss_component,
        lrc_debt,
        base_investment_amortization_this_period AS current_investment_amortization,
        base_investment_amortization AS acc_investment_amortization
    FROM measure_platform.int_measure_cx_unexpired_rein
    WHERE val_month = '{val_month}'
      AND policy_no = '{policy_no}'
      {certi_no_filter_sql}
      AND contract_id = '{contract_id}'
      AND val_method = '10';
    """
    df = pd.read_sql(text(sql), engine)
    
    if df.empty:
        return {
            "closing_balance": "数据库中无当期评估结果", "loss_component": "数据库中无当期评估结果", "lrc_debt": "数据库中无当期评估结果",
            "current_investment_amortization": "数据库中无当期评估结果", "acc_investment_amortization": "数据库中无当期评估结果"
        }
    return df.iloc[0].to_dict()


