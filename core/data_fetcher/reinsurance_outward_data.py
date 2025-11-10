import pandas as pd
from sqlalchemy.engine import Engine
from sqlalchemy import text
from typing import Dict, Any, Optional

# A centralized place for null-equivalent strings
NULL_EQUIVALENTS = ['', 'NA', 'N/A', 'NONE', 'NULL']

def _get_certi_no_condition(certi_no: str, params: Dict) -> str:
    """
    Generates a standardized SQL condition for certi_no, treating various null-like strings as equivalent.
    """
    if certi_no and certi_no.strip().upper() not in NULL_EQUIVALENTS:
        condition = "AND certi_no = :certi_no"
        params['certi_no'] = certi_no
    else:
        condition = "AND (certi_no IS NULL OR certi_no = '' OR UPPER(certi_no) = 'NA')"
    return condition

def get_reinsurance_outward_data(engine: Engine, policy_no: str, certi_no: Optional[str] = None) -> pd.DataFrame:
    """
    Fetches the latest reinsurance outward data based on policy_no and certi_no.
    从 bi_to_cas25.ri_pp_re_mon_arr 表中获取再保分出原始数据
    """
    params = {'policy_no': policy_no}
    certi_no_condition = _get_certi_no_condition(certi_no, params)
    
    query = text(f"""
        SELECT * 
        FROM bi_to_cas25.ri_pp_re_mon_arr
        WHERE 
            policy_no = :policy_no
            {certi_no_condition}
        ORDER BY stat_date DESC
        LIMIT 1
    """)
    
    try:
        with engine.connect() as connection:
            df = pd.read_sql(query, connection, params=params)
        return df
    except Exception as e:
        print(f"Error fetching reinsurance outward data: {e}")
        raise

def get_reinsurance_outward_measure_prep_data(engine: Engine, policy_no: str, certi_no: Optional[str] = None) -> pd.DataFrame:
    """
    Fetches the latest measure preparation data for a specific reinsurance outward policy.
    从 public.int_t_pp_re_mon_arr_new 表中获取再保分出计量准备数据
    """
    params = {'policy_no': policy_no}
    certi_no_condition = _get_certi_no_condition(certi_no, params)
    
    query = text(f"""
        SELECT *
        FROM public.int_t_pp_re_mon_arr_new
        WHERE policy_no = :policy_no
        {certi_no_condition}
        ORDER BY val_month DESC
        LIMIT 1
    """)
    
    try:
        with engine.connect() as connection:
            df = pd.read_sql(query, connection, params=params)
        return df
    except Exception as e:
        print(f"Error fetching reinsurance outward measure prep data: {e}")
        raise

def get_underlying_loss_amount(
    engine: Engine,
    policy_no: str,
    certi_no: Optional[str] = None,
    rein_type: str = '1',
    val_month: str = None
) -> Dict[str, Any]:
    """
    Fetches the loss amount from the underlying business (direct or reinsurance inward).
    根据 rein_type 从不同表中获取亏损金额：
    - rein_type='1': 直保业务分出 -> 从 measure_cx_unexpired 表取 lrc_loss_cost_policy
    - rein_type='2': 分入业务转分出 -> 从 int_measure_cx_unexpired_rein 表取 loss_component_allocation
    """
    params = {'policy_no': policy_no, 'val_month': val_month}
    certi_no_condition = _get_certi_no_condition(certi_no, params)
    
    try:
        if rein_type == '1':
            query_text = f"""
                SELECT 
                    lrc_loss_cost_policy,
                    lrc_loss_amt,
                    lrc_no_loss_amt
                FROM measure_platform.measure_cx_unexpired
                WHERE 
                    policy_no = :policy_no
                    {certi_no_condition}
                    AND val_month = :val_month
                ORDER BY update_time DESC
                LIMIT 1
            """
        else: # rein_type '2'
            query_text = f"""
                SELECT 
                    loss_component_allocation,
                    loss_component,
                    closing_balance
                FROM measure_platform.int_measure_cx_unexpired_rein
                WHERE 
                    policy_no = :policy_no
                    {certi_no_condition}
                    AND val_month = :val_month
                    AND val_method = '11'
                ORDER BY update_time DESC
                LIMIT 1
            """
        
        query = text(query_text)
        with engine.connect() as connection:
            df = pd.read_sql(query, connection, params=params)
        
        if df.empty:
            return {
                'loss_amount': '未找到',
                'total_loss': '未找到',
                'no_loss': '未找到',
                'rein_type': rein_type
            }
        
        if rein_type == '1':
            return {
                'loss_amount': df.iloc[0]['lrc_loss_cost_policy'],
                'total_loss': df.iloc[0]['lrc_loss_amt'],
                'no_loss': df.iloc[0]['lrc_no_loss_amt'],
                'rein_type': '1'
            }
        else:
            return {
                'loss_amount': df.iloc[0]['loss_component_allocation'],
                'total_loss': df.iloc[0]['loss_component'],
                'no_loss': df.iloc[0]['closing_balance'],
                'rein_type': '2'
            }
            
    except Exception as e:
        print(f"Error fetching underlying loss amount: {e}")
        return {
            'loss_amount': '查询失败',
            'total_loss': '查询失败',
            'no_loss': '查询失败',
            'rein_type': rein_type
        }


