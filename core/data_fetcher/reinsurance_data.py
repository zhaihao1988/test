from sqlalchemy import text, Engine
import pandas as pd
from typing import Optional

def get_reinsurance_inward_data(engine: Engine, contract_id: str) -> pd.DataFrame:
    """
    Fetches the latest reinsurance inward data based on contract_id.
    """
    
    query = """
    SELECT * 
    FROM bi_to_cas25.ri_pp_re_mon_arr_in
    WHERE 
        contract_id = :contract_id
    ORDER BY stat_date DESC
    LIMIT 1
    """
    
    try:
        with engine.connect() as connection:
            df = pd.read_sql(text(query), connection, params={'contract_id': contract_id})
        return df
    except Exception as e:
        print(f"Error fetching reinsurance inward data: {e}")
        raise

def get_reinsurance_measure_prep_data(engine: Engine, contract_id: str) -> pd.DataFrame:
    """
    Fetches the latest measure preparation data for a specific reinsurance contract.
    """
    query = """
    SELECT *
    FROM public.int_t_pp_re_mon_arr_in_new
    WHERE contract_id = :contract_id
    ORDER BY val_month DESC
    LIMIT 1
    """
    try:
        with engine.connect() as connection:
            df = pd.read_sql(text(query), connection, params={'contract_id': contract_id})
        return df
    except Exception as e:
        print(f"Error fetching reinsurance measure prep data: {e}")
        raise
