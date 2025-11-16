from sqlalchemy import text, Engine
import pandas as pd
from typing import Optional

def get_reinsurance_inward_data(
    engine: Engine, 
    contract_id: str,
    confirm_date: str,
    pi_start_date: str
) -> pd.DataFrame:
    """
    Fetches the latest reinsurance inward data based on a composite key.
    """
    query = """
    SELECT * 
    FROM bi_to_cas25.ri_pp_re_mon_arr_in
    WHERE 
        contract_id = :contract_id
        AND confirm_date = :confirm_date
        AND pi_start_date = :pi_start_date
    ORDER BY stat_date DESC
    LIMIT 1
    """
    params = {
        'contract_id': contract_id,
        'confirm_date': confirm_date,
        'pi_start_date': pi_start_date
    }
    try:
        with engine.connect() as connection:
            df = pd.read_sql(text(query), connection, params=params)
        return df
    except Exception as e:
        print(f"Error fetching reinsurance inward data: {e}")
        raise

def get_reinsurance_measure_prep_data(
    engine: Engine, 
    contract_id: str,
    confirm_date: str,
    pi_start_date: str
) -> pd.DataFrame:
    """
    Fetches the latest measure preparation data for a specific reinsurance contract.
    """
    query = """
    SELECT *
    FROM public.int_t_pp_re_mon_arr_in_new
    WHERE 
        contract_id = :contract_id
        AND confirm_date = :confirm_date
        AND pi_start_date = :pi_start_date
    ORDER BY val_month DESC
    LIMIT 1
    """
    params = {
        'contract_id': contract_id,
        'confirm_date': confirm_date,
        'pi_start_date': pi_start_date
    }
    try:
        with engine.connect() as connection:
            df = pd.read_sql(text(query), connection, params=params)
        return df
    except Exception as e:
        print(f"Error fetching reinsurance measure prep data: {e}")
        raise

def get_all_reinsurance_measure_records(
    engine: Engine, 
    contract_id: str,
    confirm_date: str,
    pi_start_date: str
) -> pd.DataFrame:
    """
    Fetches ALL historical measurement records for a given composite key.
    """
    query = """
    SELECT 
        val_month,
        ini_confirm,
        premium_cash_flow,
        net_premium_cash_flow,
        iacf_cash_flow,
        no_iacf_cash_flow
    FROM measure_platform.int_measure_cx_unexpired_rein
    WHERE
        contract_id = :contract_id
        AND confirm_date = :confirm_date
        AND pi_start_date = :pi_start_date
    ORDER BY val_month ASC
    """
    # Reformat dates from YYYY-MM-DD to YYYYMMDD to match varchar(8) in the table
    params = {
        'contract_id': contract_id,
        'confirm_date': confirm_date.replace('-', ''),
        'pi_start_date': pi_start_date.replace('-', '')
    }
    try:
        with engine.connect() as connection:
            df = pd.read_sql(text(query), connection, params=params)
        
        # Convert date strings to datetime objects for easier handling
        if 'ini_confirm' in df.columns:
            df['ini_confirm'] = pd.to_datetime(df['ini_confirm'], errors='coerce')

        return df
    except Exception as e:
        print(f"Error fetching all reinsurance measure records: {e}")
        # Return an empty DataFrame on error
        return pd.DataFrame()

def get_reinsurance_inward_versions(engine: Engine, contract_id: str) -> pd.DataFrame:
    """
    Fetches all unique contract versions for a given contract_id, identified by
    the composite key (contract_id, confirm_date, pi_start_date).
    It returns the latest record for each unique version.
    """
    query = """
    SELECT DISTINCT ON (contract_id, confirm_date, pi_start_date)
        contract_id,
        confirm_date,
        pi_start_date,
        policy_no,
        certi_no,
        val_month
    FROM public.int_t_pp_re_mon_arr_in_new
    WHERE contract_id = :contract_id
    ORDER BY contract_id, confirm_date, pi_start_date, val_month DESC;
    """
    params = {'contract_id': contract_id}
    try:
        with engine.connect() as connection:
            df = pd.read_sql(text(query), connection, params=params)
        
        # Format dates for display
        if 'confirm_date' in df.columns:
            df['confirm_date'] = pd.to_datetime(df['confirm_date']).dt.strftime('%Y-%m-%d')
        if 'pi_start_date' in df.columns:
            df['pi_start_date'] = pd.to_datetime(df['pi_start_date']).dt.strftime('%Y-%m-%d')
            
        return df
    except Exception as e:
        print(f"Error fetching reinsurance inward versions: {e}")
        raise