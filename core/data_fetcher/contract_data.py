import pandas as pd
from sqlalchemy import text
from sqlalchemy.engine import Engine

LATEST_CONTRACT_TABLE_NAME = "public.int_t_pp_jl_contract_new"

def get_latest_contract_data(engine: Engine, policy_no: str, certi_no: str | None) -> pd.DataFrame:
    """
    根据保单号和批单号查询 int_t_pp_jl_contract_new 表中最新一期 (val_month) 的数据。
    当 certi_no 为空或等于 'NA'/'N/A' 时，按空批单匹配（certi_no IS NULL 或 '' 或 'NA'/'N/A'）。
    """
    match_empty = certi_no is None or str(certi_no).strip().upper() in {"", "NA", "N/A", "NULL"}

    if match_empty:
        query = f"""
            SELECT *
            FROM {LATEST_CONTRACT_TABLE_NAME}
            WHERE policy_no = :policy_no
              AND (certi_no IS NULL OR certi_no IN ('', 'NA', 'N/A'))
            ORDER BY val_month DESC NULLS LAST, create_time DESC NULLS LAST
            LIMIT 1
        """
        params = {"policy_no": policy_no}
    else:
        query = f"""
            SELECT *
            FROM {LATEST_CONTRACT_TABLE_NAME}
            WHERE policy_no = :policy_no AND certi_no = :certi_no
            ORDER BY val_month DESC NULLS LAST, create_time DESC NULLS LAST
            LIMIT 1
        """
        params = {"policy_no": policy_no, "certi_no": str(certi_no)}

    with engine.connect() as connection:
        df = pd.read_sql_query(text(query), connection, params=params)

    return df
