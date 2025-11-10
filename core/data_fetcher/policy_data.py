import pandas as pd
from sqlalchemy import text
from sqlalchemy.engine import Engine


TABLE_NAME = "bi_to_cas25.pi_policy_data_info_mon"
SELECT_COLUMNS = [
    "policy_no", "certi_no", "stat_date", "start_date", "end_date",
    "valid_date", "class_code", "risk_code", "com_code", "channel_type",
    "coins_flag", "return_flag", "max_pay_rate", "min_pay_rate",
    "amount_limit", "sum_premium_no_tax", "sum_premium_tax", "currency",
    "create_time", "create_by", "update_time", "update_by",
]


def get_policy_data(engine: Engine, policy_no: str, endorsement_no: str = None) -> pd.DataFrame:
    """
    根据保单号和批单号查询 `pi_policy_data_info_mon` 表中 stat_date 最新的数据。
    """
    if not policy_no:
        return pd.DataFrame()

    select_clause = ", ".join([f't."{col}"' for col in SELECT_COLUMNS])
    
    base_query = f"""
        SELECT {select_clause}
        FROM (
            SELECT 
                *,
                ROW_NUMBER() OVER(PARTITION BY policy_no, certi_no ORDER BY stat_date DESC NULLS LAST, create_time DESC NULLS LAST) as rn
            FROM {TABLE_NAME}
            WHERE policy_no = :policy_no
        ) t
        WHERE t.rn = 1
    """
    
    params = {"policy_no": policy_no}

    if endorsement_no:
        query = f"""
            SELECT {select_clause}
            FROM (
                SELECT 
                    *,
                    ROW_NUMBER() OVER(PARTITION BY policy_no, certi_no ORDER BY stat_date DESC NULLS LAST, create_time DESC NULLS LAST) as rn
                FROM {TABLE_NAME}
                WHERE policy_no = :policy_no AND certi_no = :endorsement_no
            ) t
            WHERE t.rn = 1
        """
        params["endorsement_no"] = endorsement_no
    else:
        query = base_query


    with engine.connect() as connection:
        df = pd.read_sql_query(text(query), connection, params=params)

    return df
