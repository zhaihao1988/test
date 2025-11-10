import pandas as pd
from sqlalchemy import text
from sqlalchemy.engine import Engine

PREMIUM_HISTORY_TABLE_NAME = "bi_to_cas25.pi_should_rec_pay_off_mon"
GROUP_IACF_TABLE_NAME = "public.t_aoc_fee_final_measure"
GROUP_PREMIUM_TABLE_NAME = "public.int_t_pp_jl_contract_new"
ACTUARIAL_ASSUMPTION_TABLE_NAME = "measure_platform.conf_measure_actuarial_assumption"  # 假设表名
IACF_FOL_TABLE_NAME = "public.int_t_pp_jl_iacf_fol_new"
IACF_UNFOL_TABLE_NAME = "public.int_t_pp_jl_iacf_unfol_new"

def get_premium_collection_history(engine: Engine, policy_no: str, certi_no: str | None) -> pd.DataFrame:
    """从 `pi_should_rec_pay_off_mon` 表获取保费核销历史记录。"""
    
    certi_no_condition = "AND (pr.certi_no IS NULL OR pr.certi_no = '')"
    if certi_no and certi_no.strip().upper() not in ["", "NA", "N/A", "NULL"]:
        certi_no_condition = f"AND pr.certi_no = '{certi_no}'"
        
    query = text(f"""
        SELECT 
            TO_CHAR(pr.cancel_date, 'YYYY-MM-DD') as cancel_date,
            pr.cancel_amount
        FROM {PREMIUM_HISTORY_TABLE_NAME} pr
        WHERE pr.policy_no = :policy_no {certi_no_condition} AND pr.biz_type = '1'
        ORDER BY pr.cancel_date ASC
    """)
    
    with engine.connect() as connection:
        df = pd.read_sql_query(query, connection, params={"policy_no": policy_no})
    
    return df


def get_iacf_amount_for_group(engine: Engine, group_id: str, val_month: str) -> float:
    """获取指定合同组在评估月的总费用。"""
    query = text(f"""
        SELECT SUM(iacf_amount)
        FROM {GROUP_IACF_TABLE_NAME}
        WHERE group_id = :group_id AND TO_CHAR(run_date, 'YYYYMM') = :val_month
    """)
    with engine.connect() as connection:
        result = connection.execute(query, {"group_id": group_id, "val_month": val_month}).scalar_one_or_none()
    return float(result) if result is not None else 0.0


def get_total_premium_for_group(engine: Engine, group_id: str, val_month: str) -> float:
    """获取指定合同组在评估月的总保费。"""
    query = text(f"""
        SELECT SUM(premium_cny)
        FROM {GROUP_PREMIUM_TABLE_NAME}
        WHERE group_id = :group_id AND val_month = :val_month
    """)
    with engine.connect() as connection:
        result = connection.execute(query, {"group_id": group_id, "val_month": val_month}).scalar_one_or_none()
    return float(result) if result is not None else 0.0

def get_actuarial_assumption(engine: Engine, class_code: str, ini_confirm_month: str) -> float:
    """获取精算假设费率。"""
    query = text(f"""
        SELECT first_day_acquisition_expense_ratio
        FROM {ACTUARIAL_ASSUMPTION_TABLE_NAME}
        WHERE class_code = :class_code AND TO_CHAR(start_date, 'YYYYMM') <= :ini_confirm_month AND TO_CHAR(end_date, 'YYYYMM') >= :ini_confirm_month
        LIMIT 1
    """)
    with engine.connect() as connection:
        result = connection.execute(query, {"class_code": class_code, "ini_confirm_month": ini_confirm_month}).scalar_one_or_none()
    return float(result) if result is not None else 0.0

def fetch_iacf_fol_rows(engine: Engine, policy_no: str, certi_no: str | None, val_month: str) -> pd.DataFrame:
    
    empty = certi_no is None or str(certi_no).strip().upper() in {"", "NA", "N/A", "NULL"}
    if empty:
        query = text(f"""
            SELECT *
            FROM {IACF_FOL_TABLE_NAME}
            WHERE val_month = :val_month AND policy_no = :policy_no
              AND (certi_no IS NULL OR certi_no IN ('', 'NA', 'N/A'))
            ORDER BY create_time DESC NULLS LAST
        """)
        params = {"val_month": val_month, "policy_no": policy_no}
    else:
        query = text(f"""
            SELECT *
            FROM {IACF_FOL_TABLE_NAME}
            WHERE val_month = :val_month AND policy_no = :policy_no AND certi_no = :certi_no
            ORDER BY create_time DESC NULLS LAST
        """)
        params = {"val_month": val_month, "policy_no": policy_no, "certi_no": str(certi_no)}

    with engine.connect() as connection:
        return pd.read_sql_query(query, connection, params=params)


def fetch_iacf_unfol_rows(engine: Engine, policy_no: str, certi_no: str | None, val_month: str) -> pd.DataFrame:
    
    empty = certi_no is None or str(certi_no).strip().upper() in {"", "NA", "N/A", "NULL"}
    if empty:
        query = text(f"""
            SELECT *
            FROM {IACF_UNFOL_TABLE_NAME}
            WHERE val_month = :val_month AND policy_no = :policy_no
              AND (certi_no IS NULL OR certi_no IN ('', 'NA', 'N/A'))
            ORDER BY create_time DESC NULLS LAST
        """)
        params = {"val_month": val_month, "policy_no": policy_no}
    else:
        query = text(f"""
            SELECT *
            FROM {IACF_UNFOL_TABLE_NAME}
            WHERE val_month = :val_month AND policy_no = :policy_no AND certi_no = :certi_no
            ORDER BY create_time DESC NULLS LAST
        """)
        params = {"val_month": val_month, "policy_no": policy_no, "certi_no": str(certi_no)}

    with engine.connect() as connection:
        return pd.read_sql_query(query, connection, params=params)

def get_iacf_fol_grouped(engine: Engine, policy_no: str, certi_no: str | None) -> pd.DataFrame:
    """返回各评估月的跟单获取费用汇总: 列 val_month, iacf_fol_cny。"""
    
    empty = certi_no is None or str(certi_no).strip().upper() in {"", "NA", "N/A", "NULL"}
    if empty:
        query = text(f"""
            SELECT val_month, COALESCE(SUM(iacf_fol_cny), 0) AS iacf_fol_cny
            FROM {IACF_FOL_TABLE_NAME}
            WHERE policy_no = :policy_no AND (certi_no IS NULL OR certi_no IN ('', 'NA', 'N/A'))
            GROUP BY val_month
        """)
        params = {"policy_no": policy_no}
    else:
        query = text(f"""
            SELECT val_month, COALESCE(SUM(iacf_fol_cny), 0) AS iacf_fol_cny
            FROM {IACF_FOL_TABLE_NAME}
            WHERE policy_no = :policy_no AND certi_no = :certi_no
            GROUP BY val_month
        """)
        params = {"policy_no": policy_no, "certi_no": str(certi_no)}
    
    with engine.connect() as connection:
        return pd.read_sql_query(query, connection, params=params)

def get_iacf_unfol_grouped(engine: Engine, policy_no: str, certi_no: str | None) -> pd.DataFrame:
    """返回各评估月的非跟单获取费用年累计汇总: 列 val_month, iacf_amount。"""
    
    empty = certi_no is None or str(certi_no).strip().upper() in {"", "NA", "N/A", "NULL"}
    if empty:
        query = text(f"""
            SELECT val_month, COALESCE(SUM(iacf_amount), 0) AS iacf_amount
            FROM {IACF_UNFOL_TABLE_NAME}
            WHERE policy_no = :policy_no AND (certi_no IS NULL OR certi_no IN ('', 'NA', 'N/A'))
            GROUP BY val_month
        """)
        params = {"policy_no": policy_no}
    else:
        query = text(f"""
            SELECT val_month, COALESCE(SUM(iacf_amount), 0) AS iacf_amount
            FROM {IACF_UNFOL_TABLE_NAME}
            WHERE policy_no = :policy_no AND certi_no = :certi_no
            GROUP BY val_month
        """)
        params = {"policy_no": policy_no, "certi_no": str(certi_no)}
        
    with engine.connect() as connection:
        return pd.read_sql_query(query, connection, params=params)
