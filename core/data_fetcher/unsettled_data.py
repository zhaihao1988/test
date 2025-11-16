import pandas as pd
from sqlalchemy.engine import Engine
from sqlalchemy import text
import streamlit as st

@st.cache_data(ttl=3600)
def get_unsettled_distinct_options(_engine: Engine, val_method: str, selected_filters: dict) -> dict:
    """
    根据已选择的筛选条件，从 int_t_pp_jl_unsettled_group 表中获取剩余字段的可选项列表。
    """
    all_fields = [
        "val_month", "risk_code", "com_code", "accident_month"
    ]
    options = {}

    with _engine.connect() as connection:
        for field_to_query in all_fields:
            where_clauses = [f'"val_method" = :val_method']
            params = {'val_method': val_method}
            
            for filter_field, filter_value in selected_filters.items():
                if filter_field != field_to_query and filter_value is not None and filter_value != '全部':
                    where_clauses.append(f'"{filter_field}" = :{filter_field}')
                    params[filter_field] = filter_value
            
            query_str = f'SELECT DISTINCT "{field_to_query}" FROM public.int_t_pp_jl_unsettled_group'
            if where_clauses:
                query_str += " WHERE " + " AND ".join(where_clauses)
            
            sql_query = text(query_str)
            df = pd.read_sql(sql_query, connection, params=params)

            field_options = sorted(df[field_to_query].dropna().unique().tolist())
            if field_to_query in ["val_month", "accident_month"]:
                field_options.sort(reverse=True)
            
            options[field_to_query] = ["全部"] + field_options

    return options

def get_unsettled_data(_engine: Engine, val_method: str, selected_filters: dict) -> pd.DataFrame:
    """
    根据组合维度筛选条件，获取 unsettled 数据。
    """
    base_query = 'SELECT * FROM public.int_t_pp_jl_unsettled_group WHERE "val_method" = :val_method'
    params = {'val_method': val_method}
    
    for field, value in selected_filters.items():
        if value is not None and value != '全部':
            base_query += f' AND "{field}" = :{field}'
            params[field] = value
            
    with _engine.connect() as connection:
        df = pd.read_sql(text(base_query), connection, params=params)
    return df

def get_actuarial_assumptions(_engine: Engine, val_method: str, val_month: str) -> pd.DataFrame:
    """
    获取指定评估方法和评估月份的精算假设。
    """
    query = text("""
    SELECT * 
    FROM measure_platform.conf_measure_actuarial_assumption
    WHERE "val_method" = :val_method AND "val_month" = :val_month
    """)
    with _engine.connect() as connection:
        df = pd.read_sql(query, connection, params={"val_method": val_method, "val_month": val_month})
    return df

def get_claim_payment_pattern(_engine: Engine) -> pd.DataFrame:
    """
    获取所有赔付模式。
    """
    query = text('SELECT * FROM measure_platform.conf_measure_claim_model_new ORDER BY "class_code", "month_id"')
    with _engine.connect() as connection:
        df = pd.read_sql(query, connection)
    return df

def get_discount_rates(_engine: Engine) -> pd.DataFrame:
    """
    获取所有月度远期利率。
    """
    query = text('SELECT * FROM measure_platform.conf_measure_month_disrate ORDER BY "val_month", "term_month"')
    with _engine.connect() as connection:
        df = pd.read_sql(query, connection)
    return df

def get_db_unsettled_result(_engine: Engine, val_month: str, unit_id: str, val_method: str) -> pd.Series:
    """
    从最终结果表中获取指定计量单元和评估月份的计量结果,用于比对。
    """
    query = text("""
    SELECT * 
    FROM measure_platform.measure_cx_unsettled 
    WHERE "val_month" = :val_month 
      AND "unit_id" = :unit_id
      AND "val_method" = :val_method
    """)
    with _engine.connect() as connection:
        df = pd.read_sql(query, connection, params={"val_month": val_month, "unit_id": unit_id, "val_method": val_method})
    
    if df.empty:
        return pd.Series(dtype=object)
        
    return df.iloc[0]
