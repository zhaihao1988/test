import pandas as pd
from sqlalchemy.engine import Engine
from sqlalchemy import text
import sys
from datetime import datetime

from core.data_fetcher.financial_data import get_actuarial_assumption


def build_iacf_timeline(
    engine: Engine,
    policy_no: str,
    certi_no: str | None,
    ini_confirm_date,
    class_code: str | None,
    premium_cny: float | int | None,
) -> pd.DataFrame:
    """
    构建一个包含所有评估月获取费用的时间线DataFrame。
    对于旧单，在202312增加精算假设费用。
    """
    # --- 修复 certi_no 的匹配逻辑 ---
    is_certi_no_empty = certi_no in [None, "NA", "N/A", "NULL", ""]
    if is_certi_no_empty:
        # 如果批单号为空或'NA', 则匹配数据库中所有表示“空”的值
        certi_no_filter_sql = "(certi_no IS NULL OR certi_no = 'NA' OR certi_no = '')"
    else:
        # 否则，精确匹配批单号
        certi_no_filter_sql = f"certi_no = '{certi_no}'"


    # 1. 获取所有评估月的跟单费用
    sql_fol = f"""
    SELECT
        val_month,
        SUM(iacf_fol_cny) as iacf_fol_cny,
        SUM(iacf_fol_tax) as iacf_fol_tax
    FROM public.int_t_pp_jl_iacf_fol_new
    WHERE policy_no = '{policy_no}' AND {certi_no_filter_sql}
    GROUP BY val_month
    """
    df_fol = pd.read_sql(text(sql_fol), engine)

    # 2. 获取所有评估月的非跟单费用
    sql_unfol = f"""
    SELECT
        val_month,
        SUM(iacf_amount) as iacf_unfol_amt
    FROM public.int_t_pp_jl_iacf_unfol_new
    WHERE policy_no = '{policy_no}' AND {certi_no_filter_sql}
    GROUP BY val_month
    """
    df_unfol = pd.read_sql(text(sql_unfol), engine)

    # 3. 合并跟单和非跟单数据
    if not df_fol.empty and not df_unfol.empty:
        timeline_df = pd.merge(df_fol, df_unfol, on="val_month", how="outer")
    elif not df_fol.empty:
        timeline_df = df_fol
    elif not df_unfol.empty:
        timeline_df = df_unfol
    else:
        timeline_df = pd.DataFrame(columns=['val_month', 'iacf_fol_cny', 'iacf_unfol_amt', 'iacf_fol_tax'])

    timeline_df['actuarial_iacf'] = 0.0

    # 4. 如果是旧单，计算并添加精算假设费用
    print("--- IACF DEBUG START ---", file=sys.stderr)
    print(f"[DEBUG] Input ini_confirm_date: {ini_confirm_date} (Type: {type(ini_confirm_date)})", file=sys.stderr)
    
    # 确保 ini_confirm_date 是 date 对象，以便比较
    effective_ini_confirm_date = ini_confirm_date
    if hasattr(ini_confirm_date, 'date'): # 处理 pandas Timestamp 对象
        effective_ini_confirm_date = ini_confirm_date.date()

    is_old_policy = effective_ini_confirm_date and effective_ini_confirm_date < datetime(2024, 1, 1).date()
    print(f"[DEBUG] Is old policy? {is_old_policy}", file=sys.stderr)

    if is_old_policy:
        ini_confirm_month = effective_ini_confirm_date.strftime('%Y%m')
        print(f"[DEBUG] Calculating for month: {ini_confirm_month}", file=sys.stderr)
        
        actuarial_rate = get_actuarial_assumption(engine, class_code, ini_confirm_month, val_method='8')
        print(f"[DEBUG] Fetched actuarial_rate: {actuarial_rate}", file=sys.stderr)

        actuarial_iacf = (premium_cny or 0) * actuarial_rate
        print(f"[DEBUG] Calculated actuarial_iacf: {actuarial_iacf} = {premium_cny or 0} * {actuarial_rate}", file=sys.stderr)

        # 检查初始确认月是否已存在于DataFrame中
        if ini_confirm_month in timeline_df['val_month'].values:
            # 如果存在，直接更新该行的 'actuarial_iacf' 值
            timeline_df.loc[timeline_df['val_month'] == ini_confirm_month, 'actuarial_iacf'] = actuarial_iacf
        else:
            # 如果不存在，则新增一行
            new_row = pd.DataFrame({
                'val_month': [ini_confirm_month],
                'actuarial_iacf': [actuarial_iacf]
            })
            timeline_df = pd.concat([timeline_df, new_row], ignore_index=True)
    
    print("--- IACF DEBUG END ---", file=sys.stderr)

    # 5. 数据清洗和计算总计
    # --- FIX: 在计算前，确保所有必需的费用列都存在 ---
    required_cols = ['iacf_fol_cny', 'iacf_fol_tax', 'iacf_unfol_amt', 'actuarial_iacf']
    for col in required_cols:
        if col not in timeline_df.columns:
            timeline_df[col] = 0.0

    timeline_df = timeline_df.fillna(0)
    if not timeline_df.empty:
        timeline_df['total_iacf'] = timeline_df['iacf_fol_cny'] + timeline_df['iacf_unfol_amt'] + timeline_df['actuarial_iacf'] + timeline_df['iacf_fol_tax']
    
        # 6. 排序和格式化
        timeline_df = timeline_df.sort_values(by="val_month").reset_index(drop=True)
        
        # 7. 调整列顺序
        final_columns = [
            'val_month', 'iacf_fol_tax', 'iacf_fol_cny', 'iacf_unfol_amt', 
            'actuarial_iacf', 'total_iacf'
        ]
        # 确保所有列都存在，避免KeyError
        for col in final_columns:
            if col not in timeline_df.columns:
                timeline_df[col] = 0
        timeline_df = timeline_df[final_columns]
    
    return timeline_df
