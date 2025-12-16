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
    数据来源：measure_platform.measure_cf_basic_data_new
    - iacf_fol_cny：跟单获取费用（当期发生额）
    - iacf_unfol_amt：非跟单获取费用（当期发生额，取自 iacf_unfol_cny）
    - total_iacf：从初始确认月到当前评估月的累计总获取费用

    对于旧单（ini_confirm < 2024-01-01），在初始确认月增加精算假设费用。
    """
    # certi_no 过滤逻辑（与 measure_input_data.py 保持一致）
    certi_no_filter_sql = (
        f"certi_no = '{certi_no}'"
        if certi_no and certi_no not in ['NA', 'N/A', 'NULL', '']
        else "(certi_no IS NULL OR certi_no IN ('NA', 'N/A', ''))"
    )

    # 1. 获取所有评估月的跟单费用（当期发生额）
    sql_fol = f"""
    SELECT
        val_month,
        SUM(COALESCE(iacf_fol_cny, 0)) AS iacf_fol_cny
    FROM measure_platform.measure_cf_basic_data_new
    WHERE policy_no = '{policy_no}' AND {certi_no_filter_sql}
    GROUP BY val_month
    """
    df_fol = pd.read_sql(text(sql_fol), engine)

    # 2. 获取所有评估月的非跟单费用（当期发生额）
    sql_unfol = f"""
    SELECT
        val_month,
        SUM(COALESCE(iacf_unfol_cny, 0)) AS iacf_unfol_amt
    FROM measure_platform.measure_cf_basic_data_new
    WHERE policy_no = '{policy_no}' AND {certi_no_filter_sql}
    GROUP BY val_month
    """
    df_unfol = pd.read_sql(text(sql_unfol), engine)

    # 3. 合并跟单和非跟单数据
    if not df_fol.empty and not df_unfol.empty:
        timeline_df = pd.merge(df_fol, df_unfol, on="val_month", how="outer")
    elif not df_fol.empty:
        timeline_df = df_fol
        timeline_df['iacf_unfol_amt'] = 0.0
    elif not df_unfol.empty:
        timeline_df = df_unfol
        timeline_df['iacf_fol_cny'] = 0.0
    else:
        timeline_df = pd.DataFrame(columns=['val_month', 'iacf_fol_cny', 'iacf_unfol_amt'])

    # 确保必需列存在
    if 'iacf_fol_cny' not in timeline_df.columns:
        timeline_df['iacf_fol_cny'] = 0.0
    if 'iacf_unfol_amt' not in timeline_df.columns:
        timeline_df['iacf_unfol_amt'] = 0.0

    timeline_df['actuarial_iacf'] = 0.0

    # 4. 如果是旧单，计算并添加精算假设费用
    print("--- IACF DEBUG START ---", file=sys.stderr)
    print(f"[DEBUG] Input ini_confirm_date: {ini_confirm_date} (Type: {type(ini_confirm_date)})", file=sys.stderr)

    # 确保 ini_confirm_date 是 date 对象，以便比较
    effective_ini_confirm_date = ini_confirm_date
    if hasattr(ini_confirm_date, 'date'):  # 处理 pandas Timestamp 对象
        effective_ini_confirm_date = ini_confirm_date.date()

    is_old_policy = effective_ini_confirm_date and effective_ini_confirm_date < datetime(2024, 1, 1).date()
    print(f"[DEBUG] Is old policy? {is_old_policy}", file=sys.stderr)

    ini_confirm_month = None
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
                'iacf_fol_cny': [0.0],
                'iacf_unfol_amt': [0.0],
                'actuarial_iacf': [actuarial_iacf]
            })
            timeline_df = pd.concat([timeline_df, new_row], ignore_index=True)

    print("--- IACF DEBUG END ---", file=sys.stderr)

    # 5. 数据清洗
    timeline_df = timeline_df.fillna(0)

    if not timeline_df.empty:
        # 6. 排序（按 val_month 升序）
        timeline_df = timeline_df.sort_values(by="val_month").reset_index(drop=True)

        # 7. 计算累计总获取费用（从初始确认月到当前 val_month 的累计）
        if ini_confirm_month:
            start_month = ini_confirm_month
        else:
            start_month = timeline_df['val_month'].iloc[0] if len(timeline_df) > 0 else None

        if start_month:
            # 计算每月当期费用（跟单 + 非跟单 + 精算假设）
            timeline_df['monthly_iacf'] = (
                timeline_df['iacf_fol_cny'] +
                timeline_df['iacf_unfol_amt'] +
                timeline_df['actuarial_iacf']
            )

            mask = timeline_df['val_month'] >= start_month
            timeline_df.loc[mask, 'total_iacf'] = timeline_df.loc[mask, 'monthly_iacf'].cumsum()
            timeline_df.loc[~mask, 'total_iacf'] = 0.0
            timeline_df = timeline_df.drop(columns=['monthly_iacf'])
        else:
            timeline_df['total_iacf'] = 0.0

        # 8. 调整列顺序（无 iacf_fol_tax）
        final_columns = [
            'val_month', 'iacf_fol_cny', 'iacf_unfol_amt',
            'actuarial_iacf', 'total_iacf'
        ]
        for col in final_columns:
            if col not in timeline_df.columns:
                timeline_df[col] = 0.0
        timeline_df = timeline_df[final_columns]

    return timeline_df
