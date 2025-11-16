import pandas as pd
from sqlalchemy import text, Engine
from typing import Dict, Any
import sys

def get_reinsurance_discount_rates(engine: Engine) -> Dict[str, Dict[int, float]]:
    """
    获取所有评估月份的所有月度远期利率，并按 val_month 组织。
    增加了详细的控制台日志记录以供诊断。
    """
    sql = """
    SELECT val_month, term_month, forward_disrate_value, update_time, id, disrate_type, version
    FROM measure_platform.conf_measure_month_disrate
    ORDER BY val_month, term_month;
    """
    
    # --- 诊断日志 ---
    print("\n--- [DIAGNOSIS] EXECUTING SQL FOR DISCOUNT RATES ---", file=sys.stderr)
    print(sql, file=sys.stderr)
    
    df = pd.read_sql(text(sql), engine)
    
    print(f"\n--- [DIAGNOSIS] RAW DATA FROM DB (total rows: {len(df)}) ---", file=sys.stderr)
    print(df.head(), file=sys.stderr)

    if df.empty:
        return {}

    df_202412 = df[df['val_month'] == '202412']
    print("\n--- [DIAGNOSIS] ORIGINAL DATA FOR val_month='202412', term_month=2 (BEFORE drop_duplicates) ---", file=sys.stderr)
    print(df_202412[df_202412['term_month'] == 2], file=sys.stderr)

    # 关键步骤：去重
    df_after_drop = df.drop_duplicates(subset=['val_month', 'term_month'], keep='first')

    df_202412_after_drop = df_after_drop[df_after_drop['val_month'] == '202412']
    print("\n--- [DIAGNOSIS] DATA FOR val_month='202412', term_month=2 (AFTER drop_duplicates(keep='first')) ---", file=sys.stderr)
    print(df_202412_after_drop[df_202412_after_drop['term_month'] == 2], file=sys.stderr)
    
    rates_map = df_after_drop.groupby('val_month').apply(
        lambda x: pd.Series(x.forward_disrate_value.values, index=x.term_month).to_dict()
    ).to_dict()
    
    return rates_map
