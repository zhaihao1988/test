"""
此模块用于从数据库获取执行未到期责任计量所需的所有输入数据。
"""
import pandas as pd
from sqlalchemy.engine import Engine
from sqlalchemy import text
from typing import Dict

def get_measure_source_data(engine: Engine, val_month: str, policy_no: str, certi_no: str) -> pd.Series:
    """
    获取指定保单在评估月的计量源数据 (对应 Java 代码中的 MeasureCfBasicDataNew)。
    """
    certi_no_filter_sql = f"certi_no = '{certi_no}'" if certi_no and certi_no != 'NA' else "(certi_no IS NULL OR certi_no = 'NA')"
    sql = f"""
    SELECT * 
    FROM measure_platform.measure_cf_basic_data_new
    WHERE val_month = '{val_month}' 
      AND policy_no = '{policy_no}' 
      AND {certi_no_filter_sql}
    LIMIT 1;
    """
    df = pd.read_sql(text(sql), engine)
    if df.empty:
        raise ValueError(f"未找到保单 {policy_no}/{certi_no} 在评估月 {val_month} 的计量源数据。")
    return df.iloc[0]

def get_actuarial_assumptions_map(engine: Engine, val_months: list) -> Dict[str, pd.DataFrame]:
    """
    根据提供的评估月份列表，获取所有相关的精算假设(评估方法='8')，并按 val_month 和 class_code 组织成嵌套字典。
    """
    val_months_str = "','".join(val_months)
    sql = f"""
    SELECT val_month, class_code, acquisition_expense_ratio, first_day_acquisition_expense_ratio,
           loss_ratio, indirect_claims_expense_ratio, maintenance_expense_ratio, ra
    FROM measure_platform.conf_measure_actuarial_assumption
    WHERE val_month IN ('{val_months_str}') AND val_method = '8';
    """
    df = pd.read_sql(text(sql), engine)
    
    # --- 修复 "DataFrame index must be unique" 错误 ---
    # 在转换前，确保 (val_month, class_code) 组合是唯一的
    df = df.drop_duplicates(subset=['val_month', 'class_code'], keep='first')
    
    # 将DataFrame转换成嵌套字典，方便按 val_month -> class_code 查找
    assumptions_map = df.groupby('val_month').apply(lambda x: x.set_index('class_code').to_dict('index')).to_dict()
    return assumptions_map

def get_discount_rates_map(engine: Engine, val_months: list) -> Dict[str, Dict[int, float]]:
    """
    获取指定评估月份列表的所有月度远期利率，并按 val_month 组织。
    """
    val_months_str = "','".join(val_months)
    sql = f"""
    SELECT val_month, term_month, forward_disrate_value
    FROM measure_platform.conf_measure_month_disrate
    WHERE val_month IN ('{val_months_str}')
    ORDER BY term_month;
    """
    df = pd.read_sql(text(sql), engine)

    # --- 修复 "DataFrame index must be unique" 错误 ---
    # 在转换前，确保 (val_month, term_month) 组合是唯一的
    df = df.drop_duplicates(subset=['val_month', 'term_month'], keep='first')
    
    # 将DataFrame转换成嵌套字典，方便按 val_month -> term_month 查找
    rates_map = df.groupby('val_month').apply(lambda x: x.set_index('term_month')['forward_disrate_value'].to_dict()).to_dict()
    return rates_map

def get_claim_model_map(engine: Engine) -> Dict[str, list]:
    """
    获取所有赔付模式，并按 class_code 组织。
    从直保专用的新表中获取。
    """
    sql = """
    SELECT class_code, paid_ratio
    FROM measure_platform.conf_measure_claim_model_new
    ORDER BY class_code, month_id;
    """
    df = pd.read_sql(text(sql), engine)
    
    # 按 class_code 分组，并将 paid_ratio 聚合成列表
    claim_model_map = df.groupby('class_code')['paid_ratio'].apply(list).to_dict()
    return claim_model_map
    
def get_paid_premiums_map(engine: Engine, policy_no: str, certi_no: str) -> Dict[str, float]:
    """
    获取指定保单在指定统计日期的实收保费历史。
    移除 stat_date 过滤，获取全历史数据。
    """
    certi_no_filter_sql = f"certi_no = '{certi_no}'" if certi_no and certi_no != 'NA' else "(certi_no IS NULL OR certi_no = 'NA')"
    
    sql = f"""
    SELECT to_char(cancel_date, 'YYYYMM') as pay_month, SUM(cancel_amount) as amount
    FROM bi_to_cas25.pi_should_rec_pay_off_mon
    WHERE biz_type = '1'
      AND policy_no = '{policy_no}'
      AND {certi_no_filter_sql}
    GROUP BY to_char(cancel_date, 'YYYYMM');
    """
    df = pd.read_sql(text(sql), engine)
    return df.set_index('pay_month')['amount'].to_dict()

def get_iacf_fol_map(engine: Engine, policy_no: str, certi_no: str) -> Dict[str, float]:
    """
    获取指定保单所有月份的实际跟单费用 (含税)。
    """
    certi_no_filter_sql = f"certi_no = '{certi_no}'" if certi_no and certi_no != 'NA' else "(certi_no IS NULL OR certi_no = 'NA')"
    sql = f"""
    SELECT val_month, SUM(COALESCE(iacf_fol_cny, 0) + COALESCE(iacf_fol_tax, 0)) as amount
    FROM public.int_t_pp_jl_iacf_fol_new
    WHERE policy_no = '{policy_no}'
      AND {certi_no_filter_sql}
    GROUP BY val_month;
    """
    df = pd.read_sql(text(sql), engine)
    if df.empty:
        return {}
    return df.set_index('val_month')['amount'].to_dict()

def get_iacf_unfol_map(engine: Engine, policy_no: str, certi_no: str) -> Dict[str, float]:
    """
    获取指定保单所有月份的实际非跟单费用。
    """
    certi_no_filter_sql = f"certi_no = '{certi_no}'" if certi_no and certi_no != 'NA' else "(certi_no IS NULL OR certi_no = 'NA')"
    sql = f"""
    SELECT val_month, SUM(iacf_amount) as amount
    FROM public.int_t_pp_jl_iacf_unfol_new
    WHERE policy_no = '{policy_no}'
      AND {certi_no_filter_sql}
    GROUP BY val_month;
    """
    df = pd.read_sql(text(sql), engine)
    # 如果查询结果为空，返回一个空字典，调用方会将其作为0处理
    if df.empty:
        return {}
    return df.set_index('val_month')['amount'].to_dict()
