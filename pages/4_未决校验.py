import streamlit as st
import pandas as pd
from shared.db_connector import get_db_engine
from core.data_fetcher.unsettled_data import (
    get_unsettled_distinct_options, get_unsettled_data, get_claim_payment_pattern, 
    get_discount_rates, get_actuarial_assumptions, get_db_unsettled_result
)
from core.calculations.unsettled_calculator import calculate_direct_unsettled_measure
from datetime import datetime

st.set_page_config(
    page_title="未决赔款校验",
    page_icon="⚖️",
    layout="wide"
)

st.title("未决赔款校验")

# --- 定义常量 ---
WIDGET_KEYS = [
    'val_month_select', 'risk_code_select', 'com_code_select', 'accident_month_select',
    'business_nature_select', 'car_kind_code_select', 'use_nature_code_select'
]
FILTER_FIELDS = [
    'val_month', 'risk_code', 'com_code', 'accident_month',
    'business_nature', 'car_kind_code', 'use_nature_code'
]
VAL_METHOD_MAP = {'直保': '8', '再保分入': '11', '再保分出': '10'}

# --- 初始化 Session State ---
if 'unsettled' not in st.session_state:
    st.session_state.unsettled = {
        'options': {},
        'data_to_process': None,
        'manual_selection': None,
    }
if 'val_method' not in st.session_state:
    st.session_state.val_method = '8' # 默认直保

for key in WIDGET_KEYS:
    if key not in st.session_state:
        st.session_state[key] = '全部'

# --- 数据库配置 ---
st.sidebar.header("数据库配置")
env = 'test'
db_engine = get_db_engine(env)

# --- 核心回调函数 ---
def update_options():
    if db_engine:
        current_filters = {field: st.session_state[key] for field, key in zip(FILTER_FIELDS, WIDGET_KEYS)}
        st.session_state.unsettled['options'] = get_unsettled_distinct_options(
            db_engine, st.session_state.val_method, current_filters
        )

def on_val_method_change():
    st.session_state.val_method = VAL_METHOD_MAP[st.session_state.val_method_selector]
    for key in WIDGET_KEYS:
        st.session_state[key] = '全部'
    update_options()

# --- 页面加载时初始化选项 ---
if not st.session_state.unsettled.get('options'):
    update_options()

# --- UI 渲染 ---
st.header("数据筛选")

st.selectbox(
    "请选择业务类型",
    options=VAL_METHOD_MAP.keys(),
    key='val_method_selector',
    on_change=on_val_method_change
)

st.info("请通过以下级联筛选器定位唯一或多条数据记录。")

options = st.session_state.unsettled.get('options', {})

def get_key_index(key, option_list):
    try:
        return option_list.index(st.session_state[key])
    except (ValueError, KeyError):
        st.session_state[key] = '全部'
        return 0

col1, col2, col3 = st.columns(3)
with col1:
    val_month_opts = options.get('val_month', [])
    st.selectbox("评估月份 (val_month)", val_month_opts, key='val_month_select', on_change=update_options, index=get_key_index('val_month_select', val_month_opts))
    risk_code_opts = options.get('risk_code', [])
    st.selectbox("险种代码 (risk_code)", risk_code_opts, key='risk_code_select', on_change=update_options, index=get_key_index('risk_code_select', risk_code_opts))
with col2:
    com_code_opts = options.get('com_code', [])
    st.selectbox("出单机构 (com_code)", com_code_opts, key='com_code_select', on_change=update_options, index=get_key_index('com_code_select', com_code_opts))
    accident_month_opts = options.get('accident_month', [])
    st.selectbox("事故年月 (accident_month)", accident_month_opts, key='accident_month_select', on_change=update_options, index=get_key_index('accident_month_select', accident_month_opts))
with col3:
    business_nature_opts = options.get('business_nature', [])
    st.selectbox("业务性质 (business_nature)", business_nature_opts, key='business_nature_select', on_change=update_options, index=get_key_index('business_nature_select', business_nature_opts))

col4, col5, col6 = st.columns(3)
with col4:
    car_kind_code_opts = options.get('car_kind_code', [])
    st.selectbox("车辆种类 (car_kind_code)", car_kind_code_opts, key='car_kind_code_select', on_change=update_options, index=get_key_index('car_kind_code_select', car_kind_code_opts))
with col5:
    use_nature_code_opts = options.get('use_nature_code', [])
    st.selectbox("使用性质 (use_nature_code)", use_nature_code_opts, key='use_nature_code_select', on_change=update_options, index=get_key_index('use_nature_code_select', use_nature_code_opts))


if st.button("🔍 查询数据"):
    final_filters = {field: st.session_state[key] for field, key in zip(FILTER_FIELDS, WIDGET_KEYS)}
    final_filters = {k: v for k, v in final_filters.items() if v is not None and v != '全部'}
    
    with st.spinner("正在查询数据..."):
        found_data = get_unsettled_data(db_engine, st.session_state.val_method, final_filters)
        if found_data.empty:
            st.warning("未找到匹配的数据。")
            st.session_state.unsettled['data_to_process'] = None
        elif len(found_data) == 1:
            st.success("成功定位到唯一一条数据记录。")
            st.session_state.unsettled['data_to_process'] = found_data
            st.session_state.unsettled['manual_selection'] = None
        else:
            st.info(f"找到 {len(found_data)} 条匹配的数据，请手动选择一条进行计算。")
            st.session_state.unsettled['data_to_process'] = found_data
            st.session_state.unsettled['manual_selection'] = None

# --- 手动选择 ---
if st.session_state.unsettled['data_to_process'] is not None and len(st.session_state.unsettled['data_to_process']) > 1:
    st.subheader("手动选择记录")
    df_to_show = st.session_state.unsettled['data_to_process']
    
    # Add a "Select" column with buttons
    df_to_show['选择'] = [f"select_{i}" for i in range(len(df_to_show))]
    
    # Display the dataframe with buttons
    st.dataframe(df_to_show)

    # Check if any select button was clicked
    for i in range(len(df_to_show)):
        if st.button(f"选择第 {i+1} 条", key=f"select_btn_{i}"):
            # When a button is clicked, store the selected row and rerun
            selected_row = df_to_show.iloc[[i]]
            st.session_state.unsettled['manual_selection'] = selected_row.drop('选择', axis=1)
            st.rerun()

# --- 执行计算 ---
data_for_calculation = None
# 检查是否有数据可供处理
process_trigger = False
if st.session_state.unsettled['data_to_process'] is not None:
    if len(st.session_state.unsettled['data_to_process']) == 1:
        data_for_calculation = st.session_state.unsettled['data_to_process']
        process_trigger = True
    elif st.session_state.unsettled['manual_selection'] is not None:
        data_for_calculation = st.session_state.unsettled['manual_selection']
        process_trigger = True

if process_trigger and data_for_calculation is not None:
    st.header("计算结果")
    with st.spinner("正在执行计算..."):
        try:
            record = data_for_calculation.iloc[0]
            eval_month = record['val_month']
            
            # 1. 获取辅助数据
            patterns_df = get_claim_payment_pattern(db_engine)
            rates_df = get_discount_rates(db_engine)
            assumptions_df = get_actuarial_assumptions(db_engine, st.session_state.val_method, eval_month)
            
            # 获取数据库中的比对结果
            # 新逻辑：使用所有级联菜单字段 + group_id 进行匹配
            result_filters = {field: record.get(field) for field in FILTER_FIELDS}
            result_filters['group_id'] = record.get('group_id')
            
            # 移除值为 None 的过滤器，以防查询出错
            result_filters = {k: v for k, v in result_filters.items() if pd.notna(v)}

            db_results_series = get_db_unsettled_result(db_engine, st.session_state.val_method, result_filters)

            # 2. 执行计算
            py_results, logs = calculate_direct_unsettled_measure(
                unsettled_data=data_for_calculation,
                assumptions=assumptions_df,
                patterns=patterns_df,
                rates=rates_df,
                evaluation_month=eval_month,
                db_engine=db_engine
            )

            # 3. 展示结果
            st.subheader("📊 结果比对")
            if not db_results_series.empty:
                # FIX: Convert database result index (column names) to lowercase for case-insensitive matching
                db_results_series.index = db_results_series.index.str.lower()

                comparison_df = pd.DataFrame({'指标': py_results.keys(), 'Python 计算结果': py_results.values()})
                comparison_df['数据库现有结果'] = comparison_df['指标'].map(db_results_series).fillna(pd.NA)
                
                # --- 用户要求只展示6个核心指标并翻译 ---
                metrics_map = {
                    'pv_case_current': '已报案赔案现值(当期利率)',
                    'pv_case_accident': '已报案赔案现值(事故时点利率)',
                    'pv_ibnr_current': 'IBNR现值(当期利率)',
                    'pv_ibnr_accident': 'IBNR现值(事故时点利率)',
                    'pv_ulae_current': '理赔费用现值(当期利率)',
                    'pv_ulae_accident': '理赔费用现值(事故时点利率)'
                }
                metrics_to_show = list(metrics_map.keys())
                
                filtered_df = comparison_df[comparison_df['指标'].isin(metrics_to_show)].copy()
                filtered_df['指标'] = filtered_df['指标'].map(metrics_map)


                py_numeric = pd.to_numeric(filtered_df['Python 计算结果'], errors='coerce')
                db_numeric = pd.to_numeric(filtered_df['数据库现有结果'], errors='coerce')
                filtered_df['差异'] = (py_numeric - db_numeric)

                st.dataframe(filtered_df.style.format("{:.10f}", 
                                                              subset=['Python 计算结果', '数据库现有结果', '差异'],
                                                              na_rep='N/A'))
            else:
                st.warning(f"在数据库中未找到评估月份 {eval_month} 和计量单元 {unit_id} 的比对结果。")
                st.dataframe(pd.DataFrame({'指标': py_results.keys(), 'Python 计算结果': py_results.values()}))

            st.subheader("📝 详细计算过程")
            for log_item in logs:
                with st.expander(log_item['title'], expanded=False):
                    if 'summary' in log_item: 
                        st.json(log_item['summary'])
                    
                    log_df = pd.DataFrame(log_item['log'])
                    
                    if not log_df.empty:
                        # Transpose the dataframe
                        transposed_df = log_df.set_index('期数').T
                        
                        # Define which rows (previously columns) are numeric and should be formatted
                        numeric_rows = ['赔付进展因子', '现金流', '累积折现因子', '当期现值']
                        # Ensure only existing rows are selected for formatting
                        rows_to_format = [row for row in numeric_rows if row in transposed_df.index]

                        st.dataframe(transposed_df.style.format("{:.10f}", subset=pd.IndexSlice[rows_to_format, :]))
                    else:
                        st.write("没有详细的计算步骤（例如，金额为0）。")

        except Exception as e:
            st.error(f"计算过程中发生错误: {e}")
            import traceback
            st.code(traceback.format_exc())
            
    # 清空数据以准备下一次查询
    st.session_state.unsettled['data_to_process'] = None
    st.session_state.unsettled['manual_selection'] = None


if db_engine:
    db_engine.dispose()
