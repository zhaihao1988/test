import streamlit as st
import pandas as pd
import numpy as np
import sys
import os

# --- Path Setup ---
current_script_path = os.path.abspath(__file__)
pages_dir = os.path.dirname(current_script_path)
test_dir = os.path.dirname(pages_dir)
project_root = os.path.dirname(test_dir)
if project_root not in sys.path:
    sys.path.insert(0, project_root)

from validation.db_connector import get_db_engine
from test.core.data_fetcher.reinsurance_outward_data import (
    get_reinsurance_outward_data,
    get_reinsurance_outward_measure_prep_data
)
from test.core.calculations.reinsurance_outward_calculator import calculate_reinsurance_outward_unexpired_measure
from test.core.data_fetcher.comparison_data import get_db_reinsurance_outward_measure_result

st.set_page_config(
    page_title="再保分出计量",
    page_icon="📤",
    layout="wide"
)

st.title("再保分出计量工具")

# --- 1. User Input ---
st.header("保单查询")
policy_no = st.text_input("请输入保单号 (Policy No.)", key="reout_policy_no")
certi_no = st.text_input("请输入批单号 (Endorsement No.) - 可选", key="reout_certi_no")

# --- Database Config (Sidebar) ---
st.sidebar.header("数据库配置")
env = st.sidebar.radio("请选择环境:", ('test', 'uat'), index=0, key="reout_env")

# --- Session State ---
if 'reout_data' not in st.session_state:
    st.session_state.reout_data = None

if st.button("🔍 查询保单", key="reout_search"):
    if not policy_no.strip():
        st.warning("请输入保单号。")
        st.session_state.reout_data = None
    else:
        engine = get_db_engine(env)
        if engine:
            with st.spinner(f"正在从 {env} 环境查询数据..."):
                try:
                    st.session_state.reout_data = get_reinsurance_outward_data(
                        engine, policy_no.strip(), certi_no.strip() if certi_no else None
                    )
                except Exception as e:
                    st.error(f"数据查询失败: {e}")
                    st.session_state.reout_data = None
                finally:
                    engine.dispose()

# --- 2. Data Display and Detail Query ---
if st.session_state.reout_data is not None:
    df = st.session_state.reout_data
    if not df.empty:
        st.success(f"查询成功！共找到 {len(df)} 条最新记录。")
        st.dataframe(df)
        
        selected_row = df.iloc[0]
        selected_policy_no = selected_row.get('policy_no')
        selected_certi_no = selected_row.get('certi_no')

        st.markdown("---")
        st.header(f"保单详情 (保单: {selected_policy_no} | 批单: {selected_certi_no})")
        
        engine = get_db_engine(env)
        if engine:
            try:
                # --- 2.1 Latest Measure Prep Data ---
                with st.spinner("查询最新计量准备数据..."):
                    measure_prep_df = get_reinsurance_outward_measure_prep_data(engine, selected_policy_no, selected_certi_no)
                    if not measure_prep_df.empty:
                        st.subheader("计量数据准备阶段结果")
                        st.dataframe(measure_prep_df)
                    else:
                        st.warning("未找到该保批单的计量准备数据。")

                # --- 3. Run Unexpired Measure ---
                st.markdown("---")
                st.header("未到期责任资产计量 (LRA)")
                
                default_measure_month = pd.to_datetime(selected_row.get('stat_date')).strftime('%Y%m') if pd.notna(selected_row.get('stat_date')) else ""
                measure_val_month = st.text_input("请输入计量评估月 (YYYYMM)", value=default_measure_month, key="reout_measure_month")

                if st.button("🚀 执行计量", key="run_reout_measure"):
                    if not (measure_val_month and len(measure_val_month) == 6):
                        st.error("请输入有效的6位评估月份 (YYYYMM)")
                    else:
                        with st.spinner(f"正在为评估月 {measure_val_month} 执行计量..."):
                            try:
                                calculation_logs, final_result_df, cashflow_df, loss_info = calculate_reinsurance_outward_unexpired_measure(
                                    engine, measure_val_month, selected_policy_no, selected_certi_no
                                )
                                
                                if not calculation_logs:
                                    st.warning("计量未生成任何日志。")
                                    st.stop()

                                st.subheader("费用时间线 (Cash Flow)")
                                st.dataframe(cashflow_df)
                                
                                st.subheader("亏损部分信息")
                                st.json(loss_info)

                                st.subheader("详细计算过程 (逐月)")
                                for month_log in calculation_logs:
                                    final_result_df_monthly = month_log.get('result_df')
                                    with st.expander(f"月份: {month_log['month']} 的计算详情"):
                                        st.code("\n".join(month_log.get('logs', [])), language="text")

                                        # Add comparison for the final month
                                        if month_log['month'] == measure_val_month and not final_result_df.empty:
                                            st.markdown("---")
                                            st.markdown("##### 结果比对")
                                            
                                            db_result = get_db_reinsurance_outward_measure_result(engine, measure_val_month, selected_policy_no, selected_certi_no)
                                            py_result = final_result_df.iloc[-1]
                                            
                                            comparison_data = {
                                                '指标': ['非亏损部分 (closing_balance)', '亏损部分 (loss_component)', '未到期责任资产 (lrc_debt)'],
                                                'Python 计算结果': [py_result.get('closing_balance'), py_result.get('loss_component'), py_result.get('lrc_debt')],
                                                '数据库现有结果': [db_result.get('closing_balance'), db_result.get('loss_component'), db_result.get('lrc_debt')],
                                            }
                                            comparison_df = pd.DataFrame(comparison_data)
                                            
                                            # Ensure numeric columns are actually numeric before formatting
                                            comparison_df['Python 计算结果'] = pd.to_numeric(comparison_df['Python 计算结果'], errors='coerce')
                                            comparison_df['数据库现有结果'] = pd.to_numeric(comparison_df['数据库现有结果'], errors='coerce')
                                            
                                            # Calculate difference in a vectorized way
                                            comparison_df['差值'] = comparison_df['Python 计算结果'] - comparison_df['数据库现有结果']

                                            st.dataframe(comparison_df.style.format(
                                                '{:.4f}',
                                                na_rep='N/A',
                                                subset=['Python 计算结果', '数据库现有结果', '差值']
                                            ))

                            except Exception as e:
                                st.error(f"计量计算失败: {e}")
                                import traceback
                                st.code(traceback.format_exc())
            finally:
                if engine:
                    engine.dispose()
    else:
        st.info("未查询到相关保单数据。")
