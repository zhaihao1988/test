import streamlit as st
import pandas as pd
import numpy as np # Added for select_dtypes
import sys
import os

# --- Path Setup ---
# Dynamically adjust the path to include the project root
current_script_path = os.path.abspath(__file__)
pages_dir = os.path.dirname(current_script_path)
test_dir = os.path.dirname(pages_dir)
project_root = os.path.dirname(test_dir)
if project_root not in sys.path:
    sys.path.insert(0, project_root)

from validation.db_connector import get_db_engine
from test.core.data_fetcher.reinsurance_data import get_reinsurance_inward_data, get_reinsurance_measure_prep_data
from test.core.calculations.reinsurance_calculator import calculate_reinsurance_unexpired_measure
from test.core.data_fetcher.comparison_data import get_db_reinsurance_measure_result

st.set_page_config(
    page_title="再保分入计量",
    page_icon="🤝",
    layout="wide"
)

st.title("再保分入计量工具")

# --- 1. User Input ---
st.header("合约查询")
contract_id = st.text_input("请输入合约ID (Contract ID)")

# --- Database Config (Sidebar) ---
st.sidebar.header("数据库配置")
env = st.sidebar.radio("请选择环境:", ('test', 'uat'), index=0, key="reinsurance_env")

# Use session_state to store query results
if 'reinsurance_data' not in st.session_state:
    st.session_state.reinsurance_data = None

if st.button("🔍 查询合约"):
    if not contract_id.strip():
        st.warning("请输入合约ID。")
        st.session_state.reinsurance_data = None
    else:
        engine = get_db_engine(env)
        if engine:
            with st.spinner(f"正在从 {env} 环境查询数据..."):
                try:
                    st.session_state.reinsurance_data = get_reinsurance_inward_data(
                        engine, contract_id.strip()
                    )
                except Exception as e:
                    st.error(f"数据查询失败: {e}")
                    st.session_state.reinsurance_data = None
                finally:
                    engine.dispose()

# --- 2. Data Display and Detail Query ---
if st.session_state.reinsurance_data is not None:
    df = st.session_state.reinsurance_data
    if not df.empty:
        st.success(f"查询成功！共找到 {len(df)} 条最新记录。")
        st.dataframe(df)

        # Let user select a record if multiple are returned (though logic fetches latest 1)
        if len(df) > 1:
            options = [f"行 {i}: (合约: {row.get('contract_id', 'N/A')}, 保单: {row.get('policy_no', 'N/A')})" for i, row in df.iterrows()]
            selected_option = st.selectbox("请选择一条记录以执行计量:", options)
            selected_idx = options.index(selected_option)
        else:
            selected_idx = 0
            st.info("已自动选择唯一记录。")

        selected_row = df.iloc[selected_idx]
        selected_contract_id = selected_row.get('contract_id')
        selected_policy_no = selected_row.get('policy_no')
        selected_certi_no = selected_row.get('certi_no') # Can be None or empty

        # --- Divider ---
        st.markdown("---")
        st.header(f"合约详情 (ID: {selected_contract_id})")

        engine = get_db_engine(env)
        if engine:
            try:
                # --- 2.1 Latest Measure Prep Data ---
                with st.spinner("查询最新计量准备数据..."):
                    measure_prep_df = get_reinsurance_measure_prep_data(engine, selected_contract_id)
                    if not measure_prep_df.empty:
                        st.subheader("计量数据准备阶段结果")
                        st.dataframe(measure_prep_df)

                        st.subheader("关键值")
                        key_values = {
                            "毛保费 (Gross Written Premium)": measure_prep_df.iloc[0].get('premium'),
                            "非跟单获取费用 (Non-proportional Acquisition Costs)": measure_prep_df.iloc[0].get('iacf_unfol')
                        }
                        st.json(key_values)
                    else:
                        st.warning("未找到该合约的计量准备数据。")

                # --- 3. Run Unexpired Measure ---
                st.markdown("---")
                st.header("未到期责任计量 (LRC)")
                
                # Use stat_date from the original query as a sensible default
                default_measure_month = pd.to_datetime(selected_row.get('stat_date')).strftime('%Y%m') if pd.notna(selected_row.get('stat_date')) else ""
                measure_val_month = st.text_input("请输入计量评估月 (YYYYMM)", value=default_measure_month, key="rein_measure_month")

                if st.button("🚀 执行计量", key="run_rein_measure"):
                    if not (measure_val_month and len(measure_val_month) == 6):
                        st.error("请输入有效的6位评估月份 (YYYYMM)")
                    else:
                        with st.spinner(f"正在为评估月 {measure_val_month} 执行计量..."):
                            try:
                                calculation_logs, final_result_df, cashflow_df = calculate_reinsurance_unexpired_measure(
                                    engine, measure_val_month, selected_contract_id, selected_policy_no, selected_certi_no
                                )
                                
                                if not calculation_logs:
                                    st.warning("计量未生成任何日志。")
                                    st.stop()

                                st.subheader("费用时间线 (Cash Flow)")
                                st.dataframe(cashflow_df)
                                
                                st.subheader("详细计算过程 (逐月)")

                                for month_log in calculation_logs:
                                    final_result_df_monthly = month_log.get('result_df')
                                    
                                    with st.expander(f"月份: {month_log['month']} 的计算详情"):
                                        if final_result_df_monthly is None or final_result_df_monthly.empty:
                                            st.code("\n".join(month_log.get('logs', [])), language="text")
                                            st.warning("当月未生成有效计量结果。")
                                            continue

                                        st.markdown("##### 计量最终结果")
                                        # Format only numeric columns to 4 decimal places
                                        numeric_cols = final_result_df_monthly.select_dtypes(include=np.number).columns
                                        format_dict_final = {col: '{:.4f}' for col in numeric_cols}
                                        st.dataframe(final_result_df_monthly.style.format(format_dict_final))

                                        st.markdown("##### 结果比对")
                                        current_month = month_log['month'].replace("评估月: ", "")
                                        with st.spinner(f"正在为 {current_month} 获取比对数据..."):
                                            db_result = get_db_reinsurance_measure_result(engine, current_month, selected_contract_id, selected_policy_no, selected_certi_no)
                                            
                                            py_lrc_no_loss = final_result_df_monthly.iloc[0]['lrc_no_loss_amt']
                                            py_lrc_loss = final_result_df_monthly.iloc[0]['lrc_loss_amt']
                                            db_lrc_no_loss = db_result.get('lrc_no_loss_amt')
                                            db_lrc_loss = db_result.get('lrc_loss_amt')

                                            try: diff_no_loss = float(py_lrc_no_loss) - float(db_lrc_no_loss)
                                            except (TypeError, ValueError): diff_no_loss = "N/A"
                                            try: diff_loss = float(py_lrc_loss) - float(db_lrc_loss)
                                            except (TypeError, ValueError): diff_loss = "N/A"

                                            comparison_data = {
                                                '指标': ['LRC非亏损部分 (lrc_no_loss_amt)', 'LRC亏损部分 (lrc_loss_amt)'],
                                                'Python 计算结果': [py_lrc_no_loss, py_lrc_loss],
                                                '数据库现有结果': [db_lrc_no_loss, db_lrc_loss],
                                                '差值': [diff_no_loss, diff_loss]
                                            }
                                            comparison_df = pd.DataFrame(comparison_data)

                                            # Convert to numeric, coercing errors to NaN
                                            comparison_df['数据库现有结果'] = pd.to_numeric(comparison_df['数据库现有结果'], errors='coerce')
                                            comparison_df['差值'] = pd.to_numeric(comparison_df['差值'], errors='coerce')
                                            
                                            format_dict = {
                                                'Python 计算结果': '{:.4f}',
                                                '数据库现有结果': '{:.4f}',
                                                '差值': '{:.4f}'
                                            }
                                            st.dataframe(comparison_df.style.format(format_dict, na_rep='N/A'))
                                        
                                        st.markdown("##### 详细计算过程日志")
                                        st.code("\n".join(month_log.get('logs', [])), language="text")
                                        
                                        # Display detailed PV breakdown for onerous test months
                                        # Check if this is the final month (measure_val_month) which includes onerous test
                                        if month_log['month'] == measure_val_month:
                                            loss_pv_df = final_result_df_monthly.iloc[0].get('loss_pv_details_df')
                                            if loss_pv_df is not None and not loss_pv_df.empty:
                                                st.markdown("---")
                                                st.markdown("##### 未来赔付成本折现过程")
                                                # Transpose the DataFrame: set 'month' as columns
                                                loss_pv_transposed = loss_pv_df.set_index('month').T
                                                st.dataframe(loss_pv_transposed.style.format('{:.4f}'))
                                                st.markdown(f"**折现值合计 (PV):** `{loss_pv_df['present_value'].sum():.4f}`")
                                            
                                            maintenance_pv_df = final_result_df_monthly.iloc[0].get('maintenance_pv_details_df')
                                            if maintenance_pv_df is not None and not maintenance_pv_df.empty:
                                                st.markdown("---")
                                                st.markdown("##### 未来维持费用折现过程")
                                                # Transpose the DataFrame: set 'month' as columns
                                                maintenance_pv_transposed = maintenance_pv_df.set_index('month').T
                                                st.dataframe(maintenance_pv_transposed.style.format('{:.4f}'))
                                                st.markdown(f"**折现值合计 (PV):** `{maintenance_pv_df['present_value'].sum():.4f}`")

                            except Exception as e:
                                st.error(f"计量计算失败: {e}")
                                import traceback
                                st.code(traceback.format_exc())

            except Exception as e:
                st.error(f"查询详情失败: {e}")
            finally:
                if engine:
                    engine.dispose()
    elif st.session_state.reinsurance_data is not None:
        st.info("未查询到相关合约数据。")
