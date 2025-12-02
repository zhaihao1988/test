import streamlit as st
import pandas as pd
import numpy as np
import sys
import os

from shared.db_connector import get_db_engine
from core.data_fetcher.reinsurance_data import get_reinsurance_inward_versions # Updated import
from core.calculations.reinsurance_calculator import calculate_reinsurance_unexpired_measure
from core.data_fetcher.comparison_data import get_db_reinsurance_measure_result

st.set_page_config(
    page_title="再保分入计量",
    page_icon="📥",
    layout="wide"
)

st.title("再保分入计量工具 (按合约)")

# --- Session State Initialization ---
if 'rein_bills_df' not in st.session_state:
    st.session_state.rein_bills_df = pd.DataFrame()
if 'selected_rein_bill_index' not in st.session_state:
    st.session_state.selected_rein_bill_index = None

# --- 1. User Input for Contract ID ---
st.header("合约查询")
contract_id = st.text_input("请输入合约号 (Contract ID)", key="rein_contract_id")

# --- Database Config (Sidebar) ---
st.sidebar.header("数据库配置")
env = 'test' # 固定环境为test

if st.button("🔍 查询合约账单", key="rein_search"):
    if not contract_id.strip():
        st.warning("请输入合约号。")
        st.session_state.rein_bills_df = pd.DataFrame()
    else:
        engine = get_db_engine(env)
        if engine:
            with st.spinner(f"正在从 {env} 环境查询合约 '{contract_id}' 的所有账单..."):
                try:
                    st.session_state.rein_bills_df = get_reinsurance_inward_versions(
                        engine, contract_id.strip()
                    )
                    st.session_state.selected_rein_bill_index = None # Reset selection
                except Exception as e:
                    st.error(f"数据查询失败: {e}")
                    st.session_state.rein_bills_df = pd.DataFrame()
                finally:
                    engine.dispose()

# --- 2. Display Contract Bills and Allow Selection ---
if not st.session_state.rein_bills_df.empty:
    df_bills = st.session_state.rein_bills_df
    st.success(f"查询成功！共找到 {len(df_bills)} 期账单。")
    st.dataframe(df_bills)

    # --- Bill Selection ---
    st.markdown("---")
    st.subheader("请选择一期账单进行计量")
    
    # Create descriptive labels for the radio buttons
    bill_labels = [
        f"账单 {i+1}: confirm_date={row.confirm_date}, pi_start_date={row.pi_start_date}, policy_no={row.policy_no or 'N/A'}"
        for i, row in df_bills.iterrows()
    ]
    
    selected_label = st.radio(
        "选择合约账单:",
        options=bill_labels,
        key='rein_bill_selector'
    )
    
    if selected_label:
        # Find the index of the selected bill
        selected_index = bill_labels.index(selected_label)
        selected_row = df_bills.iloc[selected_index]

        # --- 3. Run Unexpired Measure based on selected bill ---
        st.markdown("---")
        st.header(f"未到期责任负债计量 (LRC)")
        st.markdown(f"**当前选择**: `合约号={selected_row.contract_id}`, `确认日期={selected_row.confirm_date}`, `责任起期={selected_row.pi_start_date}`")

        default_measure_month = selected_row.get('val_month', '')
        measure_val_month = st.text_input("请输入计量评估月 (YYYYMM)", value=default_measure_month, key="rein_measure_month")

        if st.button("🚀 执行计量", key="run_rein_measure"):
            if not (measure_val_month and len(measure_val_month) == 6):
                st.error("请输入有效的6位评估月份 (YYYYMM)")
            else:
                engine = get_db_engine(env)
                if engine:
                    with st.spinner(f"正在为评估月 {measure_val_month} 执行计量..."):
                        try:
                            # --- CRITICAL: Pass the composite key to the calculator ---
                            calculation_logs, final_result_df, cashflow_df = calculate_reinsurance_unexpired_measure(
                                engine=engine,
                                measure_val_month=measure_val_month,
                                contract_id=selected_row.contract_id,
                                policy_no=selected_row.policy_no,
                                certi_no=selected_row.certi_no,
                                confirm_date=selected_row.confirm_date,
                                pi_start_date=selected_row.pi_start_date
                            )
                            
                            if not calculation_logs:
                                st.warning("计量未生成任何日志。")
                                st.stop()

                            st.subheader("费用时间线 (Cash Flow)")
                            st.dataframe(cashflow_df)

                            # --- NEW LAYOUT: Show comparison right after main results ---
                            if not final_result_df.empty:
                                st.subheader("结果比对")
                                py_result = final_result_df[final_result_df['val_month'] == measure_val_month].iloc[0]
                                try:
                                    db_result = get_db_reinsurance_measure_result(
                                        engine,
                                        measure_val_month,
                                        selected_row.contract_id,
                                        selected_row.confirm_date,
                                        selected_row.pi_start_date
                                    )
                                except Exception as e:
                                    db_result = {'lrc_no_loss_amt': '数据库中无当期评估结果', 'lrc_loss_amt': '数据库中无当期评估结果'}
                                
                                db_lrc_no_loss = db_result.get('lrc_no_loss_amt', '数据库中无当期评估结果')
                                db_lrc_loss = db_result.get('lrc_loss_amt', '数据库中无当期评估结果')
                                
                                py_lrc_no_loss = py_result.get('lrc_no_loss_amt')
                                py_lrc_loss = py_result.get('lrc_loss_amt')
                                
                                # 计算差值
                                try:
                                    if isinstance(db_lrc_no_loss, str) and '数据库' in db_lrc_no_loss:
                                        diff_no_loss = "N/A"
                                    else:
                                        diff_no_loss = float(py_lrc_no_loss) - float(db_lrc_no_loss)
                                except (TypeError, ValueError):
                                    diff_no_loss = "N/A"
                                
                                try:
                                    if isinstance(db_lrc_loss, str) and '数据库' in db_lrc_loss:
                                        diff_loss = "N/A"
                                    else:
                                        diff_loss = float(py_lrc_loss) - float(db_lrc_loss)
                                except (TypeError, ValueError):
                                    diff_loss = "N/A"
                                
                                comparison_data = {
                                    '指标': ['非亏损部分 (lrc_no_loss_amt)', '亏损部分 (lrc_loss_amt)'],
                                    'Python 计算结果': [py_lrc_no_loss, py_lrc_loss],
                                    '数据库现有结果': [db_lrc_no_loss, db_lrc_loss],
                                    '差值': [diff_no_loss, diff_loss]
                                }
                                comparison_df = pd.DataFrame(comparison_data)
                                
                                # 格式化显示
                                formatted_data = {
                                    '指标': comparison_df['指标'],
                                    'Python 计算结果': comparison_df['Python 计算结果'].apply(lambda x: f"{float(x):.4f}"),
                                    '数据库现有结果': comparison_df['数据库现有结果'].apply(
                                        lambda x: x if isinstance(x, str) and '数据库' in x else f"{float(x):.4f}"
                                    ),
                                    '差值': comparison_df['差值'].apply(
                                        lambda x: x if isinstance(x, str) and x == "N/A" else f"{float(x):.4f}"
                                    )
                                }
                                display_df = pd.DataFrame(formatted_data)
                                st.dataframe(display_df)

                            st.subheader("详细计算过程 (逐月)")
                            for month_log in calculation_logs:
                                with st.expander(f"月份: {month_log['month']} 的计算详情"):
                                    st.code("\n".join(month_log.get('logs', [])), language="text")
                                    
                                    # Display PV details only for the last month (where the test is performed)
                                    if month_log['month'] == measure_val_month:
                                        py_result = final_result_df[final_result_df['val_month'] == measure_val_month].iloc[0]
                                        loss_pv_df = py_result.get('loss_pv_details_df')
                                        maint_pv_df = py_result.get('maintenance_pv_details_df')
                                        
                                        if loss_pv_df is not None and isinstance(loss_pv_df, pd.DataFrame) and not loss_pv_df.empty:
                                            st.markdown("##### 未来赔付成本折现详情")
                                            # Convert Decimals to float for reliable styling in Streamlit
                                            display_loss_df = loss_pv_df.apply(pd.to_numeric, errors='coerce')
                                            st.dataframe(display_loss_df.style.format('{:.4f}', na_rep='N/A'))
                                        
                                        if maint_pv_df is not None and isinstance(maint_pv_df, pd.DataFrame) and not maint_pv_df.empty:
                                            st.markdown("##### 未来维持费用折现详情")
                                            # Convert Decimals to float for reliable styling in Streamlit
                                            display_maint_df = maint_pv_df.apply(pd.to_numeric, errors='coerce')
                                            st.dataframe(display_maint_df.style.format('{:.4f}', na_rep='N/A'))

                        except Exception as e:
                            st.error(f"计量计算失败: {e}")
                            import traceback
                            st.code(traceback.format_exc())
                        finally:
                            if engine:
                                engine.dispose()
else:
    if st.session_state.rein_bills_df is not None: # Avoid showing this on first load
        st.info("未查询到相关合约账单，或该合约不存在。")
