import streamlit as st
import pandas as pd
import numpy as np
import sys
import os


from shared.db_connector import get_db_engine
from core.data_fetcher.reinsurance_outward_data import (
    get_reinsurance_outward_contracts, # Changed from get_reinsurance_outward_groups
    get_reinsurance_outward_source_data,
    get_reinsurance_outward_measure_prep_data
)
from core.calculations.reinsurance_outward_calculator import calculate_reinsurance_outward_unexpired_measure
from core.data_fetcher.comparison_data import get_db_reinsurance_outward_measure_result

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
env = 'test' # 固定环境为test

# --- Session State ---
if 'reout_contracts_df' not in st.session_state:
    st.session_state.reout_contracts_df = pd.DataFrame()

if st.button("🔍 查询保单", key="reout_search"):
    if not policy_no.strip():
        st.warning("请输入保单号。")
        st.session_state.reout_contracts_df = pd.DataFrame()
    else:
        engine = get_db_engine(env)
        if engine:
            with st.spinner(f"正在从 {env} 环境查询保单 '{policy_no}' 的所有合约..."):
                try:
                    st.session_state.reout_contracts_df = get_reinsurance_outward_contracts(
                        engine, policy_no.strip(), certi_no.strip() if certi_no else None
                    )
                except Exception as e:
                    st.error(f"数据查询失败: {e}")
                    st.session_state.reout_contracts_df = pd.DataFrame()
                finally:
                    engine.dispose()

# --- 2. Data Display and Contract Selection ---
if not st.session_state.reout_contracts_df.empty:
    df_contracts = st.session_state.reout_contracts_df
    st.success(f"查询成功！共找到 {len(df_contracts)} 个合约。")
    st.dataframe(df_contracts)
    
    # --- Contract Selection ---
    st.markdown("---")
    st.header("请选择一个合约进行计量")
    
    contract_labels = [
        f"合约 {i+1}: contract_id={row.contract_id} (最新评估月: {row.val_month})"
        for i, row in df_contracts.iterrows()
    ]
    
    selected_label = st.radio(
        "选择合约:",
        options=contract_labels,
        key='reout_contract_selector'
    )
    
    if selected_label:
        selected_index = contract_labels.index(selected_label)
        selected_row = df_contracts.iloc[selected_index]
        selected_policy_no = selected_row.policy_no
        selected_certi_no = selected_row.certi_no
        selected_contract_id = selected_row.contract_id

        st.markdown(f"**当前选择**: `保单号={selected_policy_no}`, `批单号={selected_certi_no}`, `合约ID={selected_contract_id}`")
        
        engine = get_db_engine(env)
        if engine:
            try:
                # --- 2.1 Display Source Data ---
                with st.spinner("查询该合约的源数据..."):
                    source_data_df = get_reinsurance_outward_source_data(engine, selected_policy_no, selected_certi_no, selected_contract_id)
                    if not source_data_df.empty:
                        st.subheader("源数据表结果 (bi_to_cas25.ri_pp_re_mon_arr)")
                        st.dataframe(source_data_df)
                    else:
                        st.warning("未找到该合约的源数据。")

                # --- 2.2 Latest Measure Prep Data ---
                with st.spinner("查询该合约的最新计量准备数据..."):
                    measure_prep_df = get_reinsurance_outward_measure_prep_data(engine, selected_policy_no, selected_certi_no, selected_contract_id)
                    if not measure_prep_df.empty:
                        st.subheader("计量数据准备阶段结果 (public.int_t_pp_re_mon_arr_new)")
                        st.dataframe(measure_prep_df)
                    else:
                        st.warning("未找到该合约的计量准备数据。")

                # --- 3. Run Unexpired Measure ---
                st.markdown("---")
                st.header("未到期责任资产计量 (LRA)")
                
                default_measure_month = selected_row.get('val_month', '')
                measure_val_month = st.text_input("请输入计量评估月 (YYYYMM)", value=default_measure_month, key="reout_measure_month")

                if st.button("🚀 执行计量", key="run_reout_measure"):
                    if not (measure_val_month and len(measure_val_month) == 6):
                        st.error("请输入有效的6位评估月份 (YYYYMM)")
                    else:
                        with st.spinner(f"正在为合约 {selected_contract_id} 在评估月 {measure_val_month} 执行计量..."):
                            try:
                                calculation_logs, final_result_df, cashflow_df, loss_info = calculate_reinsurance_outward_unexpired_measure(
                                    engine, measure_val_month, selected_policy_no, selected_certi_no, selected_contract_id
                                )
                                
                                if not calculation_logs:
                                    st.warning("计量未生成任何日志。")
                                    st.stop()

                                st.subheader("费用时间线 (Cash Flow)")
                                st.dataframe(cashflow_df)
                                
                                st.subheader("亏损部分信息")
                                st.json(loss_info)

                                # --- NEW LAYOUT: Show comparison right after main results ---
                                if not final_result_df.empty:
                                    st.subheader("结果比对")
                                    try:
                                        db_result = get_db_reinsurance_outward_measure_result(engine, measure_val_month, selected_policy_no, selected_certi_no, selected_contract_id)
                                    except Exception as e:
                                        db_result = {
                                            "closing_balance": "数据库中无当期评估结果", 
                                            "loss_component": "数据库中无当期评估结果", 
                                            "lrc_debt": "数据库中无当期评估结果",
                                            "current_investment_amortization": "数据库中无当期评估结果", 
                                            "acc_investment_amortization": "数据库中无当期评估结果"
                                        }
                                    
                                    py_result = final_result_df.iloc[-1]
                                    
                                    metrics = [
                                        ('非亏损部分 (closing_balance)', 'closing_balance'),
                                        ('亏损部分 (loss_component)', 'loss_component'),
                                        ('未到期责任资产 (lrc_debt)', 'lrc_debt'),
                                        ('当期投资成分摊销', 'current_investment_amortization'),
                                        ('累计投资成分摊销', 'acc_investment_amortization')
                                    ]
                                    
                                    comparison_data = {
                                        '指标': [m[0] for m in metrics],
                                        'Python 计算结果': [py_result.get(m[1], 0) for m in metrics],
                                        '数据库现有结果': [db_result.get(m[1], '数据库中无当期评估结果') for m in metrics],
                                    }
                                    
                                    # 计算差值
                                    differences = []
                                    for m in metrics:
                                        py_val = py_result.get(m[1], 0)
                                        db_val = db_result.get(m[1], '数据库中无当期评估结果')
                                        try:
                                            if isinstance(db_val, str) and '数据库' in db_val:
                                                differences.append("N/A")
                                            else:
                                                differences.append(float(py_val) - float(db_val))
                                        except (TypeError, ValueError):
                                            differences.append("N/A")
                                    
                                    comparison_data['差值'] = differences
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
                                    final_result_df_monthly = month_log.get('result_df')
                                    with st.expander(f"月份: {month_log['month']} 的计算详情"):
                                        st.code("\n".join(month_log.get('logs', [])), language="text")

                            except Exception as e:
                                st.error(f"计量计算失败: {e}")
                                import traceback
                                st.code(traceback.format_exc())
            finally:
                if engine:
                    engine.dispose()
    else:
        st.info("未查询到相关合约，或该保单不存在。")
