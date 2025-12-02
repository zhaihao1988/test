import streamlit as st
import pandas as pd
import sys
import os
import numpy as np # Added for numeric_cols_loss and numeric_cols_maintenance


# --- 应用程序的其余部分 ---
from shared.db_connector import get_db_engine
from core.data_fetcher.policy_data import get_policy_data
from core.data_fetcher.contract_data import get_latest_contract_data
from core.data_fetcher.financial_data import get_premium_collection_history
from core.calculations.iacf_calculator import build_iacf_timeline
from core.calculations.measure_unexpired_calculator import calculate_unexpired_measure
from core.data_fetcher.comparison_data import get_db_measure_result


st.set_page_config(layout="wide")
st.title("直保 - 保险计量试算工具")

# --- 1. 用户输入 ---
st.header("保单/批单查询")
policy_no = st.text_input("请输入保单号 (Policy No.)")
endorsement_no = st.text_input("请输入批单号 (Endorsement No.) - 可选")

# --- 数据库配置 (移到侧边栏) ---
st.sidebar.header("数据库配置")
env = 'test' # 固定环境为test

# 使用 session_state 来存储查询结果
if 'direct_policy_data' not in st.session_state:
    st.session_state.direct_policy_data = None

if st.button("🔍 查询保单"):
    if not policy_no.strip():
        st.warning("请输入保单号。")
        st.session_state.direct_policy_data = None
    else:
        engine = get_db_engine(env)
        if engine:
            with st.spinner(f"正在从 {env} 环境查询数据..."):
                try:
                    st.session_state.direct_policy_data = get_policy_data(
                        engine,
                        policy_no.strip(),
                        endorsement_no.strip() if endorsement_no else None
                    )
                except Exception as e:
                    st.error(f"数据查询失败: {e}")
                    st.session_state.direct_policy_data = None
                finally:
                    engine.dispose()

# --- 2. 数据展示与详情查询 ---
if st.session_state.direct_policy_data is not None:
    df = st.session_state.direct_policy_data
    if not df.empty:
        st.success(f"查询成功！共找到 {len(df)} 条最新记录。")
        st.dataframe(df)

        # 如果只有一条记录，自动选中；否则让用户选择
        if len(df) == 1:
            selected_idx = 0
            st.info("已自动选择唯一记录。")
        else:
            options = [f"行 {i}: (保单: {row.get('policy_no', 'N/A')}, 批单: {row.get('certi_no', 'N/A')})" for i, row in df.iterrows()]
            selected_option = st.selectbox("请选择一条记录以查看详情:", options)
            selected_idx = options.index(selected_option)

        selected_row = df.iloc[selected_idx]
        selected_policy_no = selected_row['policy_no']
        selected_certi_no = selected_row['certi_no']

        # --- 分隔线 ---
        st.markdown("---")
        st.header(f"保单详情 (保单: {selected_policy_no} | 批单: {selected_certi_no})")

        engine = get_db_engine(env)
        if engine:
            try:
                # --- 2.1 最新合同数据 ---
                with st.spinner("查询最新合同数据..."):
                    latest_contract_df = get_latest_contract_data(engine, selected_policy_no, selected_certi_no)
                    if not latest_contract_df.empty:
                        st.subheader("最新合同计量数据")
                        st.dataframe(latest_contract_df)

                        # --- 2.2 获取费用时间线 ---
                        ini_confirm = latest_contract_df.iloc[0].get('ini_confirm')
                        class_code = latest_contract_df.iloc[0].get('class_code')
                        premium_cny = float(latest_contract_df.iloc[0].get('premium_cny', 0) or 0)

                        timeline_df = build_iacf_timeline(
                            engine, selected_policy_no, selected_certi_no,
                            ini_confirm, class_code, premium_cny
                        )
                        st.subheader("获取费用时间线（所有评估月）")
                        st.dataframe(timeline_df)
                    else:
                        st.warning("未找到该保批单的合同计量数据。")

                # --- 2.3 保费历史 ---
                with st.spinner("查询保费历史..."):
                    history_df = get_premium_collection_history(engine, selected_policy_no, selected_certi_no)
                    if not history_df.empty:
                        st.subheader("保费实收历史")
                        st.dataframe(history_df)
                    else:
                        st.warning("未找到该保批单的保费实收历史。")

                # --- 3. 执行未到期计量 ---
                st.markdown("---")
                st.header("未到期责任计量 (LRC)")

                default_measure_month = pd.to_datetime(selected_row.get('stat_date')).strftime('%Y%m') if selected_row.get('stat_date') else ""
                measure_val_month = st.text_input("请输入计量评估月 (YYYYMM)", value=default_measure_month)

                if st.button("🚀 执行计量"):
                    if not (measure_val_month and len(measure_val_month) == 6):
                        st.error("请输入有效的6位评估月份 (YYYYMM)")
                    else:
                        with st.spinner(f"正在为评估月 {measure_val_month} 执行计量..."):
                            try:
                                final_result_df, calculation_logs = calculate_unexpired_measure(
                                    engine, selected_policy_no, selected_certi_no, measure_val_month
                                )
                                st.subheader("计量最终结果")
                                display_df = final_result_df.copy().drop(columns=['loss_pv_details_df', 'maintenance_pv_details_df'], errors='ignore')
                                st.dataframe(display_df)

                                # --- 新增：与数据库结果进行比较 ---
                                st.subheader("结果比对")
                                with st.spinner("正在从数据库获取比对数据..."):
                                    try:
                                        db_result = get_db_measure_result(engine, measure_val_month, selected_policy_no, selected_certi_no)
                                    except Exception as e:
                                        db_result = {'lrc_no_loss_amt': '数据库中无当期评估结果', 'lrc_loss_amt': '数据库中无当期评估结果'}
                                    
                                    py_lrc_no_loss = final_result_df.iloc[0]['lrc_no_loss_amt']
                                    py_lrc_loss = final_result_df.iloc[0]['lrc_loss_amt']
                                    
                                    db_lrc_no_loss = db_result.get('lrc_no_loss_amt', '数据库中无当期评估结果')
                                    db_lrc_loss = db_result.get('lrc_loss_amt', '数据库中无当期评估结果')
                                    
                                    # 计算差异
                                    try:
                                        # 确保双方都是数值类型再计算
                                        if isinstance(db_lrc_no_loss, str) and '数据库' in db_lrc_no_loss:
                                            diff_no_loss = "N/A"
                                        else:
                                            diff_no_loss = float(py_lrc_no_loss) - float(db_lrc_no_loss)
                                    except (TypeError, ValueError):
                                        diff_no_loss = "N/A" # 如果数据库值无法转换
                                    try:
                                        if isinstance(db_lrc_loss, str) and '数据库' in db_lrc_loss:
                                            diff_loss = "N/A"
                                        else:
                                            diff_loss = float(py_lrc_loss) - float(db_lrc_loss)
                                    except (TypeError, ValueError):
                                        diff_loss = "N/A"

                                    comparison_data = {
                                        '指标': ['LRC非亏损部分 (lrc_no_loss_amt)', 'LRC亏损部分 (lrc_loss_amt)'],
                                        'Python 计算结果': [py_lrc_no_loss, py_lrc_loss],
                                        '数据库现有结果': [db_lrc_no_loss, db_lrc_loss],
                                        '差值': [diff_no_loss, diff_loss]
                                    }
                                    comparison_df = pd.DataFrame(comparison_data)
                                    
                                    # 格式化显示：Python结果始终格式化，数据库结果如果是字符串则保持原样
                                    formatted_data = {
                                        '指标': comparison_df['指标'],
                                        'Python 计算结果': comparison_df['Python 计算结果'].apply(lambda x: f"{float(x):.10f}"),
                                        '数据库现有结果': comparison_df['数据库现有结果'].apply(
                                            lambda x: x if isinstance(x, str) and ('数据库' in x or 'N/A' in x) else f"{float(x):.10f}"
                                        ),
                                        '差值': comparison_df['差值'].apply(
                                            lambda x: x if isinstance(x, str) and x == "N/A" else f"{float(x):.10f}"
                                        )
                                    }
                                    display_df = pd.DataFrame(formatted_data)
                                    
                                    st.dataframe(display_df)


                                st.subheader("详细计算过程")
                                for month_log in calculation_logs:
                                    with st.expander(f"月份: {month_log['month']} 的计算详情"):
                                        st.code("\n".join(month_log['logs']), language="text")
                                        if "亏损测试" in month_log['month']:
                                            loss_pv_df = final_result_df.iloc[0].get('loss_pv_details_df')
                                            if loss_pv_df is not None and not loss_pv_df.empty:
                                                st.write("未来赔付成本折现过程:")
                                                st.dataframe(loss_pv_df)

                                            maintenance_pv_df = final_result_df.iloc[0].get('maintenance_pv_details_df')
                                            if maintenance_pv_df is not None and not maintenance_pv_df.empty:
                                                st.write("未来维持费用折现过程:")
                                                st.dataframe(maintenance_pv_df)
                            except Exception as e:
                                st.error(f"计量计算失败: {e}")
                                import traceback
                                st.code(traceback.format_exc())

            except Exception as e:
                st.error(f"查询详情失败: {e}")
            finally:
                engine.dispose()

    elif st.session_state.direct_policy_data is not None: # explicitly check for empty dataframe
        st.info("未查询到相关保单数据。")
