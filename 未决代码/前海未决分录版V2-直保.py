import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from dateutil.relativedelta import relativedelta
import os
import warnings
import time
import gc
import math

# 忽略警告
warnings.filterwarnings('ignore')

# 1. 读取源数据
print("开始读取源数据...")
source_cols = ['评估大类名称', '事故年月', '会计口径case', '会计口径ibnr', '会计口径ulae','会计口径alae',
              'val_month', 'COM_CODE', 'RISK_CODE',
               'CHANNEL_TYPE', 'CAR_KIND_CODE', 'USE_NATURE_CODE', 'UNDER_YEAR','业务类型']
source_df = pd.read_excel('/Users/fan/Desktop/前海/前海直保未决-分录版源数据.xlsx',
                          usecols=source_cols, dtype={'事故年月': str, 'val_month': str,'COM_CODE':str,'RISK_CODE':str,'CHANNEL_TYPE':str,'UNDER_YEAR':str,'CAR_KIND_CODE':str,'USE_NATURE_CODE':str})
print(f"源数据读取完成，总记录数: {len(source_df)}")

# 2. 读取赔付模式配置
pattern_cols = ['评估大类名称'] + [str(i) for i in range(1, 61)]
pattern_df = pd.read_excel('/Users/fan/Desktop/前海/前海赔付模式.xlsx', usecols=pattern_cols)
pattern_dict = {}
for _, row in pattern_df.iterrows():
    class_name = row['评估大类名称']
    pattern = []
    for month in range(1, 61):  # 1-72个月
        ratio = row[month]
        pattern.append(ratio)
    pattern_dict[class_name] = pattern

# 3. 加载所有远期利率曲线 - 保持原有处理方式
# curve_df = pd.read_excel('/Users/fan/Desktop/未决折现/月度远期利率曲线加0.5%溢价.xlsx')
curve_df = pd.read_excel('/Users/fan/Desktop/珠峰/月度远期利率曲线不加溢价.xlsx')
curve_df['日期'] = pd.to_datetime(curve_df['日期']).dt.strftime('%Y%m')
curve_dict_all = {}

for _, row in curve_df.iterrows():
    date_key = row['日期']
    curve_dict_all[date_key] = {}
    for col in curve_df.columns[1:]:
        if '-month' in col:
            month_idx = int(col.split('-')[0])
            # 保持原有处理方式，不除以12
            curve_dict_all[date_key][month_idx] = row[col]

# 4. 读取非金融风险调整参数
risk_adjustment_df = pd.read_excel('/Users/fan/Desktop/前海/非金融风险调整参数_前海.xlsx')
# 创建字典，键为(评估大类名称, 业务类型)，值为非金融风险调整参数
risk_adjustment_dict = {}
for _, row in risk_adjustment_df.iterrows():
    key = (row['评估大类名称'], row['业务类型'])
    risk_adjustment_dict[key] = row['非金融风险调整参数']


# 5. 定义折现计算函数（事故时点利率折现至评估时点） - 保持原有逻辑
def discount_from_accident_to_evaluation(acc_month_str, payment_date, amount, curve_dict):
    """
    使用事故当月利率曲线将现金流折现至评估时点(2024-12-31)
    保持原有处理逻辑不变
    """
    # 1. 计算事故锁定日期（事故当月月初）
    acc_lock = datetime.strptime(acc_month_str, '%Y%m')

    # 2. 计算关键时间点
    eval_date = datetime(2024, 12, 31)  # 评估时点 - 这里会动态替换

    # 3. 计算总月数（从事故当月到支付日期）
    total_months = (payment_date.year - acc_lock.year) * 12 + (payment_date.month - acc_lock.month)

    # 4. 计算评估时点前的月数（从事故当月到评估时点）
    months_to_eval = (eval_date.year - acc_lock.year) * 12 + (eval_date.month - acc_lock.month)

    # 5. 获取事故当月利率曲线
    curve = curve_dict.get(acc_month_str, {})

    # 6. 计算折现因子（只折现支付日期在评估时点后的部分）
    discount_factor = 1.0
    # 计算需要折现的月数（评估时点后）
    discount_months = total_months - months_to_eval

    # 7. 使用事故当月曲线中对应期限的利率进行折现
    for month_idx in range(1, discount_months + 1):
        # 总期限 = 评估时点前的月数 + 当前折现月数
        total_term = months_to_eval + month_idx
        rate = curve.get(min(total_term, 720), 0)  # 超过720个月用720个月利率
        discount_factor /= (1 + rate)

    return amount * discount_factor


# 6. 定义新的折现函数（支持动态评估时点）
def discount_to_dynamic_eval(acc_month_str, payment_date, amount, curve_dict, eval_date):
    """
    使用事故当月利率曲线将现金流折现至动态评估时点
    保持与原有函数相同的折现逻辑，但支持动态评估时点
    """
    # 1. 计算事故锁定日期（事故当月月初）
    acc_lock = datetime.strptime(acc_month_str, '%Y%m')

    # 2. 计算总月数（从事故当月到支付日期）
    total_months = (payment_date.year - acc_lock.year) * 12 + (payment_date.month - acc_lock.month)

    # 3. 计算评估时点前的月数（从事故当月到评估时点）
    months_to_eval = (eval_date.year - acc_lock.year) * 12 + (eval_date.month - acc_lock.month)

    # 4. 获取事故当月利率曲线
    curve = curve_dict.get(acc_month_str, {})

    # 5. 计算折现因子（只折现支付日期在评估时点后的部分）
    discount_factor = 1.0
    # 计算需要折现的月数（评估时点后）
    discount_months = total_months - months_to_eval

    # 6. 使用事故当月曲线中对应期限的利率进行折现
    for month_idx in range(1, discount_months + 1):
        # 总期限 = 评估时点前的月数 + 当前折现月数
        total_term = months_to_eval + month_idx
        rate = curve.get(min(total_term, 720), 0)  # 超过720个月用720个月利率
        discount_factor /= (1 + rate)

    return amount * discount_factor


# 7. 定义使用评估时点曲线折现的函数
def discount_with_eval_curve(amount, payment_date, eval_month_str, curve_dict):
    """
    使用评估时点曲线折现
    保持与原有函数相同的折现逻辑
    """
    # 评估时点（月末）
    eval_date = datetime.strptime(eval_month_str, '%Y%m') + relativedelta(day=31)

    # 如果支付日期在评估时点之前，直接返回金额
    if payment_date <= eval_date:
        return amount

    # 计算需要折现的月数
    months_to_pay = (payment_date.year - eval_date.year) * 12 + (payment_date.month - eval_date.month)

    # 获取评估时点曲线
    curve = curve_dict.get(eval_month_str, {})
    discount_factor = 1.0

    # 逐月折现
    for m in range(1, months_to_pay + 1):
        rate = curve.get(min(m, 720), 0)  # 保持原有逻辑
        discount_factor /= (1 + rate)

    return amount * discount_factor


# 8. 定义处理单行所有类型的函数（同时计算PV1和PV3）
def process_row_all_types(row, eval_month_str, next_eval_month_str, prev_totals_by_class):
    """
    处理单行数据，同时计算PV1和PV3相关指标
    """
    class_name = row['评估大类名称']
    acc_month_str = row['事故年月']
    pattern = pattern_dict.get(class_name, [0] * 60)

    # 计算评估时点（月末）
    try:
        eval_date = datetime.strptime(eval_month_str, '%Y%m') + relativedelta(day=31)
        acc_date = datetime.strptime(acc_month_str, '%Y%m')
    except:
        # 如果日期无效，返回0值
        return {
            'PV1_case': 0.0, 'PV1_ibnr': 0.0, 'PV1_ulae': 0.0, 'PV1_alae': 0.0,
            'PV3_case': 0.0, 'PV3_ibnr': 0.0, 'PV3_ulae': 0.0, 'PV3_alae': 0.0,
            'PV1_case_ra': 0.0, 'PV1_ibnr_ra': 0.0, 'PV1_ulae_ra': 0.0, 'PV1_alae_ra': 0.0,
            'PV3_case_ra': 0.0, 'PV3_ibnr_ra': 0.0, 'PV3_ulae_ra': 0.0, 'PV3_alae_ra': 0.0
        }

    # 计算已过月数（从事故发生月到评估时点的完整月数）
    months_passed = (eval_date.year - acc_date.year) * 12 + (eval_date.month - acc_date.month) + 1

    # 获取赔付模式
    paid_ratio = sum(pattern[:min(months_passed, 60)])
    unpaid_ratio = max(0, 1 - paid_ratio)  # 确保在0-1范围内

    # 处理浮点数精度问题：如果unpaid_ratio非常小，则视为0
    if unpaid_ratio < 1e-10:
        unpaid_ratio = 0.0

    # 初始化结果
    results = {
        'PV1_case': 0.0, 'PV1_ibnr': 0.0, 'PV1_ulae': 0.0, 'PV1_alae': 0.0,
        'PV3_case': 0.0, 'PV3_ibnr': 0.0, 'PV3_ulae': 0.0, 'PV3_alae': 0.0,
        'PV1_case_ra': 0.0, 'PV1_ibnr_ra': 0.0, 'PV1_ulae_ra': 0.0, 'PV1_alae_ra': 0.0,
        'PV3_case_ra': 0.0, 'PV3_ibnr_ra': 0.0, 'PV3_ulae_ra': 0.0, 'PV3_alae_ra': 0.0
    }

    # 获取非金融风险调整参数
    risk_adj = risk_adjustment_dict.get((class_name, row['业务类型']), 0.0)

    # 处理所有金额类型
    for col_type in ['case', 'ibnr', 'ulae', 'alae']:
        # 获取金额值
        value = row[f'会计口径{col_type}']

        # 跳过0值
        if value == 0 or pd.isna(value):
            continue

        # 初始化现值
        pv1_base = 0.0
        pv3_base = 0.0

        # 特殊处理：剩余比例为0的情况
        if unpaid_ratio == 0:
            # 使用当前评估时点的下一个月月末
            try:
                next_month = (datetime.strptime(eval_month_str, '%Y%m') + relativedelta(months=1))
                payment_date = next_month.replace(day=1) + relativedelta(months=1, days=-1)

                # 计算PV1（使用评估时点曲线折现）
                pv1_base = discount_with_eval_curve(value, payment_date, eval_month_str, curve_dict_all)

                # 计算PV3（使用事故曲线折现）
                pv3_base = discount_to_dynamic_eval(acc_month_str, payment_date, value, curve_dict_all, eval_date)
            except Exception as e:
                print(f"处理剩余比例为0的情况时出错: {e}")
                pv1_base = 0.0
                pv3_base = 0.0
        else:
            # 计算未来现金流分配
            try:
                # 只计算有支付比例的月份
                for future_month in range(months_passed, 60):
                    ratio = pattern[future_month]
                    # 跳过0或负值
                    if ratio <= 0:
                        continue

                    # 计算分配金额
                    alloc_amount = value * (ratio / unpaid_ratio)

                    # 计算支付日期（月末）
                    payment_date = (eval_date + relativedelta(months=future_month - months_passed + 1)).replace(
                        day=1) + relativedelta(months=1, days=-1)

                    # 计算PV1（使用评估时点曲线折现）
                    pv1_base += discount_with_eval_curve(alloc_amount, payment_date, eval_month_str, curve_dict_all)

                    # 计算PV3（使用事故曲线折现）
                    pv3_base += discount_to_dynamic_eval(acc_month_str, payment_date, alloc_amount, curve_dict_all,
                                                         eval_date)
            except Exception as e:
                print(f"处理未来现金流时出错: {e}")
                pv1_base = 0.0
                pv3_base = 0.0

        # 应用非金融风险调整参数
        pv1_ra = pv1_base * risk_adj
        pv3_ra = pv3_base * risk_adj

        # 保存结果
        results[f'PV1_{col_type}'] = pv1_base
        results[f'PV3_{col_type}'] = pv3_base
        results[f'PV1_{col_type}_ra'] = pv1_ra
        results[f'PV3_{col_type}_ra'] = pv3_ra

    return results


def main_process():
    # 开始时间
    start_time = time.time()

    # 获取所有评估时点并排序
    eval_months = sorted(source_df['val_month'].unique())

    # 初始化上一期结果总和（按分组维度）
    prev_group_totals = {}  # 格式: {group_key: {'PV1_bel': xxx, 'PV1_ra': xxx, 'PV3_bel': xxx, 'PV3_ra': xxx}}

    # 结果容器 - 只保留PV3分组汇总结果
    pv3_summary_results = []

    # 按评估时点顺序处理
    for i, month in enumerate(eval_months):
        month_df = source_df[source_df['val_month'] == month]
        print(f"处理评估时点: {month}, 记录数: {len(month_df)}")

        # 初始化本期分组总和
        current_group_totals = {}
        pv3_group_totals = {}

        # 分批处理 - 每批20000条
        batch_size = 20000
        total_rows = len(month_df)
        num_batches = math.ceil(total_rows / batch_size)

        # 处理每个批次
        for batch_idx in range(num_batches):
            batch_start = batch_idx * batch_size
            batch_end = min((batch_idx + 1) * batch_size, total_rows)
            batch_df = month_df.iloc[batch_start:batch_end]
            batch_rows = len(batch_df)

            batch_start_time = time.time()
            print(f"处理批次 {batch_idx + 1}/{num_batches}，记录数: {batch_rows}")

            # 处理批次中的每一行数据
            for idx, row in batch_df.iterrows():
                # 获取分组键
                group_key = (
                    row['业务类型'],
                    row['评估大类名称'],
                    row['COM_CODE'],
                    # row['DEPARTMENT_CODE'],
                    row['RISK_CODE'],
                    row['CHANNEL_TYPE'],
                    row['CAR_KIND_CODE'],
                    row['USE_NATURE_CODE'],
                    row['事故年月'],
                    row['UNDER_YEAR']
                )

                # 初始化分组总计
                if group_key not in pv3_group_totals:
                    pv3_group_totals[group_key] = {
                        'PV1_case': 0.0, 'PV1_ibnr': 0.0, 'PV1_ulae': 0.0, 'PV1_alae': 0.0,
                        'PV3_case': 0.0, 'PV3_ibnr': 0.0, 'PV3_ulae': 0.0, 'PV3_alae': 0.0,
                        'PV1_case_ra': 0.0, 'PV1_ibnr_ra': 0.0, 'PV1_ulae_ra': 0.0, 'PV1_alae_ra': 0.0,
                        'PV3_case_ra': 0.0, 'PV3_ibnr_ra': 0.0, 'PV3_ulae_ra': 0.0, 'PV3_alae_ra': 0.0
                    }

                # 计算该行所有PV1和PV3指标
                try:
                    row_results = process_row_all_types(row, month, None, prev_group_totals)
                except Exception as e:
                    print(f"处理行 {idx} 时出错: {e}")
                    row_results = {
                        'PV1_case': 0.0, 'PV1_ibnr': 0.0, 'PV1_ulae': 0.0, 'PV1_alae': 0.0,
                        'PV3_case': 0.0, 'PV3_ibnr': 0.0, 'PV3_ulae': 0.0, 'PV3_alae': 0.0,
                        'PV1_case_ra': 0.0, 'PV1_ibnr_ra': 0.0, 'PV1_ulae_ra': 0.0, 'PV1_alae_ra': 0.0,
                        'PV3_case_ra': 0.0, 'PV3_ibnr_ra': 0.0, 'PV3_ulae_ra': 0.0, 'PV3_alae_ra': 0.0
                    }

                # 累加到分组总计
                for key in row_results:
                    pv3_group_totals[group_key][key] += row_results[key]

            # 计算批次处理时间
            batch_time = time.time() - batch_start_time
            print(f"批次处理完成，耗时: {batch_time:.2f}秒")

        # 保存PV3分组汇总结果并计算OCI
        print(f"保存评估时点 {month} 的PV3分组汇总结果和OCI...")
        for group_key, totals in pv3_group_totals.items():
            # 计算当前期的PV1和PV3（分别计算BEL和RA部分）
            current_pv1_bel = totals['PV1_case'] + totals['PV1_ibnr'] + totals['PV1_ulae'] + totals['PV1_alae']
            current_pv1_ra = totals['PV1_case_ra'] + totals['PV1_ibnr_ra'] + totals['PV1_ulae_ra'] + totals[
                'PV1_alae_ra']
            current_pv3_bel = totals['PV3_case'] + totals['PV3_ibnr'] + totals['PV3_ulae'] + totals['PV3_alae']
            current_pv3_ra = totals['PV3_case_ra'] + totals['PV3_ibnr_ra'] + totals['PV3_ulae_ra'] + totals[
                'PV3_alae_ra']

            # 总的（用于向后兼容）
            current_pv1_total = current_pv1_bel + current_pv1_ra
            current_pv3_total = current_pv3_bel + current_pv3_ra

            # 获取上一期的PV1和PV3（分别获取BEL和RA部分）
            prev_totals = prev_group_totals.get(group_key, {
                'PV1_bel': 0.0, 'PV1_ra': 0.0,
                'PV3_bel': 0.0, 'PV3_ra': 0.0
            })
            prev_pv1_bel = prev_totals['PV1_bel']
            prev_pv1_ra = prev_totals['PV1_ra']
            prev_pv3_bel = prev_totals['PV3_bel']
            prev_pv3_ra = prev_totals['PV3_ra']

            # 总的（用于向后兼容）
            prev_pv1_total = prev_pv1_bel + prev_pv1_ra
            prev_pv3_total = prev_pv3_bel + prev_pv3_ra

            # 计算OCI：分别计算BEL部分和RA部分
            # OCI = (当前期PV1 - 当前期PV3) - (上期PV1 - 上期PV3)
            oci_bel = (current_pv1_bel - current_pv3_bel) - (prev_pv1_bel - prev_pv3_bel)
            oci_ra = (current_pv1_ra - current_pv3_ra) - (prev_pv1_ra - prev_pv3_ra)
            oci_total = oci_bel + oci_ra  # 总的OCI

            # 保存PV3分组汇总结果（包含拆分后的OCI）
            pv3_summary_results.append({
                'val_month': month,
                '业务类型': group_key[0],
                '评估大类': group_key[1],
                'COM_CODE': group_key[2],
                'RISK_CODE': group_key[3],
                'CHANNEL_TYPE': group_key[4],
                'CAR_KIND_CODE': group_key[5],
                'USE_NATURE_CODE': group_key[6],
                '事故年月': group_key[7],
                'UNDER_YEAR': group_key[8],
                'PV1_case总和': totals['PV1_case'],
                'PV1_ibnr总和': totals['PV1_ibnr'],
                'PV1_ulae总和': totals['PV1_ulae'],
                'PV1_alae总和': totals['PV1_alae'],
                'PV3_case总和': totals['PV3_case'],
                'PV3_ibnr总和': totals['PV3_ibnr'],
                'PV3_ulae总和': totals['PV3_ulae'],
                'PV3_alae总和': totals['PV3_alae'],
                'PV1_case_ra总和': totals['PV1_case_ra'],
                'PV1_ibnr_ra总和': totals['PV1_ibnr_ra'],
                'PV1_ulae_ra总和': totals['PV1_ulae_ra'],
                'PV1_alae_ra总和': totals['PV1_alae_ra'],
                'PV3_case_ra总和': totals['PV3_case_ra'],
                'PV3_ibnr_ra总和': totals['PV3_ibnr_ra'],
                'PV3_ulae_ra总和': totals['PV3_ulae_ra'],
                'PV3_alae_ra总和': totals['PV3_alae_ra'],
                # 新增拆分后的字段
                '本期PV1_BEL': current_pv1_bel,
                '本期PV1_RA': current_pv1_ra,
                '本期PV3_BEL': current_pv3_bel,
                '本期PV3_RA': current_pv3_ra,
                '上期PV1_BEL': prev_pv1_bel,
                '上期PV1_RA': prev_pv1_ra,
                '上期PV3_BEL': prev_pv3_bel,
                '上期PV3_RA': prev_pv3_ra,
                # 保留总的字段（向后兼容）
                '本期PV1总和': current_pv1_total,
                '本期PV3总和': current_pv3_total,
                '上期PV1总和': prev_pv1_total,
                '上期PV3总和': prev_pv3_total,
                # 拆分OCI
                'OCI_BEL': oci_bel,
                'OCI_RA': oci_ra,
                'OCI': oci_total
            })

            # 更新上一期总和（使用新的数据结构）
            current_group_totals[group_key] = {
                'PV1_bel': current_pv1_bel,
                'PV1_ra': current_pv1_ra,
                'PV3_bel': current_pv3_bel,
                'PV3_ra': current_pv3_ra
            }

        # 更新上一期分组总和
        prev_group_totals = current_group_totals

    # 创建结果DataFrame
    pv3_summary_df = pd.DataFrame(pv3_summary_results)

    # 保存PV3汇总结果（包含拆分后的OCI）
    pv3_summary_path = '/Users/fan/Desktop/未决折现/前海未决计量/未决202412结果.xlsx'
    pv3_summary_df.to_excel(pv3_summary_path, index=False)

    # 结束时间
    end_time = time.time()
    elapsed_time = end_time - start_time

    print(f"\n{'=' * 50}")
    print(f"处理完成!")
    print(f"PV3汇总结果（包含拆分后的OCI）保存到: {pv3_summary_path}")
    print(f"总耗时: {elapsed_time:.2f}秒 ({elapsed_time / 60:.2f}分钟)")
    print(f"{'=' * 50}")

    return pv3_summary_df

# 执行主程序
if __name__ == "__main__":
    pv3_summary_df = main_process()