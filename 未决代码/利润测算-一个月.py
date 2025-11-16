import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from dateutil.relativedelta import relativedelta
import os
import warnings

# 忽略警告
warnings.filterwarnings('ignore')

# 1. 读取源数据（增加val_month列）
source_cols = ['评估大类名称', '事故年月', '会计口径case', '会计口径ibnr', '会计口径ulae', '业务类型', 'val_month']
source_df = pd.read_excel('/Users/fan/Desktop/前海/202412&202501再保分出.xlsx',
                          usecols=source_cols, dtype={'事故年月': str, 'val_month': str})

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
curve_df = pd.read_excel('/Users/fan/Desktop/珠峰/月度远期利率曲线不加溢价.xlsx')
# curve_df = pd.read_excel('/Users/fan/Desktop/未决折现/月度远期利率曲线加0.5%溢价.xlsx')
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


# 8. 定义计息函数（使用事故曲线）
def calculate_interest_with_acc_curve(amount, acc_month_str, start_month_str, end_month_str, curve_dict):
    """
    使用事故曲线计算利息
    """
    start_date = datetime.strptime(start_month_str, '%Y%m') + relativedelta(day=31)
    end_date = datetime.strptime(end_month_str, '%Y%m') + relativedelta(day=31)

    # 计算月数
    months = (end_date.year - start_date.year) * 12 + (end_date.month - start_date.month)
    if months <= 0:
        return amount

    # 获取事故曲线
    curve = curve_dict.get(acc_month_str, {})
    current_amount = amount

    # 逐月计息
    for month_offset in range(1, months + 1):
        # 计算总期限（从事故发生时到当前月）
        total_months = (end_date.year - datetime.strptime(acc_month_str, '%Y%m').year) * 12 + \
                       (end_date.month - datetime.strptime(acc_month_str, '%Y%m').month)

        # 获取对应期限的利率
        rate = curve.get(min(total_months, 720), 0)
        current_amount *= (1 + rate)

    return current_amount


# 9. 定义核心处理函数（支持多期评估）
def process_row_for_new_logic(row, col_type, prev_total_pv1, prev_total_pv3, prev_total_pv6, eval_month_str):
    """
    处理单行数据，计算六个现值指标
    新增PV6计算（本评估时点PV3计息一个月）
    """
    class_name = row['评估大类名称']
    acc_month_str = row['事故年月']
    pattern = pattern_dict.get(class_name, [0] * 60)

    # 获取金额值
    value = row[f'会计口径{col_type}']

    # 计算评估时点（月末）
    eval_date = datetime.strptime(eval_month_str, '%Y%m') + relativedelta(day=31)

    # 计算已过月数
    acc_month = datetime.strptime(acc_month_str, '%Y%m')
    months_passed = (eval_date.year - acc_month.year) * 12 + (eval_date.month - acc_month.month) + 1

    # 获取赔付模式
    paid_ratio = sum(pattern[:min(months_passed, 60)])
    unpaid_ratio = max(1 - paid_ratio, 0)

    # 处理浮点数精度问题：如果unpaid_ratio非常小，则视为0
    if unpaid_ratio < 1e-10:
        unpaid_ratio = 0.0

    # 初始化现值
    pv1_unadj = 0.0
    pv3_unadj = 0.0

    # 特殊处理：剩余比例为0的情况
    if unpaid_ratio == 0:
        # 使用当前评估时点的下一个月月末
        next_month = (datetime.strptime(eval_month_str, '%Y%m') + relativedelta(months=1))
        payment_date = next_month.replace(day=1) + relativedelta(months=1, days=-1)

        # 1. 本评估时点现值（当期曲线）
        pv1_unadj = discount_with_eval_curve(value, payment_date, eval_month_str, curve_dict_all)

        # 3. 本评估时点现值（事故曲线）
        pv3_unadj = discount_to_dynamic_eval(acc_month_str, payment_date, value, curve_dict_all, eval_date)
    else:
        # 计算未来现金流分配
        allocs = []
        dates = []

        # 计算未来现金流分配
        for future_month in range(months_passed, 60):
            ratio = pattern[future_month]
            if ratio <= 0:
                continue

            # 计算分配金额
            alloc_amount = value * (ratio / unpaid_ratio)
            allocs.append(alloc_amount)

            # 计算支付日期（月末）
            payment_date = (acc_month + relativedelta(months=future_month)).replace(day=1) + relativedelta(months=1,
                                                                                                           days=-1)
            dates.append(payment_date)

        # 处理每个支付期
        for date, alloc in zip(dates, allocs):
            # 1. 本评估时点现值（当期曲线）
            pv1_unadj += discount_with_eval_curve(alloc, date, eval_month_str, curve_dict_all)

            # 3. 本评估时点现值（事故曲线）
            pv3_unadj += discount_to_dynamic_eval(acc_month_str, date, alloc, curve_dict_all, eval_date)

    # 应用非金融风险调整参数
    risk_adj = risk_adjustment_dict.get((class_name, row['业务类型']), 0.0)
    pv1 = pv1_unadj * (1 + risk_adj)
    pv3 = pv3_unadj * (1 + risk_adj)

    # 设置PV2、PV4、PV5为上一期的总和
    pv2 = prev_total_pv1
    pv4 = prev_total_pv3
    pv5 = prev_total_pv6

    # 计算PV6：使用本评估时点的PV3计息一个月
    next_month_str = (datetime.strptime(eval_month_str, '%Y%m') + relativedelta(months=1)).strftime('%Y%m')
    pv6 = calculate_interest_with_acc_curve(pv3, acc_month_str, eval_month_str, next_month_str, curve_dict_all)

    return pv1, pv2, pv3, pv4, pv5, pv6


# 10. 主处理流程
def main_process():
    # 获取所有评估时点并排序
    eval_months = sorted(source_df['val_month'].unique())

    # 初始化上一期结果总和
    prev_total_pv1 = 0.0
    prev_total_pv3 = 0.0
    prev_total_pv6 = 0.0

    # 结果容器
    all_results = []
    summary_results = []

    # 按评估时点顺序处理
    for month in eval_months:
        month_df = source_df[source_df['val_month'] == month]
        print(f"处理评估时点: {month}, 记录数: {len(month_df)}")

        # 初始化本期总和
        total_pv1 = 0.0
        total_pv3 = 0.0
        total_pv6 = 0.0

        # 处理每一行数据
        for _, row in month_df.iterrows():
            # 处理三种金额类型
            for col_type in ['case', 'ibnr', 'ulae']:
                # 计算现值指标
                pv1, pv2, pv3, pv4, pv5, pv6 = process_row_for_new_logic(
                    row, col_type, prev_total_pv1, prev_total_pv3, prev_total_pv6, month
                )

                # 累加到本期总和
                total_pv1 += pv1
                total_pv3 += pv3
                total_pv6 += pv6

                # 保存明细结果
                all_results.append({
                    'val_month': month,
                    '评估大类': row['评估大类名称'],
                    '事故年月': row['事故年月'],
                    '业务类型': row['业务类型'],
                    '金额类型': col_type,
                    'PV1': pv1,
                    'PV2': pv2,
                    'PV3': pv3,
                    'PV4': pv4,
                    'PV5': pv5,
                    'PV6': pv6
                })

        # 计算会计分录总金额
        oci_policy = True  # 是否使用OCI会计政策

        total_claim_change = total_pv1 - prev_total_pv1  # 已发生赔款负债变动
        total_service_cost = total_pv3 - prev_total_pv6  # 保险服务费用
        total_financial_result = prev_total_pv6 - prev_total_pv3  # 保险财务损益

        if oci_policy:
            total_oci = (total_pv1 - total_pv3) - (prev_total_pv1 - prev_total_pv3)  # 其他综合收益
        else:
            total_financial_result += (total_pv1 - total_pv3) - (prev_total_pv1 - prev_total_pv3)
            total_oci = 0.0

        # 保存汇总结果
        summary_results.append({
            'val_month': month,
            'PV1总和': total_pv1,
            'PV3总和': total_pv3,
            'PV6总和': total_pv6,
            '已发生赔款负债变动': total_claim_change,
            '保险服务费用': total_service_cost,
            '保险财务损益': total_financial_result,
            '其他综合收益': total_oci
        })

        # 更新上一期总和（供下期使用）
        prev_total_pv1 = total_pv1
        prev_total_pv3 = total_pv3
        prev_total_pv6 = total_pv6

    # 创建结果DataFrame
    detail_df = pd.DataFrame(all_results)
    summary_df = pd.DataFrame(summary_results)

    # 保存明细结果
    detail_path = '/Users/fan/Desktop/未决折现/前海未决计量/前海202501再保分出利润明细.xlsx'
    detail_df.to_excel(detail_path, index=False)

    # 保存汇总结果
    summary_path = '/Users/fan/Desktop/未决折现/前海未决计量/前海202501再保分出利润.xlsx'
    summary_df.to_excel(summary_path, index=False)

    print(f"处理完成! 明细结果保存到: {detail_path}")
    print(f"汇总结果保存到: {summary_path}")

    return detail_df, summary_df


# 执行主程序
if __name__ == "__main__":
    detail_df, summary_df = main_process()
