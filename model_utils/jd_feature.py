#!/usr/bin/python
# -*- coding: UTF-8 -*-
# 导入库
from model_utils.jd_feature_utils import *
from model_utils.public_feature_utils import *







# 获取jd特征
def get_deal_jd_data(all_deal_data_dict):
    jdInfo = all_deal_data_dict['jd_df']
    # 过滤空值
    jdInfo = jdInfo[jdInfo['jingdong_info'].notnull()]

    # 提取指定数据
    jdInfo['msg'] = jdInfo['jingdong_info'].apply(lambda x: get_specified_msg_data(x))

    # 筛选
    jdInfo = jdInfo[jdInfo['msg'] == '调用成功,有效数据'].reset_index(drop=True)

    # 将dict数据转换成df数据
    df = jdInfo
    target_col = 'jingdong_info'
    id_col = 'loan_id'
    col_list = ['data']
    jdInfo_df = analyze_dict_to_df(df, id_col, target_col, col_list)
    return jdInfo_df


# 获取特征
def  get_all_jd_feature(all_deal_data_dict, loan_id, run_mode, jd_feature_list):
    # 获取jd特征
    jdInfo_df = get_deal_jd_data(all_deal_data_dict)

    # 去除无关列
    del_col_list = ['city']
    detail_list = []
    del_col_list = del_col_list + detail_list
    bh_bh_report_finance_lease_day_summary = remove_irrelevant_columns(jdInfo_df,
                                                                       del_col_list)

    # 处理pay_preference字段
    # 首先获取，该字段所有的取值
    # pay_preference_values_list = list(set(bh_bh_report_finance_lease_day_summary['pay_preference']))
    pay_preference_values_list = read_pickle('jd_report_pay_preference_values_list', './data/pkl/')

    # 取值列表
    new_pay_preference_values_list = []
    for pay_preference_values in pay_preference_values_list:
        pay_preference_values = str(pay_preference_values)
        pay_preference_list = pay_preference_values.split(',')
        for pay_preference_one in pay_preference_list:
            pay_preference_one = pay_preference_one.replace(' ', '')
            new_pay_preference_values_list.append(pay_preference_one)

    # 根据列表添加新的字段
    new_col_list = []
    for col in new_pay_preference_values_list:
        new_col = 'pay_preference_' + str(col)
        bh_bh_report_finance_lease_day_summary[new_col] = np.nan
        if new_col not in new_col_list:
            new_col_list.append(new_col)

    # 根据取值进行0或1映射
    name = 'pay_preference'
    for col_name in new_col_list:
        bh_bh_report_finance_lease_day_summary[col_name] = bh_bh_report_finance_lease_day_summary[name].apply(
            lambda x: whether_value_exists(x, col_name, name))

    # 解析后去除
    bh_bh_report_finance_lease_day_summary = bh_bh_report_finance_lease_day_summary.drop([name], axis=1)


    # 处理pur_preference字段
    # 首先获取，该字段所有的取值
    # pur_preference_values_list = list(set(bh_bh_report_finance_lease_day_summary['pur_preference']))
    pur_preference_values_list = read_pickle('jd_report_pur_preference_values_list', './data/pkl/')



    # 取值列表
    new_pur_preference_values_list = []
    for pur_preference_values in pur_preference_values_list:
        pur_preference_list = pur_preference_values.split(',')
        for pur_preference_one in pur_preference_list:
            pur_preference_one = pur_preference_one.replace(' ', '')
            new_pur_preference_values_list.append(pur_preference_one)

    # 根据列表添加新的字段
    new_col_list = []
    for col in new_pur_preference_values_list:
        new_col = 'pur_preference_' + str(col)
        bh_bh_report_finance_lease_day_summary[new_col] = np.nan
        if new_col not in new_col_list:
            new_col_list.append(new_col)

    # 根据取值进行0或1映射
    name = 'pur_preference'
    for col_name in new_col_list:
        bh_bh_report_finance_lease_day_summary[col_name] = bh_bh_report_finance_lease_day_summary[name].apply(
            lambda x: whether_value_exists(x, col_name, name))

    # 解析后去除
    bh_bh_report_finance_lease_day_summary = bh_bh_report_finance_lease_day_summary.drop([name], axis=1)

    # 特征重命名
    feature_name = 'jd_report'
    bh_bh_report_encode_rename = feature_rename_output(bh_bh_report_finance_lease_day_summary, feature_name, ['loan_id'])

    all_feature_df_list = list(bh_bh_report_encode_rename.columns)

    new_all_feature_df = pd.DataFrame({'loan_id': [loan_id]})
    for col in jd_feature_list:
        if col in all_feature_df_list:
            new_all_feature_df[col] = bh_bh_report_encode_rename[col]
        else:
            new_all_feature_df[col] = np.nan

    return new_all_feature_df








if __name__ == '__main__':
    pass