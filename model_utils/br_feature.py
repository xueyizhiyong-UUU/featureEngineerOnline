#!/usr/bin/python
# -*- coding: UTF-8 -*-
# 导入库
from model_utils.br_feature_utils import *
from model_utils.public_feature_utils import *







# 处理br数据
def get_deal_xy_data(all_deal_data_dict):
    brInfo = all_deal_data_dict['br_df']
    # 过滤空值
    brInfo = brInfo[brInfo['bairong_info'].notnull()]

    # 提取指定数据
    brInfo['bairong_info'] = brInfo['bairong_info'].apply(lambda x: get_specified_applyLoanStr_data(x))

    first_col_list, second_col_list, third_col_list, fourth_col_list = br_data_analysis(brInfo)

    # 将dict数据转换成df数据
    df = brInfo
    target_col = 'bairong_info'
    id_col = 'loan_id'

    # 开始处理
    brInfo_df = analyze_dict_to_df_br(df, id_col, target_col, first_col_list, second_col_list, third_col_list,
                                      fourth_col_list)
    return brInfo_df



# 获取特征
def get_all_br_feature(all_deal_data_dict, loan_id, run_mode, br_feature_list):
    brInfo_df = get_deal_xy_data(all_deal_data_dict)

    # 特征重命名
    feature_name = 'br_report'
    bh_bh_report_encode_rename = feature_rename_output(brInfo_df, feature_name, ['loan_id'])

    all_feature_df_list = list(bh_bh_report_encode_rename.columns)

    new_all_feature_df = pd.DataFrame({'loan_id': [loan_id]})
    for col in br_feature_list:
        if col in all_feature_df_list:
            new_all_feature_df[col] = bh_bh_report_encode_rename[col]
        else:
            new_all_feature_df[col] = np.nan
    return new_all_feature_df









if __name__ == '__main__':
    pass