#!/usr/bin/python
# -*- coding: UTF-8 -*-
# 导入库
from model_utils.hd_feature_utils import *





# 获取hd特征
def get_deal_hd_data(all_deal_data_dict):
    # 解析数据
    hdInfo = all_deal_data_dict['hd_info']
    # 将dict数据转换成df数据
    df = hdInfo
    target_col = 'huadao_info'
    id_col = 'loan_id'
    col_list = ['data']
    hdInfo_df = analyze_dict_to_df(df, id_col, target_col, col_list)
    return hdInfo_df




# 获取hd特征
def get_all_hd_feature(all_deal_data_dict, loan_id, run_mode, hd_feature_list):
    # 数据处理
    hdInfo_df = get_deal_hd_data(all_deal_data_dict)


    # 获取特征
    # 读取订单数据
    bh_bh_report_finance_lease_day_summary = hdInfo_df

    # 去除无关列
    del_col_list = ['d001', 'd050', 'd051', 'd104', 'd105', 'd154', 'd155', 'd160', 'd417', 'd418', 'd419', 'd420',
                    'd421', 'd422', 'd423', 'd424', 'd425', 'order_time']
    detail_list = []
    del_col_list = del_col_list + detail_list
    bh_bh_report_finance_lease_day_summary = remove_irrelevant_columns(bh_bh_report_finance_lease_day_summary,
                                                                       del_col_list)

    # 将none替换成空值
    df = bh_bh_report_finance_lease_day_summary
    exclude_list = ['loan_id', 'order_time']

    # 开始处理
    mongoInfo_df_deal_appVersion_feature = none_replace_to_nan(df, exclude_list)

    # 特征重命名
    feature_name = 'hd_report_dATA'
    bh_bh_report_encode_rename = feature_rename_output(mongoInfo_df_deal_appVersion_feature, feature_name, ['loan_id'])
    hdInfo_df = bh_bh_report_encode_rename

    all_feature_df_list = list(hdInfo_df.columns)

    new_all_feature_df = pd.DataFrame({'loan_id': [loan_id]})
    for col in hd_feature_list:
        if col in all_feature_df_list:
            new_all_feature_df[col] = hdInfo_df[col]
        else:
            new_all_feature_df[col] = np.nan
    return new_all_feature_df

















if __name__ == '__main__':
    pass