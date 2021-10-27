#!/usr/bin/python
# -*- coding: UTF-8 -*-
# 导入库
from model_utils.xy_feature_utils import *
from model_utils.public_feature_utils import *



# 处理xy数据
def get_deal_xy_data(all_deal_data_dict):
    # 读取埋点数据
    xyInfo = all_deal_data_dict['xy_df']

    # 过滤空值
    xyInfo = xyInfo[xyInfo['xinyan_info'].notnull()]

    # 提取指定数据
    xyInfo['desc'] = xyInfo['xinyan_info'].apply(lambda x: get_specified_desc_data(x))

    # 筛选
    xyInfo = xyInfo[xyInfo['desc'] == '查询成功'].reset_index(drop=True)

    # 提取指定数据
    xyInfo['xinyan_info'] = xyInfo['xinyan_info'].apply(lambda x: get_specified_result_detail_data(x))
    return xyInfo

# 1 apply_report_detail
def get_apply_report_detail_feature(xyInfo):
    # 将dict数据转换成df数据
    df = xyInfo
    target_col = 'xinyan_info'
    id_col = 'loan_id'
    col_list = ['apply_report_detail']

    xyInfo_df = analyze_dict_to_df(df, id_col, target_col, col_list)

    # 去除无关列
    del_col_list = ['a22160007']
    detail_list = []
    del_col_list = del_col_list + detail_list
    xyInfo_df = remove_irrelevant_columns(xyInfo_df, del_col_list)

    # 特征重命名
    feature_name = 'xy_report_apply_report_detail'
    bh_bh_report_encode_rename = feature_rename_output(xyInfo_df, feature_name, ['loan_id'])
    bh_bh_report_encode_rename = bh_bh_report_encode_rename.drop(['loan_id'], axis=1)
    return bh_bh_report_encode_rename


# 2 behavior_report_detail
def get_behavior_report_detail_feature(xyInfo):
    # 将dict数据转换成df数据
    df = xyInfo
    target_col = 'xinyan_info'
    id_col = 'loan_id'
    col_list = ['behavior_report_detail']

    xyInfo_df = analyze_dict_to_df(df, id_col, target_col, col_list)

    # 去除无关列
    del_col_list = ['b22170054']
    detail_list = []
    del_col_list = del_col_list + detail_list
    bh_bh_report_finance_lease_day_summary = remove_irrelevant_columns(xyInfo_df,
                                                                       del_col_list)

    # 将百分号转换成数值
    bh_bh_report_finance_lease_day_summary['b22170034'] = bh_bh_report_finance_lease_day_summary['b22170034'].apply(
        lambda x: int(x.replace('%', '')))

    # 通过字典进行编码

    df = bh_bh_report_finance_lease_day_summary

    encode_col_list = ['b22170007', 'b22170008', 'b22170009', 'b22170010', 'b22170011', 'b22170031', 'b22170032',
                       'b22170033', 'b22170040', 'b22170041', 'b22170042', 'b22170043', 'b22170044', 'b22170050']
    pkl_file_path = './data/pkl/'
    file_prefix_name = col_list[0]
    for encode_col in encode_col_list:
        mongoInfo_df_deal_appVersion_encode = text_to_dict_encode(df, encode_col, pkl_file_path, file_prefix_name)

    # 进行独热转换
    discrete_variable_list = encode_col_list
    df = mongoInfo_df_deal_appVersion_encode
    limit_num = 100
    # 缺失值替换
    replace_name = 'A100000'
    # 开始处理
    mongoInfo_df_deal_appVersion = one_hot_deal_to_transform(df, discrete_variable_list, limit_num, replace_name, pkl_file_path, file_prefix_name)

    # 特征重命名
    feature_name = 'xy_report_behavior_report_detail'
    bh_bh_report_encode_rename = feature_rename_output(mongoInfo_df_deal_appVersion, feature_name, ['loan_id'])
    xyInfo_df = bh_bh_report_encode_rename
    return xyInfo_df


# 3 current_report_detail
def get_current_report_detail_feature(xyInfo):
    # 将dict数据转换成df数据
    df = xyInfo
    target_col = 'xinyan_info'
    id_col = 'loan_id'
    col_list = ['current_report_detail']

    xyInfo_df = analyze_dict_to_df(df, id_col, target_col, col_list)

    # 特征重命名
    feature_name ='xy_report_current_report_detail'
    bh_bh_report_encode_rename = feature_rename_output(xyInfo_df, feature_name, ['loan_id'])
    return bh_bh_report_encode_rename


def get_all_xy_feature(all_deal_data_dict, loan_id, run_mode, xy_feature_list):
    # 数据处理
    xyInfo = get_deal_xy_data(all_deal_data_dict)

    # 1 apply_report_detail
    apply_report_detail_feature = get_apply_report_detail_feature(xyInfo)

    # 2 behavior_report_detail
    behavior_report_detail_feature = get_behavior_report_detail_feature(xyInfo)

    # 3 current_report_detail
    current_report_detail_feature = get_current_report_detail_feature(xyInfo)

    # 数据合并
    all_xyInfo_feature = pd.concat([apply_report_detail_feature, behavior_report_detail_feature, current_report_detail_feature], axis=1)
    all_xyInfo_feature['loan_id'] = str(loan_id)

    all_feature_df_list = list(all_xyInfo_feature.columns)

    new_all_feature_df = pd.DataFrame({'loan_id': [loan_id]})
    for col in xy_feature_list:
        if col in all_feature_df_list:
            new_all_feature_df[col] = all_xyInfo_feature[col]
        else:
            new_all_feature_df[col] = np.nan

    return new_all_feature_df









if __name__ == '__main__':
    pass