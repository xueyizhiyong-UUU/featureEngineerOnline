#!/usr/bin/python
# -*- coding: UTF-8 -*-
"""
@file:rh_feature.py
@Function:
"""
from model_utils.public_feature_utils import *
from model_utils.rh_feature_utils import *
from model_utils.rh_feature_config import *
from model_utils.extract_statistics_feature_utils import *
from model_utils.deployment_util import *
import time

def get_all_part_report_df(rhInfo):
    rhInfo_df_list =['cc_rh_report','cc_rh_report_customer',
                     'cc_rh_report_customer_home',
                     'cc_rh_report_customer_profession',
                     'cc_rh_report_detail_debit_card_second',
                     'cc_rh_report_loan_special_detail_second',
                     'cc_rh_report_public_housefund',
                     'cc_rh_report_query_detail',
                     'cc_rh_report_query_summary',
                     'cc_rh_report_status',
                     'cc_rh_report_summary_credit_tips',
                     'cc_rh_report_summary_debt_loan',
                     'cc_rh_report_summary_overdue']


    df = rhInfo
    df_time = rhInfo[['loan_id', 'order_time']]
    target_col = 'pboc_info'
    id_col = 'loan_id'
    col_list = rhInfo_df_list
    save_path = ''
    all_rh_part_df_dict = analyze_dict_to_df_rh(df, df_time, id_col, target_col, col_list, save_path)
    return all_rh_part_df_dict



# 1-cc_rh_report-ok
def get_cc_rh_report_feature(all_bh_part_df_dict, file_name):
    bh_bh_report = all_bh_part_df_dict[file_name]

    bh_bh_report = bh_bh_report.rename(columns={'idcardNo': 'idcard_no'})
    cc_rh_report_loan_no = bh_bh_report.copy()


    cc_rh_report_loan_no = get_idcard_no_to_age(cc_rh_report_loan_no, 'idcard_no', 'order_time')


    cc_rh_report_loan_no['idcard_no_sex'] = cc_rh_report_loan_no['idcard_no'].apply(get_idcard_no_to_sex)


    cc_rh_report_loan_no['idcard_no_zodiac_cn'] = cc_rh_report_loan_no['idcard_no'].apply(get_zodiac_cn)

    cc_rh_report_loan_no['idcard_no_zodiac_en'] = cc_rh_report_loan_no['idcard_no'].apply(get_zodiac_en)


    cc_rh_report_loan_no['idcard_no_province_address'] = cc_rh_report_loan_no['idcard_no'].apply(
        lambda x: str(x)[:2] + '0000')


    cc_rh_report_loan_no['idcard_no_city_address'] = cc_rh_report_loan_no['idcard_no'].apply(
        lambda x: str(x)[:4] + '00')


    cc_rh_report_loan_no['idcard_no_county_address'] = cc_rh_report_loan_no['idcard_no'].apply(lambda x: str(x)[:6])


    bh_bh_report_deal = cc_rh_report_loan_no[['loan_id', 'idcard_no_age', 'idcard_no_sex',
                                              'idcard_no_zodiac_cn', 'idcard_no_zodiac_en']]

    encode_col_list = ['idcard_no_sex', 'idcard_no_zodiac_cn', 'idcard_no_zodiac_en']
    for encode_col in encode_col_list:
        file_prefix_name = 'cc_rh_report'
        bh_bh_report_deal = text_to_dict_encode(bh_bh_report_deal, encode_col, pkl_file_path, file_prefix_name, write_or_read='read')


    discrete_variable_list = ['idcard_no_sex', 'idcard_no_zodiac_cn', 'idcard_no_zodiac_en']
    df = bh_bh_report_deal.copy()
    limit_num = 100

    replace_name = 'A100000'


    file_prefix_name = 'cc_rh_report'
    bh_bh_report_encode = one_hot_deal_to_transform(df, discrete_variable_list, limit_num,
                                                                              replace_name, pkl_file_path,
                                                                              file_prefix_name)


    feature_name = file_name
    bh_bh_report_encode_rename = feature_rename_output(bh_bh_report_encode, feature_name, ['loan_id'])
    return bh_bh_report_encode_rename


# 2-cc_rh_report_customer-ok
def get_cc_rh_report_customer_feature(all_bh_part_df_dict, file_name, part_feature_list):
    bh_bh_report_finance_lease_day_summary = all_bh_part_df_dict[file_name]

    del_col_list = ['id', 'email', 'birthday', 'mobile', 'workTelNo', 'homeTelNo', 'order_time', 'reportId']
    detail_list = ['messageAddrLat', 'messageAddrLng', 'messageAddress', 'residenceAddress']
    del_col_list = del_col_list + detail_list
    bh_bh_report_finance_lease_day_summary = remove_irrelevant_columns(bh_bh_report_finance_lease_day_summary,
                                                                       del_col_list)

    encode_col_list = ['degree', 'education', 'employmentStatus', 'marry', 'nationality', 'sex']
    for encode_col in encode_col_list:
        bh_bh_report_finance_lease_day_summary[encode_col] = bh_bh_report_finance_lease_day_summary[encode_col].apply(
            int_to_str)

        file_prefix_name = 'cc_rh_report_customer'
        bh_bh_report_finance_lease_day_summary = text_to_dict_encode(bh_bh_report_finance_lease_day_summary, encode_col, pkl_file_path, file_prefix_name, write_or_read='read')

    discrete_variable_list = ['degree', 'education', 'employmentStatus', 'marry', 'nationality', 'sex']
    df = bh_bh_report_finance_lease_day_summary.copy()
    limit_num = 100

    replace_name = 'A100000'

    file_prefix_name = 'cc_rh_report_customer'
    bh_bh_report_finance_lease_day_summary_encode = one_hot_deal_to_transform(df, discrete_variable_list, limit_num,
                                                                              replace_name, pkl_file_path,
                                                                              file_prefix_name)

    feature_name = file_name
    bh_bh_report_finance_lease_day_summary_encode_rename = feature_rename_output(bh_bh_report_finance_lease_day_summary_encode, feature_name, ['loan_id'])

    del bh_bh_report_finance_lease_day_summary_encode_rename['loan_id']
    return bh_bh_report_finance_lease_day_summary_encode_rename

# 3-cc_rh_report_customer_home-ok
def get_cc_rh_report_customer_home_feature(all_bh_part_df_dict, file_name, part_feature_list):
    bh_bh_report_finance_lease_day_summary = all_bh_part_df_dict[file_name]

    del_col_list = ['id', 'address', 'phone', 'reportId', 'updateTime', 'order_time']
    detail_list = []
    del_col_list = del_col_list + detail_list
    bh_bh_report_finance_lease_day_summary = remove_irrelevant_columns(bh_bh_report_finance_lease_day_summary,
                                                                       del_col_list)

    encode_col_list = ['addressState']
    for encode_col in encode_col_list:
        bh_bh_report_finance_lease_day_summary[encode_col] = bh_bh_report_finance_lease_day_summary[encode_col].apply(
            int_to_str)

        file_prefix_name = 'cc_rh_report_customer_home'
        bh_bh_report_finance_lease_day_summary = text_to_dict_encode(bh_bh_report_finance_lease_day_summary, encode_col, pkl_file_path, file_prefix_name, write_or_read='read')

    discrete_variable_list = ['addressState']
    df = bh_bh_report_finance_lease_day_summary.copy()
    limit_num = 100

    replace_name = 'A100000'


    file_prefix_name = 'cc_rh_report_customer_home'
    bh_bh_report_finance_lease_day_summary_encode = one_hot_deal_to_transform(df, discrete_variable_list, limit_num,
                                                                              replace_name, pkl_file_path,
                                                                              file_prefix_name)
    # 过滤掉不在入模型的特征
    bh_bh_report_finance_lease_day_summary_encode = onehot_filter_out_features_name(
        bh_bh_report_finance_lease_day_summary_encode, part_feature_list)


    bh_bh_report_finance_lease_day_summary_encode['gap_time'] = 100000


    id_col = 'loan_id'
    seg_col = 'gap_time'
    minutes_list = [2, 10, 30]
    hours_list = [2, 12]
    days_list = [1, 2, 7, 30, 60, 90, 3650]
    df = bh_bh_report_finance_lease_day_summary_encode
    feature_list = [i for i in list(df.columns) if i not in [id_col, seg_col]]
    output_name = 'frequency_minute'

    
    # split_time_list, all_aggregation_list = before_filter_out_auto_var_report_feature_deploy(days_list, hours_list,
    #                                                                                          minutes_list,
    #                                                                                          part_feature_list)
    #
    # bh_bh_report_finance_lease_day_summary_feature = auto_var_report_feature_deploy(df, feature_list, split_time_list, seg_col,
    #                                                                       id_col, output_name, all_aggregation_list)


    bh_bh_report_finance_lease_day_summary_feature = auto_var_report_feature(df, feature_list, days_list, hours_list,
                                                                             minutes_list,
                                                                             seg_col, id_col, output_name)

    feature_name = file_name
    bh_bh_report_finance_lease_day_summary_encode_rename = feature_rename_output(
        bh_bh_report_finance_lease_day_summary_feature, feature_name, ['loan_id'])

    del bh_bh_report_finance_lease_day_summary_encode_rename['loan_id']
    return bh_bh_report_finance_lease_day_summary_encode_rename

# 5-cc_rh_report_customer_profession-ok
def get_cc_rh_report_customer_profession_feature(all_bh_part_df_dict, file_name, part_feature_list):
    bh_bh_report_finance_lease_day_summary = all_bh_part_df_dict[file_name]

    del_col_list = ['id', 'workUnitPhone', 'workUnit', 'entryYear', 'workUnitAddr', 'order_time', 'updateTime',
                    'reportId']
    detail_list = []
    del_col_list = del_col_list + detail_list
    bh_bh_report_finance_lease_day_summary = remove_irrelevant_columns(bh_bh_report_finance_lease_day_summary,
                                                                       del_col_list)

    encode_col_list = ['workUnitType', 'profession', 'industry', 'duty', 'technicalLevel']
    for encode_col in encode_col_list:
        bh_bh_report_finance_lease_day_summary[encode_col] = bh_bh_report_finance_lease_day_summary[encode_col].apply(
            int_to_str)

        file_prefix_name = 'cc_rh_report_customer_profession'
        bh_bh_report_finance_lease_day_summary = text_to_dict_encode(bh_bh_report_finance_lease_day_summary, encode_col, pkl_file_path, file_prefix_name, write_or_read='read')

    discrete_variable_list = ['workUnitType', 'profession', 'industry', 'duty', 'technicalLevel']
    df = bh_bh_report_finance_lease_day_summary.copy()
    limit_num = 100

    replace_name = 'A100000'


    file_prefix_name = 'cc_rh_report_customer_profession'
    bh_bh_report_finance_lease_day_summary_encode = one_hot_deal_to_transform(df, discrete_variable_list, limit_num,
                                                                              replace_name, pkl_file_path,
                                                                              file_prefix_name)

    # 过滤掉不在入模型的特征
    bh_bh_report_finance_lease_day_summary_encode = onehot_filter_out_features_name(
        bh_bh_report_finance_lease_day_summary_encode, part_feature_list)

    bh_bh_report_finance_lease_day_summary_encode['gap_time'] = 100000


    id_col = 'loan_id'
    seg_col = 'gap_time'
    minutes_list = [2, 10, 30]
    hours_list = [2, 12]
    days_list = [1, 2, 7, 30, 60, 90, 3650]
    df = bh_bh_report_finance_lease_day_summary_encode
    feature_list = [i for i in list(df.columns) if i not in [id_col, seg_col]]
    output_name = 'frequency_minute'


    
    # split_time_list, all_aggregation_list = before_filter_out_auto_var_report_feature_deploy(days_list, hours_list,
    #                                                                                          minutes_list,
    #                                                                                          part_feature_list)
    #
    # bh_bh_report_finance_lease_day_summary_feature = auto_var_report_feature_deploy(df, feature_list, split_time_list, seg_col,
    #                                                                       id_col, output_name, all_aggregation_list)


    bh_bh_report_finance_lease_day_summary_feature = auto_var_report_feature(df, feature_list, days_list, hours_list,
                                                                             minutes_list,
                                                                             seg_col, id_col, output_name)

    feature_name = file_name
    bh_bh_report_finance_lease_day_summary_encode_rename = feature_rename_output(bh_bh_report_finance_lease_day_summary_feature, feature_name, ['loan_id'])

    del bh_bh_report_finance_lease_day_summary_encode_rename['loan_id']
    return bh_bh_report_finance_lease_day_summary_encode_rename

# 14-cc_rh_report_detail_debit_card_second-ok
def get_cc_rh_report_detail_debit_card_second_feature(all_bh_part_df_dict, file_name, part_feature_list):
    bh_bh_report_finance_lease_day_summary = all_bh_part_df_dict[file_name]

    del_col_list = ['id', 'month60State', 'month60Amount', 'month60Desc', 'accountLogo',
                    'byDate', 'recentRepayDate', 'closedDate', 'dissentTaggingDate', 'statementDate']
    detail_list = ['order_time', 'grantOrg', 'currency', 'cardGrantDate', 'orgExplain', 'orgExplainDate',
                   'selfDeclare', 'selfDeclareDate', 'reportId', 'specialDesc', 'specialTagging']
    del_col_list = del_col_list + detail_list
    bh_bh_report_finance_lease_day_summary = remove_irrelevant_columns(bh_bh_report_finance_lease_day_summary,
                                                                       del_col_list)
    # bh_bh_report_finance_lease_day_summary = bh_bh_report_finance_lease_day_summary.drop_duplicates()

    encode_col_list = ['businessType', 'guaranteeForm', 'accountStatus']
    for encode_col in encode_col_list:
        bh_bh_report_finance_lease_day_summary[encode_col] = bh_bh_report_finance_lease_day_summary[encode_col].apply(
            int_to_str)

        file_prefix_name = 'cc_rh_report_detail_debit_card_second'
        bh_bh_report_finance_lease_day_summary = text_to_dict_encode(bh_bh_report_finance_lease_day_summary, encode_col, pkl_file_path, file_prefix_name, write_or_read='read')

    discrete_variable_list = ['businessType', 'guaranteeForm', 'accountStatus']
    df = bh_bh_report_finance_lease_day_summary.copy()
    limit_num = 100

    replace_name = 'A100000'

    file_prefix_name = 'cc_rh_report_detail_debit_card_second'
    bh_bh_report_finance_lease_day_summary_encode = one_hot_deal_to_transform(df, discrete_variable_list, limit_num,
                                                                              replace_name, pkl_file_path,
                                                                              file_prefix_name)

    # 过滤掉不在入模型的特征
    bh_bh_report_finance_lease_day_summary_encode = onehot_filter_out_features_name(
        bh_bh_report_finance_lease_day_summary_encode, part_feature_list)

    bh_bh_report_finance_lease_day_summary_encode['gap_time'] = 100000
    all_col_list = list(bh_bh_report_finance_lease_day_summary_encode.columns)
    all_col_list.remove('loan_id')
    all_col_list.remove('gap_time')
    for col in all_col_list:
        try:
            bh_bh_report_finance_lease_day_summary_encode[col] = bh_bh_report_finance_lease_day_summary_encode[col].astype('float')
        except Exception as e:
            del bh_bh_report_finance_lease_day_summary_encode[col]

    id_col = 'loan_id'
    seg_col = 'gap_time'
    minutes_list = [2, 10, 30]
    hours_list = [2, 12]
    days_list = [1, 2, 7, 30, 60, 90, 3650]
    df = bh_bh_report_finance_lease_day_summary_encode
    feature_list = [i for i in list(df.columns) if i not in [id_col, seg_col]]
    output_name = 'frequency_minute'

    
    # split_time_list, all_aggregation_list = before_filter_out_auto_var_report_feature_deploy(days_list, hours_list,
    #                                                                                          minutes_list,
    #                                                                                          part_feature_list)
    #
    # bh_bh_report_finance_lease_day_summary_feature = auto_var_report_feature_deploy(df, feature_list, split_time_list,
    #                                                                                 seg_col,
    #                                                                                 id_col, output_name,
    #                                                                                 all_aggregation_list)


    bh_bh_report_finance_lease_day_summary_feature = auto_var_report_feature(df, feature_list, days_list, hours_list,
                                                                             minutes_list,
                                                                             seg_col, id_col, output_name)

    feature_name = file_name
    bh_bh_report_finance_lease_day_summary_encode_rename = feature_rename_output(
        bh_bh_report_finance_lease_day_summary_feature, feature_name, ['loan_id'])

    del bh_bh_report_finance_lease_day_summary_encode_rename['loan_id']
    return bh_bh_report_finance_lease_day_summary_encode_rename


# 23-cc_rh_report_loan_special_detail_second-ok
def get_cc_rh_report_loan_special_detail_second_feature(all_bh_part_df_dict, file_name, part_feature_list):
    bh_bh_report_finance_lease_day_summary = all_bh_part_df_dict[file_name]

    del_col_list = ['id', 'specialTradeDate', 'reportId', 'specialTradeDetail', 'loanId', 'order_time']
    detail_list = []
    del_col_list = del_col_list + detail_list
    bh_bh_report_finance_lease_day_summary = remove_irrelevant_columns(bh_bh_report_finance_lease_day_summary,
                                                                       del_col_list)


    encode_col_list = ['specialTradeType']
    for encode_col in encode_col_list:
        bh_bh_report_finance_lease_day_summary[encode_col] = bh_bh_report_finance_lease_day_summary[encode_col].apply(
            int_to_str)

        file_prefix_name = 'cc_rh_report_loan_special_detail_second'
        bh_bh_report_finance_lease_day_summary = text_to_dict_encode(bh_bh_report_finance_lease_day_summary, encode_col, pkl_file_path, file_prefix_name, write_or_read='read')

    discrete_variable_list = ['specialTradeType']
    df = bh_bh_report_finance_lease_day_summary.copy()
    limit_num = 100

    replace_name = 'A100000'

    file_prefix_name = 'cc_rh_report_loan_special_detail_second'
    bh_bh_report_finance_lease_day_summary_encode = one_hot_deal_to_transform(df, discrete_variable_list, limit_num,
                                                                              replace_name, pkl_file_path,
                                                                              file_prefix_name)

    # 过滤掉不在入模型的特征
    bh_bh_report_finance_lease_day_summary_encode = onehot_filter_out_features_name(
        bh_bh_report_finance_lease_day_summary_encode, part_feature_list)


    bh_bh_report_finance_lease_day_summary_encode['gap_time'] = 100000


    id_col = 'loan_id'
    seg_col = 'gap_time'
    minutes_list = [2, 10, 30]
    hours_list = [2, 12]
    days_list = [1, 2, 7, 30, 60, 90, 3650]
    df = bh_bh_report_finance_lease_day_summary_encode
    feature_list = [i for i in list(df.columns) if i not in [id_col, seg_col]]
    output_name = 'frequency_minute'

    
    # split_time_list, all_aggregation_list = before_filter_out_auto_var_report_feature_deploy(days_list, hours_list,
    #                                                                                          minutes_list,
    #                                                                                          part_feature_list)
    #
    # bh_bh_report_finance_lease_day_summary_feature = auto_var_report_feature_deploy(df, feature_list, split_time_list,
    #                                                                                 seg_col,
    #                                                                                 id_col, output_name,
    #                                                                                 all_aggregation_list)


    bh_bh_report_finance_lease_day_summary_feature = auto_var_report_feature(df, feature_list, days_list, hours_list,
                                                                             minutes_list,
                                                                             seg_col, id_col, output_name)

    feature_name = file_name
    bh_bh_report_finance_lease_day_summary_encode_rename = feature_rename_output(
        bh_bh_report_finance_lease_day_summary_feature, feature_name, ['loan_id'])

    del bh_bh_report_finance_lease_day_summary_encode_rename['loan_id']
    return bh_bh_report_finance_lease_day_summary_encode_rename


# 26-cc_rh_report_public_housefund-ok
def get_cc_rh_report_public_housefund_feature(all_bh_part_df_dict, file_name, part_feature_list):
    bh_bh_report_finance_lease_day_summary = all_bh_part_df_dict[file_name]

    del_col_list = ['id','payDate', 'payFirstDate', 'payEndDate', 'payWorkUnit', 'payPersonPercent',
                    'payWorkUnitPercent',
                    'payArea', 'order_time', 'updateDate', 'reportId']
    detail_list = []
    del_col_list = del_col_list + detail_list
    bh_bh_report_finance_lease_day_summary = remove_irrelevant_columns(bh_bh_report_finance_lease_day_summary,
                                                                       del_col_list)

    encode_col_list = ['payStatus']
    for encode_col in encode_col_list:
        bh_bh_report_finance_lease_day_summary[encode_col] = bh_bh_report_finance_lease_day_summary[encode_col].apply(
            int_to_str)

        file_prefix_name = 'cc_rh_report_public_housefund'
        bh_bh_report_finance_lease_day_summary = text_to_dict_encode(bh_bh_report_finance_lease_day_summary, encode_col, pkl_file_path, file_prefix_name, write_or_read='read')

    discrete_variable_list = ['payStatus']
    df = bh_bh_report_finance_lease_day_summary.copy()
    limit_num = 100

    replace_name = 'A100000'

    file_prefix_name = 'cc_rh_report_public_housefund'
    bh_bh_report_finance_lease_day_summary_encode = one_hot_deal_to_transform(df, discrete_variable_list, limit_num,
                                                                              replace_name, pkl_file_path,
                                                                              file_prefix_name)

    # 过滤掉不在入模型的特征
    bh_bh_report_finance_lease_day_summary_encode = onehot_filter_out_features_name(
        bh_bh_report_finance_lease_day_summary_encode, part_feature_list)


    bh_bh_report_finance_lease_day_summary_encode['gap_time'] = 100000


    id_col = 'loan_id'
    seg_col = 'gap_time'
    minutes_list = [2, 10, 30]
    hours_list = [2, 12]
    days_list = [1, 2, 7, 30, 60, 90, 3650]
    df = bh_bh_report_finance_lease_day_summary_encode
    feature_list = [i for i in list(df.columns) if i not in [id_col, seg_col]]
    output_name = 'frequency_minute'

    
    # split_time_list, all_aggregation_list = before_filter_out_auto_var_report_feature_deploy(days_list, hours_list,
    #                                                                                          minutes_list,
    #                                                                                          part_feature_list)
    #
    # bh_bh_report_finance_lease_day_summary_feature = auto_var_report_feature_deploy(df, feature_list, split_time_list,
    #                                                                                 seg_col,
    #                                                                                 id_col, output_name,
    #                                                                                 all_aggregation_list)


    bh_bh_report_finance_lease_day_summary_feature = auto_var_report_feature(df, feature_list, days_list, hours_list,
                                                                             minutes_list,
                                                                             seg_col, id_col, output_name)

    feature_name = file_name
    bh_bh_report_finance_lease_day_summary_encode_rename = feature_rename_output(
        bh_bh_report_finance_lease_day_summary_feature, feature_name, ['loan_id'])
    # 去除id
    del bh_bh_report_finance_lease_day_summary_encode_rename['loan_id']
    return bh_bh_report_finance_lease_day_summary_encode_rename


# 27-cc_rh_report_query_detail-ok
def get_cc_rh_report_query_detail_feature(all_bh_part_df_dict, file_name, part_feature_list):
    bh_bh_report_finance_lease_day_summary = all_bh_part_df_dict[file_name]

    del_col_list = ['reportId', 'reportNo', 'queryDate', 'queryOperator', 'id', 'order_time']
    detail_list = []
    del_col_list = del_col_list + detail_list
    bh_bh_report_finance_lease_day_summary = remove_irrelevant_columns(bh_bh_report_finance_lease_day_summary,
                                                                       del_col_list)

    encode_col_list = ['queryReason']
    for encode_col in encode_col_list:
        bh_bh_report_finance_lease_day_summary[encode_col] = bh_bh_report_finance_lease_day_summary[encode_col].apply(
            int_to_str)

        file_prefix_name = 'cc_rh_report_query_detail'
        bh_bh_report_finance_lease_day_summary = text_to_dict_encode(bh_bh_report_finance_lease_day_summary, encode_col, pkl_file_path, file_prefix_name, write_or_read='read')



    discrete_variable_list = ['queryReason']
    df = bh_bh_report_finance_lease_day_summary.copy()
    limit_num = 100

    replace_name = 'A100000'

    file_prefix_name = 'cc_rh_report_query_detail'
    bh_bh_report_finance_lease_day_summary_encode = one_hot_deal_to_transform(df, discrete_variable_list, limit_num,
                                                                              replace_name, pkl_file_path,
                                                                              file_prefix_name)

    # 过滤掉不在入模型的特征
    bh_bh_report_finance_lease_day_summary_encode = onehot_filter_out_features_name(
        bh_bh_report_finance_lease_day_summary_encode, part_feature_list)


    bh_bh_report_finance_lease_day_summary_encode['gap_time'] = 100000


    id_col = 'loan_id'
    seg_col = 'gap_time'
    minutes_list = [2, 10, 30]
    hours_list = [2, 12]
    days_list = [1, 2, 7, 30, 60, 90, 3650]
    df = bh_bh_report_finance_lease_day_summary_encode
    feature_list = [i for i in list(df.columns) if i not in [id_col, seg_col]]
    output_name = 'frequency_minute'

    
    # split_time_list, all_aggregation_list = before_filter_out_auto_var_report_feature_deploy(days_list, hours_list,
    #                                                                                          minutes_list,
    #                                                                                          part_feature_list)
    #
    # bh_bh_report_finance_lease_day_summary_feature = auto_var_report_feature_deploy(df, feature_list, split_time_list,
    #                                                                                 seg_col,
    #                                                                                 id_col, output_name,
    #                                                                                 all_aggregation_list)


    bh_bh_report_finance_lease_day_summary_feature = auto_var_report_feature(df, feature_list, days_list, hours_list,
                                                                             minutes_list,
                                                                             seg_col, id_col, output_name)

    feature_name = file_name
    bh_bh_report_finance_lease_day_summary_encode_rename = feature_rename_output(bh_bh_report_finance_lease_day_summary_feature, feature_name, ['loan_id'])

    del bh_bh_report_finance_lease_day_summary_encode_rename['loan_id']
    return bh_bh_report_finance_lease_day_summary_encode_rename

# 28-cc_rh_report_query_summary-ok
def get_cc_rh_report_query_summary_feature(all_bh_part_df_dict, file_name, part_feature_list):
    bh_bh_report_finance_lease_day_summary = all_bh_part_df_dict[file_name]

    del_col_list = ['reportId', 'id', 'order_time']
    detail_list = []
    del_col_list = del_col_list + detail_list
    bh_bh_report_finance_lease_day_summary = remove_irrelevant_columns(bh_bh_report_finance_lease_day_summary,
                                                                       del_col_list)
    mongoInfo_df_deal_appVersion_feature_select = bh_bh_report_finance_lease_day_summary


    feature_name = file_name
    bh_bh_report_finance_lease_day_summary_encode_rename = feature_rename_output(
        mongoInfo_df_deal_appVersion_feature_select, feature_name, ['loan_id'])

    del bh_bh_report_finance_lease_day_summary_encode_rename['loan_id']
    return bh_bh_report_finance_lease_day_summary_encode_rename

# 30-cc_rh_report_status-ok
def get_cc_rh_report_status_feature(all_bh_part_df_dict, file_name, part_feature_list):
    bh_bh_report_finance_lease_day_summary = all_bh_part_df_dict[file_name]

    del_col_list = ['reportId', 'id', 'order_time']
    detail_list = []
    del_col_list = del_col_list + detail_list
    bh_bh_report_finance_lease_day_summary = remove_irrelevant_columns(bh_bh_report_finance_lease_day_summary,
                                                                       del_col_list)
    mongoInfo_df_deal_appVersion_feature_select = bh_bh_report_finance_lease_day_summary

    feature_name = file_name
    bh_bh_report_finance_lease_day_summary_encode_rename = feature_rename_output(
        mongoInfo_df_deal_appVersion_feature_select, feature_name, ['loan_id'])

    del bh_bh_report_finance_lease_day_summary_encode_rename['loan_id']
    return bh_bh_report_finance_lease_day_summary_encode_rename

# 32-cc_rh_report_summary_credit_tips-ok
def get_cc_rh_report_summary_credit_tips_feature(all_bh_part_df_dict, file_name, part_feature_list):
    bh_bh_report_finance_lease_day_summary = all_bh_part_df_dict[file_name]

    del_col_list = ['reportId', 'id', 'order_time', 'houseLoanFirstMonth', 'commercialHouseLoanFirstMonth',
                    'otherLoanFirstMonth',
                    'firstLoanMonth', 'firstCreditCardMonth', 'firstReadyCardMonth', 'otherFirstMonth']
    detail_list = []
    del_col_list = del_col_list + detail_list
    bh_bh_report_finance_lease_day_summary = remove_irrelevant_columns(bh_bh_report_finance_lease_day_summary,
                                                                       del_col_list)
    mongoInfo_df_deal_appVersion_feature_select = bh_bh_report_finance_lease_day_summary


    feature_name = file_name
    bh_bh_report_finance_lease_day_summary_encode_rename = feature_rename_output(
        mongoInfo_df_deal_appVersion_feature_select, feature_name, ['loan_id'])

    del bh_bh_report_finance_lease_day_summary_encode_rename['loan_id']
    return bh_bh_report_finance_lease_day_summary_encode_rename


# 35-cc_rh_report_summary_debt_loan-ok
def get_cc_rh_report_summary_debt_loan_feature(all_bh_part_df_dict, file_name, part_feature_list):
    bh_bh_report_finance_lease_day_summary = all_bh_part_df_dict[file_name]

    del_col_list = ['id', 'reportId', 'order_time']
    detail_list = []
    del_col_list = del_col_list + detail_list
    bh_bh_report_finance_lease_day_summary = remove_irrelevant_columns(bh_bh_report_finance_lease_day_summary,
                                                                       del_col_list)
    mongoInfo_df_deal_appVersion_feature_select = bh_bh_report_finance_lease_day_summary

    feature_name = file_name
    bh_bh_report_finance_lease_day_summary_encode_rename = feature_rename_output(
        mongoInfo_df_deal_appVersion_feature_select, feature_name, ['loan_id'])

    del bh_bh_report_finance_lease_day_summary_encode_rename['loan_id']
    return bh_bh_report_finance_lease_day_summary_encode_rename

# 36-cc_rh_report_summary_overdue-ok
def get_cc_rh_report_summary_overdue_feature(all_bh_part_df_dict, file_name, part_feature_list):
    bh_bh_report_finance_lease_day_summary = all_bh_part_df_dict[file_name]

    del_col_list = ['id', 'reportId', 'order_time']
    detail_list = []
    del_col_list = del_col_list + detail_list
    bh_bh_report_finance_lease_day_summary = remove_irrelevant_columns(bh_bh_report_finance_lease_day_summary,
                                                                       del_col_list)
    mongoInfo_df_deal_appVersion_feature_select = bh_bh_report_finance_lease_day_summary


    feature_name = file_name
    bh_bh_report_finance_lease_day_summary_encode_rename = feature_rename_output(
        mongoInfo_df_deal_appVersion_feature_select, feature_name, ['loan_id'])

    del bh_bh_report_finance_lease_day_summary_encode_rename['loan_id']
    return bh_bh_report_finance_lease_day_summary_encode_rename


def get_all_rh_feature(all_deal_data_dict, loan_id, run_mode, rh_feature_dict):
    rhInfo = all_deal_data_dict['rh_df']
    all_rh_feature_df_list = []


    all_bh_part_df_dict = get_all_part_report_df(rhInfo)

    # 1-cc_rh_report-ok
    file_name = 'cc_rh_report'
    if file_name in all_bh_part_df_dict:
        if all_bh_part_df_dict[file_name].shape[0] > 0:
            try:
                cc_rh_report_feature = get_cc_rh_report_feature(all_bh_part_df_dict, file_name)
                all_rh_feature_df_list.append(cc_rh_report_feature)
            except Exception as e:
                print(e)
                pass
    # 2-cc_rh_report_customer-ok
    file_name = 'cc_rh_report_customer'
    if file_name in rh_feature_dict:
        if file_name in all_bh_part_df_dict:
            if all_bh_part_df_dict[file_name].shape[0] > 0:
                try:
                    # 计算运行时间
                    
                    part_feature_list = rh_feature_dict[file_name]
                    cc_rh_report_customer_feature = get_cc_rh_report_customer_feature(all_bh_part_df_dict, file_name, part_feature_list)
                    


                    all_rh_feature_df_list.append(cc_rh_report_customer_feature)
                except Exception as e:
                    print(e)
                    pass

    # 3-cc_rh_report_customer_home-ok
    file_name = 'cc_rh_report_customer_home'
    if file_name in rh_feature_dict:
        if file_name in all_bh_part_df_dict:
            if all_bh_part_df_dict[file_name].shape[0] > 0:
                try:
                    # 计算运行时间
                    
                    part_feature_list = rh_feature_dict[file_name]
                    cc_rh_report_customer_home_feature = get_cc_rh_report_customer_home_feature(all_bh_part_df_dict, file_name, part_feature_list)
                    

                    all_rh_feature_df_list.append(cc_rh_report_customer_home_feature)
                except Exception as e:
                    print(e)
                    pass


    # 5-cc_rh_report_customer_profession-ok
    file_name = 'cc_rh_report_customer_profession'
    if file_name in rh_feature_dict:
        if file_name in all_bh_part_df_dict:
            if all_bh_part_df_dict[file_name].shape[0] > 0:
                try:
                    # 计算运行时间
                    
                    part_feature_list = rh_feature_dict[file_name]
                    cc_rh_report_customer_profession_feature = get_cc_rh_report_customer_profession_feature(all_bh_part_df_dict, file_name, part_feature_list)
                    


                    all_rh_feature_df_list.append(cc_rh_report_customer_profession_feature)
                except Exception as e:
                    print(e)
                    pass

    # 14-cc_rh_report_detail_debit_card_second-ok
    file_name = 'cc_rh_report_detail_debit_card_second'
    if file_name in rh_feature_dict:
        if file_name in all_bh_part_df_dict:
            if all_bh_part_df_dict[file_name].shape[0] > 0:
                try:
                    # 计算运行时间
                    
                    part_feature_list = rh_feature_dict[file_name]
                    cc_rh_report_detail_debit_card_second_feature = get_cc_rh_report_detail_debit_card_second_feature(all_bh_part_df_dict, file_name, part_feature_list)


                    all_rh_feature_df_list.append(cc_rh_report_detail_debit_card_second_feature)
                except Exception as e:
                    print(e)
                    pass


    # 23-cc_rh_report_loan_special_detail_second-ok
    file_name = 'cc_rh_report_loan_special_detail_second'
    if file_name in rh_feature_dict:
        if file_name in all_bh_part_df_dict:
            if all_bh_part_df_dict[file_name].shape[0] > 0:
                try:
                    # 计算运行时间
                    
                    part_feature_list = rh_feature_dict[file_name]
                    cc_rh_report_loan_special_detail_second_feature = get_cc_rh_report_loan_special_detail_second_feature(all_bh_part_df_dict, file_name, part_feature_list)
                    


                    all_rh_feature_df_list.append(cc_rh_report_loan_special_detail_second_feature)
                except Exception as e:
                    print(e)
                    pass
    # 26-cc_rh_report_public_housefund-ok
    file_name = 'cc_rh_report_public_housefund'
    if file_name in rh_feature_dict:
        if file_name in all_bh_part_df_dict:
            if all_bh_part_df_dict[file_name].shape[0] > 0:
                try:
                    # 计算运行时间
                    
                    part_feature_list = rh_feature_dict[file_name]
                    cc_rh_report_public_housefund_feature = get_cc_rh_report_public_housefund_feature(all_bh_part_df_dict, file_name, part_feature_list)


                    all_rh_feature_df_list.append(cc_rh_report_public_housefund_feature)
                except Exception as e:
                    print(e)
                    pass
    # 27-cc_rh_report_query_detail-ok
    file_name = 'cc_rh_report_query_detail'
    if file_name in rh_feature_dict:
        if file_name in all_bh_part_df_dict:
            if all_bh_part_df_dict[file_name].shape[0] > 0:
                try:
                    # 计算运行时间
                    
                    part_feature_list = rh_feature_dict[file_name]
                    cc_rh_report_query_detail_feature = get_cc_rh_report_query_detail_feature(all_bh_part_df_dict, file_name, part_feature_list)


                    all_rh_feature_df_list.append(cc_rh_report_query_detail_feature)
                except Exception as e:
                    print(e)
                    pass
    # 28-cc_rh_report_query_summary-ok
    file_name = 'cc_rh_report_query_summary'
    if file_name in rh_feature_dict:
        if file_name in all_bh_part_df_dict:
            if all_bh_part_df_dict[file_name].shape[0] > 0:
                try:
                    # 计算运行时间
                    
                    part_feature_list = rh_feature_dict[file_name]
                    cc_rh_report_query_summary_feature = get_cc_rh_report_query_summary_feature(all_bh_part_df_dict, file_name, part_feature_list)


                    all_rh_feature_df_list.append(cc_rh_report_query_summary_feature)
                except Exception as e:
                    print(e)
                    pass
    # 30-cc_rh_report_status-ok
    file_name = 'cc_rh_report_status'
    if file_name in rh_feature_dict:
        if file_name in all_bh_part_df_dict:
            if all_bh_part_df_dict[file_name].shape[0] > 0:
                try:
                    # 计算运行时间
                    
                    part_feature_list = rh_feature_dict[file_name]
                    cc_rh_report_status_feature = get_cc_rh_report_status_feature(all_bh_part_df_dict, file_name, part_feature_list)
                    


                    all_rh_feature_df_list.append(cc_rh_report_status_feature)
                except Exception as e:
                    print(e)
                    pass
    # 32-cc_rh_report_summary_credit_tips-ok
    file_name = 'cc_rh_report_summary_credit_tips'
    if file_name in rh_feature_dict:
        if file_name in all_bh_part_df_dict:
            if all_bh_part_df_dict[file_name].shape[0] > 0:
                try:
                    # 计算运行时间
                    
                    part_feature_list = rh_feature_dict[file_name]
                    cc_rh_report_summary_credit_tips_feature = get_cc_rh_report_summary_credit_tips_feature(all_bh_part_df_dict, file_name, part_feature_list)


                    all_rh_feature_df_list.append(cc_rh_report_summary_credit_tips_feature)
                except Exception as e:
                    print(e)
                    pass
    # 35-cc_rh_report_summary_debt_loan-ok
    file_name = 'cc_rh_report_summary_debt_loan'
    if file_name in rh_feature_dict:
        if file_name in all_bh_part_df_dict:
            if all_bh_part_df_dict[file_name].shape[0] > 0:
                try:
                    # 计算运行时间
                    
                    part_feature_list = rh_feature_dict[file_name]
                    cc_rh_report_summary_debt_loan_feature = get_cc_rh_report_summary_debt_loan_feature(all_bh_part_df_dict, file_name, part_feature_list)


                    all_rh_feature_df_list.append(cc_rh_report_summary_debt_loan_feature)
                except Exception as e:
                    print(e)
                    pass


    # 36-cc_rh_report_summary_overdue-ok
    file_name = 'cc_rh_report_summary_overdue'
    if file_name in rh_feature_dict:
        if file_name in all_bh_part_df_dict:
            if all_bh_part_df_dict[file_name].shape[0] > 0:
                try:
                    # 计算运行时间
                    
                    part_feature_list = rh_feature_dict[file_name]
                    cc_rh_report_summary_overdue_feature = get_cc_rh_report_summary_overdue_feature(all_bh_part_df_dict, file_name, part_feature_list)
                    


                    all_rh_feature_df_list.append(cc_rh_report_summary_overdue_feature)
                except Exception as e:
                    print(e)
                    pass



    all_bh_feature_df = pd.concat(all_rh_feature_df_list, axis=1)

    # all_feature_df_list = list(all_bh_feature_df.columns)
    #
    # new_all_feature_df = pd.DataFrame({'loan_id': [loan_id]})
    # for col in rh_feature_dict:
    #     if col in all_feature_df_list:
    #         new_all_feature_df[col] = all_bh_feature_df[col]
    #     else:
    #         new_all_feature_df[col] = np.nan

    return all_bh_feature_df





if __name__ == '__main__':
    pass




