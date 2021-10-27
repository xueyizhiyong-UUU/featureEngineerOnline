#!/usr/bin/python
# -*- coding: UTF-8 -*-
"""
@file:bh_feature.py
@Function:
"""
from model_utils.public_feature_utils import *
from model_utils.bh_feature_utils import *
from model_utils.bh_feature_config import *
from model_utils.extract_statistics_feature_utils import *
from model_utils.online_data_verification import *
from model_utils.deployment_util import *
import time



def get_all_part_report_df(bhInfo):

    bhInfo_df_list =['bh_bh_report',
                     'bh_bh_report_finance_lease_day_summary',
                     'bh_bh_report_finance_lease_summary',
                     'bh_bh_report_loan_non_revolving',
                     'bh_bh_report_loan_non_revolving_day_summary',
                     'bh_bh_report_loan_revolving',
                     'bh_bh_report_loan_revolving_day_summary',
                     'bh_bh_report_non_revolving_detail',
                     'bh_bh_report_non_revolving_detail_rp24',
                     'bh_bh_report_query_history',
                     'bh_bh_report_query_history_summary',
                     'bh_bh_report_revolving_detail',
                     'bh_bh_report_revolving_detail_rp24']


    df = bhInfo
    df_time = bhInfo[['loan_id', 'order_time']]
    target_col = 'baihang_info'
    id_col = 'loan_id'
    col_list = bhInfo_df_list
    save_path = ''
    all_bh_part_df_dict = analyze_dict_to_df_bh(df, df_time, id_col, target_col, col_list, save_path)
    return all_bh_part_df_dict


# 1-bh_bh_report-ok
def get_bh_bh_report_feature(all_bh_part_df_dict, file_name, loan_id, run_mode):
    bh_bh_report = all_bh_part_df_dict[file_name]
    bh_bh_report = bh_bh_report.rename(columns={'idcardNo': 'idcard_no'})

    cc_rh_report_loan_no = bh_bh_report.copy()

    cc_rh_report_loan_no = get_idcard_no_to_age(cc_rh_report_loan_no, 'idcard_no', 'order_time')
    cc_rh_report_loan_no['idcard_no_sex'] = cc_rh_report_loan_no['idcard_no'].apply(get_idcard_no_to_sex)
    cc_rh_report_loan_no['idcard_no_zodiac_cn'] = cc_rh_report_loan_no['idcard_no'].apply(get_zodiac_cn)
    cc_rh_report_loan_no['idcard_no_zodiac_en'] = cc_rh_report_loan_no['idcard_no'].apply(get_zodiac_en)
    cc_rh_report_loan_no['idcard_no_province_address'] = cc_rh_report_loan_no['idcard_no'].apply(lambda x: str(x)[:2] + '0000')
    cc_rh_report_loan_no['idcard_no_city_address'] = cc_rh_report_loan_no['idcard_no'].apply(lambda x: str(x)[:4] + '00')
    cc_rh_report_loan_no['idcard_no_county_address'] = cc_rh_report_loan_no['idcard_no'].apply(lambda x: str(x)[:6])
    bh_bh_report_deal = cc_rh_report_loan_no[['loan_id', 'idcard_no_age', 'idcard_no_sex',
                                              'idcard_no_zodiac_cn', 'idcard_no_zodiac_en',
                                              'mobileCount', 'queryResult']]
    encode_col_list = ['idcard_no_sex', 'idcard_no_zodiac_cn', 'idcard_no_zodiac_en']
    for encode_col in encode_col_list:
        file_prefix_name = 'bh_bh_report'
        bh_bh_report_deal = text_to_dict_encode(bh_bh_report_deal, encode_col, pkl_file_path, file_prefix_name, write_or_read='read')
    discrete_variable_list = ['idcard_no_sex', 'idcard_no_zodiac_cn', 'idcard_no_zodiac_en']
    df = bh_bh_report_deal.copy()
    limit_num = 100
    replace_name = 'A100000'
    file_prefix_name = 'bh_bh_report'
    bh_bh_report_encode = one_hot_deal_to_transform(df, discrete_variable_list, limit_num, replace_name, pkl_file_path, file_prefix_name)
    
    feature_name = 'bh_bh_report'
    bh_bh_report_encode_rename = feature_rename_output(bh_bh_report_encode, feature_name, ['loan_id'])
    return bh_bh_report_encode_rename


# 4_bh_bh_report_finance_lease_day_summary-ok
def get_bh_bh_report_finance_lease_day_summary_feature(all_bh_part_df_dict, file_name, loan_id, run_mode, part_feature_list):
    bh_bh_report_finance_lease_day_summary = all_bh_part_df_dict[file_name]
    del_col_list = ['id', 'createTime', 'order_time', 'reportId']
    bh_bh_report_finance_lease_day_summary = remove_irrelevant_columns(bh_bh_report_finance_lease_day_summary,
                                                                       del_col_list)

    type_col = 'type'
    id_col = 'loan_id'

    bh_col_list = list(bh_bh_report_finance_lease_day_summary.columns)
    if type_col in bh_col_list:
        bh_bh_report_finance_lease_day_summary[type_col] = bh_bh_report_finance_lease_day_summary[type_col].apply(
        int_to_str)
        bh_bh_report_finance_lease_day_summary_new = convert_multiple_to_single_lines(bh_bh_report_finance_lease_day_summary,
                                                                                  type_col, id_col)

    feature_name = 'bh_bh_report_finance_lease_day_summary'
    bh_bh_report_finance_lease_day_summary_rename = feature_rename_output(bh_bh_report_finance_lease_day_summary_new,
                                                                          feature_name, ['loan_id'])
    del bh_bh_report_finance_lease_day_summary_rename['loan_id']
    return bh_bh_report_finance_lease_day_summary_rename


# 5-bh_bh_report_finance_lease_summary-ok
def get_bh_bh_report_finance_lease_summary_feature(all_bh_part_df_dict, file_name, loan_id, run_mode, part_feature_list):
    bh_bh_report_finance_lease_day_summary = all_bh_part_df_dict[file_name]


    del_col_list = ['id', 'createTime', 'order_time', 'reportId', 'finLseCurrRemainingMaxOverdueStatus',
                    'finLseMaxOverdueStatus', 'finLseLastCompensationDate']
    bh_bh_report_finance_lease_day_summary = remove_irrelevant_columns(bh_bh_report_finance_lease_day_summary,
                                                                       del_col_list)

    feature_name = file_name
    bh_bh_report_finance_lease_day_summary_rename = feature_rename_output(bh_bh_report_finance_lease_day_summary,
                                                                          feature_name, ['loan_id'])

    del bh_bh_report_finance_lease_day_summary_rename['loan_id']
    return bh_bh_report_finance_lease_day_summary_rename


# 9-bh_bh_report_loan_non_revolving-ok
def get_bh_bh_report_loan_non_revolving_feature(all_bh_part_df_dict, file_name, loan_id, run_mode, part_feature_list):
    bh_bh_report_finance_lease_day_summary = all_bh_part_df_dict[file_name]

    del_col_list = ['id', 'order_time', 'reportId', 'lastCompensationDate']
    bh_bh_report_finance_lease_day_summary = remove_irrelevant_columns(bh_bh_report_finance_lease_day_summary,
                                                                       del_col_list)


    encode_col_list = ['remainingMaxOverdueStatus', 'maxOverdueStatus']
    for encode_col in encode_col_list:
        file_prefix_name = 'bh_bh_report_loan_non_revolving'
        bh_bh_report_finance_lease_day_summary = text_to_dict_encode(bh_bh_report_finance_lease_day_summary, encode_col, pkl_file_path, file_prefix_name, write_or_read='read')


    discrete_variable_list = ['remainingMaxOverdueStatus', 'maxOverdueStatus']
    df = bh_bh_report_finance_lease_day_summary.copy()
    limit_num = 100
    replace_name = 'A100000'

    file_prefix_name = 'bh_bh_report_loan_non_revolving'
    bh_bh_report_finance_lease_day_summary_encode = one_hot_deal_to_transform(df, discrete_variable_list, limit_num, replace_name, pkl_file_path, file_prefix_name)

    feature_name = file_name
    bh_bh_report_finance_lease_day_summary_rename = feature_rename_output(bh_bh_report_finance_lease_day_summary_encode,
                                                                          feature_name, ['loan_id'])

    del bh_bh_report_finance_lease_day_summary_rename['loan_id']
    return bh_bh_report_finance_lease_day_summary_rename



# 10-bh_bh_report_loan_non_revolving_day_summary
def get_bh_bh_report_loan_non_revolving_day_summary_feature(all_bh_part_df_dict, file_name, loan_id, run_mode, part_feature_list):
    bh_bh_report_finance_lease_day_summary = all_bh_part_df_dict[file_name]

    del_col_list = ['id', 'order_time', 'reportId']
    bh_bh_report_finance_lease_day_summary = remove_irrelevant_columns(bh_bh_report_finance_lease_day_summary,
                                                                       del_col_list)

    type_col = 'type'
    id_col = 'loan_id'

    bh_bh_report_finance_lease_day_summary[type_col] = bh_bh_report_finance_lease_day_summary[type_col].apply(
        int_to_str)
    bh_bh_report_finance_lease_day_summary_new = convert_multiple_to_single_lines(
        bh_bh_report_finance_lease_day_summary, type_col, id_col)

    feature_name = file_name
    bh_bh_report_finance_lease_day_summary_rename = feature_rename_output(bh_bh_report_finance_lease_day_summary_new,
                                                                          feature_name, ['loan_id'])

    del bh_bh_report_finance_lease_day_summary_rename['loan_id']
    return bh_bh_report_finance_lease_day_summary_rename





# 11-bh_bh_report_loan_revolving-ok
def get_bh_bh_report_loan_revolving_feature(all_bh_part_df_dict, file_name, loan_id, run_mode, part_feature_list):
    bh_bh_report_finance_lease_day_summary = all_bh_part_df_dict[file_name]

    del_col_list = ['id', 'order_time', 'reportId']
    bh_bh_report_finance_lease_day_summary = remove_irrelevant_columns(bh_bh_report_finance_lease_day_summary,
                                                                       del_col_list)

    encode_col_list = ['maxOverdueStatus', 'remainingMaxOverdueStatus']
    for encode_col in encode_col_list:
        file_prefix_name = 'bh_bh_report_loan_revolving'
        bh_bh_report_finance_lease_day_summary = text_to_dict_encode(bh_bh_report_finance_lease_day_summary, encode_col, pkl_file_path, file_prefix_name, write_or_read='read')


    discrete_variable_list = ['maxOverdueStatus', 'remainingMaxOverdueStatus']
    df = bh_bh_report_finance_lease_day_summary.copy()
    limit_num = 100

    replace_name = 'A100000'

    file_prefix_name = 'bh_bh_report_loan_revolving'
    bh_bh_report_finance_lease_day_summary_encode = one_hot_deal_to_transform(df, discrete_variable_list, limit_num, replace_name, pkl_file_path, file_prefix_name)

    feature_name = file_name
    bh_bh_report_finance_lease_day_summary_rename = feature_rename_output(bh_bh_report_finance_lease_day_summary_encode,
                                                                          feature_name, ['loan_id'])

    del bh_bh_report_finance_lease_day_summary_rename['loan_id']
    return bh_bh_report_finance_lease_day_summary_rename



# 12-bh_bh_report_loan_revolving_day_summary-ok
def get_bh_bh_report_loan_revolving_day_summary_feature(all_bh_part_df_dict, file_name, loan_id, run_mode, part_feature_list):
    bh_bh_report_finance_lease_day_summary = all_bh_part_df_dict[file_name]

    del_col_list = ['id', 'order_time', 'reportId']
    bh_bh_report_finance_lease_day_summary = remove_irrelevant_columns(bh_bh_report_finance_lease_day_summary,
                                                                       del_col_list)


    type_col = 'type'
    id_col = 'loan_id'

    bh_bh_report_finance_lease_day_summary[type_col] = bh_bh_report_finance_lease_day_summary[type_col].apply(
        int_to_str)
    bh_bh_report_finance_lease_day_summary_new = convert_multiple_to_single_lines(
        bh_bh_report_finance_lease_day_summary, type_col, id_col)


    feature_name = file_name
    bh_bh_report_finance_lease_day_summary_rename = feature_rename_output(bh_bh_report_finance_lease_day_summary_new,
                                                                          feature_name, ['loan_id'])

    del bh_bh_report_finance_lease_day_summary_rename['loan_id']
    return bh_bh_report_finance_lease_day_summary_rename



# 13-bh_bh_report_non_revolving_detail-ok
def get_bh_bh_report_non_revolving_detail_feature(all_bh_part_df_dict, file_name, loan_id, run_mode, part_feature_list):
    bh_bh_report_finance_lease_day_summary = all_bh_part_df_dict[file_name]
    # 重命名
    new_name_dict = {'nbgt':'nBGT', 'nca':'nCA', 'ncoa':'nCOA', 'nlca':'nLCA', 'nlp':'nLP',
                     'nlrs':'nLRS', 'nls':'nLS', 'notc':'nOTC', 'nrca':'nRCA', 'ntp':'nTP',
                     'ntrt':'nTRT', 'ntt':'nTT'}
    bh_bh_report_finance_lease_day_summary = bh_bh_report_finance_lease_day_summary.rename(columns=new_name_dict)

    select_col_list = []
    target_list = ['nBGT', 'nCA', 'nCOA', 'nLCA', 'nLP', 'nLRS', 'nLS', 'nOTC', 'nRCA', 'nTP', 'nTRT', 'nTT', 'loan_id']
    all_col_list = list(bh_bh_report_finance_lease_day_summary.columns)
    for col in target_list:
        if col in all_col_list:
            select_col_list.append(col)
    bh_bh_report_finance_lease_day_summary = bh_bh_report_finance_lease_day_summary[select_col_list]


    all_col_list = list(bh_bh_report_finance_lease_day_summary.columns)
    if 'nLRS' in all_col_list:
        encode_col_list = ['nLRS']
        for encode_col in encode_col_list:
            file_prefix_name = 'bh_bh_report_non_revolving_detail'
            bh_bh_report_finance_lease_day_summary = text_to_dict_encode(bh_bh_report_finance_lease_day_summary,
                                                                         encode_col, pkl_file_path, file_prefix_name,
                                                                         write_or_read='read')

        discrete_variable_list = ['nLRS']
        df = bh_bh_report_finance_lease_day_summary.copy()
        limit_num = 100

        replace_name = 'A100000'

        file_prefix_name = 'bh_bh_report_non_revolving_detail'
        bh_bh_report_finance_lease_day_summary_encode = one_hot_deal_to_transform(df, discrete_variable_list, limit_num,
                                                                                  replace_name, pkl_file_path,
                                                                                  file_prefix_name)

        
        bh_bh_report_finance_lease_day_summary_encode = onehot_filter_out_features_name(bh_bh_report_finance_lease_day_summary_encode, part_feature_list)

    else:
        bh_bh_report_finance_lease_day_summary_encode = bh_bh_report_finance_lease_day_summary
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
    # else:
    #     bh_bh_report_finance_lease_day_summary_feature = bh_bh_report_finance_lease_day_summary_encode

    feature_name = file_name
    bh_bh_report_finance_lease_day_summary_encode_rename = feature_rename_output(bh_bh_report_finance_lease_day_summary_feature, feature_name, ['loan_id'])

    del bh_bh_report_finance_lease_day_summary_encode_rename['loan_id']
    return bh_bh_report_finance_lease_day_summary_encode_rename


    # 15-bh_bh_report_non_revolving_detail_rp24-ok
def get_bh_bh_report_non_revolving_detail_rp24_feature(all_bh_part_df_dict, file_name, loan_id, run_mode, part_feature_list):
    bh_bh_report_finance_lease_day_summary = all_bh_part_df_dict[file_name]

    new_name_dict = {'nwos':'nWOS', 'npc':'nPC', 'ncps':'nCPS'}
    bh_bh_report_finance_lease_day_summary = bh_bh_report_finance_lease_day_summary.rename(columns=new_name_dict)




    del_col_list = ['id', 'nPC', 'detailId', 'order_time', 'createTime']
    detail_list = []
    del_col_list = del_col_list + detail_list
    bh_bh_report_finance_lease_day_summary = remove_irrelevant_columns(bh_bh_report_finance_lease_day_summary,
                                                                       del_col_list)

    encode_col_list = ['nCPS', 'nWOS']
    for encode_col in encode_col_list:
        file_prefix_name = 'bh_bh_report_non_revolving_detail_rp24'
        bh_bh_report_finance_lease_day_summary = text_to_dict_encode(bh_bh_report_finance_lease_day_summary, encode_col, pkl_file_path, file_prefix_name, write_or_read='read')

    discrete_variable_list = ['nCPS', 'nWOS']
    df = bh_bh_report_finance_lease_day_summary.copy()
    limit_num = 100

    replace_name = 'A100000'

    file_prefix_name = 'bh_bh_report_non_revolving_detail_rp24'
    bh_bh_report_finance_lease_day_summary_encode = one_hot_deal_to_transform(df, discrete_variable_list, limit_num,
                                                                              replace_name, pkl_file_path,
                                                                              file_prefix_name)
    
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
    bh_bh_report_finance_lease_day_summary_rename = feature_rename_output(bh_bh_report_finance_lease_day_summary_feature,
                                                                          feature_name, ['loan_id'])

    del bh_bh_report_finance_lease_day_summary_rename['loan_id']
    return bh_bh_report_finance_lease_day_summary_rename


# 18-bh_bh_report_query_history-ok
def get_bh_bh_report_query_history_feature(all_bh_part_df_dict, file_name, loan_id, run_mode, part_feature_list):
    bh_bh_report_finance_lease_day_summary = all_bh_part_df_dict[file_name]

    del_col_list = ['id', 'reportId', 'order_time']
    detail_list = ['tenantName', 'date', 'userId', 'tenantType']
    del_col_list = del_col_list + detail_list
    bh_bh_report_finance_lease_day_summary = remove_irrelevant_columns(bh_bh_report_finance_lease_day_summary,
                                                                       del_col_list)


    encode_col_list = ['reason']
    for encode_col in encode_col_list:
        bh_bh_report_finance_lease_day_summary[encode_col] = bh_bh_report_finance_lease_day_summary[encode_col].apply(
            int_to_str)

        file_prefix_name = 'bh_bh_report_query_history'
        bh_bh_report_finance_lease_day_summary = text_to_dict_encode(bh_bh_report_finance_lease_day_summary, encode_col, pkl_file_path, file_prefix_name, write_or_read='read')


    discrete_variable_list = ['reason']
    df = bh_bh_report_finance_lease_day_summary.copy()
    limit_num = 100

    replace_name = 'A100000'



    file_prefix_name = 'bh_bh_report_query_history'
    bh_bh_report_finance_lease_day_summary_encode = one_hot_deal_to_transform(df, discrete_variable_list, limit_num,
                                                                              replace_name, pkl_file_path,
                                                                              file_prefix_name)

    
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
    bh_bh_report_finance_lease_day_summary_rename = feature_rename_output(bh_bh_report_finance_lease_day_summary_feature,
                                                                          feature_name, ['loan_id'])

    del bh_bh_report_finance_lease_day_summary_rename['loan_id']
    return bh_bh_report_finance_lease_day_summary_rename


# 19-bh_bh_report_query_history_summary-ok
def get_bh_bh_report_query_history_summary_feature(all_bh_part_df_dict, file_name, loan_id, run_mode, part_feature_list):
    bh_bh_report_finance_lease_day_summary = all_bh_part_df_dict[file_name]

    # 重命名
    new_name_dict = {'caqc':'cAQC', 'caqi':'cAQI', 'gqc':'gQC',
                     'gqi':'gQI', 'pqc':'pQC', 'pqi':'pQI',
                     'qlqc':'qLQC', 'qlqi':'qLQI'}
    bh_bh_report_finance_lease_day_summary = bh_bh_report_finance_lease_day_summary.rename(columns=new_name_dict)

    del_col_list = ['id', 'reportId', 'order_time', 'createTime']
    detail_list = []
    del_col_list = del_col_list + detail_list
    bh_bh_report_finance_lease_day_summary = remove_irrelevant_columns(bh_bh_report_finance_lease_day_summary,
                                                                       del_col_list)

    type_col = 'type'
    id_col = 'loan_id'

    bh_bh_report_finance_lease_day_summary[type_col] = bh_bh_report_finance_lease_day_summary[type_col].apply(
        int_to_str)
    bh_bh_report_finance_lease_day_summary_new = convert_multiple_to_single_lines(
        bh_bh_report_finance_lease_day_summary, type_col, id_col)
    bh_bh_report_finance_lease_day_summary_encode = bh_bh_report_finance_lease_day_summary_new
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
    bh_bh_report_finance_lease_day_summary_rename = feature_rename_output(bh_bh_report_finance_lease_day_summary_feature,
                                                                          feature_name, ['loan_id'])

    del bh_bh_report_finance_lease_day_summary_rename['loan_id']
    return bh_bh_report_finance_lease_day_summary_rename



# 20-bh_bh_report_revolving_detail-ok
def get_bh_bh_report_revolving_detail_feature(all_bh_part_df_dict, file_name, loan_id, run_mode, part_feature_list):
    bh_bh_report_finance_lease_day_summary = all_bh_part_df_dict[file_name]

    # 重命名
    new_name_dict = {'rcai':'rCAI', 'rbon':'rBON', 'rlcd':'rLCD','rmos':'rMOS', 'rlrd':'rLRD',
                     'raod':'rAOD', 'rud':'rUD', 'raoc':'rAOC', 'rbgt':'rBGT', 'rbm':'rBM',
                     'rca':'rCA', 'rcal':'rCAL', 'rcas':'rCAS', 'rcoa':'rCOA', 'rcu':'rCU', 'rlca':'rLCA'}
    bh_bh_report_finance_lease_day_summary = bh_bh_report_finance_lease_day_summary.rename(columns=new_name_dict)

    del_col_list = ['id', 'reportId', 'order_time', 'createTime']
    detail_list = ['rCAI', 'rBON', 'rLCD', 'rMOS', 'rLRD', 'rAOD', 'rUD']
    del_col_list = del_col_list + detail_list
    bh_bh_report_finance_lease_day_summary = remove_irrelevant_columns(bh_bh_report_finance_lease_day_summary,
                                                                       del_col_list)
    bh_bh_report_finance_lease_day_summary_encode = bh_bh_report_finance_lease_day_summary
    bh_bh_report_finance_lease_day_summary_encode['gap_time'] = 100000

    id_col = 'loan_id'
    seg_col = 'gap_time'
    minutes_list = [2, 10, 30]
    hours_list = [2, 12]
    days_list = [1, 2, 7, 30, 60, 90, 3650]
    df = bh_bh_report_finance_lease_day_summary_encode
    feature_list = [i for i in list(df.columns) if i not in [id_col, seg_col]]
    output_name = 'frequency_minute'

    all_col_list = list(bh_bh_report_finance_lease_day_summary_encode.columns)
    all_col_list.remove('loan_id')
    all_col_list.remove('gap_time')
    for col in all_col_list:
        try:
            bh_bh_report_finance_lease_day_summary_encode[col] = bh_bh_report_finance_lease_day_summary_encode[col].astype('float')
        except Exception as e:
            del bh_bh_report_finance_lease_day_summary_encode[col]
            print(e)
            continue

    
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
    bh_bh_report_finance_lease_day_summary_rename = feature_rename_output(
        bh_bh_report_finance_lease_day_summary_feature, feature_name, ['loan_id'])

    del bh_bh_report_finance_lease_day_summary_rename['loan_id']
    return bh_bh_report_finance_lease_day_summary_rename



    # 22-bh_bh_report_revolving_detail_rp24-ok
def get_bh_bh_report_revolving_detail_rp24_feature(all_bh_part_df_dict, file_name, loan_id, run_mode, part_feature_list):
    bh_bh_report_finance_lease_day_summary = all_bh_part_df_dict[file_name]


    new_name_dict = {'npc':'rPC', 'rwos':'rWOS', 'rwps':'rWPS'}
    bh_bh_report_finance_lease_day_summary = bh_bh_report_finance_lease_day_summary.rename(columns=new_name_dict)


    del_col_list = ['id', 'order_time', 'createTime', 'detailId']
    detail_list = ['rPC']
    del_col_list = del_col_list + detail_list
    bh_bh_report_finance_lease_day_summary = remove_irrelevant_columns(bh_bh_report_finance_lease_day_summary,
                                                                       del_col_list)

    encode_col_list = ['rWOS', 'rWPS']
    for encode_col in encode_col_list:
        bh_bh_report_finance_lease_day_summary[encode_col] = bh_bh_report_finance_lease_day_summary[encode_col].apply(
            int_to_str)

        file_prefix_name = 'bh_bh_report_revolving_detail_rp24'
        bh_bh_report_finance_lease_day_summary = text_to_dict_encode(bh_bh_report_finance_lease_day_summary, encode_col, pkl_file_path, file_prefix_name, write_or_read='read')

    discrete_variable_list = ['rWOS', 'rWPS']
    df = bh_bh_report_finance_lease_day_summary.copy()

    limit_num = 100
    replace_name = 'A100000'

    file_prefix_name = 'bh_bh_report_revolving_detail_rp24'
    bh_bh_report_finance_lease_day_summary_encode = one_hot_deal_to_transform(df, discrete_variable_list, limit_num,
                                                                              replace_name, pkl_file_path,
                                                                              file_prefix_name)
    
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
    bh_bh_report_finance_lease_day_summary_rename = feature_rename_output(
        bh_bh_report_finance_lease_day_summary_feature, feature_name, ['loan_id'])

    del bh_bh_report_finance_lease_day_summary_rename['loan_id']
    return bh_bh_report_finance_lease_day_summary_rename


def get_all_bh_feature(all_deal_data_dict, loan_id, run_mode, bh_feature_dict):
    bhInfo = all_deal_data_dict['bh_df']
    all_bh_feature_df_list = []

    all_bh_part_df_dict = get_all_part_report_df(bhInfo)

    # 1_bh_bh_report-ok
    if 'bh_bh_report' in all_bh_part_df_dict:
        file_name = 'bh_bh_report'
        if all_bh_part_df_dict[file_name].shape[0] > 0:
            bh_bh_report_feature = get_bh_bh_report_feature(all_bh_part_df_dict, file_name, loan_id, run_mode)
            all_bh_feature_df_list.append(bh_bh_report_feature)

    # 4-bh_bh_report_finance_lease_day_summary-ok
    file_name = 'bh_bh_report_finance_lease_day_summary'
    if file_name in bh_feature_dict:
        if file_name in all_bh_part_df_dict:
            if all_bh_part_df_dict[file_name].shape[0] > 0:
                try:
                    # 计算运行时间
                    
                    part_feature_list = bh_feature_dict[file_name]
                    bh_bh_report_finance_lease_day_summary_feature = get_bh_bh_report_finance_lease_day_summary_feature(all_bh_part_df_dict, file_name, loan_id, run_mode, part_feature_list)
                    


                    all_bh_feature_df_list.append(bh_bh_report_finance_lease_day_summary_feature)
                except Exception as e:
                    print(e)
                    pass

    # 5-bh_bh_report_finance_lease_summary-ok
    file_name = 'bh_bh_report_finance_lease_summary'
    if file_name in bh_feature_dict:
        if file_name in all_bh_part_df_dict:
            if all_bh_part_df_dict[file_name].shape[0] > 0:
                try:
                    # 计算运行时间
                    
                    part_feature_list = bh_feature_dict[file_name]
                    bh_bh_report_finance_lease_summary_feature = get_bh_bh_report_finance_lease_summary_feature(all_bh_part_df_dict, file_name, loan_id, run_mode, part_feature_list)
                    


                    all_bh_feature_df_list.append(bh_bh_report_finance_lease_summary_feature)
                except Exception as e:
                    print(e)
                    pass


    # 9-bh_bh_report_loan_non_revolving-ok
    file_name = 'bh_bh_report_loan_non_revolving'
    if file_name in bh_feature_dict:
        if file_name in all_bh_part_df_dict:
            if all_bh_part_df_dict[file_name].shape[0] > 0:
                try:
                    # 计算运行时间
                    
                    part_feature_list = bh_feature_dict[file_name]
                    bh_bh_report_loan_non_revolving_feature = get_bh_bh_report_loan_non_revolving_feature(all_bh_part_df_dict, file_name, loan_id, run_mode, part_feature_list)
                    


                    all_bh_feature_df_list.append(bh_bh_report_loan_non_revolving_feature)
                except Exception as e:
                    print(e)
                    pass


    # 10-bh_bh_report_loan_non_revolving_day_summary-ok
    file_name = 'bh_bh_report_loan_non_revolving_day_summary'
    if file_name in bh_feature_dict:
        if file_name in all_bh_part_df_dict:
            if all_bh_part_df_dict[file_name].shape[0] > 0:
                try:
                    # 计算运行时间
                    
                    part_feature_list = bh_feature_dict[file_name]
                    bh_bh_report_loan_non_revolving_day_summary_feature = get_bh_bh_report_loan_non_revolving_day_summary_feature(all_bh_part_df_dict, file_name, loan_id, run_mode, part_feature_list)
                    


                    all_bh_feature_df_list.append(bh_bh_report_loan_non_revolving_day_summary_feature)
                except Exception as e:
                    print(e)
                    pass



    # 11-bh_bh_report_loan_revolving-ok
    file_name = 'bh_bh_report_loan_revolving'
    if file_name in bh_feature_dict:
        if file_name in all_bh_part_df_dict:
            if all_bh_part_df_dict[file_name].shape[0] > 0:
                try:
                    # 计算运行时间
                    
                    part_feature_list = bh_feature_dict[file_name]
                    bh_bh_report_loan_revolving_feature = get_bh_bh_report_loan_revolving_feature(all_bh_part_df_dict, file_name, loan_id, run_mode, part_feature_list)
                    


                    all_bh_feature_df_list.append(bh_bh_report_loan_revolving_feature)
                except Exception as e:
                    print(e)
                    pass


    # 12-bh_bh_report_loan_revolving_day_summary-ok
    file_name = 'bh_bh_report_loan_revolving_day_summary'
    if file_name in bh_feature_dict:
        if file_name in all_bh_part_df_dict:
            if all_bh_part_df_dict[file_name].shape[0] > 0:
                try:
                    # 计算运行时间
                    
                    part_feature_list = bh_feature_dict[file_name]
                    bh_bh_report_loan_revolving_day_summary_feature = get_bh_bh_report_loan_revolving_day_summary_feature(all_bh_part_df_dict, file_name, loan_id, run_mode, part_feature_list)
                    


                    all_bh_feature_df_list.append(bh_bh_report_loan_revolving_day_summary_feature)
                except Exception as e:
                    print(e)
                    pass

    # 13-bh_bh_report_non_revolving_detail-ok
    file_name = 'bh_bh_report_non_revolving_detail'
    if file_name in bh_feature_dict:
        if file_name in all_bh_part_df_dict:
            if all_bh_part_df_dict[file_name].shape[0] > 0:
                try:
                    # 计算运行时间
                    
                    part_feature_list = bh_feature_dict[file_name]
                    bh_bh_report_non_revolving_detail_feature = get_bh_bh_report_non_revolving_detail_feature(all_bh_part_df_dict, file_name, loan_id, run_mode, part_feature_list)
                    


                    all_bh_feature_df_list.append(bh_bh_report_non_revolving_detail_feature)
                except Exception as e:
                    print(e)
                    pass


    # 15-bh_bh_report_non_revolving_detail_rp24-ok
    file_name = 'bh_bh_report_non_revolving_detail_rp24'
    if file_name in bh_feature_dict:
        if file_name in all_bh_part_df_dict:
            if all_bh_part_df_dict[file_name].shape[0] > 0:
                try:
                    # 计算运行时间
                    
                    part_feature_list = bh_feature_dict[file_name]
                    bh_bh_report_non_revolving_detail_rp24_feature = get_bh_bh_report_non_revolving_detail_rp24_feature(all_bh_part_df_dict, file_name, loan_id, run_mode, part_feature_list)
                    


                    all_bh_feature_df_list.append(bh_bh_report_non_revolving_detail_rp24_feature)
                except Exception as e:
                    print(e)
                    pass


    # 18-bh_bh_report_query_history-ok
    file_name = 'bh_bh_report_query_history'
    if file_name in bh_feature_dict:
        if file_name in all_bh_part_df_dict:
            if all_bh_part_df_dict[file_name].shape[0] > 0:
                try:
                    # 计算运行时间
                    
                    part_feature_list = bh_feature_dict[file_name]
                    bh_bh_report_query_history_feature = get_bh_bh_report_query_history_feature(all_bh_part_df_dict, file_name, loan_id, run_mode, part_feature_list)


                    all_bh_feature_df_list.append(bh_bh_report_query_history_feature)
                except Exception as e:
                    print(e)
                    pass

    # 19-bh_bh_report_query_history_summary-ok
    file_name = 'bh_bh_report_query_history_summary'
    if file_name in bh_feature_dict:
        if file_name in all_bh_part_df_dict:
            if all_bh_part_df_dict[file_name].shape[0] > 0:
                try:
                    # 计算运行时间
                    
                    part_feature_list = bh_feature_dict[file_name]
                    bh_bh_report_query_history_summary_feature = get_bh_bh_report_query_history_summary_feature(all_bh_part_df_dict, file_name, loan_id, run_mode, part_feature_list)
                    


                    all_bh_feature_df_list.append(bh_bh_report_query_history_summary_feature)
                except Exception as e:
                    print(e)
                    pass

    # 20-bh_bh_report_revolving_detail-ok
    file_name = 'bh_bh_report_revolving_detail'
    if file_name in bh_feature_dict:
        if file_name in all_bh_part_df_dict:
            if all_bh_part_df_dict[file_name].shape[0] > 0:
                try:
                    # 计算运行时间
                    
                    part_feature_list = bh_feature_dict[file_name]
                    bh_bh_report_revolving_detail_feature = get_bh_bh_report_revolving_detail_feature(all_bh_part_df_dict, file_name, loan_id, run_mode, part_feature_list)
                    


                    all_bh_feature_df_list.append(bh_bh_report_revolving_detail_feature)
                except Exception as e:
                    print(e)
                    pass

    # 22-bh_bh_report_revolving_detail_rp24-ok
    file_name = 'bh_bh_report_revolving_detail_rp24'
    if file_name in bh_feature_dict:
        if file_name in all_bh_part_df_dict:
            if all_bh_part_df_dict[file_name].shape[0] > 0:
                try:
                    
                    part_feature_list = bh_feature_dict[file_name]
                    bh_bh_report_revolving_detail_rp24_feature = get_bh_bh_report_revolving_detail_rp24_feature(all_bh_part_df_dict, file_name, loan_id, run_mode, part_feature_list)
                    

                    all_bh_feature_df_list.append(bh_bh_report_revolving_detail_rp24_feature)
                except Exception as e:
                    print(e)
                    pass

    all_bh_feature_df = pd.concat(all_bh_feature_df_list, axis=1)

    # all_feature_df_list = list(all_bh_feature_df.columns)
    #
    # new_all_feature_df = pd.DataFrame({'loan_id': [loan_id]})
    # for col in bh_feature_dict:
    #     if col in all_feature_df_list:
    #         new_all_feature_df[col] = all_bh_feature_df[col]
    #     else:
    #         new_all_feature_df[col] = np.nan

    return all_bh_feature_df



if __name__ == '__main__':
    pass