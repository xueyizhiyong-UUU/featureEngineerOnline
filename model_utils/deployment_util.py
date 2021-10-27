#!/usr/bin/python
# -*- coding: UTF-8 -*-
"""
@file:deployment_util.py
@Function:
"""
from model_utils.extract_statistics_feature_utils import *
from model_utils.public_feature_utils import *



def get_feature_name_correspond(source_name,feature_data_list,public_feature_dict):
    public_feature_list = []
    for feature_name_col in feature_data_list:
        if source_name in feature_name_col:
            public_feature_list.append(feature_name_col)
    public_feature_dict[source_name] = public_feature_list
    return public_feature_dict



def resolution_feature_name_correspond_pablic(mongoInfo_data_list, source_name_list):
    public_feature_dict = {}
    for source_name in source_name_list:
        public_feature_dict = get_feature_name_correspond(source_name, mongoInfo_data_list, public_feature_dict)
    return public_feature_dict



def get_feature_name_specified_all(all_col_list):

    first_source_name_list = ['mongoInfo', 'bh', 'rh', 'br', 'xy', 'hd', 'jd']
    model_data_dict = resolution_feature_name_correspond_pablic(all_col_list, first_source_name_list)


    # mongoInfo
    if 'mongoInfo' in model_data_dict:
        mongoInfo_list = model_data_dict['mongoInfo']
        second_source_name_list = ['appVersion', 'businessChannel', 'eventCode',
                            'eventType', 'mobileBrand', 'mobileSystem',
                            'net', 'graph', 'one_side_id', 'both_side_id',
                            'three_side_id', 'four_side_id']

        model_data_dict['mongoInfo'] = resolution_feature_name_correspond_pablic(mongoInfo_list, second_source_name_list)

    # bh
    if 'bh' in model_data_dict:
        bh_list = model_data_dict['bh']
        second_source_name_list = ['bh_bh_report_finance_lease_day_summary',
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

        model_data_dict['bh'] = resolution_feature_name_correspond_pablic(bh_list, second_source_name_list)


    # rh
    if 'rh' in model_data_dict:
        rh_list = model_data_dict['rh']
        second_source_name_list = ['cc_rh_report_customer',
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

        model_data_dict['rh'] = resolution_feature_name_correspond_pablic(rh_list, second_source_name_list)


    return model_data_dict








def auto_var_report_feature_deploy(df, feature_list, split_time_list, seg_col, id_col, output_name, all_aggregation_list):
    df_new = df[[id_col]].drop_duplicates().reset_index(drop=True)
    aggregation_list = ['sum', 'count', 'mean', 'min', 'max', 'std', 'skew', 'var']
    aggregation_list = list(set(aggregation_list)&set(all_aggregation_list))
    new_df_list = []
    if len(aggregation_list)>0:
        df_new_Agg = auto_feature_tool(df, feature_list, split_time_list, seg_col, id_col, output_name, aggregation_list, df_new)
        del df_new_Agg[id_col]
        new_df_list.append(df_new_Agg)

    pct_ver_list = ['pct_vertical']
    pct_ver_list = list(set(pct_ver_list) & set(all_aggregation_list))
    if len(pct_ver_list)>0:
        df_new_Tot_pct_ver = Tot_pct_ver(df, feature_list, split_time_list, seg_col, id_col, output_name, df_new)
        del df_new_Tot_pct_ver[id_col]
        new_df_list.append(df_new_Tot_pct_ver)

    pct_hor_list = ['pct_horizontal']
    pct_hor_list = list(set(pct_hor_list) & set(all_aggregation_list))
    if len(pct_hor_list) > 0:
        df_new_Tot_pct_hor = Tot_pct_hor(df, feature_list, split_time_list, seg_col, id_col, output_name, df_new)
        del df_new_Tot_pct_hor[id_col]
        new_df_list.append(df_new_Tot_pct_hor)
    all_df = pd.concat(new_df_list, axis=1)
    all_df[id_col] = list(df[id_col])[0]
    return all_df



def onehot_filter_out_features_name(mongoInfo_df_deal_appVersion, mongoInfo_feature_model_list):
    mongoInfo_feature_model_list = '_'.join(mongoInfo_feature_model_list)



    mongoInfo_df_deal_appVersion_col_list = list(mongoInfo_df_deal_appVersion.columns)
    for col in mongoInfo_df_deal_appVersion_col_list:
        if col not in mongoInfo_feature_model_list and '_A1000' in col:
            del mongoInfo_df_deal_appVersion[col]
    return mongoInfo_df_deal_appVersion



def statistics_filter_out_features_name(raw_feature_list, feature_model_list):
    feature_model_list = '_'.join(feature_model_list)

    for col in raw_feature_list:
        new_col = str(col)
        if new_col not in feature_model_list:
            raw_feature_list.remove(col)
    return raw_feature_list



def before_filter_out_auto_var_report_feature_deploy(days_list, hours_list, minutes_list, mongoInfo_feature_dict):
    split_time_list = time_to_minute(days_list, hours_list, minutes_list)
    split_time_list = statistics_filter_out_features_name(split_time_list, mongoInfo_feature_dict)
    all_aggregation_list = ['sum', 'count', 'mean', 'min', 'max', 'std', 'skew', 'var', 'pct_vertical',
                            'pct_horizontal']
    all_aggregation_list = statistics_filter_out_features_name(all_aggregation_list, mongoInfo_feature_dict)
    return split_time_list, all_aggregation_list






if __name__ == '__main__':
    pass




