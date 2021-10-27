#!/usr/bin/python
# -*- coding: UTF-8 -*-
"""
@file:customer_log_feature.py
@Function:
"""
from model_utils.extract_statistics_feature_utils import *
# from public_feature_utils import *
from model_utils.customerlog_feature_utils import *
from model_utils.customerlog_feature_config import *
from model_utils.online_data_verification import *
from model_utils.deployment_util import *



def get_path_public_feature(mongoInfo_df_deal_gap, select_col, select_time, mongoInfo_feature_dict):
    mongoInfo_feature_model_list = mongoInfo_feature_dict[select_col]

    mongoInfo_df_deal_jump = start_end_difference(mongoInfo_df_deal_gap, 'createTime', 'order_time', 'time_difference')


    mongoInfo_df_deal_jump_one_side = mongoInfo_df_deal_jump[
        ['loan_id', select_col, select_time, 'time_difference']]
    mongoInfo_df_deal_jump_one_side = mongoInfo_df_deal_jump_one_side[mongoInfo_df_deal_jump_one_side[select_col].notnull()].reset_index(drop=True)


    mongoInfo_df_deal_jump_one_side = mongoInfo_df_deal_jump_one_side.rename(columns={select_time: 'time_gap'})

    df = mongoInfo_df_deal_jump_one_side
    target_col = select_col
    limit_num = 100
    file_path = pkl_file_path


    mongoInfo_df_deal_jump_one_side_select = high_frequency_path_select(df, target_col, limit_num, file_path,
                                                                        write_or_read='read')
    df = mongoInfo_df_deal_jump_one_side_select

    encode_col_list = [select_col]
    for encode_col in encode_col_list:
        file_prefix_name = 'mongoInfo'
        mongoInfo_df_deal_appVersion_encode = text_to_dict_encode(df, encode_col, pkl_file_path, file_prefix_name, write_or_read='read')

    discrete_variable_list = [select_col]
    df = mongoInfo_df_deal_appVersion_encode
    limit_num = 100

    replace_name = 'A100000'

    mongoInfo_df_deal_appVersion = one_hot_deal_to_transform_public(df, discrete_variable_list, limit_num, replace_name)

    # 过滤掉不在入模型的特征
    mongoInfo_df_deal_appVersion = onehot_filter_out_features_name(mongoInfo_df_deal_appVersion,
                                                                   mongoInfo_feature_model_list)

    mongoInfo_df_deal_appVersion = mongoInfo_df_deal_appVersion[mongoInfo_df_deal_appVersion['loan_id'].notnull()]
    mongoInfo_df_deal_appVersion = mongoInfo_df_deal_appVersion.reset_index(drop=True)

    df = mongoInfo_df_deal_appVersion
    id_col = 'loan_id'
    time_difference_col = 'time_difference'
    time_gap_col = 'time_gap'

    mongoInfo_df_deal_appVersion = variable_to_frequency_duration(df, id_col, time_difference_col, time_gap_col)

    id_col = 'loan_id'
    seg_col = 'time_difference'
    minutes_list = [2, 10, 30]
    hours_list = [2, 12]
    days_list = [1, 2, 7, 30, 60, 90, 3650]
    df = mongoInfo_df_deal_appVersion
    feature_list = [i for i in list(df.columns) if i not in [id_col, seg_col]]
    output_name = 'minute'

    
    # split_time_list, all_aggregation_list = before_filter_out_auto_var_report_feature_deploy(days_list, hours_list, minutes_list, mongoInfo_feature_dict)
    #
    # mongoInfo_df_deal_appVersion_feature = auto_var_report_feature_deploy(df, feature_list, split_time_list, seg_col, id_col, output_name, all_aggregation_list)

    mongoInfo_df_deal_appVersion_feature = auto_var_report_feature(df, feature_list, days_list, hours_list,
                                                                   minutes_list, seg_col, id_col, output_name)

    feature_name = 'mongoInfo_'+ select_col+'_'
    mongoInfo_df_deal_appVersion_feature = feature_rename_output(mongoInfo_df_deal_appVersion_feature, feature_name, ['loan_id'])
    return mongoInfo_df_deal_appVersion_feature



def get_graph_feature(mongoInfo_df_deal):
    select_col = 'graph'

    mongoInfo_df_deal_appVersion = mongoInfo_df_deal
    mongoInfo_df_deal_appVersion = mongoInfo_df_deal_appVersion.reset_index(drop=True)


    mongoInfo_df_deal_appVersion['from_id'] = mongoInfo_df_deal_appVersion['from_id'].apply(int_to_str)
    mongoInfo_df_deal_appVersion['to_id'] = mongoInfo_df_deal_appVersion['to_id'].apply(int_to_str)


    df = mongoInfo_df_deal_appVersion
    id_col = 'loan_id'
    mongoInfo_df_deal_graph_feature = feature_deal_graph_frequency(df, id_col)

    return mongoInfo_df_deal_graph_feature


def get_customer_basict_info(mongoInfo_df_deal, loan_id, run_mode):
    mongoInfo_df_deal = start_end_difference(mongoInfo_df_deal, 'createTime', 'order_time', 'time_difference')

    mongoInfo_df_deal = mongoInfo_df_deal[mongoInfo_df_deal['time_difference'] >= 0].reset_index(drop=True)
    if mongoInfo_df_deal.shape[0]>0:
        df = mongoInfo_df_deal
        id_col = 'loan_id'
        order_time_col = 'order_time'
        create_time_col = 'createTime'
        gap_time_col = 'time_gap'
        mongoInfo_df_deal_gap = get_is_next_sentence(df, id_col, order_time_col, create_time_col, gap_time_col)

        # # 检查数据
        # if run_mode == 'offline_mode':
        #     source_name = 'mongoInfo_df_deal_gap'
        #     file_path = './data/off_data/raw_data/mongoInfo/'
        #     df = mongoInfo_df_deal_gap
        #     # 检查线上线下数据是否保持一致
        #     check_online_offline_consistent_customerlog(df, loan_id, source_name, file_path)



        mongoInfo_df_select_gap = mongoInfo_df_deal_gap[['loan_id', 'createTime', 'order_time',
                                                         'time_difference', 'time_gap',
                                                         'eventCode', 'eventType', 'is_next_sentence']]


        df = mongoInfo_df_select_gap
        id_col = 'loan_id'
        eventCode_col = 'eventCode'
        create_time_col = 'createTime'
        order_time_col = 'order_time'
        is_next_sentence_col = 'is_next_sentence'


        mongoInfo_df_deal_sequence = split_sequence_duplicate(df, id_col, eventCode_col, create_time_col,
                                                              order_time_col, is_next_sentence_col)


        df = mongoInfo_df_deal_sequence
        id_col = 'loan_id'
        eventCode_col = 'eventCode'
        create_time_col = 'createTime'
        order_time_col = 'order_time'
        is_next_sentence_col = 'is_next_sentence'


        mongoInfo_df_deal_encode = sequence_id_to_encode(df, id_col, eventCode_col, create_time_col,
                                                         order_time_col, is_next_sentence_col)
        mongoInfo_df_deal_encode = mongoInfo_df_deal_encode[mongoInfo_df_deal_encode['to_id'].notnull()]


        df = mongoInfo_df_deal_encode
        id_col = 'loan_id'
        create_time_col = 'createTime'

        mongoInfo_df_deal_jump = multi_jump_action(df, id_col, create_time_col)
        # # 检查数据
        # if run_mode == 'offline_mode':
        #     source_name = 'mongoInfo_df_deal_jump'
        #     file_path = './data/off_data/raw_data/mongoInfo/'
        #     df = mongoInfo_df_deal_jump
        #     # 检查线上线下数据是否保持一致
        #     check_online_offline_consistent_customerlog(df, loan_id, source_name, file_path)


        mongoInfo_df_deal_encode_time = start_end_difference(mongoInfo_df_deal_encode, 'createTime', 'order_time',
                                                             'time_difference')


        df = mongoInfo_df_deal_encode_time
        id_col = 'loan_id'
        order_time_col = 'order_time'
        create_time_col = 'createTime'
        gap_time_col = 'time_gap'
        mongoInfo_df_deal_encode_time = get_is_next_sentence(df, id_col, order_time_col, create_time_col, gap_time_col)


        mongoInfo_df_deal_encode_time = mongoInfo_df_deal_encode_time.drop(['createTime', 'order_time', 'sequance_id', 'is_next_sentence'], axis=1)
        mongoInfo_df_deal_encode_time = mongoInfo_df_deal_encode_time[mongoInfo_df_deal_encode_time['to_id'].notnull()]
        # # 检查数据
        # if run_mode == 'offline_mode':
        #     source_name = 'mongoInfo_df_deal_encode_time'
        #     file_path = './data/off_data/raw_data/mongoInfo/'
        #     df = mongoInfo_df_deal_encode_time
        #     # 检查线上线下数据是否保持一致
        #     check_online_offline_consistent_customerlog(df, loan_id, source_name, file_path)


    return mongoInfo_df_deal_gap, mongoInfo_df_deal_encode_time, mongoInfo_df_deal_jump






def get_appVersion_feature(mongoInfo_df_deal, select_col, mongoInfo_feature_dict):
    mongoInfo_feature_model_list = mongoInfo_feature_dict[select_col]

    mongoInfo_df_deal_appVersion = mongoInfo_df_deal[['loan_id', select_col, 'time_difference', 'time_gap']]


    mongoInfo_df_deal_appVersion[select_col] = mongoInfo_df_deal_appVersion[select_col].astype('str')
    mongoInfo_df_deal_appVersion[select_col] = mongoInfo_df_deal_appVersion[select_col].apply(lambda x: x[:3])


    encode_col_list = [select_col]
    for encode_col in encode_col_list:
        file_prefix_name = 'mongoInfo'
        mongoInfo_df_deal_appVersion_encode = text_to_dict_encode(mongoInfo_df_deal_appVersion, encode_col, pkl_file_path, file_prefix_name, write_or_read='read')


    discrete_variable_list = [select_col]
    df = mongoInfo_df_deal_appVersion_encode
    limit_num = 100

    replace_name = 'A100000'

    file_prefix_name = 'mongoInfo'
    mongoInfo_df_deal_appVersion = one_hot_deal_to_transform(df, discrete_variable_list, limit_num,
                                                                              replace_name, pkl_file_path,
                                                                              file_prefix_name)

    # 过滤掉不在入模型的特征
    mongoInfo_df_deal_appVersion = onehot_filter_out_features_name(mongoInfo_df_deal_appVersion, mongoInfo_feature_model_list)


    df = mongoInfo_df_deal_appVersion
    id_col = 'loan_id'
    time_difference_col = 'time_difference'
    time_gap_col = 'time_gap'

    mongoInfo_df_deal_appVersion = variable_to_frequency_duration(df, id_col, time_difference_col, time_gap_col)


    id_col = 'loan_id'
    seg_col = 'time_difference'
    minutes_list = [2, 10, 30]
    hours_list = [2, 12]
    days_list = [1, 2, 7, 30, 60, 90, 3650]
    df = mongoInfo_df_deal_appVersion
    feature_list = [i for i in list(df.columns) if i not in [id_col, seg_col]]
    output_name = 'minute'

    
    # split_time_list, all_aggregation_list = before_filter_out_auto_var_report_feature_deploy(days_list, hours_list, minutes_list, mongoInfo_feature_dict)
    #
    # mongoInfo_df_deal_appVersion_feature = auto_var_report_feature_deploy(df, feature_list, split_time_list, seg_col, id_col, output_name, all_aggregation_list)


    mongoInfo_df_deal_appVersion_feature = auto_var_report_feature(df, feature_list, days_list, hours_list,
                                                                   minutes_list, seg_col, id_col, output_name)

    feature_name = 'mongoInfo_' + select_col
    mongoInfo_df_deal_appVersion_feature = feature_rename_output(mongoInfo_df_deal_appVersion_feature,
                                                                        feature_name, ['loan_id'])


    del mongoInfo_df_deal_appVersion_feature['loan_id']
    return mongoInfo_df_deal_appVersion_feature



def get_public_feature(mongoInfo_df_deal, select_col, mongoInfo_feature_dict):
    mongoInfo_feature_model_list = mongoInfo_feature_dict[select_col]

    mongoInfo_df_deal_appVersion = mongoInfo_df_deal[['loan_id', select_col, 'time_difference', 'time_gap']]


    mongoInfo_df_deal_appVersion[select_col] = mongoInfo_df_deal_appVersion[select_col].apply(int_to_str)


    encode_col_list = [select_col]
    for encode_col in encode_col_list:
        file_prefix_name = 'mongoInfo'
        mongoInfo_df_deal_appVersion_encode = text_to_dict_encode(mongoInfo_df_deal_appVersion, encode_col, pkl_file_path, file_prefix_name, write_or_read='read')

    discrete_variable_list = [select_col]
    df = mongoInfo_df_deal_appVersion_encode
    limit_num = 100

    replace_name = 'A100000'

    # mongoInfo_df_deal_appVersion = one_hot_deal_to_transform_public(df, discrete_variable_list, limit_num, replace_name)

    file_prefix_name = 'mongoInfo'
    mongoInfo_df_deal_appVersion = one_hot_deal_to_transform(df, discrete_variable_list, limit_num,
                                                                              replace_name, pkl_file_path,
                                                                              file_prefix_name)
    # 过滤掉不在入模型的特征
    mongoInfo_df_deal_appVersion = onehot_filter_out_features_name(mongoInfo_df_deal_appVersion, mongoInfo_feature_model_list)


    df = mongoInfo_df_deal_appVersion
    id_col = 'loan_id'
    time_difference_col = 'time_difference'
    time_gap_col = 'time_gap'

    mongoInfo_df_deal_appVersion = variable_to_frequency_duration(df, id_col, time_difference_col, time_gap_col)

    id_col = 'loan_id'
    seg_col = 'time_difference'
    minutes_list = [2, 10, 30]
    hours_list = [2, 12]
    days_list = [1, 2, 7, 30, 60, 90, 3650]
    df = mongoInfo_df_deal_appVersion
    feature_list = [i for i in list(df.columns) if i not in [id_col, seg_col]]
    output_name = 'minute'


    
    # split_time_list, all_aggregation_list = before_filter_out_auto_var_report_feature_deploy(days_list, hours_list, minutes_list, mongoInfo_feature_dict)
    #
    # mongoInfo_df_deal_appVersion_feature = auto_var_report_feature_deploy(df, feature_list, split_time_list, seg_col, id_col, output_name, all_aggregation_list)


    mongoInfo_df_deal_appVersion_feature = auto_var_report_feature(df, feature_list, days_list, hours_list,
                                                                   minutes_list, seg_col, id_col, output_name)


    feature_name = 'mongoInfo_' + select_col
    mongoInfo_df_deal_appVersion_feature = feature_rename_output(mongoInfo_df_deal_appVersion_feature,
                                                                        feature_name, ['loan_id'])

    del mongoInfo_df_deal_appVersion_feature['loan_id']
    return mongoInfo_df_deal_appVersion_feature






def get_all_customerlog_feature(all_deal_data_dict, loan_id, run_mode, mongoInfo_feature_dict):
    mongoInfo_df = all_deal_data_dict['mongoInfo_df']

    # 计算运行时间
    
    mongoInfo_df_deal_gap, mongoInfo_df_deal_encode_time, mongoInfo_df_deal_jump = get_customer_basict_info(mongoInfo_df, loan_id, run_mode)
    


    df = mongoInfo_df
    id_col = 'loan_id'
    loan_id_df = df[[id_col]].drop_duplicates(['loan_id'], keep='first')
    all_feature_df_list = [loan_id_df]

    # 2-appVersion
    if 'appVersion' in mongoInfo_feature_dict:
        for _ in range(1):
            try:
                # 计算运行时间
                
                mongoInfo_df_deal_appVersion_feature = get_appVersion_feature(mongoInfo_df_deal_gap, 'appVersion', mongoInfo_feature_dict)
                


                all_feature_df_list.append(mongoInfo_df_deal_appVersion_feature)
            except Exception as e:
                print(e)
                continue



    # 3-businessChannel
    for _ in range(1):
        try:
            # 计算运行时间
            
            mongoInfo_df_deal_businessChannel_feature = get_public_feature(mongoInfo_df_deal_gap, 'businessChannel', mongoInfo_feature_dict)
            


            all_feature_df_list.append(mongoInfo_df_deal_businessChannel_feature)
        except Exception as e:
            print(e)
            continue

    # 4-eventCode
    for _ in range(1):
        try:
            # 计算运行时间
            
            mongoInfo_df_deal_eventCode_feature = get_public_feature(mongoInfo_df_deal_gap, 'eventCode', mongoInfo_feature_dict)
            


            all_feature_df_list.append(mongoInfo_df_deal_eventCode_feature)
        except Exception as e:
            print(e)
            continue

    # 5-eventType
    for _ in range(1):
        try:
            # 计算运行时间
            
            mongoInfo_df_deal_eventType_feature = get_public_feature(mongoInfo_df_deal_gap, 'eventType', mongoInfo_feature_dict)
            


            all_feature_df_list.append(mongoInfo_df_deal_eventType_feature)
        except Exception as e:
            print(e)
            continue


    # 6-mobileBrand
    for _ in range(1):
        try:
            # 计算运行时间
            
            mongoInfo_df_deal_mobileBrand_feature = get_public_feature(mongoInfo_df_deal_gap, 'mobileBrand', mongoInfo_feature_dict)
            


            all_feature_df_list.append(mongoInfo_df_deal_mobileBrand_feature)
        except Exception as e:
            print(e)
            continue

    # 7-mobileSystem
    for _ in range(1):
        try:
            # 计算运行时间
            
            mongoInfo_df_deal_mobileSystem_feature = get_public_feature(mongoInfo_df_deal_gap, 'mobileSystem', mongoInfo_feature_dict)


            all_feature_df_list.append(mongoInfo_df_deal_mobileSystem_feature)
        except Exception as e:
            print(e)
            continue

    # 8-net
    for _ in range(1):
        try:
            # 计算运行时间
            
            mongoInfo_df_deal_net_feature = get_public_feature(mongoInfo_df_deal_gap, 'net', mongoInfo_feature_dict)
            


            all_feature_df_list.append(mongoInfo_df_deal_net_feature)
        except Exception as e:
            print(e)
            continue

    # # 11-graph
    # for _ in range(1):
    #     try:
    #         # 计算运行时间
    #         
    #         mongoInfo_df_deal_graph_feature = get_graph_feature(mongoInfo_df_deal_encode_time)
    #         
    #         print('mongoInfo_df_deal_graph_feature:', end_time - start_time)
    #
    #         all_feature_df_list.append(mongoInfo_df_deal_graph_feature)
    #     except Exception as e:
    #         print(e)
    #         continue
    #
    # # 12-one_side_id
    # for _ in range(1):
    #     try:
    #         # 计算运行时间
    #         
    #         mongoInfo_df_deal_one_side_id_feature = get_path_public_feature(mongoInfo_df_deal_jump, 'one_side_id', 'one_side_time', mongoInfo_feature_dict)
    #         
    #         print('mongoInfo_df_deal_one_side_id_feature:', end_time - start_time)
    #
    #         all_feature_df_list.append(mongoInfo_df_deal_one_side_id_feature)
    #     except Exception as e:
    #         print(e)
    #         continue
    #
    # # 13-both_side_id
    # for _ in range(1):
    #     try:
    #         # 计算运行时间
    #         
    #         mongoInfo_df_deal_both_side_id_feature = get_path_public_feature(mongoInfo_df_deal_jump, 'both_side_id', 'both_side_time', mongoInfo_feature_dict)
    #         
    #         print('mongoInfo_df_deal_both_side_id_feature:', end_time - start_time)
    #
    #         all_feature_df_list.append(mongoInfo_df_deal_both_side_id_feature)
    #     except Exception as e:
    #         print(e)
    #         continue
    #
    # # 14-three_side_id
    # for _ in range(1):
    #     try:
    #         # 计算运行时间
    #         
    #         mongoInfo_df_deal_three_side_id_feature = get_path_public_feature(mongoInfo_df_deal_jump, 'three_side_id', 'three_side_time', mongoInfo_feature_dict)
    #         
    #         print('mongoInfo_df_deal_three_side_id_feature:', end_time - start_time)
    #
    #         all_feature_df_list.append(mongoInfo_df_deal_three_side_id_feature)
    #     except Exception as e:
    #         print(e)
    #         continue
    #
    # # 15-four_side_id
    # for _ in range(1):
    #     try:
    #         # 计算运行时间
    #         
    #         mongoInfo_df_deal_four_side_id_feature = get_path_public_feature(mongoInfo_df_deal_jump, 'four_side_id', 'four_side_time', mongoInfo_feature_dict)
    #         
    #         print('mongoInfo_df_deal_four_side_id_feature:', end_time - start_time)
    #
    #         all_feature_df_list.append(mongoInfo_df_deal_four_side_id_feature)
    #     except Exception as e:
    #         print(e)
    #         continue
    all_feature_df = pd.concat(all_feature_df_list, axis=1)
    all_feature_df_list = list(all_feature_df.columns)

    # new_all_feature_df = pd.DataFrame({'loan_id': [loan_id]})
    # for col in mongoInfo_feature_list:
    #     if col in all_feature_df_list:
    #         new_all_feature_df[col] = all_feature_df[col]
    #     else:
    #         new_all_feature_df[col] = np.nan
    return all_feature_df




if __name__ == '__main__':
    pass
