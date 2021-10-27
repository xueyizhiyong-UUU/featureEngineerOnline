#!/usr/bin/python
# -*- coding: UTF-8 -*-
"""
@file:customerlog_feature_utils.py
@Function:
"""

import requests
import json
import numpy as np
import pandas as pd
import re
import os
import time
import networkx as nx
from operator import itemgetter
import matplotlib.pyplot as plt
import networkx.algorithms as nxa
from itertools import permutations, combinations
from scipy.special import perm
from collections import namedtuple, defaultdict
from sklearn.preprocessing import OneHotEncoder, LabelEncoder





# 忽略警告
import warnings
warnings.filterwarnings("ignore")




def remove_irrelevant_columns(df, del_col_list):
    for col in del_col_list:
        del df[col]
    return df



def int_to_str(text):
    for _ in range(1):
        try:
            text = str(int(text))
        except Exception as e:
            text = str(text)
    return text




def dict_replace_list(text, missing_value_dict):
    if text in missing_value_dict:
        text = missing_value_dict[text]
    return text



def missing_value_outlier(df, col, replace_name):
    missing_value_dict = {'None': replace_name, 'NaN': replace_name, 'nan': replace_name}

    df[col] = df[col].apply(lambda x: dict_replace_list(x, missing_value_dict))

    for _ in range(1):
        try:
            df[col] = df[col].astype(int).astype(str)
        except Exception as e:
            pass
    return df


def high_frequency_discrete_variable(df, col, limit_num, replace_name):

    df = missing_value_outlier(df, col, replace_name)


    discrete_variable_list = list(set(df[col]))


    if len(discrete_variable_list) > limit_num:
        remove_variable_list = discrete_variable_list[limit_num:]

        remove_variable_dict = {}
        for remove_variable in remove_variable_list:
            remove_variable_dict[remove_variable] = replace_name
        df[col] = df[col].apply(lambda x: dict_replace_list(x, remove_variable_dict))
    #     return df
    return df



def one_hot_trans(df, col_list):
    columns_list = list(df.columns)
    for col in col_list:
        if col in columns_list:
            try:
                df[col] = df[col].astype('int').astype('str')
            except Exception as e:
                pass
            data = df[col]
            new_columns = []
            label = LabelEncoder()
            oneHot = OneHotEncoder()
            la_data = label.fit_transform(data).reshape(-1, 1)
            for cla in label.classes_:
                new_columns.append(col+'_'+str(cla))
            one_data = oneHot.fit_transform(la_data).toarray()
            enc_df = pd.DataFrame(one_data , columns=new_columns)
            del df[col]
            df = pd.concat([df, enc_df], axis=1)
    return df


def one_hot_deal_to_transform_public(df, discrete_variable_list, limit_num, replace_name):
    for col in discrete_variable_list:
        df = high_frequency_discrete_variable(df, col, limit_num, replace_name)

    if df.shape[0]>0:
        df = one_hot_trans(df, discrete_variable_list)
    return df








def datatime_to_minute_int(timediff, is_abs = False):
    for _ in range(1):
        try:
            days = timediff.days
            minute = int(timediff.seconds/60)
            total_result = minute + days*24*60
            if is_abs:
                total_result = abs(total_result)
        except:
            total_result = 0
    return total_result



def datatime_to_seconds_int(timediff, is_abs = False):
    for _ in range(1):
        try:

            days = timediff.days
            seconds = int(timediff.seconds)
            total_result = seconds + days*24*60*60
            if is_abs:
                total_result = abs(total_result)
        except Exception as e:
            total_result = np.nan
    return total_result


def one_hot_trans(df, col_list):
    columns_list = list(df.columns)
    for col in col_list:
        if col in columns_list:
            data = df[col]
            new_columns = []
            label = LabelEncoder()
            oneHot = OneHotEncoder()
            la_data = label.fit_transform(data).reshape(-1, 1)
            for cla in label.classes_:
                new_columns.append(col+'_'+str(cla))
            one_data = oneHot.fit_transform(la_data).toarray()
            enc_df = pd.DataFrame(one_data , columns=new_columns)
            del df[col]
            df = pd.concat([df, enc_df], axis=1)
    return df





def get_is_next_sentence(df, id_col, order_time_col, create_time_col, gap_time_col='gap_time',
                         is_next_sentence_name = 'is_next_sentence',
                         time_limit=10,periods=-1):

    df = df[(df[order_time_col].notnull())&(df[create_time_col].notnull())]
    df = df.sort_values([id_col, order_time_col, create_time_col], ascending=[True, True, True])

    df[order_time_col] = pd.to_datetime(df[order_time_col])
    df[create_time_col] = pd.to_datetime(df[create_time_col])

    df[gap_time_col] =  (df[create_time_col].shift(periods=periods)-df[create_time_col])

    df[gap_time_col] = df[gap_time_col].apply(lambda x: datatime_to_minute_int(x, True))

    df[is_next_sentence_name] = 0
    df.loc[df[gap_time_col]>time_limit, is_next_sentence_name] = 1
    return df


def remove_irrelevant_events(df, id_col, eventCode_col, create_time_col,
                             order_time_col, is_next_sentence_col,
                             exclude_eventCode_list=[300001,800001,300002]):
    df_new = df.copy()

    df_new = df_new[[id_col, eventCode_col, create_time_col, order_time_col, is_next_sentence_col]]

    # 排序
    df_new = df_new[(df_new[order_time_col].notnull())&(df_new[create_time_col].notnull())]
    df_new = df_new.sort_values([id_col, order_time_col, create_time_col], ascending=[True, True, True])

    df_new = df_new[~df_new[eventCode_col].isin(exclude_eventCode_list)]


    df_new = df_new.reset_index(drop=True)
    return df_new





def remove_duplicate_eventCode(df, id_col, eventCode_col, is_next_sentence_col):

    df['eventCode_up'] = df[eventCode_col].shift(periods=-1)
    df['eventCode_down'] = df[eventCode_col].shift(periods=1)
    df['eventCode_up_judge'] = np.where(df[eventCode_col] == df['eventCode_up'], 1, 0)
    df['eventCode_down_judge'] = np.where(df[eventCode_col] == df['eventCode_down'], 1, 0)


    df = df[
        (df['eventCode_up_judge'] == 1) & (df['eventCode_down_judge'] == 1) & (df['is_next_sentence'] == 0) == False]

    df = df[
        (df['eventCode_up_judge'] == 0) & (df['eventCode_down_judge'] == 1) & (df['is_next_sentence'] == 0) == False]


    tem_list = ['eventCode_up', 'eventCode_down', 'eventCode_up_judge', 'eventCode_down_judge']
    for col in tem_list:
        del df[col]

    df = df.reset_index(drop=True)
    return df



def remove_Isolated_point(df, id_col, eventCode_col, is_next_sentence_col):

    df['next_sentence_up'] = df[is_next_sentence_col].shift(periods=-1)
    df['next_sentence_down'] = df[is_next_sentence_col].shift(periods=1)
    df['next_sentence_up_judge'] = np.where(df[is_next_sentence_col] == df['next_sentence_up'], 1, 0)
    df['next_sentence_down_judge'] = np.where(df[is_next_sentence_col] == df['next_sentence_down'], 1, 0)

    df = df[(df['next_sentence_up_judge'] == 1) & (df['next_sentence_down_judge'] == 1) & (
                df[is_next_sentence_col] == 1) == False]


    tem_list = ['next_sentence_up', 'next_sentence_down', 'next_sentence_up_judge', 'next_sentence_down_judge']
    for col in tem_list:
        del df[col]

    df = df.reset_index(drop=True)
    return df



def split_sequence_duplicate(df, id_col, eventCode_col, create_time_col,
                             order_time_col, is_next_sentence_col,
                             exclude_eventCode_list=[300001, 800001, 300002]):
    df_new = df.copy()

    df_new = remove_irrelevant_events(df_new, id_col, eventCode_col, create_time_col,
                                      order_time_col, is_next_sentence_col,
                                      exclude_eventCode_list=[300001, 800001, 300002])

    df_new = remove_duplicate_eventCode(df_new, id_col, eventCode_col, is_next_sentence_col)

    df_new = remove_Isolated_point(df_new, id_col, eventCode_col, is_next_sentence_col)
    return df_new




def multi_jump_action(df, id_col, create_time_col):

    df = df.drop(['to_id'], axis=1,inplace=False)

    loan_id_list = list(set(list(df[id_col])))

    df[create_time_col] = pd.to_datetime(df[create_time_col])

    df['from_id'] = df['from_id'].map(lambda x:str(x).split('.')[0])


    hue_customer_log_concat = pd.DataFrame().reindex_like(df).dropna()
    hue_customer_log_list = [hue_customer_log_concat]
    for loan_id in loan_id_list:

        one_user_df = df[df[id_col] == loan_id]

        sequance_id_list = list(set(list(df['sequance_id'])))

        one_user_df_concat = pd.DataFrame().reindex_like(one_user_df).dropna()
        one_user_df_list = [one_user_df_concat]

        for sequance_id in sequance_id_list:
            one_user_df_step = one_user_df[one_user_df['sequance_id'] == sequance_id]

            one_user_df_step_len = one_user_df_step.shape[0]

            if one_user_df_step_len==2:

                one_user_df_step['one_side_id'] = one_user_df_step['from_id'].shift(periods=-1)
                one_user_df_step['one_side_time'] = one_user_df_step['createTime'].shift(periods=-1)


            elif one_user_df_step_len==3:

                one_user_df_step['one_side_id'] = one_user_df_step['from_id'].shift(periods=-1)
                one_user_df_step['one_side_time'] = one_user_df_step['createTime'].shift(periods=-1)


                one_user_df_step['both_side_time'] = one_user_df_step['createTime'].shift(periods=-2)
                one_user_df_step['both_side_id'] = one_user_df_step['from_id'].shift(periods=-2)


            elif one_user_df_step_len==4:

                one_user_df_step['one_side_id'] = one_user_df_step['from_id'].shift(periods=-1)
                one_user_df_step['one_side_time'] = one_user_df_step['createTime'].shift(periods=-1)

                one_user_df_step['both_side_time'] = one_user_df_step['createTime'].shift(periods=-2)
                one_user_df_step['both_side_id'] = one_user_df_step['from_id'].shift(periods=-2)


                one_user_df_step['three_side_time'] = one_user_df_step['createTime'].shift(periods=-3)
                one_user_df_step['three_side_id'] = one_user_df_step['from_id'].shift(periods=-3)


            elif one_user_df_step_len>=5:

                one_user_df_step['one_side_id'] = one_user_df_step['from_id'].shift(periods=-1)
                one_user_df_step['one_side_time'] = one_user_df_step['createTime'].shift(periods=-1)


                one_user_df_step['both_side_time'] = one_user_df_step['createTime'].shift(periods=-2)
                one_user_df_step['both_side_id'] = one_user_df_step['from_id'].shift(periods=-2)


                one_user_df_step['three_side_time'] = one_user_df_step['createTime'].shift(periods=-3)
                one_user_df_step['three_side_id'] = one_user_df_step['from_id'].shift(periods=-3)


                one_user_df_step['four_side_time'] = one_user_df_step['createTime'].shift(periods=-4)
                one_user_df_step['four_side_id'] = one_user_df_step['from_id'].shift(periods=-4)
            else:
                pass

            one_user_df_list.append(one_user_df_step)
            # one_user_df_concat = pd.concat([one_user_df_concat, one_user_df_step], axis=0,ignore_index=True)
        one_user_df_concat = pd.concat(one_user_df_list, axis=0,ignore_index=True)

        hue_customer_log_list.append(one_user_df_concat)

    hue_customer_log_concat = pd.concat(hue_customer_log_list, axis=0, ignore_index=True)


    for _ in range(1):
        try:
            hue_customer_log_concat['one_side_time'] = (hue_customer_log_concat['one_side_time'] - hue_customer_log_concat['createTime'])
            hue_customer_log_concat['one_side_time'] = hue_customer_log_concat['one_side_time'].apply(lambda x: datatime_to_seconds_int(x, is_abs=False))
            hue_customer_log_concat['one_side_id'] = hue_customer_log_concat['from_id'] + '_' + hue_customer_log_concat['one_side_id']
        except Exception as e:
            print(e)



    for _ in range(1):
        try:
            hue_customer_log_concat['both_side_time'] = hue_customer_log_concat['both_side_time'] - hue_customer_log_concat['createTime']
            hue_customer_log_concat['both_side_time'] = hue_customer_log_concat['both_side_time'].apply(lambda x:datatime_to_seconds_int(x, is_abs=False))
            hue_customer_log_concat['both_side_id'] = hue_customer_log_concat['one_side_id'] + '_' + hue_customer_log_concat['both_side_id']
        except Exception as e:
            print(e)


    for _ in range(1):
        try:
            hue_customer_log_concat['three_side_time'] = (hue_customer_log_concat['three_side_time'] - hue_customer_log_concat['createTime'])
            hue_customer_log_concat['three_side_time'] = hue_customer_log_concat['three_side_time'].apply(lambda x: datatime_to_seconds_int(x, is_abs=False))
            hue_customer_log_concat['three_side_id'] = hue_customer_log_concat['both_side_id'] + '_' + hue_customer_log_concat['three_side_id']
        except Exception as e:
            print(e)


    for _ in range(1):
        try:
            hue_customer_log_concat['four_side_time'] = (hue_customer_log_concat['four_side_time'] - hue_customer_log_concat['createTime'])
            hue_customer_log_concat['four_side_time'] = hue_customer_log_concat['four_side_time'].apply(lambda x: datatime_to_seconds_int(x, is_abs=False))
            hue_customer_log_concat['four_side_id'] = hue_customer_log_concat['three_side_id'] + '_' + hue_customer_log_concat['four_side_id']
        except Exception as e:
            print(e)
    return hue_customer_log_concat





def sequence_id_to_encode(df, id_col, eventCode_col, create_time_col, order_time_col, is_next_sentence_col):

    df = df[[id_col, eventCode_col, create_time_col, order_time_col, is_next_sentence_col]]

    df['to_id'] = df[eventCode_col].shift(periods=-1)
    df.loc[df[is_next_sentence_col] == 1, 'to_id'] =int(777777)
    # from_id
    new_col_name = '{}'.format(eventCode_col)
    df.rename(columns={new_col_name:'from_id'},inplace=True)


    df['sequance_id'] = 0
    new_df = pd.DataFrame().reindex_like(df).dropna()


    loan_id_list = list(set(list(df[id_col])))


    new_df_list = [new_df]
    for loan_id in loan_id_list:
        one_user_df = df[df[id_col] == loan_id]
        index_list = one_user_df.index.tolist()

        m = 1
        for i in range(len(index_list)):

            next_sequence_name = '{}'.format(is_next_sentence_col)
            previous_row = one_user_df.loc[index_list[i], next_sequence_name]
            one_user_df.loc[index_list[i], 'sequance_id'] = m
            if previous_row==1:
                m += 1

        new_df_list.append(one_user_df)


    new_df = pd.concat(new_df_list,axis=0,ignore_index=True)

    new_df = new_df[new_df[is_next_sentence_col]==0]
    new_df = new_df.drop([is_next_sentence_col], axis=1, inplace=False)
    return new_df



def deal_var_graph_data(new_hue_customer_log_target_data):

    new_hue_customer_log_target_data['interval_time'] = (new_hue_customer_log_target_data['createTime']- new_hue_customer_log_target_data['order_time']).apply(lambda x: datatime_to_minute_int(x))



    new_hue_customer_log_target_data['from_id'] = new_hue_customer_log_target_data['from_id'].apply(lambda x: str(x).split('.')[0])
    new_hue_customer_log_target_data['to_id'] = new_hue_customer_log_target_data['to_id'].apply(lambda x: str(x).split('.')[0])


    new_hue_customer_log_target_data = new_hue_customer_log_target_data.drop(['createTime', 'order_time', 'sequance_id'], axis=1)
    return new_hue_customer_log_target_data



def split_frequency_duration_weight(temp_df):

    hue_customer_log_groupby = temp_df.groupby(['loan_id', 'from_id','to_id'])['sec_shift_up'].agg({'size', 'sum'}).reset_index()
    hue_customer_log_groupby = hue_customer_log_groupby.rename(columns={'size':'frequency', 'sum':'duration'})
    hue_customer_log_graph_frequency = hue_customer_log_groupby[['loan_id', 'from_id','to_id', 'frequency']]
    hue_customer_log_graph_duration = hue_customer_log_groupby[['loan_id', 'from_id','to_id', 'duration']]
    return hue_customer_log_graph_frequency, hue_customer_log_graph_duration



def create_directed_graph(df, feature_type):

    graph_df = df[['from_id', 'to_id', feature_type]]


    G = nx.from_pandas_edgelist(graph_df, 'from_id', 'to_id', feature_type, create_using=nx.DiGraph())
    return G, graph_df




def number_edges_nodes(G, df_new, date, feature_type):

    number_edges = G.number_of_edges()
    number_edges_col = 'number_edges_' + str(feature_type) + '_' + str(date) + 'minute'
    df_new[number_edges_col] = number_edges


    number_nodes = G.number_of_nodes()
    number_nodes_col = 'number_nodes_' + str(feature_type) + '_' + str(date) + 'minute'
    df_new[number_nodes_col] = number_nodes
    return df_new





def all_centrality(G):

    dc = nx.degree_centrality(G)
    in_dc = nx.in_degree_centrality(G)
    out_dc = nx.out_degree_centrality(G)

    dcs = sorted(dc.items(), key=itemgetter(1), reverse=True)
    in_dcs = sorted(in_dc.items(), key=itemgetter(1), reverse=True)
    out_dcs = sorted(out_dc.items(), key=itemgetter(1), reverse=True)


    cc = nx.closeness_centrality(G)
    ccs = sorted(cc.items(), key=itemgetter(1), reverse=True)


    bc = nx.betweenness_centrality(G)
    bcs = sorted(bc.items(), key=itemgetter(1), reverse=True)


    for _ in range(1):
        try:
            ec = nx.eigenvector_centrality(G, max_iter=10000)
            ecs = sorted(ec.items(), key=itemgetter(1), reverse=True)
        except Exception as e:
            print(e)
            ec = {}
            ecs = [()]

    all_centrality_list = [dc, dcs, in_dc, in_dcs, out_dc, out_dcs, cc, ccs, bc, bcs, ec, ecs]
    return all_centrality_list



def pagerank(G, graph_df, feature_type):

    pk_weight = nx.pagerank(G)
    pks_weight = sorted(pk_weight.items(), key=itemgetter(1), reverse=True)


    G_count = nx.from_pandas_edgelist(graph_df, 'from_id', 'to_id', feature_type)
    pk_count = nx.pagerank(G_count)
    pks_count = sorted(pk_count.items(), key=itemgetter(1), reverse=True)
    all_pk_list = [pk_weight, pks_weight, pk_count, pks_count]
    return all_pk_list




def average_clustering_number(G, from_id_list):

    ac_1 = {}
    ac_2 = {}
    ac_3 = {}

    for from_id in from_id_list:
        try:

            bieb_1 = nx.Graph(nx.ego_graph(G, from_id, radius=1))
            ac_1[from_id] = nx.average_clustering(bieb_1)

            bieb_2 = nx.Graph(nx.ego_graph(G, from_id, radius=2))
            ac_2[from_id] = nx.average_clustering(bieb_2)


            bieb = nx.Graph(nx.ego_graph(G, from_id, radius=3))
            ac_3[from_id] = nx.average_clustering(bieb)
        except Exception as e:
            # print(e)
            continue


    acs_1 = sorted(ac_1.items(), key=itemgetter(1), reverse=True)
    acs_2 = sorted(ac_2.items(), key=itemgetter(1), reverse=True)
    acs_3 = sorted(ac_3.items(), key=itemgetter(1), reverse=True)
    all_ac_list = [ac_1, acs_1, ac_2, acs_2, ac_3, acs_3]
    return all_ac_list




def merge_different_data(all_centrality_list, all_pk_list, all_ac_list):

    dc, dcs, in_dc, in_dcs, out_dc, out_dcs, cc, ccs, bc, bcs, ec, ecs = all_centrality_list[0], all_centrality_list[1], all_centrality_list[2], all_centrality_list[3],\
                                                                         all_centrality_list[4], all_centrality_list[5], all_centrality_list[6], all_centrality_list[7], \
                                                                         all_centrality_list[8], all_centrality_list[9], all_centrality_list[10], all_centrality_list[11]

    pk_weight, pks_weight, pk_count, pks_count = all_pk_list[0], all_pk_list[1], all_pk_list[2], all_pk_list[3]

    ac_1, acs_1, ac_2, acs_2, ac_3, acs_3 = all_ac_list[0], all_ac_list[1], all_ac_list[2], all_ac_list[3], all_ac_list[4], all_ac_list[5]

    names = set(next(zip(*dcs)))| set(next(zip(*in_dcs))) | set(next(zip(*out_dcs))) | set(next(zip(*ccs))) | set(next(zip(*bcs))) | set(next(zip(*ecs))) | set(next(zip(*pks_weight)))  | set(next(zip(*pks_count))) | set(next(zip(*acs_1))) | set(next(zip(*acs_2))) | set(next(zip(*acs_3)))
    c_data = [[dc[name], in_dc[name], out_dc[name], cc[name], bc[name], ec[name], pk_weight[name], pk_count[name], ac_1[name], ac_2[name], ac_3[name]] for name in names]


    one_df = pd.DataFrame(c_data, index=names, columns=['degree_centrality', 'in_degree_centrality', 'out_degree_centrality', 'closeness_centrality', 'betweenness_centrality',
                                                      'eigenvector_centrality', 'pagerank_weight', 'pagerank_count', 'average_clustering_1_degree',
                                                      'average_clustering_2_degree', 'average_clustering_3_degree'])


    one_df = one_df.reset_index().rename(columns={'index':'from_id'})
    return one_df


def many_to_one_trans(df, from_id_list, df_new, date, feature_type):
    new_col_dict = {}
    col_list = list(df.columns)
    col_list.remove('from_id')
    new_df = pd.DataFrame()


    for from_id in from_id_list:
        for col in col_list:
            new_col = str(from_id) + '_' + col+feature_type+'_'+str(date)+'_minute'
            new_col_dict[col] = new_col
        df_one = df[df['from_id'] == from_id].reset_index(drop=True)
        df_one = df_one.rename(columns=new_col_dict)
        del df_one['from_id']
        new_df = pd.concat([new_df, df_one], axis=1)


    df_new = pd.concat([df_new, new_df], axis=1)
    return df_new




def centrality_pagerank_clustering_merge(G, graph_df, node_id_list, df_new, date, feature_type):
    for _ in range(1):
        try:

            all_centrality_list = all_centrality(G)

            all_pk_list = pagerank(G, graph_df, feature_type)


            all_ac_list = average_clustering_number(G, node_id_list)


            one_df = merge_different_data(all_centrality_list, all_pk_list, all_ac_list)


            df_new = many_to_one_trans(one_df, node_id_list, df_new, date, feature_type)
        except Exception as e:
            pass
    return df_new





def auto_var_graph_feature(df, date_list, time_difference_col,feature_type_list, time_gap_col):

    df_new = df[['loan_id']].drop_duplicates().reset_index(drop=True)
    for date in date_list:
        for feature_type in feature_type_list:
            temp_df = df[df[time_difference_col] <= date]
            if temp_df.shape[0]>0:

                temp_df = temp_df.groupby(['loan_id', 'from_id', 'to_id'])[time_gap_col].agg({'size', 'sum'}).reset_index()
                temp_df = temp_df.rename(columns={'size': 'frequency', 'sum':'duration'})
                temp_df = temp_df[['loan_id', 'from_id', 'to_id', feature_type]]


                G, graph_df = create_directed_graph(temp_df, feature_type)

                node_id_list = list(set(list(temp_df['from_id']) + list(temp_df['to_id'])))

                df_new = number_edges_nodes(G, df_new, date, feature_type)
#                 print(len(list(set(df_new['loan_id']))))

                df_new = centrality_pagerank_clustering_merge(G, graph_df, node_id_list, df_new, date, feature_type)
    return df_new





def feature_deal_graph_frequency(temp_df, id_col):

    all_loan_id_list = list(set(temp_df[id_col]))
    all_feature_df_list = []
    #     print(all_loan_id_list)
    for loan_id in all_loan_id_list:

        one_df = temp_df[temp_df[id_col] == loan_id]

        date_list = [1, 3, 10, 60, 720, 1440, 4320, 10080, 43200, 1576800]
        time_difference_col = 'time_difference'
        feature_type_list = ['frequency', 'duration']
        time_gap_col = 'time_gap'


        var_graph_feature_df = auto_var_graph_feature(one_df, date_list, time_difference_col, feature_type_list,
                                                      time_gap_col)
        all_feature_df_list.append(var_graph_feature_df)

    all_feature_data = pd.concat(all_feature_df_list, axis=0).reset_index()
    return all_feature_data





def high_frequency_path_select(df, target_col, limit_num, file_path, write_or_read='read'):
    import pickle

    file_name = '{}_dict'.format(str(target_col))
    if write_or_read == 'write':

        df_new = df[target_col].value_counts().rename_axis('unique_values').reset_index(name='counts')
        df_new = df_new.sort_values(by="counts", ascending=False)[:limit_num]

        high_frequency_path_list = list(df_new['unique_values'])


        df_select = df[df[target_col].isin(high_frequency_path_list)]


        with open(file_path + file_name + '.pkl', 'wb') as file:
            pickle.dump(high_frequency_path_list, file)
    else:
        with open(file_path + file_name + '.pkl', 'rb') as file:
            high_frequency_path_list = pickle.load(file)

        df_select = df[df[target_col].isin(high_frequency_path_list)]

    return df_select




def variable_to_frequency_duration(df, id_col, time_difference_col, time_gap_col,
                                   frequency_suffix_name='frequency', duration_suffix_name='duration'):
    df_new = df.copy()

    columns_list = list(df_new.columns)

    for col in [id_col, time_difference_col, time_gap_col]:
        columns_list.remove(col)

    df_public = df_new[[id_col, time_difference_col]]


    df1 = df_new[columns_list]

    frequency_name_dict = {}
    for col in columns_list:
        frequency_name_dict[col] = str(col) + '_' + str(frequency_suffix_name)
    df1 = df1.rename(columns=frequency_name_dict)


    df2 = df_new[columns_list]

    duration_name_dict = {}
    for col in columns_list:
        duration_name_dict[col] = str(col) + '_' + str(duration_suffix_name)
        df2[col] = df_new[time_gap_col] * df2[col]
    df2 = df2.rename(columns=duration_name_dict)

    all_df = pd.concat([df_public, df1, df2], axis=1)
    all_df = all_df.reset_index(drop=True)

    return all_df











if __name__ == '__main__':
    pass




