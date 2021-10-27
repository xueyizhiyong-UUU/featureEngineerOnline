#!/usr/bin/python
# -*- coding: UTF-8 -*-
"""
@file:bh_feature_utils.py
@Function:
"""

import pandas as pd
import numpy as np
import os
import json
import re
import datetime
import pymysql
import string
import pickle
from sklearn.preprocessing import OneHotEncoder, LabelEncoder


def remove_irrelevant_columns(df, del_col_list):
    all_col_list = list(df.columns)
    for col in del_col_list:
        if col in all_col_list:
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


def one_hot_deal_to_transform(df, discrete_variable_list, limit_num, replace_name):
    for col in discrete_variable_list:
        df = high_frequency_discrete_variable(df, col, limit_num, replace_name)
    df = one_hot_trans(df, discrete_variable_list)
    return df




def convert_multiple_to_single_lines(df, type_col, id_col):

    all_df = df[[id_col]].drop_duplicates()


    all_columns_list = list(df.columns)

    all_columns_list.remove(id_col)
    all_columns_list.remove(type_col)

    type_value_list = list(set(df[type_col]))
    for type_value in type_value_list:
        df_one = df[df[type_col] == type_value]
        del df_one[type_col]

        columns_dict = {}
        for col in all_columns_list:
            new_col = col + '_' + str(type_value)
            columns_dict[col] = new_col
        df_one = df_one.rename(columns=columns_dict)

        all_df = pd.merge(all_df, df_one, on=id_col, how='left')
    return all_df



def deal_json_to_df(new_baihang_info, key):
    values = new_baihang_info[key]
    if type(values).__name__ == 'dict':

        df = pd.DataFrame.from_dict(values, orient='index').T
    if type(values).__name__ == 'list':

        key = json.dumps(values, ensure_ascii=True)
        df = pd.read_json(key, orient='records')
    return df


def analyze_dict_to_df_bh(df, df_time, id_col, target_col, col_list, save_path):
    loan_id_list = list(set(df[id_col]))
    all_bh_part_df_dict = {}

    for col in col_list:
        try:
            all_data_list = []
            for loan_id in loan_id_list:
                text = df[df[id_col] == loan_id]
                text = list(text[target_col])[0]
                text = json.loads(text)
                one_df = deal_json_to_df(text, col)
                one_df[id_col] = str(loan_id)
                all_data_list.append(one_df)
            all_data_merge = pd.concat(all_data_list, axis=0)
            all_data_merge = all_data_merge.reset_index(drop=True)
            df_time[id_col] = df_time[id_col].astype('str')
            merge_col_list = list(all_data_merge.columns)
            if 'order_time' not in merge_col_list:
                all_data_merge = pd.merge(all_data_merge, df_time, on=id_col, how='inner')
            all_bh_part_df_dict[col] = all_data_merge
        except Exception as e:
            # print(e)
            continue

    return all_bh_part_df_dict




if __name__ == '__main__':
    pass







