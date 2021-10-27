#!/usr/bin/python
# -*- coding: UTF-8 -*-
"""
@file:rh_feature_utils.py
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






if __name__ == '__main__':
    pass




