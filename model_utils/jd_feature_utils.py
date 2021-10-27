#!/usr/bin/python
# -*- coding: UTF-8 -*-
# 导入库
import numpy as np
import pandas as pd
# from impala.dbapi import connect
import json
import re
import os
from tqdm import tqdm,trange






def get_specified_msg_data(text):
    text = json.loads(text)
    result = text['msg']
#     result = json.dumps(result, ensure_ascii=False)
    result = str(result)
    return result



def deal_json_to_df(new_baihang_info, key):
    values = new_baihang_info[key]
    if type(values).__name__ == 'dict':
        # 转换成jdataframe格式
        df = pd.DataFrame.from_dict(values, orient='index').T
    if type(values).__name__ == 'list':
        # 转换成json格式
        key = json.dumps(values, ensure_ascii=True)
        df = pd.read_json(key, orient='records')
    return df



def analyze_dict_to_df(df, id_col, target_col, col_list):
    loan_id_list = list(set(df[id_col]))
    #     one_df = pd.DataFrame(columns=col_list)
    for col in col_list:
        all_data_list = []
        for loan_id in loan_id_list:
            try:
                text = df[df[id_col] == loan_id]
                text = list(text[target_col])[0]
                #         print(text)
                text = json.loads(text)
                #             text = text[col]
                one_df = deal_json_to_df(text, col)
                #             print(text)
                #             one_df = pd.DataFrame(text)
                one_df[id_col] = str(loan_id)
                all_data_list.append(one_df)
            except Exception as e:
                print(e)
                #                 print(col)
                continue
        # 数据拼接
        all_data_merge = pd.concat(all_data_list, axis=0)
        # 索引重置
        all_data_merge = all_data_merge.reset_index(drop=True)
    return all_data_merge



def values_to_str_list(text):
    text = str(text)
    text_list = text.split(',')
    if len(text_list) > 0:
        new_text_list = []
        for col in text_list:
            col = col.replace(' ', '')
            new_text_list.append(col)
    else:
        new_text_list = []
    return new_text_list


# 提取字段名中的数值
def col_name_to_values(col_name, name):
    value = col_name.replace(name, '')
    value = value.replace('_', '')
    value = value.replace(' ', '')
    value = str(value)
    return value


# # 根据取值进行0或1映射
def whether_value_exists(text, col_name, name):
    # 将数值转化成取值列表
    new_text_list = values_to_str_list(text)

    # 提取字段名中的数值
    value = col_name_to_values(col_name, name)

    # 进行判断
    if value in new_text_list:
        result = 1
    else:
        result = 0
    return result




if __name__ == '__main__':
    pass