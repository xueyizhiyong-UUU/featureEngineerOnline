#!/usr/bin/python
# -*- coding: UTF-8 -*-
# 导入库
import numpy as np
import pandas as pd
import json
import re
import os
from tqdm import tqdm,trange
from model_utils.public_feature_utils import *














# 提取指定数据-desc
def get_specified_desc_data(text):
    text = json.loads(text)
    result = text['data']['desc']
#     result = json.dumps(result, ensure_ascii=False)
    result = str(result)
    return result


# 提取指定数据-result_detail
def get_specified_result_detail_data(text):
    text = json.loads(text)
    result = text['data']['result_detail']
    result = json.dumps(result, ensure_ascii=False)
    result = str(result)
    return result




# 将字典转dataframe格式
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


# 将dict数据转换成df数据
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
                #                 print(e)
                #                 print(loan_id)
                continue
        # 数据拼接
        all_data_merge = pd.concat(all_data_list, axis=0)
        # 索引重置
        all_data_merge = all_data_merge.reset_index(drop=True)
    return all_data_merge






if __name__ == '__main__':
    pass