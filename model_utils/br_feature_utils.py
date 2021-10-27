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





# 提取指定数据-applyLoanStr
def get_specified_applyLoanStr_data(text):
    text = json.loads(text)
    result = text['applyLoanStr']
    result = json.dumps(result, ensure_ascii=False)
    result = str(result)
    return result



# 数据解析
def br_data_analysis(brInfo):
    # 获取分级字典中的列表
    first_col_list = []
    second_col_list = []
    third_col_list = []
    fourth_col_list = []

    # 开始逐个进行
    loan_id_list = list(set(brInfo['loan_id']))
    for loan_id in loan_id_list:
        brInfo_one = brInfo[brInfo['loan_id'] == loan_id]
        text = list(brInfo_one['bairong_info'])[0]
        text = json.loads(text)
        # 第一层
        for first_key in text.keys():
            first_text = text[first_key]
            if first_key not in first_col_list:
                first_col_list.append(first_key)
                # 第二层
                for second_key in first_text.keys():
                    second_text = first_text[second_key]
                    if second_key not in second_col_list:
                        second_col_list.append(second_key)
                    # 第三层
                    for third_key in second_text.keys():
                        try:
                            third_text = second_text[third_key]
                            #                     third_text = json.loads(third_text)
                            if third_key not in third_col_list:
                                third_col_list.append(third_key)
                            # 第四层
                            for fourth_key in third_text.keys():
                                try:
                                    fourth_text = third_text[fourth_key]
                                    if fourth_key not in fourth_col_list:
                                        fourth_col_list.append(fourth_key)
                                except Exception as e:
                                    #                                 print(fourth_key,e)
                                    continue

                        except Exception as e:
                            #                         print(fourth_key,e)
                            continue
    return first_col_list, second_col_list, third_col_list,fourth_col_list



# 将字典列表转dataframe格式
def deal_json_to_df_br(new_baihang_info):
    values = new_baihang_info
    if type(values).__name__ == 'dict':
        # 转换成jdataframe格式
        df = pd.DataFrame.from_dict(values, orient='index').T
    if type(values).__name__ == 'list':
        # 转换成json格式
        key = json.dumps(values, ensure_ascii=True)
        df = pd.read_json(key, orient='records')
    return df


# 将dict数据转换成df数据
def analyze_dict_to_df_br(df, id_col, target_col, first_col_list, second_col_list, third_col_list,
                          fourth_col_list):
    # 所有数据的空字典列表
    all_data_dict_list = []

    # 获取需要处理的订单号列表
    loan_id_list = list(set(df[id_col]))
    # 逐个进行
    for loan_id in loan_id_list:
        # 逐个获取
        one_df = df[df[id_col] == loan_id]
        text = list(one_df[target_col])[0]
        text = json.loads(text)
        # 第一层
        one_data_dict = {}
        for first_key in first_col_list:
            if first_key in text:
                first_text = text[first_key]
            else:
                first_text = {}

            # 第二层
            for second_key in second_col_list:
                if second_key in first_text:
                    second_text = first_text[second_key]
                else:
                    second_text = {}

                # 第三层
                for third_key in third_col_list:
                    if third_key in second_text:
                        third_text = second_text[third_key]
                    else:
                        third_text = {}

                    # 第四层
                    for fourth_key in fourth_col_list:
                        if fourth_key in third_text:
                            fourth_text = third_text[fourth_key]
                            new_col_name = str(first_key) + '_' + str(second_key) + '_' + str(third_key) + '_' + str(
                                fourth_key)
                            one_data_dict[new_col_name] = fourth_text
                        else:
                            new_col_name = str(first_key) + '_' + str(second_key) + '_' + str(third_key) + '_' + str(
                                fourth_key)
                            one_data_dict[new_col_name] = np.nan
        # 获取单个字典的列表
        one_data_dict[id_col] = '#' + str(loan_id)
        # 添加到列表中
        all_data_dict_list.append(one_data_dict)

    # 将字典列表转dataframe格式
    all_data_dict_df = deal_json_to_df_br(all_data_dict_list)

    # 添加订单时间
    all_data_dict_df[id_col] = all_data_dict_df[id_col].apply(lambda x: str(x.replace('#', '')))
    return all_data_dict_df


if __name__ == '__main__':
    pass