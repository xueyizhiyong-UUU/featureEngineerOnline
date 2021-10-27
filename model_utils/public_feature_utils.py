#!/usr/bin/python
# -*- coding: UTF-8 -*-
"""
@file:public_feature_utils.py
@Function:
"""

import pandas as pd
import numpy as np
import os
import json
import re
import pymysql
# import cupy as np
import os
# from pymongo import MongoClient
# from impala.dbapi import connect
import json
import re
import datetime
import pymysql
import string
import pickle
from sklearn.preprocessing import OneHotEncoder, LabelEncoder





# 公共函数

# 读取数据
def read_data(file_name, path_file):
    df = pd.read_csv(path_file+file_name+'.csv.zip')
    return df

# 读取数据
def read_csv_data(file_name, path_file):
    df = pd.read_csv(path_file+file_name+'.csv')
    return df

def read_pickle(file_name, path_file):
    import pickle
    fr = open(path_file + file_name + '.pkl', 'rb')
    dataset_pickle = pickle.load(fr)
    fr.close()
    return dataset_pickle


def save_pickle(df, file_name, path_file):
    fw = open(path_file+file_name+'.pkl', 'wb')
    pickle.dump(df, fw, -1)
    fw.close()



def save_data(df, file_name, path_file):
    df.to_csv(path_file+file_name+'.csv.zip', encoding='utf_8_sig', index=False)


def save_csv_data(df, file_name, path_file):
    df.to_csv(path_file+file_name+'.csv', encoding='utf_8_sig', index=False)


def datatime_to_minute_int(timediff, is_abs = False):
    days = timediff.days
    minute = int(timediff.seconds/60)
    total_result = minute + days*24*60
    if is_abs:
        total_result = abs(total_result)
    return total_result








# def text_to_dict_encode(df, col, file_path, write_or_read='read'):
#     import pickle
#
#     file_name = '{}_dict'.format(str(col))
#     if write_or_read == 'write':
#
#         variables_num_list = list(df[col].unique())
#
#         start_value = 100000
#
#         trans_dict = {}
#         for variables in variables_num_list:
#             variables = str(variables)
#             if len(variables) > 0:
#                 start_value += 1
#                 trans_dict[variables] = 'A' + str(start_value)
#             else:
#                 variables = np.nan
#                 start_value += 1
#                 trans_dict[variables] = 'A' + str(start_value)
#
#
#         with open(file_path + file_name + '.pkl', 'wb') as file:
#             pickle.dump(trans_dict, file)
#     else:
#         with open(file_path + file_name + '.pkl', 'rb') as file:
#             trans_dict = pickle.load(file)
#
#
#     df[col] = df[col].replace(trans_dict)
#     return df




def feature_rename_output(df, prefix_name, exclude_list):
    col_list = list(df.columns)
    new_col_dict ={}
    for col in col_list:
        if col not in exclude_list:
            new_col_dict[col] = prefix_name + '_' +col
    df = df.rename(columns=new_col_dict)
    return df




def start_end_difference(df, start_time, end_time, diff_name_col):

    df[start_time] = pd.to_datetime(df[start_time])
    df[end_time] = pd.to_datetime(df[end_time])

    df[diff_name_col] = df[end_time]-df[start_time]
    df[diff_name_col] = df[diff_name_col].apply(lambda x: datatime_to_minute_int(x, False))
    return df



def resultData_analyze(text, key_name):
    text = json.loads(text)
    if key_name in text:
        json_text = json.dumps(text[key_name], ensure_ascii=False)
    else:
        json_text = np.nan
    return json_text


def analyze_json_to_df(df, id_col, target_col):
    loan_id_list = list(set(df[id_col]))
    all_data_list = []

    for loan_id in loan_id_list:
        text = df[df[id_col]==loan_id]
        text = list(text[target_col])[0]
        one_df = pd.read_json(text, orient='records')
        one_df[id_col] = str(loan_id)
        all_data_list.append(one_df)

    all_data_merge = pd.concat(all_data_list, axis=0)

    all_data_merge = all_data_merge.reset_index(drop=True)
    return all_data_merge



def get_idcard_no_to_birth(text):
    text = str(text)
    new_text = text[6:10] +'-'+ text[10:12]+ '-'+text[12:14]
    return new_text


def get_idcard_no_to_age(df, idcard_col, order_time):

    df['date_birth'] = df[idcard_col].apply(get_idcard_no_to_birth)


    df['date_birth'] = pd.to_datetime(df['date_birth'])
    df[order_time] = pd.to_datetime(df[order_time])


    df['idcard_no_age'] = (df[order_time] - df['date_birth']).map(lambda x: int(x.days/365))
    return df






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



def datatime_to_minute_int(timediff, is_abs = False):

    days = timediff.days
    minute = int(timediff.seconds/60)
    total_result = minute + days*24*60
    if is_abs:
        total_result = abs(total_result)
    return total_result



def start_end_difference(df, start_time, end_time, diff_name_col):

    df[start_time] = pd.to_datetime(df[start_time])
    df[end_time] = pd.to_datetime(df[end_time])

    df[diff_name_col] = df[end_time]-df[start_time]
    df[diff_name_col] = df[diff_name_col].apply(lambda x: datatime_to_minute_int(x, False))
    return df



def name_convert(name):

    is_camel_name = True
    if re.match(r'[a-z][_a-z]+$', name):
        is_camel_name = False
    elif re.match(r'[a-zA-Z]+$', name) is None:
        raise ValueError(f'Value of "name" is invalid: {name}')
    if is_camel_name:
        name = re.sub('(.)([A-Z][a-z]+)', r'\1_\2', name).lower()
    else:
        contents = re.findall('_[a-z]+', name)
        for content in contents:
            name = name.replace(content, content[1:].title())
    return name



def underscore_to_hump(df):

    hump_dict = {}
    columns_list = list(df.columns)
    for columns in columns_list:
        new_columns = name_convert(columns)
        hump_dict[columns] = new_columns


    df = df.rename(columns=hump_dict, inplace=False)

    return df



def remove_irrelevant_columns(df, del_col_list):
    columns_list = list(df.columns)
    for col in del_col_list:
        if col in columns_list:
            del df[col]
    return df



def extract_chinese_context(text):
    text = str(text)
    if len(text)> 0:
        text_chinese = ''.join(re.findall('[\u4e00-\u9fa5]', text))
        if len(text_chinese)==0:
            text_chinese = 'non'
    else:
        text_chinese = 'non'
    return text_chinese



def extract_letters_context(text):
    text = str(text)
    if len(text)> 0:
        text_letters = ''.join(re.findall(r'[A-Za-z]', text))
        if len(text_letters)==0:
            text_letters = 'non'
    else:
        text_letters = 'non'
    return text_letters


def content_str_to_datetime(text):
    text = str(text)
    text = text.replace('--', '')
    if '.' in text:
        text = text.replace('.', '')
    if len(text)==6:
        text = text+'01'
    if len(text)==8:
        year = text[:4]
        month = text[4:6]
        date = text[6:8]
        new_text = year+'-'+month+'-'+date
    else:
        new_text = '1900-01-01'
    if '000' in new_text:
        new_text = '1900-01-01'
    if '信息更新' in new_text:
        new_text = '1900-01-01'
    return new_text



def content_to_datetime(df, col_list):
    for col in col_list:
        df[col] = df[col].apply(lambda x:content_str_to_datetime(x))
        df[col] = pd.to_datetime(df[col])
    return df



def extract_text_to_number(text):
    text = str(text)
    text = "".join(list(filter(str.isdigit, text)))
    return text




def phone_num_class(mobile_4):
    mobile_4 = str(mobile_4)
    mobile_3=mobile_4[-3::]
    mobile_class=-1
    if len(re.findall(r'([012356789])\1{3}',mobile_4))>0: #8"*AAAA" 尾数4连号，尾号不等于4;
        mobile_class=8
    elif len(re.findall('000$|666$|888$|999$|4444$',mobile_4))>0: #7" *000、*666、*888 、*999 、*4444"
        mobile_class=7
    elif len(re.findall(r'1588$|1688$|([012356789])\1{2}|([012356789])[8]\2[8]|88([012356789])\3{1}|[8]([012356789])[8]\4|([012356789])\5{1}88',mobile_4))>0:
    #6" *1588 ，*1688 、*AAA 、*A8A8 、*88AA 、*8A8A 、*AA88" A 不等4; X、 Y 不相同;
        mobile_class=6
    elif len(re.findall(r'0001$|444$|[012356789]{2}88|88[012356789]{2}|([012356789])\1{1}([012356789])\2{1}|888[012356789]|[012356789]888|([012356789])\3{2}8|8([012356789])\4{2}',mobile_4))>0:
    #5" *0001 、*444 、*AB88、*88AB、*AABB 、*888A 、*A888 、*AAA8、*8AAA'" A 不等于B; A 、B 均不等于4
        mobile_class=5
    elif len(re.findall(r'(0(?=1)|1(?=2)|2(?=3)|3(?=4|$)|4(?=5|$)|5(?=6|$)|6(?=7|$)|7(?=8|$)|8(?=9|$)|9(?=0|$)){4}$',mobile_4))>0:  # ABCD 为累加数
        mobile_class=4
    elif len(re.findall(r'(0(?=1)|1(?=2)|2(?=3)|3(?=4|$)|4(?=5|$)|5(?=6|$)|6(?=7|$)|7(?=8|$)|8(?=9|$)|9(?=0|$)){4}$',mobile_4[::-1]))>0:  # ABCD 为递减数
        mobile_class=4
    elif len(re.findall(r'[012356789]168|[012356789]158|[012356789]518|8([012356789])\1{1}[8]',mobile_4))>0:  #、*X168 、*X158 、*X518 、*8YY8"  X、Y 均不等于4
        mobile_class=4
    elif len(re.findall(r'([012356789])([012356789])\2{1}\1|([012356789])\3{2}[012356789]|[012356789][012356789]99|([012356789])([012356789])\4\5',mobile_4))>0:#3" *ABBA、*AAAB 、*AB99 、*ABAB"  A、B、C 均不等于4
        mobile_class=3
    elif len(re.findall(r'(0(?=1)|1(?=2)|2(?=3|$)|3(?=4|$)|4(?=5|$)|5(?=6|$)|6(?=7|$)|7(?=8|$)|8(?=9|$)|9(?=0|$)){3}$',mobile_4))>0:  # ABC 为累加数
        mobile_class=2
    elif len(re.findall(r'(0(?=1)|1(?=2)|2(?=3|$)|3(?=4|$)|4(?=5|$)|5(?=6|$)|6(?=7|$)|7(?=8|$)|8(?=9|$)|9(?=0|$)){3}$',mobile_4[::-1]))>0:  # ABC 为递减数
        mobile_class=2
    elif len(re.findall(r'([012356789])\1{1}$|[012356789]88[012356789]|8$',mobile_4))>0:  #2" *XX 、*X88Y 、*8"  X不等于Y；X、Y 均不等于4
        mobile_class=2
    elif len(re.findall('4',mobile_3))==0:  #1" 普通号 " 除以上级别号外，号码最后三位均不等于4 的号码
        mobile_class=1
    elif len(re.findall('4',mobile_3))>0:  #0" 差号 " 除以上级如同号外，号码最后三位中任意一位等于4 的号
        mobile_class=0
    return mobile_class





def is_ture_id(id):
    id = str(id)
    if len(id) == 18:
        num17 = id[0:17]
        last_num = id[-1]
        moduls = [7, 9, 10, 5, 8, 4, 2, 1, 6, 3, 7, 9, 10, 5, 8, 4, 2]
        num17 = map(int, num17)
        num_tuple = zip(num17, moduls)  # [(1, 4), (2, 5), (3, 6)]
        num = map(lambda x: x[0] * x[1], num_tuple)
        mod = sum(num) % 11
        yushu1 = [0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10]
        yushu2 = [1, 0, 'X', 9, 8, 7, 6, 5, 4, 3, 2]
        last_yushu = dict(zip(yushu1, yushu2))
        if last_num == str(last_yushu[mod]):
            return 1
        else:
            return 0
    else:
        return 0


def get_idcard_no_to_sex(text):
    text = str(text)
    num = int(text[16:17])
    if num % 2 == 0:
        result = '女'
    else:
        result = '男'
    return result


def get_idcard_no_to_birth(text):
    text = str(text)
    new_text = text[6:10] +'-'+ text[10:12]+ '-'+text[12:14]
    return new_text




def get_idcard_no_to_age(df, idcard_col, order_time):

    df['date_birth'] = df[idcard_col].apply(get_idcard_no_to_birth)


    df['date_birth'] = pd.to_datetime(df['date_birth'])
    df[order_time] = pd.to_datetime(df[order_time])


    df['idcard_no_age'] = (df[order_time] - df['date_birth']).map(lambda x: int(x.days/365))
    return df


def get_zodiac_cn(text):
    text = str(text)
    text_year = text[6:10]
    Chinese_Zodiac =  u'猴鸡狗猪鼠牛虎兔龙蛇马羊'[int(text_year)% 12]
    return Chinese_Zodiac




def get_zodiac_en(text):
    text = str(text)
    month = int(text[10:12])
    day = int(text[12:14])
    n = (u'摩羯座',u'水瓶座',u'双鱼座',u'白羊座',u'金牛座',u'双子座',u'巨蟹座',u'狮子座',u'处女座',u'天秤座',u'天蝎座',u'射手座')
    d = ((1,20),(2,19),(3,21),(4,21),(5,21),(6,22),(7,23),(8,23),(9,23),(10,23),(11,23),(12,23))
    # english_Zodiac = lambda y: y <= (month, day)
    english_Zodiac = n[len(list(filter(lambda y:y<=(month,day), d)))%12]
    return english_Zodiac


def feature_rename_output(df, prefix_name, exclude_list):
    col_list = list(df.columns)
    new_col_dict ={}
    for col in col_list:
        if col not in exclude_list:
            new_col_dict[col] = prefix_name + '_' +col
    df = df.rename(columns=new_col_dict)
    return df


def address_to_lat(text):
    text = str(text)
    if text and ',' in text:
        text = list(eval(text))

        if len(text)>0:

            lat_content =  text[0]
        else:
            lat_content = np.nan
    else:
        lat_content = np.nan
    return lat_content



def address_to_lng(text):
    text = str(text)
    if text and ',' in text:
        #         print(text)
        text = list(eval(text))

        if len(text) > 1:

            lat_content = text[1]
        else:
            lat_content = np.nan
    else:
        lat_content = np.nan
    return lat_content


def idcard_no_to_encode(file_path, fname):
    if not os.path.isfile(fname):

        idcard_no_encode = read_csv_data('idcard_no_encode', file_path)

        idcard_no_encode['address_code'] = idcard_no_encode['address_code'].astype('str')
        idcard_no_encode_dict = idcard_no_encode.to_dict('index')


        detailed_address_code_dict = {}
        for i in range(len(idcard_no_encode_dict)):
            encode_dict = idcard_no_encode_dict[i]
            key = encode_dict['detailed_address']
            value = encode_dict['address_code']
            detailed_address_code_dict[key] = value


        f = open(file_path+fname+'.pkl','wb')
        pickle.dump(detailed_address_code_dict, f)
    else:
        f = open(file_path+fname+'.pkl','rb')
        detailed_address_code_dict = pickle.load(f)
        f.close()
    return detailed_address_code_dict



def address_dict_code(text, address_code_dict):
    for _ in range(1):
        try:
            if text:
                new_text = address_code_dict[text]
            else:
                new_text = np.nan
        except Exception as e:
            new_text = '100000'
    return new_text





def timediff_to_days(df, first_col, second_col, new_name_col):

    df[first_col] = pd.to_datetime(df[first_col])
    df[second_col] = pd.to_datetime(df[second_col])


    df[new_name_col] = (df[first_col] - df[second_col]).apply(lambda x: int(x.days))


    del df[first_col]
    del df[second_col]
    return df


def merge_different_df(df1, df2, id_col, df1_name, df2_name):

    df1.to_csv(df1_name + '.csv.zip', index=False)
    df2.to_csv(df2_name + '.csv.zip', index=False)


    df1 = pd.read_csv(df1_name + '.csv.zip')
    df2 = pd.read_csv(df2_name + '.csv.zip')


    df1[id_col] = df1[id_col].astype('str')
    df2[id_col] = df2[id_col].astype('str')


    df = pd.merge(df1, df2, on=id_col, how='inner')

    import os
    os.remove(df1_name + '.csv.zip')
    os.remove(df2_name + '.csv.zip')
    return df




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




def one_hot_trans(df, col_list, pkl_file_path, file_prefix_name):

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

            import os
            file_name = '{}_dict'.format(str(col))
            pkl_file_path_namr = pkl_file_path + file_prefix_name + '_' + file_name + '.pkl'
            if os.path.exists(pkl_file_path_namr):
                with open(pkl_file_path_namr, 'rb') as file:
                    trans_dict = pickle.load(file)
                encode_col_list = []
                for _, value in trans_dict.items():
                    encode_col_list.append(col+'_'+str(value))

                del df[col]
                df = pd.concat([df, enc_df], axis=1)
                df_col_list = list(df.columns)
                for col in encode_col_list:
                    if col not in df_col_list:
                        df[col] = 0
            else:
                del df[col]
                df = pd.concat([df, enc_df], axis=1)
    return df



def one_hot_deal_to_transform(df, discrete_variable_list, limit_num, replace_name, pkl_file_path, file_prefix_name):
    for col in discrete_variable_list:
        df = high_frequency_discrete_variable(df, col, limit_num, replace_name)


    df = one_hot_trans(df, discrete_variable_list, pkl_file_path, file_prefix_name)
    return df



def add_order_time_df(df, df_name, loan_no_file_path):

    cc_rh_report_loan_no_map = pd.read_csv(loan_no_file_path + 'cc_rh_report_loan_no_map' + '.csv.zip')


    cc_rh_report_loan_no_map = cc_rh_report_loan_no_map[['reportId', 'order_time']]


    df = merge_different_df(df, cc_rh_report_loan_no_map, 'reportId', df_name, 'cc_rh_report_loan_no_map')


    df = timediff_to_days(df, 'order_time', 'updateTime', 'gap_days')


    df = df.sort_values(by=['reportId', 'gap_days'])
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



def text_to_dict_encode(df, col, file_path, file_prefix_name, write_or_read='read'):
    import pickle

    file_name = '{}_dict'.format(str(col))
    if write_or_read == 'write':

        variables_num_list = list(df[col].unique())

        start_value = 100000

        trans_dict = {}
        for variables in variables_num_list:
            variables = str(variables)
            if len(variables) > 0:
                start_value += 1
                trans_dict[variables] = 'A' + str(start_value)
            else:
                variables = np.nan
                start_value += 1
                trans_dict[variables] = 'A' + str(start_value)

        with open(file_path +file_prefix_name+'_'+file_name + '.pkl', 'wb') as file:
            pickle.dump(trans_dict, file)
    else:
        with open(file_path +file_prefix_name+'_'+file_name + '.pkl', 'rb') as file:
            trans_dict = pickle.load(file)

    df[col] = df[col].replace(trans_dict)


    df = df.reset_index(drop=True)
    return df


def int_to_str(text):
    for _ in range(1):
        try:
            text = str(int(text))
        except Exception as e:
            text = str(text)
    return text


def str_to_float(text):
    for _ in range(1):
        try:
            text = float(text)
        except Exception as e:
            text = text
    return text


def deal_json_to_df(new_baihang_info, key):
    values = new_baihang_info[key]
    if type(values).__name__ == 'dict':

        df = pd.DataFrame.from_dict(values, orient='index').T
    if type(values).__name__ == 'list':

        key = json.dumps(values, ensure_ascii=True)
        df = pd.read_json(key, orient='records')
    return df


def deal_json_to_df_unique(new_baihang_info):
    values = new_baihang_info
    if type(values).__name__ == 'dict':

        df = pd.DataFrame.from_dict(values, orient='index').T
    if type(values).__name__ == 'list':

        key = json.dumps(values, ensure_ascii=True)
        df = pd.read_json(key, orient='records')
    return df




def analyze_dict_to_df_rh(df, df_time,id_col, target_col, col_list,save_path):
    loan_id_list = list(set(df[id_col]))
    all_rh_part_df_dict = {}
    for col in col_list:
        try:
            all_data_list = []
            for loan_id in loan_id_list:
                try:
                    text = df[df[id_col]==loan_id]
                    text = list(text[target_col])[0]
                    text = json.loads(text)
                    one_df = deal_json_to_df(text, col)
                    one_df[id_col] = str(loan_id)
                    all_data_list.append(one_df)
                except Exception as e:
                    print(e)
                    continue

            all_data_merge = pd.concat(all_data_list, axis=0)

            all_data_merge = all_data_merge.reset_index(drop=True)

            df_time[id_col] = df_time[id_col].astype('str')
            all_data_merge = pd.merge(all_data_merge, df_time, on=id_col, how='inner')
            if all_data_merge.shape[0]>0:
                all_rh_part_df_dict[col] = all_data_merge
        except Exception as e:
            print(e)
            continue
    return all_rh_part_df_dict


























if __name__ == '__main__':
    pass




