#!/usr/bin/python
# -*- coding: UTF-8 -*-
"""
@file:extract_statistics_feature_utils.py
@Function:
"""


import numpy as np
import pandas as pd


def datatime_to_minute_int(timediff, is_abs = True):
    days = timediff.days
    minute = int(timediff.seconds/60)
    if days<0 or minute<0:
        total_result = -(abs(minute) + abs(days)*24*60)
    else:
        total_result = (abs(minute) + abs(days)*24*60)
    if is_abs:
        total_result = abs(total_result)
    return total_result



def Tot_pct_ver(df, feature_list, split_time_list, seg_col, id_col, output_name, df_new):
    df_vertical = df_new.copy()
    for col in feature_list:
        for date in split_time_list:
            try:
                new_col = col + '_pct_vertical_'+ str(date) + 'time_'+output_name
                total_temp_df = df[df[seg_col] <= split_time_list[-1]][[col]]
                temp_df = df[df[seg_col]<=date][[col]]
                if temp_df.shape[0] > 0:
                    auto_value = np.nansum(temp_df, axis=0)[0]/(np.nansum(total_temp_df, axis=0)[0]+0.000000001)
                    df_vertical[new_col] = auto_value
            except Exception as e:
                # print(e)
                continue
    return df_vertical



def Tot_pct_hor(df, feature_list, split_time_list, seg_col, id_col, output_name, df_new):
    df_vertical = df_new.copy()
    for date in split_time_list:
        for col in feature_list:
            try:
                new_col = col + '_pct_horizontal_'+str(date) + 'time_'+output_name
                total_temp_df = df[df[seg_col] <= date].drop([id_col], axis=1, inplace=False)
                temp_df = df[df[seg_col]<=date][[col]]
                if temp_df.shape[0] > 0:
                    auto_value = np.nansum(temp_df, axis=0)[0]/(sum(total_temp_df.sum().values)+0.000000001)
                    df_vertical[new_col] = auto_value
            except Exception as e:
                continue
    return df_vertical



def row_to_col_df(df,id_col, all_id_df):

    df_init = pd.merge(all_id_df, df, on=id_col, how='left')
    df_init = df_init.set_index([id_col],inplace=False)

    df_init_T = pd.DataFrame(df_init.values.T, index=df_init.columns, columns=df_init.index)

    df_init_T = df_init_T.reset_index(drop=False)

    df_init_T.reset_index(drop=True)
    return df_init_T


def col_to_row_df(df):
    id_col = 'index'

    df_init = df.set_index([id_col],inplace=False)
    df_init_T = pd.DataFrame(df_init.values.T, index=df_init.columns, columns=df_init.index)

    df_init_T = df_init_T.reset_index(drop=False)

    df_init_T = df_init_T.reset_index(drop=True)
    return df_init_T


def time_to_minute(days_list, hours_list, minutes_list):

    new_days_list = []
    if len(days_list)>0:
        for days in days_list:
            new_days = days *24*60
            new_days_list.append(new_days)

    new_hours_list = []
    if len(hours_list)>0:
        for hours in hours_list:
            new_hours = hours *24*60
            new_hours_list.append(new_hours)
    all_time_list = minutes_list+new_hours_list+new_days_list
    return all_time_list




def auto_feature_tool(df, feature_list, split_time_list, seg_col, id_col, output_name, aggregation_list, df_new):


    all_data_df_list = []
    for name_columns in feature_list:

        for i in split_time_list:
            try:
                df_one = df[df[seg_col]<=i]
                df_one_groupby = df_one.groupby([id_col])[name_columns].agg(aggregation_list).reset_index()
                all_data_i_col_list = list(df_one_groupby.columns)

                for col in all_data_i_col_list:
                    if col not in id_col:
                        new_col = name_columns+'_'+str(col)+'_'+str(i)+'time_'+output_name
                        df_one_groupby = df_one_groupby.rename(columns={col: new_col})

                df_one_groupby = row_to_col_df(df_one_groupby, id_col, df_new)
                all_data_df_list.append(df_one_groupby)
            except Exception as e:
                # print(e)
                continue

    all_data_merge = pd.concat(all_data_df_list, axis=0)

    all_data_merge = all_data_merge.drop_duplicates()
    all_data_merge = col_to_row_df(all_data_merge)
    return all_data_merge




def auto_var_report_feature(df, feature_list, days_list, hours_list, minutes_list, seg_col, id_col, output_name):
    df_new = df[[id_col]].drop_duplicates().reset_index(drop=True)
    split_time_list = time_to_minute(days_list, hours_list, minutes_list)
    aggregation_list = ['sum', 'count', 'mean', 'min', 'max', 'std', 'skew', 'var']
    df_new_Agg = auto_feature_tool(df, feature_list, split_time_list, seg_col, id_col, output_name, aggregation_list, df_new)
    df_new_Tot_pct_ver = Tot_pct_ver(df, feature_list, split_time_list, seg_col, id_col, output_name, df_new)
    del df_new_Tot_pct_ver[id_col]
    df_new_Tot_pct_hor = Tot_pct_hor(df, feature_list, split_time_list, seg_col, id_col, output_name, df_new)
    del df_new_Tot_pct_hor[id_col]
    all_df = pd.concat([df_new_Agg, df_new_Tot_pct_ver, df_new_Tot_pct_hor], axis=1)
    return all_df










if __name__ == '__main__':
    pass




