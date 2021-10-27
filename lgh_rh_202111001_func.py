import pandas as pd
import numpy as np
import re


#原数据转换为WOE 相关函数
def cat_map_value(table,value,map_type="woe"):
    Bin=np.nan
    for i in range(len(table)):
        #如果缺省值
        if pd.isnull(value) and (len(re.findall('Missing|NAN|nan|NaN|NA|Na|null|NULL|Null|None|none',str(table.bin_limit.iloc[i])))>0):
            if map_type=='woe':
                Bin=table.woe.iloc[i]
            elif map_type=='bin':
                Bin=table.bin.iloc[i]
            elif map_type=='score':
                Bin=table.score.iloc[i]
            elif map_type=='woe_rank':
                Bin=table.woe_rank.iloc[i]
            break
        elif str(value).strip() in str(table.bin_limit.iloc[i]):
            if map_type=='woe':
                Bin=table.woe.iloc[i]
            elif map_type=='bin':
                Bin=table.bin.iloc[i]
            elif map_type=='score':
                Bin=table.score.iloc[i]
            elif map_type=='woe_rank':
                Bin=table.woe_rank.iloc[i]
            break
    #如果分箱中没有出现的类别，归入到占比最多的箱
    if pd.isnull(Bin):
        table=table.sort_values(['Count (%)'],ascending=False)
        if map_type=='woe':
            Bin=table.woe.iloc[0]
        elif map_type=='bin':
            Bin=table.bin.iloc[0]
        elif map_type=='score':
            Bin=table.score.iloc[0]
        elif map_type=='woe_rank':
            Bin=table.woe_rank.iloc[0]
    return Bin


def cont_map_value(table,value,map_type="woe"):
    table = table.sort_values('bin',ascending=False)
    for i in range(len(table)):
        if (pd.isnull(value) and (pd.isnull(table.Bin_LowerLimit.iloc[i]) or (len(re.findall('Missing|NAN|nan|NaN|NA|Na|null|NULL|Null|None|none',str(table.bin_limit.iloc[i])))>0))):
            if map_type=='woe':
                Bin=table.woe.iloc[i]
            elif map_type=='bin':
                Bin=table.bin.iloc[i]
            elif map_type=='score':
                Bin=table.score.iloc[i]
            elif map_type=='woe_rank':
                Bin=table.woe_rank.iloc[i]
            break
        elif (i==len(table)-1) and (value>=-np.Inf):   #如果是最小的一箱  大于负无穷即可
            if map_type=='woe':
                Bin=table.woe.iloc[i]
            elif map_type=='bin':
                Bin=table.bin.iloc[i]
            elif map_type=='score':
                Bin=table.score.iloc[i]
            elif map_type=='woe_rank':
                Bin=table.woe_rank.iloc[i]
            break
        elif value>=table.Bin_LowerLimit.iloc[i]:
            if map_type=='woe':
                Bin=table.woe.iloc[i]
            elif map_type=='bin':
                Bin=table.bin.iloc[i]
            elif map_type=='score':
                Bin=table.score.iloc[i]
            elif map_type=='woe_rank':
                Bin=table.woe_rank.iloc[i]
            break
        else:
            Bin=np.nan
    return Bin


def get_raw_cross_score(dict_in,feature_names,models):
    prob_result_list = list()
    for i in range(len(models)):
        clf = models[i]
        prob_result=clf.predict_proba(pd.DataFrame([dict_in])[feature_names].fillna('-1'))[:,1]
        prob_result_list.append(prob_result[0])
    return np.mean(prob_result_list)


def get_1woe_cross_score(dict_in,feature_names,models,table,map_type='woe'):
    tmp_dict = dict()
    for var in feature_names:
        table_var = table[table.var_name==var]
        if table_var.var_type.iloc[0]!='str':
            tmp_dict[var] = cont_map_value(table_var,dict_in[var],map_type)
        else:
            tmp_dict[var] = cat_map_value(table_var,dict_in[var],map_type)
    df_woe = pd.DataFrame([tmp_dict])[feature_names]
    prob_result_list = list()
    for i in range(len(models)):
        clf = models[i]
        prob_result=clf.predict_proba(df_woe.fillna(-1))[:,1]
        prob_result_list.append(prob_result[0])
    return np.mean(prob_result_list)


def get_5woe_cross_score(dict_in,feature_names,models,table_list,map_type='woe'):
    tmp_dict = dict()
    prob_result_list = list()
    for i in range(len(models)):
        table = table_list[i]
        for var in feature_names:
            table_var = table[table.var_name==var]
            if table_var.var_type.iloc[0]!='str':
                tmp_dict[var] = cont_map_value(table_var,dict_in[var],map_type)
            else:
                tmp_dict[var] = cat_map_value(table_var,dict_in[var],map_type)
        df_woe = pd.DataFrame([tmp_dict])[feature_names]
        clf = models[i]
        prob_result=clf.predict_proba(df_woe.fillna(-1))[:,1]
        prob_result_list.append(prob_result[0])
    return np.mean(prob_result_list)