import numpy as np
import pandas as pd
from scipy.stats import chi2
import scorecardpy as sc
import sqlalchemy
from sqlalchemy import create_engine
import copy
import statsmodels.api as smf
import string
import warnings
import re
from pandas.api.types import is_numeric_dtype
from pandas.api.types import is_string_dtype
import datetime
import hashlib
import pickle
from joblib import Parallel, delayed
from sklearn.preprocessing import OneHotEncoder, LabelEncoder
from tqdm import tqdm

import difflib



from my_func_0_1 import *
# import MySQLhelper


def get_data(baihang_info=None):

    dict_in = dict()
    dict_out = dict()

    # 百行征信报告
    bh_bh_report = pd.DataFrame([baihang_info['bh_bh_report']]) if 'bh_bh_report' in baihang_info.keys() else pd.DataFrame()
    bh_bh_report['id'] = bh_bh_report['id'] if 'id' in bh_bh_report.columns else np.nan
    bh_bh_report['applyId'] = bh_bh_report['applyId'] if 'applyId' in bh_bh_report.columns else np.nan
    bh_bh_report['reportId'] = bh_bh_report['reportId'] if 'reportId' in bh_bh_report.columns else np.nan
    bh_bh_report['reportTime'] = bh_bh_report['reportTime'] if 'reportTime' in bh_bh_report.columns else np.nan
    bh_bh_report['name'] = bh_bh_report['name'] if 'name' in bh_bh_report.columns else np.nan
    bh_bh_report['idcardNo'] = bh_bh_report['idcardNo'] if 'idcardNo' in bh_bh_report.columns else np.nan
    bh_bh_report['mobile'] = bh_bh_report['mobile'] if 'mobile' in bh_bh_report.columns else np.nan
    bh_bh_report['queryResult'] = bh_bh_report['queryResult'] if 'queryResult' in bh_bh_report.columns else np.nan
    bh_bh_report['mobileCount'] = bh_bh_report['mobileCount'] if 'mobileCount' in bh_bh_report.columns else np.nan
    bh_bh_report['createTime'] = bh_bh_report['createTime'] if 'createTime' in bh_bh_report.columns else np.nan
    dict_in['bh_bh_report'] = bh_bh_report
    # print("bh_bh_report", bh_bh_report)
    try:
        dict_out["reportTime"] = dict_in['bh_bh_report']['reportTime'].values[0]
    except:
        dict_out["reportTime"] = np.nan

    # print(dict_out["reportTime"])

    # 居住信息
    bh_bh_report_customer_home = pd.DataFrame(baihang_info['bh_bh_report_customer_home']) if 'bh_bh_report_customer_home' in baihang_info.keys() else pd.DataFrame()
    bh_bh_report_customer_home['id'] = bh_bh_report_customer_home['id'] if 'id' in bh_bh_report_customer_home.columns else np.nan
    bh_bh_report_customer_home['reportId'] = bh_bh_report_customer_home['reportId'] if 'reportId' in bh_bh_report_customer_home.columns else np.nan
    bh_bh_report_customer_home['homeAddress'] = bh_bh_report_customer_home['homeAddress'] if 'homeAddress' in bh_bh_report_customer_home.columns else np.nan
    bh_bh_report_customer_home['date'] = bh_bh_report_customer_home['date'] if 'date' in bh_bh_report_customer_home.columns else np.nan
    dict_in['bh_bh_report_customer_home'] = bh_bh_report_customer_home
    # print("bh_bh_report_customer_home", bh_bh_report_customer_home)

    # 职业信息
    bh_bh_report_customer_work = pd.DataFrame(baihang_info['bh_bh_report_customer_work']) if 'bh_bh_report_customer_work' in baihang_info.keys() else pd.DataFrame()
    bh_bh_report_customer_work['id'] = bh_bh_report_customer_work['id'] if 'id' in bh_bh_report_customer_work.columns else np.nan
    bh_bh_report_customer_work['reportId'] = bh_bh_report_customer_work['reportId'] if 'reportId' in bh_bh_report_customer_work.columns else np.nan
    bh_bh_report_customer_work['workName'] = bh_bh_report_customer_work['workName'] if 'workName' in bh_bh_report_customer_work.columns else np.nan
    bh_bh_report_customer_work['workAddress'] = bh_bh_report_customer_work['workAddress'] if 'workAddress' in bh_bh_report_customer_work.columns else np.nan
    bh_bh_report_customer_work['date'] = bh_bh_report_customer_work['date'] if 'date' in bh_bh_report_customer_work.columns else np.nan
    dict_in['bh_bh_report_customer_work'] = bh_bh_report_customer_work

    # 单笔贷款信息
    bh_bh_report_loan_non_revolving = pd.DataFrame([baihang_info['bh_bh_report_loan_non_revolving']]) if 'bh_bh_report_loan_non_revolving' in baihang_info.keys() else pd.DataFrame()
    bh_bh_report_loan_non_revolving['id'] = bh_bh_report_loan_non_revolving['id'] if 'id' in bh_bh_report_loan_non_revolving.columns else np.nan
    bh_bh_report_loan_non_revolving['reportId'] = bh_bh_report_loan_non_revolving['reportId'] if 'reportId' in bh_bh_report_loan_non_revolving.columns else np.nan
    bh_bh_report_loan_non_revolving['loanCount'] = bh_bh_report_loan_non_revolving['loanCount'] if 'loanCount' in bh_bh_report_loan_non_revolving.columns else np.nan
    bh_bh_report_loan_non_revolving['openLoanCount'] = bh_bh_report_loan_non_revolving['openLoanCount'] if 'openLoanCount' in bh_bh_report_loan_non_revolving.columns else np.nan
    bh_bh_report_loan_non_revolving['remainingAmount'] = bh_bh_report_loan_non_revolving['remainingAmount'] if 'remainingAmount' in bh_bh_report_loan_non_revolving.columns else np.nan
    bh_bh_report_loan_non_revolving['remainingOverdueLoanCount'] = bh_bh_report_loan_non_revolving['remainingOverdueLoanCount'] if 'remainingOverdueLoanCount' in bh_bh_report_loan_non_revolving.columns else np.nan
    bh_bh_report_loan_non_revolving['remainingOverdueAmount'] = bh_bh_report_loan_non_revolving['remainingOverdueAmount'] if 'remainingOverdueAmount' in bh_bh_report_loan_non_revolving.columns else np.nan
    bh_bh_report_loan_non_revolving['remainingMaxOverdueStatus'] = bh_bh_report_loan_non_revolving['remainingMaxOverdueStatus'] if 'remainingMaxOverdueStatus' in bh_bh_report_loan_non_revolving.columns else np.nan
    bh_bh_report_loan_non_revolving['overdueCount'] = bh_bh_report_loan_non_revolving['overdueCount'] if 'overdueCount' in bh_bh_report_loan_non_revolving.columns else np.nan
    bh_bh_report_loan_non_revolving['maxOverdueStatus'] = bh_bh_report_loan_non_revolving['maxOverdueStatus'] if 'maxOverdueStatus' in bh_bh_report_loan_non_revolving.columns else np.nan
    bh_bh_report_loan_non_revolving['lastCompensationDate'] = bh_bh_report_loan_non_revolving['lastCompensationDate'] if 'lastCompensationDate' in bh_bh_report_loan_non_revolving.columns else np.nan
    dict_in['bh_bh_report_loan_non_revolving'] = bh_bh_report_loan_non_revolving




    # 近X天单笔贷款摘要
    bh_bh_report_loan_non_revolving_day_summary = pd.DataFrame(baihang_info['bh_bh_report_loan_non_revolving_day_summary']) if 'bh_bh_report_loan_non_revolving_day_summary' in baihang_info.keys() else pd.DataFrame()
    bh_bh_report_loan_non_revolving_day_summary['id'] = bh_bh_report_loan_non_revolving_day_summary['id'] if 'id' in bh_bh_report_loan_non_revolving_day_summary.columns else np.nan
    bh_bh_report_loan_non_revolving_day_summary['reportId'] = bh_bh_report_loan_non_revolving_day_summary['reportId'] if 'reportId' in bh_bh_report_loan_non_revolving_day_summary.columns else np.nan
    bh_bh_report_loan_non_revolving_day_summary['type'] = bh_bh_report_loan_non_revolving_day_summary['type'] if 'type' in bh_bh_report_loan_non_revolving_day_summary.columns else np.nan
    bh_bh_report_loan_non_revolving_day_summary['applyTenantCount'] = bh_bh_report_loan_non_revolving_day_summary['applyTenantCount'] if 'applyTenantCount' in bh_bh_report_loan_non_revolving_day_summary.columns else np.nan
    bh_bh_report_loan_non_revolving_day_summary['loanCount'] = bh_bh_report_loan_non_revolving_day_summary['loanCount'] if 'loanCount' in bh_bh_report_loan_non_revolving_day_summary.columns else np.nan
    bh_bh_report_loan_non_revolving_day_summary['loanAmount'] = bh_bh_report_loan_non_revolving_day_summary['loanAmount'] if 'loanAmount' in bh_bh_report_loan_non_revolving_day_summary.columns else np.nan
    bh_bh_report_loan_non_revolving_day_summary['loanTenantCount'] = bh_bh_report_loan_non_revolving_day_summary['loanTenantCount'] if 'loanTenantCount' in bh_bh_report_loan_non_revolving_day_summary.columns else np.nan
    bh_bh_report_loan_non_revolving_day_summary['maxLoanAmount'] = bh_bh_report_loan_non_revolving_day_summary['maxLoanAmount'] if 'maxLoanAmount' in bh_bh_report_loan_non_revolving_day_summary.columns else np.nan
    bh_bh_report_loan_non_revolving_day_summary['averageLoanAmount'] = bh_bh_report_loan_non_revolving_day_summary['averageLoanAmount'] if 'averageLoanAmount' in bh_bh_report_loan_non_revolving_day_summary.columns else np.nan
    bh_bh_report_loan_non_revolving_day_summary['overdueLoanCount'] = bh_bh_report_loan_non_revolving_day_summary['overdueLoanCount'] if 'overdueLoanCount' in bh_bh_report_loan_non_revolving_day_summary.columns else np.nan
    bh_bh_report_loan_non_revolving_day_summary['compensationCount'] = bh_bh_report_loan_non_revolving_day_summary['compensationCount'] if 'compensationCount' in bh_bh_report_loan_non_revolving_day_summary.columns else np.nan
    bh_bh_report_loan_non_revolving_day_summary['compensationTimes'] = bh_bh_report_loan_non_revolving_day_summary['compensationTimes'] if 'compensationTimes' in bh_bh_report_loan_non_revolving_day_summary.columns else np.nan
    bh_bh_report_loan_non_revolving_day_summary['compensationAmount'] = bh_bh_report_loan_non_revolving_day_summary['compensationAmount'] if 'compensationAmount' in bh_bh_report_loan_non_revolving_day_summary.columns else np.nan
    dict_in['bh_bh_report_loan_non_revolving_day_summary'] = bh_bh_report_loan_non_revolving_day_summary

    # 循环授信信息
    bh_bh_report_loan_revolving = pd.DataFrame([baihang_info['bh_bh_report_loan_revolving']]) if 'bh_bh_report_loan_revolving' in baihang_info.keys() else pd.DataFrame()
    bh_bh_report_loan_revolving['id'] = bh_bh_report_loan_revolving['id'] if 'id' in bh_bh_report_loan_revolving.columns else np.nan
    bh_bh_report_loan_revolving['reportId'] = bh_bh_report_loan_revolving['reportId'] if 'reportId' in bh_bh_report_loan_revolving.columns else np.nan
    bh_bh_report_loan_revolving['accountCount'] = bh_bh_report_loan_revolving['accountCount'] if 'accountCount' in bh_bh_report_loan_revolving.columns else np.nan
    bh_bh_report_loan_revolving['validAccountCount'] = bh_bh_report_loan_revolving['validAccountCount'] if 'validAccountCount' in bh_bh_report_loan_revolving.columns else np.nan
    bh_bh_report_loan_revolving['creditLimitSum'] = bh_bh_report_loan_revolving['creditLimitSum'] if 'creditLimitSum' in bh_bh_report_loan_revolving.columns else np.nan
    bh_bh_report_loan_revolving['maxCreditLimitPerTenant'] = bh_bh_report_loan_revolving['maxCreditLimitPerTenant'] if 'maxCreditLimitPerTenant' in bh_bh_report_loan_revolving.columns else np.nan
    bh_bh_report_loan_revolving['remainingAmount'] = bh_bh_report_loan_revolving['remainingAmount'] if 'remainingAmount' in bh_bh_report_loan_revolving.columns else np.nan
    bh_bh_report_loan_revolving['remainingOverdueAccountCount'] = bh_bh_report_loan_revolving['remainingOverdueAccountCount'] if 'remainingOverdueAccountCount' in bh_bh_report_loan_revolving.columns else np.nan
    bh_bh_report_loan_revolving['remainingOverdueAmount'] = bh_bh_report_loan_revolving['remainingOverdueAmount'] if 'remainingOverdueAmount' in bh_bh_report_loan_revolving.columns else np.nan
    bh_bh_report_loan_revolving['remainingMaxOverdueStatus'] = bh_bh_report_loan_revolving['remainingMaxOverdueStatus'] if 'remainingMaxOverdueStatus' in bh_bh_report_loan_revolving.columns else np.nan
    bh_bh_report_loan_revolving['overdueCount'] = bh_bh_report_loan_revolving['overdueCount'] if 'overdueCount' in bh_bh_report_loan_revolving.columns else np.nan
    bh_bh_report_loan_revolving['maxOverdueStatus'] = bh_bh_report_loan_revolving['maxOverdueStatus'] if 'maxOverdueStatus' in bh_bh_report_loan_revolving.columns else np.nan
    bh_bh_report_loan_revolving['revolvingLastCompensationDate'] = bh_bh_report_loan_revolving['revolvingLastCompensationDate'] if 'revolvingLastCompensationDate' in bh_bh_report_loan_revolving.columns else np.nan
    dict_in['bh_bh_report_loan_revolving'] = bh_bh_report_loan_revolving


    # 近X天循环授信账户摘要
    bh_bh_report_loan_revolving_day_summary = pd.DataFrame(baihang_info['bh_bh_report_loan_revolving_day_summary']) if 'bh_bh_report_loan_revolving_day_summary' in baihang_info.keys() else pd.DataFrame()
    bh_bh_report_loan_revolving_day_summary['id'] = bh_bh_report_loan_revolving_day_summary['id'] if 'id' in bh_bh_report_loan_revolving_day_summary.columns else np.nan
    bh_bh_report_loan_revolving_day_summary['reportId'] = bh_bh_report_loan_revolving_day_summary['reportId'] if 'reportId' in bh_bh_report_loan_revolving_day_summary.columns else np.nan
    bh_bh_report_loan_revolving_day_summary['type'] = bh_bh_report_loan_revolving_day_summary['type'] if 'type' in bh_bh_report_loan_revolving_day_summary.columns else np.nan
    bh_bh_report_loan_revolving_day_summary['applyTenantCount'] = bh_bh_report_loan_revolving_day_summary['applyTenantCount'] if 'applyTenantCount' in bh_bh_report_loan_revolving_day_summary.columns else np.nan
    bh_bh_report_loan_revolving_day_summary['accountCount'] = bh_bh_report_loan_revolving_day_summary['accountCount'] if 'accountCount' in bh_bh_report_loan_revolving_day_summary.columns else np.nan
    bh_bh_report_loan_revolving_day_summary['creditLimitSum'] = bh_bh_report_loan_revolving_day_summary['creditLimitSum'] if 'creditLimitSum' in bh_bh_report_loan_revolving_day_summary.columns else np.nan
    bh_bh_report_loan_revolving_day_summary['lendingAmount'] = bh_bh_report_loan_revolving_day_summary['lendingAmount'] if 'lendingAmount' in bh_bh_report_loan_revolving_day_summary.columns else np.nan
    bh_bh_report_loan_revolving_day_summary['overdueAccountCount'] = bh_bh_report_loan_revolving_day_summary['overdueAccountCount'] if 'overdueAccountCount' in bh_bh_report_loan_revolving_day_summary.columns else np.nan
    bh_bh_report_loan_revolving_day_summary['revolvingCompensationCount'] = bh_bh_report_loan_revolving_day_summary['revolvingCompensationCount'] if 'revolvingCompensationCount' in bh_bh_report_loan_revolving_day_summary.columns else np.nan
    bh_bh_report_loan_revolving_day_summary['revolvingCompensationTimes'] = bh_bh_report_loan_revolving_day_summary['revolvingCompensationTimes'] if 'revolvingCompensationTimes' in bh_bh_report_loan_revolving_day_summary.columns else np.nan
    bh_bh_report_loan_revolving_day_summary['revolvingCompensationAmount'] = bh_bh_report_loan_revolving_day_summary['revolvingCompensationAmount'] if 'revolvingCompensationAmount' in bh_bh_report_loan_revolving_day_summary.columns else np.nan
    dict_in['bh_bh_report_loan_revolving_day_summary'] = bh_bh_report_loan_revolving_day_summary

    bh_bh_report_query_history = pd.DataFrame(baihang_info['bh_bh_report_query_history']) if 'bh_bh_report_query_history' in baihang_info.keys() else pd.DataFrame()
    bh_bh_report_query_history['id'] = bh_bh_report_query_history['id'] if 'id' in bh_bh_report_query_history.columns else np.nan
    bh_bh_report_query_history['reportId'] = bh_bh_report_query_history['reportId'] if 'reportId' in bh_bh_report_query_history.columns else np.nan
    bh_bh_report_query_history['tenantType'] = bh_bh_report_query_history['tenantType'] if 'tenantType' in bh_bh_report_query_history.columns else np.nan
    bh_bh_report_query_history['tenantName'] = bh_bh_report_query_history['tenantName'] if 'tenantName' in bh_bh_report_query_history.columns else np.nan
    bh_bh_report_query_history['userId'] = bh_bh_report_query_history['userId'] if 'userId' in bh_bh_report_query_history.columns else np.nan
    bh_bh_report_query_history['reason'] = bh_bh_report_query_history['reason'] if 'reason' in bh_bh_report_query_history.columns else np.nan
    bh_bh_report_query_history['date'] = bh_bh_report_query_history['date'] if 'date' in bh_bh_report_query_history.columns else np.nan
    dict_in['bh_bh_report_query_history'] = bh_bh_report_query_history




    return dict_in, dict_out


def address(address):
    address="".join(re.findall('[a-zA-Z0-9\u4e00-\u9fa5]+',str(address)))
    url="http://restapi.amap.com/v3/geocode/geo?key=%s&address=%s"%('5ba9f811bdfa620cb8946cc833075155',address)
    data=requests.get(url).json()
    if data['info']=='OK' and len(data['geocodes'])>0:
        contest=data['geocodes'][0]
        for key,value in contest.items():
            if value==[]:
                contest[key]=''
    else:
        contest={'formatted_address': '',
   'country': '',
   'province': '',
   'citycode': '',
   'city': '',
   'district': '',
   'township': '',
   'neighborhood': {'name': '', 'type': ''},
   'building': {'name': '', 'type': ''},
   'adcode': '',
   'street': '',
   'number': '',
   'location': '',
   'level': ''}
    return contest


# 获取居住地址省份个数
def address_pro_count(dict_in):
    pro=[]
    for row in dict_in:
        addr=address(row)
        if addr['province'] != '':
            pro.append(addr['province'])
    pro = list(set(pro))
    return len(pro)


def one_hot_trans_del_report(df, col_list):
    df = df.reset_index(drop=True)
    for col in col_list:
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


def one_hot_trans_report(df, col_list):
    df = df.reset_index(drop=True)
    for col in col_list:
        data = df[col]
        new_columns = []
        label = LabelEncoder()
        oneHot = OneHotEncoder()
        la_data = label.fit_transform(data).reshape(-1, 1)
        for cla in label.classes_:
            new_columns.append(col+'_'+str(cla))
        one_data = oneHot.fit_transform(la_data).toarray()
        enc_df = pd.DataFrame(one_data , columns=new_columns)
#         del df[col]
        df = pd.concat([df, enc_df], axis=1)
    return df


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


def get_queryHistory_feature(bhInfo, col, bh_report_overview_feature):
    bh_bh_report_customer_queryHistory = deal_json_to_df(bhInfo, col)

    bh_credit_report_queryHistory = bh_bh_report_customer_queryHistory.drop(['id','reportId', 'tenantType', 'tenantName', 'userId'], axis=1, inplace=False)

    bh_credit_report_queryHistory['reportTime'] = list(bh_report_overview_feature['reportTime'])[0]

    bh_credit_report_queryHistory['reportTime'] = pd.to_datetime(bh_credit_report_queryHistory['reportTime'])

    bh_credit_report_queryHistory['date'] = pd.to_datetime(bh_credit_report_queryHistory['date'])

    bh_credit_report_queryHistory['queryHistory_diff_date'] = (bh_credit_report_queryHistory['reportTime'] - bh_credit_report_queryHistory['date']).map(lambda x: x.days)

    bh_credit_report_queryHistory = bh_credit_report_queryHistory.rename(columns={'reason': 'queryHistory_reason'}, inplace=False)

    bh_credit_report_queryHistory = one_hot_trans_report(bh_credit_report_queryHistory, ['queryHistory_reason'])

    bh_credit_report_queryHistory['apply_id'] = str(11111)


    df_new = pd.DataFrame([{'apply_id': str(11111)}])
    diff_date_list = [7, 14, 30, 60, 90, 180, 360, 3600]
    for diff_date in diff_date_list:
        bh_credit_report_queryHistory_one = bh_credit_report_queryHistory[bh_credit_report_queryHistory['queryHistory_diff_date']<=diff_date]
        if bh_credit_report_queryHistory_one.shape[0]>0:

            bh_credit_report_queryHistory_feature = bh_credit_report_queryHistory_one.groupby(['apply_id'])[ 'queryHistory_reason'].agg({'size', 'nunique'}).reset_index()

            bh_credit_report_queryHistory_feature = bh_credit_report_queryHistory_feature.rename(columns={'size': 'queryHistory_reason_size_'+str(diff_date)+'_days', 'nunique': 'queryHistory_reason_nunique_'+str(diff_date)+'_days'}, inplace=False)

            feature_name_list = ['queryHistory_reason_1',
                                 'queryHistory_reason_2',
                                 'queryHistory_reason_3',
                                 'queryHistory_reason_6']

            for feature_name in feature_name_list:
                if feature_name in list(bh_credit_report_queryHistory_one.columns):
                    temp_df = bh_credit_report_queryHistory_one.groupby(['apply_id'])[feature_name].agg({'size', 'nunique'}).reset_index()

                    temp_df = temp_df.rename(columns={'size': feature_name + '_size_'+str(diff_date)+'_days', 'nunique': feature_name + '_nunique_'+str(diff_date)+'_days'}, inplace=False)

                    df_new = pd.merge(df_new, temp_df, on='apply_id', how='left')
                    df_new[feature_name + '_size_pct_'+str(diff_date)+'_days'] = df_new[feature_name + '_size_'+str(diff_date)+'_days'] / bh_credit_report_queryHistory_feature['queryHistory_reason_size_'+str(diff_date)+'_days']
                    df_new[feature_name + '_nunique_pct_'+str(diff_date)+'_days'] = df_new[feature_name + '_nunique_'+str(diff_date)+'_days'] / bh_credit_report_queryHistory_feature['queryHistory_reason_nunique_'+str(diff_date)+'_days']

    return df_new


def get_bh_report_overview(bhInfo, col):

    bh_bh_report = deal_json_to_df(bhInfo, col)

    bh_bh_report = bh_bh_report[['reportTime', 'queryResult', 'mobileCount']]
    bh_bh_report['apply_id'] = str(11111)
    return bh_bh_report


def get_features(dict_in, dict_out, bhInfo):
    result = {}
    # 初始化
    result = initial_feature(result)
    # 报告返回时间
    result['reportTime'] = str(dict_out['reportTime'])

    try:
        # 返回结果
        result['queryResult'] = str(dict_in['bh_bh_report']['queryResult'][0])

        # 手机号数量
        result['mobileCount'] = dict_in['bh_bh_report']['mobileCount'][0]

        # 客户身份证号
        result['pid'] = dict_in['bh_bh_report']['idcardNo'][0]
    except:
        result['queryResult'] =np.nan
        result['mobileCount'] = np.nan
        result['pid']=np.nan

    if dict_in['bh_bh_report_customer_home'].empty:

        result['provice_homeAddress'] = None
        result['provice_homeAddress_6m'] = None
        result['provice_homeAddress_12m'] = None
        result['latest_homeAddress'] = None

    else:
        if dict_in['bh_bh_report_customer_home']["homeAddress"].values[0] == '':
            result['provice_homeAddress'] = None
            result['provice_homeAddress_6m'] = None
            result['provice_homeAddress_12m'] = None
            result['latest_homeAddress'] = None
        else:
            homeAddress = dict_in['bh_bh_report_customer_home'][
                dict_in['bh_bh_report_customer_home']['homeAddress'] != ''].reset_index(drop=True)

            # 居住地址省份个数
            result['provice_homeAddress'] = address_pro_count(homeAddress['homeAddress'])

            # 近6个月内居住地址省份个数
            reportTime = datetime.datetime.strptime(str(dict_out['reportTime'])[:10], '%Y-%m-%d')
            reportTime6 = reportTime - relativedelta(months=6)
            reportTime6 = datetime.datetime.strftime(reportTime6, '%Y.%m.%d')
            result['provice_homeAddress_6m'] = address_pro_count(homeAddress['homeAddress'][
                                                                     (homeAddress['date'] > reportTime6) & (
                                                                                 homeAddress['date'] < str(
                                                                             dict_out['reportTime'])[:10])])

            # 近12个月内居住地址省份个数
            reportTime = datetime.datetime.strptime(str(dict_out['reportTime'])[:10], '%Y-%m-%d')
            reportTime12 = reportTime - relativedelta(months=12)
            reportTime12 = datetime.datetime.strftime(reportTime12, '%Y.%m.%d')
            result['provice_homeAddress_12m'] = address_pro_count(homeAddress['homeAddress'][
                                                                      (homeAddress['date'] > reportTime12) & (
                                                                                  homeAddress['date'] < str(
                                                                              dict_out['reportTime'])[:10])])

            # 最近居住省份
            add = address(homeAddress['homeAddress'][homeAddress['date'] == homeAddress['date'].max()].reset_index(drop=True)[0])['province']
            if (add == '') | (add == 'None') | (add == 'none'):
                result['latest_homeAddress'] = None
            else:
                result['latest_homeAddress'] = add


    if dict_in['bh_bh_report_customer_work'].empty:


        result['provice_workAddress'] = None
        result['provice_workAddress_6m'] = None
        result['provice_workAddress_12m'] = None
        result['latest_workAddress'] = None
    else:
        if dict_in['bh_bh_report_customer_work']["workAddress"].values[0] == '':
            result['provice_workAddress'] = None
            result['provice_workAddress_6m'] = None
            result['provice_workAddress_12m'] = None
            result['latest_workAddress'] = None
        else:
            workAddress = dict_in['bh_bh_report_customer_work'][
                dict_in['bh_bh_report_customer_work']['workAddress'] != ''].reset_index(drop=True)


            # 工作地址省份个数
            result['provice_workAddress'] = address_pro_count(dict_in['bh_bh_report_customer_work']['workAddress'])

            # 近12个月内工作地址省份个数
            reportTime = datetime.datetime.strptime(str(dict_out['reportTime'])[:10], '%Y-%m-%d')
            reportTime6 = reportTime - relativedelta(months=6)
            reportTime6 = datetime.datetime.strftime(reportTime6, '%Y.%m.%d')
            result['provice_workAddress_6m'] = address_pro_count(dict_in['bh_bh_report_customer_work']['workAddress'][(
                                                                                                                                  dict_in[
                                                                                                                                      'bh_bh_report_customer_work'][
                                                                                                                                      'date'] > reportTime6) & (
                                                                                                                                  dict_in[
                                                                                                                                      'bh_bh_report_customer_work'][
                                                                                                                                      'date'] < str(
                                                                                                                              dict_out[
                                                                                                                                  'reportTime'])[
                                                                                                                                                :10])])

            # 近12个月内工作地址省份个数
            reportTime = datetime.datetime.strptime(str(dict_out['reportTime'])[:10], '%Y-%m-%d')
            reportTime12 = reportTime - relativedelta(months=12)
            reportTime12 = datetime.datetime.strftime(reportTime12, '%Y.%m.%d')
            result['provice_workAddress_12m'] = address_pro_count(dict_in['bh_bh_report_customer_work']['workAddress'][(
                                                                                                                                   dict_in[
                                                                                                                                       'bh_bh_report_customer_work'][
                                                                                                                                       'date'] > reportTime12) & (
                                                                                                                                   dict_in[
                                                                                                                                       'bh_bh_report_customer_work'][
                                                                                                                                       'date'] < str(
                                                                                                                               dict_out[
                                                                                                                                   'reportTime'])[
                                                                                                                                                 :10])])

            # 最近工作省份
            add = address(workAddress['workAddress'][workAddress['date'] == workAddress['date'].max()].reset_index(drop=True)[0])['province']
            if (add == '') | (add == 'None') | (add == 'none'):
                result['latest_workAddress'] = None
            else:
                result['latest_workAddress'] = add



    # 省份一致性判断
    result['Address_consistence'] = 1 if result['latest_workAddress'] == result['latest_homeAddress'] else 0

    if len(dict_in['bh_bh_report_loan_non_revolving']) > 0:
        # 累计贷款笔数
        result['NE_loanCount'] = dict_in['bh_bh_report_loan_non_revolving']['loanCount'][0]

        # 历史最严重逾期状态（过去 3 年）
        result['NE_maxOverdueStatus'] = dict_in['bh_bh_report_loan_non_revolving']['maxOverdueStatus'][0]

        # 未结清贷款笔数
        result['NE_openLoanCount'] = dict_in['bh_bh_report_loan_non_revolving']['openLoanCount'][0]

        # 累计逾期次数
        result['NE_overdueCount'] = dict_in['bh_bh_report_loan_non_revolving']['overdueCount'][0]

        # 未结清总余额
        result['NE_remainingAmount'] = dict_in['bh_bh_report_loan_non_revolving']['remainingAmount'][0]

        # 当前最严重逾期状态
        result['NE_remainingMaxOverdueStatus'] = \
        dict_in['bh_bh_report_loan_non_revolving']['remainingMaxOverdueStatus'][0]

        # 当前逾期金额
        result['NE_remainingOverdueAmount'] = dict_in['bh_bh_report_loan_non_revolving']['remainingOverdueAmount'][0]

        # 当前逾期贷款笔数
        result['NE_remainingOverdueLoanCount'] = \
        dict_in['bh_bh_report_loan_non_revolving']['remainingOverdueLoanCount'][0]

    else:
        result['NE_loanCount'] = 0
        result['NE_maxOverdueStatus'] = None
        result['NE_openLoanCount'] = 0
        result['NE_overdueCount'] = 0
        result['NE_remainingAmount'] = 0
        result['NE_remainingMaxOverdueStatus'] = None
        result['NE_remainingOverdueAmount'] = 0
        result['NE_remainingOverdueLoanCount'] = 0


    loan_summary = dict_in['bh_bh_report_loan_non_revolving_day_summary'].rename(index={1: 0, 2: 0, 3: 0, 4: 0})
    # D30
    if len(loan_summary[loan_summary['type'] == 1]) > 0:
        # 申请机构数量
        result['NR_D30_applyTenantCount'] = loan_summary['applyTenantCount'][loan_summary['type'] == 1][0]

        # 新增贷款笔数
        result['NR_D30_loanCount'] = loan_summary['loanCount'][loan_summary['type'] == 1][0]

        # 新增贷款金额
        result['NR_D30_loanAmount'] = loan_summary['loanAmount'][loan_summary['type'] == 1][0]

        # 新增贷款机构数
        result['NR_D30_loanTenantCount'] = loan_summary['loanTenantCount'][loan_summary['type'] == 1][0]

        # 最大贷款金额
        result['NR_D30_maxLoanAmount'] = loan_summary['maxLoanAmount'][loan_summary['type'] == 1][0]

        # 平均贷款金额
        result['NR_D30_averageLoanAmount'] = loan_summary['averageLoanAmount'][loan_summary['type'] == 1][0]

        # 发生逾期贷款笔数
        result['NR_D30_overdueLoanCount'] = loan_summary['overdueLoanCount'][loan_summary['type'] == 1][0]




    else:
        result['NR_D30_applyTenantCount'] = 0
        result['NR_D30_loanCount'] = 0
        result['NR_D30_loanAmount'] = 0.0
        result['NR_D30_loanTenantCount'] = 0
        result['NR_D30_maxLoanAmount'] = 0.0
        result['NR_D30_averageLoanAmount'] = 0.0
        result['NR_D30_overdueLoanCount'] = 0


        # D90
    # print(loan_summary['applyTenantCount'])
    # print(loan_summary['type'])
    # print(loan_summary)
    # print(loan_summary['applyTenantCount'][loan_summary['type'] == 2])
    if len(loan_summary[loan_summary['type'] == 2]) > 0:
        # 申请机构数量
        result['NR_D90_applyTenantCount'] = loan_summary['applyTenantCount'][loan_summary['type'] == 2][0]

        # 新增贷款笔数
        result['NR_D90_loanCount'] = loan_summary['loanCount'][loan_summary['type'] == 2][0]

        # 新增贷款金额
        result['NR_D90_loanAmount'] = loan_summary['loanAmount'][loan_summary['type'] == 2][0]

        # 新增贷款机构数
        result['NR_D90_loanTenantCount'] = loan_summary['loanTenantCount'][loan_summary['type'] == 2][0]

        # 最大贷款金额
        result['NR_D90_maxLoanAmount'] = loan_summary['maxLoanAmount'][loan_summary['type'] == 2][0]

        # 平均贷款金额
        result['NR_D90_averageLoanAmount'] = loan_summary['averageLoanAmount'][loan_summary['type'] == 2][0]

        # 发生逾期贷款笔数
        result['NR_D90_overdueLoanCount'] = loan_summary['overdueLoanCount'][loan_summary['type'] == 2][0]

    else:
        result['NR_D90_applyTenantCount'] = 0
        result['NR_D90_loanCount'] = 0
        result['NR_D90_loanAmount'] = 0.0
        result['NR_D90_loanTenantCount'] = 0
        result['NR_D90_maxLoanAmount'] = 0.0
        result['NR_D90_averageLoanAmount'] = 0.0
        result['NR_D90_overdueLoanCount'] = 0

    # D180
    if len(loan_summary[loan_summary['type'] == 3]) > 0:
        # 申请机构数量
        result['NR_D180_applyTenantCount'] = loan_summary['applyTenantCount'][loan_summary['type'] == 3][0]

        # 新增贷款笔数
        result['NR_D180_loanCount'] = loan_summary['loanCount'][loan_summary['type'] == 3][0]

        # 新增贷款金额
        result['NR_D180_loanAmount'] = loan_summary['loanAmount'][loan_summary['type'] == 3][0]

        # 新增贷款机构数
        result['NR_D180_loanTenantCount'] = loan_summary['loanTenantCount'][loan_summary['type'] == 3][0]

        # 最大贷款金额
        result['NR_D180_maxLoanAmount'] = loan_summary['maxLoanAmount'][loan_summary['type'] == 3][0]

        # 平均贷款金额
        result['NR_D180_averageLoanAmount'] = loan_summary['averageLoanAmount'][loan_summary['type'] == 3][0]

        # 发生逾期贷款笔数
        result['NR_D180_overdueLoanCount'] = loan_summary['overdueLoanCount'][loan_summary['type'] == 3][0]

    else:
        result['NR_D180_applyTenantCount'] = 0
        result['NR_D180_loanCount'] = 0
        result['NR_D180_loanAmount'] = 0.0
        result['NR_D180_loanTenantCount'] = 0
        result['NR_D180_maxLoanAmount'] = 0.0
        result['NR_D180_averageLoanAmount'] = 0.0
        result['NR_D180_overdueLoanCount'] = 0

    # D360
    if len(loan_summary[loan_summary['type'] == 4]) > 0:
        # 申请机构数量
        result['NR_D360_applyTenantCount'] = loan_summary['applyTenantCount'][loan_summary['type'] == 4][0]

        # 新增贷款笔数
        result['NR_D360_loanCount'] = loan_summary['loanCount'][loan_summary['type'] == 4][0]

        # 新增贷款金额
        result['NR_D360_loanAmount'] = loan_summary['loanAmount'][loan_summary['type'] == 4][0]

        # 新增贷款机构数
        result['NR_D360_loanTenantCount'] = loan_summary['loanTenantCount'][loan_summary['type'] == 4][0]

        # 最大贷款金额
        result['NR_D360_maxLoanAmount'] = loan_summary['maxLoanAmount'][loan_summary['type'] == 4][0]

        # 平均贷款金额
        result['NR_D360_averageLoanAmount'] = loan_summary['averageLoanAmount'][loan_summary['type'] == 4][0]

        # 发生逾期贷款笔数
        result['NR_D360_overdueLoanCount'] = loan_summary['overdueLoanCount'][loan_summary['type'] == 4][0]

    else:
        result['NR_D360_applyTenantCount'] = 0
        result['NR_D360_loanCount'] = 0
        result['NR_D360_loanAmount'] = 0.0
        result['NR_D360_loanTenantCount'] = 0
        result['NR_D360_maxLoanAmount'] = 0.0
        result['NR_D360_averageLoanAmount'] = 0.0
        result['NR_D360_overdueLoanCount'] = 0

    if len(dict_in['bh_bh_report_loan_revolving']) > 0:
        # 累计循环授信账户数
        result['CR_accountCount'] = dict_in['bh_bh_report_loan_revolving']['accountCount'][0]

        # 有效循环授信账户数
        result['CR_validAccountCount'] = dict_in['bh_bh_report_loan_revolving']['validAccountCount'][0]

        # 当前授信总额
        result['CR_creditLimitSum'] = dict_in['bh_bh_report_loan_revolving']['creditLimitSum'][0]

        # 单个机构最高授信额度
        result['CR_maxCreditLimitPerTenant'] = dict_in['bh_bh_report_loan_revolving']['maxCreditLimitPerTenant'][0]

        # 历史最严重逾期状态
        result['CR_maxOverdueStatus'] = dict_in['bh_bh_report_loan_revolving']['maxOverdueStatus'][0]

        # 累计逾期次数
        result['CR_overdueCount'] = dict_in['bh_bh_report_loan_revolving']['overdueCount'][0]

        # 未结清总余额
        result['CR_remainingAmount'] = dict_in['bh_bh_report_loan_revolving']['remainingAmount'][0]

        # 当前最严重逾期状态
        result['CR_remainingMaxOverdueStatus'] = dict_in['bh_bh_report_loan_revolving']['remainingMaxOverdueStatus'][0]

        # 当前逾期循环授信账户数
        result['CR_remainingOverdueAccountCount'] = \
        dict_in['bh_bh_report_loan_revolving']['remainingOverdueAccountCount'][0]

        # 当前逾期金额
        result['CR_remainingOverdueAmount'] = dict_in['bh_bh_report_loan_revolving']['remainingOverdueAmount'][0]

    else:
        result['CR_accountCount'] = 0
        result['CR_validAccountCount'] = 0
        result['CR_creditLimitSum'] = 0.0
        result['CR_maxCreditLimitPerTenant'] = 0.0
        result['CR_maxOverdueStatus'] = None
        result['CR_overdueCount'] = 0
        result['CR_remainingAmount'] = 0.0
        result['CR_remainingMaxOverdueStatus'] = None
        result['CR_remainingOverdueAccountCount'] = 0
        result['CR_remainingOverdueAmount'] = 0.0

    loan_revolving_summary = dict_in['bh_bh_report_loan_revolving_day_summary'].rename(index={1: 0, 2: 0, 3: 0, 4: 0})
    # D30
    if len(loan_revolving_summary[loan_revolving_summary['type'] == 1]) > 0:
        # 申请机构数量
        result['CR_D30_applyTenantCount'] = loan_revolving_summary['applyTenantCount'][loan_revolving_summary['type'] == 1][0]

        # 新增循环授信账户数
        result['CR_D30_accountCount'] = loan_revolving_summary['accountCount'][loan_revolving_summary['type'] == 1][0]

        # 新增循环授信额度
        result['CR_D30_creditLimitSum'] = loan_revolving_summary['creditLimitSum'][loan_revolving_summary['type'] == 1][0]

        # 新增借款金额
        result['CR_D30_lendingAmount'] = loan_revolving_summary['lendingAmount'][loan_revolving_summary['type'] == 1][0]

        # 发生逾期账户数
        result['CR_D30_overdueAccountCount'] = loan_revolving_summary['overdueAccountCount'][loan_revolving_summary['type'] == 1][0]

        # 发生代偿笔数
        result['CR_D30_revolvingCompensationCount'] = loan_revolving_summary['revolvingCompensationCount'][loan_revolving_summary['type'] == 1][0]

        # 发生代偿次数
        result['CR_D30_revolvingCompensationTimes'] = loan_revolving_summary['revolvingCompensationTimes'][loan_revolving_summary['type'] == 1][0]

        # 发生代偿总金额
        result['CR_D30_revolvingCompensationAmount'] = loan_revolving_summary['revolvingCompensationAmount'][loan_revolving_summary['type'] == 1][0]

    else:
        result['CR_D30_validAccountCount'] = 0
        result['CR_D30_accountCount'] = 0
        result['CR_D30_creditLimitSum'] = 0.0
        result['CR_D30_lendingAmount'] = 0.0
        result['CR_D30_overdueAccountCount'] = 0
        result['CR_D30_revolvingCompensationCount'] = 0
        result['CR_D30_revolvingCompensationTimes'] = 0
        result['CR_D30_revolvingCompensationAmount'] = 0.0

    # D90
    if len(loan_revolving_summary[loan_revolving_summary['type'] == 2]) > 0:
        # 申请机构数量
        result['CR_D90_applyTenantCount'] = loan_revolving_summary['applyTenantCount'][loan_revolving_summary['type'] == 2][0]

        # 新增循环授信账户数
        result['CR_D90_accountCount'] = loan_revolving_summary['accountCount'][loan_revolving_summary['type'] == 2][0]

        # 新增循环授信额度
        result['CR_D90_creditLimitSum'] = loan_revolving_summary['creditLimitSum'][loan_revolving_summary['type'] == 2][0]

        # 新增借款金额
        result['CR_D90_lendingAmount'] = loan_revolving_summary['lendingAmount'][loan_revolving_summary['type'] == 2][0]

        # 发生逾期账户数
        result['CR_D90_overdueAccountCount'] = loan_revolving_summary['overdueAccountCount'][loan_revolving_summary['type'] == 2][0]

        # 发生代偿笔数
        result['CR_D90_revolvingCompensationCount'] = loan_revolving_summary['revolvingCompensationCount'][loan_revolving_summary['type'] == 2][0]

        # 发生代偿次数
        result['CR_D90_revolvingCompensationTimes'] = loan_revolving_summary['revolvingCompensationTimes'][loan_revolving_summary['type'] == 2][0]

        # 发生代偿总金额
        result['CR_D90_revolvingCompensationAmount'] = loan_revolving_summary['revolvingCompensationAmount'][loan_revolving_summary['type'] == 2][0]

    else:
        result['CR_D90_validAccountCount'] = 0
        result['CR_D90_accountCount'] = 0
        result['CR_D90_creditLimitSum'] = 0.0
        result['CR_D90_lendingAmount'] = 0.0
        result['CR_D90_overdueAccountCount'] = 0
        result['CR_D90_revolvingCompensationCount'] = 0
        result['CR_D90_revolvingCompensationTimes'] = 0
        result['CR_D90_revolvingCompensationAmount'] = 0.0

    # D180
    if len(loan_revolving_summary[loan_revolving_summary['type'] == 3]) > 0:
        # 申请机构数量
        result['CR_D180_applyTenantCount'] = loan_revolving_summary['applyTenantCount'][loan_revolving_summary['type'] == 3][0]

        # 新增循环授信账户数
        result['CR_D180_accountCount'] = loan_revolving_summary['accountCount'][loan_revolving_summary['type'] == 3][0]

        # 新增循环授信额度
        result['CR_D180_creditLimitSum'] = loan_revolving_summary['creditLimitSum'][loan_revolving_summary['type'] == 3][0]

        # 新增借款金额
        result['CR_D180_lendingAmount'] = loan_revolving_summary['lendingAmount'][loan_revolving_summary['type'] == 3][0]

        # 发生逾期账户数
        result['CR_D180_overdueAccountCount'] = loan_revolving_summary['overdueAccountCount'][loan_revolving_summary['type'] == 3][0]

        # 发生代偿笔数
        result['CR_D180_revolvingCompensationCount'] = loan_revolving_summary['revolvingCompensationCount'][loan_revolving_summary['type'] == 3][0]

        # 发生代偿次数
        result['CR_D180_revolvingCompensationTimes'] = loan_revolving_summary['revolvingCompensationTimes'][loan_revolving_summary['type'] == 3][0]

        # 发生代偿总金额
        result['CR_D180_revolvingCompensationAmount'] = loan_revolving_summary['revolvingCompensationAmount'][loan_revolving_summary['type'] == 3][0]

    else:
        result['CR_D180_applyTenantCount'] = 0
        result['CR_D180_validAccountCount'] = 0
        result['CR_D180_accountCount'] = 0
        result['CR_D180_creditLimitSum'] = 0.0
        result['CR_D180_lendingAmount'] = 0.0
        result['CR_D180_overdueAccountCount'] = 0
        result['CR_D180_revolvingCompensationCount'] = 0
        result['CR_D180_revolvingCompensationTimes'] = 0
        result['CR_D180_revolvingCompensationAmount'] = 0.0

    # D360
    if len(loan_revolving_summary[loan_revolving_summary['type'] == 4]) > 0:
        # 申请机构数量
        result['CR_D360_applyTenantCount'] = loan_revolving_summary['applyTenantCount'][loan_revolving_summary['type'] == 4][0]

        # 新增循环授信账户数
        result['CR_D360_accountCount'] = loan_revolving_summary['accountCount'][loan_revolving_summary['type'] == 4][0]

        # 新增循环授信额度
        result['CR_D360_creditLimitSum'] = loan_revolving_summary['creditLimitSum'][loan_revolving_summary['type'] == 4][0]

        # 新增借款金额
        result['CR_D360_lendingAmount'] = loan_revolving_summary['lendingAmount'][loan_revolving_summary['type'] == 4][0]

        # 发生逾期账户数
        result['CR_D360_overdueAccountCount'] = loan_revolving_summary['overdueAccountCount'][loan_revolving_summary['type'] == 4][0]

        # 发生代偿笔数
        result['CR_D360_revolvingCompensationCount'] = loan_revolving_summary['revolvingCompensationCount'][loan_revolving_summary['type'] == 4][0]

        # 发生代偿次数
        result['CR_D360_revolvingCompensationTimes'] = loan_revolving_summary['revolvingCompensationTimes'][loan_revolving_summary['type'] == 4][0]

        # 发生代偿总金额
        result['CR_D360_revolvingCompensationAmount'] = loan_revolving_summary['revolvingCompensationAmount'][loan_revolving_summary['type'] == 4][0]

    else:
        result['CR_D360_applyTenantCount'] = 0
        result['CR_D360_validAccountCount'] = 0
        result['CR_D360_accountCount'] = 0
        result['CR_D360_creditLimitSum'] = 0.0
        result['CR_D360_lendingAmount'] = 0.0
        result['CR_D360_overdueAccountCount'] = 0
        result['CR_D360_revolvingCompensationCount'] = 0
        result['CR_D360_revolvingCompensationTimes'] = 0
        result['CR_D360_revolvingCompensationAmount'] = 0.0
    try:
        bh_report_overview_feature = get_bh_report_overview(bhInfo, 'bh_bh_report')
        query_history_feature = get_queryHistory_feature(bhInfo, 'bh_bh_report_query_history', bh_report_overview_feature)
    except:
        query_history_feature = np.nan
    try:
        result['queryHistory_reason_1_nunique_7_days'] = query_history_feature['queryHistory_reason_1_nunique_7_days'].values[0]
        result['queryHistory_reason_1_size_7_days'] = query_history_feature['queryHistory_reason_1_size_7_days'].values[0]
        result['queryHistory_reason_1_size_pct_7_days'] = query_history_feature['queryHistory_reason_1_size_pct_7_days'].values[0]
        result['queryHistory_reason_1_nunique_pct_7_days'] = query_history_feature['queryHistory_reason_1_nunique_pct_7_days'].values[0]
        result['queryHistory_reason_2_nunique_7_days'] = query_history_feature['queryHistory_reason_2_nunique_7_days'].values[0]
        result['queryHistory_reason_2_size_7_days'] = query_history_feature['queryHistory_reason_2_size_7_days'].values[0]
        result['queryHistory_reason_2_size_pct_7_days'] = query_history_feature['queryHistory_reason_2_size_pct_7_days'].values[0]
        result['queryHistory_reason_2_nunique_pct_7_days'] = query_history_feature['queryHistory_reason_2_nunique_pct_7_days'].values[0]
        result['queryHistory_reason_3_nunique_7_days'] = query_history_feature['queryHistory_reason_3_nunique_7_days'].values[0]
        result['queryHistory_reason_3_size_7_days'] = query_history_feature['queryHistory_reason_3_size_7_days'].values[0]
        result['queryHistory_reason_3_size_pct_7_days'] = query_history_feature['queryHistory_reason_3_size_pct_7_days'].values[0]
        result['queryHistory_reason_3_nunique_pct_7_days'] = query_history_feature['queryHistory_reason_3_nunique_pct_7_days'].values[0]
    except:
        result['queryHistory_reason_1_nunique_7_days'] = np.nan
        result['queryHistory_reason_1_size_7_days'] = np.nan
        result['queryHistory_reason_1_size_pct_7_days'] = np.nan
        result['queryHistory_reason_1_nunique_pct_7_days'] = np.nan
        result['queryHistory_reason_2_nunique_7_days'] = np.nan
        result['queryHistory_reason_2_size_7_days'] = np.nan
        result['queryHistory_reason_2_size_pct_7_days'] = np.nan
        result['queryHistory_reason_2_nunique_pct_7_days'] = np.nan
        result['queryHistory_reason_3_nunique_7_days'] = np.nan
        result['queryHistory_reason_3_size_7_days'] = np.nan
        result['queryHistory_reason_3_size_pct_7_days'] = np.nan
        result['queryHistory_reason_3_nunique_pct_7_days'] = np.nan

    try:
        result['queryHistory_reason_1_nunique_14_days'] = query_history_feature['queryHistory_reason_1_nunique_14_days'].values[0]
        result['queryHistory_reason_1_size_14_days'] = query_history_feature['queryHistory_reason_1_size_14_days'].values[0]
        result['queryHistory_reason_1_size_pct_14_days'] = query_history_feature['queryHistory_reason_1_size_pct_14_days'].values[0]
        result['queryHistory_reason_1_nunique_pct_14_days'] = query_history_feature['queryHistory_reason_1_nunique_pct_14_days'].values[0]
        result['queryHistory_reason_2_nunique_14_days'] = query_history_feature['queryHistory_reason_2_nunique_14_days'].values[0]
        result['queryHistory_reason_2_size_14_days'] = query_history_feature['queryHistory_reason_2_size_14_days'].values[0]
        result['queryHistory_reason_2_size_pct_14_days'] = query_history_feature['queryHistory_reason_2_size_pct_14_days'].values[0]
        result['queryHistory_reason_2_nunique_pct_14_days'] = query_history_feature['queryHistory_reason_2_nunique_pct_14_days'].values[0]
        result['queryHistory_reason_3_nunique_14_days'] = query_history_feature['queryHistory_reason_3_nunique_14_days'].values[0]
        result['queryHistory_reason_3_size_14_days'] = query_history_feature['queryHistory_reason_3_size_14_days'].values[0]
        result['queryHistory_reason_3_size_pct_14_days'] = query_history_feature['queryHistory_reason_3_size_pct_14_days'].values[0]
        result['queryHistory_reason_3_nunique_pct_14_days'] = query_history_feature['queryHistory_reason_3_nunique_pct_14_days'].values[0]
    except:
        result['queryHistory_reason_1_nunique_14_days'] = np.nan
        result['queryHistory_reason_1_size_14_days'] = np.nan
        result['queryHistory_reason_1_size_pct_14_days'] = np.nan
        result['queryHistory_reason_1_nunique_pct_14_days'] = np.nan
        result['queryHistory_reason_2_nunique_14_days'] = np.nan
        result['queryHistory_reason_2_size_14_days'] = np.nan
        result['queryHistory_reason_2_size_pct_14_days'] = np.nan
        result['queryHistory_reason_2_nunique_pct_14_days'] = np.nan
        result['queryHistory_reason_3_nunique_14_days'] = np.nan
        result['queryHistory_reason_3_size_14_days'] = np.nan
        result['queryHistory_reason_3_size_pct_14_days'] = np.nan
        result['queryHistory_reason_3_nunique_pct_14_days'] = np.nan

    try:
        result['queryHistory_reason_1_nunique_30_days'] = query_history_feature['queryHistory_reason_1_nunique_30_days'].values[0]
        result['queryHistory_reason_1_size_30_days'] = query_history_feature['queryHistory_reason_1_size_30_days'].values[0]
        result['queryHistory_reason_1_size_pct_30_days'] = query_history_feature['queryHistory_reason_1_size_pct_30_days'].values[0]
        result['queryHistory_reason_1_nunique_pct_30_days'] = query_history_feature['queryHistory_reason_1_nunique_pct_30_days'].values[0]
        result['queryHistory_reason_2_nunique_30_days'] = query_history_feature['queryHistory_reason_2_nunique_30_days'].values[0]
        result['queryHistory_reason_2_size_30_days'] = query_history_feature['queryHistory_reason_2_size_30_days'].values[0]
        result['queryHistory_reason_2_size_pct_30_days'] = query_history_feature['queryHistory_reason_2_size_pct_30_days'].values[0]
        result['queryHistory_reason_2_nunique_pct_30_days'] = query_history_feature['queryHistory_reason_2_nunique_pct_30_days'].values[0]
        result['queryHistory_reason_3_nunique_30_days'] = query_history_feature['queryHistory_reason_3_nunique_30_days'].values[0]
        result['queryHistory_reason_3_size_30_days'] = query_history_feature['queryHistory_reason_3_size_30_days'].values[0]
        result['queryHistory_reason_3_size_pct_30_days'] = query_history_feature['queryHistory_reason_3_size_pct_30_days'].values[0]
        result['queryHistory_reason_3_nunique_pct_30_days'] = query_history_feature['queryHistory_reason_3_nunique_pct_30_days'].values[0]
    except:
        result['queryHistory_reason_1_nunique_30_days'] = np.nan
        result['queryHistory_reason_1_size_30_days'] = np.nan
        result['queryHistory_reason_1_size_pct_30_days'] = np.nan
        result['queryHistory_reason_1_nunique_pct_30_days'] = np.nan
        result['queryHistory_reason_2_nunique_30_days'] = np.nan
        result['queryHistory_reason_2_size_30_days'] = np.nan
        result['queryHistory_reason_2_size_pct_30_days'] = np.nan
        result['queryHistory_reason_2_nunique_pct_30_days'] = np.nan
        result['queryHistory_reason_3_nunique_30_days'] = np.nan
        result['queryHistory_reason_3_size_30_days'] = np.nan
        result['queryHistory_reason_3_size_pct_30_days'] = np.nan
        result['queryHistory_reason_3_nunique_pct_30_days'] = np.nan

    try:
        result['queryHistory_reason_1_nunique_60_days'] = query_history_feature['queryHistory_reason_1_nunique_60_days'].values[0]
        result['queryHistory_reason_1_size_60_days'] = query_history_feature['queryHistory_reason_1_size_60_days'].values[0]
        result['queryHistory_reason_1_size_pct_60_days'] = query_history_feature['queryHistory_reason_1_size_pct_60_days'].values[0]
        result['queryHistory_reason_1_nunique_pct_60_days'] = query_history_feature['queryHistory_reason_1_nunique_pct_60_days'].values[0]
        result['queryHistory_reason_2_nunique_60_days'] = query_history_feature['queryHistory_reason_2_nunique_60_days'].values[0]
        result['queryHistory_reason_2_size_60_days'] = query_history_feature['queryHistory_reason_2_size_60_days'].values[0]
        result['queryHistory_reason_2_size_pct_60_days'] = query_history_feature['queryHistory_reason_2_size_pct_60_days'].values[0]
        result['queryHistory_reason_2_nunique_pct_60_days'] = query_history_feature['queryHistory_reason_2_nunique_pct_60_days'].values[0]
        result['queryHistory_reason_3_nunique_60_days'] = query_history_feature['queryHistory_reason_3_nunique_60_days'].values[0]
        result['queryHistory_reason_3_size_60_days'] = query_history_feature['queryHistory_reason_3_size_60_days'].values[0]
        result['queryHistory_reason_3_size_pct_60_days'] = query_history_feature['queryHistory_reason_3_size_pct_60_days'].values[0]
        result['queryHistory_reason_3_nunique_pct_60_days'] = query_history_feature['queryHistory_reason_3_nunique_pct_60_days'].values[0]
    except:
        result['queryHistory_reason_1_nunique_60_days'] = np.nan
        result['queryHistory_reason_1_size_60_days'] = np.nan
        result['queryHistory_reason_1_size_pct_60_days'] = np.nan
        result['queryHistory_reason_1_nunique_pct_60_days'] = np.nan
        result['queryHistory_reason_2_nunique_60_days'] = np.nan
        result['queryHistory_reason_2_size_60_days'] = np.nan
        result['queryHistory_reason_2_size_pct_60_days'] = np.nan
        result['queryHistory_reason_2_nunique_pct_60_days'] = np.nan
        result['queryHistory_reason_3_nunique_60_days'] = np.nan
        result['queryHistory_reason_3_size_60_days'] = np.nan
        result['queryHistory_reason_3_size_pct_60_days'] = np.nan
        result['queryHistory_reason_3_nunique_pct_60_days'] = np.nan

    try:
        result['queryHistory_reason_1_nunique_90_days'] = query_history_feature['queryHistory_reason_1_nunique_90_days'].values[0]
        result['queryHistory_reason_1_size_90_days'] = query_history_feature['queryHistory_reason_1_size_90_days'].values[0]
        result['queryHistory_reason_1_size_pct_90_days'] = query_history_feature['queryHistory_reason_1_size_pct_90_days'].values[0]
        result['queryHistory_reason_1_nunique_pct_90_days'] = query_history_feature['queryHistory_reason_1_nunique_pct_90_days'].values[0]
        result['queryHistory_reason_2_nunique_90_days'] = query_history_feature['queryHistory_reason_2_nunique_90_days'].values[0]
        result['queryHistory_reason_2_size_90_days'] = query_history_feature['queryHistory_reason_2_size_90_days'].values[0]
        result['queryHistory_reason_2_size_pct_90_days'] = query_history_feature['queryHistory_reason_2_size_pct_90_days'].values[0]
        result['queryHistory_reason_2_nunique_pct_90_days'] = query_history_feature['queryHistory_reason_2_nunique_pct_90_days'].values[0]
        result['queryHistory_reason_3_nunique_90_days'] = query_history_feature['queryHistory_reason_3_nunique_90_days'].values[0]
        result['queryHistory_reason_3_size_90_days'] = query_history_feature['queryHistory_reason_3_size_90_days'].values[0]
        result['queryHistory_reason_3_size_pct_90_days'] = query_history_feature['queryHistory_reason_3_size_pct_90_days'].values[0]
        result['queryHistory_reason_3_nunique_pct_90_days'] = query_history_feature['queryHistory_reason_3_nunique_pct_90_days'].values[0]
    except:
        result['queryHistory_reason_1_nunique_90_days'] = np.nan
        result['queryHistory_reason_1_size_90_days'] = np.nan
        result['queryHistory_reason_1_size_pct_90_days'] = np.nan
        result['queryHistory_reason_1_nunique_pct_90_days'] = np.nan
        result['queryHistory_reason_2_nunique_90_days'] = np.nan
        result['queryHistory_reason_2_size_90_days'] = np.nan
        result['queryHistory_reason_2_size_pct_90_days'] = np.nan
        result['queryHistory_reason_2_nunique_pct_90_days'] = np.nan
        result['queryHistory_reason_3_nunique_90_days'] = np.nan
        result['queryHistory_reason_3_size_90_days'] = np.nan
        result['queryHistory_reason_3_size_pct_90_days'] = np.nan
        result['queryHistory_reason_3_nunique_pct_90_days'] = np.nan

    try:
        result['queryHistory_reason_1_nunique_180_days'] = query_history_feature['queryHistory_reason_1_nunique_180_days'].values[0]
        result['queryHistory_reason_1_size_180_days'] = query_history_feature['queryHistory_reason_1_size_180_days'].values[0]
        result['queryHistory_reason_1_size_pct_180_days'] = query_history_feature['queryHistory_reason_1_size_pct_180_days'].values[0]
        result['queryHistory_reason_1_nunique_pct_180_days'] = query_history_feature['queryHistory_reason_1_nunique_pct_180_days'].values[0]
        result['queryHistory_reason_2_nunique_180_days'] = query_history_feature['queryHistory_reason_2_nunique_180_days'].values[0]
        result['queryHistory_reason_2_size_180_days'] = query_history_feature['queryHistory_reason_2_size_180_days'].values[0]
        result['queryHistory_reason_2_size_pct_180_days'] = query_history_feature['queryHistory_reason_2_size_pct_180_days'].values[0]
        result['queryHistory_reason_2_nunique_pct_180_days'] = query_history_feature['queryHistory_reason_2_nunique_pct_180_days'].values[0]
        result['queryHistory_reason_3_nunique_180_days'] = query_history_feature['queryHistory_reason_3_nunique_180_days'].values[0]
        result['queryHistory_reason_3_size_180_days'] = query_history_feature['queryHistory_reason_3_size_180_days'].values[0]
        result['queryHistory_reason_3_size_pct_180_days'] = query_history_feature['queryHistory_reason_3_size_pct_180_days'].values[0]
        result['queryHistory_reason_3_nunique_pct_180_days'] = query_history_feature['queryHistory_reason_3_nunique_pct_180_days'].values[0]
    except:
        result['queryHistory_reason_1_nunique_180_days'] = np.nan
        result['queryHistory_reason_1_size_180_days'] = np.nan
        result['queryHistory_reason_1_size_pct_180_days'] = np.nan
        result['queryHistory_reason_1_nunique_pct_180_days'] = np.nan
        result['queryHistory_reason_2_nunique_180_days'] = np.nan
        result['queryHistory_reason_2_size_180_days'] = np.nan
        result['queryHistory_reason_2_size_pct_180_days'] = np.nan
        result['queryHistory_reason_2_nunique_pct_180_days'] = np.nan
        result['queryHistory_reason_3_nunique_180_days'] = np.nan
        result['queryHistory_reason_3_size_180_days'] = np.nan
        result['queryHistory_reason_3_size_pct_180_days'] = np.nan
        result['queryHistory_reason_3_nunique_pct_180_days'] = np.nan

    try:
        result['queryHistory_reason_1_nunique_360_days'] = query_history_feature['queryHistory_reason_1_nunique_360_days'].values[0]
        result['queryHistory_reason_1_size_360_days'] = query_history_feature['queryHistory_reason_1_size_360_days'].values[0]
        result['queryHistory_reason_1_size_pct_360_days'] = query_history_feature['queryHistory_reason_1_size_pct_360_days'].values[0]
        result['queryHistory_reason_1_nunique_pct_360_days'] = query_history_feature['queryHistory_reason_1_nunique_pct_360_days'].values[0]
        result['queryHistory_reason_2_nunique_360_days'] = query_history_feature['queryHistory_reason_2_nunique_360_days'].values[0]
        result['queryHistory_reason_2_size_360_days'] = query_history_feature['queryHistory_reason_2_size_360_days'].values[0]
        result['queryHistory_reason_2_size_pct_360_days'] = query_history_feature['queryHistory_reason_2_size_pct_360_days'].values[0]
        result['queryHistory_reason_2_nunique_pct_360_days'] = query_history_feature['queryHistory_reason_2_nunique_pct_360_days'].values[0]
        result['queryHistory_reason_3_nunique_360_days'] = query_history_feature['queryHistory_reason_3_nunique_360_days'].values[0]
        result['queryHistory_reason_3_size_360_days'] = query_history_feature['queryHistory_reason_3_size_360_days'].values[0]
        result['queryHistory_reason_3_size_pct_360_days'] = query_history_feature['queryHistory_reason_3_size_pct_360_days'].values[0]
        result['queryHistory_reason_3_nunique_pct_360_days'] = query_history_feature['queryHistory_reason_3_nunique_pct_360_days'].values[0]
    except:
        result['queryHistory_reason_1_nunique_360_days'] = np.nan
        result['queryHistory_reason_1_size_360_days'] = np.nan
        result['queryHistory_reason_1_size_pct_360_days'] = np.nan
        result['queryHistory_reason_1_nunique_pct_360_days'] = np.nan
        result['queryHistory_reason_2_nunique_360_days'] = np.nan
        result['queryHistory_reason_2_size_360_days'] = np.nan
        result['queryHistory_reason_2_size_pct_360_days'] = np.nan
        result['queryHistory_reason_2_nunique_pct_360_days'] = np.nan
        result['queryHistory_reason_3_nunique_360_days'] = np.nan
        result['queryHistory_reason_3_size_360_days'] = np.nan
        result['queryHistory_reason_3_size_pct_360_days'] = np.nan
        result['queryHistory_reason_3_nunique_pct_360_days'] = np.nan

    try:
        result['queryHistory_reason_1_nunique_3600_days'] = query_history_feature['queryHistory_reason_1_nunique_3600_days'].values[0]
        result['queryHistory_reason_1_size_3600_days'] = query_history_feature['queryHistory_reason_1_size_3600_days'].values[0]
        result['queryHistory_reason_1_size_pct_3600_days'] = query_history_feature['queryHistory_reason_1_size_pct_3600_days'].values[0]
        result['queryHistory_reason_1_nunique_pct_3600_days'] = query_history_feature['queryHistory_reason_1_nunique_pct_3600_days'].values[0]
        result['queryHistory_reason_2_nunique_3600_days'] = query_history_feature['queryHistory_reason_2_nunique_3600_days'].values[0]
        result['queryHistory_reason_2_size_3600_days'] = query_history_feature['queryHistory_reason_2_size_3600_days'].values[0]
        result['queryHistory_reason_2_size_pct_3600_days'] = query_history_feature['queryHistory_reason_2_size_pct_3600_days'].values[0]
        result['queryHistory_reason_2_nunique_pct_3600_days'] = query_history_feature['queryHistory_reason_2_nunique_pct_3600_days'].values[0]
        result['queryHistory_reason_3_nunique_3600_days'] = query_history_feature['queryHistory_reason_3_nunique_3600_days'].values[0]
        result['queryHistory_reason_3_size_3600_days'] = query_history_feature['queryHistory_reason_3_size_3600_days'].values[0]
        result['queryHistory_reason_3_size_pct_3600_days'] = query_history_feature['queryHistory_reason_3_size_pct_3600_days'].values[0]
        result['queryHistory_reason_3_nunique_pct_3600_days'] = query_history_feature['queryHistory_reason_3_nunique_pct_3600_days'].values[0]
    except:
        result['queryHistory_reason_1_nunique_3600_days'] = np.nan
        result['queryHistory_reason_1_size_3600_days'] = np.nan
        result['queryHistory_reason_1_size_pct_3600_days'] = np.nan
        result['queryHistory_reason_1_nunique_pct_3600_days'] = np.nan
        result['queryHistory_reason_2_nunique_3600_days'] = np.nan
        result['queryHistory_reason_2_size_3600_days'] = np.nan
        result['queryHistory_reason_2_size_pct_3600_days'] = np.nan
        result['queryHistory_reason_2_nunique_pct_3600_days'] = np.nan
        result['queryHistory_reason_3_nunique_3600_days'] = np.nan
        result['queryHistory_reason_3_size_3600_days'] = np.nan
        result['queryHistory_reason_3_size_pct_3600_days'] = np.nan
        result['queryHistory_reason_3_nunique_pct_3600_days'] = np.nan

    del result['reportTime']
    return result


def initial_feature(result):

    result['NR_D30_loanAmount'] = np.nan
    result['reportTime'] = np.nan
    result['NR_D90_applyTenantCount'] = np.nan
    result['CR_validAccountCount'] = np.nan
    result['NR_D90_averageLoanAmount'] = np.nan
    result['NR_D30_averageLoanAmount'] = np.nan
    result['NR_D90_maxLoanAmount'] = np.nan
    result['NR_D180_averageLoanAmount'] = np.nan
    result['NR_D180_loanCount'] = np.nan
    result['provice_workAddress'] = np.nan
    result['NR_D180_maxLoanAmount'] = np.nan
    result['NR_D180_applyTenantCount'] = np.nan
    result['NR_D360_loanAmount'] = np.nan
    result['NE_overdueCount'] = np.nan
    result['NR_D90_loanTenantCount'] = np.nan
    result['CR_remainingOverdueAccountCount'] = np.nan
    result['CR_remainingOverdueAmount'] = np.nan
    result['pid'] = np.nan
    result['provice_homeAddress_6m'] = np.nan
    result['NE_remainingAmount'] = np.nan
    result['NR_D360_maxLoanAmount'] = np.nan
    result['CR_remainingMaxOverdueStatus'] = np.nan
    result['NR_D180_overdueLoanCount'] = np.nan
    result['NR_D360_loanTenantCount'] = np.nan
    result['NE_openLoanCount'] = np.nan
    result['NR_D90_loanCount'] = np.nan
    result['NR_D360_applyTenantCount'] = np.nan
    result['provice_homeAddress'] = np.nan
    result['NR_D90_overdueLoanCount'] = np.nan
    result['NR_D30_loanTenantCount'] = np.nan
    result['NR_D30_overdueLoanCount'] = np.nan
    result['NR_D90_loanAmount'] = np.nan
    result['NR_D180_loanAmount'] = np.nan
    result['NR_D30_loanCount'] = np.nan
    result['provice_workAddress_6m'] = np.nan
    result['NE_remainingOverdueLoanCount'] = np.nan
    result['provice_homeAddress_12m'] = np.nan
    result['queryResult'] = np.nan
    result['NE_loanCount'] = np.nan
    result['CR_maxCreditLimitPerTenant'] = np.nan
    result['latest_homeAddress'] = np.nan
    result['NE_remainingOverdueAmount'] = np.nan
    result['NE_remainingMaxOverdueStatus'] = np.nan
    result['CR_overdueCount'] = np.nan
    result['NR_D360_loanCount'] = np.nan
    result['NR_D360_averageLoanAmount'] = np.nan
    result['CR_creditLimitSum'] = np.nan
    result['CR_accountCount'] = np.nan
    result['Address_consistence'] = np.nan
    result['NE_maxOverdueStatus'] = np.nan
    result['CR_remainingAmount'] = np.nan
    result['latest_workAddress'] = np.nan
    result['CR_maxOverdueStatus'] = np.nan
    result['provice_workAddress_12m'] = np.nan
    result['NR_D30_maxLoanAmount'] = np.nan
    result['NR_D360_overdueLoanCount'] = np.nan
    result['NR_D30_applyTenantCount'] = np.nan
    result['mobileCount'] = np.nan
    result['NR_D180_loanTenantCount'] = np.nan

    result['CR_D30_validAccountCount'] = np.nan
    result['CR_D30_accountCount'] = np.nan
    result['CR_D30_creditLimitSum'] = np.nan
    result['CR_D30_lendingAmount'] = np.nan
    result['CR_D30_overdueAccountCount'] = np.nan
    result['CR_D30_revolvingCompensationCount'] = np.nan
    result['CR_D30_revolvingCompensationTimes'] = np.nan
    result['CR_D30_revolvingCompensationAmount'] = np.nan

    result['CR_D90_validAccountCount'] = np.nan
    result['CR_D90_accountCount'] = np.nan
    result['CR_D90_creditLimitSum'] = np.nan
    result['CR_D90_lendingAmount'] = np.nan
    result['CR_D90_overdueAccountCount'] = np.nan
    result['CR_D90_revolvingCompensationCount'] = np.nan
    result['CR_D90_revolvingCompensationTimes'] = np.nan
    result['CR_D90_revolvingCompensationAmount'] = np.nan

    result['CR_D180_validAccountCount'] = np.nan
    result['CR_D180_accountCount'] = np.nan
    result['CR_D180_creditLimitSum'] = np.nan
    result['CR_D180_lendingAmount'] = np.nan
    result['CR_D180_overdueAccountCount'] = np.nan
    result['CR_D180_revolvingCompensationCount'] = np.nan
    result['CR_D180_revolvingCompensationTimes'] = np.nan
    result['CR_D180_revolvingCompensationAmount'] = np.nan

    result['CR_D360_validAccountCount'] = np.nan
    result['CR_D360_accountCount'] = np.nan
    result['CR_D360_creditLimitSum'] = np.nan
    result['CR_D360_lendingAmount'] = np.nan
    result['CR_D360_overdueAccountCount'] = np.nan
    result['CR_D360_revolvingCompensationCount'] = np.nan
    result['CR_D360_revolvingCompensationTimes'] = np.nan
    result['CR_D360_revolvingCompensationAmount'] = np.nan

    result['queryHistory_reason_1_nunique_7_days'] = np.nan
    result['queryHistory_reason_1_size_7_days'] = np.nan
    result['queryHistory_reason_1_size_pct_7_days'] = np.nan
    result['queryHistory_reason_1_nunique_pct_7_days'] = np.nan
    result['queryHistory_reason_2_nunique_7_days'] = np.nan
    result['queryHistory_reason_2_size_7_days'] = np.nan
    result['queryHistory_reason_2_size_pct_7_days'] = np.nan
    result['queryHistory_reason_2_nunique_pct_7_days'] = np.nan
    result['queryHistory_reason_3_nunique_7_days'] = np.nan
    result['queryHistory_reason_3_size_7_days'] = np.nan
    result['queryHistory_reason_3_size_pct_7_days'] = np.nan
    result['queryHistory_reason_3_nunique_pct_7_days'] = np.nan
    result['queryHistory_reason_1_nunique_14_days'] = np.nan
    result['queryHistory_reason_1_size_14_days'] = np.nan
    result['queryHistory_reason_1_size_pct_14_days'] = np.nan
    result['queryHistory_reason_1_nunique_pct_14_days'] = np.nan
    result['queryHistory_reason_2_nunique_14_days'] = np.nan
    result['queryHistory_reason_2_size_14_days'] = np.nan
    result['queryHistory_reason_2_size_pct_14_days'] = np.nan
    result['queryHistory_reason_2_nunique_pct_14_days'] = np.nan
    result['queryHistory_reason_3_nunique_14_days'] = np.nan
    result['queryHistory_reason_3_size_14_days'] = np.nan
    result['queryHistory_reason_3_size_pct_14_days'] = np.nan
    result['queryHistory_reason_3_nunique_pct_14_days'] = np.nan
    result['queryHistory_reason_1_nunique_30_days'] = np.nan
    result['queryHistory_reason_1_size_30_days'] = np.nan
    result['queryHistory_reason_1_size_pct_30_days'] = np.nan
    result['queryHistory_reason_1_nunique_pct_30_days'] = np.nan
    result['queryHistory_reason_2_nunique_30_days'] = np.nan
    result['queryHistory_reason_2_size_30_days'] = np.nan
    result['queryHistory_reason_2_size_pct_30_days'] = np.nan
    result['queryHistory_reason_2_nunique_pct_30_days'] = np.nan
    result['queryHistory_reason_3_nunique_30_days'] = np.nan
    result['queryHistory_reason_3_size_30_days'] = np.nan
    result['queryHistory_reason_3_size_pct_30_days'] = np.nan
    result['queryHistory_reason_3_nunique_pct_30_days'] = np.nan
    result['queryHistory_reason_1_nunique_60_days'] = np.nan
    result['queryHistory_reason_1_size_60_days'] = np.nan
    result['queryHistory_reason_1_size_pct_60_days'] = np.nan
    result['queryHistory_reason_1_nunique_pct_60_days'] = np.nan
    result['queryHistory_reason_2_nunique_60_days'] = np.nan
    result['queryHistory_reason_2_size_60_days'] = np.nan
    result['queryHistory_reason_2_size_pct_60_days'] = np.nan
    result['queryHistory_reason_2_nunique_pct_60_days'] = np.nan
    result['queryHistory_reason_3_nunique_60_days'] = np.nan
    result['queryHistory_reason_3_size_60_days'] = np.nan
    result['queryHistory_reason_3_size_pct_60_days'] = np.nan
    result['queryHistory_reason_3_nunique_pct_60_days'] = np.nan
    result['queryHistory_reason_1_nunique_90_days'] = np.nan
    result['queryHistory_reason_1_size_90_days'] = np.nan
    result['queryHistory_reason_1_size_pct_90_days'] = np.nan
    result['queryHistory_reason_1_nunique_pct_90_days'] = np.nan
    result['queryHistory_reason_2_nunique_90_days'] = np.nan
    result['queryHistory_reason_2_size_90_days'] = np.nan
    result['queryHistory_reason_2_size_pct_90_days'] = np.nan
    result['queryHistory_reason_2_nunique_pct_90_days'] = np.nan
    result['queryHistory_reason_3_nunique_90_days'] = np.nan
    result['queryHistory_reason_3_size_90_days'] = np.nan
    result['queryHistory_reason_3_size_pct_90_days'] = np.nan
    result['queryHistory_reason_3_nunique_pct_90_days'] = np.nan
    result['queryHistory_reason_1_nunique_180_days'] = np.nan
    result['queryHistory_reason_1_size_180_days'] = np.nan
    result['queryHistory_reason_1_size_pct_180_days'] = np.nan
    result['queryHistory_reason_1_nunique_pct_180_days'] = np.nan
    result['queryHistory_reason_2_nunique_180_days'] = np.nan
    result['queryHistory_reason_2_size_180_days'] = np.nan
    result['queryHistory_reason_2_size_pct_180_days'] = np.nan
    result['queryHistory_reason_2_nunique_pct_180_days'] = np.nan
    result['queryHistory_reason_3_nunique_180_days'] = np.nan
    result['queryHistory_reason_3_size_180_days'] = np.nan
    result['queryHistory_reason_3_size_pct_180_days'] = np.nan
    result['queryHistory_reason_3_nunique_pct_180_days'] = np.nan
    result['queryHistory_reason_1_nunique_360_days'] = np.nan
    result['queryHistory_reason_1_size_360_days'] = np.nan
    result['queryHistory_reason_1_size_pct_360_days'] = np.nan
    result['queryHistory_reason_1_nunique_pct_360_days'] = np.nan
    result['queryHistory_reason_2_nunique_360_days'] = np.nan
    result['queryHistory_reason_2_size_360_days'] = np.nan
    result['queryHistory_reason_2_size_pct_360_days'] = np.nan
    result['queryHistory_reason_2_nunique_pct_360_days'] = np.nan
    result['queryHistory_reason_3_nunique_360_days'] = np.nan
    result['queryHistory_reason_3_size_360_days'] = np.nan
    result['queryHistory_reason_3_size_pct_360_days'] = np.nan
    result['queryHistory_reason_3_nunique_pct_360_days'] = np.nan
    result['queryHistory_reason_1_nunique_3600_days'] = np.nan
    result['queryHistory_reason_1_size_3600_days'] = np.nan
    result['queryHistory_reason_1_size_pct_3600_days'] = np.nan
    result['queryHistory_reason_1_nunique_pct_3600_days'] = np.nan
    result['queryHistory_reason_2_nunique_3600_days'] = np.nan
    result['queryHistory_reason_2_size_3600_days'] = np.nan
    result['queryHistory_reason_2_size_pct_3600_days'] = np.nan
    result['queryHistory_reason_2_nunique_pct_3600_days'] = np.nan
    result['queryHistory_reason_3_nunique_3600_days'] = np.nan
    result['queryHistory_reason_3_size_3600_days'] = np.nan
    result['queryHistory_reason_3_size_pct_3600_days'] = np.nan
    result['queryHistory_reason_3_nunique_pct_3600_days'] = np.nan

    return result


def get_data_set(baihang_info=None):

    dict_in, dict_out = get_data(baihang_info=baihang_info)
    data = get_features(dict_in, dict_out,baihang_info)  # 特征加工
    #result['is_overdue'] = is_overdue
    #print("特征：", len(dict_out)-17, "条")
    # data_1 = pd.DataFrame([data])
    # return data_1
    return data


    # try:
    #     data = get_features(dict_in, dict_out)  # 特征加工
    #     #result['is_overdue'] = is_overdue
    #     #print("特征：", len(dict_out)-17, "条")
    #     data_1 = pd.DataFrame([data])
    #     return data_1
    # except Exception as e:
    #     print("报错:", e)
    #     data = pd.DataFrame()
    #     return data

