
import numpy as np
import pandas as pd 
# from scipy.stats import chi2
import scorecardpy as sc
import sqlalchemy
from sqlalchemy import create_engine
import copy
# import statsmodels.api as smf
from dateutil.relativedelta import relativedelta
import warnings
import re
# from pandas.api.types import is_numeric_dtype
# from pandas.api.types import is_string_dtype

import datetime
import hashlib
import pickle
from joblib import Parallel, delayed
from tqdm import tqdm
import requests
import pboc_funcs
import math
import sys


import os
from scipy import stats
from multiprocessing import Pool
import multiprocessing
from dateutil.relativedelta import relativedelta
import json
from itertools import chain
import get_user_data



# 高德详细地址转经纬度API
# 申请的key,请大家自己申请哈，原来给了我的Ak，结果有人给我把一天的配额用完了
# ak='5ba9f811bdfa620cb8946cc833075155'
# 传入地址，返回对应地址的经纬度信息
def address(address):
    address = "".join(re.findall('[a-zA-Z0-9\u4e00-\u9fa5]+', str(address)))
    url = "http://restapi.amap.com/v3/geocode/geo?key=%s&address=%s" % ('5ba9f811bdfa620cb8946cc833075155', address)
    data = requests.get(url).json()
    if data['info'] == 'OK' and len(data['geocodes']) > 0:
        contest = data['geocodes'][0]
        for key, value in contest.items():
            if value == []:
                contest[key] = ''
    else:
        contest = {'formatted_address': '',
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




# 经纬度转距离和时间
import time


# 函数 两个日期的月份差
# import datetime
# from dateutil.relativedelta import relativedelta
import datetime
from dateutil.relativedelta import relativedelta


def monthdelta(startdate, enddate):
    startdate1 = re.findall(
        r'[1-9][0-9]{3} *?[0-9]{1,2} *?[0-9]{1,2}|[1-9][0-9]{3} *?[-/.年] *?[0-9]{1,2} *?[-/.月] *?[0-9]{1,2}',
        str(startdate))
    startdate2 = re.findall('[1-9][0-9]{3} *?[-/.年]? *?[0-9]{1,2} *?', str(startdate))

    if (len(startdate1) == 0) & (len(startdate2) == 0):
        return np.nan
    elif (len(startdate1) > 0):
        startdate = startdate1[0]
        startdate = re.sub('[-/.年月]', '-', re.sub(' ', '', startdate))
        sub1 = re.findall('-([0-9]{1}-)', startdate)
        startdate = re.sub('-' + sub1[0], '-0' + str(sub1[0]), startdate) if len(sub1) > 0 else startdate
        sub2 = re.findall('-([0-9]{1}$)', startdate)
        startdate = re.sub('-' + sub2[0], '-0' + str(sub2[0]), startdate) if len(sub2) > 0 else startdate
        startdate = re.sub('[-/.年月]', '', startdate)
        try:
            d1 = datetime.datetime.strptime(startdate[0:10], '%Y%m')
        except:
            d1 = datetime.datetime.strptime(startdate[0:10], '%Y%m%d')
    elif len(startdate2) > 0:
        startdate = startdate2[0]
        startdate = re.sub('[-/.年月]', '-', re.sub(' ', '', startdate))
        sub2 = re.findall('-([0-9]{1}$)', startdate)
        startdate = re.sub('-' + sub2[0], '-0' + str(sub2[0]), startdate) if len(sub2) > 0 else startdate
        startdate = re.sub('[-/.年月]', '', startdate)
        d1 = datetime.datetime.strptime(startdate[0:10], '%Y%m')
    else:
        return np.nan

    enddate1 = re.findall(
        r'[1-9][0-9]{3} *?[0-9]{1,2} *?[0-9]{1,2}|[1-9][0-9]{3} *?[-/.] *?[0-9]{1,2} *?[-/.] *?[0-9]{1,2}',
        str(enddate))
    enddate2 = re.findall('[1-9][0-9]{3} *?[-/.]? *?[0-9]{1,2} *?', str(enddate))
    if (len(enddate1) == 0) & (len(enddate2) == 0):
        return np.nan
    elif (len(enddate1) > 0):
        enddate = enddate1[0]
        enddate = re.sub('[-/.]', '-', re.sub(' ', '', enddate))
        sub1 = re.findall('-([0-9]{1}-)', enddate)
        enddate = re.sub('-' + sub1[0], '-0' + str(sub1[0]), enddate) if len(sub1) > 0 else enddate
        sub2 = re.findall('-([0-9]{1}$)', enddate)
        enddate = re.sub('-' + sub2[0], '-0' + str(sub2[0]), enddate) if len(sub2) > 0 else enddate
        enddate = re.sub('[-/.]', '', enddate)
        try:
            d2 = datetime.datetime.strptime(enddate[0:10], '%Y%m')
        except:
            d2 = datetime.datetime.strptime(enddate[0:10], '%Y%m%d')
    elif len(enddate2) > 0:
        enddate = enddate2[0]
        enddate = re.sub('[-/.]', '-', re.sub(' ', '', enddate))
        sub2 = re.findall('-([0-9]{1}$)', enddate)
        enddate = re.sub('-' + sub2[0], '-0' + str(sub2[0]), enddate) if len(sub2) > 0 else enddate
        enddate = re.sub('[-/.]', '', enddate)
        d2 = datetime.datetime.strptime(enddate[0:10], '%Y%m')
    else:
        return np.nan

    delta = 0
    if d1 <= d2:
        while True:
            d1_tmp = d1 + relativedelta(months=delta + 1)
            if d1_tmp <= d2:
                delta += 1
            else:
                break
        return delta
    else:
        while True:
            d1_tmp = d1 + relativedelta(months=delta - 1)
            if d1_tmp >= d2:
                delta -= 1
            else:
                break
        return delta


# monthdelta('2019.1.30', '2019.3.30')

# 函数 两个日期的天数差
def daysdelta(startdate, enddate):
    startdate = re.findall(
        r'[1-9][0-9]{3} *?[0-9]{1,2} *?[0-9]{1,2}|[1-9][0-9]{3} *?[-/.] *?[0-9]{1,2} *?[-/.] *?[0-9]{1,2}',
        str(startdate))
    if len(startdate) > 0:
        startdate = startdate[0]
        startdate = re.sub('[-/.]', '-', re.sub(' ', '', startdate))
        sub1 = re.findall('-([0-9]{1}-)', startdate)
        if len(sub1) > 0:
            sub1_is_0 = '1' if str(sub1[0]) == '0' else sub1[0]
            startdate = re.sub('-' + sub1[0], '-0' + str(sub1_is_0), startdate)

        sub2 = re.findall('-([0-9]{1}$)', startdate)
        if len(sub2) > 0:
            sub2_is_0 = '1' if str(sub2[0]) == '0' else sub2[0]
            startdate = re.sub('-' + sub2[0] + '$', '-0' + str(sub2_is_0), startdate)
        startdate = re.sub('[-/.]', '', startdate)
        d1 = datetime.datetime.strptime(startdate[0:10], '%Y%m%d')
    else:
        return np.nan

    enddate = re.findall(
        r'[1-9][0-9]{3} *?[0-9]{1,2} *?[0-9]{1,2}|[1-9][0-9]{3} *?[-/.] *?[0-9]{1,2} *?[-/.] *?[0-9]{1,2}',
        str(enddate))
    if len(enddate) > 0:
        enddate = enddate[0]
        enddate = re.sub('[-/.]', '-', re.sub(' ', '', enddate))  # 把间隔替换为-
        sub1 = re.findall('-([0-9]{1}-)', enddate)  # 把间2050-1-1隔替换为2050-01-1
        if len(sub1) > 0:
            sub1_is_0 = '1' if str(sub1[0]) == '0' else sub1[0]
            enddate = re.sub('-' + sub1[0], '-0' + str(sub1_is_0), enddate)
        sub2 = re.findall('-([0-9]{1}$)', enddate)  # 把间2050-01-1隔替换为2050-01-01
        if len(sub2) > 0:
            sub2_is_0 = '1' if str(sub2[0]) == '0' else sub2[0]
            enddate = re.sub('-' + sub2[0] + '$', '-0' + str(sub2_is_0), enddate)
        enddate = re.sub('[-/.]', '', enddate)
        d2 = datetime.datetime.strptime(enddate[0:10], '%Y%m%d')
    else:
        return np.nan
    return (d2 - d1).days


# daysdelta('2019.1.30', '2019.3.30')


# 基尼不纯度(Gini impurity) https://blog.csdn.net/a2099948768/article/details/82465150
def uniquecounts(rows):
    results = {}
    for row in rows:
        if row not in results:
            results[row] = 0
        results[row] += 1
    return results


def giniimpurity(rows):
    total = len(rows)
    counts = uniquecounts(rows)
    imp = 0
    for k1 in counts.keys():
        p1 = float(counts[k1]) / total
        imp += p1 * p1
    #         for k2 in counts:
    #             if k1 == k2:
    #                 continue
    #             p2 = float(counts[k2]) / total
    #             imp += p1 * p2
    return 1 - imp  # imp



def basic(dict_in,dict_out):
    import pandas as pd

    # tmp_city_level,tmp_metropolitan_area,tmp_three_area,tmp_eight_eco_area,tmp_hot_area,tmp_nation_area = pboc_funcs.load_city_level()

    dict_out,numeric_vers,loopvars = pboc_funcs.initial(dict_out)

    # dict_out={**dict_out, **pboc_funcs.get_idcard_info(dict_out['idcardNo'])}
    dict_out['age_idcard']= monthdelta(dict_out['idcardNo'][6:14] ,dict_in['apply_info']['loan_time'])/12 #申请时年龄
    # dict_out={**dict_out, **pboc_funcs.get_mobile_info(dict_out['mobile'])}
    city_level_dict={'一线城市':0,'新一线城市':1,'二线城市':2,'三线城市':3,'四线城市':4,'五线城市':5}


    #二代征信-信贷交易信息明细-被追偿信息
    recovery=copy.copy(dict_in['cc_rh_report_detail_recovery'])
    if len(recovery)>0:
        recovery['month_to_report']=recovery.apply(lambda x:monthdelta(x['rightsReceiveDate'],dict_in['cc_rh_report']['reportTime']),axis=1)


    #二代征信-贷记卡特殊交易明细



    loan_special=copy.copy(dict_in['cc_rh_report_loan_special_detail_second'])
    if len(loan_special)>0:
        loan_special['month_to_report']=loan_special.apply(lambda x: monthdelta(x.specialTradeDate,dict_in['cc_rh_report']['reportTime']) ,axis=1 )
        dict_out['loan_special_settlement_count']=sum(loan_special.specialTradeType=='提前结清') # 特殊交易类型为提前结清，分布计数

        dict_out['m3_loan_special_settlement_amt']=loan_special[(loan_special.month_to_report<3) & (loan_special.specialTradeType=='提前结清')].specialTradeAmount.sum() # 近3个月特殊交易类型为展期，发生金额总和
        dict_out['m3_loan_special_other_amt']=loan_special[(loan_special.month_to_report<3) & (loan_special.specialTradeType=='其他')].specialTradeAmount.sum() # 近3个月特殊交易类型为展期，发生金额总和
        dict_out['m6_loan_special_other_amt']=loan_special[(loan_special.month_to_report<6) & (loan_special.specialTradeType=='其他')].specialTradeAmount.sum() # 近6个月特殊交易类型为展期，发生金额总和

        dict_out['m12_loan_special_settlement_ratio']=loan_special[(loan_special.month_to_report<12) & (loan_special.specialTradeType=='提前结清')].shape[0]/loan_special[(loan_special.month_to_report<12)].shape[0] if loan_special[(loan_special.month_to_report<12)].shape[0]>0 else np.nan # 近12个月特殊交易类型为展期，分布所占比例
        dict_out['m12_loan_special_settlement_amt']=loan_special[(loan_special.month_to_report<12) & (loan_special.specialTradeType=='提前结清')].specialTradeAmount.sum() # 近12个月特殊交易类型为展期，发生金额总和

        dict_out['m18_loan_special_guarantor_count']=loan_special[(loan_special.month_to_report<18) & (loan_special.specialTradeType=='担保人（第三方）代偿')].shape[0] # 近18个月特殊交易类型为展期，分布计数
        dict_out['m18_loan_special_guarantor_amt']=loan_special[(loan_special.month_to_report<18) & (loan_special.specialTradeType=='担保人（第三方）代偿')].specialTradeAmount.sum() # 近18个月特殊交易类型为展期，发生金额总和
        dict_out['m18_loan_special_settlement_ratio']=loan_special[(loan_special.month_to_report<18) & (loan_special.specialTradeType=='提前结清')].shape[0]/loan_special[(loan_special.month_to_report<18)].shape[0] if loan_special[(loan_special.month_to_report<18)].shape[0]>0 else np.nan # 近18个月特殊交易类型为展期，分布所占比例
        dict_out['m18_loan_special_settlement_amt']=loan_special[(loan_special.month_to_report<18) & (loan_special.specialTradeType=='提前结清')].specialTradeAmount.sum() # 近18个月特殊交易类型为展期，发生金额总和
        dict_out['m18_loan_special_other_amt']=loan_special[(loan_special.month_to_report<18) & (loan_special.specialTradeType=='其他')].specialTradeAmount.sum() # 近18个月特殊交易类型为展期，发生金额总和
        dict_out['m18_loan_special_prepayment_ratio']=loan_special[(loan_special.month_to_report<18) & (loan_special.specialTradeType=='提前还款')].shape[0]/loan_special[(loan_special.month_to_report<18)].shape[0] if loan_special[(loan_special.month_to_report<18)].shape[0]>0 else np.nan # 近18个月特殊交易类型为展期，分布所占比例

        dict_out['m24_loan_special_roll_count']=loan_special[(loan_special.month_to_report<24) & (loan_special.specialTradeType=='展期')].shape[0] # 近24个月特殊交易类型为展期，分布计数
        dict_out['m24_loan_special_guarantor_amt']=loan_special[(loan_special.month_to_report<24) & (loan_special.specialTradeType=='担保人（第三方）代偿')].specialTradeAmount.sum() # 近24个月特殊交易类型为展期，发生金额总和
        dict_out['m24_loan_special_settlement_count']=loan_special[(loan_special.month_to_report<24) & (loan_special.specialTradeType=='提前结清')].shape[0] # 近24个月特殊交易类型为展期，分布计数
        dict_out['m24_loan_special_settlement_ratio']=loan_special[(loan_special.month_to_report<24) & (loan_special.specialTradeType=='提前结清')].shape[0]/loan_special[(loan_special.month_to_report<24)].shape[0] if loan_special[(loan_special.month_to_report<24)].shape[0]>0 else np.nan # 近24个月特殊交易类型为展期，分布所占比例
        dict_out['m24_loan_special_settlement_amt']=loan_special[(loan_special.month_to_report<24) & (loan_special.specialTradeType=='提前结清')].specialTradeAmount.sum() # 近24个月特殊交易类型为展期，发生金额总和
        dict_out['m24_loan_special_prepayment_ratio']=loan_special[(loan_special.month_to_report<24) & (loan_special.specialTradeType=='提前还款')].shape[0]/loan_special[(loan_special.month_to_report<24)].shape[0] if loan_special[(loan_special.month_to_report<24)].shape[0]>0 else np.nan # 近24个月特殊交易类型为展期，分布所占比例


        dict_out['m30_loan_special_settlement_count']=loan_special[(loan_special.month_to_report<30) & (loan_special.specialTradeType=='提前结清')].shape[0] # 近30个月特殊交易类型为展期，分布计数
        dict_out['m30_loan_special_prepayment_ratio']=loan_special[(loan_special.month_to_report<30) & (loan_special.specialTradeType=='提前还款')].shape[0]/loan_special[(loan_special.month_to_report<30)].shape[0] if loan_special[(loan_special.month_to_report<30)].shape[0]>0 else np.nan # 近30个月特殊交易类型为展期，分布所占比例

        dict_out['m36_loan_special_settlement_count']=loan_special[(loan_special.month_to_report<36) & (loan_special.specialTradeType=='提前结清')].shape[0] # 近36个月特殊交易类型为展期，分布计数
        dict_out['m42_loan_special_other_ratio']=loan_special[(loan_special.month_to_report<42) & (loan_special.specialTradeType=='其他')].shape[0]/loan_special[(loan_special.month_to_report<42)].shape[0] if loan_special[(loan_special.month_to_report<42)].shape[0]>0 else np.nan # 近42个月特殊交易类型为展期，分布所占比例
        dict_out['m42_loan_special_prepayment_ratio']=loan_special[(loan_special.month_to_report<42) & (loan_special.specialTradeType=='提前还款')].shape[0]/loan_special[(loan_special.month_to_report<42)].shape[0] if loan_special[(loan_special.month_to_report<42)].shape[0]>0 else np.nan # 近42个月特殊交易类型为展期，分布所占比例
        dict_out['m42_loan_special_prepayment_amt']=loan_special[(loan_special.month_to_report<42) & (loan_special.specialTradeType=='提前还款')].specialTradeAmount.sum() # 近42个月特殊交易类型为展期，发生金额总和

        dict_out['m48_loan_special_settlement_ratio']=loan_special[(loan_special.month_to_report<48) & (loan_special.specialTradeType=='提前结清')].shape[0]/loan_special[(loan_special.month_to_report<48)].shape[0] if loan_special[(loan_special.month_to_report<48)].shape[0]>0 else np.nan # 近48个月特殊交易类型为展期，分布所占比例
        dict_out['m48_loan_special_prepayment_ratio']=loan_special[(loan_special.month_to_report<48) & (loan_special.specialTradeType=='提前还款')].shape[0]/loan_special[(loan_special.month_to_report<48)].shape[0] if loan_special[(loan_special.month_to_report<48)].shape[0]>0 else np.nan # 近48个月特殊交易类型为展期，分布所占比例
        dict_out['m54_loan_special_settlement_ratio']=loan_special[(loan_special.month_to_report<54) & (loan_special.specialTradeType=='提前结清')].shape[0]/loan_special[(loan_special.month_to_report<54)].shape[0] if loan_special[(loan_special.month_to_report<54)].shape[0]>0 else np.nan # 近54个月特殊交易类型为展期，分布所占比例
        dict_out['m54_loan_special_other_ratio']=loan_special[(loan_special.month_to_report<54) & (loan_special.specialTradeType=='其他')].shape[0]/loan_special[(loan_special.month_to_report<54)].shape[0] if loan_special[(loan_special.month_to_report<54)].shape[0]>0 else np.nan # 近54个月特殊交易类型为展期，分布所占比例
        dict_out['m54_loan_special_prepayment_ratio']=loan_special[(loan_special.month_to_report<54) & (loan_special.specialTradeType=='提前还款')].shape[0]/loan_special[(loan_special.month_to_report<54)].shape[0] if loan_special[(loan_special.month_to_report<54)].shape[0]>0 else np.nan # 近54个月特殊交易类型为展期，分布所占比例
        dict_out['m54_loan_special_prepayment_amt']=loan_special[(loan_special.month_to_report<54) & (loan_special.specialTradeType=='提前还款')].specialTradeAmount.sum() # 近54个月特殊交易类型为展期，发生金额总和

        dict_out['m60_loan_special_other_ratio']=loan_special[(loan_special.month_to_report<60) & (loan_special.specialTradeType=='其他')].shape[0]/loan_special[(loan_special.month_to_report<60)].shape[0] if loan_special[(loan_special.month_to_report<60)].shape[0]>0 else np.nan # 近60个月特殊交易类型为展期，分布所占比例
        dict_out['m120_loan_special_other_ratio']=loan_special[(loan_special.month_to_report<120) & (loan_special.specialTradeType=='其他')].shape[0]/loan_special[(loan_special.month_to_report<120)].shape[0] if loan_special[(loan_special.month_to_report<120)].shape[0]>0 else np.nan # 近120个月特殊交易类型为展期，分布所占比例

        dict_out['m180_loan_special_settlement_amt']=loan_special[(loan_special.month_to_report<180) & (loan_special.specialTradeType=='提前结清')].specialTradeAmount.sum() # 近180个月特殊交易类型为展期，发生金额总和

        dict_out['m240_loan_special_other_ratio']=loan_special[(loan_special.month_to_report<240) & (loan_special.specialTradeType=='其他')].shape[0]/loan_special[(loan_special.month_to_report<240)].shape[0] if loan_special[(loan_special.month_to_report<240)].shape[0]>0 else np.nan # 近240个月特殊交易类型为展期，分布所占比例

        dict_out['m300_loan_special_other_ratio']=loan_special[(loan_special.month_to_report<300) & (loan_special.specialTradeType=='其他')].shape[0]/loan_special[(loan_special.month_to_report<300)].shape[0] if loan_special[(loan_special.month_to_report<300)].shape[0]>0 else np.nan # 近300个月特殊交易类型为展期，分布所占比例
        dict_out['m360_loan_special_other_ratio']=loan_special[(loan_special.month_to_report<360) & (loan_special.specialTradeType=='其他')].shape[0]/loan_special[(loan_special.month_to_report<360)].shape[0] if loan_special[(loan_special.month_to_report<360)].shape[0]>0 else np.nan # 近360个月特殊交易类型为展期，分布所占比例


    #公共信息-法院强制执行记录
    public_court=copy.copy(dict_in['cc_rh_report_public_court'])



    #查询明细
    query_detail=copy.copy(dict_in['cc_rh_report_query_detail'])

    if len(query_detail)>0:
        query_detail['queryOperator_new']=query_detail.apply(lambda x:re.findall('[\u4e00-\u9fa5]*',x.queryOperator)[0] if len(re.findall('[\u4e00-\u9fa5]*',x.queryOperator))>0 else np.nan,axis=1)
        query_detail['queryDate_to_report']=query_detail.apply(lambda x:monthdelta(x['queryDate'],dict_in['cc_rh_report']['reportTime']),axis=1)
        query_detail['mon_report']=query_detail.apply(lambda x:(x.queryDate_to_report//6+1)*6 ,axis=1 )

        dict_out['last_query_reason']=query_detail.queryReason[0] #上一次查询原因
        dict_out['last_query_to_report_days']=daysdelta(query_detail.queryDate.values[0], dict_in['cc_rh_report']['reportTime']) #上一次查询距离这次查询的时间间隔（天）



        dict_out['query_detail_loan_approval_count']=sum(query_detail.queryReason=='贷款审批')  # 查询原因为贷款审批，分布计数
        dict_out['query_detail_trust_company_ratio']=sum(query_detail.queryOperator_new=='信托公司')/len(query_detail)  # 查询机构为信托公司，分布所占比例
        dict_out['query_detail_small_loan_count']=sum(query_detail.queryOperator_new=='小额贷款公司')  # 查询机构为小额贷款公司，分布计数
        dict_out['query_detail_financing_guarantee_count']=sum(query_detail.queryOperator_new=='融资担保公司')  # 查询机构为融资担保公司，分布计数
        dict_out['query_detail_lease_finance_ratio']=sum(query_detail.queryOperator_new=='融资租赁公司')/len(query_detail)  # 查询机构为融资租赁公司，分布所占比例

        dict_out['query_detail_mon_report_giniimpurity']=giniimpurity(query_detail.mon_report.values)  # 查询距报告时间gini 系数
        dict_out['query_detail_mon_report_06_ratio']=sum(query_detail.mon_report==6)/len(query_detail)  # 查询距报告时间在0-6个月，分布所占比例
        dict_out['query_detail_mon_report_12_count']=sum(query_detail.mon_report==12)  # 查询距报告时间在6-12个月，分布计数
        dict_out['query_detail_mon_report_18_count']=sum(query_detail.mon_report==18)  # 查询距报告时间在12-18个月，分布计数
        dict_out['query_detail_mon_report_18_ratio']=sum(query_detail.mon_report==18)/len(query_detail)  # 查询距报告时间在12-18个月，分布所占比例

        dict_out['query_detail_queryDate_to_report_01_ratio']=sum(query_detail.queryDate_to_report<1)/len(query_detail)  # 查询距报告时间在1个月内，分布所占比例
        dict_out['query_detail_queryDate_to_report_03_ratio']=sum(query_detail.queryDate_to_report<3)/len(query_detail)  # 查询距报告时间在3个月内，分布所占比例
        dict_out['query_detail_date_to_report_mean']= query_detail['queryDate_to_report'].mean() # 查询时间距离报告月份mean

        query_detail_m01 = query_detail[query_detail.queryDate_to_report<1]


        query_detail_m03 = query_detail[query_detail.queryDate_to_report<3]
        if len(query_detail_m03)>0:
            dict_out['m03_query_detail_card_approval_ratio']=sum(query_detail_m03.queryReason=='信用卡审批')/len(query_detail_m03)  # 近03个月 查询原因为信用卡审批，分布所占比例
            dict_out['m03_query_detail_guarantee_ratio']=sum(query_detail_m03.queryReason=='担保资格审查')/len(query_detail_m03)  # 近03个月 查询原因为担保资格审查，分布所占比例
            dict_out['m03_query_detail_date_to_report_mean']= query_detail_m03['queryDate_to_report'].mean() # 近03个月 查询时间距离报告月份mean

        query_detail_m06 = query_detail[query_detail.queryDate_to_report<6]
        if len(query_detail_m06)>0:
            dict_out['m06_query_detail_reason_giniimpurity']=giniimpurity(query_detail_m06.queryReason.values)  # 近06个月 查询原因gini 系数
            dict_out['m06_query_detail_card_approval_count']=sum(query_detail_m06.queryReason=='信用卡审批')  # 近06个月 查询原因为信用卡审批，分布计数
            dict_out['m06_query_detail_card_approval_ratio']=sum(query_detail_m06.queryReason=='信用卡审批')/len(query_detail_m06)  # 近06个月 查询原因为信用卡审批，分布所占比例

            dict_out['m06_query_detail_date_to_report_mean']= query_detail_m06['queryDate_to_report'].mean() # 近06个月 查询时间距离报告月份mean

        query_detail_m12 = query_detail[query_detail.queryDate_to_report<12]
        if len(query_detail_m12)>0:
            dict_out['m12_query_detail_cnt']=len(query_detail_m12)  # 近12个月 查询记录信息单元个数
            dict_out['m12_query_detail_reason_giniimpurity']=giniimpurity(query_detail_m12.queryReason.values)  # 近12个月 查询原因gini 系数
            dict_out['m12_query_detail_card_approval_ratio']=sum(query_detail_m12.queryReason=='信用卡审批')/len(query_detail_m12)  # 近12个月 查询原因为信用卡审批，分布所占比例
            dict_out['m12_query_detail_guarantee_ratio']=sum(query_detail_m12.queryReason=='担保资格审查')/len(query_detail_m12)  # 近12个月 查询原因为担保资格审查，分布所占比例
            dict_out['m12_query_detail_funding_approval_ratio']=sum(query_detail_m12.queryReason=='融资审批')/len(query_detail_m12)  # 近12个月 查询原因为融资审批，分布所占比例
            dict_out['m12_query_detail_small_loan_count']=sum(query_detail_m12.queryOperator_new=='小额贷款公司')  # 近12个月 查询机构为小额贷款公司，分布计数
            dict_out['m12_query_detail_mon_report_giniimpurity']=giniimpurity(query_detail_m12.mon_report.values)  # 近12个月 查询距报告时间gini 系数
            dict_out['m12_query_detail_mon_report_12_count']=sum(query_detail_m12.mon_report==12)  # 近12个月 查询距报告时间在6-12个月，分布计数
            dict_out['m12_query_detail_mon_report_12_ratio']=sum(query_detail_m12.mon_report==12)/len(query_detail_m12)  # 近12个月 查询距报告时间在6-12个月，分布所占比例

            dict_out['m12_query_detail_date_to_report_mean']= query_detail_m12['queryDate_to_report'].mean() # 近12个月 查询时间距离报告月份mean

        query_detail_m24 = query_detail[query_detail.queryDate_to_report<24]
        if len(query_detail_m24)>0:
            dict_out['m24_query_detail_cnt']=len(query_detail_m24)  # 近24个月 查询记录信息单元个数
            dict_out['m24_query_detail_loan_approval_count']=sum(query_detail_m24.queryReason=='贷款审批')  # 近24个月 查询原因为贷款审批，分布计数
            dict_out['m24_query_detail_funding_approval_ratio']=sum(query_detail_m24.queryReason=='融资审批')/len(query_detail_m24)  # 近24个月 查询原因为融资审批，分布所占比例
            dict_out['m24_query_detail_commercial_bank_count']=sum(query_detail_m24.queryOperator_new=='商业银行')  # 近24个月 查询机构为商业银行，分布计数
            dict_out['m24_query_detail_commercial_bank_ratio']=sum(query_detail_m24.queryOperator_new=='商业银行')/len(query_detail_m24)  # 近24个月 查询机构为商业银行，分布所占比例
            dict_out['m24_query_detail_small_loan_count']=sum(query_detail_m24.queryOperator_new=='小额贷款公司')  # 近24个月 查询机构为小额贷款公司，分布计数
            dict_out['m24_query_detail_small_loan_ratio']=sum(query_detail_m24.queryOperator_new=='小额贷款公司')/len(query_detail_m24)  # 近24个月 查询机构为小额贷款公司，分布所占比例
            dict_out['m24_query_detail_lease_finance_ratio']=sum(query_detail_m24.queryOperator_new=='融资租赁公司')/len(query_detail_m24)  # 近24个月 查询机构为融资租赁公司，分布所占比例
            dict_out['m24_query_detail_mon_report_giniimpurity']=giniimpurity(query_detail_m24.mon_report.values)  # 近24个月 查询距报告时间gini 系数
            dict_out['m24_query_detail_mon_report_12_ratio']=sum(query_detail_m24.mon_report==12)/len(query_detail_m24)  # 近24个月 查询距报告时间在6-12个月，分布所占比例
            dict_out['m24_query_detail_mon_report_24_ratio']=sum(query_detail_m24.mon_report==24)/len(query_detail_m24)  # 近24个月 查询距报告时间在18-24个月，分布所占比例

            dict_out['m24_query_detail_queryDate_to_report_06_ratio']=sum(query_detail_m24.queryDate_to_report<6)/len(query_detail_m24)  # 近24个月 查询距报告时间在6个月内，分布所占比例
            dict_out['m24_query_detail_date_to_report_mean']= query_detail_m24['queryDate_to_report'].mean() # 近24个月 查询时间距离报告月份mean

        query_detail_m36 = query_detail[query_detail.queryDate_to_report<36]
        if len(query_detail_m36)>0:
            dict_out['m36_query_detail_loan_approval_count']=sum(query_detail_m36.queryReason=='贷款审批')  # 近36个月 查询原因为贷款审批，分布计数
            dict_out['m36_query_detail_card_approval_ratio']=sum(query_detail_m36.queryReason=='信用卡审批')/len(query_detail_m36)  # 近36个月 查询原因为信用卡审批，分布所占比例
            dict_out['m36_query_detail_commercial_bank_count']=sum(query_detail_m36.queryOperator_new=='商业银行')  # 近36个月 查询机构为商业银行，分布计数
            dict_out['m36_query_detail_small_loan_ratio']=sum(query_detail_m36.queryOperator_new=='小额贷款公司')/len(query_detail_m36)  # 近36个月 查询机构为小额贷款公司，分布所占比例
            dict_out['m36_query_detail_financing_guarantee_ratio']=sum(query_detail_m36.queryOperator_new=='融资担保公司')/len(query_detail_m36)  # 近36个月 查询机构为融资担保公司，分布所占比例

            dict_out['m36_query_detail_mon_report_06_count']=sum(query_detail_m36.mon_report==6)  # 近36个月 查询距报告时间在0-6个月，分布计数
            dict_out['m36_query_detail_mon_report_06_ratio']=sum(query_detail_m36.mon_report==6)/len(query_detail_m36)  # 近36个月 查询距报告时间在0-6个月，分布所占比例
            dict_out['m36_query_detail_mon_report_12_count']=sum(query_detail_m36.mon_report==12)  # 近36个月 查询距报告时间在6-12个月，分布计数
            dict_out['m36_query_detail_mon_report_24_count']=sum(query_detail_m36.mon_report==24)  # 近36个月 查询距报告时间在18-24个月，分布计数

        query_detail_m48 = query_detail[query_detail.queryDate_to_report<48]
        if len(query_detail_m48)>0:
            dict_out['m48_query_detail_reason_giniimpurity']=giniimpurity(query_detail_m48.queryReason.values)  # 近48个月 查询原因gini 系数
            dict_out['m48_query_detail_card_approval_ratio']=sum(query_detail_m48.queryReason=='信用卡审批')/len(query_detail_m48)  # 近48个月 查询原因为信用卡审批，分布所占比例
            dict_out['m48_query_detail_operator_giniimpurity']=giniimpurity(query_detail_m48.queryOperator_new.values)  # 近48个月 查询机构gini 系数
            dict_out['m48_query_detail_insurance_company_ratio']=sum(query_detail_m48.queryOperator_new=='保险公司')/len(query_detail_m48)  # 近48个月 查询机构为保险公司，分布所占比例
            dict_out['m48_query_detail_trust_company_ratio']=sum(query_detail_m48.queryOperator_new=='信托公司')/len(query_detail_m48)  # 近48个月 查询机构为信托公司，分布所占比例
            dict_out['m48_query_detail_commercial_bank_count']=sum(query_detail_m48.queryOperator_new=='商业银行')  # 近48个月 查询机构为商业银行，分布计数
            dict_out['m48_query_detail_lease_finance_ratio']=sum(query_detail_m48.queryOperator_new=='融资租赁公司')/len(query_detail_m48)  # 近48个月 查询机构为融资租赁公司，分布所占比例

            dict_out['m48_query_detail_mon_report_giniimpurity']=giniimpurity(query_detail_m48.mon_report.values)  # 近48个月 查询距报告时间gini 系数
            dict_out['m48_query_detail_mon_report_24_ratio']=sum(query_detail_m48.mon_report==24)/len(query_detail_m48)  # 近48个月 查询距报告时间在18-24个月，分布所占比例
            dict_out['m48_query_detail_queryDate_to_report_06_ratio']=sum(query_detail_m48.queryDate_to_report<6)/len(query_detail_m48)  # 近48个月 查询距报告时间在6个月内，分布所占比例
            dict_out['m48_query_detail_queryDate_to_report_12_ratio']=sum(query_detail_m48.queryDate_to_report<12)/len(query_detail_m48)  # 近48个月 查询距报告时间在12个月内，分布所占比例

        query_detail_m60 = query_detail[query_detail.queryDate_to_report<60]
        if len(query_detail_m60)>0:
            dict_out['m60_query_detail_reason_giniimpurity']=giniimpurity(query_detail_m60.queryReason.values)  # 近60个月 查询原因gini 系数
            dict_out['m60_query_detail_guarantee_ratio']=sum(query_detail_m60.queryReason=='担保资格审查')/len(query_detail_m60)  # 近60个月 查询原因为担保资格审查，分布所占比例

            dict_out['m60_query_detail_commercial_bank_ratio']=sum(query_detail_m60.queryOperator_new=='商业银行')/len(query_detail_m60)  # 近60个月 查询机构为商业银行，分布所占比例

            dict_out['m60_query_detail_mon_report_giniimpurity']=giniimpurity(query_detail_m60.mon_report.values)  # 近60个月 查询距报告时间gini 系数    
            dict_out['m60_query_detail_mon_report_06_ratio']=sum(query_detail_m60.mon_report==6)/len(query_detail_m60)  # 近60个月 查询距报告时间在0-6个月，分布所占比例

            dict_out['m60_query_detail_queryDate_to_report_12_ratio']=sum(query_detail_m60.queryDate_to_report<12)/len(query_detail_m60)  # 近60个月 查询距报告时间在12个月内，分布所占比例

            dict_out['m60_query_detail_date_to_report_mean']= query_detail_m60['queryDate_to_report'].mean() # 近60个月 查询时间距离报告月份mean


        query_detail_loan = query_detail[query_detail.queryReason=='贷款审批']
        if len(query_detail_loan)>0:
            dict_out['query_detail_loan_cnt']=len(query_detail_loan)  #贷款审批 查询记录信息单元个数


            dict_out['query_detail_loan_commercial_bank_count']=sum(query_detail_loan.queryOperator_new=='商业银行')  #贷款审批 查询机构为商业银行，分布计数

            dict_out['query_detail_loan_small_loan_count']=sum(query_detail_loan.queryOperator_new=='小额贷款公司')  #贷款审批 查询机构为小额贷款公司，分布计数

            dict_out['query_detail_loan_financing_guarantee_ratio']=sum(query_detail_loan.queryOperator_new=='融资担保公司')/len(query_detail_loan)  #贷款审批 查询机构为融资担保公司，分布所占比例


            dict_out['query_detail_loan_mon_report_nunique']=query_detail_loan.mon_report.nunique()  #贷款审批不同查询机构数
            dict_out['query_detail_loan_mon_report_giniimpurity']=giniimpurity(query_detail_loan.mon_report.values)  #贷款审批 查询距报告时间gini 系数    
            dict_out['query_detail_loan_mon_report_06_ratio']=sum(query_detail_loan.mon_report==6)/len(query_detail_loan)  #贷款审批 查询距报告时间在0-6个月，分布所占比例
            dict_out['query_detail_loan_mon_report_12_ratio']=sum(query_detail_loan.mon_report==12)/len(query_detail_loan)  #贷款审批 查询距报告时间在6-12个月，分布所占比例
            dict_out['query_detail_loan_mon_report_18_count']=sum(query_detail_loan.mon_report==18)  #贷款审批 查询距报告时间在12-18个月，分布计数
            dict_out['query_detail_loan_mon_report_18_ratio']=sum(query_detail_loan.mon_report==18)/len(query_detail_loan)  #贷款审批 查询距报告时间在12-18个月，分布所占比例
            dict_out['query_detail_loan_mon_report_24_count']=sum(query_detail_loan.mon_report==24)  #贷款审批 查询距报告时间在18-24个月，分布计数
            dict_out['query_detail_loan_mon_report_24_ratio']=sum(query_detail_loan.mon_report==24)/len(query_detail_loan)  #贷款审批 查询距报告时间在18-24个月，分布所占比例


            dict_out['query_detail_loan_queryDate_to_report_01_ratio']=sum(query_detail_loan.queryDate_to_report<1)/len(query_detail_loan)  #贷款审批 查询距报告时间在1个月内，分布所占比例
            dict_out['query_detail_loan_queryDate_to_report_03_ratio']=sum(query_detail_loan.queryDate_to_report<3)/len(query_detail_loan)  #贷款审批 查询距报告时间在3个月内，分布所占比例
            dict_out['query_detail_loan_queryDate_to_report_06_ratio']=sum(query_detail_loan.queryDate_to_report<6)/len(query_detail_loan)  #贷款审批 查询距报告时间在6个月内，分布所占比例
            dict_out['query_detail_loan_queryDate_to_report_12_ratio']=sum(query_detail_loan.queryDate_to_report<12)/len(query_detail_loan)  #贷款审批 查询距报告时间在12个月内，分布所占比例

            dict_out['query_detail_loan_date_to_report_max']= query_detail_loan['queryDate_to_report'].max() #贷款审批 查询时间距离报告月份max
            dict_out['query_detail_loan_date_to_report_mean']= query_detail_loan['queryDate_to_report'].mean() #贷款审批 查询时间距离报告月份mean

            query_detail_loan_m01 = query_detail_loan[query_detail_loan.queryDate_to_report<1]
            if len(query_detail_loan_m01)>0:
                dict_out['m01_query_detail_loan_financing_guarantee_count']=sum(query_detail_loan_m01.queryOperator_new=='融资担保公司')  #贷款审批 近01个月 查询机构为融资担保公司，分布计数

            query_detail_loan_m03 = query_detail_loan[query_detail_loan.queryDate_to_report<3]
            if len(query_detail_loan_m03)>0:
                dict_out['m03_query_detail_loan_commercial_bank_count']=sum(query_detail_loan_m03.queryOperator_new=='商业银行')  #贷款审批 近03个月 查询机构为商业银行，分布计数
                dict_out['m03_query_detail_loan_commercial_bank_ratio']=sum(query_detail_loan_m03.queryOperator_new=='商业银行')/len(query_detail_loan_m03)  #贷款审批 近03个月 查询机构为商业银行，分布所占比例
                dict_out['m03_query_detail_loan_consumer_finance_ratio']=sum(query_detail_loan_m03.queryOperator_new=='消费金融公司')/len(query_detail_loan_m03)  #贷款审批 近03个月 查询机构为消费金融公司，分布所占比例
                dict_out['m03_query_detail_loan_date_to_report_mean']= query_detail_loan_m03['queryDate_to_report'].mean() #贷款审批 近03个月 查询时间距离报告月份mean

            query_detail_loan_m06 = query_detail_loan[query_detail_loan.queryDate_to_report<6]
            if len(query_detail_loan_m06)>0:
                dict_out['m06_query_detail_loan_commercial_bank_count']=sum(query_detail_loan_m06.queryOperator_new=='商业银行')  #贷款审批 近06个月 查询机构为商业银行，分布计数
                dict_out['m06_query_detail_loan_commercial_bank_ratio']=sum(query_detail_loan_m06.queryOperator_new=='商业银行')/len(query_detail_loan_m06)  #贷款审批 近06个月 查询机构为商业银行，分布所占比例
                dict_out['m06_query_detail_loan_financing_guarantee_ratio']=sum(query_detail_loan_m06.queryOperator_new=='融资担保公司')/len(query_detail_loan_m06)  #贷款审批 近06个月 查询机构为融资担保公司，分布所占比例

                dict_out['m06_query_detail_loan_date_to_report_max']= query_detail_loan_m06['queryDate_to_report'].max() #贷款审批 近06个月 查询时间距离报告月份max
                dict_out['m06_query_detail_loan_date_to_report_mean']= query_detail_loan_m06['queryDate_to_report'].mean() #贷款审批 近06个月 查询时间距离报告月份mean

            query_detail_loan_m12 = query_detail_loan[query_detail_loan.queryDate_to_report<12]
            if len(query_detail_loan_m12)>0:
                dict_out['m12_query_detail_loan_cnt']=len(query_detail_loan_m12)  #贷款审批 近12个月 查询记录信息单元个数


                dict_out['m12_query_detail_loan_small_loan_count']=sum(query_detail_loan_m12.queryOperator_new=='小额贷款公司')  #贷款审批 近12个月 查询机构为小额贷款公司，分布计数

                dict_out['m12_query_detail_loan_consumer_finance_ratio']=sum(query_detail_loan_m12.queryOperator_new=='消费金融公司')/len(query_detail_loan_m12)  #贷款审批 近12个月 查询机构为消费金融公司，分布所占比例
                # dict_out['m12_query_detail_loan_financing_guarantee_count']=sum(query_detail_loan_m12.queryOperator_new=='融资担保公司')  #贷款审批 近12个月 查询机构为融资担保公司，分布计数
                dict_out['m12_query_detail_loan_financing_guarantee_ratio']=sum(query_detail_loan_m12.queryOperator_new=='融资担保公司')/len(query_detail_loan_m12)  #贷款审批 近12个月 查询机构为融资担保公司，分布所占比例

                dict_out['m12_query_detail_loan_mon_report_06_count']=sum(query_detail_loan_m12.mon_report==6)  #贷款审批 近12个月 查询距报告时间在0-6个月，分布计数
                dict_out['m12_query_detail_loan_mon_report_06_ratio']=sum(query_detail_loan_m12.mon_report==6)/len(query_detail_loan_m12)  #贷款审批 近12个月 查询距报告时间在0-6个月，分布所占比例

                dict_out['m12_query_detail_loan_queryDate_to_report_06_ratio']=sum(query_detail_loan_m12.queryDate_to_report<6)/len(query_detail_loan_m12)  #贷款审批 近12个月 查询距报告时间在6个月内，分布所占比例

                dict_out['m12_query_detail_loan_date_to_report_mean']= query_detail_loan_m12['queryDate_to_report'].mean() #贷款审批 近12个月 查询时间距离报告月份mean

            query_detail_loan_m24 = query_detail_loan[query_detail_loan.queryDate_to_report<24]
            if len(query_detail_loan_m24)>0:

                dict_out['m24_query_detail_loan_trust_company_count']=sum(query_detail_loan_m24.queryOperator_new=='信托公司')  #贷款审批 近24个月 查询机构为信托公司，分布计数
                dict_out['m24_query_detail_loan_trust_company_ratio']=sum(query_detail_loan_m24.queryOperator_new=='信托公司')/len(query_detail_loan_m24)  #贷款审批 近24个月 查询机构为信托公司，分布所占比例

                dict_out['m24_query_detail_loan_consumer_finance_ratio']=sum(query_detail_loan_m24.queryOperator_new=='消费金融公司')/len(query_detail_loan_m24)  #贷款审批 近24个月 查询机构为消费金融公司，分布所占比例
                dict_out['m24_query_detail_loan_financing_guarantee_ratio']=sum(query_detail_loan_m24.queryOperator_new=='融资担保公司')/len(query_detail_loan_m24)  #贷款审批 近24个月 查询机构为融资担保公司，分布所占比例

                dict_out['m24_query_detail_loan_mon_report_giniimpurity']=giniimpurity(query_detail_loan_m24.mon_report.values)  #贷款审批 近24个月 查询距报告时间gini 系数    
                dict_out['m24_query_detail_loan_mon_report_06_ratio']=sum(query_detail_loan_m24.mon_report==6)/len(query_detail_loan_m24)  #贷款审批 近24个月 查询距报告时间在0-6个月，分布所占比例
                dict_out['m24_query_detail_loan_mon_report_12_count']=sum(query_detail_loan_m24.mon_report==12)  #贷款审批 近24个月 查询距报告时间在6-12个月，分布计数
                dict_out['m24_query_detail_loan_mon_report_12_ratio']=sum(query_detail_loan_m24.mon_report==12)/len(query_detail_loan_m24)  #贷款审批 近24个月 查询距报告时间在6-12个月，分布所占比例
                dict_out['m24_query_detail_loan_mon_report_18_count']=sum(query_detail_loan_m24.mon_report==18)  #贷款审批 近24个月 查询距报告时间在12-18个月，分布计数
                dict_out['m24_query_detail_loan_mon_report_24_count']=sum(query_detail_loan_m24.mon_report==24)  #贷款审批 近24个月 查询距报告时间在18-24个月，分布计数
                dict_out['m24_query_detail_loan_mon_report_24_ratio']=sum(query_detail_loan_m24.mon_report==24)/len(query_detail_loan_m24)  #贷款审批 近24个月 查询距报告时间在18-24个月，分布所占比例


                dict_out['m24_query_detail_loan_queryDate_to_report_06_ratio']=sum(query_detail_loan_m24.queryDate_to_report<6)/len(query_detail_loan_m24)  #贷款审批 近24个月 查询距报告时间在6个月内，分布所占比例
                dict_out['m24_query_detail_loan_queryDate_to_report_12_ratio']=sum(query_detail_loan_m24.queryDate_to_report<12)/len(query_detail_loan_m24)  #贷款审批 近24个月 查询距报告时间在12个月内，分布所占比例

                dict_out['m24_query_detail_loan_date_to_report_max']= query_detail_loan_m24['queryDate_to_report'].max() #贷款审批 近24个月 查询时间距离报告月份max
                dict_out['m24_query_detail_loan_date_to_report_mean']= query_detail_loan_m24['queryDate_to_report'].mean() #贷款审批 近24个月 查询时间距离报告月份mean

            query_detail_loan_m36 = query_detail_loan[query_detail_loan.queryDate_to_report<36]
            if len(query_detail_loan_m36)>0:
                dict_out['m36_query_detail_loan_cnt']=len(query_detail_loan_m36)  #贷款审批 近36个月 查询记录信息单元个数


                dict_out['m36_query_detail_loan_trust_company_count']=sum(query_detail_loan_m36.queryOperator_new=='信托公司')  #贷款审批 近36个月 查询机构为信托公司，分布计数
                dict_out['m36_query_detail_loan_trust_company_ratio']=sum(query_detail_loan_m36.queryOperator_new=='信托公司')/len(query_detail_loan_m36)  #贷款审批 近36个月 查询机构为信托公司，分布所占比例

                dict_out['m36_query_detail_loan_financing_guarantee_ratio']=sum(query_detail_loan_m36.queryOperator_new=='融资担保公司')/len(query_detail_loan_m36)  #贷款审批 近36个月 查询机构为融资担保公司，分布所占比例


                dict_out['m36_query_detail_loan_mon_report_nunique']=query_detail_loan_m36.mon_report.nunique()  #贷款审批 近36个月不同查询机构数
                dict_out['m36_query_detail_loan_mon_report_giniimpurity']=giniimpurity(query_detail_loan_m36.mon_report.values)  #贷款审批 近36个月 查询距报告时间gini 系数    
                dict_out['m36_query_detail_loan_mon_report_06_ratio']=sum(query_detail_loan_m36.mon_report==6)/len(query_detail_loan_m36)  #贷款审批 近36个月 查询距报告时间在0-6个月，分布所占比例

                dict_out['m36_query_detail_loan_mon_report_18_count']=sum(query_detail_loan_m36.mon_report==18)  #贷款审批 近36个月 查询距报告时间在12-18个月，分布计数
                dict_out['m36_query_detail_loan_mon_report_24_count']=sum(query_detail_loan_m36.mon_report==24)  #贷款审批 近36个月 查询距报告时间在18-24个月，分布计数
                dict_out['m36_query_detail_loan_mon_report_24_ratio']=sum(query_detail_loan_m36.mon_report==24)/len(query_detail_loan_m36)  #贷款审批 近36个月 查询距报告时间在18-24个月，分布所占比例

                dict_out['m36_query_detail_loan_queryDate_to_report_06_ratio']=sum(query_detail_loan_m36.queryDate_to_report<6)/len(query_detail_loan_m36)  #贷款审批 近36个月 查询距报告时间在6个月内，分布所占比例

                dict_out['m36_query_detail_loan_date_to_report_max']= query_detail_loan_m36['queryDate_to_report'].max() #贷款审批 近36个月 查询时间距离报告月份max
                dict_out['m36_query_detail_loan_date_to_report_mean']= query_detail_loan_m36['queryDate_to_report'].mean() #贷款审批 近36个月 查询时间距离报告月份mean

            query_detail_loan_m48 = query_detail_loan[query_detail_loan.queryDate_to_report<48]
            if len(query_detail_loan_m48)>0:
                dict_out['m48_query_detail_loan_cnt']=len(query_detail_loan_m48)  #贷款审批 近48个月 查询记录信息单元个数


                dict_out['m48_query_detail_loan_trust_company_ratio']=sum(query_detail_loan_m48.queryOperator_new=='信托公司')/len(query_detail_loan_m48)  #贷款审批 近48个月 查询机构为信托公司，分布所占比例

                dict_out['m48_query_detail_loan_small_loan_count']=sum(query_detail_loan_m48.queryOperator_new=='小额贷款公司')  #贷款审批 近48个月 查询机构为小额贷款公司，分布计数

                dict_out['m48_query_detail_loan_financing_guarantee_ratio']=sum(query_detail_loan_m48.queryOperator_new=='融资担保公司')/len(query_detail_loan_m48)  #贷款审批 近48个月 查询机构为融资担保公司，分布所占比例

                dict_out['m48_query_detail_loan_mon_report_giniimpurity']=giniimpurity(query_detail_loan_m48.mon_report.values)  #贷款审批 近48个月 查询距报告时间gini 系数    
                dict_out['m48_query_detail_loan_mon_report_06_ratio']=sum(query_detail_loan_m48.mon_report==6)/len(query_detail_loan_m48)  #贷款审批 近48个月 查询距报告时间在0-6个月，分布所占比例

                dict_out['m48_query_detail_loan_mon_report_18_count']=sum(query_detail_loan_m48.mon_report==18)  #贷款审批 近48个月 查询距报告时间在12-18个月，分布计数
                dict_out['m48_query_detail_loan_mon_report_24_count']=sum(query_detail_loan_m48.mon_report==24)  #贷款审批 近48个月 查询距报告时间在18-24个月，分布计数

                dict_out['m48_query_detail_loan_queryDate_to_report_06_ratio']=sum(query_detail_loan_m48.queryDate_to_report<6)/len(query_detail_loan_m48)  #贷款审批 近48个月 查询距报告时间在6个月内，分布所占比例

                dict_out['m48_query_detail_loan_date_to_report_max']= query_detail_loan_m48['queryDate_to_report'].max() #贷款审批 近48个月 查询时间距离报告月份max
                dict_out['m48_query_detail_loan_date_to_report_mean']= query_detail_loan_m48['queryDate_to_report'].mean() #贷款审批 近48个月 查询时间距离报告月份mean

            query_detail_loan_m60 = query_detail_loan[query_detail_loan.queryDate_to_report<60]
            if len(query_detail_loan_m60)>0:
                dict_out['m60_query_detail_loan_cnt']=len(query_detail_loan_m60)  #贷款审批 近60个月 查询记录信息单元个数


                dict_out['m60_query_detail_loan_small_loan_count']=sum(query_detail_loan_m60.queryOperator_new=='小额贷款公司')  #贷款审批 近60个月 查询机构为小额贷款公司，分布计数

                dict_out['m60_query_detail_loan_consumer_finance_ratio']=sum(query_detail_loan_m60.queryOperator_new=='消费金融公司')/len(query_detail_loan_m60)  #贷款审批 近60个月 查询机构为消费金融公司，分布所占比例
                dict_out['m60_query_detail_loan_financing_guarantee_ratio']=sum(query_detail_loan_m60.queryOperator_new=='融资担保公司')/len(query_detail_loan_m60)  #贷款审批 近60个月 查询机构为融资担保公司，分布所占比例

                dict_out['m60_query_detail_loan_mon_report_giniimpurity']=giniimpurity(query_detail_loan_m60.mon_report.values)  #贷款审批 近60个月 查询距报告时间gini 系数    
                dict_out['m60_query_detail_loan_mon_report_06_ratio']=sum(query_detail_loan_m60.mon_report==6)/len(query_detail_loan_m60)  #贷款审批 近60个月 查询距报告时间在0-6个月，分布所占比例
                dict_out['m60_query_detail_loan_mon_report_12_count']=sum(query_detail_loan_m60.mon_report==12)  #贷款审批 近60个月 查询距报告时间在6-12个月，分布计数
                dict_out['m60_query_detail_loan_mon_report_12_ratio']=sum(query_detail_loan_m60.mon_report==12)/len(query_detail_loan_m60)  #贷款审批 近60个月 查询距报告时间在6-12个月，分布所占比例
                dict_out['m60_query_detail_loan_mon_report_18_count']=sum(query_detail_loan_m60.mon_report==18)  #贷款审批 近60个月 查询距报告时间在12-18个月，分布计数
                dict_out['m60_query_detail_loan_mon_report_24_count']=sum(query_detail_loan_m60.mon_report==24)  #贷款审批 近60个月 查询距报告时间在18-24个月，分布计数
                dict_out['m60_query_detail_loan_mon_report_24_ratio']=sum(query_detail_loan_m60.mon_report==24)/len(query_detail_loan_m60)  #贷款审批 近60个月 查询距报告时间在18-24个月，分布所占比例

                dict_out['m60_query_detail_loan_queryDate_to_report_06_ratio']=sum(query_detail_loan_m60.queryDate_to_report<6)/len(query_detail_loan_m60)  #贷款审批 近60个月 查询距报告时间在6个月内，分布所占比例
                dict_out['m60_query_detail_loan_queryDate_to_report_12_ratio']=sum(query_detail_loan_m60.queryDate_to_report<12)/len(query_detail_loan_m60)  #贷款审批 近60个月 查询距报告时间在12个月内，分布所占比例

                dict_out['m60_query_detail_loan_date_to_report_max']= query_detail_loan_m60['queryDate_to_report'].max() #贷款审批 近60个月 查询时间距离报告月份max
                dict_out['m60_query_detail_loan_date_to_report_mean']= query_detail_loan_m60['queryDate_to_report'].mean() #贷款审批 近60个月 查询时间距离报告月份mean

        query_detail_card = query_detail[query_detail.queryReason=='信用卡审批']
        if len(query_detail_card)>0:

            dict_out['query_detail_card_queryDate_to_report_01_ratio']=sum(query_detail_card.queryDate_to_report<1)/len(query_detail_card)  #信用卡审批 查询距报告时间在1个月内，分布所占比例
            dict_out['query_detail_card_queryDate_to_report_03_ratio']=sum(query_detail_card.queryDate_to_report<3)/len(query_detail_card)  #信用卡审批 查询距报告时间在3个月内，分布所占比例

            query_detail_card_m01 = query_detail_card[query_detail_card.queryDate_to_report<1]


            query_detail_card_m03 = query_detail_card[query_detail_card.queryDate_to_report<3]

            query_detail_card_m06 = query_detail_card[query_detail_card.queryDate_to_report<6]
            if len(query_detail_card_m06)>0:
                dict_out['m06_query_detail_card_cnt']=len(query_detail_card_m06)  #信用卡审批 近06个月 查询记录信息单元个数


                dict_out['m06_query_detail_card_date_to_report_mean']= query_detail_card_m06['queryDate_to_report'].mean() #信用卡审批 近06个月 查询时间距离报告月份mean

            query_detail_card_m12 = query_detail_card[query_detail_card.queryDate_to_report<12]
            if len(query_detail_card_m12)>0:

                dict_out['m12_query_detail_card_date_to_report_mean']= query_detail_card_m12['queryDate_to_report'].mean() #信用卡审批 近12个月 查询时间距离报告月份mean

            query_detail_card_m24 = query_detail_card[query_detail_card.queryDate_to_report<24]
            if len(query_detail_card_m24)>0:

                dict_out['m24_query_detail_card_mon_report_06_count']=sum(query_detail_card_m24.mon_report==6)  #信用卡审批 近24个月 查询距报告时间在0-6个月，分布计数
                dict_out['m24_query_detail_card_mon_report_06_ratio']=sum(query_detail_card_m24.mon_report==6)/len(query_detail_card_m24)  #信用卡审批 近24个月 查询距报告时间在0-6个月，分布所占比例

                dict_out['m24_query_detail_card_queryDate_to_report_06_ratio']=sum(query_detail_card_m24.queryDate_to_report<6)/len(query_detail_card_m24)  #信用卡审批 近24个月 查询距报告时间在6个月内，分布所占比例


            query_detail_card_m36 = query_detail_card[query_detail_card.queryDate_to_report<36]
            if len(query_detail_card_m36)>0:

                dict_out['m36_query_detail_card_mon_report_06_count']=sum(query_detail_card_m36.mon_report==6)  #信用卡审批 近36个月 查询距报告时间在0-6个月，分布计数
                dict_out['m36_query_detail_card_mon_report_06_ratio']=sum(query_detail_card_m36.mon_report==6)/len(query_detail_card_m36)  #信用卡审批 近36个月 查询距报告时间在0-6个月，分布所占比例


            query_detail_card_m48 = query_detail_card[query_detail_card.queryDate_to_report<48]
            # if len(query_detail_card_m48)>0:

            query_detail_card_m60 = query_detail_card[query_detail_card.queryDate_to_report<60]
            if len(query_detail_card_m60)>0:

                dict_out['m60_query_detail_card_mon_report_06_count']=sum(query_detail_card_m60.mon_report==6)  #信用卡审批 近60个月 查询距报告时间在0-6个月，分布计数
                dict_out['m60_query_detail_card_mon_report_06_ratio']=sum(query_detail_card_m60.mon_report==6)/len(query_detail_card_m60)  #信用卡审批 近60个月 查询距报告时间在0-6个月，分布所占比例

                dict_out['m60_query_detail_card_queryDate_to_report_06_ratio']=sum(query_detail_card_m60.queryDate_to_report<6)/len(query_detail_card_m60)  #信用卡审批 近60个月 查询距报告时间在6个月内，分布所占比例


        query_detail_scrutiny = query_detail[query_detail.queryReason=='保前审查']
        if len(query_detail_scrutiny)>0:
            dict_out['query_detail_scrutiny_cnt']=len(query_detail_scrutiny)  #保前审查 查询记录信息单元个数


            query_detail_scrutiny_m01 = query_detail_scrutiny[query_detail_scrutiny.queryDate_to_report<1]

            query_detail_scrutiny_m03 = query_detail_scrutiny[query_detail_scrutiny.queryDate_to_report<3]

            query_detail_scrutiny_m06 = query_detail_scrutiny[query_detail_scrutiny.queryDate_to_report<6]

            query_detail_scrutiny_m12 = query_detail_scrutiny[query_detail_scrutiny.queryDate_to_report<12]

            query_detail_scrutiny_m24 = query_detail_scrutiny[query_detail_scrutiny.queryDate_to_report<24]

            query_detail_scrutiny_m36 = query_detail_scrutiny[query_detail_scrutiny.queryDate_to_report<36]

            query_detail_scrutiny_m48 = query_detail_scrutiny[query_detail_scrutiny.queryDate_to_report<48]

            query_detail_scrutiny_m60 = query_detail_scrutiny[query_detail_scrutiny.queryDate_to_report<60]

        query_detail_guarantee = query_detail[query_detail.queryReason=='担保资格审查']
        if len(query_detail_guarantee)>0:
            dict_out['query_detail_guarantee_cnt']=len(query_detail_guarantee)  #担保资格审查 查询记录信息单元个数


            query_detail_guarantee_m01 = query_detail_guarantee[query_detail_guarantee.queryDate_to_report<1]

            query_detail_guarantee_m03 = query_detail_guarantee[query_detail_guarantee.queryDate_to_report<3]

            query_detail_guarantee_m06 = query_detail_guarantee[query_detail_guarantee.queryDate_to_report<6]

            query_detail_guarantee_m12 = query_detail_guarantee[query_detail_guarantee.queryDate_to_report<12]
            if len(query_detail_guarantee_m12)>0:
                dict_out['m12_query_detail_guarantee_date_to_report_max']= query_detail_guarantee_m12['queryDate_to_report'].max() #担保资格审查 近12个月 查询时间距离报告月份max

            query_detail_guarantee_m24 = query_detail_guarantee[query_detail_guarantee.queryDate_to_report<24]
            if len(query_detail_guarantee_m24)>0:
                dict_out['m24_query_detail_guarantee_cnt']=len(query_detail_guarantee_m24)  #担保资格审查 近24个月 查询记录信息单元个数


            query_detail_guarantee_m36 = query_detail_guarantee[query_detail_guarantee.queryDate_to_report<36]

            query_detail_guarantee_m48 = query_detail_guarantee[query_detail_guarantee.queryDate_to_report<48]

            query_detail_guarantee_m60 = query_detail_guarantee[query_detail_guarantee.queryDate_to_report<60]

        query_detail_funding = query_detail[query_detail.queryReason=='融资审批']
        if len(query_detail_funding)>0:


            query_detail_funding_m01 = query_detail_funding[query_detail_funding.queryDate_to_report<1]


            query_detail_funding_m03 = query_detail_funding[query_detail_funding.queryDate_to_report<3]


            query_detail_funding_m06 = query_detail_funding[query_detail_funding.queryDate_to_report<6]


            query_detail_funding_m12 = query_detail_funding[query_detail_funding.queryDate_to_report<12]
            if len(query_detail_funding_m12)>0:

                dict_out['m12_query_detail_funding_date_to_report_mean']= query_detail_funding_m12['queryDate_to_report'].mean() #融资审批 近12个月 查询时间距离报告月份mean

            query_detail_funding_m24 = query_detail_funding[query_detail_funding.queryDate_to_report<24]


            query_detail_funding_m36 = query_detail_funding[query_detail_funding.queryDate_to_report<36]


            query_detail_funding_m48 = query_detail_funding[query_detail_funding.queryDate_to_report<48]


            query_detail_funding_m60 = query_detail_funding[query_detail_funding.queryDate_to_report<60]

   


    #查询信息汇总
    query_summary=copy.copy(dict_in['cc_rh_report_query_summary'])
    if len(query_summary)>0:
        dict_out['query_summary_selfQueryCount']=query_summary.selfQueryCount.values[0] # 最近1个月内的查询次数（本人查询）
        dict_out['query_summary_loanAfterQueryCount']=query_summary.loanAfterQueryCount.values[0] # 最近2年内的查询次数（贷后管理）



    #呆账信息汇总
    summary_bad_debts=copy.copy(dict_in['cc_rh_report_summary_bad_debts'])


    #信用提示
    credit_tips=copy.copy(dict_in['cc_rh_report_summary_credit_tips'])
    if len(credit_tips)>0:
        credit_tips['LoanCount']=credit_tips['houseLoanCount']+credit_tips['commercialHouseLoanCount']+credit_tips['otherLoanCount']
        credit_tips['CardCount']=credit_tips['creditCardCount']+credit_tips['readyCardCount']
        credit_tips['TotalCount']=credit_tips['LoanCount']+credit_tips['CardCount']+credit_tips['otherCount']
        dict_out['credit_tips_total_count'] = credit_tips['TotalCount'].values[0]  #   信贷交易账户数合计
        dict_out['LoanCount_count'] = credit_tips['LoanCount'].values[0] #贷款账户数
        dict_out['LoanCount_ratio'] = credit_tips['LoanCount'].values[0]/dict_out['credit_tips_total_count'] if dict_out['credit_tips_total_count']>0 else np.nan #贷款账户数占比
        dict_out['CardCount_count'] = credit_tips['CardCount'].values[0] #信用卡账户数
        dict_out['CardCount_ratio'] = credit_tips['CardCount'].values[0]/dict_out['credit_tips_total_count'] if dict_out['credit_tips_total_count']>0 else np.nan #信用卡账户数占比
        dict_out['credit_tips_class_cnt'] = sum([credit_tips['LoanCount'].values[0]>0,credit_tips['CardCount'].values[0]>0,credit_tips['otherCount'].values[0]>0])  #     信贷交易业务类型数量
        dict_out['houseLoanCount'] = credit_tips['houseLoanCount'].values[0] #个人住房贷款笔数 houseLoanCount
        dict_out['houseLoanFirstMonth'] = credit_tips['houseLoanFirstMonth'].values[0] #首笔个人住房贷款发放月份 houseLoanFirstMonth
        dict_out['houseLoanFirstMonth_to_report'] = monthdelta(credit_tips['houseLoanFirstMonth'].values[0],dict_in['cc_rh_report']['reportTime']) #首笔个人住房贷款到报告月份
        dict_out['commercialHouseLoanCount'] = credit_tips['commercialHouseLoanCount'].values[0] #个人商用房（包括商住两用）贷款笔数 commercialHouseLoanCount
        dict_out['commercialHouseLoanFirstMonth'] = credit_tips['commercialHouseLoanFirstMonth'].values[0] #首笔个人商用房贷款发放月份 commercialHouseLoanFirstMonth
        dict_out['commercialHouseLoanFirstMonth_to_report'] = monthdelta(credit_tips['commercialHouseLoanFirstMonth'].values[0],dict_in['cc_rh_report']['reportTime']) #首笔个人商用房贷款到报告月份
        dict_out['otherLoanCount'] = credit_tips['otherLoanCount'].values[0] #其他贷款笔数
        dict_out['otherLoanFirstMonth'] = credit_tips['otherLoanFirstMonth'].values[0] #首笔其他类贷款发放月份
        dict_out['otherLoanFirstMonth_to_report'] = monthdelta(credit_tips['otherLoanFirstMonth'].values[0],dict_in['cc_rh_report']['reportTime']) #首笔其他类贷款到报告月份
        dict_out['firstLoanMonth'] = credit_tips['firstLoanMonth'].values[0] #首笔贷款发放月份
        dict_out['firstLoanMonth_to_report'] = monthdelta(credit_tips['firstLoanMonth'].values[0],dict_in['cc_rh_report']['reportTime']) #首笔贷款到报告月份
        dict_out['creditCardCount'] = credit_tips['creditCardCount'].values[0] #贷记卡账户笔数
        dict_out['firstCreditCardMonth'] = credit_tips['firstCreditCardMonth'].values[0] #首笔贷记卡账户发放月份
        dict_out['firstCreditCardMonth_to_report'] = monthdelta(credit_tips['firstCreditCardMonth'].values[0],dict_in['cc_rh_report']['reportTime']) #首笔贷记卡账户到报告月份
        dict_out['readyCardCount'] = credit_tips['readyCardCount'].values[0] #准贷记卡账户笔数
        dict_out['firstReadyCardMonth'] = credit_tips['firstReadyCardMonth'].values[0] #首笔准贷记卡账户发放月份
        dict_out['firstReadyCardMonth_to_report'] = monthdelta(credit_tips['firstReadyCardMonth'].values[0],dict_in['cc_rh_report']['reportTime']) #首笔准贷记卡账户到报告月份
        dict_out['otherCount'] = credit_tips['otherCount'].values[0] #首笔其他笔数
        dict_out['otherFirstMonth'] = credit_tips['otherFirstMonth'].values[0] #首笔其他发放月份
        dict_out['otherFirstMonth_to_report'] = monthdelta(credit_tips['otherFirstMonth'].values[0],dict_in['cc_rh_report']['reportTime']) #首笔其他到报告月份
        dict_out['declareCount'] = credit_tips['declareCount'].values[0] #本人声明数目
        dict_out['dissentCount'] = credit_tips['dissentCount'].values[0] #异议标注数目

        dict_out['houseLoanCount_vs_LoanCount'] = dict_out['houseLoanCount']/dict_out['LoanCount_count'] if dict_out['LoanCount_count']>0 else np.nan #个人住房贷款笔数在贷款笔数中占比
        dict_out['houseLoanCount_vs_TotalCount'] = dict_out['houseLoanCount']/dict_out['credit_tips_total_count'] if dict_out['credit_tips_total_count']>0 else np.nan #个人住房贷款笔数在总笔数中占比
        dict_out['commercialHouseLoan_vs_LoanCount'] = dict_out['commercialHouseLoanCount']/dict_out['LoanCount_count'] if dict_out['LoanCount_count']>0 else np.nan #个人商用房贷款笔数在贷款笔数中占比
        dict_out['commercialHouseLoan_vs_TotalCount'] = dict_out['commercialHouseLoanCount']/dict_out['credit_tips_total_count'] if dict_out['credit_tips_total_count']>0 else np.nan #个人商用房贷款笔数在总笔数中占比
        dict_out['otherLoanCount_vs_LoanCount'] = dict_out['otherLoanCount']/dict_out['LoanCount_count'] if dict_out['LoanCount_count']>0 else np.nan #其他贷款笔数在贷款笔数中占比
        dict_out['otherLoanCount_vs_TotalCount'] = dict_out['otherLoanCount']/dict_out['credit_tips_total_count'] if dict_out['credit_tips_total_count']>0 else np.nan #其他贷款笔数在总笔数中占比
        dict_out['creditCardCount_vs_CardCount'] = dict_out['creditCardCount']/dict_out['CardCount_count'] if dict_out['CardCount_count']>0 else np.nan #贷记卡笔数在信用卡笔数中占比
        dict_out['creditCardCount_vs_TotalCount'] = dict_out['creditCardCount']/dict_out['credit_tips_total_count'] if dict_out['credit_tips_total_count']>0 else np.nan #贷记卡笔数在总笔数中占比
        dict_out['readyCardCount_vs_CardCount'] = dict_out['readyCardCount']/dict_out['CardCount_count'] if dict_out['CardCount_count']>0 else np.nan #准贷记卡笔数在信用卡笔数中占比
        dict_out['readyCardCount_vs_TotalCount'] = dict_out['readyCardCount']/dict_out['credit_tips_total_count'] if dict_out['credit_tips_total_count']>0 else np.nan ##准贷记卡笔数在总笔数中占比


    #信贷交易授信及负债信息概要 all
    summary_debt_card=copy.copy(dict_in['cc_rh_report_summary_debt_card'])
    summary_debt_loan=copy.copy(dict_in['cc_rh_report_summary_debt_loan'])
    try:
        summary_debt_loan_c=copy.copy(summary_debt_loan[['reportId','businessType','orgCount','accountCount','creditTotalAmount','balance','avgRepaymentAmount']])
        summary_debt_loan_c['type']='loan'
    except:
        summary_debt_loan_c = pd.DataFrame()
    try:
        summary_debt_card_c=copy.copy(summary_debt_card[['reportId','businessType','orgCount','accountCount','creditTotalAmount','usedAmount','avgUsedAmount']].rename(columns={'usedAmount':'balance','avgUsedAmount':'avgRepaymentAmount'}))
        summary_debt_card_c['type']='card'
    except:
        summary_debt_card_c = pd.DataFrame()

    debt_loan_card=pd.concat([summary_debt_loan_c,summary_debt_card_c])
    if len(debt_loan_card)>0:
        dict_out['loan_card_org_count_sum'] = debt_loan_card.orgCount.sum() #1:非循环贷、2:循环额度下分账户、3:循环贷' 管理机构数求和
        dict_out['loan_card_account_count_sum'] = debt_loan_card.accountCount.sum() #1:非循环贷、2:循环额度下分账户、3:循环贷' 账户数求和
        dict_out['loan_card_total_amount_sum'] = debt_loan_card.creditTotalAmount.sum() #1:非循环贷、2:循环额度下分账户、3:循环贷' 授信总额求和
        dict_out['loan_card_balance_sum'] = debt_loan_card.balance.sum() #1:非循环贷、2:循环额度下分账户、3:循环贷' 余额求和
        dict_out['loan_card_avgRepaymentAmount_sum'] = debt_loan_card.avgRepaymentAmount.sum() #1:非循环贷、2:循环额度下分账户、3:循环贷' 最近6个月平均应还款额求和


    #信贷交易授信及负债信息概要   '业务类型 1:贷记卡、2:准贷记卡'
    if len(summary_debt_card)>0:  
        dict_out['summary_debt_card_org_count_sum'] = summary_debt_card.orgCount.sum() #贷记卡账户信息汇总 发卡机构数 求和
        dict_out['summary_debt_card_account_count_sum'] = summary_debt_card.accountCount.sum() #贷记卡账户信息汇总 账户数 求和
        dict_out['summary_debt_card_total_amount_sum'] = summary_debt_card.creditTotalAmount.sum() #贷记卡账户信息汇总 授信总额 求和
        dict_out['summary_debt_card_max_amount_sum'] = summary_debt_card.creditMaxAmount.sum() #贷记卡账户信息汇总 单家机构最高授信额 求和
        dict_out['summary_debt_card_min_amount_sum'] = summary_debt_card.creditMinAmount.sum() #贷记卡账户信息汇总 单家机构最低授信额 求和
        dict_out['summary_debt_card_used_amount_sum'] = summary_debt_card.usedAmount.sum() #贷记卡账户信息汇总 已用额度 求和
        dict_out['summary_debt_card_avgused_amount_sum'] = summary_debt_card.avgUsedAmount.sum() #贷记卡账户信息汇总 最近6个月平均使用额度 求和

        dict_out['summary_debt_card_org_count'] = summary_debt_card.orgCount.values[0]#贷记卡账户信息汇总 发卡机构数
        dict_out['summary_debt_card_account_count'] = summary_debt_card.accountCount.values[0]#贷记卡账户信息汇总 账户数
        dict_out['summary_debt_card_total_amount'] = summary_debt_card.creditTotalAmount.values[0]#贷记卡账户信息汇总 授信总额
        dict_out['summary_debt_card_max_amount'] = summary_debt_card.creditMaxAmount.values[0]#贷记卡账户信息汇总 单家机构最高授信额
        dict_out['summary_debt_card_min_amount'] = summary_debt_card.creditMinAmount.values[0]#贷记卡账户信息汇总 单家机构最低授信额
        dict_out['summary_debt_card_used_amount'] = summary_debt_card.usedAmount.values[0]#贷记卡账户信息汇总 已用额度
        dict_out['summary_debt_card_avgused_amount'] = summary_debt_card.avgUsedAmount.values[0]#贷记卡账户信息汇总 最近6个月平均使用额度

        dict_out['loan_card_card_total_amount_ratio'] = dict_out['summary_debt_card_total_amount_sum']/dict_out['loan_card_total_amount_sum'] if dict_out['loan_card_total_amount_sum']>0 else np.nan # 负债 信用卡授信总额在所有负债授信总额中的占比
        dict_out['loan_card_debt_total_amount_ratio'] = dict_out['summary_debt_card_total_amount']/dict_out['loan_card_total_amount_sum'] if dict_out['loan_card_total_amount_sum']>0 else np.nan # 负债 贷记卡款授信总额在所有负债授信总额中的占比


    #信贷交易授信及负债信息概要    1:非循环贷、2:循环额度下分账户、3:循环贷'
    if len(summary_debt_loan)>0:
        dict_out['summary_debt_loan_total_amount_sum'] = summary_debt_loan.creditTotalAmount.sum() #1:非循环贷、2:循环额度下分账户、3:循环贷' 授信总额求和

        dict_out['loan_card_loan_total_amount_ratio'] = dict_out['summary_debt_loan_total_amount_sum']/dict_out['loan_card_total_amount_sum'] if dict_out['loan_card_total_amount_sum']>0 else np.nan # 负债 贷款授信总额在所有负债授信总额中的占比

        if len(summary_debt_loan[summary_debt_loan.businessType==1])>0:
            dict_out['summary_debt_loan_notcycle_avg_repay_amt'] = summary_debt_loan[summary_debt_loan.businessType==1].avgRepaymentAmount.values[0] #非循环贷账户信息汇总 最近6个月平均应还款额

            dict_out['loan_card_notcycle_avgRepaymentAmount_ratio'] = dict_out['summary_debt_loan_notcycle_avg_repay_amt']/dict_out['loan_card_avgRepaymentAmount_sum'] if dict_out['loan_card_avgRepaymentAmount_sum']>0 else np.nan # 负债 非循环最近6个月平均应还款额求和在所有负债最近6个月平均应还款额求和中的占比




    #逾期及违约信息概要
    summary_overdue=copy.copy(dict_in['cc_rh_report_summary_overdue'])


    #被追偿信息汇总
    summary_recovery=copy.copy(dict_in['cc_rh_report_summary_recovery'])


    #二代征信-贷款信息   
    loan_second=copy.copy(dict_in['cc_rh_report_detail_loan_second'])
    l_month60 = pd.DataFrame()
    if len(loan_second)>0:    
        loan_second['startDate_to_report']=loan_second.apply(lambda x:monthdelta(x['startDate'],dict_in['cc_rh_report']['reportTime']),axis=1)
        loan_second['startDate_to_endDate']=loan_second.apply(lambda x:monthdelta(x['startDate'],x['endDate']),axis=1)
        loan_second['byDate_to_report']=loan_second.apply(lambda x:monthdelta(x['byDate'],dict_in['cc_rh_report']['reportTime']),axis=1)
        loan_second['type']='loan'
        loan_second['class']=loan_second.apply(lambda x:'非循环贷账户' if len(re.findall('--|到期一次还本付息',x.repayType))>0 else '循环额度下分账户' if len(re.findall('分期等额本息',x.repayType))>0 else  '循环贷账户',axis=1 )
        #现行（最近一次月度表现）
        loan_second['is_now']=loan_second.apply(lambda x:1 if ((x['accountStatus']=='正常') | (x['accountStatus']=='逾期')) else 0,axis=1)
        #定义有担保
        loan_second['is_vouch']=loan_second.apply(lambda x:1 if ((x['guaranteeForm']!='信用/免担保') & (x['guaranteeForm']!='组合（不含保证）') & (x['guaranteeForm']!='其他')) else 0,axis=1)
        #定义需还款月份数
        loan_second['repayMons']=loan_second.apply(lambda x:max(1,len(x['month60State'])-1) if x['repayTerms']==0 else
                                  x['repayTerms']*12 if x['repayFrequency']=='年' else 
                                  x['repayTerms']*6 if x['repayFrequency']=='半年' else 
                                  x['repayTerms']*3 if x['repayFrequency']=='季' else  
                                 x['repayTerms'] if x['repayFrequency']=='月' else  
                                 math.ceil(x['repayTerms']/4) if x['repayFrequency']=='周' else  
                                 math.ceil(x['repayTerms']/31) if x['repayFrequency']=='日' else  
                                 1 if x['repayFrequency']=='一次性' else 
                                 max(1,len(str(x['month60State']))-1) ,axis=1)
        loan_second['repayAmt']=loan_second.apply(lambda x:x.loanAmount/x.repayMons if x.repayMons>0 else np.nan ,axis=1)
        loan_second['classify5_num']=loan_second['classify5'].map({ '损失':1,'可疑':2, '关注':3, '次级':4, '正常':5})
        loan_second['org']=loan_second.apply(lambda x:re.findall('[\u4e00-\u9fa5]*',x.loanGrantOrg)[0] if len(re.findall('[\u4e00-\u9fa5]*',x.loanGrantOrg))>0 else np.nan,axis=1)
        loan_second['logo']=loan_second.apply(lambda x:1 if len(x.accountLogo)>8 else 0,axis=1)

        #定义逾期程度
        loan_second['due_class']=loan_second.apply(lambda x: np.nan if pd.isnull(x['month60State']) else np.nan if len(x['month60State'])==0 
                                                                                                     else  0 if len(re.findall('[1-7DZG]',x['month60State']))==0
                                                                                                    else 3 if len(re.findall('[4-8]', x['month60State'])) > 0
                                                                                                    else 2 if len(re.findall('[2-3DZG]', x['month60State'])) > 0
                                                                                                    else 1 if len(re.findall('[1]',x['month60State']))>0
                                                                                                    else np.nan ,axis=1)
        loan_second['is_overdue']=loan_second.apply(lambda x: np.nan if pd.isnull(x['month60State']) else np.nan if len(x['month60State'])==0 
                                                                                                     else  0 if len(re.findall('[1-7DZG]',x['month60State']))==0 
                                                                                                     else  1  ,axis=1)

        # 近60个月还款记录辅助表
        import pandas as pd
        import datetime, time
        from dateutil.relativedelta import relativedelta
        def getMonList(month60Desc):
            date_list = []
            if len(month60Desc)>5 :
                begin_end_date=re.findall('[0-9]{4}年[0-9]{2}月',month60Desc)
                begin_date=datetime.datetime.strptime(begin_end_date[0],'%Y年%m月')
                end_date=datetime.datetime.strptime(begin_end_date[1],'%Y年%m月')
                while begin_date <= end_date:
                    date_str = begin_date.strftime("%Y.%m")
                    date_list.append(date_str)
                    begin_date += relativedelta(months=1)
            return date_list
        # print(getMonList('2019年10月 —2020年01月的还款记录'))
        month60Amount=loan_second.set_index(['id','reportId'])['month60Amount'].str.split('/', expand=True).stack().reset_index(level=2, drop=True).rename('month60_Amount').reset_index(drop=False)
        month60State=loan_second.set_index(['id','reportId'])['month60State'].astype(str).apply(lambda x: '/'.join(list(x))).str.split('/', expand=True).stack().reset_index(level=2, drop=True).rename('month60_State').reset_index(drop=False)
        month60Desc=loan_second.set_index(['id','reportId'])['month60Desc'].astype(str).apply(lambda x: '/'.join(getMonList(x))).str.split('/', expand=True).stack().reset_index(level=2, drop=True).rename('month60_Desc').reset_index(drop=False)
        month60Amount['rank']=month60Amount['id'].groupby(month60Amount['id']).rank(method='first')
        month60State['rank']=month60State['id'].groupby(month60State['id']).rank(method='first')
        month60Desc['rank']=month60Desc['id'].groupby(month60Desc['id']).rank(method='first')
        month60=pd.merge(month60Desc,month60State, how='inner', on=['id', 'rank','reportId']) 
        month60=pd.merge(month60,month60Amount, how='inner', on=['id', 'rank','reportId'])
        month60['month60_to_report']=month60.apply(lambda x:monthdelta(x['month60_Desc'],dict_in['cc_rh_report']['reportTime']),axis=1)
        month60['month60_State_num']=month60.apply(lambda x:(int(re.sub('[^1-7]','0',x['month60_State'])) if re.sub('[^1-7]','0',x['month60_State'])!='' else np.nan),axis=1)
        month60['month60_Amount_num']=month60['month60_Amount'].apply(lambda x: float(re.sub('[^0-9\.]','',x)) if len(re.sub('[^0-9\.]','',x))>0 else 0)

        def countN(x):
            return sum(x=='N')
        def countNr(x):
            return sum(x=='N')/len(x)
        def countC(x):
            return sum(x=='C')
        def countCr(x):
            return sum(x=='C')/len(x)
        def countD(x):
            return sum(x=='D')
        def countDr(x):
            return sum(x=='D')/len(x)
        def countG(x):
            return sum(x=='G')
        def countGr(x):
            return sum(x=='G')/len(x)
        def countNull(x):
            return sum(x=='*')
        def countNullr(x):
            return sum(x=='*')/len(x)
        def countUnknow(x):
            return sum(x=='#') + sum(x=='')
        def countUnknowr(x):
            return (sum(x=='#') + sum(x==''))/len(x)
        def count0(x):
            return sum(x=='0')
        def count0r(x):
            return sum(x=='0')/len(x)
        def count1(x):
            return sum(x=='1')
        def count1r(x):
            return sum(x=='1')/len(x)
        def count2(x):
            return sum(x=='2')
        def count2r(x):
            return sum(x=='2')/len(x)
        def count3(x):
            return sum(x=='3')
        def count3r(x):
            return sum(x=='3')/len(x)
        def count4(x):
            return sum(x=='4')
        def count4r(x):
            return sum(x=='4')/len(x)
        def count5(x):
            return sum(x=='5')
        def count5r(x):
            return sum(x=='5')/len(x)
        def count6(x):
            return sum(x=='6')
        def count6r(x):
            return sum(x=='6')/len(x)
        def count7(x):
            return sum(x=='7')
        def count7r(x):
            return sum(x=='7')/len(x)
        def meanbig0(x):
            return sum(x)/sum(x>0) if sum(x>0)>0 else np.nan
        month60df=month60.groupby(['id','reportId']).agg({'month60_to_report':['min','mean','max'],'month60_State_num':['size','sum','max','mean',meanbig0],'month60_Amount_num':['sum','max','mean',meanbig0],'month60_State':[countN,countNr,countC,countCr,countD,countDr,countG,countGr,countNull,countNullr,countUnknow,countUnknowr,count0,count0r,count1,count1r,count2,count2r,count3,count3r,count4,count4r,count5,count5r,count6,count6r,count7,count7r]})
        month60df.columns=[i[0]+'_'+i[1] for i in month60df.columns]
        loan_second=pd.merge(loan_second,month60df,on=['id','reportId'],how='left')
        loan_second['repayMons_ratio']=loan_second.apply(lambda x: x.month60_State_num_size/x.repayMons if x.repayMons>0 else np.nan,axis=1)
        l_month60=copy.copy(month60)



        if True:
            dict_out['loan_second_month60_State_num_max'] = l_month60['month60_State_num'].max() #近5年还款情况最大值
            dict_out['loan_second_month60_State_num_min'] = l_month60['month60_State_num'].min() #近5年还款情况最小值
            dict_out['loan_second_month60_State_num_mean'] = l_month60['month60_State_num'].mean() #近5年还款情况平均值
            dict_out['loan_second_month60_State_num_sum'] = l_month60['month60_State_num'].sum() #近5年还款情况求和
            dict_out['loan_second_month60_State_big0_mean'] = l_month60[l_month60['month60_State_num']>0]['month60_State_num'].mean() #近5年还款情况大于0平均值

            dict_out['loan_second_month60_Amount_num_max'] = l_month60['month60_Amount_num'].max() #近5年还款金额最大值
            dict_out['loan_second_month60_Amount_num_min'] = l_month60['month60_Amount_num'].min() #近5年还款金额最小值
            dict_out['loan_second_month60_Amount_num_mean'] = l_month60['month60_Amount_num'].mean() #近5年还款金额平均值
            dict_out['loan_second_month60_Amount_num_sum'] = l_month60['month60_Amount_num'].sum() #近5年还款金额求和
            dict_out['loan_second_month60_Amount_big0_mean'] = l_month60[l_month60['month60_Amount_num']>0]['month60_Amount_num'].mean() #近5年还款金额大于0平均值

            dict_out['loan_second_month60_State_giniimpurity'] = giniimpurity(l_month60['month60_State']) #贷款账户近5年还款情况基尼不纯度
            dict_out['loan_second_month60_N_count'] = l_month60[l_month60['month60_State']=='N'].shape[0] #贷款账户近5年还款情况为N 计数
            dict_out['loan_second_month60_N_ratio'] = l_month60[l_month60['month60_State']=='N'].shape[0]/len(l_month60) #贷款账户近5年还款情况为N 占比
            dict_out['loan_second_month60_C_count'] = l_month60[l_month60['month60_State']=='C'].shape[0] #贷款账户近5年还款情况为C 计数
            dict_out['loan_second_month60_C_ratio'] = l_month60[l_month60['month60_State']=='C'].shape[0]/len(l_month60) #贷款账户近5年还款情况为C 占比
            dict_out['loan_second_month60_D_count'] = l_month60[l_month60['month60_State']=='D'].shape[0] #贷款账户近5年还款情况为D 计数
            dict_out['loan_second_month60_D_ratio'] = l_month60[l_month60['month60_State']=='D'].shape[0]/len(l_month60) #贷款账户近5年还款情况为D 占比
            dict_out['loan_second_month60_G_count'] = l_month60[l_month60['month60_State']=='G'].shape[0] #贷款账户近5年还款情况为G 计数
            dict_out['loan_second_month60_G_ratio'] = l_month60[l_month60['month60_State']=='G'].shape[0]/len(l_month60) #贷款账户近5年还款情况为G 占比
            dict_out['loan_second_month60_Null_count'] = l_month60[l_month60['month60_State']=='*'].shape[0] #贷款账户近5年还款情况为* 计数
            dict_out['loan_second_month60_Null_ratio'] = l_month60[l_month60['month60_State']=='*'].shape[0]/len(l_month60) #贷款账户近5年还款情况为* 占比
            dict_out['loan_second_month60_Unknow_count'] = l_month60[l_month60['month60_State']=='#'].shape[0] #贷款账户近5年还款情况为# 计数
            dict_out['loan_second_month60_Unknow_ratio'] = l_month60[l_month60['month60_State']=='#'].shape[0]/len(l_month60) #贷款账户近5年还款情况为# 占比
            dict_out['loan_second_month60_0_count'] = l_month60[l_month60['month60_State']=='0'].shape[0] #贷款账户近5年还款情况为0 计数
            dict_out['loan_second_month60_0_ratio'] = l_month60[l_month60['month60_State']=='0'].shape[0]/len(l_month60) #贷款账户近5年还款情况为0 占比
            dict_out['loan_second_month60_1_count'] = l_month60[l_month60['month60_State']=='1'].shape[0] #贷款账户近5年还款情况为1 计数
            dict_out['loan_second_month60_1_ratio'] = l_month60[l_month60['month60_State']=='1'].shape[0]/len(l_month60) #贷款账户近5年还款情况为1 占比
            dict_out['loan_second_month60_2_count'] = l_month60[l_month60['month60_State']=='2'].shape[0] #贷款账户近5年还款情况为2 计数
            dict_out['loan_second_month60_2_ratio'] = l_month60[l_month60['month60_State']=='2'].shape[0]/len(l_month60) #贷款账户近5年还款情况为2 占比
            dict_out['loan_second_month60_3_count'] = l_month60[l_month60['month60_State']=='3'].shape[0] #贷款账户近5年还款情况为3 计数
            dict_out['loan_second_month60_3_ratio'] = l_month60[l_month60['month60_State']=='3'].shape[0]/len(l_month60) #贷款账户近5年还款情况为3 占比
            dict_out['loan_second_month60_4_count'] = l_month60[l_month60['month60_State']=='4'].shape[0] #贷款账户近5年还款情况为4 计数
            dict_out['loan_second_month60_4_ratio'] = l_month60[l_month60['month60_State']=='4'].shape[0]/len(l_month60) #贷款账户近5年还款情况为4 占比
            dict_out['loan_second_month60_5_count'] = l_month60[l_month60['month60_State']=='5'].shape[0] #贷款账户近5年还款情况为5 计数
            dict_out['loan_second_month60_5_ratio'] = l_month60[l_month60['month60_State']=='5'].shape[0]/len(l_month60) #贷款账户近5年还款情况为5 占比
            dict_out['loan_second_month60_6_count'] = l_month60[l_month60['month60_State']=='6'].shape[0] #贷款账户近5年还款情况为6 计数
            dict_out['loan_second_month60_6_ratio'] = l_month60[l_month60['month60_State']=='6'].shape[0]/len(l_month60) #贷款账户近5年还款情况为6 占比
            dict_out['loan_second_month60_7_count'] = l_month60[l_month60['month60_State']=='7'].shape[0] #贷款账户近5年还款情况为7 计数
            dict_out['loan_second_month60_7_ratio'] = l_month60[l_month60['month60_State']=='7'].shape[0]/len(l_month60) #贷款账户近5年还款情况为7 占比


            numeric_vers=['repayAmt','repayMons_ratio','loanAmount','repayTerms', 'balance' ,'leftRepayTerms' ,'planRepayAmount', 'repayedAmount', 'currentOverdueTerms' ,'currentOverdueAmount' ,'overdue31Amount' ,'overdue61Amount' ,'overdue91Amount','overdue180Amount','startDate_to_report','byDate_to_report','is_now','is_vouch','repayMons','classify5_num','logo','due_class','month60_to_report_min','month60_to_report_mean','month60_to_report_max','month60_State_num_size','month60_State_num_sum','month60_State_num_max','month60_State_num_mean','month60_State_num_meanbig0','month60_Amount_num_sum','month60_Amount_num_max','month60_Amount_num_mean','month60_Amount_num_meanbig0','month60_State_countN','month60_State_countNr','month60_State_countC','month60_State_countCr','month60_State_countD','month60_State_countDr','month60_State_countG','month60_State_countGr','month60_State_countNull','month60_State_countNullr','month60_State_countUnknow','month60_State_countUnknowr','month60_State_count0','month60_State_count0r','month60_State_count1','month60_State_count1r','month60_State_count2','month60_State_count2r','month60_State_count3','month60_State_count3r','month60_State_count4','month60_State_count4r','month60_State_count5','month60_State_count5r','month60_State_count6','month60_State_count6r','month60_State_count7','month60_State_count7r']
            for var in numeric_vers:
                loan_second[var]=pd.to_numeric(loan_second[var],errors='coerce')
            loan_second['repayTerm_ratio'] =  loan_second.apply(lambda x: x.leftRepayTerms/x.repayTerms if x.repayTerms>0 else np.nan,axis=1)   # 剩余还款期数/还款期数
            loan_second['balance_ratio'] =  loan_second.apply(lambda x: x.balance/x.loanAmount if x.loanAmount>0 else np.nan,axis=1)   # 余额/借款金额
            loan_second['RepayedAmount_ratio'] =  loan_second.apply(lambda x: x.repayedAmount/x.planRepayAmount if x.planRepayAmount>0 else np.nan,axis=1)   # 本月实还款额/本月应还款
            numeric_vers=['repayTerm_ratio','balance_ratio','RepayedAmount_ratio']+numeric_vers



            for var in numeric_vers:
                dict_out['loan_second_'+var+'_max'] = loan_second[var].max() #var最大值
                dict_out['loan_second_'+var+'_min'] = loan_second[var].min() #var最小值
                dict_out['loan_second_'+var+'_mean'] = loan_second[var].mean() #var平均值
                dict_out['loan_second_'+var+'_sum'] = loan_second[var].sum() #var求和
                dict_out['loan_second_'+var+'_range'] = (dict_out['loan_second_'+var+'_max']-dict_out['loan_second_'+var+'_min'])/dict_out['loan_second_'+var+'_max'] if dict_out['loan_second_'+var+'_max']>0 else np.nan #var区间率


            dict_out['loan_second_loanGrantOrg_giniimpurity'] = giniimpurity(loan_second['loanGrantOrg']) #贷款账户管理机构详细基尼不纯度
            dict_out['loan_second_loanGrantOrg_nunique'] = loan_second['loanGrantOrg'].nunique() #贷款账户管理机构详细nunique

            dict_out['loan_second_org_giniimpurity'] = giniimpurity(loan_second['org']) #贷款账户管理机构基尼不纯度
            dict_out['loan_second_org_commercial_bank_count'] = loan_second[loan_second['org']=='商业银行'].shape[0] #贷款账户管理机构为商业银行 计数
            dict_out['loan_second_org_commercial_bank_ratio'] = loan_second[loan_second['org']=='商业银行'].shape[0]/len(loan_second) #贷款账户管理机构为商业银行 占比
            dict_out['loan_second_org_consumer_finance_count'] = loan_second[loan_second['org']=='消费金融公司'].shape[0] #贷款账户管理机构为消费金融公司 计数
            dict_out['loan_second_org_consumer_finance_ratio'] = loan_second[loan_second['org']=='消费金融公司'].shape[0]/len(loan_second) #贷款账户管理机构为消费金融公司 占比
            dict_out['loan_second_org_micro_loan_count'] = loan_second[loan_second['org']=='小额贷款公司'].shape[0] #贷款账户管理机构为小额贷款公司 计数
            dict_out['loan_second_org_micro_loan_ratio'] = loan_second[loan_second['org']=='小额贷款公司'].shape[0]/len(loan_second) #贷款账户管理机构为小额贷款公司 占比
            dict_out['loan_second_org_other_count'] = loan_second[loan_second['org']=='其他机构'].shape[0] #贷款账户管理机构为其他机构 计数
            dict_out['loan_second_org_other_ratio'] = loan_second[loan_second['org']=='其他机构'].shape[0]/len(loan_second) #贷款账户管理机构为其他机构 占比
            dict_out['loan_second_org_trust_company_count'] = loan_second[loan_second['org']=='信托公司'].shape[0] #贷款账户管理机构为信托公司 计数
            dict_out['loan_second_org_trust_company_ratio'] = loan_second[loan_second['org']=='信托公司'].shape[0]/len(loan_second) #贷款账户管理机构为信托公司 占比
            dict_out['loan_second_org_car_finance_count'] = loan_second[loan_second['org']=='汽车金融公司'].shape[0] #贷款账户管理机构为汽车金融公司 计数
            dict_out['loan_second_org_car_finance_ratio'] = loan_second[loan_second['org']=='汽车金融公司'].shape[0]/len(loan_second) #贷款账户管理机构为汽车金融公司 占比
            dict_out['loan_second_org_lease_finance_count'] = loan_second[loan_second['org']=='融资租赁公司'].shape[0] #贷款账户管理机构为融资租赁公司 计数
            dict_out['loan_second_org_lease_finance_ratio'] = loan_second[loan_second['org']=='融资租赁公司'].shape[0]/len(loan_second) #贷款账户管理机构为融资租赁公司 占比
            dict_out['loan_second_org_myself_count'] = loan_second[loan_second['org']==dict_in['cc_rh_report'].queryOperator.values[0]].shape[0] #贷款账户管理机构为本机构 计数
            dict_out['loan_second_org_myself_ratio'] = loan_second[loan_second['org']==dict_in['cc_rh_report'].queryOperator.values[0]].shape[0]/len(loan_second) #贷款账户管理机构为本机构 占比
            dict_out['loan_second_org_village_banks_count'] = loan_second[loan_second['org']=='村镇银行'].shape[0] #贷款账户管理机构为村镇银行 计数
            dict_out['loan_second_org_village_banks_ratio'] = loan_second[loan_second['org']=='村镇银行'].shape[0]/len(loan_second) #贷款账户管理机构为村镇银行 占比
            dict_out['loan_second_org_finance_company_count'] = loan_second[loan_second['org']=='财务公司'].shape[0] #贷款账户管理机构为财务公司 计数
            dict_out['loan_second_org_finance_company_ratio'] = loan_second[loan_second['org']=='财务公司'].shape[0]/len(loan_second) #贷款账户管理机构为财务公司 占比
            dict_out['loan_second_org_foreign_banks_count'] = loan_second[loan_second['org']=='外资银行'].shape[0] #贷款账户管理机构为外资银行 计数
            dict_out['loan_second_org_foreign_banks_ratio'] = loan_second[loan_second['org']=='外资银行'].shape[0]/len(loan_second) #贷款账户管理机构为外资银行 占比
            dict_out['loan_second_org_provident_fund_count'] = loan_second[loan_second['org']=='公积金管理中心'].shape[0] #贷款账户管理机构为公积金管理中心 计数
            dict_out['loan_second_org_provident_fund_ratio'] = loan_second[loan_second['org']=='公积金管理中心'].shape[0]/len(loan_second) #贷款账户管理机构为公积金管理中心 占比
            dict_out['loan_second_org_securities_firm_count'] = loan_second[loan_second['org']=='证券公司'].shape[0] #贷款账户管理机构为证券公司 计数
            dict_out['loan_second_org_securities_firm_ratio'] = loan_second[loan_second['org']=='证券公司'].shape[0]/len(loan_second) #贷款账户管理机构为证券公司 占比

            dict_out['loan_second_class_giniimpurity'] = giniimpurity(loan_second['class']) #贷款账户账户类别基尼不纯度
            dict_out['loan_second_class_ncycle_count'] = loan_second[loan_second['class']=='非循环贷账户'].shape[0] #贷款账户账户类别为非循环贷账户 计数
            dict_out['loan_second_class_ncycle_ratio'] = loan_second[loan_second['class']=='非循环贷账户'].shape[0]/len(loan_second) #贷款账户账户类别为非循环贷账户 占比
            dict_out['loan_second_class_cycle_sub_count'] = loan_second[loan_second['class']=='循环额度下分账户'].shape[0] #贷款账户账户类别为循环额度下分账户 计数
            dict_out['loan_second_class_cycle_sub_ratio'] = loan_second[loan_second['class']=='循环额度下分账户'].shape[0]/len(loan_second) #贷款账户账户类别为循环额度下分账户 占比
            dict_out['loan_second_class_cycle_count'] = loan_second[loan_second['class']=='循环贷账户'].shape[0] #贷款账户账户类别为循环贷账户 计数
            dict_out['loan_second_class_cycle_ratio'] = loan_second[loan_second['class']=='循环贷账户'].shape[0]/len(loan_second) #贷款账户账户类别为循环贷账户 占比

            dict_out['loan_second_classify5_giniimpurity'] = giniimpurity(loan_second['classify5']) #贷款账户五级分类基尼不纯度
            dict_out['loan_second_c5_unknow_count'] = loan_second[loan_second['classify5']==''].shape[0] #贷款账户五级分类为'' 计数
            dict_out['loan_second_c5_unknow_ratio'] = loan_second[loan_second['classify5']==''].shape[0]/len(loan_second) #贷款账户五级分类为'' 占比
            dict_out['loan_second_c5_normal_count'] = loan_second[loan_second['classify5']=='正常'].shape[0] #贷款账户五级分类为正常 计数
            dict_out['loan_second_c5_normal_ratio'] = loan_second[loan_second['classify5']=='正常'].shape[0]/len(loan_second) #贷款账户五级分类为正常 占比
            dict_out['loan_second_c5_loss_count'] = loan_second[loan_second['classify5']=='损失'].shape[0] #贷款账户五级分类为损失 计数
            dict_out['loan_second_c5_loss_ratio'] = loan_second[loan_second['classify5']=='损失'].shape[0]/len(loan_second) #贷款账户五级分类为损失 占比
            dict_out['loan_second_c5_attention_count'] = loan_second[loan_second['classify5']=='关注'].shape[0] #贷款账户五级分类为关注 计数
            dict_out['loan_second_c5_attention_ratio'] = loan_second[loan_second['classify5']=='关注'].shape[0]/len(loan_second) #贷款账户五级分类为关注 占比
            dict_out['loan_second_c5_suspicious_count'] = loan_second[loan_second['classify5']=='可疑'].shape[0] #贷款账户五级分类为可疑 计数
            dict_out['loan_second_c5_suspicious_ratio'] = loan_second[loan_second['classify5']=='可疑'].shape[0]/len(loan_second) #贷款账户五级分类为可疑 占比
            dict_out['loan_second_c5_secondary_count'] = loan_second[loan_second['classify5']=='次级'].shape[0] #贷款账户五级分类为次级 计数
            dict_out['loan_second_c5_secondary_ratio'] = loan_second[loan_second['classify5']=='次级'].shape[0]/len(loan_second) #贷款账户五级分类为次级 占比
            dict_out['loan_second_c5_noclass_count'] = loan_second[loan_second['classify5']=='未分类'].shape[0] #贷款账户五级分类为未分类 计数
            dict_out['loan_second_c5_noclass_ratio'] = loan_second[loan_second['classify5']=='未分类'].shape[0]/len(loan_second) #贷款账户五级分类为未分类 占比

            dict_out['loan_second_accountStatus_giniimpurity'] = giniimpurity(loan_second['accountStatus']) #贷款账户账户状态基尼不纯度
            dict_out['loan_second_as_settle_count'] = loan_second[loan_second['accountStatus']=='结清'].shape[0] #贷款账户账户状态为结清 计数
            dict_out['loan_second_as_settle_ratio'] = loan_second[loan_second['accountStatus']=='结清'].shape[0]/len(loan_second) #贷款账户账户状态为结清 占比
            dict_out['loan_second_as_normal_count'] = loan_second[loan_second['accountStatus']=='正常'].shape[0] #贷款账户账户状态为正常 计数
            dict_out['loan_second_as_normal_ratio'] = loan_second[loan_second['accountStatus']=='正常'].shape[0]/len(loan_second) #贷款账户账户状态为正常 占比
            dict_out['loan_second_as_overdue_count'] = loan_second[loan_second['accountStatus']=='逾期'].shape[0] #贷款账户账户状态为逾期 计数
            dict_out['loan_second_as_overdue_ratio'] = loan_second[loan_second['accountStatus']=='逾期'].shape[0]/len(loan_second) #贷款账户账户状态为逾期 占比
            dict_out['loan_second_as_bad_debts_count'] = loan_second[loan_second['accountStatus']=='呆账'].shape[0] #贷款账户账户状态为呆账 计数
            dict_out['loan_second_as_bad_debts_ratio'] = loan_second[loan_second['accountStatus']=='呆账'].shape[0]/len(loan_second) #贷款账户账户状态为呆账 占比
            dict_out['loan_second_as_unknow_count'] = loan_second[loan_second['accountStatus']==''].shape[0] #贷款账户账户状态为'' 计数
            dict_out['loan_second_as_unknow_ratio'] = loan_second[loan_second['accountStatus']==''].shape[0]/len(loan_second) #贷款账户账户状态为'' 占比
            dict_out['loan_second_as_roll_out_count'] = loan_second[loan_second['accountStatus']=='转出'].shape[0] #贷款账户账户状态为转出 计数
            dict_out['loan_second_as_roll_out_ratio'] = loan_second[loan_second['accountStatus']=='转出'].shape[0]/len(loan_second) #贷款账户账户状态为转出 占比

            dict_out['loan_second_repayType_giniimpurity'] = giniimpurity(loan_second['repayType']) #贷款账户还款方式基尼不纯度
            dict_out['loan_second_rt_unknow_count'] = loan_second[loan_second['repayType']=='--'].shape[0] #贷款账户还款方式为-- 计数
            dict_out['loan_second_rt_unknow_ratio'] = loan_second[loan_second['repayType']=='--'].shape[0]/len(loan_second) #贷款账户还款方式为-- 占比
            dict_out['loan_second_rt_equality_count'] = loan_second[loan_second['repayType']=='分期等额本息'].shape[0] #贷款账户还款方式为分期等额本息 计数
            dict_out['loan_second_rt_equality_ratio'] = loan_second[loan_second['repayType']=='分期等额本息'].shape[0]/len(loan_second) #贷款账户还款方式为分期等额本息 占比
            dict_out['loan_second_rt_onschedule_count'] = loan_second[loan_second['repayType']=='按期计算还本付息'].shape[0] #贷款账户还款方式为按期计算还本付息 计数
            dict_out['loan_second_rt_onschedule_ratio'] = loan_second[loan_second['repayType']=='按期计算还本付息'].shape[0]/len(loan_second) #贷款账户还款方式为按期计算还本付息 占比
            dict_out['loan_second_rt_not_distinguish_count'] = loan_second[loan_second['repayType']=='不区分还款方式'].shape[0] #贷款账户还款方式为不区分还款方式 计数
            dict_out['loan_second_rt_not_distinguish_ratio'] = loan_second[loan_second['repayType']=='不区分还款方式'].shape[0]/len(loan_second) #贷款账户还款方式为不区分还款方式 占比
            dict_out['loan_second_rt_circulation_count'] = loan_second[loan_second['repayType']=='循环贷款下其他还款方式'].shape[0] #贷款账户还款方式为循环贷款下其他还款方式 计数
            dict_out['loan_second_rt_circulation_ratio'] = loan_second[loan_second['repayType']=='循环贷款下其他还款方式'].shape[0]/len(loan_second) #贷款账户还款方式为循环贷款下其他还款方式 占比
            dict_out['loan_second_rt_once_count'] = loan_second[loan_second['repayType']=='到期一次还本付息'].shape[0] #贷款账户还款方式为到期一次还本付息 计数
            dict_out['loan_second_rt_once_ratio'] = loan_second[loan_second['repayType']=='到期一次还本付息'].shape[0]/len(loan_second) #贷款账户还款方式为到期一次还本付息 占比

            dict_out['loan_second_repayFrequency_giniimpurity'] = giniimpurity(loan_second['repayFrequency']) #贷款账户还款频率基尼不纯度
            dict_out['loan_second_rf_month_count'] = loan_second[loan_second['repayFrequency']=='月'].shape[0] #贷款账户还款频率为月 计数
            dict_out['loan_second_rf_month_ratio'] = loan_second[loan_second['repayFrequency']=='月'].shape[0]/len(loan_second) #贷款账户还款频率为月 占比
            dict_out['loan_second_rf_once_count'] = loan_second[loan_second['repayFrequency']=='一次性'].shape[0] #贷款账户还款频率为一次性 计数
            dict_out['loan_second_rf_once_ratio'] = loan_second[loan_second['repayFrequency']=='一次性'].shape[0]/len(loan_second) #贷款账户还款频率为一次性 占比
            dict_out['loan_second_rf_other_count'] = loan_second[loan_second['repayFrequency']=='其他'].shape[0] #贷款账户还款频率为其他 计数
            dict_out['loan_second_rf_other_ratio'] = loan_second[loan_second['repayFrequency']=='其他'].shape[0]/len(loan_second) #贷款账户还款频率为其他 占比
            dict_out['loan_second_rf_irregular_count'] = loan_second[loan_second['repayFrequency']=='不定期'].shape[0] #贷款账户还款频率为不定期 计数
            dict_out['loan_second_rf_irregular_ratio'] = loan_second[loan_second['repayFrequency']=='不定期'].shape[0]/len(loan_second) #贷款账户还款频率为不定期 占比
            dict_out['loan_second_rf_day_count'] = loan_second[loan_second['repayFrequency']=='日'].shape[0] #贷款账户还款频率为日 计数
            dict_out['loan_second_rf_day_ratio'] = loan_second[loan_second['repayFrequency']=='日'].shape[0]/len(loan_second) #贷款账户还款频率为日 占比
            dict_out['loan_second_rf_year_count'] = loan_second[loan_second['repayFrequency']=='年'].shape[0] #贷款账户还款频率为年 计数
            dict_out['loan_second_rf_year_ratio'] = loan_second[loan_second['repayFrequency']=='年'].shape[0]/len(loan_second) #贷款账户还款频率为年 占比
            dict_out['loan_second_rf_season_count'] = loan_second[loan_second['repayFrequency']=='季'].shape[0] #贷款账户还款频率为季 计数
            dict_out['loan_second_rf_season_ratio'] = loan_second[loan_second['repayFrequency']=='季'].shape[0]/len(loan_second) #贷款账户还款频率为季 占比
            dict_out['loan_second_rf_week_count'] = loan_second[loan_second['repayFrequency']=='周'].shape[0] #贷款账户还款频率为周 计数
            dict_out['loan_second_rf_week_ratio'] = loan_second[loan_second['repayFrequency']=='周'].shape[0]/len(loan_second) #贷款账户还款频率为周 占比
            dict_out['loan_second_rf_halfyear_count'] = loan_second[loan_second['repayFrequency']=='半年'].shape[0] #贷款账户还款频率为半年 计数
            dict_out['loan_second_rf_halfyear_ratio'] = loan_second[loan_second['repayFrequency']=='半年'].shape[0]/len(loan_second) #贷款账户还款频率为半年 占比

            dict_out['loan_second_guaranteeForm_giniimpurity'] = giniimpurity(loan_second['guaranteeForm']) #贷款账户担保方式基尼不纯度
            dict_out['loan_second_gf_crdit_count'] = loan_second[loan_second['guaranteeForm']=='信用/免担保'].shape[0] #贷款账户担保方式为其信用/免担保 计数
            dict_out['loan_second_gf_crdit_ratio'] = loan_second[loan_second['guaranteeForm']=='信用/免担保'].shape[0]/len(loan_second) #贷款账户担保方式为信用/免担保 占比
            dict_out['loan_second_gf_other_count'] = loan_second[loan_second['guaranteeForm']=='其他'].shape[0] #贷款账户担保方式为其他 计数
            dict_out['loan_second_gf_other_ratio'] = loan_second[loan_second['guaranteeForm']=='其他'].shape[0]/len(loan_second) #贷款账户担保方式为其他 占比
            dict_out['loan_second_gf_combine_nowarranty_count'] = loan_second[loan_second['guaranteeForm']=='组合（不含保证）'].shape[0] #贷款账户担保方式为组合（不含保证） 计数
            dict_out['loan_second_gf_combine_nowarranty_ratio'] = loan_second[loan_second['guaranteeForm']=='组合（不含保证）'].shape[0]/len(loan_second) #贷款账户担保方式为组合（不含保证） 占比
            dict_out['loan_second_gf_combine_warranty_count'] = loan_second[loan_second['guaranteeForm']=='组合(含保证)'].shape[0] #贷款账户担保方式为组合(含保证) 计数
            dict_out['loan_second_gf_combine_warranty_ratio'] = loan_second[loan_second['guaranteeForm']=='组合(含保证)'].shape[0]/len(loan_second) #贷款账户担保方式为组合(含保证) 占比
            dict_out['loan_second_gf_mortgage_count'] = loan_second[loan_second['guaranteeForm']=='抵押'].shape[0] #贷款账户担保方式为抵押 计数
            dict_out['loan_second_gf_mortgage_ratio'] = loan_second[loan_second['guaranteeForm']=='抵押'].shape[0]/len(loan_second) #贷款账户担保方式为抵押 占比
            dict_out['loan_second_gf_warranty_count'] = loan_second[loan_second['guaranteeForm']=='保证'].shape[0] #贷款账户担保方式为保证计数
            dict_out['loan_second_gf_warranty_ratio'] = loan_second[loan_second['guaranteeForm']=='保证'].shape[0]/len(loan_second) #贷款账户担保方式为保证 占比
            dict_out['loan_second_gf_pledge_count'] = loan_second[loan_second['guaranteeForm']=='质押'].shape[0] #贷款账户担保方式为质押 计数
            dict_out['loan_second_gf_pledge_ratio'] = loan_second[loan_second['guaranteeForm']=='质押'].shape[0]/len(loan_second) #贷款账户担保方式为质押 占比
            dict_out['loan_second_gf_farm_group_count'] = loan_second[loan_second['guaranteeForm']=='农户联保'].shape[0] #贷款账户担保方式为农户联保 计数
            dict_out['loan_second_gf_farm_group_ratio'] = loan_second[loan_second['guaranteeForm']=='农户联保'].shape[0]/len(loan_second) #贷款账户担保方式为农户联保 占比

            dict_out['loan_second_businessType_giniimpurity'] = giniimpurity(loan_second['businessType']) #贷款账户业务种类基尼不纯度
            dict_out['loan_second_bt_other_person_count'] = loan_second[loan_second['businessType']=='其他个人消费贷款'].shape[0] #贷款账户业务种类为其他个人消费贷款 计数
            dict_out['loan_second_bt_other_person_ratio'] = loan_second[loan_second['businessType']=='其他个人消费贷款'].shape[0]/len(loan_second) #贷款账户业务种类为其他个人消费贷款 占比
            dict_out['loan_second_bt_other_loan_count'] = loan_second[loan_second['businessType']=='其他贷款'].shape[0] #贷款账户业务种类为其他贷款 计数
            dict_out['loan_second_bt_other_loan_ratio'] = loan_second[loan_second['businessType']=='其他贷款'].shape[0]/len(loan_second) #贷款账户业务种类为其他贷款 占比
            dict_out['loan_second_bt_person_business_count'] = loan_second[loan_second['businessType']=='个人经营性贷款'].shape[0] #贷款账户业务种类为个人经营性贷款 计数
            dict_out['loan_second_bt_person_business_ratio'] = loan_second[loan_second['businessType']=='个人经营性贷款'].shape[0]/len(loan_second) #贷款账户业务种类为个人经营性贷款 占比
            dict_out['loan_second_bt_farm_loan_count'] = loan_second[loan_second['businessType']=='农户贷款'].shape[0] #贷款账户业务种类为农户贷款 计数
            dict_out['loan_second_bt_farm_loan_ratio'] = loan_second[loan_second['businessType']=='农户贷款'].shape[0]/len(loan_second) #贷款账户业务种类为农户贷款 占比
            dict_out['loan_second_bt_person_car_count'] = loan_second[loan_second['businessType']=='个人汽车消费贷款'].shape[0] #贷款账户业务种类为个人汽车消费贷款 计数
            dict_out['loan_second_bt_person_car_ratio'] = loan_second[loan_second['businessType']=='个人汽车消费贷款'].shape[0]/len(loan_second) #贷款账户业务种类为个人汽车消费贷款 占比
            dict_out['loan_second_bt_person_study_count'] = loan_second[loan_second['businessType']=='个人助学贷款'].shape[0] #贷款账户业务种类为个人助学贷款 计数
            dict_out['loan_second_bt_person_study_ratio'] = loan_second[loan_second['businessType']=='个人助学贷款'].shape[0]/len(loan_second) #贷款账户业务种类为个人助学贷款 占比
            dict_out['loan_second_bt_house_commercial_count'] = loan_second[loan_second['businessType']=='个人住房商业贷款'].shape[0] #贷款账户业务种类为个人住房商业贷款 计数
            dict_out['loan_second_bt_house_commercial_ratio'] = loan_second[loan_second['businessType']=='个人住房商业贷款'].shape[0]/len(loan_second) #贷款账户业务种类为个人住房商业贷款 占比
            dict_out['loan_second_bt_finance_lease_count'] = loan_second[loan_second['businessType']=='融资租赁业务'].shape[0] #贷款账户业务种类为融资租赁业务 计数
            dict_out['loan_second_bt_finance_lease_ratio'] = loan_second[loan_second['businessType']=='融资租赁业务'].shape[0]/len(loan_second) #贷款账户业务种类为融资租赁业务 占比
            dict_out['loan_second_bt_house_fund_count'] = loan_second[loan_second['businessType']=='个人住房公积金贷款'].shape[0] #贷款账户业务种类为个人住房公积金贷款 计数
            dict_out['loan_second_bt_house_fund_ratio'] = loan_second[loan_second['businessType']=='个人住房公积金贷款'].shape[0]/len(loan_second) #贷款账户业务种类为个人住房公积金贷款 占比
            dict_out['loan_second_bt_person_house_count'] = loan_second[loan_second['businessType']=='个人商用房（含商住两用）贷款'].shape[0] #贷款账户业务种类为个人商用房（含商住两用）贷款 计数
            dict_out['loan_second_bt_person_house_ratio'] = loan_second[loan_second['businessType']=='个人商用房（含商住两用）贷款'].shape[0]/len(loan_second) #贷款账户业务种类为个人商用房（含商住两用）贷款 占比
            dict_out['loan_second_bt_stock_pledge_count'] = loan_second[loan_second['businessType']=='股票质押式回购交易'].shape[0] #贷款账户业务种类为股票质押式回购交易计数
            dict_out['loan_second_bt_stock_pledge_ratio'] = loan_second[loan_second['businessType']=='股票质押式回购交易'].shape[0]/len(loan_second) #贷款账户业务种类为股票质押式回购交易 占比





        #现行
        loan_second_now = loan_second[loan_second.is_now==1]
        if len(loan_second_now)>0:

            dict_out['loan_second_now_planRepayAmount_mean'] = loan_second_now['planRepayAmount'].mean() #var平均值
            dict_out['loan_second_now_month60_State_count2r_mean'] = loan_second_now['month60_State_count2r'].mean() #var平均值
            dict_out['loan_second_now_month60_State_countUnknowr_mean'] = loan_second_now['month60_State_countUnknowr'].mean() #var平均值
            dict_out['loan_second_now_rf_other_count'] = loan_second_now[loan_second_now['repayFrequency']=='其他'].shape[0] #现行贷款账户还款频率为其他 计数
            dict_out['loan_second_now_bt_other_person_ratio'] = loan_second_now[loan_second_now['businessType']=='其他个人消费贷款'].shape[0]/len(loan_second_now) #现行贷款账户业务种类为其他个人消费贷款 占比
            dict_out['loan_second_now_gf_other_count'] = loan_second_now[loan_second_now['guaranteeForm']=='其他'].shape[0] #现行贷款账户担保方式为其他 计数
            dict_out['loan_second_now_month60_State_countNull_sum'] = loan_second_now['month60_State_countNull'].sum() #var求和
            dict_out['loan_second_now_org_commercial_bank_ratio'] = loan_second_now[loan_second_now['org']=='商业银行'].shape[0]/len(loan_second_now) #现行贷款账户管理机构为商业银行 占比
            dict_out['loan_second_now_month60_to_report_min_mean'] = loan_second_now['month60_to_report_min'].mean() #var平均值
            dict_out['loan_second_now_gf_other_ratio'] = loan_second_now[loan_second_now['guaranteeForm']=='其他'].shape[0]/len(loan_second_now) #现行贷款账户担保方式为其他 占比
            dict_out['loan_second_now_byDate_to_report_mean'] = loan_second_now['byDate_to_report'].mean() #var平均值
            dict_out['loan_second_now_repayMons_max'] = loan_second_now['repayMons'].max()#var最大值
            dict_out['loan_second_now_repayMons_min'] = loan_second_now['repayMons'].min() #var最小值
            dict_out['loan_second_now_repayMons_range'] = (dict_out['loan_second_now_repayMons_max']-dict_out['loan_second_now_repayMons_min'])/dict_out['loan_second_now_repayMons_max'] if dict_out['loan_second_now_repayMons_max']>0 else np.nan #var区间率
            dict_out['loan_second_now_bt_person_business_ratio'] = loan_second_now[loan_second_now['businessType']=='个人经营性贷款'].shape[0]/len(loan_second_now) #现行贷款账户业务种类为个人经营性贷款 占比
            dict_out['loan_second_now_gf_crdit_count'] = loan_second_now[loan_second_now['guaranteeForm']=='信用/免担保'].shape[0] #现行贷款账户担保方式为其信用/免担保 计数
            dict_out['loan_second_now_org_consumer_finance_ratio'] = loan_second_now[loan_second_now['org']=='消费金融公司'].shape[0]/len(loan_second_now) #现行贷款账户管理机构为消费金融公司 占比
            dict_out['loan_second_now_org_trust_company_count'] = loan_second_now[loan_second_now['org']=='信托公司'].shape[0] #现行贷款账户管理机构为信托公司 计数
            dict_out['loan_second_now_rf_irregular_ratio'] = loan_second_now[loan_second_now['repayFrequency']=='不定期'].shape[0]/len(loan_second_now) #现行贷款账户还款频率为不定期 占比
            dict_out['loan_second_now_guaranteeForm_giniimpurity'] = giniimpurity(loan_second_now['guaranteeForm']) #现行贷款账户担保方式基尼不纯度
            dict_out['loan_second_now_rf_other_ratio'] = loan_second_now[loan_second_now['repayFrequency']=='其他'].shape[0]/len(loan_second_now) #现行贷款账户还款频率为其他 占比
            dict_out['loan_second_now_gf_other_ratio'] = loan_second_now[loan_second_now['guaranteeForm']=='其他'].shape[0]/len(loan_second_now) #现行贷款账户担保方式为其他 占比
            dict_out['loan_second_now_bt_other_loan_count'] = loan_second_now[loan_second_now['businessType']=='其他贷款'].shape[0] #现行贷款账户业务种类为其他贷款 计数
            dict_out['loan_second_now_c5_normal_ratio'] = loan_second_now[loan_second_now['classify5']=='正常'].shape[0]/len(loan_second_now) #现行贷款账户五级分类为正常 占比
            dict_out['loan_second_now_rf_once_ratio'] = loan_second_now[loan_second_now['repayFrequency']=='一次性'].shape[0]/len(loan_second_now) #现行贷款账户还款频率为一次性 占比
            dict_out['loan_second_now_rf_other_count'] = loan_second_now[loan_second_now['repayFrequency']=='其他'].shape[0] #现行贷款账户还款频率为其他 计数
            dict_out['loan_second_now_org_consumer_finance_count'] = loan_second_now[loan_second_now['org']=='消费金融公司'].shape[0] #现行贷款账户管理机构为消费金融公司 计数
            dict_out['loan_second_now_loanGrantOrg_giniimpurity'] = giniimpurity(loan_second_now['loanGrantOrg']) #现行贷款账户管理机构详细基尼不纯度
            dict_out['loan_second_now_gf_other_count'] = loan_second_now[loan_second_now['guaranteeForm']=='其他'].shape[0] #现行贷款账户担保方式为其他 计数
            dict_out['loan_second_now_bt_other_person_ratio'] = loan_second_now[loan_second_now['businessType']=='其他个人消费贷款'].shape[0]/len(loan_second_now) #现行贷款账户业务种类为其他个人消费贷款 占比
            dict_out['loan_second_now_rf_once_count'] = loan_second_now[loan_second_now['repayFrequency']=='一次性'].shape[0] #现行贷款账户还款频率为一次性 计数
            dict_out['loan_second_now_rf_month_ratio'] = loan_second_now[loan_second_now['repayFrequency']=='月'].shape[0]/len(loan_second_now) #现行贷款账户还款频率为月 占比
            dict_out['loan_second_now_repayFrequency_giniimpurity'] = giniimpurity(loan_second_now['repayFrequency']) #现行贷款账户还款频率基尼不纯度
            dict_out['loan_second_now_org_commercial_bank_count'] = loan_second_now[loan_second_now['org']=='商业银行'].shape[0] #现行贷款账户管理机构为商业银行 计数
            dict_out['loan_second_now_startDate_to_report_mean'] = loan_second_now['startDate_to_report'].mean()  #var平均值比率
            dict_out['loan_second_now_month60_to_report_min_max'] = loan_second_now['month60_to_report_min'].max() #var最大值比率
            dict_out['loan_second_now_repayedAmount_mean'] = loan_second_now['repayedAmount'].mean()  #var平均值比率
            dict_out['loan_second_now_repayMons_ratio_mean'] = loan_second_now['repayMons_ratio'].mean()  #var平均值比率
            dict_out['loan_second_now_month60_State_countNull_max'] = loan_second_now['month60_State_countNull'].max() #var最大值比率
            dict_out['loan_second_now_month60_State_countNr_mean'] = loan_second_now['month60_State_countNr'].mean()  #var平均值比率
            dict_out['loan_second_now_repayMons_max'] = loan_second_now['repayMons'].max() #var最大值比率
            dict_out['loan_second_now_loanAmount_max'] = loan_second_now['loanAmount'].max()#var最大值
            dict_out['loan_second_now_loanAmount_min'] = loan_second_now['loanAmount'].min() #var最小值比率
            dict_out['loan_second_now_loanAmount_range'] = (dict_out['loan_second_now_loanAmount_max']-dict_out['loan_second_now_loanAmount_min'])/dict_out['loan_second_now_loanAmount_max'] if dict_out['loan_second_now_loanAmount_max']>0 else np.nan #var区间率
            dict_out['loan_second_now_byDate_to_report_mean'] = loan_second_now['byDate_to_report'].mean()  #var平均值比率
            dict_out['loan_second_now_repayTerms_mean'] = loan_second_now['repayTerms'].mean()  #var平均值比率
            dict_out['loan_second_now_startDate_to_report_max'] = loan_second_now['startDate_to_report'].max()#var最大值
            dict_out['loan_second_now_startDate_to_report_min'] = loan_second_now['startDate_to_report'].min() #var最小值比率
            dict_out['loan_second_now_startDate_to_report_range'] = (dict_out['loan_second_now_startDate_to_report_max']-dict_out['loan_second_now_startDate_to_report_min'])/dict_out['loan_second_now_startDate_to_report_max'] if dict_out['loan_second_now_startDate_to_report_max']>0 else np.nan #var区间率
            dict_out['loan_second_now_planRepayAmount_max'] = loan_second_now['planRepayAmount'].max()#var最大值
            dict_out['loan_second_now_planRepayAmount_min'] = loan_second_now['planRepayAmount'].min() #var最小值比率
            dict_out['loan_second_now_planRepayAmount_range'] = (dict_out['loan_second_now_planRepayAmount_max']-dict_out['loan_second_now_planRepayAmount_min'])/dict_out['loan_second_now_planRepayAmount_max'] if dict_out['loan_second_now_planRepayAmount_max']>0 else np.nan #var区间率
            dict_out['loan_second_now_month60_State_countN_max'] = loan_second_now['month60_State_countN'].max()#var最大值
            dict_out['loan_second_now_month60_State_countN_min'] = loan_second_now['month60_State_countN'].min() #var最小值比率
            dict_out['loan_second_now_month60_State_countN_range'] = (dict_out['loan_second_now_month60_State_countN_max']-dict_out['loan_second_now_month60_State_countN_min'])/dict_out['loan_second_now_month60_State_countN_max'] if dict_out['loan_second_now_month60_State_countN_max']>0 else np.nan #var区间率
            dict_out['loan_second_now_repayTerms_sum'] = loan_second_now['repayTerms'].sum() #var求和比率
            dict_out['loan_second_now_month60_State_countN_mean'] = loan_second_now['month60_State_countN'].mean()  #var平均值比率
            dict_out['loan_second_now_month60_State_countNullr_mean'] = loan_second_now['month60_State_countNullr'].mean()  #var平均值比率
            dict_out['loan_second_now_loanAmount_mean'] = loan_second_now['loanAmount'].mean()  #var平均值比率
            dict_out['loan_second_now_month60_State_countUnknow_sum'] = loan_second_now['month60_State_countUnknow'].sum() #var求和比率
            dict_out['loan_second_now_repayMons_ratio_max'] = loan_second_now['repayMons_ratio'].max()#var最大值
            dict_out['loan_second_now_repayMons_ratio_min'] = loan_second_now['repayMons_ratio'].min() #var最小值比率
            dict_out['loan_second_now_repayMons_ratio_range'] = (dict_out['loan_second_now_repayMons_ratio_max']-dict_out['loan_second_now_repayMons_ratio_min'])/dict_out['loan_second_now_repayMons_ratio_max'] if dict_out['loan_second_now_repayMons_ratio_max']>0 else np.nan #var区间率
            dict_out['loan_second_now_month60_State_countNullr_sum'] = loan_second_now['month60_State_countNullr'].sum() #var求和比率
            dict_out['loan_second_now_month60_State_countNull_max'] = loan_second_now['month60_State_countNull'].max()#var最大值
            dict_out['loan_second_now_month60_State_countNull_min'] = loan_second_now['month60_State_countNull'].min() #var最小值比率
            dict_out['loan_second_now_month60_State_countNull_range'] = (dict_out['loan_second_now_month60_State_countNull_max']-dict_out['loan_second_now_month60_State_countNull_min'])/dict_out['loan_second_now_month60_State_countNull_max'] if dict_out['loan_second_now_month60_State_countNull_max']>0 else np.nan #var区间率
            dict_out['loan_second_now_month60_State_countN_max'] = loan_second_now['month60_State_countN'].max() #var最大值比率
            dict_out['loan_second_now_repayMons_max'] = loan_second_now['repayMons'].max()#var最大值
            dict_out['loan_second_now_repayMons_min'] = loan_second_now['repayMons'].min() #var最小值比率
            dict_out['loan_second_now_repayMons_range'] = (dict_out['loan_second_now_repayMons_max']-dict_out['loan_second_now_repayMons_min'])/dict_out['loan_second_now_repayMons_max'] if dict_out['loan_second_now_repayMons_max']>0 else np.nan #var区间率
            dict_out['loan_second_now_repayAmt_mean'] = loan_second_now['repayAmt'].mean()  #var平均值比率
            dict_out['loan_second_now_repayMons_ratio_max'] = loan_second_now['repayMons_ratio'].max() #var最大值比率
            dict_out['loan_second_now_month60_State_countUnknowr_sum'] = loan_second_now['month60_State_countUnknowr'].sum() #var求和比率
            dict_out['loan_second_now_byDate_to_report_max'] = loan_second_now['byDate_to_report'].max() #var最大值比率
            dict_out['loan_second_now_month60_State_countNull_mean'] = loan_second_now['month60_State_countNull'].mean()  #var平均值比率
            dict_out['loan_second_now_planRepayAmount_mean'] = loan_second_now['planRepayAmount'].mean()  #var平均值比率
            dict_out['loan_second_now_bt_person_business_ratio'] = loan_second_now[loan_second_now['businessType']=='个人经营性贷款'].shape[0]/len(loan_second_now) #现行贷款账户业务种类为个人经营性贷款 占比
            dict_out['loan_second_nowR_bt_person_business_ratio'] = dict_out['loan_second_now_bt_person_business_ratio']/dict_out['loan_second_bt_person_business_ratio'] if dict_out['loan_second_bt_person_business_ratio']>0 else np.nan #var最大值比率
            dict_out['loan_second_nowR_startDate_to_report_mean'] = dict_out['loan_second_now_startDate_to_report_mean']/dict_out['loan_second_startDate_to_report_mean'] if dict_out['loan_second_startDate_to_report_mean']>0 else np.nan #var最大值比率
            dict_out['loan_second_nowR_month60_to_report_min_max'] = dict_out['loan_second_now_month60_to_report_min_max']/dict_out['loan_second_month60_to_report_min_max'] if dict_out['loan_second_month60_to_report_min_max']>0 else np.nan #var最大值比率
            dict_out['loan_second_now_gf_crdit_count'] = loan_second_now[loan_second_now['guaranteeForm']=='信用/免担保'].shape[0] #现行贷款账户担保方式为其信用/免担保 计数
            dict_out['loan_second_nowR_gf_crdit_count'] = dict_out['loan_second_now_gf_crdit_count']/dict_out['loan_second_gf_crdit_count'] if dict_out['loan_second_gf_crdit_count']>0 else np.nan #var最大值比率
            dict_out['loan_second_nowR_repayedAmount_mean'] = dict_out['loan_second_now_repayedAmount_mean']/dict_out['loan_second_repayedAmount_mean'] if dict_out['loan_second_repayedAmount_mean']>0 else np.nan #var最大值比率
            dict_out['loan_second_nowR_repayMons_ratio_mean'] = dict_out['loan_second_now_repayMons_ratio_mean']/dict_out['loan_second_repayMons_ratio_mean'] if dict_out['loan_second_repayMons_ratio_mean']>0 else np.nan #var最大值比率
            dict_out['loan_second_nowR_month60_State_countNull_max'] = dict_out['loan_second_now_month60_State_countNull_max']/dict_out['loan_second_month60_State_countNull_max'] if dict_out['loan_second_month60_State_countNull_max']>0 else np.nan #var最大值比率
            dict_out['loan_second_nowR_month60_State_countNr_mean'] = dict_out['loan_second_now_month60_State_countNr_mean']/dict_out['loan_second_month60_State_countNr_mean'] if dict_out['loan_second_month60_State_countNr_mean']>0 else np.nan #var最大值比率
            dict_out['loan_second_now_org_consumer_finance_ratio'] = loan_second_now[loan_second_now['org']=='消费金融公司'].shape[0]/len(loan_second_now) #现行贷款账户管理机构为消费金融公司 占比
            dict_out['loan_second_nowR_org_consumer_finance_ratio'] = dict_out['loan_second_now_org_consumer_finance_ratio']/dict_out['loan_second_org_consumer_finance_ratio'] if dict_out['loan_second_org_consumer_finance_ratio']>0 else np.nan #var最大值比率
            dict_out['loan_second_nowR_repayMons_max'] = dict_out['loan_second_now_repayMons_max']/dict_out['loan_second_repayMons_max'] if dict_out['loan_second_repayMons_max']>0 else np.nan #var最大值比率
            dict_out['loan_second_now_org_trust_company_count'] = loan_second_now[loan_second_now['org']=='信托公司'].shape[0] #现行贷款账户管理机构为信托公司 计数
            dict_out['loan_second_nowR_org_trust_company_count'] = dict_out['loan_second_now_org_trust_company_count']/dict_out['loan_second_org_trust_company_count'] if dict_out['loan_second_org_trust_company_count']>0 else np.nan #var最大值比率
            dict_out['loan_second_now_rf_irregular_ratio'] = loan_second_now[loan_second_now['repayFrequency']=='不定期'].shape[0]/len(loan_second_now) #现行贷款账户还款频率为不定期 占比
            dict_out['loan_second_nowR_rf_irregular_ratio'] = dict_out['loan_second_now_rf_irregular_ratio']/dict_out['loan_second_rf_irregular_ratio'] if dict_out['loan_second_rf_irregular_ratio']>0 else np.nan #var最大值比率
            dict_out['loan_second_nowR_loanAmount_range'] = dict_out['loan_second_now_loanAmount_range']/dict_out['loan_second_loanAmount_range'] if dict_out['loan_second_loanAmount_range']>0 else np.nan #var最大值比率
            dict_out['loan_second_now_guaranteeForm_giniimpurity'] = giniimpurity(loan_second_now['guaranteeForm']) #现行贷款账户担保方式基尼不纯度
            dict_out['loan_second_nowR_guaranteeForm_giniimpurity'] = dict_out['loan_second_now_guaranteeForm_giniimpurity']/dict_out['loan_second_guaranteeForm_giniimpurity'] if dict_out['loan_second_guaranteeForm_giniimpurity']>0 else np.nan #var最大值比率
            dict_out['loan_second_now_rf_other_ratio'] = loan_second_now[loan_second_now['repayFrequency']=='其他'].shape[0]/len(loan_second_now) #现行贷款账户还款频率为其他 占比
            dict_out['loan_second_nowR_rf_other_ratio'] = dict_out['loan_second_now_rf_other_ratio']/dict_out['loan_second_rf_other_ratio'] if dict_out['loan_second_rf_other_ratio']>0 else np.nan #var最大值比率
            dict_out['loan_second_nowR_byDate_to_report_mean'] = dict_out['loan_second_now_byDate_to_report_mean']/dict_out['loan_second_byDate_to_report_mean'] if dict_out['loan_second_byDate_to_report_mean']>0 else np.nan #var最大值比率
            dict_out['loan_second_nowR_repayTerms_mean'] = dict_out['loan_second_now_repayTerms_mean']/dict_out['loan_second_repayTerms_mean'] if dict_out['loan_second_repayTerms_mean']>0 else np.nan #var最大值比率
            dict_out['loan_second_nowR_startDate_to_report_range'] = dict_out['loan_second_now_startDate_to_report_range']/dict_out['loan_second_startDate_to_report_range'] if dict_out['loan_second_startDate_to_report_range']>0 else np.nan #var最大值比率
            dict_out['loan_second_now_gf_other_ratio'] = loan_second_now[loan_second_now['guaranteeForm']=='其他'].shape[0]/len(loan_second_now) #现行贷款账户担保方式为其他 占比
            dict_out['loan_second_nowR_gf_other_ratio'] = dict_out['loan_second_now_gf_other_ratio']/dict_out['loan_second_gf_other_ratio'] if dict_out['loan_second_gf_other_ratio']>0 else np.nan #var最大值比率
            dict_out['loan_second_nowR_planRepayAmount_range'] = dict_out['loan_second_now_planRepayAmount_range']/dict_out['loan_second_planRepayAmount_range'] if dict_out['loan_second_planRepayAmount_range']>0 else np.nan #var最大值比率
            dict_out['loan_second_now_bt_other_loan_count'] = loan_second_now[loan_second_now['businessType']=='其他贷款'].shape[0] #现行贷款账户业务种类为其他贷款 计数
            dict_out['loan_second_nowR_bt_other_loan_count'] = dict_out['loan_second_now_bt_other_loan_count']/dict_out['loan_second_bt_other_loan_count'] if dict_out['loan_second_bt_other_loan_count']>0 else np.nan #var最大值比率
            dict_out['loan_second_nowR_month60_State_countN_range'] = dict_out['loan_second_now_month60_State_countN_range']/dict_out['loan_second_month60_State_countN_range'] if dict_out['loan_second_month60_State_countN_range']>0 else np.nan #var最大值比率
            dict_out['loan_second_nowR_repayTerms_sum'] = dict_out['loan_second_now_repayTerms_sum']/dict_out['loan_second_repayTerms_sum'] if dict_out['loan_second_repayTerms_sum']>0 else np.nan #var最大值比率
            dict_out['loan_second_now_c5_normal_ratio'] = loan_second_now[loan_second_now['classify5']=='正常'].shape[0]/len(loan_second_now) #现行贷款账户五级分类为正常 占比
            dict_out['loan_second_nowR_c5_normal_ratio'] = dict_out['loan_second_now_c5_normal_ratio']/dict_out['loan_second_c5_normal_ratio'] if dict_out['loan_second_c5_normal_ratio']>0 else np.nan #var最大值比率
            dict_out['loan_second_nowR_month60_State_countN_mean'] = dict_out['loan_second_now_month60_State_countN_mean']/dict_out['loan_second_month60_State_countN_mean'] if dict_out['loan_second_month60_State_countN_mean']>0 else np.nan #var最大值比率
            dict_out['loan_second_now_rf_once_ratio'] = loan_second_now[loan_second_now['repayFrequency']=='一次性'].shape[0]/len(loan_second_now) #现行贷款账户还款频率为一次性 占比
            dict_out['loan_second_nowR_rf_once_ratio'] = dict_out['loan_second_now_rf_once_ratio']/dict_out['loan_second_rf_once_ratio'] if dict_out['loan_second_rf_once_ratio']>0 else np.nan #var最大值比率
            dict_out['loan_second_nowR_month60_State_countNullr_mean'] = dict_out['loan_second_now_month60_State_countNullr_mean']/dict_out['loan_second_month60_State_countNullr_mean'] if dict_out['loan_second_month60_State_countNullr_mean']>0 else np.nan #var最大值比率
            dict_out['loan_second_now_rf_other_count'] = loan_second_now[loan_second_now['repayFrequency']=='其他'].shape[0] #现行贷款账户还款频率为其他 计数
            dict_out['loan_second_nowR_rf_other_count'] = dict_out['loan_second_now_rf_other_count']/dict_out['loan_second_rf_other_count'] if dict_out['loan_second_rf_other_count']>0 else np.nan #var最大值比率
            dict_out['loan_second_now_org_consumer_finance_count'] = loan_second_now[loan_second_now['org']=='消费金融公司'].shape[0] #现行贷款账户管理机构为消费金融公司 计数
            dict_out['loan_second_nowR_org_consumer_finance_count'] = dict_out['loan_second_now_org_consumer_finance_count']/dict_out['loan_second_org_consumer_finance_count'] if dict_out['loan_second_org_consumer_finance_count']>0 else np.nan #var最大值比率
            dict_out['loan_second_nowR_loanAmount_mean'] = dict_out['loan_second_now_loanAmount_mean']/dict_out['loan_second_loanAmount_mean'] if dict_out['loan_second_loanAmount_mean']>0 else np.nan #var最大值比率
            dict_out['loan_second_now_loanGrantOrg_giniimpurity'] = giniimpurity(loan_second_now['loanGrantOrg']) #现行贷款账户管理机构详细基尼不纯度
            dict_out['loan_second_nowR_loanGrantOrg_giniimpurity'] = dict_out['loan_second_now_loanGrantOrg_giniimpurity']/dict_out['loan_second_loanGrantOrg_giniimpurity'] if dict_out['loan_second_loanGrantOrg_giniimpurity']>0 else np.nan #var最大值比率
            dict_out['loan_second_nowR_month60_State_countUnknow_sum'] = dict_out['loan_second_now_month60_State_countUnknow_sum']/dict_out['loan_second_month60_State_countUnknow_sum'] if dict_out['loan_second_month60_State_countUnknow_sum']>0 else np.nan #var最大值比率
            dict_out['loan_second_now_gf_other_count'] = loan_second_now[loan_second_now['guaranteeForm']=='其他'].shape[0] #现行贷款账户担保方式为其他 计数
            dict_out['loan_second_nowR_gf_other_count'] = dict_out['loan_second_now_gf_other_count']/dict_out['loan_second_gf_other_count'] if dict_out['loan_second_gf_other_count']>0 else np.nan #var最大值比率
            dict_out['loan_second_nowR_repayMons_ratio_range'] = dict_out['loan_second_now_repayMons_ratio_range']/dict_out['loan_second_repayMons_ratio_range'] if dict_out['loan_second_repayMons_ratio_range']>0 else np.nan #var最大值比率
            dict_out['loan_second_nowR_month60_State_countNullr_sum'] = dict_out['loan_second_now_month60_State_countNullr_sum']/dict_out['loan_second_month60_State_countNullr_sum'] if dict_out['loan_second_month60_State_countNullr_sum']>0 else np.nan #var最大值比率
            dict_out['loan_second_nowR_month60_State_countNull_range'] = dict_out['loan_second_now_month60_State_countNull_range']/dict_out['loan_second_month60_State_countNull_range'] if dict_out['loan_second_month60_State_countNull_range']>0 else np.nan #var最大值比率
            dict_out['loan_second_now_bt_other_person_ratio'] = loan_second_now[loan_second_now['businessType']=='其他个人消费贷款'].shape[0]/len(loan_second_now) #现行贷款账户业务种类为其他个人消费贷款 占比
            dict_out['loan_second_nowR_bt_other_person_ratio'] = dict_out['loan_second_now_bt_other_person_ratio']/dict_out['loan_second_bt_other_person_ratio'] if dict_out['loan_second_bt_other_person_ratio']>0 else np.nan #var最大值比率
            dict_out['loan_second_nowR_month60_State_countN_max'] = dict_out['loan_second_now_month60_State_countN_max']/dict_out['loan_second_month60_State_countN_max'] if dict_out['loan_second_month60_State_countN_max']>0 else np.nan #var最大值比率
            dict_out['loan_second_nowR_repayMons_range'] = dict_out['loan_second_now_repayMons_range']/dict_out['loan_second_repayMons_range'] if dict_out['loan_second_repayMons_range']>0 else np.nan #var最大值比率
            dict_out['loan_second_nowR_repayAmt_mean'] = dict_out['loan_second_now_repayAmt_mean']/dict_out['loan_second_repayAmt_mean'] if dict_out['loan_second_repayAmt_mean']>0 else np.nan #var最大值比率
            dict_out['loan_second_nowR_repayMons_ratio_max'] = dict_out['loan_second_now_repayMons_ratio_max']/dict_out['loan_second_repayMons_ratio_max'] if dict_out['loan_second_repayMons_ratio_max']>0 else np.nan #var最大值比率
            dict_out['loan_second_now_rf_once_count'] = loan_second_now[loan_second_now['repayFrequency']=='一次性'].shape[0] #现行贷款账户还款频率为一次性 计数
            dict_out['loan_second_nowR_rf_once_count'] = dict_out['loan_second_now_rf_once_count']/dict_out['loan_second_rf_once_count'] if dict_out['loan_second_rf_once_count']>0 else np.nan #var最大值比率
            dict_out['loan_second_now_rf_month_ratio'] = loan_second_now[loan_second_now['repayFrequency']=='月'].shape[0]/len(loan_second_now) #现行贷款账户还款频率为月 占比
            dict_out['loan_second_nowR_rf_month_ratio'] = dict_out['loan_second_now_rf_month_ratio']/dict_out['loan_second_rf_month_ratio'] if dict_out['loan_second_rf_month_ratio']>0 else np.nan #var最大值比率
            dict_out['loan_second_nowR_month60_State_countUnknowr_sum'] = dict_out['loan_second_now_month60_State_countUnknowr_sum']/dict_out['loan_second_month60_State_countUnknowr_sum'] if dict_out['loan_second_month60_State_countUnknowr_sum']>0 else np.nan #var最大值比率
            dict_out['loan_second_now_repayFrequency_giniimpurity'] = giniimpurity(loan_second_now['repayFrequency']) #现行贷款账户还款频率基尼不纯度
            dict_out['loan_second_nowR_repayFrequency_giniimpurity'] = dict_out['loan_second_now_repayFrequency_giniimpurity']/dict_out['loan_second_repayFrequency_giniimpurity'] if dict_out['loan_second_repayFrequency_giniimpurity']>0 else np.nan #var最大值比率
            dict_out['loan_second_nowR_byDate_to_report_max'] = dict_out['loan_second_now_byDate_to_report_max']/dict_out['loan_second_byDate_to_report_max'] if dict_out['loan_second_byDate_to_report_max']>0 else np.nan #var最大值比率
            dict_out['loan_second_nowR_month60_State_countNull_mean'] = dict_out['loan_second_now_month60_State_countNull_mean']/dict_out['loan_second_month60_State_countNull_mean'] if dict_out['loan_second_month60_State_countNull_mean']>0 else np.nan #var最大值比率
            dict_out['loan_second_nowR_planRepayAmount_mean'] = dict_out['loan_second_now_planRepayAmount_mean']/dict_out['loan_second_planRepayAmount_mean'] if dict_out['loan_second_planRepayAmount_mean']>0 else np.nan #var最大值比率
            dict_out['loan_second_now_org_commercial_bank_count'] = loan_second_now[loan_second_now['org']=='商业银行'].shape[0] #现行贷款账户管理机构为商业银行 计数
            dict_out['loan_second_nowR_org_commercial_bank_count'] = dict_out['loan_second_now_org_commercial_bank_count']/dict_out['loan_second_org_commercial_bank_count'] if dict_out['loan_second_org_commercial_bank_count']>0 else np.nan #var最大值比率


        #非循环贷账户
        loan_second_ncycle = loan_second[loan_second['class']=='非循环贷账户']
        if len(loan_second_ncycle)>0:
            dict_out['loan_second_ncycle_repayTerms_min'] = loan_second_ncycle['repayTerms'].min() #var最小值
            dict_out['loan_second_ncycle_bt_other_person_count'] = loan_second_ncycle[loan_second_ncycle['businessType']=='其他个人消费贷款'].shape[0] #非循环贷账户 贷款账户业务种类为其他个人消费贷款 计数
            dict_out['loan_second_ncycle_repayMons_ratio_min'] = loan_second_ncycle['repayMons_ratio'].min() #var最小值
            dict_out['loan_second_ncycle_org_consumer_finance_ratio'] = loan_second_ncycle[loan_second_ncycle['org']=='消费金融公司'].shape[0]/len(loan_second_ncycle) #非循环贷账户 贷款账户管理机构为消费金融公司 占比
            dict_out['loan_second_ncycle_month60_State_countNullr_max'] = loan_second_ncycle['month60_State_countNullr'].max()#var最大值
            dict_out['loan_second_ncycle_month60_State_countN_max'] = loan_second_ncycle['month60_State_countN'].max()#var最大值
            dict_out['loan_second_ncycle_month60_State_countUnknow_sum'] = loan_second_ncycle['month60_State_countUnknow'].sum() #var求和
            dict_out['loan_second_ncycle_org_giniimpurity'] = giniimpurity(loan_second_ncycle['org']) #非循环贷账户 贷款账户管理机构基尼不纯度
            dict_out['loan_second_ncycle_due_class_mean'] = loan_second_ncycle['due_class'].mean() #var平均值
            dict_out['loan_second_ncycle_as_settle_count'] = loan_second_ncycle[loan_second_ncycle['accountStatus']=='结清'].shape[0] #非循环贷账户 贷款账户账户状态为结清 计数
            dict_out['loan_second_ncycle_month60_State_countUnknow_mean'] = loan_second_ncycle['month60_State_countUnknow'].mean() #var平均值
            dict_out['loan_second_ncycle_month60_to_report_max_mean'] = loan_second_ncycle['month60_to_report_max'].mean() #var平均值
            dict_out['loan_second_ncycle_month60_State_countNr_max'] = loan_second_ncycle['month60_State_countNr'].max()#var最大值
            dict_out['loan_second_ncycle_org_commercial_bank_count'] = loan_second_ncycle[loan_second_ncycle['org']=='商业银行'].shape[0] #非循环贷账户 贷款账户管理机构为商业银行 计数
            dict_out['loan_second_ncycle_balance_mean'] = loan_second_ncycle['balance'].mean() #var平均值
            dict_out['loan_second_ncycle_class_ncycle_count'] = loan_second_ncycle[loan_second_ncycle['class']=='非循环贷账户'].shape[0] #非循环贷账户 贷款账户账户类别为非循环贷账户 计数
            dict_out['loan_second_ncycle_month60_State_countUnknow_max'] = loan_second_ncycle['month60_State_countUnknow'].max()#var最大值
            dict_out['loan_second_ncycle_loanGrantOrg_nunique'] = loan_second_ncycle['loanGrantOrg'].nunique() #非循环贷账户 贷款账户管理机构详细nunique
            dict_out['loan_second_ncycle_month60_to_report_min_sum'] = loan_second_ncycle['month60_to_report_min'].sum() #var求和
            dict_out['loan_second_ncycle_org_micro_loan_count'] = loan_second_ncycle[loan_second_ncycle['org']=='小额贷款公司'].shape[0] #非循环贷账户 贷款账户管理机构为小额贷款公司 计数
            dict_out['loan_second_ncycle_month60_State_num_mean_max'] = loan_second_ncycle['month60_State_num_mean'].max()#var最大值
            dict_out['loan_second_ncycle_month60_to_report_max_sum'] = loan_second_ncycle['month60_to_report_max'].sum() #var求和
            dict_out['loan_second_ncycle_gf_crdit_ratio'] = loan_second_ncycle[loan_second_ncycle['guaranteeForm']=='信用/免担保'].shape[0]/len(loan_second_ncycle) #非循环贷账户 贷款账户担保方式为信用/免担保 占比
            dict_out['loan_second_ncycle_repayMons_ratio_max'] = loan_second_ncycle['repayMons_ratio'].max()#var最大值
            dict_out['loan_second_ncycle_month60_State_countCr_sum'] = loan_second_ncycle['month60_State_countCr'].sum() #var求和
            dict_out['loan_second_ncycle_month60_State_num_size_min'] = loan_second_ncycle['month60_State_num_size'].min() #var最小值
            dict_out['loan_second_ncycle_rt_unknow_count'] = loan_second_ncycle[loan_second_ncycle['repayType']=='--'].shape[0] #非循环贷账户 贷款账户还款方式为-- 计数
            dict_out['loan_second_ncycle_repayedAmount_max'] = loan_second_ncycle['repayedAmount'].max()#var最大值
            dict_out['loan_second_ncycle_repayedAmount_min'] = loan_second_ncycle['repayedAmount'].min() #var最小值
            dict_out['loan_second_ncycle_repayedAmount_range'] = (dict_out['loan_second_ncycle_repayedAmount_max']-dict_out['loan_second_ncycle_repayedAmount_min'])/dict_out['loan_second_ncycle_repayedAmount_max'] if dict_out['loan_second_ncycle_repayedAmount_max']>0 else np.nan #var区间率
            dict_out['loan_second_ncycle_rf_once_count'] = loan_second_ncycle[loan_second_ncycle['repayFrequency']=='一次性'].shape[0] #非循环贷账户 贷款账户还款频率为一次性 计数
            dict_out['loan_second_ncycle_month60_to_report_min_max'] = loan_second_ncycle['month60_to_report_min'].max()#var最大值
            dict_out['loan_second_ncycle_month60_to_report_min_min'] = loan_second_ncycle['month60_to_report_min'].min() #var最小值
            dict_out['loan_second_ncycle_month60_to_report_min_range'] = (dict_out['loan_second_ncycle_month60_to_report_min_max']-dict_out['loan_second_ncycle_month60_to_report_min_min'])/dict_out['loan_second_ncycle_month60_to_report_min_max'] if dict_out['loan_second_ncycle_month60_to_report_min_max']>0 else np.nan #var区间率
            dict_out['loan_second_ncycle_repayMons_ratio_sum'] = loan_second_ncycle['repayMons_ratio'].sum() #var求和
            dict_out['loan_second_ncycle_month60_State_num_mean_mean'] = loan_second_ncycle['month60_State_num_mean'].mean() #var平均值
            dict_out['loan_second_ncycle_month60_to_report_mean_sum'] = loan_second_ncycle['month60_to_report_mean'].sum() #var求和
            dict_out['loan_second_ncycle_org_trust_company_count'] = loan_second_ncycle[loan_second_ncycle['org']=='信托公司'].shape[0] #非循环贷账户 贷款账户管理机构为信托公司 计数
            dict_out['loan_second_ncycle_month60_State_countNull_sum'] = loan_second_ncycle['month60_State_countNull'].sum() #var求和
            dict_out['loan_second_ncycle_gf_combine_nowarranty_count'] = loan_second_ncycle[loan_second_ncycle['guaranteeForm']=='组合（不含保证）'].shape[0] #非循环贷账户 贷款账户担保方式为组合（不含保证） 计数
            dict_out['loan_second_ncycle_month60_State_countNr_sum'] = loan_second_ncycle['month60_State_countNr'].sum() #var求和
            dict_out['loan_second_ncycle_rf_month_count'] = loan_second_ncycle[loan_second_ncycle['repayFrequency']=='月'].shape[0] #非循环贷账户 贷款账户还款频率为月 计数
            dict_out['loan_second_ncycle_repayAmt_max'] = loan_second_ncycle['repayAmt'].max()#var最大值
            dict_out['loan_second_ncycle_businessType_giniimpurity'] = giniimpurity(loan_second_ncycle['businessType']) #非循环贷账户 贷款账户业务种类基尼不纯度
            dict_out['loan_second_ncycle_byDate_to_report_mean'] = loan_second_ncycle['byDate_to_report'].mean() #var平均值
            dict_out['loan_second_ncycle_month60_State_num_size_max'] = loan_second_ncycle['month60_State_num_size'].max()#var最大值
            dict_out['loan_second_ncycle_month60_State_num_size_min'] = loan_second_ncycle['month60_State_num_size'].min() #var最小值
            dict_out['loan_second_ncycle_month60_State_num_size_range'] = (dict_out['loan_second_ncycle_month60_State_num_size_max']-dict_out['loan_second_ncycle_month60_State_num_size_min'])/dict_out['loan_second_ncycle_month60_State_num_size_max'] if dict_out['loan_second_ncycle_month60_State_num_size_max']>0 else np.nan #var区间率
            dict_out['loan_second_ncycle_org_trust_company_ratio'] = loan_second_ncycle[loan_second_ncycle['org']=='信托公司'].shape[0]/len(loan_second_ncycle) #非循环贷账户 贷款账户管理机构为信托公司 占比
            dict_out['loan_second_ncycle_byDate_to_report_sum'] = loan_second_ncycle['byDate_to_report'].sum() #var求和
            dict_out['loan_second_ncycle_repayAmt_min'] = loan_second_ncycle['repayAmt'].min() #var最小值
            dict_out['loan_second_ncycle_rf_once_ratio'] = loan_second_ncycle[loan_second_ncycle['repayFrequency']=='一次性'].shape[0]/len(loan_second_ncycle) #非循环贷账户 贷款账户还款频率为一次性 占比
            dict_out['loan_second_ncycle_month60_State_num_size_max'] = loan_second_ncycle['month60_State_num_size'].max()#var最大值
            dict_out['loan_second_ncycle_month60_State_countUnknowr_max'] = loan_second_ncycle['month60_State_countUnknowr'].max()#var最大值
            dict_out['loan_second_ncycle_month60_State_countCr_mean'] = loan_second_ncycle['month60_State_countCr'].mean() #var平均值
            dict_out['loan_second_ncycle_RepayedAmount_ratio_max'] = loan_second_ncycle['RepayedAmount_ratio'].max()#var最大值
            dict_out['loan_second_ncycle_RepayedAmount_ratio_min'] = loan_second_ncycle['RepayedAmount_ratio'].min() #var最小值
            dict_out['loan_second_ncycle_RepayedAmount_ratio_range'] = (dict_out['loan_second_ncycle_RepayedAmount_ratio_max']-dict_out['loan_second_ncycle_RepayedAmount_ratio_min'])/dict_out['loan_second_ncycle_RepayedAmount_ratio_max'] if dict_out['loan_second_ncycle_RepayedAmount_ratio_max']>0 else np.nan #var区间率
            dict_out['loan_second_ncycle_month60_to_report_max_max'] = loan_second_ncycle['month60_to_report_max'].max()#var最大值
            dict_out['loan_second_ncycle_month60_to_report_max_min'] = loan_second_ncycle['month60_to_report_max'].min() #var最小值
            dict_out['loan_second_ncycle_month60_to_report_max_range'] = (dict_out['loan_second_ncycle_month60_to_report_max_max']-dict_out['loan_second_ncycle_month60_to_report_max_min'])/dict_out['loan_second_ncycle_month60_to_report_max_max'] if dict_out['loan_second_ncycle_month60_to_report_max_max']>0 else np.nan #var区间率
            dict_out['loan_second_ncycle_month60_to_report_mean_max'] = loan_second_ncycle['month60_to_report_mean'].max()#var最大值
            dict_out['loan_second_ncycle_c5_unknow_count'] = loan_second_ncycle[loan_second_ncycle['classify5']==''].shape[0] #非循环贷账户 贷款账户五级分类为'' 计数
            dict_out['loan_second_ncycle_month60_State_num_size_sum'] = loan_second_ncycle['month60_State_num_size'].sum() #var求和
            dict_out['loan_second_ncycle_repayFrequency_giniimpurity'] = giniimpurity(loan_second_ncycle['repayFrequency']) #非循环贷账户 贷款账户还款频率基尼不纯度
            dict_out['loan_second_ncycle_month60_State_countN_sum'] = loan_second_ncycle['month60_State_countN'].sum() #var求和
            dict_out['loan_second_ncycle_org_commercial_bank_ratio'] = loan_second_ncycle[loan_second_ncycle['org']=='商业银行'].shape[0]/len(loan_second_ncycle) #非循环贷账户 贷款账户管理机构为商业银行 占比
            dict_out['loan_second_ncycle_repayMons_sum'] = loan_second_ncycle['repayMons'].sum() #var求和
            dict_out['loan_second_ncycle_rf_other_ratio'] = loan_second_ncycle[loan_second_ncycle['repayFrequency']=='其他'].shape[0]/len(loan_second_ncycle) #非循环贷账户 贷款账户还款频率为其他 占比
            dict_out['loan_second_ncycle_loanAmount_max'] = loan_second_ncycle['loanAmount'].max()#var最大值
            dict_out['loan_second_ncycle_loanAmount_min'] = loan_second_ncycle['loanAmount'].min() #var最小值
            dict_out['loan_second_ncycle_loanAmount_range'] = (dict_out['loan_second_ncycle_loanAmount_max']-dict_out['loan_second_ncycle_loanAmount_min'])/dict_out['loan_second_ncycle_loanAmount_max'] if dict_out['loan_second_ncycle_loanAmount_max']>0 else np.nan #var区间率
            dict_out['loan_second_ncycle_month60_State_countC_sum'] = loan_second_ncycle['month60_State_countC'].sum() #var求和
            dict_out['loan_second_ncycle_gf_crdit_count'] = loan_second_ncycle[loan_second_ncycle['guaranteeForm']=='信用/免担保'].shape[0] #非循环贷账户 贷款账户担保方式为其信用/免担保 计数
            dict_out['loan_second_ncycle_as_settle_ratio'] = loan_second_ncycle[loan_second_ncycle['accountStatus']=='结清'].shape[0]/len(loan_second_ncycle) #非循环贷账户 贷款账户账户状态为结清 占比
            dict_out['loan_second_ncycle_RepayedAmount_ratio_sum'] = loan_second_ncycle['RepayedAmount_ratio'].sum() #var求和
            dict_out['loan_second_ncycle_month60_State_countNull_mean'] = loan_second_ncycle['month60_State_countNull'].mean() #var平均值
            dict_out['loan_second_ncycle_month60_State_countNullr_sum'] = loan_second_ncycle['month60_State_countNullr'].sum() #var求和
            dict_out['loan_second_ncycle_repayMons_ratio_mean'] = loan_second_ncycle['repayMons_ratio'].mean() #var平均值
            dict_out['loan_second_ncycle_loanGrantOrg_giniimpurity'] = giniimpurity(loan_second_ncycle['loanGrantOrg']) #非循环贷账户 贷款账户管理机构详细基尼不纯度
            dict_out['loan_second_ncycle_month60_State_countUnknow_max'] = loan_second_ncycle['month60_State_countUnknow'].max()#var最大值
            dict_out['loan_second_ncycle_month60_State_countUnknow_min'] = loan_second_ncycle['month60_State_countUnknow'].min() #var最小值
            dict_out['loan_second_ncycle_month60_State_countUnknow_range'] = (dict_out['loan_second_ncycle_month60_State_countUnknow_max']-dict_out['loan_second_ncycle_month60_State_countUnknow_min'])/dict_out['loan_second_ncycle_month60_State_countUnknow_max'] if dict_out['loan_second_ncycle_month60_State_countUnknow_max']>0 else np.nan #var区间率
            dict_out['loan_second_ncycle_org_consumer_finance_count'] = loan_second_ncycle[loan_second_ncycle['org']=='消费金融公司'].shape[0] #非循环贷账户 贷款账户管理机构为消费金融公司 计数
            dict_out['loan_second_ncycle_bt_other_loan_count'] = loan_second_ncycle[loan_second_ncycle['businessType']=='其他贷款'].shape[0] #非循环贷账户 贷款账户业务种类为其他贷款 计数
            dict_out['loan_second_ncycle_month60_State_countNull_max'] = loan_second_ncycle['month60_State_countNull'].max()#var最大值
            dict_out['loan_second_ncycle_repayAmt_max'] = loan_second_ncycle['repayAmt'].max()#var最大值
            dict_out['loan_second_ncycle_repayAmt_min'] = loan_second_ncycle['repayAmt'].min() #var最小值
            dict_out['loan_second_ncycle_repayAmt_range'] = (dict_out['loan_second_ncycle_repayAmt_max']-dict_out['loan_second_ncycle_repayAmt_min'])/dict_out['loan_second_ncycle_repayAmt_max'] if dict_out['loan_second_ncycle_repayAmt_max']>0 else np.nan #var区间率
            dict_out['loan_second_ncycle_startDate_to_report_sum'] = loan_second_ncycle['startDate_to_report'].sum() #var求和
            dict_out['loan_second_ncycle_loanAmount_min'] = loan_second_ncycle['loanAmount'].min() #var最小值
            dict_out['loan_second_ncycle_org_giniimpurity'] = giniimpurity(loan_second_ncycle['org']) #非循环贷账户 贷款账户管理机构基尼不纯度
            dict_out['loan_second_ncycle_org_commercial_bank_ratio'] = loan_second_ncycle[loan_second_ncycle['org']=='商业银行'].shape[0]/len(loan_second_ncycle) #非循环贷账户 贷款账户管理机构为商业银行 占比
            dict_out['loan_second_ncycle_gf_crdit_ratio'] = loan_second_ncycle[loan_second_ncycle['guaranteeForm']=='信用/免担保'].shape[0]/len(loan_second_ncycle) #非循环贷账户 贷款账户担保方式为信用/免担保 占比
            dict_out['loan_second_ncycle_month60_State_countNull_sum'] = loan_second_ncycle['month60_State_countNull'].sum() #var求和比率
            dict_out['loan_second_ncycle_month60_to_report_max_sum'] = loan_second_ncycle['month60_to_report_max'].sum() #var求和比率
            dict_out['loan_second_ncycle_month60_State_countNr_mean'] = loan_second_ncycle['month60_State_countNr'].mean()  #var平均值比率
            dict_out['loan_second_ncycle_repayMons_ratio_mean'] = loan_second_ncycle['repayMons_ratio'].mean()  #var平均值比率
            dict_out['loan_second_ncycle_balance_mean'] = loan_second_ncycle['balance'].mean()  #var平均值比率
            dict_out['loan_second_ncycle_repayTerms_mean'] = loan_second_ncycle['repayTerms'].mean()  #var平均值比率
            dict_out['loan_second_ncycle_month60_to_report_min_sum'] = loan_second_ncycle['month60_to_report_min'].sum() #var求和比率
            dict_out['loan_second_ncycle_month60_State_countNullr_mean'] = loan_second_ncycle['month60_State_countNullr'].mean()  #var平均值比率
            dict_out['loan_second_ncycle_month60_to_report_max_min'] = loan_second_ncycle['month60_to_report_max'].min() #var最小值比率
            dict_out['loan_second_ncycleR_month60_State_countNull_sum'] = dict_out['loan_second_ncycle_month60_State_countNull_sum']/dict_out['loan_second_month60_State_countNull_sum'] if dict_out['loan_second_month60_State_countNull_sum']>0 else np.nan #var最大值比率
            dict_out['loan_second_ncycleR_month60_to_report_max_sum'] = dict_out['loan_second_ncycle_month60_to_report_max_sum']/dict_out['loan_second_month60_to_report_max_sum'] if dict_out['loan_second_month60_to_report_max_sum']>0 else np.nan #var最大值比率
            dict_out['loan_second_ncycle_org_giniimpurity'] = giniimpurity(loan_second_ncycle['org']) #非循环贷账户 贷款账户管理机构基尼不纯度
            dict_out['loan_second_ncycleR_org_giniimpurity'] = dict_out['loan_second_ncycle_org_giniimpurity']/dict_out['loan_second_org_giniimpurity'] if dict_out['loan_second_org_giniimpurity']>0 else np.nan #var最大值比率
            dict_out['loan_second_ncycleR_month60_State_countNr_mean'] = dict_out['loan_second_ncycle_month60_State_countNr_mean']/dict_out['loan_second_month60_State_countNr_mean'] if dict_out['loan_second_month60_State_countNr_mean']>0 else np.nan #var最大值比率
            dict_out['loan_second_ncycleR_repayMons_ratio_mean'] = dict_out['loan_second_ncycle_repayMons_ratio_mean']/dict_out['loan_second_repayMons_ratio_mean'] if dict_out['loan_second_repayMons_ratio_mean']>0 else np.nan #var最大值比率
            dict_out['loan_second_ncycleR_balance_mean'] = dict_out['loan_second_ncycle_balance_mean']/dict_out['loan_second_balance_mean'] if dict_out['loan_second_balance_mean']>0 else np.nan #var最大值比率
            dict_out['loan_second_ncycle_org_commercial_bank_ratio'] = loan_second_ncycle[loan_second_ncycle['org']=='商业银行'].shape[0]/len(loan_second_ncycle) #非循环贷账户 贷款账户管理机构为商业银行 占比
            dict_out['loan_second_ncycleR_org_commercial_bank_ratio'] = dict_out['loan_second_ncycle_org_commercial_bank_ratio']/dict_out['loan_second_org_commercial_bank_ratio'] if dict_out['loan_second_org_commercial_bank_ratio']>0 else np.nan #var最大值比率
            dict_out['loan_second_ncycleR_repayTerms_mean'] = dict_out['loan_second_ncycle_repayTerms_mean']/dict_out['loan_second_repayTerms_mean'] if dict_out['loan_second_repayTerms_mean']>0 else np.nan #var最大值比率
            dict_out['loan_second_ncycle_gf_crdit_ratio'] = loan_second_ncycle[loan_second_ncycle['guaranteeForm']=='信用/免担保'].shape[0]/len(loan_second_ncycle) #非循环贷账户 贷款账户担保方式为信用/免担保 占比
            dict_out['loan_second_ncycleR_gf_crdit_ratio'] = dict_out['loan_second_ncycle_gf_crdit_ratio']/dict_out['loan_second_gf_crdit_ratio'] if dict_out['loan_second_gf_crdit_ratio']>0 else np.nan #var最大值比率
            dict_out['loan_second_ncycleR_month60_to_report_min_sum'] = dict_out['loan_second_ncycle_month60_to_report_min_sum']/dict_out['loan_second_month60_to_report_min_sum'] if dict_out['loan_second_month60_to_report_min_sum']>0 else np.nan #var最大值比率
            dict_out['loan_second_ncycleR_month60_State_countNullr_mean'] = dict_out['loan_second_ncycle_month60_State_countNullr_mean']/dict_out['loan_second_month60_State_countNullr_mean'] if dict_out['loan_second_month60_State_countNullr_mean']>0 else np.nan #var最大值比率
            dict_out['loan_second_ncycleR_month60_to_report_max_min'] = dict_out['loan_second_ncycle_month60_to_report_max_min']/dict_out['loan_second_month60_to_report_max_min'] if dict_out['loan_second_month60_to_report_max_min']>0 else np.nan #var最大值比率



        #循环贷账户
        loan_second_cycle = loan_second[loan_second['class']=='循环贷账户']
        if len(loan_second_cycle)>0:

            dict_out['loan_second_cycle_repayTerms_max'] = loan_second_cycle['repayTerms'].max()#var最大值
            dict_out['loan_second_cycle_repayTerms_min'] = loan_second_cycle['repayTerms'].min() #var最小值
            dict_out['loan_second_cycle_repayTerms_range'] = (dict_out['loan_second_cycle_repayTerms_max']-dict_out['loan_second_cycle_repayTerms_min'])/dict_out['loan_second_cycle_repayTerms_max'] if dict_out['loan_second_cycle_repayTerms_max']>0 else np.nan #var区间率
            dict_out['loan_second_cycle_month60_State_countNull_sum'] = loan_second_cycle['month60_State_countNull'].sum() #var求和比率
            dict_out['loan_second_cycleR_month60_State_countNull_sum'] = dict_out['loan_second_cycle_month60_State_countNull_sum']/dict_out['loan_second_month60_State_countNull_sum'] if dict_out['loan_second_month60_State_countNull_sum']>0 else np.nan #var最大值比率

        #有担保 is_vouch 
        loan_second_vouch = loan_second[loan_second['is_vouch']==1]
        if len(loan_second_vouch)>0:

            dict_out['loan_second_vouch_month60_State_countNr_mean'] = loan_second_vouch['month60_State_countNr'].mean() #var平均值
            dict_out['loan_second_vouch_month60_State_countNr_min'] = loan_second_vouch['month60_State_countNr'].min() #var最小值
            dict_out['loan_second_vouch_loanAmount_mean'] = loan_second_vouch['loanAmount'].mean() #var平均值
            dict_out['loan_second_vouch_month60_State_countCr_sum'] = loan_second_vouch['month60_State_countCr'].sum() #var求和
            dict_out['loan_second_vouch_month60_to_report_max_sum'] = loan_second_vouch['month60_to_report_max'].sum() #var求和
            dict_out['loan_second_vouch_loanAmount_min'] = loan_second_vouch['loanAmount'].min() #var最小值
            dict_out['loan_second_vouch_class_ncycle_count'] = loan_second_vouch[loan_second_vouch['class']=='非循环贷账户'].shape[0] #有担保贷款账户账户类别为非循环贷账户 计数
            dict_out['loan_second_vouch_rf_month_count'] = loan_second_vouch[loan_second_vouch['repayFrequency']=='月'].shape[0] #有担保贷款账户还款频率为月 计数
            dict_out['loan_second_vouch_rf_month_ratio'] = loan_second_vouch[loan_second_vouch['repayFrequency']=='月'].shape[0]/len(loan_second_vouch) #有担保贷款账户还款频率为月 占比
            dict_out['loan_second_vouch_loanGrantOrg_nunique'] = loan_second_vouch['loanGrantOrg'].nunique() #有担保贷款账户管理机构详细nunique
            dict_out['loan_second_vouch_repayMons_sum'] = loan_second_vouch['repayMons'].sum() #var求和比率
            dict_out['loan_second_vouch_repayMons_ratio_mean'] = loan_second_vouch['repayMons_ratio'].mean()  #var平均值比率
            dict_out['loan_second_vouch_month60_State_countN_sum'] = loan_second_vouch['month60_State_countN'].sum() #var求和比率
            dict_out['loan_second_vouch_month60_to_report_max_sum'] = loan_second_vouch['month60_to_report_max'].sum() #var求和比率
            dict_out['loan_second_vouch_balance_ratio_mean'] = loan_second_vouch['balance_ratio'].mean()  #var平均值比率
            dict_out['loan_second_vouch_classify5_num_sum'] = loan_second_vouch['classify5_num'].sum() #var求和比率
            dict_out['loan_second_vouch_repayAmt_min'] = loan_second_vouch['repayAmt'].min() #var最小值比率
            dict_out['loan_second_vouch_month60_State_countNull_sum'] = loan_second_vouch['month60_State_countNull'].sum() #var求和比率
            dict_out['loan_second_vouch_month60_State_num_size_sum'] = loan_second_vouch['month60_State_num_size'].sum() #var求和比率
            dict_out['loan_second_vouch_repayTerms_sum'] = loan_second_vouch['repayTerms'].sum() #var求和比率
            dict_out['loan_second_vouchR_repayMons_sum'] = dict_out['loan_second_vouch_repayMons_sum']/dict_out['loan_second_repayMons_sum'] if dict_out['loan_second_repayMons_sum']>0 else np.nan #var最大值比率
            dict_out['loan_second_vouch_class_ncycle_count'] = loan_second_vouch[loan_second_vouch['class']=='非循环贷账户'].shape[0] #有担保贷款账户账户类别为非循环贷账户 计数
            dict_out['loan_second_vouchR_class_ncycle_count'] = dict_out['loan_second_vouch_class_ncycle_count']/dict_out['loan_second_class_ncycle_count'] if dict_out['loan_second_class_ncycle_count']>0 else np.nan #var最大值比率
            dict_out['loan_second_vouchR_repayMons_ratio_mean'] = dict_out['loan_second_vouch_repayMons_ratio_mean']/dict_out['loan_second_repayMons_ratio_mean'] if dict_out['loan_second_repayMons_ratio_mean']>0 else np.nan #var最大值比率
            dict_out['loan_second_vouchR_month60_State_countN_sum'] = dict_out['loan_second_vouch_month60_State_countN_sum']/dict_out['loan_second_month60_State_countN_sum'] if dict_out['loan_second_month60_State_countN_sum']>0 else np.nan #var最大值比率
            dict_out['loan_second_vouchR_month60_to_report_max_sum'] = dict_out['loan_second_vouch_month60_to_report_max_sum']/dict_out['loan_second_month60_to_report_max_sum'] if dict_out['loan_second_month60_to_report_max_sum']>0 else np.nan #var最大值比率
            dict_out['loan_second_vouch_rf_month_count'] = loan_second_vouch[loan_second_vouch['repayFrequency']=='月'].shape[0] #有担保贷款账户还款频率为月 计数
            dict_out['loan_second_vouchR_rf_month_count'] = dict_out['loan_second_vouch_rf_month_count']/dict_out['loan_second_rf_month_count'] if dict_out['loan_second_rf_month_count']>0 else np.nan #var最大值比率
            dict_out['loan_second_vouchR_balance_ratio_mean'] = dict_out['loan_second_vouch_balance_ratio_mean']/dict_out['loan_second_balance_ratio_mean'] if dict_out['loan_second_balance_ratio_mean']>0 else np.nan #var最大值比率
            dict_out['loan_second_vouchR_classify5_num_sum'] = dict_out['loan_second_vouch_classify5_num_sum']/dict_out['loan_second_classify5_num_sum'] if dict_out['loan_second_classify5_num_sum']>0 else np.nan #var最大值比率
            dict_out['loan_second_vouchR_repayAmt_min'] = dict_out['loan_second_vouch_repayAmt_min']/dict_out['loan_second_repayAmt_min'] if dict_out['loan_second_repayAmt_min']>0 else np.nan #var最大值比率
            dict_out['loan_second_vouch_rf_month_ratio'] = loan_second_vouch[loan_second_vouch['repayFrequency']=='月'].shape[0]/len(loan_second_vouch) #有担保贷款账户还款频率为月 占比
            dict_out['loan_second_vouchR_rf_month_ratio'] = dict_out['loan_second_vouch_rf_month_ratio']/dict_out['loan_second_rf_month_ratio'] if dict_out['loan_second_rf_month_ratio']>0 else np.nan #var最大值比率
            dict_out['loan_second_vouchR_month60_State_countNull_sum'] = dict_out['loan_second_vouch_month60_State_countNull_sum']/dict_out['loan_second_month60_State_countNull_sum'] if dict_out['loan_second_month60_State_countNull_sum']>0 else np.nan #var最大值比率
            dict_out['loan_second_vouchR_month60_State_num_size_sum'] = dict_out['loan_second_vouch_month60_State_num_size_sum']/dict_out['loan_second_month60_State_num_size_sum'] if dict_out['loan_second_month60_State_num_size_sum']>0 else np.nan #var最大值比率
            dict_out['loan_second_vouchR_repayTerms_sum'] = dict_out['loan_second_vouch_repayTerms_sum']/dict_out['loan_second_repayTerms_sum'] if dict_out['loan_second_repayTerms_sum']>0 else np.nan #var最大值比率
            dict_out['loan_second_vouch_loanGrantOrg_nunique'] = loan_second_vouch['loanGrantOrg'].nunique() #有担保贷款账户管理机构详细nunique
            dict_out['loan_second_vouchR_loanGrantOrg_nunique'] = dict_out['loan_second_vouch_loanGrantOrg_nunique']/dict_out['loan_second_loanGrantOrg_nunique'] if dict_out['loan_second_loanGrantOrg_nunique']>0 else np.nan #var最大值比率

            #有担保率
            for var in numeric_vers:
                dict_out['loan_second_vouchR_'+var+'_max'] = dict_out['loan_second_vouch_'+var+'_max']/dict_out['loan_second_'+var+'_max'] if dict_out['loan_second_'+var+'_max']>0 else np.nan #有担保var最大值比率
                dict_out['loan_second_vouchR_'+var+'_min'] = dict_out['loan_second_vouch_'+var+'_min']/dict_out['loan_second_'+var+'_min'] if dict_out['loan_second_'+var+'_min']>0 else np.nan #有担保var最小值比率
                dict_out['loan_second_vouchR_'+var+'_mean'] = dict_out['loan_second_vouch_'+var+'_mean']/dict_out['loan_second_'+var+'_mean'] if dict_out['loan_second_'+var+'_mean']>0 else np.nan  #有担保var平均值比率
                dict_out['loan_second_vouchR_'+var+'_sum'] = dict_out['loan_second_vouch_'+var+'_sum']/dict_out['loan_second_'+var+'_sum'] if dict_out['loan_second_'+var+'_sum']>0 else np.nan  #有担保var求和比率
                dict_out['loan_second_vouchR_'+var+'_range'] = dict_out['loan_second_vouch_'+var+'_range']/dict_out['loan_second_'+var+'_range'] if dict_out['loan_second_'+var+'_range']>0 else np.nan  #有担保var区间率比率
            for var in loopvars:
                dict_out['loan_second_vouchR_'+var] = dict_out['loan_second_vouch_'+var]/dict_out['loan_second_'+var] if dict_out['loan_second_'+var]>0 else np.nan #有担保var最大值比率

        #历史逾期
        loan_second_hdue1 = loan_second[loan_second['is_overdue']==1]
        if len(loan_second_hdue1)>0:
            dict_out['loan_second_hdue1_month60_to_report_mean_max'] = loan_second_hdue1['month60_to_report_mean'].max()#var最大值
            dict_out['loan_second_hdue1_repayAmt_mean'] = loan_second_hdue1['repayAmt'].mean() #var平均值
            dict_out['loan_second_hdue1_month60_State_num_mean_min'] = loan_second_hdue1['month60_State_num_mean'].min() #var最小值
            dict_out['loan_second_hdue1_month60_State_countNr_min'] = loan_second_hdue1['month60_State_countNr'].min() #var最小值
            dict_out['loan_second_hdue1_month60_State_count1r_min'] = loan_second_hdue1['month60_State_count1r'].min() #var最小值
            dict_out['loan_second_hdue1_month60_to_report_mean_mean'] = loan_second_hdue1['month60_to_report_mean'].mean() #var平均值
            dict_out['loan_second_hdue1_month60_Amount_num_max_mean'] = loan_second_hdue1['month60_Amount_num_max'].mean() #var平均值
            dict_out['loan_second_hdue1_month60_State_countN_sum'] = loan_second_hdue1['month60_State_countN'].sum() #var求和
            dict_out['loan_second_hdue1_repayMons_ratio_mean'] = loan_second_hdue1['repayMons_ratio'].mean() #var平均值
            dict_out['loan_second_hdue1_month60_Amount_num_mean_mean'] = loan_second_hdue1['month60_Amount_num_mean'].mean() #var平均值
            dict_out['loan_second_hdue1_month60_State_num_size_min'] = loan_second_hdue1['month60_State_num_size'].min() #var最小值
            dict_out['loan_second_hdue1_month60_to_report_max_min'] = loan_second_hdue1['month60_to_report_max'].min() #var最小值
            dict_out['loan_second_hdue1_month60_State_num_mean_mean'] = loan_second_hdue1['month60_State_num_mean'].mean() #var平均值
            dict_out['loan_second_hdue1_loanGrantOrg_nunique'] = loan_second_hdue1['loanGrantOrg'].nunique() #历史逾期贷款账户管理机构详细nunique
            dict_out['loan_second_hdue1_org_micro_loan_ratio'] = loan_second_hdue1[loan_second_hdue1['org']=='小额贷款公司'].shape[0]/len(loan_second_hdue1) #历史逾期贷款账户管理机构为小额贷款公司 占比
            dict_out['loan_second_hdue1_rt_unknow_count'] = loan_second_hdue1[loan_second_hdue1['repayType']=='--'].shape[0] #历史逾期贷款账户还款方式为-- 计数
            dict_out['loan_second_hdue1_org_trust_company_ratio'] = loan_second_hdue1[loan_second_hdue1['org']=='信托公司'].shape[0]/len(loan_second_hdue1) #历史逾期贷款账户管理机构为信托公司 占比
            dict_out['loan_second_hdue1_class_ncycle_count'] = loan_second_hdue1[loan_second_hdue1['class']=='非循环贷账户'].shape[0] #历史逾期贷款账户账户类别为非循环贷账户 计数
            dict_out['loan_second_hdue1_month60_to_report_max_max'] = loan_second_hdue1['month60_to_report_max'].max()#var最大值
            dict_out['loan_second_hdue1_month60_to_report_max_min'] = loan_second_hdue1['month60_to_report_max'].min() #var最小值比率
            dict_out['loan_second_hdue1_month60_to_report_max_range'] = (dict_out['loan_second_hdue1_month60_to_report_max_max']-dict_out['loan_second_hdue1_month60_to_report_max_min'])/dict_out['loan_second_hdue1_month60_to_report_max_max'] if dict_out['loan_second_hdue1_month60_to_report_max_max']>0 else np.nan #var区间率
            dict_out['loan_second_hdue1_month60_State_countNullr_sum'] = loan_second_hdue1['month60_State_countNullr'].sum() #var求和比率
            dict_out['loan_second_hdue1_repayMons_ratio_min'] = loan_second_hdue1['repayMons_ratio'].min() #var最小值比率
            dict_out['loan_second_hdue1_month60_State_countN_sum'] = loan_second_hdue1['month60_State_countN'].sum() #var求和比率
            dict_out['loan_second_hdue1_month60_State_countNr_sum'] = loan_second_hdue1['month60_State_countNr'].sum() #var求和比率
            dict_out['loan_second_hdue1_loanAmount_mean'] = loan_second_hdue1['loanAmount'].mean()  #var平均值比率
            dict_out['loan_second_hdue1_startDate_to_report_max'] = loan_second_hdue1['startDate_to_report'].max() #var最大值比率
            dict_out['loan_second_hdue1_month60_State_countCr_sum'] = loan_second_hdue1['month60_State_countCr'].sum() #var求和比率
            dict_out['loan_second_hdue1_repayTerms_sum'] = loan_second_hdue1['repayTerms'].sum() #var求和比率
            dict_out['loan_second_hdue1_month60_to_report_max_min'] = loan_second_hdue1['month60_to_report_max'].min() #var最小值比率
            dict_out['loan_second_hdue1_month60_to_report_mean_min'] = loan_second_hdue1['month60_to_report_mean'].min() #var最小值比率
            dict_out['loan_second_hdue1_month60_to_report_mean_max'] = loan_second_hdue1['month60_to_report_mean'].max() #var最大值比率
            dict_out['loan_second_hdue1_loanGrantOrg_nunique'] = loan_second_hdue1['loanGrantOrg'].nunique() #历史逾期贷款账户管理机构详细nunique
            dict_out['loan_second_hdue1R_loanGrantOrg_nunique'] = dict_out['loan_second_hdue1_loanGrantOrg_nunique']/dict_out['loan_second_loanGrantOrg_nunique'] if dict_out['loan_second_loanGrantOrg_nunique']>0 else np.nan #var最大值比率
            dict_out['loan_second_hdue1_org_micro_loan_ratio'] = loan_second_hdue1[loan_second_hdue1['org']=='小额贷款公司'].shape[0]/len(loan_second_hdue1) #历史逾期贷款账户管理机构为小额贷款公司 占比
            dict_out['loan_second_hdue1R_org_micro_loan_ratio'] = dict_out['loan_second_hdue1_org_micro_loan_ratio']/dict_out['loan_second_org_micro_loan_ratio'] if dict_out['loan_second_org_micro_loan_ratio']>0 else np.nan #var最大值比率
            dict_out['loan_second_hdue1R_month60_to_report_max_range'] = dict_out['loan_second_hdue1_month60_to_report_max_range']/dict_out['loan_second_month60_to_report_max_range'] if dict_out['loan_second_month60_to_report_max_range']>0 else np.nan #var最大值比率
            dict_out['loan_second_hdue1R_month60_State_countNullr_sum'] = dict_out['loan_second_hdue1_month60_State_countNullr_sum']/dict_out['loan_second_month60_State_countNullr_sum'] if dict_out['loan_second_month60_State_countNullr_sum']>0 else np.nan #var最大值比率
            dict_out['loan_second_hdue1_rt_unknow_count'] = loan_second_hdue1[loan_second_hdue1['repayType']=='--'].shape[0] #历史逾期贷款账户还款方式为-- 计数
            dict_out['loan_second_hdue1R_rt_unknow_count'] = dict_out['loan_second_hdue1_rt_unknow_count']/dict_out['loan_second_rt_unknow_count'] if dict_out['loan_second_rt_unknow_count']>0 else np.nan #var最大值比率
            dict_out['loan_second_hdue1R_repayMons_ratio_min'] = dict_out['loan_second_hdue1_repayMons_ratio_min']/dict_out['loan_second_repayMons_ratio_min'] if dict_out['loan_second_repayMons_ratio_min']>0 else np.nan #var最大值比率
            dict_out['loan_second_hdue1R_month60_State_countN_sum'] = dict_out['loan_second_hdue1_month60_State_countN_sum']/dict_out['loan_second_month60_State_countN_sum'] if dict_out['loan_second_month60_State_countN_sum']>0 else np.nan #var最大值比率
            dict_out['loan_second_hdue1R_month60_State_countNr_sum'] = dict_out['loan_second_hdue1_month60_State_countNr_sum']/dict_out['loan_second_month60_State_countNr_sum'] if dict_out['loan_second_month60_State_countNr_sum']>0 else np.nan #var最大值比率
            dict_out['loan_second_hdue1_org_trust_company_ratio'] = loan_second_hdue1[loan_second_hdue1['org']=='信托公司'].shape[0]/len(loan_second_hdue1) #历史逾期贷款账户管理机构为信托公司 占比
            dict_out['loan_second_hdue1R_org_trust_company_ratio'] = dict_out['loan_second_hdue1_org_trust_company_ratio']/dict_out['loan_second_org_trust_company_ratio'] if dict_out['loan_second_org_trust_company_ratio']>0 else np.nan #var最大值比率
            dict_out['loan_second_hdue1R_loanAmount_mean'] = dict_out['loan_second_hdue1_loanAmount_mean']/dict_out['loan_second_loanAmount_mean'] if dict_out['loan_second_loanAmount_mean']>0 else np.nan #var最大值比率
            dict_out['loan_second_hdue1R_startDate_to_report_max'] = dict_out['loan_second_hdue1_startDate_to_report_max']/dict_out['loan_second_startDate_to_report_max'] if dict_out['loan_second_startDate_to_report_max']>0 else np.nan #var最大值比率
            dict_out['loan_second_hdue1R_month60_State_countCr_sum'] = dict_out['loan_second_hdue1_month60_State_countCr_sum']/dict_out['loan_second_month60_State_countCr_sum'] if dict_out['loan_second_month60_State_countCr_sum']>0 else np.nan #var最大值比率
            dict_out['loan_second_hdue1R_repayTerms_sum'] = dict_out['loan_second_hdue1_repayTerms_sum']/dict_out['loan_second_repayTerms_sum'] if dict_out['loan_second_repayTerms_sum']>0 else np.nan #var最大值比率
            dict_out['loan_second_hdue1R_month60_to_report_max_min'] = dict_out['loan_second_hdue1_month60_to_report_max_min']/dict_out['loan_second_month60_to_report_max_min'] if dict_out['loan_second_month60_to_report_max_min']>0 else np.nan #var最大值比率
            dict_out['loan_second_hdue1_class_ncycle_count'] = loan_second_hdue1[loan_second_hdue1['class']=='非循环贷账户'].shape[0] #历史逾期贷款账户账户类别为非循环贷账户 计数
            dict_out['loan_second_hdue1R_class_ncycle_count'] = dict_out['loan_second_hdue1_class_ncycle_count']/dict_out['loan_second_class_ncycle_count'] if dict_out['loan_second_class_ncycle_count']>0 else np.nan #var最大值比率
            dict_out['loan_second_hdue1R_month60_to_report_mean_min'] = dict_out['loan_second_hdue1_month60_to_report_mean_min']/dict_out['loan_second_month60_to_report_mean_min'] if dict_out['loan_second_month60_to_report_mean_min']>0 else np.nan #var最大值比率
            dict_out['loan_second_hdue1R_month60_to_report_mean_max'] = dict_out['loan_second_hdue1_month60_to_report_mean_max']/dict_out['loan_second_month60_to_report_mean_max'] if dict_out['loan_second_month60_to_report_mean_max']>0 else np.nan #var最大值比率

        #历史严重逾期
        loan_second_hdue3 = loan_second[loan_second['due_class']==3]


        #当前逾期
        loan_second_cdue = loan_second[loan_second['accountStatus']=='逾期']




        #近06个月开立
        loan_second_m06 = loan_second[loan_second.startDate_to_report<6]
        if len(loan_second_m06)>0:

            if True:
                for var in numeric_vers:
                    dict_out['loan_second_m06_'+var+'_max'] = loan_second_m06[var].max() #近06个月开立 var最大值
                    dict_out['loan_second_m06_'+var+'_min'] = loan_second_m06[var].min() #近06个月开立 var最小值
                    dict_out['loan_second_m06_'+var+'_mean'] = loan_second_m06[var].mean() #近06个月开立 var平均值
                    dict_out['loan_second_m06_'+var+'_sum'] = loan_second_m06[var].sum() #近06个月开立 var求和
                    dict_out['loan_second_m06_'+var+'_range'] = (dict_out['loan_second_m06_'+var+'_max']-dict_out['loan_second_m06_'+var+'_min'])/dict_out['loan_second_m06_'+var+'_max'] if dict_out['loan_second_m06_'+var+'_max']>0 else np.nan #近06个月开立 var区间率



                dict_out['loan_second_m06_loanGrantOrg_giniimpurity'] = giniimpurity(loan_second_m06['loanGrantOrg']) #近06个月开立 贷款账户管理机构详细基尼不纯度
                dict_out['loan_second_m06_loanGrantOrg_nunique'] = loan_second_m06['loanGrantOrg'].nunique() #近06个月开立 贷款账户管理机构详细nunique

                dict_out['loan_second_m06_org_giniimpurity'] = giniimpurity(loan_second_m06['org']) #近06个月开立 贷款账户管理机构基尼不纯度
                dict_out['loan_second_m06_org_commercial_bank_count'] = loan_second_m06[loan_second_m06['org']=='商业银行'].shape[0] #近06个月开立 贷款账户管理机构为商业银行 计数
                dict_out['loan_second_m06_org_commercial_bank_ratio'] = loan_second_m06[loan_second_m06['org']=='商业银行'].shape[0]/len(loan_second_m06) #近06个月开立 贷款账户管理机构为商业银行 占比
                dict_out['loan_second_m06_org_consumer_finance_count'] = loan_second_m06[loan_second_m06['org']=='消费金融公司'].shape[0] #近06个月开立 贷款账户管理机构为消费金融公司 计数
                dict_out['loan_second_m06_org_consumer_finance_ratio'] = loan_second_m06[loan_second_m06['org']=='消费金融公司'].shape[0]/len(loan_second_m06) #近06个月开立 贷款账户管理机构为消费金融公司 占比
                dict_out['loan_second_m06_org_micro_loan_count'] = loan_second_m06[loan_second_m06['org']=='小额贷款公司'].shape[0] #近06个月开立 贷款账户管理机构为小额贷款公司 计数
                dict_out['loan_second_m06_org_micro_loan_ratio'] = loan_second_m06[loan_second_m06['org']=='小额贷款公司'].shape[0]/len(loan_second_m06) #近06个月开立 贷款账户管理机构为小额贷款公司 占比
                dict_out['loan_second_m06_org_other_count'] = loan_second_m06[loan_second_m06['org']=='其他机构'].shape[0] #近06个月开立 贷款账户管理机构为其他机构 计数
                dict_out['loan_second_m06_org_other_ratio'] = loan_second_m06[loan_second_m06['org']=='其他机构'].shape[0]/len(loan_second_m06) #近06个月开立 贷款账户管理机构为其他机构 占比
                dict_out['loan_second_m06_org_trust_company_count'] = loan_second_m06[loan_second_m06['org']=='信托公司'].shape[0] #近06个月开立 贷款账户管理机构为信托公司 计数
                dict_out['loan_second_m06_org_trust_company_ratio'] = loan_second_m06[loan_second_m06['org']=='信托公司'].shape[0]/len(loan_second_m06) #近06个月开立 贷款账户管理机构为信托公司 占比
                dict_out['loan_second_m06_org_car_finance_count'] = loan_second_m06[loan_second_m06['org']=='汽车金融公司'].shape[0] #近06个月开立 贷款账户管理机构为汽车金融公司 计数
                dict_out['loan_second_m06_org_car_finance_ratio'] = loan_second_m06[loan_second_m06['org']=='汽车金融公司'].shape[0]/len(loan_second_m06) #近06个月开立 贷款账户管理机构为汽车金融公司 占比
                dict_out['loan_second_m06_org_lease_finance_count'] = loan_second_m06[loan_second_m06['org']=='融资租赁公司'].shape[0] #近06个月开立 贷款账户管理机构为融资租赁公司 计数
                dict_out['loan_second_m06_org_lease_finance_ratio'] = loan_second_m06[loan_second_m06['org']=='融资租赁公司'].shape[0]/len(loan_second_m06) #近06个月开立 贷款账户管理机构为融资租赁公司 占比
                dict_out['loan_second_m06_org_myself_count'] = loan_second_m06[loan_second_m06['org']==dict_in['cc_rh_report'].queryOperator.values[0]].shape[0] #近06个月开立 贷款账户管理机构为本机构 计数
                dict_out['loan_second_m06_org_myself_ratio'] = loan_second_m06[loan_second_m06['org']==dict_in['cc_rh_report'].queryOperator.values[0]].shape[0]/len(loan_second_m06) #近06个月开立 贷款账户管理机构为本机构 占比
                dict_out['loan_second_m06_org_village_banks_count'] = loan_second_m06[loan_second_m06['org']=='村镇银行'].shape[0] #近06个月开立 贷款账户管理机构为村镇银行 计数
                dict_out['loan_second_m06_org_village_banks_ratio'] = loan_second_m06[loan_second_m06['org']=='村镇银行'].shape[0]/len(loan_second_m06) #近06个月开立 贷款账户管理机构为村镇银行 占比
                dict_out['loan_second_m06_org_finance_company_count'] = loan_second_m06[loan_second_m06['org']=='财务公司'].shape[0] #近06个月开立 贷款账户管理机构为财务公司 计数
                dict_out['loan_second_m06_org_finance_company_ratio'] = loan_second_m06[loan_second_m06['org']=='财务公司'].shape[0]/len(loan_second_m06) #近06个月开立 贷款账户管理机构为财务公司 占比
                dict_out['loan_second_m06_org_foreign_banks_count'] = loan_second_m06[loan_second_m06['org']=='外资银行'].shape[0] #近06个月开立 贷款账户管理机构为外资银行 计数
                dict_out['loan_second_m06_org_foreign_banks_ratio'] = loan_second_m06[loan_second_m06['org']=='外资银行'].shape[0]/len(loan_second_m06) #近06个月开立 贷款账户管理机构为外资银行 占比
                dict_out['loan_second_m06_org_provident_fund_count'] = loan_second_m06[loan_second_m06['org']=='公积金管理中心'].shape[0] #近06个月开立 贷款账户管理机构为公积金管理中心 计数
                dict_out['loan_second_m06_org_provident_fund_ratio'] = loan_second_m06[loan_second_m06['org']=='公积金管理中心'].shape[0]/len(loan_second_m06) #近06个月开立 贷款账户管理机构为公积金管理中心 占比
                dict_out['loan_second_m06_org_securities_firm_count'] = loan_second_m06[loan_second_m06['org']=='证券公司'].shape[0] #近06个月开立 贷款账户管理机构为证券公司 计数
                dict_out['loan_second_m06_org_securities_firm_ratio'] = loan_second_m06[loan_second_m06['org']=='证券公司'].shape[0]/len(loan_second_m06) #近06个月开立 贷款账户管理机构为证券公司 占比

                dict_out['loan_second_m06_class_giniimpurity'] = giniimpurity(loan_second_m06['class']) #近06个月开立 贷款账户账户类别基尼不纯度
                dict_out['loan_second_m06_class_ncycle_count'] = loan_second_m06[loan_second_m06['class']=='非循环贷账户'].shape[0] #近06个月开立 贷款账户账户类别为非循环贷账户 计数
                dict_out['loan_second_m06_class_ncycle_ratio'] = loan_second_m06[loan_second_m06['class']=='非循环贷账户'].shape[0]/len(loan_second_m06) #近06个月开立 贷款账户账户类别为非循环贷账户 占比
                dict_out['loan_second_m06_class_cycle_sub_count'] = loan_second_m06[loan_second_m06['class']=='循环额度下分账户'].shape[0] #近06个月开立 贷款账户账户类别为循环额度下分账户 计数
                dict_out['loan_second_m06_class_cycle_sub_ratio'] = loan_second_m06[loan_second_m06['class']=='循环额度下分账户'].shape[0]/len(loan_second_m06) #近06个月开立 贷款账户账户类别为循环额度下分账户 占比
                dict_out['loan_second_m06_class_cycle_count'] = loan_second_m06[loan_second_m06['class']=='循环贷账户'].shape[0] #近06个月开立 贷款账户账户类别为循环贷账户 计数
                dict_out['loan_second_m06_class_cycle_ratio'] = loan_second_m06[loan_second_m06['class']=='循环贷账户'].shape[0]/len(loan_second_m06) #近06个月开立 贷款账户账户类别为循环贷账户 占比

                dict_out['loan_second_m06_classify5_giniimpurity'] = giniimpurity(loan_second_m06['classify5']) #近06个月开立 贷款账户五级分类基尼不纯度
                dict_out['loan_second_m06_c5_unknow_count'] = loan_second_m06[loan_second_m06['classify5']==''].shape[0] #近06个月开立 贷款账户五级分类为'' 计数
                dict_out['loan_second_m06_c5_unknow_ratio'] = loan_second_m06[loan_second_m06['classify5']==''].shape[0]/len(loan_second_m06) #近06个月开立 贷款账户五级分类为'' 占比
                dict_out['loan_second_m06_c5_normal_count'] = loan_second_m06[loan_second_m06['classify5']=='正常'].shape[0] #近06个月开立 贷款账户五级分类为正常 计数
                dict_out['loan_second_m06_c5_normal_ratio'] = loan_second_m06[loan_second_m06['classify5']=='正常'].shape[0]/len(loan_second_m06) #近06个月开立 贷款账户五级分类为正常 占比
                dict_out['loan_second_m06_c5_loss_count'] = loan_second_m06[loan_second_m06['classify5']=='损失'].shape[0] #近06个月开立 贷款账户五级分类为损失 计数
                dict_out['loan_second_m06_c5_loss_ratio'] = loan_second_m06[loan_second_m06['classify5']=='损失'].shape[0]/len(loan_second_m06) #近06个月开立 贷款账户五级分类为损失 占比
                dict_out['loan_second_m06_c5_attention_count'] = loan_second_m06[loan_second_m06['classify5']=='关注'].shape[0] #近06个月开立 贷款账户五级分类为关注 计数
                dict_out['loan_second_m06_c5_attention_ratio'] = loan_second_m06[loan_second_m06['classify5']=='关注'].shape[0]/len(loan_second_m06) #近06个月开立 贷款账户五级分类为关注 占比
                dict_out['loan_second_m06_c5_suspicious_count'] = loan_second_m06[loan_second_m06['classify5']=='可疑'].shape[0] #近06个月开立 贷款账户五级分类为可疑 计数
                dict_out['loan_second_m06_c5_suspicious_ratio'] = loan_second_m06[loan_second_m06['classify5']=='可疑'].shape[0]/len(loan_second_m06) #近06个月开立 贷款账户五级分类为可疑 占比
                dict_out['loan_second_m06_c5_secondary_count'] = loan_second_m06[loan_second_m06['classify5']=='次级'].shape[0] #近06个月开立 贷款账户五级分类为次级 计数
                dict_out['loan_second_m06_c5_secondary_ratio'] = loan_second_m06[loan_second_m06['classify5']=='次级'].shape[0]/len(loan_second_m06) #近06个月开立 贷款账户五级分类为次级 占比
                dict_out['loan_second_m06_c5_noclass_count'] = loan_second_m06[loan_second_m06['classify5']=='未分类'].shape[0] #近06个月开立 贷款账户五级分类为未分类 计数
                dict_out['loan_second_m06_c5_noclass_ratio'] = loan_second_m06[loan_second_m06['classify5']=='未分类'].shape[0]/len(loan_second_m06) #近06个月开立 贷款账户五级分类为未分类 占比

                dict_out['loan_second_m06_accountStatus_giniimpurity'] = giniimpurity(loan_second_m06['accountStatus']) #近06个月开立 贷款账户账户状态基尼不纯度
                dict_out['loan_second_m06_as_settle_count'] = loan_second_m06[loan_second_m06['accountStatus']=='结清'].shape[0] #近06个月开立 贷款账户账户状态为结清 计数
                dict_out['loan_second_m06_as_settle_ratio'] = loan_second_m06[loan_second_m06['accountStatus']=='结清'].shape[0]/len(loan_second_m06) #近06个月开立 贷款账户账户状态为结清 占比
                dict_out['loan_second_m06_as_normal_count'] = loan_second_m06[loan_second_m06['accountStatus']=='正常'].shape[0] #近06个月开立 贷款账户账户状态为正常 计数
                dict_out['loan_second_m06_as_normal_ratio'] = loan_second_m06[loan_second_m06['accountStatus']=='正常'].shape[0]/len(loan_second_m06) #近06个月开立 贷款账户账户状态为正常 占比
                dict_out['loan_second_m06_as_overdue_count'] = loan_second_m06[loan_second_m06['accountStatus']=='逾期'].shape[0] #近06个月开立 贷款账户账户状态为逾期 计数
                dict_out['loan_second_m06_as_overdue_ratio'] = loan_second_m06[loan_second_m06['accountStatus']=='逾期'].shape[0]/len(loan_second_m06) #近06个月开立 贷款账户账户状态为逾期 占比
                dict_out['loan_second_m06_as_bad_debts_count'] = loan_second_m06[loan_second_m06['accountStatus']=='呆账'].shape[0] #近06个月开立 贷款账户账户状态为呆账 计数
                dict_out['loan_second_m06_as_bad_debts_ratio'] = loan_second_m06[loan_second_m06['accountStatus']=='呆账'].shape[0]/len(loan_second_m06) #近06个月开立 贷款账户账户状态为呆账 占比
                dict_out['loan_second_m06_as_unknow_count'] = loan_second_m06[loan_second_m06['accountStatus']==''].shape[0] #近06个月开立 贷款账户账户状态为'' 计数
                dict_out['loan_second_m06_as_unknow_ratio'] = loan_second_m06[loan_second_m06['accountStatus']==''].shape[0]/len(loan_second_m06) #近06个月开立 贷款账户账户状态为'' 占比
                dict_out['loan_second_m06_as_roll_out_count'] = loan_second_m06[loan_second_m06['accountStatus']=='转出'].shape[0] #近06个月开立 贷款账户账户状态为转出 计数
                dict_out['loan_second_m06_as_roll_out_ratio'] = loan_second_m06[loan_second_m06['accountStatus']=='转出'].shape[0]/len(loan_second_m06) #近06个月开立 贷款账户账户状态为转出 占比

                dict_out['loan_second_m06_repayType_giniimpurity'] = giniimpurity(loan_second_m06['repayType']) #近06个月开立 贷款账户还款方式基尼不纯度
                dict_out['loan_second_m06_rt_unknow_count'] = loan_second_m06[loan_second_m06['repayType']=='--'].shape[0] #近06个月开立 贷款账户还款方式为-- 计数
                dict_out['loan_second_m06_rt_unknow_ratio'] = loan_second_m06[loan_second_m06['repayType']=='--'].shape[0]/len(loan_second_m06) #近06个月开立 贷款账户还款方式为-- 占比
                dict_out['loan_second_m06_rt_equality_count'] = loan_second_m06[loan_second_m06['repayType']=='分期等额本息'].shape[0] #近06个月开立 贷款账户还款方式为分期等额本息 计数
                dict_out['loan_second_m06_rt_equality_ratio'] = loan_second_m06[loan_second_m06['repayType']=='分期等额本息'].shape[0]/len(loan_second_m06) #近06个月开立 贷款账户还款方式为分期等额本息 占比
                dict_out['loan_second_m06_rt_onschedule_count'] = loan_second_m06[loan_second_m06['repayType']=='按期计算还本付息'].shape[0] #近06个月开立 贷款账户还款方式为按期计算还本付息 计数
                dict_out['loan_second_m06_rt_onschedule_ratio'] = loan_second_m06[loan_second_m06['repayType']=='按期计算还本付息'].shape[0]/len(loan_second_m06) #近06个月开立 贷款账户还款方式为按期计算还本付息 占比
                dict_out['loan_second_m06_rt_not_distinguish_count'] = loan_second_m06[loan_second_m06['repayType']=='不区分还款方式'].shape[0] #近06个月开立 贷款账户还款方式为不区分还款方式 计数
                dict_out['loan_second_m06_rt_not_distinguish_ratio'] = loan_second_m06[loan_second_m06['repayType']=='不区分还款方式'].shape[0]/len(loan_second_m06) #近06个月开立 贷款账户还款方式为不区分还款方式 占比
                dict_out['loan_second_m06_rt_circulation_count'] = loan_second_m06[loan_second_m06['repayType']=='循环贷款下其他还款方式'].shape[0] #近06个月开立 贷款账户还款方式为循环贷款下其他还款方式 计数
                dict_out['loan_second_m06_rt_circulation_ratio'] = loan_second_m06[loan_second_m06['repayType']=='循环贷款下其他还款方式'].shape[0]/len(loan_second_m06) #近06个月开立 贷款账户还款方式为循环贷款下其他还款方式 占比
                dict_out['loan_second_m06_rt_once_count'] = loan_second_m06[loan_second_m06['repayType']=='到期一次还本付息'].shape[0] #近06个月开立 贷款账户还款方式为到期一次还本付息 计数
                dict_out['loan_second_m06_rt_once_ratio'] = loan_second_m06[loan_second_m06['repayType']=='到期一次还本付息'].shape[0]/len(loan_second_m06) #近06个月开立 贷款账户还款方式为到期一次还本付息 占比

                dict_out['loan_second_m06_repayFrequency_giniimpurity'] = giniimpurity(loan_second_m06['repayFrequency']) #近06个月开立 贷款账户还款频率基尼不纯度
                dict_out['loan_second_m06_rf_month_count'] = loan_second_m06[loan_second_m06['repayFrequency']=='月'].shape[0] #近06个月开立 贷款账户还款频率为月 计数
                dict_out['loan_second_m06_rf_month_ratio'] = loan_second_m06[loan_second_m06['repayFrequency']=='月'].shape[0]/len(loan_second_m06) #近06个月开立 贷款账户还款频率为月 占比
                dict_out['loan_second_m06_rf_once_count'] = loan_second_m06[loan_second_m06['repayFrequency']=='一次性'].shape[0] #近06个月开立 贷款账户还款频率为一次性 计数
                dict_out['loan_second_m06_rf_once_ratio'] = loan_second_m06[loan_second_m06['repayFrequency']=='一次性'].shape[0]/len(loan_second_m06) #近06个月开立 贷款账户还款频率为一次性 占比
                dict_out['loan_second_m06_rf_other_count'] = loan_second_m06[loan_second_m06['repayFrequency']=='其他'].shape[0] #近06个月开立 贷款账户还款频率为其他 计数
                dict_out['loan_second_m06_rf_other_ratio'] = loan_second_m06[loan_second_m06['repayFrequency']=='其他'].shape[0]/len(loan_second_m06) #近06个月开立 贷款账户还款频率为其他 占比
                dict_out['loan_second_m06_rf_irregular_count'] = loan_second_m06[loan_second_m06['repayFrequency']=='不定期'].shape[0] #近06个月开立 贷款账户还款频率为不定期 计数
                dict_out['loan_second_m06_rf_irregular_ratio'] = loan_second_m06[loan_second_m06['repayFrequency']=='不定期'].shape[0]/len(loan_second_m06) #近06个月开立 贷款账户还款频率为不定期 占比
                dict_out['loan_second_m06_rf_day_count'] = loan_second_m06[loan_second_m06['repayFrequency']=='日'].shape[0] #近06个月开立 贷款账户还款频率为日 计数
                dict_out['loan_second_m06_rf_day_ratio'] = loan_second_m06[loan_second_m06['repayFrequency']=='日'].shape[0]/len(loan_second_m06) #近06个月开立 贷款账户还款频率为日 占比
                dict_out['loan_second_m06_rf_year_count'] = loan_second_m06[loan_second_m06['repayFrequency']=='年'].shape[0] #近06个月开立 贷款账户还款频率为年 计数
                dict_out['loan_second_m06_rf_year_ratio'] = loan_second_m06[loan_second_m06['repayFrequency']=='年'].shape[0]/len(loan_second_m06) #近06个月开立 贷款账户还款频率为年 占比
                dict_out['loan_second_m06_rf_season_count'] = loan_second_m06[loan_second_m06['repayFrequency']=='季'].shape[0] #近06个月开立 贷款账户还款频率为季 计数
                dict_out['loan_second_m06_rf_season_ratio'] = loan_second_m06[loan_second_m06['repayFrequency']=='季'].shape[0]/len(loan_second_m06) #近06个月开立 贷款账户还款频率为季 占比
                dict_out['loan_second_m06_rf_week_count'] = loan_second_m06[loan_second_m06['repayFrequency']=='周'].shape[0] #近06个月开立 贷款账户还款频率为周 计数
                dict_out['loan_second_m06_rf_week_ratio'] = loan_second_m06[loan_second_m06['repayFrequency']=='周'].shape[0]/len(loan_second_m06) #近06个月开立 贷款账户还款频率为周 占比
                dict_out['loan_second_m06_rf_halfyear_count'] = loan_second_m06[loan_second_m06['repayFrequency']=='半年'].shape[0] #近06个月开立 贷款账户还款频率为半年 计数
                dict_out['loan_second_m06_rf_halfyear_ratio'] = loan_second_m06[loan_second_m06['repayFrequency']=='半年'].shape[0]/len(loan_second_m06) #近06个月开立 贷款账户还款频率为半年 占比

                dict_out['loan_second_m06_guaranteeForm_giniimpurity'] = giniimpurity(loan_second_m06['guaranteeForm']) #近06个月开立 贷款账户担保方式基尼不纯度
                dict_out['loan_second_m06_gf_crdit_count'] = loan_second_m06[loan_second_m06['guaranteeForm']=='信用/免担保'].shape[0] #近06个月开立 贷款账户担保方式为其信用/免担保 计数
                dict_out['loan_second_m06_gf_crdit_ratio'] = loan_second_m06[loan_second_m06['guaranteeForm']=='信用/免担保'].shape[0]/len(loan_second_m06) #近06个月开立 贷款账户担保方式为信用/免担保 占比
                dict_out['loan_second_m06_gf_other_count'] = loan_second_m06[loan_second_m06['guaranteeForm']=='其他'].shape[0] #近06个月开立 贷款账户担保方式为其他 计数
                dict_out['loan_second_m06_gf_other_ratio'] = loan_second_m06[loan_second_m06['guaranteeForm']=='其他'].shape[0]/len(loan_second_m06) #近06个月开立 贷款账户担保方式为其他 占比
                dict_out['loan_second_m06_gf_combine_nowarranty_count'] = loan_second_m06[loan_second_m06['guaranteeForm']=='组合（不含保证）'].shape[0] #近06个月开立 贷款账户担保方式为组合（不含保证） 计数
                dict_out['loan_second_m06_gf_combine_nowarranty_ratio'] = loan_second_m06[loan_second_m06['guaranteeForm']=='组合（不含保证）'].shape[0]/len(loan_second_m06) #近06个月开立 贷款账户担保方式为组合（不含保证） 占比
                dict_out['loan_second_m06_gf_combine_warranty_count'] = loan_second_m06[loan_second_m06['guaranteeForm']=='组合(含保证)'].shape[0] #近06个月开立 贷款账户担保方式为组合(含保证) 计数
                dict_out['loan_second_m06_gf_combine_warranty_ratio'] = loan_second_m06[loan_second_m06['guaranteeForm']=='组合(含保证)'].shape[0]/len(loan_second_m06) #近06个月开立 贷款账户担保方式为组合(含保证) 占比
                dict_out['loan_second_m06_gf_mortgage_count'] = loan_second_m06[loan_second_m06['guaranteeForm']=='抵押'].shape[0] #近06个月开立 贷款账户担保方式为抵押 计数
                dict_out['loan_second_m06_gf_mortgage_ratio'] = loan_second_m06[loan_second_m06['guaranteeForm']=='抵押'].shape[0]/len(loan_second_m06) #近06个月开立 贷款账户担保方式为抵押 占比
                dict_out['loan_second_m06_gf_warranty_count'] = loan_second_m06[loan_second_m06['guaranteeForm']=='保证'].shape[0] #近06个月开立 贷款账户担保方式为保证计数
                dict_out['loan_second_m06_gf_warranty_ratio'] = loan_second_m06[loan_second_m06['guaranteeForm']=='保证'].shape[0]/len(loan_second_m06) #近06个月开立 贷款账户担保方式为保证 占比
                dict_out['loan_second_m06_gf_pledge_count'] = loan_second_m06[loan_second_m06['guaranteeForm']=='质押'].shape[0] #近06个月开立 贷款账户担保方式为质押 计数
                dict_out['loan_second_m06_gf_pledge_ratio'] = loan_second_m06[loan_second_m06['guaranteeForm']=='质押'].shape[0]/len(loan_second_m06) #近06个月开立 贷款账户担保方式为质押 占比
                dict_out['loan_second_m06_gf_farm_group_count'] = loan_second_m06[loan_second_m06['guaranteeForm']=='农户联保'].shape[0] #近06个月开立 贷款账户担保方式为农户联保 计数
                dict_out['loan_second_m06_gf_farm_group_ratio'] = loan_second_m06[loan_second_m06['guaranteeForm']=='农户联保'].shape[0]/len(loan_second_m06) #近06个月开立 贷款账户担保方式为农户联保 占比

                dict_out['loan_second_m06_businessType_giniimpurity'] = giniimpurity(loan_second_m06['businessType']) #近06个月开立 贷款账户业务种类基尼不纯度
                dict_out['loan_second_m06_bt_other_person_count'] = loan_second_m06[loan_second_m06['businessType']=='其他个人消费贷款'].shape[0] #近06个月开立 贷款账户业务种类为其他个人消费贷款 计数
                dict_out['loan_second_m06_bt_other_person_ratio'] = loan_second_m06[loan_second_m06['businessType']=='其他个人消费贷款'].shape[0]/len(loan_second_m06) #近06个月开立 贷款账户业务种类为其他个人消费贷款 占比
                dict_out['loan_second_m06_bt_other_loan_count'] = loan_second_m06[loan_second_m06['businessType']=='其他贷款'].shape[0] #近06个月开立 贷款账户业务种类为其他贷款 计数
                dict_out['loan_second_m06_bt_other_loan_ratio'] = loan_second_m06[loan_second_m06['businessType']=='其他贷款'].shape[0]/len(loan_second_m06) #近06个月开立 贷款账户业务种类为其他贷款 占比
                dict_out['loan_second_m06_bt_person_business_count'] = loan_second_m06[loan_second_m06['businessType']=='个人经营性贷款'].shape[0] #近06个月开立 贷款账户业务种类为个人经营性贷款 计数
                dict_out['loan_second_m06_bt_person_business_ratio'] = loan_second_m06[loan_second_m06['businessType']=='个人经营性贷款'].shape[0]/len(loan_second_m06) #近06个月开立 贷款账户业务种类为个人经营性贷款 占比
                dict_out['loan_second_m06_bt_farm_loan_count'] = loan_second_m06[loan_second_m06['businessType']=='农户贷款'].shape[0] #近06个月开立 贷款账户业务种类为农户贷款 计数
                dict_out['loan_second_m06_bt_farm_loan_ratio'] = loan_second_m06[loan_second_m06['businessType']=='农户贷款'].shape[0]/len(loan_second_m06) #近06个月开立 贷款账户业务种类为农户贷款 占比
                dict_out['loan_second_m06_bt_person_car_count'] = loan_second_m06[loan_second_m06['businessType']=='个人汽车消费贷款'].shape[0] #近06个月开立 贷款账户业务种类为个人汽车消费贷款 计数
                dict_out['loan_second_m06_bt_person_car_ratio'] = loan_second_m06[loan_second_m06['businessType']=='个人汽车消费贷款'].shape[0]/len(loan_second_m06) #近06个月开立 贷款账户业务种类为个人汽车消费贷款 占比
                dict_out['loan_second_m06_bt_person_study_count'] = loan_second_m06[loan_second_m06['businessType']=='个人助学贷款'].shape[0] #近06个月开立 贷款账户业务种类为个人助学贷款 计数
                dict_out['loan_second_m06_bt_person_study_ratio'] = loan_second_m06[loan_second_m06['businessType']=='个人助学贷款'].shape[0]/len(loan_second_m06) #近06个月开立 贷款账户业务种类为个人助学贷款 占比
                dict_out['loan_second_m06_bt_house_commercial_count'] = loan_second_m06[loan_second_m06['businessType']=='个人住房商业贷款'].shape[0] #近06个月开立 贷款账户业务种类为个人住房商业贷款 计数
                dict_out['loan_second_m06_bt_house_commercial_ratio'] = loan_second_m06[loan_second_m06['businessType']=='个人住房商业贷款'].shape[0]/len(loan_second_m06) #近06个月开立 贷款账户业务种类为个人住房商业贷款 占比
                dict_out['loan_second_m06_bt_finance_lease_count'] = loan_second_m06[loan_second_m06['businessType']=='融资租赁业务'].shape[0] #近06个月开立 贷款账户业务种类为融资租赁业务 计数
                dict_out['loan_second_m06_bt_finance_lease_ratio'] = loan_second_m06[loan_second_m06['businessType']=='融资租赁业务'].shape[0]/len(loan_second_m06) #近06个月开立 贷款账户业务种类为融资租赁业务 占比
                dict_out['loan_second_m06_bt_house_fund_count'] = loan_second_m06[loan_second_m06['businessType']=='个人住房公积金贷款'].shape[0] #近06个月开立 贷款账户业务种类为个人住房公积金贷款 计数
                dict_out['loan_second_m06_bt_house_fund_ratio'] = loan_second_m06[loan_second_m06['businessType']=='个人住房公积金贷款'].shape[0]/len(loan_second_m06) #近06个月开立 贷款账户业务种类为个人住房公积金贷款 占比
                dict_out['loan_second_m06_bt_person_house_count'] = loan_second_m06[loan_second_m06['businessType']=='个人商用房（含商住两用）贷款'].shape[0] #近06个月开立 贷款账户业务种类为个人商用房（含商住两用）贷款 计数
                dict_out['loan_second_m06_bt_person_house_ratio'] = loan_second_m06[loan_second_m06['businessType']=='个人商用房（含商住两用）贷款'].shape[0]/len(loan_second_m06) #近06个月开立 贷款账户业务种类为个人商用房（含商住两用）贷款 占比
                dict_out['loan_second_m06_bt_stock_pledge_count'] = loan_second_m06[loan_second_m06['businessType']=='股票质押式回购交易'].shape[0] #近06个月开立 贷款账户业务种类为股票质押式回购交易计数
                dict_out['loan_second_m06_bt_stock_pledge_ratio'] = loan_second_m06[loan_second_m06['businessType']=='股票质押式回购交易'].shape[0]/len(loan_second_m06) #近06个月开立 贷款账户业务种类为股票质押式回购交易 占比



            #近06个月开立 现行
            loan_second_m06_now = loan_second_m06[loan_second_m06.is_now==1]
            if len(loan_second_m06_now)>0:
                dict_out['loan_second_m06_now_repayedAmount_mean'] = loan_second_m06_now['repayedAmount'].mean() #var平均值
                dict_out['loan_second_m06_now_balance_ratio_mean'] = loan_second_m06_now['balance_ratio'].mean() #var平均值
                dict_out['loan_second_m06_now_repayedAmount_max'] = loan_second_m06_now['repayedAmount'].max()#var最大值
                dict_out['loan_second_m06_now_repayTerms_max'] = loan_second_m06_now['repayTerms'].max()#var最大值
                dict_out['loan_second_m06_now_repayTerms_min'] = loan_second_m06_now['repayTerms'].min() #var最小值
                dict_out['loan_second_m06_now_repayTerms_range'] = (dict_out['loan_second_m06_now_repayTerms_max']-dict_out['loan_second_m06_now_repayTerms_min'])/dict_out['loan_second_m06_now_repayTerms_max'] if dict_out['loan_second_m06_now_repayTerms_max']>0 else np.nan #var区间率
                dict_out['loan_second_m06_now_month60_State_count1r_mean'] = loan_second_m06_now['month60_State_count1r'].mean() #var平均值
                dict_out['loan_second_m06_now_repayedAmount_max'] = loan_second_m06_now['repayedAmount'].max()#var最大值
                dict_out['loan_second_m06_now_repayedAmount_min'] = loan_second_m06_now['repayedAmount'].min() #var最小值
                dict_out['loan_second_m06_now_repayedAmount_range'] = (dict_out['loan_second_m06_now_repayedAmount_max']-dict_out['loan_second_m06_now_repayedAmount_min'])/dict_out['loan_second_m06_now_repayedAmount_max'] if dict_out['loan_second_m06_now_repayedAmount_max']>0 else np.nan #var区间率
                dict_out['loan_second_m06_now_month60_State_countNullr_mean'] = loan_second_m06_now['month60_State_countNullr'].mean() #var平均值
                dict_out['loan_second_m06_now_leftRepayTerms_mean'] = loan_second_m06_now['leftRepayTerms'].mean() #var平均值
                dict_out['loan_second_m06_now_month60_Amount_num_meanbig0_sum'] = loan_second_m06_now['month60_Amount_num_meanbig0'].sum() #var求和
                dict_out['loan_second_m06_now_repayedAmount_sum'] = loan_second_m06_now['repayedAmount'].sum() #var求和
                dict_out['loan_second_m06_now_startDate_to_report_mean'] = loan_second_m06_now['startDate_to_report'].mean() #var平均值
                dict_out['loan_second_m06_now_businessType_giniimpurity'] = giniimpurity(loan_second_m06_now['businessType']) #近06个月开立 现行贷款账户业务种类基尼不纯度
                dict_out['loan_second_m06_now_org_trust_company_ratio'] = loan_second_m06_now[loan_second_m06_now['org']=='信托公司'].shape[0]/len(loan_second_m06_now) #近06个月开立 现行贷款账户管理机构为信托公司 占比
                dict_out['loan_second_m06_now_c5_normal_ratio'] = loan_second_m06_now[loan_second_m06_now['classify5']=='正常'].shape[0]/len(loan_second_m06_now) #近06个月开立 现行贷款账户五级分类为正常 占比
                dict_out['loan_second_m06_now_rt_onschedule_ratio'] = loan_second_m06_now[loan_second_m06_now['repayType']=='按期计算还本付息'].shape[0]/len(loan_second_m06_now) #近06个月开立 现行贷款账户还款方式为按期计算还本付息 占比
                dict_out['loan_second_m06_now_month60_to_report_min_sum'] = loan_second_m06_now['month60_to_report_min'].sum() #var求和
                dict_out['loan_second_m06_now_org_other_ratio'] = loan_second_m06_now[loan_second_m06_now['org']=='其他机构'].shape[0]/len(loan_second_m06_now) #近06个月开立 现行贷款账户管理机构为其他机构 占比
                dict_out['loan_second_m06_now_month60_State_num_sum_sum'] = loan_second_m06_now['month60_State_num_sum'].sum() #var求和
                dict_out['loan_second_m06_now_repayAmt_min'] = loan_second_m06_now['repayAmt'].min() #var最小值
                dict_out['loan_second_m06_now_month60_to_report_max_mean'] = loan_second_m06_now['month60_to_report_max'].mean() #var平均值
                dict_out['loan_second_m06_now_month60_to_report_mean_mean'] = loan_second_m06_now['month60_to_report_mean'].mean() #var平均值
                dict_out['loan_second_m06_now_month60_State_countNr_mean'] = loan_second_m06_now['month60_State_countNr'].mean() #var平均值
                dict_out['loan_second_m06_now_is_now_sum'] = loan_second_m06_now['is_now'].sum() #var求和
                dict_out['loan_second_m06_now_rf_once_ratio'] = loan_second_m06_now[loan_second_m06_now['repayFrequency']=='一次性'].shape[0]/len(loan_second_m06_now) #近06个月开立 现行贷款账户还款频率为一次性 占比
                dict_out['loan_second_m06_now_month60_State_countNull_mean'] = loan_second_m06_now['month60_State_countNull'].mean() #var平均值
                dict_out['loan_second_m06_now_gf_other_ratio'] = loan_second_m06_now[loan_second_m06_now['guaranteeForm']=='其他'].shape[0]/len(loan_second_m06_now) #近06个月开立 现行贷款账户担保方式为其他 占比
                dict_out['loan_second_m06_now_org_giniimpurity'] = giniimpurity(loan_second_m06_now['org']) #近06个月开立 现行贷款账户管理机构基尼不纯度
                dict_out['loan_second_m06_now_due_class_mean'] = loan_second_m06_now['due_class'].mean() #var平均值
                dict_out['loan_second_m06_now_byDate_to_report_mean'] = loan_second_m06_now['byDate_to_report'].mean() #var平均值
                dict_out['loan_second_m06_now_repayMons_ratio_sum'] = loan_second_m06_now['repayMons_ratio'].sum() #var求和
                dict_out['loan_second_m06_now_month60_Amount_num_mean_sum'] = loan_second_m06_now['month60_Amount_num_mean'].sum() #var求和
                dict_out['loan_second_m06_now_month60_State_countNr_max'] = loan_second_m06_now['month60_State_countNr'].max()#var最大值
                dict_out['loan_second_m06_now_balance_ratio_max'] = loan_second_m06_now['balance_ratio'].max()#var最大值
                dict_out['loan_second_m06_now_balance_ratio_min'] = loan_second_m06_now['balance_ratio'].min() #var最小值
                dict_out['loan_second_m06_now_balance_ratio_range'] = (dict_out['loan_second_m06_now_balance_ratio_max']-dict_out['loan_second_m06_now_balance_ratio_min'])/dict_out['loan_second_m06_now_balance_ratio_max'] if dict_out['loan_second_m06_now_balance_ratio_max']>0 else np.nan #var区间率
                dict_out['loan_second_m06_now_planRepayAmount_min'] = loan_second_m06_now['planRepayAmount'].min() #var最小值
                dict_out['loan_second_m06_now_bt_other_loan_ratio'] = loan_second_m06_now[loan_second_m06_now['businessType']=='其他贷款'].shape[0]/len(loan_second_m06_now) #近06个月开立 现行贷款账户业务种类为其他贷款 占比
                dict_out['loan_second_m06_now_repayMons_max'] = loan_second_m06_now['repayMons'].max()#var最大值
                dict_out['loan_second_m06_now_repayMons_min'] = loan_second_m06_now['repayMons'].min() #var最小值
                dict_out['loan_second_m06_now_repayMons_range'] = (dict_out['loan_second_m06_now_repayMons_max']-dict_out['loan_second_m06_now_repayMons_min'])/dict_out['loan_second_m06_now_repayMons_max'] if dict_out['loan_second_m06_now_repayMons_max']>0 else np.nan #var区间率
                dict_out['loan_second_m06_now_planRepayAmount_mean'] = loan_second_m06_now['planRepayAmount'].mean() #var平均值
                dict_out['loan_second_m06_now_due_class_max'] = loan_second_m06_now['due_class'].max()#var最大值
                dict_out['loan_second_m06_now_leftRepayTerms_max'] = loan_second_m06_now['leftRepayTerms'].max()#var最大值
                dict_out['loan_second_m06_now_leftRepayTerms_min'] = loan_second_m06_now['leftRepayTerms'].min() #var最小值
                dict_out['loan_second_m06_now_leftRepayTerms_range'] = (dict_out['loan_second_m06_now_leftRepayTerms_max']-dict_out['loan_second_m06_now_leftRepayTerms_min'])/dict_out['loan_second_m06_now_leftRepayTerms_max'] if dict_out['loan_second_m06_now_leftRepayTerms_max']>0 else np.nan #var区间率
                dict_out['loan_second_m06_now_month60_Amount_num_sum_max'] = loan_second_m06_now['month60_Amount_num_sum'].max()#var最大值
                dict_out['loan_second_m06_now_repayMons_mean'] = loan_second_m06_now['repayMons'].mean() #var平均值
                dict_out['loan_second_m06_now_balance_max'] = loan_second_m06_now['balance'].max()#var最大值
                dict_out['loan_second_m06_now_balance_min'] = loan_second_m06_now['balance'].min() #var最小值
                dict_out['loan_second_m06_now_balance_range'] = (dict_out['loan_second_m06_now_balance_max']-dict_out['loan_second_m06_now_balance_min'])/dict_out['loan_second_m06_now_balance_max'] if dict_out['loan_second_m06_now_balance_max']>0 else np.nan #var区间率
                dict_out['loan_second_m06_now_loanAmount_min'] = loan_second_m06_now['loanAmount'].min() #var最小值
                dict_out['loan_second_m06_now_businessType_giniimpurity'] = giniimpurity(loan_second_m06_now['businessType']) #近06个月开立 现行贷款账户业务种类基尼不纯度
                dict_out['loan_second_m06_now_bt_other_loan_ratio'] = loan_second_m06_now[loan_second_m06_now['businessType']=='其他贷款'].shape[0]/len(loan_second_m06_now) #近06个月开立 现行贷款账户业务种类为其他贷款 占比
                dict_out['loan_second_m06_now_org_giniimpurity'] = giniimpurity(loan_second_m06_now['org']) #近06个月开立 现行贷款账户管理机构基尼不纯度
                dict_out['loan_second_m06_now_month60_State_num_size_min'] = loan_second_m06_now['month60_State_num_size'].min() #var最小值比率
                dict_out['loan_second_m06_now_loanAmount_max'] = loan_second_m06_now['loanAmount'].max()#var最大值
                dict_out['loan_second_m06_now_loanAmount_min'] = loan_second_m06_now['loanAmount'].min() #var最小值比率
                dict_out['loan_second_m06_now_loanAmount_range'] = (dict_out['loan_second_m06_now_loanAmount_max']-dict_out['loan_second_m06_now_loanAmount_min'])/dict_out['loan_second_m06_now_loanAmount_max'] if dict_out['loan_second_m06_now_loanAmount_max']>0 else np.nan #var区间率
                dict_out['loan_second_m06_now_month60_to_report_min_max'] = loan_second_m06_now['month60_to_report_min'].max() #var最大值比率
                dict_out['loan_second_m06_now_repayTerms_mean'] = loan_second_m06_now['repayTerms'].mean()  #var平均值比率
                dict_out['loan_second_m06_now_repayAmt_mean'] = loan_second_m06_now['repayAmt'].mean()  #var平均值比率
                dict_out['loan_second_m06_now_month60_to_report_max_mean'] = loan_second_m06_now['month60_to_report_max'].mean()  #var平均值比率
                dict_out['loan_second_m06_now_month60_to_report_mean_mean'] = loan_second_m06_now['month60_to_report_mean'].mean()  #var平均值比率
                dict_out['loan_second_m06_now_byDate_to_report_sum'] = loan_second_m06_now['byDate_to_report'].sum() #var求和比率
                dict_out['loan_second_m06_now_repayMons_mean'] = loan_second_m06_now['repayMons'].mean()  #var平均值比率
                dict_out['loan_second_m06_now_byDate_to_report_mean'] = loan_second_m06_now['byDate_to_report'].mean()  #var平均值比率
                dict_out['loan_second_m06_now_repayMons_max'] = loan_second_m06_now['repayMons'].max()#var最大值
                dict_out['loan_second_m06_now_repayMons_min'] = loan_second_m06_now['repayMons'].min() #var最小值比率
                dict_out['loan_second_m06_now_repayMons_range'] = (dict_out['loan_second_m06_now_repayMons_max']-dict_out['loan_second_m06_now_repayMons_min'])/dict_out['loan_second_m06_now_repayMons_max'] if dict_out['loan_second_m06_now_repayMons_max']>0 else np.nan #var区间率
                dict_out['loan_second_m06_now_repayMons_min'] = loan_second_m06_now['repayMons'].min() #var最小值比率
                dict_out['loan_second_m06_now_balance_max'] = loan_second_m06_now['balance'].max()#var最大值
                dict_out['loan_second_m06_now_balance_min'] = loan_second_m06_now['balance'].min() #var最小值比率
                dict_out['loan_second_m06_now_balance_range'] = (dict_out['loan_second_m06_now_balance_max']-dict_out['loan_second_m06_now_balance_min'])/dict_out['loan_second_m06_now_balance_max'] if dict_out['loan_second_m06_now_balance_max']>0 else np.nan #var区间率
                dict_out['loan_second_m06_now_month60_State_countNullr_mean'] = loan_second_m06_now['month60_State_countNullr'].mean()  #var平均值比率
                dict_out['loan_second_m06_now_repayAmt_max'] = loan_second_m06_now['repayAmt'].max() #var最大值比率
                dict_out['loan_second_m06_now_repayMons_sum'] = loan_second_m06_now['repayMons'].sum() #var求和比率
                dict_out['loan_second_m06_now_repayMons_ratio_max'] = loan_second_m06_now['repayMons_ratio'].max()#var最大值
                dict_out['loan_second_m06_now_repayMons_ratio_min'] = loan_second_m06_now['repayMons_ratio'].min() #var最小值比率
                dict_out['loan_second_m06_now_repayMons_ratio_range'] = (dict_out['loan_second_m06_now_repayMons_ratio_max']-dict_out['loan_second_m06_now_repayMons_ratio_min'])/dict_out['loan_second_m06_now_repayMons_ratio_max'] if dict_out['loan_second_m06_now_repayMons_ratio_max']>0 else np.nan #var区间率
                dict_out['loan_second_m06_now_repayMons_ratio_mean'] = loan_second_m06_now['repayMons_ratio'].mean()  #var平均值比率
                dict_out['loan_second_m06_now_month60_to_report_min_mean'] = loan_second_m06_now['month60_to_report_min'].mean()  #var平均值比率
                dict_out['loan_second_m06_now_month60_to_report_mean_max'] = loan_second_m06_now['month60_to_report_mean'].max() #var最大值比率
                dict_out['loan_second_m06_now_balance_ratio_max'] = loan_second_m06_now['balance_ratio'].max()#var最大值
                dict_out['loan_second_m06_now_balance_ratio_min'] = loan_second_m06_now['balance_ratio'].min() #var最小值比率
                dict_out['loan_second_m06_now_balance_ratio_range'] = (dict_out['loan_second_m06_now_balance_ratio_max']-dict_out['loan_second_m06_now_balance_ratio_min'])/dict_out['loan_second_m06_now_balance_ratio_max'] if dict_out['loan_second_m06_now_balance_ratio_max']>0 else np.nan #var区间率
                dict_out['loan_second_m06_now_month60_State_countNull_mean'] = loan_second_m06_now['month60_State_countNull'].mean()  #var平均值比率
                dict_out['loan_second_m06_now_startDate_to_report_mean'] = loan_second_m06_now['startDate_to_report'].mean()  #var平均值比率
                dict_out['loan_second_m06_now_month60_to_report_mean_max'] = loan_second_m06_now['month60_to_report_mean'].max()#var最大值
                dict_out['loan_second_m06_now_month60_to_report_mean_min'] = loan_second_m06_now['month60_to_report_mean'].min() #var最小值比率
                dict_out['loan_second_m06_now_month60_to_report_mean_range'] = (dict_out['loan_second_m06_now_month60_to_report_mean_max']-dict_out['loan_second_m06_now_month60_to_report_mean_min'])/dict_out['loan_second_m06_now_month60_to_report_mean_max'] if dict_out['loan_second_m06_now_month60_to_report_mean_max']>0 else np.nan #var区间率
                dict_out['loan_second_m06_now_month60_to_report_mean_sum'] = loan_second_m06_now['month60_to_report_mean'].sum() #var求和比率
                dict_out['loan_second_m06_nowR_month60_State_num_size_min'] = dict_out['loan_second_m06_now_month60_State_num_size_min']/dict_out['loan_second_m06_month60_State_num_size_min'] if dict_out['loan_second_m06_month60_State_num_size_min']>0 else np.nan #var最大值比率
                dict_out['loan_second_m06_nowR_loanAmount_range'] = dict_out['loan_second_m06_now_loanAmount_range']/dict_out['loan_second_m06_loanAmount_range'] if dict_out['loan_second_m06_loanAmount_range']>0 else np.nan #var最大值比率
                dict_out['loan_second_m06_nowR_month60_to_report_min_max'] = dict_out['loan_second_m06_now_month60_to_report_min_max']/dict_out['loan_second_m06_month60_to_report_min_max'] if dict_out['loan_second_m06_month60_to_report_min_max']>0 else np.nan #var最大值比率
                dict_out['loan_second_m06_nowR_repayTerms_mean'] = dict_out['loan_second_m06_now_repayTerms_mean']/dict_out['loan_second_m06_repayTerms_mean'] if dict_out['loan_second_m06_repayTerms_mean']>0 else np.nan #var最大值比率
                dict_out['loan_second_m06_nowR_repayAmt_mean'] = dict_out['loan_second_m06_now_repayAmt_mean']/dict_out['loan_second_m06_repayAmt_mean'] if dict_out['loan_second_m06_repayAmt_mean']>0 else np.nan #var最大值比率
                dict_out['loan_second_m06_nowR_month60_to_report_max_mean'] = dict_out['loan_second_m06_now_month60_to_report_max_mean']/dict_out['loan_second_m06_month60_to_report_max_mean'] if dict_out['loan_second_m06_month60_to_report_max_mean']>0 else np.nan #var最大值比率
                dict_out['loan_second_m06_nowR_month60_to_report_mean_mean'] = dict_out['loan_second_m06_now_month60_to_report_mean_mean']/dict_out['loan_second_m06_month60_to_report_mean_mean'] if dict_out['loan_second_m06_month60_to_report_mean_mean']>0 else np.nan #var最大值比率
                dict_out['loan_second_m06_nowR_byDate_to_report_sum'] = dict_out['loan_second_m06_now_byDate_to_report_sum']/dict_out['loan_second_m06_byDate_to_report_sum'] if dict_out['loan_second_m06_byDate_to_report_sum']>0 else np.nan #var最大值比率
                dict_out['loan_second_m06_nowR_repayMons_mean'] = dict_out['loan_second_m06_now_repayMons_mean']/dict_out['loan_second_m06_repayMons_mean'] if dict_out['loan_second_m06_repayMons_mean']>0 else np.nan #var最大值比率
                dict_out['loan_second_m06_nowR_byDate_to_report_mean'] = dict_out['loan_second_m06_now_byDate_to_report_mean']/dict_out['loan_second_m06_byDate_to_report_mean'] if dict_out['loan_second_m06_byDate_to_report_mean']>0 else np.nan #var最大值比率
                dict_out['loan_second_m06_nowR_repayMons_range'] = dict_out['loan_second_m06_now_repayMons_range']/dict_out['loan_second_m06_repayMons_range'] if dict_out['loan_second_m06_repayMons_range']>0 else np.nan #var最大值比率
                dict_out['loan_second_m06_nowR_repayMons_min'] = dict_out['loan_second_m06_now_repayMons_min']/dict_out['loan_second_m06_repayMons_min'] if dict_out['loan_second_m06_repayMons_min']>0 else np.nan #var最大值比率
                dict_out['loan_second_m06_nowR_balance_range'] = dict_out['loan_second_m06_now_balance_range']/dict_out['loan_second_m06_balance_range'] if dict_out['loan_second_m06_balance_range']>0 else np.nan #var最大值比率
                dict_out['loan_second_m06_nowR_month60_State_countNullr_mean'] = dict_out['loan_second_m06_now_month60_State_countNullr_mean']/dict_out['loan_second_m06_month60_State_countNullr_mean'] if dict_out['loan_second_m06_month60_State_countNullr_mean']>0 else np.nan #var最大值比率
                dict_out['loan_second_m06_nowR_repayAmt_max'] = dict_out['loan_second_m06_now_repayAmt_max']/dict_out['loan_second_m06_repayAmt_max'] if dict_out['loan_second_m06_repayAmt_max']>0 else np.nan #var最大值比率
                dict_out['loan_second_m06_now_businessType_giniimpurity'] = giniimpurity(loan_second_m06_now['businessType']) #近06个月开立 现行贷款账户业务种类基尼不纯度
                dict_out['loan_second_m06_nowR_businessType_giniimpurity'] = dict_out['loan_second_m06_now_businessType_giniimpurity']/dict_out['loan_second_m06_businessType_giniimpurity'] if dict_out['loan_second_m06_businessType_giniimpurity']>0 else np.nan #var最大值比率
                dict_out['loan_second_m06_now_bt_other_loan_ratio'] = loan_second_m06_now[loan_second_m06_now['businessType']=='其他贷款'].shape[0]/len(loan_second_m06_now) #近06个月开立 现行贷款账户业务种类为其他贷款 占比
                dict_out['loan_second_m06_nowR_bt_other_loan_ratio'] = dict_out['loan_second_m06_now_bt_other_loan_ratio']/dict_out['loan_second_m06_bt_other_loan_ratio'] if dict_out['loan_second_m06_bt_other_loan_ratio']>0 else np.nan #var最大值比率
                dict_out['loan_second_m06_nowR_repayMons_sum'] = dict_out['loan_second_m06_now_repayMons_sum']/dict_out['loan_second_m06_repayMons_sum'] if dict_out['loan_second_m06_repayMons_sum']>0 else np.nan #var最大值比率
                dict_out['loan_second_m06_nowR_repayMons_ratio_range'] = dict_out['loan_second_m06_now_repayMons_ratio_range']/dict_out['loan_second_m06_repayMons_ratio_range'] if dict_out['loan_second_m06_repayMons_ratio_range']>0 else np.nan #var最大值比率
                dict_out['loan_second_m06_nowR_repayMons_ratio_mean'] = dict_out['loan_second_m06_now_repayMons_ratio_mean']/dict_out['loan_second_m06_repayMons_ratio_mean'] if dict_out['loan_second_m06_repayMons_ratio_mean']>0 else np.nan #var最大值比率
                dict_out['loan_second_m06_nowR_month60_to_report_min_mean'] = dict_out['loan_second_m06_now_month60_to_report_min_mean']/dict_out['loan_second_m06_month60_to_report_min_mean'] if dict_out['loan_second_m06_month60_to_report_min_mean']>0 else np.nan #var最大值比率
                dict_out['loan_second_m06_now_org_giniimpurity'] = giniimpurity(loan_second_m06_now['org']) #近06个月开立 现行贷款账户管理机构基尼不纯度
                dict_out['loan_second_m06_nowR_org_giniimpurity'] = dict_out['loan_second_m06_now_org_giniimpurity']/dict_out['loan_second_m06_org_giniimpurity'] if dict_out['loan_second_m06_org_giniimpurity']>0 else np.nan #var最大值比率
                dict_out['loan_second_m06_nowR_month60_to_report_mean_max'] = dict_out['loan_second_m06_now_month60_to_report_mean_max']/dict_out['loan_second_m06_month60_to_report_mean_max'] if dict_out['loan_second_m06_month60_to_report_mean_max']>0 else np.nan #var最大值比率
                dict_out['loan_second_m06_nowR_balance_ratio_range'] = dict_out['loan_second_m06_now_balance_ratio_range']/dict_out['loan_second_m06_balance_ratio_range'] if dict_out['loan_second_m06_balance_ratio_range']>0 else np.nan #var最大值比率
                dict_out['loan_second_m06_nowR_month60_State_countNull_mean'] = dict_out['loan_second_m06_now_month60_State_countNull_mean']/dict_out['loan_second_m06_month60_State_countNull_mean'] if dict_out['loan_second_m06_month60_State_countNull_mean']>0 else np.nan #var最大值比率
                dict_out['loan_second_m06_nowR_startDate_to_report_mean'] = dict_out['loan_second_m06_now_startDate_to_report_mean']/dict_out['loan_second_m06_startDate_to_report_mean'] if dict_out['loan_second_m06_startDate_to_report_mean']>0 else np.nan #var最大值比率
                dict_out['loan_second_m06_nowR_month60_to_report_mean_range'] = dict_out['loan_second_m06_now_month60_to_report_mean_range']/dict_out['loan_second_m06_month60_to_report_mean_range'] if dict_out['loan_second_m06_month60_to_report_mean_range']>0 else np.nan #var最大值比率
                dict_out['loan_second_m06_nowR_month60_to_report_mean_sum'] = dict_out['loan_second_m06_now_month60_to_report_mean_sum']/dict_out['loan_second_m06_month60_to_report_mean_sum'] if dict_out['loan_second_m06_month60_to_report_mean_sum']>0 else np.nan #var最大值比率

            #近06个月开立 非循环贷账户
            loan_second_m06_ncycle = loan_second_m06[loan_second_m06['class']=='非循环贷账户']
            if len(loan_second_m06_ncycle)>0:
                dict_out['loan_second_m06_ncycle_startDate_to_report_mean'] = loan_second_m06_ncycle['startDate_to_report'].mean() #var平均值
                dict_out['loan_second_m06_ncycle_month60_to_report_mean_mean'] = loan_second_m06_ncycle['month60_to_report_mean'].mean() #var平均值
                dict_out['loan_second_m06_ncycle_org_consumer_finance_ratio'] = loan_second_m06_ncycle[loan_second_m06_ncycle['org']=='消费金融公司'].shape[0]/len(loan_second_m06_ncycle) #近06个月开立 非循环贷账户 贷款账户管理机构为消费金融公司 占比
                dict_out['loan_second_m06_ncycle_due_class_mean'] = loan_second_m06_ncycle['due_class'].mean() #var平均值
                dict_out['loan_second_m06_ncycle_month60_State_countNr_mean'] = loan_second_m06_ncycle['month60_State_countNr'].mean() #var平均值
                dict_out['loan_second_m06_ncycle_gf_other_count'] = loan_second_m06_ncycle[loan_second_m06_ncycle['guaranteeForm']=='其他'].shape[0] #近06个月开立 非循环贷账户 贷款账户担保方式为其他 计数
                dict_out['loan_second_m06_ncycle_repayFrequency_giniimpurity'] = giniimpurity(loan_second_m06_ncycle['repayFrequency']) #近06个月开立 非循环贷账户 贷款账户还款频率基尼不纯度
                dict_out['loan_second_m06_ncycle_month60_State_num_max_sum'] = loan_second_m06_ncycle['month60_State_num_max'].sum() #var求和
                dict_out['loan_second_m06_ncycle_repayedAmount_sum'] = loan_second_m06_ncycle['repayedAmount'].sum() #var求和
                dict_out['loan_second_m06_ncycle_org_commercial_bank_ratio'] = loan_second_m06_ncycle[loan_second_m06_ncycle['org']=='商业银行'].shape[0]/len(loan_second_m06_ncycle) #近06个月开立 非循环贷账户 贷款账户管理机构为商业银行 占比
                dict_out['loan_second_m06_ncycle_month60_Amount_num_max_max'] = loan_second_m06_ncycle['month60_Amount_num_max'].max()#var最大值
                dict_out['loan_second_m06_ncycle_month60_State_num_size_sum'] = loan_second_m06_ncycle['month60_State_num_size'].sum() #var求和
                dict_out['loan_second_m06_ncycle_balance_ratio_max'] = loan_second_m06_ncycle['balance_ratio'].max()#var最大值
                dict_out['loan_second_m06_ncycle_month60_to_report_max_mean'] = loan_second_m06_ncycle['month60_to_report_max'].mean() #var平均值
                dict_out['loan_second_m06_ncycle_guaranteeForm_giniimpurity'] = giniimpurity(loan_second_m06_ncycle['guaranteeForm']) #近06个月开立 非循环贷账户 贷款账户担保方式基尼不纯度
                dict_out['loan_second_m06_ncycle_month60_State_countCr_mean'] = loan_second_m06_ncycle['month60_State_countCr'].mean() #var平均值
                dict_out['loan_second_m06_ncycle_repayTerm_ratio_max'] = loan_second_m06_ncycle['repayTerm_ratio'].max()#var最大值
                dict_out['loan_second_m06_ncycle_repayTerm_ratio_min'] = loan_second_m06_ncycle['repayTerm_ratio'].min() #var最小值
                dict_out['loan_second_m06_ncycle_repayTerm_ratio_range'] = (dict_out['loan_second_m06_ncycle_repayTerm_ratio_max']-dict_out['loan_second_m06_ncycle_repayTerm_ratio_min'])/dict_out['loan_second_m06_ncycle_repayTerm_ratio_max'] if dict_out['loan_second_m06_ncycle_repayTerm_ratio_max']>0 else np.nan #var区间率
                dict_out['loan_second_m06_ncycle_month60_State_countNullr_mean'] = loan_second_m06_ncycle['month60_State_countNullr'].mean() #var平均值
                dict_out['loan_second_m06_ncycle_month60_State_num_mean_max'] = loan_second_m06_ncycle['month60_State_num_mean'].max()#var最大值
                dict_out['loan_second_m06_ncycle_balance_ratio_max'] = loan_second_m06_ncycle['balance_ratio'].max()#var最大值
                dict_out['loan_second_m06_ncycle_balance_ratio_min'] = loan_second_m06_ncycle['balance_ratio'].min() #var最小值
                dict_out['loan_second_m06_ncycle_balance_ratio_range'] = (dict_out['loan_second_m06_ncycle_balance_ratio_max']-dict_out['loan_second_m06_ncycle_balance_ratio_min'])/dict_out['loan_second_m06_ncycle_balance_ratio_max'] if dict_out['loan_second_m06_ncycle_balance_ratio_max']>0 else np.nan #var区间率
                dict_out['loan_second_m06_ncycle_repayedAmount_max'] = loan_second_m06_ncycle['repayedAmount'].max()#var最大值
                dict_out['loan_second_m06_ncycle_balance_min'] = loan_second_m06_ncycle['balance'].min() #var最小值
                dict_out['loan_second_m06_ncycle_month60_Amount_num_sum_mean'] = loan_second_m06_ncycle['month60_Amount_num_sum'].mean() #var平均值
                dict_out['loan_second_m06_ncycle_repayMons_ratio_min'] = loan_second_m06_ncycle['repayMons_ratio'].min() #var最小值
                dict_out['loan_second_m06_ncycle_byDate_to_report_mean'] = loan_second_m06_ncycle['byDate_to_report'].mean() #var平均值
                dict_out['loan_second_m06_ncycle_org_giniimpurity'] = giniimpurity(loan_second_m06_ncycle['org']) #近06个月开立 非循环贷账户 贷款账户管理机构基尼不纯度
                dict_out['loan_second_m06_ncycle_balance_ratio_min'] = loan_second_m06_ncycle['balance_ratio'].min() #var最小值
                dict_out['loan_second_m06_ncycle_leftRepayTerms_max'] = loan_second_m06_ncycle['leftRepayTerms'].max()#var最大值
                dict_out['loan_second_m06_ncycle_balance_ratio_mean'] = loan_second_m06_ncycle['balance_ratio'].mean() #var平均值
                dict_out['loan_second_m06_ncycle_org_other_ratio'] = loan_second_m06_ncycle[loan_second_m06_ncycle['org']=='其他机构'].shape[0]/len(loan_second_m06_ncycle) #近06个月开立 非循环贷账户 贷款账户管理机构为其他机构 占比
                dict_out['loan_second_m06_ncycle_loanAmount_min'] = loan_second_m06_ncycle['loanAmount'].min() #var最小值
                dict_out['loan_second_m06_ncycle_month60_to_report_mean_sum'] = loan_second_m06_ncycle['month60_to_report_mean'].sum() #var求和
                dict_out['loan_second_m06_ncycle_repayedAmount_mean'] = loan_second_m06_ncycle['repayedAmount'].mean() #var平均值
                dict_out['loan_second_m06_ncycle_gf_crdit_ratio'] = loan_second_m06_ncycle[loan_second_m06_ncycle['guaranteeForm']=='信用/免担保'].shape[0]/len(loan_second_m06_ncycle) #近06个月开立 非循环贷账户 贷款账户担保方式为信用/免担保 占比
                dict_out['loan_second_m06_ncycle_guaranteeForm_giniimpurity'] = giniimpurity(loan_second_m06_ncycle['guaranteeForm']) #近06个月开立 非循环贷账户 贷款账户担保方式基尼不纯度
                dict_out['loan_second_m06_ncycle_org_commercial_bank_ratio'] = loan_second_m06_ncycle[loan_second_m06_ncycle['org']=='商业银行'].shape[0]/len(loan_second_m06_ncycle) #近06个月开立 非循环贷账户 贷款账户管理机构为商业银行 占比
                dict_out['loan_second_m06_ncycle_leftRepayTerms_mean'] = loan_second_m06_ncycle['leftRepayTerms'].mean()  #var平均值比率
                dict_out['loan_second_m06_ncycle_balance_sum'] = loan_second_m06_ncycle['balance'].sum() #var求和比率
                dict_out['loan_second_m06_ncycle_repayTerms_mean'] = loan_second_m06_ncycle['repayTerms'].mean()  #var平均值比率
                dict_out['loan_second_m06_ncycle_gf_crdit_ratio'] = loan_second_m06_ncycle[loan_second_m06_ncycle['guaranteeForm']=='信用/免担保'].shape[0]/len(loan_second_m06_ncycle) #近06个月开立 非循环贷账户 贷款账户担保方式为信用/免担保 占比
                dict_out['loan_second_m06_ncycleR_gf_crdit_ratio'] = dict_out['loan_second_m06_ncycle_gf_crdit_ratio']/dict_out['loan_second_m06_gf_crdit_ratio'] if dict_out['loan_second_m06_gf_crdit_ratio']>0 else np.nan #var最大值比率
                dict_out['loan_second_m06_ncycle_guaranteeForm_giniimpurity'] = giniimpurity(loan_second_m06_ncycle['guaranteeForm']) #近06个月开立 非循环贷账户 贷款账户担保方式基尼不纯度
                dict_out['loan_second_m06_ncycleR_guaranteeForm_giniimpurity'] = dict_out['loan_second_m06_ncycle_guaranteeForm_giniimpurity']/dict_out['loan_second_m06_guaranteeForm_giniimpurity'] if dict_out['loan_second_m06_guaranteeForm_giniimpurity']>0 else np.nan #var最大值比率
                dict_out['loan_second_m06_ncycleR_leftRepayTerms_mean'] = dict_out['loan_second_m06_ncycle_leftRepayTerms_mean']/dict_out['loan_second_m06_leftRepayTerms_mean'] if dict_out['loan_second_m06_leftRepayTerms_mean']>0 else np.nan #var最大值比率
                dict_out['loan_second_m06_ncycleR_balance_sum'] = dict_out['loan_second_m06_ncycle_balance_sum']/dict_out['loan_second_m06_balance_sum'] if dict_out['loan_second_m06_balance_sum']>0 else np.nan #var最大值比率
                dict_out['loan_second_m06_ncycleR_repayTerms_mean'] = dict_out['loan_second_m06_ncycle_repayTerms_mean']/dict_out['loan_second_m06_repayTerms_mean'] if dict_out['loan_second_m06_repayTerms_mean']>0 else np.nan #var最大值比率
                dict_out['loan_second_m06_ncycle_org_commercial_bank_ratio'] = loan_second_m06_ncycle[loan_second_m06_ncycle['org']=='商业银行'].shape[0]/len(loan_second_m06_ncycle) #近06个月开立 非循环贷账户 贷款账户管理机构为商业银行 占比
                dict_out['loan_second_m06_ncycleR_org_commercial_bank_ratio'] = dict_out['loan_second_m06_ncycle_org_commercial_bank_ratio']/dict_out['loan_second_m06_org_commercial_bank_ratio'] if dict_out['loan_second_m06_org_commercial_bank_ratio']>0 else np.nan #var最大值比率



            #近06个月开立 循环贷账户
            loan_second_m06_cycle = loan_second_m06[loan_second_m06['class']=='循环贷账户']


            #近06个月开立 有担保 is_vouch 
            loan_second_m06_vouch = loan_second_m06[loan_second_m06['is_vouch']==1]


            #近06个月开立 历史逾期
            loan_second_m06_hdue1 = loan_second_m06[loan_second_m06['is_overdue']==1]


            #近06个月开立 历史严重逾期
            loan_second_m06_hdue3 = loan_second_m06[loan_second_m06['due_class']==3]


            #近06个月开立 当前逾期
            loan_second_m06_cdue = loan_second_m06[loan_second_m06['accountStatus']=='逾期']


        #近12个月开立
        loan_second_m12 = loan_second[loan_second.startDate_to_report<12]
        if len(loan_second_m12)>0:

            if True:
                for var in numeric_vers:
                    dict_out['loan_second_m12_'+var+'_max'] = loan_second_m12[var].max() #近12个月开立 var最大值
                    dict_out['loan_second_m12_'+var+'_min'] = loan_second_m12[var].min() #近12个月开立 var最小值
                    dict_out['loan_second_m12_'+var+'_mean'] = loan_second_m12[var].mean() #近12个月开立 var平均值
                    dict_out['loan_second_m12_'+var+'_sum'] = loan_second_m12[var].sum() #近12个月开立 var求和
                    dict_out['loan_second_m12_'+var+'_range'] = (dict_out['loan_second_m12_'+var+'_max']-dict_out['loan_second_m12_'+var+'_min'])/dict_out['loan_second_m12_'+var+'_max'] if dict_out['loan_second_m12_'+var+'_max']>0 else np.nan #近12个月开立 var区间率



                dict_out['loan_second_m12_loanGrantOrg_giniimpurity'] = giniimpurity(loan_second_m12['loanGrantOrg']) #近12个月开立 贷款账户管理机构详细基尼不纯度
                dict_out['loan_second_m12_loanGrantOrg_nunique'] = loan_second_m12['loanGrantOrg'].nunique() #近12个月开立 贷款账户管理机构详细nunique

                dict_out['loan_second_m12_org_giniimpurity'] = giniimpurity(loan_second_m12['org']) #近12个月开立 贷款账户管理机构基尼不纯度
                dict_out['loan_second_m12_org_commercial_bank_count'] = loan_second_m12[loan_second_m12['org']=='商业银行'].shape[0] #近12个月开立 贷款账户管理机构为商业银行 计数
                dict_out['loan_second_m12_org_commercial_bank_ratio'] = loan_second_m12[loan_second_m12['org']=='商业银行'].shape[0]/len(loan_second_m12) #近12个月开立 贷款账户管理机构为商业银行 占比
                dict_out['loan_second_m12_org_consumer_finance_count'] = loan_second_m12[loan_second_m12['org']=='消费金融公司'].shape[0] #近12个月开立 贷款账户管理机构为消费金融公司 计数
                dict_out['loan_second_m12_org_consumer_finance_ratio'] = loan_second_m12[loan_second_m12['org']=='消费金融公司'].shape[0]/len(loan_second_m12) #近12个月开立 贷款账户管理机构为消费金融公司 占比
                dict_out['loan_second_m12_org_micro_loan_count'] = loan_second_m12[loan_second_m12['org']=='小额贷款公司'].shape[0] #近12个月开立 贷款账户管理机构为小额贷款公司 计数
                dict_out['loan_second_m12_org_micro_loan_ratio'] = loan_second_m12[loan_second_m12['org']=='小额贷款公司'].shape[0]/len(loan_second_m12) #近12个月开立 贷款账户管理机构为小额贷款公司 占比
                dict_out['loan_second_m12_org_other_count'] = loan_second_m12[loan_second_m12['org']=='其他机构'].shape[0] #近12个月开立 贷款账户管理机构为其他机构 计数
                dict_out['loan_second_m12_org_other_ratio'] = loan_second_m12[loan_second_m12['org']=='其他机构'].shape[0]/len(loan_second_m12) #近12个月开立 贷款账户管理机构为其他机构 占比
                dict_out['loan_second_m12_org_trust_company_count'] = loan_second_m12[loan_second_m12['org']=='信托公司'].shape[0] #近12个月开立 贷款账户管理机构为信托公司 计数
                dict_out['loan_second_m12_org_trust_company_ratio'] = loan_second_m12[loan_second_m12['org']=='信托公司'].shape[0]/len(loan_second_m12) #近12个月开立 贷款账户管理机构为信托公司 占比
                dict_out['loan_second_m12_org_car_finance_count'] = loan_second_m12[loan_second_m12['org']=='汽车金融公司'].shape[0] #近12个月开立 贷款账户管理机构为汽车金融公司 计数
                dict_out['loan_second_m12_org_car_finance_ratio'] = loan_second_m12[loan_second_m12['org']=='汽车金融公司'].shape[0]/len(loan_second_m12) #近12个月开立 贷款账户管理机构为汽车金融公司 占比
                dict_out['loan_second_m12_org_lease_finance_count'] = loan_second_m12[loan_second_m12['org']=='融资租赁公司'].shape[0] #近12个月开立 贷款账户管理机构为融资租赁公司 计数
                dict_out['loan_second_m12_org_lease_finance_ratio'] = loan_second_m12[loan_second_m12['org']=='融资租赁公司'].shape[0]/len(loan_second_m12) #近12个月开立 贷款账户管理机构为融资租赁公司 占比
                dict_out['loan_second_m12_org_myself_count'] = loan_second_m12[loan_second_m12['org']==dict_in['cc_rh_report'].queryOperator.values[0]].shape[0] #近12个月开立 贷款账户管理机构为本机构 计数
                dict_out['loan_second_m12_org_myself_ratio'] = loan_second_m12[loan_second_m12['org']==dict_in['cc_rh_report'].queryOperator.values[0]].shape[0]/len(loan_second_m12) #近12个月开立 贷款账户管理机构为本机构 占比
                dict_out['loan_second_m12_org_village_banks_count'] = loan_second_m12[loan_second_m12['org']=='村镇银行'].shape[0] #近12个月开立 贷款账户管理机构为村镇银行 计数
                dict_out['loan_second_m12_org_village_banks_ratio'] = loan_second_m12[loan_second_m12['org']=='村镇银行'].shape[0]/len(loan_second_m12) #近12个月开立 贷款账户管理机构为村镇银行 占比
                dict_out['loan_second_m12_org_finance_company_count'] = loan_second_m12[loan_second_m12['org']=='财务公司'].shape[0] #近12个月开立 贷款账户管理机构为财务公司 计数
                dict_out['loan_second_m12_org_finance_company_ratio'] = loan_second_m12[loan_second_m12['org']=='财务公司'].shape[0]/len(loan_second_m12) #近12个月开立 贷款账户管理机构为财务公司 占比
                dict_out['loan_second_m12_org_foreign_banks_count'] = loan_second_m12[loan_second_m12['org']=='外资银行'].shape[0] #近12个月开立 贷款账户管理机构为外资银行 计数
                dict_out['loan_second_m12_org_foreign_banks_ratio'] = loan_second_m12[loan_second_m12['org']=='外资银行'].shape[0]/len(loan_second_m12) #近12个月开立 贷款账户管理机构为外资银行 占比
                dict_out['loan_second_m12_org_provident_fund_count'] = loan_second_m12[loan_second_m12['org']=='公积金管理中心'].shape[0] #近12个月开立 贷款账户管理机构为公积金管理中心 计数
                dict_out['loan_second_m12_org_provident_fund_ratio'] = loan_second_m12[loan_second_m12['org']=='公积金管理中心'].shape[0]/len(loan_second_m12) #近12个月开立 贷款账户管理机构为公积金管理中心 占比
                dict_out['loan_second_m12_org_securities_firm_count'] = loan_second_m12[loan_second_m12['org']=='证券公司'].shape[0] #近12个月开立 贷款账户管理机构为证券公司 计数
                dict_out['loan_second_m12_org_securities_firm_ratio'] = loan_second_m12[loan_second_m12['org']=='证券公司'].shape[0]/len(loan_second_m12) #近12个月开立 贷款账户管理机构为证券公司 占比

                dict_out['loan_second_m12_class_giniimpurity'] = giniimpurity(loan_second_m12['class']) #近12个月开立 贷款账户账户类别基尼不纯度
                dict_out['loan_second_m12_class_ncycle_count'] = loan_second_m12[loan_second_m12['class']=='非循环贷账户'].shape[0] #近12个月开立 贷款账户账户类别为非循环贷账户 计数
                dict_out['loan_second_m12_class_ncycle_ratio'] = loan_second_m12[loan_second_m12['class']=='非循环贷账户'].shape[0]/len(loan_second_m12) #近12个月开立 贷款账户账户类别为非循环贷账户 占比
                dict_out['loan_second_m12_class_cycle_sub_count'] = loan_second_m12[loan_second_m12['class']=='循环额度下分账户'].shape[0] #近12个月开立 贷款账户账户类别为循环额度下分账户 计数
                dict_out['loan_second_m12_class_cycle_sub_ratio'] = loan_second_m12[loan_second_m12['class']=='循环额度下分账户'].shape[0]/len(loan_second_m12) #近12个月开立 贷款账户账户类别为循环额度下分账户 占比
                dict_out['loan_second_m12_class_cycle_count'] = loan_second_m12[loan_second_m12['class']=='循环贷账户'].shape[0] #近12个月开立 贷款账户账户类别为循环贷账户 计数
                dict_out['loan_second_m12_class_cycle_ratio'] = loan_second_m12[loan_second_m12['class']=='循环贷账户'].shape[0]/len(loan_second_m12) #近12个月开立 贷款账户账户类别为循环贷账户 占比

                dict_out['loan_second_m12_classify5_giniimpurity'] = giniimpurity(loan_second_m12['classify5']) #近12个月开立 贷款账户五级分类基尼不纯度
                dict_out['loan_second_m12_c5_unknow_count'] = loan_second_m12[loan_second_m12['classify5']==''].shape[0] #近12个月开立 贷款账户五级分类为'' 计数
                dict_out['loan_second_m12_c5_unknow_ratio'] = loan_second_m12[loan_second_m12['classify5']==''].shape[0]/len(loan_second_m12) #近12个月开立 贷款账户五级分类为'' 占比
                dict_out['loan_second_m12_c5_normal_count'] = loan_second_m12[loan_second_m12['classify5']=='正常'].shape[0] #近12个月开立 贷款账户五级分类为正常 计数
                dict_out['loan_second_m12_c5_normal_ratio'] = loan_second_m12[loan_second_m12['classify5']=='正常'].shape[0]/len(loan_second_m12) #近12个月开立 贷款账户五级分类为正常 占比
                dict_out['loan_second_m12_c5_loss_count'] = loan_second_m12[loan_second_m12['classify5']=='损失'].shape[0] #近12个月开立 贷款账户五级分类为损失 计数
                dict_out['loan_second_m12_c5_loss_ratio'] = loan_second_m12[loan_second_m12['classify5']=='损失'].shape[0]/len(loan_second_m12) #近12个月开立 贷款账户五级分类为损失 占比
                dict_out['loan_second_m12_c5_attention_count'] = loan_second_m12[loan_second_m12['classify5']=='关注'].shape[0] #近12个月开立 贷款账户五级分类为关注 计数
                dict_out['loan_second_m12_c5_attention_ratio'] = loan_second_m12[loan_second_m12['classify5']=='关注'].shape[0]/len(loan_second_m12) #近12个月开立 贷款账户五级分类为关注 占比
                dict_out['loan_second_m12_c5_suspicious_count'] = loan_second_m12[loan_second_m12['classify5']=='可疑'].shape[0] #近12个月开立 贷款账户五级分类为可疑 计数
                dict_out['loan_second_m12_c5_suspicious_ratio'] = loan_second_m12[loan_second_m12['classify5']=='可疑'].shape[0]/len(loan_second_m12) #近12个月开立 贷款账户五级分类为可疑 占比
                dict_out['loan_second_m12_c5_secondary_count'] = loan_second_m12[loan_second_m12['classify5']=='次级'].shape[0] #近12个月开立 贷款账户五级分类为次级 计数
                dict_out['loan_second_m12_c5_secondary_ratio'] = loan_second_m12[loan_second_m12['classify5']=='次级'].shape[0]/len(loan_second_m12) #近12个月开立 贷款账户五级分类为次级 占比
                dict_out['loan_second_m12_c5_noclass_count'] = loan_second_m12[loan_second_m12['classify5']=='未分类'].shape[0] #近12个月开立 贷款账户五级分类为未分类 计数
                dict_out['loan_second_m12_c5_noclass_ratio'] = loan_second_m12[loan_second_m12['classify5']=='未分类'].shape[0]/len(loan_second_m12) #近12个月开立 贷款账户五级分类为未分类 占比

                dict_out['loan_second_m12_accountStatus_giniimpurity'] = giniimpurity(loan_second_m12['accountStatus']) #近12个月开立 贷款账户账户状态基尼不纯度
                dict_out['loan_second_m12_as_settle_count'] = loan_second_m12[loan_second_m12['accountStatus']=='结清'].shape[0] #近12个月开立 贷款账户账户状态为结清 计数
                dict_out['loan_second_m12_as_settle_ratio'] = loan_second_m12[loan_second_m12['accountStatus']=='结清'].shape[0]/len(loan_second_m12) #近12个月开立 贷款账户账户状态为结清 占比
                dict_out['loan_second_m12_as_normal_count'] = loan_second_m12[loan_second_m12['accountStatus']=='正常'].shape[0] #近12个月开立 贷款账户账户状态为正常 计数
                dict_out['loan_second_m12_as_normal_ratio'] = loan_second_m12[loan_second_m12['accountStatus']=='正常'].shape[0]/len(loan_second_m12) #近12个月开立 贷款账户账户状态为正常 占比
                dict_out['loan_second_m12_as_overdue_count'] = loan_second_m12[loan_second_m12['accountStatus']=='逾期'].shape[0] #近12个月开立 贷款账户账户状态为逾期 计数
                dict_out['loan_second_m12_as_overdue_ratio'] = loan_second_m12[loan_second_m12['accountStatus']=='逾期'].shape[0]/len(loan_second_m12) #近12个月开立 贷款账户账户状态为逾期 占比
                dict_out['loan_second_m12_as_bad_debts_count'] = loan_second_m12[loan_second_m12['accountStatus']=='呆账'].shape[0] #近12个月开立 贷款账户账户状态为呆账 计数
                dict_out['loan_second_m12_as_bad_debts_ratio'] = loan_second_m12[loan_second_m12['accountStatus']=='呆账'].shape[0]/len(loan_second_m12) #近12个月开立 贷款账户账户状态为呆账 占比
                dict_out['loan_second_m12_as_unknow_count'] = loan_second_m12[loan_second_m12['accountStatus']==''].shape[0] #近12个月开立 贷款账户账户状态为'' 计数
                dict_out['loan_second_m12_as_unknow_ratio'] = loan_second_m12[loan_second_m12['accountStatus']==''].shape[0]/len(loan_second_m12) #近12个月开立 贷款账户账户状态为'' 占比
                dict_out['loan_second_m12_as_roll_out_count'] = loan_second_m12[loan_second_m12['accountStatus']=='转出'].shape[0] #近12个月开立 贷款账户账户状态为转出 计数
                dict_out['loan_second_m12_as_roll_out_ratio'] = loan_second_m12[loan_second_m12['accountStatus']=='转出'].shape[0]/len(loan_second_m12) #近12个月开立 贷款账户账户状态为转出 占比

                dict_out['loan_second_m12_repayType_giniimpurity'] = giniimpurity(loan_second_m12['repayType']) #近12个月开立 贷款账户还款方式基尼不纯度
                dict_out['loan_second_m12_rt_unknow_count'] = loan_second_m12[loan_second_m12['repayType']=='--'].shape[0] #近12个月开立 贷款账户还款方式为-- 计数
                dict_out['loan_second_m12_rt_unknow_ratio'] = loan_second_m12[loan_second_m12['repayType']=='--'].shape[0]/len(loan_second_m12) #近12个月开立 贷款账户还款方式为-- 占比
                dict_out['loan_second_m12_rt_equality_count'] = loan_second_m12[loan_second_m12['repayType']=='分期等额本息'].shape[0] #近12个月开立 贷款账户还款方式为分期等额本息 计数
                dict_out['loan_second_m12_rt_equality_ratio'] = loan_second_m12[loan_second_m12['repayType']=='分期等额本息'].shape[0]/len(loan_second_m12) #近12个月开立 贷款账户还款方式为分期等额本息 占比
                dict_out['loan_second_m12_rt_onschedule_count'] = loan_second_m12[loan_second_m12['repayType']=='按期计算还本付息'].shape[0] #近12个月开立 贷款账户还款方式为按期计算还本付息 计数
                dict_out['loan_second_m12_rt_onschedule_ratio'] = loan_second_m12[loan_second_m12['repayType']=='按期计算还本付息'].shape[0]/len(loan_second_m12) #近12个月开立 贷款账户还款方式为按期计算还本付息 占比
                dict_out['loan_second_m12_rt_not_distinguish_count'] = loan_second_m12[loan_second_m12['repayType']=='不区分还款方式'].shape[0] #近12个月开立 贷款账户还款方式为不区分还款方式 计数
                dict_out['loan_second_m12_rt_not_distinguish_ratio'] = loan_second_m12[loan_second_m12['repayType']=='不区分还款方式'].shape[0]/len(loan_second_m12) #近12个月开立 贷款账户还款方式为不区分还款方式 占比
                dict_out['loan_second_m12_rt_circulation_count'] = loan_second_m12[loan_second_m12['repayType']=='循环贷款下其他还款方式'].shape[0] #近12个月开立 贷款账户还款方式为循环贷款下其他还款方式 计数
                dict_out['loan_second_m12_rt_circulation_ratio'] = loan_second_m12[loan_second_m12['repayType']=='循环贷款下其他还款方式'].shape[0]/len(loan_second_m12) #近12个月开立 贷款账户还款方式为循环贷款下其他还款方式 占比
                dict_out['loan_second_m12_rt_once_count'] = loan_second_m12[loan_second_m12['repayType']=='到期一次还本付息'].shape[0] #近12个月开立 贷款账户还款方式为到期一次还本付息 计数
                dict_out['loan_second_m12_rt_once_ratio'] = loan_second_m12[loan_second_m12['repayType']=='到期一次还本付息'].shape[0]/len(loan_second_m12) #近12个月开立 贷款账户还款方式为到期一次还本付息 占比

                dict_out['loan_second_m12_repayFrequency_giniimpurity'] = giniimpurity(loan_second_m12['repayFrequency']) #近12个月开立 贷款账户还款频率基尼不纯度
                dict_out['loan_second_m12_rf_month_count'] = loan_second_m12[loan_second_m12['repayFrequency']=='月'].shape[0] #近12个月开立 贷款账户还款频率为月 计数
                dict_out['loan_second_m12_rf_month_ratio'] = loan_second_m12[loan_second_m12['repayFrequency']=='月'].shape[0]/len(loan_second_m12) #近12个月开立 贷款账户还款频率为月 占比
                dict_out['loan_second_m12_rf_once_count'] = loan_second_m12[loan_second_m12['repayFrequency']=='一次性'].shape[0] #近12个月开立 贷款账户还款频率为一次性 计数
                dict_out['loan_second_m12_rf_once_ratio'] = loan_second_m12[loan_second_m12['repayFrequency']=='一次性'].shape[0]/len(loan_second_m12) #近12个月开立 贷款账户还款频率为一次性 占比
                dict_out['loan_second_m12_rf_other_count'] = loan_second_m12[loan_second_m12['repayFrequency']=='其他'].shape[0] #近12个月开立 贷款账户还款频率为其他 计数
                dict_out['loan_second_m12_rf_other_ratio'] = loan_second_m12[loan_second_m12['repayFrequency']=='其他'].shape[0]/len(loan_second_m12) #近12个月开立 贷款账户还款频率为其他 占比
                dict_out['loan_second_m12_rf_irregular_count'] = loan_second_m12[loan_second_m12['repayFrequency']=='不定期'].shape[0] #近12个月开立 贷款账户还款频率为不定期 计数
                dict_out['loan_second_m12_rf_irregular_ratio'] = loan_second_m12[loan_second_m12['repayFrequency']=='不定期'].shape[0]/len(loan_second_m12) #近12个月开立 贷款账户还款频率为不定期 占比
                dict_out['loan_second_m12_rf_day_count'] = loan_second_m12[loan_second_m12['repayFrequency']=='日'].shape[0] #近12个月开立 贷款账户还款频率为日 计数
                dict_out['loan_second_m12_rf_day_ratio'] = loan_second_m12[loan_second_m12['repayFrequency']=='日'].shape[0]/len(loan_second_m12) #近12个月开立 贷款账户还款频率为日 占比
                dict_out['loan_second_m12_rf_year_count'] = loan_second_m12[loan_second_m12['repayFrequency']=='年'].shape[0] #近12个月开立 贷款账户还款频率为年 计数
                dict_out['loan_second_m12_rf_year_ratio'] = loan_second_m12[loan_second_m12['repayFrequency']=='年'].shape[0]/len(loan_second_m12) #近12个月开立 贷款账户还款频率为年 占比
                dict_out['loan_second_m12_rf_season_count'] = loan_second_m12[loan_second_m12['repayFrequency']=='季'].shape[0] #近12个月开立 贷款账户还款频率为季 计数
                dict_out['loan_second_m12_rf_season_ratio'] = loan_second_m12[loan_second_m12['repayFrequency']=='季'].shape[0]/len(loan_second_m12) #近12个月开立 贷款账户还款频率为季 占比
                dict_out['loan_second_m12_rf_week_count'] = loan_second_m12[loan_second_m12['repayFrequency']=='周'].shape[0] #近12个月开立 贷款账户还款频率为周 计数
                dict_out['loan_second_m12_rf_week_ratio'] = loan_second_m12[loan_second_m12['repayFrequency']=='周'].shape[0]/len(loan_second_m12) #近12个月开立 贷款账户还款频率为周 占比
                dict_out['loan_second_m12_rf_halfyear_count'] = loan_second_m12[loan_second_m12['repayFrequency']=='半年'].shape[0] #近12个月开立 贷款账户还款频率为半年 计数
                dict_out['loan_second_m12_rf_halfyear_ratio'] = loan_second_m12[loan_second_m12['repayFrequency']=='半年'].shape[0]/len(loan_second_m12) #近12个月开立 贷款账户还款频率为半年 占比

                dict_out['loan_second_m12_guaranteeForm_giniimpurity'] = giniimpurity(loan_second_m12['guaranteeForm']) #近12个月开立 贷款账户担保方式基尼不纯度
                dict_out['loan_second_m12_gf_crdit_count'] = loan_second_m12[loan_second_m12['guaranteeForm']=='信用/免担保'].shape[0] #近12个月开立 贷款账户担保方式为其信用/免担保 计数
                dict_out['loan_second_m12_gf_crdit_ratio'] = loan_second_m12[loan_second_m12['guaranteeForm']=='信用/免担保'].shape[0]/len(loan_second_m12) #近12个月开立 贷款账户担保方式为信用/免担保 占比
                dict_out['loan_second_m12_gf_other_count'] = loan_second_m12[loan_second_m12['guaranteeForm']=='其他'].shape[0] #近12个月开立 贷款账户担保方式为其他 计数
                dict_out['loan_second_m12_gf_other_ratio'] = loan_second_m12[loan_second_m12['guaranteeForm']=='其他'].shape[0]/len(loan_second_m12) #近12个月开立 贷款账户担保方式为其他 占比
                dict_out['loan_second_m12_gf_combine_nowarranty_count'] = loan_second_m12[loan_second_m12['guaranteeForm']=='组合（不含保证）'].shape[0] #近12个月开立 贷款账户担保方式为组合（不含保证） 计数
                dict_out['loan_second_m12_gf_combine_nowarranty_ratio'] = loan_second_m12[loan_second_m12['guaranteeForm']=='组合（不含保证）'].shape[0]/len(loan_second_m12) #近12个月开立 贷款账户担保方式为组合（不含保证） 占比
                dict_out['loan_second_m12_gf_combine_warranty_count'] = loan_second_m12[loan_second_m12['guaranteeForm']=='组合(含保证)'].shape[0] #近12个月开立 贷款账户担保方式为组合(含保证) 计数
                dict_out['loan_second_m12_gf_combine_warranty_ratio'] = loan_second_m12[loan_second_m12['guaranteeForm']=='组合(含保证)'].shape[0]/len(loan_second_m12) #近12个月开立 贷款账户担保方式为组合(含保证) 占比
                dict_out['loan_second_m12_gf_mortgage_count'] = loan_second_m12[loan_second_m12['guaranteeForm']=='抵押'].shape[0] #近12个月开立 贷款账户担保方式为抵押 计数
                dict_out['loan_second_m12_gf_mortgage_ratio'] = loan_second_m12[loan_second_m12['guaranteeForm']=='抵押'].shape[0]/len(loan_second_m12) #近12个月开立 贷款账户担保方式为抵押 占比
                dict_out['loan_second_m12_gf_warranty_count'] = loan_second_m12[loan_second_m12['guaranteeForm']=='保证'].shape[0] #近12个月开立 贷款账户担保方式为保证计数
                dict_out['loan_second_m12_gf_warranty_ratio'] = loan_second_m12[loan_second_m12['guaranteeForm']=='保证'].shape[0]/len(loan_second_m12) #近12个月开立 贷款账户担保方式为保证 占比
                dict_out['loan_second_m12_gf_pledge_count'] = loan_second_m12[loan_second_m12['guaranteeForm']=='质押'].shape[0] #近12个月开立 贷款账户担保方式为质押 计数
                dict_out['loan_second_m12_gf_pledge_ratio'] = loan_second_m12[loan_second_m12['guaranteeForm']=='质押'].shape[0]/len(loan_second_m12) #近12个月开立 贷款账户担保方式为质押 占比
                dict_out['loan_second_m12_gf_farm_group_count'] = loan_second_m12[loan_second_m12['guaranteeForm']=='农户联保'].shape[0] #近12个月开立 贷款账户担保方式为农户联保 计数
                dict_out['loan_second_m12_gf_farm_group_ratio'] = loan_second_m12[loan_second_m12['guaranteeForm']=='农户联保'].shape[0]/len(loan_second_m12) #近12个月开立 贷款账户担保方式为农户联保 占比

                dict_out['loan_second_m12_businessType_giniimpurity'] = giniimpurity(loan_second_m12['businessType']) #近12个月开立 贷款账户业务种类基尼不纯度
                dict_out['loan_second_m12_bt_other_person_count'] = loan_second_m12[loan_second_m12['businessType']=='其他个人消费贷款'].shape[0] #近12个月开立 贷款账户业务种类为其他个人消费贷款 计数
                dict_out['loan_second_m12_bt_other_person_ratio'] = loan_second_m12[loan_second_m12['businessType']=='其他个人消费贷款'].shape[0]/len(loan_second_m12) #近12个月开立 贷款账户业务种类为其他个人消费贷款 占比
                dict_out['loan_second_m12_bt_other_loan_count'] = loan_second_m12[loan_second_m12['businessType']=='其他贷款'].shape[0] #近12个月开立 贷款账户业务种类为其他贷款 计数
                dict_out['loan_second_m12_bt_other_loan_ratio'] = loan_second_m12[loan_second_m12['businessType']=='其他贷款'].shape[0]/len(loan_second_m12) #近12个月开立 贷款账户业务种类为其他贷款 占比
                dict_out['loan_second_m12_bt_person_business_count'] = loan_second_m12[loan_second_m12['businessType']=='个人经营性贷款'].shape[0] #近12个月开立 贷款账户业务种类为个人经营性贷款 计数
                dict_out['loan_second_m12_bt_person_business_ratio'] = loan_second_m12[loan_second_m12['businessType']=='个人经营性贷款'].shape[0]/len(loan_second_m12) #近12个月开立 贷款账户业务种类为个人经营性贷款 占比
                dict_out['loan_second_m12_bt_farm_loan_count'] = loan_second_m12[loan_second_m12['businessType']=='农户贷款'].shape[0] #近12个月开立 贷款账户业务种类为农户贷款 计数
                dict_out['loan_second_m12_bt_farm_loan_ratio'] = loan_second_m12[loan_second_m12['businessType']=='农户贷款'].shape[0]/len(loan_second_m12) #近12个月开立 贷款账户业务种类为农户贷款 占比
                dict_out['loan_second_m12_bt_person_car_count'] = loan_second_m12[loan_second_m12['businessType']=='个人汽车消费贷款'].shape[0] #近12个月开立 贷款账户业务种类为个人汽车消费贷款 计数
                dict_out['loan_second_m12_bt_person_car_ratio'] = loan_second_m12[loan_second_m12['businessType']=='个人汽车消费贷款'].shape[0]/len(loan_second_m12) #近12个月开立 贷款账户业务种类为个人汽车消费贷款 占比
                dict_out['loan_second_m12_bt_person_study_count'] = loan_second_m12[loan_second_m12['businessType']=='个人助学贷款'].shape[0] #近12个月开立 贷款账户业务种类为个人助学贷款 计数
                dict_out['loan_second_m12_bt_person_study_ratio'] = loan_second_m12[loan_second_m12['businessType']=='个人助学贷款'].shape[0]/len(loan_second_m12) #近12个月开立 贷款账户业务种类为个人助学贷款 占比
                dict_out['loan_second_m12_bt_house_commercial_count'] = loan_second_m12[loan_second_m12['businessType']=='个人住房商业贷款'].shape[0] #近12个月开立 贷款账户业务种类为个人住房商业贷款 计数
                dict_out['loan_second_m12_bt_house_commercial_ratio'] = loan_second_m12[loan_second_m12['businessType']=='个人住房商业贷款'].shape[0]/len(loan_second_m12) #近12个月开立 贷款账户业务种类为个人住房商业贷款 占比
                dict_out['loan_second_m12_bt_finance_lease_count'] = loan_second_m12[loan_second_m12['businessType']=='融资租赁业务'].shape[0] #近12个月开立 贷款账户业务种类为融资租赁业务 计数
                dict_out['loan_second_m12_bt_finance_lease_ratio'] = loan_second_m12[loan_second_m12['businessType']=='融资租赁业务'].shape[0]/len(loan_second_m12) #近12个月开立 贷款账户业务种类为融资租赁业务 占比
                dict_out['loan_second_m12_bt_house_fund_count'] = loan_second_m12[loan_second_m12['businessType']=='个人住房公积金贷款'].shape[0] #近12个月开立 贷款账户业务种类为个人住房公积金贷款 计数
                dict_out['loan_second_m12_bt_house_fund_ratio'] = loan_second_m12[loan_second_m12['businessType']=='个人住房公积金贷款'].shape[0]/len(loan_second_m12) #近12个月开立 贷款账户业务种类为个人住房公积金贷款 占比
                dict_out['loan_second_m12_bt_person_house_count'] = loan_second_m12[loan_second_m12['businessType']=='个人商用房（含商住两用）贷款'].shape[0] #近12个月开立 贷款账户业务种类为个人商用房（含商住两用）贷款 计数
                dict_out['loan_second_m12_bt_person_house_ratio'] = loan_second_m12[loan_second_m12['businessType']=='个人商用房（含商住两用）贷款'].shape[0]/len(loan_second_m12) #近12个月开立 贷款账户业务种类为个人商用房（含商住两用）贷款 占比
                dict_out['loan_second_m12_bt_stock_pledge_count'] = loan_second_m12[loan_second_m12['businessType']=='股票质押式回购交易'].shape[0] #近12个月开立 贷款账户业务种类为股票质押式回购交易计数
                dict_out['loan_second_m12_bt_stock_pledge_ratio'] = loan_second_m12[loan_second_m12['businessType']=='股票质押式回购交易'].shape[0]/len(loan_second_m12) #近12个月开立 贷款账户业务种类为股票质押式回购交易 占比



            #近12个月开立 现行
            loan_second_m12_now = loan_second_m12[loan_second_m12.is_now==1]
            if len(loan_second_m12_now)>0:
                dict_out['loan_second_m12_now_repayAmt_max'] = loan_second_m12_now['repayAmt'].max()#var最大值
                dict_out['loan_second_m12_now_startDate_to_report_mean'] = loan_second_m12_now['startDate_to_report'].mean() #var平均值
                dict_out['loan_second_m12_now_repayAmt_mean'] = loan_second_m12_now['repayAmt'].mean() #var平均值
                dict_out['loan_second_m12_now_RepayedAmount_ratio_max'] = loan_second_m12_now['RepayedAmount_ratio'].max()#var最大值
                dict_out['loan_second_m12_now_repayMons_ratio_mean'] = loan_second_m12_now['repayMons_ratio'].mean() #var平均值
                dict_out['loan_second_m12_now_org_consumer_finance_ratio'] = loan_second_m12_now[loan_second_m12_now['org']=='消费金融公司'].shape[0]/len(loan_second_m12_now) #近12个月开立 现行贷款账户管理机构为消费金融公司 占比
                dict_out['loan_second_m12_now_month60_State_countN_max'] = loan_second_m12_now['month60_State_countN'].max()#var最大值
                dict_out['loan_second_m12_now_month60_State_countUnknowr_mean'] = loan_second_m12_now['month60_State_countUnknowr'].mean() #var平均值
                dict_out['loan_second_m12_now_repayTerm_ratio_min'] = loan_second_m12_now['repayTerm_ratio'].min() #var最小值
                dict_out['loan_second_m12_now_repayedAmount_mean'] = loan_second_m12_now['repayedAmount'].mean() #var平均值
                dict_out['loan_second_m12_now_repayAmt_min'] = loan_second_m12_now['repayAmt'].min() #var最小值
                dict_out['loan_second_m12_now_repayedAmount_min'] = loan_second_m12_now['repayedAmount'].min() #var最小值
                dict_out['loan_second_m12_now_month60_Amount_num_mean_sum'] = loan_second_m12_now['month60_Amount_num_mean'].sum() #var求和
                dict_out['loan_second_m12_now_leftRepayTerms_max'] = loan_second_m12_now['leftRepayTerms'].max()#var最大值
                dict_out['loan_second_m12_now_leftRepayTerms_min'] = loan_second_m12_now['leftRepayTerms'].min() #var最小值
                dict_out['loan_second_m12_now_leftRepayTerms_range'] = (dict_out['loan_second_m12_now_leftRepayTerms_max']-dict_out['loan_second_m12_now_leftRepayTerms_min'])/dict_out['loan_second_m12_now_leftRepayTerms_max'] if dict_out['loan_second_m12_now_leftRepayTerms_max']>0 else np.nan #var区间率
                dict_out['loan_second_m12_now_repayTerm_ratio_mean'] = loan_second_m12_now['repayTerm_ratio'].mean() #var平均值
                dict_out['loan_second_m12_now_RepayedAmount_ratio_max'] = loan_second_m12_now['RepayedAmount_ratio'].max()#var最大值
                dict_out['loan_second_m12_now_RepayedAmount_ratio_min'] = loan_second_m12_now['RepayedAmount_ratio'].min() #var最小值
                dict_out['loan_second_m12_now_RepayedAmount_ratio_range'] = (dict_out['loan_second_m12_now_RepayedAmount_ratio_max']-dict_out['loan_second_m12_now_RepayedAmount_ratio_min'])/dict_out['loan_second_m12_now_RepayedAmount_ratio_max'] if dict_out['loan_second_m12_now_RepayedAmount_ratio_max']>0 else np.nan #var区间率
                dict_out['loan_second_m12_now_month60_Amount_num_mean_max'] = loan_second_m12_now['month60_Amount_num_mean'].max()#var最大值
                dict_out['loan_second_m12_now_gf_crdit_ratio'] = loan_second_m12_now[loan_second_m12_now['guaranteeForm']=='信用/免担保'].shape[0]/len(loan_second_m12_now) #近12个月开立 现行贷款账户担保方式为信用/免担保 占比
                dict_out['loan_second_m12_now_month60_to_report_min_sum'] = loan_second_m12_now['month60_to_report_min'].sum() #var求和
                dict_out['loan_second_m12_now_rf_other_count'] = loan_second_m12_now[loan_second_m12_now['repayFrequency']=='其他'].shape[0] #近12个月开立 现行贷款账户还款频率为其他 计数
                dict_out['loan_second_m12_now_loanAmount_max'] = loan_second_m12_now['loanAmount'].max()#var最大值
                dict_out['loan_second_m12_now_loanAmount_min'] = loan_second_m12_now['loanAmount'].min() #var最小值
                dict_out['loan_second_m12_now_loanAmount_range'] = (dict_out['loan_second_m12_now_loanAmount_max']-dict_out['loan_second_m12_now_loanAmount_min'])/dict_out['loan_second_m12_now_loanAmount_max'] if dict_out['loan_second_m12_now_loanAmount_max']>0 else np.nan #var区间率
                dict_out['loan_second_m12_now_month60_State_count1r_sum'] = loan_second_m12_now['month60_State_count1r'].sum() #var求和
                dict_out['loan_second_m12_now_month60_State_countNullr_mean'] = loan_second_m12_now['month60_State_countNullr'].mean() #var平均值
                dict_out['loan_second_m12_now_byDate_to_report_mean'] = loan_second_m12_now['byDate_to_report'].mean() #var平均值
                dict_out['loan_second_m12_now_month60_State_num_size_mean'] = loan_second_m12_now['month60_State_num_size'].mean() #var平均值
                dict_out['loan_second_m12_now_repayTerms_max'] = loan_second_m12_now['repayTerms'].max()#var最大值
                dict_out['loan_second_m12_now_repayTerms_min'] = loan_second_m12_now['repayTerms'].min() #var最小值
                dict_out['loan_second_m12_now_repayTerms_range'] = (dict_out['loan_second_m12_now_repayTerms_max']-dict_out['loan_second_m12_now_repayTerms_min'])/dict_out['loan_second_m12_now_repayTerms_max'] if dict_out['loan_second_m12_now_repayTerms_max']>0 else np.nan #var区间率
                dict_out['loan_second_m12_now_byDate_to_report_sum'] = loan_second_m12_now['byDate_to_report'].sum() #var求和
                dict_out['loan_second_m12_now_businessType_giniimpurity'] = giniimpurity(loan_second_m12_now['businessType']) #近12个月开立 现行贷款账户业务种类基尼不纯度
                dict_out['loan_second_m12_now_balance_ratio_mean'] = loan_second_m12_now['balance_ratio'].mean() #var平均值
                dict_out['loan_second_m12_now_rf_other_ratio'] = loan_second_m12_now[loan_second_m12_now['repayFrequency']=='其他'].shape[0]/len(loan_second_m12_now) #近12个月开立 现行贷款账户还款频率为其他 占比
                dict_out['loan_second_m12_now_gf_combine_warranty_count'] = loan_second_m12_now[loan_second_m12_now['guaranteeForm']=='组合(含保证)'].shape[0] #近12个月开立 现行贷款账户担保方式为组合(含保证) 计数
                dict_out['loan_second_m12_now_month60_State_countN_mean'] = loan_second_m12_now['month60_State_countN'].mean() #var平均值
                dict_out['loan_second_m12_now_month60_State_countN_max'] = loan_second_m12_now['month60_State_countN'].max()#var最大值
                dict_out['loan_second_m12_now_month60_State_countN_min'] = loan_second_m12_now['month60_State_countN'].min() #var最小值
                dict_out['loan_second_m12_now_month60_State_countN_range'] = (dict_out['loan_second_m12_now_month60_State_countN_max']-dict_out['loan_second_m12_now_month60_State_countN_min'])/dict_out['loan_second_m12_now_month60_State_countN_max'] if dict_out['loan_second_m12_now_month60_State_countN_max']>0 else np.nan #var区间率
                dict_out['loan_second_m12_now_repayedAmount_max'] = loan_second_m12_now['repayedAmount'].max()#var最大值
                dict_out['loan_second_m12_now_repayedAmount_min'] = loan_second_m12_now['repayedAmount'].min() #var最小值
                dict_out['loan_second_m12_now_repayedAmount_range'] = (dict_out['loan_second_m12_now_repayedAmount_max']-dict_out['loan_second_m12_now_repayedAmount_min'])/dict_out['loan_second_m12_now_repayedAmount_max'] if dict_out['loan_second_m12_now_repayedAmount_max']>0 else np.nan #var区间率
                dict_out['loan_second_m12_now_repayMons_ratio_max'] = loan_second_m12_now['repayMons_ratio'].max()#var最大值
                dict_out['loan_second_m12_now_repayMons_ratio_min'] = loan_second_m12_now['repayMons_ratio'].min() #var最小值
                dict_out['loan_second_m12_now_repayMons_ratio_range'] = (dict_out['loan_second_m12_now_repayMons_ratio_max']-dict_out['loan_second_m12_now_repayMons_ratio_min'])/dict_out['loan_second_m12_now_repayMons_ratio_max'] if dict_out['loan_second_m12_now_repayMons_ratio_max']>0 else np.nan #var区间率
                dict_out['loan_second_m12_now_bt_person_business_ratio'] = loan_second_m12_now[loan_second_m12_now['businessType']=='个人经营性贷款'].shape[0]/len(loan_second_m12_now) #近12个月开立 现行贷款账户业务种类为个人经营性贷款 占比
                dict_out['loan_second_m12_now_repayMons_max'] = loan_second_m12_now['repayMons'].max()#var最大值
                dict_out['loan_second_m12_now_repayMons_min'] = loan_second_m12_now['repayMons'].min() #var最小值
                dict_out['loan_second_m12_now_repayMons_range'] = (dict_out['loan_second_m12_now_repayMons_max']-dict_out['loan_second_m12_now_repayMons_min'])/dict_out['loan_second_m12_now_repayMons_max'] if dict_out['loan_second_m12_now_repayMons_max']>0 else np.nan #var区间率
                dict_out['loan_second_m12_now_month60_State_countNr_sum'] = loan_second_m12_now['month60_State_countNr'].sum() #var求和
                dict_out['loan_second_m12_now_gf_other_ratio'] = loan_second_m12_now[loan_second_m12_now['guaranteeForm']=='其他'].shape[0]/len(loan_second_m12_now) #近12个月开立 现行贷款账户担保方式为其他 占比
                dict_out['loan_second_m12_now_bt_other_person_ratio'] = loan_second_m12_now[loan_second_m12_now['businessType']=='其他个人消费贷款'].shape[0]/len(loan_second_m12_now) #近12个月开立 现行贷款账户业务种类为其他个人消费贷款 占比
                dict_out['loan_second_m12_now_org_consumer_finance_ratio'] = loan_second_m12_now[loan_second_m12_now['org']=='消费金融公司'].shape[0]/len(loan_second_m12_now) #近12个月开立 现行贷款账户管理机构为消费金融公司 占比
                dict_out['loan_second_m12_now_gf_crdit_ratio'] = loan_second_m12_now[loan_second_m12_now['guaranteeForm']=='信用/免担保'].shape[0]/len(loan_second_m12_now) #近12个月开立 现行贷款账户担保方式为信用/免担保 占比
                dict_out['loan_second_m12_now_rf_once_ratio'] = loan_second_m12_now[loan_second_m12_now['repayFrequency']=='一次性'].shape[0]/len(loan_second_m12_now) #近12个月开立 现行贷款账户还款频率为一次性 占比
                dict_out['loan_second_m12_now_org_commercial_bank_ratio'] = loan_second_m12_now[loan_second_m12_now['org']=='商业银行'].shape[0]/len(loan_second_m12_now) #近12个月开立 现行贷款账户管理机构为商业银行 占比
                dict_out['loan_second_m12_now_businessType_giniimpurity'] = giniimpurity(loan_second_m12_now['businessType']) #近12个月开立 现行贷款账户业务种类基尼不纯度
                dict_out['loan_second_m12_now_org_trust_company_ratio'] = loan_second_m12_now[loan_second_m12_now['org']=='信托公司'].shape[0]/len(loan_second_m12_now) #近12个月开立 现行贷款账户管理机构为信托公司 占比
                dict_out['loan_second_m12_now_org_consumer_finance_count'] = loan_second_m12_now[loan_second_m12_now['org']=='消费金融公司'].shape[0] #近12个月开立 现行贷款账户管理机构为消费金融公司 计数
                dict_out['loan_second_m12_now_org_micro_loan_ratio'] = loan_second_m12_now[loan_second_m12_now['org']=='小额贷款公司'].shape[0]/len(loan_second_m12_now) #近12个月开立 现行贷款账户管理机构为小额贷款公司 占比
                dict_out['loan_second_m12_now_gf_crdit_count'] = loan_second_m12_now[loan_second_m12_now['guaranteeForm']=='信用/免担保'].shape[0] #近12个月开立 现行贷款账户担保方式为其信用/免担保 计数
                dict_out['loan_second_m12_now_rf_month_ratio'] = loan_second_m12_now[loan_second_m12_now['repayFrequency']=='月'].shape[0]/len(loan_second_m12_now) #近12个月开立 现行贷款账户还款频率为月 占比
                dict_out['loan_second_m12_now_rf_other_count'] = loan_second_m12_now[loan_second_m12_now['repayFrequency']=='其他'].shape[0] #近12个月开立 现行贷款账户还款频率为其他 计数
                dict_out['loan_second_m12_now_month60_State_countNullr_mean'] = loan_second_m12_now['month60_State_countNullr'].mean()  #var平均值比率
                dict_out['loan_second_m12_now_month60_to_report_min_mean'] = loan_second_m12_now['month60_to_report_min'].mean()  #var平均值比率
                dict_out['loan_second_m12_now_repayMons_ratio_mean'] = loan_second_m12_now['repayMons_ratio'].mean()  #var平均值比率
                dict_out['loan_second_m12_now_month60_to_report_mean_max'] = loan_second_m12_now['month60_to_report_mean'].max()#var最大值
                dict_out['loan_second_m12_now_month60_to_report_mean_min'] = loan_second_m12_now['month60_to_report_mean'].min() #var最小值比率
                dict_out['loan_second_m12_now_month60_to_report_mean_range'] = (dict_out['loan_second_m12_now_month60_to_report_mean_max']-dict_out['loan_second_m12_now_month60_to_report_mean_min'])/dict_out['loan_second_m12_now_month60_to_report_mean_max'] if dict_out['loan_second_m12_now_month60_to_report_mean_max']>0 else np.nan #var区间率
                dict_out['loan_second_m12_now_month60_State_countNullr_sum'] = loan_second_m12_now['month60_State_countNullr'].sum() #var求和比率
                dict_out['loan_second_m12_now_month60_State_num_size_sum'] = loan_second_m12_now['month60_State_num_size'].sum() #var求和比率
                dict_out['loan_second_m12_now_planRepayAmount_mean'] = loan_second_m12_now['planRepayAmount'].mean()  #var平均值比率
                dict_out['loan_second_m12_now_month60_State_countUnknowr_mean'] = loan_second_m12_now['month60_State_countUnknowr'].mean()  #var平均值比率
                dict_out['loan_second_m12_now_repayedAmount_mean'] = loan_second_m12_now['repayedAmount'].mean()  #var平均值比率
                dict_out['loan_second_m12_now_repayMons_ratio_min'] = loan_second_m12_now['repayMons_ratio'].min() #var最小值比率
                dict_out['loan_second_m12_now_month60_State_num_size_max'] = loan_second_m12_now['month60_State_num_size'].max()#var最大值
                dict_out['loan_second_m12_now_month60_State_num_size_min'] = loan_second_m12_now['month60_State_num_size'].min() #var最小值比率
                dict_out['loan_second_m12_now_month60_State_num_size_range'] = (dict_out['loan_second_m12_now_month60_State_num_size_max']-dict_out['loan_second_m12_now_month60_State_num_size_min'])/dict_out['loan_second_m12_now_month60_State_num_size_max'] if dict_out['loan_second_m12_now_month60_State_num_size_max']>0 else np.nan #var区间率
                dict_out['loan_second_m12_now_byDate_to_report_sum'] = loan_second_m12_now['byDate_to_report'].sum() #var求和比率
                dict_out['loan_second_m12_now_month60_State_countUnknow_mean'] = loan_second_m12_now['month60_State_countUnknow'].mean()  #var平均值比率
                dict_out['loan_second_m12_now_balance_ratio_max'] = loan_second_m12_now['balance_ratio'].max()#var最大值
                dict_out['loan_second_m12_now_balance_ratio_min'] = loan_second_m12_now['balance_ratio'].min() #var最小值比率
                dict_out['loan_second_m12_now_balance_ratio_range'] = (dict_out['loan_second_m12_now_balance_ratio_max']-dict_out['loan_second_m12_now_balance_ratio_min'])/dict_out['loan_second_m12_now_balance_ratio_max'] if dict_out['loan_second_m12_now_balance_ratio_max']>0 else np.nan #var区间率
                dict_out['loan_second_m12_now_repayMons_ratio_max'] = loan_second_m12_now['repayMons_ratio'].max()#var最大值
                dict_out['loan_second_m12_now_repayMons_ratio_min'] = loan_second_m12_now['repayMons_ratio'].min() #var最小值比率
                dict_out['loan_second_m12_now_repayMons_ratio_range'] = (dict_out['loan_second_m12_now_repayMons_ratio_max']-dict_out['loan_second_m12_now_repayMons_ratio_min'])/dict_out['loan_second_m12_now_repayMons_ratio_max'] if dict_out['loan_second_m12_now_repayMons_ratio_max']>0 else np.nan #var区间率
                dict_out['loan_second_m12_now_repayedAmount_max'] = loan_second_m12_now['repayedAmount'].max()#var最大值
                dict_out['loan_second_m12_now_repayedAmount_min'] = loan_second_m12_now['repayedAmount'].min() #var最小值比率
                dict_out['loan_second_m12_now_repayedAmount_range'] = (dict_out['loan_second_m12_now_repayedAmount_max']-dict_out['loan_second_m12_now_repayedAmount_min'])/dict_out['loan_second_m12_now_repayedAmount_max'] if dict_out['loan_second_m12_now_repayedAmount_max']>0 else np.nan #var区间率
                dict_out['loan_second_m12_now_repayAmt_max'] = loan_second_m12_now['repayAmt'].max() #var最大值比率
                dict_out['loan_second_m12_now_month60_State_countN_sum'] = loan_second_m12_now['month60_State_countN'].sum() #var求和比率
                dict_out['loan_second_m12_now_loanAmount_min'] = loan_second_m12_now['loanAmount'].min() #var最小值比率
                dict_out['loan_second_m12_now_repayTerms_mean'] = loan_second_m12_now['repayTerms'].mean()  #var平均值比率
                dict_out['loan_second_m12_now_repayTerms_sum'] = loan_second_m12_now['repayTerms'].sum() #var求和比率
                dict_out['loan_second_m12_now_month60_State_countNull_mean'] = loan_second_m12_now['month60_State_countNull'].mean()  #var平均值比率
                dict_out['loan_second_m12_now_repayAmt_mean'] = loan_second_m12_now['repayAmt'].mean()  #var平均值比率
                dict_out['loan_second_m12_now_repayMons_ratio_max'] = loan_second_m12_now['repayMons_ratio'].max() #var最大值比率
                dict_out['loan_second_m12_now_month60_State_countNr_mean'] = loan_second_m12_now['month60_State_countNr'].mean()  #var平均值比率
                dict_out['loan_second_m12_nowR_month60_State_countNullr_mean'] = dict_out['loan_second_m12_now_month60_State_countNullr_mean']/dict_out['loan_second_m12_month60_State_countNullr_mean'] if dict_out['loan_second_m12_month60_State_countNullr_mean']>0 else np.nan #var最大值比率
                dict_out['loan_second_m12_nowR_month60_to_report_min_mean'] = dict_out['loan_second_m12_now_month60_to_report_min_mean']/dict_out['loan_second_m12_month60_to_report_min_mean'] if dict_out['loan_second_m12_month60_to_report_min_mean']>0 else np.nan #var最大值比率
                dict_out['loan_second_m12_nowR_repayMons_ratio_mean'] = dict_out['loan_second_m12_now_repayMons_ratio_mean']/dict_out['loan_second_m12_repayMons_ratio_mean'] if dict_out['loan_second_m12_repayMons_ratio_mean']>0 else np.nan #var最大值比率
                dict_out['loan_second_m12_nowR_month60_to_report_mean_range'] = dict_out['loan_second_m12_now_month60_to_report_mean_range']/dict_out['loan_second_m12_month60_to_report_mean_range'] if dict_out['loan_second_m12_month60_to_report_mean_range']>0 else np.nan #var最大值比率
                dict_out['loan_second_m12_now_org_consumer_finance_ratio'] = loan_second_m12_now[loan_second_m12_now['org']=='消费金融公司'].shape[0]/len(loan_second_m12_now) #近12个月开立 现行贷款账户管理机构为消费金融公司 占比
                dict_out['loan_second_m12_nowR_org_consumer_finance_ratio'] = dict_out['loan_second_m12_now_org_consumer_finance_ratio']/dict_out['loan_second_m12_org_consumer_finance_ratio'] if dict_out['loan_second_m12_org_consumer_finance_ratio']>0 else np.nan #var最大值比率
                dict_out['loan_second_m12_nowR_month60_State_countNullr_sum'] = dict_out['loan_second_m12_now_month60_State_countNullr_sum']/dict_out['loan_second_m12_month60_State_countNullr_sum'] if dict_out['loan_second_m12_month60_State_countNullr_sum']>0 else np.nan #var最大值比率
                dict_out['loan_second_m12_nowR_month60_State_num_size_sum'] = dict_out['loan_second_m12_now_month60_State_num_size_sum']/dict_out['loan_second_m12_month60_State_num_size_sum'] if dict_out['loan_second_m12_month60_State_num_size_sum']>0 else np.nan #var最大值比率
                dict_out['loan_second_m12_now_gf_crdit_ratio'] = loan_second_m12_now[loan_second_m12_now['guaranteeForm']=='信用/免担保'].shape[0]/len(loan_second_m12_now) #近12个月开立 现行贷款账户担保方式为信用/免担保 占比
                dict_out['loan_second_m12_nowR_gf_crdit_ratio'] = dict_out['loan_second_m12_now_gf_crdit_ratio']/dict_out['loan_second_m12_gf_crdit_ratio'] if dict_out['loan_second_m12_gf_crdit_ratio']>0 else np.nan #var最大值比率
                dict_out['loan_second_m12_now_rf_once_ratio'] = loan_second_m12_now[loan_second_m12_now['repayFrequency']=='一次性'].shape[0]/len(loan_second_m12_now) #近12个月开立 现行贷款账户还款频率为一次性 占比
                dict_out['loan_second_m12_nowR_rf_once_ratio'] = dict_out['loan_second_m12_now_rf_once_ratio']/dict_out['loan_second_m12_rf_once_ratio'] if dict_out['loan_second_m12_rf_once_ratio']>0 else np.nan #var最大值比率
                dict_out['loan_second_m12_now_org_commercial_bank_ratio'] = loan_second_m12_now[loan_second_m12_now['org']=='商业银行'].shape[0]/len(loan_second_m12_now) #近12个月开立 现行贷款账户管理机构为商业银行 占比
                dict_out['loan_second_m12_nowR_org_commercial_bank_ratio'] = dict_out['loan_second_m12_now_org_commercial_bank_ratio']/dict_out['loan_second_m12_org_commercial_bank_ratio'] if dict_out['loan_second_m12_org_commercial_bank_ratio']>0 else np.nan #var最大值比率
                dict_out['loan_second_m12_nowR_planRepayAmount_mean'] = dict_out['loan_second_m12_now_planRepayAmount_mean']/dict_out['loan_second_m12_planRepayAmount_mean'] if dict_out['loan_second_m12_planRepayAmount_mean']>0 else np.nan #var最大值比率
                dict_out['loan_second_m12_nowR_month60_State_countUnknowr_mean'] = dict_out['loan_second_m12_now_month60_State_countUnknowr_mean']/dict_out['loan_second_m12_month60_State_countUnknowr_mean'] if dict_out['loan_second_m12_month60_State_countUnknowr_mean']>0 else np.nan #var最大值比率
                dict_out['loan_second_m12_now_businessType_giniimpurity'] = giniimpurity(loan_second_m12_now['businessType']) #近12个月开立 现行贷款账户业务种类基尼不纯度
                dict_out['loan_second_m12_nowR_businessType_giniimpurity'] = dict_out['loan_second_m12_now_businessType_giniimpurity']/dict_out['loan_second_m12_businessType_giniimpurity'] if dict_out['loan_second_m12_businessType_giniimpurity']>0 else np.nan #var最大值比率
                dict_out['loan_second_m12_nowR_repayedAmount_mean'] = dict_out['loan_second_m12_now_repayedAmount_mean']/dict_out['loan_second_m12_repayedAmount_mean'] if dict_out['loan_second_m12_repayedAmount_mean']>0 else np.nan #var最大值比率
                dict_out['loan_second_m12_nowR_repayMons_ratio_min'] = dict_out['loan_second_m12_now_repayMons_ratio_min']/dict_out['loan_second_m12_repayMons_ratio_min'] if dict_out['loan_second_m12_repayMons_ratio_min']>0 else np.nan #var最大值比率
                dict_out['loan_second_m12_nowR_month60_State_num_size_range'] = dict_out['loan_second_m12_now_month60_State_num_size_range']/dict_out['loan_second_m12_month60_State_num_size_range'] if dict_out['loan_second_m12_month60_State_num_size_range']>0 else np.nan #var最大值比率
                dict_out['loan_second_m12_nowR_byDate_to_report_sum'] = dict_out['loan_second_m12_now_byDate_to_report_sum']/dict_out['loan_second_m12_byDate_to_report_sum'] if dict_out['loan_second_m12_byDate_to_report_sum']>0 else np.nan #var最大值比率
                dict_out['loan_second_m12_now_org_trust_company_ratio'] = loan_second_m12_now[loan_second_m12_now['org']=='信托公司'].shape[0]/len(loan_second_m12_now) #近12个月开立 现行贷款账户管理机构为信托公司 占比
                dict_out['loan_second_m12_nowR_org_trust_company_ratio'] = dict_out['loan_second_m12_now_org_trust_company_ratio']/dict_out['loan_second_m12_org_trust_company_ratio'] if dict_out['loan_second_m12_org_trust_company_ratio']>0 else np.nan #var最大值比率
                dict_out['loan_second_m12_nowR_month60_State_countUnknow_mean'] = dict_out['loan_second_m12_now_month60_State_countUnknow_mean']/dict_out['loan_second_m12_month60_State_countUnknow_mean'] if dict_out['loan_second_m12_month60_State_countUnknow_mean']>0 else np.nan #var最大值比率
                dict_out['loan_second_m12_now_org_consumer_finance_count'] = loan_second_m12_now[loan_second_m12_now['org']=='消费金融公司'].shape[0] #近12个月开立 现行贷款账户管理机构为消费金融公司 计数
                dict_out['loan_second_m12_nowR_org_consumer_finance_count'] = dict_out['loan_second_m12_now_org_consumer_finance_count']/dict_out['loan_second_m12_org_consumer_finance_count'] if dict_out['loan_second_m12_org_consumer_finance_count']>0 else np.nan #var最大值比率
                dict_out['loan_second_m12_nowR_balance_ratio_range'] = dict_out['loan_second_m12_now_balance_ratio_range']/dict_out['loan_second_m12_balance_ratio_range'] if dict_out['loan_second_m12_balance_ratio_range']>0 else np.nan #var最大值比率
                dict_out['loan_second_m12_nowR_repayMons_ratio_range'] = dict_out['loan_second_m12_now_repayMons_ratio_range']/dict_out['loan_second_m12_repayMons_ratio_range'] if dict_out['loan_second_m12_repayMons_ratio_range']>0 else np.nan #var最大值比率
                dict_out['loan_second_m12_now_org_micro_loan_ratio'] = loan_second_m12_now[loan_second_m12_now['org']=='小额贷款公司'].shape[0]/len(loan_second_m12_now) #近12个月开立 现行贷款账户管理机构为小额贷款公司 占比
                dict_out['loan_second_m12_nowR_org_micro_loan_ratio'] = dict_out['loan_second_m12_now_org_micro_loan_ratio']/dict_out['loan_second_m12_org_micro_loan_ratio'] if dict_out['loan_second_m12_org_micro_loan_ratio']>0 else np.nan #var最大值比率
                dict_out['loan_second_m12_now_gf_crdit_count'] = loan_second_m12_now[loan_second_m12_now['guaranteeForm']=='信用/免担保'].shape[0] #近12个月开立 现行贷款账户担保方式为其信用/免担保 计数
                dict_out['loan_second_m12_nowR_gf_crdit_count'] = dict_out['loan_second_m12_now_gf_crdit_count']/dict_out['loan_second_m12_gf_crdit_count'] if dict_out['loan_second_m12_gf_crdit_count']>0 else np.nan #var最大值比率
                dict_out['loan_second_m12_nowR_repayedAmount_range'] = dict_out['loan_second_m12_now_repayedAmount_range']/dict_out['loan_second_m12_repayedAmount_range'] if dict_out['loan_second_m12_repayedAmount_range']>0 else np.nan #var最大值比率
                dict_out['loan_second_m12_nowR_repayAmt_max'] = dict_out['loan_second_m12_now_repayAmt_max']/dict_out['loan_second_m12_repayAmt_max'] if dict_out['loan_second_m12_repayAmt_max']>0 else np.nan #var最大值比率
                dict_out['loan_second_m12_now_rf_month_ratio'] = loan_second_m12_now[loan_second_m12_now['repayFrequency']=='月'].shape[0]/len(loan_second_m12_now) #近12个月开立 现行贷款账户还款频率为月 占比
                dict_out['loan_second_m12_nowR_rf_month_ratio'] = dict_out['loan_second_m12_now_rf_month_ratio']/dict_out['loan_second_m12_rf_month_ratio'] if dict_out['loan_second_m12_rf_month_ratio']>0 else np.nan #var最大值比率
                dict_out['loan_second_m12_nowR_month60_State_countN_sum'] = dict_out['loan_second_m12_now_month60_State_countN_sum']/dict_out['loan_second_m12_month60_State_countN_sum'] if dict_out['loan_second_m12_month60_State_countN_sum']>0 else np.nan #var最大值比率
                dict_out['loan_second_m12_nowR_loanAmount_min'] = dict_out['loan_second_m12_now_loanAmount_min']/dict_out['loan_second_m12_loanAmount_min'] if dict_out['loan_second_m12_loanAmount_min']>0 else np.nan #var最大值比率
                dict_out['loan_second_m12_nowR_repayTerms_mean'] = dict_out['loan_second_m12_now_repayTerms_mean']/dict_out['loan_second_m12_repayTerms_mean'] if dict_out['loan_second_m12_repayTerms_mean']>0 else np.nan #var最大值比率
                dict_out['loan_second_m12_nowR_repayTerms_sum'] = dict_out['loan_second_m12_now_repayTerms_sum']/dict_out['loan_second_m12_repayTerms_sum'] if dict_out['loan_second_m12_repayTerms_sum']>0 else np.nan #var最大值比率
                dict_out['loan_second_m12_nowR_month60_State_countNull_mean'] = dict_out['loan_second_m12_now_month60_State_countNull_mean']/dict_out['loan_second_m12_month60_State_countNull_mean'] if dict_out['loan_second_m12_month60_State_countNull_mean']>0 else np.nan #var最大值比率
                dict_out['loan_second_m12_nowR_repayAmt_mean'] = dict_out['loan_second_m12_now_repayAmt_mean']/dict_out['loan_second_m12_repayAmt_mean'] if dict_out['loan_second_m12_repayAmt_mean']>0 else np.nan #var最大值比率
                dict_out['loan_second_m12_nowR_repayMons_ratio_max'] = dict_out['loan_second_m12_now_repayMons_ratio_max']/dict_out['loan_second_m12_repayMons_ratio_max'] if dict_out['loan_second_m12_repayMons_ratio_max']>0 else np.nan #var最大值比率
                dict_out['loan_second_m12_nowR_month60_State_countNr_mean'] = dict_out['loan_second_m12_now_month60_State_countNr_mean']/dict_out['loan_second_m12_month60_State_countNr_mean'] if dict_out['loan_second_m12_month60_State_countNr_mean']>0 else np.nan #var最大值比率
                dict_out['loan_second_m12_now_rf_other_count'] = loan_second_m12_now[loan_second_m12_now['repayFrequency']=='其他'].shape[0] #近12个月开立 现行贷款账户还款频率为其他 计数
                dict_out['loan_second_m12_nowR_rf_other_count'] = dict_out['loan_second_m12_now_rf_other_count']/dict_out['loan_second_m12_rf_other_count'] if dict_out['loan_second_m12_rf_other_count']>0 else np.nan #var最大值比率

            #近12个月开立 非循环贷账户
            loan_second_m12_ncycle = loan_second_m12[loan_second_m12['class']=='非循环贷账户']
            if len(loan_second_m12_ncycle)>0:
                dict_out['loan_second_m12_ncycle_bt_other_loan_ratio'] = loan_second_m12_ncycle[loan_second_m12_ncycle['businessType']=='其他贷款'].shape[0]/len(loan_second_m12_ncycle) #近12个月开立 非循环贷账户 贷款账户业务种类为其他贷款 占比
                dict_out['loan_second_m12_ncycle_loanGrantOrg_nunique'] = loan_second_m12_ncycle['loanGrantOrg'].nunique() #近12个月开立 非循环贷账户 贷款账户管理机构详细nunique
                dict_out['loan_second_m12_ncycle_gf_combine_nowarranty_ratio'] = loan_second_m12_ncycle[loan_second_m12_ncycle['guaranteeForm']=='组合（不含保证）'].shape[0]/len(loan_second_m12_ncycle) #近12个月开立 非循环贷账户 贷款账户担保方式为组合（不含保证） 占比
                dict_out['loan_second_m12_ncycle_rf_other_count'] = loan_second_m12_ncycle[loan_second_m12_ncycle['repayFrequency']=='其他'].shape[0] #近12个月开立 非循环贷账户 贷款账户还款频率为其他 计数
                dict_out['loan_second_m12_ncycle_repayedAmount_sum'] = loan_second_m12_ncycle['repayedAmount'].sum() #var求和
                dict_out['loan_second_m12_ncycle_org_micro_loan_ratio'] = loan_second_m12_ncycle[loan_second_m12_ncycle['org']=='小额贷款公司'].shape[0]/len(loan_second_m12_ncycle) #近12个月开立 非循环贷账户 贷款账户管理机构为小额贷款公司 占比
                dict_out['loan_second_m12_ncycle_org_trust_company_ratio'] = loan_second_m12_ncycle[loan_second_m12_ncycle['org']=='信托公司'].shape[0]/len(loan_second_m12_ncycle) #近12个月开立 非循环贷账户 贷款账户管理机构为信托公司 占比
                dict_out['loan_second_m12_ncycle_org_trust_company_count'] = loan_second_m12_ncycle[loan_second_m12_ncycle['org']=='信托公司'].shape[0] #近12个月开立 非循环贷账户 贷款账户管理机构为信托公司 计数
                dict_out['loan_second_m12_ncycle_month60_Amount_num_mean_mean'] = loan_second_m12_ncycle['month60_Amount_num_mean'].mean() #var平均值
                dict_out['loan_second_m12_ncycle_balance_ratio_max'] = loan_second_m12_ncycle['balance_ratio'].max()#var最大值
                dict_out['loan_second_m12_ncycle_startDate_to_report_sum'] = loan_second_m12_ncycle['startDate_to_report'].sum() #var求和
                dict_out['loan_second_m12_ncycle_org_consumer_finance_count'] = loan_second_m12_ncycle[loan_second_m12_ncycle['org']=='消费金融公司'].shape[0] #近12个月开立 非循环贷账户 贷款账户管理机构为消费金融公司 计数
                dict_out['loan_second_m12_ncycle_repayAmt_max'] = loan_second_m12_ncycle['repayAmt'].max()#var最大值
                dict_out['loan_second_m12_ncycle_repayAmt_min'] = loan_second_m12_ncycle['repayAmt'].min() #var最小值
                dict_out['loan_second_m12_ncycle_repayAmt_range'] = (dict_out['loan_second_m12_ncycle_repayAmt_max']-dict_out['loan_second_m12_ncycle_repayAmt_min'])/dict_out['loan_second_m12_ncycle_repayAmt_max'] if dict_out['loan_second_m12_ncycle_repayAmt_max']>0 else np.nan #var区间率
                dict_out['loan_second_m12_ncycle_byDate_to_report_mean'] = loan_second_m12_ncycle['byDate_to_report'].mean() #var平均值
                dict_out['loan_second_m12_ncycle_RepayedAmount_ratio_max'] = loan_second_m12_ncycle['RepayedAmount_ratio'].max()#var最大值
                dict_out['loan_second_m12_ncycle_RepayedAmount_ratio_max'] = loan_second_m12_ncycle['RepayedAmount_ratio'].max()#var最大值
                dict_out['loan_second_m12_ncycle_RepayedAmount_ratio_min'] = loan_second_m12_ncycle['RepayedAmount_ratio'].min() #var最小值
                dict_out['loan_second_m12_ncycle_RepayedAmount_ratio_range'] = (dict_out['loan_second_m12_ncycle_RepayedAmount_ratio_max']-dict_out['loan_second_m12_ncycle_RepayedAmount_ratio_min'])/dict_out['loan_second_m12_ncycle_RepayedAmount_ratio_max'] if dict_out['loan_second_m12_ncycle_RepayedAmount_ratio_max']>0 else np.nan #var区间率
                dict_out['loan_second_m12_ncycle_balance_ratio_mean'] = loan_second_m12_ncycle['balance_ratio'].mean() #var平均值
                dict_out['loan_second_m12_ncycle_RepayedAmount_ratio_min'] = loan_second_m12_ncycle['RepayedAmount_ratio'].min() #var最小值
                dict_out['loan_second_m12_ncycle_leftRepayTerms_max'] = loan_second_m12_ncycle['leftRepayTerms'].max()#var最大值
                dict_out['loan_second_m12_ncycle_repayAmt_min'] = loan_second_m12_ncycle['repayAmt'].min() #var最小值
                dict_out['loan_second_m12_ncycle_balance_ratio_max'] = loan_second_m12_ncycle['balance_ratio'].max()#var最大值
                dict_out['loan_second_m12_ncycle_balance_ratio_min'] = loan_second_m12_ncycle['balance_ratio'].min() #var最小值
                dict_out['loan_second_m12_ncycle_balance_ratio_range'] = (dict_out['loan_second_m12_ncycle_balance_ratio_max']-dict_out['loan_second_m12_ncycle_balance_ratio_min'])/dict_out['loan_second_m12_ncycle_balance_ratio_max'] if dict_out['loan_second_m12_ncycle_balance_ratio_max']>0 else np.nan #var区间率
                dict_out['loan_second_m12_ncycle_month60_State_num_sum_mean'] = loan_second_m12_ncycle['month60_State_num_sum'].mean() #var平均值
                dict_out['loan_second_m12_ncycle_month60_State_num_max_sum'] = loan_second_m12_ncycle['month60_State_num_max'].sum() #var求和
                dict_out['loan_second_m12_ncycle_classify5_num_max'] = loan_second_m12_ncycle['classify5_num'].max()#var最大值
                dict_out['loan_second_m12_ncycle_classify5_num_min'] = loan_second_m12_ncycle['classify5_num'].min() #var最小值
                dict_out['loan_second_m12_ncycle_classify5_num_range'] = (dict_out['loan_second_m12_ncycle_classify5_num_max']-dict_out['loan_second_m12_ncycle_classify5_num_min'])/dict_out['loan_second_m12_ncycle_classify5_num_max'] if dict_out['loan_second_m12_ncycle_classify5_num_max']>0 else np.nan #var区间率
                dict_out['loan_second_m12_ncycle_month60_State_countNullr_mean'] = loan_second_m12_ncycle['month60_State_countNullr'].mean() #var平均值
                dict_out['loan_second_m12_ncycle_month60_State_num_mean_mean'] = loan_second_m12_ncycle['month60_State_num_mean'].mean() #var平均值
                dict_out['loan_second_m12_ncycle_month60_State_num_sum_max'] = loan_second_m12_ncycle['month60_State_num_sum'].max()#var最大值
                dict_out['loan_second_m12_ncycle_repayMons_max'] = loan_second_m12_ncycle['repayMons'].max()#var最大值
                dict_out['loan_second_m12_ncycle_repayMons_min'] = loan_second_m12_ncycle['repayMons'].min() #var最小值
                dict_out['loan_second_m12_ncycle_repayMons_range'] = (dict_out['loan_second_m12_ncycle_repayMons_max']-dict_out['loan_second_m12_ncycle_repayMons_min'])/dict_out['loan_second_m12_ncycle_repayMons_max'] if dict_out['loan_second_m12_ncycle_repayMons_max']>0 else np.nan #var区间率
                dict_out['loan_second_m12_ncycle_month60_State_countCr_mean'] = loan_second_m12_ncycle['month60_State_countCr'].mean() #var平均值
                dict_out['loan_second_m12_ncycle_repayTerms_mean'] = loan_second_m12_ncycle['repayTerms'].mean() #var平均值
                dict_out['loan_second_m12_ncycle_byDate_to_report_max'] = loan_second_m12_ncycle['byDate_to_report'].max()#var最大值
                dict_out['loan_second_m12_ncycle_planRepayAmount_mean'] = loan_second_m12_ncycle['planRepayAmount'].mean() #var平均值
                dict_out['loan_second_m12_ncycle_month60_State_num_mean_max'] = loan_second_m12_ncycle['month60_State_num_mean'].max()#var最大值
                dict_out['loan_second_m12_ncycle_guaranteeForm_giniimpurity'] = giniimpurity(loan_second_m12_ncycle['guaranteeForm']) #近12个月开立 非循环贷账户 贷款账户担保方式基尼不纯度
                dict_out['loan_second_m12_ncycle_month60_State_countNr_max'] = loan_second_m12_ncycle['month60_State_countNr'].max()#var最大值
                dict_out['loan_second_m12_ncycle_month60_State_countNr_min'] = loan_second_m12_ncycle['month60_State_countNr'].min() #var最小值
                dict_out['loan_second_m12_ncycle_month60_State_countNr_range'] = (dict_out['loan_second_m12_ncycle_month60_State_countNr_max']-dict_out['loan_second_m12_ncycle_month60_State_countNr_min'])/dict_out['loan_second_m12_ncycle_month60_State_countNr_max'] if dict_out['loan_second_m12_ncycle_month60_State_countNr_max']>0 else np.nan #var区间率
                dict_out['loan_second_m12_ncycle_repayedAmount_max'] = loan_second_m12_ncycle['repayedAmount'].max()#var最大值
                dict_out['loan_second_m12_ncycle_repayedAmount_min'] = loan_second_m12_ncycle['repayedAmount'].min() #var最小值
                dict_out['loan_second_m12_ncycle_repayedAmount_range'] = (dict_out['loan_second_m12_ncycle_repayedAmount_max']-dict_out['loan_second_m12_ncycle_repayedAmount_min'])/dict_out['loan_second_m12_ncycle_repayedAmount_max'] if dict_out['loan_second_m12_ncycle_repayedAmount_max']>0 else np.nan #var区间率
                dict_out['loan_second_m12_ncycle_bt_other_person_count'] = loan_second_m12_ncycle[loan_second_m12_ncycle['businessType']=='其他个人消费贷款'].shape[0] #近12个月开立 非循环贷账户 贷款账户业务种类为其他个人消费贷款 计数
                dict_out['loan_second_m12_ncycle_month60_State_count1r_mean'] = loan_second_m12_ncycle['month60_State_count1r'].mean() #var平均值
                dict_out['loan_second_m12_ncycle_gf_crdit_ratio'] = loan_second_m12_ncycle[loan_second_m12_ncycle['guaranteeForm']=='信用/免担保'].shape[0]/len(loan_second_m12_ncycle) #近12个月开立 非循环贷账户 贷款账户担保方式为信用/免担保 占比
                dict_out['loan_second_m12_ncycle_month60_State_countNr_mean'] = loan_second_m12_ncycle['month60_State_countNr'].mean() #var平均值
                dict_out['loan_second_m12_ncycle_repayAmt_mean'] = loan_second_m12_ncycle['repayAmt'].mean() #var平均值
                dict_out['loan_second_m12_ncycle_month60_to_report_mean_max'] = loan_second_m12_ncycle['month60_to_report_mean'].max()#var最大值
                dict_out['loan_second_m12_ncycle_month60_to_report_mean_min'] = loan_second_m12_ncycle['month60_to_report_mean'].min() #var最小值
                dict_out['loan_second_m12_ncycle_month60_to_report_mean_range'] = (dict_out['loan_second_m12_ncycle_month60_to_report_mean_max']-dict_out['loan_second_m12_ncycle_month60_to_report_mean_min'])/dict_out['loan_second_m12_ncycle_month60_to_report_mean_max'] if dict_out['loan_second_m12_ncycle_month60_to_report_mean_max']>0 else np.nan #var区间率
                dict_out['loan_second_m12_ncycle_repayedAmount_max'] = loan_second_m12_ncycle['repayedAmount'].max()#var最大值
                dict_out['loan_second_m12_ncycle_month60_State_num_max_mean'] = loan_second_m12_ncycle['month60_State_num_max'].mean() #var平均值
                dict_out['loan_second_m12_ncycle_org_other_ratio'] = loan_second_m12_ncycle[loan_second_m12_ncycle['org']=='其他机构'].shape[0]/len(loan_second_m12_ncycle) #近12个月开立 非循环贷账户 贷款账户管理机构为其他机构 占比
                dict_out['loan_second_m12_ncycle_repayedAmount_mean'] = loan_second_m12_ncycle['repayedAmount'].mean() #var平均值
                dict_out['loan_second_m12_ncycle_month60_State_count2r_mean'] = loan_second_m12_ncycle['month60_State_count2r'].mean() #var平均值
                dict_out['loan_second_m12_ncycle_due_class_max'] = loan_second_m12_ncycle['due_class'].max()#var最大值
                dict_out['loan_second_m12_ncycle_balance_ratio_min'] = loan_second_m12_ncycle['balance_ratio'].min() #var最小值
                dict_out['loan_second_m12_ncycle_balance_max'] = loan_second_m12_ncycle['balance'].max()#var最大值
                dict_out['loan_second_m12_ncycle_balance_sum'] = loan_second_m12_ncycle['balance'].sum() #var求和
                dict_out['loan_second_m12_ncycle_month60_State_countUnknowr_mean'] = loan_second_m12_ncycle['month60_State_countUnknowr'].mean()  #var平均值比率
                dict_out['loan_second_m12_ncycle_repayTerm_ratio_max'] = loan_second_m12_ncycle['repayTerm_ratio'].max()#var最大值
                dict_out['loan_second_m12_ncycle_repayTerm_ratio_min'] = loan_second_m12_ncycle['repayTerm_ratio'].min() #var最小值比率
                dict_out['loan_second_m12_ncycle_repayTerm_ratio_range'] = (dict_out['loan_second_m12_ncycle_repayTerm_ratio_max']-dict_out['loan_second_m12_ncycle_repayTerm_ratio_min'])/dict_out['loan_second_m12_ncycle_repayTerm_ratio_max'] if dict_out['loan_second_m12_ncycle_repayTerm_ratio_max']>0 else np.nan #var区间率
                dict_out['loan_second_m12_ncycle_balance_max'] = loan_second_m12_ncycle['balance'].max()#var最大值
                dict_out['loan_second_m12_ncycle_balance_min'] = loan_second_m12_ncycle['balance'].min() #var最小值比率
                dict_out['loan_second_m12_ncycle_balance_range'] = (dict_out['loan_second_m12_ncycle_balance_max']-dict_out['loan_second_m12_ncycle_balance_min'])/dict_out['loan_second_m12_ncycle_balance_max'] if dict_out['loan_second_m12_ncycle_balance_max']>0 else np.nan #var区间率
                dict_out['loan_second_m12_ncycle_month60_State_countUnknow_mean'] = loan_second_m12_ncycle['month60_State_countUnknow'].mean()  #var平均值比率
                dict_out['loan_second_m12_ncycle_byDate_to_report_mean'] = loan_second_m12_ncycle['byDate_to_report'].mean()  #var平均值比率
                dict_out['loan_second_m12_ncycleR_month60_State_countUnknowr_mean'] = dict_out['loan_second_m12_ncycle_month60_State_countUnknowr_mean']/dict_out['loan_second_m12_month60_State_countUnknowr_mean'] if dict_out['loan_second_m12_month60_State_countUnknowr_mean']>0 else np.nan #var最大值比率
                dict_out['loan_second_m12_ncycleR_repayTerm_ratio_range'] = dict_out['loan_second_m12_ncycle_repayTerm_ratio_range']/dict_out['loan_second_m12_repayTerm_ratio_range'] if dict_out['loan_second_m12_repayTerm_ratio_range']>0 else np.nan #var最大值比率
                dict_out['loan_second_m12_ncycleR_balance_range'] = dict_out['loan_second_m12_ncycle_balance_range']/dict_out['loan_second_m12_balance_range'] if dict_out['loan_second_m12_balance_range']>0 else np.nan #var最大值比率
                dict_out['loan_second_m12_ncycleR_month60_State_countUnknow_mean'] = dict_out['loan_second_m12_ncycle_month60_State_countUnknow_mean']/dict_out['loan_second_m12_month60_State_countUnknow_mean'] if dict_out['loan_second_m12_month60_State_countUnknow_mean']>0 else np.nan #var最大值比率
                dict_out['loan_second_m12_ncycleR_byDate_to_report_mean'] = dict_out['loan_second_m12_ncycle_byDate_to_report_mean']/dict_out['loan_second_m12_byDate_to_report_mean'] if dict_out['loan_second_m12_byDate_to_report_mean']>0 else np.nan #var最大值比率

            #近12个月开立 循环贷账户
            loan_second_m12_cycle = loan_second_m12[loan_second_m12['class']=='循环贷账户']


            #近12个月开立 有担保 is_vouch 
            loan_second_m12_vouch = loan_second_m12[loan_second_m12['is_vouch']==1]


            #近12个月开立 历史逾期
            loan_second_m12_hdue1 = loan_second_m12[loan_second_m12['is_overdue']==1]
            

            #近12个月开立 历史严重逾期
            loan_second_m12_hdue3 = loan_second_m12[loan_second_m12['due_class']==3]
            

            #近12个月开立 当前逾期
            loan_second_m12_cdue = loan_second_m12[loan_second_m12['accountStatus']=='逾期']
            


        #近24个月开立
        loan_second_m24 = loan_second[loan_second.startDate_to_report<24]
        if len(loan_second_m24)>0:

            if True:
                for var in numeric_vers:
                    dict_out['loan_second_m24_'+var+'_max'] = loan_second_m24[var].max() #近24个月开立 var最大值
                    dict_out['loan_second_m24_'+var+'_min'] = loan_second_m24[var].min() #近24个月开立 var最小值
                    dict_out['loan_second_m24_'+var+'_mean'] = loan_second_m24[var].mean() #近24个月开立 var平均值
                    dict_out['loan_second_m24_'+var+'_sum'] = loan_second_m24[var].sum() #近24个月开立 var求和
                    dict_out['loan_second_m24_'+var+'_range'] = (dict_out['loan_second_m24_'+var+'_max']-dict_out['loan_second_m24_'+var+'_min'])/dict_out['loan_second_m24_'+var+'_max'] if dict_out['loan_second_m24_'+var+'_max']>0 else np.nan #近24个月开立 var区间率



                dict_out['loan_second_m24_loanGrantOrg_giniimpurity'] = giniimpurity(loan_second_m24['loanGrantOrg']) #近24个月开立 贷款账户管理机构详细基尼不纯度
                dict_out['loan_second_m24_loanGrantOrg_nunique'] = loan_second_m24['loanGrantOrg'].nunique() #近24个月开立 贷款账户管理机构详细nunique

                dict_out['loan_second_m24_org_giniimpurity'] = giniimpurity(loan_second_m24['org']) #近24个月开立 贷款账户管理机构基尼不纯度
                dict_out['loan_second_m24_org_commercial_bank_count'] = loan_second_m24[loan_second_m24['org']=='商业银行'].shape[0] #近24个月开立 贷款账户管理机构为商业银行 计数
                dict_out['loan_second_m24_org_commercial_bank_ratio'] = loan_second_m24[loan_second_m24['org']=='商业银行'].shape[0]/len(loan_second_m24) #近24个月开立 贷款账户管理机构为商业银行 占比
                dict_out['loan_second_m24_org_consumer_finance_count'] = loan_second_m24[loan_second_m24['org']=='消费金融公司'].shape[0] #近24个月开立 贷款账户管理机构为消费金融公司 计数
                dict_out['loan_second_m24_org_consumer_finance_ratio'] = loan_second_m24[loan_second_m24['org']=='消费金融公司'].shape[0]/len(loan_second_m24) #近24个月开立 贷款账户管理机构为消费金融公司 占比
                dict_out['loan_second_m24_org_micro_loan_count'] = loan_second_m24[loan_second_m24['org']=='小额贷款公司'].shape[0] #近24个月开立 贷款账户管理机构为小额贷款公司 计数
                dict_out['loan_second_m24_org_micro_loan_ratio'] = loan_second_m24[loan_second_m24['org']=='小额贷款公司'].shape[0]/len(loan_second_m24) #近24个月开立 贷款账户管理机构为小额贷款公司 占比
                dict_out['loan_second_m24_org_other_count'] = loan_second_m24[loan_second_m24['org']=='其他机构'].shape[0] #近24个月开立 贷款账户管理机构为其他机构 计数
                dict_out['loan_second_m24_org_other_ratio'] = loan_second_m24[loan_second_m24['org']=='其他机构'].shape[0]/len(loan_second_m24) #近24个月开立 贷款账户管理机构为其他机构 占比
                dict_out['loan_second_m24_org_trust_company_count'] = loan_second_m24[loan_second_m24['org']=='信托公司'].shape[0] #近24个月开立 贷款账户管理机构为信托公司 计数
                dict_out['loan_second_m24_org_trust_company_ratio'] = loan_second_m24[loan_second_m24['org']=='信托公司'].shape[0]/len(loan_second_m24) #近24个月开立 贷款账户管理机构为信托公司 占比
                dict_out['loan_second_m24_org_car_finance_count'] = loan_second_m24[loan_second_m24['org']=='汽车金融公司'].shape[0] #近24个月开立 贷款账户管理机构为汽车金融公司 计数
                dict_out['loan_second_m24_org_car_finance_ratio'] = loan_second_m24[loan_second_m24['org']=='汽车金融公司'].shape[0]/len(loan_second_m24) #近24个月开立 贷款账户管理机构为汽车金融公司 占比
                dict_out['loan_second_m24_org_lease_finance_count'] = loan_second_m24[loan_second_m24['org']=='融资租赁公司'].shape[0] #近24个月开立 贷款账户管理机构为融资租赁公司 计数
                dict_out['loan_second_m24_org_lease_finance_ratio'] = loan_second_m24[loan_second_m24['org']=='融资租赁公司'].shape[0]/len(loan_second_m24) #近24个月开立 贷款账户管理机构为融资租赁公司 占比
                dict_out['loan_second_m24_org_myself_count'] = loan_second_m24[loan_second_m24['org']==dict_in['cc_rh_report'].queryOperator.values[0]].shape[0] #近24个月开立 贷款账户管理机构为本机构 计数
                dict_out['loan_second_m24_org_myself_ratio'] = loan_second_m24[loan_second_m24['org']==dict_in['cc_rh_report'].queryOperator.values[0]].shape[0]/len(loan_second_m24) #近24个月开立 贷款账户管理机构为本机构 占比
                dict_out['loan_second_m24_org_village_banks_count'] = loan_second_m24[loan_second_m24['org']=='村镇银行'].shape[0] #近24个月开立 贷款账户管理机构为村镇银行 计数
                dict_out['loan_second_m24_org_village_banks_ratio'] = loan_second_m24[loan_second_m24['org']=='村镇银行'].shape[0]/len(loan_second_m24) #近24个月开立 贷款账户管理机构为村镇银行 占比
                dict_out['loan_second_m24_org_finance_company_count'] = loan_second_m24[loan_second_m24['org']=='财务公司'].shape[0] #近24个月开立 贷款账户管理机构为财务公司 计数
                dict_out['loan_second_m24_org_finance_company_ratio'] = loan_second_m24[loan_second_m24['org']=='财务公司'].shape[0]/len(loan_second_m24) #近24个月开立 贷款账户管理机构为财务公司 占比
                dict_out['loan_second_m24_org_foreign_banks_count'] = loan_second_m24[loan_second_m24['org']=='外资银行'].shape[0] #近24个月开立 贷款账户管理机构为外资银行 计数
                dict_out['loan_second_m24_org_foreign_banks_ratio'] = loan_second_m24[loan_second_m24['org']=='外资银行'].shape[0]/len(loan_second_m24) #近24个月开立 贷款账户管理机构为外资银行 占比
                dict_out['loan_second_m24_org_provident_fund_count'] = loan_second_m24[loan_second_m24['org']=='公积金管理中心'].shape[0] #近24个月开立 贷款账户管理机构为公积金管理中心 计数
                dict_out['loan_second_m24_org_provident_fund_ratio'] = loan_second_m24[loan_second_m24['org']=='公积金管理中心'].shape[0]/len(loan_second_m24) #近24个月开立 贷款账户管理机构为公积金管理中心 占比
                dict_out['loan_second_m24_org_securities_firm_count'] = loan_second_m24[loan_second_m24['org']=='证券公司'].shape[0] #近24个月开立 贷款账户管理机构为证券公司 计数
                dict_out['loan_second_m24_org_securities_firm_ratio'] = loan_second_m24[loan_second_m24['org']=='证券公司'].shape[0]/len(loan_second_m24) #近24个月开立 贷款账户管理机构为证券公司 占比

                dict_out['loan_second_m24_class_giniimpurity'] = giniimpurity(loan_second_m24['class']) #近24个月开立 贷款账户账户类别基尼不纯度
                dict_out['loan_second_m24_class_ncycle_count'] = loan_second_m24[loan_second_m24['class']=='非循环贷账户'].shape[0] #近24个月开立 贷款账户账户类别为非循环贷账户 计数
                dict_out['loan_second_m24_class_ncycle_ratio'] = loan_second_m24[loan_second_m24['class']=='非循环贷账户'].shape[0]/len(loan_second_m24) #近24个月开立 贷款账户账户类别为非循环贷账户 占比
                dict_out['loan_second_m24_class_cycle_sub_count'] = loan_second_m24[loan_second_m24['class']=='循环额度下分账户'].shape[0] #近24个月开立 贷款账户账户类别为循环额度下分账户 计数
                dict_out['loan_second_m24_class_cycle_sub_ratio'] = loan_second_m24[loan_second_m24['class']=='循环额度下分账户'].shape[0]/len(loan_second_m24) #近24个月开立 贷款账户账户类别为循环额度下分账户 占比
                dict_out['loan_second_m24_class_cycle_count'] = loan_second_m24[loan_second_m24['class']=='循环贷账户'].shape[0] #近24个月开立 贷款账户账户类别为循环贷账户 计数
                dict_out['loan_second_m24_class_cycle_ratio'] = loan_second_m24[loan_second_m24['class']=='循环贷账户'].shape[0]/len(loan_second_m24) #近24个月开立 贷款账户账户类别为循环贷账户 占比

                dict_out['loan_second_m24_classify5_giniimpurity'] = giniimpurity(loan_second_m24['classify5']) #近24个月开立 贷款账户五级分类基尼不纯度
                dict_out['loan_second_m24_c5_unknow_count'] = loan_second_m24[loan_second_m24['classify5']==''].shape[0] #近24个月开立 贷款账户五级分类为'' 计数
                dict_out['loan_second_m24_c5_unknow_ratio'] = loan_second_m24[loan_second_m24['classify5']==''].shape[0]/len(loan_second_m24) #近24个月开立 贷款账户五级分类为'' 占比
                dict_out['loan_second_m24_c5_normal_count'] = loan_second_m24[loan_second_m24['classify5']=='正常'].shape[0] #近24个月开立 贷款账户五级分类为正常 计数
                dict_out['loan_second_m24_c5_normal_ratio'] = loan_second_m24[loan_second_m24['classify5']=='正常'].shape[0]/len(loan_second_m24) #近24个月开立 贷款账户五级分类为正常 占比
                dict_out['loan_second_m24_c5_loss_count'] = loan_second_m24[loan_second_m24['classify5']=='损失'].shape[0] #近24个月开立 贷款账户五级分类为损失 计数
                dict_out['loan_second_m24_c5_loss_ratio'] = loan_second_m24[loan_second_m24['classify5']=='损失'].shape[0]/len(loan_second_m24) #近24个月开立 贷款账户五级分类为损失 占比
                dict_out['loan_second_m24_c5_attention_count'] = loan_second_m24[loan_second_m24['classify5']=='关注'].shape[0] #近24个月开立 贷款账户五级分类为关注 计数
                dict_out['loan_second_m24_c5_attention_ratio'] = loan_second_m24[loan_second_m24['classify5']=='关注'].shape[0]/len(loan_second_m24) #近24个月开立 贷款账户五级分类为关注 占比
                dict_out['loan_second_m24_c5_suspicious_count'] = loan_second_m24[loan_second_m24['classify5']=='可疑'].shape[0] #近24个月开立 贷款账户五级分类为可疑 计数
                dict_out['loan_second_m24_c5_suspicious_ratio'] = loan_second_m24[loan_second_m24['classify5']=='可疑'].shape[0]/len(loan_second_m24) #近24个月开立 贷款账户五级分类为可疑 占比
                dict_out['loan_second_m24_c5_secondary_count'] = loan_second_m24[loan_second_m24['classify5']=='次级'].shape[0] #近24个月开立 贷款账户五级分类为次级 计数
                dict_out['loan_second_m24_c5_secondary_ratio'] = loan_second_m24[loan_second_m24['classify5']=='次级'].shape[0]/len(loan_second_m24) #近24个月开立 贷款账户五级分类为次级 占比
                dict_out['loan_second_m24_c5_noclass_count'] = loan_second_m24[loan_second_m24['classify5']=='未分类'].shape[0] #近24个月开立 贷款账户五级分类为未分类 计数
                dict_out['loan_second_m24_c5_noclass_ratio'] = loan_second_m24[loan_second_m24['classify5']=='未分类'].shape[0]/len(loan_second_m24) #近24个月开立 贷款账户五级分类为未分类 占比

                dict_out['loan_second_m24_accountStatus_giniimpurity'] = giniimpurity(loan_second_m24['accountStatus']) #近24个月开立 贷款账户账户状态基尼不纯度
                dict_out['loan_second_m24_as_settle_count'] = loan_second_m24[loan_second_m24['accountStatus']=='结清'].shape[0] #近24个月开立 贷款账户账户状态为结清 计数
                dict_out['loan_second_m24_as_settle_ratio'] = loan_second_m24[loan_second_m24['accountStatus']=='结清'].shape[0]/len(loan_second_m24) #近24个月开立 贷款账户账户状态为结清 占比
                dict_out['loan_second_m24_as_normal_count'] = loan_second_m24[loan_second_m24['accountStatus']=='正常'].shape[0] #近24个月开立 贷款账户账户状态为正常 计数
                dict_out['loan_second_m24_as_normal_ratio'] = loan_second_m24[loan_second_m24['accountStatus']=='正常'].shape[0]/len(loan_second_m24) #近24个月开立 贷款账户账户状态为正常 占比
                dict_out['loan_second_m24_as_overdue_count'] = loan_second_m24[loan_second_m24['accountStatus']=='逾期'].shape[0] #近24个月开立 贷款账户账户状态为逾期 计数
                dict_out['loan_second_m24_as_overdue_ratio'] = loan_second_m24[loan_second_m24['accountStatus']=='逾期'].shape[0]/len(loan_second_m24) #近24个月开立 贷款账户账户状态为逾期 占比
                dict_out['loan_second_m24_as_bad_debts_count'] = loan_second_m24[loan_second_m24['accountStatus']=='呆账'].shape[0] #近24个月开立 贷款账户账户状态为呆账 计数
                dict_out['loan_second_m24_as_bad_debts_ratio'] = loan_second_m24[loan_second_m24['accountStatus']=='呆账'].shape[0]/len(loan_second_m24) #近24个月开立 贷款账户账户状态为呆账 占比
                dict_out['loan_second_m24_as_unknow_count'] = loan_second_m24[loan_second_m24['accountStatus']==''].shape[0] #近24个月开立 贷款账户账户状态为'' 计数
                dict_out['loan_second_m24_as_unknow_ratio'] = loan_second_m24[loan_second_m24['accountStatus']==''].shape[0]/len(loan_second_m24) #近24个月开立 贷款账户账户状态为'' 占比
                dict_out['loan_second_m24_as_roll_out_count'] = loan_second_m24[loan_second_m24['accountStatus']=='转出'].shape[0] #近24个月开立 贷款账户账户状态为转出 计数
                dict_out['loan_second_m24_as_roll_out_ratio'] = loan_second_m24[loan_second_m24['accountStatus']=='转出'].shape[0]/len(loan_second_m24) #近24个月开立 贷款账户账户状态为转出 占比

                dict_out['loan_second_m24_repayType_giniimpurity'] = giniimpurity(loan_second_m24['repayType']) #近24个月开立 贷款账户还款方式基尼不纯度
                dict_out['loan_second_m24_rt_unknow_count'] = loan_second_m24[loan_second_m24['repayType']=='--'].shape[0] #近24个月开立 贷款账户还款方式为-- 计数
                dict_out['loan_second_m24_rt_unknow_ratio'] = loan_second_m24[loan_second_m24['repayType']=='--'].shape[0]/len(loan_second_m24) #近24个月开立 贷款账户还款方式为-- 占比
                dict_out['loan_second_m24_rt_equality_count'] = loan_second_m24[loan_second_m24['repayType']=='分期等额本息'].shape[0] #近24个月开立 贷款账户还款方式为分期等额本息 计数
                dict_out['loan_second_m24_rt_equality_ratio'] = loan_second_m24[loan_second_m24['repayType']=='分期等额本息'].shape[0]/len(loan_second_m24) #近24个月开立 贷款账户还款方式为分期等额本息 占比
                dict_out['loan_second_m24_rt_onschedule_count'] = loan_second_m24[loan_second_m24['repayType']=='按期计算还本付息'].shape[0] #近24个月开立 贷款账户还款方式为按期计算还本付息 计数
                dict_out['loan_second_m24_rt_onschedule_ratio'] = loan_second_m24[loan_second_m24['repayType']=='按期计算还本付息'].shape[0]/len(loan_second_m24) #近24个月开立 贷款账户还款方式为按期计算还本付息 占比
                dict_out['loan_second_m24_rt_not_distinguish_count'] = loan_second_m24[loan_second_m24['repayType']=='不区分还款方式'].shape[0] #近24个月开立 贷款账户还款方式为不区分还款方式 计数
                dict_out['loan_second_m24_rt_not_distinguish_ratio'] = loan_second_m24[loan_second_m24['repayType']=='不区分还款方式'].shape[0]/len(loan_second_m24) #近24个月开立 贷款账户还款方式为不区分还款方式 占比
                dict_out['loan_second_m24_rt_circulation_count'] = loan_second_m24[loan_second_m24['repayType']=='循环贷款下其他还款方式'].shape[0] #近24个月开立 贷款账户还款方式为循环贷款下其他还款方式 计数
                dict_out['loan_second_m24_rt_circulation_ratio'] = loan_second_m24[loan_second_m24['repayType']=='循环贷款下其他还款方式'].shape[0]/len(loan_second_m24) #近24个月开立 贷款账户还款方式为循环贷款下其他还款方式 占比
                dict_out['loan_second_m24_rt_once_count'] = loan_second_m24[loan_second_m24['repayType']=='到期一次还本付息'].shape[0] #近24个月开立 贷款账户还款方式为到期一次还本付息 计数
                dict_out['loan_second_m24_rt_once_ratio'] = loan_second_m24[loan_second_m24['repayType']=='到期一次还本付息'].shape[0]/len(loan_second_m24) #近24个月开立 贷款账户还款方式为到期一次还本付息 占比

                dict_out['loan_second_m24_repayFrequency_giniimpurity'] = giniimpurity(loan_second_m24['repayFrequency']) #近24个月开立 贷款账户还款频率基尼不纯度
                dict_out['loan_second_m24_rf_month_count'] = loan_second_m24[loan_second_m24['repayFrequency']=='月'].shape[0] #近24个月开立 贷款账户还款频率为月 计数
                dict_out['loan_second_m24_rf_month_ratio'] = loan_second_m24[loan_second_m24['repayFrequency']=='月'].shape[0]/len(loan_second_m24) #近24个月开立 贷款账户还款频率为月 占比
                dict_out['loan_second_m24_rf_once_count'] = loan_second_m24[loan_second_m24['repayFrequency']=='一次性'].shape[0] #近24个月开立 贷款账户还款频率为一次性 计数
                dict_out['loan_second_m24_rf_once_ratio'] = loan_second_m24[loan_second_m24['repayFrequency']=='一次性'].shape[0]/len(loan_second_m24) #近24个月开立 贷款账户还款频率为一次性 占比
                dict_out['loan_second_m24_rf_other_count'] = loan_second_m24[loan_second_m24['repayFrequency']=='其他'].shape[0] #近24个月开立 贷款账户还款频率为其他 计数
                dict_out['loan_second_m24_rf_other_ratio'] = loan_second_m24[loan_second_m24['repayFrequency']=='其他'].shape[0]/len(loan_second_m24) #近24个月开立 贷款账户还款频率为其他 占比
                dict_out['loan_second_m24_rf_irregular_count'] = loan_second_m24[loan_second_m24['repayFrequency']=='不定期'].shape[0] #近24个月开立 贷款账户还款频率为不定期 计数
                dict_out['loan_second_m24_rf_irregular_ratio'] = loan_second_m24[loan_second_m24['repayFrequency']=='不定期'].shape[0]/len(loan_second_m24) #近24个月开立 贷款账户还款频率为不定期 占比
                dict_out['loan_second_m24_rf_day_count'] = loan_second_m24[loan_second_m24['repayFrequency']=='日'].shape[0] #近24个月开立 贷款账户还款频率为日 计数
                dict_out['loan_second_m24_rf_day_ratio'] = loan_second_m24[loan_second_m24['repayFrequency']=='日'].shape[0]/len(loan_second_m24) #近24个月开立 贷款账户还款频率为日 占比
                dict_out['loan_second_m24_rf_year_count'] = loan_second_m24[loan_second_m24['repayFrequency']=='年'].shape[0] #近24个月开立 贷款账户还款频率为年 计数
                dict_out['loan_second_m24_rf_year_ratio'] = loan_second_m24[loan_second_m24['repayFrequency']=='年'].shape[0]/len(loan_second_m24) #近24个月开立 贷款账户还款频率为年 占比
                dict_out['loan_second_m24_rf_season_count'] = loan_second_m24[loan_second_m24['repayFrequency']=='季'].shape[0] #近24个月开立 贷款账户还款频率为季 计数
                dict_out['loan_second_m24_rf_season_ratio'] = loan_second_m24[loan_second_m24['repayFrequency']=='季'].shape[0]/len(loan_second_m24) #近24个月开立 贷款账户还款频率为季 占比
                dict_out['loan_second_m24_rf_week_count'] = loan_second_m24[loan_second_m24['repayFrequency']=='周'].shape[0] #近24个月开立 贷款账户还款频率为周 计数
                dict_out['loan_second_m24_rf_week_ratio'] = loan_second_m24[loan_second_m24['repayFrequency']=='周'].shape[0]/len(loan_second_m24) #近24个月开立 贷款账户还款频率为周 占比
                dict_out['loan_second_m24_rf_halfyear_count'] = loan_second_m24[loan_second_m24['repayFrequency']=='半年'].shape[0] #近24个月开立 贷款账户还款频率为半年 计数
                dict_out['loan_second_m24_rf_halfyear_ratio'] = loan_second_m24[loan_second_m24['repayFrequency']=='半年'].shape[0]/len(loan_second_m24) #近24个月开立 贷款账户还款频率为半年 占比

                dict_out['loan_second_m24_guaranteeForm_giniimpurity'] = giniimpurity(loan_second_m24['guaranteeForm']) #近24个月开立 贷款账户担保方式基尼不纯度
                dict_out['loan_second_m24_gf_crdit_count'] = loan_second_m24[loan_second_m24['guaranteeForm']=='信用/免担保'].shape[0] #近24个月开立 贷款账户担保方式为其信用/免担保 计数
                dict_out['loan_second_m24_gf_crdit_ratio'] = loan_second_m24[loan_second_m24['guaranteeForm']=='信用/免担保'].shape[0]/len(loan_second_m24) #近24个月开立 贷款账户担保方式为信用/免担保 占比
                dict_out['loan_second_m24_gf_other_count'] = loan_second_m24[loan_second_m24['guaranteeForm']=='其他'].shape[0] #近24个月开立 贷款账户担保方式为其他 计数
                dict_out['loan_second_m24_gf_other_ratio'] = loan_second_m24[loan_second_m24['guaranteeForm']=='其他'].shape[0]/len(loan_second_m24) #近24个月开立 贷款账户担保方式为其他 占比
                dict_out['loan_second_m24_gf_combine_nowarranty_count'] = loan_second_m24[loan_second_m24['guaranteeForm']=='组合（不含保证）'].shape[0] #近24个月开立 贷款账户担保方式为组合（不含保证） 计数
                dict_out['loan_second_m24_gf_combine_nowarranty_ratio'] = loan_second_m24[loan_second_m24['guaranteeForm']=='组合（不含保证）'].shape[0]/len(loan_second_m24) #近24个月开立 贷款账户担保方式为组合（不含保证） 占比
                dict_out['loan_second_m24_gf_combine_warranty_count'] = loan_second_m24[loan_second_m24['guaranteeForm']=='组合(含保证)'].shape[0] #近24个月开立 贷款账户担保方式为组合(含保证) 计数
                dict_out['loan_second_m24_gf_combine_warranty_ratio'] = loan_second_m24[loan_second_m24['guaranteeForm']=='组合(含保证)'].shape[0]/len(loan_second_m24) #近24个月开立 贷款账户担保方式为组合(含保证) 占比
                dict_out['loan_second_m24_gf_mortgage_count'] = loan_second_m24[loan_second_m24['guaranteeForm']=='抵押'].shape[0] #近24个月开立 贷款账户担保方式为抵押 计数
                dict_out['loan_second_m24_gf_mortgage_ratio'] = loan_second_m24[loan_second_m24['guaranteeForm']=='抵押'].shape[0]/len(loan_second_m24) #近24个月开立 贷款账户担保方式为抵押 占比
                dict_out['loan_second_m24_gf_warranty_count'] = loan_second_m24[loan_second_m24['guaranteeForm']=='保证'].shape[0] #近24个月开立 贷款账户担保方式为保证计数
                dict_out['loan_second_m24_gf_warranty_ratio'] = loan_second_m24[loan_second_m24['guaranteeForm']=='保证'].shape[0]/len(loan_second_m24) #近24个月开立 贷款账户担保方式为保证 占比
                dict_out['loan_second_m24_gf_pledge_count'] = loan_second_m24[loan_second_m24['guaranteeForm']=='质押'].shape[0] #近24个月开立 贷款账户担保方式为质押 计数
                dict_out['loan_second_m24_gf_pledge_ratio'] = loan_second_m24[loan_second_m24['guaranteeForm']=='质押'].shape[0]/len(loan_second_m24) #近24个月开立 贷款账户担保方式为质押 占比
                dict_out['loan_second_m24_gf_farm_group_count'] = loan_second_m24[loan_second_m24['guaranteeForm']=='农户联保'].shape[0] #近24个月开立 贷款账户担保方式为农户联保 计数
                dict_out['loan_second_m24_gf_farm_group_ratio'] = loan_second_m24[loan_second_m24['guaranteeForm']=='农户联保'].shape[0]/len(loan_second_m24) #近24个月开立 贷款账户担保方式为农户联保 占比

                dict_out['loan_second_m24_businessType_giniimpurity'] = giniimpurity(loan_second_m24['businessType']) #近24个月开立 贷款账户业务种类基尼不纯度
                dict_out['loan_second_m24_bt_other_person_count'] = loan_second_m24[loan_second_m24['businessType']=='其他个人消费贷款'].shape[0] #近24个月开立 贷款账户业务种类为其他个人消费贷款 计数
                dict_out['loan_second_m24_bt_other_person_ratio'] = loan_second_m24[loan_second_m24['businessType']=='其他个人消费贷款'].shape[0]/len(loan_second_m24) #近24个月开立 贷款账户业务种类为其他个人消费贷款 占比
                dict_out['loan_second_m24_bt_other_loan_count'] = loan_second_m24[loan_second_m24['businessType']=='其他贷款'].shape[0] #近24个月开立 贷款账户业务种类为其他贷款 计数
                dict_out['loan_second_m24_bt_other_loan_ratio'] = loan_second_m24[loan_second_m24['businessType']=='其他贷款'].shape[0]/len(loan_second_m24) #近24个月开立 贷款账户业务种类为其他贷款 占比
                dict_out['loan_second_m24_bt_person_business_count'] = loan_second_m24[loan_second_m24['businessType']=='个人经营性贷款'].shape[0] #近24个月开立 贷款账户业务种类为个人经营性贷款 计数
                dict_out['loan_second_m24_bt_person_business_ratio'] = loan_second_m24[loan_second_m24['businessType']=='个人经营性贷款'].shape[0]/len(loan_second_m24) #近24个月开立 贷款账户业务种类为个人经营性贷款 占比
                dict_out['loan_second_m24_bt_farm_loan_count'] = loan_second_m24[loan_second_m24['businessType']=='农户贷款'].shape[0] #近24个月开立 贷款账户业务种类为农户贷款 计数
                dict_out['loan_second_m24_bt_farm_loan_ratio'] = loan_second_m24[loan_second_m24['businessType']=='农户贷款'].shape[0]/len(loan_second_m24) #近24个月开立 贷款账户业务种类为农户贷款 占比
                dict_out['loan_second_m24_bt_person_car_count'] = loan_second_m24[loan_second_m24['businessType']=='个人汽车消费贷款'].shape[0] #近24个月开立 贷款账户业务种类为个人汽车消费贷款 计数
                dict_out['loan_second_m24_bt_person_car_ratio'] = loan_second_m24[loan_second_m24['businessType']=='个人汽车消费贷款'].shape[0]/len(loan_second_m24) #近24个月开立 贷款账户业务种类为个人汽车消费贷款 占比
                dict_out['loan_second_m24_bt_person_study_count'] = loan_second_m24[loan_second_m24['businessType']=='个人助学贷款'].shape[0] #近24个月开立 贷款账户业务种类为个人助学贷款 计数
                dict_out['loan_second_m24_bt_person_study_ratio'] = loan_second_m24[loan_second_m24['businessType']=='个人助学贷款'].shape[0]/len(loan_second_m24) #近24个月开立 贷款账户业务种类为个人助学贷款 占比
                dict_out['loan_second_m24_bt_house_commercial_count'] = loan_second_m24[loan_second_m24['businessType']=='个人住房商业贷款'].shape[0] #近24个月开立 贷款账户业务种类为个人住房商业贷款 计数
                dict_out['loan_second_m24_bt_house_commercial_ratio'] = loan_second_m24[loan_second_m24['businessType']=='个人住房商业贷款'].shape[0]/len(loan_second_m24) #近24个月开立 贷款账户业务种类为个人住房商业贷款 占比
                dict_out['loan_second_m24_bt_finance_lease_count'] = loan_second_m24[loan_second_m24['businessType']=='融资租赁业务'].shape[0] #近24个月开立 贷款账户业务种类为融资租赁业务 计数
                dict_out['loan_second_m24_bt_finance_lease_ratio'] = loan_second_m24[loan_second_m24['businessType']=='融资租赁业务'].shape[0]/len(loan_second_m24) #近24个月开立 贷款账户业务种类为融资租赁业务 占比
                dict_out['loan_second_m24_bt_house_fund_count'] = loan_second_m24[loan_second_m24['businessType']=='个人住房公积金贷款'].shape[0] #近24个月开立 贷款账户业务种类为个人住房公积金贷款 计数
                dict_out['loan_second_m24_bt_house_fund_ratio'] = loan_second_m24[loan_second_m24['businessType']=='个人住房公积金贷款'].shape[0]/len(loan_second_m24) #近24个月开立 贷款账户业务种类为个人住房公积金贷款 占比
                dict_out['loan_second_m24_bt_person_house_count'] = loan_second_m24[loan_second_m24['businessType']=='个人商用房（含商住两用）贷款'].shape[0] #近24个月开立 贷款账户业务种类为个人商用房（含商住两用）贷款 计数
                dict_out['loan_second_m24_bt_person_house_ratio'] = loan_second_m24[loan_second_m24['businessType']=='个人商用房（含商住两用）贷款'].shape[0]/len(loan_second_m24) #近24个月开立 贷款账户业务种类为个人商用房（含商住两用）贷款 占比
                dict_out['loan_second_m24_bt_stock_pledge_count'] = loan_second_m24[loan_second_m24['businessType']=='股票质押式回购交易'].shape[0] #近24个月开立 贷款账户业务种类为股票质押式回购交易计数
                dict_out['loan_second_m24_bt_stock_pledge_ratio'] = loan_second_m24[loan_second_m24['businessType']=='股票质押式回购交易'].shape[0]/len(loan_second_m24) #近24个月开立 贷款账户业务种类为股票质押式回购交易 占比



            #近24个月开立 现行
            loan_second_m24_now = loan_second_m24[loan_second_m24.is_now==1]
            if len(loan_second_m24_now)>0:
                dict_out['loan_second_m24_now_month60_State_countN_mean'] = loan_second_m24_now['month60_State_countN'].mean() #var平均值
                dict_out['loan_second_m24_now_org_micro_loan_ratio'] = loan_second_m24_now[loan_second_m24_now['org']=='小额贷款公司'].shape[0]/len(loan_second_m24_now) #近24个月开立 现行贷款账户管理机构为小额贷款公司 占比
                dict_out['loan_second_m24_now_month60_to_report_max_max'] = loan_second_m24_now['month60_to_report_max'].max()#var最大值
                dict_out['loan_second_m24_now_month60_to_report_max_min'] = loan_second_m24_now['month60_to_report_max'].min() #var最小值
                dict_out['loan_second_m24_now_month60_to_report_max_range'] = (dict_out['loan_second_m24_now_month60_to_report_max_max']-dict_out['loan_second_m24_now_month60_to_report_max_min'])/dict_out['loan_second_m24_now_month60_to_report_max_max'] if dict_out['loan_second_m24_now_month60_to_report_max_max']>0 else np.nan #var区间率
                dict_out['loan_second_m24_now_month60_to_report_mean_max'] = loan_second_m24_now['month60_to_report_mean'].max()#var最大值
                dict_out['loan_second_m24_now_month60_to_report_mean_min'] = loan_second_m24_now['month60_to_report_mean'].min() #var最小值
                dict_out['loan_second_m24_now_month60_to_report_mean_range'] = (dict_out['loan_second_m24_now_month60_to_report_mean_max']-dict_out['loan_second_m24_now_month60_to_report_mean_min'])/dict_out['loan_second_m24_now_month60_to_report_mean_max'] if dict_out['loan_second_m24_now_month60_to_report_mean_max']>0 else np.nan #var区间率
                dict_out['loan_second_m24_now_planRepayAmount_min'] = loan_second_m24_now['planRepayAmount'].min() #var最小值
                dict_out['loan_second_m24_now_org_trust_company_ratio'] = loan_second_m24_now[loan_second_m24_now['org']=='信托公司'].shape[0]/len(loan_second_m24_now) #近24个月开立 现行贷款账户管理机构为信托公司 占比
                dict_out['loan_second_m24_now_gf_other_count'] = loan_second_m24_now[loan_second_m24_now['guaranteeForm']=='其他'].shape[0] #近24个月开立 现行贷款账户担保方式为其他 计数
                dict_out['loan_second_m24_now_month60_State_countUnknowr_max'] = loan_second_m24_now['month60_State_countUnknowr'].max()#var最大值
                dict_out['loan_second_m24_now_repayAmt_mean'] = loan_second_m24_now['repayAmt'].mean() #var平均值
                dict_out['loan_second_m24_now_repayMons_ratio_sum'] = loan_second_m24_now['repayMons_ratio'].sum() #var求和
                dict_out['loan_second_m24_now_leftRepayTerms_max'] = loan_second_m24_now['leftRepayTerms'].max()#var最大值
                dict_out['loan_second_m24_now_leftRepayTerms_max'] = loan_second_m24_now['leftRepayTerms'].max()#var最大值
                dict_out['loan_second_m24_now_leftRepayTerms_min'] = loan_second_m24_now['leftRepayTerms'].min() #var最小值
                dict_out['loan_second_m24_now_leftRepayTerms_range'] = (dict_out['loan_second_m24_now_leftRepayTerms_max']-dict_out['loan_second_m24_now_leftRepayTerms_min'])/dict_out['loan_second_m24_now_leftRepayTerms_max'] if dict_out['loan_second_m24_now_leftRepayTerms_max']>0 else np.nan #var区间率
                dict_out['loan_second_m24_now_repayTerm_ratio_min'] = loan_second_m24_now['repayTerm_ratio'].min() #var最小值
                dict_out['loan_second_m24_now_month60_State_countNr_mean'] = loan_second_m24_now['month60_State_countNr'].mean() #var平均值
                dict_out['loan_second_m24_now_balance_ratio_min'] = loan_second_m24_now['balance_ratio'].min() #var最小值
                dict_out['loan_second_m24_now_balance_max'] = loan_second_m24_now['balance'].max()#var最大值
                dict_out['loan_second_m24_now_RepayedAmount_ratio_max'] = loan_second_m24_now['RepayedAmount_ratio'].max()#var最大值
                dict_out['loan_second_m24_now_RepayedAmount_ratio_min'] = loan_second_m24_now['RepayedAmount_ratio'].min() #var最小值
                dict_out['loan_second_m24_now_RepayedAmount_ratio_range'] = (dict_out['loan_second_m24_now_RepayedAmount_ratio_max']-dict_out['loan_second_m24_now_RepayedAmount_ratio_min'])/dict_out['loan_second_m24_now_RepayedAmount_ratio_max'] if dict_out['loan_second_m24_now_RepayedAmount_ratio_max']>0 else np.nan #var区间率
                dict_out['loan_second_m24_now_repayTerms_mean'] = loan_second_m24_now['repayTerms'].mean() #var平均值
                dict_out['loan_second_m24_now_repayedAmount_min'] = loan_second_m24_now['repayedAmount'].min() #var最小值
                dict_out['loan_second_m24_now_month60_State_countNullr_max'] = loan_second_m24_now['month60_State_countNullr'].max()#var最大值
                dict_out['loan_second_m24_now_month60_State_countNullr_min'] = loan_second_m24_now['month60_State_countNullr'].min() #var最小值
                dict_out['loan_second_m24_now_month60_State_countNullr_range'] = (dict_out['loan_second_m24_now_month60_State_countNullr_max']-dict_out['loan_second_m24_now_month60_State_countNullr_min'])/dict_out['loan_second_m24_now_month60_State_countNullr_max'] if dict_out['loan_second_m24_now_month60_State_countNullr_max']>0 else np.nan #var区间率
                dict_out['loan_second_m24_now_rf_other_ratio'] = loan_second_m24_now[loan_second_m24_now['repayFrequency']=='其他'].shape[0]/len(loan_second_m24_now) #近24个月开立 现行贷款账户还款频率为其他 占比
                dict_out['loan_second_m24_now_rt_onschedule_ratio'] = loan_second_m24_now[loan_second_m24_now['repayType']=='按期计算还本付息'].shape[0]/len(loan_second_m24_now) #近24个月开立 现行贷款账户还款方式为按期计算还本付息 占比
                dict_out['loan_second_m24_now_guaranteeForm_giniimpurity'] = giniimpurity(loan_second_m24_now['guaranteeForm']) #近24个月开立 现行贷款账户担保方式基尼不纯度
                dict_out['loan_second_m24_now_month60_to_report_min_sum'] = loan_second_m24_now['month60_to_report_min'].sum() #var求和
                dict_out['loan_second_m24_now_month60_State_countNull_mean'] = loan_second_m24_now['month60_State_countNull'].mean() #var平均值
                dict_out['loan_second_m24_now_loanAmount_max'] = loan_second_m24_now['loanAmount'].max()#var最大值
                dict_out['loan_second_m24_now_loanAmount_min'] = loan_second_m24_now['loanAmount'].min() #var最小值
                dict_out['loan_second_m24_now_loanAmount_range'] = (dict_out['loan_second_m24_now_loanAmount_max']-dict_out['loan_second_m24_now_loanAmount_min'])/dict_out['loan_second_m24_now_loanAmount_max'] if dict_out['loan_second_m24_now_loanAmount_max']>0 else np.nan #var区间率
                dict_out['loan_second_m24_now_repayedAmount_mean'] = loan_second_m24_now['repayedAmount'].mean() #var平均值
                dict_out['loan_second_m24_now_month60_State_countUnknowr_mean'] = loan_second_m24_now['month60_State_countUnknowr'].mean() #var平均值
                dict_out['loan_second_m24_now_month60_State_count2r_sum'] = loan_second_m24_now['month60_State_count2r'].sum() #var求和
                dict_out['loan_second_m24_now_repayFrequency_giniimpurity'] = giniimpurity(loan_second_m24_now['repayFrequency']) #近24个月开立 现行贷款账户还款频率基尼不纯度
                dict_out['loan_second_m24_now_loanAmount_sum'] = loan_second_m24_now['loanAmount'].sum() #var求和
                dict_out['loan_second_m24_now_repayMons_max'] = loan_second_m24_now['repayMons'].max()#var最大值
                dict_out['loan_second_m24_now_repayMons_min'] = loan_second_m24_now['repayMons'].min() #var最小值
                dict_out['loan_second_m24_now_repayMons_range'] = (dict_out['loan_second_m24_now_repayMons_max']-dict_out['loan_second_m24_now_repayMons_min'])/dict_out['loan_second_m24_now_repayMons_max'] if dict_out['loan_second_m24_now_repayMons_max']>0 else np.nan #var区间率
                dict_out['loan_second_m24_now_repayTerm_ratio_max'] = loan_second_m24_now['repayTerm_ratio'].max()#var最大值
                dict_out['loan_second_m24_now_repayTerm_ratio_min'] = loan_second_m24_now['repayTerm_ratio'].min() #var最小值
                dict_out['loan_second_m24_now_repayTerm_ratio_range'] = (dict_out['loan_second_m24_now_repayTerm_ratio_max']-dict_out['loan_second_m24_now_repayTerm_ratio_min'])/dict_out['loan_second_m24_now_repayTerm_ratio_max'] if dict_out['loan_second_m24_now_repayTerm_ratio_max']>0 else np.nan #var区间率
                dict_out['loan_second_m24_now_bt_person_business_ratio'] = loan_second_m24_now[loan_second_m24_now['businessType']=='个人经营性贷款'].shape[0]/len(loan_second_m24_now) #近24个月开立 现行贷款账户业务种类为个人经营性贷款 占比
                dict_out['loan_second_m24_now_org_commercial_bank_ratio'] = loan_second_m24_now[loan_second_m24_now['org']=='商业银行'].shape[0]/len(loan_second_m24_now) #近24个月开立 现行贷款账户管理机构为商业银行 占比
                dict_out['loan_second_m24_now_rf_once_count'] = loan_second_m24_now[loan_second_m24_now['repayFrequency']=='一次性'].shape[0] #近24个月开立 现行贷款账户还款频率为一次性 计数
                dict_out['loan_second_m24_now_rf_month_ratio'] = loan_second_m24_now[loan_second_m24_now['repayFrequency']=='月'].shape[0]/len(loan_second_m24_now) #近24个月开立 现行贷款账户还款频率为月 占比
                dict_out['loan_second_m24_now_rf_other_ratio'] = loan_second_m24_now[loan_second_m24_now['repayFrequency']=='其他'].shape[0]/len(loan_second_m24_now) #近24个月开立 现行贷款账户还款频率为其他 占比
                dict_out['loan_second_m24_now_rf_once_ratio'] = loan_second_m24_now[loan_second_m24_now['repayFrequency']=='一次性'].shape[0]/len(loan_second_m24_now) #近24个月开立 现行贷款账户还款频率为一次性 占比
                dict_out['loan_second_m24_now_org_consumer_finance_count'] = loan_second_m24_now[loan_second_m24_now['org']=='消费金融公司'].shape[0] #近24个月开立 现行贷款账户管理机构为消费金融公司 计数
                dict_out['loan_second_m24_now_bt_other_loan_count'] = loan_second_m24_now[loan_second_m24_now['businessType']=='其他贷款'].shape[0] #近24个月开立 现行贷款账户业务种类为其他贷款 计数
                dict_out['loan_second_m24_now_bt_other_person_ratio'] = loan_second_m24_now[loan_second_m24_now['businessType']=='其他个人消费贷款'].shape[0]/len(loan_second_m24_now) #近24个月开立 现行贷款账户业务种类为其他个人消费贷款 占比
                dict_out['loan_second_m24_now_repayFrequency_giniimpurity'] = giniimpurity(loan_second_m24_now['repayFrequency']) #近24个月开立 现行贷款账户还款频率基尼不纯度
                dict_out['loan_second_m24_now_org_consumer_finance_ratio'] = loan_second_m24_now[loan_second_m24_now['org']=='消费金融公司'].shape[0]/len(loan_second_m24_now) #近24个月开立 现行贷款账户管理机构为消费金融公司 占比
                dict_out['loan_second_m24_now_loanAmount_max'] = loan_second_m24_now['loanAmount'].max() #var最大值比率
                dict_out['loan_second_m24_now_month60_to_report_mean_mean'] = loan_second_m24_now['month60_to_report_mean'].mean()  #var平均值比率
                dict_out['loan_second_m24_now_month60_to_report_max_sum'] = loan_second_m24_now['month60_to_report_max'].sum() #var求和比率
                dict_out['loan_second_m24_now_byDate_to_report_sum'] = loan_second_m24_now['byDate_to_report'].sum() #var求和比率
                dict_out['loan_second_m24_now_repayTerms_max'] = loan_second_m24_now['repayTerms'].max()#var最大值
                dict_out['loan_second_m24_now_repayTerms_min'] = loan_second_m24_now['repayTerms'].min() #var最小值比率
                dict_out['loan_second_m24_now_repayTerms_range'] = (dict_out['loan_second_m24_now_repayTerms_max']-dict_out['loan_second_m24_now_repayTerms_min'])/dict_out['loan_second_m24_now_repayTerms_max'] if dict_out['loan_second_m24_now_repayTerms_max']>0 else np.nan #var区间率
                dict_out['loan_second_m24_now_month60_to_report_min_mean'] = loan_second_m24_now['month60_to_report_min'].mean()  #var平均值比率
                dict_out['loan_second_m24_now_month60_to_report_mean_max'] = loan_second_m24_now['month60_to_report_mean'].max()#var最大值
                dict_out['loan_second_m24_now_month60_to_report_mean_min'] = loan_second_m24_now['month60_to_report_mean'].min() #var最小值比率
                dict_out['loan_second_m24_now_month60_to_report_mean_range'] = (dict_out['loan_second_m24_now_month60_to_report_mean_max']-dict_out['loan_second_m24_now_month60_to_report_mean_min'])/dict_out['loan_second_m24_now_month60_to_report_mean_max'] if dict_out['loan_second_m24_now_month60_to_report_mean_max']>0 else np.nan #var区间率
                dict_out['loan_second_m24_now_repayMons_max'] = loan_second_m24_now['repayMons'].max() #var最大值比率
                dict_out['loan_second_m24_now_month60_State_countNullr_mean'] = loan_second_m24_now['month60_State_countNullr'].mean()  #var平均值比率
                dict_out['loan_second_m24_now_month60_State_countNull_mean'] = loan_second_m24_now['month60_State_countNull'].mean()  #var平均值比率
                dict_out['loan_second_m24_now_month60_State_countNull_max'] = loan_second_m24_now['month60_State_countNull'].max() #var最大值比率
                dict_out['loan_second_m24_now_month60_State_countN_mean'] = loan_second_m24_now['month60_State_countN'].mean()  #var平均值比率
                dict_out['loan_second_m24_now_month60_to_report_min_max'] = loan_second_m24_now['month60_to_report_min'].max() #var最大值比率
                dict_out['loan_second_m24_now_repayTerms_min'] = loan_second_m24_now['repayTerms'].min() #var最小值比率
                dict_out['loan_second_m24_now_month60_State_countN_sum'] = loan_second_m24_now['month60_State_countN'].sum() #var求和比率
                dict_out['loan_second_m24_now_repayTerms_sum'] = loan_second_m24_now['repayTerms'].sum() #var求和比率
                dict_out['loan_second_m24_now_month60_to_report_mean_max'] = loan_second_m24_now['month60_to_report_mean'].max() #var最大值比率
                dict_out['loan_second_m24_now_repayMons_ratio_mean'] = loan_second_m24_now['repayMons_ratio'].mean()  #var平均值比率
                dict_out['loan_second_m24_now_startDate_to_report_max'] = loan_second_m24_now['startDate_to_report'].max() #var最大值比率
                dict_out['loan_second_m24_now_month60_State_countN_max'] = loan_second_m24_now['month60_State_countN'].max() #var最大值比率
                dict_out['loan_second_m24_now_loanAmount_mean'] = loan_second_m24_now['loanAmount'].mean()  #var平均值比率
                dict_out['loan_second_m24_now_loanAmount_max'] = loan_second_m24_now['loanAmount'].max()#var最大值
                dict_out['loan_second_m24_now_loanAmount_min'] = loan_second_m24_now['loanAmount'].min() #var最小值比率
                dict_out['loan_second_m24_now_loanAmount_range'] = (dict_out['loan_second_m24_now_loanAmount_max']-dict_out['loan_second_m24_now_loanAmount_min'])/dict_out['loan_second_m24_now_loanAmount_max'] if dict_out['loan_second_m24_now_loanAmount_max']>0 else np.nan #var区间率
                dict_out['loan_second_m24_now_month60_to_report_max_max'] = loan_second_m24_now['month60_to_report_max'].max() #var最大值比率
                dict_out['loan_second_m24_now_repayMons_ratio_sum'] = loan_second_m24_now['repayMons_ratio'].sum() #var求和比率
                dict_out['loan_second_m24_now_byDate_to_report_mean'] = loan_second_m24_now['byDate_to_report'].mean()  #var平均值比率
                dict_out['loan_second_m24_now_repayAmt_mean'] = loan_second_m24_now['repayAmt'].mean()  #var平均值比率
                dict_out['loan_second_m24_now_balance_ratio_max'] = loan_second_m24_now['balance_ratio'].max()#var最大值
                dict_out['loan_second_m24_now_balance_ratio_min'] = loan_second_m24_now['balance_ratio'].min() #var最小值比率
                dict_out['loan_second_m24_now_balance_ratio_range'] = (dict_out['loan_second_m24_now_balance_ratio_max']-dict_out['loan_second_m24_now_balance_ratio_min'])/dict_out['loan_second_m24_now_balance_ratio_max'] if dict_out['loan_second_m24_now_balance_ratio_max']>0 else np.nan #var区间率
                dict_out['loan_second_m24_now_bt_person_business_ratio'] = loan_second_m24_now[loan_second_m24_now['businessType']=='个人经营性贷款'].shape[0]/len(loan_second_m24_now) #近24个月开立 现行贷款账户业务种类为个人经营性贷款 占比
                dict_out['loan_second_m24_nowR_bt_person_business_ratio'] = dict_out['loan_second_m24_now_bt_person_business_ratio']/dict_out['loan_second_m24_bt_person_business_ratio'] if dict_out['loan_second_m24_bt_person_business_ratio']>0 else np.nan #var最大值比率
                dict_out['loan_second_m24_nowR_loanAmount_max'] = dict_out['loan_second_m24_now_loanAmount_max']/dict_out['loan_second_m24_loanAmount_max'] if dict_out['loan_second_m24_loanAmount_max']>0 else np.nan #var最大值比率
                dict_out['loan_second_m24_nowR_month60_to_report_mean_mean'] = dict_out['loan_second_m24_now_month60_to_report_mean_mean']/dict_out['loan_second_m24_month60_to_report_mean_mean'] if dict_out['loan_second_m24_month60_to_report_mean_mean']>0 else np.nan #var最大值比率
                dict_out['loan_second_m24_nowR_month60_to_report_max_sum'] = dict_out['loan_second_m24_now_month60_to_report_max_sum']/dict_out['loan_second_m24_month60_to_report_max_sum'] if dict_out['loan_second_m24_month60_to_report_max_sum']>0 else np.nan #var最大值比率
                dict_out['loan_second_m24_nowR_byDate_to_report_sum'] = dict_out['loan_second_m24_now_byDate_to_report_sum']/dict_out['loan_second_m24_byDate_to_report_sum'] if dict_out['loan_second_m24_byDate_to_report_sum']>0 else np.nan #var最大值比率
                dict_out['loan_second_m24_now_org_commercial_bank_ratio'] = loan_second_m24_now[loan_second_m24_now['org']=='商业银行'].shape[0]/len(loan_second_m24_now) #近24个月开立 现行贷款账户管理机构为商业银行 占比
                dict_out['loan_second_m24_nowR_org_commercial_bank_ratio'] = dict_out['loan_second_m24_now_org_commercial_bank_ratio']/dict_out['loan_second_m24_org_commercial_bank_ratio'] if dict_out['loan_second_m24_org_commercial_bank_ratio']>0 else np.nan #var最大值比率
                dict_out['loan_second_m24_now_rf_once_count'] = loan_second_m24_now[loan_second_m24_now['repayFrequency']=='一次性'].shape[0] #近24个月开立 现行贷款账户还款频率为一次性 计数
                dict_out['loan_second_m24_nowR_rf_once_count'] = dict_out['loan_second_m24_now_rf_once_count']/dict_out['loan_second_m24_rf_once_count'] if dict_out['loan_second_m24_rf_once_count']>0 else np.nan #var最大值比率
                dict_out['loan_second_m24_nowR_repayTerms_range'] = dict_out['loan_second_m24_now_repayTerms_range']/dict_out['loan_second_m24_repayTerms_range'] if dict_out['loan_second_m24_repayTerms_range']>0 else np.nan #var最大值比率
                dict_out['loan_second_m24_nowR_month60_to_report_min_mean'] = dict_out['loan_second_m24_now_month60_to_report_min_mean']/dict_out['loan_second_m24_month60_to_report_min_mean'] if dict_out['loan_second_m24_month60_to_report_min_mean']>0 else np.nan #var最大值比率
                dict_out['loan_second_m24_now_rf_month_ratio'] = loan_second_m24_now[loan_second_m24_now['repayFrequency']=='月'].shape[0]/len(loan_second_m24_now) #近24个月开立 现行贷款账户还款频率为月 占比
                dict_out['loan_second_m24_nowR_rf_month_ratio'] = dict_out['loan_second_m24_now_rf_month_ratio']/dict_out['loan_second_m24_rf_month_ratio'] if dict_out['loan_second_m24_rf_month_ratio']>0 else np.nan #var最大值比率
                dict_out['loan_second_m24_nowR_month60_to_report_mean_range'] = dict_out['loan_second_m24_now_month60_to_report_mean_range']/dict_out['loan_second_m24_month60_to_report_mean_range'] if dict_out['loan_second_m24_month60_to_report_mean_range']>0 else np.nan #var最大值比率
                dict_out['loan_second_m24_nowR_repayMons_max'] = dict_out['loan_second_m24_now_repayMons_max']/dict_out['loan_second_m24_repayMons_max'] if dict_out['loan_second_m24_repayMons_max']>0 else np.nan #var最大值比率
                dict_out['loan_second_m24_nowR_month60_State_countNullr_mean'] = dict_out['loan_second_m24_now_month60_State_countNullr_mean']/dict_out['loan_second_m24_month60_State_countNullr_mean'] if dict_out['loan_second_m24_month60_State_countNullr_mean']>0 else np.nan #var最大值比率
                dict_out['loan_second_m24_nowR_month60_State_countNull_mean'] = dict_out['loan_second_m24_now_month60_State_countNull_mean']/dict_out['loan_second_m24_month60_State_countNull_mean'] if dict_out['loan_second_m24_month60_State_countNull_mean']>0 else np.nan #var最大值比率
                dict_out['loan_second_m24_nowR_month60_State_countNull_max'] = dict_out['loan_second_m24_now_month60_State_countNull_max']/dict_out['loan_second_m24_month60_State_countNull_max'] if dict_out['loan_second_m24_month60_State_countNull_max']>0 else np.nan #var最大值比率
                dict_out['loan_second_m24_nowR_month60_State_countN_mean'] = dict_out['loan_second_m24_now_month60_State_countN_mean']/dict_out['loan_second_m24_month60_State_countN_mean'] if dict_out['loan_second_m24_month60_State_countN_mean']>0 else np.nan #var最大值比率
                dict_out['loan_second_m24_nowR_month60_to_report_min_max'] = dict_out['loan_second_m24_now_month60_to_report_min_max']/dict_out['loan_second_m24_month60_to_report_min_max'] if dict_out['loan_second_m24_month60_to_report_min_max']>0 else np.nan #var最大值比率
                dict_out['loan_second_m24_nowR_repayTerms_min'] = dict_out['loan_second_m24_now_repayTerms_min']/dict_out['loan_second_m24_repayTerms_min'] if dict_out['loan_second_m24_repayTerms_min']>0 else np.nan #var最大值比率
                dict_out['loan_second_m24_nowR_month60_State_countN_sum'] = dict_out['loan_second_m24_now_month60_State_countN_sum']/dict_out['loan_second_m24_month60_State_countN_sum'] if dict_out['loan_second_m24_month60_State_countN_sum']>0 else np.nan #var最大值比率
                dict_out['loan_second_m24_now_rf_other_ratio'] = loan_second_m24_now[loan_second_m24_now['repayFrequency']=='其他'].shape[0]/len(loan_second_m24_now) #近24个月开立 现行贷款账户还款频率为其他 占比
                dict_out['loan_second_m24_nowR_rf_other_ratio'] = dict_out['loan_second_m24_now_rf_other_ratio']/dict_out['loan_second_m24_rf_other_ratio'] if dict_out['loan_second_m24_rf_other_ratio']>0 else np.nan #var最大值比率
                dict_out['loan_second_m24_now_rf_once_ratio'] = loan_second_m24_now[loan_second_m24_now['repayFrequency']=='一次性'].shape[0]/len(loan_second_m24_now) #近24个月开立 现行贷款账户还款频率为一次性 占比
                dict_out['loan_second_m24_nowR_rf_once_ratio'] = dict_out['loan_second_m24_now_rf_once_ratio']/dict_out['loan_second_m24_rf_once_ratio'] if dict_out['loan_second_m24_rf_once_ratio']>0 else np.nan #var最大值比率
                dict_out['loan_second_m24_nowR_repayTerms_sum'] = dict_out['loan_second_m24_now_repayTerms_sum']/dict_out['loan_second_m24_repayTerms_sum'] if dict_out['loan_second_m24_repayTerms_sum']>0 else np.nan #var最大值比率
                dict_out['loan_second_m24_nowR_month60_to_report_mean_max'] = dict_out['loan_second_m24_now_month60_to_report_mean_max']/dict_out['loan_second_m24_month60_to_report_mean_max'] if dict_out['loan_second_m24_month60_to_report_mean_max']>0 else np.nan #var最大值比率
                dict_out['loan_second_m24_nowR_repayMons_ratio_mean'] = dict_out['loan_second_m24_now_repayMons_ratio_mean']/dict_out['loan_second_m24_repayMons_ratio_mean'] if dict_out['loan_second_m24_repayMons_ratio_mean']>0 else np.nan #var最大值比率
                dict_out['loan_second_m24_nowR_startDate_to_report_max'] = dict_out['loan_second_m24_now_startDate_to_report_max']/dict_out['loan_second_m24_startDate_to_report_max'] if dict_out['loan_second_m24_startDate_to_report_max']>0 else np.nan #var最大值比率
                dict_out['loan_second_m24_nowR_month60_State_countN_max'] = dict_out['loan_second_m24_now_month60_State_countN_max']/dict_out['loan_second_m24_month60_State_countN_max'] if dict_out['loan_second_m24_month60_State_countN_max']>0 else np.nan #var最大值比率
                dict_out['loan_second_m24_nowR_loanAmount_mean'] = dict_out['loan_second_m24_now_loanAmount_mean']/dict_out['loan_second_m24_loanAmount_mean'] if dict_out['loan_second_m24_loanAmount_mean']>0 else np.nan #var最大值比率
                dict_out['loan_second_m24_nowR_loanAmount_range'] = dict_out['loan_second_m24_now_loanAmount_range']/dict_out['loan_second_m24_loanAmount_range'] if dict_out['loan_second_m24_loanAmount_range']>0 else np.nan #var最大值比率
                dict_out['loan_second_m24_now_org_consumer_finance_count'] = loan_second_m24_now[loan_second_m24_now['org']=='消费金融公司'].shape[0] #近24个月开立 现行贷款账户管理机构为消费金融公司 计数
                dict_out['loan_second_m24_nowR_org_consumer_finance_count'] = dict_out['loan_second_m24_now_org_consumer_finance_count']/dict_out['loan_second_m24_org_consumer_finance_count'] if dict_out['loan_second_m24_org_consumer_finance_count']>0 else np.nan #var最大值比率
                dict_out['loan_second_m24_now_bt_other_loan_count'] = loan_second_m24_now[loan_second_m24_now['businessType']=='其他贷款'].shape[0] #近24个月开立 现行贷款账户业务种类为其他贷款 计数
                dict_out['loan_second_m24_nowR_bt_other_loan_count'] = dict_out['loan_second_m24_now_bt_other_loan_count']/dict_out['loan_second_m24_bt_other_loan_count'] if dict_out['loan_second_m24_bt_other_loan_count']>0 else np.nan #var最大值比率
                dict_out['loan_second_m24_now_bt_other_person_ratio'] = loan_second_m24_now[loan_second_m24_now['businessType']=='其他个人消费贷款'].shape[0]/len(loan_second_m24_now) #近24个月开立 现行贷款账户业务种类为其他个人消费贷款 占比
                dict_out['loan_second_m24_nowR_bt_other_person_ratio'] = dict_out['loan_second_m24_now_bt_other_person_ratio']/dict_out['loan_second_m24_bt_other_person_ratio'] if dict_out['loan_second_m24_bt_other_person_ratio']>0 else np.nan #var最大值比率
                dict_out['loan_second_m24_now_repayFrequency_giniimpurity'] = giniimpurity(loan_second_m24_now['repayFrequency']) #近24个月开立 现行贷款账户还款频率基尼不纯度
                dict_out['loan_second_m24_nowR_repayFrequency_giniimpurity'] = dict_out['loan_second_m24_now_repayFrequency_giniimpurity']/dict_out['loan_second_m24_repayFrequency_giniimpurity'] if dict_out['loan_second_m24_repayFrequency_giniimpurity']>0 else np.nan #var最大值比率
                dict_out['loan_second_m24_nowR_month60_to_report_max_max'] = dict_out['loan_second_m24_now_month60_to_report_max_max']/dict_out['loan_second_m24_month60_to_report_max_max'] if dict_out['loan_second_m24_month60_to_report_max_max']>0 else np.nan #var最大值比率
                dict_out['loan_second_m24_nowR_repayMons_ratio_sum'] = dict_out['loan_second_m24_now_repayMons_ratio_sum']/dict_out['loan_second_m24_repayMons_ratio_sum'] if dict_out['loan_second_m24_repayMons_ratio_sum']>0 else np.nan #var最大值比率
                dict_out['loan_second_m24_nowR_byDate_to_report_mean'] = dict_out['loan_second_m24_now_byDate_to_report_mean']/dict_out['loan_second_m24_byDate_to_report_mean'] if dict_out['loan_second_m24_byDate_to_report_mean']>0 else np.nan #var最大值比率
                dict_out['loan_second_m24_now_org_consumer_finance_ratio'] = loan_second_m24_now[loan_second_m24_now['org']=='消费金融公司'].shape[0]/len(loan_second_m24_now) #近24个月开立 现行贷款账户管理机构为消费金融公司 占比
                dict_out['loan_second_m24_nowR_org_consumer_finance_ratio'] = dict_out['loan_second_m24_now_org_consumer_finance_ratio']/dict_out['loan_second_m24_org_consumer_finance_ratio'] if dict_out['loan_second_m24_org_consumer_finance_ratio']>0 else np.nan #var最大值比率
                dict_out['loan_second_m24_nowR_repayAmt_mean'] = dict_out['loan_second_m24_now_repayAmt_mean']/dict_out['loan_second_m24_repayAmt_mean'] if dict_out['loan_second_m24_repayAmt_mean']>0 else np.nan #var最大值比率
                dict_out['loan_second_m24_nowR_balance_ratio_range'] = dict_out['loan_second_m24_now_balance_ratio_range']/dict_out['loan_second_m24_balance_ratio_range'] if dict_out['loan_second_m24_balance_ratio_range']>0 else np.nan #var最大值比率

            #近24个月开立 非循环贷账户
            loan_second_m24_ncycle = loan_second_m24[loan_second_m24['class']=='非循环贷账户']
            if len(loan_second_m24_ncycle)>0:
                dict_out['loan_second_m24_ncycle_repayMons_ratio_mean'] = loan_second_m24_ncycle['repayMons_ratio'].mean() #var平均值
                dict_out['loan_second_m24_ncycle_leftRepayTerms_max'] = loan_second_m24_ncycle['leftRepayTerms'].max()#var最大值
                dict_out['loan_second_m24_ncycle_startDate_to_report_max'] = loan_second_m24_ncycle['startDate_to_report'].max()#var最大值
                dict_out['loan_second_m24_ncycle_month60_to_report_mean_mean'] = loan_second_m24_ncycle['month60_to_report_mean'].mean() #var平均值
                dict_out['loan_second_m24_ncycle_loanGrantOrg_nunique'] = loan_second_m24_ncycle['loanGrantOrg'].nunique() #近24个月开立 非循环贷账户 贷款账户管理机构详细nunique
                dict_out['loan_second_m24_ncycle_gf_crdit_ratio'] = loan_second_m24_ncycle[loan_second_m24_ncycle['guaranteeForm']=='信用/免担保'].shape[0]/len(loan_second_m24_ncycle) #近24个月开立 非循环贷账户 贷款账户担保方式为信用/免担保 占比
                dict_out['loan_second_m24_ncycle_repayTerms_mean'] = loan_second_m24_ncycle['repayTerms'].mean() #var平均值
                dict_out['loan_second_m24_ncycle_org_trust_company_ratio'] = loan_second_m24_ncycle[loan_second_m24_ncycle['org']=='信托公司'].shape[0]/len(loan_second_m24_ncycle) #近24个月开立 非循环贷账户 贷款账户管理机构为信托公司 占比
                dict_out['loan_second_m24_ncycle_month60_State_num_mean_max'] = loan_second_m24_ncycle['month60_State_num_mean'].max()#var最大值
                dict_out['loan_second_m24_ncycle_month60_State_countNr_sum'] = loan_second_m24_ncycle['month60_State_countNr'].sum() #var求和
                dict_out['loan_second_m24_ncycle_businessType_giniimpurity'] = giniimpurity(loan_second_m24_ncycle['businessType']) #近24个月开立 非循环贷账户 贷款账户业务种类基尼不纯度
                dict_out['loan_second_m24_ncycle_startDate_to_report_sum'] = loan_second_m24_ncycle['startDate_to_report'].sum() #var求和
                dict_out['loan_second_m24_ncycle_rf_once_count'] = loan_second_m24_ncycle[loan_second_m24_ncycle['repayFrequency']=='一次性'].shape[0] #近24个月开立 非循环贷账户 贷款账户还款频率为一次性 计数
                dict_out['loan_second_m24_ncycle_logo_mean'] = loan_second_m24_ncycle['logo'].mean() #var平均值
                dict_out['loan_second_m24_ncycle_month60_State_countCr_max'] = loan_second_m24_ncycle['month60_State_countCr'].max()#var最大值
                dict_out['loan_second_m24_ncycle_loanAmount_min'] = loan_second_m24_ncycle['loanAmount'].min() #var最小值
                dict_out['loan_second_m24_ncycle_month60_State_countNull_mean'] = loan_second_m24_ncycle['month60_State_countNull'].mean() #var平均值
                dict_out['loan_second_m24_ncycle_month60_to_report_mean_sum'] = loan_second_m24_ncycle['month60_to_report_mean'].sum() #var求和
                dict_out['loan_second_m24_ncycle_org_consumer_finance_count'] = loan_second_m24_ncycle[loan_second_m24_ncycle['org']=='消费金融公司'].shape[0] #近24个月开立 非循环贷账户 贷款账户管理机构为消费金融公司 计数
                dict_out['loan_second_m24_ncycle_month60_State_countNr_mean'] = loan_second_m24_ncycle['month60_State_countNr'].mean() #var平均值
                dict_out['loan_second_m24_ncycle_month60_State_num_size_min'] = loan_second_m24_ncycle['month60_State_num_size'].min() #var最小值
                dict_out['loan_second_m24_ncycle_repayTerm_ratio_max'] = loan_second_m24_ncycle['repayTerm_ratio'].max()#var最大值
                dict_out['loan_second_m24_ncycle_loanGrantOrg_giniimpurity'] = giniimpurity(loan_second_m24_ncycle['loanGrantOrg']) #近24个月开立 非循环贷账户 贷款账户管理机构详细基尼不纯度
                dict_out['loan_second_m24_ncycle_rf_month_ratio'] = loan_second_m24_ncycle[loan_second_m24_ncycle['repayFrequency']=='月'].shape[0]/len(loan_second_m24_ncycle) #近24个月开立 非循环贷账户 贷款账户还款频率为月 占比
                dict_out['loan_second_m24_ncycle_repayAmt_max'] = loan_second_m24_ncycle['repayAmt'].max()#var最大值
                dict_out['loan_second_m24_ncycle_org_other_ratio'] = loan_second_m24_ncycle[loan_second_m24_ncycle['org']=='其他机构'].shape[0]/len(loan_second_m24_ncycle) #近24个月开立 非循环贷账户 贷款账户管理机构为其他机构 占比
                dict_out['loan_second_m24_ncycle_month60_State_num_size_mean'] = loan_second_m24_ncycle['month60_State_num_size'].mean() #var平均值
                dict_out['loan_second_m24_ncycle_org_trust_company_count'] = loan_second_m24_ncycle[loan_second_m24_ncycle['org']=='信托公司'].shape[0] #近24个月开立 非循环贷账户 贷款账户管理机构为信托公司 计数
                dict_out['loan_second_m24_ncycle_balance_mean'] = loan_second_m24_ncycle['balance'].mean() #var平均值
                dict_out['loan_second_m24_ncycle_repayTerm_ratio_max'] = loan_second_m24_ncycle['repayTerm_ratio'].max()#var最大值
                dict_out['loan_second_m24_ncycle_repayTerm_ratio_min'] = loan_second_m24_ncycle['repayTerm_ratio'].min() #var最小值
                dict_out['loan_second_m24_ncycle_repayTerm_ratio_range'] = (dict_out['loan_second_m24_ncycle_repayTerm_ratio_max']-dict_out['loan_second_m24_ncycle_repayTerm_ratio_min'])/dict_out['loan_second_m24_ncycle_repayTerm_ratio_max'] if dict_out['loan_second_m24_ncycle_repayTerm_ratio_max']>0 else np.nan #var区间率
                dict_out['loan_second_m24_ncycle_repayFrequency_giniimpurity'] = giniimpurity(loan_second_m24_ncycle['repayFrequency']) #近24个月开立 非循环贷账户 贷款账户还款频率基尼不纯度
                dict_out['loan_second_m24_ncycle_byDate_to_report_max'] = loan_second_m24_ncycle['byDate_to_report'].max()#var最大值
                dict_out['loan_second_m24_ncycle_byDate_to_report_min'] = loan_second_m24_ncycle['byDate_to_report'].min() #var最小值
                dict_out['loan_second_m24_ncycle_byDate_to_report_range'] = (dict_out['loan_second_m24_ncycle_byDate_to_report_max']-dict_out['loan_second_m24_ncycle_byDate_to_report_min'])/dict_out['loan_second_m24_ncycle_byDate_to_report_max'] if dict_out['loan_second_m24_ncycle_byDate_to_report_max']>0 else np.nan #var区间率
                dict_out['loan_second_m24_ncycle_org_giniimpurity'] = giniimpurity(loan_second_m24_ncycle['org']) #近24个月开立 非循环贷账户 贷款账户管理机构基尼不纯度
                dict_out['loan_second_m24_ncycle_repayAmt_max'] = loan_second_m24_ncycle['repayAmt'].max()#var最大值
                dict_out['loan_second_m24_ncycle_repayAmt_min'] = loan_second_m24_ncycle['repayAmt'].min() #var最小值
                dict_out['loan_second_m24_ncycle_repayAmt_range'] = (dict_out['loan_second_m24_ncycle_repayAmt_max']-dict_out['loan_second_m24_ncycle_repayAmt_min'])/dict_out['loan_second_m24_ncycle_repayAmt_max'] if dict_out['loan_second_m24_ncycle_repayAmt_max']>0 else np.nan #var区间率
                dict_out['loan_second_m24_ncycle_rf_other_ratio'] = loan_second_m24_ncycle[loan_second_m24_ncycle['repayFrequency']=='其他'].shape[0]/len(loan_second_m24_ncycle) #近24个月开立 非循环贷账户 贷款账户还款频率为其他 占比
                dict_out['loan_second_m24_ncycle_is_now_max'] = loan_second_m24_ncycle['is_now'].max()#var最大值
                dict_out['loan_second_m24_ncycle_is_now_min'] = loan_second_m24_ncycle['is_now'].min() #var最小值
                dict_out['loan_second_m24_ncycle_is_now_range'] = (dict_out['loan_second_m24_ncycle_is_now_max']-dict_out['loan_second_m24_ncycle_is_now_min'])/dict_out['loan_second_m24_ncycle_is_now_max'] if dict_out['loan_second_m24_ncycle_is_now_max']>0 else np.nan #var区间率
                dict_out['loan_second_m24_ncycle_month60_to_report_max_mean'] = loan_second_m24_ncycle['month60_to_report_max'].mean() #var平均值
                dict_out['loan_second_m24_ncycle_month60_to_report_min_max'] = loan_second_m24_ncycle['month60_to_report_min'].max()#var最大值
                dict_out['loan_second_m24_ncycle_RepayedAmount_ratio_mean'] = loan_second_m24_ncycle['RepayedAmount_ratio'].mean() #var平均值
                dict_out['loan_second_m24_ncycle_planRepayAmount_mean'] = loan_second_m24_ncycle['planRepayAmount'].mean() #var平均值
                dict_out['loan_second_m24_ncycle_month60_to_report_max_max'] = loan_second_m24_ncycle['month60_to_report_max'].max()#var最大值
                dict_out['loan_second_m24_ncycle_repayTerms_max'] = loan_second_m24_ncycle['repayTerms'].max()#var最大值
                dict_out['loan_second_m24_ncycle_repayTerms_min'] = loan_second_m24_ncycle['repayTerms'].min() #var最小值
                dict_out['loan_second_m24_ncycle_repayTerms_range'] = (dict_out['loan_second_m24_ncycle_repayTerms_max']-dict_out['loan_second_m24_ncycle_repayTerms_min'])/dict_out['loan_second_m24_ncycle_repayTerms_max'] if dict_out['loan_second_m24_ncycle_repayTerms_max']>0 else np.nan #var区间率
                dict_out['loan_second_m24_ncycle_month60_State_countCr_mean'] = loan_second_m24_ncycle['month60_State_countCr'].mean() #var平均值
                dict_out['loan_second_m24_ncycle_logo_max'] = loan_second_m24_ncycle['logo'].max()#var最大值
                dict_out['loan_second_m24_ncycle_month60_State_countCr_sum'] = loan_second_m24_ncycle['month60_State_countCr'].sum() #var求和
                dict_out['loan_second_m24_ncycle_org_consumer_finance_ratio'] = loan_second_m24_ncycle[loan_second_m24_ncycle['org']=='消费金融公司'].shape[0]/len(loan_second_m24_ncycle) #近24个月开立 非循环贷账户 贷款账户管理机构为消费金融公司 占比
                dict_out['loan_second_m24_ncycle_loanAmount_max'] = loan_second_m24_ncycle['loanAmount'].max()#var最大值
                dict_out['loan_second_m24_ncycle_loanAmount_min'] = loan_second_m24_ncycle['loanAmount'].min() #var最小值
                dict_out['loan_second_m24_ncycle_loanAmount_range'] = (dict_out['loan_second_m24_ncycle_loanAmount_max']-dict_out['loan_second_m24_ncycle_loanAmount_min'])/dict_out['loan_second_m24_ncycle_loanAmount_max'] if dict_out['loan_second_m24_ncycle_loanAmount_max']>0 else np.nan #var区间率
                dict_out['loan_second_m24_ncycle_guaranteeForm_giniimpurity'] = giniimpurity(loan_second_m24_ncycle['guaranteeForm']) #近24个月开立 非循环贷账户 贷款账户担保方式基尼不纯度
                dict_out['loan_second_m24_ncycle_repayMons_min'] = loan_second_m24_ncycle['repayMons'].min() #var最小值
                dict_out['loan_second_m24_ncycle_bt_other_person_ratio'] = loan_second_m24_ncycle[loan_second_m24_ncycle['businessType']=='其他个人消费贷款'].shape[0]/len(loan_second_m24_ncycle) #近24个月开立 非循环贷账户 贷款账户业务种类为其他个人消费贷款 占比
                dict_out['loan_second_m24_ncycle_month60_State_countUnknowr_mean'] = loan_second_m24_ncycle['month60_State_countUnknowr'].mean() #var平均值
                dict_out['loan_second_m24_ncycle_balance_ratio_min'] = loan_second_m24_ncycle['balance_ratio'].min() #var最小值
                dict_out['loan_second_m24_ncycle_balance_ratio_max'] = loan_second_m24_ncycle['balance_ratio'].max()#var最大值
                dict_out['loan_second_m24_ncycle_month60_Amount_num_mean_sum'] = loan_second_m24_ncycle['month60_Amount_num_mean'].sum() #var求和
                dict_out['loan_second_m24_ncycle_month60_State_countNr_max'] = loan_second_m24_ncycle['month60_State_countNr'].max()#var最大值
                dict_out['loan_second_m24_ncycle_month60_State_countNr_min'] = loan_second_m24_ncycle['month60_State_countNr'].min() #var最小值
                dict_out['loan_second_m24_ncycle_month60_State_countNr_range'] = (dict_out['loan_second_m24_ncycle_month60_State_countNr_max']-dict_out['loan_second_m24_ncycle_month60_State_countNr_min'])/dict_out['loan_second_m24_ncycle_month60_State_countNr_max'] if dict_out['loan_second_m24_ncycle_month60_State_countNr_max']>0 else np.nan #var区间率
                dict_out['loan_second_m24_ncycle_classify5_num_min'] = loan_second_m24_ncycle['classify5_num'].min() #var最小值
                dict_out['loan_second_m24_ncycle_repayAmt_mean'] = loan_second_m24_ncycle['repayAmt'].mean() #var平均值
                dict_out['loan_second_m24_ncycle_month60_State_countC_sum'] = loan_second_m24_ncycle['month60_State_countC'].sum() #var求和
                dict_out['loan_second_m24_ncycle_month60_to_report_mean_max'] = loan_second_m24_ncycle['month60_to_report_mean'].max()#var最大值
                dict_out['loan_second_m24_ncycle_month60_State_num_size_max'] = loan_second_m24_ncycle['month60_State_num_size'].max()#var最大值
                dict_out['loan_second_m24_ncycle_month60_State_num_size_min'] = loan_second_m24_ncycle['month60_State_num_size'].min() #var最小值
                dict_out['loan_second_m24_ncycle_month60_State_num_size_range'] = (dict_out['loan_second_m24_ncycle_month60_State_num_size_max']-dict_out['loan_second_m24_ncycle_month60_State_num_size_min'])/dict_out['loan_second_m24_ncycle_month60_State_num_size_max'] if dict_out['loan_second_m24_ncycle_month60_State_num_size_max']>0 else np.nan #var区间率
                dict_out['loan_second_m24_ncycle_repayFrequency_giniimpurity'] = giniimpurity(loan_second_m24_ncycle['repayFrequency']) #近24个月开立 非循环贷账户 贷款账户还款频率基尼不纯度
                dict_out['loan_second_m24_ncycle_org_trust_company_count'] = loan_second_m24_ncycle[loan_second_m24_ncycle['org']=='信托公司'].shape[0] #近24个月开立 非循环贷账户 贷款账户管理机构为信托公司 计数
                dict_out['loan_second_m24_ncycle_rf_once_ratio'] = loan_second_m24_ncycle[loan_second_m24_ncycle['repayFrequency']=='一次性'].shape[0]/len(loan_second_m24_ncycle) #近24个月开立 非循环贷账户 贷款账户还款频率为一次性 占比
                dict_out['loan_second_m24_ncycle_month60_State_countNr_mean'] = loan_second_m24_ncycle['month60_State_countNr'].mean()  #var平均值比率
                dict_out['loan_second_m24_ncycle_month60_State_countCr_max'] = loan_second_m24_ncycle['month60_State_countCr'].max() #var最大值比率
                dict_out['loan_second_m24_ncycle_month60_State_countCr_max'] = loan_second_m24_ncycle['month60_State_countCr'].max()#var最大值
                dict_out['loan_second_m24_ncycle_month60_State_countCr_min'] = loan_second_m24_ncycle['month60_State_countCr'].min() #var最小值比率
                dict_out['loan_second_m24_ncycle_month60_State_countCr_range'] = (dict_out['loan_second_m24_ncycle_month60_State_countCr_max']-dict_out['loan_second_m24_ncycle_month60_State_countCr_min'])/dict_out['loan_second_m24_ncycle_month60_State_countCr_max'] if dict_out['loan_second_m24_ncycle_month60_State_countCr_max']>0 else np.nan #var区间率
                dict_out['loan_second_m24_ncycle_month60_State_countNullr_mean'] = loan_second_m24_ncycle['month60_State_countNullr'].mean()  #var平均值比率
                dict_out['loan_second_m24_ncycle_month60_State_countNr_min'] = loan_second_m24_ncycle['month60_State_countNr'].min() #var最小值比率
                dict_out['loan_second_m24_ncycle_month60_State_countNr_sum'] = loan_second_m24_ncycle['month60_State_countNr'].sum() #var求和比率
                dict_out['loan_second_m24_ncycle_byDate_to_report_max'] = loan_second_m24_ncycle['byDate_to_report'].max() #var最大值比率
                dict_out['loan_second_m24_ncycle_month60_to_report_min_max'] = loan_second_m24_ncycle['month60_to_report_min'].max()#var最大值
                dict_out['loan_second_m24_ncycle_month60_to_report_min_min'] = loan_second_m24_ncycle['month60_to_report_min'].min() #var最小值比率
                dict_out['loan_second_m24_ncycle_month60_to_report_min_range'] = (dict_out['loan_second_m24_ncycle_month60_to_report_min_max']-dict_out['loan_second_m24_ncycle_month60_to_report_min_min'])/dict_out['loan_second_m24_ncycle_month60_to_report_min_max'] if dict_out['loan_second_m24_ncycle_month60_to_report_min_max']>0 else np.nan #var区间率
                dict_out['loan_second_m24_ncycleR_month60_State_countNr_mean'] = dict_out['loan_second_m24_ncycle_month60_State_countNr_mean']/dict_out['loan_second_m24_month60_State_countNr_mean'] if dict_out['loan_second_m24_month60_State_countNr_mean']>0 else np.nan #var最大值比率
                dict_out['loan_second_m24_ncycleR_month60_State_countCr_max'] = dict_out['loan_second_m24_ncycle_month60_State_countCr_max']/dict_out['loan_second_m24_month60_State_countCr_max'] if dict_out['loan_second_m24_month60_State_countCr_max']>0 else np.nan #var最大值比率
                dict_out['loan_second_m24_ncycle_repayFrequency_giniimpurity'] = giniimpurity(loan_second_m24_ncycle['repayFrequency']) #近24个月开立 非循环贷账户 贷款账户还款频率基尼不纯度
                dict_out['loan_second_m24_ncycleR_repayFrequency_giniimpurity'] = dict_out['loan_second_m24_ncycle_repayFrequency_giniimpurity']/dict_out['loan_second_m24_repayFrequency_giniimpurity'] if dict_out['loan_second_m24_repayFrequency_giniimpurity']>0 else np.nan #var最大值比率
                dict_out['loan_second_m24_ncycleR_month60_State_countCr_range'] = dict_out['loan_second_m24_ncycle_month60_State_countCr_range']/dict_out['loan_second_m24_month60_State_countCr_range'] if dict_out['loan_second_m24_month60_State_countCr_range']>0 else np.nan #var最大值比率
                dict_out['loan_second_m24_ncycleR_month60_State_countNullr_mean'] = dict_out['loan_second_m24_ncycle_month60_State_countNullr_mean']/dict_out['loan_second_m24_month60_State_countNullr_mean'] if dict_out['loan_second_m24_month60_State_countNullr_mean']>0 else np.nan #var最大值比率
                dict_out['loan_second_m24_ncycle_org_trust_company_count'] = loan_second_m24_ncycle[loan_second_m24_ncycle['org']=='信托公司'].shape[0] #近24个月开立 非循环贷账户 贷款账户管理机构为信托公司 计数
                dict_out['loan_second_m24_ncycleR_org_trust_company_count'] = dict_out['loan_second_m24_ncycle_org_trust_company_count']/dict_out['loan_second_m24_org_trust_company_count'] if dict_out['loan_second_m24_org_trust_company_count']>0 else np.nan #var最大值比率
                dict_out['loan_second_m24_ncycleR_month60_State_countNr_min'] = dict_out['loan_second_m24_ncycle_month60_State_countNr_min']/dict_out['loan_second_m24_month60_State_countNr_min'] if dict_out['loan_second_m24_month60_State_countNr_min']>0 else np.nan #var最大值比率
                dict_out['loan_second_m24_ncycleR_month60_State_countNr_sum'] = dict_out['loan_second_m24_ncycle_month60_State_countNr_sum']/dict_out['loan_second_m24_month60_State_countNr_sum'] if dict_out['loan_second_m24_month60_State_countNr_sum']>0 else np.nan #var最大值比率
                dict_out['loan_second_m24_ncycle_rf_once_ratio'] = loan_second_m24_ncycle[loan_second_m24_ncycle['repayFrequency']=='一次性'].shape[0]/len(loan_second_m24_ncycle) #近24个月开立 非循环贷账户 贷款账户还款频率为一次性 占比
                dict_out['loan_second_m24_ncycleR_rf_once_ratio'] = dict_out['loan_second_m24_ncycle_rf_once_ratio']/dict_out['loan_second_m24_rf_once_ratio'] if dict_out['loan_second_m24_rf_once_ratio']>0 else np.nan #var最大值比率
                dict_out['loan_second_m24_ncycleR_byDate_to_report_max'] = dict_out['loan_second_m24_ncycle_byDate_to_report_max']/dict_out['loan_second_m24_byDate_to_report_max'] if dict_out['loan_second_m24_byDate_to_report_max']>0 else np.nan #var最大值比率
                dict_out['loan_second_m24_ncycleR_month60_to_report_min_range'] = dict_out['loan_second_m24_ncycle_month60_to_report_min_range']/dict_out['loan_second_m24_month60_to_report_min_range'] if dict_out['loan_second_m24_month60_to_report_min_range']>0 else np.nan #var最大值比率

            #近24个月开立 循环贷账户
            loan_second_m24_cycle = loan_second_m24[loan_second_m24['class']=='循环贷账户']


            #近24个月开立 有担保 is_vouch 
            loan_second_m24_vouch = loan_second_m24[loan_second_m24['is_vouch']==1]
            if len(loan_second_m24_vouch)>0:
                dict_out['loan_second_m24_vouch_month60_State_countCr_max'] = loan_second_m24_vouch['month60_State_countCr'].max()#var最大值
                dict_out['loan_second_m24_vouch_month60_State_countNr_max'] = loan_second_m24_vouch['month60_State_countNr'].max()#var最大值
                dict_out['loan_second_m24_vouch_month60_State_countN_max'] = loan_second_m24_vouch['month60_State_countN'].max()#var最大值
                dict_out['loan_second_m24_vouch_month60_State_countNr_mean'] = loan_second_m24_vouch['month60_State_countNr'].mean() #var平均值
                dict_out['loan_second_m24_vouch_loanAmount_max'] = loan_second_m24_vouch['loanAmount'].max()#var最大值
                dict_out['loan_second_m24_vouch_repayMons_ratio_sum'] = loan_second_m24_vouch['repayMons_ratio'].sum() #var求和
                dict_out['loan_second_m24_vouch_repayMons_mean'] = loan_second_m24_vouch['repayMons'].mean() #var平均值
                dict_out['loan_second_m24_vouch_loanAmount_sum'] = loan_second_m24_vouch['loanAmount'].sum() #var求和
                dict_out['loan_second_m24_vouch_balance_max'] = loan_second_m24_vouch['balance'].max()#var最大值
                dict_out['loan_second_m24_vouch_month60_State_countNr_min'] = loan_second_m24_vouch['month60_State_countNr'].min() #var最小值
                dict_out['loan_second_m24_vouch_repayTerms_mean'] = loan_second_m24_vouch['repayTerms'].mean() #var平均值
                dict_out['loan_second_m24_vouch_bt_other_person_count'] = loan_second_m24_vouch[loan_second_m24_vouch['businessType']=='其他个人消费贷款'].shape[0] #近24个月开立 有担保贷款账户业务种类为其他个人消费贷款 计数
                dict_out['loan_second_m24_vouch_bt_other_person_ratio'] = loan_second_m24_vouch[loan_second_m24_vouch['businessType']=='其他个人消费贷款'].shape[0]/len(loan_second_m24_vouch) #近24个月开立 有担保贷款账户业务种类为其他个人消费贷款 占比
                dict_out['loan_second_m24_vouch_loanGrantOrg_nunique'] = loan_second_m24_vouch['loanGrantOrg'].nunique() #近24个月开立 有担保贷款账户管理机构详细nunique
                dict_out['loan_second_m24_vouch_org_micro_loan_count'] = loan_second_m24_vouch[loan_second_m24_vouch['org']=='小额贷款公司'].shape[0] #近24个月开立 有担保贷款账户管理机构为小额贷款公司 计数
                dict_out['loan_second_m24_vouch_startDate_to_report_max'] = loan_second_m24_vouch['startDate_to_report'].max() #var最大值比率
                dict_out['loan_second_m24_vouch_month60_State_countNr_sum'] = loan_second_m24_vouch['month60_State_countNr'].sum() #var求和比率
                dict_out['loan_second_m24_vouch_loanAmount_max'] = loan_second_m24_vouch['loanAmount'].max() #var最大值比率
                dict_out['loan_second_m24_vouch_month60_State_countNr_max'] = loan_second_m24_vouch['month60_State_countNr'].max() #var最大值比率
                dict_out['loan_second_m24_vouch_month60_State_num_size_sum'] = loan_second_m24_vouch['month60_State_num_size'].sum() #var求和比率
                dict_out['loan_second_m24_vouch_leftRepayTerms_sum'] = loan_second_m24_vouch['leftRepayTerms'].sum() #var求和比率
                dict_out['loan_second_m24_vouch_loanAmount_sum'] = loan_second_m24_vouch['loanAmount'].sum() #var求和比率
                dict_out['loan_second_m24_vouch_repayTerms_sum'] = loan_second_m24_vouch['repayTerms'].sum() #var求和比率
                dict_out['loan_second_m24_vouch_repayMons_min'] = loan_second_m24_vouch['repayMons'].min() #var最小值比率
                dict_out['loan_second_m24_vouch_month60_State_countNull_mean'] = loan_second_m24_vouch['month60_State_countNull'].mean()  #var平均值比率
                dict_out['loan_second_m24_vouch_byDate_to_report_mean'] = loan_second_m24_vouch['byDate_to_report'].mean()  #var平均值比率
                dict_out['loan_second_m24_vouch_month60_State_countN_sum'] = loan_second_m24_vouch['month60_State_countN'].sum() #var求和比率
                dict_out['loan_second_m24_vouchR_startDate_to_report_max'] = dict_out['loan_second_m24_vouch_startDate_to_report_max']/dict_out['loan_second_m24_startDate_to_report_max'] if dict_out['loan_second_m24_startDate_to_report_max']>0 else np.nan #var最大值比率
                dict_out['loan_second_m24_vouch_bt_other_person_count'] = loan_second_m24_vouch[loan_second_m24_vouch['businessType']=='其他个人消费贷款'].shape[0] #近24个月开立 有担保贷款账户业务种类为其他个人消费贷款 计数
                dict_out['loan_second_m24_vouchR_bt_other_person_count'] = dict_out['loan_second_m24_vouch_bt_other_person_count']/dict_out['loan_second_m24_bt_other_person_count'] if dict_out['loan_second_m24_bt_other_person_count']>0 else np.nan #var最大值比率
                dict_out['loan_second_m24_vouchR_month60_State_countNr_sum'] = dict_out['loan_second_m24_vouch_month60_State_countNr_sum']/dict_out['loan_second_m24_month60_State_countNr_sum'] if dict_out['loan_second_m24_month60_State_countNr_sum']>0 else np.nan #var最大值比率
                dict_out['loan_second_m24_vouchR_loanAmount_max'] = dict_out['loan_second_m24_vouch_loanAmount_max']/dict_out['loan_second_m24_loanAmount_max'] if dict_out['loan_second_m24_loanAmount_max']>0 else np.nan #var最大值比率
                dict_out['loan_second_m24_vouchR_month60_State_countNr_max'] = dict_out['loan_second_m24_vouch_month60_State_countNr_max']/dict_out['loan_second_m24_month60_State_countNr_max'] if dict_out['loan_second_m24_month60_State_countNr_max']>0 else np.nan #var最大值比率
                dict_out['loan_second_m24_vouchR_month60_State_num_size_sum'] = dict_out['loan_second_m24_vouch_month60_State_num_size_sum']/dict_out['loan_second_m24_month60_State_num_size_sum'] if dict_out['loan_second_m24_month60_State_num_size_sum']>0 else np.nan #var最大值比率
                dict_out['loan_second_m24_vouchR_leftRepayTerms_sum'] = dict_out['loan_second_m24_vouch_leftRepayTerms_sum']/dict_out['loan_second_m24_leftRepayTerms_sum'] if dict_out['loan_second_m24_leftRepayTerms_sum']>0 else np.nan #var最大值比率
                dict_out['loan_second_m24_vouchR_loanAmount_sum'] = dict_out['loan_second_m24_vouch_loanAmount_sum']/dict_out['loan_second_m24_loanAmount_sum'] if dict_out['loan_second_m24_loanAmount_sum']>0 else np.nan #var最大值比率
                dict_out['loan_second_m24_vouchR_repayTerms_sum'] = dict_out['loan_second_m24_vouch_repayTerms_sum']/dict_out['loan_second_m24_repayTerms_sum'] if dict_out['loan_second_m24_repayTerms_sum']>0 else np.nan #var最大值比率
                dict_out['loan_second_m24_vouch_bt_other_person_ratio'] = loan_second_m24_vouch[loan_second_m24_vouch['businessType']=='其他个人消费贷款'].shape[0]/len(loan_second_m24_vouch) #近24个月开立 有担保贷款账户业务种类为其他个人消费贷款 占比
                dict_out['loan_second_m24_vouchR_bt_other_person_ratio'] = dict_out['loan_second_m24_vouch_bt_other_person_ratio']/dict_out['loan_second_m24_bt_other_person_ratio'] if dict_out['loan_second_m24_bt_other_person_ratio']>0 else np.nan #var最大值比率
                dict_out['loan_second_m24_vouch_loanGrantOrg_nunique'] = loan_second_m24_vouch['loanGrantOrg'].nunique() #近24个月开立 有担保贷款账户管理机构详细nunique
                dict_out['loan_second_m24_vouchR_loanGrantOrg_nunique'] = dict_out['loan_second_m24_vouch_loanGrantOrg_nunique']/dict_out['loan_second_m24_loanGrantOrg_nunique'] if dict_out['loan_second_m24_loanGrantOrg_nunique']>0 else np.nan #var最大值比率
                dict_out['loan_second_m24_vouchR_repayMons_min'] = dict_out['loan_second_m24_vouch_repayMons_min']/dict_out['loan_second_m24_repayMons_min'] if dict_out['loan_second_m24_repayMons_min']>0 else np.nan #var最大值比率
                dict_out['loan_second_m24_vouchR_month60_State_countNull_mean'] = dict_out['loan_second_m24_vouch_month60_State_countNull_mean']/dict_out['loan_second_m24_month60_State_countNull_mean'] if dict_out['loan_second_m24_month60_State_countNull_mean']>0 else np.nan #var最大值比率
                dict_out['loan_second_m24_vouch_org_micro_loan_count'] = loan_second_m24_vouch[loan_second_m24_vouch['org']=='小额贷款公司'].shape[0] #近24个月开立 有担保贷款账户管理机构为小额贷款公司 计数
                dict_out['loan_second_m24_vouchR_org_micro_loan_count'] = dict_out['loan_second_m24_vouch_org_micro_loan_count']/dict_out['loan_second_m24_org_micro_loan_count'] if dict_out['loan_second_m24_org_micro_loan_count']>0 else np.nan #var最大值比率
                dict_out['loan_second_m24_vouchR_byDate_to_report_mean'] = dict_out['loan_second_m24_vouch_byDate_to_report_mean']/dict_out['loan_second_m24_byDate_to_report_mean'] if dict_out['loan_second_m24_byDate_to_report_mean']>0 else np.nan #var最大值比率
                dict_out['loan_second_m24_vouchR_month60_State_countN_sum'] = dict_out['loan_second_m24_vouch_month60_State_countN_sum']/dict_out['loan_second_m24_month60_State_countN_sum'] if dict_out['loan_second_m24_month60_State_countN_sum']>0 else np.nan #var最大值比率

            #近24个月开立 历史逾期
            loan_second_m24_hdue1 = loan_second_m24[loan_second_m24['is_overdue']==1]
            if len(loan_second_m24_hdue1)>0:
                dict_out['loan_second_m24_hdue1_month60_State_num_mean_min'] = loan_second_m24_hdue1['month60_State_num_mean'].min() #var最小值
                dict_out['loan_second_m24_hdue1_loanGrantOrg_nunique'] = loan_second_m24_hdue1['loanGrantOrg'].nunique() #近24个月开立 历史逾期贷款账户管理机构详细nunique
                dict_out['loan_second_m24_hdue1_month60_State_countNullr_sum'] = loan_second_m24_hdue1['month60_State_countNullr'].sum() #var求和比率
                dict_out['loan_second_m24_hdue1_month60_State_num_size_sum'] = loan_second_m24_hdue1['month60_State_num_size'].sum() #var求和比率
                dict_out['loan_second_m24_hdue1R_month60_State_countNullr_sum'] = dict_out['loan_second_m24_hdue1_month60_State_countNullr_sum']/dict_out['loan_second_m24_month60_State_countNullr_sum'] if dict_out['loan_second_m24_month60_State_countNullr_sum']>0 else np.nan #var最大值比率
                dict_out['loan_second_m24_hdue1R_month60_State_num_size_sum'] = dict_out['loan_second_m24_hdue1_month60_State_num_size_sum']/dict_out['loan_second_m24_month60_State_num_size_sum'] if dict_out['loan_second_m24_month60_State_num_size_sum']>0 else np.nan #var最大值比率
                dict_out['loan_second_m24_hdue1_loanGrantOrg_nunique'] = loan_second_m24_hdue1['loanGrantOrg'].nunique() #近24个月开立 历史逾期贷款账户管理机构详细nunique
                dict_out['loan_second_m24_hdue1R_loanGrantOrg_nunique'] = dict_out['loan_second_m24_hdue1_loanGrantOrg_nunique']/dict_out['loan_second_m24_loanGrantOrg_nunique'] if dict_out['loan_second_m24_loanGrantOrg_nunique']>0 else np.nan #var最大值比率

            #近24个月开立 历史严重逾期
            loan_second_m24_hdue3 = loan_second_m24[loan_second_m24['due_class']==3]


            #近24个月开立 当前逾期
            loan_second_m24_cdue = loan_second_m24[loan_second_m24['accountStatus']=='逾期']



        #近03个月截至
        loan_second_by03 = loan_second[loan_second.byDate_to_report<3]
        if len(loan_second_by03)>0:

            if True:
                for var in numeric_vers:
                    dict_out['loan_second_by03_'+var+'_max'] = loan_second_by03[var].max() #近03个月截至 var最大值
                    dict_out['loan_second_by03_'+var+'_min'] = loan_second_by03[var].min() #近03个月截至 var最小值
                    dict_out['loan_second_by03_'+var+'_mean'] = loan_second_by03[var].mean() #近03个月截至 var平均值
                    dict_out['loan_second_by03_'+var+'_sum'] = loan_second_by03[var].sum() #近03个月截至 var求和
                    dict_out['loan_second_by03_'+var+'_range'] = (dict_out['loan_second_by03_'+var+'_max']-dict_out['loan_second_by03_'+var+'_min'])/dict_out['loan_second_by03_'+var+'_max'] if dict_out['loan_second_by03_'+var+'_max']>0 else np.nan #近03个月截至 var区间率



                dict_out['loan_second_by03_loanGrantOrg_giniimpurity'] = giniimpurity(loan_second_by03['loanGrantOrg']) #近03个月截至 贷款账户管理机构详细基尼不纯度
                dict_out['loan_second_by03_loanGrantOrg_nunique'] = loan_second_by03['loanGrantOrg'].nunique() #近03个月截至 贷款账户管理机构详细nunique

                dict_out['loan_second_by03_org_giniimpurity'] = giniimpurity(loan_second_by03['org']) #近03个月截至 贷款账户管理机构基尼不纯度
                dict_out['loan_second_by03_org_commercial_bank_count'] = loan_second_by03[loan_second_by03['org']=='商业银行'].shape[0] #近03个月截至 贷款账户管理机构为商业银行 计数
                dict_out['loan_second_by03_org_commercial_bank_ratio'] = loan_second_by03[loan_second_by03['org']=='商业银行'].shape[0]/len(loan_second_by03) #近03个月截至 贷款账户管理机构为商业银行 占比
                dict_out['loan_second_by03_org_consumer_finance_count'] = loan_second_by03[loan_second_by03['org']=='消费金融公司'].shape[0] #近03个月截至 贷款账户管理机构为消费金融公司 计数
                dict_out['loan_second_by03_org_consumer_finance_ratio'] = loan_second_by03[loan_second_by03['org']=='消费金融公司'].shape[0]/len(loan_second_by03) #近03个月截至 贷款账户管理机构为消费金融公司 占比
                dict_out['loan_second_by03_org_micro_loan_count'] = loan_second_by03[loan_second_by03['org']=='小额贷款公司'].shape[0] #近03个月截至 贷款账户管理机构为小额贷款公司 计数
                dict_out['loan_second_by03_org_micro_loan_ratio'] = loan_second_by03[loan_second_by03['org']=='小额贷款公司'].shape[0]/len(loan_second_by03) #近03个月截至 贷款账户管理机构为小额贷款公司 占比
                dict_out['loan_second_by03_org_other_count'] = loan_second_by03[loan_second_by03['org']=='其他机构'].shape[0] #近03个月截至 贷款账户管理机构为其他机构 计数
                dict_out['loan_second_by03_org_other_ratio'] = loan_second_by03[loan_second_by03['org']=='其他机构'].shape[0]/len(loan_second_by03) #近03个月截至 贷款账户管理机构为其他机构 占比
                dict_out['loan_second_by03_org_trust_company_count'] = loan_second_by03[loan_second_by03['org']=='信托公司'].shape[0] #近03个月截至 贷款账户管理机构为信托公司 计数
                dict_out['loan_second_by03_org_trust_company_ratio'] = loan_second_by03[loan_second_by03['org']=='信托公司'].shape[0]/len(loan_second_by03) #近03个月截至 贷款账户管理机构为信托公司 占比
                dict_out['loan_second_by03_org_car_finance_count'] = loan_second_by03[loan_second_by03['org']=='汽车金融公司'].shape[0] #近03个月截至 贷款账户管理机构为汽车金融公司 计数
                dict_out['loan_second_by03_org_car_finance_ratio'] = loan_second_by03[loan_second_by03['org']=='汽车金融公司'].shape[0]/len(loan_second_by03) #近03个月截至 贷款账户管理机构为汽车金融公司 占比
                dict_out['loan_second_by03_org_lease_finance_count'] = loan_second_by03[loan_second_by03['org']=='融资租赁公司'].shape[0] #近03个月截至 贷款账户管理机构为融资租赁公司 计数
                dict_out['loan_second_by03_org_lease_finance_ratio'] = loan_second_by03[loan_second_by03['org']=='融资租赁公司'].shape[0]/len(loan_second_by03) #近03个月截至 贷款账户管理机构为融资租赁公司 占比
                dict_out['loan_second_by03_org_myself_count'] = loan_second_by03[loan_second_by03['org']==dict_in['cc_rh_report'].queryOperator.values[0]].shape[0] #近03个月截至 贷款账户管理机构为本机构 计数
                dict_out['loan_second_by03_org_myself_ratio'] = loan_second_by03[loan_second_by03['org']==dict_in['cc_rh_report'].queryOperator.values[0]].shape[0]/len(loan_second_by03) #近03个月截至 贷款账户管理机构为本机构 占比
                dict_out['loan_second_by03_org_village_banks_count'] = loan_second_by03[loan_second_by03['org']=='村镇银行'].shape[0] #近03个月截至 贷款账户管理机构为村镇银行 计数
                dict_out['loan_second_by03_org_village_banks_ratio'] = loan_second_by03[loan_second_by03['org']=='村镇银行'].shape[0]/len(loan_second_by03) #近03个月截至 贷款账户管理机构为村镇银行 占比
                dict_out['loan_second_by03_org_finance_company_count'] = loan_second_by03[loan_second_by03['org']=='财务公司'].shape[0] #近03个月截至 贷款账户管理机构为财务公司 计数
                dict_out['loan_second_by03_org_finance_company_ratio'] = loan_second_by03[loan_second_by03['org']=='财务公司'].shape[0]/len(loan_second_by03) #近03个月截至 贷款账户管理机构为财务公司 占比
                dict_out['loan_second_by03_org_foreign_banks_count'] = loan_second_by03[loan_second_by03['org']=='外资银行'].shape[0] #近03个月截至 贷款账户管理机构为外资银行 计数
                dict_out['loan_second_by03_org_foreign_banks_ratio'] = loan_second_by03[loan_second_by03['org']=='外资银行'].shape[0]/len(loan_second_by03) #近03个月截至 贷款账户管理机构为外资银行 占比
                dict_out['loan_second_by03_org_provident_fund_count'] = loan_second_by03[loan_second_by03['org']=='公积金管理中心'].shape[0] #近03个月截至 贷款账户管理机构为公积金管理中心 计数
                dict_out['loan_second_by03_org_provident_fund_ratio'] = loan_second_by03[loan_second_by03['org']=='公积金管理中心'].shape[0]/len(loan_second_by03) #近03个月截至 贷款账户管理机构为公积金管理中心 占比
                dict_out['loan_second_by03_org_securities_firm_count'] = loan_second_by03[loan_second_by03['org']=='证券公司'].shape[0] #近03个月截至 贷款账户管理机构为证券公司 计数
                dict_out['loan_second_by03_org_securities_firm_ratio'] = loan_second_by03[loan_second_by03['org']=='证券公司'].shape[0]/len(loan_second_by03) #近03个月截至 贷款账户管理机构为证券公司 占比

                dict_out['loan_second_by03_class_giniimpurity'] = giniimpurity(loan_second_by03['class']) #近03个月截至 贷款账户账户类别基尼不纯度
                dict_out['loan_second_by03_class_ncycle_count'] = loan_second_by03[loan_second_by03['class']=='非循环贷账户'].shape[0] #近03个月截至 贷款账户账户类别为非循环贷账户 计数
                dict_out['loan_second_by03_class_ncycle_ratio'] = loan_second_by03[loan_second_by03['class']=='非循环贷账户'].shape[0]/len(loan_second_by03) #近03个月截至 贷款账户账户类别为非循环贷账户 占比
                dict_out['loan_second_by03_class_cycle_sub_count'] = loan_second_by03[loan_second_by03['class']=='循环额度下分账户'].shape[0] #近03个月截至 贷款账户账户类别为循环额度下分账户 计数
                dict_out['loan_second_by03_class_cycle_sub_ratio'] = loan_second_by03[loan_second_by03['class']=='循环额度下分账户'].shape[0]/len(loan_second_by03) #近03个月截至 贷款账户账户类别为循环额度下分账户 占比
                dict_out['loan_second_by03_class_cycle_count'] = loan_second_by03[loan_second_by03['class']=='循环贷账户'].shape[0] #近03个月截至 贷款账户账户类别为循环贷账户 计数
                dict_out['loan_second_by03_class_cycle_ratio'] = loan_second_by03[loan_second_by03['class']=='循环贷账户'].shape[0]/len(loan_second_by03) #近03个月截至 贷款账户账户类别为循环贷账户 占比

                dict_out['loan_second_by03_classify5_giniimpurity'] = giniimpurity(loan_second_by03['classify5']) #近03个月截至 贷款账户五级分类基尼不纯度
                dict_out['loan_second_by03_c5_unknow_count'] = loan_second_by03[loan_second_by03['classify5']==''].shape[0] #近03个月截至 贷款账户五级分类为'' 计数
                dict_out['loan_second_by03_c5_unknow_ratio'] = loan_second_by03[loan_second_by03['classify5']==''].shape[0]/len(loan_second_by03) #近03个月截至 贷款账户五级分类为'' 占比
                dict_out['loan_second_by03_c5_normal_count'] = loan_second_by03[loan_second_by03['classify5']=='正常'].shape[0] #近03个月截至 贷款账户五级分类为正常 计数
                dict_out['loan_second_by03_c5_normal_ratio'] = loan_second_by03[loan_second_by03['classify5']=='正常'].shape[0]/len(loan_second_by03) #近03个月截至 贷款账户五级分类为正常 占比
                dict_out['loan_second_by03_c5_loss_count'] = loan_second_by03[loan_second_by03['classify5']=='损失'].shape[0] #近03个月截至 贷款账户五级分类为损失 计数
                dict_out['loan_second_by03_c5_loss_ratio'] = loan_second_by03[loan_second_by03['classify5']=='损失'].shape[0]/len(loan_second_by03) #近03个月截至 贷款账户五级分类为损失 占比
                dict_out['loan_second_by03_c5_attention_count'] = loan_second_by03[loan_second_by03['classify5']=='关注'].shape[0] #近03个月截至 贷款账户五级分类为关注 计数
                dict_out['loan_second_by03_c5_attention_ratio'] = loan_second_by03[loan_second_by03['classify5']=='关注'].shape[0]/len(loan_second_by03) #近03个月截至 贷款账户五级分类为关注 占比
                dict_out['loan_second_by03_c5_suspicious_count'] = loan_second_by03[loan_second_by03['classify5']=='可疑'].shape[0] #近03个月截至 贷款账户五级分类为可疑 计数
                dict_out['loan_second_by03_c5_suspicious_ratio'] = loan_second_by03[loan_second_by03['classify5']=='可疑'].shape[0]/len(loan_second_by03) #近03个月截至 贷款账户五级分类为可疑 占比
                dict_out['loan_second_by03_c5_secondary_count'] = loan_second_by03[loan_second_by03['classify5']=='次级'].shape[0] #近03个月截至 贷款账户五级分类为次级 计数
                dict_out['loan_second_by03_c5_secondary_ratio'] = loan_second_by03[loan_second_by03['classify5']=='次级'].shape[0]/len(loan_second_by03) #近03个月截至 贷款账户五级分类为次级 占比
                dict_out['loan_second_by03_c5_noclass_count'] = loan_second_by03[loan_second_by03['classify5']=='未分类'].shape[0] #近03个月截至 贷款账户五级分类为未分类 计数
                dict_out['loan_second_by03_c5_noclass_ratio'] = loan_second_by03[loan_second_by03['classify5']=='未分类'].shape[0]/len(loan_second_by03) #近03个月截至 贷款账户五级分类为未分类 占比

                dict_out['loan_second_by03_accountStatus_giniimpurity'] = giniimpurity(loan_second_by03['accountStatus']) #近03个月截至 贷款账户账户状态基尼不纯度
                dict_out['loan_second_by03_as_settle_count'] = loan_second_by03[loan_second_by03['accountStatus']=='结清'].shape[0] #近03个月截至 贷款账户账户状态为结清 计数
                dict_out['loan_second_by03_as_settle_ratio'] = loan_second_by03[loan_second_by03['accountStatus']=='结清'].shape[0]/len(loan_second_by03) #近03个月截至 贷款账户账户状态为结清 占比
                dict_out['loan_second_by03_as_normal_count'] = loan_second_by03[loan_second_by03['accountStatus']=='正常'].shape[0] #近03个月截至 贷款账户账户状态为正常 计数
                dict_out['loan_second_by03_as_normal_ratio'] = loan_second_by03[loan_second_by03['accountStatus']=='正常'].shape[0]/len(loan_second_by03) #近03个月截至 贷款账户账户状态为正常 占比
                dict_out['loan_second_by03_as_overdue_count'] = loan_second_by03[loan_second_by03['accountStatus']=='逾期'].shape[0] #近03个月截至 贷款账户账户状态为逾期 计数
                dict_out['loan_second_by03_as_overdue_ratio'] = loan_second_by03[loan_second_by03['accountStatus']=='逾期'].shape[0]/len(loan_second_by03) #近03个月截至 贷款账户账户状态为逾期 占比
                dict_out['loan_second_by03_as_bad_debts_count'] = loan_second_by03[loan_second_by03['accountStatus']=='呆账'].shape[0] #近03个月截至 贷款账户账户状态为呆账 计数
                dict_out['loan_second_by03_as_bad_debts_ratio'] = loan_second_by03[loan_second_by03['accountStatus']=='呆账'].shape[0]/len(loan_second_by03) #近03个月截至 贷款账户账户状态为呆账 占比
                dict_out['loan_second_by03_as_unknow_count'] = loan_second_by03[loan_second_by03['accountStatus']==''].shape[0] #近03个月截至 贷款账户账户状态为'' 计数
                dict_out['loan_second_by03_as_unknow_ratio'] = loan_second_by03[loan_second_by03['accountStatus']==''].shape[0]/len(loan_second_by03) #近03个月截至 贷款账户账户状态为'' 占比
                dict_out['loan_second_by03_as_roll_out_count'] = loan_second_by03[loan_second_by03['accountStatus']=='转出'].shape[0] #近03个月截至 贷款账户账户状态为转出 计数
                dict_out['loan_second_by03_as_roll_out_ratio'] = loan_second_by03[loan_second_by03['accountStatus']=='转出'].shape[0]/len(loan_second_by03) #近03个月截至 贷款账户账户状态为转出 占比

                dict_out['loan_second_by03_repayType_giniimpurity'] = giniimpurity(loan_second_by03['repayType']) #近03个月截至 贷款账户还款方式基尼不纯度
                dict_out['loan_second_by03_rt_unknow_count'] = loan_second_by03[loan_second_by03['repayType']=='--'].shape[0] #近03个月截至 贷款账户还款方式为-- 计数
                dict_out['loan_second_by03_rt_unknow_ratio'] = loan_second_by03[loan_second_by03['repayType']=='--'].shape[0]/len(loan_second_by03) #近03个月截至 贷款账户还款方式为-- 占比
                dict_out['loan_second_by03_rt_equality_count'] = loan_second_by03[loan_second_by03['repayType']=='分期等额本息'].shape[0] #近03个月截至 贷款账户还款方式为分期等额本息 计数
                dict_out['loan_second_by03_rt_equality_ratio'] = loan_second_by03[loan_second_by03['repayType']=='分期等额本息'].shape[0]/len(loan_second_by03) #近03个月截至 贷款账户还款方式为分期等额本息 占比
                dict_out['loan_second_by03_rt_onschedule_count'] = loan_second_by03[loan_second_by03['repayType']=='按期计算还本付息'].shape[0] #近03个月截至 贷款账户还款方式为按期计算还本付息 计数
                dict_out['loan_second_by03_rt_onschedule_ratio'] = loan_second_by03[loan_second_by03['repayType']=='按期计算还本付息'].shape[0]/len(loan_second_by03) #近03个月截至 贷款账户还款方式为按期计算还本付息 占比
                dict_out['loan_second_by03_rt_not_distinguish_count'] = loan_second_by03[loan_second_by03['repayType']=='不区分还款方式'].shape[0] #近03个月截至 贷款账户还款方式为不区分还款方式 计数
                dict_out['loan_second_by03_rt_not_distinguish_ratio'] = loan_second_by03[loan_second_by03['repayType']=='不区分还款方式'].shape[0]/len(loan_second_by03) #近03个月截至 贷款账户还款方式为不区分还款方式 占比
                dict_out['loan_second_by03_rt_circulation_count'] = loan_second_by03[loan_second_by03['repayType']=='循环贷款下其他还款方式'].shape[0] #近03个月截至 贷款账户还款方式为循环贷款下其他还款方式 计数
                dict_out['loan_second_by03_rt_circulation_ratio'] = loan_second_by03[loan_second_by03['repayType']=='循环贷款下其他还款方式'].shape[0]/len(loan_second_by03) #近03个月截至 贷款账户还款方式为循环贷款下其他还款方式 占比
                dict_out['loan_second_by03_rt_once_count'] = loan_second_by03[loan_second_by03['repayType']=='到期一次还本付息'].shape[0] #近03个月截至 贷款账户还款方式为到期一次还本付息 计数
                dict_out['loan_second_by03_rt_once_ratio'] = loan_second_by03[loan_second_by03['repayType']=='到期一次还本付息'].shape[0]/len(loan_second_by03) #近03个月截至 贷款账户还款方式为到期一次还本付息 占比

                dict_out['loan_second_by03_repayFrequency_giniimpurity'] = giniimpurity(loan_second_by03['repayFrequency']) #近03个月截至 贷款账户还款频率基尼不纯度
                dict_out['loan_second_by03_rf_month_count'] = loan_second_by03[loan_second_by03['repayFrequency']=='月'].shape[0] #近03个月截至 贷款账户还款频率为月 计数
                dict_out['loan_second_by03_rf_month_ratio'] = loan_second_by03[loan_second_by03['repayFrequency']=='月'].shape[0]/len(loan_second_by03) #近03个月截至 贷款账户还款频率为月 占比
                dict_out['loan_second_by03_rf_once_count'] = loan_second_by03[loan_second_by03['repayFrequency']=='一次性'].shape[0] #近03个月截至 贷款账户还款频率为一次性 计数
                dict_out['loan_second_by03_rf_once_ratio'] = loan_second_by03[loan_second_by03['repayFrequency']=='一次性'].shape[0]/len(loan_second_by03) #近03个月截至 贷款账户还款频率为一次性 占比
                dict_out['loan_second_by03_rf_other_count'] = loan_second_by03[loan_second_by03['repayFrequency']=='其他'].shape[0] #近03个月截至 贷款账户还款频率为其他 计数
                dict_out['loan_second_by03_rf_other_ratio'] = loan_second_by03[loan_second_by03['repayFrequency']=='其他'].shape[0]/len(loan_second_by03) #近03个月截至 贷款账户还款频率为其他 占比
                dict_out['loan_second_by03_rf_irregular_count'] = loan_second_by03[loan_second_by03['repayFrequency']=='不定期'].shape[0] #近03个月截至 贷款账户还款频率为不定期 计数
                dict_out['loan_second_by03_rf_irregular_ratio'] = loan_second_by03[loan_second_by03['repayFrequency']=='不定期'].shape[0]/len(loan_second_by03) #近03个月截至 贷款账户还款频率为不定期 占比
                dict_out['loan_second_by03_rf_day_count'] = loan_second_by03[loan_second_by03['repayFrequency']=='日'].shape[0] #近03个月截至 贷款账户还款频率为日 计数
                dict_out['loan_second_by03_rf_day_ratio'] = loan_second_by03[loan_second_by03['repayFrequency']=='日'].shape[0]/len(loan_second_by03) #近03个月截至 贷款账户还款频率为日 占比
                dict_out['loan_second_by03_rf_year_count'] = loan_second_by03[loan_second_by03['repayFrequency']=='年'].shape[0] #近03个月截至 贷款账户还款频率为年 计数
                dict_out['loan_second_by03_rf_year_ratio'] = loan_second_by03[loan_second_by03['repayFrequency']=='年'].shape[0]/len(loan_second_by03) #近03个月截至 贷款账户还款频率为年 占比
                dict_out['loan_second_by03_rf_season_count'] = loan_second_by03[loan_second_by03['repayFrequency']=='季'].shape[0] #近03个月截至 贷款账户还款频率为季 计数
                dict_out['loan_second_by03_rf_season_ratio'] = loan_second_by03[loan_second_by03['repayFrequency']=='季'].shape[0]/len(loan_second_by03) #近03个月截至 贷款账户还款频率为季 占比
                dict_out['loan_second_by03_rf_week_count'] = loan_second_by03[loan_second_by03['repayFrequency']=='周'].shape[0] #近03个月截至 贷款账户还款频率为周 计数
                dict_out['loan_second_by03_rf_week_ratio'] = loan_second_by03[loan_second_by03['repayFrequency']=='周'].shape[0]/len(loan_second_by03) #近03个月截至 贷款账户还款频率为周 占比
                dict_out['loan_second_by03_rf_halfyear_count'] = loan_second_by03[loan_second_by03['repayFrequency']=='半年'].shape[0] #近03个月截至 贷款账户还款频率为半年 计数
                dict_out['loan_second_by03_rf_halfyear_ratio'] = loan_second_by03[loan_second_by03['repayFrequency']=='半年'].shape[0]/len(loan_second_by03) #近03个月截至 贷款账户还款频率为半年 占比

                dict_out['loan_second_by03_guaranteeForm_giniimpurity'] = giniimpurity(loan_second_by03['guaranteeForm']) #近03个月截至 贷款账户担保方式基尼不纯度
                dict_out['loan_second_by03_gf_crdit_count'] = loan_second_by03[loan_second_by03['guaranteeForm']=='信用/免担保'].shape[0] #近03个月截至 贷款账户担保方式为其信用/免担保 计数
                dict_out['loan_second_by03_gf_crdit_ratio'] = loan_second_by03[loan_second_by03['guaranteeForm']=='信用/免担保'].shape[0]/len(loan_second_by03) #近03个月截至 贷款账户担保方式为信用/免担保 占比
                dict_out['loan_second_by03_gf_other_count'] = loan_second_by03[loan_second_by03['guaranteeForm']=='其他'].shape[0] #近03个月截至 贷款账户担保方式为其他 计数
                dict_out['loan_second_by03_gf_other_ratio'] = loan_second_by03[loan_second_by03['guaranteeForm']=='其他'].shape[0]/len(loan_second_by03) #近03个月截至 贷款账户担保方式为其他 占比
                dict_out['loan_second_by03_gf_combine_nowarranty_count'] = loan_second_by03[loan_second_by03['guaranteeForm']=='组合（不含保证）'].shape[0] #近03个月截至 贷款账户担保方式为组合（不含保证） 计数
                dict_out['loan_second_by03_gf_combine_nowarranty_ratio'] = loan_second_by03[loan_second_by03['guaranteeForm']=='组合（不含保证）'].shape[0]/len(loan_second_by03) #近03个月截至 贷款账户担保方式为组合（不含保证） 占比
                dict_out['loan_second_by03_gf_combine_warranty_count'] = loan_second_by03[loan_second_by03['guaranteeForm']=='组合(含保证)'].shape[0] #近03个月截至 贷款账户担保方式为组合(含保证) 计数
                dict_out['loan_second_by03_gf_combine_warranty_ratio'] = loan_second_by03[loan_second_by03['guaranteeForm']=='组合(含保证)'].shape[0]/len(loan_second_by03) #近03个月截至 贷款账户担保方式为组合(含保证) 占比
                dict_out['loan_second_by03_gf_mortgage_count'] = loan_second_by03[loan_second_by03['guaranteeForm']=='抵押'].shape[0] #近03个月截至 贷款账户担保方式为抵押 计数
                dict_out['loan_second_by03_gf_mortgage_ratio'] = loan_second_by03[loan_second_by03['guaranteeForm']=='抵押'].shape[0]/len(loan_second_by03) #近03个月截至 贷款账户担保方式为抵押 占比
                dict_out['loan_second_by03_gf_warranty_count'] = loan_second_by03[loan_second_by03['guaranteeForm']=='保证'].shape[0] #近03个月截至 贷款账户担保方式为保证计数
                dict_out['loan_second_by03_gf_warranty_ratio'] = loan_second_by03[loan_second_by03['guaranteeForm']=='保证'].shape[0]/len(loan_second_by03) #近03个月截至 贷款账户担保方式为保证 占比
                dict_out['loan_second_by03_gf_pledge_count'] = loan_second_by03[loan_second_by03['guaranteeForm']=='质押'].shape[0] #近03个月截至 贷款账户担保方式为质押 计数
                dict_out['loan_second_by03_gf_pledge_ratio'] = loan_second_by03[loan_second_by03['guaranteeForm']=='质押'].shape[0]/len(loan_second_by03) #近03个月截至 贷款账户担保方式为质押 占比
                dict_out['loan_second_by03_gf_farm_group_count'] = loan_second_by03[loan_second_by03['guaranteeForm']=='农户联保'].shape[0] #近03个月截至 贷款账户担保方式为农户联保 计数
                dict_out['loan_second_by03_gf_farm_group_ratio'] = loan_second_by03[loan_second_by03['guaranteeForm']=='农户联保'].shape[0]/len(loan_second_by03) #近03个月截至 贷款账户担保方式为农户联保 占比

                dict_out['loan_second_by03_businessType_giniimpurity'] = giniimpurity(loan_second_by03['businessType']) #近03个月截至 贷款账户业务种类基尼不纯度
                dict_out['loan_second_by03_bt_other_person_count'] = loan_second_by03[loan_second_by03['businessType']=='其他个人消费贷款'].shape[0] #近03个月截至 贷款账户业务种类为其他个人消费贷款 计数
                dict_out['loan_second_by03_bt_other_person_ratio'] = loan_second_by03[loan_second_by03['businessType']=='其他个人消费贷款'].shape[0]/len(loan_second_by03) #近03个月截至 贷款账户业务种类为其他个人消费贷款 占比
                dict_out['loan_second_by03_bt_other_loan_count'] = loan_second_by03[loan_second_by03['businessType']=='其他贷款'].shape[0] #近03个月截至 贷款账户业务种类为其他贷款 计数
                dict_out['loan_second_by03_bt_other_loan_ratio'] = loan_second_by03[loan_second_by03['businessType']=='其他贷款'].shape[0]/len(loan_second_by03) #近03个月截至 贷款账户业务种类为其他贷款 占比
                dict_out['loan_second_by03_bt_person_business_count'] = loan_second_by03[loan_second_by03['businessType']=='个人经营性贷款'].shape[0] #近03个月截至 贷款账户业务种类为个人经营性贷款 计数
                dict_out['loan_second_by03_bt_person_business_ratio'] = loan_second_by03[loan_second_by03['businessType']=='个人经营性贷款'].shape[0]/len(loan_second_by03) #近03个月截至 贷款账户业务种类为个人经营性贷款 占比
                dict_out['loan_second_by03_bt_farm_loan_count'] = loan_second_by03[loan_second_by03['businessType']=='农户贷款'].shape[0] #近03个月截至 贷款账户业务种类为农户贷款 计数
                dict_out['loan_second_by03_bt_farm_loan_ratio'] = loan_second_by03[loan_second_by03['businessType']=='农户贷款'].shape[0]/len(loan_second_by03) #近03个月截至 贷款账户业务种类为农户贷款 占比
                dict_out['loan_second_by03_bt_person_car_count'] = loan_second_by03[loan_second_by03['businessType']=='个人汽车消费贷款'].shape[0] #近03个月截至 贷款账户业务种类为个人汽车消费贷款 计数
                dict_out['loan_second_by03_bt_person_car_ratio'] = loan_second_by03[loan_second_by03['businessType']=='个人汽车消费贷款'].shape[0]/len(loan_second_by03) #近03个月截至 贷款账户业务种类为个人汽车消费贷款 占比
                dict_out['loan_second_by03_bt_person_study_count'] = loan_second_by03[loan_second_by03['businessType']=='个人助学贷款'].shape[0] #近03个月截至 贷款账户业务种类为个人助学贷款 计数
                dict_out['loan_second_by03_bt_person_study_ratio'] = loan_second_by03[loan_second_by03['businessType']=='个人助学贷款'].shape[0]/len(loan_second_by03) #近03个月截至 贷款账户业务种类为个人助学贷款 占比
                dict_out['loan_second_by03_bt_house_commercial_count'] = loan_second_by03[loan_second_by03['businessType']=='个人住房商业贷款'].shape[0] #近03个月截至 贷款账户业务种类为个人住房商业贷款 计数
                dict_out['loan_second_by03_bt_house_commercial_ratio'] = loan_second_by03[loan_second_by03['businessType']=='个人住房商业贷款'].shape[0]/len(loan_second_by03) #近03个月截至 贷款账户业务种类为个人住房商业贷款 占比
                dict_out['loan_second_by03_bt_finance_lease_count'] = loan_second_by03[loan_second_by03['businessType']=='融资租赁业务'].shape[0] #近03个月截至 贷款账户业务种类为融资租赁业务 计数
                dict_out['loan_second_by03_bt_finance_lease_ratio'] = loan_second_by03[loan_second_by03['businessType']=='融资租赁业务'].shape[0]/len(loan_second_by03) #近03个月截至 贷款账户业务种类为融资租赁业务 占比
                dict_out['loan_second_by03_bt_house_fund_count'] = loan_second_by03[loan_second_by03['businessType']=='个人住房公积金贷款'].shape[0] #近03个月截至 贷款账户业务种类为个人住房公积金贷款 计数
                dict_out['loan_second_by03_bt_house_fund_ratio'] = loan_second_by03[loan_second_by03['businessType']=='个人住房公积金贷款'].shape[0]/len(loan_second_by03) #近03个月截至 贷款账户业务种类为个人住房公积金贷款 占比
                dict_out['loan_second_by03_bt_person_house_count'] = loan_second_by03[loan_second_by03['businessType']=='个人商用房（含商住两用）贷款'].shape[0] #近03个月截至 贷款账户业务种类为个人商用房（含商住两用）贷款 计数
                dict_out['loan_second_by03_bt_person_house_ratio'] = loan_second_by03[loan_second_by03['businessType']=='个人商用房（含商住两用）贷款'].shape[0]/len(loan_second_by03) #近03个月截至 贷款账户业务种类为个人商用房（含商住两用）贷款 占比
                dict_out['loan_second_by03_bt_stock_pledge_count'] = loan_second_by03[loan_second_by03['businessType']=='股票质押式回购交易'].shape[0] #近03个月截至 贷款账户业务种类为股票质押式回购交易计数
                dict_out['loan_second_by03_bt_stock_pledge_ratio'] = loan_second_by03[loan_second_by03['businessType']=='股票质押式回购交易'].shape[0]/len(loan_second_by03) #近03个月截至 贷款账户业务种类为股票质押式回购交易 占比



            #近03个月截至 现行
            loan_second_by03_now = loan_second_by03[loan_second_by03.is_now==1]
            if len(loan_second_by03_now)>0:

                dict_out['loan_second_by03_now_loanGrantOrg_giniimpurity'] = giniimpurity(loan_second_by03_now['loanGrantOrg']) #近03个月截至 现行贷款账户管理机构详细基尼不纯度
                dict_out['loan_second_by03_now_rt_unknow_count'] = loan_second_by03_now[loan_second_by03_now['repayType']=='--'].shape[0] #近03个月截至 现行贷款账户还款方式为-- 计数
                dict_out['loan_second_by03_now_org_consumer_finance_count'] = loan_second_by03_now[loan_second_by03_now['org']=='消费金融公司'].shape[0] #近03个月截至 现行贷款账户管理机构为消费金融公司 计数
                dict_out['loan_second_by03_now_org_lease_finance_ratio'] = loan_second_by03_now[loan_second_by03_now['org']=='融资租赁公司'].shape[0]/len(loan_second_by03_now) #近03个月截至 现行贷款账户管理机构为融资租赁公司 占比
                dict_out['loan_second_by03_now_month60_State_countN_max'] = loan_second_by03_now['month60_State_countN'].max()#var最大值
                dict_out['loan_second_by03_now_month60_State_countNr_max'] = loan_second_by03_now['month60_State_countNr'].max()#var最大值
                dict_out['loan_second_by03_now_month60_State_countNr_min'] = loan_second_by03_now['month60_State_countNr'].min() #var最小值
                dict_out['loan_second_by03_now_month60_State_countNr_range'] = (dict_out['loan_second_by03_now_month60_State_countNr_max']-dict_out['loan_second_by03_now_month60_State_countNr_min'])/dict_out['loan_second_by03_now_month60_State_countNr_max'] if dict_out['loan_second_by03_now_month60_State_countNr_max']>0 else np.nan #var区间率
                dict_out['loan_second_by03_now_rf_other_count'] = loan_second_by03_now[loan_second_by03_now['repayFrequency']=='其他'].shape[0] #近03个月截至 现行贷款账户还款频率为其他 计数
                dict_out['loan_second_by03_now_month60_State_count2_max'] = loan_second_by03_now['month60_State_count2'].max()#var最大值
                dict_out['loan_second_by03_now_repayTerm_ratio_min'] = loan_second_by03_now['repayTerm_ratio'].min() #var最小值
                dict_out['loan_second_by03_now_month60_State_count2_mean'] = loan_second_by03_now['month60_State_count2'].mean() #var平均值
                dict_out['loan_second_by03_now_month60_to_report_mean_sum'] = loan_second_by03_now['month60_to_report_mean'].sum() #var求和
                dict_out['loan_second_by03_now_bt_other_person_ratio'] = loan_second_by03_now[loan_second_by03_now['businessType']=='其他个人消费贷款'].shape[0]/len(loan_second_by03_now) #近03个月截至 现行贷款账户业务种类为其他个人消费贷款 占比
                dict_out['loan_second_by03_now_balance_ratio_mean'] = loan_second_by03_now['balance_ratio'].mean() #var平均值
                dict_out['loan_second_by03_now_month60_State_countNullr_mean'] = loan_second_by03_now['month60_State_countNullr'].mean() #var平均值
                dict_out['loan_second_by03_now_org_consumer_finance_ratio'] = loan_second_by03_now[loan_second_by03_now['org']=='消费金融公司'].shape[0]/len(loan_second_by03_now) #近03个月截至 现行贷款账户管理机构为消费金融公司 占比
                dict_out['loan_second_by03_now_loanAmount_max'] = loan_second_by03_now['loanAmount'].max()#var最大值
                dict_out['loan_second_by03_now_month60_State_count1r_mean'] = loan_second_by03_now['month60_State_count1r'].mean() #var平均值
                dict_out['loan_second_by03_now_rf_month_ratio'] = loan_second_by03_now[loan_second_by03_now['repayFrequency']=='月'].shape[0]/len(loan_second_by03_now) #近03个月截至 现行贷款账户还款频率为月 占比
                dict_out['loan_second_by03_now_month60_State_count2r_sum'] = loan_second_by03_now['month60_State_count2r'].sum() #var求和
                dict_out['loan_second_by03_now_loanGrantOrg_nunique'] = loan_second_by03_now['loanGrantOrg'].nunique() #近03个月截至 现行贷款账户管理机构详细nunique
                dict_out['loan_second_by03_now_org_commercial_bank_ratio'] = loan_second_by03_now[loan_second_by03_now['org']=='商业银行'].shape[0]/len(loan_second_by03_now) #近03个月截至 现行贷款账户管理机构为商业银行 占比
                dict_out['loan_second_by03_now_gf_crdit_ratio'] = loan_second_by03_now[loan_second_by03_now['guaranteeForm']=='信用/免担保'].shape[0]/len(loan_second_by03_now) #近03个月截至 现行贷款账户担保方式为信用/免担保 占比
                dict_out['loan_second_by03_now_rf_other_count'] = loan_second_by03_now[loan_second_by03_now['repayFrequency']=='其他'].shape[0] #近03个月截至 现行贷款账户还款频率为其他 计数
                dict_out['loan_second_by03_now_repayFrequency_giniimpurity'] = giniimpurity(loan_second_by03_now['repayFrequency']) #近03个月截至 现行贷款账户还款频率基尼不纯度
                dict_out['loan_second_by03_now_org_consumer_finance_ratio'] = loan_second_by03_now[loan_second_by03_now['org']=='消费金融公司'].shape[0]/len(loan_second_by03_now) #近03个月截至 现行贷款账户管理机构为消费金融公司 占比
                dict_out['loan_second_by03_now_bt_other_person_count'] = loan_second_by03_now[loan_second_by03_now['businessType']=='其他个人消费贷款'].shape[0] #近03个月截至 现行贷款账户业务种类为其他个人消费贷款 计数
                dict_out['loan_second_by03_now_org_micro_loan_count'] = loan_second_by03_now[loan_second_by03_now['org']=='小额贷款公司'].shape[0] #近03个月截至 现行贷款账户管理机构为小额贷款公司 计数
                dict_out['loan_second_by03_now_org_consumer_finance_count'] = loan_second_by03_now[loan_second_by03_now['org']=='消费金融公司'].shape[0] #近03个月截至 现行贷款账户管理机构为消费金融公司 计数
                dict_out['loan_second_by03_now_gf_other_ratio'] = loan_second_by03_now[loan_second_by03_now['guaranteeForm']=='其他'].shape[0]/len(loan_second_by03_now) #近03个月截至 现行贷款账户担保方式为其他 占比
                dict_out['loan_second_by03_now_month60_to_report_mean_max'] = loan_second_by03_now['month60_to_report_mean'].max()#var最大值
                dict_out['loan_second_by03_now_month60_to_report_mean_min'] = loan_second_by03_now['month60_to_report_mean'].min() #var最小值比率
                dict_out['loan_second_by03_now_month60_to_report_mean_range'] = (dict_out['loan_second_by03_now_month60_to_report_mean_max']-dict_out['loan_second_by03_now_month60_to_report_mean_min'])/dict_out['loan_second_by03_now_month60_to_report_mean_max'] if dict_out['loan_second_by03_now_month60_to_report_mean_max']>0 else np.nan #var区间率
                dict_out['loan_second_by03_now_month60_State_countNullr_mean'] = loan_second_by03_now['month60_State_countNullr'].mean()  #var平均值比率
                dict_out['loan_second_by03_now_repayMons_ratio_max'] = loan_second_by03_now['repayMons_ratio'].max()#var最大值
                dict_out['loan_second_by03_now_repayMons_ratio_min'] = loan_second_by03_now['repayMons_ratio'].min() #var最小值比率
                dict_out['loan_second_by03_now_repayMons_ratio_range'] = (dict_out['loan_second_by03_now_repayMons_ratio_max']-dict_out['loan_second_by03_now_repayMons_ratio_min'])/dict_out['loan_second_by03_now_repayMons_ratio_max'] if dict_out['loan_second_by03_now_repayMons_ratio_max']>0 else np.nan #var区间率
                dict_out['loan_second_by03_now_repayMons_sum'] = loan_second_by03_now['repayMons'].sum() #var求和比率
                dict_out['loan_second_by03_now_month60_State_num_size_max'] = loan_second_by03_now['month60_State_num_size'].max()#var最大值
                dict_out['loan_second_by03_now_month60_State_num_size_min'] = loan_second_by03_now['month60_State_num_size'].min() #var最小值比率
                dict_out['loan_second_by03_now_month60_State_num_size_range'] = (dict_out['loan_second_by03_now_month60_State_num_size_max']-dict_out['loan_second_by03_now_month60_State_num_size_min'])/dict_out['loan_second_by03_now_month60_State_num_size_max'] if dict_out['loan_second_by03_now_month60_State_num_size_max']>0 else np.nan #var区间率
                dict_out['loan_second_by03_now_month60_State_countC_mean'] = loan_second_by03_now['month60_State_countC'].mean()  #var平均值比率
                dict_out['loan_second_by03_now_repayMons_max'] = loan_second_by03_now['repayMons'].max()#var最大值
                dict_out['loan_second_by03_now_repayMons_min'] = loan_second_by03_now['repayMons'].min() #var最小值比率
                dict_out['loan_second_by03_now_repayMons_range'] = (dict_out['loan_second_by03_now_repayMons_max']-dict_out['loan_second_by03_now_repayMons_min'])/dict_out['loan_second_by03_now_repayMons_max'] if dict_out['loan_second_by03_now_repayMons_max']>0 else np.nan #var区间率
                dict_out['loan_second_by03_now_startDate_to_report_max'] = loan_second_by03_now['startDate_to_report'].max() #var最大值比率
                dict_out['loan_second_by03_now_month60_State_countNullr_max'] = loan_second_by03_now['month60_State_countNullr'].max()#var最大值
                dict_out['loan_second_by03_now_month60_State_countNullr_min'] = loan_second_by03_now['month60_State_countNullr'].min() #var最小值比率
                dict_out['loan_second_by03_now_month60_State_countNullr_range'] = (dict_out['loan_second_by03_now_month60_State_countNullr_max']-dict_out['loan_second_by03_now_month60_State_countNullr_min'])/dict_out['loan_second_by03_now_month60_State_countNullr_max'] if dict_out['loan_second_by03_now_month60_State_countNullr_max']>0 else np.nan #var区间率
                dict_out['loan_second_by03_now_month60_to_report_max_max'] = loan_second_by03_now['month60_to_report_max'].max()#var最大值
                dict_out['loan_second_by03_now_month60_to_report_max_min'] = loan_second_by03_now['month60_to_report_max'].min() #var最小值比率
                dict_out['loan_second_by03_now_month60_to_report_max_range'] = (dict_out['loan_second_by03_now_month60_to_report_max_max']-dict_out['loan_second_by03_now_month60_to_report_max_min'])/dict_out['loan_second_by03_now_month60_to_report_max_max'] if dict_out['loan_second_by03_now_month60_to_report_max_max']>0 else np.nan #var区间率
                dict_out['loan_second_by03_now_startDate_to_report_mean'] = loan_second_by03_now['startDate_to_report'].mean()  #var平均值比率
                dict_out['loan_second_by03_now_month60_State_countNull_sum'] = loan_second_by03_now['month60_State_countNull'].sum() #var求和比率
                dict_out['loan_second_by03_now_month60_State_num_size_max'] = loan_second_by03_now['month60_State_num_size'].max() #var最大值比率
                dict_out['loan_second_by03_now_repayAmt_max'] = loan_second_by03_now['repayAmt'].max() #var最大值比率
                dict_out['loan_second_by03_now_month60_State_countN_mean'] = loan_second_by03_now['month60_State_countN'].mean()  #var平均值比率
                dict_out['loan_second_by03_now_repayAmt_max'] = loan_second_by03_now['repayAmt'].max()#var最大值
                dict_out['loan_second_by03_now_repayAmt_min'] = loan_second_by03_now['repayAmt'].min() #var最小值比率
                dict_out['loan_second_by03_now_repayAmt_range'] = (dict_out['loan_second_by03_now_repayAmt_max']-dict_out['loan_second_by03_now_repayAmt_min'])/dict_out['loan_second_by03_now_repayAmt_max'] if dict_out['loan_second_by03_now_repayAmt_max']>0 else np.nan #var区间率
                dict_out['loan_second_by03_now_month60_to_report_max_mean'] = loan_second_by03_now['month60_to_report_max'].mean()  #var平均值比率
                dict_out['loan_second_by03_now_month60_to_report_mean_mean'] = loan_second_by03_now['month60_to_report_mean'].mean()  #var平均值比率
                dict_out['loan_second_by03_now_repayTerms_sum'] = loan_second_by03_now['repayTerms'].sum() #var求和比率
                dict_out['loan_second_by03_now_month60_to_report_min_sum'] = loan_second_by03_now['month60_to_report_min'].sum() #var求和比率
                dict_out['loan_second_by03_now_planRepayAmount_max'] = loan_second_by03_now['planRepayAmount'].max()#var最大值
                dict_out['loan_second_by03_now_planRepayAmount_min'] = loan_second_by03_now['planRepayAmount'].min() #var最小值比率
                dict_out['loan_second_by03_now_planRepayAmount_range'] = (dict_out['loan_second_by03_now_planRepayAmount_max']-dict_out['loan_second_by03_now_planRepayAmount_min'])/dict_out['loan_second_by03_now_planRepayAmount_max'] if dict_out['loan_second_by03_now_planRepayAmount_max']>0 else np.nan #var区间率
                dict_out['loan_second_by03_now_loanAmount_min'] = loan_second_by03_now['loanAmount'].min() #var最小值比率
                dict_out['loan_second_by03_now_loanAmount_mean'] = loan_second_by03_now['loanAmount'].mean()  #var平均值比率
                dict_out['loan_second_by03_now_byDate_to_report_sum'] = loan_second_by03_now['byDate_to_report'].sum() #var求和比率
                dict_out['loan_second_by03_now_month60_to_report_min_mean'] = loan_second_by03_now['month60_to_report_min'].mean()  #var平均值比率
                dict_out['loan_second_by03_now_repayedAmount_max'] = loan_second_by03_now['repayedAmount'].max()#var最大值
                dict_out['loan_second_by03_now_repayedAmount_min'] = loan_second_by03_now['repayedAmount'].min() #var最小值比率
                dict_out['loan_second_by03_now_repayedAmount_range'] = (dict_out['loan_second_by03_now_repayedAmount_max']-dict_out['loan_second_by03_now_repayedAmount_min'])/dict_out['loan_second_by03_now_repayedAmount_max'] if dict_out['loan_second_by03_now_repayedAmount_max']>0 else np.nan #var区间率
                dict_out['loan_second_by03_now_month60_State_countNullr_max'] = loan_second_by03_now['month60_State_countNullr'].max() #var最大值比率
                dict_out['loan_second_by03_now_loanAmount_max'] = loan_second_by03_now['loanAmount'].max() #var最大值比率
                dict_out['loan_second_by03_now_repayMons_ratio_max'] = loan_second_by03_now['repayMons_ratio'].max() #var最大值比率
                dict_out['loan_second_by03_now_balance_max'] = loan_second_by03_now['balance'].max()#var最大值
                dict_out['loan_second_by03_now_balance_min'] = loan_second_by03_now['balance'].min() #var最小值比率
                dict_out['loan_second_by03_now_balance_range'] = (dict_out['loan_second_by03_now_balance_max']-dict_out['loan_second_by03_now_balance_min'])/dict_out['loan_second_by03_now_balance_max'] if dict_out['loan_second_by03_now_balance_max']>0 else np.nan #var区间率
                dict_out['loan_second_by03_nowR_month60_to_report_mean_range'] = dict_out['loan_second_by03_now_month60_to_report_mean_range']/dict_out['loan_second_by03_month60_to_report_mean_range'] if dict_out['loan_second_by03_month60_to_report_mean_range']>0 else np.nan #var最大值比率
                dict_out['loan_second_by03_nowR_month60_State_countNullr_mean'] = dict_out['loan_second_by03_now_month60_State_countNullr_mean']/dict_out['loan_second_by03_month60_State_countNullr_mean'] if dict_out['loan_second_by03_month60_State_countNullr_mean']>0 else np.nan #var最大值比率
                dict_out['loan_second_by03_nowR_repayMons_ratio_range'] = dict_out['loan_second_by03_now_repayMons_ratio_range']/dict_out['loan_second_by03_repayMons_ratio_range'] if dict_out['loan_second_by03_repayMons_ratio_range']>0 else np.nan #var最大值比率
                dict_out['loan_second_by03_now_loanGrantOrg_nunique'] = loan_second_by03_now['loanGrantOrg'].nunique() #近03个月截至 现行贷款账户管理机构详细nunique
                dict_out['loan_second_by03_nowR_loanGrantOrg_nunique'] = dict_out['loan_second_by03_now_loanGrantOrg_nunique']/dict_out['loan_second_by03_loanGrantOrg_nunique'] if dict_out['loan_second_by03_loanGrantOrg_nunique']>0 else np.nan #var最大值比率
                dict_out['loan_second_by03_nowR_repayMons_sum'] = dict_out['loan_second_by03_now_repayMons_sum']/dict_out['loan_second_by03_repayMons_sum'] if dict_out['loan_second_by03_repayMons_sum']>0 else np.nan #var最大值比率
                dict_out['loan_second_by03_nowR_month60_State_num_size_range'] = dict_out['loan_second_by03_now_month60_State_num_size_range']/dict_out['loan_second_by03_month60_State_num_size_range'] if dict_out['loan_second_by03_month60_State_num_size_range']>0 else np.nan #var最大值比率
                dict_out['loan_second_by03_nowR_month60_State_countC_mean'] = dict_out['loan_second_by03_now_month60_State_countC_mean']/dict_out['loan_second_by03_month60_State_countC_mean'] if dict_out['loan_second_by03_month60_State_countC_mean']>0 else np.nan #var最大值比率
                dict_out['loan_second_by03_now_org_commercial_bank_ratio'] = loan_second_by03_now[loan_second_by03_now['org']=='商业银行'].shape[0]/len(loan_second_by03_now) #近03个月截至 现行贷款账户管理机构为商业银行 占比
                dict_out['loan_second_by03_nowR_org_commercial_bank_ratio'] = dict_out['loan_second_by03_now_org_commercial_bank_ratio']/dict_out['loan_second_by03_org_commercial_bank_ratio'] if dict_out['loan_second_by03_org_commercial_bank_ratio']>0 else np.nan #var最大值比率
                dict_out['loan_second_by03_nowR_repayMons_range'] = dict_out['loan_second_by03_now_repayMons_range']/dict_out['loan_second_by03_repayMons_range'] if dict_out['loan_second_by03_repayMons_range']>0 else np.nan #var最大值比率
                dict_out['loan_second_by03_nowR_startDate_to_report_max'] = dict_out['loan_second_by03_now_startDate_to_report_max']/dict_out['loan_second_by03_startDate_to_report_max'] if dict_out['loan_second_by03_startDate_to_report_max']>0 else np.nan #var最大值比率
                dict_out['loan_second_by03_nowR_month60_State_countNullr_range'] = dict_out['loan_second_by03_now_month60_State_countNullr_range']/dict_out['loan_second_by03_month60_State_countNullr_range'] if dict_out['loan_second_by03_month60_State_countNullr_range']>0 else np.nan #var最大值比率
                dict_out['loan_second_by03_nowR_month60_to_report_max_range'] = dict_out['loan_second_by03_now_month60_to_report_max_range']/dict_out['loan_second_by03_month60_to_report_max_range'] if dict_out['loan_second_by03_month60_to_report_max_range']>0 else np.nan #var最大值比率
                dict_out['loan_second_by03_nowR_startDate_to_report_mean'] = dict_out['loan_second_by03_now_startDate_to_report_mean']/dict_out['loan_second_by03_startDate_to_report_mean'] if dict_out['loan_second_by03_startDate_to_report_mean']>0 else np.nan #var最大值比率
                dict_out['loan_second_by03_now_gf_crdit_ratio'] = loan_second_by03_now[loan_second_by03_now['guaranteeForm']=='信用/免担保'].shape[0]/len(loan_second_by03_now) #近03个月截至 现行贷款账户担保方式为信用/免担保 占比
                dict_out['loan_second_by03_nowR_gf_crdit_ratio'] = dict_out['loan_second_by03_now_gf_crdit_ratio']/dict_out['loan_second_by03_gf_crdit_ratio'] if dict_out['loan_second_by03_gf_crdit_ratio']>0 else np.nan #var最大值比率
                dict_out['loan_second_by03_nowR_month60_State_countNull_sum'] = dict_out['loan_second_by03_now_month60_State_countNull_sum']/dict_out['loan_second_by03_month60_State_countNull_sum'] if dict_out['loan_second_by03_month60_State_countNull_sum']>0 else np.nan #var最大值比率
                dict_out['loan_second_by03_nowR_month60_State_num_size_max'] = dict_out['loan_second_by03_now_month60_State_num_size_max']/dict_out['loan_second_by03_month60_State_num_size_max'] if dict_out['loan_second_by03_month60_State_num_size_max']>0 else np.nan #var最大值比率
                dict_out['loan_second_by03_now_rf_other_count'] = loan_second_by03_now[loan_second_by03_now['repayFrequency']=='其他'].shape[0] #近03个月截至 现行贷款账户还款频率为其他 计数
                dict_out['loan_second_by03_nowR_rf_other_count'] = dict_out['loan_second_by03_now_rf_other_count']/dict_out['loan_second_by03_rf_other_count'] if dict_out['loan_second_by03_rf_other_count']>0 else np.nan #var最大值比率
                dict_out['loan_second_by03_nowR_repayAmt_max'] = dict_out['loan_second_by03_now_repayAmt_max']/dict_out['loan_second_by03_repayAmt_max'] if dict_out['loan_second_by03_repayAmt_max']>0 else np.nan #var最大值比率
                dict_out['loan_second_by03_nowR_month60_State_countN_mean'] = dict_out['loan_second_by03_now_month60_State_countN_mean']/dict_out['loan_second_by03_month60_State_countN_mean'] if dict_out['loan_second_by03_month60_State_countN_mean']>0 else np.nan #var最大值比率
                dict_out['loan_second_by03_nowR_repayAmt_range'] = dict_out['loan_second_by03_now_repayAmt_range']/dict_out['loan_second_by03_repayAmt_range'] if dict_out['loan_second_by03_repayAmt_range']>0 else np.nan #var最大值比率
                dict_out['loan_second_by03_now_repayFrequency_giniimpurity'] = giniimpurity(loan_second_by03_now['repayFrequency']) #近03个月截至 现行贷款账户还款频率基尼不纯度
                dict_out['loan_second_by03_nowR_repayFrequency_giniimpurity'] = dict_out['loan_second_by03_now_repayFrequency_giniimpurity']/dict_out['loan_second_by03_repayFrequency_giniimpurity'] if dict_out['loan_second_by03_repayFrequency_giniimpurity']>0 else np.nan #var最大值比率
                dict_out['loan_second_by03_nowR_month60_to_report_max_mean'] = dict_out['loan_second_by03_now_month60_to_report_max_mean']/dict_out['loan_second_by03_month60_to_report_max_mean'] if dict_out['loan_second_by03_month60_to_report_max_mean']>0 else np.nan #var最大值比率
                dict_out['loan_second_by03_now_org_consumer_finance_ratio'] = loan_second_by03_now[loan_second_by03_now['org']=='消费金融公司'].shape[0]/len(loan_second_by03_now) #近03个月截至 现行贷款账户管理机构为消费金融公司 占比
                dict_out['loan_second_by03_nowR_org_consumer_finance_ratio'] = dict_out['loan_second_by03_now_org_consumer_finance_ratio']/dict_out['loan_second_by03_org_consumer_finance_ratio'] if dict_out['loan_second_by03_org_consumer_finance_ratio']>0 else np.nan #var最大值比率
                dict_out['loan_second_by03_nowR_month60_to_report_mean_mean'] = dict_out['loan_second_by03_now_month60_to_report_mean_mean']/dict_out['loan_second_by03_month60_to_report_mean_mean'] if dict_out['loan_second_by03_month60_to_report_mean_mean']>0 else np.nan #var最大值比率
                dict_out['loan_second_by03_nowR_repayTerms_sum'] = dict_out['loan_second_by03_now_repayTerms_sum']/dict_out['loan_second_by03_repayTerms_sum'] if dict_out['loan_second_by03_repayTerms_sum']>0 else np.nan #var最大值比率
                dict_out['loan_second_by03_now_bt_other_person_count'] = loan_second_by03_now[loan_second_by03_now['businessType']=='其他个人消费贷款'].shape[0] #近03个月截至 现行贷款账户业务种类为其他个人消费贷款 计数
                dict_out['loan_second_by03_nowR_bt_other_person_count'] = dict_out['loan_second_by03_now_bt_other_person_count']/dict_out['loan_second_by03_bt_other_person_count'] if dict_out['loan_second_by03_bt_other_person_count']>0 else np.nan #var最大值比率
                dict_out['loan_second_by03_nowR_month60_to_report_min_sum'] = dict_out['loan_second_by03_now_month60_to_report_min_sum']/dict_out['loan_second_by03_month60_to_report_min_sum'] if dict_out['loan_second_by03_month60_to_report_min_sum']>0 else np.nan #var最大值比率
                dict_out['loan_second_by03_nowR_planRepayAmount_range'] = dict_out['loan_second_by03_now_planRepayAmount_range']/dict_out['loan_second_by03_planRepayAmount_range'] if dict_out['loan_second_by03_planRepayAmount_range']>0 else np.nan #var最大值比率
                dict_out['loan_second_by03_now_org_micro_loan_count'] = loan_second_by03_now[loan_second_by03_now['org']=='小额贷款公司'].shape[0] #近03个月截至 现行贷款账户管理机构为小额贷款公司 计数
                dict_out['loan_second_by03_nowR_org_micro_loan_count'] = dict_out['loan_second_by03_now_org_micro_loan_count']/dict_out['loan_second_by03_org_micro_loan_count'] if dict_out['loan_second_by03_org_micro_loan_count']>0 else np.nan #var最大值比率
                dict_out['loan_second_by03_nowR_loanAmount_min'] = dict_out['loan_second_by03_now_loanAmount_min']/dict_out['loan_second_by03_loanAmount_min'] if dict_out['loan_second_by03_loanAmount_min']>0 else np.nan #var最大值比率
                dict_out['loan_second_by03_now_org_consumer_finance_count'] = loan_second_by03_now[loan_second_by03_now['org']=='消费金融公司'].shape[0] #近03个月截至 现行贷款账户管理机构为消费金融公司 计数
                dict_out['loan_second_by03_nowR_org_consumer_finance_count'] = dict_out['loan_second_by03_now_org_consumer_finance_count']/dict_out['loan_second_by03_org_consumer_finance_count'] if dict_out['loan_second_by03_org_consumer_finance_count']>0 else np.nan #var最大值比率
                dict_out['loan_second_by03_nowR_loanAmount_mean'] = dict_out['loan_second_by03_now_loanAmount_mean']/dict_out['loan_second_by03_loanAmount_mean'] if dict_out['loan_second_by03_loanAmount_mean']>0 else np.nan #var最大值比率
                dict_out['loan_second_by03_nowR_byDate_to_report_sum'] = dict_out['loan_second_by03_now_byDate_to_report_sum']/dict_out['loan_second_by03_byDate_to_report_sum'] if dict_out['loan_second_by03_byDate_to_report_sum']>0 else np.nan #var最大值比率
                dict_out['loan_second_by03_nowR_month60_to_report_min_mean'] = dict_out['loan_second_by03_now_month60_to_report_min_mean']/dict_out['loan_second_by03_month60_to_report_min_mean'] if dict_out['loan_second_by03_month60_to_report_min_mean']>0 else np.nan #var最大值比率
                dict_out['loan_second_by03_nowR_repayedAmount_range'] = dict_out['loan_second_by03_now_repayedAmount_range']/dict_out['loan_second_by03_repayedAmount_range'] if dict_out['loan_second_by03_repayedAmount_range']>0 else np.nan #var最大值比率
                dict_out['loan_second_by03_nowR_month60_State_countNullr_max'] = dict_out['loan_second_by03_now_month60_State_countNullr_max']/dict_out['loan_second_by03_month60_State_countNullr_max'] if dict_out['loan_second_by03_month60_State_countNullr_max']>0 else np.nan #var最大值比率
                dict_out['loan_second_by03_now_gf_other_ratio'] = loan_second_by03_now[loan_second_by03_now['guaranteeForm']=='其他'].shape[0]/len(loan_second_by03_now) #近03个月截至 现行贷款账户担保方式为其他 占比
                dict_out['loan_second_by03_nowR_gf_other_ratio'] = dict_out['loan_second_by03_now_gf_other_ratio']/dict_out['loan_second_by03_gf_other_ratio'] if dict_out['loan_second_by03_gf_other_ratio']>0 else np.nan #var最大值比率
                dict_out['loan_second_by03_nowR_loanAmount_max'] = dict_out['loan_second_by03_now_loanAmount_max']/dict_out['loan_second_by03_loanAmount_max'] if dict_out['loan_second_by03_loanAmount_max']>0 else np.nan #var最大值比率
                dict_out['loan_second_by03_nowR_repayMons_ratio_max'] = dict_out['loan_second_by03_now_repayMons_ratio_max']/dict_out['loan_second_by03_repayMons_ratio_max'] if dict_out['loan_second_by03_repayMons_ratio_max']>0 else np.nan #var最大值比率
                dict_out['loan_second_by03_nowR_balance_range'] = dict_out['loan_second_by03_now_balance_range']/dict_out['loan_second_by03_balance_range'] if dict_out['loan_second_by03_balance_range']>0 else np.nan #var最大值比率

            
            #近03个月截至 非循环贷账户
            loan_second_by03_ncycle = loan_second_by03[loan_second_by03['class']=='非循环贷账户']
            if len(loan_second_by03_ncycle)>0:

                dict_out['loan_second_by03_ncycle_bt_finance_lease_ratio'] = loan_second_by03_ncycle[loan_second_by03_ncycle['businessType']=='融资租赁业务'].shape[0]/len(loan_second_by03_ncycle) #近03个月截至 非循环贷账户 贷款账户业务种类为融资租赁业务 占比
                dict_out['loan_second_by03_ncycle_balance_ratio_max'] = loan_second_by03_ncycle['balance_ratio'].max()#var最大值
                dict_out['loan_second_by03_ncycle_balance_ratio_min'] = loan_second_by03_ncycle['balance_ratio'].min() #var最小值
                dict_out['loan_second_by03_ncycle_balance_ratio_range'] = (dict_out['loan_second_by03_ncycle_balance_ratio_max']-dict_out['loan_second_by03_ncycle_balance_ratio_min'])/dict_out['loan_second_by03_ncycle_balance_ratio_max'] if dict_out['loan_second_by03_ncycle_balance_ratio_max']>0 else np.nan #var区间率
                dict_out['loan_second_by03_ncycle_month60_State_countN_sum'] = loan_second_by03_ncycle['month60_State_countN'].sum() #var求和
                dict_out['loan_second_by03_ncycle_rf_month_ratio'] = loan_second_by03_ncycle[loan_second_by03_ncycle['repayFrequency']=='月'].shape[0]/len(loan_second_by03_ncycle) #近03个月截至 非循环贷账户 贷款账户还款频率为月 占比
                dict_out['loan_second_by03_ncycle_planRepayAmount_max'] = loan_second_by03_ncycle['planRepayAmount'].max()#var最大值
                dict_out['loan_second_by03_ncycle_planRepayAmount_min'] = loan_second_by03_ncycle['planRepayAmount'].min() #var最小值
                dict_out['loan_second_by03_ncycle_planRepayAmount_range'] = (dict_out['loan_second_by03_ncycle_planRepayAmount_max']-dict_out['loan_second_by03_ncycle_planRepayAmount_min'])/dict_out['loan_second_by03_ncycle_planRepayAmount_max'] if dict_out['loan_second_by03_ncycle_planRepayAmount_max']>0 else np.nan #var区间率
                dict_out['loan_second_by03_ncycle_org_commercial_bank_ratio'] = loan_second_by03_ncycle[loan_second_by03_ncycle['org']=='商业银行'].shape[0]/len(loan_second_by03_ncycle) #近03个月截至 非循环贷账户 贷款账户管理机构为商业银行 占比
                dict_out['loan_second_by03_ncycle_bt_other_loan_ratio'] = loan_second_by03_ncycle[loan_second_by03_ncycle['businessType']=='其他贷款'].shape[0]/len(loan_second_by03_ncycle) #近03个月截至 非循环贷账户 贷款账户业务种类为其他贷款 占比
                dict_out['loan_second_by03_ncycle_rf_other_ratio'] = loan_second_by03_ncycle[loan_second_by03_ncycle['repayFrequency']=='其他'].shape[0]/len(loan_second_by03_ncycle) #近03个月截至 非循环贷账户 贷款账户还款频率为其他 占比
                dict_out['loan_second_by03_ncycle_gf_crdit_ratio'] = loan_second_by03_ncycle[loan_second_by03_ncycle['guaranteeForm']=='信用/免担保'].shape[0]/len(loan_second_by03_ncycle) #近03个月截至 非循环贷账户 贷款账户担保方式为信用/免担保 占比
                dict_out['loan_second_by03_ncycle_org_consumer_finance_ratio'] = loan_second_by03_ncycle[loan_second_by03_ncycle['org']=='消费金融公司'].shape[0]/len(loan_second_by03_ncycle) #近03个月截至 非循环贷账户 贷款账户管理机构为消费金融公司 占比
                dict_out['loan_second_by03_ncycle_month60_State_count2r_max'] = loan_second_by03_ncycle['month60_State_count2r'].max()#var最大值
                dict_out['loan_second_by03_ncycle_repayMons_mean'] = loan_second_by03_ncycle['repayMons'].mean() #var平均值
                dict_out['loan_second_by03_ncycle_startDate_to_report_max'] = loan_second_by03_ncycle['startDate_to_report'].max()#var最大值
                dict_out['loan_second_by03_ncycle_balance_ratio_mean'] = loan_second_by03_ncycle['balance_ratio'].mean() #var平均值
                dict_out['loan_second_by03_ncycle_leftRepayTerms_mean'] = loan_second_by03_ncycle['leftRepayTerms'].mean() #var平均值
                dict_out['loan_second_by03_ncycle_repayAmt_max'] = loan_second_by03_ncycle['repayAmt'].max()#var最大值
                dict_out['loan_second_by03_ncycle_repayedAmount_min'] = loan_second_by03_ncycle['repayedAmount'].min() #var最小值
                dict_out['loan_second_by03_ncycle_month60_State_count3r_max'] = loan_second_by03_ncycle['month60_State_count3r'].max()#var最大值
                dict_out['loan_second_by03_ncycle_month60_State_count2r_sum'] = loan_second_by03_ncycle['month60_State_count2r'].sum() #var求和
                dict_out['loan_second_by03_ncycle_balance_min'] = loan_second_by03_ncycle['balance'].min() #var最小值
                dict_out['loan_second_by03_ncycle_month60_State_countNullr_max'] = loan_second_by03_ncycle['month60_State_countNullr'].max()#var最大值
                dict_out['loan_second_by03_ncycle_month60_State_countNullr_min'] = loan_second_by03_ncycle['month60_State_countNullr'].min() #var最小值
                dict_out['loan_second_by03_ncycle_month60_State_countNullr_range'] = (dict_out['loan_second_by03_ncycle_month60_State_countNullr_max']-dict_out['loan_second_by03_ncycle_month60_State_countNullr_min'])/dict_out['loan_second_by03_ncycle_month60_State_countNullr_max'] if dict_out['loan_second_by03_ncycle_month60_State_countNullr_max']>0 else np.nan #var区间率
                dict_out['loan_second_by03_ncycle_month60_State_countC_sum'] = loan_second_by03_ncycle['month60_State_countC'].sum() #var求和
                dict_out['loan_second_by03_ncycle_repayMons_max'] = loan_second_by03_ncycle['repayMons'].max()#var最大值
                dict_out['loan_second_by03_ncycle_repayMons_min'] = loan_second_by03_ncycle['repayMons'].min() #var最小值
                dict_out['loan_second_by03_ncycle_repayMons_range'] = (dict_out['loan_second_by03_ncycle_repayMons_max']-dict_out['loan_second_by03_ncycle_repayMons_min'])/dict_out['loan_second_by03_ncycle_repayMons_max'] if dict_out['loan_second_by03_ncycle_repayMons_max']>0 else np.nan #var区间率
                dict_out['loan_second_by03_ncycle_month60_State_countCr_sum'] = loan_second_by03_ncycle['month60_State_countCr'].sum() #var求和
                dict_out['loan_second_by03_ncycle_planRepayAmount_mean'] = loan_second_by03_ncycle['planRepayAmount'].mean() #var平均值
                dict_out['loan_second_by03_ncycle_month60_State_count3r_mean'] = loan_second_by03_ncycle['month60_State_count3r'].mean() #var平均值
                dict_out['loan_second_by03_ncycle_repayTerms_mean'] = loan_second_by03_ncycle['repayTerms'].mean() #var平均值
                dict_out['loan_second_by03_ncycle_month60_State_countN_mean'] = loan_second_by03_ncycle['month60_State_countN'].mean() #var平均值
                dict_out['loan_second_by03_ncycle_gf_crdit_ratio'] = loan_second_by03_ncycle[loan_second_by03_ncycle['guaranteeForm']=='信用/免担保'].shape[0]/len(loan_second_by03_ncycle) #近03个月截至 非循环贷账户 贷款账户担保方式为信用/免担保 占比
                dict_out['loan_second_by03_ncycle_balance_max'] = loan_second_by03_ncycle['balance'].max()#var最大值
                dict_out['loan_second_by03_ncycle_balance_min'] = loan_second_by03_ncycle['balance'].min() #var最小值比率
                dict_out['loan_second_by03_ncycle_balance_range'] = (dict_out['loan_second_by03_ncycle_balance_max']-dict_out['loan_second_by03_ncycle_balance_min'])/dict_out['loan_second_by03_ncycle_balance_max'] if dict_out['loan_second_by03_ncycle_balance_max']>0 else np.nan #var区间率
                dict_out['loan_second_by03_ncycle_startDate_to_report_sum'] = loan_second_by03_ncycle['startDate_to_report'].sum() #var求和比率
                dict_out['loan_second_by03_ncycle_month60_State_countNull_max'] = loan_second_by03_ncycle['month60_State_countNull'].max() #var最大值比率
                dict_out['loan_second_by03_ncycle_month60_State_countNr_max'] = loan_second_by03_ncycle['month60_State_countNr'].max() #var最大值比率
                dict_out['loan_second_by03_ncycle_month60_to_report_mean_sum'] = loan_second_by03_ncycle['month60_to_report_mean'].sum() #var求和比率
                dict_out['loan_second_by03_ncycle_month60_State_countN_max'] = loan_second_by03_ncycle['month60_State_countN'].max() #var最大值比率
                dict_out['loan_second_by03_ncycle_month60_State_num_size_sum'] = loan_second_by03_ncycle['month60_State_num_size'].sum() #var求和比率
                dict_out['loan_second_by03_ncycleR_balance_range'] = dict_out['loan_second_by03_ncycle_balance_range']/dict_out['loan_second_by03_balance_range'] if dict_out['loan_second_by03_balance_range']>0 else np.nan #var最大值比率
                dict_out['loan_second_by03_ncycleR_startDate_to_report_sum'] = dict_out['loan_second_by03_ncycle_startDate_to_report_sum']/dict_out['loan_second_by03_startDate_to_report_sum'] if dict_out['loan_second_by03_startDate_to_report_sum']>0 else np.nan #var最大值比率
                dict_out['loan_second_by03_ncycleR_month60_State_countNull_max'] = dict_out['loan_second_by03_ncycle_month60_State_countNull_max']/dict_out['loan_second_by03_month60_State_countNull_max'] if dict_out['loan_second_by03_month60_State_countNull_max']>0 else np.nan #var最大值比率
                dict_out['loan_second_by03_ncycleR_month60_State_countNr_max'] = dict_out['loan_second_by03_ncycle_month60_State_countNr_max']/dict_out['loan_second_by03_month60_State_countNr_max'] if dict_out['loan_second_by03_month60_State_countNr_max']>0 else np.nan #var最大值比率
                dict_out['loan_second_by03_ncycle_gf_crdit_ratio'] = loan_second_by03_ncycle[loan_second_by03_ncycle['guaranteeForm']=='信用/免担保'].shape[0]/len(loan_second_by03_ncycle) #近03个月截至 非循环贷账户 贷款账户担保方式为信用/免担保 占比
                dict_out['loan_second_by03_ncycleR_gf_crdit_ratio'] = dict_out['loan_second_by03_ncycle_gf_crdit_ratio']/dict_out['loan_second_by03_gf_crdit_ratio'] if dict_out['loan_second_by03_gf_crdit_ratio']>0 else np.nan #var最大值比率
                dict_out['loan_second_by03_ncycleR_month60_to_report_mean_sum'] = dict_out['loan_second_by03_ncycle_month60_to_report_mean_sum']/dict_out['loan_second_by03_month60_to_report_mean_sum'] if dict_out['loan_second_by03_month60_to_report_mean_sum']>0 else np.nan #var最大值比率
                dict_out['loan_second_by03_ncycleR_month60_State_countN_max'] = dict_out['loan_second_by03_ncycle_month60_State_countN_max']/dict_out['loan_second_by03_month60_State_countN_max'] if dict_out['loan_second_by03_month60_State_countN_max']>0 else np.nan #var最大值比率
                dict_out['loan_second_by03_ncycleR_month60_State_num_size_sum'] = dict_out['loan_second_by03_ncycle_month60_State_num_size_sum']/dict_out['loan_second_by03_month60_State_num_size_sum'] if dict_out['loan_second_by03_month60_State_num_size_sum']>0 else np.nan #var最大值比率

            
            #近03个月截至 循环贷账户
            loan_second_by03_cycle = loan_second_by03[loan_second_by03['class']=='循环贷账户']


            #近03个月截至 有担保 is_vouch 
            loan_second_by03_vouch = loan_second_by03[loan_second_by03['is_vouch']==1]
            if len(loan_second_by03_vouch)>0:

                dict_out['loan_second_by03_vouch_month60_to_report_mean_sum'] = loan_second_by03_vouch['month60_to_report_mean'].sum() #var求和比率
                dict_out['loan_second_by03_vouchR_month60_to_report_mean_sum'] = dict_out['loan_second_by03_vouch_month60_to_report_mean_sum']/dict_out['loan_second_by03_month60_to_report_mean_sum'] if dict_out['loan_second_by03_month60_to_report_mean_sum']>0 else np.nan #var最大值比率

            #近03个月截至 历史逾期
            loan_second_by03_hdue1 = loan_second_by03[loan_second_by03['is_overdue']==1]
            

            #近03个月截至 历史严重逾期
            loan_second_by03_hdue3 = loan_second_by03[loan_second_by03['due_class']==3]
            

            #近03个月截至 当前逾期
            loan_second_by03_cdue = loan_second_by03[loan_second_by03['accountStatus']=='逾期']
            

        #近06个月截至
        loan_second_by06 = loan_second[loan_second.byDate_to_report<6]
        if len(loan_second_by06)>0:

            if True:
                for var in numeric_vers:
                    dict_out['loan_second_by06_'+var+'_max'] = loan_second_by06[var].max() #近06个月截至 var最大值
                    dict_out['loan_second_by06_'+var+'_min'] = loan_second_by06[var].min() #近06个月截至 var最小值
                    dict_out['loan_second_by06_'+var+'_mean'] = loan_second_by06[var].mean() #近06个月截至 var平均值
                    dict_out['loan_second_by06_'+var+'_sum'] = loan_second_by06[var].sum() #近06个月截至 var求和
                    dict_out['loan_second_by06_'+var+'_range'] = (dict_out['loan_second_by06_'+var+'_max']-dict_out['loan_second_by06_'+var+'_min'])/dict_out['loan_second_by06_'+var+'_max'] if dict_out['loan_second_by06_'+var+'_max']>0 else np.nan #近06个月截至 var区间率



                dict_out['loan_second_by06_loanGrantOrg_giniimpurity'] = giniimpurity(loan_second_by06['loanGrantOrg']) #近06个月截至 贷款账户管理机构详细基尼不纯度
                dict_out['loan_second_by06_loanGrantOrg_nunique'] = loan_second_by06['loanGrantOrg'].nunique() #近06个月截至 贷款账户管理机构详细nunique

                dict_out['loan_second_by06_org_giniimpurity'] = giniimpurity(loan_second_by06['org']) #近06个月截至 贷款账户管理机构基尼不纯度
                dict_out['loan_second_by06_org_commercial_bank_count'] = loan_second_by06[loan_second_by06['org']=='商业银行'].shape[0] #近06个月截至 贷款账户管理机构为商业银行 计数
                dict_out['loan_second_by06_org_commercial_bank_ratio'] = loan_second_by06[loan_second_by06['org']=='商业银行'].shape[0]/len(loan_second_by06) #近06个月截至 贷款账户管理机构为商业银行 占比
                dict_out['loan_second_by06_org_consumer_finance_count'] = loan_second_by06[loan_second_by06['org']=='消费金融公司'].shape[0] #近06个月截至 贷款账户管理机构为消费金融公司 计数
                dict_out['loan_second_by06_org_consumer_finance_ratio'] = loan_second_by06[loan_second_by06['org']=='消费金融公司'].shape[0]/len(loan_second_by06) #近06个月截至 贷款账户管理机构为消费金融公司 占比
                dict_out['loan_second_by06_org_micro_loan_count'] = loan_second_by06[loan_second_by06['org']=='小额贷款公司'].shape[0] #近06个月截至 贷款账户管理机构为小额贷款公司 计数
                dict_out['loan_second_by06_org_micro_loan_ratio'] = loan_second_by06[loan_second_by06['org']=='小额贷款公司'].shape[0]/len(loan_second_by06) #近06个月截至 贷款账户管理机构为小额贷款公司 占比
                dict_out['loan_second_by06_org_other_count'] = loan_second_by06[loan_second_by06['org']=='其他机构'].shape[0] #近06个月截至 贷款账户管理机构为其他机构 计数
                dict_out['loan_second_by06_org_other_ratio'] = loan_second_by06[loan_second_by06['org']=='其他机构'].shape[0]/len(loan_second_by06) #近06个月截至 贷款账户管理机构为其他机构 占比
                dict_out['loan_second_by06_org_trust_company_count'] = loan_second_by06[loan_second_by06['org']=='信托公司'].shape[0] #近06个月截至 贷款账户管理机构为信托公司 计数
                dict_out['loan_second_by06_org_trust_company_ratio'] = loan_second_by06[loan_second_by06['org']=='信托公司'].shape[0]/len(loan_second_by06) #近06个月截至 贷款账户管理机构为信托公司 占比
                dict_out['loan_second_by06_org_car_finance_count'] = loan_second_by06[loan_second_by06['org']=='汽车金融公司'].shape[0] #近06个月截至 贷款账户管理机构为汽车金融公司 计数
                dict_out['loan_second_by06_org_car_finance_ratio'] = loan_second_by06[loan_second_by06['org']=='汽车金融公司'].shape[0]/len(loan_second_by06) #近06个月截至 贷款账户管理机构为汽车金融公司 占比
                dict_out['loan_second_by06_org_lease_finance_count'] = loan_second_by06[loan_second_by06['org']=='融资租赁公司'].shape[0] #近06个月截至 贷款账户管理机构为融资租赁公司 计数
                dict_out['loan_second_by06_org_lease_finance_ratio'] = loan_second_by06[loan_second_by06['org']=='融资租赁公司'].shape[0]/len(loan_second_by06) #近06个月截至 贷款账户管理机构为融资租赁公司 占比
                dict_out['loan_second_by06_org_myself_count'] = loan_second_by06[loan_second_by06['org']==dict_in['cc_rh_report'].queryOperator.values[0]].shape[0] #近06个月截至 贷款账户管理机构为本机构 计数
                dict_out['loan_second_by06_org_myself_ratio'] = loan_second_by06[loan_second_by06['org']==dict_in['cc_rh_report'].queryOperator.values[0]].shape[0]/len(loan_second_by06) #近06个月截至 贷款账户管理机构为本机构 占比
                dict_out['loan_second_by06_org_village_banks_count'] = loan_second_by06[loan_second_by06['org']=='村镇银行'].shape[0] #近06个月截至 贷款账户管理机构为村镇银行 计数
                dict_out['loan_second_by06_org_village_banks_ratio'] = loan_second_by06[loan_second_by06['org']=='村镇银行'].shape[0]/len(loan_second_by06) #近06个月截至 贷款账户管理机构为村镇银行 占比
                dict_out['loan_second_by06_org_finance_company_count'] = loan_second_by06[loan_second_by06['org']=='财务公司'].shape[0] #近06个月截至 贷款账户管理机构为财务公司 计数
                dict_out['loan_second_by06_org_finance_company_ratio'] = loan_second_by06[loan_second_by06['org']=='财务公司'].shape[0]/len(loan_second_by06) #近06个月截至 贷款账户管理机构为财务公司 占比
                dict_out['loan_second_by06_org_foreign_banks_count'] = loan_second_by06[loan_second_by06['org']=='外资银行'].shape[0] #近06个月截至 贷款账户管理机构为外资银行 计数
                dict_out['loan_second_by06_org_foreign_banks_ratio'] = loan_second_by06[loan_second_by06['org']=='外资银行'].shape[0]/len(loan_second_by06) #近06个月截至 贷款账户管理机构为外资银行 占比
                dict_out['loan_second_by06_org_provident_fund_count'] = loan_second_by06[loan_second_by06['org']=='公积金管理中心'].shape[0] #近06个月截至 贷款账户管理机构为公积金管理中心 计数
                dict_out['loan_second_by06_org_provident_fund_ratio'] = loan_second_by06[loan_second_by06['org']=='公积金管理中心'].shape[0]/len(loan_second_by06) #近06个月截至 贷款账户管理机构为公积金管理中心 占比
                dict_out['loan_second_by06_org_securities_firm_count'] = loan_second_by06[loan_second_by06['org']=='证券公司'].shape[0] #近06个月截至 贷款账户管理机构为证券公司 计数
                dict_out['loan_second_by06_org_securities_firm_ratio'] = loan_second_by06[loan_second_by06['org']=='证券公司'].shape[0]/len(loan_second_by06) #近06个月截至 贷款账户管理机构为证券公司 占比

                dict_out['loan_second_by06_class_giniimpurity'] = giniimpurity(loan_second_by06['class']) #近06个月截至 贷款账户账户类别基尼不纯度
                dict_out['loan_second_by06_class_ncycle_count'] = loan_second_by06[loan_second_by06['class']=='非循环贷账户'].shape[0] #近06个月截至 贷款账户账户类别为非循环贷账户 计数
                dict_out['loan_second_by06_class_ncycle_ratio'] = loan_second_by06[loan_second_by06['class']=='非循环贷账户'].shape[0]/len(loan_second_by06) #近06个月截至 贷款账户账户类别为非循环贷账户 占比
                dict_out['loan_second_by06_class_cycle_sub_count'] = loan_second_by06[loan_second_by06['class']=='循环额度下分账户'].shape[0] #近06个月截至 贷款账户账户类别为循环额度下分账户 计数
                dict_out['loan_second_by06_class_cycle_sub_ratio'] = loan_second_by06[loan_second_by06['class']=='循环额度下分账户'].shape[0]/len(loan_second_by06) #近06个月截至 贷款账户账户类别为循环额度下分账户 占比
                dict_out['loan_second_by06_class_cycle_count'] = loan_second_by06[loan_second_by06['class']=='循环贷账户'].shape[0] #近06个月截至 贷款账户账户类别为循环贷账户 计数
                dict_out['loan_second_by06_class_cycle_ratio'] = loan_second_by06[loan_second_by06['class']=='循环贷账户'].shape[0]/len(loan_second_by06) #近06个月截至 贷款账户账户类别为循环贷账户 占比

                dict_out['loan_second_by06_classify5_giniimpurity'] = giniimpurity(loan_second_by06['classify5']) #近06个月截至 贷款账户五级分类基尼不纯度
                dict_out['loan_second_by06_c5_unknow_count'] = loan_second_by06[loan_second_by06['classify5']==''].shape[0] #近06个月截至 贷款账户五级分类为'' 计数
                dict_out['loan_second_by06_c5_unknow_ratio'] = loan_second_by06[loan_second_by06['classify5']==''].shape[0]/len(loan_second_by06) #近06个月截至 贷款账户五级分类为'' 占比
                dict_out['loan_second_by06_c5_normal_count'] = loan_second_by06[loan_second_by06['classify5']=='正常'].shape[0] #近06个月截至 贷款账户五级分类为正常 计数
                dict_out['loan_second_by06_c5_normal_ratio'] = loan_second_by06[loan_second_by06['classify5']=='正常'].shape[0]/len(loan_second_by06) #近06个月截至 贷款账户五级分类为正常 占比
                dict_out['loan_second_by06_c5_loss_count'] = loan_second_by06[loan_second_by06['classify5']=='损失'].shape[0] #近06个月截至 贷款账户五级分类为损失 计数
                dict_out['loan_second_by06_c5_loss_ratio'] = loan_second_by06[loan_second_by06['classify5']=='损失'].shape[0]/len(loan_second_by06) #近06个月截至 贷款账户五级分类为损失 占比
                dict_out['loan_second_by06_c5_attention_count'] = loan_second_by06[loan_second_by06['classify5']=='关注'].shape[0] #近06个月截至 贷款账户五级分类为关注 计数
                dict_out['loan_second_by06_c5_attention_ratio'] = loan_second_by06[loan_second_by06['classify5']=='关注'].shape[0]/len(loan_second_by06) #近06个月截至 贷款账户五级分类为关注 占比
                dict_out['loan_second_by06_c5_suspicious_count'] = loan_second_by06[loan_second_by06['classify5']=='可疑'].shape[0] #近06个月截至 贷款账户五级分类为可疑 计数
                dict_out['loan_second_by06_c5_suspicious_ratio'] = loan_second_by06[loan_second_by06['classify5']=='可疑'].shape[0]/len(loan_second_by06) #近06个月截至 贷款账户五级分类为可疑 占比
                dict_out['loan_second_by06_c5_secondary_count'] = loan_second_by06[loan_second_by06['classify5']=='次级'].shape[0] #近06个月截至 贷款账户五级分类为次级 计数
                dict_out['loan_second_by06_c5_secondary_ratio'] = loan_second_by06[loan_second_by06['classify5']=='次级'].shape[0]/len(loan_second_by06) #近06个月截至 贷款账户五级分类为次级 占比
                dict_out['loan_second_by06_c5_noclass_count'] = loan_second_by06[loan_second_by06['classify5']=='未分类'].shape[0] #近06个月截至 贷款账户五级分类为未分类 计数
                dict_out['loan_second_by06_c5_noclass_ratio'] = loan_second_by06[loan_second_by06['classify5']=='未分类'].shape[0]/len(loan_second_by06) #近06个月截至 贷款账户五级分类为未分类 占比

                dict_out['loan_second_by06_accountStatus_giniimpurity'] = giniimpurity(loan_second_by06['accountStatus']) #近06个月截至 贷款账户账户状态基尼不纯度
                dict_out['loan_second_by06_as_settle_count'] = loan_second_by06[loan_second_by06['accountStatus']=='结清'].shape[0] #近06个月截至 贷款账户账户状态为结清 计数
                dict_out['loan_second_by06_as_settle_ratio'] = loan_second_by06[loan_second_by06['accountStatus']=='结清'].shape[0]/len(loan_second_by06) #近06个月截至 贷款账户账户状态为结清 占比
                dict_out['loan_second_by06_as_normal_count'] = loan_second_by06[loan_second_by06['accountStatus']=='正常'].shape[0] #近06个月截至 贷款账户账户状态为正常 计数
                dict_out['loan_second_by06_as_normal_ratio'] = loan_second_by06[loan_second_by06['accountStatus']=='正常'].shape[0]/len(loan_second_by06) #近06个月截至 贷款账户账户状态为正常 占比
                dict_out['loan_second_by06_as_overdue_count'] = loan_second_by06[loan_second_by06['accountStatus']=='逾期'].shape[0] #近06个月截至 贷款账户账户状态为逾期 计数
                dict_out['loan_second_by06_as_overdue_ratio'] = loan_second_by06[loan_second_by06['accountStatus']=='逾期'].shape[0]/len(loan_second_by06) #近06个月截至 贷款账户账户状态为逾期 占比
                dict_out['loan_second_by06_as_bad_debts_count'] = loan_second_by06[loan_second_by06['accountStatus']=='呆账'].shape[0] #近06个月截至 贷款账户账户状态为呆账 计数
                dict_out['loan_second_by06_as_bad_debts_ratio'] = loan_second_by06[loan_second_by06['accountStatus']=='呆账'].shape[0]/len(loan_second_by06) #近06个月截至 贷款账户账户状态为呆账 占比
                dict_out['loan_second_by06_as_unknow_count'] = loan_second_by06[loan_second_by06['accountStatus']==''].shape[0] #近06个月截至 贷款账户账户状态为'' 计数
                dict_out['loan_second_by06_as_unknow_ratio'] = loan_second_by06[loan_second_by06['accountStatus']==''].shape[0]/len(loan_second_by06) #近06个月截至 贷款账户账户状态为'' 占比
                dict_out['loan_second_by06_as_roll_out_count'] = loan_second_by06[loan_second_by06['accountStatus']=='转出'].shape[0] #近06个月截至 贷款账户账户状态为转出 计数
                dict_out['loan_second_by06_as_roll_out_ratio'] = loan_second_by06[loan_second_by06['accountStatus']=='转出'].shape[0]/len(loan_second_by06) #近06个月截至 贷款账户账户状态为转出 占比

                dict_out['loan_second_by06_repayType_giniimpurity'] = giniimpurity(loan_second_by06['repayType']) #近06个月截至 贷款账户还款方式基尼不纯度
                dict_out['loan_second_by06_rt_unknow_count'] = loan_second_by06[loan_second_by06['repayType']=='--'].shape[0] #近06个月截至 贷款账户还款方式为-- 计数
                dict_out['loan_second_by06_rt_unknow_ratio'] = loan_second_by06[loan_second_by06['repayType']=='--'].shape[0]/len(loan_second_by06) #近06个月截至 贷款账户还款方式为-- 占比
                dict_out['loan_second_by06_rt_equality_count'] = loan_second_by06[loan_second_by06['repayType']=='分期等额本息'].shape[0] #近06个月截至 贷款账户还款方式为分期等额本息 计数
                dict_out['loan_second_by06_rt_equality_ratio'] = loan_second_by06[loan_second_by06['repayType']=='分期等额本息'].shape[0]/len(loan_second_by06) #近06个月截至 贷款账户还款方式为分期等额本息 占比
                dict_out['loan_second_by06_rt_onschedule_count'] = loan_second_by06[loan_second_by06['repayType']=='按期计算还本付息'].shape[0] #近06个月截至 贷款账户还款方式为按期计算还本付息 计数
                dict_out['loan_second_by06_rt_onschedule_ratio'] = loan_second_by06[loan_second_by06['repayType']=='按期计算还本付息'].shape[0]/len(loan_second_by06) #近06个月截至 贷款账户还款方式为按期计算还本付息 占比
                dict_out['loan_second_by06_rt_not_distinguish_count'] = loan_second_by06[loan_second_by06['repayType']=='不区分还款方式'].shape[0] #近06个月截至 贷款账户还款方式为不区分还款方式 计数
                dict_out['loan_second_by06_rt_not_distinguish_ratio'] = loan_second_by06[loan_second_by06['repayType']=='不区分还款方式'].shape[0]/len(loan_second_by06) #近06个月截至 贷款账户还款方式为不区分还款方式 占比
                dict_out['loan_second_by06_rt_circulation_count'] = loan_second_by06[loan_second_by06['repayType']=='循环贷款下其他还款方式'].shape[0] #近06个月截至 贷款账户还款方式为循环贷款下其他还款方式 计数
                dict_out['loan_second_by06_rt_circulation_ratio'] = loan_second_by06[loan_second_by06['repayType']=='循环贷款下其他还款方式'].shape[0]/len(loan_second_by06) #近06个月截至 贷款账户还款方式为循环贷款下其他还款方式 占比
                dict_out['loan_second_by06_rt_once_count'] = loan_second_by06[loan_second_by06['repayType']=='到期一次还本付息'].shape[0] #近06个月截至 贷款账户还款方式为到期一次还本付息 计数
                dict_out['loan_second_by06_rt_once_ratio'] = loan_second_by06[loan_second_by06['repayType']=='到期一次还本付息'].shape[0]/len(loan_second_by06) #近06个月截至 贷款账户还款方式为到期一次还本付息 占比

                dict_out['loan_second_by06_repayFrequency_giniimpurity'] = giniimpurity(loan_second_by06['repayFrequency']) #近06个月截至 贷款账户还款频率基尼不纯度
                dict_out['loan_second_by06_rf_month_count'] = loan_second_by06[loan_second_by06['repayFrequency']=='月'].shape[0] #近06个月截至 贷款账户还款频率为月 计数
                dict_out['loan_second_by06_rf_month_ratio'] = loan_second_by06[loan_second_by06['repayFrequency']=='月'].shape[0]/len(loan_second_by06) #近06个月截至 贷款账户还款频率为月 占比
                dict_out['loan_second_by06_rf_once_count'] = loan_second_by06[loan_second_by06['repayFrequency']=='一次性'].shape[0] #近06个月截至 贷款账户还款频率为一次性 计数
                dict_out['loan_second_by06_rf_once_ratio'] = loan_second_by06[loan_second_by06['repayFrequency']=='一次性'].shape[0]/len(loan_second_by06) #近06个月截至 贷款账户还款频率为一次性 占比
                dict_out['loan_second_by06_rf_other_count'] = loan_second_by06[loan_second_by06['repayFrequency']=='其他'].shape[0] #近06个月截至 贷款账户还款频率为其他 计数
                dict_out['loan_second_by06_rf_other_ratio'] = loan_second_by06[loan_second_by06['repayFrequency']=='其他'].shape[0]/len(loan_second_by06) #近06个月截至 贷款账户还款频率为其他 占比
                dict_out['loan_second_by06_rf_irregular_count'] = loan_second_by06[loan_second_by06['repayFrequency']=='不定期'].shape[0] #近06个月截至 贷款账户还款频率为不定期 计数
                dict_out['loan_second_by06_rf_irregular_ratio'] = loan_second_by06[loan_second_by06['repayFrequency']=='不定期'].shape[0]/len(loan_second_by06) #近06个月截至 贷款账户还款频率为不定期 占比
                dict_out['loan_second_by06_rf_day_count'] = loan_second_by06[loan_second_by06['repayFrequency']=='日'].shape[0] #近06个月截至 贷款账户还款频率为日 计数
                dict_out['loan_second_by06_rf_day_ratio'] = loan_second_by06[loan_second_by06['repayFrequency']=='日'].shape[0]/len(loan_second_by06) #近06个月截至 贷款账户还款频率为日 占比
                dict_out['loan_second_by06_rf_year_count'] = loan_second_by06[loan_second_by06['repayFrequency']=='年'].shape[0] #近06个月截至 贷款账户还款频率为年 计数
                dict_out['loan_second_by06_rf_year_ratio'] = loan_second_by06[loan_second_by06['repayFrequency']=='年'].shape[0]/len(loan_second_by06) #近06个月截至 贷款账户还款频率为年 占比
                dict_out['loan_second_by06_rf_season_count'] = loan_second_by06[loan_second_by06['repayFrequency']=='季'].shape[0] #近06个月截至 贷款账户还款频率为季 计数
                dict_out['loan_second_by06_rf_season_ratio'] = loan_second_by06[loan_second_by06['repayFrequency']=='季'].shape[0]/len(loan_second_by06) #近06个月截至 贷款账户还款频率为季 占比
                dict_out['loan_second_by06_rf_week_count'] = loan_second_by06[loan_second_by06['repayFrequency']=='周'].shape[0] #近06个月截至 贷款账户还款频率为周 计数
                dict_out['loan_second_by06_rf_week_ratio'] = loan_second_by06[loan_second_by06['repayFrequency']=='周'].shape[0]/len(loan_second_by06) #近06个月截至 贷款账户还款频率为周 占比
                dict_out['loan_second_by06_rf_halfyear_count'] = loan_second_by06[loan_second_by06['repayFrequency']=='半年'].shape[0] #近06个月截至 贷款账户还款频率为半年 计数
                dict_out['loan_second_by06_rf_halfyear_ratio'] = loan_second_by06[loan_second_by06['repayFrequency']=='半年'].shape[0]/len(loan_second_by06) #近06个月截至 贷款账户还款频率为半年 占比

                dict_out['loan_second_by06_guaranteeForm_giniimpurity'] = giniimpurity(loan_second_by06['guaranteeForm']) #近06个月截至 贷款账户担保方式基尼不纯度
                dict_out['loan_second_by06_gf_crdit_count'] = loan_second_by06[loan_second_by06['guaranteeForm']=='信用/免担保'].shape[0] #近06个月截至 贷款账户担保方式为其信用/免担保 计数
                dict_out['loan_second_by06_gf_crdit_ratio'] = loan_second_by06[loan_second_by06['guaranteeForm']=='信用/免担保'].shape[0]/len(loan_second_by06) #近06个月截至 贷款账户担保方式为信用/免担保 占比
                dict_out['loan_second_by06_gf_other_count'] = loan_second_by06[loan_second_by06['guaranteeForm']=='其他'].shape[0] #近06个月截至 贷款账户担保方式为其他 计数
                dict_out['loan_second_by06_gf_other_ratio'] = loan_second_by06[loan_second_by06['guaranteeForm']=='其他'].shape[0]/len(loan_second_by06) #近06个月截至 贷款账户担保方式为其他 占比
                dict_out['loan_second_by06_gf_combine_nowarranty_count'] = loan_second_by06[loan_second_by06['guaranteeForm']=='组合（不含保证）'].shape[0] #近06个月截至 贷款账户担保方式为组合（不含保证） 计数
                dict_out['loan_second_by06_gf_combine_nowarranty_ratio'] = loan_second_by06[loan_second_by06['guaranteeForm']=='组合（不含保证）'].shape[0]/len(loan_second_by06) #近06个月截至 贷款账户担保方式为组合（不含保证） 占比
                dict_out['loan_second_by06_gf_combine_warranty_count'] = loan_second_by06[loan_second_by06['guaranteeForm']=='组合(含保证)'].shape[0] #近06个月截至 贷款账户担保方式为组合(含保证) 计数
                dict_out['loan_second_by06_gf_combine_warranty_ratio'] = loan_second_by06[loan_second_by06['guaranteeForm']=='组合(含保证)'].shape[0]/len(loan_second_by06) #近06个月截至 贷款账户担保方式为组合(含保证) 占比
                dict_out['loan_second_by06_gf_mortgage_count'] = loan_second_by06[loan_second_by06['guaranteeForm']=='抵押'].shape[0] #近06个月截至 贷款账户担保方式为抵押 计数
                dict_out['loan_second_by06_gf_mortgage_ratio'] = loan_second_by06[loan_second_by06['guaranteeForm']=='抵押'].shape[0]/len(loan_second_by06) #近06个月截至 贷款账户担保方式为抵押 占比
                dict_out['loan_second_by06_gf_warranty_count'] = loan_second_by06[loan_second_by06['guaranteeForm']=='保证'].shape[0] #近06个月截至 贷款账户担保方式为保证计数
                dict_out['loan_second_by06_gf_warranty_ratio'] = loan_second_by06[loan_second_by06['guaranteeForm']=='保证'].shape[0]/len(loan_second_by06) #近06个月截至 贷款账户担保方式为保证 占比
                dict_out['loan_second_by06_gf_pledge_count'] = loan_second_by06[loan_second_by06['guaranteeForm']=='质押'].shape[0] #近06个月截至 贷款账户担保方式为质押 计数
                dict_out['loan_second_by06_gf_pledge_ratio'] = loan_second_by06[loan_second_by06['guaranteeForm']=='质押'].shape[0]/len(loan_second_by06) #近06个月截至 贷款账户担保方式为质押 占比
                dict_out['loan_second_by06_gf_farm_group_count'] = loan_second_by06[loan_second_by06['guaranteeForm']=='农户联保'].shape[0] #近06个月截至 贷款账户担保方式为农户联保 计数
                dict_out['loan_second_by06_gf_farm_group_ratio'] = loan_second_by06[loan_second_by06['guaranteeForm']=='农户联保'].shape[0]/len(loan_second_by06) #近06个月截至 贷款账户担保方式为农户联保 占比

                dict_out['loan_second_by06_businessType_giniimpurity'] = giniimpurity(loan_second_by06['businessType']) #近06个月截至 贷款账户业务种类基尼不纯度
                dict_out['loan_second_by06_bt_other_person_count'] = loan_second_by06[loan_second_by06['businessType']=='其他个人消费贷款'].shape[0] #近06个月截至 贷款账户业务种类为其他个人消费贷款 计数
                dict_out['loan_second_by06_bt_other_person_ratio'] = loan_second_by06[loan_second_by06['businessType']=='其他个人消费贷款'].shape[0]/len(loan_second_by06) #近06个月截至 贷款账户业务种类为其他个人消费贷款 占比
                dict_out['loan_second_by06_bt_other_loan_count'] = loan_second_by06[loan_second_by06['businessType']=='其他贷款'].shape[0] #近06个月截至 贷款账户业务种类为其他贷款 计数
                dict_out['loan_second_by06_bt_other_loan_ratio'] = loan_second_by06[loan_second_by06['businessType']=='其他贷款'].shape[0]/len(loan_second_by06) #近06个月截至 贷款账户业务种类为其他贷款 占比
                dict_out['loan_second_by06_bt_person_business_count'] = loan_second_by06[loan_second_by06['businessType']=='个人经营性贷款'].shape[0] #近06个月截至 贷款账户业务种类为个人经营性贷款 计数
                dict_out['loan_second_by06_bt_person_business_ratio'] = loan_second_by06[loan_second_by06['businessType']=='个人经营性贷款'].shape[0]/len(loan_second_by06) #近06个月截至 贷款账户业务种类为个人经营性贷款 占比
                dict_out['loan_second_by06_bt_farm_loan_count'] = loan_second_by06[loan_second_by06['businessType']=='农户贷款'].shape[0] #近06个月截至 贷款账户业务种类为农户贷款 计数
                dict_out['loan_second_by06_bt_farm_loan_ratio'] = loan_second_by06[loan_second_by06['businessType']=='农户贷款'].shape[0]/len(loan_second_by06) #近06个月截至 贷款账户业务种类为农户贷款 占比
                dict_out['loan_second_by06_bt_person_car_count'] = loan_second_by06[loan_second_by06['businessType']=='个人汽车消费贷款'].shape[0] #近06个月截至 贷款账户业务种类为个人汽车消费贷款 计数
                dict_out['loan_second_by06_bt_person_car_ratio'] = loan_second_by06[loan_second_by06['businessType']=='个人汽车消费贷款'].shape[0]/len(loan_second_by06) #近06个月截至 贷款账户业务种类为个人汽车消费贷款 占比
                dict_out['loan_second_by06_bt_person_study_count'] = loan_second_by06[loan_second_by06['businessType']=='个人助学贷款'].shape[0] #近06个月截至 贷款账户业务种类为个人助学贷款 计数
                dict_out['loan_second_by06_bt_person_study_ratio'] = loan_second_by06[loan_second_by06['businessType']=='个人助学贷款'].shape[0]/len(loan_second_by06) #近06个月截至 贷款账户业务种类为个人助学贷款 占比
                dict_out['loan_second_by06_bt_house_commercial_count'] = loan_second_by06[loan_second_by06['businessType']=='个人住房商业贷款'].shape[0] #近06个月截至 贷款账户业务种类为个人住房商业贷款 计数
                dict_out['loan_second_by06_bt_house_commercial_ratio'] = loan_second_by06[loan_second_by06['businessType']=='个人住房商业贷款'].shape[0]/len(loan_second_by06) #近06个月截至 贷款账户业务种类为个人住房商业贷款 占比
                dict_out['loan_second_by06_bt_finance_lease_count'] = loan_second_by06[loan_second_by06['businessType']=='融资租赁业务'].shape[0] #近06个月截至 贷款账户业务种类为融资租赁业务 计数
                dict_out['loan_second_by06_bt_finance_lease_ratio'] = loan_second_by06[loan_second_by06['businessType']=='融资租赁业务'].shape[0]/len(loan_second_by06) #近06个月截至 贷款账户业务种类为融资租赁业务 占比
                dict_out['loan_second_by06_bt_house_fund_count'] = loan_second_by06[loan_second_by06['businessType']=='个人住房公积金贷款'].shape[0] #近06个月截至 贷款账户业务种类为个人住房公积金贷款 计数
                dict_out['loan_second_by06_bt_house_fund_ratio'] = loan_second_by06[loan_second_by06['businessType']=='个人住房公积金贷款'].shape[0]/len(loan_second_by06) #近06个月截至 贷款账户业务种类为个人住房公积金贷款 占比
                dict_out['loan_second_by06_bt_person_house_count'] = loan_second_by06[loan_second_by06['businessType']=='个人商用房（含商住两用）贷款'].shape[0] #近06个月截至 贷款账户业务种类为个人商用房（含商住两用）贷款 计数
                dict_out['loan_second_by06_bt_person_house_ratio'] = loan_second_by06[loan_second_by06['businessType']=='个人商用房（含商住两用）贷款'].shape[0]/len(loan_second_by06) #近06个月截至 贷款账户业务种类为个人商用房（含商住两用）贷款 占比
                dict_out['loan_second_by06_bt_stock_pledge_count'] = loan_second_by06[loan_second_by06['businessType']=='股票质押式回购交易'].shape[0] #近06个月截至 贷款账户业务种类为股票质押式回购交易计数
                dict_out['loan_second_by06_bt_stock_pledge_ratio'] = loan_second_by06[loan_second_by06['businessType']=='股票质押式回购交易'].shape[0]/len(loan_second_by06) #近06个月截至 贷款账户业务种类为股票质押式回购交易 占比



            #近06个月截至 现行
            loan_second_by06_now = loan_second_by06[loan_second_by06.is_now==1]
            if len(loan_second_by06_now)>0:

                dict_out['loan_second_by06_now_rt_unknow_count'] = loan_second_by06_now[loan_second_by06_now['repayType']=='--'].shape[0] #近06个月截至 现行贷款账户还款方式为-- 计数
                dict_out['loan_second_by06_now_planRepayAmount_sum'] = loan_second_by06_now['planRepayAmount'].sum() #var求和
                dict_out['loan_second_by06_now_balance_ratio_mean'] = loan_second_by06_now['balance_ratio'].mean() #var平均值
                dict_out['loan_second_by06_now_rf_other_ratio'] = loan_second_by06_now[loan_second_by06_now['repayFrequency']=='其他'].shape[0]/len(loan_second_by06_now) #近06个月截至 现行贷款账户还款频率为其他 占比
                dict_out['loan_second_by06_now_month60_State_countN_mean'] = loan_second_by06_now['month60_State_countN'].mean() #var平均值
                dict_out['loan_second_by06_now_org_commercial_bank_count'] = loan_second_by06_now[loan_second_by06_now['org']=='商业银行'].shape[0] #近06个月截至 现行贷款账户管理机构为商业银行 计数
                dict_out['loan_second_by06_now_byDate_to_report_mean'] = loan_second_by06_now['byDate_to_report'].mean() #var平均值
                dict_out['loan_second_by06_now_month60_State_countNr_max'] = loan_second_by06_now['month60_State_countNr'].max()#var最大值
                dict_out['loan_second_by06_now_month60_State_countNr_min'] = loan_second_by06_now['month60_State_countNr'].min() #var最小值
                dict_out['loan_second_by06_now_month60_State_countNr_range'] = (dict_out['loan_second_by06_now_month60_State_countNr_max']-dict_out['loan_second_by06_now_month60_State_countNr_min'])/dict_out['loan_second_by06_now_month60_State_countNr_max'] if dict_out['loan_second_by06_now_month60_State_countNr_max']>0 else np.nan #var区间率
                dict_out['loan_second_by06_now_month60_State_countNull_max'] = loan_second_by06_now['month60_State_countNull'].max()#var最大值
                dict_out['loan_second_by06_now_month60_State_countNull_min'] = loan_second_by06_now['month60_State_countNull'].min() #var最小值
                dict_out['loan_second_by06_now_month60_State_countNull_range'] = (dict_out['loan_second_by06_now_month60_State_countNull_max']-dict_out['loan_second_by06_now_month60_State_countNull_min'])/dict_out['loan_second_by06_now_month60_State_countNull_max'] if dict_out['loan_second_by06_now_month60_State_countNull_max']>0 else np.nan #var区间率
                dict_out['loan_second_by06_now_org_myself_count'] = loan_second_by06_now[loan_second_by06_now['org']==dict_in['cc_rh_report'].queryOperator.values[0]].shape[0] #近06个月截至 现行贷款账户管理机构为本机构 计数
                dict_out['loan_second_by06_now_startDate_to_report_max'] = loan_second_by06_now['startDate_to_report'].max()#var最大值
                dict_out['loan_second_by06_now_startDate_to_report_min'] = loan_second_by06_now['startDate_to_report'].min() #var最小值
                dict_out['loan_second_by06_now_startDate_to_report_range'] = (dict_out['loan_second_by06_now_startDate_to_report_max']-dict_out['loan_second_by06_now_startDate_to_report_min'])/dict_out['loan_second_by06_now_startDate_to_report_max'] if dict_out['loan_second_by06_now_startDate_to_report_max']>0 else np.nan #var区间率
                dict_out['loan_second_by06_now_leftRepayTerms_max'] = loan_second_by06_now['leftRepayTerms'].max()#var最大值
                dict_out['loan_second_by06_now_leftRepayTerms_min'] = loan_second_by06_now['leftRepayTerms'].min() #var最小值
                dict_out['loan_second_by06_now_leftRepayTerms_range'] = (dict_out['loan_second_by06_now_leftRepayTerms_max']-dict_out['loan_second_by06_now_leftRepayTerms_min'])/dict_out['loan_second_by06_now_leftRepayTerms_max'] if dict_out['loan_second_by06_now_leftRepayTerms_max']>0 else np.nan #var区间率
                dict_out['loan_second_by06_now_bt_other_loan_count'] = loan_second_by06_now[loan_second_by06_now['businessType']=='其他贷款'].shape[0] #近06个月截至 现行贷款账户业务种类为其他贷款 计数
                dict_out['loan_second_by06_now_month60_State_num_size_mean'] = loan_second_by06_now['month60_State_num_size'].mean() #var平均值
                dict_out['loan_second_by06_now_guaranteeForm_giniimpurity'] = giniimpurity(loan_second_by06_now['guaranteeForm']) #近06个月截至 现行贷款账户担保方式基尼不纯度
                dict_out['loan_second_by06_now_gf_combine_nowarranty_ratio'] = loan_second_by06_now[loan_second_by06_now['guaranteeForm']=='组合（不含保证）'].shape[0]/len(loan_second_by06_now) #近06个月截至 现行贷款账户担保方式为组合（不含保证） 占比
                dict_out['loan_second_by06_now_loanGrantOrg_giniimpurity'] = giniimpurity(loan_second_by06_now['loanGrantOrg']) #近06个月截至 现行贷款账户管理机构详细基尼不纯度
                dict_out['loan_second_by06_now_org_commercial_bank_ratio'] = loan_second_by06_now[loan_second_by06_now['org']=='商业银行'].shape[0]/len(loan_second_by06_now) #近06个月截至 现行贷款账户管理机构为商业银行 占比
                dict_out['loan_second_by06_now_gf_other_ratio'] = loan_second_by06_now[loan_second_by06_now['guaranteeForm']=='其他'].shape[0]/len(loan_second_by06_now) #近06个月截至 现行贷款账户担保方式为其他 占比
                dict_out['loan_second_by06_now_org_commercial_bank_count'] = loan_second_by06_now[loan_second_by06_now['org']=='商业银行'].shape[0] #近06个月截至 现行贷款账户管理机构为商业银行 计数
                dict_out['loan_second_by06_now_rf_once_ratio'] = loan_second_by06_now[loan_second_by06_now['repayFrequency']=='一次性'].shape[0]/len(loan_second_by06_now) #近06个月截至 现行贷款账户还款频率为一次性 占比
                dict_out['loan_second_by06_now_rf_month_ratio'] = loan_second_by06_now[loan_second_by06_now['repayFrequency']=='月'].shape[0]/len(loan_second_by06_now) #近06个月截至 现行贷款账户还款频率为月 占比
                dict_out['loan_second_by06_now_loanGrantOrg_nunique'] = loan_second_by06_now['loanGrantOrg'].nunique() #近06个月截至 现行贷款账户管理机构详细nunique
                dict_out['loan_second_by06_now_bt_other_person_ratio'] = loan_second_by06_now[loan_second_by06_now['businessType']=='其他个人消费贷款'].shape[0]/len(loan_second_by06_now) #近06个月截至 现行贷款账户业务种类为其他个人消费贷款 占比
                dict_out['loan_second_by06_now_loanAmount_max'] = loan_second_by06_now['loanAmount'].max()#var最大值
                dict_out['loan_second_by06_now_loanAmount_min'] = loan_second_by06_now['loanAmount'].min() #var最小值比率
                dict_out['loan_second_by06_now_loanAmount_range'] = (dict_out['loan_second_by06_now_loanAmount_max']-dict_out['loan_second_by06_now_loanAmount_min'])/dict_out['loan_second_by06_now_loanAmount_max'] if dict_out['loan_second_by06_now_loanAmount_max']>0 else np.nan #var区间率
                dict_out['loan_second_by06_now_startDate_to_report_max'] = loan_second_by06_now['startDate_to_report'].max()#var最大值
                dict_out['loan_second_by06_now_startDate_to_report_min'] = loan_second_by06_now['startDate_to_report'].min() #var最小值比率
                dict_out['loan_second_by06_now_startDate_to_report_range'] = (dict_out['loan_second_by06_now_startDate_to_report_max']-dict_out['loan_second_by06_now_startDate_to_report_min'])/dict_out['loan_second_by06_now_startDate_to_report_max'] if dict_out['loan_second_by06_now_startDate_to_report_max']>0 else np.nan #var区间率
                dict_out['loan_second_by06_now_month60_State_countNull_mean'] = loan_second_by06_now['month60_State_countNull'].mean()  #var平均值比率
                dict_out['loan_second_by06_now_month60_to_report_mean_mean'] = loan_second_by06_now['month60_to_report_mean'].mean()  #var平均值比率
                dict_out['loan_second_by06_now_repayMons_ratio_max'] = loan_second_by06_now['repayMons_ratio'].max()#var最大值
                dict_out['loan_second_by06_now_repayMons_ratio_min'] = loan_second_by06_now['repayMons_ratio'].min() #var最小值比率
                dict_out['loan_second_by06_now_repayMons_ratio_range'] = (dict_out['loan_second_by06_now_repayMons_ratio_max']-dict_out['loan_second_by06_now_repayMons_ratio_min'])/dict_out['loan_second_by06_now_repayMons_ratio_max'] if dict_out['loan_second_by06_now_repayMons_ratio_max']>0 else np.nan #var区间率
                dict_out['loan_second_by06_now_repayMons_ratio_mean'] = loan_second_by06_now['repayMons_ratio'].mean()  #var平均值比率
                dict_out['loan_second_by06_now_startDate_to_report_max'] = loan_second_by06_now['startDate_to_report'].max() #var最大值比率
                dict_out['loan_second_by06_now_month60_to_report_min_sum'] = loan_second_by06_now['month60_to_report_min'].sum() #var求和比率
                dict_out['loan_second_by06_now_repayAmt_sum'] = loan_second_by06_now['repayAmt'].sum() #var求和比率
                dict_out['loan_second_by06_now_byDate_to_report_mean'] = loan_second_by06_now['byDate_to_report'].mean()  #var平均值比率
                dict_out['loan_second_by06_now_month60_State_countN_max'] = loan_second_by06_now['month60_State_countN'].max() #var最大值比率
                dict_out['loan_second_by06_now_month60_to_report_max_mean'] = loan_second_by06_now['month60_to_report_max'].mean()  #var平均值比率
                dict_out['loan_second_by06_now_repayAmt_max'] = loan_second_by06_now['repayAmt'].max()#var最大值
                dict_out['loan_second_by06_now_repayAmt_min'] = loan_second_by06_now['repayAmt'].min() #var最小值比率
                dict_out['loan_second_by06_now_repayAmt_range'] = (dict_out['loan_second_by06_now_repayAmt_max']-dict_out['loan_second_by06_now_repayAmt_min'])/dict_out['loan_second_by06_now_repayAmt_max'] if dict_out['loan_second_by06_now_repayAmt_max']>0 else np.nan #var区间率
                dict_out['loan_second_by06_now_month60_to_report_max_max'] = loan_second_by06_now['month60_to_report_max'].max() #var最大值比率
                dict_out['loan_second_by06_now_balance_ratio_max'] = loan_second_by06_now['balance_ratio'].max()#var最大值
                dict_out['loan_second_by06_now_balance_ratio_min'] = loan_second_by06_now['balance_ratio'].min() #var最小值比率
                dict_out['loan_second_by06_now_balance_ratio_range'] = (dict_out['loan_second_by06_now_balance_ratio_max']-dict_out['loan_second_by06_now_balance_ratio_min'])/dict_out['loan_second_by06_now_balance_ratio_max'] if dict_out['loan_second_by06_now_balance_ratio_max']>0 else np.nan #var区间率
                dict_out['loan_second_by06_now_repayAmt_max'] = loan_second_by06_now['repayAmt'].max() #var最大值比率
                dict_out['loan_second_by06_now_month60_State_countNullr_mean'] = loan_second_by06_now['month60_State_countNullr'].mean()  #var平均值比率
                dict_out['loan_second_by06_now_month60_to_report_max_max'] = loan_second_by06_now['month60_to_report_max'].max()#var最大值
                dict_out['loan_second_by06_now_month60_to_report_max_min'] = loan_second_by06_now['month60_to_report_max'].min() #var最小值比率
                dict_out['loan_second_by06_now_month60_to_report_max_range'] = (dict_out['loan_second_by06_now_month60_to_report_max_max']-dict_out['loan_second_by06_now_month60_to_report_max_min'])/dict_out['loan_second_by06_now_month60_to_report_max_max'] if dict_out['loan_second_by06_now_month60_to_report_max_max']>0 else np.nan #var区间率
                dict_out['loan_second_by06_now_byDate_to_report_sum'] = loan_second_by06_now['byDate_to_report'].sum() #var求和比率
                dict_out['loan_second_by06_now_month60_to_report_mean_max'] = loan_second_by06_now['month60_to_report_mean'].max() #var最大值比率
                dict_out['loan_second_by06_now_month60_to_report_max_sum'] = loan_second_by06_now['month60_to_report_max'].sum() #var求和比率
                dict_out['loan_second_by06_now_month60_to_report_mean_max'] = loan_second_by06_now['month60_to_report_mean'].max()#var最大值
                dict_out['loan_second_by06_now_month60_to_report_mean_min'] = loan_second_by06_now['month60_to_report_mean'].min() #var最小值比率
                dict_out['loan_second_by06_now_month60_to_report_mean_range'] = (dict_out['loan_second_by06_now_month60_to_report_mean_max']-dict_out['loan_second_by06_now_month60_to_report_mean_min'])/dict_out['loan_second_by06_now_month60_to_report_mean_max'] if dict_out['loan_second_by06_now_month60_to_report_mean_max']>0 else np.nan #var区间率
                dict_out['loan_second_by06_now_month60_State_countUnknowr_mean'] = loan_second_by06_now['month60_State_countUnknowr'].mean()  #var平均值比率
                dict_out['loan_second_by06_now_guaranteeForm_giniimpurity'] = giniimpurity(loan_second_by06_now['guaranteeForm']) #近06个月截至 现行贷款账户担保方式基尼不纯度
                dict_out['loan_second_by06_nowR_guaranteeForm_giniimpurity'] = dict_out['loan_second_by06_now_guaranteeForm_giniimpurity']/dict_out['loan_second_by06_guaranteeForm_giniimpurity'] if dict_out['loan_second_by06_guaranteeForm_giniimpurity']>0 else np.nan #var最大值比率
                dict_out['loan_second_by06_nowR_loanAmount_range'] = dict_out['loan_second_by06_now_loanAmount_range']/dict_out['loan_second_by06_loanAmount_range'] if dict_out['loan_second_by06_loanAmount_range']>0 else np.nan #var最大值比率
                dict_out['loan_second_by06_nowR_startDate_to_report_range'] = dict_out['loan_second_by06_now_startDate_to_report_range']/dict_out['loan_second_by06_startDate_to_report_range'] if dict_out['loan_second_by06_startDate_to_report_range']>0 else np.nan #var最大值比率
                dict_out['loan_second_by06_nowR_month60_State_countNull_mean'] = dict_out['loan_second_by06_now_month60_State_countNull_mean']/dict_out['loan_second_by06_month60_State_countNull_mean'] if dict_out['loan_second_by06_month60_State_countNull_mean']>0 else np.nan #var最大值比率
                dict_out['loan_second_by06_nowR_month60_to_report_mean_mean'] = dict_out['loan_second_by06_now_month60_to_report_mean_mean']/dict_out['loan_second_by06_month60_to_report_mean_mean'] if dict_out['loan_second_by06_month60_to_report_mean_mean']>0 else np.nan #var最大值比率
                dict_out['loan_second_by06_nowR_repayMons_ratio_range'] = dict_out['loan_second_by06_now_repayMons_ratio_range']/dict_out['loan_second_by06_repayMons_ratio_range'] if dict_out['loan_second_by06_repayMons_ratio_range']>0 else np.nan #var最大值比率
                dict_out['loan_second_by06_nowR_repayMons_ratio_mean'] = dict_out['loan_second_by06_now_repayMons_ratio_mean']/dict_out['loan_second_by06_repayMons_ratio_mean'] if dict_out['loan_second_by06_repayMons_ratio_mean']>0 else np.nan #var最大值比率
                dict_out['loan_second_by06_nowR_startDate_to_report_max'] = dict_out['loan_second_by06_now_startDate_to_report_max']/dict_out['loan_second_by06_startDate_to_report_max'] if dict_out['loan_second_by06_startDate_to_report_max']>0 else np.nan #var最大值比率
                dict_out['loan_second_by06_nowR_month60_to_report_min_sum'] = dict_out['loan_second_by06_now_month60_to_report_min_sum']/dict_out['loan_second_by06_month60_to_report_min_sum'] if dict_out['loan_second_by06_month60_to_report_min_sum']>0 else np.nan #var最大值比率
                dict_out['loan_second_by06_now_gf_combine_nowarranty_ratio'] = loan_second_by06_now[loan_second_by06_now['guaranteeForm']=='组合（不含保证）'].shape[0]/len(loan_second_by06_now) #近06个月截至 现行贷款账户担保方式为组合（不含保证） 占比
                dict_out['loan_second_by06_nowR_gf_combine_nowarranty_ratio'] = dict_out['loan_second_by06_now_gf_combine_nowarranty_ratio']/dict_out['loan_second_by06_gf_combine_nowarranty_ratio'] if dict_out['loan_second_by06_gf_combine_nowarranty_ratio']>0 else np.nan #var最大值比率
                dict_out['loan_second_by06_nowR_repayAmt_sum'] = dict_out['loan_second_by06_now_repayAmt_sum']/dict_out['loan_second_by06_repayAmt_sum'] if dict_out['loan_second_by06_repayAmt_sum']>0 else np.nan #var最大值比率
                dict_out['loan_second_by06_now_loanGrantOrg_giniimpurity'] = giniimpurity(loan_second_by06_now['loanGrantOrg']) #近06个月截至 现行贷款账户管理机构详细基尼不纯度
                dict_out['loan_second_by06_nowR_loanGrantOrg_giniimpurity'] = dict_out['loan_second_by06_now_loanGrantOrg_giniimpurity']/dict_out['loan_second_by06_loanGrantOrg_giniimpurity'] if dict_out['loan_second_by06_loanGrantOrg_giniimpurity']>0 else np.nan #var最大值比率
                dict_out['loan_second_by06_now_org_commercial_bank_ratio'] = loan_second_by06_now[loan_second_by06_now['org']=='商业银行'].shape[0]/len(loan_second_by06_now) #近06个月截至 现行贷款账户管理机构为商业银行 占比
                dict_out['loan_second_by06_nowR_org_commercial_bank_ratio'] = dict_out['loan_second_by06_now_org_commercial_bank_ratio']/dict_out['loan_second_by06_org_commercial_bank_ratio'] if dict_out['loan_second_by06_org_commercial_bank_ratio']>0 else np.nan #var最大值比率
                dict_out['loan_second_by06_nowR_byDate_to_report_mean'] = dict_out['loan_second_by06_now_byDate_to_report_mean']/dict_out['loan_second_by06_byDate_to_report_mean'] if dict_out['loan_second_by06_byDate_to_report_mean']>0 else np.nan #var最大值比率
                dict_out['loan_second_by06_nowR_month60_State_countN_max'] = dict_out['loan_second_by06_now_month60_State_countN_max']/dict_out['loan_second_by06_month60_State_countN_max'] if dict_out['loan_second_by06_month60_State_countN_max']>0 else np.nan #var最大值比率
                dict_out['loan_second_by06_nowR_month60_to_report_max_mean'] = dict_out['loan_second_by06_now_month60_to_report_max_mean']/dict_out['loan_second_by06_month60_to_report_max_mean'] if dict_out['loan_second_by06_month60_to_report_max_mean']>0 else np.nan #var最大值比率
                dict_out['loan_second_by06_now_gf_other_ratio'] = loan_second_by06_now[loan_second_by06_now['guaranteeForm']=='其他'].shape[0]/len(loan_second_by06_now) #近06个月截至 现行贷款账户担保方式为其他 占比
                dict_out['loan_second_by06_nowR_gf_other_ratio'] = dict_out['loan_second_by06_now_gf_other_ratio']/dict_out['loan_second_by06_gf_other_ratio'] if dict_out['loan_second_by06_gf_other_ratio']>0 else np.nan #var最大值比率
                dict_out['loan_second_by06_now_org_commercial_bank_count'] = loan_second_by06_now[loan_second_by06_now['org']=='商业银行'].shape[0] #近06个月截至 现行贷款账户管理机构为商业银行 计数
                dict_out['loan_second_by06_nowR_org_commercial_bank_count'] = dict_out['loan_second_by06_now_org_commercial_bank_count']/dict_out['loan_second_by06_org_commercial_bank_count'] if dict_out['loan_second_by06_org_commercial_bank_count']>0 else np.nan #var最大值比率
                dict_out['loan_second_by06_nowR_repayAmt_range'] = dict_out['loan_second_by06_now_repayAmt_range']/dict_out['loan_second_by06_repayAmt_range'] if dict_out['loan_second_by06_repayAmt_range']>0 else np.nan #var最大值比率
                dict_out['loan_second_by06_nowR_month60_to_report_max_max'] = dict_out['loan_second_by06_now_month60_to_report_max_max']/dict_out['loan_second_by06_month60_to_report_max_max'] if dict_out['loan_second_by06_month60_to_report_max_max']>0 else np.nan #var最大值比率
                dict_out['loan_second_by06_nowR_balance_ratio_range'] = dict_out['loan_second_by06_now_balance_ratio_range']/dict_out['loan_second_by06_balance_ratio_range'] if dict_out['loan_second_by06_balance_ratio_range']>0 else np.nan #var最大值比率
                dict_out['loan_second_by06_nowR_repayAmt_max'] = dict_out['loan_second_by06_now_repayAmt_max']/dict_out['loan_second_by06_repayAmt_max'] if dict_out['loan_second_by06_repayAmt_max']>0 else np.nan #var最大值比率
                dict_out['loan_second_by06_nowR_month60_State_countNullr_mean'] = dict_out['loan_second_by06_now_month60_State_countNullr_mean']/dict_out['loan_second_by06_month60_State_countNullr_mean'] if dict_out['loan_second_by06_month60_State_countNullr_mean']>0 else np.nan #var最大值比率
                dict_out['loan_second_by06_now_rf_once_ratio'] = loan_second_by06_now[loan_second_by06_now['repayFrequency']=='一次性'].shape[0]/len(loan_second_by06_now) #近06个月截至 现行贷款账户还款频率为一次性 占比
                dict_out['loan_second_by06_nowR_rf_once_ratio'] = dict_out['loan_second_by06_now_rf_once_ratio']/dict_out['loan_second_by06_rf_once_ratio'] if dict_out['loan_second_by06_rf_once_ratio']>0 else np.nan #var最大值比率
                dict_out['loan_second_by06_nowR_month60_to_report_max_range'] = dict_out['loan_second_by06_now_month60_to_report_max_range']/dict_out['loan_second_by06_month60_to_report_max_range'] if dict_out['loan_second_by06_month60_to_report_max_range']>0 else np.nan #var最大值比率
                dict_out['loan_second_by06_nowR_byDate_to_report_sum'] = dict_out['loan_second_by06_now_byDate_to_report_sum']/dict_out['loan_second_by06_byDate_to_report_sum'] if dict_out['loan_second_by06_byDate_to_report_sum']>0 else np.nan #var最大值比率
                dict_out['loan_second_by06_nowR_month60_to_report_mean_max'] = dict_out['loan_second_by06_now_month60_to_report_mean_max']/dict_out['loan_second_by06_month60_to_report_mean_max'] if dict_out['loan_second_by06_month60_to_report_mean_max']>0 else np.nan #var最大值比率
                dict_out['loan_second_by06_nowR_month60_to_report_max_sum'] = dict_out['loan_second_by06_now_month60_to_report_max_sum']/dict_out['loan_second_by06_month60_to_report_max_sum'] if dict_out['loan_second_by06_month60_to_report_max_sum']>0 else np.nan #var最大值比率
                dict_out['loan_second_by06_now_rf_month_ratio'] = loan_second_by06_now[loan_second_by06_now['repayFrequency']=='月'].shape[0]/len(loan_second_by06_now) #近06个月截至 现行贷款账户还款频率为月 占比
                dict_out['loan_second_by06_nowR_rf_month_ratio'] = dict_out['loan_second_by06_now_rf_month_ratio']/dict_out['loan_second_by06_rf_month_ratio'] if dict_out['loan_second_by06_rf_month_ratio']>0 else np.nan #var最大值比率
                dict_out['loan_second_by06_now_loanGrantOrg_nunique'] = loan_second_by06_now['loanGrantOrg'].nunique() #近06个月截至 现行贷款账户管理机构详细nunique
                dict_out['loan_second_by06_nowR_loanGrantOrg_nunique'] = dict_out['loan_second_by06_now_loanGrantOrg_nunique']/dict_out['loan_second_by06_loanGrantOrg_nunique'] if dict_out['loan_second_by06_loanGrantOrg_nunique']>0 else np.nan #var最大值比率
                dict_out['loan_second_by06_nowR_month60_to_report_mean_range'] = dict_out['loan_second_by06_now_month60_to_report_mean_range']/dict_out['loan_second_by06_month60_to_report_mean_range'] if dict_out['loan_second_by06_month60_to_report_mean_range']>0 else np.nan #var最大值比率
                dict_out['loan_second_by06_nowR_month60_State_countUnknowr_mean'] = dict_out['loan_second_by06_now_month60_State_countUnknowr_mean']/dict_out['loan_second_by06_month60_State_countUnknowr_mean'] if dict_out['loan_second_by06_month60_State_countUnknowr_mean']>0 else np.nan #var最大值比率
                dict_out['loan_second_by06_now_bt_other_person_ratio'] = loan_second_by06_now[loan_second_by06_now['businessType']=='其他个人消费贷款'].shape[0]/len(loan_second_by06_now) #近06个月截至 现行贷款账户业务种类为其他个人消费贷款 占比
                dict_out['loan_second_by06_nowR_bt_other_person_ratio'] = dict_out['loan_second_by06_now_bt_other_person_ratio']/dict_out['loan_second_by06_bt_other_person_ratio'] if dict_out['loan_second_by06_bt_other_person_ratio']>0 else np.nan #var最大值比率


            #近06个月截至 非循环贷账户
            loan_second_by06_ncycle = loan_second_by06[loan_second_by06['class']=='非循环贷账户']
            if len(loan_second_by06_ncycle)>0:

                dict_out['loan_second_by06_ncycle_month60_State_countCr_max'] = loan_second_by06_ncycle['month60_State_countCr'].max()#var最大值
                dict_out['loan_second_by06_ncycle_bt_other_loan_count'] = loan_second_by06_ncycle[loan_second_by06_ncycle['businessType']=='其他贷款'].shape[0] #近06个月截至 非循环贷账户 贷款账户业务种类为其他贷款 计数
                dict_out['loan_second_by06_ncycle_repayAmt_max'] = loan_second_by06_ncycle['repayAmt'].max()#var最大值
                dict_out['loan_second_by06_ncycle_repayAmt_min'] = loan_second_by06_ncycle['repayAmt'].min() #var最小值
                dict_out['loan_second_by06_ncycle_repayAmt_range'] = (dict_out['loan_second_by06_ncycle_repayAmt_max']-dict_out['loan_second_by06_ncycle_repayAmt_min'])/dict_out['loan_second_by06_ncycle_repayAmt_max'] if dict_out['loan_second_by06_ncycle_repayAmt_max']>0 else np.nan #var区间率
                dict_out['loan_second_by06_ncycle_month60_State_countN_sum'] = loan_second_by06_ncycle['month60_State_countN'].sum() #var求和
                dict_out['loan_second_by06_ncycle_month60_State_countNull_sum'] = loan_second_by06_ncycle['month60_State_countNull'].sum() #var求和
                dict_out['loan_second_by06_ncycle_repayAmt_mean'] = loan_second_by06_ncycle['repayAmt'].mean() #var平均值
                dict_out['loan_second_by06_ncycle_rf_other_count'] = loan_second_by06_ncycle[loan_second_by06_ncycle['repayFrequency']=='其他'].shape[0] #近06个月截至 非循环贷账户 贷款账户还款频率为其他 计数
                dict_out['loan_second_by06_ncycle_guaranteeForm_giniimpurity'] = giniimpurity(loan_second_by06_ncycle['guaranteeForm']) #近06个月截至 非循环贷账户 贷款账户担保方式基尼不纯度
                dict_out['loan_second_by06_ncycle_startDate_to_report_sum'] = loan_second_by06_ncycle['startDate_to_report'].sum() #var求和
                dict_out['loan_second_by06_ncycle_balance_ratio_mean'] = loan_second_by06_ncycle['balance_ratio'].mean() #var平均值
                dict_out['loan_second_by06_ncycle_classify5_num_max'] = loan_second_by06_ncycle['classify5_num'].max()#var最大值
                dict_out['loan_second_by06_ncycle_classify5_num_min'] = loan_second_by06_ncycle['classify5_num'].min() #var最小值
                dict_out['loan_second_by06_ncycle_classify5_num_range'] = (dict_out['loan_second_by06_ncycle_classify5_num_max']-dict_out['loan_second_by06_ncycle_classify5_num_min'])/dict_out['loan_second_by06_ncycle_classify5_num_max'] if dict_out['loan_second_by06_ncycle_classify5_num_max']>0 else np.nan #var区间率
                dict_out['loan_second_by06_ncycle_org_giniimpurity'] = giniimpurity(loan_second_by06_ncycle['org']) #近06个月截至 非循环贷账户 贷款账户管理机构基尼不纯度
                dict_out['loan_second_by06_ncycle_repayMons_sum'] = loan_second_by06_ncycle['repayMons'].sum() #var求和
                dict_out['loan_second_by06_ncycle_org_other_ratio'] = loan_second_by06_ncycle[loan_second_by06_ncycle['org']=='其他机构'].shape[0]/len(loan_second_by06_ncycle) #近06个月截至 非循环贷账户 贷款账户管理机构为其他机构 占比
                dict_out['loan_second_by06_ncycle_month60_to_report_mean_mean'] = loan_second_by06_ncycle['month60_to_report_mean'].mean() #var平均值
                dict_out['loan_second_by06_ncycle_org_commercial_bank_count'] = loan_second_by06_ncycle[loan_second_by06_ncycle['org']=='商业银行'].shape[0] #近06个月截至 非循环贷账户 贷款账户管理机构为商业银行 计数
                dict_out['loan_second_by06_ncycle_month60_State_countCr_max'] = loan_second_by06_ncycle['month60_State_countCr'].max()#var最大值
                dict_out['loan_second_by06_ncycle_month60_State_countCr_min'] = loan_second_by06_ncycle['month60_State_countCr'].min() #var最小值
                dict_out['loan_second_by06_ncycle_month60_State_countCr_range'] = (dict_out['loan_second_by06_ncycle_month60_State_countCr_max']-dict_out['loan_second_by06_ncycle_month60_State_countCr_min'])/dict_out['loan_second_by06_ncycle_month60_State_countCr_max'] if dict_out['loan_second_by06_ncycle_month60_State_countCr_max']>0 else np.nan #var区间率
                dict_out['loan_second_by06_ncycle_month60_to_report_min_mean'] = loan_second_by06_ncycle['month60_to_report_min'].mean() #var平均值
                dict_out['loan_second_by06_ncycle_rt_unknow_count'] = loan_second_by06_ncycle[loan_second_by06_ncycle['repayType']=='--'].shape[0] #近06个月截至 非循环贷账户 贷款账户还款方式为-- 计数
                dict_out['loan_second_by06_ncycle_repayMons_ratio_max'] = loan_second_by06_ncycle['repayMons_ratio'].max()#var最大值
                dict_out['loan_second_by06_ncycle_repayMons_ratio_min'] = loan_second_by06_ncycle['repayMons_ratio'].min() #var最小值
                dict_out['loan_second_by06_ncycle_repayMons_ratio_range'] = (dict_out['loan_second_by06_ncycle_repayMons_ratio_max']-dict_out['loan_second_by06_ncycle_repayMons_ratio_min'])/dict_out['loan_second_by06_ncycle_repayMons_ratio_max'] if dict_out['loan_second_by06_ncycle_repayMons_ratio_max']>0 else np.nan #var区间率
                dict_out['loan_second_by06_ncycle_month60_State_countCr_mean'] = loan_second_by06_ncycle['month60_State_countCr'].mean() #var平均值
                dict_out['loan_second_by06_ncycle_month60_State_count2_max'] = loan_second_by06_ncycle['month60_State_count2'].max()#var最大值
                dict_out['loan_second_by06_ncycle_rf_other_ratio'] = loan_second_by06_ncycle[loan_second_by06_ncycle['repayFrequency']=='其他'].shape[0]/len(loan_second_by06_ncycle) #近06个月截至 非循环贷账户 贷款账户还款频率为其他 占比
                dict_out['loan_second_by06_ncycle_month60_State_countUnknow_mean'] = loan_second_by06_ncycle['month60_State_countUnknow'].mean() #var平均值
                dict_out['loan_second_by06_ncycle_org_commercial_bank_ratio'] = loan_second_by06_ncycle[loan_second_by06_ncycle['org']=='商业银行'].shape[0]/len(loan_second_by06_ncycle) #近06个月截至 非循环贷账户 贷款账户管理机构为商业银行 占比
                dict_out['loan_second_by06_ncycle_repayTerm_ratio_max'] = loan_second_by06_ncycle['repayTerm_ratio'].max()#var最大值
                dict_out['loan_second_by06_ncycle_repayTerm_ratio_min'] = loan_second_by06_ncycle['repayTerm_ratio'].min() #var最小值
                dict_out['loan_second_by06_ncycle_repayTerm_ratio_range'] = (dict_out['loan_second_by06_ncycle_repayTerm_ratio_max']-dict_out['loan_second_by06_ncycle_repayTerm_ratio_min'])/dict_out['loan_second_by06_ncycle_repayTerm_ratio_max'] if dict_out['loan_second_by06_ncycle_repayTerm_ratio_max']>0 else np.nan #var区间率
                dict_out['loan_second_by06_ncycle_repayedAmount_mean'] = loan_second_by06_ncycle['repayedAmount'].mean() #var平均值
                dict_out['loan_second_by06_ncycle_month60_to_report_max_sum'] = loan_second_by06_ncycle['month60_to_report_max'].sum() #var求和
                dict_out['loan_second_by06_ncycle_bt_person_business_ratio'] = loan_second_by06_ncycle[loan_second_by06_ncycle['businessType']=='个人经营性贷款'].shape[0]/len(loan_second_by06_ncycle) #近06个月截至 非循环贷账户 贷款账户业务种类为个人经营性贷款 占比
                dict_out['loan_second_by06_ncycle_month60_to_report_mean_max'] = loan_second_by06_ncycle['month60_to_report_mean'].max()#var最大值
                dict_out['loan_second_by06_ncycle_month60_to_report_mean_min'] = loan_second_by06_ncycle['month60_to_report_mean'].min() #var最小值
                dict_out['loan_second_by06_ncycle_month60_to_report_mean_range'] = (dict_out['loan_second_by06_ncycle_month60_to_report_mean_max']-dict_out['loan_second_by06_ncycle_month60_to_report_mean_min'])/dict_out['loan_second_by06_ncycle_month60_to_report_mean_max'] if dict_out['loan_second_by06_ncycle_month60_to_report_mean_max']>0 else np.nan #var区间率
                dict_out['loan_second_by06_ncycle_repayMons_max'] = loan_second_by06_ncycle['repayMons'].max()#var最大值
                dict_out['loan_second_by06_ncycle_repayMons_min'] = loan_second_by06_ncycle['repayMons'].min() #var最小值
                dict_out['loan_second_by06_ncycle_repayMons_range'] = (dict_out['loan_second_by06_ncycle_repayMons_max']-dict_out['loan_second_by06_ncycle_repayMons_min'])/dict_out['loan_second_by06_ncycle_repayMons_max'] if dict_out['loan_second_by06_ncycle_repayMons_max']>0 else np.nan #var区间率
                dict_out['loan_second_by06_ncycle_balance_max'] = loan_second_by06_ncycle['balance'].max()#var最大值
                dict_out['loan_second_by06_ncycle_month60_Amount_num_mean_mean'] = loan_second_by06_ncycle['month60_Amount_num_mean'].mean() #var平均值
                dict_out['loan_second_by06_ncycle_byDate_to_report_mean'] = loan_second_by06_ncycle['byDate_to_report'].mean() #var平均值
                dict_out['loan_second_by06_ncycle_month60_State_countUnknowr_mean'] = loan_second_by06_ncycle['month60_State_countUnknowr'].mean() #var平均值
                dict_out['loan_second_by06_ncycle_gf_other_ratio'] = loan_second_by06_ncycle[loan_second_by06_ncycle['guaranteeForm']=='其他'].shape[0]/len(loan_second_by06_ncycle) #近06个月截至 非循环贷账户 贷款账户担保方式为其他 占比
                dict_out['loan_second_by06_ncycle_repayMons_mean'] = loan_second_by06_ncycle['repayMons'].mean() #var平均值
                dict_out['loan_second_by06_ncycle_repayTerm_ratio_min'] = loan_second_by06_ncycle['repayTerm_ratio'].min() #var最小值
                dict_out['loan_second_by06_ncycle_gf_combine_nowarranty_ratio'] = loan_second_by06_ncycle[loan_second_by06_ncycle['guaranteeForm']=='组合（不含保证）'].shape[0]/len(loan_second_by06_ncycle) #近06个月截至 非循环贷账户 贷款账户担保方式为组合（不含保证） 占比
                dict_out['loan_second_by06_ncycle_is_now_min'] = loan_second_by06_ncycle['is_now'].min() #var最小值
                dict_out['loan_second_by06_ncycle_bt_other_person_ratio'] = loan_second_by06_ncycle[loan_second_by06_ncycle['businessType']=='其他个人消费贷款'].shape[0]/len(loan_second_by06_ncycle) #近06个月截至 非循环贷账户 贷款账户业务种类为其他个人消费贷款 占比
                dict_out['loan_second_by06_ncycle_repayTerm_ratio_max'] = loan_second_by06_ncycle['repayTerm_ratio'].max() #var最大值比率
                dict_out['loan_second_by06_ncycle_RepayedAmount_ratio_max'] = loan_second_by06_ncycle['RepayedAmount_ratio'].max() #var最大值比率
                dict_out['loan_second_by06_ncycle_month60_to_report_min_sum'] = loan_second_by06_ncycle['month60_to_report_min'].sum() #var求和比率
                dict_out['loan_second_by06_ncycle_month60_State_countNullr_sum'] = loan_second_by06_ncycle['month60_State_countNullr'].sum() #var求和比率
                dict_out['loan_second_by06_ncycle_repayedAmount_sum'] = loan_second_by06_ncycle['repayedAmount'].sum() #var求和比率
                dict_out['loan_second_by06_ncycle_balance_max'] = loan_second_by06_ncycle['balance'].max()#var最大值
                dict_out['loan_second_by06_ncycle_balance_min'] = loan_second_by06_ncycle['balance'].min() #var最小值比率
                dict_out['loan_second_by06_ncycle_balance_range'] = (dict_out['loan_second_by06_ncycle_balance_max']-dict_out['loan_second_by06_ncycle_balance_min'])/dict_out['loan_second_by06_ncycle_balance_max'] if dict_out['loan_second_by06_ncycle_balance_max']>0 else np.nan #var区间率
                dict_out['loan_second_by06_ncycleR_repayTerm_ratio_max'] = dict_out['loan_second_by06_ncycle_repayTerm_ratio_max']/dict_out['loan_second_by06_repayTerm_ratio_max'] if dict_out['loan_second_by06_repayTerm_ratio_max']>0 else np.nan #var最大值比率
                dict_out['loan_second_by06_ncycleR_RepayedAmount_ratio_max'] = dict_out['loan_second_by06_ncycle_RepayedAmount_ratio_max']/dict_out['loan_second_by06_RepayedAmount_ratio_max'] if dict_out['loan_second_by06_RepayedAmount_ratio_max']>0 else np.nan #var最大值比率
                dict_out['loan_second_by06_ncycleR_month60_to_report_min_sum'] = dict_out['loan_second_by06_ncycle_month60_to_report_min_sum']/dict_out['loan_second_by06_month60_to_report_min_sum'] if dict_out['loan_second_by06_month60_to_report_min_sum']>0 else np.nan #var最大值比率
                dict_out['loan_second_by06_ncycleR_month60_State_countNullr_sum'] = dict_out['loan_second_by06_ncycle_month60_State_countNullr_sum']/dict_out['loan_second_by06_month60_State_countNullr_sum'] if dict_out['loan_second_by06_month60_State_countNullr_sum']>0 else np.nan #var最大值比率
                dict_out['loan_second_by06_ncycle_bt_other_person_ratio'] = loan_second_by06_ncycle[loan_second_by06_ncycle['businessType']=='其他个人消费贷款'].shape[0]/len(loan_second_by06_ncycle) #近06个月截至 非循环贷账户 贷款账户业务种类为其他个人消费贷款 占比
                dict_out['loan_second_by06_ncycleR_bt_other_person_ratio'] = dict_out['loan_second_by06_ncycle_bt_other_person_ratio']/dict_out['loan_second_by06_bt_other_person_ratio'] if dict_out['loan_second_by06_bt_other_person_ratio']>0 else np.nan #var最大值比率
                dict_out['loan_second_by06_ncycleR_repayedAmount_sum'] = dict_out['loan_second_by06_ncycle_repayedAmount_sum']/dict_out['loan_second_by06_repayedAmount_sum'] if dict_out['loan_second_by06_repayedAmount_sum']>0 else np.nan #var最大值比率
                dict_out['loan_second_by06_ncycleR_balance_range'] = dict_out['loan_second_by06_ncycle_balance_range']/dict_out['loan_second_by06_balance_range'] if dict_out['loan_second_by06_balance_range']>0 else np.nan #var最大值比率


            #近06个月截至 循环贷账户
            loan_second_by06_cycle = loan_second_by06[loan_second_by06['class']=='循环贷账户']
            if len(loan_second_by06_cycle)>0:

                dict_out['loan_second_by06_cycle_repayMons_ratio_sum'] = loan_second_by06_cycle['repayMons_ratio'].sum() #var求和比率
                dict_out['loan_second_by06_cycleR_repayMons_ratio_sum'] = dict_out['loan_second_by06_cycle_repayMons_ratio_sum']/dict_out['loan_second_by06_repayMons_ratio_sum'] if dict_out['loan_second_by06_repayMons_ratio_sum']>0 else np.nan #var最大值比率


            #近06个月截至 有担保 is_vouch 
            loan_second_by06_vouch = loan_second_by06[loan_second_by06['is_vouch']==1]
            if len(loan_second_by06_vouch)>0:

                dict_out['loan_second_by06_vouch_loanAmount_sum'] = loan_second_by06_vouch['loanAmount'].sum() #var求和
                dict_out['loan_second_by06_vouch_loanGrantOrg_nunique'] = loan_second_by06_vouch['loanGrantOrg'].nunique() #近06个月截至 有担保贷款账户管理机构详细nunique
                dict_out['loan_second_by06_vouch_month60_State_num_size_sum'] = loan_second_by06_vouch['month60_State_num_size'].sum() #var求和比率
                dict_out['loan_second_by06_vouch_repayTerm_ratio_min'] = loan_second_by06_vouch['repayTerm_ratio'].min() #var最小值比率
                dict_out['loan_second_by06_vouchR_month60_State_num_size_sum'] = dict_out['loan_second_by06_vouch_month60_State_num_size_sum']/dict_out['loan_second_by06_month60_State_num_size_sum'] if dict_out['loan_second_by06_month60_State_num_size_sum']>0 else np.nan #var最大值比率
                dict_out['loan_second_by06_vouchR_repayTerm_ratio_min'] = dict_out['loan_second_by06_vouch_repayTerm_ratio_min']/dict_out['loan_second_by06_repayTerm_ratio_min'] if dict_out['loan_second_by06_repayTerm_ratio_min']>0 else np.nan #var最大值比率
                dict_out['loan_second_by06_vouch_loanGrantOrg_nunique'] = loan_second_by06_vouch['loanGrantOrg'].nunique() #近06个月截至 有担保贷款账户管理机构详细nunique
                dict_out['loan_second_by06_vouchR_loanGrantOrg_nunique'] = dict_out['loan_second_by06_vouch_loanGrantOrg_nunique']/dict_out['loan_second_by06_loanGrantOrg_nunique'] if dict_out['loan_second_by06_loanGrantOrg_nunique']>0 else np.nan #var最大值比率


            #近06个月截至 历史逾期
            loan_second_by06_hdue1 = loan_second_by06[loan_second_by06['is_overdue']==1]
            if len(loan_second_by06_hdue1)>0:

                dict_out['loan_second_by06_hdue1_month60_State_num_mean_mean'] = loan_second_by06_hdue1['month60_State_num_mean'].mean() #var平均值
                dict_out['loan_second_by06_hdue1_month60_State_countNull_sum'] = loan_second_by06_hdue1['month60_State_countNull'].sum() #var求和比率
                dict_out['loan_second_by06_hdue1_month60_State_countNullr_sum'] = loan_second_by06_hdue1['month60_State_countNullr'].sum() #var求和比率
                dict_out['loan_second_by06_hdue1_repayMons_ratio_sum'] = loan_second_by06_hdue1['repayMons_ratio'].sum() #var求和比率
                dict_out['loan_second_by06_hdue1R_month60_State_countNull_sum'] = dict_out['loan_second_by06_hdue1_month60_State_countNull_sum']/dict_out['loan_second_by06_month60_State_countNull_sum'] if dict_out['loan_second_by06_month60_State_countNull_sum']>0 else np.nan #var最大值比率
                dict_out['loan_second_by06_hdue1R_month60_State_countNullr_sum'] = dict_out['loan_second_by06_hdue1_month60_State_countNullr_sum']/dict_out['loan_second_by06_month60_State_countNullr_sum'] if dict_out['loan_second_by06_month60_State_countNullr_sum']>0 else np.nan #var最大值比率
                dict_out['loan_second_by06_hdue1R_repayMons_ratio_sum'] = dict_out['loan_second_by06_hdue1_repayMons_ratio_sum']/dict_out['loan_second_by06_repayMons_ratio_sum'] if dict_out['loan_second_by06_repayMons_ratio_sum']>0 else np.nan #var最大值比率

            #近06个月截至 历史严重逾期
            loan_second_by06_hdue3 = loan_second_by06[loan_second_by06['due_class']==3]


            #近06个月截至 当前逾期
            loan_second_by06_cdue = loan_second_by06[loan_second_by06['accountStatus']=='逾期']
            


        #近12个月截至
        loan_second_by12 = loan_second[loan_second.byDate_to_report<12]
        if len(loan_second_by12)>0:

            if True:
                for var in numeric_vers:
                    dict_out['loan_second_by12_'+var+'_max'] = loan_second_by12[var].max() #近12个月截至 var最大值
                    dict_out['loan_second_by12_'+var+'_min'] = loan_second_by12[var].min() #近12个月截至 var最小值
                    dict_out['loan_second_by12_'+var+'_mean'] = loan_second_by12[var].mean() #近12个月截至 var平均值
                    dict_out['loan_second_by12_'+var+'_sum'] = loan_second_by12[var].sum() #近12个月截至 var求和
                    dict_out['loan_second_by12_'+var+'_range'] = (dict_out['loan_second_by12_'+var+'_max']-dict_out['loan_second_by12_'+var+'_min'])/dict_out['loan_second_by12_'+var+'_max'] if dict_out['loan_second_by12_'+var+'_max']>0 else np.nan #近12个月截至 var区间率



                dict_out['loan_second_by12_loanGrantOrg_giniimpurity'] = giniimpurity(loan_second_by12['loanGrantOrg']) #近12个月截至 贷款账户管理机构详细基尼不纯度
                dict_out['loan_second_by12_loanGrantOrg_nunique'] = loan_second_by12['loanGrantOrg'].nunique() #近12个月截至 贷款账户管理机构详细nunique

                dict_out['loan_second_by12_org_giniimpurity'] = giniimpurity(loan_second_by12['org']) #近12个月截至 贷款账户管理机构基尼不纯度
                dict_out['loan_second_by12_org_commercial_bank_count'] = loan_second_by12[loan_second_by12['org']=='商业银行'].shape[0] #近12个月截至 贷款账户管理机构为商业银行 计数
                dict_out['loan_second_by12_org_commercial_bank_ratio'] = loan_second_by12[loan_second_by12['org']=='商业银行'].shape[0]/len(loan_second_by12) #近12个月截至 贷款账户管理机构为商业银行 占比
                dict_out['loan_second_by12_org_consumer_finance_count'] = loan_second_by12[loan_second_by12['org']=='消费金融公司'].shape[0] #近12个月截至 贷款账户管理机构为消费金融公司 计数
                dict_out['loan_second_by12_org_consumer_finance_ratio'] = loan_second_by12[loan_second_by12['org']=='消费金融公司'].shape[0]/len(loan_second_by12) #近12个月截至 贷款账户管理机构为消费金融公司 占比
                dict_out['loan_second_by12_org_micro_loan_count'] = loan_second_by12[loan_second_by12['org']=='小额贷款公司'].shape[0] #近12个月截至 贷款账户管理机构为小额贷款公司 计数
                dict_out['loan_second_by12_org_micro_loan_ratio'] = loan_second_by12[loan_second_by12['org']=='小额贷款公司'].shape[0]/len(loan_second_by12) #近12个月截至 贷款账户管理机构为小额贷款公司 占比
                dict_out['loan_second_by12_org_other_count'] = loan_second_by12[loan_second_by12['org']=='其他机构'].shape[0] #近12个月截至 贷款账户管理机构为其他机构 计数
                dict_out['loan_second_by12_org_other_ratio'] = loan_second_by12[loan_second_by12['org']=='其他机构'].shape[0]/len(loan_second_by12) #近12个月截至 贷款账户管理机构为其他机构 占比
                dict_out['loan_second_by12_org_trust_company_count'] = loan_second_by12[loan_second_by12['org']=='信托公司'].shape[0] #近12个月截至 贷款账户管理机构为信托公司 计数
                dict_out['loan_second_by12_org_trust_company_ratio'] = loan_second_by12[loan_second_by12['org']=='信托公司'].shape[0]/len(loan_second_by12) #近12个月截至 贷款账户管理机构为信托公司 占比
                dict_out['loan_second_by12_org_car_finance_count'] = loan_second_by12[loan_second_by12['org']=='汽车金融公司'].shape[0] #近12个月截至 贷款账户管理机构为汽车金融公司 计数
                dict_out['loan_second_by12_org_car_finance_ratio'] = loan_second_by12[loan_second_by12['org']=='汽车金融公司'].shape[0]/len(loan_second_by12) #近12个月截至 贷款账户管理机构为汽车金融公司 占比
                dict_out['loan_second_by12_org_lease_finance_count'] = loan_second_by12[loan_second_by12['org']=='融资租赁公司'].shape[0] #近12个月截至 贷款账户管理机构为融资租赁公司 计数
                dict_out['loan_second_by12_org_lease_finance_ratio'] = loan_second_by12[loan_second_by12['org']=='融资租赁公司'].shape[0]/len(loan_second_by12) #近12个月截至 贷款账户管理机构为融资租赁公司 占比
                dict_out['loan_second_by12_org_myself_count'] = loan_second_by12[loan_second_by12['org']==dict_in['cc_rh_report'].queryOperator.values[0]].shape[0] #近12个月截至 贷款账户管理机构为本机构 计数
                dict_out['loan_second_by12_org_myself_ratio'] = loan_second_by12[loan_second_by12['org']==dict_in['cc_rh_report'].queryOperator.values[0]].shape[0]/len(loan_second_by12) #近12个月截至 贷款账户管理机构为本机构 占比
                dict_out['loan_second_by12_org_village_banks_count'] = loan_second_by12[loan_second_by12['org']=='村镇银行'].shape[0] #近12个月截至 贷款账户管理机构为村镇银行 计数
                dict_out['loan_second_by12_org_village_banks_ratio'] = loan_second_by12[loan_second_by12['org']=='村镇银行'].shape[0]/len(loan_second_by12) #近12个月截至 贷款账户管理机构为村镇银行 占比
                dict_out['loan_second_by12_org_finance_company_count'] = loan_second_by12[loan_second_by12['org']=='财务公司'].shape[0] #近12个月截至 贷款账户管理机构为财务公司 计数
                dict_out['loan_second_by12_org_finance_company_ratio'] = loan_second_by12[loan_second_by12['org']=='财务公司'].shape[0]/len(loan_second_by12) #近12个月截至 贷款账户管理机构为财务公司 占比
                dict_out['loan_second_by12_org_foreign_banks_count'] = loan_second_by12[loan_second_by12['org']=='外资银行'].shape[0] #近12个月截至 贷款账户管理机构为外资银行 计数
                dict_out['loan_second_by12_org_foreign_banks_ratio'] = loan_second_by12[loan_second_by12['org']=='外资银行'].shape[0]/len(loan_second_by12) #近12个月截至 贷款账户管理机构为外资银行 占比
                dict_out['loan_second_by12_org_provident_fund_count'] = loan_second_by12[loan_second_by12['org']=='公积金管理中心'].shape[0] #近12个月截至 贷款账户管理机构为公积金管理中心 计数
                dict_out['loan_second_by12_org_provident_fund_ratio'] = loan_second_by12[loan_second_by12['org']=='公积金管理中心'].shape[0]/len(loan_second_by12) #近12个月截至 贷款账户管理机构为公积金管理中心 占比
                dict_out['loan_second_by12_org_securities_firm_count'] = loan_second_by12[loan_second_by12['org']=='证券公司'].shape[0] #近12个月截至 贷款账户管理机构为证券公司 计数
                dict_out['loan_second_by12_org_securities_firm_ratio'] = loan_second_by12[loan_second_by12['org']=='证券公司'].shape[0]/len(loan_second_by12) #近12个月截至 贷款账户管理机构为证券公司 占比

                dict_out['loan_second_by12_class_giniimpurity'] = giniimpurity(loan_second_by12['class']) #近12个月截至 贷款账户账户类别基尼不纯度
                dict_out['loan_second_by12_class_ncycle_count'] = loan_second_by12[loan_second_by12['class']=='非循环贷账户'].shape[0] #近12个月截至 贷款账户账户类别为非循环贷账户 计数
                dict_out['loan_second_by12_class_ncycle_ratio'] = loan_second_by12[loan_second_by12['class']=='非循环贷账户'].shape[0]/len(loan_second_by12) #近12个月截至 贷款账户账户类别为非循环贷账户 占比
                dict_out['loan_second_by12_class_cycle_sub_count'] = loan_second_by12[loan_second_by12['class']=='循环额度下分账户'].shape[0] #近12个月截至 贷款账户账户类别为循环额度下分账户 计数
                dict_out['loan_second_by12_class_cycle_sub_ratio'] = loan_second_by12[loan_second_by12['class']=='循环额度下分账户'].shape[0]/len(loan_second_by12) #近12个月截至 贷款账户账户类别为循环额度下分账户 占比
                dict_out['loan_second_by12_class_cycle_count'] = loan_second_by12[loan_second_by12['class']=='循环贷账户'].shape[0] #近12个月截至 贷款账户账户类别为循环贷账户 计数
                dict_out['loan_second_by12_class_cycle_ratio'] = loan_second_by12[loan_second_by12['class']=='循环贷账户'].shape[0]/len(loan_second_by12) #近12个月截至 贷款账户账户类别为循环贷账户 占比

                dict_out['loan_second_by12_classify5_giniimpurity'] = giniimpurity(loan_second_by12['classify5']) #近12个月截至 贷款账户五级分类基尼不纯度
                dict_out['loan_second_by12_c5_unknow_count'] = loan_second_by12[loan_second_by12['classify5']==''].shape[0] #近12个月截至 贷款账户五级分类为'' 计数
                dict_out['loan_second_by12_c5_unknow_ratio'] = loan_second_by12[loan_second_by12['classify5']==''].shape[0]/len(loan_second_by12) #近12个月截至 贷款账户五级分类为'' 占比
                dict_out['loan_second_by12_c5_normal_count'] = loan_second_by12[loan_second_by12['classify5']=='正常'].shape[0] #近12个月截至 贷款账户五级分类为正常 计数
                dict_out['loan_second_by12_c5_normal_ratio'] = loan_second_by12[loan_second_by12['classify5']=='正常'].shape[0]/len(loan_second_by12) #近12个月截至 贷款账户五级分类为正常 占比
                dict_out['loan_second_by12_c5_loss_count'] = loan_second_by12[loan_second_by12['classify5']=='损失'].shape[0] #近12个月截至 贷款账户五级分类为损失 计数
                dict_out['loan_second_by12_c5_loss_ratio'] = loan_second_by12[loan_second_by12['classify5']=='损失'].shape[0]/len(loan_second_by12) #近12个月截至 贷款账户五级分类为损失 占比
                dict_out['loan_second_by12_c5_attention_count'] = loan_second_by12[loan_second_by12['classify5']=='关注'].shape[0] #近12个月截至 贷款账户五级分类为关注 计数
                dict_out['loan_second_by12_c5_attention_ratio'] = loan_second_by12[loan_second_by12['classify5']=='关注'].shape[0]/len(loan_second_by12) #近12个月截至 贷款账户五级分类为关注 占比
                dict_out['loan_second_by12_c5_suspicious_count'] = loan_second_by12[loan_second_by12['classify5']=='可疑'].shape[0] #近12个月截至 贷款账户五级分类为可疑 计数
                dict_out['loan_second_by12_c5_suspicious_ratio'] = loan_second_by12[loan_second_by12['classify5']=='可疑'].shape[0]/len(loan_second_by12) #近12个月截至 贷款账户五级分类为可疑 占比
                dict_out['loan_second_by12_c5_secondary_count'] = loan_second_by12[loan_second_by12['classify5']=='次级'].shape[0] #近12个月截至 贷款账户五级分类为次级 计数
                dict_out['loan_second_by12_c5_secondary_ratio'] = loan_second_by12[loan_second_by12['classify5']=='次级'].shape[0]/len(loan_second_by12) #近12个月截至 贷款账户五级分类为次级 占比
                dict_out['loan_second_by12_c5_noclass_count'] = loan_second_by12[loan_second_by12['classify5']=='未分类'].shape[0] #近12个月截至 贷款账户五级分类为未分类 计数
                dict_out['loan_second_by12_c5_noclass_ratio'] = loan_second_by12[loan_second_by12['classify5']=='未分类'].shape[0]/len(loan_second_by12) #近12个月截至 贷款账户五级分类为未分类 占比

                dict_out['loan_second_by12_accountStatus_giniimpurity'] = giniimpurity(loan_second_by12['accountStatus']) #近12个月截至 贷款账户账户状态基尼不纯度
                dict_out['loan_second_by12_as_settle_count'] = loan_second_by12[loan_second_by12['accountStatus']=='结清'].shape[0] #近12个月截至 贷款账户账户状态为结清 计数
                dict_out['loan_second_by12_as_settle_ratio'] = loan_second_by12[loan_second_by12['accountStatus']=='结清'].shape[0]/len(loan_second_by12) #近12个月截至 贷款账户账户状态为结清 占比
                dict_out['loan_second_by12_as_normal_count'] = loan_second_by12[loan_second_by12['accountStatus']=='正常'].shape[0] #近12个月截至 贷款账户账户状态为正常 计数
                dict_out['loan_second_by12_as_normal_ratio'] = loan_second_by12[loan_second_by12['accountStatus']=='正常'].shape[0]/len(loan_second_by12) #近12个月截至 贷款账户账户状态为正常 占比
                dict_out['loan_second_by12_as_overdue_count'] = loan_second_by12[loan_second_by12['accountStatus']=='逾期'].shape[0] #近12个月截至 贷款账户账户状态为逾期 计数
                dict_out['loan_second_by12_as_overdue_ratio'] = loan_second_by12[loan_second_by12['accountStatus']=='逾期'].shape[0]/len(loan_second_by12) #近12个月截至 贷款账户账户状态为逾期 占比
                dict_out['loan_second_by12_as_bad_debts_count'] = loan_second_by12[loan_second_by12['accountStatus']=='呆账'].shape[0] #近12个月截至 贷款账户账户状态为呆账 计数
                dict_out['loan_second_by12_as_bad_debts_ratio'] = loan_second_by12[loan_second_by12['accountStatus']=='呆账'].shape[0]/len(loan_second_by12) #近12个月截至 贷款账户账户状态为呆账 占比
                dict_out['loan_second_by12_as_unknow_count'] = loan_second_by12[loan_second_by12['accountStatus']==''].shape[0] #近12个月截至 贷款账户账户状态为'' 计数
                dict_out['loan_second_by12_as_unknow_ratio'] = loan_second_by12[loan_second_by12['accountStatus']==''].shape[0]/len(loan_second_by12) #近12个月截至 贷款账户账户状态为'' 占比
                dict_out['loan_second_by12_as_roll_out_count'] = loan_second_by12[loan_second_by12['accountStatus']=='转出'].shape[0] #近12个月截至 贷款账户账户状态为转出 计数
                dict_out['loan_second_by12_as_roll_out_ratio'] = loan_second_by12[loan_second_by12['accountStatus']=='转出'].shape[0]/len(loan_second_by12) #近12个月截至 贷款账户账户状态为转出 占比

                dict_out['loan_second_by12_repayType_giniimpurity'] = giniimpurity(loan_second_by12['repayType']) #近12个月截至 贷款账户还款方式基尼不纯度
                dict_out['loan_second_by12_rt_unknow_count'] = loan_second_by12[loan_second_by12['repayType']=='--'].shape[0] #近12个月截至 贷款账户还款方式为-- 计数
                dict_out['loan_second_by12_rt_unknow_ratio'] = loan_second_by12[loan_second_by12['repayType']=='--'].shape[0]/len(loan_second_by12) #近12个月截至 贷款账户还款方式为-- 占比
                dict_out['loan_second_by12_rt_equality_count'] = loan_second_by12[loan_second_by12['repayType']=='分期等额本息'].shape[0] #近12个月截至 贷款账户还款方式为分期等额本息 计数
                dict_out['loan_second_by12_rt_equality_ratio'] = loan_second_by12[loan_second_by12['repayType']=='分期等额本息'].shape[0]/len(loan_second_by12) #近12个月截至 贷款账户还款方式为分期等额本息 占比
                dict_out['loan_second_by12_rt_onschedule_count'] = loan_second_by12[loan_second_by12['repayType']=='按期计算还本付息'].shape[0] #近12个月截至 贷款账户还款方式为按期计算还本付息 计数
                dict_out['loan_second_by12_rt_onschedule_ratio'] = loan_second_by12[loan_second_by12['repayType']=='按期计算还本付息'].shape[0]/len(loan_second_by12) #近12个月截至 贷款账户还款方式为按期计算还本付息 占比
                dict_out['loan_second_by12_rt_not_distinguish_count'] = loan_second_by12[loan_second_by12['repayType']=='不区分还款方式'].shape[0] #近12个月截至 贷款账户还款方式为不区分还款方式 计数
                dict_out['loan_second_by12_rt_not_distinguish_ratio'] = loan_second_by12[loan_second_by12['repayType']=='不区分还款方式'].shape[0]/len(loan_second_by12) #近12个月截至 贷款账户还款方式为不区分还款方式 占比
                dict_out['loan_second_by12_rt_circulation_count'] = loan_second_by12[loan_second_by12['repayType']=='循环贷款下其他还款方式'].shape[0] #近12个月截至 贷款账户还款方式为循环贷款下其他还款方式 计数
                dict_out['loan_second_by12_rt_circulation_ratio'] = loan_second_by12[loan_second_by12['repayType']=='循环贷款下其他还款方式'].shape[0]/len(loan_second_by12) #近12个月截至 贷款账户还款方式为循环贷款下其他还款方式 占比
                dict_out['loan_second_by12_rt_once_count'] = loan_second_by12[loan_second_by12['repayType']=='到期一次还本付息'].shape[0] #近12个月截至 贷款账户还款方式为到期一次还本付息 计数
                dict_out['loan_second_by12_rt_once_ratio'] = loan_second_by12[loan_second_by12['repayType']=='到期一次还本付息'].shape[0]/len(loan_second_by12) #近12个月截至 贷款账户还款方式为到期一次还本付息 占比

                dict_out['loan_second_by12_repayFrequency_giniimpurity'] = giniimpurity(loan_second_by12['repayFrequency']) #近12个月截至 贷款账户还款频率基尼不纯度
                dict_out['loan_second_by12_rf_month_count'] = loan_second_by12[loan_second_by12['repayFrequency']=='月'].shape[0] #近12个月截至 贷款账户还款频率为月 计数
                dict_out['loan_second_by12_rf_month_ratio'] = loan_second_by12[loan_second_by12['repayFrequency']=='月'].shape[0]/len(loan_second_by12) #近12个月截至 贷款账户还款频率为月 占比
                dict_out['loan_second_by12_rf_once_count'] = loan_second_by12[loan_second_by12['repayFrequency']=='一次性'].shape[0] #近12个月截至 贷款账户还款频率为一次性 计数
                dict_out['loan_second_by12_rf_once_ratio'] = loan_second_by12[loan_second_by12['repayFrequency']=='一次性'].shape[0]/len(loan_second_by12) #近12个月截至 贷款账户还款频率为一次性 占比
                dict_out['loan_second_by12_rf_other_count'] = loan_second_by12[loan_second_by12['repayFrequency']=='其他'].shape[0] #近12个月截至 贷款账户还款频率为其他 计数
                dict_out['loan_second_by12_rf_other_ratio'] = loan_second_by12[loan_second_by12['repayFrequency']=='其他'].shape[0]/len(loan_second_by12) #近12个月截至 贷款账户还款频率为其他 占比
                dict_out['loan_second_by12_rf_irregular_count'] = loan_second_by12[loan_second_by12['repayFrequency']=='不定期'].shape[0] #近12个月截至 贷款账户还款频率为不定期 计数
                dict_out['loan_second_by12_rf_irregular_ratio'] = loan_second_by12[loan_second_by12['repayFrequency']=='不定期'].shape[0]/len(loan_second_by12) #近12个月截至 贷款账户还款频率为不定期 占比
                dict_out['loan_second_by12_rf_day_count'] = loan_second_by12[loan_second_by12['repayFrequency']=='日'].shape[0] #近12个月截至 贷款账户还款频率为日 计数
                dict_out['loan_second_by12_rf_day_ratio'] = loan_second_by12[loan_second_by12['repayFrequency']=='日'].shape[0]/len(loan_second_by12) #近12个月截至 贷款账户还款频率为日 占比
                dict_out['loan_second_by12_rf_year_count'] = loan_second_by12[loan_second_by12['repayFrequency']=='年'].shape[0] #近12个月截至 贷款账户还款频率为年 计数
                dict_out['loan_second_by12_rf_year_ratio'] = loan_second_by12[loan_second_by12['repayFrequency']=='年'].shape[0]/len(loan_second_by12) #近12个月截至 贷款账户还款频率为年 占比
                dict_out['loan_second_by12_rf_season_count'] = loan_second_by12[loan_second_by12['repayFrequency']=='季'].shape[0] #近12个月截至 贷款账户还款频率为季 计数
                dict_out['loan_second_by12_rf_season_ratio'] = loan_second_by12[loan_second_by12['repayFrequency']=='季'].shape[0]/len(loan_second_by12) #近12个月截至 贷款账户还款频率为季 占比
                dict_out['loan_second_by12_rf_week_count'] = loan_second_by12[loan_second_by12['repayFrequency']=='周'].shape[0] #近12个月截至 贷款账户还款频率为周 计数
                dict_out['loan_second_by12_rf_week_ratio'] = loan_second_by12[loan_second_by12['repayFrequency']=='周'].shape[0]/len(loan_second_by12) #近12个月截至 贷款账户还款频率为周 占比
                dict_out['loan_second_by12_rf_halfyear_count'] = loan_second_by12[loan_second_by12['repayFrequency']=='半年'].shape[0] #近12个月截至 贷款账户还款频率为半年 计数
                dict_out['loan_second_by12_rf_halfyear_ratio'] = loan_second_by12[loan_second_by12['repayFrequency']=='半年'].shape[0]/len(loan_second_by12) #近12个月截至 贷款账户还款频率为半年 占比

                dict_out['loan_second_by12_guaranteeForm_giniimpurity'] = giniimpurity(loan_second_by12['guaranteeForm']) #近12个月截至 贷款账户担保方式基尼不纯度
                dict_out['loan_second_by12_gf_crdit_count'] = loan_second_by12[loan_second_by12['guaranteeForm']=='信用/免担保'].shape[0] #近12个月截至 贷款账户担保方式为其信用/免担保 计数
                dict_out['loan_second_by12_gf_crdit_ratio'] = loan_second_by12[loan_second_by12['guaranteeForm']=='信用/免担保'].shape[0]/len(loan_second_by12) #近12个月截至 贷款账户担保方式为信用/免担保 占比
                dict_out['loan_second_by12_gf_other_count'] = loan_second_by12[loan_second_by12['guaranteeForm']=='其他'].shape[0] #近12个月截至 贷款账户担保方式为其他 计数
                dict_out['loan_second_by12_gf_other_ratio'] = loan_second_by12[loan_second_by12['guaranteeForm']=='其他'].shape[0]/len(loan_second_by12) #近12个月截至 贷款账户担保方式为其他 占比
                dict_out['loan_second_by12_gf_combine_nowarranty_count'] = loan_second_by12[loan_second_by12['guaranteeForm']=='组合（不含保证）'].shape[0] #近12个月截至 贷款账户担保方式为组合（不含保证） 计数
                dict_out['loan_second_by12_gf_combine_nowarranty_ratio'] = loan_second_by12[loan_second_by12['guaranteeForm']=='组合（不含保证）'].shape[0]/len(loan_second_by12) #近12个月截至 贷款账户担保方式为组合（不含保证） 占比
                dict_out['loan_second_by12_gf_combine_warranty_count'] = loan_second_by12[loan_second_by12['guaranteeForm']=='组合(含保证)'].shape[0] #近12个月截至 贷款账户担保方式为组合(含保证) 计数
                dict_out['loan_second_by12_gf_combine_warranty_ratio'] = loan_second_by12[loan_second_by12['guaranteeForm']=='组合(含保证)'].shape[0]/len(loan_second_by12) #近12个月截至 贷款账户担保方式为组合(含保证) 占比
                dict_out['loan_second_by12_gf_mortgage_count'] = loan_second_by12[loan_second_by12['guaranteeForm']=='抵押'].shape[0] #近12个月截至 贷款账户担保方式为抵押 计数
                dict_out['loan_second_by12_gf_mortgage_ratio'] = loan_second_by12[loan_second_by12['guaranteeForm']=='抵押'].shape[0]/len(loan_second_by12) #近12个月截至 贷款账户担保方式为抵押 占比
                dict_out['loan_second_by12_gf_warranty_count'] = loan_second_by12[loan_second_by12['guaranteeForm']=='保证'].shape[0] #近12个月截至 贷款账户担保方式为保证计数
                dict_out['loan_second_by12_gf_warranty_ratio'] = loan_second_by12[loan_second_by12['guaranteeForm']=='保证'].shape[0]/len(loan_second_by12) #近12个月截至 贷款账户担保方式为保证 占比
                dict_out['loan_second_by12_gf_pledge_count'] = loan_second_by12[loan_second_by12['guaranteeForm']=='质押'].shape[0] #近12个月截至 贷款账户担保方式为质押 计数
                dict_out['loan_second_by12_gf_pledge_ratio'] = loan_second_by12[loan_second_by12['guaranteeForm']=='质押'].shape[0]/len(loan_second_by12) #近12个月截至 贷款账户担保方式为质押 占比
                dict_out['loan_second_by12_gf_farm_group_count'] = loan_second_by12[loan_second_by12['guaranteeForm']=='农户联保'].shape[0] #近12个月截至 贷款账户担保方式为农户联保 计数
                dict_out['loan_second_by12_gf_farm_group_ratio'] = loan_second_by12[loan_second_by12['guaranteeForm']=='农户联保'].shape[0]/len(loan_second_by12) #近12个月截至 贷款账户担保方式为农户联保 占比

                dict_out['loan_second_by12_businessType_giniimpurity'] = giniimpurity(loan_second_by12['businessType']) #近12个月截至 贷款账户业务种类基尼不纯度
                dict_out['loan_second_by12_bt_other_person_count'] = loan_second_by12[loan_second_by12['businessType']=='其他个人消费贷款'].shape[0] #近12个月截至 贷款账户业务种类为其他个人消费贷款 计数
                dict_out['loan_second_by12_bt_other_person_ratio'] = loan_second_by12[loan_second_by12['businessType']=='其他个人消费贷款'].shape[0]/len(loan_second_by12) #近12个月截至 贷款账户业务种类为其他个人消费贷款 占比
                dict_out['loan_second_by12_bt_other_loan_count'] = loan_second_by12[loan_second_by12['businessType']=='其他贷款'].shape[0] #近12个月截至 贷款账户业务种类为其他贷款 计数
                dict_out['loan_second_by12_bt_other_loan_ratio'] = loan_second_by12[loan_second_by12['businessType']=='其他贷款'].shape[0]/len(loan_second_by12) #近12个月截至 贷款账户业务种类为其他贷款 占比
                dict_out['loan_second_by12_bt_person_business_count'] = loan_second_by12[loan_second_by12['businessType']=='个人经营性贷款'].shape[0] #近12个月截至 贷款账户业务种类为个人经营性贷款 计数
                dict_out['loan_second_by12_bt_person_business_ratio'] = loan_second_by12[loan_second_by12['businessType']=='个人经营性贷款'].shape[0]/len(loan_second_by12) #近12个月截至 贷款账户业务种类为个人经营性贷款 占比
                dict_out['loan_second_by12_bt_farm_loan_count'] = loan_second_by12[loan_second_by12['businessType']=='农户贷款'].shape[0] #近12个月截至 贷款账户业务种类为农户贷款 计数
                dict_out['loan_second_by12_bt_farm_loan_ratio'] = loan_second_by12[loan_second_by12['businessType']=='农户贷款'].shape[0]/len(loan_second_by12) #近12个月截至 贷款账户业务种类为农户贷款 占比
                dict_out['loan_second_by12_bt_person_car_count'] = loan_second_by12[loan_second_by12['businessType']=='个人汽车消费贷款'].shape[0] #近12个月截至 贷款账户业务种类为个人汽车消费贷款 计数
                dict_out['loan_second_by12_bt_person_car_ratio'] = loan_second_by12[loan_second_by12['businessType']=='个人汽车消费贷款'].shape[0]/len(loan_second_by12) #近12个月截至 贷款账户业务种类为个人汽车消费贷款 占比
                dict_out['loan_second_by12_bt_person_study_count'] = loan_second_by12[loan_second_by12['businessType']=='个人助学贷款'].shape[0] #近12个月截至 贷款账户业务种类为个人助学贷款 计数
                dict_out['loan_second_by12_bt_person_study_ratio'] = loan_second_by12[loan_second_by12['businessType']=='个人助学贷款'].shape[0]/len(loan_second_by12) #近12个月截至 贷款账户业务种类为个人助学贷款 占比
                dict_out['loan_second_by12_bt_house_commercial_count'] = loan_second_by12[loan_second_by12['businessType']=='个人住房商业贷款'].shape[0] #近12个月截至 贷款账户业务种类为个人住房商业贷款 计数
                dict_out['loan_second_by12_bt_house_commercial_ratio'] = loan_second_by12[loan_second_by12['businessType']=='个人住房商业贷款'].shape[0]/len(loan_second_by12) #近12个月截至 贷款账户业务种类为个人住房商业贷款 占比
                dict_out['loan_second_by12_bt_finance_lease_count'] = loan_second_by12[loan_second_by12['businessType']=='融资租赁业务'].shape[0] #近12个月截至 贷款账户业务种类为融资租赁业务 计数
                dict_out['loan_second_by12_bt_finance_lease_ratio'] = loan_second_by12[loan_second_by12['businessType']=='融资租赁业务'].shape[0]/len(loan_second_by12) #近12个月截至 贷款账户业务种类为融资租赁业务 占比
                dict_out['loan_second_by12_bt_house_fund_count'] = loan_second_by12[loan_second_by12['businessType']=='个人住房公积金贷款'].shape[0] #近12个月截至 贷款账户业务种类为个人住房公积金贷款 计数
                dict_out['loan_second_by12_bt_house_fund_ratio'] = loan_second_by12[loan_second_by12['businessType']=='个人住房公积金贷款'].shape[0]/len(loan_second_by12) #近12个月截至 贷款账户业务种类为个人住房公积金贷款 占比
                dict_out['loan_second_by12_bt_person_house_count'] = loan_second_by12[loan_second_by12['businessType']=='个人商用房（含商住两用）贷款'].shape[0] #近12个月截至 贷款账户业务种类为个人商用房（含商住两用）贷款 计数
                dict_out['loan_second_by12_bt_person_house_ratio'] = loan_second_by12[loan_second_by12['businessType']=='个人商用房（含商住两用）贷款'].shape[0]/len(loan_second_by12) #近12个月截至 贷款账户业务种类为个人商用房（含商住两用）贷款 占比
                dict_out['loan_second_by12_bt_stock_pledge_count'] = loan_second_by12[loan_second_by12['businessType']=='股票质押式回购交易'].shape[0] #近12个月截至 贷款账户业务种类为股票质押式回购交易计数
                dict_out['loan_second_by12_bt_stock_pledge_ratio'] = loan_second_by12[loan_second_by12['businessType']=='股票质押式回购交易'].shape[0]/len(loan_second_by12) #近12个月截至 贷款账户业务种类为股票质押式回购交易 占比



            #近12个月截至 现行
            loan_second_by12_now = loan_second_by12[loan_second_by12.is_now==1]
            if len(loan_second_by12_now)>0:

                dict_out['loan_second_by12_now_rf_other_count'] = loan_second_by12_now[loan_second_by12_now['repayFrequency']=='其他'].shape[0] #近12个月截至 现行贷款账户还款频率为其他 计数
                dict_out['loan_second_by12_now_rf_other_ratio'] = loan_second_by12_now[loan_second_by12_now['repayFrequency']=='其他'].shape[0]/len(loan_second_by12_now) #近12个月截至 现行贷款账户还款频率为其他 占比
                dict_out['loan_second_by12_now_month60_State_countNull_mean'] = loan_second_by12_now['month60_State_countNull'].mean() #var平均值
                dict_out['loan_second_by12_now_month60_to_report_min_mean'] = loan_second_by12_now['month60_to_report_min'].mean() #var平均值
                dict_out['loan_second_by12_now_repayMons_ratio_max'] = loan_second_by12_now['repayMons_ratio'].max()#var最大值
                dict_out['loan_second_by12_now_gf_other_ratio'] = loan_second_by12_now[loan_second_by12_now['guaranteeForm']=='其他'].shape[0]/len(loan_second_by12_now) #近12个月截至 现行贷款账户担保方式为其他 占比
                dict_out['loan_second_by12_now_repayTerm_ratio_max'] = loan_second_by12_now['repayTerm_ratio'].max()#var最大值
                dict_out['loan_second_by12_now_repayTerm_ratio_min'] = loan_second_by12_now['repayTerm_ratio'].min() #var最小值
                dict_out['loan_second_by12_now_repayTerm_ratio_range'] = (dict_out['loan_second_by12_now_repayTerm_ratio_max']-dict_out['loan_second_by12_now_repayTerm_ratio_min'])/dict_out['loan_second_by12_now_repayTerm_ratio_max'] if dict_out['loan_second_by12_now_repayTerm_ratio_max']>0 else np.nan #var区间率
                dict_out['loan_second_by12_now_byDate_to_report_mean'] = loan_second_by12_now['byDate_to_report'].mean() #var平均值
                dict_out['loan_second_by12_now_balance_ratio_mean'] = loan_second_by12_now['balance_ratio'].mean() #var平均值
                dict_out['loan_second_by12_now_repayAmt_min'] = loan_second_by12_now['repayAmt'].min() #var最小值
                dict_out['loan_second_by12_now_balance_ratio_min'] = loan_second_by12_now['balance_ratio'].min() #var最小值
                dict_out['loan_second_by12_now_classify5_num_max'] = loan_second_by12_now['classify5_num'].max()#var最大值
                dict_out['loan_second_by12_now_classify5_num_min'] = loan_second_by12_now['classify5_num'].min() #var最小值
                dict_out['loan_second_by12_now_classify5_num_range'] = (dict_out['loan_second_by12_now_classify5_num_max']-dict_out['loan_second_by12_now_classify5_num_min'])/dict_out['loan_second_by12_now_classify5_num_max'] if dict_out['loan_second_by12_now_classify5_num_max']>0 else np.nan #var区间率
                dict_out['loan_second_by12_now_repayAmt_max'] = loan_second_by12_now['repayAmt'].max()#var最大值
                dict_out['loan_second_by12_now_repayedAmount_mean'] = loan_second_by12_now['repayedAmount'].mean() #var平均值
                dict_out['loan_second_by12_now_repayMons_ratio_mean'] = loan_second_by12_now['repayMons_ratio'].mean() #var平均值
                dict_out['loan_second_by12_now_repayMons_min'] = loan_second_by12_now['repayMons'].min() #var最小值
                dict_out['loan_second_by12_now_balance_ratio_max'] = loan_second_by12_now['balance_ratio'].max()#var最大值
                dict_out['loan_second_by12_now_balance_ratio_min'] = loan_second_by12_now['balance_ratio'].min() #var最小值
                dict_out['loan_second_by12_now_balance_ratio_range'] = (dict_out['loan_second_by12_now_balance_ratio_max']-dict_out['loan_second_by12_now_balance_ratio_min'])/dict_out['loan_second_by12_now_balance_ratio_max'] if dict_out['loan_second_by12_now_balance_ratio_max']>0 else np.nan #var区间率
                dict_out['loan_second_by12_now_bt_other_loan_ratio'] = loan_second_by12_now[loan_second_by12_now['businessType']=='其他贷款'].shape[0]/len(loan_second_by12_now) #近12个月截至 现行贷款账户业务种类为其他贷款 占比
                dict_out['loan_second_by12_now_month60_State_countN_mean'] = loan_second_by12_now['month60_State_countN'].mean() #var平均值
                dict_out['loan_second_by12_now_bt_other_loan_count'] = loan_second_by12_now[loan_second_by12_now['businessType']=='其他贷款'].shape[0] #近12个月截至 现行贷款账户业务种类为其他贷款 计数
                dict_out['loan_second_by12_now_org_commercial_bank_count'] = loan_second_by12_now[loan_second_by12_now['org']=='商业银行'].shape[0] #近12个月截至 现行贷款账户管理机构为商业银行 计数
                dict_out['loan_second_by12_now_loanGrantOrg_nunique'] = loan_second_by12_now['loanGrantOrg'].nunique() #近12个月截至 现行贷款账户管理机构详细nunique
                dict_out['loan_second_by12_now_org_micro_loan_count'] = loan_second_by12_now[loan_second_by12_now['org']=='小额贷款公司'].shape[0] #近12个月截至 现行贷款账户管理机构为小额贷款公司 计数
                dict_out['loan_second_by12_now_class_ncycle_ratio'] = loan_second_by12_now[loan_second_by12_now['class']=='非循环贷账户'].shape[0]/len(loan_second_by12_now) #近12个月截至 现行贷款账户账户类别为非循环贷账户 占比
                dict_out['loan_second_by12_now_repayFrequency_giniimpurity'] = giniimpurity(loan_second_by12_now['repayFrequency']) #近12个月截至 现行贷款账户还款频率基尼不纯度
                dict_out['loan_second_by12_now_org_consumer_finance_ratio'] = loan_second_by12_now[loan_second_by12_now['org']=='消费金融公司'].shape[0]/len(loan_second_by12_now) #近12个月截至 现行贷款账户管理机构为消费金融公司 占比
                dict_out['loan_second_by12_now_org_commercial_bank_ratio'] = loan_second_by12_now[loan_second_by12_now['org']=='商业银行'].shape[0]/len(loan_second_by12_now) #近12个月截至 现行贷款账户管理机构为商业银行 占比
                dict_out['loan_second_by12_now_byDate_to_report_mean'] = loan_second_by12_now['byDate_to_report'].mean()  #var平均值比率
                dict_out['loan_second_by12_now_balance_ratio_max'] = loan_second_by12_now['balance_ratio'].max()#var最大值
                dict_out['loan_second_by12_now_balance_ratio_min'] = loan_second_by12_now['balance_ratio'].min() #var最小值比率
                dict_out['loan_second_by12_now_balance_ratio_range'] = (dict_out['loan_second_by12_now_balance_ratio_max']-dict_out['loan_second_by12_now_balance_ratio_min'])/dict_out['loan_second_by12_now_balance_ratio_max'] if dict_out['loan_second_by12_now_balance_ratio_max']>0 else np.nan #var区间率
                dict_out['loan_second_by12_now_repayMons_max'] = loan_second_by12_now['repayMons'].max() #var最大值比率
                dict_out['loan_second_by12_now_planRepayAmount_max'] = loan_second_by12_now['planRepayAmount'].max()#var最大值
                dict_out['loan_second_by12_now_planRepayAmount_min'] = loan_second_by12_now['planRepayAmount'].min() #var最小值比率
                dict_out['loan_second_by12_now_planRepayAmount_range'] = (dict_out['loan_second_by12_now_planRepayAmount_max']-dict_out['loan_second_by12_now_planRepayAmount_min'])/dict_out['loan_second_by12_now_planRepayAmount_max'] if dict_out['loan_second_by12_now_planRepayAmount_max']>0 else np.nan #var区间率
                dict_out['loan_second_by12_now_month60_to_report_min_sum'] = loan_second_by12_now['month60_to_report_min'].sum() #var求和比率
                dict_out['loan_second_by12_now_repayAmt_max'] = loan_second_by12_now['repayAmt'].max() #var最大值比率
                dict_out['loan_second_by12_now_month60_State_countN_sum'] = loan_second_by12_now['month60_State_countN'].sum() #var求和比率
                dict_out['loan_second_by12_now_month60_to_report_mean_mean'] = loan_second_by12_now['month60_to_report_mean'].mean()  #var平均值比率
                dict_out['loan_second_by12_now_loanAmount_sum'] = loan_second_by12_now['loanAmount'].sum() #var求和比率
                dict_out['loan_second_by12_now_month60_State_countNull_max'] = loan_second_by12_now['month60_State_countNull'].max() #var最大值比率
                dict_out['loan_second_by12_now_repayMons_ratio_mean'] = loan_second_by12_now['repayMons_ratio'].mean()  #var平均值比率
                dict_out['loan_second_by12_now_month60_State_num_size_max'] = loan_second_by12_now['month60_State_num_size'].max()#var最大值
                dict_out['loan_second_by12_now_month60_State_num_size_min'] = loan_second_by12_now['month60_State_num_size'].min() #var最小值比率
                dict_out['loan_second_by12_now_month60_State_num_size_range'] = (dict_out['loan_second_by12_now_month60_State_num_size_max']-dict_out['loan_second_by12_now_month60_State_num_size_min'])/dict_out['loan_second_by12_now_month60_State_num_size_max'] if dict_out['loan_second_by12_now_month60_State_num_size_max']>0 else np.nan #var区间率
                dict_out['loan_second_by12_now_month60_to_report_mean_max'] = loan_second_by12_now['month60_to_report_mean'].max()#var最大值
                dict_out['loan_second_by12_now_month60_to_report_mean_min'] = loan_second_by12_now['month60_to_report_mean'].min() #var最小值比率
                dict_out['loan_second_by12_now_month60_to_report_mean_range'] = (dict_out['loan_second_by12_now_month60_to_report_mean_max']-dict_out['loan_second_by12_now_month60_to_report_mean_min'])/dict_out['loan_second_by12_now_month60_to_report_mean_max'] if dict_out['loan_second_by12_now_month60_to_report_mean_max']>0 else np.nan #var区间率
                dict_out['loan_second_by12_now_startDate_to_report_mean'] = loan_second_by12_now['startDate_to_report'].mean()  #var平均值比率
                dict_out['loan_second_by12_now_month60_State_countUnknow_mean'] = loan_second_by12_now['month60_State_countUnknow'].mean()  #var平均值比率
                dict_out['loan_second_by12_now_repayTerms_sum'] = loan_second_by12_now['repayTerms'].sum() #var求和比率
                dict_out['loan_second_by12_now_repayAmt_max'] = loan_second_by12_now['repayAmt'].max()#var最大值
                dict_out['loan_second_by12_now_repayAmt_min'] = loan_second_by12_now['repayAmt'].min() #var最小值比率
                dict_out['loan_second_by12_now_repayAmt_range'] = (dict_out['loan_second_by12_now_repayAmt_max']-dict_out['loan_second_by12_now_repayAmt_min'])/dict_out['loan_second_by12_now_repayAmt_max'] if dict_out['loan_second_by12_now_repayAmt_max']>0 else np.nan #var区间率
                dict_out['loan_second_by12_now_month60_to_report_min_mean'] = loan_second_by12_now['month60_to_report_min'].mean()  #var平均值比率
                dict_out['loan_second_by12_now_month60_State_countNull_sum'] = loan_second_by12_now['month60_State_countNull'].sum() #var求和比率
                dict_out['loan_second_by12_now_loanAmount_mean'] = loan_second_by12_now['loanAmount'].mean()  #var平均值比率
                dict_out['loan_second_by12_now_repayAmt_sum'] = loan_second_by12_now['repayAmt'].sum() #var求和比率
                dict_out['loan_second_by12_now_loanAmount_max'] = loan_second_by12_now['loanAmount'].max() #var最大值比率
                dict_out['loan_second_by12_now_repayMons_ratio_min'] = loan_second_by12_now['repayMons_ratio'].min() #var最小值比率
                dict_out['loan_second_by12_now_startDate_to_report_max'] = loan_second_by12_now['startDate_to_report'].max()#var最大值
                dict_out['loan_second_by12_now_startDate_to_report_min'] = loan_second_by12_now['startDate_to_report'].min() #var最小值比率
                dict_out['loan_second_by12_now_startDate_to_report_range'] = (dict_out['loan_second_by12_now_startDate_to_report_max']-dict_out['loan_second_by12_now_startDate_to_report_min'])/dict_out['loan_second_by12_now_startDate_to_report_max'] if dict_out['loan_second_by12_now_startDate_to_report_max']>0 else np.nan #var区间率
                dict_out['loan_second_by12_now_month60_State_num_size_mean'] = loan_second_by12_now['month60_State_num_size'].mean()  #var平均值比率
                dict_out['loan_second_by12_nowR_byDate_to_report_mean'] = dict_out['loan_second_by12_now_byDate_to_report_mean']/dict_out['loan_second_by12_byDate_to_report_mean'] if dict_out['loan_second_by12_byDate_to_report_mean']>0 else np.nan #var最大值比率
                dict_out['loan_second_by12_nowR_balance_ratio_range'] = dict_out['loan_second_by12_now_balance_ratio_range']/dict_out['loan_second_by12_balance_ratio_range'] if dict_out['loan_second_by12_balance_ratio_range']>0 else np.nan #var最大值比率
                dict_out['loan_second_by12_nowR_repayMons_max'] = dict_out['loan_second_by12_now_repayMons_max']/dict_out['loan_second_by12_repayMons_max'] if dict_out['loan_second_by12_repayMons_max']>0 else np.nan #var最大值比率
                dict_out['loan_second_by12_nowR_planRepayAmount_range'] = dict_out['loan_second_by12_now_planRepayAmount_range']/dict_out['loan_second_by12_planRepayAmount_range'] if dict_out['loan_second_by12_planRepayAmount_range']>0 else np.nan #var最大值比率
                dict_out['loan_second_by12_nowR_month60_to_report_min_sum'] = dict_out['loan_second_by12_now_month60_to_report_min_sum']/dict_out['loan_second_by12_month60_to_report_min_sum'] if dict_out['loan_second_by12_month60_to_report_min_sum']>0 else np.nan #var最大值比率
                dict_out['loan_second_by12_nowR_repayAmt_max'] = dict_out['loan_second_by12_now_repayAmt_max']/dict_out['loan_second_by12_repayAmt_max'] if dict_out['loan_second_by12_repayAmt_max']>0 else np.nan #var最大值比率
                dict_out['loan_second_by12_nowR_month60_State_countN_sum'] = dict_out['loan_second_by12_now_month60_State_countN_sum']/dict_out['loan_second_by12_month60_State_countN_sum'] if dict_out['loan_second_by12_month60_State_countN_sum']>0 else np.nan #var最大值比率
                dict_out['loan_second_by12_nowR_month60_to_report_mean_mean'] = dict_out['loan_second_by12_now_month60_to_report_mean_mean']/dict_out['loan_second_by12_month60_to_report_mean_mean'] if dict_out['loan_second_by12_month60_to_report_mean_mean']>0 else np.nan #var最大值比率
                dict_out['loan_second_by12_nowR_loanAmount_sum'] = dict_out['loan_second_by12_now_loanAmount_sum']/dict_out['loan_second_by12_loanAmount_sum'] if dict_out['loan_second_by12_loanAmount_sum']>0 else np.nan #var最大值比率
                dict_out['loan_second_by12_now_bt_other_loan_count'] = loan_second_by12_now[loan_second_by12_now['businessType']=='其他贷款'].shape[0] #近12个月截至 现行贷款账户业务种类为其他贷款 计数
                dict_out['loan_second_by12_nowR_bt_other_loan_count'] = dict_out['loan_second_by12_now_bt_other_loan_count']/dict_out['loan_second_by12_bt_other_loan_count'] if dict_out['loan_second_by12_bt_other_loan_count']>0 else np.nan #var最大值比率
                dict_out['loan_second_by12_nowR_month60_State_countNull_max'] = dict_out['loan_second_by12_now_month60_State_countNull_max']/dict_out['loan_second_by12_month60_State_countNull_max'] if dict_out['loan_second_by12_month60_State_countNull_max']>0 else np.nan #var最大值比率
                dict_out['loan_second_by12_nowR_repayMons_ratio_mean'] = dict_out['loan_second_by12_now_repayMons_ratio_mean']/dict_out['loan_second_by12_repayMons_ratio_mean'] if dict_out['loan_second_by12_repayMons_ratio_mean']>0 else np.nan #var最大值比率
                dict_out['loan_second_by12_now_org_commercial_bank_count'] = loan_second_by12_now[loan_second_by12_now['org']=='商业银行'].shape[0] #近12个月截至 现行贷款账户管理机构为商业银行 计数
                dict_out['loan_second_by12_nowR_org_commercial_bank_count'] = dict_out['loan_second_by12_now_org_commercial_bank_count']/dict_out['loan_second_by12_org_commercial_bank_count'] if dict_out['loan_second_by12_org_commercial_bank_count']>0 else np.nan #var最大值比率
                dict_out['loan_second_by12_nowR_month60_State_num_size_range'] = dict_out['loan_second_by12_now_month60_State_num_size_range']/dict_out['loan_second_by12_month60_State_num_size_range'] if dict_out['loan_second_by12_month60_State_num_size_range']>0 else np.nan #var最大值比率
                dict_out['loan_second_by12_nowR_month60_to_report_mean_range'] = dict_out['loan_second_by12_now_month60_to_report_mean_range']/dict_out['loan_second_by12_month60_to_report_mean_range'] if dict_out['loan_second_by12_month60_to_report_mean_range']>0 else np.nan #var最大值比率
                dict_out['loan_second_by12_now_loanGrantOrg_nunique'] = loan_second_by12_now['loanGrantOrg'].nunique() #近12个月截至 现行贷款账户管理机构详细nunique
                dict_out['loan_second_by12_nowR_loanGrantOrg_nunique'] = dict_out['loan_second_by12_now_loanGrantOrg_nunique']/dict_out['loan_second_by12_loanGrantOrg_nunique'] if dict_out['loan_second_by12_loanGrantOrg_nunique']>0 else np.nan #var最大值比率
                dict_out['loan_second_by12_now_org_micro_loan_count'] = loan_second_by12_now[loan_second_by12_now['org']=='小额贷款公司'].shape[0] #近12个月截至 现行贷款账户管理机构为小额贷款公司 计数
                dict_out['loan_second_by12_nowR_org_micro_loan_count'] = dict_out['loan_second_by12_now_org_micro_loan_count']/dict_out['loan_second_by12_org_micro_loan_count'] if dict_out['loan_second_by12_org_micro_loan_count']>0 else np.nan #var最大值比率
                dict_out['loan_second_by12_nowR_startDate_to_report_mean'] = dict_out['loan_second_by12_now_startDate_to_report_mean']/dict_out['loan_second_by12_startDate_to_report_mean'] if dict_out['loan_second_by12_startDate_to_report_mean']>0 else np.nan #var最大值比率
                dict_out['loan_second_by12_nowR_month60_State_countUnknow_mean'] = dict_out['loan_second_by12_now_month60_State_countUnknow_mean']/dict_out['loan_second_by12_month60_State_countUnknow_mean'] if dict_out['loan_second_by12_month60_State_countUnknow_mean']>0 else np.nan #var最大值比率
                dict_out['loan_second_by12_now_class_ncycle_ratio'] = loan_second_by12_now[loan_second_by12_now['class']=='非循环贷账户'].shape[0]/len(loan_second_by12_now) #近12个月截至 现行贷款账户账户类别为非循环贷账户 占比
                dict_out['loan_second_by12_nowR_class_ncycle_ratio'] = dict_out['loan_second_by12_now_class_ncycle_ratio']/dict_out['loan_second_by12_class_ncycle_ratio'] if dict_out['loan_second_by12_class_ncycle_ratio']>0 else np.nan #var最大值比率
                dict_out['loan_second_by12_nowR_repayTerms_sum'] = dict_out['loan_second_by12_now_repayTerms_sum']/dict_out['loan_second_by12_repayTerms_sum'] if dict_out['loan_second_by12_repayTerms_sum']>0 else np.nan #var最大值比率
                dict_out['loan_second_by12_now_repayFrequency_giniimpurity'] = giniimpurity(loan_second_by12_now['repayFrequency']) #近12个月截至 现行贷款账户还款频率基尼不纯度
                dict_out['loan_second_by12_nowR_repayFrequency_giniimpurity'] = dict_out['loan_second_by12_now_repayFrequency_giniimpurity']/dict_out['loan_second_by12_repayFrequency_giniimpurity'] if dict_out['loan_second_by12_repayFrequency_giniimpurity']>0 else np.nan #var最大值比率
                dict_out['loan_second_by12_nowR_repayAmt_range'] = dict_out['loan_second_by12_now_repayAmt_range']/dict_out['loan_second_by12_repayAmt_range'] if dict_out['loan_second_by12_repayAmt_range']>0 else np.nan #var最大值比率
                dict_out['loan_second_by12_nowR_month60_to_report_min_mean'] = dict_out['loan_second_by12_now_month60_to_report_min_mean']/dict_out['loan_second_by12_month60_to_report_min_mean'] if dict_out['loan_second_by12_month60_to_report_min_mean']>0 else np.nan #var最大值比率
                dict_out['loan_second_by12_now_org_consumer_finance_ratio'] = loan_second_by12_now[loan_second_by12_now['org']=='消费金融公司'].shape[0]/len(loan_second_by12_now) #近12个月截至 现行贷款账户管理机构为消费金融公司 占比
                dict_out['loan_second_by12_nowR_org_consumer_finance_ratio'] = dict_out['loan_second_by12_now_org_consumer_finance_ratio']/dict_out['loan_second_by12_org_consumer_finance_ratio'] if dict_out['loan_second_by12_org_consumer_finance_ratio']>0 else np.nan #var最大值比率
                dict_out['loan_second_by12_nowR_month60_State_countNull_sum'] = dict_out['loan_second_by12_now_month60_State_countNull_sum']/dict_out['loan_second_by12_month60_State_countNull_sum'] if dict_out['loan_second_by12_month60_State_countNull_sum']>0 else np.nan #var最大值比率
                dict_out['loan_second_by12_nowR_loanAmount_mean'] = dict_out['loan_second_by12_now_loanAmount_mean']/dict_out['loan_second_by12_loanAmount_mean'] if dict_out['loan_second_by12_loanAmount_mean']>0 else np.nan #var最大值比率
                dict_out['loan_second_by12_now_org_commercial_bank_ratio'] = loan_second_by12_now[loan_second_by12_now['org']=='商业银行'].shape[0]/len(loan_second_by12_now) #近12个月截至 现行贷款账户管理机构为商业银行 占比
                dict_out['loan_second_by12_nowR_org_commercial_bank_ratio'] = dict_out['loan_second_by12_now_org_commercial_bank_ratio']/dict_out['loan_second_by12_org_commercial_bank_ratio'] if dict_out['loan_second_by12_org_commercial_bank_ratio']>0 else np.nan #var最大值比率
                dict_out['loan_second_by12_nowR_repayAmt_sum'] = dict_out['loan_second_by12_now_repayAmt_sum']/dict_out['loan_second_by12_repayAmt_sum'] if dict_out['loan_second_by12_repayAmt_sum']>0 else np.nan #var最大值比率
                dict_out['loan_second_by12_nowR_loanAmount_max'] = dict_out['loan_second_by12_now_loanAmount_max']/dict_out['loan_second_by12_loanAmount_max'] if dict_out['loan_second_by12_loanAmount_max']>0 else np.nan #var最大值比率
                dict_out['loan_second_by12_nowR_repayMons_ratio_min'] = dict_out['loan_second_by12_now_repayMons_ratio_min']/dict_out['loan_second_by12_repayMons_ratio_min'] if dict_out['loan_second_by12_repayMons_ratio_min']>0 else np.nan #var最大值比率
                dict_out['loan_second_by12_nowR_startDate_to_report_range'] = dict_out['loan_second_by12_now_startDate_to_report_range']/dict_out['loan_second_by12_startDate_to_report_range'] if dict_out['loan_second_by12_startDate_to_report_range']>0 else np.nan #var最大值比率
                dict_out['loan_second_by12_nowR_month60_State_num_size_mean'] = dict_out['loan_second_by12_now_month60_State_num_size_mean']/dict_out['loan_second_by12_month60_State_num_size_mean'] if dict_out['loan_second_by12_month60_State_num_size_mean']>0 else np.nan #var最大值比率



            #近12个月截至 非循环贷账户
            loan_second_by12_ncycle = loan_second_by12[loan_second_by12['class']=='非循环贷账户']
            if len(loan_second_by12_ncycle)>0:
                dict_out['loan_second_by12_ncycle_month60_State_countNullr_sum'] = loan_second_by12_ncycle['month60_State_countNullr'].sum() #var求和
                dict_out['loan_second_by12_ncycle_month60_to_report_min_max'] = loan_second_by12_ncycle['month60_to_report_min'].max()#var最大值
                dict_out['loan_second_by12_ncycle_month60_State_countNr_mean'] = loan_second_by12_ncycle['month60_State_countNr'].mean() #var平均值
                dict_out['loan_second_by12_ncycle_balance_min'] = loan_second_by12_ncycle['balance'].min() #var最小值
                dict_out['loan_second_by12_ncycle_month60_State_countN_mean'] = loan_second_by12_ncycle['month60_State_countN'].mean() #var平均值
                dict_out['loan_second_by12_ncycle_repayAmt_mean'] = loan_second_by12_ncycle['repayAmt'].mean() #var平均值
                dict_out['loan_second_by12_ncycle_month60_State_countCr_mean'] = loan_second_by12_ncycle['month60_State_countCr'].mean() #var平均值
                dict_out['loan_second_by12_ncycle_month60_State_countN_sum'] = loan_second_by12_ncycle['month60_State_countN'].sum() #var求和
                dict_out['loan_second_by12_ncycle_month60_State_count1r_mean'] = loan_second_by12_ncycle['month60_State_count1r'].mean() #var平均值
                dict_out['loan_second_by12_ncycle_classify5_giniimpurity'] = giniimpurity(loan_second_by12_ncycle['classify5']) #近12个月截至 非循环贷账户 贷款账户五级分类基尼不纯度
                dict_out['loan_second_by12_ncycle_repayAmt_max'] = loan_second_by12_ncycle['repayAmt'].max()#var最大值
                dict_out['loan_second_by12_ncycle_repayAmt_min'] = loan_second_by12_ncycle['repayAmt'].min() #var最小值
                dict_out['loan_second_by12_ncycle_repayAmt_range'] = (dict_out['loan_second_by12_ncycle_repayAmt_max']-dict_out['loan_second_by12_ncycle_repayAmt_min'])/dict_out['loan_second_by12_ncycle_repayAmt_max'] if dict_out['loan_second_by12_ncycle_repayAmt_max']>0 else np.nan #var区间率
                dict_out['loan_second_by12_ncycle_startDate_to_report_max'] = loan_second_by12_ncycle['startDate_to_report'].max()#var最大值
                dict_out['loan_second_by12_ncycle_month60_to_report_min_sum'] = loan_second_by12_ncycle['month60_to_report_min'].sum() #var求和
                dict_out['loan_second_by12_ncycle_balance_ratio_mean'] = loan_second_by12_ncycle['balance_ratio'].mean() #var平均值
                dict_out['loan_second_by12_ncycle_repayTerm_ratio_max'] = loan_second_by12_ncycle['repayTerm_ratio'].max()#var最大值
                dict_out['loan_second_by12_ncycle_repayTerm_ratio_min'] = loan_second_by12_ncycle['repayTerm_ratio'].min() #var最小值
                dict_out['loan_second_by12_ncycle_repayTerm_ratio_range'] = (dict_out['loan_second_by12_ncycle_repayTerm_ratio_max']-dict_out['loan_second_by12_ncycle_repayTerm_ratio_min'])/dict_out['loan_second_by12_ncycle_repayTerm_ratio_max'] if dict_out['loan_second_by12_ncycle_repayTerm_ratio_max']>0 else np.nan #var区间率
                dict_out['loan_second_by12_ncycle_balance_ratio_max'] = loan_second_by12_ncycle['balance_ratio'].max()#var最大值
                dict_out['loan_second_by12_ncycle_rf_month_count'] = loan_second_by12_ncycle[loan_second_by12_ncycle['repayFrequency']=='月'].shape[0] #近12个月截至 非循环贷账户 贷款账户还款频率为月 计数
                dict_out['loan_second_by12_ncycle_repayTerm_ratio_mean'] = loan_second_by12_ncycle['repayTerm_ratio'].mean() #var平均值
                dict_out['loan_second_by12_ncycle_month60_to_report_mean_max'] = loan_second_by12_ncycle['month60_to_report_mean'].max()#var最大值
                dict_out['loan_second_by12_ncycle_repayMons_ratio_mean'] = loan_second_by12_ncycle['repayMons_ratio'].mean() #var平均值
                dict_out['loan_second_by12_ncycle_byDate_to_report_max'] = loan_second_by12_ncycle['byDate_to_report'].max()#var最大值
                dict_out['loan_second_by12_ncycle_month60_to_report_max_mean'] = loan_second_by12_ncycle['month60_to_report_max'].mean() #var平均值
                dict_out['loan_second_by12_ncycle_org_commercial_bank_ratio'] = loan_second_by12_ncycle[loan_second_by12_ncycle['org']=='商业银行'].shape[0]/len(loan_second_by12_ncycle) #近12个月截至 非循环贷账户 贷款账户管理机构为商业银行 占比
                dict_out['loan_second_by12_ncycle_month60_State_num_sum_mean'] = loan_second_by12_ncycle['month60_State_num_sum'].mean() #var平均值
                dict_out['loan_second_by12_ncycle_repayedAmount_sum'] = loan_second_by12_ncycle['repayedAmount'].sum() #var求和
                dict_out['loan_second_by12_ncycle_startDate_to_report_max'] = loan_second_by12_ncycle['startDate_to_report'].max()#var最大值
                dict_out['loan_second_by12_ncycle_startDate_to_report_min'] = loan_second_by12_ncycle['startDate_to_report'].min() #var最小值
                dict_out['loan_second_by12_ncycle_startDate_to_report_range'] = (dict_out['loan_second_by12_ncycle_startDate_to_report_max']-dict_out['loan_second_by12_ncycle_startDate_to_report_min'])/dict_out['loan_second_by12_ncycle_startDate_to_report_max'] if dict_out['loan_second_by12_ncycle_startDate_to_report_max']>0 else np.nan #var区间率
                dict_out['loan_second_by12_ncycle_class_ncycle_count'] = loan_second_by12_ncycle[loan_second_by12_ncycle['class']=='非循环贷账户'].shape[0] #近12个月截至 非循环贷账户 贷款账户账户类别为非循环贷账户 计数
                dict_out['loan_second_by12_ncycle_leftRepayTerms_mean'] = loan_second_by12_ncycle['leftRepayTerms'].mean() #var平均值
                dict_out['loan_second_by12_ncycle_repayTerms_mean'] = loan_second_by12_ncycle['repayTerms'].mean() #var平均值
                dict_out['loan_second_by12_ncycle_gf_combine_nowarranty_ratio'] = loan_second_by12_ncycle[loan_second_by12_ncycle['guaranteeForm']=='组合（不含保证）'].shape[0]/len(loan_second_by12_ncycle) #近12个月截至 非循环贷账户 贷款账户担保方式为组合（不含保证） 占比
                dict_out['loan_second_by12_ncycle_byDate_to_report_sum'] = loan_second_by12_ncycle['byDate_to_report'].sum() #var求和
                dict_out['loan_second_by12_ncycle_guaranteeForm_giniimpurity'] = giniimpurity(loan_second_by12_ncycle['guaranteeForm']) #近12个月截至 非循环贷账户 贷款账户担保方式基尼不纯度
                dict_out['loan_second_by12_ncycle_month60_State_num_mean_mean'] = loan_second_by12_ncycle['month60_State_num_mean'].mean() #var平均值
                dict_out['loan_second_by12_ncycle_startDate_to_report_mean'] = loan_second_by12_ncycle['startDate_to_report'].mean() #var平均值
                dict_out['loan_second_by12_ncycle_org_micro_loan_count'] = loan_second_by12_ncycle[loan_second_by12_ncycle['org']=='小额贷款公司'].shape[0] #近12个月截至 非循环贷账户 贷款账户管理机构为小额贷款公司 计数
                dict_out['loan_second_by12_ncycle_month60_State_count1_mean'] = loan_second_by12_ncycle['month60_State_count1'].mean() #var平均值
                dict_out['loan_second_by12_ncycle_org_myself_count'] = loan_second_by12_ncycle[loan_second_by12_ncycle['org']==dict_in['cc_rh_report'].queryOperator.values[0]].shape[0] #近12个月截至 非循环贷账户 贷款账户管理机构为本机构 计数
                dict_out['loan_second_by12_ncycle_org_micro_loan_ratio'] = loan_second_by12_ncycle[loan_second_by12_ncycle['org']=='小额贷款公司'].shape[0]/len(loan_second_by12_ncycle) #近12个月截至 非循环贷账户 贷款账户管理机构为小额贷款公司 占比
                dict_out['loan_second_by12_ncycle_org_trust_company_ratio'] = loan_second_by12_ncycle[loan_second_by12_ncycle['org']=='信托公司'].shape[0]/len(loan_second_by12_ncycle) #近12个月截至 非循环贷账户 贷款账户管理机构为信托公司 占比
                dict_out['loan_second_by12_ncycle_balance_ratio_min'] = loan_second_by12_ncycle['balance_ratio'].min() #var最小值
                dict_out['loan_second_by12_ncycle_month60_Amount_num_mean_mean'] = loan_second_by12_ncycle['month60_Amount_num_mean'].mean() #var平均值
                dict_out['loan_second_by12_ncycle_bt_other_person_count'] = loan_second_by12_ncycle[loan_second_by12_ncycle['businessType']=='其他个人消费贷款'].shape[0] #近12个月截至 非循环贷账户 贷款账户业务种类为其他个人消费贷款 计数
                dict_out['loan_second_by12_ncycle_repayMons_ratio_max'] = loan_second_by12_ncycle['repayMons_ratio'].max()#var最大值
                dict_out['loan_second_by12_ncycle_repayMons_ratio_min'] = loan_second_by12_ncycle['repayMons_ratio'].min() #var最小值
                dict_out['loan_second_by12_ncycle_repayMons_ratio_range'] = (dict_out['loan_second_by12_ncycle_repayMons_ratio_max']-dict_out['loan_second_by12_ncycle_repayMons_ratio_min'])/dict_out['loan_second_by12_ncycle_repayMons_ratio_max'] if dict_out['loan_second_by12_ncycle_repayMons_ratio_max']>0 else np.nan #var区间率
                dict_out['loan_second_by12_ncycle_rf_other_ratio'] = loan_second_by12_ncycle[loan_second_by12_ncycle['repayFrequency']=='其他'].shape[0]/len(loan_second_by12_ncycle) #近12个月截至 非循环贷账户 贷款账户还款频率为其他 占比
                dict_out['loan_second_by12_ncycle_month60_State_countUnknow_max'] = loan_second_by12_ncycle['month60_State_countUnknow'].max()#var最大值
                dict_out['loan_second_by12_ncycle_month60_State_num_size_mean'] = loan_second_by12_ncycle['month60_State_num_size'].mean() #var平均值
                dict_out['loan_second_by12_ncycle_month60_to_report_mean_sum'] = loan_second_by12_ncycle['month60_to_report_mean'].sum() #var求和
                dict_out['loan_second_by12_ncycle_repayTerm_ratio_min'] = loan_second_by12_ncycle['repayTerm_ratio'].min() #var最小值
                dict_out['loan_second_by12_ncycle_month60_State_countNr_sum'] = loan_second_by12_ncycle['month60_State_countNr'].sum() #var求和
                dict_out['loan_second_by12_ncycle_org_consumer_finance_count'] = loan_second_by12_ncycle[loan_second_by12_ncycle['org']=='消费金融公司'].shape[0] #近12个月截至 非循环贷账户 贷款账户管理机构为消费金融公司 计数
                dict_out['loan_second_by12_ncycle_rf_month_ratio'] = loan_second_by12_ncycle[loan_second_by12_ncycle['repayFrequency']=='月'].shape[0]/len(loan_second_by12_ncycle) #近12个月截至 非循环贷账户 贷款账户还款频率为月 占比
                dict_out['loan_second_by12_ncycle_bt_other_person_ratio'] = loan_second_by12_ncycle[loan_second_by12_ncycle['businessType']=='其他个人消费贷款'].shape[0]/len(loan_second_by12_ncycle) #近12个月截至 非循环贷账户 贷款账户业务种类为其他个人消费贷款 占比
                dict_out['loan_second_by12_ncycle_loanAmount_max'] = loan_second_by12_ncycle['loanAmount'].max()#var最大值
                dict_out['loan_second_by12_ncycle_loanAmount_min'] = loan_second_by12_ncycle['loanAmount'].min() #var最小值
                dict_out['loan_second_by12_ncycle_loanAmount_range'] = (dict_out['loan_second_by12_ncycle_loanAmount_max']-dict_out['loan_second_by12_ncycle_loanAmount_min'])/dict_out['loan_second_by12_ncycle_loanAmount_max'] if dict_out['loan_second_by12_ncycle_loanAmount_max']>0 else np.nan #var区间率
                dict_out['loan_second_by12_ncycle_loanGrantOrg_giniimpurity'] = giniimpurity(loan_second_by12_ncycle['loanGrantOrg']) #近12个月截至 非循环贷账户 贷款账户管理机构详细基尼不纯度
                dict_out['loan_second_by12_ncycle_org_giniimpurity'] = giniimpurity(loan_second_by12_ncycle['org']) #近12个月截至 非循环贷账户 贷款账户管理机构基尼不纯度
                dict_out['loan_second_by12_ncycle_month60_State_countUnknowr_max'] = loan_second_by12_ncycle['month60_State_countUnknowr'].max() #var最大值比率
                dict_out['loan_second_by12_ncycle_month60_to_report_mean_mean'] = loan_second_by12_ncycle['month60_to_report_mean'].mean()  #var平均值比率
                dict_out['loan_second_by12_ncycle_month60_State_countNull_sum'] = loan_second_by12_ncycle['month60_State_countNull'].sum() #var求和比率
                dict_out['loan_second_by12_ncycle_org_giniimpurity'] = giniimpurity(loan_second_by12_ncycle['org']) #近12个月截至 非循环贷账户 贷款账户管理机构基尼不纯度
                dict_out['loan_second_by12_ncycleR_org_giniimpurity'] = dict_out['loan_second_by12_ncycle_org_giniimpurity']/dict_out['loan_second_by12_org_giniimpurity'] if dict_out['loan_second_by12_org_giniimpurity']>0 else np.nan #var最大值比率
                dict_out['loan_second_by12_ncycleR_month60_State_countUnknowr_max'] = dict_out['loan_second_by12_ncycle_month60_State_countUnknowr_max']/dict_out['loan_second_by12_month60_State_countUnknowr_max'] if dict_out['loan_second_by12_month60_State_countUnknowr_max']>0 else np.nan #var最大值比率
                dict_out['loan_second_by12_ncycleR_month60_to_report_mean_mean'] = dict_out['loan_second_by12_ncycle_month60_to_report_mean_mean']/dict_out['loan_second_by12_month60_to_report_mean_mean'] if dict_out['loan_second_by12_month60_to_report_mean_mean']>0 else np.nan #var最大值比率
                dict_out['loan_second_by12_ncycleR_month60_State_countNull_sum'] = dict_out['loan_second_by12_ncycle_month60_State_countNull_sum']/dict_out['loan_second_by12_month60_State_countNull_sum'] if dict_out['loan_second_by12_month60_State_countNull_sum']>0 else np.nan #var最大值比率

            #近12个月截至 循环贷账户
            loan_second_by12_cycle = loan_second_by12[loan_second_by12['class']=='循环贷账户']
            

            #近12个月截至 有担保 is_vouch 
            loan_second_by12_vouch = loan_second_by12[loan_second_by12['is_vouch']==1]
            if len(loan_second_by12_vouch)>0:
                dict_out['loan_second_by12_vouch_org_micro_loan_ratio'] = loan_second_by12_vouch[loan_second_by12_vouch['org']=='小额贷款公司'].shape[0]/len(loan_second_by12_vouch) #近12个月截至 有担保贷款账户管理机构为小额贷款公司 占比
                dict_out['loan_second_by12_vouch_loanAmount_min'] = loan_second_by12_vouch['loanAmount'].min() #var最小值
                dict_out['loan_second_by12_vouch_repayAmt_min'] = loan_second_by12_vouch['repayAmt'].min() #var最小值
                dict_out['loan_second_by12_vouch_org_micro_loan_ratio'] = loan_second_by12_vouch[loan_second_by12_vouch['org']=='小额贷款公司'].shape[0]/len(loan_second_by12_vouch) #近12个月截至 有担保贷款账户管理机构为小额贷款公司 占比
                dict_out['loan_second_by12_vouch_loanGrantOrg_nunique'] = loan_second_by12_vouch['loanGrantOrg'].nunique() #近12个月截至 有担保贷款账户管理机构详细nunique
                dict_out['loan_second_by12_vouch_month60_to_report_mean_sum'] = loan_second_by12_vouch['month60_to_report_mean'].sum() #var求和比率
                dict_out['loan_second_by12_vouch_month60_State_countNr_sum'] = loan_second_by12_vouch['month60_State_countNr'].sum() #var求和比率
                dict_out['loan_second_by12_vouch_month60_State_countNullr_sum'] = loan_second_by12_vouch['month60_State_countNullr'].sum() #var求和比率
                dict_out['loan_second_by12_vouch_repayMons_ratio_mean'] = loan_second_by12_vouch['repayMons_ratio'].mean()  #var平均值比率
                dict_out['loan_second_by12_vouch_org_micro_loan_ratio'] = loan_second_by12_vouch[loan_second_by12_vouch['org']=='小额贷款公司'].shape[0]/len(loan_second_by12_vouch) #近12个月截至 有担保贷款账户管理机构为小额贷款公司 占比
                dict_out['loan_second_by12_vouchR_org_micro_loan_ratio'] = dict_out['loan_second_by12_vouch_org_micro_loan_ratio']/dict_out['loan_second_by12_org_micro_loan_ratio'] if dict_out['loan_second_by12_org_micro_loan_ratio']>0 else np.nan #var最大值比率
                dict_out['loan_second_by12_vouchR_month60_to_report_mean_sum'] = dict_out['loan_second_by12_vouch_month60_to_report_mean_sum']/dict_out['loan_second_by12_month60_to_report_mean_sum'] if dict_out['loan_second_by12_month60_to_report_mean_sum']>0 else np.nan #var最大值比率
                dict_out['loan_second_by12_vouchR_month60_State_countNr_sum'] = dict_out['loan_second_by12_vouch_month60_State_countNr_sum']/dict_out['loan_second_by12_month60_State_countNr_sum'] if dict_out['loan_second_by12_month60_State_countNr_sum']>0 else np.nan #var最大值比率
                dict_out['loan_second_by12_vouchR_month60_State_countNullr_sum'] = dict_out['loan_second_by12_vouch_month60_State_countNullr_sum']/dict_out['loan_second_by12_month60_State_countNullr_sum'] if dict_out['loan_second_by12_month60_State_countNullr_sum']>0 else np.nan #var最大值比率
                dict_out['loan_second_by12_vouchR_repayMons_ratio_mean'] = dict_out['loan_second_by12_vouch_repayMons_ratio_mean']/dict_out['loan_second_by12_repayMons_ratio_mean'] if dict_out['loan_second_by12_repayMons_ratio_mean']>0 else np.nan #var最大值比率
                dict_out['loan_second_by12_vouch_loanGrantOrg_nunique'] = loan_second_by12_vouch['loanGrantOrg'].nunique() #近12个月截至 有担保贷款账户管理机构详细nunique
                dict_out['loan_second_by12_vouchR_loanGrantOrg_nunique'] = dict_out['loan_second_by12_vouch_loanGrantOrg_nunique']/dict_out['loan_second_by12_loanGrantOrg_nunique'] if dict_out['loan_second_by12_loanGrantOrg_nunique']>0 else np.nan #var最大值比率

            #近12个月截至 历史逾期
            loan_second_by12_hdue1 = loan_second_by12[loan_second_by12['is_overdue']==1]
            if len(loan_second_by12_hdue1)>0:
                dict_out['loan_second_by12_hdue1_month60_State_countNr_mean'] = loan_second_by12_hdue1['month60_State_countNr'].mean() #var平均值
                dict_out['loan_second_by12_hdue1_loanGrantOrg_nunique'] = loan_second_by12_hdue1['loanGrantOrg'].nunique() #近12个月截至 历史逾期贷款账户管理机构详细nunique
                dict_out['loan_second_by12_hdue1_bt_other_person_count'] = loan_second_by12_hdue1[loan_second_by12_hdue1['businessType']=='其他个人消费贷款'].shape[0] #近12个月截至 历史逾期贷款账户业务种类为其他个人消费贷款 计数
                dict_out['loan_second_by12_hdue1_month60_State_countNullr_sum'] = loan_second_by12_hdue1['month60_State_countNullr'].sum() #var求和比率
                dict_out['loan_second_by12_hdue1_month60_to_report_mean_min'] = loan_second_by12_hdue1['month60_to_report_mean'].min() #var最小值比率
                dict_out['loan_second_by12_hdue1_repayAmt_sum'] = loan_second_by12_hdue1['repayAmt'].sum() #var求和比率
                dict_out['loan_second_by12_hdue1_month60_State_num_max_mean'] = loan_second_by12_hdue1['month60_State_num_max'].mean()  #var平均值比率
                dict_out['loan_second_by12_hdue1_repayMons_ratio_sum'] = loan_second_by12_hdue1['repayMons_ratio'].sum() #var求和比率
                dict_out['loan_second_by12_hdue1_loanGrantOrg_nunique'] = loan_second_by12_hdue1['loanGrantOrg'].nunique() #近12个月截至 历史逾期贷款账户管理机构详细nunique
                dict_out['loan_second_by12_hdue1R_loanGrantOrg_nunique'] = dict_out['loan_second_by12_hdue1_loanGrantOrg_nunique']/dict_out['loan_second_by12_loanGrantOrg_nunique'] if dict_out['loan_second_by12_loanGrantOrg_nunique']>0 else np.nan #var最大值比率
                dict_out['loan_second_by12_hdue1R_month60_State_countNullr_sum'] = dict_out['loan_second_by12_hdue1_month60_State_countNullr_sum']/dict_out['loan_second_by12_month60_State_countNullr_sum'] if dict_out['loan_second_by12_month60_State_countNullr_sum']>0 else np.nan #var最大值比率
                dict_out['loan_second_by12_hdue1R_month60_to_report_mean_min'] = dict_out['loan_second_by12_hdue1_month60_to_report_mean_min']/dict_out['loan_second_by12_month60_to_report_mean_min'] if dict_out['loan_second_by12_month60_to_report_mean_min']>0 else np.nan #var最大值比率
                dict_out['loan_second_by12_hdue1R_repayAmt_sum'] = dict_out['loan_second_by12_hdue1_repayAmt_sum']/dict_out['loan_second_by12_repayAmt_sum'] if dict_out['loan_second_by12_repayAmt_sum']>0 else np.nan #var最大值比率
                dict_out['loan_second_by12_hdue1R_month60_State_num_max_mean'] = dict_out['loan_second_by12_hdue1_month60_State_num_max_mean']/dict_out['loan_second_by12_month60_State_num_max_mean'] if dict_out['loan_second_by12_month60_State_num_max_mean']>0 else np.nan #var最大值比率
                dict_out['loan_second_by12_hdue1_bt_other_person_count'] = loan_second_by12_hdue1[loan_second_by12_hdue1['businessType']=='其他个人消费贷款'].shape[0] #近12个月截至 历史逾期贷款账户业务种类为其他个人消费贷款 计数
                dict_out['loan_second_by12_hdue1R_bt_other_person_count'] = dict_out['loan_second_by12_hdue1_bt_other_person_count']/dict_out['loan_second_by12_bt_other_person_count'] if dict_out['loan_second_by12_bt_other_person_count']>0 else np.nan #var最大值比率
                dict_out['loan_second_by12_hdue1R_repayMons_ratio_sum'] = dict_out['loan_second_by12_hdue1_repayMons_ratio_sum']/dict_out['loan_second_by12_repayMons_ratio_sum'] if dict_out['loan_second_by12_repayMons_ratio_sum']>0 else np.nan #var最大值比率

            #近12个月截至 历史严重逾期
            loan_second_by12_hdue3 = loan_second_by12[loan_second_by12['due_class']==3]


            #近12个月截至 当前逾期
            loan_second_by12_cdue = loan_second_by12[loan_second_by12['accountStatus']=='逾期']




    #二代征信-贷记卡信息
    card_second=copy.copy(dict_in['cc_rh_report_detail_debit_card_second'])
    c_month60=pd.DataFrame()
    if len(card_second)>0: 
        card_second['startDate_to_report']=card_second.apply(lambda x:monthdelta(x['cardGrantDate'],dict_in['cc_rh_report']['reportTime']),axis=1)
        card_second['byDate_to_report']=card_second.apply(lambda x:monthdelta(x['byDate'],dict_in['cc_rh_report']['reportTime']),axis=1)
        card_second['type']='card'
        card_second['class']=card_second.apply(lambda x:'贷记卡' if x.businessType=='贷记卡' else '准贷记卡',axis=1 )
        #现行（最近一次月度表现）
        card_second['is_now']=card_second.apply(lambda x:1 if ((x['accountStatus']=='正常') ) else 0,axis=1)
        card_second['usedAmount']=pd.to_numeric(card_second.usedAmount,errors='coerce')
        card_second['installmentBalance']=pd.to_numeric(card_second.installmentBalance,errors='coerce')

        card_second['balance']=pd.to_numeric(card_second.balance,errors='coerce')
        card_second['planRepayAmount']=pd.to_numeric(card_second.planRepayAmount,errors='coerce')
        card_second['repayedAmount']=pd.to_numeric(card_second.repayedAmount,errors='coerce')
        card_second['currentOverdueTerms']=pd.to_numeric(card_second.currentOverdueTerms,errors='coerce')
        card_second['currentOverdueAmount']=pd.to_numeric(card_second.currentOverdueAmount,errors='coerce')
        card_second['amount']=pd.to_numeric(card_second.amount,errors='coerce')
        card_second['remainingTerms']=pd.to_numeric(card_second.remainingTerms,errors='coerce')

        # 近60个月还款记录辅助表
        import pandas as pd
        import datetime, time
        from dateutil.relativedelta import relativedelta

        def getMonList(month60Desc):
            date_list = []
            if len(month60Desc) > 5:
                begin_end_date = re.findall('[0-9]{4}年[0-9]{2}月', month60Desc)
                begin_date = datetime.datetime.strptime(begin_end_date[0], '%Y年%m月')
                end_date = datetime.datetime.strptime(begin_end_date[1], '%Y年%m月')
                while begin_date <= end_date:
                    date_str = begin_date.strftime("%Y.%m")
                    date_list.append(date_str)
                    begin_date += relativedelta(months=1)
            return date_list

        # print(getMonList('2019年10月 —2020年01月的还款记录'))
        month60Amount = card_second.set_index(['id', 'reportId'])['month60Amount'].str.split('/', expand=True).stack().reset_index(level=2, drop=True).rename('month60_Amount').reset_index(drop=False)
        month60State = card_second.set_index(['id', 'reportId'])['month60State'].astype(str).apply(lambda x: '/'.join(list(x))).str.split('/', expand=True).stack().reset_index(level=2, drop=True).rename('month60_State').reset_index(drop=False)
        month60Desc = card_second.set_index(['id', 'reportId'])['month60Desc'].astype(str).apply(lambda x: '/'.join(getMonList(x))).str.split('/', expand=True).stack().reset_index(level=2, drop=True).rename('month60_Desc').reset_index(drop=False)
        month60Amount['rank'] = month60Amount['id'].groupby(month60Amount['id']).rank(method='first')
        month60State['rank'] = month60State['id'].groupby(month60State['id']).rank(method='first')
        month60Desc['rank'] = month60Desc['id'].groupby(month60Desc['id']).rank(method='first')
        month60 = pd.merge(month60Desc, month60State, how='inner', on=['id', 'rank', 'reportId'])
        month60 = pd.merge(month60, month60Amount, how='inner', on=['id', 'rank', 'reportId'])
        month60['month60_to_report'] = month60.apply(lambda x: monthdelta(x['month60_Desc'], dict_in['cc_rh_report']['reportTime']), axis=1)
        month60['month60_State_num'] = month60.apply(lambda x: (int(re.sub('[^1-7]', '0', x['month60_State'])) if re.sub('[^1-7]', '0', x['month60_State']) != '' else np.nan), axis=1)
        month60['month60_Amount_num'] = month60['month60_Amount'].apply(lambda x: float(re.sub('[^0-9\.]', '', x)) if len(re.sub('[^0-9\.]', '', x)) > 0 else 0)

        def countN(x):
            return sum(x == 'N')

        def countNr(x):
            return sum(x == 'N') / len(x)

        def countC(x):
            return sum(x == 'C')

        def countCr(x):
            return sum(x == 'C') / len(x)

        def countD(x):
            return sum(x == 'D')

        def countDr(x):
            return sum(x == 'D') / len(x)

        def countG(x):
            return sum(x == 'G')

        def countGr(x):
            return sum(x == 'G') / len(x)

        def countNull(x):
            return sum(x == '*')

        def countNullr(x):
            return sum(x == '*') / len(x)

        def countUnknow(x):
            return sum(x == '#') + sum(x == '')

        def countUnknowr(x):
            return (sum(x == '#') + sum(x == '')) / len(x)

        def count0(x):
            return sum(x == '0')

        def count0r(x):
            return sum(x == '0') / len(x)

        def count1(x):
            return sum(x == '1')

        def count1r(x):
            return sum(x == '1') / len(x)

        def count2(x):
            return sum(x == '2')

        def count2r(x):
            return sum(x == '2') / len(x)

        def count3(x):
            return sum(x == '3')

        def count3r(x):
            return sum(x == '3') / len(x)

        def count4(x):
            return sum(x == '4')

        def count4r(x):
            return sum(x == '4') / len(x)

        def count5(x):
            return sum(x == '5')

        def count5r(x):
            return sum(x == '5') / len(x)

        def count6(x):
            return sum(x == '6')

        def count6r(x):
            return sum(x == '6') / len(x)

        def count7(x):
            return sum(x == '7')

        def count7r(x):
            return sum(x == '7') / len(x)

        def meanbig0(x):
            return sum(x) / sum(x > 0) if sum(x > 0) > 0 else np.nan

        month60df = month60.groupby(['id', 'reportId']).agg({'month60_to_report': ['min', 'mean', 'max'], 'month60_State_num': ['size', 'sum', 'max', 'mean', meanbig0], 'month60_Amount_num': ['sum', 'max', 'mean', meanbig0],
                                                             'month60_State': [countN, countNr, countC, countCr, countD, countDr, countG, countGr, countNull, countNullr, countUnknow, countUnknowr, count0, count0r, count1, count1r, count2, count2r, count3, count3r, count4, count4r, count5, count5r,
                                                                               count6, count6r, count7, count7r]})
        month60df.columns = [i[0] + '_' + i[1] for i in month60df.columns]
        card_second = pd.merge(card_second, month60df, on=['id', 'reportId'], how='left')
        # card_second['repayMons_ratio'] = card_second.apply(lambda x: x.month60_State_num_size / x.repayMons if x.repayMons > 0 else np.nan, axis=1)
        c_month60 = copy.copy(month60)

        card_second_now=card_second[card_second.is_now==1]


        card_second_m12=card_second[card_second.byDate_to_report<12]
        if len(card_second_m12)>0:

            dict_out['card_second_m12_sumUsed_vs_sumAmount'] = card_second_m12.usedAmount.sum()/card_second_m12.amount.sum() if card_second_m12.amount.sum()>0 else np.nan #近12个月总已使用信用额度占总信用额度比例

        card_second_m06=card_second[card_second.byDate_to_report<6]
        if len(card_second_m06)>0:
            dict_out['card_second_m06_maxUsed_vs_sumAmount'] = card_second_m12.usedAmount.max()/card_second_m12.amount.sum() if card_second_m12.amount.sum()>0 else np.nan #近12个月最大已使用信用额度占总信用额度比例


        # 全真
        if True:
            df=dict_in['cc_rh_report_detail_debit_card_second']
            accountNum=len(df[df.businessType != '']) #贷记卡数量
            semiAccountNum=len(df[df.businessType == '']) #准贷记卡数量



            month60State=dict_in['cc_rh_report_detail_debit_card_second']['month60State']
            month60StateStr=''
            for i in month60State:
                month60StateStr=month60StateStr+str(i)

            def str_count_one(strs:str, find_str:str):
                return strs.count(find_str)



            # 最新表现账户类型为贷记卡账户，还款状态为正常还款，分布所占比例
            dict_out['debit_card_repayment_normal_ratio'] = str_count_one(month60StateStr, 'N')/len(month60StateStr) if len(month60StateStr)>0 else 0

            # 最新表现账户类型为贷记卡账户，还款状态为账单日调整，当月不出单  ，分布计数
            dict_out['debit_card_repayment_billday_count'] = str_count_one(month60StateStr, '*')



            # 最新表现账户类型为贷记卡账户，还款状态为逾期1 - 30天  ，分布计数
            dict_out['debit_card_repayment_m1_count'] = str_count_one(month60StateStr, '1')



            semimonth60State = df['month60State'][df.businessType == '']
            semimonth60StateStr = ''
            for i in semimonth60State:
                semimonth60StateStr = semimonth60StateStr + str(i)


            balance_normal = df[df.accountStatus == '正常']['balance'].tolist()  ##余额
            planRepayAmount_normal = df[df.accountStatus == '正常']['planRepayAmount'].tolist()  ##本月应还款
            repayedAmount_normal = df[df.accountStatus == '正常']['repayedAmount'].tolist()  ##本月实还款


            ##当前逾期期数最大值
            def currentoverdue_max(state):
                co_max = 0
                for i in state:
                    cc = i[-1:]
                    if cc in [1, 2, 3, 4, 5, 6, 7]:
                        if int(cc) > co_max:
                            co_max = cc
                return co_max

            ##当前逾期期数总和
            def currentoverdue_sum(state):
                co_count = 0
                for i in state:
                    cc = i[-1:]
                    if cc in [1, 2, 3, 4, 5, 6, 7]:
                        co_count += int(cc)
                return co_count

            ##当前逾期期数平均值
            def currentoverdue_avg(state):
                co_count = 0
                for i in state:
                    cc = i[-1:]
                    if cc in [1, 2, 3, 4, 5, 6, 7]:
                        co_count += int(cc)
                return co_count/len(state) if len(state)>0 else 0

            ##当前逾期期数分布计数
            def currentoverdue_count(state):
                co_count = 0
                for i in state:
                    cc = i[-1:]
                    if cc in [1, 2, 3, 4, 5, 6, 7]:
                        co_count += 1
                return co_count



            ##分布计数
            def cal_count(state):
                return len(state)
            ##最大值
            def cal_max(state):
                return max(state) if len(state)>0 else 0

            ##总和
            def cal_sum(state):
                return sum(state)

            ##平均值
            def cal_avg(state):
                return sum(state) / len(state) if len(state)>0 else 0

            ##逾期31—60天未还本金 1 分布计数 2 最大值 3 总和 4 平均值
            def cal_m2_amount(state, status):
                amount = 0
                for i in state:
                    i = i.replace('--', '0')
                    i = i.replace('N', '0')
                    cc = i.split('/')
                    if '*' in list(cc):
                        return np.nan
                    else:
                        cc = int(cc[-2:-1][0].replace(',', '')) if len(cc) >= 2 else 0
                    if status == 1 and cc > 0:
                        amount += 1
                    elif status == 2:
                        if cc > amount:
                            amount = cc
                    elif status == 3:
                        amount += cc
                    elif status == 4:
                        amount = cal_m2_amount(state, 3) / cal_m2_amount(state, 1) if cal_m2_amount(state, 1) > 0 else 0
                return amount

            ##逾期61－90天未还本金 1 分布计数 2 最大值 3 总和 4 平均值
            def cal_m3_amount(state, status):
                amount = 0
                for i in state:
                    i = i.replace('--', '0')
                    i = i.replace('N', '0')
                    cc = i.split('/')
                    if '*' in list(cc):
                        return np.nan
                    else:
                        cc = int(cc[-3:-2][0].replace(',', '')) if len(cc) >= 3 else 0
                    if status == 1:
                        amount += 1
                    elif status == 2:
                        if cc > amount:
                            amount = cc
                    elif status == 3:
                        amount += cc
                    elif status == 4:
                        amount = cal_m3_amount(state, 3) / cal_m3_amount(state, 1) if cal_m3_amount(state, 1) > 0 else 0
                return amount

            ##逾期91－180天未还本金 1 分布计数 2 最大值 3 总和 4 平均值
            def cal_m456_amount(state, status):
                amount = 0
                for i in state:
                    i = i.replace('--', '0')
                    i = i.replace('N', '0')
                    cc = i.split('/')
                    if '*' in list(cc):
                        return np.nan
                    else:
                        cc1 = int(cc[-4:-3][0].replace(',', '')) if len(cc) >= 4 else 0
                        cc2 = int(cc[-5:-4][0].replace(',', '')) if len(cc) >= 5 else 0
                        cc3 = int(cc[-6:-5][0].replace(',', '')) if len(cc) >= 6 else 0
                        cc4 = cc1 + cc2 + cc3
                    if status == 1:
                        amount += 1
                    elif status == 2:
                        if cc4 > amount:
                            amount = cc4
                    elif status == 3:
                        amount += cc4
                    elif status == 4:
                        amount = cal_m456_amount(state, 3) / cal_m456_amount(state, 1) if cal_m456_amount(state, 1) > 0 else 0
                return amount

            ##逾期180天以上未还本金 1 分布计数 2 最大值 3 总和 4 平均值
            def cal_m7_amount(state, status):
                amount = 0
                for i in state:
                    i = i.replace('--', '0')
                    i = i.replace('N', '0')
                    cc = i.split('/')
                    if '*' in list(cc):
                        return np.nan
                    else:
                        cc = int(cc[-7:-6][0].replace(',', '')) if len(cc) >= 7 else 0
                    amount = cc
                    if status == 1:
                        amount += 1
                    elif status == 2:
                        if cc > amount:
                            amount = cc
                    elif status == 3:
                        amount += cc
                    elif status == 4:
                        amount = cal_m7_amount(state, 3) / cal_m7_amount(state, 1) if cal_m7_amount(state, 1) > 0 else 0
                return amount


            dict_out['debit_card_lastmonth_normal_balance_avg'] = cal_avg(balance_normal)

            dict_out['debit_card_lastmonth_normal_planRepayAmount_count'] = cal_avg(planRepayAmount_normal)

            dict_out['debit_card_lastmonth_normal_repayedAmount_count'] = cal_avg(repayedAmount_normal)


            def state_cal(month60State,status):
                for i in month60State:
                    return str_count_one(i,status)
                return 0
            def state_cal_count(month60State):
                str_len=0
                for i in month60State:
                    str_len+=len(i)
                return str_len

            five_years_month60State = df['month60State'].tolist()
            five_years_month60State_semi = df[df.businessType == '']['month60State'].tolist()
            # 最近5年内历史表现，贷记卡账户还款状态为正常还款，分布计数
            dict_out['debit_card_five_years_normal_count']=state_cal(five_years_month60State,'N')
            # 最近5年内历史表现，贷记卡账户还款状态为正常还款，分布所占比例
            dict_out['debit_card_five_years_normal_ratio'] = state_cal(five_years_month60State, 'N')/state_cal_count(five_years_month60State) if state_cal_count(five_years_month60State)>0 else 0



    # todo:
    #贷款和贷记卡    
    same_vars=['is_now','class','type','startDate_to_report','byDate_to_report','reportId','accountLogo','currency','businessType','guaranteeForm','accountStatus','byDate','recentRepayDate','balance','planRepayAmount','repayedAmount','currentOverdueTerms','currentOverdueAmount','month60State','month60Amount','month60Desc','orgExplain','orgExplainDate','selfDeclare','selfDeclareDate','dissentTagging','dissentTaggingDate','specialTagging','specailTaggingDate']
    loan_second_c=copy.copy(loan_second[same_vars+['loanGrantOrg','startDate','loanAmount','leftRepayTerms','planRepayDate']]).rename(columns={'loanGrantOrg':'grantOrg','loanAmount':'amount','leftRepayTerms':'remainingTerms'}) if len(loan_second)>0 else pd.DataFrame()
    card_second_c=copy.copy(card_second[same_vars+['grantOrg','cardGrantDate','amount','remainingTerms','statementDate']]).rename(columns={'cardGrantDate':'startDate','statementDate':'planRepayDate'}) if len(card_second)>0 else pd.DataFrame()
    loan_card=pd.concat([loan_second_c,card_second_c])
    if len(loan_card)>0:        
        dict_out['loan_card_have_record'] = 1 if len(loan_card)>0 else 0 #是否有信贷记录
        dict_out['loan_card_car_loan_count'] = len(re.findall('汽车',str(loan_card.businessType))) #车贷账户数
        dict_out['loan_card_startdata_min'] = min(re.findall('[\d]{4}[./-][\d]{2}[./-][\d]{2}',str(loan_card.startDate))) if len(re.findall('[\d]{4}[./-][\d]{2}[./-][\d]{2}',str(loan_card.startDate)))>0 else np.nan #最早人行信贷记录日期
        dict_out['loan_card_startdata_min_to_report_days'] = daysdelta(dict_out['loan_card_startdata_min'] ,dict_in['cc_rh_report']['reportTime'])
        dict_out['loan_card_comsumOrCard_amount_max'] = loan_card[[len(re.findall('消费|贷记卡',x))>0 for x in loan_card.businessType]].amount.max() #贷记卡授信额及消金最大额
        dict_out['loan_card_comsum_amount_max'] = loan_card[[len(re.findall('消费',x))>0 for x in loan_card.businessType]].amount.max() #消金最大额
        dict_out['loan_card_card_amount_max'] = loan_card[[len(re.findall('贷记卡',x))>0 for x in loan_card.businessType]].amount.max() #贷记卡授信最大额
        dict_out['loan_card_amount_maxCardVsMaxComsum'] = dict_out['loan_card_card_amount_max']/dict_out['loan_card_comsum_amount_max'] if dict_out['loan_card_comsum_amount_max'] >0 else np.nan# 贷记卡授信最大额比消金最大额

        dict_out['loan_card_type_loan_count']=loan_card[loan_card.type=='loan'].shape[0]  #  信贷交易业务大类为贷款，分布计数
        dict_out['loan_card_type_loan_org_cnt']=loan_card[loan_card.type=='loan'].grantOrg.nunique()  # 信贷交易业务大类为贷款，账户数合计(机构数)
        dict_out['loan_card_other_consumer_count']=loan_card[loan_card.businessType=='其他个人消费贷款'].shape[0]    #信贷交易业务类型为其他个人消费贷款，分布计数
        dict_out['loan_card_other_consumer_ratio']=loan_card[loan_card.businessType=='其他个人消费贷款'].shape[0]/loan_card.shape[0]    #信贷交易业务类型为其他个人消费贷款，分布比例
        dict_out['loan_card_other_consumer_org_cnt']=loan_card[loan_card.businessType=='其他个人消费贷款'].grantOrg.nunique()  #信贷交易业务类型为其他个人消费贷款，账户数合计(机构数)
        dict_out['loan_card_other_loan_count']=loan_card[loan_card.businessType=='其他贷款'].shape[0]    #信贷交易业务类型为其他贷款，分布计数
        dict_out['loan_card_other_loan_ratio']=loan_card[loan_card.businessType=='其他贷款'].shape[0]/loan_card.shape[0]    #信贷交易业务类型为其他贷款，分布比例
        dict_out['loan_card_other_loan_org_cnt']=loan_card[loan_card.businessType=='其他贷款'].grantOrg.nunique()  #信贷交易业务类型为其他贷款，账户数合计(机构数)
        dict_out['loan_card_credit_card_ratio']=loan_card[loan_card.businessType=='贷记卡'].shape[0]/loan_card.shape[0]    #信贷交易业务类型为贷记卡，分布比例
        dict_out['loan_card_balance_mean'] = loan_card.balance.mean() # 贷款和贷记卡余额平均值
        dict_out['loan_card_planRepayAmount_sum'] = loan_card.planRepayAmount.sum() # 贷款和贷记卡本月应还款额总和
        dict_out['loan_card_RepayedAmount_count'] = sum(loan_card.repayedAmount>0) # 贷款和贷记卡本月实还款额,分布计数
        dict_out['loan_card_RepayedAmount_max'] = loan_card.repayedAmount.max() # 贷款和贷记卡本月实还款额最大值
        dict_out['loan_card_RepayedAmount_sum'] = loan_card.repayedAmount.sum() # 贷款和贷记卡本月实还款额总和
        dict_out['loan_card_RepayedAmount_mean'] = loan_card.repayedAmount.mean() # 贷款和贷记卡本月实还款额平均值
        dict_out['loan_card_amount_count'] = sum(loan_card.amount>0) # 贷款和贷记卡额度,分布计数
        dict_out['loan_card_amount_sum'] = loan_card.amount.sum() # 贷款和贷记卡额度总和



        loan_card_now=loan_card[loan_card.is_now==1]
        if len(loan_card_now)>0:
            dict_out['loan_card_now_startdata_min'] = min(re.findall('[\d]{4}[./-][\d]{2}[./-][\d]{2}',str(loan_card_now.startDate))) if len(re.findall('[\d]{4}[./-][\d]{2}[./-][\d]{2}',str(loan_card_now.startDate)))>0 else np.nan #现行最早人行信贷记录日期
            dict_out['loan_card_now_startdata_min_to_report_days'] = daysdelta(dict_out['loan_card_now_startdata_min'] ,dict_in['cc_rh_report']['reportTime'])
            dict_out['loan_card_now_other_consumer_org_cnt']=loan_card_now[loan_card_now.businessType=='其他个人消费贷款'].grantOrg.nunique()  #现行信贷交易业务类型为其他个人消费贷款，账户数合计(机构数)
            dict_out['loan_card_now_other_loan_count']=loan_card_now[loan_card_now.businessType=='其他贷款'].shape[0]    #现行信贷交易业务类型为其他贷款，分布计数
            dict_out['loan_card_now_other_loan_ratio']=loan_card_now[loan_card_now.businessType=='其他贷款'].shape[0]/loan_card_now.shape[0]    #现行信贷交易业务类型为其他贷款，分布比例
            dict_out['loan_card_now_other_loan_org_cnt']=loan_card_now[loan_card_now.businessType=='其他贷款'].grantOrg.nunique()  #现行信贷交易业务类型为其他贷款，账户数合计(机构数)
            dict_out['loan_card_now_planRepayAmount_max'] = loan_card_now.planRepayAmount.max() #现行 贷款和贷记卡本月应还款额最大值
            dict_out['loan_card_now_planRepayAmount_mean'] = loan_card_now.planRepayAmount.mean() #现行 贷款和贷记卡本月应还款额平均值
            dict_out['loan_card_now_RepayedAmount_mean'] = loan_card_now.repayedAmount.mean() #现行 贷款和贷记卡本月实还款额平均值
            dict_out['loan_card_now_remainingTerms_sum'] = loan_card_now.remainingTerms.sum() #现行 贷款和贷记卡剩余还款期数总和

        loan_card_m01=loan_card[loan_card.byDate_to_report<1]
        if len(loan_card_m01)>0:
            dict_out['loan_card_m01_have_record'] = 1 if len(loan_card_m01)>0 else 0 #近01个月截至是否有信贷记录
            dict_out['loan_card_m01_car_loan_count'] = len(re.findall('汽车',str(loan_card_m01.businessType))) #近01个月截至车贷账户数
            dict_out['loan_card_m01_startdata_min'] = min(re.findall('[\d]{4}[./-][\d]{2}[./-][\d]{2}',str(loan_card_m01.startDate))) if len(re.findall('[\d]{4}[./-][\d]{2}[./-][\d]{2}',str(loan_card_m01.startDate)))>0 else np.nan #近01个月截至最早人行信贷记录日期
            dict_out['loan_card_m01_startdata_min_to_report_days'] = daysdelta(dict_out['loan_card_m01_startdata_min'] ,dict_in['cc_rh_report']['reportTime'])
            dict_out['loan_card_m01_comsumOrCard_amount_max'] = loan_card_m01[[len(re.findall('消费|贷记卡',x))>0 for x in loan_card_m01.businessType]].amount.max() #近01个月截至贷记卡授信额及消金最大额
            dict_out['loan_card_m01_comsum_amount_max'] = loan_card_m01[[len(re.findall('消费',x))>0 for x in loan_card_m01.businessType]].amount.max() #近01个月截至消金最大额
            dict_out['loan_card_m01_card_amount_max'] = loan_card_m01[[len(re.findall('贷记卡',x))>0 for x in loan_card_m01.businessType]].amount.max() #近01个月截至贷记卡授信最大额
            dict_out['loan_card_m01_amount_maxCardVsMaxComsum'] = dict_out['loan_card_m01_card_amount_max']/dict_out['loan_card_m01_comsum_amount_max'] if dict_out['loan_card_m01_comsum_amount_max'] >0 else np.nan#近01个月截至 贷记卡授信最大额比消金最大额

            dict_out['loan_card_m01_type_loan_count']=loan_card_m01[loan_card_m01.type=='loan'].shape[0]  #近01个月截至  信贷交易业务大类为贷款，分布计数
            dict_out['loan_card_m01_type_loan_ratio']=loan_card_m01[loan_card_m01.type=='loan'].shape[0]/loan_card_m01.shape[0] #近01个月截至  信贷交易业务大类为贷款，分布比例
            dict_out['loan_card_m01_type_loan_org_cnt']=loan_card_m01[loan_card_m01.type=='loan'].grantOrg.nunique()  #近01个月截至 信贷交易业务大类为贷款，账户数合计(机构数)
            dict_out['loan_card_m01_type_card_count']=loan_card_m01[loan_card_m01.type=='card'].shape[0]  #近01个月截至  信贷交易业务大类为信用卡，分布计数
            dict_out['loan_card_m01_type_card_ratio']=loan_card_m01[loan_card_m01.type=='card'].shape[0]/loan_card_m01.shape[0] #近01个月截至  信贷交易业务大类为信用卡，分布比例
            dict_out['loan_card_m01_type_card_org_cnt']=loan_card_m01[loan_card_m01.type=='card'].grantOrg.nunique()  #近01个月截至 信贷交易业务大类为信用卡，账户数合计(机构数)

            dict_out['loan_card_m01_hous_fund_loan_count']=loan_card_m01[loan_card_m01.businessType=='个人住房公积金贷款'].shape[0]    #近01个月截至信贷交易业务类型为个人住房公积金贷款，分布计数
            dict_out['loan_card_m01_hous_fund_loan_ratio']=loan_card_m01[loan_card_m01.businessType=='个人住房公积金贷款'].shape[0]/loan_card_m01.shape[0]    #近01个月截至信贷交易业务类型为个人住房公积金贷款，分布比例
            dict_out['loan_card_m01_hous_fund_loan_org_cnt']=loan_card_m01[loan_card_m01.businessType=='个人住房公积金贷款'].grantOrg.nunique()  #近01个月截至信贷交易业务类型为个人住房公积金贷款，账户数合计(机构数)
            dict_out['loan_card_m01_hous_commercial_loan_count']=loan_card_m01[loan_card_m01.businessType=='个人住房商业贷款'].shape[0]    #近01个月截至信贷交易业务类型为个人住房商业贷款，分布计数
            dict_out['loan_card_m01_hous_commercial_loan_ratio']=loan_card_m01[loan_card_m01.businessType=='个人住房商业贷款'].shape[0]/loan_card_m01.shape[0]    #近01个月截至信贷交易业务类型为个人住房商业贷款，分布比例
            dict_out['loan_card_m01_hous_commercial_loan_org_cnt']=loan_card_m01[loan_card_m01.businessType=='个人住房商业贷款'].grantOrg.nunique()  #近01个月截至信贷交易业务类型为个人住房商业贷款，账户数合计(机构数)
            dict_out['loan_card_m01_hous_loan_count']=loan_card_m01[[len(re.findall('个人住房',x))>0 for x in loan_card_m01.businessType ]].shape[0]    #近01个月截至信贷交易业务类型包含个人住房，分布计数
            dict_out['loan_card_m01_hous_loan_ratio']=loan_card_m01[[len(re.findall('个人住房',x))>0 for x in loan_card_m01.businessType ]].shape[0]/loan_card_m01.shape[0]    #近01个月截至信贷交易业务类型包含个人住房，分布比例
            dict_out['loan_card_m01_hous_loan_org_cnt']=loan_card_m01[[len(re.findall('个人住房',x))>0 for x in loan_card_m01.businessType ]].grantOrg.nunique()  #近01个月截至信贷交易业务类型包含个人住房，账户数合计(机构数)
            dict_out['loan_card_m01_student_loan_count']=loan_card_m01[loan_card_m01.businessType=='个人助学贷款'].shape[0]    #近01个月截至信贷交易业务类型为个人助学贷款，分布计数
            dict_out['loan_card_m01_student_loan_ratio']=loan_card_m01[loan_card_m01.businessType=='个人助学贷款'].shape[0]/loan_card_m01.shape[0]    #近01个月截至信贷交易业务类型为个人助学贷款，分布比例
            dict_out['loan_card_m01_student_loan_org_cnt']=loan_card_m01[loan_card_m01.businessType=='个人助学贷款'].grantOrg.nunique()  #近01个月截至信贷交易业务类型为个人助学贷款，账户数合计(机构数)
            dict_out['loan_card_m01_commercial_housing_count']=loan_card_m01[[len(re.findall('个人商用房',x))>0 for x in loan_card_m01.businessType ]].shape[0]    #近01个月截至信贷交易业务类型包含个人商用房，分布计数
            dict_out['loan_card_m01_commercial_housing_ratio']=loan_card_m01[[len(re.findall('个人商用房',x))>0 for x in loan_card_m01.businessType ]].shape[0]/loan_card_m01.shape[0]    #近01个月截至信贷交易业务类型包含个人商用房，分布比例
            dict_out['loan_card_m01_commercial_housing_org_cnt']=loan_card_m01[[len(re.findall('个人商用房',x))>0 for x in loan_card_m01.businessType ]].grantOrg.nunique()  #近01个月截至信贷交易业务类型包含个人商用房，账户数合计(机构数)
            dict_out['loan_card_m01_car_loan_count']=loan_card_m01[loan_card_m01.businessType=='个人汽车消费贷款'].shape[0]    #近01个月截至信贷交易业务类型为个人汽车消费贷款，分布计数
            dict_out['loan_card_m01_car_loan_ratio']=loan_card_m01[loan_card_m01.businessType=='个人汽车消费贷款'].shape[0]/loan_card_m01.shape[0]    #近01个月截至信贷交易业务类型为个人汽车消费贷款，分布比例
            dict_out['loan_card_m01_car_loan_org_cnt']=loan_card_m01[loan_card_m01.businessType=='个人汽车消费贷款'].grantOrg.nunique()  #近01个月截至信贷交易业务类型为个人汽车消费贷款，账户数合计(机构数)
            dict_out['loan_card_m01_operation_loan_count']=loan_card_m01[loan_card_m01.businessType=='个人经营性贷款'].shape[0]    #近01个月截至信贷交易业务类型为个人经营性贷款，分布计数
            dict_out['loan_card_m01_operation_loan_ratio']=loan_card_m01[loan_card_m01.businessType=='个人经营性贷款'].shape[0]/loan_card_m01.shape[0]    #近01个月截至信贷交易业务类型为个人经营性贷款，分布比例
            dict_out['loan_card_m01_operation_loan_org_cnt']=loan_card_m01[loan_card_m01.businessType=='个人经营性贷款'].grantOrg.nunique()  #近01个月截至信贷交易业务类型为个人经营性贷款，账户数合计(机构数)
            dict_out['loan_card_m01_other_consumer_count']=loan_card_m01[loan_card_m01.businessType=='其他个人消费贷款'].shape[0]    #近01个月截至信贷交易业务类型为其他个人消费贷款，分布计数
            dict_out['loan_card_m01_other_consumer_ratio']=loan_card_m01[loan_card_m01.businessType=='其他个人消费贷款'].shape[0]/loan_card_m01.shape[0]    #近01个月截至信贷交易业务类型为其他个人消费贷款，分布比例
            dict_out['loan_card_m01_other_consumer_org_cnt']=loan_card_m01[loan_card_m01.businessType=='其他个人消费贷款'].grantOrg.nunique()  #近01个月截至信贷交易业务类型为其他个人消费贷款，账户数合计(机构数)
            dict_out['loan_card_m01_other_loan_count']=loan_card_m01[loan_card_m01.businessType=='其他贷款'].shape[0]    #近01个月截至信贷交易业务类型为其他贷款，分布计数
            dict_out['loan_card_m01_other_loan_ratio']=loan_card_m01[loan_card_m01.businessType=='其他贷款'].shape[0]/loan_card_m01.shape[0]    #近01个月截至信贷交易业务类型为其他贷款，分布比例
            dict_out['loan_card_m01_other_loan_org_cnt']=loan_card_m01[loan_card_m01.businessType=='其他贷款'].grantOrg.nunique()  #近01个月截至信贷交易业务类型为其他贷款，账户数合计(机构数)
            dict_out['loan_card_m01_farmers_loan_count']=loan_card_m01[loan_card_m01.businessType=='农户贷款'].shape[0]    #近01个月截至信贷交易业务类型为农户贷款，分布计数
            dict_out['loan_card_m01_farmers_loan_ratio']=loan_card_m01[loan_card_m01.businessType=='农户贷款'].shape[0]/loan_card_m01.shape[0]    #近01个月截至信贷交易业务类型为农户贷款，分布比例
            dict_out['loan_card_m01_farmers_loan_org_cnt']=loan_card_m01[loan_card_m01.businessType=='农户贷款'].grantOrg.nunique()  #近01个月截至信贷交易业务类型为农户贷款，账户数合计(机构数)
            dict_out['loan_card_m01_stock_pledging_count']=loan_card_m01[loan_card_m01.businessType=='股票质押式回购交易'].shape[0]    #近01个月截至信贷交易业务类型为股票质押式回购交易，分布计数
            dict_out['loan_card_m01_stock_pledging_ratio']=loan_card_m01[loan_card_m01.businessType=='股票质押式回购交易'].shape[0]/loan_card_m01.shape[0]    #近01个月截至信贷交易业务类型为股票质押式回购交易，分布比例
            dict_out['loan_card_m01_stock_pledging_org_cnt']=loan_card_m01[loan_card_m01.businessType=='股票质押式回购交易'].grantOrg.nunique()  #近01个月截至信贷交易业务类型为股票质押式回购交易，账户数合计(机构数)
            dict_out['loan_card_m01_finance_lease_count']=loan_card_m01[loan_card_m01.businessType=='融资租赁业务'].shape[0]    #近01个月截至信贷交易业务类型为融资租赁业务，分布计数
            dict_out['loan_card_m01_finance_lease_ratio']=loan_card_m01[loan_card_m01.businessType=='融资租赁业务'].shape[0]/loan_card_m01.shape[0]    #近01个月截至信贷交易业务类型为融资租赁业务，分布比例
            dict_out['loan_card_m01_finance_lease_org_cnt']=loan_card_m01[loan_card_m01.businessType=='融资租赁业务'].grantOrg.nunique()  #近01个月截至信贷交易业务类型为融资租赁业务，账户数合计(机构数)
            dict_out['loan_card_m01_credit_card_count']=loan_card_m01[loan_card_m01.businessType=='贷记卡'].shape[0]    #近01个月截至信贷交易业务类型为贷记卡，分布计数
            dict_out['loan_card_m01_credit_card_ratio']=loan_card_m01[loan_card_m01.businessType=='贷记卡'].shape[0]/loan_card_m01.shape[0]    #近01个月截至信贷交易业务类型为贷记卡，分布比例
            dict_out['loan_card_m01_credit_card_org_cnt']=loan_card_m01[loan_card_m01.businessType=='贷记卡'].grantOrg.nunique()  #近01个月截至信贷交易业务类型为贷记卡，账户数合计(机构数)
            dict_out['loan_card_m01_not_card_count']=loan_card_m01[loan_card_m01.businessType==''].shape[0]    #近01个月截至信贷交易业务类型为，分布计数
            dict_out['loan_card_m01_not_card_ratio']=loan_card_m01[loan_card_m01.businessType==''].shape[0]/loan_card_m01.shape[0]    #近01个月截至信贷交易业务类型为，分布比例
            dict_out['loan_card_m01_not_card_org_cnt']=loan_card_m01[loan_card_m01.businessType==''].grantOrg.nunique()  #近01个月截至信贷交易业务类型为，账户数合计(机构数)

            dict_out['loan_card_m01_balance_count'] = sum(loan_card_m01.balance>0) #近01个月截至 贷款和贷记卡余额,分布计数
            dict_out['loan_card_m01_balance_max'] = loan_card_m01.balance.max() #近01个月截至 贷款和贷记卡余额最大值
            dict_out['loan_card_m01_balance_sum'] = loan_card_m01.balance.sum() #近01个月截至 贷款和贷记卡余额总和
            dict_out['loan_card_m01_balance_mean'] = loan_card_m01.balance.mean() #近01个月截至 贷款和贷记卡余额平均值
            dict_out['loan_card_m01_planRepayAmount_count'] = sum(loan_card_m01.planRepayAmount>0) #近01个月截至 贷款和贷记卡本月应还款额,分布计数
            dict_out['loan_card_m01_planRepayAmount_max'] = loan_card_m01.planRepayAmount.max() #近01个月截至 贷款和贷记卡本月应还款额最大值
            dict_out['loan_card_m01_planRepayAmount_sum'] = loan_card_m01.planRepayAmount.sum() #近01个月截至 贷款和贷记卡本月应还款额总和
            dict_out['loan_card_m01_planRepayAmount_mean'] = loan_card_m01.planRepayAmount.mean() #近01个月截至 贷款和贷记卡本月应还款额平均值
            dict_out['loan_card_m01_RepayedAmount_count'] = sum(loan_card_m01.repayedAmount>0) #近01个月截至 贷款和贷记卡本月实还款额,分布计数
            dict_out['loan_card_m01_RepayedAmount_max'] = loan_card_m01.repayedAmount.max() #近01个月截至 贷款和贷记卡本月实还款额最大值
            dict_out['loan_card_m01_RepayedAmount_sum'] = loan_card_m01.repayedAmount.sum() #近01个月截至 贷款和贷记卡本月实还款额总和
            dict_out['loan_card_m01_RepayedAmount_mean'] = loan_card_m01.repayedAmount.mean() #近01个月截至 贷款和贷记卡本月实还款额平均值
            dict_out['loan_card_m01_currentOverdueTerms_count'] = sum(loan_card_m01.currentOverdueTerms>0) #近01个月截至 贷款和贷记卡当前逾期期数,分布计数
            dict_out['loan_card_m01_currentOverdueTerms_max'] = loan_card_m01.currentOverdueTerms.max() #近01个月截至 贷款和贷记卡当前逾期期数最大值
            dict_out['loan_card_m01_currentOverdueTerms_sum'] = loan_card_m01.currentOverdueTerms.sum() #近01个月截至 贷款和贷记卡当前逾期期数总和
            dict_out['loan_card_m01_currentOverdueTerms_mean'] = loan_card_m01.currentOverdueTerms.mean() #近01个月截至 贷款和贷记卡当前逾期期数平均值
            dict_out['loan_card_m01_currentOverdueAmount_count'] = sum(loan_card_m01.currentOverdueAmount>0) #近01个月截至 贷款和贷记卡当前逾期金额,分布计数
            dict_out['loan_card_m01_currentOverdueAmount_max'] = loan_card_m01.currentOverdueAmount.max() #近01个月截至 贷款和贷记卡当前逾期金额最大值
            dict_out['loan_card_m01_currentOverdueAmount_sum'] = loan_card_m01.currentOverdueAmount.sum() #近01个月截至 贷款和贷记卡当前逾期金额总和
            dict_out['loan_card_m01_currentOverdueAmount_mean'] = loan_card_m01.currentOverdueAmount.mean() #近01个月截至 贷款和贷记卡当前逾期金额平均值
            dict_out['loan_card_m01_amount_count'] = sum(loan_card_m01.amount>0) #近01个月截至 贷款和贷记卡额度,分布计数
            dict_out['loan_card_m01_amount_max'] = loan_card_m01.amount.max() #近01个月截至 贷款和贷记卡额度最大值
            dict_out['loan_card_m01_amount_sum'] = loan_card_m01.amount.sum() #近01个月截至 贷款和贷记卡额度总和
            dict_out['loan_card_m01_amount_mean'] = loan_card_m01.amount.mean() #近01个月截至 贷款和贷记卡额度平均值
            dict_out['loan_card_m01_remainingTerms_count'] = sum(loan_card_m01.remainingTerms>0) #近01个月截至 贷款和贷记卡剩余还款期数,分布计数
            dict_out['loan_card_m01_remainingTerms_max'] = loan_card_m01.remainingTerms.max() #近01个月截至 贷款和贷记卡剩余还款期数最大值
            dict_out['loan_card_m01_remainingTerms_sum'] = loan_card_m01.remainingTerms.sum() #近01个月截至 贷款和贷记卡剩余还款期数总和
            dict_out['loan_card_m01_remainingTerms_mean'] = loan_card_m01.remainingTerms.mean() #近01个月截至 贷款和贷记卡剩余还款期数平均值

        loan_card_m06=loan_card[loan_card.byDate_to_report<6]
        if len(loan_card_m06)>0:
            dict_out['loan_card_m06_other_loan_org_cnt']=loan_card_m06[loan_card_m06.businessType=='其他贷款'].grantOrg.nunique()  #近06个月截至信贷交易业务类型为其他贷款，账户数合计(机构数)
            dict_out['loan_card_m06_planRepayAmount_mean'] = loan_card_m06.planRepayAmount.mean() #近06个月截至 贷款和贷记卡本月应还款额平均值
            dict_out['loan_card_m06_RepayedAmount_max'] = loan_card_m06.repayedAmount.max() #近06个月截至 贷款和贷记卡本月实还款额最大值
            dict_out['loan_card_m06_RepayedAmount_mean'] = loan_card_m06.repayedAmount.mean() #近06个月截至 贷款和贷记卡本月实还款额平均值
            dict_out['loan_card_m06_amount_count'] = sum(loan_card_m06.amount>0) #近06个月截至 贷款和贷记卡额度,分布计数
            dict_out['loan_card_m06_remainingTerms_max'] = loan_card_m06.remainingTerms.max() #近06个月截至 贷款和贷记卡剩余还款期数最大值

        loan_card_gm06=loan_card[loan_card.startDate_to_report<6]
        if len(loan_card_gm06)>0:
            dict_out['loan_card_gm06_other_consumer_ratio']=loan_card_gm06[loan_card_gm06.businessType=='其他个人消费贷款'].shape[0]/loan_card_gm06.shape[0]    #近06个月开立信贷交易业务类型为其他个人消费贷款，分布比例
            dict_out['loan_card_gm06_credit_card_ratio']=loan_card_gm06[loan_card_gm06.businessType=='贷记卡'].shape[0]/loan_card_gm06.shape[0]    #近06个月开立信贷交易业务类型为贷记卡，分布比例
            dict_out['loan_card_gm06_planRepayAmount_count'] = sum(loan_card_gm06.planRepayAmount>0) #近06个月开立 贷款和贷记卡本月应还款额,分布计数
            dict_out['loan_card_gm06_planRepayAmount_max'] = loan_card_gm06.planRepayAmount.max() #近06个月开立 贷款和贷记卡本月应还款额最大值
            dict_out['loan_card_gm06_remainingTerms_max'] = loan_card_gm06.remainingTerms.max() #近06个月开立 贷款和贷记卡剩余还款期数最大值
            dict_out['loan_card_gm06_remainingTerms_sum'] = loan_card_gm06.remainingTerms.sum() #近06个月开立 贷款和贷记卡剩余还款期数总和


        loan_card_gm12=loan_card[loan_card.startDate_to_report<12]
        if len(loan_card_gm12)>0:
            dict_out['loan_card_gm12_have_record'] = 1 if len(loan_card_gm12)>0 else 0 #近12个月开立是否有信贷记录
            dict_out['loan_card_gm12_car_loan_count'] = len(re.findall('汽车',str(loan_card_gm12.businessType))) #近12个月开立车贷账户数
            dict_out['loan_card_gm12_startdata_min'] = min(re.findall('[\d]{4}[./-][\d]{2}[./-][\d]{2}',str(loan_card_gm12.startDate))) if len(re.findall('[\d]{4}[./-][\d]{2}[./-][\d]{2}',str(loan_card_gm12.startDate)))>0 else np.nan #近12个月开立最早人行信贷记录日期
            dict_out['loan_card_gm12_startdata_min_to_report_days'] = daysdelta(dict_out['loan_card_gm12_startdata_min'] ,dict_in['cc_rh_report']['reportTime'])
            dict_out['loan_card_gm12_comsumOrCard_amount_max'] = loan_card_gm12[[len(re.findall('消费|贷记卡',x))>0 for x in loan_card_gm12.businessType]].amount.max() #近12个月开立贷记卡授信额及消金最大额
            dict_out['loan_card_gm12_comsum_amount_max'] = loan_card_gm12[[len(re.findall('消费',x))>0 for x in loan_card_gm12.businessType]].amount.max() #近12个月开立消金最大额
            dict_out['loan_card_gm12_card_amount_max'] = loan_card_gm12[[len(re.findall('贷记卡',x))>0 for x in loan_card_gm12.businessType]].amount.max() #近12个月开立贷记卡授信最大额
            dict_out['loan_card_gm12_amount_maxCardVsMaxComsum'] = dict_out['loan_card_gm12_card_amount_max']/dict_out['loan_card_gm12_comsum_amount_max'] if dict_out['loan_card_gm12_comsum_amount_max'] >0 else np.nan#近12个月开立 贷记卡授信最大额比消金最大额

            dict_out['loan_card_gm12_type_loan_count']=loan_card_gm12[loan_card_gm12.type=='loan'].shape[0]  #近12个月开立  信贷交易业务大类为贷款，分布计数
            dict_out['loan_card_gm12_type_loan_ratio']=loan_card_gm12[loan_card_gm12.type=='loan'].shape[0]/loan_card_gm12.shape[0] #近12个月开立  信贷交易业务大类为贷款，分布比例
            dict_out['loan_card_gm12_type_loan_org_cnt']=loan_card_gm12[loan_card_gm12.type=='loan'].grantOrg.nunique()  #近12个月开立 信贷交易业务大类为贷款，账户数合计(机构数)
            dict_out['loan_card_gm12_type_card_count']=loan_card_gm12[loan_card_gm12.type=='card'].shape[0]  #近12个月开立  信贷交易业务大类为信用卡，分布计数
            dict_out['loan_card_gm12_type_card_ratio']=loan_card_gm12[loan_card_gm12.type=='card'].shape[0]/loan_card_gm12.shape[0] #近12个月开立  信贷交易业务大类为信用卡，分布比例
            dict_out['loan_card_gm12_type_card_org_cnt']=loan_card_gm12[loan_card_gm12.type=='card'].grantOrg.nunique()  #近12个月开立 信贷交易业务大类为信用卡，账户数合计(机构数)

            dict_out['loan_card_gm12_hous_fund_loan_count']=loan_card_gm12[loan_card_gm12.businessType=='个人住房公积金贷款'].shape[0]    #近12个月开立信贷交易业务类型为个人住房公积金贷款，分布计数
            dict_out['loan_card_gm12_hous_fund_loan_ratio']=loan_card_gm12[loan_card_gm12.businessType=='个人住房公积金贷款'].shape[0]/loan_card_gm12.shape[0]    #近12个月开立信贷交易业务类型为个人住房公积金贷款，分布比例
            dict_out['loan_card_gm12_hous_fund_loan_org_cnt']=loan_card_gm12[loan_card_gm12.businessType=='个人住房公积金贷款'].grantOrg.nunique()  #近12个月开立信贷交易业务类型为个人住房公积金贷款，账户数合计(机构数)
            dict_out['loan_card_gm12_hous_commercial_loan_count']=loan_card_gm12[loan_card_gm12.businessType=='个人住房商业贷款'].shape[0]    #近12个月开立信贷交易业务类型为个人住房商业贷款，分布计数
            dict_out['loan_card_gm12_hous_commercial_loan_ratio']=loan_card_gm12[loan_card_gm12.businessType=='个人住房商业贷款'].shape[0]/loan_card_gm12.shape[0]    #近12个月开立信贷交易业务类型为个人住房商业贷款，分布比例
            dict_out['loan_card_gm12_hous_commercial_loan_org_cnt']=loan_card_gm12[loan_card_gm12.businessType=='个人住房商业贷款'].grantOrg.nunique()  #近12个月开立信贷交易业务类型为个人住房商业贷款，账户数合计(机构数)
            dict_out['loan_card_gm12_hous_loan_count']=loan_card_gm12[[len(re.findall('个人住房',x))>0 for x in loan_card_gm12.businessType ]].shape[0]    #近12个月开立信贷交易业务类型包含个人住房，分布计数
            dict_out['loan_card_gm12_hous_loan_ratio']=loan_card_gm12[[len(re.findall('个人住房',x))>0 for x in loan_card_gm12.businessType ]].shape[0]/loan_card_gm12.shape[0]    #近12个月开立信贷交易业务类型包含个人住房，分布比例
            dict_out['loan_card_gm12_hous_loan_org_cnt']=loan_card_gm12[[len(re.findall('个人住房',x))>0 for x in loan_card_gm12.businessType ]].grantOrg.nunique()  #近12个月开立信贷交易业务类型包含个人住房，账户数合计(机构数)
            dict_out['loan_card_gm12_student_loan_count']=loan_card_gm12[loan_card_gm12.businessType=='个人助学贷款'].shape[0]    #近12个月开立信贷交易业务类型为个人助学贷款，分布计数
            dict_out['loan_card_gm12_student_loan_ratio']=loan_card_gm12[loan_card_gm12.businessType=='个人助学贷款'].shape[0]/loan_card_gm12.shape[0]    #近12个月开立信贷交易业务类型为个人助学贷款，分布比例
            dict_out['loan_card_gm12_student_loan_org_cnt']=loan_card_gm12[loan_card_gm12.businessType=='个人助学贷款'].grantOrg.nunique()  #近12个月开立信贷交易业务类型为个人助学贷款，账户数合计(机构数)
            dict_out['loan_card_gm12_commercial_housing_count']=loan_card_gm12[[len(re.findall('个人商用房',x))>0 for x in loan_card_gm12.businessType ]].shape[0]    #近12个月开立信贷交易业务类型包含个人商用房，分布计数
            dict_out['loan_card_gm12_commercial_housing_ratio']=loan_card_gm12[[len(re.findall('个人商用房',x))>0 for x in loan_card_gm12.businessType ]].shape[0]/loan_card_gm12.shape[0]    #近12个月开立信贷交易业务类型包含个人商用房，分布比例
            dict_out['loan_card_gm12_commercial_housing_org_cnt']=loan_card_gm12[[len(re.findall('个人商用房',x))>0 for x in loan_card_gm12.businessType ]].grantOrg.nunique()  #近12个月开立信贷交易业务类型包含个人商用房，账户数合计(机构数)
            dict_out['loan_card_gm12_car_loan_count']=loan_card_gm12[loan_card_gm12.businessType=='个人汽车消费贷款'].shape[0]    #近12个月开立信贷交易业务类型为个人汽车消费贷款，分布计数
            dict_out['loan_card_gm12_car_loan_ratio']=loan_card_gm12[loan_card_gm12.businessType=='个人汽车消费贷款'].shape[0]/loan_card_gm12.shape[0]    #近12个月开立信贷交易业务类型为个人汽车消费贷款，分布比例
            dict_out['loan_card_gm12_car_loan_org_cnt']=loan_card_gm12[loan_card_gm12.businessType=='个人汽车消费贷款'].grantOrg.nunique()  #近12个月开立信贷交易业务类型为个人汽车消费贷款，账户数合计(机构数)
            dict_out['loan_card_gm12_operation_loan_count']=loan_card_gm12[loan_card_gm12.businessType=='个人经营性贷款'].shape[0]    #近12个月开立信贷交易业务类型为个人经营性贷款，分布计数
            dict_out['loan_card_gm12_operation_loan_ratio']=loan_card_gm12[loan_card_gm12.businessType=='个人经营性贷款'].shape[0]/loan_card_gm12.shape[0]    #近12个月开立信贷交易业务类型为个人经营性贷款，分布比例
            dict_out['loan_card_gm12_operation_loan_org_cnt']=loan_card_gm12[loan_card_gm12.businessType=='个人经营性贷款'].grantOrg.nunique()  #近12个月开立信贷交易业务类型为个人经营性贷款，账户数合计(机构数)
            dict_out['loan_card_gm12_other_consumer_count']=loan_card_gm12[loan_card_gm12.businessType=='其他个人消费贷款'].shape[0]    #近12个月开立信贷交易业务类型为其他个人消费贷款，分布计数
            dict_out['loan_card_gm12_other_consumer_ratio']=loan_card_gm12[loan_card_gm12.businessType=='其他个人消费贷款'].shape[0]/loan_card_gm12.shape[0]    #近12个月开立信贷交易业务类型为其他个人消费贷款，分布比例
            dict_out['loan_card_gm12_other_consumer_org_cnt']=loan_card_gm12[loan_card_gm12.businessType=='其他个人消费贷款'].grantOrg.nunique()  #近12个月开立信贷交易业务类型为其他个人消费贷款，账户数合计(机构数)
            dict_out['loan_card_gm12_other_loan_count']=loan_card_gm12[loan_card_gm12.businessType=='其他贷款'].shape[0]    #近12个月开立信贷交易业务类型为其他贷款，分布计数
            dict_out['loan_card_gm12_other_loan_ratio']=loan_card_gm12[loan_card_gm12.businessType=='其他贷款'].shape[0]/loan_card_gm12.shape[0]    #近12个月开立信贷交易业务类型为其他贷款，分布比例
            dict_out['loan_card_gm12_other_loan_org_cnt']=loan_card_gm12[loan_card_gm12.businessType=='其他贷款'].grantOrg.nunique()  #近12个月开立信贷交易业务类型为其他贷款，账户数合计(机构数)
            dict_out['loan_card_gm12_farmers_loan_count']=loan_card_gm12[loan_card_gm12.businessType=='农户贷款'].shape[0]    #近12个月开立信贷交易业务类型为农户贷款，分布计数
            dict_out['loan_card_gm12_farmers_loan_ratio']=loan_card_gm12[loan_card_gm12.businessType=='农户贷款'].shape[0]/loan_card_gm12.shape[0]    #近12个月开立信贷交易业务类型为农户贷款，分布比例
            dict_out['loan_card_gm12_farmers_loan_org_cnt']=loan_card_gm12[loan_card_gm12.businessType=='农户贷款'].grantOrg.nunique()  #近12个月开立信贷交易业务类型为农户贷款，账户数合计(机构数)
            dict_out['loan_card_gm12_stock_pledging_count']=loan_card_gm12[loan_card_gm12.businessType=='股票质押式回购交易'].shape[0]    #近12个月开立信贷交易业务类型为股票质押式回购交易，分布计数
            dict_out['loan_card_gm12_stock_pledging_ratio']=loan_card_gm12[loan_card_gm12.businessType=='股票质押式回购交易'].shape[0]/loan_card_gm12.shape[0]    #近12个月开立信贷交易业务类型为股票质押式回购交易，分布比例
            dict_out['loan_card_gm12_stock_pledging_org_cnt']=loan_card_gm12[loan_card_gm12.businessType=='股票质押式回购交易'].grantOrg.nunique()  #近12个月开立信贷交易业务类型为股票质押式回购交易，账户数合计(机构数)
            dict_out['loan_card_gm12_finance_lease_count']=loan_card_gm12[loan_card_gm12.businessType=='融资租赁业务'].shape[0]    #近12个月开立信贷交易业务类型为融资租赁业务，分布计数
            dict_out['loan_card_gm12_finance_lease_ratio']=loan_card_gm12[loan_card_gm12.businessType=='融资租赁业务'].shape[0]/loan_card_gm12.shape[0]    #近12个月开立信贷交易业务类型为融资租赁业务，分布比例
            dict_out['loan_card_gm12_finance_lease_org_cnt']=loan_card_gm12[loan_card_gm12.businessType=='融资租赁业务'].grantOrg.nunique()  #近12个月开立信贷交易业务类型为融资租赁业务，账户数合计(机构数)
            dict_out['loan_card_gm12_credit_card_count']=loan_card_gm12[loan_card_gm12.businessType=='贷记卡'].shape[0]    #近12个月开立信贷交易业务类型为贷记卡，分布计数
            dict_out['loan_card_gm12_credit_card_ratio']=loan_card_gm12[loan_card_gm12.businessType=='贷记卡'].shape[0]/loan_card_gm12.shape[0]    #近12个月开立信贷交易业务类型为贷记卡，分布比例
            dict_out['loan_card_gm12_credit_card_org_cnt']=loan_card_gm12[loan_card_gm12.businessType=='贷记卡'].grantOrg.nunique()  #近12个月开立信贷交易业务类型为贷记卡，账户数合计(机构数)
            dict_out['loan_card_gm12_not_card_count']=loan_card_gm12[loan_card_gm12.businessType==''].shape[0]    #近12个月开立信贷交易业务类型为，分布计数
            dict_out['loan_card_gm12_not_card_ratio']=loan_card_gm12[loan_card_gm12.businessType==''].shape[0]/loan_card_gm12.shape[0]    #近12个月开立信贷交易业务类型为，分布比例
            dict_out['loan_card_gm12_not_card_org_cnt']=loan_card_gm12[loan_card_gm12.businessType==''].grantOrg.nunique()  #近12个月开立信贷交易业务类型为，账户数合计(机构数)

            dict_out['loan_card_gm12_balance_count'] = sum(loan_card_gm12.balance>0) #近12个月开立 贷款和贷记卡余额,分布计数
            dict_out['loan_card_gm12_balance_max'] = loan_card_gm12.balance.max() #近12个月开立 贷款和贷记卡余额最大值
            dict_out['loan_card_gm12_balance_sum'] = loan_card_gm12.balance.sum() #近12个月开立 贷款和贷记卡余额总和
            dict_out['loan_card_gm12_balance_mean'] = loan_card_gm12.balance.mean() #近12个月开立 贷款和贷记卡余额平均值
            dict_out['loan_card_gm12_planRepayAmount_count'] = sum(loan_card_gm12.planRepayAmount>0) #近12个月开立 贷款和贷记卡本月应还款额,分布计数
            dict_out['loan_card_gm12_planRepayAmount_max'] = loan_card_gm12.planRepayAmount.max() #近12个月开立 贷款和贷记卡本月应还款额最大值
            dict_out['loan_card_gm12_planRepayAmount_sum'] = loan_card_gm12.planRepayAmount.sum() #近12个月开立 贷款和贷记卡本月应还款额总和
            dict_out['loan_card_gm12_planRepayAmount_mean'] = loan_card_gm12.planRepayAmount.mean() #近12个月开立 贷款和贷记卡本月应还款额平均值
            dict_out['loan_card_gm12_RepayedAmount_count'] = sum(loan_card_gm12.repayedAmount>0) #近12个月开立 贷款和贷记卡本月实还款额,分布计数
            dict_out['loan_card_gm12_RepayedAmount_max'] = loan_card_gm12.repayedAmount.max() #近12个月开立 贷款和贷记卡本月实还款额最大值
            dict_out['loan_card_gm12_RepayedAmount_sum'] = loan_card_gm12.repayedAmount.sum() #近12个月开立 贷款和贷记卡本月实还款额总和
            dict_out['loan_card_gm12_RepayedAmount_mean'] = loan_card_gm12.repayedAmount.mean() #近12个月开立 贷款和贷记卡本月实还款额平均值
            dict_out['loan_card_gm12_currentOverdueTerms_count'] = sum(loan_card_gm12.currentOverdueTerms>0) #近12个月开立 贷款和贷记卡当前逾期期数,分布计数
            dict_out['loan_card_gm12_currentOverdueTerms_max'] = loan_card_gm12.currentOverdueTerms.max() #近12个月开立 贷款和贷记卡当前逾期期数最大值
            dict_out['loan_card_gm12_currentOverdueTerms_sum'] = loan_card_gm12.currentOverdueTerms.sum() #近12个月开立 贷款和贷记卡当前逾期期数总和
            dict_out['loan_card_gm12_currentOverdueTerms_mean'] = loan_card_gm12.currentOverdueTerms.mean() #近12个月开立 贷款和贷记卡当前逾期期数平均值
            dict_out['loan_card_gm12_currentOverdueAmount_count'] = sum(loan_card_gm12.currentOverdueAmount>0) #近12个月开立 贷款和贷记卡当前逾期金额,分布计数
            dict_out['loan_card_gm12_currentOverdueAmount_max'] = loan_card_gm12.currentOverdueAmount.max() #近12个月开立 贷款和贷记卡当前逾期金额最大值
            dict_out['loan_card_gm12_currentOverdueAmount_sum'] = loan_card_gm12.currentOverdueAmount.sum() #近12个月开立 贷款和贷记卡当前逾期金额总和
            dict_out['loan_card_gm12_currentOverdueAmount_mean'] = loan_card_gm12.currentOverdueAmount.mean() #近12个月开立 贷款和贷记卡当前逾期金额平均值
            dict_out['loan_card_gm12_amount_count'] = sum(loan_card_gm12.amount>0) #近12个月开立 贷款和贷记卡额度,分布计数
            dict_out['loan_card_gm12_amount_max'] = loan_card_gm12.amount.max() #近12个月开立 贷款和贷记卡额度最大值
            dict_out['loan_card_gm12_amount_sum'] = loan_card_gm12.amount.sum() #近12个月开立 贷款和贷记卡额度总和
            dict_out['loan_card_gm12_amount_mean'] = loan_card_gm12.amount.mean() #近12个月开立 贷款和贷记卡额度平均值
            dict_out['loan_card_gm12_remainingTerms_count'] = sum(loan_card_gm12.remainingTerms>0) #近12个月开立 贷款和贷记卡剩余还款期数,分布计数
            dict_out['loan_card_gm12_remainingTerms_max'] = loan_card_gm12.remainingTerms.max() #近12个月开立 贷款和贷记卡剩余还款期数最大值
            dict_out['loan_card_gm12_remainingTerms_sum'] = loan_card_gm12.remainingTerms.sum() #近12个月开立 贷款和贷记卡剩余还款期数总和
            dict_out['loan_card_gm12_remainingTerms_mean'] = loan_card_gm12.remainingTerms.mean() #近12个月开立 贷款和贷记卡剩余还款期数平均值

    lc_month60=pd.concat([l_month60,c_month60])
    if len(lc_month60)>0:
        dict_out['lc_month60_State_big0_mean'] = lc_month60[lc_month60['month60_State_num'] > 0]['month60_State_num'].mean()  # 贷款贷记卡  近5年还款情况大于0平均值

        dict_out['lc_month60_Amount_num_mean'] = lc_month60['month60_Amount_num'].mean()  # 贷款贷记卡  近5年还款金额平均值
        dict_out['lc_month60_N_count'] = lc_month60[lc_month60['month60_State'] == 'N'].shape[0]  # 贷款贷记卡  近5年还款情况为N 计数
        dict_out['lc_month60_N_ratio'] = lc_month60[lc_month60['month60_State'] == 'N'].shape[0] / len(lc_month60)  # 贷款贷记卡  近5年还款情况为N 占比
        dict_out['lc_month60_C_count'] = lc_month60[lc_month60['month60_State'] == 'C'].shape[0]  # 贷款贷记卡  近5年还款情况为C 计数
        dict_out['lc_month60_Null_count'] = lc_month60[lc_month60['month60_State'] == '*'].shape[0]  # 贷款贷记卡  近5年还款情况为* 计数

        lc_month60_m01 = lc_month60[lc_month60.month60_to_report<1]
        if len(lc_month60_m01)>0:
            dict_out['lc_month60_m01_N_ratio'] = lc_month60_m01[lc_month60_m01['month60_State'] == 'N'].shape[0] / len(lc_month60_m01)  # 近01个月 贷款贷记卡  近5年还款情况为N 占比
            dict_out['lc_month60_m01_C_ratio'] = lc_month60_m01[lc_month60_m01['month60_State'] == 'C'].shape[0] / len(lc_month60_m01)  # 近01个月 贷款贷记卡  近5年还款情况为C 占比
            dict_out['lc_month60_m01_Null_count'] = lc_month60_m01[lc_month60_m01['month60_State'] == '*'].shape[0]  # 近01个月 贷款贷记卡  近5年还款情况为* 计数

        lc_month60_m03 = lc_month60[lc_month60.month60_to_report < 3]
        if len(lc_month60_m03) > 0:
            dict_out['lc_month60_m03_Amount_num_mean'] = lc_month60_m03['month60_Amount_num'].mean()  # 近03个月 贷款贷记卡  近5年还款金额平均值
            dict_out['lc_month60_m03_N_count'] = lc_month60_m03[lc_month60_m03['month60_State'] == 'N'].shape[0]  # 近03个月 贷款贷记卡  近5年还款情况为N 计数
            dict_out['lc_month60_m03_Null_ratio'] = lc_month60_m03[lc_month60_m03['month60_State'] == '*'].shape[0] / len(lc_month60_m03)  # 近03个月 贷款贷记卡  近5年还款情况为* 占比

        lc_month60_m06 = lc_month60[lc_month60.month60_to_report < 6]
        if len(lc_month60_m06) > 0:
            dict_out['lc_month60_m06_State_num_mean'] = lc_month60_m06['month60_State_num'].mean()  # 近06个月 贷款贷记卡  近5年还款情况平均值

            dict_out['lc_month60_m06_Amount_num_mean'] = lc_month60_m06['month60_Amount_num'].mean()  # 近06个月 贷款贷记卡  近5年还款金额平均值

            dict_out['lc_month60_m06_N_count'] = lc_month60_m06[lc_month60_m06['month60_State'] == 'N'].shape[0]  # 近06个月 贷款贷记卡  近5年还款情况为N 计数
            dict_out['lc_month60_m06_N_ratio'] = lc_month60_m06[lc_month60_m06['month60_State'] == 'N'].shape[0] / len(lc_month60_m06)  # 近06个月 贷款贷记卡  近5年还款情况为N 占比
            dict_out['lc_month60_m06_C_ratio'] = lc_month60_m06[lc_month60_m06['month60_State'] == 'C'].shape[0] / len(lc_month60_m06)  # 近06个月 贷款贷记卡  近5年还款情况为C 占比
            dict_out['lc_month60_m06_Null_ratio'] = lc_month60_m06[lc_month60_m06['month60_State'] == '*'].shape[0] / len(lc_month60_m06)  # 近06个月 贷款贷记卡  近5年还款情况为* 占比

        lc_month60_m12 = lc_month60[lc_month60.month60_to_report < 12]
        if len(lc_month60_m12) > 0:
            dict_out['lc_month60_m12_State_num_mean'] = lc_month60_m12['month60_State_num'].mean()  # 近12个月 贷款贷记卡  近5年还款情况平均值

            dict_out['lc_month60_m12_N_count'] = lc_month60_m12[lc_month60_m12['month60_State'] == 'N'].shape[0]  # 近12个月 贷款贷记卡  近5年还款情况为N 计数
            dict_out['lc_month60_m12_N_ratio'] = lc_month60_m12[lc_month60_m12['month60_State'] == 'N'].shape[0] / len(lc_month60_m12)  # 近12个月 贷款贷记卡  近5年还款情况为N 占比
            dict_out['lc_month60_m12_C_count'] = lc_month60_m12[lc_month60_m12['month60_State'] == 'C'].shape[0]  # 近12个月 贷款贷记卡  近5年还款情况为C 计数
            dict_out['lc_month60_m12_C_ratio'] = lc_month60_m12[lc_month60_m12['month60_State'] == 'C'].shape[0] / len(lc_month60_m12)  # 近12个月 贷款贷记卡  近5年还款情况为C 占比
            dict_out['lc_month60_m12_Null_count'] = lc_month60_m12[lc_month60_m12['month60_State'] == '*'].shape[0]  # 近12个月 贷款贷记卡  近5年还款情况为* 计数
            dict_out['lc_month60_m12_Null_ratio'] = lc_month60_m12[lc_month60_m12['month60_State'] == '*'].shape[0] / len(lc_month60_m12)  # 近12个月 贷款贷记卡  近5年还款情况为* 占比

        lc_month60_m24 = lc_month60[lc_month60.month60_to_report < 24]
        if len(lc_month60_m24) > 0:
            dict_out['lc_month60_m24_State_num_mean'] = lc_month60_m24['month60_State_num'].mean()  # 近24个月 贷款贷记卡  近5年还款情况平均值

            dict_out['lc_month60_m24_Amount_num_mean'] = lc_month60_m24['month60_Amount_num'].mean()  # 近24个月 贷款贷记卡  近5年还款金额平均值

            dict_out['lc_month60_m24_State_giniimpurity'] = giniimpurity(lc_month60_m24['month60_State'])  # 近24个月 贷款贷记卡  近5年还款情况基尼不纯度
            dict_out['lc_month60_m24_N_count'] = lc_month60_m24[lc_month60_m24['month60_State'] == 'N'].shape[0]  # 近24个月 贷款贷记卡  近5年还款情况为N 计数
            dict_out['lc_month60_m24_C_count'] = lc_month60_m24[lc_month60_m24['month60_State'] == 'C'].shape[0]  # 近24个月 贷款贷记卡  近5年还款情况为C 计数
            dict_out['lc_month60_m24_1_ratio'] = lc_month60_m24[lc_month60_m24['month60_State'] == '1'].shape[0] / len(lc_month60_m24)  # 近24个月 贷款贷记卡  近5年还款情况为1 占比

        lc_month60_m36 = lc_month60[lc_month60.month60_to_report < 36]
        if len(lc_month60_m36) > 0:
            dict_out['lc_month60_m36_State_big0_mean'] = lc_month60_m36[lc_month60_m36['month60_State_num'] > 0]['month60_State_num'].mean()  # 近36个月 贷款贷记卡  近5年还款情况大于0平均值

            dict_out['lc_month60_m36_Amount_num_mean'] = lc_month60_m36['month60_Amount_num'].mean()  # 近36个月 贷款贷记卡  近5年还款金额平均值

            dict_out['lc_month60_m36_State_giniimpurity'] = giniimpurity(lc_month60_m36['month60_State'])  # 近36个月 贷款贷记卡  近5年还款情况基尼不纯度
            dict_out['lc_month60_m36_N_count'] = lc_month60_m36[lc_month60_m36['month60_State'] == 'N'].shape[0]  # 近36个月 贷款贷记卡  近5年还款情况为N 计数
            dict_out['lc_month60_m36_N_ratio'] = lc_month60_m36[lc_month60_m36['month60_State'] == 'N'].shape[0] / len(lc_month60_m36)  # 近36个月 贷款贷记卡  近5年还款情况为N 占比
            dict_out['lc_month60_m36_C_count'] = lc_month60_m36[lc_month60_m36['month60_State'] == 'C'].shape[0]  # 近36个月 贷款贷记卡  近5年还款情况为C 计数
            dict_out['lc_month60_m36_Null_count'] = lc_month60_m36[lc_month60_m36['month60_State'] == '*'].shape[0]  # 近36个月 贷款贷记卡  近5年还款情况为* 计数
            dict_out['lc_month60_m36_Null_ratio'] = lc_month60_m36[lc_month60_m36['month60_State'] == '*'].shape[0] / len(lc_month60_m36)  # 近36个月 贷款贷记卡  近5年还款情况为* 占比
            dict_out['lc_month60_m36_1_ratio'] = lc_month60_m36[lc_month60_m36['month60_State'] == '1'].shape[0] / len(lc_month60_m36)  # 近36个月 贷款贷记卡  近5年还款情况为1 占比

        lc_month60_m48 = lc_month60[lc_month60.month60_to_report < 48]
        if len(lc_month60_m48) > 0:
            dict_out['lc_month60_m48_State_num_mean'] = lc_month60_m48['month60_State_num'].mean()  # 近48个月 贷款贷记卡  近5年还款情况平均值
            dict_out['lc_month60_m48_N_count'] = lc_month60_m48[lc_month60_m48['month60_State'] == 'N'].shape[0]  # 近48个月 贷款贷记卡  近5年还款情况为N 计数
            dict_out['lc_month60_m48_C_count'] = lc_month60_m48[lc_month60_m48['month60_State'] == 'C'].shape[0]  # 近48个月 贷款贷记卡  近5年还款情况为C 计数
            dict_out['lc_month60_m48_Null_count'] = lc_month60_m48[lc_month60_m48['month60_State'] == '*'].shape[0]  # 近48个月 贷款贷记卡  近5年还款情况为* 计数
            dict_out['lc_month60_m48_Null_ratio'] = lc_month60_m48[lc_month60_m48['month60_State'] == '*'].shape[0] / len(lc_month60_m48)  # 近48个月 贷款贷记卡  近5年还款情况为* 占比

        lc_month60_m60 = lc_month60[lc_month60.month60_to_report < 60]
        if len(lc_month60_m60) > 0:
            dict_out['lc_month60_m60_N_count'] = lc_month60_m60[lc_month60_m60['month60_State'] == 'N'].shape[0]  # 近60个月 贷款贷记卡  近5年还款情况为N 计数
            dict_out['lc_month60_m60_C_count'] = lc_month60_m60[lc_month60_m60['month60_State'] == 'C'].shape[0]  # 近60个月 贷款贷记卡  近5年还款情况为C 计数
            dict_out['lc_month60_m60_Null_count'] = lc_month60_m60[lc_month60_m60['month60_State'] == '*'].shape[0]  # 近60个月 贷款贷记卡  近5年还款情况为* 计数

    #贷款和贷记卡和催收
    if len(recovery)>0:
        recovery_tmp=recovery[['reportId','orgName','businessType','accountStatus','month_to_report']].rename(columns={'orgName':'grantOrg'})
        recovery_tmp['type']=recovery_tmp['class']=recovery_tmp['currency']='催收'
    else:
        recovery_tmp=pd.DataFrame()
    loan_card_r=pd.concat([loan_card,recovery_tmp],sort=False)
    if len(loan_card_r)>0:
        #  --	1365377	            非循环贷账户
        # 不区分还款方式	128	     循环贷账户
        # 分期等额本息	1928      循环额度下分账户
        # 到期一次还本付息	1       非循环贷账户
        # 循环贷款下其他还款方式	6   循环贷账户
        # 按期计算还本付息	600     循环贷账户

        dict_out['loan_card_r_ncycle_count'] = loan_card_r[loan_card_r['class']=='非循环贷账户'].shape[0] # 借贷账户信息账户类型为非循环贷账户，分布计数
        dict_out['loan_card_r_card_ratio'] = loan_card_r[loan_card_r['class']=='贷记卡'].shape[0]/len(loan_card_r) # 借贷账户信息账户类型为贷记卡账户，分布所占比例


        dict_out['loan_card_r_operation_loan_ratio'] = loan_card_r[loan_card_r.businessType=='个人经营性贷款'].shape[0]/len(loan_card_r) # 借贷账户信息业务种类为个人经营性贷款，分布所占比例
        dict_out['loan_card_r_other_consumer_count'] = loan_card_r[loan_card_r.businessType=='其他个人消费贷款'].shape[0] # 借贷账户信息业务种类为其他个人消费贷款，分布计数
        dict_out['loan_card_r_finance_lease_ratio'] = loan_card_r[loan_card_r.businessType=='融资租赁业务'].shape[0]/len(loan_card_r) # 借贷账户信息业务种类为融资租赁业务，分布所占比例


        dict_out['loan_card_r_guaranty_pledge_count'] = loan_card_r[loan_card_r.guaranteeForm=='质押'].shape[0] # 借贷账户信息担保方式为质押，分布计数
        dict_out['loan_card_r_guaranty_credit_no_count'] = loan_card_r[loan_card_r.guaranteeForm=='信用/免担保'].shape[0] # 借贷账户信息担保方式为信用/免担保，分布计数
        dict_out['loan_card_r_guaranty_credit_no_ratio'] = loan_card_r[loan_card_r.guaranteeForm=='信用/免担保'].shape[0]/len(loan_card_r) # 借贷账户信息 担保方式为信用/免担保，分布所占比例
        dict_out['loan_card_r_guaranty_combine_nhave_count'] = loan_card_r[loan_card_r.guaranteeForm=='组合（不含保证）'].shape[0] # 借贷账户信息担保方式为组合（不含保证），分布计数
        dict_out['loan_card_r_guaranty_combine_nhave_ratio'] = loan_card_r[loan_card_r.guaranteeForm=='组合（不含保证）'].shape[0]/len(loan_card_r) # 借贷账户信息担保方式为组合（不含保证），分布所占比例





    return dict_out


def pboc_f(apply_info=None, pboc_info=None):
    dict_in = dict()
    dict_out = dict()
    dict_out['name'] = apply_info['name'] if 'name' in apply_info.keys() else np.nan
    dict_out['receiverAddress'] = apply_info['receiverAddress'] if 'receiverAddress' in apply_info.keys() else np.nan
    dict_out['mobile'] = apply_info['mobile'] if 'mobile' in apply_info.keys() else np.nan
    dict_out['receiverMobile'] = apply_info['receiverMobile'] if 'receiverMobile' in apply_info.keys() else np.nan
    dict_out['idCardAddress'] = apply_info['idCardAddress'] if 'idCardAddress' in apply_info.keys() else np.nan
    dict_out['loanNo'] = apply_info['loanNo'] if 'loanNo' in apply_info.keys() else np.nan
    dict_out['idcardNo'] = apply_info['idcardNo'] if 'idcardNo' in apply_info.keys() else np.nan
    dict_out['businessChannel'] = apply_info['businessChannel'] if 'businessChannel' in apply_info.keys() else np.nan
    dict_out['loan_time'] = apply_info['loan_time'] if 'loan_time' in apply_info.keys() else np.nan


    dict_in['apply_info'] = apply_info



    cc_rh_report = pd.DataFrame([pboc_info['cc_rh_report']]) if 'cc_rh_report' in pboc_info.keys() else pd.DataFrame()

    cc_rh_report['id'] = cc_rh_report['id'] if 'id' in cc_rh_report.columns else np.nan
    cc_rh_report['applyId'] = cc_rh_report['applyId'] if 'applyId' in cc_rh_report.columns else np.nan
    cc_rh_report['queryTime'] = cc_rh_report['queryTime'] if 'queryTime' in cc_rh_report.columns else np.nan
    cc_rh_report['reportTime'] = cc_rh_report['reportTime'] if 'reportTime' in cc_rh_report.columns else np.nan
    cc_rh_report['createTime'] = cc_rh_report['createTime'] if 'createTime' in cc_rh_report.columns else np.nan
    cc_rh_report['name'] = cc_rh_report['name'] if 'name' in cc_rh_report.columns else np.nan
    cc_rh_report['idType'] = cc_rh_report['idType'] if 'idType' in cc_rh_report.columns else np.nan
    cc_rh_report['idcardNo'] = cc_rh_report['idcardNo'] if 'idcardNo' in cc_rh_report.columns else np.nan
    cc_rh_report['queryOperator'] = cc_rh_report['queryOperator'] if 'queryOperator' in cc_rh_report.columns else np.nan
    cc_rh_report['queryReason'] = cc_rh_report['queryReason'] if 'queryReason' in cc_rh_report.columns else np.nan
    cc_rh_report['reportNo'] = cc_rh_report['reportNo'] if 'reportNo' in cc_rh_report.columns else np.nan

    dict_in['cc_rh_report'] = cc_rh_report
    # print("cc_rh_report:", cc_rh_report)

    cc_rh_report_customer = pd.DataFrame([pboc_info['cc_rh_report_customer']]) if 'cc_rh_report_customer' in pboc_info.keys() else pd.DataFrame()
    cc_rh_report_customer['id'] = cc_rh_report_customer['id'] if 'id' in cc_rh_report_customer.columns else np.nan
    cc_rh_report_customer['reportId'] = cc_rh_report_customer['reportId'] if 'reportId' in cc_rh_report_customer.columns else np.nan
    cc_rh_report_customer['sex'] = cc_rh_report_customer['sex'] if 'sex' in cc_rh_report_customer.columns else np.nan
    cc_rh_report_customer['birthday'] = cc_rh_report_customer['birthday'] if 'birthday' in cc_rh_report_customer.columns else np.nan
    cc_rh_report_customer['employmentStatus'] = cc_rh_report_customer['employmentStatus'] if 'employmentStatus' in cc_rh_report_customer.columns else np.nan
    cc_rh_report_customer['nationality'] = cc_rh_report_customer['nationality'] if 'nationality' in cc_rh_report_customer.columns else np.nan
    cc_rh_report_customer['email'] = cc_rh_report_customer['email'] if 'email' in cc_rh_report_customer.columns else np.nan
    cc_rh_report_customer['marry'] = cc_rh_report_customer['marry'] if 'marry' in cc_rh_report_customer.columns else np.nan
    cc_rh_report_customer['mobile'] = cc_rh_report_customer['mobile'] if 'mobile' in cc_rh_report_customer.columns else np.nan
    cc_rh_report_customer['workTelNo'] = cc_rh_report_customer['workTelNo'] if 'workTelNo' in cc_rh_report_customer.columns else np.nan
    cc_rh_report_customer['homeTelNo'] = cc_rh_report_customer['homeTelNo'] if 'homeTelNo' in cc_rh_report_customer.columns else np.nan
    cc_rh_report_customer['education'] = cc_rh_report_customer['education'] if 'education' in cc_rh_report_customer.columns else np.nan
    cc_rh_report_customer['degree'] = cc_rh_report_customer['degree'] if 'degree' in cc_rh_report_customer.columns else np.nan
    cc_rh_report_customer['messageAddress'] = cc_rh_report_customer['messageAddress'] if 'messageAddress' in cc_rh_report_customer.columns else np.nan
    cc_rh_report_customer['messageAddrLng'] = cc_rh_report_customer['messageAddrLng'] if 'messageAddrLng' in cc_rh_report_customer.columns else np.nan
    cc_rh_report_customer['messageAddrLat'] = cc_rh_report_customer['messageAddrLat'] if 'messageAddrLat' in cc_rh_report_customer.columns else np.nan
    cc_rh_report_customer['residenceAddress'] = cc_rh_report_customer['residenceAddress'] if 'residenceAddress' in cc_rh_report_customer.columns else np.nan
    dict_in['cc_rh_report_customer'] = cc_rh_report_customer
    # print("cc_rh_report_customer:", cc_rh_report_customer)

    cc_rh_report_customer_home = pd.DataFrame(pboc_info['cc_rh_report_customer_home']) if 'cc_rh_report_customer_home' in pboc_info.keys() else pd.DataFrame()
    cc_rh_report_customer_home['id'] = cc_rh_report_customer_home['id'] if 'id' in cc_rh_report_customer_home.columns else np.nan
    cc_rh_report_customer_home['reportId'] = cc_rh_report_customer_home['reportId'] if 'reportId' in cc_rh_report_customer_home.columns else np.nan
    cc_rh_report_customer_home['address'] = cc_rh_report_customer_home['address'] if 'address' in cc_rh_report_customer_home.columns else np.nan
    cc_rh_report_customer_home['addressState'] = cc_rh_report_customer_home['addressState'] if 'addressState' in cc_rh_report_customer_home.columns else np.nan
    cc_rh_report_customer_home['phone'] = cc_rh_report_customer_home['phone'] if 'phone' in cc_rh_report_customer_home.columns else np.nan
    cc_rh_report_customer_home['updateTime'] = cc_rh_report_customer_home['updateTime'] if 'updateTime' in cc_rh_report_customer_home.columns else np.nan
    dict_in['cc_rh_report_customer_home'] = cc_rh_report_customer_home
    # print("cc_rh_report_customer_home:", cc_rh_report_customer_home)

    cc_rh_report_customer_mobile = pd.DataFrame(pboc_info['cc_rh_report_customer_mobile']) if 'cc_rh_report_customer_mobile' in pboc_info.keys() else pd.DataFrame()
    cc_rh_report_customer_mobile['id'] = cc_rh_report_customer_mobile['id'] if 'id' in cc_rh_report_customer_mobile.columns else np.nan
    cc_rh_report_customer_mobile['reportId'] = cc_rh_report_customer_mobile['reportId'] if 'reportId' in cc_rh_report_customer_mobile.columns else np.nan
    cc_rh_report_customer_mobile['mobile'] = cc_rh_report_customer_mobile['mobile'] if 'mobile' in cc_rh_report_customer_mobile.columns else np.nan
    cc_rh_report_customer_mobile['updateDate'] = cc_rh_report_customer_mobile['updateDate'] if 'updateDate' in cc_rh_report_customer_mobile.columns else np.nan
    dict_in['cc_rh_report_customer_mobile'] = cc_rh_report_customer_mobile
    # print("cc_rh_report_customer_mobile:", cc_rh_report_customer_mobile)

    cc_rh_report_customer_profession = pd.DataFrame(pboc_info['cc_rh_report_customer_profession']) if 'cc_rh_report_customer_profession' in pboc_info.keys() else pd.DataFrame()
    cc_rh_report_customer_profession['id'] = cc_rh_report_customer_profession['id'] if 'id' in cc_rh_report_customer_profession.columns else np.nan
    cc_rh_report_customer_profession['reportId'] = cc_rh_report_customer_profession['reportId'] if 'reportId' in cc_rh_report_customer_profession.columns else np.nan
    cc_rh_report_customer_profession['workUnit'] = cc_rh_report_customer_profession['workUnit'] if 'workUnit' in cc_rh_report_customer_profession.columns else np.nan
    cc_rh_report_customer_profession['workUnitAddr'] = cc_rh_report_customer_profession['workUnitAddr'] if 'workUnitAddr' in cc_rh_report_customer_profession.columns else np.nan
    cc_rh_report_customer_profession['workUnitType'] = cc_rh_report_customer_profession['workUnitType'] if 'workUnitType' in cc_rh_report_customer_profession.columns else np.nan
    cc_rh_report_customer_profession['workUnitPhone'] = cc_rh_report_customer_profession['workUnitPhone'] if 'workUnitPhone' in cc_rh_report_customer_profession.columns else np.nan
    cc_rh_report_customer_profession['profession'] = cc_rh_report_customer_profession['profession'] if 'profession' in cc_rh_report_customer_profession.columns else np.nan
    cc_rh_report_customer_profession['industry'] = cc_rh_report_customer_profession['industry'] if 'industry' in cc_rh_report_customer_profession.columns else np.nan
    cc_rh_report_customer_profession['duty'] = cc_rh_report_customer_profession['duty'] if 'duty' in cc_rh_report_customer_profession.columns else np.nan
    cc_rh_report_customer_profession['technicalLevel'] = cc_rh_report_customer_profession['technicalLevel'] if 'technicalLevel' in cc_rh_report_customer_profession.columns else np.nan
    cc_rh_report_customer_profession['entryYear'] = cc_rh_report_customer_profession['entryYear'] if 'entryYear' in cc_rh_report_customer_profession.columns else np.nan
    cc_rh_report_customer_profession['updateTime'] = cc_rh_report_customer_profession['updateTime'] if 'updateTime' in cc_rh_report_customer_profession.columns else np.nan
    dict_in['cc_rh_report_customer_profession'] = cc_rh_report_customer_profession
    # print("cc_rh_report_customer_profession:", cc_rh_report_customer_profession)



    cc_rh_report_customer_spouse = pd.DataFrame([pboc_info['cc_rh_report_customer_spouse']]) if 'cc_rh_report_customer_spouse' in pboc_info.keys() else pd.DataFrame()
    cc_rh_report_customer_spouse['id'] = cc_rh_report_customer_spouse['id'] if 'id' in cc_rh_report_customer_spouse.columns else np.nan
    cc_rh_report_customer_spouse['reportId'] = cc_rh_report_customer_spouse['reportId'] if 'reportId' in cc_rh_report_customer_spouse.columns else np.nan
    cc_rh_report_customer_spouse['name'] = cc_rh_report_customer_spouse['name'] if 'name' in cc_rh_report_customer_spouse.columns else np.nan
    cc_rh_report_customer_spouse['idType'] = cc_rh_report_customer_spouse['idType'] if 'idType' in cc_rh_report_customer_spouse.columns else np.nan
    cc_rh_report_customer_spouse['idcardNo'] = cc_rh_report_customer_spouse['idcardNo'] if 'idcardNo' in cc_rh_report_customer_spouse.columns else np.nan
    cc_rh_report_customer_spouse['telephone'] = cc_rh_report_customer_spouse['telephone'] if 'telephone' in cc_rh_report_customer_spouse.columns else np.nan
    cc_rh_report_customer_spouse['workUnit'] = cc_rh_report_customer_spouse['workUnit'] if 'workUnit' in cc_rh_report_customer_spouse.columns else np.nan
    dict_in['cc_rh_report_customer_spouse'] = cc_rh_report_customer_spouse
    # print("cc_rh_report_customer_spouse:", cc_rh_report_customer_spouse)

    cc_rh_report_detail_debit_card_second = pd.DataFrame(pboc_info['cc_rh_report_detail_debit_card_second']) if 'cc_rh_report_detail_debit_card_second' in pboc_info.keys() else pd.DataFrame()
    cc_rh_report_detail_debit_card_second['id'] = cc_rh_report_detail_debit_card_second['id'] if 'id' in cc_rh_report_detail_debit_card_second.columns else np.nan
    cc_rh_report_detail_debit_card_second['reportId'] = cc_rh_report_detail_debit_card_second['reportId'] if 'reportId' in cc_rh_report_detail_debit_card_second.columns else np.nan
    cc_rh_report_detail_debit_card_second['grantOrg'] = cc_rh_report_detail_debit_card_second['grantOrg'] if 'grantOrg' in cc_rh_report_detail_debit_card_second.columns else np.nan
    cc_rh_report_detail_debit_card_second['accountLogo'] = cc_rh_report_detail_debit_card_second['accountLogo'] if 'accountLogo' in cc_rh_report_detail_debit_card_second.columns else np.nan
    cc_rh_report_detail_debit_card_second['cardGrantDate'] = cc_rh_report_detail_debit_card_second['cardGrantDate'] if 'cardGrantDate' in cc_rh_report_detail_debit_card_second.columns else np.nan
    cc_rh_report_detail_debit_card_second['amount'] = cc_rh_report_detail_debit_card_second['amount'] if 'amount' in cc_rh_report_detail_debit_card_second.columns else np.nan
    cc_rh_report_detail_debit_card_second['shareAmount'] = cc_rh_report_detail_debit_card_second['shareAmount'] if 'shareAmount' in cc_rh_report_detail_debit_card_second.columns else np.nan
    cc_rh_report_detail_debit_card_second['currency'] = cc_rh_report_detail_debit_card_second['currency'] if 'currency' in cc_rh_report_detail_debit_card_second.columns else np.nan
    cc_rh_report_detail_debit_card_second['businessType'] = cc_rh_report_detail_debit_card_second['businessType'] if 'businessType' in cc_rh_report_detail_debit_card_second.columns else np.nan
    cc_rh_report_detail_debit_card_second['guaranteeForm'] = cc_rh_report_detail_debit_card_second['guaranteeForm'] if 'guaranteeForm' in cc_rh_report_detail_debit_card_second.columns else np.nan
    cc_rh_report_detail_debit_card_second['byDate'] = cc_rh_report_detail_debit_card_second['byDate'] if 'byDate' in cc_rh_report_detail_debit_card_second.columns else np.nan
    cc_rh_report_detail_debit_card_second['accountStatus'] = cc_rh_report_detail_debit_card_second['accountStatus'] if 'accountStatus' in cc_rh_report_detail_debit_card_second.columns else np.nan
    cc_rh_report_detail_debit_card_second['balance'] = cc_rh_report_detail_debit_card_second['balance'] if 'balance' in cc_rh_report_detail_debit_card_second.columns else np.nan
    cc_rh_report_detail_debit_card_second['usedAmount'] = cc_rh_report_detail_debit_card_second['usedAmount'] if 'usedAmount' in cc_rh_report_detail_debit_card_second.columns else np.nan
    cc_rh_report_detail_debit_card_second['installmentBalance'] = cc_rh_report_detail_debit_card_second['installmentBalance'] if 'installmentBalance' in cc_rh_report_detail_debit_card_second.columns else np.nan
    cc_rh_report_detail_debit_card_second['remainingTerms'] = cc_rh_report_detail_debit_card_second['remainingTerms'] if 'remainingTerms' in cc_rh_report_detail_debit_card_second.columns else np.nan
    cc_rh_report_detail_debit_card_second['avgUsedAmount'] = cc_rh_report_detail_debit_card_second['avgUsedAmount'] if 'avgUsedAmount' in cc_rh_report_detail_debit_card_second.columns else np.nan
    cc_rh_report_detail_debit_card_second['maxUsedAmount'] = cc_rh_report_detail_debit_card_second['maxUsedAmount'] if 'maxUsedAmount' in cc_rh_report_detail_debit_card_second.columns else np.nan
    cc_rh_report_detail_debit_card_second['statementDate'] = cc_rh_report_detail_debit_card_second['statementDate'] if 'statementDate' in cc_rh_report_detail_debit_card_second.columns else np.nan
    cc_rh_report_detail_debit_card_second['planRepayAmount'] = cc_rh_report_detail_debit_card_second['planRepayAmount'] if 'planRepayAmount' in cc_rh_report_detail_debit_card_second.columns else np.nan
    cc_rh_report_detail_debit_card_second['repayedAmount'] = cc_rh_report_detail_debit_card_second['repayedAmount'] if 'repayedAmount' in cc_rh_report_detail_debit_card_second.columns else np.nan
    cc_rh_report_detail_debit_card_second['recentRepayDate'] = cc_rh_report_detail_debit_card_second['recentRepayDate'] if 'recentRepayDate' in cc_rh_report_detail_debit_card_second.columns else np.nan
    cc_rh_report_detail_debit_card_second['closedDate'] = cc_rh_report_detail_debit_card_second['closedDate'] if 'closedDate' in cc_rh_report_detail_debit_card_second.columns else np.nan
    cc_rh_report_detail_debit_card_second['currentOverdueTerms'] = cc_rh_report_detail_debit_card_second['currentOverdueTerms'] if 'currentOverdueTerms' in cc_rh_report_detail_debit_card_second.columns else np.nan
    cc_rh_report_detail_debit_card_second['currentOverdueAmount'] = cc_rh_report_detail_debit_card_second['currentOverdueAmount'] if 'currentOverdueAmount' in cc_rh_report_detail_debit_card_second.columns else np.nan
    cc_rh_report_detail_debit_card_second['unPayAmount'] = cc_rh_report_detail_debit_card_second['unPayAmount'] if 'unPayAmount' in cc_rh_report_detail_debit_card_second.columns else np.nan
    cc_rh_report_detail_debit_card_second['month60State'] = cc_rh_report_detail_debit_card_second['month60State'] if 'month60State' in cc_rh_report_detail_debit_card_second.columns else np.nan
    cc_rh_report_detail_debit_card_second['month60Amount'] = cc_rh_report_detail_debit_card_second['month60Amount'] if 'month60Amount' in cc_rh_report_detail_debit_card_second.columns else np.nan
    cc_rh_report_detail_debit_card_second['month60Desc'] = cc_rh_report_detail_debit_card_second['month60Desc'] if 'month60Desc' in cc_rh_report_detail_debit_card_second.columns else np.nan
    cc_rh_report_detail_debit_card_second['specialDesc'] = cc_rh_report_detail_debit_card_second['specialDesc'] if 'specialDesc' in cc_rh_report_detail_debit_card_second.columns else np.nan
    cc_rh_report_detail_debit_card_second['orgExplain'] = cc_rh_report_detail_debit_card_second['orgExplain'] if 'orgExplain' in cc_rh_report_detail_debit_card_second.columns else np.nan
    cc_rh_report_detail_debit_card_second['orgExplainDate'] = cc_rh_report_detail_debit_card_second['orgExplainDate'] if 'orgExplainDate' in cc_rh_report_detail_debit_card_second.columns else np.nan
    cc_rh_report_detail_debit_card_second['selfDeclare'] = cc_rh_report_detail_debit_card_second['selfDeclare'] if 'selfDeclare' in cc_rh_report_detail_debit_card_second.columns else np.nan
    cc_rh_report_detail_debit_card_second['selfDeclareDate'] = cc_rh_report_detail_debit_card_second['selfDeclareDate'] if 'selfDeclareDate' in cc_rh_report_detail_debit_card_second.columns else np.nan
    cc_rh_report_detail_debit_card_second['dissentTagging'] = cc_rh_report_detail_debit_card_second['dissentTagging'] if 'dissentTagging' in cc_rh_report_detail_debit_card_second.columns else np.nan
    cc_rh_report_detail_debit_card_second['dissentTaggingDate'] = cc_rh_report_detail_debit_card_second['dissentTaggingDate'] if 'dissentTaggingDate' in cc_rh_report_detail_debit_card_second.columns else np.nan
    cc_rh_report_detail_debit_card_second['specialTagging'] = cc_rh_report_detail_debit_card_second['specialTagging'] if 'specialTagging' in cc_rh_report_detail_debit_card_second.columns else np.nan
    cc_rh_report_detail_debit_card_second['specailTaggingDate'] = cc_rh_report_detail_debit_card_second['specailTaggingDate'] if 'specailTaggingDate' in cc_rh_report_detail_debit_card_second.columns else np.nan
    dict_in['cc_rh_report_detail_debit_card_second'] = cc_rh_report_detail_debit_card_second
    # print("cc_rh_report_detail_debit_card_second:", cc_rh_report_detail_debit_card_second)



    cc_rh_report_detail_loan_second = pd.DataFrame(pboc_info['cc_rh_report_detail_loan_second']) if 'cc_rh_report_detail_loan_second' in pboc_info.keys() else pd.DataFrame()
    cc_rh_report_detail_loan_second['id'] = cc_rh_report_detail_loan_second['id'] if 'id' in cc_rh_report_detail_loan_second.columns else np.nan
    cc_rh_report_detail_loan_second['reportId'] = cc_rh_report_detail_loan_second['reportId'] if 'reportId' in cc_rh_report_detail_loan_second.columns else np.nan
    cc_rh_report_detail_loan_second['loanType'] = cc_rh_report_detail_loan_second['loanType'] if 'loanType' in cc_rh_report_detail_loan_second.columns else np.nan
    cc_rh_report_detail_loan_second['loanGrantOrg'] = cc_rh_report_detail_loan_second['loanGrantOrg'] if 'loanGrantOrg' in cc_rh_report_detail_loan_second.columns else np.nan
    cc_rh_report_detail_loan_second['accountLogo'] = cc_rh_report_detail_loan_second['accountLogo'] if 'accountLogo' in cc_rh_report_detail_loan_second.columns else np.nan
    cc_rh_report_detail_loan_second['startDate'] = cc_rh_report_detail_loan_second['startDate'] if 'startDate' in cc_rh_report_detail_loan_second.columns else np.nan
    cc_rh_report_detail_loan_second['endDate'] = cc_rh_report_detail_loan_second['endDate'] if 'endDate' in cc_rh_report_detail_loan_second.columns else np.nan
    cc_rh_report_detail_loan_second['loanAmount'] = cc_rh_report_detail_loan_second['loanAmount'] if 'loanAmount' in cc_rh_report_detail_loan_second.columns else np.nan
    cc_rh_report_detail_loan_second['currency'] = cc_rh_report_detail_loan_second['currency'] if 'currency' in cc_rh_report_detail_loan_second.columns else np.nan
    cc_rh_report_detail_loan_second['businessType'] = cc_rh_report_detail_loan_second['businessType'] if 'businessType' in cc_rh_report_detail_loan_second.columns else np.nan
    cc_rh_report_detail_loan_second['guaranteeForm'] = cc_rh_report_detail_loan_second['guaranteeForm'] if 'guaranteeForm' in cc_rh_report_detail_loan_second.columns else np.nan
    cc_rh_report_detail_loan_second['repayTerms'] = cc_rh_report_detail_loan_second['repayTerms'] if 'repayTerms' in cc_rh_report_detail_loan_second.columns else np.nan
    cc_rh_report_detail_loan_second['repayFrequency'] = cc_rh_report_detail_loan_second['repayFrequency'] if 'repayFrequency' in cc_rh_report_detail_loan_second.columns else np.nan
    cc_rh_report_detail_loan_second['repayType'] = cc_rh_report_detail_loan_second['repayType'] if 'repayType' in cc_rh_report_detail_loan_second.columns else np.nan
    cc_rh_report_detail_loan_second['mutualFlag'] = cc_rh_report_detail_loan_second['mutualFlag'] if 'mutualFlag' in cc_rh_report_detail_loan_second.columns else np.nan
    cc_rh_report_detail_loan_second['byDate'] = cc_rh_report_detail_loan_second['byDate'] if 'byDate' in cc_rh_report_detail_loan_second.columns else np.nan
    cc_rh_report_detail_loan_second['accountStatus'] = cc_rh_report_detail_loan_second['accountStatus'] if 'accountStatus' in cc_rh_report_detail_loan_second.columns else np.nan
    cc_rh_report_detail_loan_second['classify5'] = cc_rh_report_detail_loan_second['classify5'] if 'classify5' in cc_rh_report_detail_loan_second.columns else np.nan
    cc_rh_report_detail_loan_second['balance'] = cc_rh_report_detail_loan_second['balance'] if 'balance' in cc_rh_report_detail_loan_second.columns else np.nan
    cc_rh_report_detail_loan_second['leftRepayTerms'] = cc_rh_report_detail_loan_second['leftRepayTerms'] if 'leftRepayTerms' in cc_rh_report_detail_loan_second.columns else np.nan
    cc_rh_report_detail_loan_second['planRepayAmount'] = cc_rh_report_detail_loan_second['planRepayAmount'] if 'planRepayAmount' in cc_rh_report_detail_loan_second.columns else np.nan
    cc_rh_report_detail_loan_second['planRepayDate'] = cc_rh_report_detail_loan_second['planRepayDate'] if 'planRepayDate' in cc_rh_report_detail_loan_second.columns else np.nan
    cc_rh_report_detail_loan_second['RepayedAmount'] = cc_rh_report_detail_loan_second['RepayedAmount'] if 'RepayedAmount' in cc_rh_report_detail_loan_second.columns else np.nan
    cc_rh_report_detail_loan_second['recentRepayDate'] = cc_rh_report_detail_loan_second['recentRepayDate'] if 'recentRepayDate' in cc_rh_report_detail_loan_second.columns else np.nan
    cc_rh_report_detail_loan_second['outDate'] = cc_rh_report_detail_loan_second['outDate'] if 'outDate' in cc_rh_report_detail_loan_second.columns else np.nan
    cc_rh_report_detail_loan_second['currentOverdueTerms'] = cc_rh_report_detail_loan_second['currentOverdueTerms'] if 'currentOverdueTerms' in cc_rh_report_detail_loan_second.columns else np.nan
    cc_rh_report_detail_loan_second['currentOverdueAmount'] = cc_rh_report_detail_loan_second['currentOverdueAmount'] if 'currentOverdueAmount' in cc_rh_report_detail_loan_second.columns else np.nan
    cc_rh_report_detail_loan_second['overdue31Amount'] = cc_rh_report_detail_loan_second['overdue31Amount'] if 'overdue31Amount' in cc_rh_report_detail_loan_second.columns else np.nan
    cc_rh_report_detail_loan_second['overdue61Amount'] = cc_rh_report_detail_loan_second['overdue61Amount'] if 'overdue61Amount' in cc_rh_report_detail_loan_second.columns else np.nan
    cc_rh_report_detail_loan_second['overdue91Amount'] = cc_rh_report_detail_loan_second['overdue91Amount'] if 'overdue91Amount' in cc_rh_report_detail_loan_second.columns else np.nan
    cc_rh_report_detail_loan_second['overdue180Amount'] = cc_rh_report_detail_loan_second['overdue180Amount'] if 'overdue180Amount' in cc_rh_report_detail_loan_second.columns else np.nan
    cc_rh_report_detail_loan_second['month60State'] = cc_rh_report_detail_loan_second['month60State'] if 'month60State' in cc_rh_report_detail_loan_second.columns else np.nan
    cc_rh_report_detail_loan_second['month60Amount'] = cc_rh_report_detail_loan_second['month60Amount'] if 'month60Amount' in cc_rh_report_detail_loan_second.columns else np.nan
    cc_rh_report_detail_loan_second['month60Desc'] = cc_rh_report_detail_loan_second['month60Desc'] if 'month60Desc' in cc_rh_report_detail_loan_second.columns else np.nan
    cc_rh_report_detail_loan_second['orgExplain'] = cc_rh_report_detail_loan_second['orgExplain'] if 'orgExplain' in cc_rh_report_detail_loan_second.columns else np.nan
    cc_rh_report_detail_loan_second['orgExplainDate'] = cc_rh_report_detail_loan_second['orgExplainDate'] if 'orgExplainDate' in cc_rh_report_detail_loan_second.columns else np.nan
    cc_rh_report_detail_loan_second['selfDeclare'] = cc_rh_report_detail_loan_second['selfDeclare'] if 'selfDeclare' in cc_rh_report_detail_loan_second.columns else np.nan
    cc_rh_report_detail_loan_second['selfDeclareDate'] = cc_rh_report_detail_loan_second['selfDeclareDate'] if 'selfDeclareDate' in cc_rh_report_detail_loan_second.columns else np.nan
    cc_rh_report_detail_loan_second['dissentTagging'] = cc_rh_report_detail_loan_second['dissentTagging'] if 'dissentTagging' in cc_rh_report_detail_loan_second.columns else np.nan
    cc_rh_report_detail_loan_second['dissentTaggingDate'] = cc_rh_report_detail_loan_second['dissentTaggingDate'] if 'dissentTaggingDate' in cc_rh_report_detail_loan_second.columns else np.nan
    cc_rh_report_detail_loan_second['specialTagging'] = cc_rh_report_detail_loan_second['specialTagging'] if 'specialTagging' in cc_rh_report_detail_loan_second.columns else np.nan
    cc_rh_report_detail_loan_second['specailTaggingDate'] = cc_rh_report_detail_loan_second['specailTaggingDate'] if 'specailTaggingDate' in cc_rh_report_detail_loan_second.columns else np.nan
    cc_rh_report_detail_loan_second['closeDate'] = cc_rh_report_detail_loan_second['closeDate'] if 'closeDate' in cc_rh_report_detail_loan_second.columns else np.nan
    dict_in['cc_rh_report_detail_loan_second'] = cc_rh_report_detail_loan_second
    # print("cc_rh_report_detail_loan_second:", cc_rh_report_detail_loan_second)
    # print(cc_rh_report_detail_loan_second['accountStatus'].values.tolist())



    cc_rh_report_detail_recovery = pd.DataFrame(pboc_info['cc_rh_report_detail_recovery']) if 'cc_rh_report_detail_recovery' in pboc_info.keys() else pd.DataFrame()
    cc_rh_report_detail_recovery['id'] = cc_rh_report_detail_recovery['id'] if 'id' in cc_rh_report_detail_recovery.columns else np.nan
    cc_rh_report_detail_recovery['reportId'] = cc_rh_report_detail_recovery['reportId'] if 'reportId' in cc_rh_report_detail_recovery.columns else np.nan
    cc_rh_report_detail_recovery['orgName'] = cc_rh_report_detail_recovery['orgName'] if 'orgName' in cc_rh_report_detail_recovery.columns else np.nan
    cc_rh_report_detail_recovery['businessType'] = cc_rh_report_detail_recovery['businessType'] if 'businessType' in cc_rh_report_detail_recovery.columns else np.nan
    cc_rh_report_detail_recovery['rightsReceiveDate'] = cc_rh_report_detail_recovery['rightsReceiveDate'] if 'rightsReceiveDate' in cc_rh_report_detail_recovery.columns else np.nan
    cc_rh_report_detail_recovery['rightsAmount'] = cc_rh_report_detail_recovery['rightsAmount'] if 'rightsAmount' in cc_rh_report_detail_recovery.columns else np.nan
    cc_rh_report_detail_recovery['repaymentStatus'] = cc_rh_report_detail_recovery['repaymentStatus'] if 'repaymentStatus' in cc_rh_report_detail_recovery.columns else np.nan
    cc_rh_report_detail_recovery['byDate'] = cc_rh_report_detail_recovery['byDate'] if 'byDate' in cc_rh_report_detail_recovery.columns else np.nan
    cc_rh_report_detail_recovery['accountStatus'] = cc_rh_report_detail_recovery['accountStatus'] if 'accountStatus' in cc_rh_report_detail_recovery.columns else np.nan
    cc_rh_report_detail_recovery['balance'] = cc_rh_report_detail_recovery['balance'] if 'balance' in cc_rh_report_detail_recovery.columns else np.nan
    cc_rh_report_detail_recovery['lastRepayDate'] = cc_rh_report_detail_recovery['lastRepayDate'] if 'lastRepayDate' in cc_rh_report_detail_recovery.columns else np.nan
    cc_rh_report_detail_recovery['closedDate'] = cc_rh_report_detail_recovery['closedDate'] if 'closedDate' in cc_rh_report_detail_recovery.columns else np.nan
    dict_in['cc_rh_report_detail_recovery'] = cc_rh_report_detail_recovery
    # print("cc_rh_report_detail_recovery:", cc_rh_report_detail_recovery)


    cc_rh_report_dissent_tips = pd.DataFrame([pboc_info['cc_rh_report_dissent_tips']]) if 'cc_rh_report_dissent_tips' in pboc_info.keys() else pd.DataFrame()
    cc_rh_report_dissent_tips['id'] = cc_rh_report_dissent_tips['id'] if 'id' in cc_rh_report_dissent_tips.columns else np.nan
    cc_rh_report_dissent_tips['reportId'] = cc_rh_report_dissent_tips['reportId'] if 'reportId' in cc_rh_report_dissent_tips.columns else np.nan
    cc_rh_report_dissent_tips['content'] = cc_rh_report_dissent_tips['content'] if 'content' in cc_rh_report_dissent_tips.columns else np.nan
    dict_in['cc_rh_report_dissent_tips'] = cc_rh_report_dissent_tips
    # print("cc_rh_report_dissent_tips:", cc_rh_report_dissent_tips)


    cc_rh_report_fraud_warn = pd.DataFrame([pboc_info['cc_rh_report_fraud_warn']]) if 'cc_rh_report_fraud_warn' in pboc_info.keys() else pd.DataFrame()
    cc_rh_report_fraud_warn['id'] = cc_rh_report_fraud_warn['id'] if 'id' in cc_rh_report_fraud_warn.columns else np.nan
    cc_rh_report_fraud_warn['reportId'] = cc_rh_report_fraud_warn['reportId'] if 'reportId' in cc_rh_report_fraud_warn.columns else np.nan
    cc_rh_report_fraud_warn['content'] = cc_rh_report_fraud_warn['content'] if 'content' in cc_rh_report_fraud_warn.columns else np.nan
    cc_rh_report_fraud_warn['startDate'] = cc_rh_report_fraud_warn['startDate'] if 'startDate' in cc_rh_report_fraud_warn.columns else np.nan
    cc_rh_report_fraud_warn['endDate'] = cc_rh_report_fraud_warn['endDate'] if 'endDate' in cc_rh_report_fraud_warn.columns else np.nan
    dict_in['cc_rh_report_fraud_warn'] = cc_rh_report_fraud_warn
    # print("cc_rh_report_fraud_warn:", cc_rh_report_fraud_warn)

    cc_rh_report_loan_last_repay = pd.DataFrame([pboc_info['cc_rh_report_loan_last_repay']]) if 'cc_rh_report_loan_last_repay' in pboc_info.keys() else pd.DataFrame()
    cc_rh_report_loan_last_repay['id'] = cc_rh_report_loan_last_repay['id'] if 'id' in cc_rh_report_loan_last_repay.columns else np.nan
    cc_rh_report_loan_last_repay['reportId'] = cc_rh_report_loan_last_repay['reportId'] if 'reportId' in cc_rh_report_loan_last_repay.columns else np.nan
    cc_rh_report_loan_last_repay['loanId'] = cc_rh_report_loan_last_repay['loanId'] if 'loanId' in cc_rh_report_loan_last_repay.columns else np.nan
    cc_rh_report_loan_last_repay['classify5'] = cc_rh_report_loan_last_repay['classify5'] if 'classify5' in cc_rh_report_loan_last_repay.columns else np.nan
    cc_rh_report_loan_last_repay['balance'] = cc_rh_report_loan_last_repay['balance'] if 'balance' in cc_rh_report_loan_last_repay.columns else np.nan
    cc_rh_report_loan_last_repay['repayDate'] = cc_rh_report_loan_last_repay['repayDate'] if 'repayDate' in cc_rh_report_loan_last_repay.columns else np.nan
    cc_rh_report_loan_last_repay['repayAmount'] = cc_rh_report_loan_last_repay['repayAmount'] if 'repayAmount' in cc_rh_report_loan_last_repay.columns else np.nan
    cc_rh_report_loan_last_repay['repayStatus'] = cc_rh_report_loan_last_repay['repayStatus'] if 'repayStatus' in cc_rh_report_loan_last_repay.columns else np.nan
    dict_in['cc_rh_report_loan_last_repay'] = cc_rh_report_loan_last_repay
    # print("cc_rh_report_loan_last_repay:", cc_rh_report_loan_last_repay)



    cc_rh_report_loan_special_detail_second = pd.DataFrame(pboc_info['cc_rh_report_loan_special_detail_second']) if 'cc_rh_report_loan_special_detail_second' in pboc_info.keys() else pd.DataFrame()
    cc_rh_report_loan_special_detail_second['id'] = cc_rh_report_loan_special_detail_second['id'] if 'id' in cc_rh_report_loan_special_detail_second.columns else np.nan
    cc_rh_report_loan_special_detail_second['reportId'] = cc_rh_report_loan_special_detail_second['reportId'] if 'reportId' in cc_rh_report_loan_special_detail_second.columns else np.nan
    cc_rh_report_loan_special_detail_second['loanId'] = cc_rh_report_loan_special_detail_second['loanId'] if 'loanId' in cc_rh_report_loan_special_detail_second.columns else np.nan
    cc_rh_report_loan_special_detail_second['specialTradeType'] = cc_rh_report_loan_special_detail_second['specialTradeType'] if 'specialTradeType' in cc_rh_report_loan_special_detail_second.columns else np.nan
    cc_rh_report_loan_special_detail_second['specialTradeDate'] = cc_rh_report_loan_special_detail_second['specialTradeDate'] if 'specialTradeDate' in cc_rh_report_loan_special_detail_second.columns else np.nan
    cc_rh_report_loan_special_detail_second['specialTradeChangeMonth'] = cc_rh_report_loan_special_detail_second['specialTradeChangeMonth'] if 'specialTradeChangeMonth' in cc_rh_report_loan_special_detail_second.columns else np.nan
    cc_rh_report_loan_special_detail_second['specialTradeAmount'] = cc_rh_report_loan_special_detail_second['specialTradeAmount'] if 'specialTradeAmount' in cc_rh_report_loan_special_detail_second.columns else np.nan
    cc_rh_report_loan_special_detail_second['specialTradeDetail'] = cc_rh_report_loan_special_detail_second['specialTradeDetail'] if 'specialTradeDetail' in cc_rh_report_loan_special_detail_second.columns else np.nan
    dict_in['cc_rh_report_loan_special_detail_second'] = cc_rh_report_loan_special_detail_second
    # print("cc_rh_report_loan_special_detail_second:", cc_rh_report_loan_special_detail_second)

    cc_rh_report_public_court = pd.DataFrame(pboc_info['cc_rh_report_public_court']) if 'cc_rh_report_public_court' in pboc_info.keys() else pd.DataFrame()
    cc_rh_report_public_court['id'] = cc_rh_report_public_court['id'] if 'id' in cc_rh_report_public_court.columns else np.nan
    cc_rh_report_public_court['reportId'] = cc_rh_report_public_court['reportId'] if 'reportId' in cc_rh_report_public_court.columns else np.nan
    cc_rh_report_public_court['courtName'] = cc_rh_report_public_court['courtName'] if 'courtName' in cc_rh_report_public_court.columns else np.nan
    cc_rh_report_public_court['caseReason'] = cc_rh_report_public_court['caseReason'] if 'caseReason' in cc_rh_report_public_court.columns else np.nan
    cc_rh_report_public_court['caseBeginDate'] = cc_rh_report_public_court['caseBeginDate'] if 'caseBeginDate' in cc_rh_report_public_court.columns else np.nan
    cc_rh_report_public_court['caseEndWay'] = cc_rh_report_public_court['caseEndWay'] if 'caseEndWay' in cc_rh_report_public_court.columns else np.nan
    cc_rh_report_public_court['caseStatus'] = cc_rh_report_public_court['caseStatus'] if 'caseStatus' in cc_rh_report_public_court.columns else np.nan
    cc_rh_report_public_court['caseEndDate'] = cc_rh_report_public_court['caseEndDate'] if 'caseEndDate' in cc_rh_report_public_court.columns else np.nan
    cc_rh_report_public_court['caseTarget'] = cc_rh_report_public_court['caseTarget'] if 'caseTarget' in cc_rh_report_public_court.columns else np.nan
    cc_rh_report_public_court['caseTargetAmount'] = cc_rh_report_public_court['caseTargetAmount'] if 'caseTargetAmount' in cc_rh_report_public_court.columns else np.nan
    cc_rh_report_public_court['executeTarget'] = cc_rh_report_public_court['executeTarget'] if 'executeTarget' in cc_rh_report_public_court.columns else np.nan
    cc_rh_report_public_court['executedAmount'] = cc_rh_report_public_court['executedAmount'] if 'executedAmount' in cc_rh_report_public_court.columns else np.nan
    dict_in['cc_rh_report_public_court'] = cc_rh_report_public_court
    # print("cc_rh_report_public_court:", cc_rh_report_public_court)



    cc_rh_report_public_housefund = pd.DataFrame(pboc_info['cc_rh_report_public_housefund']) if 'cc_rh_report_public_housefund' in pboc_info.keys() else pd.DataFrame()
    cc_rh_report_public_housefund['id'] = cc_rh_report_public_housefund['id'] if 'id' in cc_rh_report_public_housefund.columns else np.nan
    cc_rh_report_public_housefund['reportId'] = cc_rh_report_public_housefund['reportId'] if 'reportId' in cc_rh_report_public_housefund.columns else np.nan
    cc_rh_report_public_housefund['payArea'] = cc_rh_report_public_housefund['payArea'] if 'payArea' in cc_rh_report_public_housefund.columns else np.nan
    cc_rh_report_public_housefund['payDate'] = cc_rh_report_public_housefund['payDate'] if 'payDate' in cc_rh_report_public_housefund.columns else np.nan
    cc_rh_report_public_housefund['payFirstDate'] = cc_rh_report_public_housefund['payFirstDate'] if 'payFirstDate' in cc_rh_report_public_housefund.columns else np.nan
    cc_rh_report_public_housefund['payEndDate'] = cc_rh_report_public_housefund['payEndDate'] if 'payEndDate' in cc_rh_report_public_housefund.columns else np.nan
    cc_rh_report_public_housefund['payStatus'] = cc_rh_report_public_housefund['payStatus'] if 'payStatus' in cc_rh_report_public_housefund.columns else np.nan
    cc_rh_report_public_housefund['payMonthAmount'] = cc_rh_report_public_housefund['payMonthAmount'] if 'payMonthAmount' in cc_rh_report_public_housefund.columns else np.nan
    cc_rh_report_public_housefund['payPersonPercent'] = cc_rh_report_public_housefund['payPersonPercent'] if 'payPersonPercent' in cc_rh_report_public_housefund.columns else np.nan
    cc_rh_report_public_housefund['payWorkUnitPercent'] = cc_rh_report_public_housefund['payWorkUnitPercent'] if 'payWorkUnitPercent' in cc_rh_report_public_housefund.columns else np.nan
    cc_rh_report_public_housefund['payWorkUnit'] = cc_rh_report_public_housefund['payWorkUnit'] if 'payWorkUnit' in cc_rh_report_public_housefund.columns else np.nan
    cc_rh_report_public_housefund['updateDate'] = cc_rh_report_public_housefund['updateDate'] if 'updateDate' in cc_rh_report_public_housefund.columns else np.nan
    dict_in['cc_rh_report_public_housefund'] = cc_rh_report_public_housefund
    # print("cc_rh_report_public_housefund:", cc_rh_report_public_housefund)



    cc_rh_report_query_detail = pd.DataFrame(pboc_info['cc_rh_report_query_detail']) if 'cc_rh_report_query_detail' in pboc_info.keys() else pd.DataFrame()
    cc_rh_report_query_detail['id'] = cc_rh_report_query_detail['id'] if 'id' in cc_rh_report_query_detail.columns else np.nan
    cc_rh_report_query_detail['reportId'] = cc_rh_report_query_detail['reportId'] if 'reportId' in cc_rh_report_query_detail.columns else np.nan
    cc_rh_report_query_detail['reportNo'] = cc_rh_report_query_detail['reportNo'] if 'reportNo' in cc_rh_report_query_detail.columns else np.nan
    cc_rh_report_query_detail['queryDate'] = cc_rh_report_query_detail['queryDate'] if 'queryDate' in cc_rh_report_query_detail.columns else np.nan
    cc_rh_report_query_detail['queryOperator'] = cc_rh_report_query_detail['queryOperator'] if 'queryOperator' in cc_rh_report_query_detail.columns else np.nan
    cc_rh_report_query_detail['queryReason'] = cc_rh_report_query_detail['queryReason'] if 'queryReason' in cc_rh_report_query_detail.columns else np.nan
    dict_in['cc_rh_report_query_detail'] = cc_rh_report_query_detail
    # print("cc_rh_report_query_detail:", cc_rh_report_query_detail)

    cc_rh_report_query_summary = pd.DataFrame([pboc_info['cc_rh_report_query_summary']]) if 'cc_rh_report_query_summary' in pboc_info.keys() else pd.DataFrame()
    cc_rh_report_query_summary['id'] = cc_rh_report_query_summary['id'] if 'id' in cc_rh_report_query_summary.columns else np.nan
    cc_rh_report_query_summary['reportId'] = cc_rh_report_query_summary['reportId'] if 'reportId' in cc_rh_report_query_summary.columns else np.nan
    cc_rh_report_query_summary['loanQueryOrgCount'] = cc_rh_report_query_summary['loanQueryOrgCount'] if 'loanQueryOrgCount' in cc_rh_report_query_summary.columns else np.nan
    cc_rh_report_query_summary['cardQueryOrgCount'] = cc_rh_report_query_summary['cardQueryOrgCount'] if 'cardQueryOrgCount' in cc_rh_report_query_summary.columns else np.nan
    cc_rh_report_query_summary['loanQueryCount'] = cc_rh_report_query_summary['loanQueryCount'] if 'loanQueryCount' in cc_rh_report_query_summary.columns else np.nan
    cc_rh_report_query_summary['cardQueryCount'] = cc_rh_report_query_summary['cardQueryCount'] if 'cardQueryCount' in cc_rh_report_query_summary.columns else np.nan
    cc_rh_report_query_summary['selfQueryCount'] = cc_rh_report_query_summary['selfQueryCount'] if 'selfQueryCount' in cc_rh_report_query_summary.columns else np.nan
    cc_rh_report_query_summary['loanAfterQueryCount'] = cc_rh_report_query_summary['loanAfterQueryCount'] if 'loanAfterQueryCount' in cc_rh_report_query_summary.columns else np.nan
    cc_rh_report_query_summary['guaranteeQueryCount'] = cc_rh_report_query_summary['guaranteeQueryCount'] if 'guaranteeQueryCount' in cc_rh_report_query_summary.columns else np.nan
    cc_rh_report_query_summary['bussinessRealNameQueryCount'] = cc_rh_report_query_summary['bussinessRealNameQueryCount'] if 'bussinessRealNameQueryCount' in cc_rh_report_query_summary.columns else np.nan
    dict_in['cc_rh_report_query_summary'] = cc_rh_report_query_summary




    cc_rh_report_summary_credit_tips = pd.DataFrame([pboc_info['cc_rh_report_summary_credit_tips']]) if 'cc_rh_report_summary_credit_tips' in pboc_info.keys() else pd.DataFrame()
    cc_rh_report_summary_credit_tips['id'] = cc_rh_report_summary_credit_tips['id'] if 'id' in cc_rh_report_summary_credit_tips.columns else np.nan
    cc_rh_report_summary_credit_tips['reportId'] = cc_rh_report_summary_credit_tips['reportId'] if 'reportId' in cc_rh_report_summary_credit_tips.columns else np.nan
    cc_rh_report_summary_credit_tips['houseLoanCount'] = cc_rh_report_summary_credit_tips['houseLoanCount'] if 'houseLoanCount' in cc_rh_report_summary_credit_tips.columns else np.nan
    cc_rh_report_summary_credit_tips['houseLoanFirstMonth'] = cc_rh_report_summary_credit_tips['houseLoanFirstMonth'] if 'houseLoanFirstMonth' in cc_rh_report_summary_credit_tips.columns else np.nan
    cc_rh_report_summary_credit_tips['commercialHouseLoanCount'] = cc_rh_report_summary_credit_tips['commercialHouseLoanCount'] if 'commercialHouseLoanCount' in cc_rh_report_summary_credit_tips.columns else np.nan
    cc_rh_report_summary_credit_tips['commercialHouseLoanFirstMonth'] = cc_rh_report_summary_credit_tips['commercialHouseLoanFirstMonth'] if 'commercialHouseLoanFirstMonth' in cc_rh_report_summary_credit_tips.columns else np.nan
    cc_rh_report_summary_credit_tips['otherLoanCount'] = cc_rh_report_summary_credit_tips['otherLoanCount'] if 'otherLoanCount' in cc_rh_report_summary_credit_tips.columns else np.nan
    cc_rh_report_summary_credit_tips['otherLoanFirstMonth'] = cc_rh_report_summary_credit_tips['otherLoanFirstMonth'] if 'otherLoanFirstMonth' in cc_rh_report_summary_credit_tips.columns else np.nan
    cc_rh_report_summary_credit_tips['firstLoanMonth'] = cc_rh_report_summary_credit_tips['firstLoanMonth'] if 'firstLoanMonth' in cc_rh_report_summary_credit_tips.columns else np.nan
    cc_rh_report_summary_credit_tips['creditCardCount'] = cc_rh_report_summary_credit_tips['creditCardCount'] if 'creditCardCount' in cc_rh_report_summary_credit_tips.columns else np.nan
    cc_rh_report_summary_credit_tips['firstCreditCardMonth'] = cc_rh_report_summary_credit_tips['firstCreditCardMonth'] if 'firstCreditCardMonth' in cc_rh_report_summary_credit_tips.columns else np.nan
    cc_rh_report_summary_credit_tips['readyCardCount'] = cc_rh_report_summary_credit_tips['readyCardCount'] if 'readyCardCount' in cc_rh_report_summary_credit_tips.columns else np.nan
    cc_rh_report_summary_credit_tips['firstReadyCardMonth'] = cc_rh_report_summary_credit_tips['firstReadyCardMonth'] if 'firstReadyCardMonth' in cc_rh_report_summary_credit_tips.columns else np.nan
    cc_rh_report_summary_credit_tips['otherCount'] = cc_rh_report_summary_credit_tips['otherCount'] if 'otherCount' in cc_rh_report_summary_credit_tips.columns else np.nan
    cc_rh_report_summary_credit_tips['otherFirstMonth'] = cc_rh_report_summary_credit_tips['otherFirstMonth'] if 'otherFirstMonth' in cc_rh_report_summary_credit_tips.columns else np.nan
    cc_rh_report_summary_credit_tips['declareCount'] = cc_rh_report_summary_credit_tips['declareCount'] if 'declareCount' in cc_rh_report_summary_credit_tips.columns else np.nan
    cc_rh_report_summary_credit_tips['dissentCount'] = cc_rh_report_summary_credit_tips['dissentCount'] if 'dissentCount' in cc_rh_report_summary_credit_tips.columns else np.nan
    dict_in['cc_rh_report_summary_credit_tips'] = cc_rh_report_summary_credit_tips
    # print("cc_rh_report_summary_credit_tips:", cc_rh_report_summary_credit_tips)


    cc_rh_report_summary_debt_card = pd.DataFrame([pboc_info['cc_rh_report_summary_debt_card']]) if 'cc_rh_report_summary_debt_card' in pboc_info.keys() else pd.DataFrame()
    cc_rh_report_summary_debt_card['id'] = cc_rh_report_summary_debt_card['id'] if 'id' in cc_rh_report_summary_debt_card.columns else np.nan
    cc_rh_report_summary_debt_card['reportId'] = cc_rh_report_summary_debt_card['reportId'] if 'reportId' in cc_rh_report_summary_debt_card.columns else np.nan
    cc_rh_report_summary_debt_card['businessType'] = cc_rh_report_summary_debt_card['businessType'] if 'businessType' in cc_rh_report_summary_debt_card.columns else np.nan
    cc_rh_report_summary_debt_card['orgCount'] = cc_rh_report_summary_debt_card['orgCount'] if 'orgCount' in cc_rh_report_summary_debt_card.columns else np.nan
    cc_rh_report_summary_debt_card['accountCount'] = cc_rh_report_summary_debt_card['accountCount'] if 'accountCount' in cc_rh_report_summary_debt_card.columns else np.nan
    cc_rh_report_summary_debt_card['creditTotalAmount'] = cc_rh_report_summary_debt_card['creditTotalAmount'] if 'creditTotalAmount' in cc_rh_report_summary_debt_card.columns else np.nan
    cc_rh_report_summary_debt_card['creditMaxAmount'] = cc_rh_report_summary_debt_card['creditMaxAmount'] if 'creditMaxAmount' in cc_rh_report_summary_debt_card.columns else np.nan
    cc_rh_report_summary_debt_card['creditMinAmount'] = cc_rh_report_summary_debt_card['creditMinAmount'] if 'creditMinAmount' in cc_rh_report_summary_debt_card.columns else np.nan
    cc_rh_report_summary_debt_card['usedAmount'] = cc_rh_report_summary_debt_card['usedAmount'] if 'usedAmount' in cc_rh_report_summary_debt_card.columns else np.nan
    cc_rh_report_summary_debt_card['avgUsedAmount'] = cc_rh_report_summary_debt_card['avgUsedAmount'] if 'avgUsedAmount' in cc_rh_report_summary_debt_card.columns else np.nan
    dict_in['cc_rh_report_summary_debt_card'] = cc_rh_report_summary_debt_card
    # print("cc_rh_report_summary_debt_card:", cc_rh_report_summary_debt_card)

    cc_rh_report_summary_debt_loan = pd.DataFrame([pboc_info['cc_rh_report_summary_debt_loan']]) if 'cc_rh_report_summary_debt_loan' in pboc_info.keys() else pd.DataFrame()
    cc_rh_report_summary_debt_loan['id'] = cc_rh_report_summary_debt_loan['id'] if 'id' in cc_rh_report_summary_debt_loan.columns else np.nan
    cc_rh_report_summary_debt_loan['reportId'] = cc_rh_report_summary_debt_loan['reportId'] if 'reportId' in cc_rh_report_summary_debt_loan.columns else np.nan
    cc_rh_report_summary_debt_loan['businessType'] = cc_rh_report_summary_debt_loan['businessType'] if 'businessType' in cc_rh_report_summary_debt_loan.columns else np.nan
    cc_rh_report_summary_debt_loan['orgCount'] = cc_rh_report_summary_debt_loan['orgCount'] if 'orgCount' in cc_rh_report_summary_debt_loan.columns else np.nan
    cc_rh_report_summary_debt_loan['accountCount'] = cc_rh_report_summary_debt_loan['accountCount'] if 'accountCount' in cc_rh_report_summary_debt_loan.columns else np.nan
    cc_rh_report_summary_debt_loan['creditTotalAmount'] = cc_rh_report_summary_debt_loan['creditTotalAmount'] if 'creditTotalAmount' in cc_rh_report_summary_debt_loan.columns else np.nan
    cc_rh_report_summary_debt_loan['balance'] = cc_rh_report_summary_debt_loan['balance'] if 'balance' in cc_rh_report_summary_debt_loan.columns else np.nan
    cc_rh_report_summary_debt_loan['avgRepaymentAmount'] = cc_rh_report_summary_debt_loan['avgRepaymentAmount'] if 'avgRepaymentAmount' in cc_rh_report_summary_debt_loan.columns else np.nan
    dict_in['cc_rh_report_summary_debt_loan'] = cc_rh_report_summary_debt_loan
    # print("cc_rh_report_summary_debt_loan:", cc_rh_report_summary_debt_loan)



    cc_rh_report_summary_overdue = pd.DataFrame([pboc_info['cc_rh_report_summary_overdue']]) if 'cc_rh_report_summary_overdue' in pboc_info.keys() else pd.DataFrame()
    cc_rh_report_summary_overdue['id'] = cc_rh_report_summary_overdue['id'] if 'id' in cc_rh_report_summary_overdue.columns else np.nan
    cc_rh_report_summary_overdue['reportId'] = cc_rh_report_summary_overdue['reportId'] if 'reportId' in cc_rh_report_summary_overdue.columns else np.nan
    cc_rh_report_summary_overdue['badDebtCount'] = cc_rh_report_summary_overdue['badDebtCount'] if 'badDebtCount' in cc_rh_report_summary_overdue.columns else np.nan
    cc_rh_report_summary_overdue['badDebtBalance'] = cc_rh_report_summary_overdue['badDebtBalance'] if 'badDebtBalance' in cc_rh_report_summary_overdue.columns else np.nan
    cc_rh_report_summary_overdue['disposalCount'] = cc_rh_report_summary_overdue['disposalCount'] if 'disposalCount' in cc_rh_report_summary_overdue.columns else np.nan
    cc_rh_report_summary_overdue['disposalBalance'] = cc_rh_report_summary_overdue['disposalBalance'] if 'disposalBalance' in cc_rh_report_summary_overdue.columns else np.nan
    cc_rh_report_summary_overdue['compensationCount'] = cc_rh_report_summary_overdue['compensationCount'] if 'compensationCount' in cc_rh_report_summary_overdue.columns else np.nan
    cc_rh_report_summary_overdue['compensationBalance'] = cc_rh_report_summary_overdue['compensationBalance'] if 'compensationBalance' in cc_rh_report_summary_overdue.columns else np.nan
    cc_rh_report_summary_overdue['loanOverdueCount'] = cc_rh_report_summary_overdue['loanOverdueCount'] if 'loanOverdueCount' in cc_rh_report_summary_overdue.columns else np.nan
    cc_rh_report_summary_overdue['loanOverdueMonthSum'] = cc_rh_report_summary_overdue['loanOverdueMonthSum'] if 'loanOverdueMonthSum' in cc_rh_report_summary_overdue.columns else np.nan
    cc_rh_report_summary_overdue['loanOverdueMonthMaxAmount'] = cc_rh_report_summary_overdue['loanOverdueMonthMaxAmount'] if 'loanOverdueMonthMaxAmount' in cc_rh_report_summary_overdue.columns else np.nan
    cc_rh_report_summary_overdue['loanOverdueMaxMonth'] = cc_rh_report_summary_overdue['loanOverdueMaxMonth'] if 'loanOverdueMaxMonth' in cc_rh_report_summary_overdue.columns else np.nan
    cc_rh_report_summary_overdue['subAccountCount'] = cc_rh_report_summary_overdue['subAccountCount'] if 'subAccountCount' in cc_rh_report_summary_overdue.columns else np.nan
    cc_rh_report_summary_overdue['subAccountTotalMonth'] = cc_rh_report_summary_overdue['subAccountTotalMonth'] if 'subAccountTotalMonth' in cc_rh_report_summary_overdue.columns else np.nan
    cc_rh_report_summary_overdue['subAccountMaxAmount'] = cc_rh_report_summary_overdue['subAccountMaxAmount'] if 'subAccountMaxAmount' in cc_rh_report_summary_overdue.columns else np.nan
    cc_rh_report_summary_overdue['subAccountMaxMonth'] = cc_rh_report_summary_overdue['subAccountMaxMonth'] if 'subAccountMaxMonth' in cc_rh_report_summary_overdue.columns else np.nan
    cc_rh_report_summary_overdue['loopLoanCount'] = cc_rh_report_summary_overdue['loopLoanCount'] if 'loopLoanCount' in cc_rh_report_summary_overdue.columns else np.nan
    cc_rh_report_summary_overdue['loopLoanTotalMonth'] = cc_rh_report_summary_overdue['loopLoanTotalMonth'] if 'loopLoanTotalMonth' in cc_rh_report_summary_overdue.columns else np.nan
    cc_rh_report_summary_overdue['loopLoanMaxAmount'] = cc_rh_report_summary_overdue['loopLoanMaxAmount'] if 'loopLoanMaxAmount' in cc_rh_report_summary_overdue.columns else np.nan
    cc_rh_report_summary_overdue['loopLoanMaxMonth'] = cc_rh_report_summary_overdue['loopLoanMaxMonth'] if 'loopLoanMaxMonth' in cc_rh_report_summary_overdue.columns else np.nan
    cc_rh_report_summary_overdue['cardOverdueCount'] = cc_rh_report_summary_overdue['cardOverdueCount'] if 'cardOverdueCount' in cc_rh_report_summary_overdue.columns else np.nan
    cc_rh_report_summary_overdue['cardOverdueMonthSum'] = cc_rh_report_summary_overdue['cardOverdueMonthSum'] if 'cardOverdueMonthSum' in cc_rh_report_summary_overdue.columns else np.nan
    cc_rh_report_summary_overdue['cardOverdueMonthMaxAmount'] = cc_rh_report_summary_overdue['cardOverdueMonthMaxAmount'] if 'cardOverdueMonthMaxAmount' in cc_rh_report_summary_overdue.columns else np.nan
    cc_rh_report_summary_overdue['cardOverdueMonthMax'] = cc_rh_report_summary_overdue['cardOverdueMonthMax'] if 'cardOverdueMonthMax' in cc_rh_report_summary_overdue.columns else np.nan
    cc_rh_report_summary_overdue['semiCardOverdueMonthMaxAmount'] = cc_rh_report_summary_overdue['semiCardOverdueMonthMaxAmount'] if 'semiCardOverdueMonthMaxAmount' in cc_rh_report_summary_overdue.columns else np.nan
    cc_rh_report_summary_overdue['semiCardOverdueCount'] = cc_rh_report_summary_overdue['semiCardOverdueCount'] if 'semiCardOverdueCount' in cc_rh_report_summary_overdue.columns else np.nan
    cc_rh_report_summary_overdue['semiCardOverdueMonthSum'] = cc_rh_report_summary_overdue['semiCardOverdueMonthSum'] if 'semiCardOverdueMonthSum' in cc_rh_report_summary_overdue.columns else np.nan
    cc_rh_report_summary_overdue['semiCardOverdueMonthMax'] = cc_rh_report_summary_overdue['semiCardOverdueMonthMax'] if 'semiCardOverdueMonthMax' in cc_rh_report_summary_overdue.columns else np.nan
    dict_in['cc_rh_report_summary_overdue'] = cc_rh_report_summary_overdue
    # print("cc_rh_report_summary_overdue:", cc_rh_report_summary_overdue)


    cc_rh_report_summary_bad_debts = pd.DataFrame([pboc_info['cc_rh_report_summary_bad_debts']]) if 'cc_rh_report_summary_bad_debts' in pboc_info.keys() else pd.DataFrame()
    cc_rh_report_summary_bad_debts['id'] = cc_rh_report_summary_bad_debts['id'] if 'id' in cc_rh_report_summary_bad_debts.columns else np.nan
    cc_rh_report_summary_bad_debts['reportId'] = cc_rh_report_summary_bad_debts['reportId'] if 'reportId' in cc_rh_report_summary_bad_debts.columns else np.nan
    cc_rh_report_summary_bad_debts['account'] = cc_rh_report_summary_bad_debts['account'] if 'account' in cc_rh_report_summary_bad_debts.columns else np.nan
    cc_rh_report_summary_bad_debts['balance'] = cc_rh_report_summary_bad_debts['balance'] if 'balance' in cc_rh_report_summary_bad_debts.columns else np.nan
    dict_in['cc_rh_report_summary_bad_debts'] = cc_rh_report_summary_bad_debts
    # print("cc_rh_report_summary_bad_debts:", cc_rh_report_summary_bad_debts)

    cc_rh_report_summary_recovery = pd.DataFrame(pboc_info['cc_rh_report_summary_recovery']) if 'cc_rh_report_summary_recovery' in pboc_info.keys() else pd.DataFrame()
    cc_rh_report_summary_recovery['id'] = cc_rh_report_summary_recovery['id'] if 'id' in cc_rh_report_summary_recovery.columns else np.nan
    cc_rh_report_summary_recovery['reportId'] = cc_rh_report_summary_recovery['reportId'] if 'reportId' in cc_rh_report_summary_recovery.columns else np.nan
    cc_rh_report_summary_recovery['businessType'] = cc_rh_report_summary_recovery['businessType'] if 'businessType' in cc_rh_report_summary_recovery.columns else np.nan
    cc_rh_report_summary_recovery['account'] = cc_rh_report_summary_recovery['account'] if 'account' in cc_rh_report_summary_recovery.columns else np.nan
    cc_rh_report_summary_recovery['balance'] = cc_rh_report_summary_recovery['balance'] if 'balance' in cc_rh_report_summary_recovery.columns else np.nan
    dict_in['cc_rh_report_summary_recovery'] = cc_rh_report_summary_recovery
    # print("cc_rh_report_summary_recovery:", cc_rh_report_summary_recovery)


    cc_rh_report_summary_debt = pd.DataFrame([pboc_info['cc_rh_report_summary_debt']]) if 'cc_rh_report_summary_debt' in pboc_info.keys() else pd.DataFrame()
    cc_rh_report_summary_debt['id'] = cc_rh_report_summary_debt['id'] if 'id' in cc_rh_report_summary_debt.columns else np.nan
    cc_rh_report_summary_debt['reportId'] = cc_rh_report_summary_debt['reportId'] if 'reportId' in cc_rh_report_summary_debt.columns else np.nan
    cc_rh_report_summary_debt['unclearedLoanLegalPersonOrgCount'] = cc_rh_report_summary_debt['unclearedLoanLegalPersonOrgCount'] if 'unclearedLoanLegalPersonOrgCount' in cc_rh_report_summary_debt.columns else np.nan
    cc_rh_report_summary_debt['unclearedLoanOrgCount'] = cc_rh_report_summary_debt['unclearedLoanOrgCount'] if 'unclearedLoanOrgCount' in cc_rh_report_summary_debt.columns else np.nan
    cc_rh_report_summary_debt['unclearedLoanCount'] = cc_rh_report_summary_debt['unclearedLoanCount'] if 'unclearedLoanCount' in cc_rh_report_summary_debt.columns else np.nan
    cc_rh_report_summary_debt['unclearedLoanContractAmount'] = cc_rh_report_summary_debt['unclearedLoanContractAmount'] if 'unclearedLoanContractAmount' in cc_rh_report_summary_debt.columns else np.nan
    cc_rh_report_summary_debt['unclearedLoanBalance'] = cc_rh_report_summary_debt['unclearedLoanBalance'] if 'unclearedLoanBalance' in cc_rh_report_summary_debt.columns else np.nan
    cc_rh_report_summary_debt['unclearedLoanAvgRepaymentAmount'] = cc_rh_report_summary_debt['unclearedLoanAvgRepaymentAmount'] if 'unclearedLoanAvgRepaymentAmount' in cc_rh_report_summary_debt.columns else np.nan
    cc_rh_report_summary_debt['uncancelledCardLegalPersonOrgCount'] = cc_rh_report_summary_debt['uncancelledCardLegalPersonOrgCount'] if 'uncancelledCardLegalPersonOrgCount' in cc_rh_report_summary_debt.columns else np.nan
    cc_rh_report_summary_debt['uncancelledCardOrgCount'] = cc_rh_report_summary_debt['uncancelledCardOrgCount'] if 'uncancelledCardOrgCount' in cc_rh_report_summary_debt.columns else np.nan
    cc_rh_report_summary_debt['uncancelledCardCount'] = cc_rh_report_summary_debt['uncancelledCardCount'] if 'uncancelledCardCount' in cc_rh_report_summary_debt.columns else np.nan
    cc_rh_report_summary_debt['uncancelledCardGrantAmount'] = cc_rh_report_summary_debt['uncancelledCardGrantAmount'] if 'uncancelledCardGrantAmount' in cc_rh_report_summary_debt.columns else np.nan
    cc_rh_report_summary_debt['uncancelledCardSingleMaxGrantAmount'] = cc_rh_report_summary_debt['uncancelledCardSingleMaxGrantAmount'] if 'uncancelledCardSingleMaxGrantAmount' in cc_rh_report_summary_debt.columns else np.nan
    cc_rh_report_summary_debt['uncancelledCardSingleMinGrantAmount'] = cc_rh_report_summary_debt['uncancelledCardSingleMinGrantAmount'] if 'uncancelledCardSingleMinGrantAmount' in cc_rh_report_summary_debt.columns else np.nan
    cc_rh_report_summary_debt['uncancelledCardUsedAmount'] = cc_rh_report_summary_debt['uncancelledCardUsedAmount'] if 'uncancelledCardUsedAmount' in cc_rh_report_summary_debt.columns else np.nan
    cc_rh_report_summary_debt['uncancelledCardAvgUseAmount'] = cc_rh_report_summary_debt['uncancelledCardAvgUseAmount'] if 'uncancelledCardAvgUseAmount' in cc_rh_report_summary_debt.columns else np.nan
    cc_rh_report_summary_debt['guaranteeCount'] = cc_rh_report_summary_debt['guaranteeCount'] if 'guaranteeCount' in cc_rh_report_summary_debt.columns else np.nan
    cc_rh_report_summary_debt['guaranteeAmount'] = cc_rh_report_summary_debt['guaranteeAmount'] if 'guaranteeAmount' in cc_rh_report_summary_debt.columns else np.nan
    cc_rh_report_summary_debt['guaranteeCapitalBalance'] = cc_rh_report_summary_debt['guaranteeCapitalBalance'] if 'guaranteeCapitalBalance' in cc_rh_report_summary_debt.columns else np.nan
    dict_in['cc_rh_report_summary_debt'] = cc_rh_report_summary_debt




    dict_out = basic(dict_in, dict_out)


    # 另一部分人行数据
    dict_out2 = get_user_data.data_process(dict_in)
    dict_out.update(dict_out2)
    return dict_out


