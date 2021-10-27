import numpy as np
import pandas as pd
from scipy.stats import chi2
import scorecardpy as sc
import sqlalchemy
from sqlalchemy import create_engine
import copy
import statsmodels.api as smf

import warnings
import re
from pandas.api.types import is_numeric_dtype
from pandas.api.types import is_string_dtype
import datetime
import hashlib
import pickle
from joblib import Parallel, delayed
from tqdm import tqdm
from collections import Counter

import sys
# sys.path.append("E:\\work\\征信系统变量")
from my_func_0_1 import *

import warnings
warnings.filterwarnings("ignore")

pd.set_option('display.max_rows', 500)
pd.set_option('display.max_columns', 20)
pd.set_option('display.width', 1000)

import time
time_start = time.time()  # 开始计时


'''
# 指定样本
connect_info = 'mysql+pymysql://{}:{}@{}:{}/{}?charset=utf8'.format("root", "Cq9NYpAc0ydxOj22N2NB", "121.199.3.118", 3306, "ods_tps")
engine = create_engine(connect_info)
tmp_sql = "select * from risk_alchemist.loan where businessChannel in (11) and substr(createTime,1,10) >= '2021-01-01' and substr(createTime,1,10) <= '2021-01-01' "
data_usr = pd.read_sql(tmp_sql, engine)
data_usr_c = data_usr[['loanId', 'businessChannel', 'createTime', 'user_id_number', 'user_phone', 'currentOverdue', 'maxOverdueDays']].rename(columns={'user_id_number': 'idcardNo', 'createTime': 'loan_time', 'user_phone': 'mobile'})
# print(data_usr_c.shape)
'''

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

# 定义学历函数
def get_diploma(dict_in):
    dip = dict_in['cc_rh_report_customer']['education'][0]
    if dip == '研究生':
        return '研究生'
    elif dip == '大学本科（简称"大学"）':
        return '本科'
    elif dip == '大学专科和专科学校（简称"大专"）':
        return '大专'
    elif dip == '中等专业学校或中等技术学校':
        return '中专、职高、技校'
    elif dip == '高中':
        return '高中'
    elif dip == '初中':
        return '初中及以下'
    elif dip == '--':
        return '未知'
    elif dip == '':
        return 'null'
    else:
        return '其他'


# 定义学位函数
def get_degree(dict_in):
    deg = dict_in['cc_rh_report_customer']['degree'][0]
    if deg == '':
        return 'null'
    elif deg == '--':
        return '无'
    else:
        return deg


# 定义就业状况函数
def get_emp_status(dict_in):
    pro = dict_in['cc_rh_report_customer_profession']
    try:
        job = pro['profession'][pro['updateTime'] == pro['updateTime'].max()][0]
    except:
        job = '--'
    if (job == '不便分类的其它从业人员') | (job == '不便分类的其他从业人员'):
        return '其他'
    elif job == '--':
        return '未知'
    else:
        return job

# 计算逾期期数和
def duecount(month):
    count=[]
    for list in month:
        sum=0
        for i in list:
            if i>=1:
                sum = sum+1
        count.append(sum)
    if len(count)>0:
        return float(max(count))
    else:
        return 0.0


#获取还款记录月份列表
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

#定义时间差函数
def time_del(time1,time2):
    try:
        t1 = datetime.datetime.strptime(time1, "%Y.%m.%d %H:%M:%S")
        t2 = datetime.datetime.strptime(time2, "%Y.%m.%d")
    except:
        t1 = datetime.datetime.strptime(time1, "%Y.%m.%d %H:%M:%S")
        t2 = datetime.datetime.strptime(time2, "%Y.%m")
    t = t1 - t2
    return t.days

#计算月数差
def months(str1,str2):
    year1=datetime.datetime.strptime(str1[0:10],"%Y.%m.%d").year
    year2=datetime.datetime.strptime(str2[0:7],"%Y.%m").year
    month1=datetime.datetime.strptime(str1[0:10],"%Y.%m.%d").month
    month2=datetime.datetime.strptime(str2[0:7],"%Y.%m").month
    num=(year1-year2)*12+(month1-month2)
    return num

# 定义数字判断函数
def is_number(s):
    try:
        float(s)
        return True
    except ValueError:
        pass

    try:
        import unicodedata
        unicodedata.numeric(s)
        return True
    except (TypeError, ValueError):
        pass

    return False

# 计算贷记卡正常还款历史月数(记录为：以第一个N或C开始，到最后一个N或C结束）
def repay_month_debit_normal(s):
    a = 0
    b = 0
    for i in reversed(s):
        a = a + 1
        if (i == 'N') | (i == 'C'):
            break
    for i in s:
        b = b + 1
        if (i == 'N') | (i == 'C'):
            break
    return len(s)-a-b+2

# 计算贷记卡全部还款历史月数（记录为：以第一个N、C或数字开始，到最后一个N、C或数字结束）
def repay_month_debit_all(s):
    a = 0
    b = 0
    for i in reversed(s):
        a = a + 1
        if (i == 'N') | (i == 'C') | (is_number(i)):
            break
    for i in s:
        b = b+1
        if (i == 'N') | (i == 'C') | (is_number(i)):
            break
    return len(s)-a-b+2

# 计算贷款正常还款历史月数（记录为：以第一个N开始，到最后一个N或C结束）
def repay_month_loan_normal(s):
    a = 0
    b = 0
    for i in reversed(s):
        a = a+1
        if (i == 'N') | (i == 'C'):
            break
    for i in s:
        b = b+1
        if i == 'N':
            break
    return len(s)-a-b+2

# 计算贷款全部还款历史月数（记录为：以第一个N、C或数字开始，到最后一个N、C或数字结束）
def repay_month_loan_all(s):
    a = 0
    b = 0
    for i in reversed(s):
        a = a+1
        if (i == 'N') | (i == 'C') | (is_number(i)):
            break
    for i in s:
        b = b+1
        if (i == 'N') | (is_number(i)):
            break
    return len(s)-a-b+2

# 数据预加工
def data_process(dict_in):
    # 初始化处理
    dict_out = dict()
    dict_out = initial_featrue(dict_out)

    cc_rh_report = copy.copy(dict_in['cc_rh_report'])
    if len(dict_in['cc_rh_report_detail_loan_second']) > 0:
        loan_second = copy.copy(dict_in['cc_rh_report_detail_loan_second'])


        # 定义现行
        loan_second['is_now'] = loan_second.apply(
            lambda x: 1 if ((x['accountStatus'] == '正常') | (x['accountStatus'] == '逾期')) else 0, axis=1)

        # 定义逾期月数(没有逾期月数则设为0)
        month = []
        for row, value in loan_second.month60State.items():
            month.append(re.findall(r'\d+', value))
            for i in range(len(month[row])):
                month[row][i] = len(month[row][i])
            month[row] = [0] if month[row] == [] else month[row]
        loan_second['overdue_month'] = month

        # 定义近12个月贷款逾期月数(没有逾期月数则设为0)
        month12 = []
        for row, value in loan_second.month60State.items():
            s = []
            for i in value[-12:]:
                if i in ['1', '2', '3', '4', '5', '6', '7']:
                    s.append(int(i))
                elif i in ['B', 'D', 'Z']:
                    s.append(8)
                else:
                    s.append(0)
            month12.append(s)
        loan_second['overdue_month_12'] = month12

        # 定义近24个月贷款逾期月数(没有逾期月数则设为0)
        month24 = []
        for row, value in loan_second.month60State.items():
            s = []
            for i in value[-24:]:
                if i in ['1', '2', '3', '4', '5', '6', '7']:
                    s.append(int(i))
                elif i in ['B', 'D', 'Z']:
                    s.append(8)
                else:
                    s.append(0)
            month24.append(s)
        loan_second['overdue_month_24'] = month24

        # 定义有担保
        loan_second['is_vouch'] = loan_second.apply(lambda x: 1 if (
                    (x['guaranteeForm'] != '信用/免担保') & (x['guaranteeForm'] != '组合（不含保证）') & (
                        x['guaranteeForm'] != '其他')) else 0, axis=1)

        # 定义需还款月份数
        loan_second['repayMons'] = loan_second.apply(
            lambda x: max(1, len(x['month60State']) - 1) if x['repayTerms'] == 0 else
            x['repayTerms'] * 12 if x['repayFrequency'] == '年' else
            x['repayTerms'] * 6 if x['repayFrequency'] == '半年' else
            x['repayTerms'] * 3 if x['repayFrequency'] == '季' else
            x['repayTerms'] if x['repayFrequency'] == '月' else
            math.ceil(x['repayTerms'] / 4) if x['repayFrequency'] == '周' else
            math.ceil(x['repayTerms'] / 31) if x['repayFrequency'] == '日' else
            1 if x['repayFrequency'] == '一次性' else
            max(1, len(str(x['month60State'])) - 1), axis=1)

        # 定义开户时间（开立时间到报告时间）
        loan_second['month_startDate_report'] = loan_second.apply(
            lambda x: monthdelta(x['startDate'], cc_rh_report['reportTime']), axis=1)

        # 定义贷款记录截止时间到报告时间的月数
        loan_second['month_Desc_report'] = loan_second.apply(
            lambda x: monthdelta(getMonList(str(x['month60Desc']))[-1], cc_rh_report['reportTime']) if str(x['month60Desc']) != '' else -1, axis=1)

        # rating转换
        loan_second['classify5_num'] = loan_second['classify5'].map({'损失': 1.0, '可疑': 2.0, '关注': 3.0, '次级': 4.0, '正常': 5.0})

        # 定义逾期程度
        loan_second['overdue_class'] = loan_second.apply(lambda x: np.nan if pd.isnull(x['month60State'])
        else np.nan if len(x['month60State']) == 0
        else 0 if len(re.findall('[1-7DZ]', x['month60State'])) == 0
        else 3 if len(re.findall('[4-8]', x['month60State'])) > 0  # 逾期四个月及以上且未偿还，定义为严重逾期
        else 2 if len(re.findall('[2-3DZ]', x['month60State'])) > 0
        else 1 if len(re.findall('[1]', x['month60State'])) > 0  # 逾期一个月且未偿还，定义为轻度逾期
        else np.nan, axis=1)

        # 定义账户类型
        loan_second['class'] = loan_second.apply(
            lambda x: '非循环贷账户' if len(re.findall('--|到期一次还本付息', x.repayType)) > 0 else '循环额度下分账户' if len(
                re.findall('分期等额本息', x.repayType)) > 0 else '循环贷账户', axis=1)

        # 定义还款记录到报告时间的月数
        # a = getMonList(str(loan_second['month60Desc'][0]))
        # monthdelta(a[-1], cc_rh_report['reportTime'])
        loan_second.apply(lambda x: monthdelta(getMonList(str(x['month60Desc']))[-1], cc_rh_report['reportTime']) if str(x['month60Desc']) != '' else -1, axis=1)

        # 定义当前逾期
        recent_overdue = []
        for row, value in loan_second.month60State.items():
            if value != '':
                recent_overdue.append(value[-1])
            else:
                recent_overdue.append(-1)
            recent_overdue[row] = 0 if (
                        recent_overdue[row] == 'N' or recent_overdue[row] == 'D' or recent_overdue[row] == 'Z' or
                        recent_overdue[row] == 'C') else 1
        loan_second['recent_overdue'] = recent_overdue

        # 定义违约
        loan_second['is_break'] = loan_second.apply(lambda x: 1 if len(re.findall('[1-8]', x['month60State'])) > 0 else 0,
                                                    axis=1)

        # 定义总逾期月数
        overdue_month_sum = []
        for row, value in loan_second.month60State.items():
            numbers = re.findall(r'\d+', value)
            month_max = 0
            for item in numbers:
                month_max = month_max + len(item)

            overdue_month_sum.append(month_max)
        loan_second['overdue_month_sum'] = overdue_month_sum

        # 定义最大逾期月数
        overdue_month_max = []
        for row in loan_second['overdue_month']:
            overdue_month_max.append(max(row))
        loan_second['overdue_month_max'] = overdue_month_max

        # 定义正常还款历史(月数)
        repay_history_month_normal = []
        for row in loan_second['month60State']:
            try:
                repay_history_month_normal.append(repay_month_loan_normal(row))
            except:
                repay_history_month_normal.append(-1)
        loan_second['repay_history_month_normal'] = repay_history_month_normal

        # 定义全部还款历史(月数)
        repay_history_month_all = []
        for row in loan_second['month60State']:
            try:
                repay_history_month_all.append(repay_month_loan_all(row))
            except:
                repay_history_month_all.append(-1)
        loan_second['repay_history_month_all'] = repay_history_month_all

        # 定义贷款最大逾期金额
        loan_overdue_amount_max = []
        for row in loan_second['month60Amount']:
            try:
                loan_overdue_amount_max.append(float(max(row.split('/'))))
            except:
                loan_overdue_amount_max.append(-1.0)
        loan_second['loan_overdue_amount_max'] = loan_overdue_amount_max
    else:
        loan_second = pd.DataFrame()

    if len(dict_in['cc_rh_report_detail_debit_card_second']) > 0:
        debit_card_second = copy.copy(dict_in['cc_rh_report_detail_debit_card_second'])

        # 定义现行
        debit_card_second['is_now'] = debit_card_second.apply(
            lambda x: 1 if ((x['accountStatus'] == '正常') | (x['accountStatus'] == '逾期')) else 0, axis=1)

        # 定义贷记卡的贷款月数
        month_debit = []
        for i in range(len(debit_card_second['month60Desc'])):
            try:
                month_debit.append(months(cc_rh_report['reportTime'], getMonList(debit_card_second['month60Desc'][i])[0]) - (len(debit_card_second['month60State'][i]) - len(debit_card_second['month60State'][i].lstrip('*'))))
            except:
                month_debit.append(-1)
        debit_card_second['month_debit'] = month_debit

        # 定义贷记卡最大逾期金额
        card_overdue_amount_max = []
        for row in debit_card_second['month60Amount']:
            try:
                card_overdue_amount_max.append(float(max(row.split('/'))))
            except:
                card_overdue_amount_max.append(-1.0)
        debit_card_second['card_overdue_amount_max'] = card_overdue_amount_max

        # 定义近12个月贷记卡逾期月数(没有逾期月数则设为0)
        month12 = []
        for row, value in debit_card_second.month60State.items():
            s = []
            for i in value[-12:]:
                if i in ['1', '2', '3', '4', '5', '6', '7']:
                    s.append(int(i))
                elif i in ['B', 'D', 'Z']:
                    s.append(8)
                else:
                    s.append(0)
            month12.append(s)
        debit_card_second['overdue_month_12'] = month12

        # 定义近24个月贷记卡逾期月数(没有逾期月数则设为0)
        month24 = []
        for row, value in debit_card_second.month60State.items():
            s = []
            for i in value[-24:]:
                if i in ['1', '2', '3', '4', '5', '6', '7']:
                    s.append(int(i))
                elif i in ['B', 'D', 'Z']:
                    s.append(8)
                else:
                    s.append(0)
            month24.append(s)
        debit_card_second['overdue_month_24'] = month24

        # 定义贷记卡正常还款历史月数
        repay_month_normal = []
        for i in debit_card_second['month60State']:
            month = repay_month_debit_normal(i)
            if month > 0:
                repay_month_normal.append(month)
            else:
                repay_month_normal.append(0)
        debit_card_second['repay_month_normal'] = repay_month_normal

        # 定义贷记卡全部还款历史月数
        repay_month_all = []
        for i in debit_card_second['month60State']:
            month = repay_month_debit_all(i)
            if month > 0:
                repay_month_all.append(month)
            else:
                repay_month_all.append(0)
        debit_card_second['repay_month_all'] = repay_month_all

    else:
        debit_card_second = pd.DataFrame()



    dict_out["reportTime"] = cc_rh_report['reportTime'][0]
    data_info = get_features(loan_second, debit_card_second, dict_out, dict_in)
    return data_info



def initial_featrue(dict_out):

    dict_out['consume_loan_slight_overdue_RepayedAmount_sum_03m_now'] = np.nan
    dict_out['loan_serious_overdue_RepayedAmount_max_12m_now'] = np.nan
    dict_out['car_loan_slight_overdue_RepayedAmount_max_now'] = np.nan
    dict_out['consume_loan_is_vouch_serious_overdue_RepayedAmount_sum_24m_now'] = np.nan
    dict_out['crt_12m_duecount'] = np.nan
    dict_out['business_loan_overdue_month_max_03m_now'] = np.nan
    dict_out['consume_loan_slight_overdue_planRepayAmount_sum_24m_now'] = np.nan
    dict_out['consume_loan_serious_overdue_account_count_24m_now'] = np.nan
    dict_out['consume_loan_ndue_is_vouch_account_count_03m'] = np.nan
    dict_out['query_1m_lnsum'] = np.nan
    dict_out['business_loan_currentOverdueAmount_sum_now'] = np.nan
    dict_out['consume_loan_ndue_is_vouch_account_count_24m_now'] = np.nan
    dict_out['laco06_lacn'] = np.nan
    dict_out['loan_is_vouch_serious_overdue_month_sum_24m_now'] = np.nan
    dict_out['business_loan_balance_sum_now'] = np.nan
    dict_out['loan_serious_overdue_month_sum_12m_now'] = np.nan
    dict_out['business_loan_amount_sum_now'] = np.nan
    dict_out['stu_loan_serious_overdue_RepayedAmount_max_now'] = np.nan
    dict_out['loan_is_vouch_serious_overdue_account_count_24m'] = np.nan
    dict_out['clnas_clas'] = np.nan
    dict_out['consume_loan_serious_overdue_balance_max_now'] = np.nan
    dict_out['consume_loan_is_vouch_serious_overdue_planRepayAmount_max_03m_now'] = np.nan
    dict_out['consume_loan_slight_overdue_account_count_06m'] = np.nan
    dict_out['loan_slight_overdue_month_sum_12m'] = np.nan
    dict_out['business_loan_balance_max_now'] = np.nan
    dict_out['business_loan_is_vouch_ndue_RepayedAmount_max_06m_now'] = np.nan
    dict_out['loan_is_vouch_serious_overdue_RepayedAmount_max_12m_now'] = np.nan
    dict_out['house_loan_slight_overdue_planRepayAmount_max_now'] = np.nan
    dict_out['business_loan_is_vouch_serious_overdue_RepayedAmount_max_06m_now'] = np.nan
    dict_out['ln_24m_duesum'] = np.nan
    dict_out['business_loan_serious_overdue_RepayedAmount_sum_03m_now'] = np.nan
    dict_out['blsoacn_lac'] = np.nan
    dict_out['settlehouselon_cnt'] = np.nan
    dict_out['clacn_lacn'] = np.nan
    dict_out['stu_loan_planRepayAmount_sum_now'] = np.nan
    dict_out['loan_is_vouch_slight_overdue_account_count_24m_now'] = np.nan
    dict_out['loan_is_vouch_serious_overdue_planRepayAmount_sum_24m_now'] = np.nan
    dict_out['consume_loan_is_vouch_serious_overdue_RepayedAmount_max_24m_now'] = np.nan
    dict_out['loan_ndue_is_vouch_account_count_03m_now'] = np.nan
    dict_out['business_loan_ndue_account_count_24m_now'] = np.nan
    dict_out['business_loan_is_vouch_serious_overdue_month_sum_03m'] = np.nan
    dict_out['house_loan_overdue31Amount_max_now'] = np.nan
    dict_out['loan_ndue_planRepayAmount_sum_12m_now'] = np.nan
    dict_out['stu_loan_overdue_month_sum'] = np.nan
    dict_out['business_loan_is_vouch_overdue_month_sum_03m_now'] = np.nan
    dict_out['loan_slight_overdue_account_count_24m_now'] = np.nan
    dict_out['business_loan_slight_overdue_RepayedAmount_sum_06m_now'] = np.nan
    dict_out['consume_loan_ndue_planRepayAmount_sum_24m_now'] = np.nan
    dict_out['consume_loan_overdue61Amount_sum_now'] = np.nan
    dict_out['loan_ndue_is_vouch_RepayedAmount_max_12m_now'] = np.nan
    dict_out['loan_ndue_account_count_03m_now'] = np.nan
    dict_out['business_loan_serious_overdue_amount_sum'] = np.nan
    dict_out['loan_ndue_RepayedAmount_max_12m_now'] = np.nan
    dict_out['loan_slight_overdue_account_count_06m_now'] = np.nan
    dict_out['loan_is_vouch_serious_overdue_account_count_03m'] = np.nan
    dict_out['consume_loan_serious_overdue_account_count_12m'] = np.nan
    dict_out['business_loan_serious_overdue_planRepayAmount_max_24m_now'] = np.nan
    dict_out['consume_loan_ndue_amount_max'] = np.nan
    dict_out['house_loan_slight_amount_sum'] = np.nan
    dict_out['loan_is_vouch_overdue_month_max_03m'] = np.nan
    dict_out['consume_loan_ndue_RepayedAmount_sum_03m_now'] = np.nan
    dict_out['consume_loan_serious_overdue_planRepayAmount_max_24m_now'] = np.nan
    dict_out['blacn_lacn'] = np.nan
    dict_out['business_loan_is_vouch_serious_overdue_month_sum_03m_now'] = np.nan
    dict_out['laco06_lac'] = np.nan
    dict_out['stu_loan_serious_overdue_account_count_sum_now'] = np.nan
    dict_out['loan_ndue_is_vouch_planRepayAmount_sum_06m_now'] = np.nan
    dict_out['crt_fstmth'] = np.nan
    dict_out['loan_is_vouch_serious_overdue_planRepayAmount_sum_03m_now'] = np.nan
    dict_out['business_loan_slight_overdue_RepayedAmount_sum_12m_now'] = np.nan
    dict_out['loan_planRepayAmount_sum_now'] = np.nan
    dict_out['consume_loan_slight_overdue_month_sum_06m'] = np.nan
    dict_out['clnbsn_clnasn'] = np.nan
    dict_out['loan_slight_overdue_is_vouch_month_sum_now'] = np.nan
    dict_out['noln_ovd_maxamt'] = np.nan
    dict_out['loan_GrantOrg_CD'] = np.nan
    dict_out['business_loan_slight_overdue_balance_sum_now'] = np.nan
    dict_out['loan_ndue_is_vouch_planRepayAmount_sum_24m_now'] = np.nan
    dict_out['business_loan_ndue_account_count_24m'] = np.nan
    dict_out['loan_ndue_is_vouch_RepayedAmount_sum_now'] = np.nan
    dict_out['crt_12m_duesum'] = np.nan
    dict_out['business_loan_slight_overdue_is_vouch_amount_sum'] = np.nan
    dict_out['car_loan_slight_overdue_RepayedAmount_sum_now'] = np.nan
    dict_out['blivacn_lacn'] = np.nan
    dict_out['query_1m_crtsum'] = np.nan
    dict_out['loan_serious_overdue_account_count_now'] = np.nan
    dict_out['consume_loan_account_count'] = np.nan
    dict_out['consume_loan_ndue_is_vouch_planRepayAmount_sum_03m_now'] = np.nan
    dict_out['consume_loan_is_vouch_serious_overdue_account_count'] = np.nan
    dict_out['stu_loan_overdue61Amount_sum_now'] = np.nan
    dict_out['car_loan_slight_overdue_balance_sum_now'] = np.nan
    dict_out['business_loan_is_vouch_rating_worst'] = np.nan
    dict_out['business_loan_is_vouch_serious_overdue_balance_max_now'] = np.nan
    dict_out['loan_amount_max_open_03m'] = np.nan
    dict_out['loan_current_ndue_RepayedAmount_max'] = np.nan
    dict_out['consume_loan_slight_overdue_RepayedAmount_sum_06m_now'] = np.nan
    dict_out['stu_loan_slight_overdue_balance_sum_now'] = np.nan
    dict_out['business_loan_slight_overdue_currentOverdueAmount_max_now'] = np.nan
    dict_out['consume_loan_ndue_is_vouch_amount_sum'] = np.nan
    dict_out['loan_ndue_is_vouch_RepayedAmount_sum_03m_now'] = np.nan
    dict_out['consume_loan_currentOverdueAmount_sum_now'] = np.nan
    dict_out['business_loan_slight_overdue_account_count_03m'] = np.nan
    dict_out['business_loan_is_vouch_ndue_account_count_06m_now'] = np.nan
    dict_out['consume_loan_is_vouch_slight_overdue_amount_max'] = np.nan
    dict_out['consume_loan_overdue31Amount_max_now'] = np.nan
    dict_out['consume_loan_is_vouch_overdue_month_sum_12m'] = np.nan
    dict_out['stu_loan_rating_worst'] = np.nan
    dict_out['business_loan_slight_overdue_RepayedAmount_max_now'] = np.nan
    dict_out['consume_loan_is_vouch_serious_overdue_planRepayAmount_sum_12m_now'] = np.nan
    dict_out['loan_overdue_month_sum_open_06m'] = np.nan
    dict_out['consume_loan_slight_overdue_planRepayAmount_sum_03m_now'] = np.nan
    dict_out['business_loan_slight_overdue_RepayedAmount_man_03m_now'] = np.nan
    dict_out['loan_is_vouch_RepayedAmount_max_now'] = np.nan
    dict_out['business_loan_serious_overdue_amount_max_now'] = np.nan
    dict_out['business_loan_slight_overdue_balance_max_now'] = np.nan
    dict_out['stu_loan_slight_overdue_amount_max'] = np.nan
    dict_out['business_loan_is_vouch_ndue_RepayedAmount_max_now'] = np.nan
    dict_out['business_loan_overdue61Amount_sum_now'] = np.nan
    dict_out['house_loan_overdue61Amount_sum_now'] = np.nan
    dict_out['business_loan_serious_overdue_planRepayAmount_sum_06m_now'] = np.nan
    dict_out['consume_loan_ndue_is_vouch_RepayedAmount_max_12m_now'] = np.nan
    dict_out['grt_2y_qrynum'] = np.nan
    dict_out['consume_loan_serious_overdue_RepayedAmount_max_06m_now'] = np.nan
    dict_out['loan_amount_sum_now'] = np.nan
    dict_out['consume_loan_is_vouch_slight_overdue_planRepayAmount_max_03m_now'] = np.nan
    dict_out['consume_loan_overdue_month_max_24m_now'] = np.nan
    dict_out['consume_loan_is_vouch_slight_overdue_RepayedAmount_max_06m_now'] = np.nan
    dict_out['stu_loan_slight_overdue_account_count'] = np.nan
    dict_out['consume_loan_serious_overdue_amount_max_now'] = np.nan
    dict_out['house_loan_RepayedAmount_sum_now'] = np.nan
    dict_out['blnbsn_blnasn'] = np.nan
    dict_out['car_loan_overdue_month_max_now'] = np.nan
    dict_out['loan_recent_serious_overdue_currentOverdueAmount_max'] = np.nan
    dict_out['loan_ndue_planRepayAmount_sum_now'] = np.nan
    dict_out['consume_loan_overdue_month_max_24m'] = np.nan
    dict_out['nloc_bal'] = np.nan
    dict_out['consume_loan_currentOverdueTerms_max_now'] = np.nan
    dict_out['consume_loan_amount_sum'] = np.nan
    dict_out['loan_serious_overdue_planRepayAmount_max_12m_now'] = np.nan
    dict_out['loan_serious_overdue_RepayedAmount_sum_12m_now'] = np.nan
    dict_out['loan_is_vouch_planRepayAmount_max_now'] = np.nan
    dict_out['loan_ndue_is_vouch_planRepayAmount_sum_03m_now'] = np.nan
    dict_out['business_loan_slight_overdue_month_sum_06m_now'] = np.nan
    dict_out['consume_loan_slight_overdue_RepayedAmount_sum_12m_now'] = np.nan
    dict_out['loan_account_count_open_06m'] = np.nan
    dict_out['business_loan_slight_overdue_planRepayAmount_max_06m_now'] = np.nan
    dict_out['house_loan_serious_overdue_balance_sum_now'] = np.nan
    dict_out['loan_slight_overdue_is_vouch_account_count_now'] = np.nan
    dict_out['mhouseloan_num'] = np.nan
    dict_out['loan_overdue_month_max_06m'] = np.nan
    dict_out['loan_recent_slight_overdue_planRepayAmount_max'] = np.nan
    dict_out['business_loan_slight_overdue_is_vouch_month_sum'] = np.nan
    dict_out['car_loan_overdue_month_max'] = np.nan
    dict_out['business_loan_is_vouch_slight_overdue_month_sum_06m_now'] = np.nan
    dict_out['clivsoacn_lac'] = np.nan
    dict_out['car_loan_overdue61Amount_sum_now'] = np.nan
    dict_out['stu_loan_slight_overdue_balance_max_now'] = np.nan
    dict_out['loan_is_vouch_serious_overdue_RepayedAmount_max_24m_now'] = np.nan
    dict_out['house_loan_rating_worst'] = np.nan
    dict_out['loan_ndue_RepayedAmount_max_06m_now'] = np.nan
    dict_out['loan_is_vouch_is_break_amount_sum'] = np.nan
    dict_out['loan_is_vouch_slight_overdue_planRepayAmount_sum_24m_now'] = np.nan
    dict_out['car_loan_serious_overdue_month_sum_now'] = np.nan
    dict_out['business_loan_serious_overdue_month_sum_06m'] = np.nan
    dict_out['car_loan_ndue_planRepayAmount_max_now'] = np.nan
    dict_out['consume_loan_serious_overdue_month_sum'] = np.nan
    dict_out['consume_loan_ndue_is_vouch_RepayedAmount_sum_03m_now'] = np.nan
    dict_out['business_loan_slight_overdue_month_sum_06m'] = np.nan
    dict_out['car_loan_account_count'] = np.nan
    dict_out['house_loan_serious_overdue_month_sum_now'] = np.nan
    dict_out['house_loan_overdue_month_sum'] = np.nan
    dict_out['business_loan_is_vouch_ndue_amount_sum_now'] = np.nan
    dict_out['business_loan_amount_max_now'] = np.nan
    dict_out['minuseday'] = np.nan
    dict_out['consume_loan_ndue_account_count_06m'] = np.nan
    dict_out['business_loan_serious_overdue_RepayedAmount_sum_12m_now'] = np.nan
    dict_out['business_loan_ndue_RepayedAmount_max_now'] = np.nan
    dict_out['loan_slight_overdue_is_vouch_currentOverdueAmount_sum_now'] = np.nan
    dict_out['consume_loan_serious_overdue_account_count_06m'] = np.nan
    dict_out['clnacn_lacn'] = np.nan
    dict_out['business_loan_is_vouch_ndue_planRepayAmount_sum_now'] = np.nan
    dict_out['loan_serious_overdue_balance_sum_now'] = np.nan
    dict_out['loan_slight_overdue_amount_sum_now'] = np.nan
    dict_out['loan_current_ndue_RepayedAmount_sum'] = np.nan
    dict_out['business_loan_is_vouch_serious_overdue_month_sum_06m'] = np.nan
    dict_out['business_loan_is_vouch_overdue_month_sum'] = np.nan
    dict_out['business_loan_serious_overdue_account_count_03m_now'] = np.nan
    dict_out['loan_serious_overdue_planRepayAmount_max_now'] = np.nan
    dict_out['recent_loan_overdue_month_sum'] = np.nan
    dict_out['consume_loan_is_vouch_planRepayAmount_max_now'] = np.nan
    dict_out['business_loan_account_count_now'] = np.nan
    dict_out['loan_is_vouch_slight_overdue_month_sum_06m'] = np.nan
    dict_out['consume_loan_balance_max_now'] = np.nan
    dict_out['loan_is_vouch_overdue_month_max_06m'] = np.nan
    dict_out['car_loan_amount_sum_now'] = np.nan
    dict_out['loan_is_vouch_slight_overdue_account_count_06m'] = np.nan
    dict_out['loan_is_vouch_slight_overdue_RepayedAmount_sum_06m_now'] = np.nan
    dict_out['business_loan_is_vouch_serious_overdue_RepayedAmount_max_03m_now'] = np.nan
    dict_out['consume_loan_ndue_amount_max_now'] = np.nan
    dict_out['loan_slight_overdue_is_vouch_amount_max_now'] = np.nan
    dict_out['business_loan_is_vouch_ndue_RepayedAmount_sum_24m_now'] = np.nan
    dict_out['business_loan_serious_overdue_month_sum'] = np.nan
    dict_out['cqln_ovd_num'] = np.nan
    dict_out['lsoacn_lac'] = np.nan
    dict_out['stu_loan_serious_overdue_balance_sum_now'] = np.nan
    dict_out['loan_slight_overdue_RepayedAmount_sum_now'] = np.nan
    dict_out['crt_ovd_maxmth'] = np.nan
    dict_out['loan_slight_overdue_currentOverdueAmount_max_now'] = np.nan
    dict_out['business_loan_is_vouch_overdue_month_max_03m'] = np.nan
    dict_out['loan_slight_overdue_planRepayAmount_max_03m_now'] = np.nan
    dict_out['business_loan_slight_overdue_planRepayAmount_max_24m_now'] = np.nan
    dict_out['consume_loan_is_vouch_slight_overdue_RepayedAmount_sum_12m_now'] = np.nan
    dict_out['consume_loan_slight_overdue_account_count_03m'] = np.nan
    dict_out['house_loan_account_count'] = np.nan
    dict_out['consume_loan_ndue_is_vouch_account_count_06m_now'] = np.nan
    dict_out['business_loan_is_vouch_slight_overdue_account_count_06m'] = np.nan
    dict_out['lsobsn_lsoasn'] = np.nan
    dict_out['business_loan_overdue180Amount_max_now'] = np.nan
    dict_out['car_loan_serious_overdue_account_count_now'] = np.nan
    dict_out['loan_is_vouch_serious_overdue_currentOverdueAmount_max_now'] = np.nan
    dict_out['consume_loan_is_vouch_overdue_month_max_now'] = np.nan
    dict_out['business_loan_is_vouch_balance_max_now'] = np.nan
    dict_out['business_loan_ndue_RepayedAmount_max_12m_now'] = np.nan
    dict_out['crt_24m_duemax'] = np.nan
    dict_out['loan_account_CD'] = np.nan
    dict_out['loan_slight_overdue_RepayedAmount_sum_03m_now'] = np.nan
    dict_out['consume_loan_overdue_month_sum_06m_now'] = np.nan
    dict_out['loan_rating_worst'] = np.nan
    dict_out['loan_is_vouch_slight_overdue_RepayedAmount_sum_12m_now'] = np.nan
    dict_out['consume_loan_serious_overdue_month_sum_06m_now'] = np.nan
    dict_out['cln_6mpay_amt'] = np.nan
    dict_out['house_loan_serious_overdue_balance_max_now'] = np.nan
    dict_out['loan_slight_overdue_month_sum_24m'] = np.nan
    dict_out['lsoasn_lasn'] = np.nan
    dict_out['consume_loan_planRepayAmount_max_now'] = np.nan
    dict_out['loan_is_vouch_currentOverdueAmount_sum_now'] = np.nan
    dict_out['business_loan_overdue91Amount_max_now'] = np.nan
    dict_out['consume_loan_is_vouch_slight_overdue_planRepayAmount_sum_now'] = np.nan
    dict_out['consume_loan_overdue_month_sum_24m_now'] = np.nan
    dict_out['consume_loan_is_vouch_serious_overdue_RepayedAmount_sum_03m_now'] = np.nan
    dict_out['stu_loan_slight_overdue_month_sum'] = np.nan
    dict_out['loan_is_vouch_serious_overdue_month_sum_03m_now'] = np.nan
    dict_out['consume_loan_is_vouch_overdue_month_sum_24m'] = np.nan
    dict_out['business_loan_slight_overdue_planRepayAmount_max_now'] = np.nan
    dict_out['business_loan_ndue_account_count_06m'] = np.nan
    dict_out['business_loan_slight_overdue_planRepayAmount_max_12m_now'] = np.nan
    dict_out['consume_loan_is_vouch_amount_max_now'] = np.nan
    dict_out['query_6m_lnsum'] = np.nan
    dict_out['loan_serious_overdue_planRepayAmount_sum_06m_now'] = np.nan
    dict_out['house_loan_balance_sum_now'] = np.nan
    dict_out['consume_loan_is_vouch_overdue_month_sum_06m_now'] = np.nan
    dict_out['consume_loan_ndue_is_vouch_RepayedAmount_max_24m_now'] = np.nan
    dict_out['stu_loan_overdue91Amount_sum_now'] = np.nan
    dict_out['house_loan_planRepayAmount_max_now'] = np.nan
    dict_out['loan_is_vouch_slight_overdue_account_count_24m'] = np.nan
    dict_out['business_loan_is_vouch_serious_overdue_account_count_12m'] = np.nan
    dict_out['business_loan_serious_overdue_month_sum_12m_now'] = np.nan
    dict_out['consume_loan_is_vouch_slight_overdue_month_sum'] = np.nan
    dict_out['loan_overdue_month_sum_03m'] = np.nan
    dict_out['loan_recent_slight_overdue_balance_sum'] = np.nan
    dict_out['consume_loan_ndue_RepayedAmount_max_06m_now'] = np.nan
    dict_out['loan_overdue61Amount_max_now'] = np.nan
    dict_out['loan_amount_max_overdue_month_sum'] = np.nan
    dict_out['business_loan_slight_overdue_amount_max'] = np.nan
    dict_out['loan_overdue_month_max'] = np.nan
    dict_out['loan_overdue_month_sum_12m_now'] = np.nan
    dict_out['business_loan_ndue_account_count_03m'] = np.nan
    dict_out['loan_slight_overdue_planRepayAmount_sum_06m_now'] = np.nan
    dict_out['consume_loan_serious_overdue_currentOverdueAmount_sum_now'] = np.nan
    dict_out['consume_loan_is_vouch_rating_worst'] = np.nan
    dict_out['stu_loan_slight_overdue_planRepayAmount_sum_now'] = np.nan
    dict_out['business_loan_is_vouch_slight_overdue_account_count_12m'] = np.nan
    dict_out['business_loan_is_vouch_ndue_account_count_12m_now'] = np.nan
    dict_out['business_loan_is_vouch_ndue_account_count_24m_now'] = np.nan
    dict_out['loan_slight_overdue_is_vouch_amount_max'] = np.nan
    dict_out['business_loan_is_vouch_overdue_month_max_24m'] = np.nan
    dict_out['consume_loan_ndue_is_vouch_RepayedAmount_sum_24m_now'] = np.nan
    dict_out['cln_amt'] = np.nan
    dict_out['loan_balance_sum_now'] = np.nan
    dict_out['consume_loan_ndue_is_vouch_RepayedAmount_max_03m_now'] = np.nan
    dict_out['consume_loan_slight_overdue_account_count_06m_now'] = np.nan
    dict_out['loan_is_vouch_loanAmount_sum'] = np.nan
    dict_out['loan_slight_overdue_is_vouch_account_count'] = np.nan
    dict_out['consume_loan_slight_overdue_amount_sum'] = np.nan
    dict_out['business_loan_slight_overdue_RepayedAmount_man_24m_now'] = np.nan
    dict_out['business_loan_is_vouch_serious_overdue_account_count_now'] = np.nan
    dict_out['query_6m_reasonsum'] = np.nan
    dict_out['consume_loan_overdue_month_max_06m'] = np.nan
    dict_out['loan_overdue_month_sum_06m_now'] = np.nan
    dict_out['consume_loan_is_vouch_slight_overdue_planRepayAmount_max_now'] = np.nan
    dict_out['consume_loan_is_vouch_overdue61Amount_sum_now'] = np.nan
    dict_out['business_loan_serious_overdue_account_count_06m'] = np.nan
    dict_out['consume_loan_overdue_month_max_12m_now'] = np.nan
    dict_out['livsoacn_lac'] = np.nan
    dict_out['otherloan_num'] = np.nan
    dict_out['consume_loan_is_vouch_overdue31Amount_max_now'] = np.nan
    dict_out['business_loan_slight_overdue_planRepayAmount_sum_12m_now'] = np.nan
    dict_out['clibac_lac'] = np.nan
    dict_out['blivasn_lasn'] = np.nan
    dict_out['loan_is_vouch_serious_overdue_account_count_03m_now'] = np.nan
    dict_out['business_loan_slight_overdue_account_count_now'] = np.nan
    dict_out['consume_loan_serious_overdue_amount_sum'] = np.nan
    dict_out['consume_loan_slight_overdue_month_sum_06m_now'] = np.nan
    dict_out['nloc_num'] = np.nan
    dict_out['loan_serious_overdue_account_count_24m'] = np.nan
    dict_out['loan_overdue_month_max_12m_now'] = np.nan
    dict_out['business_loan_is_vouch_ndue_account_count_03m'] = np.nan
    dict_out['business_loan_is_vouch_serious_overdue_account_count_06m'] = np.nan
    dict_out['consume_loan_serious_overdue_planRepayAmount_sum_24m_now'] = np.nan
    dict_out['business_loan_ndue_account_count_now'] = np.nan
    dict_out['consume_loan_ndue_is_vouch_amount_max_now'] = np.nan
    dict_out['collect_acct_num'] = np.nan
    dict_out['stu_loan_serious_overdue_currentOverdueAmount_max_now'] = np.nan
    dict_out['loan_is_vouch_RepayedAmount_sum_now'] = np.nan
    dict_out['consume_loan_is_vouch_is_break_account_count'] = np.nan
    dict_out['loan_ndue_is_vouch_account_count_24m'] = np.nan
    dict_out['crt_num'] = np.nan
    dict_out['consume_loan_is_vouch_overdue_month_max_12m_now'] = np.nan
    dict_out['business_loan_ndue_planRepayAmount_max_12m_now'] = np.nan
    dict_out['consume_loan_balance_sum_now'] = np.nan
    dict_out['business_loan_overdue61Amount_max_now'] = np.nan
    dict_out['laco03_lac'] = np.nan
    dict_out['loan_slight_overdue_is_vouch_amount_sum'] = np.nan
    dict_out['house_loan_ndue_planRepayAmount_sum_now'] = np.nan
    dict_out['consume_loan_is_vouch_slight_overdue_month_sum_03m'] = np.nan
    dict_out['loan_ndue_balance_max_now'] = np.nan
    dict_out['business_loan_is_vouch_serious_overdue_account_count_06m_now'] = np.nan
    dict_out['car_loan_ndue_amount_max'] = np.nan
    dict_out['car_loan_amount_sum'] = np.nan
    dict_out['consume_loan_ndue_account_count_24m'] = np.nan
    dict_out['loan_is_vouch_serious_overdue_RepayedAmount_sum_12m_now'] = np.nan
    dict_out['business_loan_overdue_month_sum_now'] = np.nan
    dict_out['blnasn_lasn'] = np.nan
    dict_out['clnas_las'] = np.nan
    dict_out['car_loan_ndue_balance_max_now'] = np.nan
    dict_out['consume_loan_is_vouch_slight_overdue_month_sum_06m'] = np.nan
    dict_out['consume_loan_amount_max_now'] = np.nan
    dict_out['consume_loan_serious_overdue_month_sum_24m'] = np.nan
    dict_out['business_loan_is_vouch_overdue_month_max_now'] = np.nan
    dict_out['loan_slight_overdue_is_vouch_balance_sum_now'] = np.nan
    dict_out['loan_is_vouch_serious_overdue_account_count_06m'] = np.nan
    dict_out['consume_loan_slight_overdue_currentOverdueAmount_max_now'] = np.nan
    dict_out['loan_ndue_planRepayAmount_max_now'] = np.nan
    dict_out['cln_ovd_mth'] = np.nan
    dict_out['house_loan_currentOverdueTerms_sum_now'] = np.nan
    dict_out['car_loan_planRepayAmount_max_now'] = np.nan
    dict_out['loan_is_vouch_serious_overdue_month_sum'] = np.nan
    dict_out['consume_loan_ndue_planRepayAmount_max_03m_now'] = np.nan
    dict_out['consume_loan_slight_overdue_account_count_now'] = np.nan
    dict_out['ln_1m_orgnum'] = np.nan
    dict_out['stu_loan_currentOverdueAmount_max_now'] = np.nan
    dict_out['business_loan_slight_overdue_account_count_06m_now'] = np.nan
    dict_out['car_loan_balance_sum_now'] = np.nan
    dict_out['consume_loan_is_vouch_overdue_month_max_06m_now'] = np.nan
    dict_out['consume_loan_ndue_planRepayAmount_max_06m_now'] = np.nan
    dict_out['loan_current_ndue_balance_sum'] = np.nan
    dict_out['business_loan_is_vouch_ndue_RepayedAmount_sum_12m_now'] = np.nan
    dict_out['loan_is_vouch_serious_overdue_month_sum_06m_now'] = np.nan
    dict_out['house_loan_planRepayAmount_sum_now'] = np.nan
    dict_out['stu_loan_overdue_month_max'] = np.nan
    dict_out['loan_slight_overdue_planRepayAmount_sum_now'] = np.nan
    dict_out['consume_loan_is_vouch_slight_overdue_account_count_12m'] = np.nan
    dict_out['business_loan_is_vouch_overdue_month_max_03m_now'] = np.nan
    dict_out['consume_loan_is_vouch_balance_sum_now'] = np.nan
    dict_out['business_loan_is_vouch_serious_overdue_account_count_12m_now'] = np.nan
    dict_out['business_loan_is_break_account_count'] = np.nan
    dict_out['consume_loan_slight_overdue_balance_max_now'] = np.nan
    dict_out['clsobsn_clsoasn'] = np.nan
    dict_out['house_loan_serious_overdue_planRepayAmount_sum_now'] = np.nan
    dict_out['clsoacn_lac'] = np.nan
    dict_out['loan_ndue_is_vouch_account_count_03m'] = np.nan
    dict_out['consume_loan_is_vouch_slight_overdue_RepayedAmount_max_03m_now'] = np.nan
    dict_out['loan_is_vouch_overdue_month_max_12m'] = np.nan
    dict_out['loan_is_vouch_overdue_month_sum_24m'] = np.nan
    dict_out['laco03_lacn'] = np.nan
    dict_out['car_loan_serious_overdue_RepayedAmount_max_now'] = np.nan
    dict_out['consume_loan_slight_overdue_planRepayAmount_sum_06m_now'] = np.nan
    dict_out['loan_recent_serious_overdue_balance_max'] = np.nan
    dict_out['crt_6mavg_userate'] = np.nan
    dict_out['consume_loan_is_vouch_serious_overdue_account_count_03m'] = np.nan
    dict_out['crt_max_amt'] = np.nan
    dict_out['business_loan_slight_overdue_month_sum_03m'] = np.nan
    dict_out['degree'] = np.nan
    dict_out['house_loan_overdue91Amount_max_now'] = np.nan
    dict_out['house_loan_ndue_amount_max_now'] = np.nan
    dict_out['loan_slight_overdue_planRepayAmount_sum_24m_now'] = np.nan
    dict_out['business_loan_is_vouch_ndue_balance_sum_now'] = np.nan
    dict_out['stu_loan_overdue61Amount_max_now'] = np.nan
    dict_out['business_loan_slight_overdue_account_count_12m_now'] = np.nan
    dict_out['car_loan_balance_max_now'] = np.nan
    dict_out['business_loan_is_vouch_overdue_month_sum_03m'] = np.nan
    dict_out['consume_loan_is_vouch_serious_overdue_planRepayAmount_sum_03m_now'] = np.nan
    dict_out['consume_loan_is_vouch_slight_overdue_month_sum_now'] = np.nan
    dict_out['consume_loan_ndue_is_vouch_RepayedAmount_sum_06m_now'] = np.nan
    dict_out['loan_ndue_is_vouch_RepayedAmount_sum_12m_now'] = np.nan
    dict_out['consume_loan_is_vouch_serious_overdue_month_sum_06m'] = np.nan
    dict_out['consume_loan_serious_overdue_account_count_03m_now'] = np.nan
    dict_out['loan_serious_overdue_month_sum_24m_now'] = np.nan
    dict_out['house_loan_serious_overdue_currentOverdueAmount_sum_now'] = np.nan
    dict_out['loan_is_vouch_overdue_month_max_03m_now'] = np.nan
    dict_out['loan_amount_sum'] = np.nan
    dict_out['consume_loan_serious_overdue_currentOverdueAmount_max_now'] = np.nan
    dict_out['loan_current_ndue_balance_max'] = np.nan
    dict_out['loan_ndue_is_vouch_amount_max_now'] = np.nan
    dict_out['business_loan_currentOverdueTerms_max_now'] = np.nan
    dict_out['consume_loan_is_vouch_account_count'] = np.nan
    dict_out['consume_loan_slight_overdue_planRepayAmount_sum_now'] = np.nan
    dict_out['consume_loan_overdue_month_max'] = np.nan
    dict_out['consume_loan_is_vouch_serious_overdue_planRepayAmount_max_24m_now'] = np.nan
    dict_out['business_loan_is_vouch_ndue_account_count_24m'] = np.nan
    dict_out['car_loan_slight_overdue_month_sum_now'] = np.nan
    dict_out['blacn_lac'] = np.nan
    dict_out['loan_current_ndue_planRepayAmount_max'] = np.nan
    dict_out['car_loan_currentOverdueAmount_sum_now'] = np.nan
    dict_out['cqln_bal'] = np.nan
    dict_out['loan_is_vouch_serious_overdue_account_count_24m_now'] = np.nan
    dict_out['loan_ndue_planRepayAmount_sum_24m_now'] = np.nan
    dict_out['business_loan_is_vouch_slight_overdue_month_sum_03m_now'] = np.nan
    dict_out['stu_loan_slight_overdue_planRepayAmount_max_now'] = np.nan
    dict_out['car_loan_serious_overdue_planRepayAmount_max_now'] = np.nan
    dict_out['consume_loan_serious_overdue_month_sum_12m_now'] = np.nan
    dict_out['consume_loan_is_vouch_slight_overdue_month_sum_12m_now'] = np.nan
    dict_out['cqln_amt'] = np.nan
    dict_out['business_loan_is_vouch_currentOverdueAmount_max_now'] = np.nan
    dict_out['consume_loan_is_vouch_overdue91Amount_max_now'] = np.nan
    dict_out['consume_loan_overdue_month_sum_12m_now'] = np.nan
    dict_out['car_loan_account_count_now'] = np.nan
    dict_out['lnivacn_lac'] = np.nan
    dict_out['business_loan_is_vouch_slight_overdue_account_count_12m_now'] = np.nan
    dict_out['house_loan_overdue_month_max_now'] = np.nan
    dict_out['loan_ndue_RepayedAmount_sum_06m_now'] = np.nan
    dict_out['consume_loan_is_vouch_serious_overdue_balance_sum_now'] = np.nan
    dict_out['consume_loan_overdue61Amount_max_now'] = np.nan
    dict_out['business_loan_slight_overdue_is_vouch_amount_max_now'] = np.nan
    dict_out['consume_loan_is_vouch_slight_overdue_month_sum_24m'] = np.nan
    dict_out['business_loan_is_vouch_serious_overdue_account_count_03m_now'] = np.nan
    dict_out['business_loan_RepayedAmount_sum_now'] = np.nan
    dict_out['loan_current_ndue_account_count'] = np.nan
    dict_out['consume_loan_slight_overdue_month_sum_now'] = np.nan
    dict_out['business_loan_is_vouch_serious_overdue_RepayedAmount_sum_now'] = np.nan
    dict_out['business_loan_ndue_planRepayAmount_max_06m_now'] = np.nan
    dict_out['business_loan_slight_overdue_amount_max_now'] = np.nan
    dict_out['loan_ndue_is_vouch_account_count_24m_now'] = np.nan
    dict_out['house_loan_ndue_amount_sum'] = np.nan
    dict_out['loan_is_vouch_serious_overdue_planRepayAmount_max_12m_now'] = np.nan
    dict_out['business_loan_is_vouch_account_count_now'] = np.nan
    dict_out['house_loan_slight_overdue_balance_max_now'] = np.nan
    dict_out['house_loan_ndue_account_count'] = np.nan
    dict_out['stu_loan_ndue_planRepayAmount_sum_now'] = np.nan
    dict_out['lacn_lac'] = np.nan
    dict_out['consume_loan_serious_overdue_planRepayAmount_sum_12m_now'] = np.nan
    dict_out['consume_loan_overdue31Amount_sum_now'] = np.nan
    dict_out['loan_currentOverdueTerms_max_now'] = np.nan
    dict_out['loan_is_vouch_slight_overdue_RepayedAmount_max_24m_now'] = np.nan
    dict_out['loan_ndue_account_count_06m_now'] = np.nan
    dict_out['house_loan_slight_overdue_RepayedAmount_sum_now'] = np.nan
    dict_out['stu_loan_overdue_month_sum_now'] = np.nan
    dict_out['consume_loan_is_vouch_serious_overdue_month_sum_24m_now'] = np.nan
    dict_out['business_loan_overdue_month_max_12m'] = np.nan
    dict_out['business_loan_ndue_planRepayAmount_sum_06m_now'] = np.nan
    dict_out['diploma'] = np.nan
    dict_out['crt_12m_duemax'] = np.nan
    dict_out['query_1m_reasonsum'] = np.nan
    dict_out['consume_loan_is_vouch_slight_overdue_balance_sum_now'] = np.nan
    dict_out['house_loan_overdue91Amount_sum_now'] = np.nan
    dict_out['blbsn_blasn'] = np.nan
    dict_out['house_loan_slight_overdue_account_count'] = np.nan
    dict_out['business_loan_is_vouch_slight_overdue_planRepayAmount_max_06m_now'] = np.nan
    dict_out['consume_loan_overdue180Amount_sum_now'] = np.nan
    dict_out['loan_ndue_is_vouch_RepayedAmount_sum_06m_now'] = np.nan
    dict_out['first_loan_amount'] = np.nan
    dict_out['business_loan_is_vouch_ndue_RepayedAmount_sum_03m_now'] = np.nan
    dict_out['qcrt_num'] = np.nan
    dict_out['business_loan_ndue_planRepayAmount_sum_12m_now'] = np.nan
    dict_out['consume_loan_is_vouch_slight_overdue_amount_sum_now'] = np.nan
    dict_out['house_loan_overdue31Amount_sum_now'] = np.nan
    dict_out['loan_ndue_is_vouch_account_count_12m_now'] = np.nan
    dict_out['consume_loan_is_vouch_slight_overdue_RepayedAmount_max_24m_now'] = np.nan
    dict_out['blsoas_las'] = np.nan
    dict_out['qcrt_ovd_num'] = np.nan
    dict_out['loan_slight_overdue_month_sum_06m_now'] = np.nan
    dict_out['consume_loan_serious_overdue_RepayedAmount_sum_now'] = np.nan
    dict_out['birthday'] = np.nan
    dict_out['business_loan_serious_overdue_planRepayAmount_sum_24m_now'] = np.nan
    dict_out['loan_ndue_amount_max'] = np.nan
    dict_out['consume_loan_is_vouch_overdue180Amount_max_now'] = np.nan
    dict_out['stu_loan_serious_overdue_balance_max_now'] = np.nan
    dict_out['consume_loan_is_vouch_serious_overdue_planRepayAmount_sum_now'] = np.nan
    dict_out['business_loan_serious_overdue_month_sum_12m'] = np.nan
    dict_out['loan_is_vouch_overdue_month_sum_24m_now'] = np.nan
    dict_out['consume_loan_is_vouch_overdue180Amount_sum_now'] = np.nan
    dict_out['car_loan_serious_overdue_amount_max_now'] = np.nan
    dict_out['loan_currentOverdueAmount_sum_now'] = np.nan
    dict_out['consume_loan_is_vouch_slight_overdue_RepayedAmount_max_12m_now'] = np.nan
    dict_out['lst_qry_reason'] = np.nan
    dict_out['consume_loan_is_vouch_overdue_month_max_24m'] = np.nan
    dict_out['ln_24m_duemax'] = np.nan
    dict_out['car_loan_is_break_account_count'] = np.nan
    dict_out['consume_loan_is_vouch_slight_overdue_month_sum_24m_now'] = np.nan
    dict_out['qcrt_amt'] = np.nan
    dict_out['loan_overdue_month_max_12m'] = np.nan
    dict_out['car_loan_slight_overdue_currentOverdueAmount_max_now'] = np.nan
    dict_out['loan_is_vouch_overdue_month_max_24m'] = np.nan
    dict_out['car_loan_slight_overdue_planRepayAmount_max_now'] = np.nan
    dict_out['business_loan_serious_overdue_RepayedAmount_max_03m_now'] = np.nan
    dict_out['loan_currentOverdueAmount_max_now'] = np.nan
    dict_out['house_loan_overdue_month_max'] = np.nan
    dict_out['loan_ndue_account_count_06m'] = np.nan
    dict_out['car_loan_serious_overdue_month_sum'] = np.nan
    dict_out['business_loan_is_vouch_overdue_month_sum_now'] = np.nan
    dict_out['loan_slight_overdue_account_count_06m'] = np.nan
    dict_out['consume_loan_slight_overdue_amount_sum_now'] = np.nan
    dict_out['qcrt_ovd_maxmth'] = np.nan
    dict_out['loan_slight_overdue_amount_max_now'] = np.nan
    dict_out['house_loan_currentOverdueTerms_max_now'] = np.nan
    dict_out['consume_loan_RepayedAmount_max_now'] = np.nan
    dict_out['consume_loan_ndue_planRepayAmount_sum_03m_now'] = np.nan
    dict_out['consume_loan_ndue_planRepayAmount_sum_06m_now'] = np.nan
    dict_out['house_loan_slight_overdue_month_sum'] = np.nan
    dict_out['consume_loan_slight_overdue_RepayedAmount_man_06m_now'] = np.nan
    dict_out['loan_serious_overdue_planRepayAmount_max_24m_now'] = np.nan
    dict_out['business_loan_is_vouch_overdue_month_max_06m_now'] = np.nan
    dict_out['loan_slight_overdue_amount_max'] = np.nan
    dict_out['stu_loan_balance_sum_now'] = np.nan
    dict_out['loan_serious_overdue_amount_sum'] = np.nan
    dict_out['crt_org_num'] = np.nan
    dict_out['business_loan_is_vouch_overdue_month_max_24m_now'] = np.nan
    dict_out['business_loan_is_vouch_ndue_balance_max_now'] = np.nan
    dict_out['loan_slight_overdue_RepayedAmount_max_06m_now'] = np.nan
    dict_out['crt_6m_avgamt'] = np.nan
    dict_out['consume_loan_serious_overdue_account_count_03m'] = np.nan
    dict_out['car_loan_serious_overdue_balance_max_now'] = np.nan
    dict_out['loan_is_vouch_overdue_month_max_24m_now'] = np.nan
    dict_out['business_loan_is_vouch_ndue_amount_max_now'] = np.nan
    dict_out['consume_loan_is_vouch_slight_overdue_account_count_now'] = np.nan
    dict_out['clnacn_lac'] = np.nan
    dict_out['business_loan_slight_overdue_account_count_03m_now'] = np.nan
    dict_out['loan_is_vouch_slight_overdue_planRepayAmount_max_12m_now'] = np.nan
    dict_out['business_loan_is_vouch_overdue_month_sum_24m'] = np.nan
    dict_out['stu_loan_serious_overdue_month_sum'] = np.nan
    dict_out['stu_loan_slight_overdue_month_sum_now'] = np.nan
    dict_out['crt_cnt'] = np.nan
    dict_out['loan_overdue31Amount_sum_now'] = np.nan
    dict_out['loan_is_vouch_loanAmount_sum_now'] = np.nan
    dict_out['loan_is_vouch_slight_overdue_planRepayAmount_sum_03m_now'] = np.nan
    dict_out['blsoas_blas'] = np.nan
    dict_out['loan_slight_overdue_month_sum'] = np.nan
    dict_out['loan_balance_max_now'] = np.nan
    dict_out['loan_overdue_month_max_24m_now'] = np.nan
    dict_out['consume_loan_serious_overdue_planRepayAmount_max_12m_now'] = np.nan
    dict_out['consume_loan_slight_overdue_month_sum_03m_now'] = np.nan
    dict_out['loan_slight_overdue_account_count'] = np.nan
    dict_out['house_loan_slight_overdue_amount_max_now'] = np.nan
    dict_out['loan_overdue_month_sum_now'] = np.nan
    dict_out['consume_loan_ndue_is_vouch_account_count_06m'] = np.nan
    dict_out['business_loan_slight_overdue_is_vouch_amount_max'] = np.nan
    dict_out['loan_is_vouch_slight_overdue_month_sum_12m'] = np.nan
    dict_out['business_loan_planRepayAmount_max_now'] = np.nan
    dict_out['loan_slight_overdue_is_vouch_month_sum'] = np.nan
    dict_out['consume_loan_ndue_account_count_03m_now'] = np.nan
    dict_out['car_loan_ndue_amount_max_now'] = np.nan
    dict_out['business_loan_overdue180Amount_sum_now'] = np.nan
    dict_out['loan_account_CD_now'] = np.nan
    dict_out['business_loan_ndue_amount_max_now'] = np.nan
    dict_out['loan_is_vouch_serious_overdue_aomunt_max'] = np.nan
    dict_out['nloc_6mpay_amt'] = np.nan
    dict_out['ovd_type_num'] = np.nan
    dict_out['loan_serious_overdue_planRepayAmount_sum_now'] = np.nan
    dict_out['lbsn_lasn'] = np.nan
    dict_out['business_loan_slight_overdue_account_count_24m_now'] = np.nan
    dict_out['consume_loan_overdue_month_sum_24m'] = np.nan
    dict_out['clivacn_lacn'] = np.nan
    dict_out['business_loan_serious_overdue_amount_max'] = np.nan
    dict_out['houseloan_bal_sum'] = np.nan
    dict_out['ln_12m_duesum'] = np.nan
    dict_out['loan_ndue_RepayedAmount_sum_24m_now'] = np.nan
    dict_out['consume_loan_is_vouch_slight_overdue_balance_max_now'] = np.nan
    dict_out['ln_3m_expiresum'] = np.nan
    dict_out['business_loan_is_vouch_ndue_account_count_12m'] = np.nan
    dict_out['loan_slight_overdue_month_sum_24m_now'] = np.nan
    dict_out['loan_overdue91Amount_max_now'] = np.nan
    dict_out['business_loan_is_vouch_ndue_account_count_06m'] = np.nan
    dict_out['business_loan_serious_overdue_account_count_12m'] = np.nan
    dict_out['consume_loan_is_vouch_slight_overdue_planRepayAmount_sum_06m_now'] = np.nan
    dict_out['libac_lac'] = np.nan
    dict_out['clivacn_lac'] = np.nan
    dict_out['consume_loan_overdue_month_sum'] = np.nan
    dict_out['business_loan_is_vouch_overdue61Amount_max_now'] = np.nan
    dict_out['loan_overdue_month_sum_12m'] = np.nan
    dict_out['loan_is_vouch_serious_overdue_balance_sum_now'] = np.nan
    dict_out['consume_loan_rating_worst'] = np.nan
    dict_out['house_loan_slight_overdue_month_sum_now'] = np.nan
    dict_out['business_loan_serious_overdue_month_sum_now'] = np.nan
    dict_out['crt_1m_qrynum'] = np.nan
    dict_out['liv_36m_cnt'] = np.nan
    dict_out['consume_loan_ndue_account_count_03m'] = np.nan
    dict_out['consume_loan_slight_overdue_month_sum_12m_now'] = np.nan
    dict_out['consume_loan_is_vouch_slight_overdue_RepayedAmount_sum_24m_now'] = np.nan
    dict_out['lsoacn_lacn'] = np.nan
    dict_out['business_loan_is_vouch_serious_overdue_planRepayAmount_sum_now'] = np.nan
    dict_out['loan_amount_max_time_till_now'] = np.nan
    dict_out['consume_loan_slight_overdue_account_count_24m_now'] = np.nan
    dict_out['house_loan_overdue61Amount_max_now'] = np.nan
    dict_out['consume_loan_is_vouch_overdue_month_sum_12m_now'] = np.nan
    dict_out['loan_overdue_month_max_24m'] = np.nan
    dict_out['car_loan_slight_overdue_account_count'] = np.nan
    dict_out['loan_ndue_account_count'] = np.nan
    dict_out['consume_loan_serious_overdue_month_sum_03m_now'] = np.nan
    dict_out['consume_loan_is_vouch_slight_overdue_account_count_24m'] = np.nan
    dict_out['stu_loan_amount_sum'] = np.nan
    dict_out['loan_slight_overdue_account_count_12m'] = np.nan
    dict_out['loan_ndue_amount_sum'] = np.nan
    dict_out['business_loan_is_vouch_ndue_planRepayAmount_sum_03m_now'] = np.nan
    dict_out['business_loan_is_vouch_serious_overdue_account_count_24m'] = np.nan
    dict_out['consume_loan_is_vouch_overdue61Amount_max_now'] = np.nan
    dict_out['loan_loanAmount_max_now'] = np.nan
    dict_out['loan_overdue_month_sum_03m_now'] = np.nan
    dict_out['loan_slight_overdue_RepayedAmount_max_03m_now'] = np.nan
    dict_out['loan_serious_overdue_account_count_24m_now'] = np.nan
    dict_out['consume_loan_is_vouch_overdue_month_sum_03m_now'] = np.nan
    dict_out['stu_loan_overdue31Amount_sum_now'] = np.nan
    dict_out['consume_loan_ndue_account_count_24m_now'] = np.nan
    dict_out['consume_loan_slight_overdue_account_count_24m'] = np.nan
    dict_out['business_loan_is_vouch_serious_overdue_account_count_24m_now'] = np.nan
    dict_out['car_loan_currentOverdueAmount_max_now'] = np.nan
    dict_out['business_loan_is_vouch_serious_overdue_RepayedAmount_sum_12m_now'] = np.nan
    dict_out['car_loan_ndue_planRepayAmount_sum_now'] = np.nan
    dict_out['loan_is_vouch_overdue_month_max_06m_now'] = np.nan
    dict_out['business_loan_is_vouch_serious_overdue_amount_max_now'] = np.nan
    dict_out['loan_is_vouch_serious_overdue_RepayedAmount_sum_24m_now'] = np.nan
    dict_out['qcrt_min_amt'] = np.nan
    dict_out['consume_loan_is_vouch_overdue_month_sum_now'] = np.nan
    dict_out['loan_account_count_open_03m'] = np.nan
    dict_out['consume_loan_is_vouch_overdue_month_max_03m_now'] = np.nan
    dict_out['business_loan_is_vouch_serious_overdue_month_sum_12m_now'] = np.nan
    dict_out['consume_loan_slight_overdue_planRepayAmount_max_03m_now'] = np.nan
    dict_out['business_loan_serious_overdue_RepayedAmount_sum_06m_now'] = np.nan
    dict_out['consume_loan_serious_overdue_account_count_12m_now'] = np.nan
    dict_out['stu_loan_ndue_balance_sum_now'] = np.nan
    dict_out['business_loan_is_vouch_serious_overdue_planRepayAmount_max_06m_now'] = np.nan
    dict_out['query_24m_crtsum'] = np.nan
    dict_out['loan_recent_slight_overdue_month_sum'] = np.nan
    dict_out['consume_loan_serious_overdue_month_sum_24m_now'] = np.nan
    dict_out['loan_serious_overdue_month_sum_12m'] = np.nan
    dict_out['loan_recent_slight_overdue_balance_max'] = np.nan
    dict_out['consume_loan_serious_overdue_account_count_24m'] = np.nan
    dict_out['business_loan_is_vouch_ndue_planRepayAmount_sum_12m_now'] = np.nan
    dict_out['stu_loan_ndue_balance_max_now'] = np.nan
    dict_out['consume_loan_is_vouch_serious_overdue_account_count_12m'] = np.nan
    dict_out['consume_loan_slight_overdue_account_count_12m_now'] = np.nan
    dict_out['loan_serious_overdue_month_sum_06m'] = np.nan
    dict_out['otherloan_fstmth'] = np.nan
    dict_out['id_type'] = np.nan
    dict_out['consume_loan_is_vouch_RepayedAmount_max_now'] = np.nan
    dict_out['paidby_num'] = np.nan
    dict_out['blivacn_lac'] = np.nan
    dict_out['loan_is_vouch_serious_overdue_amount_sum_now'] = np.nan
    dict_out['loan_slight_overdue_balance_sum_now'] = np.nan
    dict_out['business_loan_is_vouch_slight_overdue_account_count_03m_now'] = np.nan
    dict_out['paidby_bal'] = np.nan
    dict_out['consume_loan_is_vouch_slight_overdue_planRepayAmount_max_12m_now'] = np.nan
    dict_out['business_loan_is_vouch_overdue_month_max_12m'] = np.nan
    dict_out['crt_24m_duesum'] = np.nan
    dict_out['crt_1m_orgnum'] = np.nan
    dict_out['business_loan_serious_overdue_RepayedAmount_max_06m_now'] = np.nan
    dict_out['crt_ovd_maxamt'] = np.nan
    dict_out['business_loan_is_vouch_slight_overdue_month_sum_12m_now'] = np.nan
    dict_out['loan_is_vouch_slight_overdue_account_count_12m'] = np.nan
    dict_out['consume_loan_is_vouch_serious_overdue_amount_max'] = np.nan
    dict_out['consume_loan_is_vouch_slight_overdue_account_count_06m'] = np.nan
    dict_out['consume_loan_is_vouch_slight_overdue_month_sum_06m_now'] = np.nan
    dict_out['business_loan_serious_overdue_planRepayAmount_sum_now'] = np.nan
    dict_out['consume_loan_slight_overdue_month_sum'] = np.nan
    dict_out['business_loan_is_vouch_slight_overdue_account_count_24m_now'] = np.nan
    dict_out['loan_recent_slight_overdue_account_count'] = np.nan
    dict_out['business_loan_serious_overdue_balance_sum_now'] = np.nan
    dict_out['business_loan_serious_overdue_month_sum_24m_now'] = np.nan
    dict_out['loan_overdue_month_max_open_03m'] = np.nan
    dict_out['business_loan_is_vouch_overdue_month_max'] = np.nan
    dict_out['car_loan_overdue91Amount_sum_now'] = np.nan
    dict_out['business_loan_is_vouch_overdue_month_max_06m'] = np.nan
    dict_out['cln_ovd_cnt'] = np.nan
    dict_out['crt_ovd_mth'] = np.nan
    dict_out['loan_slight_overdue_is_vouch_planRepayAmount_max_now'] = np.nan
    dict_out['loan_serious_overdue_account_count_06m'] = np.nan
    dict_out['cqln_6mpay_amt'] = np.nan
    dict_out['stu_loan_slight_overdue_RepayedAmount_max_now'] = np.nan
    dict_out['accfund_24m_amount'] = np.nan
    dict_out['consume_loan_slight_overdue_month_sum_03m'] = np.nan
    dict_out['loan_slight_overdue_is_vouch_amount_sum_now'] = np.nan
    dict_out['business_loan_account_count'] = np.nan
    dict_out['loan_is_vouch_overdue91Amount_max_now'] = np.nan
    dict_out['consume_loan_serious_overdue_account_count_06m_now'] = np.nan
    dict_out['house_loan_ndue_RepayedAmount_max_now'] = np.nan
    dict_out['business_loan_is_vouch_ndue_account_count_03m_now'] = np.nan
    dict_out['consume_loan_slight_overdue_RepayedAmount_man_12m_now'] = np.nan
    dict_out['stu_loan_overdue_month_max_now'] = np.nan
    dict_out['loan_ndue_amount_max_now'] = np.nan
    dict_out['business_loan_slight_overdue_planRepayAmount_sum_06m_now'] = np.nan
    dict_out['business_loan_is_vouch_ndue_RepayedAmount_sum_06m_now'] = np.nan
    dict_out['loan_ndue_is_vouch_planRepayAmount_max_24m_now'] = np.nan
    dict_out['consume_loan_is_vouch_RepayedAmount_sum_now'] = np.nan
    dict_out['business_loan_is_vouch_serious_overdue_month_sum_24m'] = np.nan
    dict_out['qcrt_max_amt'] = np.nan
    dict_out['consume_loan_is_vouch_slight_overdue_planRepayAmount_sum_24m_now'] = np.nan
    dict_out['business_loan_ndue_account_count'] = np.nan
    dict_out['consume_loan_slight_overdue_month_sum_24m'] = np.nan
    dict_out['loan_slight_overdue_currentOverdueAmount_sum_now'] = np.nan
    dict_out['loan_serious_overdue_RepayedAmount_max_06m_now'] = np.nan
    dict_out['business_loan_is_vouch_slight_overdue_RepayedAmount_sum_06m_now'] = np.nan
    dict_out['business_loan_ndue_account_count_06m_now'] = np.nan
    dict_out['loan_is_vouch_slight_overdue_month_sum_12m_now'] = np.nan
    dict_out['loan_ndue_is_vouch_balance_sum_now'] = np.nan
    dict_out['business_loan_is_vouch_slight_overdue_RepayedAmount_max_24m_now'] = np.nan
    dict_out['consume_loan_slight_overdue_planRepayAmount_max_24m_now'] = np.nan
    dict_out['loan_ndue_RepayedAmount_max_now'] = np.nan
    dict_out['consume_loan_is_vouch_serious_overdue_month_sum_now'] = np.nan
    dict_out['recent_loan_time_till_now'] = np.nan
    dict_out['loan_is_vouch_overdue_month_sum'] = np.nan
    dict_out['stu_loan_ndue_RepayedAmount_sum_now'] = np.nan
    dict_out['consume_loan_serious_overdue_account_count_now'] = np.nan
    dict_out['house_loan_overdue180Amount_max_now'] = np.nan
    dict_out['loan_is_vouch_overdue91Amount_sum_now'] = np.nan
    dict_out['consume_loan_ndue_is_vouch_account_count_03m_now'] = np.nan
    dict_out['loan_serious_overdue_account_count_12m'] = np.nan
    dict_out['business_loan_ndue_planRepayAmount_sum_24m_now'] = np.nan
    dict_out['loan_is_vouch_slight_overdue_account_count_06m_now'] = np.nan
    dict_out['loan_is_vouch_is_break_amount_max'] = np.nan
    dict_out['loan_serious_overdue_balance_max_now'] = np.nan
    dict_out['business_loan_overdue_month_max_03m'] = np.nan
    dict_out['query_12m_crtsum'] = np.nan
    dict_out['loan_is_break_amount_max'] = np.nan
    dict_out['houseloan_num'] = np.nan
    dict_out['consume_loan_ndue_is_vouch_account_count'] = np.nan
    dict_out['loan_ndue_planRepayAmount_sum_06m_now'] = np.nan
    dict_out['consume_loan_serious_overdue_RepayedAmount_sum_12m_now'] = np.nan
    dict_out['business_loan_serious_overdue_planRepayAmount_max_03m_now'] = np.nan
    dict_out['loan_slight_overdue_RepayedAmount_max_24m_now'] = np.nan
    dict_out['business_loan_is_vouch_slight_overdue_planRepayAmount_sum_24m_now'] = np.nan
    dict_out['blsoasn_lasn'] = np.nan
    dict_out['house_loan_slight_overdue_planRepayAmount_sum_now'] = np.nan
    dict_out['query_12m_lnsum'] = np.nan
    dict_out['car_loan_overdue31Amount_sum_now'] = np.nan
    dict_out['consume_loan_serious_overdue_month_sum_06m'] = np.nan
    dict_out['consume_loan_slight_overdue_planRepayAmount_max_12m_now'] = np.nan
    dict_out['post_2y_qrynum'] = np.nan
    dict_out['business_loan_is_vouch_ndue_planRepayAmount_max_now'] = np.nan
    dict_out['loan_is_vouch_overdue_month_max'] = np.nan
    dict_out['consume_loan_ndue_RepayedAmount_max_now'] = np.nan
    dict_out['consume_loan_is_vouch_serious_overdue_account_count_06m_now'] = np.nan
    dict_out['loan_ndue_planRepayAmount_sum_03m_now'] = np.nan
    dict_out['business_loan_overdue_month_sum_03m'] = np.nan
    dict_out['business_loan_is_vouch_amount_max'] = np.nan
    dict_out['business_loan_is_vouch_overdue31Amount_sum_now'] = np.nan
    dict_out['emp_status'] = np.nan
    dict_out['business_loan_slight_overdue_is_vouch_amount_sum_now'] = np.nan
    dict_out['business_loan_slight_overdue_month_sum_12m'] = np.nan
    dict_out['business_loan_is_vouch_is_break_amount_sum'] = np.nan
    dict_out['house_loan_ndue_account_count_now'] = np.nan
    dict_out['cln_num'] = np.nan
    dict_out['consume_loan_is_break_account_count'] = np.nan
    dict_out['business_loan_slight_overdue_account_count_12m'] = np.nan
    dict_out['consume_loan_ndue_RepayedAmount_max_03m_now'] = np.nan
    dict_out['loan_GrantOrg_CD_now'] = np.nan
    dict_out['business_loan_is_vouch_slight_overdue_planRepayAmount_max_12m_now'] = np.nan
    dict_out['consume_loan_is_vouch_serious_overdue_account_count_03m_now'] = np.nan
    dict_out['business_loan_is_vouch_serious_overdue_RepayedAmount_max_now'] = np.nan
    dict_out['consume_loan_is_vouch_overdue_month_max'] = np.nan
    dict_out['loan_ndue_is_vouch_account_count_06m'] = np.nan
    dict_out['loan_overdue180Amount_max_now'] = np.nan
    dict_out['livacn_lac'] = np.nan
    dict_out['car_loan_planRepayAmount_sum_now'] = np.nan
    dict_out['business_loan_is_vouch_slight_overdue_planRepayAmount_max_24m_now'] = np.nan
    dict_out['stu_loan_currentOverdueTerms_max_now'] = np.nan
    dict_out['loan_ndue_is_vouch_planRepayAmount_max_03m_now'] = np.nan
    dict_out['business_loan_is_vouch_serious_overdue_amount_sum_now'] = np.nan
    dict_out['loan_is_vouch_serious_overdue_planRepayAmount_max_06m_now'] = np.nan
    dict_out['business_loan_is_vouch_serious_overdue_amount_sum'] = np.nan
    dict_out['loan_ndue_account_count_24m_now'] = np.nan
    dict_out['loan_is_vouch_serious_overdue_account_count_06m_now'] = np.nan
    dict_out['stu_loan_is_break_amount_max'] = np.nan
    dict_out['consume_loan_is_vouch_serious_overdue_balance_max_now'] = np.nan
    dict_out['house_loan_amount_sum'] = np.nan
    dict_out['consume_loan_is_vouch_overdue_month_max_06m'] = np.nan
    dict_out['business_loan_ndue_balance_sum_now'] = np.nan
    dict_out['consume_loan_overdue_month_sum_03m'] = np.nan
    dict_out['stu_loan_account_count'] = np.nan
    dict_out['car_loan_overdue_month_sum'] = np.nan
    dict_out['loan_age'] = np.nan
    dict_out['house_loan_is_break_account_count'] = np.nan
    dict_out['business_loan_ndue_amount_max'] = np.nan
    dict_out['loan_slight_overdue_account_count_12m_now'] = np.nan
    dict_out['car_loan_serious_overdue_amount_sum_now'] = np.nan
    dict_out['house_loan_amount_max_now'] = np.nan
    dict_out['loan_is_vouch_slight_overdue_RepayedAmount_max_12m_now'] = np.nan
    dict_out['recent_loan_rating_worst'] = np.nan
    dict_out['clbsn_clasn'] = np.nan
    dict_out['stu_loan_serious_overdue_account_count'] = np.nan
    dict_out['house_loan_serious_overdue_RepayedAmount_max_now'] = np.nan
    dict_out['business_loan_is_vouch_planRepayAmount_max_now'] = np.nan
    dict_out['qcrt_6m_avgamt'] = np.nan
    dict_out['lnacn_lac'] = np.nan
    dict_out['collect_type_num'] = np.nan
    dict_out['business_loan_is_vouch_ndue_account_count'] = np.nan
    dict_out['loan_recent_serious_overdue_currentOverdueAmount_sum'] = np.nan
    dict_out['nloc_amt'] = np.nan
    dict_out['business_loan_is_vouch_overdue180Amount_sum_now'] = np.nan
    dict_out['business_loan_is_vouch_currentOverdueAmount_sum_now'] = np.nan
    dict_out['car_loan_ndue_account_count'] = np.nan
    dict_out['consume_loan_is_vouch_serious_overdue_month_sum_03m'] = np.nan
    dict_out['loan_amount_max_rating_worst'] = np.nan
    dict_out['business_loan_is_vouch_serious_overdue_amount_max'] = np.nan
    dict_out['consume_loan_is_vouch_serious_overdue_planRepayAmount_sum_24m_now'] = np.nan
    dict_out['stu_loan_ndue_account_count_now'] = np.nan
    dict_out['car_loan_overdue31Amount_max_now'] = np.nan
    dict_out['consume_loan_is_vouch_currentOverdueAmount_max_now'] = np.nan
    dict_out['stu_loan_amount_sum_now'] = np.nan
    dict_out['business_loan_is_vouch_serious_overdue_month_sum_06m_now'] = np.nan
    dict_out['loan_ndue_is_vouch_planRepayAmount_max_now'] = np.nan
    dict_out['house_loan_serious_overdue_amount_max_now'] = np.nan
    dict_out['loan_currentOverdueTerms_sum_now'] = np.nan
    dict_out['loan_ndue_is_vouch_amount_sum_now'] = np.nan
    dict_out['loan_is_vouch_overdue_month_max_12m_now'] = np.nan
    dict_out['consume_loan_ndue_RepayedAmount_sum_12m_now'] = np.nan
    dict_out['consume_loan_slight_overdue_planRepayAmount_max_now'] = np.nan
    dict_out['consume_loan_is_vouch_slight_overdue_account_count_03m'] = np.nan
    dict_out['business_loan_overdue91Amount_sum_now'] = np.nan
    dict_out['cqln_num'] = np.nan
    dict_out['query_3m_lnsum'] = np.nan
    dict_out['consume_loan_is_vouch_slight_overdue_month_sum_03m_now'] = np.nan
    dict_out['loan_ndue_planRepayAmount_max_06m_now'] = np.nan
    dict_out['consume_loan_slight_overdue_amount_max_now'] = np.nan
    dict_out['consume_loan_is_vouch_account_count_now'] = np.nan
    dict_out['business_loan_is_vouch_overdue_month_sum_12m'] = np.nan
    dict_out['car_loan_RepayedAmount_sum_now'] = np.nan
    dict_out['loan_is_vouch_slight_overdue_RepayedAmount_sum_24m_now'] = np.nan
    dict_out['loan_is_vouch_serious_overdue_RepayedAmount_sum_03m_now'] = np.nan
    dict_out['loan_serious_overdue_planRepayAmount_max_06m_now'] = np.nan
    dict_out['business_loan_is_vouch_slight_overdue_account_count_24m'] = np.nan
    dict_out['business_loan_is_vouch_serious_overdue_RepayedAmount_sum_03m_now'] = np.nan
    dict_out['consume_loan_serious_overdue_amount_max'] = np.nan
    dict_out['business_loan_is_vouch_overdue_month_sum_06m_now'] = np.nan
    dict_out['loan_is_vouch_slight_overdue_month_sum_06m_now'] = np.nan
    dict_out['house_loan_serious_overdue_month_sum'] = np.nan
    dict_out['consume_loan_is_vouch_serious_overdue_account_count_24m'] = np.nan
    dict_out['blnas_blas'] = np.nan
    dict_out['consume_loan_slight_overdue_RepayedAmount_sum_now'] = np.nan
    dict_out['business_loan_ndue_RepayedAmount_sum_now'] = np.nan
    dict_out['business_loan_is_vouch_slight_overdue_month_sum_24m_now'] = np.nan
    dict_out['stu_loan_serious_overdue_currentOverdueAmount_sum_now'] = np.nan
    dict_out['business_loan_slight_overdue_is_vouch_currentOverdueAmount_max_now'] = np.nan
    dict_out['loan_serious_overdue_currentOverdueAmount_sum_now'] = np.nan
    dict_out['business_loan_is_vouch_ndue_planRepayAmount_sum_06m_now'] = np.nan
    dict_out['consume_loan_is_vouch_serious_overdue_month_sum_12m'] = np.nan
    dict_out['business_loan_ndue_RepayedAmount_sum_06m_now'] = np.nan
    dict_out['noln_ovd_maxmth'] = np.nan
    dict_out['loan_serious_overdue_planRepayAmount_sum_24m_now'] = np.nan
    dict_out['loan_is_vouch_slight_overdue_planRepayAmount_sum_06m_now'] = np.nan
    dict_out['consume_loan_is_vouch_overdue_month_max_03m'] = np.nan
    dict_out['stu_loan_amount_max_now'] = np.nan
    dict_out['business_loan_is_vouch_ndue_planRepayAmount_max_06m_now'] = np.nan
    dict_out['stu_loan_slight_overdue_RepayedAmount_sum_now'] = np.nan
    dict_out['car_loan_serious_overdue_currentOverdueAmount_sum_now'] = np.nan
    dict_out['liveaddr_num'] = np.nan
    dict_out['loan_recent_slight_overdue_currentOverdueAmount_max'] = np.nan
    dict_out['recent_loan_amount'] = np.nan
    dict_out['stu_loan_currentOverdueTerms_sum_now'] = np.nan
    dict_out['cln_ovd_maxmth'] = np.nan
    dict_out['consume_loan_ndue_is_vouch_RepayedAmount_sum_12m_now'] = np.nan
    dict_out['business_loan_is_vouch_amount_max_now'] = np.nan
    dict_out['loan_is_vouch_serious_overdue_RepayedAmount_max_03m_now'] = np.nan
    dict_out['consume_loan_is_vouch_slight_overdue_amount_sum'] = np.nan
    dict_out['consume_loan_serious_overdue_planRepayAmount_max_03m_now'] = np.nan
    dict_out['clivsoacn_lacn'] = np.nan
    dict_out['houseloan_ever_cnt'] = np.nan
    dict_out['business_loan_slight_overdue_planRepayAmount_sum_03m_now'] = np.nan
    dict_out['consume_loan_is_vouch_overdue_month_max_24m_now'] = np.nan
    dict_out['car_loan_ndue_RepayedAmount_sum_now'] = np.nan
    dict_out['crt_amt'] = np.nan
    dict_out['stu_loan_slight_overdue_amount_sum'] = np.nan
    dict_out['consume_loan_ndue_RepayedAmount_sum_24m_now'] = np.nan
    dict_out['uncqcrt_num'] = np.nan
    dict_out['loan_is_vouch_overdue31Amount_max_now'] = np.nan
    dict_out['business_loan_is_vouch_slight_overdue_RepayedAmount_sum_24m_now'] = np.nan
    dict_out['consume_loan_serious_overdue_planRepayAmount_sum_now'] = np.nan
    dict_out['consume_loan_planRepayAmount_sum_now'] = np.nan
    dict_out['business_loan_ndue_RepayedAmount_max_03m_now'] = np.nan
    dict_out['blnas_las'] = np.nan
    dict_out['car_loan_slight_overdue_currentOverdueAmount_sum_now'] = np.nan
    dict_out['consume_loan_is_vouch_serious_overdue_amount_sum_now'] = np.nan
    dict_out['house_loan_slight_overdue_account_count_now'] = np.nan
    dict_out['consume_loan_ndue_is_vouch_amount_max'] = np.nan
    dict_out['business_loan_slight_overdue_account_count_24m'] = np.nan
    dict_out['reportTime'] = np.nan
    dict_out['consume_loan_ndue_is_vouch_account_count_now'] = np.nan
    dict_out['business_loan_serious_overdue_account_count_12m_now'] = np.nan
    dict_out['business_loan_ndue_amount_sum'] = np.nan
    dict_out['loan_is_vouch_serious_overdue_account_count_12m'] = np.nan
    dict_out['consume_loan_is_vouch_serious_overdue_RepayedAmount_max_12m_now'] = np.nan
    dict_out['loan_slight_overdue_month_sum_03m_now'] = np.nan
    dict_out['consume_loan_is_vouch_slight_overdue_planRepayAmount_sum_03m_now'] = np.nan
    dict_out['business_loan_amount_max'] = np.nan
    dict_out['loan_serious_overdue_account_count_03m_now'] = np.nan
    dict_out['query_24m_reasonsum'] = np.nan
    dict_out['loan_slight_overdue_planRepayAmount_sum_03m_now'] = np.nan
    dict_out['consume_loan_slight_overdue_RepayedAmount_man_24m_now'] = np.nan
    dict_out['consume_loan_is_vouch_serious_overdue_currentOverdueAmount_sum_now'] = np.nan
    dict_out['consume_loan_ndue_amount_sum'] = np.nan
    dict_out['business_loan_is_vouch_slight_overdue_month_sum_03m'] = np.nan
    dict_out['business_loan_is_vouch_serious_overdue_balance_sum_now'] = np.nan
    dict_out['loan_is_vouch_serious_overdue_RepayedAmount_sum_06m_now'] = np.nan
    dict_out['business_loan_is_vouch_serious_overdue_account_count'] = np.nan
    dict_out['consume_loan_overdue_month_sum_03m_now'] = np.nan
    dict_out['loan_is_vouch_loanAmount_max'] = np.nan
    dict_out['house_loan_serious_overdue_amount_sum_now'] = np.nan
    dict_out['clsoas_las'] = np.nan
    dict_out['car_loan_slight_overdue_amount_max'] = np.nan
    dict_out['consume_loan_overdue180Amount_max_now'] = np.nan
    dict_out['loan_ndue_balance_sum_now'] = np.nan
    dict_out['loan_is_vouch_serious_overdue_month_sum_now'] = np.nan
    dict_out['consume_loan_ndue_is_vouch_RepayedAmount_max_06m_now'] = np.nan
    dict_out['qcrt_ovd_mth'] = np.nan
    dict_out['consume_loan_ndue_is_vouch_account_count_24m'] = np.nan
    dict_out['consume_loan_is_vouch_planRepayAmount_sum_now'] = np.nan
    dict_out['ln_1m_qrynum'] = np.nan
    dict_out['consume_loan_ndue_account_count_12m'] = np.nan
    dict_out['consume_loan_is_vouch_serious_overdue_account_count_12m_now'] = np.nan
    dict_out['consume_loan_overdue_month_max_03m'] = np.nan
    dict_out['business_loan_serious_overdue_currentOverdueAmount_max_now'] = np.nan
    dict_out['stu_loan_slight_overdue_currentOverdueAmount_sum_now'] = np.nan
    dict_out['consume_loan_is_vouch_serious_overdue_account_count_now'] = np.nan
    dict_out['consume_loan_overdue91Amount_sum_now'] = np.nan
    dict_out['loan_amount_max'] = np.nan
    dict_out['consume_loan_slight_overdue_account_count'] = np.nan
    dict_out['car_loan_ndue_account_count_now'] = np.nan
    dict_out['other_fstmth'] = np.nan
    dict_out['house_loan_serious_overdue_RepayedAmount_sum_now'] = np.nan
    dict_out['house_loan_serious_overdue_currentOverdueAmount_max_now'] = np.nan
    dict_out['business_loan_is_vouch_serious_overdue_month_sum'] = np.nan
    dict_out['car_loan_serious_overdue_account_count'] = np.nan
    dict_out['business_loan_slight_overdue_amount_sum_now'] = np.nan
    dict_out['stu_loan_ndue_account_count'] = np.nan
    dict_out['car_loan_overdue_month_sum_now'] = np.nan
    dict_out['self_2y_qrynum'] = np.nan
    dict_out['loan_is_vouch_currentOverdueTerms_max_now'] = np.nan
    dict_out['consume_loan_overdue_month_sum_06m'] = np.nan
    dict_out['business_loan_overdue_month_sum_12m'] = np.nan
    dict_out['loan_ndue_is_vouch_planRepayAmount_max_06m_now'] = np.nan
    dict_out['disp_num'] = np.nan
    dict_out['loan_is_vouch_count'] = np.nan
    dict_out['loan_serious_overdue_amount_sum_now'] = np.nan
    dict_out['loan_is_vouch_slight_overdue_month_sum_24m_now'] = np.nan
    dict_out['business_loan_slight_overdue_month_sum'] = np.nan
    dict_out['business_loan_is_vouch_ndue_planRepayAmount_max_24m_now'] = np.nan
    dict_out['house_loan_balance_max_now'] = np.nan
    dict_out['car_loan_amount_max_now'] = np.nan
    dict_out['loan_serious_overdue_planRepayAmount_sum_03m_now'] = np.nan
    dict_out['loan_overdue_month_sum_24m_now'] = np.nan
    dict_out['consume_loan_serious_overdue_month_sum_03m'] = np.nan
    dict_out['business_loan_is_vouch_serious_overdue_planRepayAmount_max_24m_now'] = np.nan
    dict_out['blivnacn_lac'] = np.nan
    dict_out['stu_loan_serious_overdue_planRepayAmount_sum_now'] = np.nan
    dict_out['npl_bal'] = np.nan
    dict_out['business_loan_serious_overdue_planRepayAmount_max_now'] = np.nan
    dict_out['business_loan_serious_overdue_planRepayAmount_sum_03m_now'] = np.nan
    dict_out['loan_planRepayAmount_max_now'] = np.nan
    dict_out['car_loan_slight_overdue_account_count_now'] = np.nan
    dict_out['business_loan_serious_overdue_planRepayAmount_sum_12m_now'] = np.nan
    dict_out['loan_is_vouch_slight_overdue_RepayedAmount_max_06m_now'] = np.nan
    dict_out['consume_loan_is_vouch_serious_overdue_amount_sum'] = np.nan
    dict_out['consume_loan_ndue_is_vouch_account_count_12m_now'] = np.nan
    dict_out['stu_loan_balance_max_now'] = np.nan
    dict_out['house_loan_currentOverdueAmount_sum_now'] = np.nan
    dict_out['business_loan_slight_overdue_planRepayAmount_sum_now'] = np.nan
    dict_out['business_loan_slight_overdue_is_vouch_month_sum_now'] = np.nan
    dict_out['business_loan_slight_overdue_is_vouch_currentOverdueAmount_sum_now'] = np.nan
    dict_out['consume_loan_is_vouch_slight_overdue_planRepayAmount_max_06m_now'] = np.nan
    dict_out['houseloan_amount_sum'] = np.nan
    dict_out['business_loan_slight_overdue_is_vouch_balance_sum_now'] = np.nan
    dict_out['consume_loan_overdue91Amount_max_now'] = np.nan
    dict_out['loan_slight_overdue_month_sum_now'] = np.nan
    dict_out['loan_ndue_is_vouch_account_count'] = np.nan
    dict_out['car_loan_RepayedAmount_max_now'] = np.nan
    dict_out['loan_serious_overdue_account_count_06m_now'] = np.nan
    dict_out['car_loan_serious_overdue_RepayedAmount_sum_now'] = np.nan
    dict_out['business_loan_rating_worst'] = np.nan
    dict_out['consume_loan_is_vouch_amount_max'] = np.nan
    dict_out['house_loan_slight_overdue_currentOverdueAmount_max_now'] = np.nan
    dict_out['business_loan_ndue_planRepayAmount_max_24m_now'] = np.nan
    dict_out['business_loan_is_vouch_is_break_amount_max'] = np.nan
    dict_out['consume_loan_overdue_month_max_06m_now'] = np.nan
    dict_out['stu_loan_slight_overdue_amount_max_now'] = np.nan
    dict_out['loan_ndue_planRepayAmount_max_03m_now'] = np.nan
    dict_out['query_24m_lnsum'] = np.nan
    dict_out['house_loan_serious_overdue_amount_max'] = np.nan
    dict_out['consume_loan_ndue_is_vouch_RepayedAmount_max_now'] = np.nan
    dict_out['loan_recent_slight_overdue_RepayedAmount_max'] = np.nan
    dict_out['business_loan_overdue_month_sum'] = np.nan
    dict_out['business_loan_is_vouch_serious_overdue_RepayedAmount_sum_24m_now'] = np.nan
    dict_out['consume_loan_is_vouch_serious_overdue_RepayedAmount_sum_now'] = np.nan
    dict_out['loan_is_vouch_overdue61Amount_sum_now'] = np.nan
    dict_out['consume_loan_is_vouch_amount_sum'] = np.nan
    dict_out['loan_amount_sum_open_03m'] = np.nan
    dict_out['query_12m_reasonsum'] = np.nan
    dict_out['mhouseloan_fstmth'] = np.nan
    dict_out['consume_loan_ndue_account_count_06m_now'] = np.nan
    dict_out['consume_loan_ndue_RepayedAmount_max_24m_now'] = np.nan
    dict_out['house_loan_ndue_RepayedAmount_sum_now'] = np.nan
    dict_out['loan_ndue_account_count_24m'] = np.nan
    dict_out['business_loan_serious_overdue_month_sum_24m'] = np.nan
    dict_out['first_loan_rating_worst'] = np.nan
    dict_out['business_loan_slight_overdue_planRepayAmount_max_03m_now'] = np.nan
    dict_out['business_loan_serious_overdue_account_count'] = np.nan
    dict_out['business_loan_currentOverdueTerms_sum_now'] = np.nan
    dict_out['stu_loan_serious_overdue_amount_sum'] = np.nan
    dict_out['consume_loan_is_vouch_overdue_month_sum_06m'] = np.nan
    dict_out['loan_slight_overdue_month_sum_03m'] = np.nan
    dict_out['inq_reason'] = np.nan
    dict_out['clacn_lac'] = np.nan
    dict_out['disp_bal'] = np.nan
    dict_out['loan_serious_overdue_month_sum_03m'] = np.nan
    dict_out['loan_is_vouch_serious_overdue_planRepayAmount_sum_now'] = np.nan
    dict_out['clivasn_lasn'] = np.nan
    dict_out['car_loan_slight_overdue_amount_sum'] = np.nan
    dict_out['consume_loan_is_vouch_serious_overdue_planRepayAmount_max_now'] = np.nan
    dict_out['business_loan_ndue_RepayedAmount_sum_24m_now'] = np.nan
    dict_out['business_loan_is_vouch_slight_overdue_account_count_06m_now'] = np.nan
    dict_out['loan_overdue61Amount_sum_now'] = np.nan
    dict_out['consume_loan_is_vouch_slight_overdue_account_count_03m_now'] = np.nan
    dict_out['consume_loan_ndue_planRepayAmount_max_now'] = np.nan
    dict_out['consume_loan_serious_overdue_RepayedAmount_max_12m_now'] = np.nan
    dict_out['crt_min_amt'] = np.nan
    dict_out['business_loan_serious_overdue_RepayedAmount_max_24m_now'] = np.nan
    dict_out['consume_loan_overdue_month_sum_12m'] = np.nan
    dict_out['business_loan_is_vouch_ndue_RepayedAmount_max_24m_now'] = np.nan
    dict_out['loan_slight_overdue_account_count_03m_now'] = np.nan
    dict_out['stu_loan_slight_overdue_amount_sum_now'] = np.nan
    dict_out['consume_loan_overdue_month_max_12m'] = np.nan
    dict_out['loan_amount_sum_open_06m'] = np.nan
    dict_out['lnbsn_lnasn'] = np.nan
    dict_out['stu_loan_ndue_amount_max'] = np.nan
    dict_out['blivsoacn_lacn'] = np.nan
    dict_out['loan_recent_serious_overdue_month_sum'] = np.nan
    dict_out['consume_loan_ndue_account_count_12m_now'] = np.nan
    dict_out['loan_is_vouch_serious_overdue_planRepayAmount_sum_06m_now'] = np.nan
    dict_out['merch_2y_qrynum'] = np.nan
    dict_out['loan_ndue_is_vouch_amount_max'] = np.nan
    dict_out['consume_loan_is_vouch_overdue_month_sum_03m'] = np.nan
    dict_out['consume_loan_serious_overdue_amount_sum_now'] = np.nan
    dict_out['loan_is_vouch_serious_overdue_RepayedAmount_max_06m_now'] = np.nan
    dict_out['loan_serious_overdue_RepayedAmount_max_03m_now'] = np.nan
    dict_out['stu_loan_currentOverdueAmount_sum_now'] = np.nan
    dict_out['loan_serious_overdue_currentOverdueAmount_max_now'] = np.nan
    dict_out['consume_loan_is_vouch_serious_overdue_RepayedAmount_max_06m_now'] = np.nan
    dict_out['cqln_ovd_maxmth'] = np.nan
    dict_out['consume_loan_ndue_planRepayAmount_sum_12m_now'] = np.nan
    dict_out['loan_ndue_account_count_now'] = np.nan
    dict_out['house_loan_ndue_balance_sum_now'] = np.nan
    dict_out['loan_serious_overdue_RepayedAmount_sum_24m_now'] = np.nan
    dict_out['houseloan_fstmth'] = np.nan
    dict_out['consume_loan_serious_overdue_RepayedAmount_sum_06m_now'] = np.nan
    dict_out['loan_serious_overdue_account_count'] = np.nan
    dict_out['business_loan_is_vouch_slight_overdue_month_sum_06m'] = np.nan
    dict_out['first_loan_time_till_now'] = np.nan
    dict_out['house_loan_account_count_now'] = np.nan
    dict_out['business_loan_is_vouch_ndue_planRepayAmount_max_03m_now'] = np.nan
    dict_out['lst_qry_date'] = np.nan
    dict_out['business_loan_serious_overdue_account_count_now'] = np.nan
    dict_out['consume_loan_serious_overdue_planRepayAmount_max_06m_now'] = np.nan
    dict_out['car_loan_serious_overdue_currentOverdueAmount_max_now'] = np.nan
    dict_out['stu_loan_account_count_now'] = np.nan
    dict_out['consume_loan_is_vouch_currentOverdueTerms_sum_now'] = np.nan
    dict_out['clnivacn_lac'] = np.nan
    dict_out['loan_ndue_account_count_12m'] = np.nan
    dict_out['business_loan_serious_overdue_amount_sum_now'] = np.nan
    dict_out['ln_12m_duecount'] = np.nan
    dict_out['business_loan_is_vouch_ndue_account_count_now'] = np.nan
    dict_out['business_loan_is_break_amount_sum'] = np.nan
    dict_out['car_loan_ndue_amount_sum'] = np.nan
    dict_out['consume_loan_ndue_account_count_now'] = np.nan
    dict_out['consume_loan_is_vouch_overdue_month_sum_24m_now'] = np.nan
    dict_out['car_loan_serious_overdue_amount_max'] = np.nan
    dict_out['business_loan_serious_overdue_planRepayAmount_max_06m_now'] = np.nan
    dict_out['phone_6m_cnt'] = np.nan
    dict_out['business_loan_slight_overdue_month_sum_24m'] = np.nan
    dict_out['loan_is_break_account_count'] = np.nan
    dict_out['loan_RepayedAmount_max_now'] = np.nan
    dict_out['ln_12m_duemax'] = np.nan
    dict_out['business_loan_is_vouch_account_count'] = np.nan
    dict_out['loan_recent_slight_overdue_currentOverdueAmount_sum'] = np.nan
    dict_out['consume_loan_slight_overdue_planRepayAmount_sum_12m_now'] = np.nan
    dict_out['credit_acct_num'] = np.nan
    dict_out['stu_loan_overdue180Amount_max_now'] = np.nan
    dict_out['business_loan_is_vouch_slight_overdue_RepayedAmount_sum_03m_now'] = np.nan
    dict_out['consume_loan_ndue_is_vouch_planRepayAmount_max_24m_now'] = np.nan
    dict_out['business_loan_is_vouch_slight_overdue_month_sum_12m'] = np.nan
    dict_out['loan_is_vouch_serious_overdue_RepayedAmount_sum_now'] = np.nan
    dict_out['loan_overdue_month_sum_open_03m'] = np.nan
    dict_out['house_loan_ndue_amount_sum_now'] = np.nan
    dict_out['business_loan_is_vouch_ndue_planRepayAmount_max_12m_now'] = np.nan
    dict_out['consume_loan_serious_overdue_RepayedAmount_sum_24m_now'] = np.nan
    dict_out['consume_loan_serious_overdue_RepayedAmount_max_now'] = np.nan
    dict_out['loan_slight_overdue_planRepayAmount_max_06m_now'] = np.nan
    dict_out['loan_ndue_is_vouch_balance_max_now'] = np.nan
    dict_out['loan_is_vouch_slight_overdue_month_sum_03m_now'] = np.nan
    dict_out['business_loan_slight_overdue_currentOverdueAmount_sum_now'] = np.nan
    dict_out['consume_loan_ndue_is_vouch_account_count_12m'] = np.nan
    dict_out['collect_acct_bal'] = np.nan
    dict_out['business_loan_is_vouch_serious_overdue_currentOverdueAmount_max_now'] = np.nan
    dict_out['loan_is_vouch_serious_overdue_month_sum_12m'] = np.nan
    dict_out['loan_overdue_month_max_open_06m'] = np.nan
    dict_out['query_6m_crtsum'] = np.nan
    dict_out['loan_is_vouch_serious_overdue_month_sum_06m'] = np.nan
    dict_out['noln_ovd_mth'] = np.nan
    dict_out['marital'] = np.nan
    dict_out['loan_is_vouch_serious_overdue_month_sum_03m'] = np.nan
    dict_out['stu_loan_serious_overdue_planRepayAmount_max_now'] = np.nan
    dict_out['consume_loan_amount_sum_now'] = np.nan
    dict_out['consume_loan_is_vouch_serious_overdue_planRepayAmount_max_06m_now'] = np.nan
    dict_out['loan_slight_overdue_RepayedAmount_sum_06m_now'] = np.nan
    dict_out['car_loan_slight_overdue_amount_max_now'] = np.nan
    dict_out['consume_loan_is_vouch_serious_overdue_RepayedAmount_max_03m_now'] = np.nan
    dict_out['business_loan_is_vouch_amount_sum'] = np.nan
    dict_out['business_loan_is_vouch_slight_overdue_RepayedAmount_sum_12m_now'] = np.nan
    dict_out['car_loan_overdue180Amount_max_now'] = np.nan
    dict_out['business_loan_ndue_planRepayAmount_max_03m_now'] = np.nan
    dict_out['business_loan_serious_overdue_month_sum_06m_now'] = np.nan
    dict_out['consume_loan_is_vouch_serious_overdue_currentOverdueAmount_max_now'] = np.nan
    dict_out['loan_is_vouch_is_break_account_count'] = np.nan
    dict_out['loan_serious_overdue_RepayedAmount_sum_now'] = np.nan
    dict_out['loan_slight_overdue_is_vouch_RepayedAmount_max_now'] = np.nan
    dict_out['house_loan_amount_max'] = np.nan
    dict_out['loan_ndue_is_vouch_RepayedAmount_max_now'] = np.nan
    dict_out['loan_recent_slight_overdue_RepayedAmount_sum'] = np.nan
    dict_out['consume_loan_ndue_is_vouch_planRepayAmount_max_now'] = np.nan
    dict_out['loan_ndue_is_vouch_planRepayAmount_sum_now'] = np.nan
    dict_out['loan_overdue_month_max_now'] = np.nan
    dict_out['consume_loan_serious_overdue_planRepayAmount_sum_06m_now'] = np.nan
    dict_out['clnasn_lasn'] = np.nan
    dict_out['consume_loan_is_vouch_slight_overdue_currentOverdueAmount_max_now'] = np.nan
    dict_out['business_loan_is_vouch_serious_overdue_month_sum_12m'] = np.nan
    dict_out['loan_slight_overdue_is_vouch_balance_max_now'] = np.nan
    dict_out['loan_is_vouch_serious_overdue_balance_max_now'] = np.nan
    dict_out['loan_is_vouch_slight_overdue_planRepayAmount_max_03m_now'] = np.nan
    dict_out['house_loan_serious_overdue_amount_sum'] = np.nan
    dict_out['lnasn_lasn'] = np.nan
    dict_out['business_loan_ndue_planRepayAmount_sum_now'] = np.nan
    dict_out['loan_is_vouch_overdue_month_sum_03m'] = np.nan
    dict_out['consume_loan_is_vouch_overdue91Amount_sum_now'] = np.nan
    dict_out['business_loan_is_vouch_slight_overdue_RepayedAmount_max_03m_now'] = np.nan
    dict_out['loan_is_vouch_slight_overdue_account_count_12m_now'] = np.nan
    dict_out['business_loan_slight_overdue_is_vouch_planRepayAmount_sum_now'] = np.nan
    dict_out['consume_loan_slight_overdue_amount_max'] = np.nan
    dict_out['business_loan_serious_overdue_currentOverdueAmount_sum_now'] = np.nan
    dict_out['loan_serious_overdue_month_sum_24m'] = np.nan
    dict_out['loan_serious_overdue_month_sum_now'] = np.nan
    dict_out['loan_serious_overdue_amount_max_now'] = np.nan
    dict_out['loan_overdue_month_max_06m_now'] = np.nan
    dict_out['loan_serious_overdue_month_sum_06m_now'] = np.nan
    dict_out['consume_loan_is_vouch_serious_overdue_planRepayAmount_max_12m_now'] = np.nan
    dict_out['consume_loan_is_vouch_serious_overdue_month_sum_03m_now'] = np.nan
    dict_out['livacn_lacn'] = np.nan
    dict_out['loan_overdue31Amount_max_now'] = np.nan
    dict_out['consume_loan_is_vouch_slight_overdue_RepayedAmount_sum_03m_now'] = np.nan
    dict_out['business_loan_overdue_month_max_06m_now'] = np.nan
    dict_out['consume_loan_is_vouch_serious_overdue_month_sum_06m_now'] = np.nan
    dict_out['consume_loan_slight_overdue_RepayedAmount_sum_24m_now'] = np.nan
    dict_out['loan_ndue_account_count_12m_now'] = np.nan
    dict_out['house_loan_ndue_planRepayAmount_max_now'] = np.nan
    dict_out['business_loan_is_vouch_RepayedAmount_max_now'] = np.nan
    dict_out['consume_loan_is_vouch_overdue_month_sum'] = np.nan
    dict_out['loan_slight_overdue_is_vouch_RepayedAmount_sum_now'] = np.nan
    dict_out['loan_is_vouch_serious_overdue_planRepayAmount_max_03m_now'] = np.nan
    dict_out['business_loan_overdue31Amount_sum_now'] = np.nan
    dict_out['business_loan_slight_overdue_is_vouch_account_count'] = np.nan
    dict_out['consume_loan_is_vouch_serious_overdue_RepayedAmount_sum_06m_now'] = np.nan
    dict_out['business_loan_is_vouch_overdue61Amount_sum_now'] = np.nan
    dict_out['consume_loan_ndue_account_count'] = np.nan
    dict_out['consume_loan_serious_overdue_month_sum_now'] = np.nan
    dict_out['consume_loan_is_vouch_amount_sum_now'] = np.nan
    dict_out['clnivacn_lacn'] = np.nan
    dict_out['business_loan_slight_overdue_month_sum_03m_now'] = np.nan
    dict_out['workaddr_num'] = np.nan
    dict_out['consume_loan_slight_overdue_account_count_12m'] = np.nan
    dict_out['business_loan_serious_overdue_account_count_24m_now'] = np.nan
    dict_out['business_loan_serious_overdue_RepayedAmount_max_12m_now'] = np.nan
    dict_out['business_loan_ndue_account_count_03m_now'] = np.nan
    dict_out['business_loan_is_vouch_slight_overdue_RepayedAmount_max_12m_now'] = np.nan
    dict_out['business_loan_slight_overdue_amount_sum'] = np.nan
    dict_out['loan_is_vouch_loanAmount_max_now'] = np.nan
    dict_out['house_loan_ndue_amount_max'] = np.nan
    dict_out['consume_loan_ndue_planRepayAmount_max_24m_now'] = np.nan
    dict_out['consume_loan_ndue_is_vouch_planRepayAmount_max_03m_now'] = np.nan
    dict_out['consume_loan_slight_overdue_RepayedAmount_man_03m_now'] = np.nan
    dict_out['business_loan_serious_overdue_account_count_06m_now'] = np.nan
    dict_out['loan_ndue_RepayedAmount_max_03m_now'] = np.nan
    dict_out['loan_is_vouch_serious_overdue_planRepayAmount_sum_12m_now'] = np.nan
    dict_out['loan_ndue_is_vouch_planRepayAmount_max_12m_now'] = np.nan
    dict_out['loan_is_vouch_currentOverdueAmount_max_now'] = np.nan
    dict_out['house_loan_overdue_month_sum_now'] = np.nan
    dict_out['business_loan_is_vouch_overdue_month_sum_24m_now'] = np.nan
    dict_out['loan_is_vouch_slight_overdue_RepayedAmount_sum_03m_now'] = np.nan
    dict_out['query_3m_crtsum'] = np.nan
    dict_out['business_loan_slight_overdue_is_vouch_balance_max_now'] = np.nan
    dict_out['loan_overdue_month_max_03m_now'] = np.nan
    dict_out['car_loan_currentOverdueTerms_max_now'] = np.nan
    dict_out['car_loan_slight_overdue_amount_sum_now'] = np.nan
    dict_out['consume_loan_serious_overdue_planRepayAmount_max_now'] = np.nan
    dict_out['consume_loan_serious_overdue_account_count'] = np.nan
    dict_out['loan_RepayedAmount_sum_now'] = np.nan
    dict_out['car_loan_serious_overdue_amount_sum'] = np.nan
    dict_out['stu_loan_ndue_amount_sum_now'] = np.nan
    dict_out['business_loan_overdue31Amount_max_now'] = np.nan
    dict_out['loan_is_vouch_balance_max_now'] = np.nan
    dict_out['consume_loan_is_vouch_currentOverdueAmount_sum_now'] = np.nan
    dict_out['car_loan_overdue91Amount_max_now'] = np.nan
    dict_out['consume_loan_is_vouch_serious_overdue_amount_max_now'] = np.nan
    dict_out['business_loan_is_vouch_serious_overdue_RepayedAmount_max_12m_now'] = np.nan
    dict_out['loan_is_vouch_slight_overdue_account_count_03m'] = np.nan
    dict_out['loan_ndue_RepayedAmount_max_24m_now'] = np.nan
    dict_out['qcrt_ovd_maxamt'] = np.nan
    dict_out['loan_is_vouch_slight_overdue_planRepayAmount_max_06m_now'] = np.nan
    dict_out['loan_overdue_month_max_03m'] = np.nan
    dict_out['loan_serious_overdue_RepayedAmount_max_24m_now'] = np.nan
    dict_out['business_loan_is_vouch_serious_overdue_RepayedAmount_sum_06m_now'] = np.nan
    dict_out['loan_is_vouch_serious_overdue_month_sum_24m'] = np.nan
    dict_out['first_loan_overdue_month_sum'] = np.nan
    dict_out['consume_loan_currentOverdueTerms_sum_now'] = np.nan
    dict_out['car_loan_overdue180Amount_sum_now'] = np.nan
    dict_out['loan_is_vouch_overdue_month_max_now'] = np.nan
    dict_out['consume_loan_is_vouch_slight_overdue_account_count_24m_now'] = np.nan
    dict_out['consume_loan_serious_overdue_planRepayAmount_sum_03m_now'] = np.nan
    dict_out['loan_serious_overdue_planRepayAmount_sum_12m_now'] = np.nan
    dict_out['loan_ndue_is_vouch_RepayedAmount_max_06m_now'] = np.nan
    dict_out['business_loan_is_vouch_currentOverdueTerms_sum_now'] = np.nan
    dict_out['consume_loan_ndue_is_vouch_balance_sum_now'] = np.nan
    dict_out['loan_recent_serious_overdue_RepayedAmount_sum'] = np.nan
    dict_out['business_loan_serious_overdue_month_sum_03m_now'] = np.nan
    dict_out['car_loan_amount_max'] = np.nan
    dict_out['loan_is_vouch_serious_overdue_RepayedAmount_max_now'] = np.nan
    dict_out['blivsoacn_lac'] = np.nan
    dict_out['house_loan_serious_overdue_account_count'] = np.nan
    dict_out['consume_loan_ndue_is_vouch_amount_sum_now'] = np.nan
    dict_out['loan_is_vouch_overdue_month_sum_now'] = np.nan
    dict_out['business_loan_overdue_month_sum_24m'] = np.nan
    dict_out['accfund_his_cnt'] = np.nan
    dict_out['business_loan_is_vouch_slight_overdue_RepayedAmount_max_06m_now'] = np.nan
    dict_out['loan_account_count_now'] = np.nan
    dict_out['business_loan_serious_overdue_planRepayAmount_max_12m_now'] = np.nan
    dict_out['consume_loan_is_vouch_serious_overdue_month_sum_12m_now'] = np.nan
    dict_out['loan_is_vouch_overdue_month_sum_12m'] = np.nan
    dict_out['loan_amount_max_overdue_month_max'] = np.nan
    dict_out['loan_slight_overdue_is_vouch_planRepayAmount_sum_now'] = np.nan
    dict_out['loan_overdue_month_sum_24m'] = np.nan
    dict_out['business_loan_is_vouch_ndue_RepayedAmount_max_03m_now'] = np.nan
    dict_out['stu_loan_amount_max'] = np.nan
    dict_out['business_loan_is_vouch_balance_sum_now'] = np.nan
    dict_out['loan_is_vouch_serious_overdue_aomunt_sum'] = np.nan
    dict_out['stu_loan_is_break_account_count'] = np.nan
    dict_out['business_loan_ndue_planRepayAmount_sum_03m_now'] = np.nan
    dict_out['stu_loan_ndue_RepayedAmount_max_now'] = np.nan
    dict_out['business_loan_is_vouch_currentOverdueTerms_max_now'] = np.nan
    dict_out['business_loan_slight_overdue_RepayedAmount_sum_now'] = np.nan
    dict_out['loan_is_vouch_currentOverdueTerms_sum_now'] = np.nan
    dict_out['lsoas_las'] = np.nan
    dict_out['loan_is_vouch_overdue_month_sum_12m_now'] = np.nan
    dict_out['consume_loan_is_vouch_slight_overdue_planRepayAmount_max_24m_now'] = np.nan
    dict_out['crt_ovd_num'] = np.nan
    dict_out['consume_loan_is_vouch_currentOverdueTerms_max_now'] = np.nan
    dict_out['stu_loan_RepayedAmount_sum_now'] = np.nan
    dict_out['consume_loan_slight_overdue_month_sum_24m_now'] = np.nan
    dict_out['consume_loan_ndue_is_vouch_planRepayAmount_max_12m_now'] = np.nan
    dict_out['business_loan_slight_overdue_RepayedAmount_sum_03m_now'] = np.nan
    dict_out['loan_overdue180Amount_sum_now'] = np.nan
    dict_out['business_loan_serious_overdue_RepayedAmount_sum_24m_now'] = np.nan
    dict_out['loan_is_vouch_serious_overdue_currentOverdueAmount_sum_now'] = np.nan
    dict_out['loan_recent_slight_overdue_planRepayAmount_sum'] = np.nan
    dict_out['business_loan_serious_overdue_RepayedAmount_sum_now'] = np.nan
    dict_out['consume_loan_ndue_is_vouch_RepayedAmount_sum_now'] = np.nan
    dict_out['business_loan_is_vouch_overdue_month_max_12m_now'] = np.nan
    dict_out['car_loan_ndue_balance_sum_now'] = np.nan
    dict_out['consume_loan_is_vouch_slight_overdue_currentOverdueAmount_sum_now'] = np.nan
    dict_out['business_loan_is_vouch_planRepayAmount_sum_now'] = np.nan
    dict_out['consume_loan_is_vouch_serious_overdue_account_count_24m_now'] = np.nan
    dict_out['business_loan_serious_overdue_account_count_03m'] = np.nan
    dict_out['ln_1m_expiresum'] = np.nan
    dict_out['business_loan_slight_overdue_is_vouch_RepayedAmount_sum_now'] = np.nan
    dict_out['first_loan_overdue_month_max'] = np.nan
    dict_out['stu_loan_ndue_planRepayAmount_max_now'] = np.nan
    dict_out['house_loan_serious_overdue_planRepayAmount_max_now'] = np.nan
    dict_out['business_loan_is_vouch_ndue_amount_max'] = np.nan
    dict_out['stu_loan_slight_overdue_currentOverdueAmount_max_now'] = np.nan
    dict_out['blnacn_lacn'] = np.nan
    dict_out['loan_serious_overdue_planRepayAmount_max_03m_now'] = np.nan
    dict_out['business_loan_overdue_month_max_24m_now'] = np.nan
    dict_out['blivnacn_lacn'] = np.nan
    dict_out['consume_loan_slight_overdue_balance_sum_now'] = np.nan
    dict_out['blasn_lasn'] = np.nan
    dict_out['stu_loan_planRepayAmount_max_now'] = np.nan
    dict_out['consume_loan_is_vouch_serious_overdue_planRepayAmount_sum_06m_now'] = np.nan
    dict_out['business_loan_is_vouch_overdue91Amount_max_now'] = np.nan
    dict_out['business_loan_is_vouch_slight_overdue_month_sum_24m'] = np.nan
    dict_out['loan_is_vouch_overdue31Amount_sum_now'] = np.nan
    dict_out['business_loan_overdue_month_sum_06m'] = np.nan
    dict_out['npl_num'] = np.nan
    dict_out['loan_recent_serious_overdue_balance_sum'] = np.nan
    dict_out['query_3m_reasonsum'] = np.nan
    dict_out['loan_is_vouch_overdue_month_sum_06m'] = np.nan
    dict_out['loan_ndue_RepayedAmount_sum_12m_now'] = np.nan
    dict_out['accfund_12m_amount'] = np.nan
    dict_out['business_loan_overdue_month_max'] = np.nan
    dict_out['business_loan_is_vouch_serious_overdue_account_count_03m'] = np.nan
    dict_out['business_loan_slight_overdue_RepayedAmount_man_06m_now'] = np.nan
    dict_out['loan_ndue_is_vouch_RepayedAmount_max_24m_now'] = np.nan
    dict_out['loan_is_vouch_serious_overdue_month_sum_12m_now'] = np.nan
    dict_out['loan_slight_overdue_month_sum_06m'] = np.nan
    dict_out['loan_is_vouch_slight_overdue_planRepayAmount_sum_12m_now'] = np.nan
    dict_out['loan_recent_serious_overdue_account_count'] = np.nan
    dict_out['stu_loan_overdue91Amount_max_now'] = np.nan
    dict_out['emp_character'] = np.nan
    dict_out['business_loan_slight_overdue_month_sum_now'] = np.nan
    dict_out['loan_is_vouch_overdue_month_sum_06m_now'] = np.nan
    dict_out['loan_is_vouch_overdue180Amount_max_now'] = np.nan
    dict_out['consume_loan_slight_overdue_planRepayAmount_max_06m_now'] = np.nan
    dict_out['house_loan_is_break_amount_sum'] = np.nan
    dict_out['business_loan_is_vouch_serious_overdue_planRepayAmount_sum_06m_now'] = np.nan
    dict_out['loan_recent_serious_overdue_planRepayAmount_max'] = np.nan
    dict_out['house_loan_amount_sum_now'] = np.nan
    dict_out['blsobsn_blsoasn'] = np.nan
    dict_out['business_loan_ndue_amount_sum_now'] = np.nan
    dict_out['loan_slight_overdue_month_sum_12m_now'] = np.nan
    dict_out['consume_loan_ndue_planRepayAmount_max_12m_now'] = np.nan
    dict_out['house_loan_slight_overdue_RepayedAmount_max_now'] = np.nan
    dict_out['business_loan_slight_overdue_account_count'] = np.nan
    dict_out['business_loan_is_vouch_slight_overdue_planRepayAmount_sum_03m_now'] = np.nan
    dict_out['loan_ndue_RepayedAmount_sum_03m_now'] = np.nan
    dict_out['loan_is_vouch_balance_sum_now'] = np.nan
    dict_out['consume_loan_serious_overdue_month_sum_12m'] = np.nan
    dict_out['business_loan_is_vouch_serious_overdue_currentOverdueAmount_sum_now'] = np.nan
    dict_out['loan_ndue_is_vouch_account_count_12m'] = np.nan
    dict_out['business_loan_is_vouch_amount_sum_now'] = np.nan
    dict_out['livsoacn_lacn'] = np.nan
    dict_out['clsoacn_lacn'] = np.nan
    dict_out['business_loan_ndue_balance_max_now'] = np.nan
    dict_out['business_loan_is_vouch_RepayedAmount_sum_now'] = np.nan
    dict_out['crt_24m_duecount'] = np.nan
    dict_out['business_loan_overdue_month_sum_12m_now'] = np.nan
    dict_out['consume_loan_is_vouch_slight_overdue_RepayedAmount_sum_now'] = np.nan
    dict_out['loan_ndue_RepayedAmount_sum_now'] = np.nan
    dict_out['loan_serious_overdue_month_sum_03m_now'] = np.nan
    dict_out['loan_ndue_is_vouch_RepayedAmount_sum_24m_now'] = np.nan
    dict_out['consume_loan_is_vouch_serious_overdue_month_sum'] = np.nan
    dict_out['business_loan_amount_sum'] = np.nan
    dict_out['consume_loan_is_vouch_overdue_month_max_12m'] = np.nan
    dict_out['business_loan_is_vouch_serious_overdue_RepayedAmount_max_24m_now'] = np.nan
    dict_out['business_loan_ndue_RepayedAmount_max_06m_now'] = np.nan
    dict_out['loan_recent_serious_overdue_planRepayAmount_sum'] = np.nan
    dict_out['business_loan_slight_overdue_RepayedAmount_sum_24m_now'] = np.nan
    dict_out['stu_loan_is_break_amount_sum'] = np.nan
    dict_out['loan_slight_overdue_RepayedAmount_sum_12m_now'] = np.nan
    dict_out['loan_ndue_is_vouch_planRepayAmount_sum_12m_now'] = np.nan
    dict_out['loan_slight_overdue_RepayedAmount_max_now'] = np.nan
    dict_out['loan_is_vouch_planRepayAmount_sum_now'] = np.nan
    dict_out['consume_loan_ndue_is_vouch_planRepayAmount_sum_12m_now'] = np.nan
    dict_out['blsoacn_lacn'] = np.nan
    dict_out['loan_serious_overdue_RepayedAmount_sum_03m_now'] = np.nan
    dict_out['loan_ndue_is_vouch_account_count_06m_now'] = np.nan
    dict_out['house_loan_overdue180Amount_sum_now'] = np.nan
    dict_out['consume_loan_overdue_month_max_now'] = np.nan
    dict_out['car_loan_rating_worst'] = np.nan
    dict_out['house_loan_serious_overdue_account_count_now'] = np.nan
    dict_out['consume_loan_serious_overdue_balance_sum_now'] = np.nan
    dict_out['business_loan_is_vouch_ndue_RepayedAmount_max_12m_now'] = np.nan
    dict_out['consume_loan_ndue_is_vouch_planRepayAmount_sum_06m_now'] = np.nan
    dict_out['loan_is_break_amount_sum'] = np.nan
    dict_out['loan_serious_overdue_account_count_03m'] = np.nan
    dict_out['loan_slight_overdue_RepayedAmount_sum_24m_now'] = np.nan
    dict_out['business_loan_is_vouch_serious_overdue_planRepayAmount_max_12m_now'] = np.nan
    dict_out['consume_loan_is_vouch_serious_overdue_RepayedAmount_sum_12m_now'] = np.nan
    dict_out['business_loan_slight_overdue_month_sum_24m_now'] = np.nan
    dict_out['consume_loan_is_vouch_balance_max_now'] = np.nan
    dict_out['consume_loan_is_vouch_overdue31Amount_sum_now'] = np.nan
    dict_out['stu_loan_serious_overdue_amount_max_now'] = np.nan
    dict_out['consume_loan_slight_overdue_RepayedAmount_max_now'] = np.nan
    dict_out['consume_loan_is_vouch_slight_overdue_planRepayAmount_sum_12m_now'] = np.nan
    dict_out['loan_serious_overdue_RepayedAmount_sum_06m_now'] = np.nan
    dict_out['consume_loan_RepayedAmount_sum_now'] = np.nan
    dict_out['business_loan_overdue_month_sum_06m_now'] = np.nan
    dict_out['consume_loan_is_vouch_slight_overdue_amount_max_now'] = np.nan
    dict_out['business_loan_serious_overdue_month_sum_03m'] = np.nan
    dict_out['stu_loan_serious_overdue_amount_sum_now'] = np.nan
    dict_out['cln_bal'] = np.nan
    dict_out['consume_loan_account_count_now'] = np.nan
    dict_out['consume_loan_is_vouch_slight_overdue_RepayedAmount_sum_06m_now'] = np.nan
    dict_out['consume_loan_slight_overdue_currentOverdueAmount_sum_now'] = np.nan
    dict_out['clsoas_clas'] = np.nan
    dict_out['business_loan_is_vouch_serious_overdue_planRepayAmount_sum_12m_now'] = np.nan
    dict_out['loan_is_vouch_slight_overdue_planRepayAmount_max_24m_now'] = np.nan
    dict_out['business_loan_slight_overdue_account_count_06m'] = np.nan
    dict_out['business_loan_is_vouch_overdue_month_sum_12m_now'] = np.nan
    dict_out['business_loan_slight_overdue_planRepayAmount_sum_24m_now'] = np.nan
    dict_out['qcrt_org_num'] = np.nan
    dict_out['house_loan_slight_overdue_balance_sum_now'] = np.nan
    dict_out['business_loan_is_vouch_serious_overdue_month_sum_24m_now'] = np.nan
    dict_out['house_loan_currentOverdueAmount_max_now'] = np.nan
    dict_out['loan_slight_overdue_account_count_now'] = np.nan
    dict_out['business_loan_is_vouch_serious_overdue_month_sum_now'] = np.nan
    dict_out['clsoasn_lasn'] = np.nan
    dict_out['ln_24m_duecount'] = np.nan
    dict_out['loan_is_vouch_slight_overdue_RepayedAmount_max_03m_now'] = np.nan
    dict_out['loan_slight_overdue_balance_max_now'] = np.nan
    dict_out['consume_loan_ndue_planRepayAmount_sum_now'] = np.nan
    dict_out['consume_loan_ndue_RepayedAmount_sum_06m_now'] = np.nan
    dict_out['house_loan_is_break_amount_max'] = np.nan
    dict_out['loan_ndue_planRepayAmount_max_12m_now'] = np.nan
    dict_out['lnas_las'] = np.nan
    dict_out['consume_loan_slight_overdue_account_count_03m_now'] = np.nan
    dict_out['loan_is_vouch_serious_overdue_account_count_12m_now'] = np.nan
    dict_out['business_loan_ndue_planRepayAmount_max_now'] = np.nan
    dict_out['house_loan_ndue_balance_max_now'] = np.nan
    dict_out['loan_is_vouch_slight_overdue_month_sum_24m'] = np.nan
    dict_out['business_loan_overdue_month_max_06m'] = np.nan
    dict_out['blnacn_lac'] = np.nan
    dict_out['stu_loan_slight_overdue_account_count_now'] = np.nan
    dict_out['loan_is_vouch_rating_worst'] = np.nan
    dict_out['credit_type_num'] = np.nan
    dict_out['cqln_org_num'] = np.nan
    dict_out['accfund_6m_amount'] = np.nan
    dict_out['consume_loan_ndue_balance_max_now'] = np.nan
    dict_out['business_loan_is_vouch_overdue180Amount_max_now'] = np.nan
    dict_out['business_loan_is_vouch_ndue_amount_sum'] = np.nan
    dict_out['noln_org_num'] = np.nan
    dict_out['business_loan_serious_overdue_account_count_24m'] = np.nan
    dict_out['business_loan_is_vouch_serious_overdue_planRepayAmount_sum_03m_now'] = np.nan
    dict_out['car_loan_slight_overdue_balance_max_now'] = np.nan
    dict_out['loan_slight_overdue_account_count_03m'] = np.nan
    dict_out['consume_loan_is_vouch_serious_overdue_month_sum_24m'] = np.nan
    dict_out['loan_serious_overdue_account_count_12m_now'] = np.nan
    dict_out['loan_overdue_month_sum_06m'] = np.nan
    dict_out['cqln_ovd_maxamt'] = np.nan
    dict_out['loan_serious_overdue_RepayedAmount_max_now'] = np.nan
    dict_out['cln_org_num'] = np.nan
    dict_out['consume_loan_is_vouch_slight_overdue_account_count'] = np.nan
    dict_out['house_loan_RepayedAmount_max_now'] = np.nan
    dict_out['sort_rh'] = np.nan
    dict_out['business_loan_is_vouch_is_break_account_count'] = np.nan
    dict_out['consume_loan_is_break_amount_max'] = np.nan
    dict_out['business_loan_ndue_RepayedAmount_sum_12m_now'] = np.nan
    dict_out['stu_loan_RepayedAmount_max_now'] = np.nan
    dict_out['consume_loan_is_vouch_slight_overdue_RepayedAmount_max_now'] = np.nan
    dict_out['gender'] = np.nan
    dict_out['business_loan_is_break_amount_max'] = np.nan
    dict_out['stu_ownlive'] = np.nan
    dict_out['business_loan_overdue_month_max_24m'] = np.nan
    dict_out['business_loan_ndue_RepayedAmount_sum_03m_now'] = np.nan
    dict_out['consume_loan_is_vouch_is_break_amount_max'] = np.nan
    dict_out['fiveclass_lnflag'] = np.nan
    dict_out['loan_is_vouch_serious_overdue_planRepayAmount_max_24m_now'] = np.nan
    dict_out['stu_loan_overdue180Amount_sum_now'] = np.nan
    dict_out['business_loan_RepayedAmount_max_now'] = np.nan
    dict_out['loan_recent_serious_overdue_RepayedAmount_max'] = np.nan
    dict_out['consume_loan_is_vouch_serious_overdue_RepayedAmount_max_now'] = np.nan
    dict_out['business_loan_slight_overdue_RepayedAmount_man_12m_now'] = np.nan
    dict_out['car_loan_is_break_amount_sum'] = np.nan
    dict_out['loan_account_count'] = np.nan
    dict_out['business_loan_is_vouch_ndue_planRepayAmount_sum_24m_now'] = np.nan
    dict_out['business_loan_is_vouch_slight_overdue_planRepayAmount_sum_12m_now'] = np.nan
    dict_out['maxlimit'] = np.nan
    dict_out['business_loan_overdue_month_sum_24m_now'] = np.nan
    dict_out['stu_loan_overdue31Amount_max_now'] = np.nan
    dict_out['loan_is_vouch_serious_overdue_account_count_now'] = np.nan
    dict_out['loan_slight_overdue_planRepayAmount_max_now'] = np.nan
    dict_out['car_loan_ndue_RepayedAmount_max_now'] = np.nan
    dict_out['loan_slight_overdue_RepayedAmount_max_12m_now'] = np.nan
    dict_out['loan_is_vouch_overdue_month_sum_03m_now'] = np.nan
    dict_out['business_loan_is_vouch_ndue_RepayedAmount_sum_now'] = np.nan
    dict_out['consume_loan_is_vouch_serious_overdue_account_count_06m'] = np.nan
    dict_out['loan_ndue_amount_sum_now'] = np.nan
    dict_out['car_loan_is_break_amount_max'] = np.nan
    dict_out['loan_serious_overdue_amount_max'] = np.nan
    dict_out['car_loan_serious_overdue_balance_sum_now'] = np.nan
    dict_out['consume_loan_ndue_is_vouch_planRepayAmount_sum_now'] = np.nan
    dict_out['cqln_ovd_mth'] = np.nan
    dict_out['business_loan_slight_overdue_is_vouch_RepayedAmount_max_now'] = np.nan
    dict_out['loan_ndue_is_vouch_amount_sum'] = np.nan
    dict_out['car_loan_currentOverdueTerms_sum_now'] = np.nan
    dict_out['loan_slight_overdue_planRepayAmount_max_12m_now'] = np.nan
    dict_out['consume_loan_serious_overdue_RepayedAmount_max_03m_now'] = np.nan
    dict_out['loan_ndue_is_vouch_account_count_now'] = np.nan
    dict_out['car_loan_ndue_amount_sum_now'] = np.nan
    dict_out['loan_current_ndue_planRepayAmount_sum'] = np.nan
    dict_out['consume_loan_ndue_is_vouch_balance_max_now'] = np.nan
    dict_out['loan_is_vouch_account_count_now'] = np.nan
    dict_out['stu_loan_serious_overdue_RepayedAmount_sum_now'] = np.nan
    dict_out['consume_loan_is_vouch_slight_overdue_account_count_06m_now'] = np.nan
    dict_out['loan_is_vouch_slight_overdue_account_count_03m_now'] = np.nan
    dict_out['business_loan_slight_overdue_is_vouch_account_count_now'] = np.nan
    dict_out['cln_ovd_maxamt'] = np.nan
    dict_out['loan_serious_overdue_month_sum'] = np.nan
    dict_out['house_loan_slight_overdue_amount_sum_now'] = np.nan
    dict_out['consume_loan_currentOverdueAmount_max_now'] = np.nan
    dict_out['consume_loan_is_break_amount_sum'] = np.nan
    dict_out['business_loan_overdue_month_max_12m_now'] = np.nan
    dict_out['loan_slight_overdue_amount_sum'] = np.nan
    dict_out['business_loan_is_vouch_overdue91Amount_sum_now'] = np.nan
    dict_out['business_loan_is_vouch_overdue_month_sum_06m'] = np.nan
    dict_out['consume_loan_is_vouch_slight_overdue_account_count_12m_now'] = np.nan
    dict_out['business_loan_currentOverdueAmount_max_now'] = np.nan
    dict_out['blibac_lac'] = np.nan
    dict_out['loan_slight_overdue_planRepayAmount_sum_12m_now'] = np.nan
    dict_out['loan_ndue_account_count_03m'] = np.nan
    dict_out['consume_loan_slight_overdue_month_sum_12m'] = np.nan
    dict_out['stu_loan_serious_overdue_amount_max'] = np.nan
    dict_out['house_loan_slight_overdue_currentOverdueAmount_sum_now'] = np.nan
    dict_out['consume_loan_ndue_is_vouch_planRepayAmount_max_06m_now'] = np.nan
    dict_out['loan_is_vouch_overdue180Amount_sum_now'] = np.nan
    dict_out['consume_loan_ndue_is_vouch_planRepayAmount_sum_24m_now'] = np.nan
    dict_out['loan_overdue91Amount_sum_now'] = np.nan
    dict_out['consume_loan_amount_max'] = np.nan
    dict_out['consume_loan_overdue_month_sum_now'] = np.nan
    dict_out['car_loan_serious_overdue_planRepayAmount_sum_now'] = np.nan
    dict_out['business_loan_is_vouch_slight_overdue_planRepayAmount_max_03m_now'] = np.nan
    dict_out['business_loan_planRepayAmount_sum_now'] = np.nan
    dict_out['business_loan_serious_overdue_balance_max_now'] = np.nan
    dict_out['business_loan_overdue_month_sum_03m_now'] = np.nan
    dict_out['loan_is_vouch_serious_overdue_planRepayAmount_max_now'] = np.nan
    dict_out['loan_slight_overdue_is_vouch_currentOverdueAmount_max_now'] = np.nan
    dict_out['loan_slight_overdue_planRepayAmount_max_24m_now'] = np.nan
    dict_out['stu_loan_serious_overdue_month_sum_now'] = np.nan
    dict_out['loan_is_vouch_overdue61Amount_max_now'] = np.nan
    dict_out['consume_loan_serious_overdue_RepayedAmount_max_24m_now'] = np.nan
    dict_out['consume_loan_is_vouch_is_break_amount_sum'] = np.nan
    dict_out['qcrt_ovdrft_amt'] = np.nan
    dict_out['business_loan_ndue_account_count_12m_now'] = np.nan
    dict_out['recent_loan_overdue_month_max'] = np.nan
    dict_out['business_loan_slight_overdue_is_vouch_planRepayAmount_max_now'] = np.nan
    dict_out['consume_loan_overdue_month_max_03m_now'] = np.nan
    dict_out['car_loan_slight_overdue_month_sum'] = np.nan
    dict_out['business_loan_serious_overdue_RepayedAmount_max_now'] = np.nan
    dict_out['stu_loan_ndue_amount_sum'] = np.nan
    dict_out['loan_is_vouch_serious_overdue_amount_max_now'] = np.nan
    dict_out['business_loan_is_vouch_serious_overdue_planRepayAmount_max_now'] = np.nan
    dict_out['car_loan_slight_overdue_planRepayAmount_sum_now'] = np.nan
    dict_out['business_loan_ndue_RepayedAmount_max_24m_now'] = np.nan
    dict_out['crt_used_amt'] = np.nan
    dict_out['consume_loan_is_vouch_slight_overdue_month_sum_12m'] = np.nan
    dict_out['business_loan_ndue_account_count_12m'] = np.nan
    dict_out['loan_overdue_month_sum'] = np.nan
    dict_out['car_loan_overdue61Amount_max_now'] = np.nan
    dict_out['lnacn_lacn'] = np.nan
    dict_out['loan_ndue_is_vouch_RepayedAmount_max_03m_now'] = np.nan
    dict_out['consume_loan_ndue_amount_sum_now'] = np.nan
    dict_out['business_loan_overdue_month_max_now'] = np.nan
    dict_out['consume_loan_ndue_RepayedAmount_sum_now'] = np.nan
    dict_out['loan_slight_overdue_account_count_24m'] = np.nan
    dict_out['business_loan_is_vouch_overdue31Amount_max_now'] = np.nan
    dict_out['lnivacn_lacn'] = np.nan
    dict_out['business_loan_is_vouch_slight_overdue_account_count_03m'] = np.nan
    dict_out['business_loan_slight_overdue_month_sum_12m_now'] = np.nan
    dict_out['consume_loan_serious_overdue_RepayedAmount_sum_03m_now'] = np.nan
    dict_out['loan_is_vouch_slight_overdue_month_sum_03m'] = np.nan
    dict_out['business_loan_is_vouch_slight_overdue_planRepayAmount_sum_06m_now'] = np.nan
    dict_out['loan_is_vouch_serious_overdue_account_count'] = np.nan
    dict_out['business_loan_is_vouch_serious_overdue_planRepayAmount_max_03m_now'] = np.nan
    dict_out['clasn_lasn'] = np.nan
    dict_out['house_loan_slight_amount_max'] = np.nan
    dict_out['stu_loan_ndue_amount_max_now'] = np.nan
    dict_out['loan_ndue_planRepayAmount_max_24m_now'] = np.nan
    dict_out['loan_amount_max_open_06m'] = np.nan
    dict_out['consume_loan_ndue_balance_sum_now'] = np.nan
    dict_out['business_loan_is_vouch_serious_overdue_planRepayAmount_sum_24m_now'] = np.nan
    dict_out['consume_loan_ndue_RepayedAmount_max_12m_now'] = np.nan

    return dict_out


def get_features(loan_second, debit_card_second, dict_out, dict_in):

    dict_out['id_type'] = dict_in['cc_rh_report']['idType'][0]  # 被查询者证件类型
    dict_out['inq_reason'] = dict_in['cc_rh_report']['queryReason'][0]  # 查询原因代码
    dict_out['gender'] = dict_in['cc_rh_report_customer']['sex'][0]  # 性别
    dict_out['birthday'] = dict_in['cc_rh_report_customer']['birthday'][0]  # 出生日期
    dict_out['diploma'] = get_diploma(dict_in)  # 学历
    dict_out['degree'] = get_degree(dict_in)  # 学位
    dict_out['emp_status'] = get_emp_status(dict_in)  # 就业状况  这里取的是最新数据，要求为当前状况，需要确认
    dict_out['marital'] = dict_in['cc_rh_report_customer']['marry'][0]  # 婚姻状态

    # 居住地址个数  尚未去除相同地址
    dict_out['liveaddr_num'] = len(
        dict_in['cc_rh_report_customer_home'][dict_in['cc_rh_report_customer_home']['address'] != '--'].value_counts())

    # 工作地址个数  尚未去除相同地址
    dict_out['workaddr_num'] = len(dict_in['cc_rh_report_customer_profession'][
                                     dict_in['cc_rh_report_customer_profession'][
                                         'workUnitAddr'] != '--'].value_counts())

    # 账户数合计   计算的是贷记卡账户数量+贷款账户数量，一卡多账户未去重
    dict_out['credit_acct_num'] = float(len(debit_card_second) + len(loan_second))

    if len(loan_second) > 0:
        # 业务类型数量   计算的是贷款账户业务类型数量
        dict_out['credit_type_num'] = float(len(loan_second.drop_duplicates(subset='businessType', keep='first')))

        # 个人住房贷款账户数  此处分类需要确认
        dict_out['houseloan_num'] = float(len(
            loan_second[(loan_second['businessType'] == '个人住房商业贷款') | (loan_second['businessType'] == '个人住房公积金贷款')]))

        # 个人商用房（包括商住两用房）贷款账户数
        dict_out['mhouseloan_num'] = float(len(loan_second[loan_second['businessType'] == '个人商用房（含商住两用）贷款']))
    else:
        dict_out['credit_type_num'] = np.nan
        dict_out['houseloan_num'] = np.nan
        dict_out['mhouseloan_num'] = np.nan

    # 贷记卡账户数
    try:
        dict_out['crt_num'] = float(dict_in['cc_rh_report_summary_credit_tips']['creditCardCount'][0])
    except:
        dict_out['crt_num'] = 0.0

    # 准贷记卡账户数
    try:
        dict_out['qcrt_num'] = float(dict_in['cc_rh_report_summary_credit_tips']['readyCardCount'][0])
    except:
        dict_out['qcrt_num'] = 0.0

    # 其他贷款账户数  businessType字段为“其他贷款”
    try:
        dict_out['otherloan_num'] = float(len(loan_second[loan_second['businessType'] == '其他贷款']))
    except:
        dict_out['otherloan_num'] = np.nan

    if len(dict_in['cc_rh_report_summary_credit_tips']) > 0:
        # 个人住房贷款首笔业务发放月份
        dict_out['houseloan_fstmth'] = dict_in['cc_rh_report_summary_credit_tips']['houseLoanFirstMonth'][0]

        # 个人商用房（包括商住两用房）贷款首笔业务发放月份
        dict_out['mhouseloan_fstmth'] = dict_in['cc_rh_report_summary_credit_tips']['commercialHouseLoanFirstMonth'][0]

        #  其他类型贷款首笔业务发放月份    数据库字段为‘首笔其他类贷款发放月份’
        dict_out['otherloan_fstmth'] = dict_in['cc_rh_report_summary_credit_tips']['otherLoanFirstMonth'][0]

        # 其他首笔业务发放月份    数据库字段‘首笔其他发放月份’
        dict_out['other_fstmth'] = dict_in['cc_rh_report_summary_credit_tips']['otherFirstMonth'][0]
    else:
        dict_out['houseloan_fstmth'] = np.nan
        dict_out['mhouseloan_fstmth'] = np.nan
        dict_out['otherloan_fstmth'] = np.nan
        dict_out['other_fstmth'] = np.nan

    # 贷记卡首笔业务发放月份    这里取开立日期，需要确认是否需要改为第一笔借款日期
    try:
        dict_out['crt_fstmth'] = debit_card_second['cardGrantDate'].min()
    except:
        dict_out['crt_fstmth'] = np.nan

    # 被追偿
    if len(dict_in['cc_rh_report_summary_recovery']) > 0:
        # 被追偿账户数合计
        dict_out['collect_acct_num'] = float(dict_in['cc_rh_report_summary_recovery']['account'].sum())

        # 被追偿余额合计
        dict_out['collect_acct_bal'] = float(dict_in['cc_rh_report_summary_recovery']['balance'].sum())

        # 被追偿业务类型数量
        dict_out['collect_type_num'] = float(len(dict_in['cc_rh_report_summary_recovery']))

        # 被追偿资产处置业务账户数
        try:
            dict_out['disp_num'] = float(dict_in['cc_rh_report_summary_recovery']['account'][
                                           dict_in['cc_rh_report_summary_recovery']['businessType'] == 1][0])
        except:
            dict_out['disp_num'] = 0.0

        # 被追偿资产处置业务余额
        try:
            dict_out['disp_bal'] = float(dict_in['cc_rh_report_summary_recovery']['balance'][
                                           dict_in['cc_rh_report_summary_recovery']['businessType'] == 1][0])
        except:
            dict_out['disp_bal'] = 0.0

        # 被追偿垫款业务账户数
        try:
            dict_out['paidby_num'] = float(dict_in['cc_rh_report_summary_recovery']['account'][
                                             dict_in['cc_rh_report_summary_recovery']['businessType'] == 2][0])
        except:
            dict_out['paidby_num'] = 0.0

        # 被追偿垫款业务余额
        try:
            dict_out['paidby_bal'] = float(dict_in['cc_rh_report_summary_recovery']['balance'][
                                             dict_in['cc_rh_report_summary_recovery']['businessType'] == 2][0])
        except:
            dict_out['paidby_bal'] = 0.0
    else:
        dict_out['collect_acct_num'] = 0.0
        dict_out['collect_acct_bal'] = 0.0
        dict_out['collect_type_num'] = 0.0
        dict_out['disp_num'] = 0.0
        dict_out['disp_bal'] = 0.0
        dict_out['paidby_num'] = 0.0
        dict_out['paidby_bal'] = 0.0

    # 呆账
    if len(dict_in['cc_rh_report_summary_bad_debts']) > 0:
        # 呆账账户数
        dict_out['npl_num'] = float(dict_in['cc_rh_report_summary_bad_debts']['account'][0])

        # 呆账余额
        dict_out['npl_bal'] = float(dict_in['cc_rh_report_summary_bad_debts']['balance'][0])

    else:
        dict_out['npl_num'] = 0.0
        dict_out['npl_bal'] = 0.0

    if len(loan_second) > 0:
        # 逾期（透支）账户类型数量
        dict_out['ovd_type_num'] = float(
            len(loan_second[loan_second['accountStatus'] == '逾期'].drop_duplicates(subset='businessType', keep='first')))

        # 非循环贷账户逾期（透支）账户数
        dict_out['ovd_type_num'] = float(
            len(loan_second[(loan_second['accountStatus'] == '逾期') & (loan_second['class'] == '非循环贷账户')]))

        # 非循环贷账户逾期（透支）月份数   需确认是否为求和
        dict_out['noln_ovd_mth'] = float(
            sum(list(map(sum, loan_second['overdue_month'][loan_second['class'] == '非循环贷账户']))))

        # 非循环贷账户单月最高逾期（透支）总额
        dict_out['noln_ovd_maxamt'] = loan_second['loan_overdue_amount_max'][loan_second['class'] == '非循环贷账户'].sum()

        # 非循环贷账户最长逾期（透支）月数   无数据记为0
        if len(loan_second['overdue_month_max'][loan_second['class'] == '非循环贷账户']) > 0:
            dict_out['noln_ovd_maxmth'] = float(loan_second['overdue_month_max'][loan_second['class'] == '非循环贷账户'].max())
        else:
            dict_out['noln_ovd_maxmth'] = 0.0

        # 循环额度下分账户类逾期（透支）账户数
        dict_out['cqln_ovd_num'] = float(len(loan_second[loan_second['class'] == '循环额度下分账户']))

        # 循环额度下分账户类逾期（透支）月份数   需确认是否为求和
        dict_out['cqln_ovd_mth'] = float(
            sum(list(map(sum, loan_second['overdue_month'][loan_second['class'] == '循环额度下分账户']))))

        # 循环额度下分账户类单月最高逾期（透支）总额
        dict_out['cqln_ovd_maxamt'] = loan_second['loan_overdue_amount_max'][loan_second['class'] == '循环额度下分账户'].sum()

        # 循环额度下分账户类最长逾期（透支）月数   无数据记为0
        if len(loan_second['overdue_month_max'][loan_second['class'] == '循环额度下分账户']) > 0:
            dict_out['cqln_ovd_maxmth'] = float(
                loan_second['overdue_month_max'][loan_second['class'] == '循环额度下分账户'].max())
        else:
            dict_out['cqln_ovd_maxmth'] = 0.0

        # 循环贷账户逾期（透支）账户数
        dict_out['cln_ovd_cnt'] = float(len(loan_second[loan_second['class'] == '循环贷账户']))

        # 循环贷账户逾期（透支）月份数   需确认是否为求和
        dict_out['cln_ovd_mth'] = float(
            sum(list(map(sum, loan_second['overdue_month'][loan_second['class'] == '循环贷账户']))))

        # 循环贷账户单月最高逾期（透支）总额
        dict_out['cln_ovd_maxamt'] = loan_second['loan_overdue_amount_max'][loan_second['class'] == '循环贷账户'].sum()

        # 循环贷账户最长逾期（透支）月数   无数据记为0
        if len(loan_second['overdue_month_max'][loan_second['class'] == '循环贷账户']) > 0:
            dict_out['cln_ovd_maxmth'] = float(loan_second['overdue_month_max'][loan_second['class'] == '循环贷账户'].max())
        else:
            dict_out['cln_ovd_maxmth'] = 0.0
    else:
        dict_out['ovd_type_num'] = "--"
        dict_out['ovd_type_num'] = "--"
        dict_out['noln_ovd_mth'] = "--"
        dict_out['noln_ovd_maxamt'] = "--"
        dict_out['noln_ovd_maxmth'] = "--"
        dict_out['cqln_ovd_num'] = "--"
        dict_out['cqln_ovd_mth'] = "--"
        dict_out['cqln_ovd_maxamt'] = "--"
        dict_out['cqln_ovd_maxmth'] = "--"
        dict_out['cln_ovd_cnt'] = "--"
        dict_out['cln_ovd_mth'] = "--"
        dict_out['cln_ovd_maxamt'] = "--"
        dict_out['cln_ovd_maxmth'] = "--"

    if len(dict_in['cc_rh_report_summary_overdue']) > 0:
        # 贷记卡账户逾期（透支）账户数
        dict_out['crt_ovd_num'] = float(dict_in['cc_rh_report_summary_overdue']['cardOverdueCount'][0])

        # 贷记卡账户逾期（透支）月份数
        dict_out['crt_ovd_mth'] = float(dict_in['cc_rh_report_summary_overdue']['cardOverdueMonthSum'][0])

        # 贷记卡账户单月最高逾期（透支）总额
        dict_out['crt_ovd_maxamt'] = float(dict_in['cc_rh_report_summary_overdue']['cardOverdueMonthMaxAmount'][0])

        # 贷记卡账户最长逾期（透支）月数
        dict_out['crt_ovd_maxmth'] = float(dict_in['cc_rh_report_summary_overdue']['cardOverdueMonthMax'][0])

        # 准贷记卡账户逾期（透支）账户数
        dict_out['qcrt_ovd_num'] = float(dict_in['cc_rh_report_summary_overdue']['semiCardOverdueCount'][0])

        # 准贷记卡账户逾期（透支）月份数
        dict_out['qcrt_ovd_mth'] = float(dict_in['cc_rh_report_summary_overdue']['semiCardOverdueMonthSum'][0])

        # 准贷记卡账户单月最高逾期（透支）总额
        dict_out['qcrt_ovd_maxamt'] = float(dict_in['cc_rh_report_summary_overdue']['semiCardOverdueMonthMaxAmount'][0])

        # 准贷记卡账户最长逾期（透支）月数
        dict_out['qcrt_ovd_maxmth'] = float(dict_in['cc_rh_report_summary_overdue']['semiCardOverdueMonthMax'][0])
    else:
        dict_out['crt_ovd_num'] = np.nan
        dict_out['crt_ovd_mth'] = np.nan
        dict_out['crt_ovd_maxamt'] = np.nan
        dict_out['crt_ovd_maxmth'] = np.nan
        dict_out['qcrt_ovd_num'] = np.nan
        dict_out['qcrt_ovd_mth'] = np.nan
        dict_out['qcrt_ovd_maxamt'] = np.nan
        dict_out['qcrt_ovd_maxmth'] = np.nan

    # 导入 二代征信-授信及负债信息概要-贷款账号信息汇总
    summary_debt_loan = dict_in['cc_rh_report_summary_debt_loan']

    try:
        if len(summary_debt_loan[(summary_debt_loan['creditTotalAmount'] != summary_debt_loan['balance']) & (
                summary_debt_loan['businessType'] == 1)]) > 0:
            # 未结清非循环贷管理机构数
            dict_out['noln_org_num'] = float(summary_debt_loan['orgCount'][(summary_debt_loan['creditTotalAmount'] !=
                                                                          summary_debt_loan['balance']) & (
                                                                                     summary_debt_loan[
                                                                                         'businessType'] == 1)][0])
            # dict_out['noln_org_num'] = float(len(loan_second[(loan_second['accountStatus']!='结清') & (loan_second['class']=='非循环贷账户')].drop_duplicates(subset='loanGrantOrg',keep='first')))

            # 未结清非循环贷账户数
            dict_out['nloc_num'] = float(summary_debt_loan['accountCount'][
                                           (summary_debt_loan['creditTotalAmount'] != summary_debt_loan['balance']) & (
                                                       summary_debt_loan['businessType'] == 1)][0])
            # dict_out['nloc_num'] = float(len(loan_second[(loan_second['accountStatus']!='结清') & (loan_second['class']=='非循环贷账户')]))

            # 未结清非循环贷授信总额
            dict_out['nloc_amt'] = float(summary_debt_loan['creditTotalAmount'][
                                           (summary_debt_loan['creditTotalAmount'] != summary_debt_loan['balance']) & (
                                                       summary_debt_loan['businessType'] == 1)][0])

            # 未结清非循环贷余额
            dict_out['nloc_bal'] = float(summary_debt_loan['balance'][
                                           (summary_debt_loan['creditTotalAmount'] != summary_debt_loan['balance']) & (
                                                       summary_debt_loan['businessType'] == 1)][0])

            # 未结清非循环贷最近6个月平均应还款
            dict_out['nloc_6mpay_amt'] = float(summary_debt_loan['avgRepaymentAmount'][(summary_debt_loan[
                                                                                          'creditTotalAmount'] !=
                                                                                      summary_debt_loan['balance']) & (
                                                                                                 summary_debt_loan[
                                                                                                     'businessType'] == 1)][
                                                 0])

        elif len(loan_second) > 0:
            dict_out['noln_org_num'] = float(len(loan_second[(loan_second['accountStatus'] != '结清') & (
                        loan_second['class'] == '非循环贷账户')].drop_duplicates(subset='loanGrantOrg', keep='first')))
            dict_out['nloc_num'] = float(
                len(loan_second[(loan_second['accountStatus'] != '结清') & (loan_second['class'] == '非循环贷账户')]))
            dict_out['nloc_amt'] = np.nan
            dict_out['nloc_bal'] = np.nan
            dict_out['nloc_6mpay_amt'] = np.nan

    except:
        dict_out['noln_org_num'] = np.nan
        dict_out['nloc_num'] = np.nan
        dict_out['nloc_amt'] = np.nan
        dict_out['nloc_bal'] = np.nan
        dict_out['nloc_6mpay_amt'] = np.nan

    try:
        if len(summary_debt_loan[(summary_debt_loan['creditTotalAmount'] != summary_debt_loan['balance']) & (
                summary_debt_loan['businessType'] == 3)]) > 0:
            # 未结清循环额度下分账户类管理机构数
            dict_out['cqln_org_num'] = float(summary_debt_loan['orgCount'][(summary_debt_loan['creditTotalAmount'] !=
                                                                          summary_debt_loan['balance']) & (
                                                                                     summary_debt_loan[
                                                                                         'businessType'] == 3)][0])
            # dict_out['cqln_org_num'] = float(len(loan_second[(loan_second['accountStatus']!='结清') & (loan_second['class']=='循环额度下分账户')].drop_duplicates(subset='loanGrantOrg',keep='first')))

            # 未结清循环额度下分账户类账户数
            dict_out['cqln_num'] = float(summary_debt_loan['accountCount'][
                                           (summary_debt_loan['creditTotalAmount'] != summary_debt_loan['balance']) & (
                                                       summary_debt_loan['businessType'] == 3)][0])
            # dict_out['cqln_num'] = float(len(loan_second[(loan_second['accountStatus']!='结清') & (loan_second['class']=='循环额度下分账户')]))

            # 未结清循环额度下分账户类授信总额
            dict_out['cqln_amt'] = float(summary_debt_loan['creditTotalAmount'][
                                           (summary_debt_loan['creditTotalAmount'] != summary_debt_loan['balance']) & (
                                                       summary_debt_loan['businessType'] == 3)][0])

            # 未结清循环额度下分账户类余额
            dict_out['cqln_bal'] = float(summary_debt_loan['balance'][
                                           (summary_debt_loan['creditTotalAmount'] != summary_debt_loan['balance']) & (
                                                       summary_debt_loan['businessType'] == 3)][0])

            # 未结清循环额度下分账户类最近6个月平均应还款
            dict_out['cqln_6mpay_amt'] = float(summary_debt_loan['avgRepaymentAmount'][(summary_debt_loan[
                                                                                          'creditTotalAmount'] !=
                                                                                      summary_debt_loan['balance']) & (
                                                                                                 summary_debt_loan[
                                                                                                     'businessType'] == 3)][
                                                 0])

        elif len(loan_second) > 0:
            dict_out['cqln_org_num'] = float(len(loan_second[(loan_second['accountStatus'] != '结清') & (
                        loan_second['class'] == '循环额度下分账户')].drop_duplicates(subset='loanGrantOrg', keep='first')))
            dict_out['cqln_num'] = float(
                len(loan_second[(loan_second['accountStatus'] != '结清') & (loan_second['class'] == '循环额度下分账户')]))
            dict_out['cqln_amt'] = np.nan
            dict_out['cqln_bal'] = np.nan
            dict_out['cqln_6mpay_amt'] = np.nan

    except:
        dict_out['cqln_org_num'] = np.nan
        dict_out['cqln_num'] = np.nan
        dict_out['cqln_amt'] = np.nan
        dict_out['cqln_bal'] = np.nan
        dict_out['cqln_6mpay_amt'] = np.nan

    try:
        if len(summary_debt_loan[(summary_debt_loan['creditTotalAmount'] != summary_debt_loan['balance']) & (
                summary_debt_loan['businessType'] == 2)]) > 0:
            # 未结清循环贷账户管理机构数
            dict_out['cln_org_num'] = float(summary_debt_loan['orgCount'][(summary_debt_loan['creditTotalAmount'] !=
                                                                         summary_debt_loan['balance']) & (
                                                                                    summary_debt_loan[
                                                                                        'businessType'] == 2)][0])
            # dict_out['cqln_org_num'] = float(len(loan_second[(loan_second['accountStatus']!='结清') & (loan_second['class']=='循环贷账户')].drop_duplicates(subset='loanGrantOrg',keep='first')))

            # 未结清循环额度下分账户类账户数
            dict_out['cln_num'] = float(summary_debt_loan['accountCount'][
                                          (summary_debt_loan['creditTotalAmount'] != summary_debt_loan['balance']) & (
                                                      summary_debt_loan['businessType'] == 2)][0])
            # dict_out['cqln_num'] = float(len(loan_second[(loan_second['accountStatus']!='结清') & (loan_second['class']=='循环贷账户')]))

            # 未结清循环额度下分账户类授信总额
            dict_out['cln_amt'] = float(summary_debt_loan['creditTotalAmount'][
                                          (summary_debt_loan['creditTotalAmount'] != summary_debt_loan['balance']) & (
                                                      summary_debt_loan['businessType'] == 2)][0])

            # 未结清循环额度下分账户类余额
            dict_out['cln_bal'] = float(summary_debt_loan['balance'][
                                          (summary_debt_loan['creditTotalAmount'] != summary_debt_loan['balance']) & (
                                                      summary_debt_loan['businessType'] == 2)][0])

            # 未结清循环额度下分账户类最近6个月平均应还款
            dict_out['cln_6mpay_amt'] = float(summary_debt_loan['avgRepaymentAmount'][(summary_debt_loan[
                                                                                         'creditTotalAmount'] !=
                                                                                     summary_debt_loan['balance']) & (
                                                                                                summary_debt_loan[
                                                                                                    'businessType'] == 2)][
                                                0])

        elif len(loan_second) > 0:
            dict_out['cln_org_num'] = float(len(
                loan_second[(loan_second['accountStatus'] != '结清') & (loan_second['class'] == '循环贷账户')].drop_duplicates(
                    subset='loanGrantOrg', keep='first')))
            dict_out['cln_num'] = float(
                len(loan_second[(loan_second['accountStatus'] != '结清') & (loan_second['class'] == '循环贷账户')]))
            dict_out['cln_amt'] = np.nan
            dict_out['cln_bal'] = np.nan
            dict_out['cln_6mpay_amt'] = np.nan

    except:
        dict_out['cln_org_num'] = np.nan
        dict_out['cln_num'] = np.nan
        dict_out['cln_amt'] = np.nan
        dict_out['cln_bal'] = np.nan
        dict_out['cln_6mpay_amt'] = np.nan

    # cc_rh_report_summary_debt_card 表中，统计的是非‘销户’的账户。（与cc_rh_report_detail_debit_card_second 表对比发现当取状态不为‘销户’的记录时，结果可以对得上）
    # cc_rh_report_summary_debt 表 与 cc_rh_report_detail_debit_card_second(debit_card_second) 表没有相同的reportId
    # 由于cc_rh_report_summary_debt 表中有现成的“未销户贷记卡”字段，因此优先从该表中取数，若取不到则从 cc_rh_report_detail_debit_card_second 表中取

    # 未销户贷记卡
    if len(dict_in['cc_rh_report_summary_debt']) > 0:
        # 未销户贷记卡发卡机构数
        dict_out['crt_org_num'] = float(dict_in['cc_rh_report_summary_debt']['uncancelledCardOrgCount'][0])

        # 未销户贷记卡账户数
        dict_out['crt_cnt'] = float(dict_in['cc_rh_report_summary_debt']['uncancelledCardCount'][0])

        # 未销户贷记卡授信总额
        dict_out['crt_amt'] = float(dict_in['cc_rh_report_summary_debt']['uncancelledCardGrantAmount'][0])

        # 未销户贷记卡单家行最高授信额
        dict_out['crt_max_amt'] = float(dict_in['cc_rh_report_summary_debt']['uncancelledCardSingleMaxGrantAmount'][0])

        # 未销户贷记卡单价行最低授信额
        dict_out['crt_min_amt'] = float(dict_in['cc_rh_report_summary_debt']['uncancelledCardSingleMinGrantAmount'][0])

        # 未销户贷记卡已用额度
        dict_out['crt_used_amt'] = float(dict_in['cc_rh_report_summary_debt']['uncancelledCardUsedAmount'][0])

        # 未销户贷记卡最近6个月平均使用额度
        dict_out['crt_6m_avgamt'] = float(dict_in['cc_rh_report_summary_debt']['uncancelledCardAvgUseAmount'][0])

    elif len(debit_card_second) > 0:
        # 未销户贷记卡发卡机构数
        dict_out['crt_org_num'] = float(len(
            debit_card_second[debit_card_second['accountStatus'] != '销户'].drop_duplicates(subset='grantOrg',
                                                                                          keep='first')))

        # 未销户贷记卡账户数
        dict_out['crt_cnt'] = float(len(debit_card_second[debit_card_second['accountStatus'] != '销户']))

        # 未销户贷记卡授信总额
        dict_out['crt_amt'] = float(debit_card_second['shareAmount'][debit_card_second['accountStatus'] != '销户'].sum())

        # 未销户贷记卡单家行最高授信额
        dict_out['crt_max_amt'] = float(debit_card_second['amount'][debit_card_second['accountStatus'] != '销户'].max())

        # 未销户贷记卡单价行最低授信额
        dict_out['crt_min_amt'] = float(debit_card_second['amount'][debit_card_second['accountStatus'] != '销户'].min())

        # 未销户贷记卡已用额度
        dict_out['crt_used_amt'] = float(
            debit_card_second['usedAmount'][debit_card_second['accountStatus'] != '销户'].sum())

        # 未销户贷记卡最近6个月平均使用额度
        dict_out['crt_6m_avgamt'] = float(
            debit_card_second['avgUsedAmount'][debit_card_second['accountStatus'] != '销户'].sum())
    else:
        dict_out['crt_org_num'] = np.nan
        dict_out['crt_cnt'] = np.nan
        dict_out['crt_amt'] = np.nan
        dict_out['crt_max_amt'] = np.nan
        dict_out['crt_min_amt'] = np.nan
        dict_out['crt_used_amt'] = np.nan
        dict_out['crt_6m_avgamt'] = np.nan

    # 由于 cc_rh_report_summary_debt_card 表中贷记卡的记录与“非销户”相吻合，那么同理认为准贷记卡的记录也为“非销户”
    # 未销户准贷记卡
    summary_debt_card = dict_in['cc_rh_report_summary_debt_card']
    if len(summary_debt_card[summary_debt_card['businessType'] == 2]) > 0:
        # 未销户准贷记卡发卡机构数
        dict_out['qcrt_org_num'] = float(
            summary_debt_card['orgCount'][summary_debt_card['businessType'] == 2].reset_index(drop=True)[0])

        # 未销户准贷记卡账户数
        dict_out['uncqcrt_num'] = float(
            summary_debt_card['accountCount'][summary_debt_card['businessType'] == 2].reset_index(drop=True)[0])

        # 未销户准贷记卡授信总额
        dict_out['qcrt_amt'] = float(
            summary_debt_card['accountCount'][summary_debt_card['businessType'] == 2].reset_index(drop=True)[0])

        # 未销户准贷记卡单家行最高授信额
        dict_out['qcrt_max_amt'] = float(
            summary_debt_card['creditMaxAmount'][summary_debt_card['businessType'] == 2].reset_index(drop=True)[0])

        # 未销户准贷记卡单家行最低授信额
        dict_out['qcrt_min_amt'] = float(
            summary_debt_card['creditMinAmount'][summary_debt_card['businessType'] == 2].reset_index(drop=True)[0])

        # 未销户准贷记卡透支余额
        dict_out['qcrt_ovdrft_amt'] = float(
            summary_debt_card['usedAmount'][summary_debt_card['businessType'] == 2].reset_index(drop=True)[0])

        # 未销户准贷记卡最近6个月平均透支余额
        dict_out['qcrt_6m_avgamt'] = float(
            summary_debt_card['avgUsedAmount'][summary_debt_card['businessType'] == 2].reset_index(drop=True)[0])

    else:
        dict_out['qcrt_org_num'] = np.nan
        dict_out['uncqcrt_num'] = np.nan
        dict_out['qcrt_amt'] = np.nan
        dict_out['qcrt_max_amt'] = np.nan
        dict_out['qcrt_min_amt'] = np.nan
        dict_out['qcrt_ovdrft_amt'] = np.nan
        dict_out['qcrt_6m_avgamt'] = np.nan

    # 上一次查询日期
    if dict_in['cc_rh_report']['queryTime'][0] != '':
        dict_out['lst_qry_date'] = dict_in['cc_rh_report']['queryTime'][0]

    elif len(dict_in['cc_rh_report_query_detail']) > 0:
        dict_out['lst_qry_date'] = dict_in['cc_rh_report_query_detail']['queryDate'].max()

    else:
        dict_out['lst_qry_date'] = np.nan

    # 上一次查询原因
    dict_out['lst_qry_reason'] = dict_in['cc_rh_report']['queryReason'][0]

    if len(dict_in['cc_rh_report_query_summary']) > 0:
        # 最近1个月内的查询机构数（贷款审批）
        dict_out['ln_1m_orgnum'] = float(dict_in['cc_rh_report_query_summary']['loanQueryOrgCount'][0])

        # 最近1个月内的查询机构数（信用卡审批）
        dict_out['crt_1m_orgnum'] = float(dict_in['cc_rh_report_query_summary']['cardQueryOrgCount'][0])

        # 最近1个月内的查询次数（贷款审批）
        dict_out['ln_1m_qrynum'] = float(dict_in['cc_rh_report_query_summary']['loanQueryCount'][0])

        # 最近1个月内的查询次数（信用卡审批）
        dict_out['crt_1m_qrynum'] = float(dict_in['cc_rh_report_query_summary']['cardQueryCount'][0])

        # 最近1个月内的查询次数（本人查询）
        dict_out['self_2y_qrynum'] = float(dict_in['cc_rh_report_query_summary']['selfQueryCount'][0])

        # 最近2年内的查询次数（贷后管理）
        dict_out['post_2y_qrynum'] = float(dict_in['cc_rh_report_query_summary']['loanAfterQueryCount'][0])

        # 最近2年内的查询次数（担保资格审查）
        dict_out['grt_2y_qrynum'] = float(dict_in['cc_rh_report_query_summary']['guaranteeQueryCount'][0])

        # 最近2年内的查询次数（特约商户实名审查）
        dict_out['merch_2y_qrynum'] = float(dict_in['cc_rh_report_query_summary']['bussinessRealNameQueryCount'][0])

    else:
        dict_out['ln_1m_orgnum'] = np.nan
        dict_out['crt_1m_orgnum'] = np.nan
        dict_out['ln_1m_qrynum'] = np.nan
        dict_out['crt_1m_qrynum'] = np.nan
        dict_out['self_2y_qrynum'] = np.nan
        dict_out['post_2y_qrynum'] = np.nan
        dict_out['grt_2y_qrynum'] = np.nan
        dict_out['merch_2y_qrynum'] = np.nan

    # 转换时间
    reportTime = datetime.datetime.strptime(dict_out['reportTime'], '%Y.%m.%d %H:%M:%S')

    # 机构查询和    1.若无查询记录，结果展示为 0 还是 --  2.机构是否需要去重
    if len(dict_in['cc_rh_report_query_detail']) > 0:
        # 近1个月机构查询和
        query_time = reportTime - relativedelta(months=1)
        query_time = datetime.datetime.strftime(query_time, '%Y.%m.%d %H:%M:%S')
        dict_out['query_1m_reasonsum'] = float(len(dict_in['cc_rh_report_query_detail'][
                                                     dict_in['cc_rh_report_query_detail']['queryDate'] >= query_time[
                                                                                                          :10]].drop_duplicates(
            subset='queryOperator', keep='first')))

        # 近3个月机构查询和
        query_time = reportTime - relativedelta(months=3)
        query_time = datetime.datetime.strftime(query_time, '%Y.%m.%d %H:%M:%S')
        dict_out['query_3m_reasonsum'] = float(len(dict_in['cc_rh_report_query_detail'][
                                                     dict_in['cc_rh_report_query_detail']['queryDate'] >= query_time[
                                                                                                          :10]].drop_duplicates(
            subset='queryOperator', keep='first')))

        # 近6个月机构查询和
        query_time = reportTime - relativedelta(months=6)
        query_time = datetime.datetime.strftime(query_time, '%Y.%m.%d %H:%M:%S')
        dict_out['query_6m_reasonsum'] = float(len(dict_in['cc_rh_report_query_detail'][
                                                     dict_in['cc_rh_report_query_detail']['queryDate'] >= query_time[
                                                                                                          :10]].drop_duplicates(
            subset='queryOperator', keep='first')))

        # 近12个月机构查询和
        query_time = reportTime - relativedelta(months=12)
        query_time = datetime.datetime.strftime(query_time, '%Y.%m.%d %H:%M:%S')
        dict_out['query_12m_reasonsum'] = float(len(dict_in['cc_rh_report_query_detail'][
                                                      dict_in['cc_rh_report_query_detail']['queryDate'] >= query_time[
                                                                                                           :10]].drop_duplicates(
            subset='queryOperator', keep='first')))

        # 近24个月机构查询和
        query_time = reportTime - relativedelta(months=24)
        query_time = datetime.datetime.strftime(query_time, '%Y.%m.%d %H:%M:%S')
        dict_out['query_24m_reasonsum'] = float(len(dict_in['cc_rh_report_query_detail'][
                                                      dict_in['cc_rh_report_query_detail']['queryDate'] >= query_time[
                                                                                                           :10]].drop_duplicates(
            subset='queryOperator', keep='first')))

    else:
        dict_out['query_1m_reasonsum'] = np.nan
        dict_out['query_3m_reasonsum'] = np.nan
        dict_out['query_6m_reasonsum'] = np.nan
        dict_out['query_12m_reasonsum'] = np.nan
        dict_out['query_24m_reasonsum'] = np.nan

    # 贷款查询和
    if len(dict_in['cc_rh_report_query_detail']['queryReason'] == '贷款审批') > 0:
        # 近1个月贷款查询和
        time = datetime.datetime.strptime(dict_out['reportTime'], '%Y.%m.%d %H:%M:%S')
        query_time = time - relativedelta(months=1)
        query_time = datetime.datetime.strftime(query_time, '%Y.%m.%d %H:%M:%S')
        dict_out['query_1m_lnsum'] = float(len(dict_in['cc_rh_report_query_detail'][
                                                 (dict_in['cc_rh_report_query_detail']['queryReason'] == '贷款审批') & (
                                                             dict_in['cc_rh_report_query_detail'][
                                                                 'queryDate'] >= query_time[:10])]))

        # 近3个月贷款查询和
        time = datetime.datetime.strptime(dict_out['reportTime'], '%Y.%m.%d %H:%M:%S')
        query_time = time - relativedelta(months=3)
        query_time = datetime.datetime.strftime(query_time, '%Y.%m.%d %H:%M:%S')
        dict_out['query_3m_lnsum'] = float(len(dict_in['cc_rh_report_query_detail'][
                                                 (dict_in['cc_rh_report_query_detail']['queryReason'] == '贷款审批') & (
                                                             dict_in['cc_rh_report_query_detail'][
                                                                 'queryDate'] >= query_time[:10])]))

        # 近6个月贷款查询和
        time = datetime.datetime.strptime(dict_out['reportTime'], '%Y.%m.%d %H:%M:%S')
        query_time = time - relativedelta(months=6)
        query_time = datetime.datetime.strftime(query_time, '%Y.%m.%d %H:%M:%S')
        dict_out['query_6m_lnsum'] = float(len(dict_in['cc_rh_report_query_detail'][
                                                 (dict_in['cc_rh_report_query_detail']['queryReason'] == '贷款审批') & (
                                                             dict_in['cc_rh_report_query_detail'][
                                                                 'queryDate'] >= query_time[:10])]))

        # 近12个月贷款查询和
        time = datetime.datetime.strptime(dict_out['reportTime'], '%Y.%m.%d %H:%M:%S')
        query_time = time - relativedelta(months=12)
        query_time = datetime.datetime.strftime(query_time, '%Y.%m.%d %H:%M:%S')
        dict_out['query_12m_lnsum'] = float(len(dict_in['cc_rh_report_query_detail'][
                                                  (dict_in['cc_rh_report_query_detail']['queryReason'] == '贷款审批') & (
                                                              dict_in['cc_rh_report_query_detail'][
                                                                  'queryDate'] >= query_time[:10])]))

        # 近24个月贷款查询和
        time = datetime.datetime.strptime(dict_out['reportTime'], '%Y.%m.%d %H:%M:%S')
        query_time = time - relativedelta(months=24)
        query_time = datetime.datetime.strftime(query_time, '%Y.%m.%d %H:%M:%S')
        dict_out['query_24m_lnsum'] = float(len(dict_in['cc_rh_report_query_detail'][
                                                  (dict_in['cc_rh_report_query_detail']['queryReason'] == '贷款审批') & (
                                                              dict_in['cc_rh_report_query_detail'][
                                                                  'queryDate'] >= query_time[:10])]))

    else:
        dict_out['query_1m_lnsum'] = np.nan
        dict_out['query_3m_lnsum'] = np.nan
        dict_out['query_6m_lnsum'] = np.nan
        dict_out['query_12m_lnsum'] = np.nan
        dict_out['query_24m_lnsum'] = np.nan

    # 贷记卡查询和
    if len(dict_in['cc_rh_report_query_detail']['queryReason'] == '信用卡审批') > 0:
        # 近1个月贷记卡查询和
        time = datetime.datetime.strptime(dict_out['reportTime'], '%Y.%m.%d %H:%M:%S')
        query_time = time - relativedelta(months=1)
        query_time = datetime.datetime.strftime(query_time, '%Y.%m.%d %H:%M:%S')
        dict_out['query_1m_crtsum'] = float(len(dict_in['cc_rh_report_query_detail'][
                                                  (dict_in['cc_rh_report_query_detail']['queryReason'] == '信用卡审批') & (
                                                              dict_in['cc_rh_report_query_detail'][
                                                                  'queryDate'] >= query_time[:10])]))

        # 近3个月贷记卡查询和
        time = datetime.datetime.strptime(dict_out['reportTime'], '%Y.%m.%d %H:%M:%S')
        query_time = time - relativedelta(months=3)
        query_time = datetime.datetime.strftime(query_time, '%Y.%m.%d %H:%M:%S')
        dict_out['query_3m_crtsum'] = float(len(dict_in['cc_rh_report_query_detail'][
                                                  (dict_in['cc_rh_report_query_detail']['queryReason'] == '信用卡审批') & (
                                                              dict_in['cc_rh_report_query_detail'][
                                                                  'queryDate'] >= query_time[:10])]))

        # 近6个月贷记卡查询和
        time = datetime.datetime.strptime(dict_out['reportTime'], '%Y.%m.%d %H:%M:%S')
        query_time = time - relativedelta(months=6)
        query_time = datetime.datetime.strftime(query_time, '%Y.%m.%d %H:%M:%S')
        dict_out['query_6m_crtsum'] = float(len(dict_in['cc_rh_report_query_detail'][
                                                  (dict_in['cc_rh_report_query_detail']['queryReason'] == '信用卡审批') & (
                                                              dict_in['cc_rh_report_query_detail'][
                                                                  'queryDate'] >= query_time[:10])]))

        # 近12个月贷记卡查询和
        time = datetime.datetime.strptime(dict_out['reportTime'], '%Y.%m.%d %H:%M:%S')
        query_time = time - relativedelta(months=12)
        query_time = datetime.datetime.strftime(query_time, '%Y.%m.%d %H:%M:%S')
        dict_out['query_12m_crtsum'] = float(len(dict_in['cc_rh_report_query_detail'][
                                                   (dict_in['cc_rh_report_query_detail']['queryReason'] == '信用卡审批') & (
                                                               dict_in['cc_rh_report_query_detail'][
                                                                   'queryDate'] >= query_time[:10])]))

        # 近24个月贷记卡查询和
        time = datetime.datetime.strptime(dict_out['reportTime'], '%Y.%m.%d %H:%M:%S')
        query_time = time - relativedelta(months=24)
        query_time = datetime.datetime.strftime(query_time, '%Y.%m.%d %H:%M:%S')
        dict_out['query_24m_crtsum'] = float(len(dict_in['cc_rh_report_query_detail'][
                                                   (dict_in['cc_rh_report_query_detail']['queryReason'] == '信用卡审批') & (
                                                               dict_in['cc_rh_report_query_detail'][
                                                                   'queryDate'] >= query_time[:10])]))

    else:
        dict_out['query_1m_crtsum'] = np.nan
        dict_out['query_3m_crtsum'] = np.nan
        dict_out['query_6m_crtsum'] = np.nan
        dict_out['query_12m_crtsum'] = np.nan
        dict_out['query_24m_crtsum'] = np.nan

    # 近12期贷款最大逾期数
    try:
        dict_out['ln_12m_duemax'] = float(max(list(map(max, loan_second['overdue_month_12']))))
    except:
        dict_out['ln_12m_duemax'] = np.nan

    # 近12期贷记卡最大逾期数
    try:
        dict_out['crt_12m_duemax'] = float(max(list(map(max, debit_card_second['overdue_month_12']))))
    except:
        dict_out['crt_12m_duemax'] = np.nan

    # 近12期贷款逾期总期数
    try:
        dict_out['ln_12m_duesum'] = float(sum(list(map(sum, loan_second['overdue_month_12']))))
    except:
        dict_out['ln_12m_duesum'] = np.nan

    # 近12期贷记卡逾期总期数
    try:
        dict_out['crt_12m_duesum'] = float(sum(list(map(sum, debit_card_second['overdue_month_12']))))
    except:
        dict_out['crt_12m_duesum'] = np.nan

    # 近12期贷款逾期期次
    try:
        dict_out['ln_12m_duecount'] = duecount(loan_second['overdue_month_12'])
    except:
        dict_out['ln_12m_duecount'] = np.nan

    # 近12期贷记卡逾期期次
    try:
        dict_out['crt_12m_duecount'] = duecount(debit_card_second['overdue_month_12'])
    except:
        dict_out['crt_12m_duecount'] = np.nan

    # 近24期贷款最大逾期数
    try:
        dict_out['ln_24m_duemax'] = float(max(list(map(max, loan_second['overdue_month_24']))))
    except:
        dict_out['ln_24m_duemax'] = np.nan

    # 近24期贷记卡最大逾期数
    try:
        dict_out['crt_24m_duemax'] = float(max(list(map(max, debit_card_second['overdue_month_24']))))
    except:
        dict_out['crt_24m_duemax'] = np.nan

    # 近24期贷款逾期总期数
    try:
        dict_out['ln_24m_duesum'] = float(sum(list(map(sum, loan_second['overdue_month_24']))))
    except:
        dict_out['ln_24m_duesum'] = np.nan

    # 近24期贷记卡逾期总期数
    try:
        dict_out['crt_24m_duesum'] = float(sum(list(map(sum, debit_card_second['overdue_month_24']))))
    except:
        dict_out['crt_24m_duesum'] = np.nan

    # 近24期贷款逾期期次
    try:
        dict_out['ln_24m_duecount'] = duecount(loan_second['overdue_month_24'])
    except:
        dict_out['ln_24m_duecount'] = np.nan

    # 近24期贷记卡逾期期次
    try:
        dict_out['crt_24m_duecount'] = duecount(debit_card_second['overdue_month_24'])
    except:
        dict_out['crt_24m_duecount'] = np.nan

    # 贷款五级分类
    try:
        dict_out['fiveclass_lnflag'] = float(len(loan_second[loan_second['classify5_num'] > 1.0]))
    except:
        dict_out['fiveclass_lnflag'] = np.nan

    # 最近1个月内到期贷款余额
    try:
        time = datetime.datetime.strptime(dict_out['reportTime'], '%Y.%m.%d %H:%M:%S')
        tmp_time = time + relativedelta(months=1)
        tmp_time = datetime.datetime.strftime(tmp_time, '%Y.%m.%d %H:%M:%S')
        dict_out['ln_1m_expiresum'] = float(loan_second['balance'][loan_second['endDate'] <= tmp_time[:10]].sum())
    except:
        dict_out['ln_1m_expiresum'] = np.nan

    # 最近3个月内到期贷款余额
    try:
        time = datetime.datetime.strptime(dict_out['reportTime'], '%Y.%m.%d %H:%M:%S')
        tmp_time = time + relativedelta(months=3)
        tmp_time = datetime.datetime.strftime(tmp_time, '%Y.%m.%d %H:%M:%S')
        dict_out['ln_3m_expiresum'] = float(loan_second['balance'][loan_second['endDate'] <= tmp_time[:10]].sum())
    except:
        dict_out['ln_3m_expiresum'] = np.nan

    # 客户工作单位性质
    try:
        dict_out['emp_character'] = \
        {'机关、事业单位': 10, '国有企业': 20, '外资企业': 30, '个体、私营企业': 40, '其他（包括三资企业、民营企业、民间团体等）': 50, '--': 99, '': 99}[
            dict_in['cc_rh_report_customer_profession']['workUnitType'][
                dict_in['cc_rh_report_customer_profession']['updateTime'] ==
                dict_in['cc_rh_report_customer_profession']['updateTime'].max()][0]]
    except:
        dict_out['emp_character'] = 99

    # 客户申贷时年龄
    if (dict_in['cc_rh_report_customer']['birthday'][0] != '') & (dict_in['cc_rh_report_customer']['birthday'][0] != '--'):
        birthday = datetime.datetime.strptime(dict_in['cc_rh_report_customer']['birthday'][0], '%Y.%m.%d')
        reportTime = datetime.datetime.strptime(dict_out['reportTime'][:10], '%Y.%m.%d')
        dict_out['loan_age'] = (reportTime - birthday).days // 365
    else:
        dict_out['loan_age'] = np.nan

    # 客户历史缴纳公积金数
    dict_out['accfund_his_cnt'] = float(len(dict_in['cc_rh_report_public_housefund']))

    # 客户最早贷款或贷记卡使用时间与本次查询日期间隔天数
    if (len(loan_second) > 0) & (len(debit_card_second) > 0):
        first_time = datetime.datetime.strptime(
            min([loan_second['startDate'].min(), debit_card_second['cardGrantDate'].min()]), '%Y.%m.%d')
        reportTime = datetime.datetime.strptime(dict_out['reportTime'][:10], '%Y.%m.%d')
        dict_out['minuseday'] = float((reportTime - first_time).days)
    else:
        dict_out['minuseday'] = np.nan

    # 客户房贷总额
    try:
        dict_out['houseloan_amount_sum'] = float(
            loan_second['loanAmount'][loan_second['businessType'] == '个人住房商业贷款'].sum())
    except:
        dict_out['houseloan_amount_sum'] = np.nan

    # 客户房贷余额
    try:
        dict_out['houseloan_bal_sum'] = float(loan_second['balance'][loan_second['businessType'] == '个人住房商业贷款'].sum())
    except:
        dict_out['houseloan_bal_sum'] = np.nan

    # 客户房贷总笔数
    try:
        dict_out['houseloan_ever_cnt'] = float(len(loan_second[loan_second['businessType'] == '个人住房商业贷款']))
    except:
        dict_out['houseloan_ever_cnt'] = np.nan

    # 客户公积金近X个月缴纳总额
    try:
        if (len(dict_in['cc_rh_report_public_housefund']) > 0) & (
                dict_in['cc_rh_report_public_housefund']['updateDate'][0] != '信息更新日期') & (
                dict_in['cc_rh_report_public_housefund']['payFirstDate'][0] not in ['000000', '--']):
            # last_updateDate = datetime.datetime.strptime(dict_in['cc_rh_report_public_housefund']['updateDate'][0],'%Y.%m')
            reportTime = datetime.datetime.strptime(dict_out['reportTime'][:7], '%Y.%m')
            reportTime6 = reportTime - relativedelta(months=6)
            reportTime12 = reportTime - relativedelta(months=12)
            reportTime24 = reportTime - relativedelta(months=24)
            payFirstDate = datetime.datetime.strptime(dict_in['cc_rh_report_public_housefund']['payFirstDate'][0],
                                                      '%Y.%m')
            if dict_in['cc_rh_report_public_housefund']['payEndDate'][0] == '000000':
                # 客户公积金近24个月缴纳总额
                if payFirstDate <= reportTime24:
                    dict_out['accfund_24m_amount'] = 24.0 * dict_in['cc_rh_report_public_housefund']['payMonthAmount'][0]
                else:
                    dict_out['accfund_24m_amount'] = ((reportTime - payFirstDate).days // 30.0 + 1) * \
                                                   dict_in['cc_rh_report_public_housefund']['payMonthAmount'][0]

                # 客户公积金近12个月缴纳总额
                if payFirstDate <= reportTime12:
                    dict_out['accfund_12m_amount'] = 12.0 * dict_in['cc_rh_report_public_housefund']['payMonthAmount'][0]
                else:
                    dict_out['accfund_12m_amount'] = ((reportTime - payFirstDate).days // 30.0 + 1) * \
                                                   dict_in['cc_rh_report_public_housefund']['payMonthAmount'][0]

                # 客户公积金近6个月缴纳总额
                if payFirstDate <= reportTime6:
                    dict_out['accfund_6m_amount'] = 6.0 * dict_in['cc_rh_report_public_housefund']['payMonthAmount'][0]
                else:
                    dict_out['accfund_6m_amount'] = ((reportTime - payFirstDate).days // 30.0 + 1) * \
                                                  dict_in['cc_rh_report_public_housefund']['payMonthAmount'][0]
            else:
                payEndDate = datetime.datetime.strptime(dict_in['cc_rh_report_public_housefund']['payEndDate'][0],
                                                        '%Y.%m')
                # 客户公积金近24个月缴纳总额
                if reportTime24 < payFirstDate:
                    dict_out['accfund_24m_amount'] = ((payEndDate - payFirstDate).days // 30.0 + 1) * \
                                                   dict_in['cc_rh_report_public_housefund']['payMonthAmount'][0]
                elif payFirstDate <= reportTime24 <= payEndDate:
                    dict_out['accfund_24m_amount'] = ((payEndDate - reportTime24).days // 30.0 + 1) * \
                                                   dict_in['cc_rh_report_public_housefund']['payMonthAmount'][0]
                elif payEndDate < reportTime24:
                    dict_out['accfund_24m_amount'] = 0.0
                else:
                    dict_out['accfund_24m_amount'] = np.nan

                # 客户公积金近12个月缴纳总额
                if reportTime12 < payFirstDate:
                    dict_out['accfund_12m_amount'] = ((payEndDate - payFirstDate).days // 30.0 + 1) * \
                                                   dict_in['cc_rh_report_public_housefund']['payMonthAmount'][0]
                elif payFirstDate <= reportTime12 <= payEndDate:
                    dict_out['accfund_12m_amount'] = ((payEndDate - reportTime12).days // 30.0 + 1) * \
                                                   dict_in['cc_rh_report_public_housefund']['payMonthAmount'][0]
                elif payEndDate < reportTime12:
                    dict_out['accfund_12m_amount'] = 0.0
                else:
                    dict_out['accfund_12m_amount'] = np.nan

                # 客户公积金近6个月缴纳总额
                if reportTime6 < payFirstDate:
                    dict_out['accfund_6m_amount'] = ((payEndDate - payFirstDate).days // 30.0 + 1) * \
                                                  dict_in['cc_rh_report_public_housefund']['payMonthAmount'][0]
                elif payFirstDate <= reportTime6 <= payEndDate:
                    dict_out['accfund_6m_amount'] = ((payEndDate - reportTime6).days // 30.0 + 1) * \
                                                  dict_in['cc_rh_report_public_housefund']['payMonthAmount'][0]
                elif payEndDate < reportTime6:
                    dict_out['accfund_6m_amount'] = 0.0
                else:
                    dict_out['accfund_6m_amount'] = np.nan
        else:
            dict_out['accfund_24m_amount'] = np.nan
            dict_out['accfund_12m_amount'] = np.nan
            dict_out['accfund_6m_amount'] = np.nan
    except:
        dict_out['accfund_24m_amount'] = np.nan
        dict_out['accfund_12m_amount'] = np.nan
        dict_out['accfund_6m_amount'] = np.nan

    # 客户信用卡近六个月额度使用率
    try:
        dict_out['crt_6mavg_userate'] = dict_out['crt_6m_avgamt'] / dict_out['crt_amt']
    except:
        dict_out['crt_6mavg_userate'] = np.nan

    # 已结清住房贷款笔数
    try:
        dict_out['settlehouselon_cnt'] = float(
            len(loan_second[(loan_second['businessType'] == '个人住房商业贷款') & (loan_second['accountStatus'] == '结清')]))
    except:
        dict_out['settlehouselon_cnt'] = np.nan

    # 近36个月居住地个数
    reportTime = datetime.datetime.strptime(dict_out['reportTime'][:10], '%Y.%m.%d')
    reportTime36 = reportTime - relativedelta(months=36)
    reportTime36 = datetime.datetime.strftime(reportTime36, '%Y.%m.%d')
    try:
        dict_out['liv_36m_cnt'] = float(len(dict_in['cc_rh_report_customer_home'][
                                          (dict_in['cc_rh_report_customer_home']['updateTime'] >= reportTime36) & (
                                                      dict_in['cc_rh_report_customer_home']['updateTime'] <= dict_out[
                                                  'reportTime'])]))
    except:
        dict_out['liv_36m_cnt'] = np.nan

    # 近6个月手机号个数
    reportTime = datetime.datetime.strptime(dict_out['reportTime'][:10], '%Y.%m.%d')
    reportTime6 = reportTime - relativedelta(months=6)
    reportTime6 = datetime.datetime.strftime(reportTime6, '%Y.%m.%d')

    try:
        dict_out['phone_6m_cnt'] = float(len(dict_in['cc_rh_report_customer_mobile'][
                                           (dict_in['cc_rh_report_customer_mobile']['updateDate'] >= reportTime6) & (
                                                       dict_in['cc_rh_report_customer_mobile']['updateDate'] <=
                                                       dict_out['reportTime'])]))
    except:
        dict_out['phone_6m_cnt'] = np.nan

    # 贷记卡单家行最高授信
    try:
        dict_out['maxlimit'] = debit_card_second['shareAmount'].max()
    except:
        dict_out['maxlimit'] = np.nan

    # 居住类型
    try:
        dict_out['stu_ownlive'] = dict_in['cc_rh_report_customer_home']['addressState'][
            dict_in['cc_rh_report_customer_home']['updateTime'] == dict_in['cc_rh_report_customer_home'][
                'updateTime'].max()][0]
    except:
        dict_out['stu_ownlive'] = np.nan
    # 短人行客群
    try:
        dict_out['sort_rh'] = 1 if dict_out['minuseday'] <= 60 else 0
    except:
        dict_out['sort_rh'] = np.nan

    if len(loan_second) > 0:
        dict_out['loan_account_count']=len(loan_second)  # 贷款账户计数
        dict_out['loan_overdue_month_sum']=sum(list(map(sum, loan_second['overdue_month'])))#贷款逾期月数求和
        dict_out['loan_amount_sum']=loan_second['loanAmount'].sum()#贷款本金求和
        dict_out['loan_overdue_month_max']=loan_second['overdue_month_max'].max()#贷款逾期持续月数最大
        dict_out['loan_amount_max']=loan_second['loanAmount'].max()#贷款本金最大
        dict_out['loan_rating_worst']={'1.0':'损失', '2.0':'可疑', '3.0':'关注', '4.0':'次级', '5.0':'正常','nan':"无"}[str(loan_second['classify5_num'].min())]#贷款Rating最差 1:损失；2:可疑；3:关注；4:次级；5:正常
        dict_out['loan_account_CD']=len(Counter(loan_second['class']))#贷款类型CD
        dict_out['loan_GrantOrg_CD']=len(Counter(loan_second['loanGrantOrg']))#贷款机构CD

        #现行
        dict_out['loan_account_count_now']=len(loan_second[loan_second['is_now']==1])#现行贷款账户计数
        dict_out['loan_overdue_month_sum_now']=sum(list(map(sum,loan_second['overdue_month'][loan_second['is_now']==1])))#现行贷款逾期月数求和
        dict_out['loan_amount_sum_now']=sum(list(map(sum,loan_second['overdue_month'][loan_second['is_now']==1])))#现行贷款本金求和
        dict_out['loan_currentOverdueTerms_sum_now']=loan_second['currentOverdueTerms'][loan_second['is_now']==1].sum()#现行贷款当前逾期期数求和
        dict_out['loan_balance_sum_now']=loan_second['balance'][loan_second['is_now']==1].sum()#现行贷款本金余额求和
        dict_out['loan_planRepayAmount_sum_now']=loan_second['planRepayAmount'][loan_second['is_now']==1].sum()#现行贷款本月应还款求和
        dict_out['loan_RepayedAmount_sum_now']=loan_second['RepayedAmount'][loan_second['is_now']==1].sum()#现行贷款本月实还款求和
        dict_out['loan_currentOverdueAmount_sum_now']=loan_second['currentOverdueAmount'][loan_second['is_now']==1].sum()#现行贷款当前逾期金额求和
        dict_out['loan_overdue31Amount_sum_now']=loan_second['overdue31Amount'][loan_second['is_now']==1].sum()#现行贷款逾期31-60天未还本金求和
        dict_out['loan_overdue61Amount_sum_now']=loan_second['overdue61Amount'][loan_second['is_now']==1].sum()#现行贷款逾期61-90天未还本金求和
        dict_out['loan_overdue91Amount_sum_now']=loan_second['overdue91Amount'][loan_second['is_now']==1].sum()#现行贷款逾期91-180天未还本金求和
        dict_out['loan_overdue180Amount_sum_now']=loan_second['overdue180Amount'][loan_second['is_now']==1].sum()#现行贷款逾期180天以上未还本金求和
        dict_out['loan_overdue_month_max_now']=loan_second['overdue_month_max'][loan_second['is_now']==1].max()#现行贷款逾期持续月数最大
        dict_out['loan_loanAmount_max_now']=loan_second['loanAmount'][loan_second['is_now']==1].max()#现行贷款本金最大
        dict_out['loan_currentOverdueTerms_max_now']=loan_second['currentOverdueTerms'][loan_second['is_now']==1].max()#现行贷款当前逾期期数最大
        dict_out['loan_balance_max_now']=loan_second['balance'][loan_second['is_now']==1].max()#现行贷款本金余额最大
        dict_out['loan_planRepayAmount_max_now']=loan_second['planRepayAmount'][loan_second['is_now']==1].max()#现行贷款本月应还款最大
        dict_out['loan_RepayedAmount_max_now']=loan_second['RepayedAmount'][loan_second['is_now']==1].max()#现行贷款本月实还款最大
        dict_out['loan_currentOverdueAmount_max_now']=loan_second['currentOverdueAmount'][loan_second['is_now']==1].max()#现行贷款当前逾期金额最大
        dict_out['loan_overdue31Amount_max_now']=loan_second['overdue31Amount'][loan_second['is_now']==1].max()#现行贷款逾期31-60天未还本金最大
        dict_out['loan_overdue61Amount_max_now']=loan_second['overdue61Amount'][loan_second['is_now']==1].max()#现行贷款逾期61-90天未还本金最大
        dict_out['loan_overdue91Amount_max_now']=loan_second['overdue91Amount'][loan_second['is_now']==1].max()#现行贷款逾期91-180天未还本金最大
        dict_out['loan_overdue180Amount_max_now']=loan_second['overdue180Amount'][loan_second['is_now']==1].max()#现行贷款逾期180天以上未还本金最大
        dict_out['loan_account_CD_now']=len(Counter(loan_second['class'][loan_second['is_now']==1]))#现行贷款类型CD
        dict_out['loan_GrantOrg_CD_now']=len(Counter(loan_second['loanGrantOrg'][loan_second['is_now']==1]))#现行贷款机构CD

        #有担保
        dict_out['loan_is_vouch_count']=len(loan_second[loan_second['is_vouch']==1])#有担保贷款账户计数
        dict_out['loan_is_vouch_overdue_month_sum']=sum(list(map(sum,loan_second['overdue_month'][loan_second['is_vouch']==1])))#有担保贷款逾期月数求和
        dict_out['loan_is_vouch_loanAmount_sum']=loan_second['loanAmount'][loan_second['is_vouch']==1].sum()#有担保贷款本金求和
        dict_out['loan_is_vouch_overdue_month_max']=loan_second['overdue_month_max'][loan_second['is_vouch']==1].max()#有担保贷款逾期持续月数最大
        dict_out['loan_is_vouch_loanAmount_max']=loan_second['loanAmount'][loan_second['is_vouch']==1].max()#有担保贷款本金最大
        dict_out['loan_is_vouch_rating_worst']={'1.0':'损失', '2.0':'可疑', '3.0':'关注', '4.0':'次级', '5.0':'正常','nan':"无"}[str(loan_second['classify5_num'][loan_second['is_vouch']==1].min())]#有担保贷款Rating最差

        #现行有担保
        dict_out['loan_is_vouch_account_count_now']=loan_second['loanGrantOrg'][(loan_second['is_now']==1) & (loan_second['is_vouch']==1)].count()#现行有担保贷款账户计数
        dict_out['loan_is_vouch_overdue_month_sum_now']=sum(list(map(sum,loan_second['overdue_month'][(loan_second['is_now']==1) & (loan_second['is_vouch']==1)])))#现行有担保贷款逾期月数求和
        dict_out['loan_is_vouch_loanAmount_sum_now']=loan_second['loanAmount'][(loan_second['is_now']==1) & (loan_second['is_vouch']==1)].sum()#现行有担保贷款本金求和
        dict_out['loan_is_vouch_currentOverdueTerms_sum_now']=loan_second['currentOverdueTerms'][(loan_second['is_now']==1) & (loan_second['is_vouch']==1)].sum()#现行有担保贷款当前逾期期数求和
        dict_out['loan_is_vouch_balance_sum_now']=loan_second['balance'][(loan_second['is_now']==1) & (loan_second['is_vouch']==1)].sum()#现行有担保贷款本金余额求和
        dict_out['loan_is_vouch_planRepayAmount_sum_now']=loan_second['planRepayAmount'][(loan_second['is_now']==1) & (loan_second['is_vouch']==1)].sum()#现行有担保贷款本月应还款求和
        dict_out['loan_is_vouch_RepayedAmount_sum_now']=loan_second['RepayedAmount'][(loan_second['is_now']==1) & (loan_second['is_vouch']==1)].sum()#现行有担保贷款本月实还款求和
        dict_out['loan_is_vouch_currentOverdueAmount_sum_now']=loan_second['currentOverdueAmount'][(loan_second['is_now']==1) & (loan_second['is_vouch']==1)].sum()#现行有担保贷款当前逾期金额求和
        dict_out['loan_is_vouch_overdue31Amount_sum_now']=loan_second['overdue31Amount'][(loan_second['is_now']==1) & (loan_second['is_vouch']==1)].sum()#现行有担保贷款逾期31-60天未还本金求和
        dict_out['loan_is_vouch_overdue61Amount_sum_now']=loan_second['overdue61Amount'][(loan_second['is_now']==1) & (loan_second['is_vouch']==1)].sum()#现行有担保贷款逾期61-90天未还本金求和
        dict_out['loan_is_vouch_overdue91Amount_sum_now']=loan_second['overdue91Amount'][(loan_second['is_now']==1) & (loan_second['is_vouch']==1)].sum()#现行有担保贷款逾期91-180天未还本金求和
        dict_out['loan_is_vouch_overdue180Amount_sum_now']=loan_second['overdue180Amount'][(loan_second['is_now']==1) & (loan_second['is_vouch']==1)].sum()#现行有担保贷款逾期180天以上未还本金求和
        dict_out['loan_is_vouch_overdue_month_max_now']=loan_second['overdue_month_max'][(loan_second['is_now']==1) & (loan_second['is_vouch']==1)].max()#现行有担保贷款逾期持续月数最大
        dict_out['loan_is_vouch_loanAmount_max_now']=loan_second['loanAmount'][(loan_second['is_now']==1) & (loan_second['is_vouch']==1)].max()#现行有担保贷款本金最大
        dict_out['loan_is_vouch_currentOverdueTerms_max_now']=loan_second['currentOverdueTerms'][(loan_second['is_now']==1) & (loan_second['is_vouch']==1)].max()#现行有担保贷款当前逾期期数最大
        dict_out['loan_is_vouch_balance_max_now']=loan_second['balance'][(loan_second['is_now']==1) & (loan_second['is_vouch']==1)].max()#现行有担保贷款本金余额最大
        dict_out['loan_is_vouch_planRepayAmount_max_now']=loan_second['planRepayAmount'][(loan_second['is_now']==1) & (loan_second['is_vouch']==1)].max()#现行有担保贷款本月应还款最大
        dict_out['loan_is_vouch_RepayedAmount_max_now']=loan_second['RepayedAmount'][(loan_second['is_now']==1) & (loan_second['is_vouch']==1)].max()#现行有担保贷款本月实还款最大
        dict_out['loan_is_vouch_currentOverdueAmount_max_now']=loan_second['currentOverdueAmount'][(loan_second['is_now']==1) & (loan_second['is_vouch']==1)].max()#现行有担保贷款当前逾期金额最大
        dict_out['loan_is_vouch_overdue31Amount_max_now']=loan_second['overdue31Amount'][(loan_second['is_now']==1) & (loan_second['is_vouch']==1)].max()#现行有担保贷款逾期31-60天未还本金最大
        dict_out['loan_is_vouch_overdue61Amount_max_now']=loan_second['overdue61Amount'][(loan_second['is_now']==1) & (loan_second['is_vouch']==1)].max()#现行有担保贷款逾期61-90天未还本金最大
        dict_out['loan_is_vouch_overdue91Amount_max_now']=loan_second['overdue91Amount'][(loan_second['is_now']==1) & (loan_second['is_vouch']==1)].max()#现行有担保贷款逾期91-180天未还本金最大
        dict_out['loan_is_vouch_overdue180Amount_max_now']=loan_second['overdue180Amount'][(loan_second['is_now']==1) & (loan_second['is_vouch']==1)].max()#现行有担保贷款逾期180天以上未还本金最大

        #最近3个月
        dict_out['loan_overdue_month_sum_03m']=sum(list(map(sum,loan_second['overdue_month'][(loan_second['month_Desc_report']<3)])))#最近3个月贷款逾期月数求和
        dict_out['loan_overdue_month_max_03m']=loan_second['overdue_month_max'][(loan_second['month_Desc_report']<3)].max()#最近3个月贷款逾期持续月数最大

        #最近3个月开户
        dict_out['loan_account_count_open_03m']=loan_second['overdue_month'][loan_second['month_startDate_report']<3].count()#最近3个月开户贷款账户计数
        dict_out['loan_overdue_month_sum_open_03m']=sum(list(map(sum,loan_second['overdue_month'][(loan_second['month_Desc_report']<3)])))#最近3个月开户贷款逾期月数求和
        dict_out['loan_amount_sum_open_03m']=loan_second['loanAmount'][loan_second['month_startDate_report']<3].sum()#最近3个月开户贷款本金求和
        dict_out['loan_overdue_month_max_open_03m']=loan_second['overdue_month_max'][(loan_second['month_Desc_report']<3)].max()#最近3个月开户贷款逾期持续月数最大
        dict_out['loan_amount_max_open_03m']=loan_second['loanAmount'][(loan_second['month_startDate_report']<3)].max()#最近3个月开户贷款本金最大

        #现行最近3个月
        dict_out['loan_overdue_month_sum_03m_now']=sum(list(map(sum,loan_second['overdue_month'][(loan_second['month_Desc_report']<3) & (loan_second['is_now']==1)])))#现行最近3个月贷款逾期月数求和
        dict_out['loan_overdue_month_max_03m_now']=loan_second['overdue_month_max'][(loan_second['month_Desc_report']<3) & (loan_second['is_now']==1)].max()#现行最近3个月贷款逾期持续月数最大

        #最近3个月有担保
        dict_out['loan_is_vouch_overdue_month_sum_03m']=sum(list(map(sum,loan_second['overdue_month'][(loan_second['month_Desc_report']<3) & (loan_second['is_vouch']==1)])))#最近3个月有担保贷款逾期月数求和
        dict_out['loan_is_vouch_overdue_month_max_03m']=loan_second['overdue_month_max'][(loan_second['month_Desc_report']<3) & (loan_second['is_vouch']==1)].max()#最近3个月有担保贷款逾期持续月数最大

        #现行最近3个月有担保
        dict_out['loan_is_vouch_overdue_month_sum_03m_now']=sum(list(map(sum,loan_second['overdue_month'][(loan_second['month_Desc_report']<3) & (loan_second['is_vouch']==1) & (loan_second['is_now']==1)])))#现行最近3个月有担保贷款逾期月数求和
        dict_out['loan_is_vouch_overdue_month_max_03m_now']=loan_second['overdue_month_max'][(loan_second['month_Desc_report']<3) & (loan_second['is_vouch']==1) & (loan_second['is_now']==1)].max()#现行最近3个月有担保贷款逾期持续月数最大

         #最近6个月
        dict_out['loan_overdue_month_sum_06m']=sum(list(map(sum,loan_second['overdue_month'][(loan_second['month_Desc_report']<6)])))#最近6个月贷款逾期月数求和
        dict_out['loan_overdue_month_max_06m']=loan_second['overdue_month_max'][(loan_second['month_Desc_report']<6)].max()#最近6个月贷款逾期持续月数最大

        #最近6个月开户
        dict_out['loan_account_count_open_06m']=loan_second['overdue_month'][loan_second['month_startDate_report']<6].count()#最近6个月开户贷款账户计数
        dict_out['loan_overdue_month_sum_open_06m']=sum(list(map(sum,loan_second['overdue_month'][(loan_second['month_Desc_report']<6)])))#最近6个月开户贷款逾期月数求和
        dict_out['loan_amount_sum_open_06m']=loan_second['loanAmount'][loan_second['month_startDate_report']<6].sum()#最近6个月开户贷款本金求和
        dict_out['loan_overdue_month_max_open_06m']=loan_second['overdue_month_max'][(loan_second['month_Desc_report']<6)].max()#最近6个月开户贷款逾期持续月数最大
        dict_out['loan_amount_max_open_06m']=loan_second['loanAmount'][(loan_second['month_startDate_report']<6)].max()

        #现行最近6个月
        dict_out['loan_overdue_month_sum_06m_now']=sum(list(map(sum,loan_second['overdue_month'][(loan_second['month_Desc_report']<6) & (loan_second['is_now']==1)])))#现行最近6个月贷款逾期月数求和
        dict_out['loan_overdue_month_max_06m_now']=loan_second['overdue_month_max'][(loan_second['month_Desc_report']<6) & (loan_second['is_now']==1)].max()#现行最近6个月贷款逾期持续月数最大

        #最近6个月有担保
        dict_out['loan_is_vouch_overdue_month_sum_06m']=sum(list(map(sum,loan_second['overdue_month'][(loan_second['month_Desc_report']<6) & (loan_second['is_vouch']==1)])))#最近6个月有担保贷款逾期月数求和
        dict_out['loan_is_vouch_overdue_month_max_06m']=loan_second['overdue_month_max'][(loan_second['month_Desc_report']<6) & (loan_second['is_vouch']==1)].max()#最近6个月有担保贷款逾期持续月数最大

        #现行最近6个月有担保
        dict_out['loan_is_vouch_overdue_month_sum_06m_now']=sum(list(map(sum,loan_second['overdue_month'][(loan_second['month_Desc_report']<6) & (loan_second['is_vouch']==1) & (loan_second['is_now']==1)])))#现行最近6个月有担保贷款逾期月数求和
        dict_out['loan_is_vouch_overdue_month_max_06m_now']=loan_second['overdue_month_max'][(loan_second['month_Desc_report']<6) & (loan_second['is_vouch']==1) & (loan_second['is_now']==1)].max()#现行最近6个月有担保贷款逾期持续月数最大

        #最近12个月
        dict_out['loan_overdue_month_sum_12m']=sum(list(map(sum,loan_second['overdue_month'][(loan_second['month_Desc_report']<12)])))#最近12个月贷款逾期月数求和
        dict_out['loan_overdue_month_max_12m']=loan_second['overdue_month_max'][(loan_second['month_Desc_report']<12)].max()#最近12个月贷款逾期持续月数最大

        #现行最近12个月
        dict_out['loan_overdue_month_sum_12m_now']=sum(list(map(sum,loan_second['overdue_month'][(loan_second['month_Desc_report']<12) & (loan_second['is_now']==1)])))#现行最近12个月贷款逾期月数求和
        dict_out['loan_overdue_month_max_12m_now']=loan_second['overdue_month_max'][(loan_second['month_Desc_report']<12) & (loan_second['is_now']==1)].max()#现行最近12个月贷款逾期持续月数最大

        #最近12个月有担保
        dict_out['loan_is_vouch_overdue_month_sum_12m']=sum(list(map(sum,loan_second['overdue_month'][(loan_second['month_Desc_report']<12) & (loan_second['is_vouch']==1)])))#最近12个月有担保贷款逾期月数求和
        dict_out['loan_is_vouch_overdue_month_max_12m']=loan_second['overdue_month_max'][(loan_second['month_Desc_report']<12) & (loan_second['is_vouch']==1)].max()#最近12个月有担保贷款逾期持续月数最大

        #现行最近12个月有担保
        dict_out['loan_is_vouch_overdue_month_sum_12m_now']=sum(list(map(sum,loan_second['overdue_month'][(loan_second['month_Desc_report']<12) & (loan_second['is_vouch']==1) & (loan_second['is_now']==1)])))#现行最近12个月有担保贷款逾期月数求和
        dict_out['loan_is_vouch_overdue_month_max_12m_now']=loan_second['overdue_month_max'][(loan_second['month_Desc_report']<12) & (loan_second['is_vouch']==1) & (loan_second['is_now']==1)].max()#现行最近12个月有担保贷款逾期持续月数最大

        #最近24个月
        dict_out['loan_overdue_month_sum_24m']=sum(list(map(sum,loan_second['overdue_month'][(loan_second['month_Desc_report']<24)])))#最近24个月贷款逾期月数求和
        dict_out['loan_overdue_month_max_24m']=loan_second['overdue_month_max'][(loan_second['month_Desc_report']<24)].max()#最近24个月贷款逾期持续月数最大

        #现行最近24个月
        dict_out['loan_overdue_month_sum_24m_now']=sum(list(map(sum,loan_second['overdue_month'][(loan_second['month_Desc_report']<24) & (loan_second['is_now']==1)])))#现行最近24个月贷款逾期月数求和
        dict_out['loan_overdue_month_max_24m_now']=loan_second['overdue_month_max'][(loan_second['month_Desc_report']<24) & (loan_second['is_now']==1)].max()#现行最近24个月贷款逾期持续月数最大

        #最近24个月有担保
        dict_out['loan_is_vouch_overdue_month_sum_24m']=sum(list(map(sum,loan_second['overdue_month'][(loan_second['month_Desc_report']<24) & (loan_second['is_vouch']==1)])))#最近24个月有担保贷款逾期月数求和
        dict_out['loan_is_vouch_overdue_month_max_24m']=loan_second['overdue_month_max'][(loan_second['month_Desc_report']<24) & (loan_second['is_vouch']==1)].max()#最近24个月有担保贷款逾期持续月数最大

        #现行最近24个月有担保
        dict_out['loan_is_vouch_overdue_month_sum_24m_now']=sum(list(map(sum,loan_second['overdue_month'][(loan_second['month_Desc_report']<24) & (loan_second['is_vouch']==1) & (loan_second['is_now']==1)])))#现行最近24个月有担保贷款逾期月数求和
        dict_out['loan_is_vouch_overdue_month_max_24m_now']=loan_second['overdue_month_max'][(loan_second['month_Desc_report']<24) & (loan_second['is_vouch']==1) & (loan_second['is_now']==1)].max()#现行最近24个月有担保贷款逾期持续月数最大

        #无逾期(ndue)
        dict_out['loan_ndue_account_count']=len(loan_second[loan_second['overdue_month_sum']==0])#无逾期贷款账户计数
        dict_out['loan_ndue_amount_sum']=loan_second['loanAmount'][loan_second['overdue_month_sum']==0].sum()#无逾期贷款本金求和
        dict_out['loan_ndue_amount_max']=loan_second['loanAmount'][loan_second['overdue_month_sum']==0].max()#无逾期贷款本金最大

        #现行无逾期
        dict_out['loan_ndue_account_count_now']=len(loan_second[(loan_second['overdue_month']==0) & (loan_second['is_now']==1)])#现行无逾期贷款账户计数
        dict_out['loan_ndue_amount_sum_now']=loan_second['loanAmount'][(loan_second['overdue_month']==0) & (loan_second['is_now']==1)].sum()#现行无逾期贷款本金求和
        dict_out['loan_ndue_balance_sum_now']=loan_second['balance'][(loan_second['overdue_month']==0) & (loan_second['is_now']==1)].sum()#现行无逾期贷款本金余额求和
        dict_out['loan_ndue_planRepayAmount_sum_now']=loan_second['planRepayAmount'][(loan_second['overdue_month']==0) & (loan_second['is_now']==1)].sum()#现行无逾期贷款本月应还款求和
        dict_out['loan_ndue_RepayedAmount_sum_now']=loan_second['RepayedAmount'][(loan_second['overdue_month']==0) & (loan_second['is_now']==1)].sum()#现行无逾期贷款本月实还款求和
        dict_out['loan_ndue_amount_max_now']=loan_second['loanAmount'][(loan_second['overdue_month']==0) & (loan_second['is_now']==1)].max()#现行无逾期贷款本金最大
        dict_out['loan_ndue_balance_max_now']=loan_second['balance'][(loan_second['overdue_month']==0) & (loan_second['is_now']==1)].max()#现行无逾期贷款本金余额最大
        dict_out['loan_ndue_planRepayAmount_max_now']=loan_second['planRepayAmount'][(loan_second['overdue_month']==0) & (loan_second['is_now']==1)].max()#现行无逾期贷款本月应还款最大
        dict_out['loan_ndue_RepayedAmount_max_now']=loan_second['RepayedAmount'][(loan_second['overdue_month']==0) & (loan_second['is_now']==1)].max()#现行无逾期贷款本月实还款最大

        #无逾期有担保
        dict_out['loan_ndue_is_vouch_account_count']=len(loan_second[(loan_second['overdue_month']==0) & (loan_second['is_vouch']==1)])#无逾期有担保贷款账户计数
        dict_out['loan_ndue_is_vouch_amount_sum']=loan_second['loanAmount'][(loan_second['overdue_month']==0) & (loan_second['is_vouch']==1)].sum()#无逾期有担保贷款本金求和
        dict_out['loan_ndue_is_vouch_amount_max']=loan_second['loanAmount'][(loan_second['overdue_month']==0) & (loan_second['is_vouch']==1)].max()#无逾期有担保贷款本金最大

        #现行无逾期有担保
        dict_out['loan_ndue_is_vouch_account_count_now']=len(loan_second[(loan_second['overdue_month']==0) & (loan_second['is_now']==1) & (loan_second['is_vouch']==1)])#现行无逾期有担保贷款账户计数
        dict_out['loan_ndue_is_vouch_amount_sum_now']=loan_second['loanAmount'][(loan_second['overdue_month']==0) & (loan_second['is_now']==1) & (loan_second['is_vouch']==1)].sum()#现行无逾期有担保贷款本金求和
        dict_out['loan_ndue_is_vouch_balance_sum_now']=loan_second['balance'][(loan_second['overdue_month']==0) & (loan_second['is_now']==1) & (loan_second['is_vouch']==1)].sum()#现行无逾期有担保贷款本金余额求和
        dict_out['loan_ndue_is_vouch_planRepayAmount_sum_now']=loan_second['planRepayAmount'][(loan_second['overdue_month']==0) & (loan_second['is_now']==1) & (loan_second['is_vouch']==1)].sum()#现行无逾期有担保贷款本月应还款求和
        dict_out['loan_ndue_is_vouch_RepayedAmount_sum_now']=loan_second['RepayedAmount'][(loan_second['overdue_month']==0) & (loan_second['is_now']==1) & (loan_second['is_vouch']==1)].sum()#现行无逾期有担保贷款本月实还款求和
        dict_out['loan_ndue_is_vouch_amount_max_now']=loan_second['loanAmount'][(loan_second['overdue_month']==0) & (loan_second['is_now']==1) & (loan_second['is_vouch']==1)].max()#现行无逾期有担保贷款本金最大
        dict_out['loan_ndue_is_vouch_balance_max_now']=loan_second['balance'][(loan_second['overdue_month']==0) & (loan_second['is_now']==1) & (loan_second['is_vouch']==1)].max()#现行无逾期有担保贷款本金余额最大
        dict_out['loan_ndue_is_vouch_planRepayAmount_max_now']=loan_second['planRepayAmount'][(loan_second['overdue_month']==0) & (loan_second['is_now']==1) & (loan_second['is_vouch']==1)].max()#现行无逾期有担保贷款本月应还款最大
        dict_out['loan_ndue_is_vouch_RepayedAmount_max_now']=loan_second['RepayedAmount'][(loan_second['overdue_month']==0) & (loan_second['is_now']==1) & (loan_second['is_vouch']==1)].max()#现行无逾期有担保贷款本月实还款最大

        #当前（accountStatus）无逾期
        dict_out['loan_current_ndue_account_count']=len(loan_second[loan_second['accountStatus']!='逾期'])#当前无逾期贷款账户计数
        dict_out['loan_current_ndue_balance_sum']=loan_second['balance'][loan_second['accountStatus']!='逾期'].sum()#当前无逾期贷款本金余额求和
        dict_out['loan_current_ndue_planRepayAmount_sum']=loan_second['planRepayAmount'][loan_second['accountStatus']!='逾期'].sum()#当前无逾期贷款本月应还款求和
        dict_out['loan_current_ndue_RepayedAmount_sum']=loan_second['RepayedAmount'][loan_second['accountStatus']!='逾期'].sum()#当前无逾期贷款本月实还款求和
        dict_out['loan_current_ndue_balance_max']=loan_second['balance'][loan_second['accountStatus']!='逾期'].max()#当前无逾期贷款本金余额最大
        dict_out['loan_current_ndue_planRepayAmount_max']=loan_second['planRepayAmount'][loan_second['accountStatus']!='逾期'].max()#当前无逾期贷款本月应还款最大
        dict_out['loan_current_ndue_RepayedAmount_max']=loan_second['RepayedAmount'][loan_second['accountStatus']!='逾期'].max()#当前无逾期贷款本月实还款最大

        #最近3个月无逾期
        dict_out['loan_ndue_account_count_03m']=len(loan_second[(loan_second['accountStatus']!='逾期') & (loan_second['month_Desc_report']<3)])#最近3个月无逾期贷款账户计数

        #现行最近3个月无逾期
        dict_out['loan_ndue_account_count_03m_now']=len(loan_second[(loan_second['accountStatus']!='逾期') & (loan_second['month_Desc_report']<3) & (loan_second['is_now']==1)])#现行最近3个月无逾期贷款账户计数
        dict_out['loan_ndue_planRepayAmount_sum_03m_now']=loan_second['planRepayAmount'][(loan_second['accountStatus']!='逾期') & (loan_second['month_Desc_report']<3) & (loan_second['is_now']==1)].sum()#现行最近3个月无逾期贷款本月应还款求和
        dict_out['loan_ndue_RepayedAmount_sum_03m_now']=loan_second['RepayedAmount'][(loan_second['accountStatus']!='逾期') & (loan_second['month_Desc_report']<3) & (loan_second['is_now']==1)].sum()#现行最近3个月无逾期贷款本月实还款求和
        dict_out['loan_ndue_planRepayAmount_max_03m_now']=loan_second['planRepayAmount'][(loan_second['accountStatus']!='逾期') & (loan_second['month_Desc_report']<3) & (loan_second['is_now']==1)].max()#现行最近3个月无逾期贷款本月应还款最大
        dict_out['loan_ndue_RepayedAmount_max_03m_now']=loan_second['RepayedAmount'][(loan_second['accountStatus']!='逾期') & (loan_second['month_Desc_report']<3) & (loan_second['is_now']==1)].max()#现行最近3个月无逾期贷款本月实还款最大

        #最近3个月无逾期有担保
        dict_out['loan_ndue_is_vouch_account_count_03m']=len(loan_second[(loan_second['accountStatus']!='逾期') & (loan_second['month_Desc_report']<3) & (loan_second['is_vouch']==1)])#最近3个月无逾期有担保贷款账户计数

        #现行最近3个月无逾期有担保
        dict_out['loan_ndue_is_vouch_account_count_03m_now']=len(loan_second[(loan_second['accountStatus']!='逾期') & (loan_second['month_Desc_report']<3) & (loan_second['is_vouch']==1) & (loan_second['is_now']==1)])#现行最近3个月无逾期有担保贷款账户计数
        dict_out['loan_ndue_is_vouch_planRepayAmount_sum_03m_now']=loan_second['planRepayAmount'][(loan_second['accountStatus']!='逾期') & (loan_second['month_Desc_report']<3) & (loan_second['is_vouch']==1) & (loan_second['is_now']==1)].sum()#现行最近3个月无逾期有担保贷款本月应还款求和
        dict_out['loan_ndue_is_vouch_RepayedAmount_sum_03m_now']=loan_second['RepayedAmount'][(loan_second['accountStatus']!='逾期') & (loan_second['month_Desc_report']<3) & (loan_second['is_vouch']==1) & (loan_second['is_now']==1)].sum()#现行最近3个月无逾期有担保贷款本月实还款求和
        dict_out['loan_ndue_is_vouch_planRepayAmount_max_03m_now']=loan_second['planRepayAmount'][(loan_second['accountStatus']!='逾期') & (loan_second['month_Desc_report']<3) & (loan_second['is_vouch']==1) & (loan_second['is_now']==1)].max()#现行最近3个月无逾期有担保贷款本月应还款最大
        dict_out['loan_ndue_is_vouch_RepayedAmount_max_03m_now']=loan_second['RepayedAmount'][(loan_second['accountStatus']!='逾期') & (loan_second['month_Desc_report']<3) & (loan_second['is_vouch']==1) & (loan_second['is_now']==1)].max()#现行最近3个月无逾期有担保贷款本月实还款最大

        #最近6个月无逾期
        dict_out['loan_ndue_account_count_06m']=len(loan_second[(loan_second['accountStatus']!='逾期') & (loan_second['month_Desc_report']<6)])#最近6个月无逾期贷款账户计数

        #现行最近6个月无逾期
        dict_out['loan_ndue_account_count_06m_now']=len(loan_second[(loan_second['accountStatus']!='逾期') & (loan_second['month_Desc_report']<6) & (loan_second['is_now']==1)])#现行最近6个月无逾期贷款账户计数
        dict_out['loan_ndue_planRepayAmount_sum_06m_now']=loan_second['planRepayAmount'][(loan_second['accountStatus']!='逾期') & (loan_second['month_Desc_report']<6) & (loan_second['is_now']==1)].sum()#现行最近6个月无逾期贷款本月应还款求和
        dict_out['loan_ndue_RepayedAmount_sum_06m_now']=loan_second['RepayedAmount'][(loan_second['accountStatus']!='逾期') & (loan_second['month_Desc_report']<6) & (loan_second['is_now']==1)].sum()#现行最近6个月无逾期贷款本月实还款求和
        dict_out['loan_ndue_planRepayAmount_max_06m_now']=loan_second['planRepayAmount'][(loan_second['accountStatus']!='逾期') & (loan_second['month_Desc_report']<6) & (loan_second['is_now']==1)].max()#现行最近6个月无逾期贷款本月应还款最大
        dict_out['loan_ndue_RepayedAmount_max_06m_now']=loan_second['RepayedAmount'][(loan_second['accountStatus']!='逾期') & (loan_second['month_Desc_report']<6) & (loan_second['is_now']==1)].max()#现行最近6个月无逾期贷款本月实还款最大

        #最近6个月无逾期有担保
        dict_out['loan_ndue_is_vouch_account_count_06m']=len(loan_second[(loan_second['accountStatus']!='逾期') & (loan_second['month_Desc_report']<6) & (loan_second['is_vouch']==1)])#最近6个月无逾期有担保贷款账户计数

        #现行最近6个月无逾期有担保
        dict_out['loan_ndue_is_vouch_account_count_06m_now']=len(loan_second[(loan_second['accountStatus']!='逾期') & (loan_second['month_Desc_report']<6) & (loan_second['is_vouch']==1) & (loan_second['is_now']==1)])#现行最近6个月无逾期有担保贷款账户计数
        dict_out['loan_ndue_is_vouch_planRepayAmount_sum_06m_now']=loan_second['planRepayAmount'][(loan_second['accountStatus']!='逾期') & (loan_second['month_Desc_report']<6) & (loan_second['is_vouch']==1) & (loan_second['is_now']==1)].sum()#现行最近6个月无逾期有担保贷款本月应还款求和
        dict_out['loan_ndue_is_vouch_RepayedAmount_sum_06m_now']=loan_second['RepayedAmount'][(loan_second['accountStatus']!='逾期') & (loan_second['month_Desc_report']<6) & (loan_second['is_vouch']==1) & (loan_second['is_now']==1)].sum()#现行最近6个月无逾期有担保贷款本月实还款求和
        dict_out['loan_ndue_is_vouch_planRepayAmount_max_06m_now']=loan_second['planRepayAmount'][(loan_second['accountStatus']!='逾期') & (loan_second['month_Desc_report']<6) & (loan_second['is_vouch']==1) & (loan_second['is_now']==1)].max()#现行最近6个月无逾期有担保贷款本月应还款最大
        dict_out['loan_ndue_is_vouch_RepayedAmount_max_06m_now']=loan_second['RepayedAmount'][(loan_second['accountStatus']!='逾期') & (loan_second['month_Desc_report']<6) & (loan_second['is_vouch']==1) & (loan_second['is_now']==1)].max()#现行最近6个月无逾期有担保贷款本月实还款最大

        #最近12个月无逾期
        dict_out['loan_ndue_account_count_12m']=len(loan_second[(loan_second['accountStatus']!='逾期') & (loan_second['month_Desc_report']<12)])#最近12个月无逾期贷款账户计数

        #现行最近12个月无逾期
        dict_out['loan_ndue_account_count_12m_now']=len(loan_second[(loan_second['accountStatus']!='逾期') & (loan_second['month_Desc_report']<12) & (loan_second['is_now']==1)])#现行最近12个月无逾期贷款账户计数
        dict_out['loan_ndue_planRepayAmount_sum_12m_now']=loan_second['planRepayAmount'][(loan_second['accountStatus']!='逾期') & (loan_second['month_Desc_report']<12) & (loan_second['is_now']==1)].sum()#现行最近12个月无逾期贷款本月应还款求和
        dict_out['loan_ndue_RepayedAmount_sum_12m_now']=loan_second['RepayedAmount'][(loan_second['accountStatus']!='逾期') & (loan_second['month_Desc_report']<12) & (loan_second['is_now']==1)].sum()#现行最近12个月无逾期贷款本月实还款求和
        dict_out['loan_ndue_planRepayAmount_max_12m_now']=loan_second['planRepayAmount'][(loan_second['accountStatus']!='逾期') & (loan_second['month_Desc_report']<12) & (loan_second['is_now']==1)].max()#现行最近12个月无逾期贷款本月应还款最大
        dict_out['loan_ndue_RepayedAmount_max_12m_now']=loan_second['RepayedAmount'][(loan_second['accountStatus']!='逾期') & (loan_second['month_Desc_report']<12) & (loan_second['is_now']==1)].max()#现行最近12个月无逾期贷款本月实还款最大

        #最近12个月无逾期有担保
        dict_out['loan_ndue_is_vouch_account_count_12m']=len(loan_second[(loan_second['accountStatus']!='逾期') & (loan_second['month_Desc_report']<12) & (loan_second['is_vouch']==1)])#最近12个月无逾期有担保贷款账户计数

        #现行最近12个月无逾期有担保
        dict_out['loan_ndue_is_vouch_account_count_12m_now']=len(loan_second[(loan_second['accountStatus']!='逾期') & (loan_second['month_Desc_report']<12) & (loan_second['is_vouch']==1) & (loan_second['is_now']==1)])#现行最近12个月无逾期有担保贷款账户计数
        dict_out['loan_ndue_is_vouch_planRepayAmount_sum_12m_now']=loan_second['planRepayAmount'][(loan_second['accountStatus']!='逾期') & (loan_second['month_Desc_report']<12) & (loan_second['is_vouch']==1) & (loan_second['is_now']==1)].sum()#现行最近12个月无逾期有担保贷款本月应还款求和
        dict_out['loan_ndue_is_vouch_RepayedAmount_sum_12m_now']=loan_second['RepayedAmount'][(loan_second['accountStatus']!='逾期') & (loan_second['month_Desc_report']<12) & (loan_second['is_vouch']==1) & (loan_second['is_now']==1)].sum()#现行最近12个月无逾期有担保贷款本月实还款求和
        dict_out['loan_ndue_is_vouch_planRepayAmount_max_12m_now']=loan_second['planRepayAmount'][(loan_second['accountStatus']!='逾期') & (loan_second['month_Desc_report']<12) & (loan_second['is_vouch']==1) & (loan_second['is_now']==1)].max()#现行最近12个月无逾期有担保贷款本月应还款最大
        dict_out['loan_ndue_is_vouch_RepayedAmount_max_12m_now']=loan_second['RepayedAmount'][(loan_second['accountStatus']!='逾期') & (loan_second['month_Desc_report']<12) & (loan_second['is_vouch']==1) & (loan_second['is_now']==1)].max()#现行最近12个月无逾期有担保贷款本月实还款最大

        #最近24个月无逾期
        dict_out['loan_ndue_account_count_24m']=len(loan_second[(loan_second['accountStatus']!='逾期') & (loan_second['month_Desc_report']<24)])#最近24个月无逾期贷款账户计数

        #现行最近24个月无逾期
        dict_out['loan_ndue_account_count_24m_now']=len(loan_second[(loan_second['accountStatus']!='逾期') & (loan_second['month_Desc_report']<24) & (loan_second['is_now']==1)])#现行最近24个月无逾期贷款账户计数
        dict_out['loan_ndue_planRepayAmount_sum_24m_now']=loan_second['planRepayAmount'][(loan_second['accountStatus']!='逾期') & (loan_second['month_Desc_report']<24) & (loan_second['is_now']==1)].sum()#现行最近24个月无逾期贷款本月应还款求和
        dict_out['loan_ndue_RepayedAmount_sum_24m_now']=loan_second['RepayedAmount'][(loan_second['accountStatus']!='逾期') & (loan_second['month_Desc_report']<24) & (loan_second['is_now']==1)].sum()#现行最近24个月无逾期贷款本月实还款求和
        dict_out['loan_ndue_planRepayAmount_max_24m_now']=loan_second['planRepayAmount'][(loan_second['accountStatus']!='逾期') & (loan_second['month_Desc_report']<24) & (loan_second['is_now']==1)].max()#现行最近24个月无逾期贷款本月应还款最大
        dict_out['loan_ndue_RepayedAmount_max_24m_now']=loan_second['RepayedAmount'][(loan_second['accountStatus']!='逾期') & (loan_second['month_Desc_report']<24) & (loan_second['is_now']==1)].max()#现行最近24个月无逾期贷款本月实还款最大

        #最近24个月无逾期有担保
        dict_out['loan_ndue_is_vouch_account_count_24m']=len(loan_second[(loan_second['accountStatus']!='逾期') & (loan_second['month_Desc_report']<24) & (loan_second['is_vouch']==1)])#最近24个月无逾期有担保贷款账户计数

        #现行最近24个月无逾期有担保
        dict_out['loan_ndue_is_vouch_account_count_24m_now']=len(loan_second[(loan_second['accountStatus']!='逾期') & (loan_second['month_Desc_report']<24) & (loan_second['is_vouch']==1) & (loan_second['is_now']==1)])#现行最近24个月无逾期有担保贷款账户计数
        dict_out['loan_ndue_is_vouch_planRepayAmount_sum_24m_now']=loan_second['planRepayAmount'][(loan_second['accountStatus']!='逾期') & (loan_second['month_Desc_report']<24) & (loan_second['is_vouch']==1) & (loan_second['is_now']==1)].sum()#现行最近24个月无逾期有担保贷款本月应还款求和
        dict_out['loan_ndue_is_vouch_RepayedAmount_sum_24m_now']=loan_second['RepayedAmount'][(loan_second['accountStatus']!='逾期') & (loan_second['month_Desc_report']<24) & (loan_second['is_vouch']==1) & (loan_second['is_now']==1)].sum()#现行最近24个月无逾期有担保贷款本月实还款求和
        dict_out['loan_ndue_is_vouch_planRepayAmount_max_24m_now']=loan_second['planRepayAmount'][(loan_second['accountStatus']!='逾期') & (loan_second['month_Desc_report']<24) & (loan_second['is_vouch']==1) & (loan_second['is_now']==1)].max()#现行最近24个月无逾期有担保贷款本月应还款最大
        dict_out['loan_ndue_is_vouch_RepayedAmount_max_24m_now']=loan_second['RepayedAmount'][(loan_second['accountStatus']!='逾期') & (loan_second['month_Desc_report']<24) & (loan_second['is_vouch']==1) & (loan_second['is_now']==1)].max()#现行最近24个月无逾期有担保贷款本月实还款最大

        #轻度逾期
        dict_out['loan_slight_overdue_account_count']=len(loan_second[loan_second['overdue_class']==1])#轻度逾期贷款账户计数
        dict_out['loan_slight_overdue_month_sum']=sum(list(map(sum,loan_second['overdue_month'][loan_second['overdue_class']==1])))#轻度逾期贷款逾期月数求和
        dict_out['loan_slight_overdue_amount_sum']=loan_second['loanAmount'][loan_second['overdue_class']==1].sum()#轻度逾期贷款本金求和
        dict_out['loan_slight_overdue_amount_max']=loan_second['loanAmount'][loan_second['overdue_class']==1].max()#轻度逾期贷款本金最大

        #现行轻度逾期
        dict_out['loan_slight_overdue_account_count_now']=len(loan_second[(loan_second['overdue_class']==1) & (loan_second['is_now']==1)])#现行轻度逾期贷款账户计数
        dict_out['loan_slight_overdue_month_sum_now']=sum(list(map(sum,loan_second['overdue_month'][(loan_second['overdue_class']==1) & (loan_second['is_now']==1)])))#现行轻度逾期贷款逾期月数求和
        dict_out['loan_slight_overdue_amount_sum_now']=loan_second['loanAmount'][(loan_second['overdue_class']==1) & (loan_second['is_now']==1)].sum()#现行轻度逾期贷款本金求和
        dict_out['loan_slight_overdue_balance_sum_now']=loan_second['balance'][(loan_second['overdue_class']==1) & (loan_second['is_now']==1)].sum()#现行轻度逾期贷款本金余额求和
        dict_out['loan_slight_overdue_planRepayAmount_sum_now']=loan_second['planRepayAmount'][(loan_second['overdue_class']==1) & (loan_second['is_now']==1)].sum()#现行轻度逾期贷款本月应还款求和
        dict_out['loan_slight_overdue_RepayedAmount_sum_now']=loan_second['RepayedAmount'][(loan_second['overdue_class']==1) & (loan_second['is_now']==1)].sum()#现行轻度逾期贷款本月实还款求和
        dict_out['loan_slight_overdue_currentOverdueAmount_sum_now']=loan_second['currentOverdueAmount'][(loan_second['overdue_class']==1) & (loan_second['is_now']==1)].sum()#现行轻度逾期贷款当前逾期金额求和
        dict_out['loan_slight_overdue_amount_max_now']=loan_second['loanAmount'][(loan_second['overdue_class']==1) & (loan_second['is_now']==1)].max()#现行轻度逾期贷款本金最大
        dict_out['loan_slight_overdue_balance_max_now']=loan_second['balance'][(loan_second['overdue_class']==1) & (loan_second['is_now']==1)].max()#现行轻度逾期贷款本金余额最大
        dict_out['loan_slight_overdue_planRepayAmount_max_now']=loan_second['planRepayAmount'][(loan_second['overdue_class']==1) & (loan_second['is_now']==1)].max()#现行轻度逾期贷款本月应还款最大
        dict_out['loan_slight_overdue_RepayedAmount_max_now']=loan_second['RepayedAmount'][(loan_second['overdue_class']==1) & (loan_second['is_now']==1)].max()#现行轻度逾期贷款本月实还款最大
        dict_out['loan_slight_overdue_currentOverdueAmount_max_now']=loan_second['currentOverdueAmount'][(loan_second['overdue_class']==1) & (loan_second['is_now']==1)].max()#现行轻度逾期贷款当前逾期金额最大

        #轻度逾期有担保
        dict_out['loan_slight_overdue_is_vouch_account_count']=len(loan_second[(loan_second['overdue_class']==1) & (loan_second['is_vouch']==1)])#轻度逾期有担保贷款账户计数
        dict_out['loan_slight_overdue_is_vouch_month_sum']=sum(list(map(sum,loan_second['overdue_month'][(loan_second['overdue_class']==1) & (loan_second['is_vouch']==1)])))#轻度逾期有担保贷款逾期月数求和
        dict_out['loan_slight_overdue_is_vouch_amount_sum']=loan_second['loanAmount'][(loan_second['overdue_class']==1) & (loan_second['is_vouch']==1)].sum()#轻度逾期有担保贷款本金求和
        dict_out['loan_slight_overdue_is_vouch_amount_max']=loan_second['loanAmount'][(loan_second['overdue_class']==1) & (loan_second['is_vouch']==1)].max()#轻度逾期有担保贷款本金最大

        #现行轻度逾期有担保
        dict_out['loan_slight_overdue_is_vouch_account_count_now']=len(loan_second[(loan_second['overdue_class']==1) & (loan_second['is_vouch']==1) & (loan_second['is_now']==1)])#现行轻度逾期有担保贷款账户计数
        dict_out['loan_slight_overdue_is_vouch_month_sum_now']=sum(list(map(sum,loan_second['overdue_month'][(loan_second['overdue_class']==1) & (loan_second['is_vouch']==1) & (loan_second['is_now']==1)])))#现行轻度逾期有担保贷款逾期月数求和
        dict_out['loan_slight_overdue_is_vouch_amount_sum_now']=loan_second['loanAmount'][(loan_second['overdue_class']==1) & (loan_second['is_vouch']==1) & (loan_second['is_now']==1)].sum()#现行轻度逾期有担保贷款本金求和
        dict_out['loan_slight_overdue_is_vouch_balance_sum_now']=loan_second['balance'][(loan_second['overdue_class']==1) & (loan_second['is_vouch']==1) & (loan_second['is_now']==1)].sum()#现行轻度逾期有担保贷款本金余额求和
        dict_out['loan_slight_overdue_is_vouch_planRepayAmount_sum_now']=loan_second['planRepayAmount'][(loan_second['overdue_class']==1) & (loan_second['is_vouch']==1) & (loan_second['is_now']==1)].sum()#现行轻度逾期有担保贷款本月应还款求和
        dict_out['loan_slight_overdue_is_vouch_RepayedAmount_sum_now']=loan_second['RepayedAmount'][(loan_second['overdue_class']==1) & (loan_second['is_vouch']==1) & (loan_second['is_now']==1)].sum()#现行轻度逾期有担保贷款本月实还款求和
        dict_out['loan_slight_overdue_is_vouch_currentOverdueAmount_sum_now']=loan_second['currentOverdueAmount'][(loan_second['overdue_class']==1) & (loan_second['is_vouch']==1) & (loan_second['is_now']==1)].sum()#现行轻度逾期有担保贷款当前逾期金额求和
        dict_out['loan_slight_overdue_is_vouch_amount_max_now']=loan_second['loanAmount'][(loan_second['overdue_class']==1) & (loan_second['is_vouch']==1) & (loan_second['is_now']==1)].max()#现行轻度逾期有担保贷款本金最大
        dict_out['loan_slight_overdue_is_vouch_balance_max_now']=loan_second['balance'][(loan_second['overdue_class']==1) & (loan_second['is_vouch']==1) & (loan_second['is_now']==1)].max()#现行轻度逾期有担保贷款本金余额最大
        dict_out['loan_slight_overdue_is_vouch_planRepayAmount_max_now']=loan_second['planRepayAmount'][(loan_second['overdue_class']==1) & (loan_second['is_vouch']==1) & (loan_second['is_now']==1)].max()#现行轻度逾期有担保贷款本月应还款最大
        dict_out['loan_slight_overdue_is_vouch_RepayedAmount_max_now']=loan_second['RepayedAmount'][(loan_second['overdue_class']==1) & (loan_second['is_vouch']==1) & (loan_second['is_now']==1)].max()#现行轻度逾期有担保贷款本月实还款最大
        dict_out['loan_slight_overdue_is_vouch_currentOverdueAmount_max_now']=loan_second['currentOverdueAmount'][(loan_second['overdue_class']==1) & (loan_second['is_vouch']==1) & (loan_second['is_now']==1)].max()#现行轻度逾期有担保贷款当前逾期金额最大

        #房贷
        dict_out['house_loan_account_count']=len(loan_second[loan_second['businessType']=='个人住房商业贷款'])#房贷账户计数
        dict_out['house_loan_overdue_month_sum']=sum(list(map(sum,loan_second['overdue_month'][loan_second['businessType']=='个人住房商业贷款'])))#房贷逾期月数求和
        dict_out['house_loan_amount_sum']=loan_second['loanAmount'][loan_second['businessType']=='个人住房商业贷款'].sum()#房贷本金求和
        dict_out['house_loan_overdue_month_max']=loan_second['overdue_month_max'][loan_second['businessType']=='个人住房商业贷款'].max()#房贷逾期持续月数最大
        dict_out['house_loan_amount_max']=loan_second['loanAmount'][loan_second['businessType']=='个人住房商业贷款'].max()#房贷本金最大
        dict_out['house_loan_rating_worst']={'1.0':'损失', '2.0':'可疑', '3.0':'关注', '4.0':'次级', '5.0':'正常','nan':"无"}[str(loan_second['classify5_num'][loan_second['businessType']=='个人住房商业贷款'].min())]#房贷Rating最差

        #现行房贷
        dict_out['house_loan_account_count_now']=len(loan_second[(loan_second['businessType']=='个人住房商业贷款') & (loan_second['is_now']==1)])#现行房贷账户计数
        dict_out['house_loan_overdue_month_sum_now']=sum(list(map(sum,loan_second['overdue_month'][(loan_second['businessType']=='个人住房商业贷款') & (loan_second['is_now']==1)])))#现行房贷逾期月数求和
        dict_out['house_loan_amount_sum_now']=loan_second['loanAmount'][(loan_second['businessType']=='个人住房商业贷款') & (loan_second['is_now']==1)].sum()#现行房贷本金求和
        dict_out['house_loan_currentOverdueTerms_sum_now']=loan_second['currentOverdueTerms'][(loan_second['businessType']=='个人住房商业贷款') & (loan_second['is_now']==1)].sum()#现行房贷当前逾期期数求和
        dict_out['house_loan_balance_sum_now']=loan_second['balance'][(loan_second['businessType']=='个人住房商业贷款') & (loan_second['is_now']==1)].sum()#现行房贷本金余额求和
        dict_out['house_loan_planRepayAmount_sum_now']=loan_second['planRepayAmount'][(loan_second['businessType']=='个人住房商业贷款') & (loan_second['is_now']==1)].sum()#现行房贷本月应还款求和
        dict_out['house_loan_RepayedAmount_sum_now']=loan_second['RepayedAmount'][(loan_second['businessType']=='个人住房商业贷款') & (loan_second['is_now']==1)].sum()#现行房贷本月实还款求和
        dict_out['house_loan_currentOverdueAmount_sum_now']=loan_second['currentOverdueAmount'][(loan_second['businessType']=='个人住房商业贷款') & (loan_second['is_now']==1)].sum()#现行房贷当前逾期金额求和
        dict_out['house_loan_overdue31Amount_sum_now']=loan_second['overdue31Amount'][(loan_second['businessType']=='个人住房商业贷款') & (loan_second['is_now']==1)].sum()#现行房贷逾期31-60天未还本金求和
        dict_out['house_loan_overdue61Amount_sum_now']=loan_second['overdue61Amount'][(loan_second['businessType']=='个人住房商业贷款') & (loan_second['is_now']==1)].sum()#现行房贷逾期61-90天未还本金求和
        dict_out['house_loan_overdue91Amount_sum_now']=loan_second['overdue91Amount'][(loan_second['businessType']=='个人住房商业贷款') & (loan_second['is_now']==1)].sum()#现行房贷逾期91-180天未还本金求和
        dict_out['house_loan_overdue180Amount_sum_now']=loan_second['overdue180Amount'][(loan_second['businessType']=='个人住房商业贷款') & (loan_second['is_now']==1)].sum()#现行房贷逾期180天以上未还本金求和
        dict_out['house_loan_overdue_month_max_now']=loan_second['overdue_month_max'][(loan_second['businessType']=='个人住房商业贷款') & (loan_second['is_now']==1)].max()#现行房贷逾期持续月数最大
        dict_out['house_loan_amount_max_now']=loan_second['loanAmount'][(loan_second['businessType']=='个人住房商业贷款') & (loan_second['is_now']==1)].max()#现行房贷本金最大
        dict_out['house_loan_currentOverdueTerms_max_now']=loan_second['currentOverdueTerms'][(loan_second['businessType']=='个人住房商业贷款') & (loan_second['is_now']==1)].max()#现行房贷当前逾期期数最大
        dict_out['house_loan_balance_max_now']=loan_second['balance'][(loan_second['businessType']=='个人住房商业贷款') & (loan_second['is_now']==1)].max()#现行房贷本金余额最大
        dict_out['house_loan_planRepayAmount_max_now']=loan_second['planRepayAmount'][(loan_second['businessType']=='个人住房商业贷款') & (loan_second['is_now']==1)].max()#现行房贷本月应还款最大
        dict_out['house_loan_RepayedAmount_max_now']=loan_second['RepayedAmount'][(loan_second['businessType']=='个人住房商业贷款') & (loan_second['is_now']==1)].max()#现行房贷本月实还款最大
        dict_out['house_loan_currentOverdueAmount_max_now']=loan_second['currentOverdueAmount'][(loan_second['businessType']=='个人住房商业贷款') & (loan_second['is_now']==1)].max()#现行房贷当前逾期金额最大
        dict_out['house_loan_overdue31Amount_max_now']=loan_second['overdue31Amount'][(loan_second['businessType']=='个人住房商业贷款') & (loan_second['is_now']==1)].max()#现行房贷逾期31-60天未还本金最大
        dict_out['house_loan_overdue61Amount_max_now']=loan_second['overdue61Amount'][(loan_second['businessType']=='个人住房商业贷款') & (loan_second['is_now']==1)].max()#现行房贷逾期61-90天未还本金最大
        dict_out['house_loan_overdue91Amount_max_now']=loan_second['overdue91Amount'][(loan_second['businessType']=='个人住房商业贷款') & (loan_second['is_now']==1)].max()#现行房贷逾期91-180天未还本金最大
        dict_out['house_loan_overdue180Amount_max_now']=loan_second['overdue180Amount'][(loan_second['businessType']=='个人住房商业贷款') & (loan_second['is_now']==1)].max()#现行房贷逾期180天以上未还本金最大

        #无逾期房贷
        dict_out['house_loan_ndue_account_count']=len(loan_second[(loan_second['overdue_month']==0) & (loan_second['businessType']=='个人住房商业贷款')])#无逾期房贷账户计数
        dict_out['house_loan_ndue_amount_sum']=loan_second['loanAmount'][(loan_second['overdue_month']==0) & (loan_second['businessType']=='个人住房商业贷款')].sum()#无逾期房贷本金求和
        dict_out['house_loan_ndue_amount_max']=loan_second['loanAmount'][(loan_second['overdue_month']==0) & (loan_second['businessType']=='个人住房商业贷款')].max()#无逾期房贷本金最大

        #现行无逾期房贷
        dict_out['house_loan_ndue_account_count_now']=len(loan_second[(loan_second['overdue_month']==0) & (loan_second['businessType']=='个人住房商业贷款') & (loan_second['is_now']==1)])#现行无逾期房贷账户计数
        dict_out['house_loan_ndue_amount_sum_now']=loan_second['loanAmount'][(loan_second['overdue_month']==0) & (loan_second['businessType']=='个人住房商业贷款') & (loan_second['is_now']==1)].sum()#现行无逾期房贷本金求和
        dict_out['house_loan_ndue_balance_sum_now']=loan_second['balance'][(loan_second['overdue_month']==0) & (loan_second['businessType']=='个人住房商业贷款') & (loan_second['is_now']==1)].sum()#现行无逾期房贷本金余额求和
        dict_out['house_loan_ndue_planRepayAmount_sum_now']=loan_second['planRepayAmount'][(loan_second['overdue_month']==0) & (loan_second['businessType']=='个人住房商业贷款') & (loan_second['is_now']==1)].sum()#现行无逾期房贷本月应还款求和
        dict_out['house_loan_ndue_RepayedAmount_sum_now']=loan_second['RepayedAmount'][(loan_second['overdue_month']==0) & (loan_second['businessType']=='个人住房商业贷款') & (loan_second['is_now']==1)].sum()#现行无逾期房贷本月实还款求和
        dict_out['house_loan_ndue_amount_max_now']=loan_second['loanAmount'][(loan_second['overdue_month']==0) & (loan_second['businessType']=='个人住房商业贷款') & (loan_second['is_now']==1)].max()#现行无逾期房贷本金最大
        dict_out['house_loan_ndue_balance_max_now']=loan_second['balance'][(loan_second['overdue_month']==0) & (loan_second['businessType']=='个人住房商业贷款') & (loan_second['is_now']==1)].max()#现行无逾期房贷本金余额最大
        dict_out['house_loan_ndue_planRepayAmount_max_now']=loan_second['planRepayAmount'][(loan_second['overdue_month']==0) & (loan_second['businessType']=='个人住房商业贷款') & (loan_second['is_now']==1)].max()#现行无逾期房贷本月应还款最大
        dict_out['house_loan_ndue_RepayedAmount_max_now']=loan_second['RepayedAmount'][(loan_second['overdue_month']==0) & (loan_second['businessType']=='个人住房商业贷款') & (loan_second['is_now']==1)].max()#现行无逾期房贷本月实还款最大

        #轻度逾期房贷
        dict_out['house_loan_slight_overdue_account_count']=len(loan_second[(loan_second['overdue_class']==1) & (loan_second['businessType']=='个人住房商业贷款')])#轻度逾期房贷账户计数
        dict_out['house_loan_slight_overdue_month_sum']=sum(list(map(sum,loan_second['overdue_month'][(loan_second['overdue_class']==1) & (loan_second['businessType']=='个人住房商业贷款')])))#轻度逾期房贷逾期月数求和
        dict_out['house_loan_slight_amount_sum']=loan_second['loanAmount'][(loan_second['overdue_class']==1) & (loan_second['businessType']=='个人住房商业贷款')].sum()#轻度逾期房贷本金求和
        dict_out['house_loan_slight_amount_max']=loan_second['loanAmount'][(loan_second['overdue_class']==1) & (loan_second['businessType']=='个人住房商业贷款')].max()#轻度逾期房贷本金最大

        #现行轻度逾期房贷
        dict_out['house_loan_slight_overdue_account_count_now']=len(loan_second[(loan_second['overdue_class']==1) & (loan_second['businessType']=='个人住房商业贷款') & (loan_second['is_now']==1)])#现行轻度逾期房贷账户计数
        dict_out['house_loan_slight_overdue_month_sum_now']=sum(list(map(sum,loan_second['overdue_month'][(loan_second['overdue_class']==1) & (loan_second['businessType']=='个人住房商业贷款') & (loan_second['is_now']==1)])))#现行轻度逾期房贷逾期月数求和
        dict_out['house_loan_slight_overdue_amount_sum_now']=loan_second['loanAmount'][(loan_second['overdue_class']==1) & (loan_second['businessType']=='个人住房商业贷款') & (loan_second['is_now']==1)].sum()#现行轻度逾期房贷本金求和
        dict_out['house_loan_slight_overdue_balance_sum_now']=loan_second['balance'][(loan_second['overdue_class']==1) & (loan_second['businessType']=='个人住房商业贷款') & (loan_second['is_now']==1)].sum()#现行轻度逾期房贷本金余额求和
        dict_out['house_loan_slight_overdue_planRepayAmount_sum_now']=loan_second['planRepayAmount'][(loan_second['overdue_class']==1) & (loan_second['businessType']=='个人住房商业贷款') & (loan_second['is_now']==1)].sum()#现行轻度逾期房贷本月应还款求和
        dict_out['house_loan_slight_overdue_RepayedAmount_sum_now']=loan_second['RepayedAmount'][(loan_second['overdue_class']==1) & (loan_second['businessType']=='个人住房商业贷款') & (loan_second['is_now']==1)].sum()#现行轻度逾期房贷本月实还款求和
        dict_out['house_loan_slight_overdue_currentOverdueAmount_sum_now']=loan_second['currentOverdueAmount'][(loan_second['overdue_class']==1) & (loan_second['businessType']=='个人住房商业贷款') & (loan_second['is_now']==1)].sum()#现行轻度逾期房贷当前逾期金额求和
        dict_out['house_loan_slight_overdue_amount_max_now']=loan_second['loanAmount'][(loan_second['overdue_class']==1) & (loan_second['businessType']=='个人住房商业贷款') & (loan_second['is_now']==1)].max()#现行轻度逾期房贷本金最大
        dict_out['house_loan_slight_overdue_balance_max_now']=loan_second['balance'][(loan_second['overdue_class']==1) & (loan_second['businessType']=='个人住房商业贷款') & (loan_second['is_now']==1)].max()#现行轻度逾期房贷本金余额最大
        dict_out['house_loan_slight_overdue_planRepayAmount_max_now']=loan_second['planRepayAmount'][(loan_second['overdue_class']==1) & (loan_second['businessType']=='个人住房商业贷款') & (loan_second['is_now']==1)].max()#现行轻度逾期房贷本月应还款最大
        dict_out['house_loan_slight_overdue_RepayedAmount_max_now']=loan_second['RepayedAmount'][(loan_second['overdue_class']==1) & (loan_second['businessType']=='个人住房商业贷款') & (loan_second['is_now']==1)].max()#现行轻度逾期房贷本月实还款最大
        dict_out['house_loan_slight_overdue_currentOverdueAmount_max_now']=loan_second['currentOverdueAmount'][(loan_second['overdue_class']==1) & (loan_second['businessType']=='个人住房商业贷款') & (loan_second['is_now']==1)].max()#现行轻度逾期房贷当前逾期金额最大

        #个人汽车贷款
        dict_out['car_loan_account_count']=len(loan_second[loan_second['businessType']=='个人汽车消费贷款'])#个人汽车贷款账户计数
        dict_out['car_loan_overdue_month_sum']=sum(list(map(sum,loan_second['overdue_month'][loan_second['businessType']=='个人汽车消费贷款'])))#个人汽车贷款逾期月数求和
        dict_out['car_loan_amount_sum']=loan_second['loanAmount'][loan_second['businessType']=='个人汽车消费贷款'].sum()#个人汽车贷款本金求和
        dict_out['car_loan_overdue_month_max']=loan_second['overdue_month_max'][loan_second['businessType']=='个人汽车消费贷款'].max()#个人汽车贷款逾期持续月数最大
        dict_out['car_loan_amount_max']=loan_second['loanAmount'][loan_second['businessType']=='个人汽车消费贷款'].max()#个人汽车贷款本金最大
        dict_out['car_loan_rating_worst']={'1.0':'损失', '2.0':'可疑', '3.0':'关注', '4.0':'次级', '5.0':'正常','nan':"无"}[str(loan_second['classify5_num'][loan_second['businessType']=='个人汽车消费贷款'].min())]#个人汽车贷款Rating最差

        #现行个人汽车贷款
        dict_out['car_loan_account_count_now']=len(loan_second[(loan_second['businessType']=='个人汽车消费贷款') & (loan_second['is_now']==1)])#现行个人汽车贷款账户计数
        dict_out['car_loan_overdue_month_sum_now']=sum(list(map(sum,loan_second['overdue_month'][(loan_second['businessType']=='个人汽车消费贷款') & (loan_second['is_now']==1)])))#现行个人汽车贷款逾期月数求和
        dict_out['car_loan_amount_sum_now']=loan_second['loanAmount'][(loan_second['businessType']=='个人汽车消费贷款') & (loan_second['is_now']==1)].sum()#现行个人汽车贷款本金求和
        dict_out['car_loan_currentOverdueTerms_sum_now']=loan_second['currentOverdueTerms'][(loan_second['businessType']=='个人汽车消费贷款') & (loan_second['is_now']==1)].sum()#现行个人汽车贷款当前逾期期数求和
        dict_out['car_loan_balance_sum_now']=loan_second['balance'][(loan_second['businessType']=='个人汽车消费贷款') & (loan_second['is_now']==1)].sum()#现行个人汽车贷款本金余额求和
        dict_out['car_loan_planRepayAmount_sum_now']=loan_second['planRepayAmount'][(loan_second['businessType']=='个人汽车消费贷款') & (loan_second['is_now']==1)].sum()#现行个人汽车贷款本月应还款求和
        dict_out['car_loan_RepayedAmount_sum_now']=loan_second['RepayedAmount'][(loan_second['businessType']=='个人汽车消费贷款') & (loan_second['is_now']==1)].sum()#现行个人汽车贷款本月实还款求和
        dict_out['car_loan_currentOverdueAmount_sum_now']=loan_second['currentOverdueAmount'][(loan_second['businessType']=='个人汽车消费贷款') & (loan_second['is_now']==1)].sum()#现行个人汽车贷款当前逾期金额求和
        dict_out['car_loan_overdue31Amount_sum_now']=loan_second['overdue31Amount'][(loan_second['businessType']=='个人汽车消费贷款') & (loan_second['is_now']==1)].sum()#现行个人汽车贷款逾期31-60天未还本金求和
        dict_out['car_loan_overdue61Amount_sum_now']=loan_second['overdue61Amount'][(loan_second['businessType']=='个人汽车消费贷款') & (loan_second['is_now']==1)].sum()#现行个人汽车贷款逾期61-90天未还本金求和
        dict_out['car_loan_overdue91Amount_sum_now']=loan_second['overdue91Amount'][(loan_second['businessType']=='个人汽车消费贷款') & (loan_second['is_now']==1)].sum()#现行个人汽车贷款逾期91-180天未还本金求和
        dict_out['car_loan_overdue180Amount_sum_now']=loan_second['overdue180Amount'][(loan_second['businessType']=='个人汽车消费贷款') & (loan_second['is_now']==1)].sum()#现行个人汽车贷款逾期180天以上未还本金求和
        dict_out['car_loan_overdue_month_max_now']=loan_second['overdue_month_max'][(loan_second['businessType']=='个人汽车消费贷款') & (loan_second['is_now']==1)].max()#现行个人汽车贷款逾期持续月数最大
        dict_out['car_loan_amount_max_now']=loan_second['loanAmount'][(loan_second['businessType']=='个人汽车消费贷款') & (loan_second['is_now']==1)].max()#现行个人汽车贷款本金最大
        dict_out['car_loan_currentOverdueTerms_max_now']=loan_second['currentOverdueTerms'][(loan_second['businessType']=='个人汽车消费贷款') & (loan_second['is_now']==1)].max()#现行个人汽车贷款当前逾期期数最大
        dict_out['car_loan_balance_max_now']=loan_second['balance'][(loan_second['businessType']=='个人汽车消费贷款') & (loan_second['is_now']==1)].max()#现行个人汽车贷款本金余额最大
        dict_out['car_loan_planRepayAmount_max_now']=loan_second['planRepayAmount'][(loan_second['businessType']=='个人汽车消费贷款') & (loan_second['is_now']==1)].max()#现行个人汽车贷款本月应还款最大
        dict_out['car_loan_RepayedAmount_max_now']=loan_second['RepayedAmount'][(loan_second['businessType']=='个人汽车消费贷款') & (loan_second['is_now']==1)].max()#现行个人汽车贷款本月实还款最大
        dict_out['car_loan_currentOverdueAmount_max_now']=loan_second['currentOverdueAmount'][(loan_second['businessType']=='个人汽车消费贷款') & (loan_second['is_now']==1)].max()#现行个人汽车贷款当前逾期金额最大
        dict_out['car_loan_overdue31Amount_max_now']=loan_second['overdue31Amount'][(loan_second['businessType']=='个人汽车消费贷款') & (loan_second['is_now']==1)].max()#现行个人汽车贷款逾期31-60天未还本金最大
        dict_out['car_loan_overdue61Amount_max_now']=loan_second['overdue61Amount'][(loan_second['businessType']=='个人汽车消费贷款') & (loan_second['is_now']==1)].max()#现行个人汽车贷款逾期61-90天未还本金最大
        dict_out['car_loan_overdue91Amount_max_now']=loan_second['overdue91Amount'][(loan_second['businessType']=='个人汽车消费贷款') & (loan_second['is_now']==1)].max()#现行个人汽车贷款逾期91-180天未还本金最大
        dict_out['car_loan_overdue180Amount_max_now']=loan_second['overdue180Amount'][(loan_second['businessType']=='个人汽车消费贷款') & (loan_second['is_now']==1)].max()#现行个人汽车贷款逾期180天以上未还本金最大

        #无逾期(ndue)个人汽车贷款
        dict_out['car_loan_ndue_account_count']=len(loan_second[(loan_second['businessType']=='个人汽车消费贷款') & (loan_second['overdue_month']==0)])#无逾期个人汽车贷款账户计数
        dict_out['car_loan_ndue_amount_sum']=loan_second['loanAmount'][(loan_second['businessType']=='个人汽车消费贷款') & (loan_second['overdue_month']==0)].sum()#无逾期个人汽车贷款本金求和
        dict_out['car_loan_ndue_amount_max']=loan_second['loanAmount'][(loan_second['businessType']=='个人汽车消费贷款') & (loan_second['overdue_month']==0)].max()#无逾期个人汽车贷款本金最大

        #现行无逾期(ndue)个人汽车贷款
        dict_out['car_loan_ndue_account_count_now']=len(loan_second[(loan_second['businessType']=='个人汽车消费贷款') & (loan_second['overdue_month']==0) & (loan_second['is_now']==1)])#现行无逾期个人汽车贷款账户计数
        dict_out['car_loan_ndue_amount_sum_now']=loan_second['loanAmount'][(loan_second['businessType']=='个人汽车消费贷款') & (loan_second['overdue_month']==0) & (loan_second['is_now']==1)].sum()#现行无逾期个人汽车贷款本金求和
        dict_out['car_loan_ndue_balance_sum_now']=loan_second['balance'][(loan_second['businessType']=='个人汽车消费贷款') & (loan_second['overdue_month']==0) & (loan_second['is_now']==1)].sum()#现行无逾期个人汽车贷款本金余额求和
        dict_out['car_loan_ndue_planRepayAmount_sum_now']=loan_second['planRepayAmount'][(loan_second['businessType']=='个人汽车消费贷款') & (loan_second['overdue_month']==0) & (loan_second['is_now']==1)].sum()#现行无逾期个人汽车贷款本月应还款求和
        dict_out['car_loan_ndue_RepayedAmount_sum_now']=loan_second['RepayedAmount'][(loan_second['businessType']=='个人汽车消费贷款') & (loan_second['overdue_month']==0) & (loan_second['is_now']==1)].sum()#现行无逾期个人汽车贷款本月实还款求和
        dict_out['car_loan_ndue_amount_max_now']=loan_second['loanAmount'][(loan_second['businessType']=='个人汽车消费贷款') & (loan_second['overdue_month']==0) & (loan_second['is_now']==1)].max()#现行无逾期个人汽车贷款本金最大
        dict_out['car_loan_ndue_balance_max_now']=loan_second['balance'][(loan_second['businessType']=='个人汽车消费贷款') & (loan_second['overdue_month']==0) & (loan_second['is_now']==1)].max()#现行无逾期个人汽车贷款本金余额最大
        dict_out['car_loan_ndue_planRepayAmount_max_now']=loan_second['planRepayAmount'][(loan_second['businessType']=='个人汽车消费贷款') & (loan_second['overdue_month']==0) & (loan_second['is_now']==1)].max()#现行无逾期个人汽车贷款本月应还款最大
        dict_out['car_loan_ndue_RepayedAmount_max_now']=loan_second['RepayedAmount'][(loan_second['businessType']=='个人汽车消费贷款') & (loan_second['overdue_month']==0) & (loan_second['is_now']==1)].max()#现行无逾期个人汽车贷款本月实还款最大

        #轻度逾期个人汽车贷款
        dict_out['car_loan_slight_overdue_account_count']=len(loan_second[(loan_second['overdue_class']==1) & (loan_second['businessType']=='个人汽车消费贷款')])#轻度逾期个人汽车贷款账户计数
        dict_out['car_loan_slight_overdue_month_sum']=sum(list(map(sum,loan_second['overdue_month'][(loan_second['overdue_class']==1) & (loan_second['businessType']=='个人汽车消费贷款')])))#轻度逾期个人汽车贷款逾期月数求和
        dict_out['car_loan_slight_overdue_amount_sum']=loan_second['loanAmount'][(loan_second['overdue_class']==1) & (loan_second['businessType']=='个人汽车消费贷款')].sum()#轻度逾期个人汽车贷款本金求和
        dict_out['car_loan_slight_overdue_amount_max']=loan_second['loanAmount'][(loan_second['overdue_class']==1) & (loan_second['businessType']=='个人汽车消费贷款')].max()#轻度逾期个人汽车贷款本金最大

        #现行轻度逾期个人汽车贷款
        dict_out['car_loan_slight_overdue_account_count_now']=len(loan_second[(loan_second['overdue_class']==1) & (loan_second['businessType']=='个人汽车消费贷款') & (loan_second['is_now']==1)])#现行轻度逾期个人汽车贷款账户计数
        dict_out['car_loan_slight_overdue_month_sum_now']=sum(list(map(sum,loan_second['overdue_month'][(loan_second['overdue_class']==1) & (loan_second['businessType']=='个人汽车消费贷款') & (loan_second['is_now']==1)])))#现行轻度逾期个人汽车贷款逾期月数求和
        dict_out['car_loan_slight_overdue_amount_sum_now']=loan_second['loanAmount'][(loan_second['overdue_class']==1) & (loan_second['businessType']=='个人汽车消费贷款') & (loan_second['is_now']==1)].sum()#现行轻度逾期个人汽车贷款本金求和
        dict_out['car_loan_slight_overdue_balance_sum_now']=loan_second['balance'][(loan_second['overdue_class']==1) & (loan_second['businessType']=='个人汽车消费贷款') & (loan_second['is_now']==1)].sum()#现行轻度逾期个人汽车贷款本金余额求和
        dict_out['car_loan_slight_overdue_planRepayAmount_sum_now']=loan_second['planRepayAmount'][(loan_second['overdue_class']==1) & (loan_second['businessType']=='个人汽车消费贷款') & (loan_second['is_now']==1)].sum()#现行轻度逾期个人汽车贷款本月应还款求和
        dict_out['car_loan_slight_overdue_RepayedAmount_sum_now']=loan_second['RepayedAmount'][(loan_second['overdue_class']==1) & (loan_second['businessType']=='个人汽车消费贷款') & (loan_second['is_now']==1)].sum()#现行轻度逾期个人汽车贷款本月实还款求和
        dict_out['car_loan_slight_overdue_currentOverdueAmount_sum_now']=loan_second['currentOverdueAmount'][(loan_second['overdue_class']==1) & (loan_second['businessType']=='个人汽车消费贷款') & (loan_second['is_now']==1)].sum()#现行轻度逾期个人汽车贷款当前逾期金额求和
        dict_out['car_loan_slight_overdue_amount_max_now']=loan_second['loanAmount'][(loan_second['overdue_class']==1) & (loan_second['businessType']=='个人汽车消费贷款') & (loan_second['is_now']==1)].max()#现行轻度逾期个人汽车贷款本金最大
        dict_out['car_loan_slight_overdue_balance_max_now']=loan_second['balance'][(loan_second['overdue_class']==1) & (loan_second['businessType']=='个人汽车消费贷款') & (loan_second['is_now']==1)].max()#现行轻度逾期个人汽车贷款本金余额最大
        dict_out['car_loan_slight_overdue_planRepayAmount_max_now']=loan_second['planRepayAmount'][(loan_second['overdue_class']==1) & (loan_second['businessType']=='个人汽车消费贷款') & (loan_second['is_now']==1)].max()#现行轻度逾期个人汽车贷款本月应还款最大
        dict_out['car_loan_slight_overdue_RepayedAmount_max_now']=loan_second['RepayedAmount'][(loan_second['overdue_class']==1) & (loan_second['businessType']=='个人汽车消费贷款') & (loan_second['is_now']==1)].max()#现行轻度逾期个人汽车贷款本月实还款最大
        dict_out['car_loan_slight_overdue_currentOverdueAmount_max_now']=loan_second['currentOverdueAmount'][(loan_second['overdue_class']==1) & (loan_second['businessType']=='个人汽车消费贷款') & (loan_second['is_now']==1)].max()#现行轻度逾期个人汽车贷款当前逾期金额最大

        #个人助学贷款
        dict_out['stu_loan_account_count']=len(loan_second[loan_second['businessType']=='个人助学贷款'])#个人助学贷款账户计数
        dict_out['stu_loan_overdue_month_sum']=sum(list(map(sum,loan_second['overdue_month'][loan_second['businessType']=='个人助学贷款'])))#个人助学贷款逾期月数求和
        dict_out['stu_loan_amount_sum']=loan_second['loanAmount'][loan_second['businessType']=='个人助学贷款'].sum()#个人助学贷款本金求和
        dict_out['stu_loan_overdue_month_max']=loan_second['overdue_month_max'][loan_second['businessType']=='个人助学贷款'].max()#个人助学贷款逾期持续月数最大
        dict_out['stu_loan_amount_max']=loan_second['loanAmount'][loan_second['businessType']=='个人助学贷款'].max()#个人助学贷款本金最大
        dict_out['stu_loan_rating_worst']={'1.0':'损失', '2.0':'可疑', '3.0':'关注', '4.0':'次级', '5.0':'正常','nan':"无"}[str(loan_second['classify5_num'][loan_second['businessType']=='个人助学贷款'].min())]#个人助学贷款Rating最差

        #现行个人助学贷款
        dict_out['stu_loan_account_count_now']=len(loan_second[(loan_second['businessType']=='个人助学贷款') & (loan_second['is_now']==1)])#现行个人助学贷款账户计数
        dict_out['stu_loan_overdue_month_sum_now']=sum(list(map(sum,loan_second['overdue_month'][(loan_second['businessType']=='个人助学贷款') & (loan_second['is_now']==1)])))#现行个人助学贷款逾期月数求和
        dict_out['stu_loan_amount_sum_now']=loan_second['loanAmount'][(loan_second['businessType']=='个人助学贷款') & (loan_second['is_now']==1)].sum()#现行个人助学贷款本金求和
        dict_out['stu_loan_currentOverdueTerms_sum_now']=loan_second['currentOverdueTerms'][(loan_second['businessType']=='个人助学贷款') & (loan_second['is_now']==1)].sum()#现行个人助学贷款当前逾期期数求和
        dict_out['stu_loan_balance_sum_now']=loan_second['balance'][(loan_second['businessType']=='个人助学贷款') & (loan_second['is_now']==1)].sum()#现行个人助学贷款本金余额求和
        dict_out['stu_loan_planRepayAmount_sum_now']=loan_second['planRepayAmount'][(loan_second['businessType']=='个人助学贷款') & (loan_second['is_now']==1)].sum()#现行个人助学贷款本月应还款求和
        dict_out['stu_loan_RepayedAmount_sum_now']=loan_second['RepayedAmount'][(loan_second['businessType']=='个人助学贷款') & (loan_second['is_now']==1)].sum()#现行个人助学贷款本月实还款求和
        dict_out['stu_loan_currentOverdueAmount_sum_now']=loan_second['currentOverdueAmount'][(loan_second['businessType']=='个人助学贷款') & (loan_second['is_now']==1)].sum()#现行个人助学贷款当前逾期金额求和
        dict_out['stu_loan_overdue31Amount_sum_now']=loan_second['overdue31Amount'][(loan_second['businessType']=='个人助学贷款') & (loan_second['is_now']==1)].sum()#现行个人助学贷款逾期31-60天未还本金求和
        dict_out['stu_loan_overdue61Amount_sum_now']=loan_second['overdue61Amount'][(loan_second['businessType']=='个人助学贷款') & (loan_second['is_now']==1)].sum()#现行个人助学贷款逾期61-90天未还本金求和
        dict_out['stu_loan_overdue91Amount_sum_now']=loan_second['overdue91Amount'][(loan_second['businessType']=='个人助学贷款') & (loan_second['is_now']==1)].sum()#现行个人助学贷款逾期91-180天未还本金求和
        dict_out['stu_loan_overdue180Amount_sum_now']=loan_second['overdue180Amount'][(loan_second['businessType']=='个人助学贷款') & (loan_second['is_now']==1)].sum()#现行个人助学贷款逾期180天以上未还本金求和
        dict_out['stu_loan_overdue_month_max_now']=sum(list(map(sum,loan_second['overdue_month'][(loan_second['businessType']=='个人助学贷款') & (loan_second['is_now']==1)])))#现行个人助学贷款逾期持续月数最大
        dict_out['stu_loan_amount_max_now']=loan_second['loanAmount'][(loan_second['businessType']=='个人助学贷款') & (loan_second['is_now']==1)].max()#现行个人助学贷款本金最大
        dict_out['stu_loan_currentOverdueTerms_max_now']=loan_second['currentOverdueTerms'][(loan_second['businessType']=='个人助学贷款') & (loan_second['is_now']==1)].max()#现行个人助学贷款当前逾期期数最大
        dict_out['stu_loan_balance_max_now']=loan_second['balance'][(loan_second['businessType']=='个人助学贷款') & (loan_second['is_now']==1)].max()#现行个人助学贷款本金余额最大
        dict_out['stu_loan_planRepayAmount_max_now']=loan_second['planRepayAmount'][(loan_second['businessType']=='个人助学贷款') & (loan_second['is_now']==1)].max()#现行个人助学贷款本月应还款最大
        dict_out['stu_loan_RepayedAmount_max_now']=loan_second['RepayedAmount'][(loan_second['businessType']=='个人助学贷款') & (loan_second['is_now']==1)].max()#现行个人助学贷款本月实还款最大
        dict_out['stu_loan_currentOverdueAmount_max_now']=loan_second['currentOverdueAmount'][(loan_second['businessType']=='个人助学贷款') & (loan_second['is_now']==1)].max()#现行个人助学贷款当前逾期金额最大
        dict_out['stu_loan_overdue31Amount_max_now']=loan_second['overdue31Amount'][(loan_second['businessType']=='个人助学贷款') & (loan_second['is_now']==1)].max()#现行个人助学贷款逾期31-60天未还本金最大
        dict_out['stu_loan_overdue61Amount_max_now']=loan_second['overdue61Amount'][(loan_second['businessType']=='个人助学贷款') & (loan_second['is_now']==1)].max()#现行个人助学贷款逾期61-90天未还本金最大
        dict_out['stu_loan_overdue91Amount_max_now']=loan_second['overdue91Amount'][(loan_second['businessType']=='个人助学贷款') & (loan_second['is_now']==1)].max()#现行个人助学贷款逾期91-180天未还本金最大
        dict_out['stu_loan_overdue180Amount_max_now']=loan_second['overdue180Amount'][(loan_second['businessType']=='个人助学贷款') & (loan_second['is_now']==1)].max()#现行个人助学贷款逾期180天以上未还本金最大

        #无逾期个人助学贷款
        dict_out['stu_loan_ndue_account_count']=len(loan_second[(loan_second['businessType']=='个人助学贷款') & (loan_second['overdue_month']==0)])#无逾期个人助学贷款账户计数
        dict_out['stu_loan_ndue_amount_sum']=loan_second['loanAmount'][(loan_second['businessType']=='个人助学贷款') & (loan_second['overdue_month']==0)].sum()#无逾期个人助学贷款本金求和
        dict_out['stu_loan_ndue_amount_max']=loan_second['loanAmount'][(loan_second['businessType']=='个人助学贷款') & (loan_second['overdue_month']==0)].max()#无逾期个人助学贷款本金最大

        #现行无逾期个人助学贷款
        dict_out['stu_loan_ndue_account_count_now']=len(loan_second[(loan_second['businessType']=='个人助学贷款') & (loan_second['overdue_month']==0) & (loan_second['is_now']==1)])#现行无逾期个人助学贷款账户计数
        dict_out['stu_loan_ndue_amount_sum_now']=loan_second['loanAmount'][(loan_second['businessType']=='个人助学贷款') & (loan_second['overdue_month']==0) & (loan_second['is_now']==1)].sum()#现行无逾期个人助学贷款本金求和
        dict_out['stu_loan_ndue_balance_sum_now']=loan_second['balance'][(loan_second['businessType']=='个人助学贷款') & (loan_second['overdue_month']==0) & (loan_second['is_now']==1)].sum()#现行无逾期个人助学贷款本金余额求和
        dict_out['stu_loan_ndue_planRepayAmount_sum_now']=loan_second['planRepayAmount'][(loan_second['businessType']=='个人助学贷款') & (loan_second['overdue_month']==0) & (loan_second['is_now']==1)].sum()#现行无逾期个人助学贷款本月应还款求和
        dict_out['stu_loan_ndue_RepayedAmount_sum_now']=loan_second['RepayedAmount'][(loan_second['businessType']=='个人助学贷款') & (loan_second['overdue_month']==0) & (loan_second['is_now']==1)].sum()#现行无逾期个人助学贷款本月实还款求和
        dict_out['stu_loan_ndue_amount_max_now']=loan_second['loanAmount'][(loan_second['businessType']=='个人助学贷款') & (loan_second['overdue_month']==0) & (loan_second['is_now']==1)].max()#现行无逾期个人助学贷款本金最大
        dict_out['stu_loan_ndue_balance_max_now']=loan_second['balance'][(loan_second['businessType']=='个人助学贷款') & (loan_second['overdue_month']==0) & (loan_second['is_now']==1)].max()#现行无逾期个人助学贷款本金余额最大
        dict_out['stu_loan_ndue_planRepayAmount_max_now']=loan_second['planRepayAmount'][(loan_second['businessType']=='个人助学贷款') & (loan_second['overdue_month']==0) & (loan_second['is_now']==1)].max()#现行无逾期个人助学贷款本月应还款最大
        dict_out['stu_loan_ndue_RepayedAmount_max_now']=loan_second['RepayedAmount'][(loan_second['businessType']=='个人助学贷款') & (loan_second['overdue_month']==0) & (loan_second['is_now']==1)].max()#现行无逾期个人助学贷款本月实还款最大

        #轻度逾期个人助学贷款
        dict_out['stu_loan_slight_overdue_account_count']=len(loan_second[(loan_second['businessType']=='个人助学贷款') & (loan_second['overdue_class']==1)])#轻度逾期个人助学贷款账户计数
        dict_out['stu_loan_slight_overdue_month_sum']=sum(list(map(sum,loan_second['overdue_month'][(loan_second['businessType']=='个人助学贷款') & (loan_second['overdue_class']==1)])))#轻度逾期个人助学贷款逾期月数求和
        dict_out['stu_loan_slight_overdue_amount_sum']=loan_second['loanAmount'][(loan_second['businessType']=='个人助学贷款') & (loan_second['overdue_class']==1)].sum()#轻度逾期个人助学贷款本金求和
        dict_out['stu_loan_slight_overdue_amount_max']=loan_second['loanAmount'][(loan_second['businessType']=='个人助学贷款') & (loan_second['overdue_class']==1)].max()#轻度逾期个人助学贷款本金最大

        #现行轻度逾期个人助学贷款
        dict_out['stu_loan_slight_overdue_account_count_now']=len(loan_second[(loan_second['businessType']=='个人助学贷款') & (loan_second['overdue_class']==1) & (loan_second['is_now']==1)])#现行轻度逾期个人助学贷款账户计数
        dict_out['stu_loan_slight_overdue_month_sum_now']=sum(list(map(sum,loan_second['overdue_month'][(loan_second['businessType']=='个人助学贷款') & (loan_second['overdue_class']==1) & (loan_second['is_now']==1)])))#现行轻度逾期个人助学贷款逾期月数求和
        dict_out['stu_loan_slight_overdue_amount_sum_now']=loan_second['loanAmount'][(loan_second['businessType']=='个人助学贷款') & (loan_second['overdue_class']==1) & (loan_second['is_now']==1)].sum()#现行轻度逾期个人助学贷款本金求和
        dict_out['stu_loan_slight_overdue_balance_sum_now']=loan_second['balance'][(loan_second['businessType']=='个人助学贷款') & (loan_second['overdue_class']==1) & (loan_second['is_now']==1)].sum()#现行轻度逾期个人助学贷款本金余额求和
        dict_out['stu_loan_slight_overdue_planRepayAmount_sum_now']=loan_second['planRepayAmount'][(loan_second['businessType']=='个人助学贷款') & (loan_second['overdue_class']==1) & (loan_second['is_now']==1)].sum()#现行轻度逾期个人助学贷款本月应还款求和
        dict_out['stu_loan_slight_overdue_RepayedAmount_sum_now']=loan_second['RepayedAmount'][(loan_second['businessType']=='个人助学贷款') & (loan_second['overdue_class']==1) & (loan_second['is_now']==1)].sum()#现行轻度逾期个人助学贷款本月实还款求和
        dict_out['stu_loan_slight_overdue_currentOverdueAmount_sum_now']=loan_second['currentOverdueAmount'][(loan_second['businessType']=='个人助学贷款') & (loan_second['overdue_class']==1) & (loan_second['is_now']==1)].sum()#现行轻度逾期个人助学贷款当前逾期金额求和
        dict_out['stu_loan_slight_overdue_amount_max_now']=loan_second['loanAmount'][(loan_second['businessType']=='个人助学贷款') & (loan_second['overdue_class']==1) & (loan_second['is_now']==1)].max()#现行轻度逾期个人助学贷款本金最大
        dict_out['stu_loan_slight_overdue_balance_max_now']=loan_second['balance'][(loan_second['businessType']=='个人助学贷款') & (loan_second['overdue_class']==1) & (loan_second['is_now']==1)].max()#现行轻度逾期个人助学贷款本金余额最大
        dict_out['stu_loan_slight_overdue_planRepayAmount_max_now']=loan_second['planRepayAmount'][(loan_second['businessType']=='个人助学贷款') & (loan_second['overdue_class']==1) & (loan_second['is_now']==1)].max()#现行轻度逾期个人助学贷款本月应还款最大
        dict_out['stu_loan_slight_overdue_RepayedAmount_max_now']=loan_second['RepayedAmount'][(loan_second['businessType']=='个人助学贷款') & (loan_second['overdue_class']==1) & (loan_second['is_now']==1)].max()#现行轻度逾期个人助学贷款本月实还款最大
        dict_out['stu_loan_slight_overdue_currentOverdueAmount_max_now']=loan_second['currentOverdueAmount'][(loan_second['businessType']=='个人助学贷款') & (loan_second['overdue_class']==1) & (loan_second['is_now']==1)].max()#现行轻度逾期个人助学贷款当前逾期金额最大

        #个人经营性贷款
        dict_out['business_loan_account_count']=len(loan_second[loan_second['businessType']=='个人经营性贷款'])#个人经营性贷款账户计数
        dict_out['business_loan_overdue_month_sum']=sum(list(map(sum,loan_second['overdue_month'][loan_second['businessType']=='个人经营性贷款'])))#个人经营性贷款逾期月数求和
        dict_out['business_loan_amount_sum']=loan_second['loanAmount'][loan_second['businessType']=='个人经营性贷款'].sum()#个人经营性贷款本金求和
        dict_out['business_loan_overdue_month_max']=loan_second['overdue_month_max'][loan_second['businessType']=='个人经营性贷款'].max()#个人经营性贷款逾期持续月数最大
        dict_out['business_loan_amount_max']=loan_second['loanAmount'][loan_second['businessType']=='个人经营性贷款'].max()#个人经营性贷款本金最大
        dict_out['business_loan_rating_worst']={'1.0':'损失', '2.0':'可疑', '3.0':'关注', '4.0':'次级', '5.0':'正常','nan':"无"}[str(loan_second['classify5_num'][loan_second['businessType']=='个人经营性贷款'].min())]#个人经营性贷款Rating最差

        #现行个人经营性贷款
        dict_out['business_loan_account_count_now']=len(loan_second[(loan_second['businessType']=='个人经营性贷款') & (loan_second['is_now']==1)])#现行个人经营性贷款账户计数
        dict_out['business_loan_overdue_month_sum_now']=sum(list(map(sum,loan_second['overdue_month'][(loan_second['businessType']=='个人经营性贷款') & (loan_second['is_now']==1)])))#现行个人经营性贷款逾期月数求和
        dict_out['business_loan_amount_sum_now']=loan_second['loanAmount'][(loan_second['businessType']=='个人经营性贷款') & (loan_second['is_now']==1)].sum()#现行个人经营性贷款本金求和
        dict_out['business_loan_currentOverdueTerms_sum_now']=loan_second['currentOverdueTerms'][(loan_second['businessType']=='个人经营性贷款') & (loan_second['is_now']==1)].sum()#现行个人经营性贷款当前逾期期数求和
        dict_out['business_loan_balance_sum_now']=loan_second['balance'][(loan_second['businessType']=='个人经营性贷款') & (loan_second['is_now']==1)].sum()#现行个人经营性贷款本金余额求和
        dict_out['business_loan_planRepayAmount_sum_now']=loan_second['planRepayAmount'][(loan_second['businessType']=='个人经营性贷款') & (loan_second['is_now']==1)].sum()#现行个人经营性贷款本月应还款求和
        dict_out['business_loan_RepayedAmount_sum_now']=loan_second['RepayedAmount'][(loan_second['businessType']=='个人经营性贷款') & (loan_second['is_now']==1)].sum()#现行个人经营性贷款本月实还款求和
        dict_out['business_loan_currentOverdueAmount_sum_now']=loan_second['currentOverdueAmount'][(loan_second['businessType']=='个人经营性贷款') & (loan_second['is_now']==1)].sum()#现行个人经营性贷款当前逾期金额求和
        dict_out['business_loan_overdue31Amount_sum_now']=loan_second['overdue31Amount'][(loan_second['businessType']=='个人经营性贷款') & (loan_second['is_now']==1)].sum()#现行个人经营性贷款逾期31-60天未还本金求和
        dict_out['business_loan_overdue61Amount_sum_now']=loan_second['overdue61Amount'][(loan_second['businessType']=='个人经营性贷款') & (loan_second['is_now']==1)].sum()#现行个人经营性贷款逾期61-90天未还本金求和
        dict_out['business_loan_overdue91Amount_sum_now']=loan_second['overdue91Amount'][(loan_second['businessType']=='个人经营性贷款') & (loan_second['is_now']==1)].sum()#现行个人经营性贷款逾期91-180天未还本金求和
        dict_out['business_loan_overdue180Amount_sum_now']=loan_second['overdue180Amount'][(loan_second['businessType']=='个人经营性贷款') & (loan_second['is_now']==1)].sum()#现行个人经营性贷款逾期180天以上未还本金求和
        dict_out['business_loan_overdue_month_max_now']=loan_second['overdue_month_max'][(loan_second['businessType']=='个人经营性贷款') & (loan_second['is_now']==1)].max()#现行个人经营性贷款逾期持续月数最大
        dict_out['business_loan_amount_max_now']=loan_second['loanAmount'][(loan_second['businessType']=='个人经营性贷款') & (loan_second['is_now']==1)].max()#现行个人经营性贷款本金最大
        dict_out['business_loan_currentOverdueTerms_max_now']=loan_second['currentOverdueTerms'][(loan_second['businessType']=='个人经营性贷款') & (loan_second['is_now']==1)].max()#现行个人经营性贷款当前逾期期数最大
        dict_out['business_loan_balance_max_now']=loan_second['balance'][(loan_second['businessType']=='个人经营性贷款') & (loan_second['is_now']==1)].max()#现行个人经营性贷款本金余额最大
        dict_out['business_loan_planRepayAmount_max_now']=loan_second['planRepayAmount'][(loan_second['businessType']=='个人经营性贷款') & (loan_second['is_now']==1)].max()#现行个人经营性贷款本月应还款最大
        dict_out['business_loan_RepayedAmount_max_now']=loan_second['RepayedAmount'][(loan_second['businessType']=='个人经营性贷款') & (loan_second['is_now']==1)].max()#现行个人经营性贷款本月实还款最大
        dict_out['business_loan_currentOverdueAmount_max_now']=loan_second['currentOverdueAmount'][(loan_second['businessType']=='个人经营性贷款') & (loan_second['is_now']==1)].max()#现行个人经营性贷款当前逾期金额最大
        dict_out['business_loan_overdue31Amount_max_now']=loan_second['overdue31Amount'][(loan_second['businessType']=='个人经营性贷款') & (loan_second['is_now']==1)].max()#现行个人经营性贷款逾期31-60天未还本金最大
        dict_out['business_loan_overdue61Amount_max_now']=loan_second['overdue61Amount'][(loan_second['businessType']=='个人经营性贷款') & (loan_second['is_now']==1)].max()#现行个人经营性贷款逾期61-90天未还本金最大
        dict_out['business_loan_overdue91Amount_max_now']=loan_second['overdue91Amount'][(loan_second['businessType']=='个人经营性贷款') & (loan_second['is_now']==1)].max()#现行个人经营性贷款逾期91-180天未还本金最大
        dict_out['business_loan_overdue180Amount_max_now']=loan_second['overdue180Amount'][(loan_second['businessType']=='个人经营性贷款') & (loan_second['is_now']==1)].max()#现行个人经营性贷款逾期180天以上未还本金最大

        #有担保个人经营性贷款
        dict_out['business_loan_is_vouch_account_count']=len(loan_second[(loan_second['businessType']=='个人经营性贷款') & (loan_second['is_vouch']==1)])#有担保个人经营性贷款账户计数
        dict_out['business_loan_is_vouch_overdue_month_sum']=sum(list(map(sum,loan_second['overdue_month'][(loan_second['businessType']=='个人经营性贷款') & (loan_second['is_vouch']==1)])))#有担保个人经营性贷款逾期月数求和
        dict_out['business_loan_is_vouch_amount_sum']=loan_second['loanAmount'][(loan_second['businessType']=='个人经营性贷款') & (loan_second['is_vouch']==1)].sum()#有担保个人经营性贷款本金求和
        dict_out['business_loan_is_vouch_overdue_month_max']=loan_second['overdue_month_max'][(loan_second['businessType']=='个人经营性贷款') & (loan_second['is_vouch']==1)].max()#有担保个人经营性贷款逾期持续月数最大
        dict_out['business_loan_is_vouch_amount_max']=loan_second['loanAmount'][(loan_second['businessType']=='个人经营性贷款') & (loan_second['is_vouch']==1)].max()#有担保个人经营性贷款本金最大
        dict_out['business_loan_is_vouch_rating_worst']={'1.0':'损失', '2.0':'可疑', '3.0':'关注', '4.0':'次级', '5.0':'正常','nan':"无"}[str(loan_second['classify5_num'][(loan_second['businessType']=='个人经营性贷款') & (loan_second['is_vouch']==1)].min())]#有担保个人经营性贷款Rating最差

        #现行有担保个人经营性贷款
        dict_out['business_loan_is_vouch_account_count_now']=len(loan_second[(loan_second['businessType']=='个人经营性贷款') & (loan_second['is_vouch']==1) & (loan_second['is_now']==1)])#现行有担保个人经营性贷款账户计数
        dict_out['business_loan_is_vouch_overdue_month_sum_now']=sum(list(map(sum,loan_second['overdue_month'][(loan_second['businessType']=='个人经营性贷款') & (loan_second['is_vouch']==1) & (loan_second['is_now']==1)])))#现行有担保个人经营性贷款逾期月数求和
        dict_out['business_loan_is_vouch_amount_sum_now']=loan_second['loanAmount'][(loan_second['businessType']=='个人经营性贷款') & (loan_second['is_vouch']==1) & (loan_second['is_now']==1)].sum()#现行有担保个人经营性贷款本金求和
        dict_out['business_loan_is_vouch_currentOverdueTerms_sum_now']=loan_second['currentOverdueTerms'][(loan_second['businessType']=='个人经营性贷款') & (loan_second['is_vouch']==1) & (loan_second['is_now']==1)].sum()#现行有担保个人经营性贷款当前逾期期数求和
        dict_out['business_loan_is_vouch_balance_sum_now']=loan_second['balance'][(loan_second['businessType']=='个人经营性贷款') & (loan_second['is_vouch']==1) & (loan_second['is_now']==1)].sum()#现行有担保个人经营性贷款本金余额求和
        dict_out['business_loan_is_vouch_planRepayAmount_sum_now']=loan_second['planRepayAmount'][(loan_second['businessType']=='个人经营性贷款') & (loan_second['is_vouch']==1) & (loan_second['is_now']==1)].sum()#现行有担保个人经营性贷款本月应还款求和
        dict_out['business_loan_is_vouch_RepayedAmount_sum_now']=loan_second['RepayedAmount'][(loan_second['businessType']=='个人经营性贷款') & (loan_second['is_vouch']==1) & (loan_second['is_now']==1)].sum()#现行有担保个人经营性贷款本月实还款求和
        dict_out['business_loan_is_vouch_currentOverdueAmount_sum_now']=loan_second['currentOverdueAmount'][(loan_second['businessType']=='个人经营性贷款') & (loan_second['is_vouch']==1) & (loan_second['is_now']==1)].sum()#现行有担保个人经营性贷款当前逾期金额求和
        dict_out['business_loan_is_vouch_overdue31Amount_sum_now']=loan_second['overdue31Amount'][(loan_second['businessType']=='个人经营性贷款') & (loan_second['is_vouch']==1) & (loan_second['is_now']==1)].sum()#现行有担保个人经营性贷款逾期31-60天未还本金求和
        dict_out['business_loan_is_vouch_overdue61Amount_sum_now']=loan_second['overdue61Amount'][(loan_second['businessType']=='个人经营性贷款') & (loan_second['is_vouch']==1) & (loan_second['is_now']==1)].sum()#现行有担保个人经营性贷款逾期61-90天未还本金求和
        dict_out['business_loan_is_vouch_overdue91Amount_sum_now']=loan_second['overdue91Amount'][(loan_second['businessType']=='个人经营性贷款') & (loan_second['is_vouch']==1) & (loan_second['is_now']==1)].sum()#现行有担保个人经营性贷款逾期91-180天未还本金求和
        dict_out['business_loan_is_vouch_overdue180Amount_sum_now']=loan_second['overdue180Amount'][(loan_second['businessType']=='个人经营性贷款') & (loan_second['is_vouch']==1) & (loan_second['is_now']==1)].sum()#现行有担保个人经营性贷款逾期180天以上未还本金求和
        dict_out['business_loan_is_vouch_overdue_month_max_now']=loan_second['overdue_month_max'][(loan_second['businessType']=='个人经营性贷款') & (loan_second['is_vouch']==1) & (loan_second['is_now']==1)].max()#现行有担保个人经营性贷款逾期持续月数最大
        dict_out['business_loan_is_vouch_amount_max_now']=loan_second['loanAmount'][(loan_second['businessType']=='个人经营性贷款') & (loan_second['is_vouch']==1) & (loan_second['is_now']==1)].max()#现行有担保个人经营性贷款本金最大
        dict_out['business_loan_is_vouch_currentOverdueTerms_max_now']=loan_second['currentOverdueTerms'][(loan_second['businessType']=='个人经营性贷款') & (loan_second['is_vouch']==1) & (loan_second['is_now']==1)].max()#现行有担保个人经营性贷款当前逾期期数最大
        dict_out['business_loan_is_vouch_balance_max_now']=loan_second['balance'][(loan_second['businessType']=='个人经营性贷款') & (loan_second['is_vouch']==1) & (loan_second['is_now']==1)].max()#现行有担保个人经营性贷款本金余额最大
        dict_out['business_loan_is_vouch_planRepayAmount_max_now']=loan_second['planRepayAmount'][(loan_second['businessType']=='个人经营性贷款') & (loan_second['is_vouch']==1) & (loan_second['is_now']==1)].max()#现行有担保个人经营性贷款本月应还款最大
        dict_out['business_loan_is_vouch_RepayedAmount_max_now']=loan_second['RepayedAmount'][(loan_second['businessType']=='个人经营性贷款') & (loan_second['is_vouch']==1) & (loan_second['is_now']==1)].max()#现行有担保个人经营性贷款本月实还款最大
        dict_out['business_loan_is_vouch_currentOverdueAmount_max_now']=loan_second['currentOverdueAmount'][(loan_second['businessType']=='个人经营性贷款') & (loan_second['is_vouch']==1) & (loan_second['is_now']==1)].max()#现行有担保个人经营性贷款当前逾期金额最大
        dict_out['business_loan_is_vouch_overdue31Amount_max_now']=loan_second['overdue31Amount'][(loan_second['businessType']=='个人经营性贷款') & (loan_second['is_vouch']==1) & (loan_second['is_now']==1)].max()#现行有担保个人经营性贷款逾期31-60天未还本金最大
        dict_out['business_loan_is_vouch_overdue61Amount_max_now']=loan_second['overdue61Amount'][(loan_second['businessType']=='个人经营性贷款') & (loan_second['is_vouch']==1) & (loan_second['is_now']==1)].max()#现行有担保个人经营性贷款逾期61-90天未还本金最大
        dict_out['business_loan_is_vouch_overdue91Amount_max_now']=loan_second['overdue91Amount'][(loan_second['businessType']=='个人经营性贷款') & (loan_second['is_vouch']==1) & (loan_second['is_now']==1)].max()#现行有担保个人经营性贷款逾期91-180天未还本金最大
        dict_out['business_loan_is_vouch_overdue180Amount_max_now']=loan_second['overdue180Amount'][(loan_second['businessType']=='个人经营性贷款') & (loan_second['is_vouch']==1) & (loan_second['is_now']==1)].max()#现行有担保个人经营性贷款逾期180天以上未还本金最大

        #最近3个月个人经营性贷款
        dict_out['business_loan_overdue_month_sum_03m']=sum(list(map(sum,loan_second['overdue_month'][(loan_second['businessType']=='个人经营性贷款') & (loan_second['month_Desc_report']<3)])))#最近3个月个人经营性贷款逾期月数求和
        dict_out['business_loan_overdue_month_max_03m']=loan_second['overdue_month_max'][(loan_second['businessType']=='个人经营性贷款') & (loan_second['month_Desc_report']<3)].max()#最近3个月个人经营性贷款逾期持续月数最大

        #现行最近3个月个人经营性贷款
        dict_out['business_loan_overdue_month_sum_03m_now']=sum(list(map(sum,loan_second['overdue_month'][(loan_second['businessType']=='个人经营性贷款') & (loan_second['month_Desc_report']<3) & (loan_second['is_now']==1)])))#现行最近3个月个人经营性贷款逾期月数求和
        dict_out['business_loan_overdue_month_max_03m_now']=loan_second['overdue_month_max'][(loan_second['businessType']=='个人经营性贷款') & (loan_second['month_Desc_report']<3) & (loan_second['is_now']==1)].max()#现行最近3个月个人经营性贷款逾期持续月数最大

        #最近3个月有担保个人经营性贷款
        dict_out['business_loan_is_vouch_overdue_month_sum_03m']=sum(list(map(sum,loan_second['overdue_month'][(loan_second['businessType']=='个人经营性贷款') & (loan_second['month_Desc_report']<3) & (loan_second['is_vouch']==1)])))#最近3个月有担保个人经营性贷款逾期月数求和
        dict_out['business_loan_is_vouch_overdue_month_max_03m']=loan_second['overdue_month_max'][(loan_second['businessType']=='个人经营性贷款') & (loan_second['month_Desc_report']<3) & (loan_second['is_vouch']==1)].max()#最近3个月有担保个人经营性贷款逾期持续月数最大

        #现行最近3个月有担保个人经营性贷款
        dict_out['business_loan_is_vouch_overdue_month_sum_03m_now']=sum(list(map(sum,loan_second['overdue_month'][(loan_second['businessType']=='个人经营性贷款') & (loan_second['month_Desc_report']<3) & (loan_second['is_vouch']==1) & (loan_second['is_now']==1)])))#现行最近3个月有担保个人经营性贷款逾期月数求和
        dict_out['business_loan_is_vouch_overdue_month_max_03m_now']=loan_second['overdue_month_max'][(loan_second['businessType']=='个人经营性贷款') & (loan_second['month_Desc_report']<3) & (loan_second['is_vouch']==1) & (loan_second['is_now']==1)].max()#现行最近3个月有担保个人经营性贷款逾期持续月数最大

        #最近6个月个人经营性贷款
        dict_out['business_loan_overdue_month_sum_06m']=sum(list(map(sum,loan_second['overdue_month'][(loan_second['businessType']=='个人经营性贷款') & (loan_second['month_Desc_report']<6)])))#最近6个月个人经营性贷款逾期月数求和
        dict_out['business_loan_overdue_month_max_06m']=loan_second['overdue_month_max'][(loan_second['businessType']=='个人经营性贷款') & (loan_second['month_Desc_report']<6)].max()#最近6个月个人经营性贷款逾期持续月数最大

        #现行最近6个月个人经营性贷款
        dict_out['business_loan_overdue_month_sum_06m_now']=sum(list(map(sum,loan_second['overdue_month'][(loan_second['businessType']=='个人经营性贷款') & (loan_second['month_Desc_report']<6) & (loan_second['is_now']==1)])))#现行最近6个月个人经营性贷款逾期月数求和
        dict_out['business_loan_overdue_month_max_06m_now']=loan_second['overdue_month_max'][(loan_second['businessType']=='个人经营性贷款') & (loan_second['month_Desc_report']<6) & (loan_second['is_now']==1)].max()#现行最近6个月个人经营性贷款逾期持续月数最大

        #最近6个月有担保个人经营性贷款
        dict_out['business_loan_is_vouch_overdue_month_sum_06m']=sum(list(map(sum,loan_second['overdue_month'][(loan_second['businessType']=='个人经营性贷款') & (loan_second['month_Desc_report']<6) & (loan_second['is_vouch']==1)])))#最近6个月有担保个人经营性贷款逾期月数求和
        dict_out['business_loan_is_vouch_overdue_month_max_06m']=loan_second['overdue_month_max'][(loan_second['businessType']=='个人经营性贷款') & (loan_second['month_Desc_report']<6) & (loan_second['is_vouch']==1)].max()#最近6个月有担保个人经营性贷款逾期持续月数最大

        #现行最近6个月有担保个人经营性贷款
        dict_out['business_loan_is_vouch_overdue_month_sum_06m_now']=sum(list(map(sum,loan_second['overdue_month'][(loan_second['businessType']=='个人经营性贷款') & (loan_second['month_Desc_report']<6) & (loan_second['is_vouch']==1) & (loan_second['is_now']==1)])))#现行最近6个月有担保个人经营性贷款逾期月数求和
        dict_out['business_loan_is_vouch_overdue_month_max_06m_now']=loan_second['overdue_month_max'][(loan_second['businessType']=='个人经营性贷款') & (loan_second['month_Desc_report']<6) & (loan_second['is_vouch']==1) & (loan_second['is_now']==1)].max()#现行最近6个月有担保个人经营性贷款逾期持续月数最大

        #最近12个月个人经营性贷款
        dict_out['business_loan_overdue_month_sum_12m']=sum(list(map(sum,loan_second['overdue_month'][(loan_second['businessType']=='个人经营性贷款') & (loan_second['month_Desc_report']<12)])))#最近12个月个人经营性贷款逾期月数求和
        dict_out['business_loan_overdue_month_max_12m']=loan_second['overdue_month_max'][(loan_second['businessType']=='个人经营性贷款') & (loan_second['month_Desc_report']<12)].max()#最近12个月个人经营性贷款逾期持续月数最大

        #现行最近12个月个人经营性贷款
        dict_out['business_loan_overdue_month_sum_12m_now']=sum(list(map(sum,loan_second['overdue_month'][(loan_second['businessType']=='个人经营性贷款') & (loan_second['month_Desc_report']<12) & (loan_second['is_now']==1)])))#现行最近12个月个人经营性贷款逾期月数求和
        dict_out['business_loan_overdue_month_max_12m_now']=loan_second['overdue_month_max'][(loan_second['businessType']=='个人经营性贷款') & (loan_second['month_Desc_report']<12) & (loan_second['is_now']==1)].max()#现行最近12个月个人经营性贷款逾期持续月数最大

        #最近12个月有担保个人经营性贷款
        dict_out['business_loan_is_vouch_overdue_month_sum_12m']=sum(list(map(sum,loan_second['overdue_month'][(loan_second['businessType']=='个人经营性贷款') & (loan_second['month_Desc_report']<12) & (loan_second['is_vouch']==1)])))#最近12个月有担保个人经营性贷款逾期月数求和
        dict_out['business_loan_is_vouch_overdue_month_max_12m']=loan_second['overdue_month_max'][(loan_second['businessType']=='个人经营性贷款') & (loan_second['month_Desc_report']<12) & (loan_second['is_vouch']==1)].max()#最近12个月有担保个人经营性贷款逾期持续月数最大

        #现行最近12个月有担保个人经营性贷款
        dict_out['business_loan_is_vouch_overdue_month_sum_12m_now']=sum(list(map(sum,loan_second['overdue_month'][(loan_second['businessType']=='个人经营性贷款') & (loan_second['month_Desc_report']<12) & (loan_second['is_vouch']==1) & (loan_second['is_now']==1)])))#现行最近12个月有担保个人经营性贷款逾期月数求和
        dict_out['business_loan_is_vouch_overdue_month_max_12m_now']=loan_second['overdue_month_max'][(loan_second['businessType']=='个人经营性贷款') & (loan_second['month_Desc_report']<12) & (loan_second['is_vouch']==1) & (loan_second['is_now']==1)].max()#现行最近12个月有担保个人经营性贷款逾期持续月数最大

        #最近24个月个人经营性贷款
        dict_out['business_loan_overdue_month_sum_24m']=sum(list(map(sum,loan_second['overdue_month'][(loan_second['businessType']=='个人经营性贷款') & (loan_second['month_Desc_report']<24)])))#最近24个月个人经营性贷款逾期月数求和
        dict_out['business_loan_overdue_month_max_24m']=loan_second['overdue_month_max'][(loan_second['businessType']=='个人经营性贷款') & (loan_second['month_Desc_report']<24)].max()#最近24个月个人经营性贷款逾期持续月数最大

        #现行最近24个月个人经营性贷款
        dict_out['business_loan_overdue_month_sum_24m_now']=sum(list(map(sum,loan_second['overdue_month'][(loan_second['businessType']=='个人经营性贷款') & (loan_second['month_Desc_report']<24) & (loan_second['is_now']==1)])))#现行最近24个月个人经营性贷款逾期月数求和
        dict_out['business_loan_overdue_month_max_24m_now']=loan_second['overdue_month_max'][(loan_second['businessType']=='个人经营性贷款') & (loan_second['month_Desc_report']<24) & (loan_second['is_now']==1)].max()#现行最近24个月个人经营性贷款逾期持续月数最大

        #最近24个月有担保个人经营性贷款
        dict_out['business_loan_is_vouch_overdue_month_sum_24m']=sum(list(map(sum,loan_second['overdue_month'][(loan_second['businessType']=='个人经营性贷款') & (loan_second['month_Desc_report']<24) & (loan_second['is_vouch']==1)])))#最近24个月有担保个人经营性贷款逾期月数求和
        dict_out['business_loan_is_vouch_overdue_month_max_24m']=loan_second['overdue_month_max'][(loan_second['businessType']=='个人经营性贷款') & (loan_second['month_Desc_report']<24) & (loan_second['is_vouch']==1)].max()#最近24个月有担保个人经营性贷款逾期持续月数最大

        #现行最近24个月有担保个人经营性贷款
        dict_out['business_loan_is_vouch_overdue_month_sum_24m_now']=sum(list(map(sum,loan_second['overdue_month'][(loan_second['businessType']=='个人经营性贷款') & (loan_second['month_Desc_report']<24) & (loan_second['is_vouch']==1) & (loan_second['is_now']==1)])))#现行最近24个月有担保个人经营性贷款逾期月数求和
        dict_out['business_loan_is_vouch_overdue_month_max_24m_now']=loan_second['overdue_month_max'][(loan_second['businessType']=='个人经营性贷款') & (loan_second['month_Desc_report']<24) & (loan_second['is_vouch']==1) & (loan_second['is_now']==1)].max()#现行最近24个月有担保个人经营性贷款逾期持续月数最大

        #无逾期个人经营性贷款
        dict_out['business_loan_ndue_account_count']=len(loan_second[(loan_second['businessType']=='个人经营性贷款') & (loan_second['overdue_month']==0)])#无逾期个人经营性贷款账户计数
        dict_out['business_loan_ndue_amount_sum']=loan_second['loanAmount'][(loan_second['businessType']=='个人经营性贷款') & (loan_second['overdue_month']==0)].sum()#无逾期个人经营性贷款本金求和
        dict_out['business_loan_ndue_amount_max']=loan_second['loanAmount'][(loan_second['businessType']=='个人经营性贷款') & (loan_second['overdue_month']==0)].max()#无逾期个人经营性贷款本金最大

        #现行无逾期个人经营性贷款
        dict_out['business_loan_ndue_account_count_now']=len(loan_second[(loan_second['businessType']=='个人经营性贷款') & (loan_second['overdue_month']==0) & (loan_second['is_now']==1)])#现行无逾期个人经营性贷款账户计数
        dict_out['business_loan_ndue_amount_sum_now']=loan_second['loanAmount'][(loan_second['businessType']=='个人经营性贷款') & (loan_second['overdue_month']==0) & (loan_second['is_now']==1)].sum()#现行无逾期个人经营性贷款本金求和
        dict_out['business_loan_ndue_balance_sum_now']=loan_second['balance'][(loan_second['businessType']=='个人经营性贷款') & (loan_second['overdue_month']==0) & (loan_second['is_now']==1)].sum()#现行无逾期个人经营性贷款本金余额求和
        dict_out['business_loan_ndue_planRepayAmount_sum_now']=loan_second['planRepayAmount'][(loan_second['businessType']=='个人经营性贷款') & (loan_second['overdue_month']==0) & (loan_second['is_now']==1)].sum()#现行无逾期个人经营性贷款本月应还款求和
        dict_out['business_loan_ndue_RepayedAmount_sum_now']=loan_second['RepayedAmount'][(loan_second['businessType']=='个人经营性贷款') & (loan_second['overdue_month']==0) & (loan_second['is_now']==1)].sum()#现行无逾期个人经营性贷款本月实还款求和
        dict_out['business_loan_ndue_amount_max_now']=loan_second['loanAmount'][(loan_second['businessType']=='个人经营性贷款') & (loan_second['overdue_month']==0) & (loan_second['is_now']==1)].max()#现行无逾期个人经营性贷款本金最大
        dict_out['business_loan_ndue_balance_max_now']=loan_second['balance'][(loan_second['businessType']=='个人经营性贷款') & (loan_second['overdue_month']==0) & (loan_second['is_now']==1)].max()#现行无逾期个人经营性贷款本金余额最大
        dict_out['business_loan_ndue_planRepayAmount_max_now']=loan_second['planRepayAmount'][(loan_second['businessType']=='个人经营性贷款') & (loan_second['overdue_month']==0) & (loan_second['is_now']==1)].max()#现行无逾期个人经营性贷款本月应还款最大
        dict_out['business_loan_ndue_RepayedAmount_max_now']=loan_second['RepayedAmount'][(loan_second['businessType']=='个人经营性贷款') & (loan_second['overdue_month']==0) & (loan_second['is_now']==1)].max()#现行无逾期个人经营性贷款本月实还款最大

        #无逾期有担保个人经营性贷款
        dict_out['business_loan_is_vouch_ndue_account_count']=len(loan_second[(loan_second['businessType']=='个人经营性贷款') & (loan_second['overdue_month']==0) & (loan_second['is_vouch']==1)])#无逾期有担保个人经营性贷款账户计数
        dict_out['business_loan_is_vouch_ndue_amount_sum']=loan_second['loanAmount'][(loan_second['businessType']=='个人经营性贷款') & (loan_second['overdue_month']==0) & (loan_second['is_vouch']==1)].sum()#无逾期有担保个人经营性贷款本金求和
        dict_out['business_loan_is_vouch_ndue_amount_max']=loan_second['loanAmount'][(loan_second['businessType']=='个人经营性贷款') & (loan_second['overdue_month']==0) & (loan_second['is_vouch']==1)].max()#无逾期有担保个人经营性贷款本金最大

        #现行无逾期有担保个人经营性贷款
        dict_out['business_loan_is_vouch_ndue_account_count_now']=len(loan_second[(loan_second['businessType']=='个人经营性贷款') & (loan_second['overdue_month']==0) & (loan_second['is_vouch']==1) & (loan_second['is_now']==1)])#现行无逾期有担保个人经营性贷款账户计数
        dict_out['business_loan_is_vouch_ndue_amount_sum_now']=loan_second['loanAmount'][(loan_second['businessType']=='个人经营性贷款') & (loan_second['overdue_month']==0) & (loan_second['is_vouch']==1) & (loan_second['is_now']==1)].sum()#现行无逾期有担保个人经营性贷款本金求和
        dict_out['business_loan_is_vouch_ndue_balance_sum_now']=loan_second['balance'][(loan_second['businessType']=='个人经营性贷款') & (loan_second['overdue_month']==0) & (loan_second['is_vouch']==1) & (loan_second['is_now']==1)].sum()#现行无逾期有担保个人经营性贷款本金余额求和
        dict_out['business_loan_is_vouch_ndue_planRepayAmount_sum_now']=loan_second['planRepayAmount'][(loan_second['businessType']=='个人经营性贷款') & (loan_second['overdue_month']==0) & (loan_second['is_vouch']==1) & (loan_second['is_now']==1)].sum()#现行无逾期有担保个人经营性贷款本月应还款求和
        dict_out['business_loan_is_vouch_ndue_RepayedAmount_sum_now']=loan_second['RepayedAmount'][(loan_second['businessType']=='个人经营性贷款') & (loan_second['overdue_month']==0) & (loan_second['is_vouch']==1) & (loan_second['is_now']==1)].sum()#现行无逾期有担保个人经营性贷款本月实还款求和
        dict_out['business_loan_is_vouch_ndue_amount_max_now']=loan_second['loanAmount'][(loan_second['businessType']=='个人经营性贷款') & (loan_second['overdue_month']==0) & (loan_second['is_vouch']==1) & (loan_second['is_now']==1)].max()#现行无逾期有担保个人经营性贷款本金最大
        dict_out['business_loan_is_vouch_ndue_balance_max_now']=loan_second['balance'][(loan_second['businessType']=='个人经营性贷款') & (loan_second['overdue_month']==0) & (loan_second['is_vouch']==1) & (loan_second['is_now']==1)].max()#现行无逾期有担保个人经营性贷款本金余额最大
        dict_out['business_loan_is_vouch_ndue_planRepayAmount_max_now']=loan_second['planRepayAmount'][(loan_second['businessType']=='个人经营性贷款') & (loan_second['overdue_month']==0) & (loan_second['is_vouch']==1) & (loan_second['is_now']==1)].max()#现行无逾期有担保个人经营性贷款本月应还款最大
        dict_out['business_loan_is_vouch_ndue_RepayedAmount_max_now']=loan_second['RepayedAmount'][(loan_second['businessType']=='个人经营性贷款') & (loan_second['overdue_month']==0) & (loan_second['is_vouch']==1) & (loan_second['is_now']==1)].max()#现行无逾期有担保个人经营性贷款本月实还款最大

        #最近3个月无逾期个人经营性贷款
        dict_out['business_loan_ndue_account_count_03m']=len(loan_second[(loan_second['businessType']=='个人经营性贷款') & (loan_second['overdue_month']==0) & (loan_second['month_Desc_report']<3)])#最近3个月无逾期个人经营性贷款账户计数

        #现行最近3个月无逾期个人经营性贷款
        dict_out['business_loan_ndue_account_count_03m_now']=len(loan_second[(loan_second['businessType']=='个人经营性贷款') & (loan_second['overdue_month']==0) & (loan_second['is_now']==1) & (loan_second['month_Desc_report']<3)])#现行最近3个月无逾期个人经营性贷款账户计数
        dict_out['business_loan_ndue_planRepayAmount_sum_03m_now']=loan_second['planRepayAmount'][(loan_second['businessType']=='个人经营性贷款') & (loan_second['overdue_month']==0) & (loan_second['is_now']==1) & (loan_second['month_Desc_report']<3)].sum()#现行最近3个月无逾期个人经营性贷款本月应还款求和
        dict_out['business_loan_ndue_RepayedAmount_sum_03m_now']=loan_second['RepayedAmount'][(loan_second['businessType']=='个人经营性贷款') & (loan_second['overdue_month']==0) & (loan_second['is_now']==1) & (loan_second['month_Desc_report']<3)].sum()#现行最近3个月无逾期个人经营性贷款本月实还款求和
        dict_out['business_loan_ndue_planRepayAmount_max_03m_now']=loan_second['planRepayAmount'][(loan_second['businessType']=='个人经营性贷款') & (loan_second['overdue_month']==0) & (loan_second['is_now']==1) & (loan_second['month_Desc_report']<3)].max()#现行最近3个月无逾期个人经营性贷款本月应还款最大
        dict_out['business_loan_ndue_RepayedAmount_max_03m_now']=loan_second['RepayedAmount'][(loan_second['businessType']=='个人经营性贷款') & (loan_second['overdue_month']==0) & (loan_second['is_now']==1) & (loan_second['month_Desc_report']<3)].max()#现行最近3个月无逾期个人经营性贷款本月实还款最大

        #最近3个月无逾期有担保个人经营性贷款
        dict_out['business_loan_is_vouch_ndue_account_count_03m']=len(loan_second[(loan_second['businessType']=='个人经营性贷款') & (loan_second['overdue_month']==0) & (loan_second['is_vouch']==1) & (loan_second['month_Desc_report']<3)])#最近3个月无逾期有担保个人经营性贷款账户计数

        #现行最近3个月无逾期有担保个人经营性贷款
        dict_out['business_loan_is_vouch_ndue_account_count_03m_now']=len(loan_second[(loan_second['businessType']=='个人经营性贷款') & (loan_second['overdue_month']==0) & (loan_second['is_now']==1) & (loan_second['is_vouch']==1) & (loan_second['month_Desc_report']<3)])#现行最近3个月无逾期有担保个人经营性贷款账户计数
        dict_out['business_loan_is_vouch_ndue_planRepayAmount_sum_03m_now']=loan_second['planRepayAmount'][(loan_second['businessType']=='个人经营性贷款') & (loan_second['overdue_month']==0) & (loan_second['is_now']==1) & (loan_second['is_vouch']==1) & (loan_second['month_Desc_report']<3)].sum()#现行最近3个月无逾期有担保个人经营性贷款本月应还款求和
        dict_out['business_loan_is_vouch_ndue_RepayedAmount_sum_03m_now']=loan_second['RepayedAmount'][(loan_second['businessType']=='个人经营性贷款') & (loan_second['overdue_month']==0) & (loan_second['is_now']==1) & (loan_second['is_vouch']==1) & (loan_second['month_Desc_report']<3)].sum()#现行最近3个月无逾期有担保个人经营性贷款本月实还款求和
        dict_out['business_loan_is_vouch_ndue_planRepayAmount_max_03m_now']=loan_second['planRepayAmount'][(loan_second['businessType']=='个人经营性贷款') & (loan_second['overdue_month']==0) & (loan_second['is_now']==1) & (loan_second['is_vouch']==1) & (loan_second['month_Desc_report']<3)].max()#现行最近3个月无逾期有担保个人经营性贷款本月应还款最大
        dict_out['business_loan_is_vouch_ndue_RepayedAmount_max_03m_now']=loan_second['RepayedAmount'][(loan_second['businessType']=='个人经营性贷款') & (loan_second['overdue_month']==0) & (loan_second['is_now']==1) & (loan_second['is_vouch']==1) & (loan_second['month_Desc_report']<3)].max()#现行最近3个月无逾期有担保个人经营性贷款本月实还款最大

        #最近6个月无逾期个人经营性贷款
        dict_out['business_loan_ndue_account_count_06m']=len(loan_second[(loan_second['businessType']=='个人经营性贷款') & (loan_second['overdue_month']==0) & (loan_second['month_Desc_report']<6)])#最近6个月无逾期个人经营性贷款账户计数

        #现行最近6个月无逾期个人经营性贷款
        dict_out['business_loan_ndue_account_count_06m_now']=len(loan_second[(loan_second['businessType']=='个人经营性贷款') & (loan_second['overdue_month']==0) & (loan_second['is_now']==1) & (loan_second['month_Desc_report']<6)])#现行最近6个月无逾期个人经营性贷款账户计数
        dict_out['business_loan_ndue_planRepayAmount_sum_06m_now']=loan_second['planRepayAmount'][(loan_second['businessType']=='个人经营性贷款') & (loan_second['overdue_month']==0) & (loan_second['is_now']==1) & (loan_second['month_Desc_report']<6)].sum()#现行最近6个月无逾期个人经营性贷款本月应还款求和
        dict_out['business_loan_ndue_RepayedAmount_sum_06m_now']=loan_second['RepayedAmount'][(loan_second['businessType']=='个人经营性贷款') & (loan_second['overdue_month']==0) & (loan_second['is_now']==1) & (loan_second['month_Desc_report']<6)].sum()#现行最近6个月无逾期个人经营性贷款本月实还款求和
        dict_out['business_loan_ndue_planRepayAmount_max_06m_now']=loan_second['planRepayAmount'][(loan_second['businessType']=='个人经营性贷款') & (loan_second['overdue_month']==0) & (loan_second['is_now']==1) & (loan_second['month_Desc_report']<6)].max()#现行最近6个月无逾期个人经营性贷款本月应还款最大
        dict_out['business_loan_ndue_RepayedAmount_max_06m_now']=loan_second['RepayedAmount'][(loan_second['businessType']=='个人经营性贷款') & (loan_second['overdue_month']==0) & (loan_second['is_now']==1) & (loan_second['month_Desc_report']<6)].max()#现行最近6个月无逾期个人经营性贷款本月实还款最大

        #最近6个月无逾期有担保个人经营性贷款
        dict_out['business_loan_is_vouch_ndue_account_count_06m']=len(loan_second[(loan_second['businessType']=='个人经营性贷款') & (loan_second['overdue_month']==0) & (loan_second['is_vouch']==1) & (loan_second['month_Desc_report']<6)])#最近6个月无逾期有担保个人经营性贷款账户计数

        #现行最近6个月无逾期有担保个人经营性贷款
        dict_out['business_loan_is_vouch_ndue_account_count_06m_now']=len(loan_second[(loan_second['businessType']=='个人经营性贷款') & (loan_second['overdue_month']==0) & (loan_second['is_now']==1) & (loan_second['is_vouch']==1) & (loan_second['month_Desc_report']<6)])#现行最近6个月无逾期有担保个人经营性贷款账户计数
        dict_out['business_loan_is_vouch_ndue_planRepayAmount_sum_06m_now']=loan_second['planRepayAmount'][(loan_second['businessType']=='个人经营性贷款') & (loan_second['overdue_month']==0) & (loan_second['is_now']==1) & (loan_second['is_vouch']==1) & (loan_second['month_Desc_report']<6)].sum()#现行最近6个月无逾期有担保个人经营性贷款本月应还款求和
        dict_out['business_loan_is_vouch_ndue_RepayedAmount_sum_06m_now']=loan_second['RepayedAmount'][(loan_second['businessType']=='个人经营性贷款') & (loan_second['overdue_month']==0) & (loan_second['is_now']==1) & (loan_second['is_vouch']==1) & (loan_second['month_Desc_report']<6)].sum()#现行最近6个月无逾期有担保个人经营性贷款本月实还款求和
        dict_out['business_loan_is_vouch_ndue_planRepayAmount_max_06m_now']=loan_second['planRepayAmount'][(loan_second['businessType']=='个人经营性贷款') & (loan_second['overdue_month']==0) & (loan_second['is_now']==1) & (loan_second['is_vouch']==1) & (loan_second['month_Desc_report']<6)].max()#现行最近6个月无逾期有担保个人经营性贷款本月应还款最大
        dict_out['business_loan_is_vouch_ndue_RepayedAmount_max_06m_now']=loan_second['RepayedAmount'][(loan_second['businessType']=='个人经营性贷款') & (loan_second['overdue_month']==0) & (loan_second['is_now']==1) & (loan_second['is_vouch']==1) & (loan_second['month_Desc_report']<6)].max()#现行最近6个月无逾期有担保个人经营性贷款本月实还款最大

        #最近12个月无逾期个人经营性贷款
        dict_out['business_loan_ndue_account_count_12m']=len(loan_second[(loan_second['businessType']=='个人经营性贷款') & (loan_second['overdue_month']==0) & (loan_second['month_Desc_report']<12)])#最近12个月无逾期个人经营性贷款账户计数

        #现行最近12个月无逾期个人经营性贷款
        dict_out['business_loan_ndue_account_count_12m_now']=len(loan_second[(loan_second['businessType']=='个人经营性贷款') & (loan_second['overdue_month']==0) & (loan_second['is_now']==1) & (loan_second['month_Desc_report']<12)])#现行最近12个月无逾期个人经营性贷款账户计数
        dict_out['business_loan_ndue_planRepayAmount_sum_12m_now']=loan_second['planRepayAmount'][(loan_second['businessType']=='个人经营性贷款') & (loan_second['overdue_month']==0) & (loan_second['is_now']==1) & (loan_second['month_Desc_report']<12)].sum()#现行最近12个月无逾期个人经营性贷款本月应还款求和
        dict_out['business_loan_ndue_RepayedAmount_sum_12m_now']=loan_second['RepayedAmount'][(loan_second['businessType']=='个人经营性贷款') & (loan_second['overdue_month']==0) & (loan_second['is_now']==1) & (loan_second['month_Desc_report']<12)].sum()#现行最近12个月无逾期个人经营性贷款本月实还款求和
        dict_out['business_loan_ndue_planRepayAmount_max_12m_now']=loan_second['planRepayAmount'][(loan_second['businessType']=='个人经营性贷款') & (loan_second['overdue_month']==0) & (loan_second['is_now']==1) & (loan_second['month_Desc_report']<12)].max()#现行最近12个月无逾期个人经营性贷款本月应还款最大
        dict_out['business_loan_ndue_RepayedAmount_max_12m_now']=loan_second['RepayedAmount'][(loan_second['businessType']=='个人经营性贷款') & (loan_second['overdue_month']==0) & (loan_second['is_now']==1) & (loan_second['month_Desc_report']<12)].max()#现行最近12个月无逾期个人经营性贷款本月实还款最大

        #最近12个月无逾期有担保个人经营性贷款
        dict_out['business_loan_is_vouch_ndue_account_count_12m']=len(loan_second[(loan_second['businessType']=='个人经营性贷款') & (loan_second['overdue_month']==0) & (loan_second['is_vouch']==1) & (loan_second['month_Desc_report']<12)])#最近12个月无逾期有担保个人经营性贷款账户计数

        #现行最近12个月无逾期有担保个人经营性贷款
        dict_out['business_loan_is_vouch_ndue_account_count_12m_now']=len(loan_second[(loan_second['businessType']=='个人经营性贷款') & (loan_second['overdue_month']==0) & (loan_second['is_now']==1) & (loan_second['is_vouch']==1) & (loan_second['month_Desc_report']<12)])#现行最近12个月无逾期有担保个人经营性贷款账户计数
        dict_out['business_loan_is_vouch_ndue_planRepayAmount_sum_12m_now']=loan_second['planRepayAmount'][(loan_second['businessType']=='个人经营性贷款') & (loan_second['overdue_month']==0) & (loan_second['is_now']==1) & (loan_second['is_vouch']==1) & (loan_second['month_Desc_report']<12)].sum()#现行最近12个月无逾期有担保个人经营性贷款本月应还款求和
        dict_out['business_loan_is_vouch_ndue_RepayedAmount_sum_12m_now']=loan_second['RepayedAmount'][(loan_second['businessType']=='个人经营性贷款') & (loan_second['overdue_month']==0) & (loan_second['is_now']==1) & (loan_second['is_vouch']==1) & (loan_second['month_Desc_report']<12)].sum()#现行最近12个月无逾期有担保个人经营性贷款本月实还款求和
        dict_out['business_loan_is_vouch_ndue_planRepayAmount_max_12m_now']=loan_second['planRepayAmount'][(loan_second['businessType']=='个人经营性贷款') & (loan_second['overdue_month']==0) & (loan_second['is_now']==1) & (loan_second['is_vouch']==1) & (loan_second['month_Desc_report']<12)].max()#现行最近12个月无逾期有担保个人经营性贷款本月应还款最大
        dict_out['business_loan_is_vouch_ndue_RepayedAmount_max_12m_now']=loan_second['RepayedAmount'][(loan_second['businessType']=='个人经营性贷款') & (loan_second['overdue_month']==0) & (loan_second['is_now']==1) & (loan_second['is_vouch']==1) & (loan_second['month_Desc_report']<12)].max()#现行最近12个月无逾期有担保个人经营性贷款本月实还款最大

        #最近24个月无逾期个人经营性贷款
        dict_out['business_loan_ndue_account_count_24m']=len(loan_second[(loan_second['businessType']=='个人经营性贷款') & (loan_second['overdue_month']==0) & (loan_second['month_Desc_report']<24)])#最近24个月无逾期个人经营性贷款账户计数

        #现行最近24个月无逾期个人经营性贷款
        dict_out['business_loan_ndue_account_count_24m_now']=len(loan_second[(loan_second['businessType']=='个人经营性贷款') & (loan_second['overdue_month']==0) & (loan_second['is_now']==1) & (loan_second['month_Desc_report']<24)])#现行最近24个月无逾期个人经营性贷款账户计数
        dict_out['business_loan_ndue_planRepayAmount_sum_24m_now']=loan_second['planRepayAmount'][(loan_second['businessType']=='个人经营性贷款') & (loan_second['overdue_month']==0) & (loan_second['is_now']==1) & (loan_second['month_Desc_report']<24)].sum()#现行最近24个月无逾期个人经营性贷款本月应还款求和
        dict_out['business_loan_ndue_RepayedAmount_sum_24m_now']=loan_second['RepayedAmount'][(loan_second['businessType']=='个人经营性贷款') & (loan_second['overdue_month']==0) & (loan_second['is_now']==1) & (loan_second['month_Desc_report']<24)].sum()#现行最近24个月无逾期个人经营性贷款本月实还款求和
        dict_out['business_loan_ndue_planRepayAmount_max_24m_now']=loan_second['planRepayAmount'][(loan_second['businessType']=='个人经营性贷款') & (loan_second['overdue_month']==0) & (loan_second['is_now']==1) & (loan_second['month_Desc_report']<24)].max()#现行最近24个月无逾期个人经营性贷款本月应还款最大
        dict_out['business_loan_ndue_RepayedAmount_max_24m_now']=loan_second['RepayedAmount'][(loan_second['businessType']=='个人经营性贷款') & (loan_second['overdue_month']==0) & (loan_second['is_now']==1) & (loan_second['month_Desc_report']<24)].max()#现行最近24个月无逾期个人经营性贷款本月实还款最大

        #最近24个月无逾期有担保个人经营性贷款
        dict_out['business_loan_is_vouch_ndue_account_count_24m']=len(loan_second[(loan_second['businessType']=='个人经营性贷款') & (loan_second['overdue_month']==0) & (loan_second['is_vouch']==1) & (loan_second['month_Desc_report']<24)])#最近24个月无逾期有担保个人经营性贷款账户计数

        #现行最近24个月无逾期有担保个人经营性贷款
        dict_out['business_loan_is_vouch_ndue_account_count_24m_now']=len(loan_second[(loan_second['businessType']=='个人经营性贷款') & (loan_second['overdue_month']==0) & (loan_second['is_now']==1) & (loan_second['is_vouch']==1) & (loan_second['month_Desc_report']<24)])#现行最近24个月无逾期有担保个人经营性贷款账户计数
        dict_out['business_loan_is_vouch_ndue_planRepayAmount_sum_24m_now']=loan_second['planRepayAmount'][(loan_second['businessType']=='个人经营性贷款') & (loan_second['overdue_month']==0) & (loan_second['is_now']==1) & (loan_second['is_vouch']==1) & (loan_second['month_Desc_report']<24)].sum()#现行最近24个月无逾期有担保个人经营性贷款本月应还款求和
        dict_out['business_loan_is_vouch_ndue_RepayedAmount_sum_24m_now']=loan_second['RepayedAmount'][(loan_second['businessType']=='个人经营性贷款') & (loan_second['overdue_month']==0) & (loan_second['is_now']==1) & (loan_second['is_vouch']==1) & (loan_second['month_Desc_report']<24)].sum()#现行最近24个月无逾期有担保个人经营性贷款本月实还款求和
        dict_out['business_loan_is_vouch_ndue_planRepayAmount_max_24m_now']=loan_second['planRepayAmount'][(loan_second['businessType']=='个人经营性贷款') & (loan_second['overdue_month']==0) & (loan_second['is_now']==1) & (loan_second['is_vouch']==1) & (loan_second['month_Desc_report']<24)].max()#现行最近24个月无逾期有担保个人经营性贷款本月应还款最大
        dict_out['business_loan_is_vouch_ndue_RepayedAmount_max_24m_now']=loan_second['RepayedAmount'][(loan_second['businessType']=='个人经营性贷款') & (loan_second['overdue_month']==0) & (loan_second['is_now']==1) & (loan_second['is_vouch']==1) & (loan_second['month_Desc_report']<24)].max()#现行最近24个月无逾期有担保个人经营性贷款本月实还款最大

        #轻度逾期个人经营性贷款
        dict_out['business_loan_slight_overdue_account_count']=len(loan_second[(loan_second['businessType']=='个人经营性贷款') & (loan_second['overdue_class']==1)])#轻度逾期个人经营性贷款账户计数
        dict_out['business_loan_slight_overdue_month_sum']=sum(list(map(sum,loan_second['overdue_month'][(loan_second['businessType']=='个人经营性贷款') & (loan_second['overdue_class']==1)])))#轻度逾期个人经营性贷款逾期月数求和
        dict_out['business_loan_slight_overdue_amount_sum']=loan_second['loanAmount'][(loan_second['businessType']=='个人经营性贷款') & (loan_second['overdue_class']==1)].sum()#轻度逾期个人经营性贷款本金求和
        dict_out['business_loan_slight_overdue_amount_max']=loan_second['loanAmount'][(loan_second['businessType']=='个人经营性贷款') & (loan_second['overdue_class']==1)].max()#轻度逾期个人经营性贷款本金最大

        #现行轻度逾期个人经营性贷款
        dict_out['business_loan_slight_overdue_account_count_now']=len(loan_second[(loan_second['businessType']=='个人经营性贷款') & (loan_second['overdue_class']==1) & (loan_second['is_now']==1)])#现行轻度逾期个人经营性贷款账户计数
        dict_out['business_loan_slight_overdue_month_sum_now']=sum(list(map(sum,loan_second['overdue_month'][(loan_second['businessType']=='个人经营性贷款') & (loan_second['overdue_class']==1) & (loan_second['is_now']==1)])))#现行轻度逾期个人经营性贷款逾期月数求和
        dict_out['business_loan_slight_overdue_amount_sum_now']=loan_second['loanAmount'][(loan_second['businessType']=='个人经营性贷款') & (loan_second['overdue_class']==1) & (loan_second['is_now']==1)].sum()#现行轻度逾期个人经营性贷款本金求和
        dict_out['business_loan_slight_overdue_balance_sum_now']=loan_second['balance'][(loan_second['businessType']=='个人经营性贷款') & (loan_second['overdue_class']==1) & (loan_second['is_now']==1)].sum()#现行轻度逾期个人经营性贷款本金余额求和
        dict_out['business_loan_slight_overdue_planRepayAmount_sum_now']=loan_second['planRepayAmount'][(loan_second['businessType']=='个人经营性贷款') & (loan_second['overdue_class']==1) & (loan_second['is_now']==1)].sum()#现行轻度逾期个人经营性贷款本月应还款求和
        dict_out['business_loan_slight_overdue_RepayedAmount_sum_now']=loan_second['RepayedAmount'][(loan_second['businessType']=='个人经营性贷款') & (loan_second['overdue_class']==1) & (loan_second['is_now']==1)].sum()#现行轻度逾期个人经营性贷款本月实还款求和
        dict_out['business_loan_slight_overdue_currentOverdueAmount_sum_now']=loan_second['currentOverdueAmount'][(loan_second['businessType']=='个人经营性贷款') & (loan_second['overdue_class']==1) & (loan_second['is_now']==1)].sum()#现行轻度逾期个人经营性贷款z当前逾期金额求和
        dict_out['business_loan_slight_overdue_amount_max_now']=loan_second['loanAmount'][(loan_second['businessType']=='个人经营性贷款') & (loan_second['overdue_class']==1) & (loan_second['is_now']==1)].max()#现行轻度逾期个人经营性贷款本金最大
        dict_out['business_loan_slight_overdue_balance_max_now']=loan_second['balance'][(loan_second['businessType']=='个人经营性贷款') & (loan_second['overdue_class']==1) & (loan_second['is_now']==1)].max()#现行轻度逾期个人经营性贷款本金余额最大
        dict_out['business_loan_slight_overdue_planRepayAmount_max_now']=loan_second['planRepayAmount'][(loan_second['businessType']=='个人经营性贷款') & (loan_second['overdue_class']==1) & (loan_second['is_now']==1)].max()#现行轻度逾期个人经营性贷款本月应还款最大
        dict_out['business_loan_slight_overdue_RepayedAmount_max_now']=loan_second['RepayedAmount'][(loan_second['businessType']=='个人经营性贷款') & (loan_second['overdue_class']==1) & (loan_second['is_now']==1)].max()#现行轻度逾期个人经营性贷款本月实还款最大
        dict_out['business_loan_slight_overdue_currentOverdueAmount_max_now']=loan_second['currentOverdueAmount'][(loan_second['businessType']=='个人经营性贷款') & (loan_second['overdue_class']==1) & (loan_second['is_now']==1)].max()#现行轻度逾期个人经营性贷款当前逾期金额最大

        #轻度逾期有担保个人经营性贷款
        dict_out['business_loan_slight_overdue_is_vouch_account_count']=len(loan_second[(loan_second['businessType']=='个人经营性贷款') & (loan_second['overdue_class']==1) & (loan_second['is_vouch']==1)])#轻度逾期有担保个人经营性贷款账户计数
        dict_out['business_loan_slight_overdue_is_vouch_month_sum']=sum(list(map(sum,loan_second['overdue_month'][(loan_second['businessType']=='个人经营性贷款') & (loan_second['overdue_class']==1) & (loan_second['is_vouch']==1)])))#轻度逾期有担保个人经营性贷款逾期月数求和
        dict_out['business_loan_slight_overdue_is_vouch_amount_sum']=loan_second['loanAmount'][(loan_second['businessType']=='个人经营性贷款') & (loan_second['overdue_class']==1) & (loan_second['is_vouch']==1)].sum()#轻度逾期有担保个人经营性贷款本金求和
        dict_out['business_loan_slight_overdue_is_vouch_amount_max']=loan_second['loanAmount'][(loan_second['businessType']=='个人经营性贷款') & (loan_second['overdue_class']==1) & (loan_second['is_vouch']==1)].max()#轻度逾期有担保个人经营性贷款本金最大

        #现行轻度逾期有担保个人经营性贷款
        dict_out['business_loan_slight_overdue_is_vouch_account_count_now']=len(loan_second[(loan_second['businessType']=='个人经营性贷款') & (loan_second['overdue_class']==1) & (loan_second['is_vouch']==1) & (loan_second['is_now']==1)])#现行轻度逾期有担保个人经营性贷款账户计数
        dict_out['business_loan_slight_overdue_is_vouch_month_sum_now']=sum(list(map(sum,loan_second['overdue_month'][(loan_second['businessType']=='个人经营性贷款') & (loan_second['overdue_class']==1) & (loan_second['is_vouch']==1) & (loan_second['is_now']==1)])))#现行轻度逾期有担保个人经营性贷款逾期月数求和
        dict_out['business_loan_slight_overdue_is_vouch_amount_sum_now']=loan_second['loanAmount'][(loan_second['businessType']=='个人经营性贷款') & (loan_second['overdue_class']==1) & (loan_second['is_vouch']==1) & (loan_second['is_now']==1)].sum()#现行轻度逾期有担保个人经营性贷款本金求和
        dict_out['business_loan_slight_overdue_is_vouch_balance_sum_now']=loan_second['balance'][(loan_second['businessType']=='个人经营性贷款') & (loan_second['overdue_class']==1) & (loan_second['is_vouch']==1) & (loan_second['is_now']==1)].sum()#现行轻度逾期有担保个人经营性贷款本金余额求和
        dict_out['business_loan_slight_overdue_is_vouch_planRepayAmount_sum_now']=loan_second['planRepayAmount'][(loan_second['businessType']=='个人经营性贷款') & (loan_second['overdue_class']==1) & (loan_second['is_vouch']==1) & (loan_second['is_now']==1)].sum()#现行轻度逾期有担保个人经营性贷款本月应还款求和
        dict_out['business_loan_slight_overdue_is_vouch_RepayedAmount_sum_now']=loan_second['RepayedAmount'][(loan_second['businessType']=='个人经营性贷款') & (loan_second['overdue_class']==1) & (loan_second['is_vouch']==1) & (loan_second['is_now']==1)].sum()#现行轻度逾期有担保个人经营性贷款本月实还款求和
        dict_out['business_loan_slight_overdue_is_vouch_currentOverdueAmount_sum_now']=loan_second['currentOverdueAmount'][(loan_second['businessType']=='个人经营性贷款') & (loan_second['overdue_class']==1) & (loan_second['is_vouch']==1) & (loan_second['is_now']==1)].sum()#现行轻度逾期有担保个人经营性贷款当前逾期金额求和
        dict_out['business_loan_slight_overdue_is_vouch_amount_max_now']=loan_second['loanAmount'][(loan_second['businessType']=='个人经营性贷款') & (loan_second['overdue_class']==1) & (loan_second['is_vouch']==1) & (loan_second['is_now']==1)].max()#现行轻度逾期有担保个人经营性贷款本金最大
        dict_out['business_loan_slight_overdue_is_vouch_balance_max_now']=loan_second['balance'][(loan_second['businessType']=='个人经营性贷款') & (loan_second['overdue_class']==1) & (loan_second['is_vouch']==1) & (loan_second['is_now']==1)].max()#现行轻度逾期有担保个人经营性贷款本金余额最大
        dict_out['business_loan_slight_overdue_is_vouch_planRepayAmount_max_now']=loan_second['planRepayAmount'][(loan_second['businessType']=='个人经营性贷款') & (loan_second['overdue_class']==1) & (loan_second['is_vouch']==1) & (loan_second['is_now']==1)].max()#现行轻度逾期有担保个人经营性贷款本月应还款最大
        dict_out['business_loan_slight_overdue_is_vouch_RepayedAmount_max_now']=loan_second['RepayedAmount'][(loan_second['businessType']=='个人经营性贷款') & (loan_second['overdue_class']==1) & (loan_second['is_vouch']==1) & (loan_second['is_now']==1)].max()#现行轻度逾期有担保个人经营性贷款本月实还款最大
        dict_out['business_loan_slight_overdue_is_vouch_currentOverdueAmount_max_now']=loan_second['currentOverdueAmount'][(loan_second['businessType']=='个人经营性贷款') & (loan_second['overdue_class']==1) & (loan_second['is_vouch']==1) & (loan_second['is_now']==1)].max()#现行轻度逾期有担保个人经营性贷款当前逾期金额最大

        #个人消费贷款
        dict_out['consume_loan_account_count']=len(loan_second[loan_second['businessType']=='其他个人消费贷款'])#个人消费贷款账户计数
        dict_out['consume_loan_overdue_month_sum']=sum(list(map(sum,loan_second['overdue_month'][loan_second['businessType']=='其他个人消费贷款'])))#个人消费贷款逾期月数求和
        dict_out['consume_loan_amount_sum']=loan_second['loanAmount'][loan_second['businessType']=='其他个人消费贷款'].sum()#个人消费贷款本金求和
        dict_out['consume_loan_overdue_month_max']=loan_second['overdue_month_max'][loan_second['businessType']=='其他个人消费贷款'].max()#个人消费贷款逾期月数最大
        dict_out['consume_loan_amount_max']=loan_second['loanAmount'][loan_second['businessType']=='其他个人消费贷款'].max()#个人消费贷款本金最大
        dict_out['consume_loan_rating_worst']={'1.0':'损失', '2.0':'可疑', '3.0':'关注', '4.0':'次级', '5.0':'正常','nan':"无"}[str(loan_second['classify5_num'][(loan_second['businessType']=='其他个人消费贷款')].min())]#个人消费贷款Rating最差

        #现行个人消费贷款
        dict_out['consume_loan_account_count_now']=len(loan_second[(loan_second['businessType']=='其他个人消费贷款') & (loan_second['is_now']==1)])#现行个人消费贷款账户计数
        dict_out['consume_loan_overdue_month_sum_now']=sum(list(map(sum,loan_second['overdue_month'][(loan_second['businessType']=='其他个人消费贷款') & (loan_second['is_now']==1)])))#现行个人消费贷款逾期月数求和
        dict_out['consume_loan_amount_sum_now']=loan_second['loanAmount'][(loan_second['businessType']=='其他个人消费贷款') & (loan_second['is_now']==1)].sum()#现行个人消费贷款本金求和
        dict_out['consume_loan_currentOverdueTerms_sum_now']=loan_second['currentOverdueTerms'][(loan_second['businessType']=='其他个人消费贷款') & (loan_second['is_now']==1)].sum()#现行个人消费贷款当前逾期期数求和
        dict_out['consume_loan_balance_sum_now']=loan_second['balance'][(loan_second['businessType']=='其他个人消费贷款') & (loan_second['is_now']==1)].sum()#现行个人消费贷款本金余额求和
        dict_out['consume_loan_planRepayAmount_sum_now']=loan_second['planRepayAmount'][(loan_second['businessType']=='其他个人消费贷款') & (loan_second['is_now']==1)].sum()#现行个人消费贷款本月应还款求和
        dict_out['consume_loan_RepayedAmount_sum_now']=loan_second['RepayedAmount'][(loan_second['businessType']=='其他个人消费贷款') & (loan_second['is_now']==1)].sum()#现行个人消费贷款本月实还款求和
        dict_out['consume_loan_currentOverdueAmount_sum_now']=loan_second['currentOverdueAmount'][(loan_second['businessType']=='其他个人消费贷款') & (loan_second['is_now']==1)].sum()#现行个人消费贷款当前逾期金额求和
        dict_out['consume_loan_overdue31Amount_sum_now']=loan_second['overdue31Amount'][(loan_second['businessType']=='其他个人消费贷款') & (loan_second['is_now']==1)].sum()#现行个人消费贷款逾期31-60天未还本金求和
        dict_out['consume_loan_overdue61Amount_sum_now']=loan_second['overdue61Amount'][(loan_second['businessType']=='其他个人消费贷款') & (loan_second['is_now']==1)].sum()#现行个人消费贷款逾期61-90天未还本金求和
        dict_out['consume_loan_overdue91Amount_sum_now']=loan_second['overdue91Amount'][(loan_second['businessType']=='其他个人消费贷款') & (loan_second['is_now']==1)].sum()#现行个人消费贷款逾期91-180天未还本金求和
        dict_out['consume_loan_overdue180Amount_sum_now']=loan_second['overdue180Amount'][(loan_second['businessType']=='其他个人消费贷款') & (loan_second['is_now']==1)].sum()#现行个人消费贷款逾期180天以上未还本金求和
        dict_out['consume_loan_overdue_month_max_now']=loan_second['overdue_month_max'][(loan_second['businessType']=='其他个人消费贷款') & (loan_second['is_now']==1)].max()#现行个人消费贷款逾期月数最大
        dict_out['consume_loan_amount_max_now']=loan_second['loanAmount'][(loan_second['businessType']=='其他个人消费贷款') & (loan_second['is_now']==1)].max()#现行个人消费贷款本金最大
        dict_out['consume_loan_currentOverdueTerms_max_now']=loan_second['currentOverdueTerms'][(loan_second['businessType']=='其他个人消费贷款') & (loan_second['is_now']==1)].max()#现行个人消费贷款当前逾期期数最大
        dict_out['consume_loan_balance_max_now']=loan_second['balance'][(loan_second['businessType']=='其他个人消费贷款') & (loan_second['is_now']==1)].max()#现行个人消费贷款本金余额最大
        dict_out['consume_loan_planRepayAmount_max_now']=loan_second['planRepayAmount'][(loan_second['businessType']=='其他个人消费贷款') & (loan_second['is_now']==1)].max()#现行个人消费贷款本月应还款最大
        dict_out['consume_loan_RepayedAmount_max_now']=loan_second['RepayedAmount'][(loan_second['businessType']=='其他个人消费贷款') & (loan_second['is_now']==1)].max()#现行个人消费贷款本月实还款最大
        dict_out['consume_loan_currentOverdueAmount_max_now']=loan_second['currentOverdueAmount'][(loan_second['businessType']=='其他个人消费贷款') & (loan_second['is_now']==1)].max()#现行个人消费贷款当前逾期金额最大
        dict_out['consume_loan_overdue31Amount_max_now']=loan_second['overdue31Amount'][(loan_second['businessType']=='其他个人消费贷款') & (loan_second['is_now']==1)].max()#现行个人消费贷款逾期31-60天未还本金最大
        dict_out['consume_loan_overdue61Amount_max_now']=loan_second['overdue61Amount'][(loan_second['businessType']=='其他个人消费贷款') & (loan_second['is_now']==1)].max()#现行个人消费贷款逾期61-90天未还本金最大
        dict_out['consume_loan_overdue91Amount_max_now']=loan_second['overdue91Amount'][(loan_second['businessType']=='其他个人消费贷款') & (loan_second['is_now']==1)].max()#现行个人消费贷款逾期91-180天未还本金最大
        dict_out['consume_loan_overdue180Amount_max_now']=loan_second['overdue180Amount'][(loan_second['businessType']=='其他个人消费贷款') & (loan_second['is_now']==1)].max()#现行个人消费贷款逾期180天以上未还本金最大

        #有担保个人消费贷款
        dict_out['consume_loan_is_vouch_account_count']=len(loan_second[(loan_second['businessType']=='其他个人消费贷款') & (loan_second['is_vouch']==1)])#有担保个人消费贷款账户计数
        dict_out['consume_loan_is_vouch_overdue_month_sum']=sum(list(map(sum,loan_second['overdue_month'][(loan_second['businessType']=='其他个人消费贷款') & (loan_second['is_vouch']==1)])))#有担保个人消费贷款逾期月数求和
        dict_out['consume_loan_is_vouch_amount_sum']=loan_second['loanAmount'][(loan_second['businessType']=='其他个人消费贷款') & (loan_second['is_vouch']==1)].sum()#有担保个人消费贷款本金求和
        dict_out['consume_loan_is_vouch_overdue_month_max']=loan_second['overdue_month_max'][(loan_second['businessType']=='其他个人消费贷款') & (loan_second['is_vouch']==1)].max()#有担保个人消费贷款逾期月数最大
        dict_out['consume_loan_is_vouch_amount_max']=loan_second['loanAmount'][(loan_second['businessType']=='其他个人消费贷款') & (loan_second['is_vouch']==1)].max()#有担保个人消费贷款本金最大
        dict_out['consume_loan_is_vouch_rating_worst']={'1.0':'损失', '2.0':'可疑', '3.0':'关注', '4.0':'次级', '5.0':'正常','nan':"无"}[str(loan_second['classify5_num'][(loan_second['businessType']=='其他个人消费贷款') & (loan_second['is_vouch']==1)].min())]#有担保个人消费贷款Rating最差

        #现行有担保个人消费贷款
        dict_out['consume_loan_is_vouch_account_count_now']=len(loan_second[(loan_second['businessType']=='其他个人消费贷款') & (loan_second['is_vouch']==1) & (loan_second['is_now']==1)])#现行有担保个人消费贷款账户计数
        dict_out['consume_loan_is_vouch_overdue_month_sum_now']=sum(list(map(sum,loan_second['overdue_month'][(loan_second['businessType']=='其他个人消费贷款') & (loan_second['is_vouch']==1) & (loan_second['is_now']==1)])))#现行有担保个人消费贷款逾期月数求和
        dict_out['consume_loan_is_vouch_amount_sum_now']=loan_second['loanAmount'][(loan_second['businessType']=='其他个人消费贷款') & (loan_second['is_vouch']==1) & (loan_second['is_now']==1)].sum()#现行有担保个人消费贷款本金求和
        dict_out['consume_loan_is_vouch_currentOverdueTerms_sum_now']=loan_second['currentOverdueTerms'][(loan_second['businessType']=='其他个人消费贷款') & (loan_second['is_vouch']==1) & (loan_second['is_now']==1)].sum()#现行有担保个人消费贷款当前逾期期数求和
        dict_out['consume_loan_is_vouch_balance_sum_now']=loan_second['balance'][(loan_second['businessType']=='其他个人消费贷款') & (loan_second['is_vouch']==1) & (loan_second['is_now']==1)].sum()#现行有担保个人消费贷款本金余额求和
        dict_out['consume_loan_is_vouch_planRepayAmount_sum_now']=loan_second['planRepayAmount'][(loan_second['businessType']=='其他个人消费贷款') & (loan_second['is_vouch']==1) & (loan_second['is_now']==1)].sum()#现行有担保个人消费贷款本月应还款求和
        dict_out['consume_loan_is_vouch_RepayedAmount_sum_now']=loan_second['RepayedAmount'][(loan_second['businessType']=='其他个人消费贷款') & (loan_second['is_vouch']==1) & (loan_second['is_now']==1)].sum()#现行有担保个人消费贷款本月实还款求和
        dict_out['consume_loan_is_vouch_currentOverdueAmount_sum_now']=loan_second['currentOverdueAmount'][(loan_second['businessType']=='其他个人消费贷款') & (loan_second['is_vouch']==1) & (loan_second['is_now']==1)].sum()#现行有担保个人消费贷款当前逾期金额求和
        dict_out['consume_loan_is_vouch_overdue31Amount_sum_now']=loan_second['overdue31Amount'][(loan_second['businessType']=='其他个人消费贷款') & (loan_second['is_vouch']==1) & (loan_second['is_now']==1)].sum()#现行有担保个人消费贷款逾期31-60天未还本金求和
        dict_out['consume_loan_is_vouch_overdue61Amount_sum_now']=loan_second['overdue61Amount'][(loan_second['businessType']=='其他个人消费贷款') & (loan_second['is_vouch']==1) & (loan_second['is_now']==1)].sum()#现行有担保个人消费贷款逾期61-90天未还本金求和
        dict_out['consume_loan_is_vouch_overdue91Amount_sum_now']=loan_second['overdue91Amount'][(loan_second['businessType']=='其他个人消费贷款') & (loan_second['is_vouch']==1) & (loan_second['is_now']==1)].sum()#现行有担保个人消费贷款逾期91-180天未还本金求和
        dict_out['consume_loan_is_vouch_overdue180Amount_sum_now']=loan_second['overdue180Amount'][(loan_second['businessType']=='其他个人消费贷款') & (loan_second['is_vouch']==1) & (loan_second['is_now']==1)].sum()#现行有担保个人消费贷款逾期180天以上未还本金求和
        dict_out['consume_loan_is_vouch_overdue_month_max_now']=loan_second['overdue_month_max'][(loan_second['businessType']=='其他个人消费贷款') & (loan_second['is_vouch']==1) & (loan_second['is_now']==1)].max()#现行有担保个人消费贷款逾期月数最大
        dict_out['consume_loan_is_vouch_amount_max_now']=loan_second['loanAmount'][(loan_second['businessType']=='其他个人消费贷款') & (loan_second['is_vouch']==1) & (loan_second['is_now']==1)].max()#现行有担保个人消费贷款本金最大
        dict_out['consume_loan_is_vouch_currentOverdueTerms_max_now']=loan_second['currentOverdueTerms'][(loan_second['businessType']=='其他个人消费贷款') & (loan_second['is_vouch']==1) & (loan_second['is_now']==1)].max()#现行有担保个人消费贷款当前逾期期数最大
        dict_out['consume_loan_is_vouch_balance_max_now']=loan_second['balance'][(loan_second['businessType']=='其他个人消费贷款') & (loan_second['is_vouch']==1) & (loan_second['is_now']==1)].max()#现行有担保个人消费贷款本金余额最大
        dict_out['consume_loan_is_vouch_planRepayAmount_max_now']=loan_second['planRepayAmount'][(loan_second['businessType']=='其他个人消费贷款') & (loan_second['is_vouch']==1) & (loan_second['is_now']==1)].max()#现行有担保个人消费贷款本月应还款最大
        dict_out['consume_loan_is_vouch_RepayedAmount_max_now']=loan_second['RepayedAmount'][(loan_second['businessType']=='其他个人消费贷款') & (loan_second['is_vouch']==1) & (loan_second['is_now']==1)].max()#现行有担保个人消费贷款本月实还款最大
        dict_out['consume_loan_is_vouch_currentOverdueAmount_max_now']=loan_second['currentOverdueAmount'][(loan_second['businessType']=='其他个人消费贷款') & (loan_second['is_vouch']==1) & (loan_second['is_now']==1)].max()#现行有担保个人消费贷款当前逾期金额最大
        dict_out['consume_loan_is_vouch_overdue31Amount_max_now']=loan_second['overdue31Amount'][(loan_second['businessType']=='其他个人消费贷款') & (loan_second['is_vouch']==1) & (loan_second['is_now']==1)].max()#现行有担保个人消费贷款逾期31-60天未还本金最大
        dict_out['consume_loan_is_vouch_overdue61Amount_max_now']=loan_second['overdue61Amount'][(loan_second['businessType']=='其他个人消费贷款') & (loan_second['is_vouch']==1) & (loan_second['is_now']==1)].max()#现行有担保个人消费贷款逾期61-90天未还本金最大
        dict_out['consume_loan_is_vouch_overdue91Amount_max_now']=loan_second['overdue91Amount'][(loan_second['businessType']=='其他个人消费贷款') & (loan_second['is_vouch']==1) & (loan_second['is_now']==1)].max()#现行有担保个人消费贷款逾期91-180天未还本金最大
        dict_out['consume_loan_is_vouch_overdue180Amount_max_now']=loan_second['overdue180Amount'][(loan_second['businessType']=='其他个人消费贷款') & (loan_second['is_vouch']==1) & (loan_second['is_now']==1)].max()#现行有担保个人消费贷款逾期180天以上未还本金最大

        #最近3个月个人消费贷款
        dict_out['consume_loan_overdue_month_sum_03m']=sum(list(map(sum,loan_second['overdue_month'][(loan_second['businessType']=='其他个人消费贷款') & (loan_second['month_Desc_report']<3)])))#最近3个月个人消费贷款逾期月数求和
        dict_out['consume_loan_overdue_month_max_03m']=loan_second['overdue_month_max'][(loan_second['businessType']=='其他个人消费贷款') & (loan_second['month_Desc_report']<3)].max()#最近3个月个人消费贷款逾期持续月数最大

        #现行最近3个月个人消费贷款
        dict_out['consume_loan_overdue_month_sum_03m_now']=sum(list(map(sum,loan_second['overdue_month'][(loan_second['businessType']=='其他个人消费贷款') & (loan_second['month_Desc_report']<3) & (loan_second['is_now']==1)])))#现行最近3个月个人消费贷款逾期月数求和
        dict_out['consume_loan_overdue_month_max_03m_now']=loan_second['overdue_month_max'][(loan_second['businessType']=='其他个人消费贷款') & (loan_second['month_Desc_report']<3) & (loan_second['is_now']==1)].max()#现行最近3个月个人消费贷款逾期持续月数最大

        #最近3个月有担保个人消费贷款
        dict_out['consume_loan_is_vouch_overdue_month_sum_03m']=sum(list(map(sum,loan_second['overdue_month'][(loan_second['businessType']=='其他个人消费贷款') & (loan_second['month_Desc_report']<3) & (loan_second['is_vouch']==1)])))#最近3个月有担保个人消费贷款逾期月数求和
        dict_out['consume_loan_is_vouch_overdue_month_max_03m']=loan_second['overdue_month_max'][(loan_second['businessType']=='其他个人消费贷款') & (loan_second['month_Desc_report']<3) & (loan_second['is_vouch']==1)].max()#最近3个月有担保个人消费贷款逾期持续月数最大

        #现行最近3个月有担保个人消费贷款
        dict_out['consume_loan_is_vouch_overdue_month_sum_03m_now']=sum(list(map(sum,loan_second['overdue_month'][(loan_second['businessType']=='其他个人消费贷款') & (loan_second['month_Desc_report']<3) & (loan_second['is_vouch']==1) & (loan_second['is_now']==1)])))#现行最近3个月有担保个人消费贷款逾期月数求和
        dict_out['consume_loan_is_vouch_overdue_month_max_03m_now']=loan_second['overdue_month_max'][(loan_second['businessType']=='其他个人消费贷款') & (loan_second['month_Desc_report']<3) & (loan_second['is_vouch']==1) & (loan_second['is_now']==1)].max()#现行最近3个月有担保个人消费贷款逾期持续月数最大

        #最近6个月个人消费贷款
        dict_out['consume_loan_overdue_month_sum_06m']=sum(list(map(sum,loan_second['overdue_month'][(loan_second['businessType']=='其他个人消费贷款') & (loan_second['month_Desc_report']<6)])))#最近6个月个人消费贷款逾期月数求和
        dict_out['consume_loan_overdue_month_max_06m']=loan_second['overdue_month_max'][(loan_second['businessType']=='其他个人消费贷款') & (loan_second['month_Desc_report']<6)].max()#最近6个月个人消费贷款逾期持续月数最大

        #现行最近6个月个人消费贷款
        dict_out['consume_loan_overdue_month_sum_06m_now']=sum(list(map(sum,loan_second['overdue_month'][(loan_second['businessType']=='其他个人消费贷款') & (loan_second['month_Desc_report']<6) & (loan_second['is_now']==1)])))#现行最近6个月个人消费贷款逾期月数求和
        dict_out['consume_loan_overdue_month_max_06m_now']=loan_second['overdue_month_max'][(loan_second['businessType']=='其他个人消费贷款') & (loan_second['month_Desc_report']<6) & (loan_second['is_now']==1)].max()#现行最近6个月个人消费贷款逾期持续月数最大

        #最近6个月有担保个人消费贷款
        dict_out['consume_loan_is_vouch_overdue_month_sum_06m']=sum(list(map(sum,loan_second['overdue_month'][(loan_second['businessType']=='其他个人消费贷款') & (loan_second['month_Desc_report']<6) & (loan_second['is_vouch']==1)])))#最近6个月有担保个人消费贷款逾期月数求和
        dict_out['consume_loan_is_vouch_overdue_month_max_06m']=loan_second['overdue_month_max'][(loan_second['businessType']=='其他个人消费贷款') & (loan_second['month_Desc_report']<6) & (loan_second['is_vouch']==1)].max()#最近6个月有担保个人消费贷款逾期持续月数最大

        #现行最近6个月有担保个人消费贷款
        dict_out['consume_loan_is_vouch_overdue_month_sum_06m_now']=sum(list(map(sum,loan_second['overdue_month'][(loan_second['businessType']=='其他个人消费贷款') & (loan_second['month_Desc_report']<6) & (loan_second['is_vouch']==1) & (loan_second['is_now']==1)])))#现行最近6个月有担保个人消费贷款逾期月数求和
        dict_out['consume_loan_is_vouch_overdue_month_max_06m_now']=loan_second['overdue_month_max'][(loan_second['businessType']=='其他个人消费贷款') & (loan_second['month_Desc_report']<6) & (loan_second['is_vouch']==1) & (loan_second['is_now']==1)].max()#现行最近6个月有担保个人消费贷款逾期持续月数最大

        #最近12个月个人消费贷款
        dict_out['consume_loan_overdue_month_sum_12m']=sum(list(map(sum,loan_second['overdue_month'][(loan_second['businessType']=='其他个人消费贷款') & (loan_second['month_Desc_report']<12)])))#最近12个月个人消费贷款逾期月数求和
        dict_out['consume_loan_overdue_month_max_12m']=loan_second['overdue_month_max'][(loan_second['businessType']=='其他个人消费贷款') & (loan_second['month_Desc_report']<12)].max()#最近12个月个人消费贷款逾期持续月数最大

        #现行最近12个月个人消费贷款
        dict_out['consume_loan_overdue_month_sum_12m_now']=sum(list(map(sum,loan_second['overdue_month'][(loan_second['businessType']=='其他个人消费贷款') & (loan_second['month_Desc_report']<12) & (loan_second['is_now']==1)])))#现行最近12个月个人消费贷款逾期月数求和
        dict_out['consume_loan_overdue_month_max_12m_now']=loan_second['overdue_month_max'][(loan_second['businessType']=='其他个人消费贷款') & (loan_second['month_Desc_report']<12) & (loan_second['is_now']==1)].max()#现行最近12个月个人消费贷款逾期持续月数最大

        #最近12个月有担保个人消费贷款
        dict_out['consume_loan_is_vouch_overdue_month_sum_12m']=sum(list(map(sum,loan_second['overdue_month'][(loan_second['businessType']=='其他个人消费贷款') & (loan_second['month_Desc_report']<12) & (loan_second['is_vouch']==1)])))#最近12个月有担保个人消费贷款逾期月数求和
        dict_out['consume_loan_is_vouch_overdue_month_max_12m']=loan_second['overdue_month_max'][(loan_second['businessType']=='其他个人消费贷款') & (loan_second['month_Desc_report']<12) & (loan_second['is_vouch']==1)].max()#最近12个月有担保个人消费贷款逾期持续月数最大

        #现行最近12个月有担保个人消费贷款
        dict_out['consume_loan_is_vouch_overdue_month_sum_12m_now']=sum(list(map(sum,loan_second['overdue_month'][(loan_second['businessType']=='其他个人消费贷款') & (loan_second['month_Desc_report']<12) & (loan_second['is_vouch']==1) & (loan_second['is_now']==1)])))#现行最近12个月有担保个人消费贷款逾期月数求和
        dict_out['consume_loan_is_vouch_overdue_month_max_12m_now']=loan_second['overdue_month_max'][(loan_second['businessType']=='其他个人消费贷款') & (loan_second['month_Desc_report']<12) & (loan_second['is_vouch']==1) & (loan_second['is_now']==1)].max()#现行最近12个月有担保个人消费贷款逾期持续月数最大

        #最近24个月个人消费贷款
        dict_out['consume_loan_overdue_month_sum_24m']=sum(list(map(sum,loan_second['overdue_month'][(loan_second['businessType']=='其他个人消费贷款') & (loan_second['month_Desc_report']<24)])))#最近24个月个人消费贷款逾期月数求和
        dict_out['consume_loan_overdue_month_max_24m']=loan_second['overdue_month_max'][(loan_second['businessType']=='其他个人消费贷款') & (loan_second['month_Desc_report']<24)].max()#最近24个月个人消费贷款逾期持续月数最大

        #现行最近24个月个人消费贷款
        dict_out['consume_loan_overdue_month_sum_24m_now']=sum(list(map(sum,loan_second['overdue_month'][(loan_second['businessType']=='其他个人消费贷款') & (loan_second['month_Desc_report']<24) & (loan_second['is_now']==1)])))#现行最近24个月个人消费贷款逾期月数求和
        dict_out['consume_loan_overdue_month_max_24m_now']=loan_second['overdue_month_max'][(loan_second['businessType']=='其他个人消费贷款') & (loan_second['month_Desc_report']<24) & (loan_second['is_now']==1)].max()#现行最近24个月个人消费贷款逾期持续月数最大

        #最近24个月有担保个人消费贷款
        dict_out['consume_loan_is_vouch_overdue_month_sum_24m']=sum(list(map(sum,loan_second['overdue_month'][(loan_second['businessType']=='其他个人消费贷款') & (loan_second['month_Desc_report']<24) & (loan_second['is_vouch']==1)])))#最近24个月有担保个人消费贷款逾期月数求和
        dict_out['consume_loan_is_vouch_overdue_month_max_24m']=loan_second['overdue_month_max'][(loan_second['businessType']=='其他个人消费贷款') & (loan_second['month_Desc_report']<24) & (loan_second['is_vouch']==1)].max()#最近24个月有担保个人消费贷款逾期持续月数最大

        #现行最近24个月有担保个人消费贷款
        dict_out['consume_loan_is_vouch_overdue_month_sum_24m_now']=sum(list(map(sum,loan_second['overdue_month'][(loan_second['businessType']=='其他个人消费贷款') & (loan_second['month_Desc_report']<24) & (loan_second['is_vouch']==1) & (loan_second['is_now']==1)])))#现行最近24个月有担保个人消费贷款逾期月数求和
        dict_out['consume_loan_is_vouch_overdue_month_max_24m_now']=loan_second['overdue_month_max'][(loan_second['businessType']=='其他个人消费贷款') & (loan_second['month_Desc_report']<24) & (loan_second['is_vouch']==1) & (loan_second['is_now']==1)].max()#现行最近24个月有担保个人消费贷款逾期持续月数最大

        #无逾期个人消费贷款
        dict_out['consume_loan_ndue_account_count']=len(loan_second[(loan_second['businessType']=='其他个人消费贷款') & (loan_second['overdue_month']==0)])#无逾期个人消费贷款账户计数
        dict_out['consume_loan_ndue_amount_sum']=loan_second['loanAmount'][(loan_second['businessType']=='其他个人消费贷款') & (loan_second['overdue_month']==0)].sum()#无逾期个人消费贷款本金求和
        dict_out['consume_loan_ndue_amount_max']=loan_second['loanAmount'][(loan_second['businessType']=='其他个人消费贷款') & (loan_second['overdue_month']==0)].max()#无逾期个人消费贷款本金最大

        #现行无逾期个人消费贷款
        dict_out['consume_loan_ndue_account_count_now']=len(loan_second[(loan_second['businessType']=='其他个人消费贷款') & (loan_second['overdue_month']==0) & (loan_second['is_now']==1)])#现行无逾期个人消费贷款账户计数
        dict_out['consume_loan_ndue_amount_sum_now']=loan_second['loanAmount'][(loan_second['businessType']=='其他个人消费贷款') & (loan_second['overdue_month']==0) & (loan_second['is_now']==1)].sum()#现行无逾期个人消费贷款本金求和
        dict_out['consume_loan_ndue_balance_sum_now']=loan_second['balance'][(loan_second['businessType']=='其他个人消费贷款') & (loan_second['overdue_month']==0) & (loan_second['is_now']==1)].sum()#现行无逾期个人消费贷款本金余额求和
        dict_out['consume_loan_ndue_planRepayAmount_sum_now']=loan_second['planRepayAmount'][(loan_second['businessType']=='其他个人消费贷款') & (loan_second['overdue_month']==0) & (loan_second['is_now']==1)].sum()#现行无逾期个人消费贷款本月应还款求和
        dict_out['consume_loan_ndue_RepayedAmount_sum_now']=loan_second['RepayedAmount'][(loan_second['businessType']=='其他个人消费贷款') & (loan_second['overdue_month']==0) & (loan_second['is_now']==1)].sum()#现行无逾期个人消费贷款本月实还款求和
        dict_out['consume_loan_ndue_amount_max_now']=loan_second['loanAmount'][(loan_second['businessType']=='其他个人消费贷款') & (loan_second['overdue_month']==0) & (loan_second['is_now']==1)].max()#现行无逾期个人消费贷款本金最大
        dict_out['consume_loan_ndue_balance_max_now']=loan_second['balance'][(loan_second['businessType']=='其他个人消费贷款') & (loan_second['overdue_month']==0) & (loan_second['is_now']==1)].max()#现行无逾期个人消费贷款本金余额最大
        dict_out['consume_loan_ndue_planRepayAmount_max_now']=loan_second['planRepayAmount'][(loan_second['businessType']=='其他个人消费贷款') & (loan_second['overdue_month']==0) & (loan_second['is_now']==1)].max()#现行无逾期个人消费贷款本月应还款最大
        dict_out['consume_loan_ndue_RepayedAmount_max_now']=loan_second['RepayedAmount'][(loan_second['businessType']=='其他个人消费贷款') & (loan_second['overdue_month']==0) & (loan_second['is_now']==1)].max()#现行无逾期个人消费贷款本月实还款最大

        #无逾期有担保个人消费贷款
        dict_out['consume_loan_ndue_is_vouch_account_count']=len(loan_second[(loan_second['businessType']=='其他个人消费贷款') & (loan_second['overdue_month']==0) & (loan_second['is_vouch']==1)])#无逾期有担保个人消费贷款账户计数
        dict_out['consume_loan_ndue_is_vouch_amount_sum']=loan_second['loanAmount'][(loan_second['businessType']=='其他个人消费贷款') & (loan_second['overdue_month']==0) & (loan_second['is_vouch']==1)].sum()#无逾期有担保个人消费贷款本金求和
        dict_out['consume_loan_ndue_is_vouch_amount_max']=loan_second['loanAmount'][(loan_second['businessType']=='其他个人消费贷款') & (loan_second['overdue_month']==0) & (loan_second['is_vouch']==1)].max()#无逾期有担保个人消费贷款本金最大

        #现行无逾期有担保个人消费贷款
        dict_out['consume_loan_ndue_is_vouch_account_count_now']=len(loan_second[(loan_second['businessType']=='其他个人消费贷款') & (loan_second['overdue_month']==0) & (loan_second['is_vouch']==1) & (loan_second['is_now']==1)])#现行无逾期有担保个人消费贷款账户计数
        dict_out['consume_loan_ndue_is_vouch_amount_sum_now']=loan_second['loanAmount'][(loan_second['businessType']=='其他个人消费贷款') & (loan_second['overdue_month']==0) & (loan_second['is_vouch']==1) & (loan_second['is_now']==1)].sum()#现行无逾期有担保个人消费贷款本金求和
        dict_out['consume_loan_ndue_is_vouch_balance_sum_now']=loan_second['balance'][(loan_second['businessType']=='其他个人消费贷款') & (loan_second['overdue_month']==0) & (loan_second['is_vouch']==1) & (loan_second['is_now']==1)].sum()#现行无逾期有担保个人消费贷款本金余额求和
        dict_out['consume_loan_ndue_is_vouch_planRepayAmount_sum_now']=loan_second['planRepayAmount'][(loan_second['businessType']=='其他个人消费贷款') & (loan_second['overdue_month']==0) & (loan_second['is_vouch']==1) & (loan_second['is_now']==1)].sum()#现行无逾期有担保个人消费贷款本月应还款求和
        dict_out['consume_loan_ndue_is_vouch_RepayedAmount_sum_now']=loan_second['RepayedAmount'][(loan_second['businessType']=='其他个人消费贷款') & (loan_second['overdue_month']==0) & (loan_second['is_vouch']==1) & (loan_second['is_now']==1)].sum()#现行无逾期有担保个人消费贷款本月实还款求和
        dict_out['consume_loan_ndue_is_vouch_amount_max_now']=loan_second['loanAmount'][(loan_second['businessType']=='其他个人消费贷款') & (loan_second['overdue_month']==0) & (loan_second['is_vouch']==1) & (loan_second['is_now']==1)].max()#现行无逾期有担保个人消费贷款本金最大
        dict_out['consume_loan_ndue_is_vouch_balance_max_now']=loan_second['balance'][(loan_second['businessType']=='其他个人消费贷款') & (loan_second['overdue_month']==0) & (loan_second['is_vouch']==1) & (loan_second['is_now']==1)].max()#现行无逾期有担保个人消费贷款本金余额最大
        dict_out['consume_loan_ndue_is_vouch_planRepayAmount_max_now']=loan_second['planRepayAmount'][(loan_second['businessType']=='其他个人消费贷款') & (loan_second['overdue_month']==0) & (loan_second['is_vouch']==1) & (loan_second['is_now']==1)].max()#现行无逾期有担保个人消费贷款本月应还款最大
        dict_out['consume_loan_ndue_is_vouch_RepayedAmount_max_now']=loan_second['RepayedAmount'][(loan_second['businessType']=='其他个人消费贷款') & (loan_second['overdue_month']==0) & (loan_second['is_vouch']==1) & (loan_second['is_now']==1)].max()#现行无逾期有担保个人消费贷款本月实还款最大

        #最近3个月无逾期个人消费贷款
        dict_out['consume_loan_ndue_account_count_03m']=len(loan_second[(loan_second['businessType']=='其他个人消费贷款') & (loan_second['overdue_month']==0) & (loan_second['month_Desc_report']<3)])#最近3个月无逾期个人消费贷款账户计数

        #现行最近3个月无逾期个人消费贷款
        dict_out['consume_loan_ndue_account_count_03m_now']=len(loan_second[(loan_second['businessType']=='其他个人消费贷款') & (loan_second['overdue_month']==0) & (loan_second['month_Desc_report']<3) & (loan_second['is_now']==1)])#现行最近3个月无逾期个人消费贷款账户计数
        dict_out['consume_loan_ndue_planRepayAmount_sum_03m_now']=loan_second['planRepayAmount'][(loan_second['businessType']=='其他个人消费贷款') & (loan_second['overdue_month']==0) & (loan_second['month_Desc_report']<3) & (loan_second['is_now']==1)].sum()#现行最近3个月无逾期个人消费贷款本月应还款求和
        dict_out['consume_loan_ndue_RepayedAmount_sum_03m_now']=loan_second['RepayedAmount'][(loan_second['businessType']=='其他个人消费贷款') & (loan_second['overdue_month']==0) & (loan_second['month_Desc_report']<3) & (loan_second['is_now']==1)].sum()#现行最近3个月无逾期个人消费贷款本月实还款求和
        dict_out['consume_loan_ndue_planRepayAmount_max_03m_now']=loan_second['planRepayAmount'][(loan_second['businessType']=='其他个人消费贷款') & (loan_second['overdue_month']==0) & (loan_second['month_Desc_report']<3) & (loan_second['is_now']==1)].max()#现行最近3个月无逾期个人消费贷款本月应还款最大
        dict_out['consume_loan_ndue_RepayedAmount_max_03m_now']=loan_second['RepayedAmount'][(loan_second['businessType']=='其他个人消费贷款') & (loan_second['overdue_month']==0) & (loan_second['month_Desc_report']<3) & (loan_second['is_now']==1)].max()#现行最近3个月无逾期个人消费贷款本月实还款最大

        #最近3个月无逾期有担保个人消费贷款
        dict_out['consume_loan_ndue_is_vouch_account_count_03m']=len(loan_second[(loan_second['businessType']=='其他个人消费贷款') & (loan_second['overdue_month']==0) & (loan_second['is_vouch']==1) & (loan_second['month_Desc_report']<3)])#最近3个月无逾期有担保个人消费贷款账户计数

        #现行最近3个月无逾期有担保个人消费贷款
        dict_out['consume_loan_ndue_is_vouch_account_count_03m_now']=len(loan_second[(loan_second['businessType']=='其他个人消费贷款') & (loan_second['overdue_month']==0) & (loan_second['is_vouch']==1) & (loan_second['month_Desc_report']<3) & (loan_second['is_now']==1)])#现行最近3个月无逾期有担保个人消费贷款账户计数
        dict_out['consume_loan_ndue_is_vouch_planRepayAmount_sum_03m_now']=loan_second['planRepayAmount'][(loan_second['businessType']=='其他个人消费贷款') & (loan_second['overdue_month']==0) & (loan_second['is_vouch']==1) & (loan_second['month_Desc_report']<3) & (loan_second['is_now']==1)].sum()#现行最近3个月无逾期有担保个人消费贷款本月应还款求和
        dict_out['consume_loan_ndue_is_vouch_RepayedAmount_sum_03m_now']=loan_second['RepayedAmount'][(loan_second['businessType']=='其他个人消费贷款') & (loan_second['overdue_month']==0) & (loan_second['is_vouch']==1) & (loan_second['month_Desc_report']<3) & (loan_second['is_now']==1)].sum()#现行最近3个月无逾期有担保个人消费贷款本月实还款求和
        dict_out['consume_loan_ndue_is_vouch_planRepayAmount_max_03m_now']=loan_second['planRepayAmount'][(loan_second['businessType']=='其他个人消费贷款') & (loan_second['overdue_month']==0) & (loan_second['is_vouch']==1) & (loan_second['month_Desc_report']<3) & (loan_second['is_now']==1)].max()#现行最近3个月无逾期有担保个人消费贷款本月应还款最大
        dict_out['consume_loan_ndue_is_vouch_RepayedAmount_max_03m_now']=loan_second['RepayedAmount'][(loan_second['businessType']=='其他个人消费贷款') & (loan_second['overdue_month']==0) & (loan_second['is_vouch']==1) & (loan_second['month_Desc_report']<3) & (loan_second['is_now']==1)].max()#现行最近3个月无逾期有担保个人消费贷款本月实还款最大

        #最近6个月无逾期个人消费贷款
        dict_out['consume_loan_ndue_account_count_06m']=len(loan_second[(loan_second['businessType']=='其他个人消费贷款') & (loan_second['overdue_month']==0) & (loan_second['month_Desc_report']<6)])#最近6个月无逾期个人消费贷款账户计数

        #现行最近6个月无逾期个人消费贷款
        dict_out['consume_loan_ndue_account_count_06m_now']=len(loan_second[(loan_second['businessType']=='其他个人消费贷款') & (loan_second['overdue_month']==0) & (loan_second['month_Desc_report']<6) & (loan_second['is_now']==1)])#现行最近6个月无逾期个人消费贷款账户计数
        dict_out['consume_loan_ndue_planRepayAmount_sum_06m_now']=loan_second['planRepayAmount'][(loan_second['businessType']=='其他个人消费贷款') & (loan_second['overdue_month']==0) & (loan_second['month_Desc_report']<6) & (loan_second['is_now']==1)].sum()#现行最近6个月无逾期个人消费贷款本月应还款求和
        dict_out['consume_loan_ndue_RepayedAmount_sum_06m_now']=loan_second['RepayedAmount'][(loan_second['businessType']=='其他个人消费贷款') & (loan_second['overdue_month']==0) & (loan_second['month_Desc_report']<6) & (loan_second['is_now']==1)].sum()#现行最近6个月无逾期个人消费贷款本月实还款求和
        dict_out['consume_loan_ndue_planRepayAmount_max_06m_now']=loan_second['planRepayAmount'][(loan_second['businessType']=='其他个人消费贷款') & (loan_second['overdue_month']==0) & (loan_second['month_Desc_report']<6) & (loan_second['is_now']==1)].max()#现行最近6个月无逾期个人消费贷款本月应还款最大
        dict_out['consume_loan_ndue_RepayedAmount_max_06m_now']=loan_second['RepayedAmount'][(loan_second['businessType']=='其他个人消费贷款') & (loan_second['overdue_month']==0) & (loan_second['month_Desc_report']<6) & (loan_second['is_now']==1)].max()#现行最近6个月无逾期个人消费贷款本月实还款最大

        #最近6个月无逾期有担保个人消费贷款
        dict_out['consume_loan_ndue_is_vouch_account_count_06m']=len(loan_second[(loan_second['businessType']=='其他个人消费贷款') & (loan_second['overdue_month']==0) & (loan_second['is_vouch']==1) & (loan_second['month_Desc_report']<6)])#最近6个月无逾期有担保个人消费贷款账户计数

        #现行最近6个月无逾期有担保个人消费贷款
        dict_out['consume_loan_ndue_is_vouch_account_count_06m_now']=len(loan_second[(loan_second['businessType']=='其他个人消费贷款') & (loan_second['overdue_month']==0) & (loan_second['is_vouch']==1) & (loan_second['month_Desc_report']<6) & (loan_second['is_now']==1)])#现行最近6个月无逾期有担保个人消费贷款账户计数
        dict_out['consume_loan_ndue_is_vouch_planRepayAmount_sum_06m_now']=loan_second['planRepayAmount'][(loan_second['businessType']=='其他个人消费贷款') & (loan_second['overdue_month']==0) & (loan_second['is_vouch']==1) & (loan_second['month_Desc_report']<6) & (loan_second['is_now']==1)].sum()#现行最近6个月无逾期有担保个人消费贷款本月应还款求和
        dict_out['consume_loan_ndue_is_vouch_RepayedAmount_sum_06m_now']=loan_second['RepayedAmount'][(loan_second['businessType']=='其他个人消费贷款') & (loan_second['overdue_month']==0) & (loan_second['is_vouch']==1) & (loan_second['month_Desc_report']<6) & (loan_second['is_now']==1)].sum()#现行最近6个月无逾期有担保个人消费贷款本月实还款求和
        dict_out['consume_loan_ndue_is_vouch_planRepayAmount_max_06m_now']=loan_second['planRepayAmount'][(loan_second['businessType']=='其他个人消费贷款') & (loan_second['overdue_month']==0) & (loan_second['is_vouch']==1) & (loan_second['month_Desc_report']<6) & (loan_second['is_now']==1)].max()#现行最近6个月无逾期有担保个人消费贷款本月应还款最大
        dict_out['consume_loan_ndue_is_vouch_RepayedAmount_max_06m_now']=loan_second['RepayedAmount'][(loan_second['businessType']=='其他个人消费贷款') & (loan_second['overdue_month']==0) & (loan_second['is_vouch']==1) & (loan_second['month_Desc_report']<6) & (loan_second['is_now']==1)].max()#现行最近6个月无逾期有担保个人消费贷款本月实还款最大

        #最近12个月无逾期个人消费贷款
        dict_out['consume_loan_ndue_account_count_12m']=len(loan_second[(loan_second['businessType']=='其他个人消费贷款') & (loan_second['overdue_month']==0) & (loan_second['month_Desc_report']<12)])#最近12个月无逾期个人消费贷款账户计数

        #现行最近12个月无逾期个人消费贷款
        dict_out['consume_loan_ndue_account_count_12m_now']=len(loan_second[(loan_second['businessType']=='其他个人消费贷款') & (loan_second['overdue_month']==0) & (loan_second['month_Desc_report']<12) & (loan_second['is_now']==1)])#现行最近12个月无逾期个人消费贷款账户计数
        dict_out['consume_loan_ndue_planRepayAmount_sum_12m_now']=loan_second['planRepayAmount'][(loan_second['businessType']=='其他个人消费贷款') & (loan_second['overdue_month']==0) & (loan_second['month_Desc_report']<12) & (loan_second['is_now']==1)].sum()#现行最近12个月无逾期个人消费贷款本月应还款求和
        dict_out['consume_loan_ndue_RepayedAmount_sum_12m_now']=loan_second['RepayedAmount'][(loan_second['businessType']=='其他个人消费贷款') & (loan_second['overdue_month']==0) & (loan_second['month_Desc_report']<12) & (loan_second['is_now']==1)].sum()#现行最近12个月无逾期个人消费贷款本月实还款求和
        dict_out['consume_loan_ndue_planRepayAmount_max_12m_now']=loan_second['planRepayAmount'][(loan_second['businessType']=='其他个人消费贷款') & (loan_second['overdue_month']==0) & (loan_second['month_Desc_report']<12) & (loan_second['is_now']==1)].max()#现行最近12个月无逾期个人消费贷款本月应还款最大
        dict_out['consume_loan_ndue_RepayedAmount_max_12m_now']=loan_second['RepayedAmount'][(loan_second['businessType']=='其他个人消费贷款') & (loan_second['overdue_month']==0) & (loan_second['month_Desc_report']<12) & (loan_second['is_now']==1)].max()#现行最近12个月无逾期个人消费贷款本月实还款最大

        #最近12个月无逾期有担保个人消费贷款
        dict_out['consume_loan_ndue_is_vouch_account_count_12m']=len(loan_second[(loan_second['businessType']=='其他个人消费贷款') & (loan_second['overdue_month']==0) & (loan_second['is_vouch']==1) & (loan_second['month_Desc_report']<12)])#最近12个月无逾期有担保个人消费贷款账户计数

        #现行最近12个月无逾期有担保个人消费贷款
        dict_out['consume_loan_ndue_is_vouch_account_count_12m_now']=len(loan_second[(loan_second['businessType']=='其他个人消费贷款') & (loan_second['overdue_month']==0) & (loan_second['is_vouch']==1) & (loan_second['month_Desc_report']<12) & (loan_second['is_now']==1)])#现行最近12个月无逾期有担保个人消费贷款账户计数
        dict_out['consume_loan_ndue_is_vouch_planRepayAmount_sum_12m_now']=loan_second['planRepayAmount'][(loan_second['businessType']=='其他个人消费贷款') & (loan_second['overdue_month']==0) & (loan_second['is_vouch']==1) & (loan_second['month_Desc_report']<12) & (loan_second['is_now']==1)].sum()#现行最近12个月无逾期有担保个人消费贷款本月应还款求和
        dict_out['consume_loan_ndue_is_vouch_RepayedAmount_sum_12m_now']=loan_second['RepayedAmount'][(loan_second['businessType']=='其他个人消费贷款') & (loan_second['overdue_month']==0) & (loan_second['is_vouch']==1) & (loan_second['month_Desc_report']<12) & (loan_second['is_now']==1)].sum()#现行最近12个月无逾期有担保个人消费贷款本月实还款求和
        dict_out['consume_loan_ndue_is_vouch_planRepayAmount_max_12m_now']=loan_second['planRepayAmount'][(loan_second['businessType']=='其他个人消费贷款') & (loan_second['overdue_month']==0) & (loan_second['is_vouch']==1) & (loan_second['month_Desc_report']<12) & (loan_second['is_now']==1)].max()#现行最近12个月无逾期有担保个人消费贷款本月应还款最大
        dict_out['consume_loan_ndue_is_vouch_RepayedAmount_max_12m_now']=loan_second['RepayedAmount'][(loan_second['businessType']=='其他个人消费贷款') & (loan_second['overdue_month']==0) & (loan_second['is_vouch']==1) & (loan_second['month_Desc_report']<12) & (loan_second['is_now']==1)].max()#现行最近12个月无逾期有担保个人消费贷款本月实还款最大

        #最近24个月无逾期个人消费贷款
        dict_out['consume_loan_ndue_account_count_24m']=len(loan_second[(loan_second['businessType']=='其他个人消费贷款') & (loan_second['overdue_month']==0) & (loan_second['month_Desc_report']<24)])#最近24个月无逾期个人消费贷款账户计数

        #现行最近24个月无逾期个人消费贷款
        dict_out['consume_loan_ndue_account_count_24m_now']=len(loan_second[(loan_second['businessType']=='其他个人消费贷款') & (loan_second['overdue_month']==0) & (loan_second['month_Desc_report']<24) & (loan_second['is_now']==1)])#现行最近24个月无逾期个人消费贷款账户计数
        dict_out['consume_loan_ndue_planRepayAmount_sum_24m_now']=loan_second['planRepayAmount'][(loan_second['businessType']=='其他个人消费贷款') & (loan_second['overdue_month']==0) & (loan_second['month_Desc_report']<24) & (loan_second['is_now']==1)].sum()#现行最近24个月无逾期个人消费贷款本月应还款求和
        dict_out['consume_loan_ndue_RepayedAmount_sum_24m_now']=loan_second['RepayedAmount'][(loan_second['businessType']=='其他个人消费贷款') & (loan_second['overdue_month']==0) & (loan_second['month_Desc_report']<24) & (loan_second['is_now']==1)].sum()#现行最近24个月无逾期个人消费贷款本月实还款求和
        dict_out['consume_loan_ndue_planRepayAmount_max_24m_now']=loan_second['planRepayAmount'][(loan_second['businessType']=='其他个人消费贷款') & (loan_second['overdue_month']==0) & (loan_second['month_Desc_report']<24) & (loan_second['is_now']==1)].max()#现行最近24个月无逾期个人消费贷款本月应还款最大
        dict_out['consume_loan_ndue_RepayedAmount_max_24m_now']=loan_second['RepayedAmount'][(loan_second['businessType']=='其他个人消费贷款') & (loan_second['overdue_month']==0) & (loan_second['month_Desc_report']<24) & (loan_second['is_now']==1)].max()#现行最近24个月无逾期个人消费贷款本月实还款最大

        #最近24个月无逾期有担保个人消费贷款
        dict_out['consume_loan_ndue_is_vouch_account_count_24m']=len(loan_second[(loan_second['businessType']=='其他个人消费贷款') & (loan_second['overdue_month']==0) & (loan_second['is_vouch']==1) & (loan_second['month_Desc_report']<24)])#最近24个月无逾期有担保个人消费贷款账户计数

        #现行最近24个月无逾期有担保个人消费贷款
        dict_out['consume_loan_ndue_is_vouch_account_count_24m_now']=len(loan_second[(loan_second['businessType']=='其他个人消费贷款') & (loan_second['overdue_month']==0) & (loan_second['is_vouch']==1) & (loan_second['month_Desc_report']<24) & (loan_second['is_now']==1)])#现行最近24个月无逾期有担保个人消费贷款账户计数
        dict_out['consume_loan_ndue_is_vouch_planRepayAmount_sum_24m_now']=loan_second['planRepayAmount'][(loan_second['businessType']=='其他个人消费贷款') & (loan_second['overdue_month']==0) & (loan_second['is_vouch']==1) & (loan_second['month_Desc_report']<24) & (loan_second['is_now']==1)].sum()#现行最近24个月无逾期有担保个人消费贷款本月应还款求和
        dict_out['consume_loan_ndue_is_vouch_RepayedAmount_sum_24m_now']=loan_second['RepayedAmount'][(loan_second['businessType']=='其他个人消费贷款') & (loan_second['overdue_month']==0) & (loan_second['is_vouch']==1) & (loan_second['month_Desc_report']<24) & (loan_second['is_now']==1)].sum()#现行最近24个月无逾期有担保个人消费贷款本月实还款求和
        dict_out['consume_loan_ndue_is_vouch_planRepayAmount_max_24m_now']=loan_second['planRepayAmount'][(loan_second['businessType']=='其他个人消费贷款') & (loan_second['overdue_month']==0) & (loan_second['is_vouch']==1) & (loan_second['month_Desc_report']<24) & (loan_second['is_now']==1)].max()#现行最近24个月无逾期有担保个人消费贷款本月应还款最大
        dict_out['consume_loan_ndue_is_vouch_RepayedAmount_max_24m_now']=loan_second['RepayedAmount'][(loan_second['businessType']=='其他个人消费贷款') & (loan_second['overdue_month']==0) & (loan_second['is_vouch']==1) & (loan_second['month_Desc_report']<24) & (loan_second['is_now']==1)].max()#现行最近24个月无逾期有担保个人消费贷款本月实还款最大

        #轻度逾期个人消费贷款
        dict_out['consume_loan_slight_overdue_account_count']=len(loan_second[(loan_second['businessType']=='其他个人消费贷款') & (loan_second['overdue_class']==1)])#轻度逾期个人消费贷款账户计数
        dict_out['consume_loan_slight_overdue_month_sum']=sum(list(map(sum,loan_second['overdue_month'][(loan_second['businessType']=='其他个人消费贷款') & (loan_second['overdue_class']==1)])))#轻度逾期个人消费贷款逾期月数求和
        dict_out['consume_loan_slight_overdue_amount_sum']=loan_second['loanAmount'][(loan_second['businessType']=='其他个人消费贷款') & (loan_second['overdue_class']==1)].sum()#轻度逾期个人消费贷款本金求和
        dict_out['consume_loan_slight_overdue_amount_max']=loan_second['loanAmount'][(loan_second['businessType']=='其他个人消费贷款') & (loan_second['overdue_class']==1)].max()#轻度逾期个人消费贷款本金最大

        #现行轻度逾期个人消费贷款
        dict_out['consume_loan_slight_overdue_account_count_now']=len(loan_second[(loan_second['businessType']=='其他个人消费贷款') & (loan_second['overdue_class']==1) & (loan_second['is_now']==1)])#现行轻度逾期个人消费贷款账户计数
        dict_out['consume_loan_slight_overdue_month_sum_now']=sum(list(map(sum,loan_second['overdue_month'][(loan_second['businessType']=='其他个人消费贷款') & (loan_second['overdue_class']==1) & (loan_second['is_now']==1)])))#现行轻度逾期个人消费贷款逾期月数求和
        dict_out['consume_loan_slight_overdue_amount_sum_now']=loan_second['loanAmount'][(loan_second['businessType']=='其他个人消费贷款') & (loan_second['overdue_class']==1) & (loan_second['is_now']==1)].sum()#现行轻度逾期个人消费贷款本金求和
        dict_out['consume_loan_slight_overdue_balance_sum_now']=loan_second['balance'][(loan_second['businessType']=='其他个人消费贷款') & (loan_second['overdue_class']==1) & (loan_second['is_now']==1)].sum()#现行轻度逾期个人消费贷款本金余额求和
        dict_out['consume_loan_slight_overdue_planRepayAmount_sum_now']=loan_second['planRepayAmount'][(loan_second['businessType']=='其他个人消费贷款') & (loan_second['overdue_class']==1) & (loan_second['is_now']==1)].sum()#现行轻度逾期个人消费贷款本月应还款求和
        dict_out['consume_loan_slight_overdue_RepayedAmount_sum_now']=loan_second['RepayedAmount'][(loan_second['businessType']=='其他个人消费贷款') & (loan_second['overdue_class']==1) & (loan_second['is_now']==1)].sum()#现行轻度逾期个人消费贷款本月实还款求和
        dict_out['consume_loan_slight_overdue_currentOverdueAmount_sum_now']=loan_second['currentOverdueAmount'][(loan_second['businessType']=='其他个人消费贷款') & (loan_second['overdue_class']==1) & (loan_second['is_now']==1)].sum()#现行轻度逾期个人消费贷款当前逾期金额求和
        dict_out['consume_loan_slight_overdue_amount_max_now']=loan_second['loanAmount'][(loan_second['businessType']=='其他个人消费贷款') & (loan_second['overdue_class']==1) & (loan_second['is_now']==1)].max()#现行轻度逾期个人消费贷款本金最大
        dict_out['consume_loan_slight_overdue_balance_max_now']=loan_second['balance'][(loan_second['businessType']=='其他个人消费贷款') & (loan_second['overdue_class']==1) & (loan_second['is_now']==1)].max()#现行轻度逾期个人消费贷款本金余额最大
        dict_out['consume_loan_slight_overdue_planRepayAmount_max_now']=loan_second['planRepayAmount'][(loan_second['businessType']=='其他个人消费贷款') & (loan_second['overdue_class']==1) & (loan_second['is_now']==1)].max()#现行轻度逾期个人消费贷款本月应还款最大
        dict_out['consume_loan_slight_overdue_RepayedAmount_max_now']=loan_second['RepayedAmount'][(loan_second['businessType']=='其他个人消费贷款') & (loan_second['overdue_class']==1) & (loan_second['is_now']==1)].max()#现行轻度逾期个人消费贷款本月实还款最大
        dict_out['consume_loan_slight_overdue_currentOverdueAmount_max_now']=loan_second['currentOverdueAmount'][(loan_second['businessType']=='其他个人消费贷款') & (loan_second['overdue_class']==1) & (loan_second['is_now']==1)].max()#现行轻度逾期个人消费贷款当前逾期金额最大

        #轻度逾期有担保个人消费贷款
        dict_out['consume_loan_is_vouch_slight_overdue_account_count']=len(loan_second[(loan_second['businessType']=='其他个人消费贷款') & (loan_second['overdue_class']==1) & (loan_second['is_vouch']==1)])#轻度逾期有担保个人消费贷款账户计数
        dict_out['consume_loan_is_vouch_slight_overdue_month_sum']=sum(list(map(sum,loan_second['overdue_month'][(loan_second['businessType']=='其他个人消费贷款') & (loan_second['overdue_class']==1) & (loan_second['is_vouch']==1)])))#轻度逾期有担保个人消费贷款逾期月数求和
        dict_out['consume_loan_is_vouch_slight_overdue_amount_sum']=loan_second['loanAmount'][(loan_second['businessType']=='其他个人消费贷款') & (loan_second['overdue_class']==1) & (loan_second['is_vouch']==1)].sum()#轻度逾期有担保个人消费贷款本金求和
        dict_out['consume_loan_is_vouch_slight_overdue_amount_max']=loan_second['loanAmount'][(loan_second['businessType']=='其他个人消费贷款') & (loan_second['overdue_class']==1) & (loan_second['is_vouch']==1)].max()#轻度逾期有担保个人消费贷款本金最大

        #现行轻度逾期有担保个人消费贷款
        dict_out['consume_loan_is_vouch_slight_overdue_account_count_now']=len(loan_second[(loan_second['businessType']=='其他个人消费贷款') & (loan_second['overdue_class']==1) & (loan_second['is_vouch']==1) & (loan_second['is_now']==1)])#现行轻度逾期有担保个人消费贷款账户计数
        dict_out['consume_loan_is_vouch_slight_overdue_month_sum_now']=sum(list(map(sum,loan_second['overdue_month'][(loan_second['businessType']=='其他个人消费贷款') & (loan_second['overdue_class']==1) & (loan_second['is_vouch']==1) & (loan_second['is_now']==1)])))#现行轻度逾期有担保个人消费贷款逾期月数求和
        dict_out['consume_loan_is_vouch_slight_overdue_amount_sum_now']=loan_second['loanAmount'][(loan_second['businessType']=='其他个人消费贷款') & (loan_second['overdue_class']==1) & (loan_second['is_vouch']==1) & (loan_second['is_now']==1)].sum()#现行轻度逾期有担保个人消费贷款本金求和
        dict_out['consume_loan_is_vouch_slight_overdue_balance_sum_now']=loan_second['balance'][(loan_second['businessType']=='其他个人消费贷款') & (loan_second['overdue_class']==1) & (loan_second['is_vouch']==1) & (loan_second['is_now']==1)].sum()#现行轻度逾期有担保个人消费贷款本金余额求和
        dict_out['consume_loan_is_vouch_slight_overdue_planRepayAmount_sum_now']=loan_second['planRepayAmount'][(loan_second['businessType']=='其他个人消费贷款') & (loan_second['overdue_class']==1) & (loan_second['is_vouch']==1) & (loan_second['is_now']==1)].sum()#现行轻度逾期有担保个人消费贷款本月应还款求和
        dict_out['consume_loan_is_vouch_slight_overdue_RepayedAmount_sum_now']=loan_second['RepayedAmount'][(loan_second['businessType']=='其他个人消费贷款') & (loan_second['overdue_class']==1) & (loan_second['is_vouch']==1) & (loan_second['is_now']==1)].sum()#现行轻度逾期有担保个人消费贷款本月实还款求和
        dict_out['consume_loan_is_vouch_slight_overdue_currentOverdueAmount_sum_now']=loan_second['currentOverdueAmount'][(loan_second['businessType']=='其他个人消费贷款') & (loan_second['overdue_class']==1) & (loan_second['is_vouch']==1) & (loan_second['is_now']==1)].sum()#现行轻度逾期有担保个人消费贷款当前逾期金额求和
        dict_out['consume_loan_is_vouch_slight_overdue_amount_max_now']=loan_second['loanAmount'][(loan_second['businessType']=='其他个人消费贷款') & (loan_second['overdue_class']==1) & (loan_second['is_vouch']==1) & (loan_second['is_now']==1)].max()#现行轻度逾期有担保个人消费贷款本金最大
        dict_out['consume_loan_is_vouch_slight_overdue_balance_max_now']=loan_second['balance'][(loan_second['businessType']=='其他个人消费贷款') & (loan_second['overdue_class']==1) & (loan_second['is_vouch']==1) & (loan_second['is_now']==1)].max()#现行轻度逾期有担保个人消费贷款本金余额最大
        dict_out['consume_loan_is_vouch_slight_overdue_planRepayAmount_max_now']=loan_second['planRepayAmount'][(loan_second['businessType']=='其他个人消费贷款') & (loan_second['overdue_class']==1) & (loan_second['is_vouch']==1) & (loan_second['is_now']==1)].max()#现行轻度逾期有担保个人消费贷款本月应还款最大
        dict_out['consume_loan_is_vouch_slight_overdue_RepayedAmount_max_now']=loan_second['RepayedAmount'][(loan_second['businessType']=='其他个人消费贷款') & (loan_second['overdue_class']==1) & (loan_second['is_vouch']==1) & (loan_second['is_now']==1)].max()#现行轻度逾期有担保个人消费贷款本月实还款最大
        dict_out['consume_loan_is_vouch_slight_overdue_currentOverdueAmount_max_now']=loan_second['currentOverdueAmount'][(loan_second['businessType']=='其他个人消费贷款') & (loan_second['overdue_class']==1) & (loan_second['is_vouch']==1) & (loan_second['is_now']==1)].max()#现行轻度逾期有担保个人消费贷款当前逾期金额最大

        #当前轻度逾期贷款
        dict_out['loan_recent_slight_overdue_account_count']=len(loan_second[(loan_second['recent_overdue']==1) & (loan_second['overdue_class']==1)])#当前轻度逾期贷款账户计数
        dict_out['loan_recent_slight_overdue_month_sum']=sum(list(map(sum,loan_second['overdue_month'][(loan_second['recent_overdue']==1) & (loan_second['overdue_class']==1)])))#当前轻度逾期贷款逾期月数求和
        dict_out['loan_recent_slight_overdue_balance_sum']=loan_second['balance'][(loan_second['recent_overdue']==1) & (loan_second['overdue_class']==1)].sum()#当前轻度逾期贷款本金余额求和
        dict_out['loan_recent_slight_overdue_planRepayAmount_sum']=loan_second['planRepayAmount'][(loan_second['recent_overdue']==1) & (loan_second['overdue_class']==1)].sum()#当前轻度逾期贷款本月应还款求和
        dict_out['loan_recent_slight_overdue_RepayedAmount_sum']=loan_second['RepayedAmount'][(loan_second['recent_overdue']==1) & (loan_second['overdue_class']==1)].sum()#当前轻度逾期贷款本月实还款求和
        dict_out['loan_recent_slight_overdue_currentOverdueAmount_sum']=loan_second['currentOverdueAmount'][(loan_second['recent_overdue']==1) & (loan_second['overdue_class']==1)].sum()#当前轻度逾期贷款当前逾期金额求和
        dict_out['loan_recent_slight_overdue_balance_max']=loan_second['balance'][(loan_second['recent_overdue']==1) & (loan_second['overdue_class']==1)].max()#当前轻度逾期贷款本金余额最大
        dict_out['loan_recent_slight_overdue_planRepayAmount_max']=loan_second['planRepayAmount'][(loan_second['recent_overdue']==1) & (loan_second['overdue_class']==1)].max()#当前轻度逾期贷款本月应还款最大
        dict_out['loan_recent_slight_overdue_RepayedAmount_max']=loan_second['RepayedAmount'][(loan_second['recent_overdue']==1) & (loan_second['overdue_class']==1)].max()#当前轻度逾期贷款本月实还款最大
        dict_out['loan_recent_slight_overdue_currentOverdueAmount_max']=loan_second['currentOverdueAmount'][(loan_second['recent_overdue']==1) & (loan_second['overdue_class']==1)].max()#当前轻度逾期贷款当前逾期金额最大

        #最近3个月轻度逾期贷款
        dict_out['loan_slight_overdue_account_count_03m']=len(loan_second[(loan_second['overdue_class']==1) & (loan_second['month_Desc_report']<3)])#最近3个月轻度逾期贷款账户计数
        dict_out['loan_slight_overdue_month_sum_03m']=sum(list(map(sum,loan_second['overdue_month'][(loan_second['overdue_class']==1) & (loan_second['month_Desc_report']<3)])))#最近3个月轻度逾期贷款逾期月数求和

        #现行最近3个月轻度逾期贷款
        dict_out['loan_slight_overdue_account_count_03m_now']=len(loan_second[(loan_second['overdue_class']==1) & (loan_second['month_Desc_report']<3) & (loan_second['is_now']==1)])#现行最近3个月轻度逾期贷款账户计数
        dict_out['loan_slight_overdue_month_sum_03m_now']=sum(list(map(sum,loan_second['overdue_month'][(loan_second['overdue_class']==1) & (loan_second['month_Desc_report']<3) & (loan_second['is_now']==1)])))#现行最近3个月轻度逾期贷款逾期月数求和
        dict_out['loan_slight_overdue_planRepayAmount_sum_03m_now']=loan_second['planRepayAmount'][(loan_second['overdue_class']==1) & (loan_second['month_Desc_report']<3) & (loan_second['is_now']==1)].sum()#现行最近3个月轻度逾期贷款本月应还款求和
        dict_out['loan_slight_overdue_RepayedAmount_sum_03m_now']=loan_second['RepayedAmount'][(loan_second['overdue_class']==1) & (loan_second['month_Desc_report']<3) & (loan_second['is_now']==1)].sum()#现行最近3个月轻度逾期贷款本月实还款求和
        dict_out['loan_slight_overdue_planRepayAmount_max_03m_now']=loan_second['planRepayAmount'][(loan_second['overdue_class']==1) & (loan_second['month_Desc_report']<3) & (loan_second['is_now']==1)].max()#现行最近3个月轻度逾期贷款本月应还款最大
        dict_out['loan_slight_overdue_RepayedAmount_max_03m_now']=loan_second['RepayedAmount'][(loan_second['overdue_class']==1) & (loan_second['month_Desc_report']<3) & (loan_second['is_now']==1)].max()#现行最近3个月轻度逾期贷款本月实还款最大

        #最近3个月轻度逾期有担保贷款
        dict_out['loan_is_vouch_slight_overdue_account_count_03m']=len(loan_second[(loan_second['overdue_class']==1) & (loan_second['month_Desc_report']<3) & (loan_second['is_vouch']==1)])#最近3个月轻度逾期有担保贷款账户计数
        dict_out['loan_is_vouch_slight_overdue_month_sum_03m']=sum(list(map(sum,loan_second['overdue_month'][(loan_second['overdue_class']==1) & (loan_second['month_Desc_report']<3) & (loan_second['is_vouch']==1)])))#最近3个月轻度逾期有担保贷款逾期月数求和

        #现行最近3个月轻度逾期有担保贷款
        dict_out['loan_is_vouch_slight_overdue_account_count_03m_now']=len(loan_second[(loan_second['overdue_class']==1) & (loan_second['month_Desc_report']<3) & (loan_second['is_vouch']==1) & (loan_second['is_now']==1)])#现行最近3个月轻度逾期有担保贷款账户计数
        dict_out['loan_is_vouch_slight_overdue_month_sum_03m_now']=sum(list(map(sum,loan_second['overdue_month'][(loan_second['overdue_class']==1) & (loan_second['month_Desc_report']<3) & (loan_second['is_vouch']==1) & (loan_second['is_now']==1)])))#现行最近3个月轻度逾期有担保贷款逾期月数求和
        dict_out['loan_is_vouch_slight_overdue_planRepayAmount_sum_03m_now']=loan_second['planRepayAmount'][(loan_second['overdue_class']==1) & (loan_second['month_Desc_report']<3) & (loan_second['is_vouch']==1) & (loan_second['is_now']==1)].sum()#现行最近3个月轻度逾期有担保贷款本月应还款求和
        dict_out['loan_is_vouch_slight_overdue_RepayedAmount_sum_03m_now']=loan_second['RepayedAmount'][(loan_second['overdue_class']==1) & (loan_second['month_Desc_report']<3) & (loan_second['is_vouch']==1) & (loan_second['is_now']==1)].sum()#现行最近3个月轻度逾期有担保贷款本月实还款求和
        dict_out['loan_is_vouch_slight_overdue_planRepayAmount_max_03m_now']=loan_second['planRepayAmount'][(loan_second['overdue_class']==1) & (loan_second['month_Desc_report']<3) & (loan_second['is_vouch']==1) & (loan_second['is_now']==1)].max()#现行最近3个月轻度逾期有担保贷款本月应还款最大
        dict_out['loan_is_vouch_slight_overdue_RepayedAmount_max_03m_now']=loan_second['RepayedAmount'][(loan_second['overdue_class']==1) & (loan_second['month_Desc_report']<3) & (loan_second['is_vouch']==1) & (loan_second['is_now']==1)].max()#现行最近3个月轻度逾期有担保贷款本月实还款最大

        #最近6个月轻度逾期贷款
        dict_out['loan_slight_overdue_account_count_06m']=len(loan_second[(loan_second['overdue_class']==1) & (loan_second['month_Desc_report']<6)])#最近6个月轻度逾期贷款账户计数
        dict_out['loan_slight_overdue_month_sum_06m']=sum(list(map(sum,loan_second['overdue_month'][(loan_second['overdue_class']==1) & (loan_second['month_Desc_report']<6)])))#最近6个月轻度逾期贷款逾期月数求和

        #现行最近6个月轻度逾期贷款
        dict_out['loan_slight_overdue_account_count_06m_now']=len(loan_second[(loan_second['overdue_class']==1) & (loan_second['month_Desc_report']<6) & (loan_second['is_now']==1)])#现行最近6个月轻度逾期贷款账户计数
        dict_out['loan_slight_overdue_month_sum_06m_now']=sum(list(map(sum,loan_second['overdue_month'][(loan_second['overdue_class']==1) & (loan_second['month_Desc_report']<6) & (loan_second['is_now']==1)])))#现行最近6个月轻度逾期贷款逾期月数求和
        dict_out['loan_slight_overdue_planRepayAmount_sum_06m_now']=loan_second['planRepayAmount'][(loan_second['overdue_class']==1) & (loan_second['month_Desc_report']<6) & (loan_second['is_now']==1)].sum()#现行最近6个月轻度逾期贷款本月应还款求和
        dict_out['loan_slight_overdue_RepayedAmount_sum_06m_now']=loan_second['RepayedAmount'][(loan_second['overdue_class']==1) & (loan_second['month_Desc_report']<6) & (loan_second['is_now']==1)].sum()#现行最近6个月轻度逾期贷款本月实还款求和
        dict_out['loan_slight_overdue_planRepayAmount_max_06m_now']=loan_second['planRepayAmount'][(loan_second['overdue_class']==1) & (loan_second['month_Desc_report']<6) & (loan_second['is_now']==1)].max()#现行最近6个月轻度逾期贷款本月应还款最大
        dict_out['loan_slight_overdue_RepayedAmount_max_06m_now']=loan_second['RepayedAmount'][(loan_second['overdue_class']==1) & (loan_second['month_Desc_report']<6) & (loan_second['is_now']==1)].max()#现行最近6个月轻度逾期贷款本月实还款最大

        #最近6个月轻度逾期有担保贷款
        dict_out['loan_is_vouch_slight_overdue_account_count_06m']=len(loan_second[(loan_second['overdue_class']==1) & (loan_second['month_Desc_report']<6) & (loan_second['is_vouch']==1)])#最近6个月轻度逾期有担保贷款账户计数
        dict_out['loan_is_vouch_slight_overdue_month_sum_06m']=sum(list(map(sum,loan_second['overdue_month'][(loan_second['overdue_class']==1) & (loan_second['month_Desc_report']<6) & (loan_second['is_vouch']==1)])))#最近6个月轻度逾期有担保贷款逾期月数求和

        #现行最近6个月轻度逾期有担保贷款
        dict_out['loan_is_vouch_slight_overdue_account_count_06m_now']=len(loan_second[(loan_second['overdue_class']==1) & (loan_second['month_Desc_report']<6) & (loan_second['is_vouch']==1) & (loan_second['is_now']==1)])#现行最近6个月轻度逾期有担保贷款账户计数
        dict_out['loan_is_vouch_slight_overdue_month_sum_06m_now']=sum(list(map(sum,loan_second['overdue_month'][(loan_second['overdue_class']==1) & (loan_second['month_Desc_report']<6) & (loan_second['is_vouch']==1) & (loan_second['is_now']==1)])))#现行最近6个月轻度逾期有担保贷款逾期月数求和
        dict_out['loan_is_vouch_slight_overdue_planRepayAmount_sum_06m_now']=loan_second['planRepayAmount'][(loan_second['overdue_class']==1) & (loan_second['month_Desc_report']<6) & (loan_second['is_vouch']==1) & (loan_second['is_now']==1)].sum()#现行最近6个月轻度逾期有担保贷款本月应还款求和
        dict_out['loan_is_vouch_slight_overdue_RepayedAmount_sum_06m_now']=loan_second['RepayedAmount'][(loan_second['overdue_class']==1) & (loan_second['month_Desc_report']<6) & (loan_second['is_vouch']==1) & (loan_second['is_now']==1)].sum()#现行最近6个月轻度逾期有担保贷款本月实还款求和
        dict_out['loan_is_vouch_slight_overdue_planRepayAmount_max_06m_now']=loan_second['planRepayAmount'][(loan_second['overdue_class']==1) & (loan_second['month_Desc_report']<6) & (loan_second['is_vouch']==1) & (loan_second['is_now']==1)].max()#现行最近6个月轻度逾期有担保贷款本月应还款最大
        dict_out['loan_is_vouch_slight_overdue_RepayedAmount_max_06m_now']=loan_second['RepayedAmount'][(loan_second['overdue_class']==1) & (loan_second['month_Desc_report']<6) & (loan_second['is_vouch']==1) & (loan_second['is_now']==1)].max()#现行最近6个月轻度逾期有担保贷款本月实还款最大

        #最近12个月轻度逾期贷款
        dict_out['loan_slight_overdue_account_count_12m']=len(loan_second[(loan_second['overdue_class']==1) & (loan_second['month_Desc_report']<12)])#最近12个月轻度逾期贷款账户计数
        dict_out['loan_slight_overdue_month_sum_12m']=sum(list(map(sum,loan_second['overdue_month'][(loan_second['overdue_class']==1) & (loan_second['month_Desc_report']<12)])))#最近12个月轻度逾期贷款逾期月数求和

        #现行最近12个月轻度逾期贷款
        dict_out['loan_slight_overdue_account_count_12m_now']=len(loan_second[(loan_second['overdue_class']==1) & (loan_second['month_Desc_report']<12) & (loan_second['is_now']==1)])#现行最近12个月轻度逾期贷款账户计数
        dict_out['loan_slight_overdue_month_sum_12m_now']=sum(list(map(sum,loan_second['overdue_month'][(loan_second['overdue_class']==1) & (loan_second['month_Desc_report']<12) & (loan_second['is_now']==1)])))#现行最近12个月轻度逾期贷款逾期月数求和
        dict_out['loan_slight_overdue_planRepayAmount_sum_12m_now']=loan_second['planRepayAmount'][(loan_second['overdue_class']==1) & (loan_second['month_Desc_report']<12) & (loan_second['is_now']==1)].sum()#现行最近12个月轻度逾期贷款本月应还款求和
        dict_out['loan_slight_overdue_RepayedAmount_sum_12m_now']=loan_second['RepayedAmount'][(loan_second['overdue_class']==1) & (loan_second['month_Desc_report']<12) & (loan_second['is_now']==1)].sum()#现行最近12个月轻度逾期贷款本月实还款求和
        dict_out['loan_slight_overdue_planRepayAmount_max_12m_now']=loan_second['planRepayAmount'][(loan_second['overdue_class']==1) & (loan_second['month_Desc_report']<12) & (loan_second['is_now']==1)].max()#现行最近12个月轻度逾期贷款本月应还款最大
        dict_out['loan_slight_overdue_RepayedAmount_max_12m_now']=loan_second['RepayedAmount'][(loan_second['overdue_class']==1) & (loan_second['month_Desc_report']<12) & (loan_second['is_now']==1)].max()#现行最近12个月轻度逾期贷款本月实还款最大

        #最近12个月轻度逾期有担保贷款
        dict_out['loan_is_vouch_slight_overdue_account_count_12m']=len(loan_second[(loan_second['overdue_class']==1) & (loan_second['month_Desc_report']<12) & (loan_second['is_vouch']==1)])#最近12个月轻度逾期有担保贷款账户计数
        dict_out['loan_is_vouch_slight_overdue_month_sum_12m']=sum(list(map(sum,loan_second['overdue_month'][(loan_second['overdue_class']==1) & (loan_second['month_Desc_report']<12) & (loan_second['is_vouch']==1)])))#最近12个月轻度逾期有担保贷款逾期月数求和

        #现行最近12个月轻度逾期有担保贷款
        dict_out['loan_is_vouch_slight_overdue_account_count_12m_now']=len(loan_second[(loan_second['overdue_class']==1) & (loan_second['month_Desc_report']<12) & (loan_second['is_vouch']==1) & (loan_second['is_now']==1)])#现行最近12个月轻度逾期有担保贷款账户计数
        dict_out['loan_is_vouch_slight_overdue_month_sum_12m_now']=sum(list(map(sum,loan_second['overdue_month'][(loan_second['overdue_class']==1) & (loan_second['month_Desc_report']<12) & (loan_second['is_vouch']==1) & (loan_second['is_now']==1)])))#现行最近12个月轻度逾期有担保贷款逾期月数求和
        dict_out['loan_is_vouch_slight_overdue_planRepayAmount_sum_12m_now']=loan_second['planRepayAmount'][(loan_second['overdue_class']==1) & (loan_second['month_Desc_report']<12) & (loan_second['is_vouch']==1) & (loan_second['is_now']==1)].sum()#现行最近12个月轻度逾期有担保贷款本月应还款求和
        dict_out['loan_is_vouch_slight_overdue_RepayedAmount_sum_12m_now']=loan_second['RepayedAmount'][(loan_second['overdue_class']==1) & (loan_second['month_Desc_report']<12) & (loan_second['is_vouch']==1) & (loan_second['is_now']==1)].sum()#现行最近12个月轻度逾期有担保贷款本月实还款求和
        dict_out['loan_is_vouch_slight_overdue_planRepayAmount_max_12m_now']=loan_second['planRepayAmount'][(loan_second['overdue_class']==1) & (loan_second['month_Desc_report']<12) & (loan_second['is_vouch']==1) & (loan_second['is_now']==1)].max()#现行最近12个月轻度逾期有担保贷款本月应还款最大
        dict_out['loan_is_vouch_slight_overdue_RepayedAmount_max_12m_now']=loan_second['RepayedAmount'][(loan_second['overdue_class']==1) & (loan_second['month_Desc_report']<12) & (loan_second['is_vouch']==1) & (loan_second['is_now']==1)].max()#现行最近12个月轻度逾期有担保贷款本月实还款最大

        #最近24个月轻度逾期贷款
        dict_out['loan_slight_overdue_account_count_24m']=len(loan_second[(loan_second['overdue_class']==1) & (loan_second['month_Desc_report']<24)])#最近24个月轻度逾期贷款账户计数
        dict_out['loan_slight_overdue_month_sum_24m']=sum(list(map(sum,loan_second['overdue_month'][(loan_second['overdue_class']==1) & (loan_second['month_Desc_report']<24)])))#最近24个月轻度逾期贷款逾期月数求和

        #现行最近24个月轻度逾期贷款
        dict_out['loan_slight_overdue_account_count_24m_now']=len(loan_second[(loan_second['overdue_class']==1) & (loan_second['month_Desc_report']<24) & (loan_second['is_now']==1)])#现行最近24个月轻度逾期贷款账户计数
        dict_out['loan_slight_overdue_month_sum_24m_now']=sum(list(map(sum,loan_second['overdue_month'][(loan_second['overdue_class']==1) & (loan_second['month_Desc_report']<24) & (loan_second['is_now']==1)])))#现行最近24个月轻度逾期贷款逾期月数求和
        dict_out['loan_slight_overdue_planRepayAmount_sum_24m_now']=loan_second['planRepayAmount'][(loan_second['overdue_class']==1) & (loan_second['month_Desc_report']<24) & (loan_second['is_now']==1)].sum()#现行最近24个月轻度逾期贷款本月应还款求和
        dict_out['loan_slight_overdue_RepayedAmount_sum_24m_now']=loan_second['RepayedAmount'][(loan_second['overdue_class']==1) & (loan_second['month_Desc_report']<24) & (loan_second['is_now']==1)].sum()#现行最近24个月轻度逾期贷款本月实还款求和
        dict_out['loan_slight_overdue_planRepayAmount_max_24m_now']=loan_second['planRepayAmount'][(loan_second['overdue_class']==1) & (loan_second['month_Desc_report']<24) & (loan_second['is_now']==1)].max()#现行最近24个月轻度逾期贷款本月应还款最大
        dict_out['loan_slight_overdue_RepayedAmount_max_24m_now']=loan_second['RepayedAmount'][(loan_second['overdue_class']==1) & (loan_second['month_Desc_report']<24) & (loan_second['is_now']==1)].max()#现行最近24个月轻度逾期贷款本月实还款最大

        #最近24个月轻度逾期有担保贷款
        dict_out['loan_is_vouch_slight_overdue_account_count_24m']=len(loan_second[(loan_second['overdue_class']==1) & (loan_second['month_Desc_report']<24) & (loan_second['is_vouch']==1)])#最近24个月轻度逾期有担保贷款账户计数
        dict_out['loan_is_vouch_slight_overdue_month_sum_24m']=sum(list(map(sum,loan_second['overdue_month'][(loan_second['overdue_class']==1) & (loan_second['month_Desc_report']<24) & (loan_second['is_vouch']==1)])))#最近24个月轻度逾期有担保贷款逾期月数求和

        #现行最近24个月轻度逾期有担保贷款
        dict_out['loan_is_vouch_slight_overdue_account_count_24m_now']=len(loan_second[(loan_second['overdue_class']==1) & (loan_second['month_Desc_report']<24) & (loan_second['is_vouch']==1) & (loan_second['is_now']==1)])#现行最近24个月轻度逾期有担保贷款账户计数
        dict_out['loan_is_vouch_slight_overdue_month_sum_24m_now']=loan_second['overdue_month'][(loan_second['overdue_class']==1) & (loan_second['month_Desc_report']<24) & (loan_second['is_vouch']==1) & (loan_second['is_now']==1)].sum()#现行最近24个月轻度逾期有担保贷款逾期月数求和
        dict_out['loan_is_vouch_slight_overdue_planRepayAmount_sum_24m_now']=loan_second['planRepayAmount'][(loan_second['overdue_class']==1) & (loan_second['month_Desc_report']<24) & (loan_second['is_vouch']==1) & (loan_second['is_now']==1)].sum()#现行最近24个月轻度逾期有担保贷款本月应还款求和
        dict_out['loan_is_vouch_slight_overdue_RepayedAmount_sum_24m_now']=loan_second['RepayedAmount'][(loan_second['overdue_class']==1) & (loan_second['month_Desc_report']<24) & (loan_second['is_vouch']==1) & (loan_second['is_now']==1)].sum()#现行最近24个月轻度逾期有担保贷款本月实还款求和
        dict_out['loan_is_vouch_slight_overdue_planRepayAmount_max_24m_now']=loan_second['planRepayAmount'][(loan_second['overdue_class']==1) & (loan_second['month_Desc_report']<24) & (loan_second['is_vouch']==1) & (loan_second['is_now']==1)].max()#现行最近24个月轻度逾期有担保贷款本月应还款最大
        dict_out['loan_is_vouch_slight_overdue_RepayedAmount_max_24m_now']=loan_second['RepayedAmount'][(loan_second['overdue_class']==1) & (loan_second['month_Desc_report']<24) & (loan_second['is_vouch']==1) & (loan_second['is_now']==1)].max()#现行最近24个月轻度逾期有担保贷款本月实还款最大

        #严重逾期贷款
        dict_out['loan_serious_overdue_account_count']=len(loan_second[(loan_second['overdue_class']==3)])#严重逾期贷款账户计数
        dict_out['loan_serious_overdue_month_sum']=sum(list(map(sum,loan_second['overdue_month'][(loan_second['overdue_class']==3)])))#严重逾期贷款逾期月数求和
        dict_out['loan_serious_overdue_amount_sum']=loan_second['loanAmount'][(loan_second['overdue_class']==3)].sum()#严重逾期贷款本金求和
        dict_out['loan_serious_overdue_amount_max']=loan_second['loanAmount'][(loan_second['overdue_class']==3)].max()#严重逾期贷款本金最大

        #现行严重逾期贷款
        dict_out['loan_serious_overdue_account_count_now']=len(loan_second[(loan_second['overdue_class']==3) & (loan_second['is_now']==1)])#现行严重逾期贷款账户计数
        dict_out['loan_serious_overdue_month_sum_now']=sum(list(map(sum,loan_second['overdue_month'][(loan_second['overdue_class']==3) & (loan_second['is_now']==1)])))#现行严重逾期贷款逾期月数求和
        dict_out['loan_serious_overdue_amount_sum_now']=loan_second['loanAmount'][(loan_second['overdue_class']==3) & (loan_second['is_now']==1)].sum()#现行严重逾期贷款本金求和
        dict_out['loan_serious_overdue_balance_sum_now']=loan_second['balance'][(loan_second['overdue_class']==3) & (loan_second['is_now']==1)].sum()#现行严重逾期贷款本金余额求和
        dict_out['loan_serious_overdue_planRepayAmount_sum_now']=loan_second['planRepayAmount'][(loan_second['overdue_class']==3) & (loan_second['is_now']==1)].sum()#现行严重逾期贷款本月应还款求和
        dict_out['loan_serious_overdue_RepayedAmount_sum_now']=loan_second['RepayedAmount'][(loan_second['overdue_class']==3) & (loan_second['is_now']==1)].sum()#现行严重逾期贷款本月实还款求和
        dict_out['loan_serious_overdue_currentOverdueAmount_sum_now']=loan_second['currentOverdueAmount'][(loan_second['overdue_class']==3) & (loan_second['is_now']==1)].sum()#现行严重逾期贷款当前逾期金额求和
        dict_out['loan_serious_overdue_amount_max_now']=loan_second['loanAmount'][(loan_second['overdue_class']==3) & (loan_second['is_now']==1)].max()#现行严重逾期贷款本金最大
        dict_out['loan_serious_overdue_balance_max_now']=loan_second['balance'][(loan_second['overdue_class']==3) & (loan_second['is_now']==1)].max()#现行严重逾期贷款本金余额最大
        dict_out['loan_serious_overdue_planRepayAmount_max_now']=loan_second['planRepayAmount'][(loan_second['overdue_class']==3) & (loan_second['is_now']==1)].max()#现行严重逾期贷款本月应还款最大
        dict_out['loan_serious_overdue_RepayedAmount_max_now']=loan_second['RepayedAmount'][(loan_second['overdue_class']==3) & (loan_second['is_now']==1)].max()#现行严重逾期贷款本月实还款最大
        dict_out['loan_serious_overdue_currentOverdueAmount_max_now']=loan_second['currentOverdueAmount'][(loan_second['overdue_class']==3) & (loan_second['is_now']==1)].max()#现行严重逾期贷款当前逾期金额最大

        #严重逾期有担保贷款
        dict_out['loan_is_vouch_serious_overdue_account_count']=len(loan_second[(loan_second['overdue_class']==3) & (loan_second['is_vouch']==1)])#严重逾期有担保贷款账户计数
        dict_out['loan_is_vouch_serious_overdue_month_sum']=sum(list(map(sum,loan_second['overdue_month'][(loan_second['overdue_class']==3) & (loan_second['is_vouch']==1)])))#严重逾期有担保贷款逾期月数求和
        dict_out['loan_is_vouch_serious_overdue_aomunt_sum']=loan_second['loanAmount'][(loan_second['overdue_class']==3) & (loan_second['is_vouch']==1)].sum()#严重逾期有担保贷款本金求和
        dict_out['loan_is_vouch_serious_overdue_aomunt_max']=loan_second['loanAmount'][(loan_second['overdue_class']==3) & (loan_second['is_vouch']==1)].max()#严重逾期有担保贷款本金最大

        #行严重逾期有担保贷款
        dict_out['loan_is_vouch_serious_overdue_account_count_now']=len(loan_second[(loan_second['overdue_class']==3) & (loan_second['is_vouch']==1) & (loan_second['is_now']==1)])#现行严重逾期有担保贷款账户计数
        dict_out['loan_is_vouch_serious_overdue_month_sum_now']=sum(list(map(sum,loan_second['overdue_month'][(loan_second['overdue_class']==3) & (loan_second['is_vouch']==1) & (loan_second['is_now']==1)])))#现行严重逾期有担保贷款逾期月数求和
        dict_out['loan_is_vouch_serious_overdue_amount_sum_now']=loan_second['loanAmount'][(loan_second['overdue_class']==3) & (loan_second['is_vouch']==1) & (loan_second['is_now']==1)].sum()#现行严重逾期有担保贷款本金求和
        dict_out['loan_is_vouch_serious_overdue_balance_sum_now']=loan_second['balance'][(loan_second['overdue_class']==3) & (loan_second['is_vouch']==1) & (loan_second['is_now']==1)].sum()#现行严重逾期有担保贷款本金余额求和
        dict_out['loan_is_vouch_serious_overdue_planRepayAmount_sum_now']=loan_second['planRepayAmount'][(loan_second['overdue_class']==3) & (loan_second['is_vouch']==1) & (loan_second['is_now']==1)].sum()#现行严重逾期有担保贷款本月应还款求和
        dict_out['loan_is_vouch_serious_overdue_RepayedAmount_sum_now']=loan_second['RepayedAmount'][(loan_second['overdue_class']==3) & (loan_second['is_vouch']==1) & (loan_second['is_now']==1)].sum()#现行严重逾期有担保贷款本月实还款求和
        dict_out['loan_is_vouch_serious_overdue_currentOverdueAmount_sum_now']=loan_second['currentOverdueAmount'][(loan_second['overdue_class']==3) & (loan_second['is_vouch']==1) & (loan_second['is_now']==1)].sum()#现行严重逾期有担保贷款当前逾期金额求和
        dict_out['loan_is_vouch_serious_overdue_amount_max_now']=loan_second['loanAmount'][(loan_second['overdue_class']==3) & (loan_second['is_vouch']==1) & (loan_second['is_now']==1)].max()#现行严重逾期有担保贷款本金最大
        dict_out['loan_is_vouch_serious_overdue_balance_max_now']=loan_second['balance'][(loan_second['overdue_class']==3) & (loan_second['is_vouch']==1) & (loan_second['is_now']==1)].max()#现行严重逾期有担保贷款本金余额最大
        dict_out['loan_is_vouch_serious_overdue_planRepayAmount_max_now']=loan_second['planRepayAmount'][(loan_second['overdue_class']==3) & (loan_second['is_vouch']==1) & (loan_second['is_now']==1)].max()#现行严重逾期有担保贷款本月应还款最大
        dict_out['loan_is_vouch_serious_overdue_RepayedAmount_max_now']=loan_second['RepayedAmount'][(loan_second['overdue_class']==3) & (loan_second['is_vouch']==1) & (loan_second['is_now']==1)].max()#现行严重逾期有担保贷款本月实还款最大
        dict_out['loan_is_vouch_serious_overdue_currentOverdueAmount_max_now']=loan_second['currentOverdueAmount'][(loan_second['overdue_class']==3) & (loan_second['is_vouch']==1) & (loan_second['is_now']==1)].max()#现行严重逾期有担保贷款当前逾期金额最大

        #当前严重逾期贷款
        dict_out['loan_recent_serious_overdue_account_count']=len(loan_second[(loan_second['overdue_class']==3) & (loan_second['recent_overdue']==1)])#当前严重逾期贷款账户计数
        dict_out['loan_recent_serious_overdue_month_sum']=sum(list(map(sum,loan_second['overdue_month'][(loan_second['overdue_class']==3) & (loan_second['recent_overdue']==1)])))#当前严重逾期贷款逾期月数求和
        dict_out['loan_recent_serious_overdue_balance_sum']=loan_second['balance'][(loan_second['overdue_class']==3) & (loan_second['recent_overdue']==1)].sum()#当前严重逾期贷款本金余额求和
        dict_out['loan_recent_serious_overdue_planRepayAmount_sum']=loan_second['planRepayAmount'][(loan_second['overdue_class']==3) & (loan_second['recent_overdue']==1)].sum()#当前严重逾期贷款本月应还款求和
        dict_out['loan_recent_serious_overdue_RepayedAmount_sum']=loan_second['RepayedAmount'][(loan_second['overdue_class']==3) & (loan_second['recent_overdue']==1)].sum()#当前严重逾期贷款本月实还款求和
        dict_out['loan_recent_serious_overdue_currentOverdueAmount_sum']=loan_second['currentOverdueAmount'][(loan_second['overdue_class']==3) & (loan_second['recent_overdue']==1)].sum()#当前严重逾期贷款当前逾期金额求和
        dict_out['loan_recent_serious_overdue_balance_max']=loan_second['balance'][(loan_second['overdue_class']==3) & (loan_second['recent_overdue']==1)].max()#当前严重逾期贷款本金余额最大
        dict_out['loan_recent_serious_overdue_planRepayAmount_max']=loan_second['planRepayAmount'][(loan_second['overdue_class']==3) & (loan_second['recent_overdue']==1)].max()#当前严重逾期贷款本月应还款最大
        dict_out['loan_recent_serious_overdue_RepayedAmount_max']=loan_second['RepayedAmount'][(loan_second['overdue_class']==3) & (loan_second['recent_overdue']==1)].max()#当前严重逾期贷款本月实还款最大
        dict_out['loan_recent_serious_overdue_currentOverdueAmount_max']=loan_second['currentOverdueAmount'][(loan_second['overdue_class']==3) & (loan_second['recent_overdue']==1)].max()#当前严重逾期贷款当前逾期金额最大

        #严重逾期房贷
        dict_out['house_loan_serious_overdue_account_count']=len(loan_second[(loan_second['businessType']=='个人住房商业贷款') & (loan_second['overdue_class']==3)])#严重逾期房贷账户计数
        dict_out['house_loan_serious_overdue_month_sum']=sum(list(map(sum,loan_second['overdue_month'][(loan_second['businessType']=='个人住房商业贷款') & (loan_second['overdue_class']==3)])))#严重逾期房贷逾期月数求和
        dict_out['house_loan_serious_overdue_amount_sum']=loan_second['loanAmount'][(loan_second['businessType']=='个人住房商业贷款') & (loan_second['overdue_class']==3)].sum()#严重逾期房贷本金求和
        dict_out['house_loan_serious_overdue_amount_max']=loan_second['loanAmount'][(loan_second['businessType']=='个人住房商业贷款') & (loan_second['overdue_class']==3)].max()#严重逾期房贷本金最大

        #现行严重逾期房贷
        dict_out['house_loan_serious_overdue_account_count_now']=len(loan_second[(loan_second['businessType']=='个人住房商业贷款') & (loan_second['overdue_class']==3) & (loan_second['is_now']==1)])#现行严重逾期房贷账户计数
        dict_out['house_loan_serious_overdue_month_sum_now']=sum(list(map(sum,loan_second['overdue_month'][(loan_second['businessType']=='个人住房商业贷款') & (loan_second['overdue_class']==3) & (loan_second['is_now']==1)])))#现行严重逾期房贷逾期月数求和
        dict_out['house_loan_serious_overdue_amount_sum_now']=loan_second['loanAmount'][(loan_second['businessType']=='个人住房商业贷款') & (loan_second['overdue_class']==3) & (loan_second['is_now']==1)].sum()#现行严重逾期房贷本金求和
        dict_out['house_loan_serious_overdue_balance_sum_now']=loan_second['balance'][(loan_second['businessType']=='个人住房商业贷款') & (loan_second['overdue_class']==3) & (loan_second['is_now']==1)].sum()#现行严重逾期房贷本金余额求和
        dict_out['house_loan_serious_overdue_planRepayAmount_sum_now']=loan_second['planRepayAmount'][(loan_second['businessType']=='个人住房商业贷款') & (loan_second['overdue_class']==3) & (loan_second['is_now']==1)].sum()#现行严重逾期房贷本月应还款求和
        dict_out['house_loan_serious_overdue_RepayedAmount_sum_now']=loan_second['RepayedAmount'][(loan_second['businessType']=='个人住房商业贷款') & (loan_second['overdue_class']==3) & (loan_second['is_now']==1)].sum()#现行严重逾期房贷本月实还款求和
        dict_out['house_loan_serious_overdue_currentOverdueAmount_sum_now']=loan_second['currentOverdueAmount'][(loan_second['businessType']=='个人住房商业贷款') & (loan_second['overdue_class']==3) & (loan_second['is_now']==1)].sum()#现行严重逾期房贷当前逾期金额求和
        dict_out['house_loan_serious_overdue_amount_max_now']=loan_second['loanAmount'][(loan_second['businessType']=='个人住房商业贷款') & (loan_second['overdue_class']==3) & (loan_second['is_now']==1)].max()#现行严重逾期房贷本金最大
        dict_out['house_loan_serious_overdue_balance_max_now']=loan_second['balance'][(loan_second['businessType']=='个人住房商业贷款') & (loan_second['overdue_class']==3) & (loan_second['is_now']==1)].max()#现行严重逾期房贷本金余额最大
        dict_out['house_loan_serious_overdue_planRepayAmount_max_now']=loan_second['planRepayAmount'][(loan_second['businessType']=='个人住房商业贷款') & (loan_second['overdue_class']==3) & (loan_second['is_now']==1)].max()#现行严重逾期房贷本月应还款最大
        dict_out['house_loan_serious_overdue_RepayedAmount_max_now']=loan_second['RepayedAmount'][(loan_second['businessType']=='个人住房商业贷款') & (loan_second['overdue_class']==3) & (loan_second['is_now']==1)].max()#现行严重逾期房贷本月实还款最大
        dict_out['house_loan_serious_overdue_currentOverdueAmount_max_now']=loan_second['currentOverdueAmount'][(loan_second['businessType']=='个人住房商业贷款') & (loan_second['overdue_class']==3) & (loan_second['is_now']==1)].max()#现行严重逾期房贷当前逾期金额最大

        #严重逾期个人汽车贷款
        dict_out['car_loan_serious_overdue_account_count']=len(loan_second[(loan_second['businessType']=='个人汽车消费贷款') & (loan_second['overdue_class']==3)])#严重逾期个人汽车贷款账户计数
        dict_out['car_loan_serious_overdue_month_sum']=sum(list(map(sum,loan_second['overdue_month'][(loan_second['businessType']=='个人汽车消费贷款') & (loan_second['overdue_class']==3)])))#严重逾期个人汽车贷款逾期月数求和
        dict_out['car_loan_serious_overdue_amount_sum']=loan_second['loanAmount'][(loan_second['businessType']=='个人汽车消费贷款') & (loan_second['overdue_class']==3)].sum()#严重逾期个人汽车贷款本金求和
        dict_out['car_loan_serious_overdue_amount_max']=loan_second['loanAmount'][(loan_second['businessType']=='个人汽车消费贷款') & (loan_second['overdue_class']==3)].max()#严重逾期个人汽车贷款本金最大

        #现行严重逾期个人汽车贷款
        dict_out['car_loan_serious_overdue_account_count_now']=len(loan_second[(loan_second['businessType']=='个人汽车消费贷款') & (loan_second['overdue_class']==3) & (loan_second['is_now']==1)])#现行严重逾期个人汽车贷款账户计数
        dict_out['car_loan_serious_overdue_month_sum_now']=sum(list(map(sum,loan_second['overdue_month'][(loan_second['businessType']=='个人汽车消费贷款') & (loan_second['overdue_class']==3) & (loan_second['is_now']==1)])))#现行严重逾期个人汽车贷款逾期月数求和
        dict_out['car_loan_serious_overdue_amount_sum_now']=loan_second['loanAmount'][(loan_second['businessType']=='个人汽车消费贷款') & (loan_second['overdue_class']==3) & (loan_second['is_now']==1)].sum()#现行严重逾期个人汽车贷款本金求和
        dict_out['car_loan_serious_overdue_balance_sum_now']=loan_second['balance'][(loan_second['businessType']=='个人汽车消费贷款') & (loan_second['overdue_class']==3) & (loan_second['is_now']==1)].sum()#现行严重逾期个人汽车贷款本金余额求和
        dict_out['car_loan_serious_overdue_planRepayAmount_sum_now']=loan_second['planRepayAmount'][(loan_second['businessType']=='个人汽车消费贷款') & (loan_second['overdue_class']==3) & (loan_second['is_now']==1)].sum()#现行严重逾期个人汽车贷款本月应还款求和
        dict_out['car_loan_serious_overdue_RepayedAmount_sum_now']=loan_second['RepayedAmount'][(loan_second['businessType']=='个人汽车消费贷款') & (loan_second['overdue_class']==3) & (loan_second['is_now']==1)].sum()#现行严重逾期个人汽车贷款本月实还款求和
        dict_out['car_loan_serious_overdue_currentOverdueAmount_sum_now']=loan_second['currentOverdueAmount'][(loan_second['businessType']=='个人汽车消费贷款') & (loan_second['overdue_class']==3) & (loan_second['is_now']==1)].sum()#现行严重逾期个人汽车贷款当前逾期金额求和
        dict_out['car_loan_serious_overdue_amount_max_now']=loan_second['loanAmount'][(loan_second['businessType']=='个人汽车消费贷款') & (loan_second['overdue_class']==3) & (loan_second['is_now']==1)].max()#现行严重逾期个人汽车贷款本金最大
        dict_out['car_loan_serious_overdue_balance_max_now']=loan_second['balance'][(loan_second['businessType']=='个人汽车消费贷款') & (loan_second['overdue_class']==3) & (loan_second['is_now']==1)].max()#现行严重逾期个人汽车贷款本金余额最大
        dict_out['car_loan_serious_overdue_planRepayAmount_max_now']=loan_second['planRepayAmount'][(loan_second['businessType']=='个人汽车消费贷款') & (loan_second['overdue_class']==3) & (loan_second['is_now']==1)].max()#现行严重逾期个人汽车贷款本月应还款最大
        dict_out['car_loan_serious_overdue_RepayedAmount_max_now']=loan_second['RepayedAmount'][(loan_second['businessType']=='个人汽车消费贷款') & (loan_second['overdue_class']==3) & (loan_second['is_now']==1)].max()#现行严重逾期个人汽车贷款本月实还款最大
        dict_out['car_loan_serious_overdue_currentOverdueAmount_max_now']=loan_second['currentOverdueAmount'][(loan_second['businessType']=='个人汽车消费贷款') & (loan_second['overdue_class']==3) & (loan_second['is_now']==1)].max()#现行严重逾期个人汽车贷款当前逾期金额最大

        #严重逾期个人助学贷款
        dict_out['stu_loan_serious_overdue_account_count']=len(loan_second[(loan_second['businessType']=='个人助学贷款') & (loan_second['overdue_class']==3)])#严重逾期个人助学贷款账户计数
        dict_out['stu_loan_serious_overdue_month_sum']=sum(list(map(sum,loan_second['overdue_month'][(loan_second['businessType']=='个人助学贷款') & (loan_second['overdue_class']==3)])))#严重逾期个人助学贷款逾期月数求和
        dict_out['stu_loan_serious_overdue_amount_sum']=loan_second['loanAmount'][(loan_second['businessType']=='个人助学贷款') & (loan_second['overdue_class']==3)].sum()#严重逾期个人助学贷款本金求和
        dict_out['stu_loan_serious_overdue_amount_max']=loan_second['loanAmount'][(loan_second['businessType']=='个人助学贷款') & (loan_second['overdue_class']==3)].max()#严重逾期个人助学贷款本金最大

        #现行严重逾期个人助学贷款
        dict_out['stu_loan_serious_overdue_account_count_sum_now']=len(loan_second[(loan_second['businessType']=='个人助学贷款') & (loan_second['overdue_class']==3) & (loan_second['is_now']==1)])#现行严重逾期个人助学贷款账户计数
        dict_out['stu_loan_serious_overdue_month_sum_now']=sum(list(map(sum,loan_second['overdue_month'][(loan_second['businessType']=='个人助学贷款') & (loan_second['overdue_class']==3) & (loan_second['is_now']==1)])))#现行严重逾期个人助学贷款逾期月数求和
        dict_out['stu_loan_serious_overdue_amount_sum_now']=loan_second['loanAmount'][(loan_second['businessType']=='个人助学贷款') & (loan_second['overdue_class']==3) & (loan_second['is_now']==1)].sum()#现行严重逾期个人助学贷款本金求和
        dict_out['stu_loan_serious_overdue_balance_sum_now']=loan_second['balance'][(loan_second['businessType']=='个人助学贷款') & (loan_second['overdue_class']==3) & (loan_second['is_now']==1)].sum()#现行严重逾期个人助学贷款本金余额求和
        dict_out['stu_loan_serious_overdue_planRepayAmount_sum_now']=loan_second['planRepayAmount'][(loan_second['businessType']=='个人助学贷款') & (loan_second['overdue_class']==3) & (loan_second['is_now']==1)].sum()#现行严重逾期个人助学贷款本月应还款求和
        dict_out['stu_loan_serious_overdue_RepayedAmount_sum_now']=loan_second['RepayedAmount'][(loan_second['businessType']=='个人助学贷款') & (loan_second['overdue_class']==3) & (loan_second['is_now']==1)].sum()#现行严重逾期个人助学贷款本月实还款求和
        dict_out['stu_loan_serious_overdue_currentOverdueAmount_sum_now']=loan_second['currentOverdueAmount'][(loan_second['businessType']=='个人助学贷款') & (loan_second['overdue_class']==3) & (loan_second['is_now']==1)].sum()#现行严重逾期个人助学贷款当前逾期金额求和
        dict_out['stu_loan_serious_overdue_amount_max_now']=loan_second['loanAmount'][(loan_second['businessType']=='个人助学贷款') & (loan_second['overdue_class']==3) & (loan_second['is_now']==1)].max()#现行严重逾期个人助学贷款本金最大
        dict_out['stu_loan_serious_overdue_balance_max_now']=loan_second['balance'][(loan_second['businessType']=='个人助学贷款') & (loan_second['overdue_class']==3) & (loan_second['is_now']==1)].max()#现行严重逾期个人助学贷款本金余额最大
        dict_out['stu_loan_serious_overdue_planRepayAmount_max_now']=loan_second['planRepayAmount'][(loan_second['businessType']=='个人助学贷款') & (loan_second['overdue_class']==3) & (loan_second['is_now']==1)].max()#现行严重逾期个人助学贷款本月应还款最大
        dict_out['stu_loan_serious_overdue_RepayedAmount_max_now']=loan_second['RepayedAmount'][(loan_second['businessType']=='个人助学贷款') & (loan_second['overdue_class']==3) & (loan_second['is_now']==1)].max()#现行严重逾期个人助学贷款本月实还款最大
        dict_out['stu_loan_serious_overdue_currentOverdueAmount_max_now']=loan_second['currentOverdueAmount'][(loan_second['businessType']=='个人助学贷款') & (loan_second['overdue_class']==3) & (loan_second['is_now']==1)].max()#现行严重逾期个人助学贷款当前逾期金额最大

        #最近3个月轻度逾期个人经营性贷款
        dict_out['business_loan_slight_overdue_account_count_03m']=len(loan_second[(loan_second['businessType']=='个人经营性贷款') & (loan_second['month_Desc_report']<3) & (loan_second['overdue_class']==1)])#最近3个月轻度逾期个人经营性贷款账户计数
        dict_out['business_loan_slight_overdue_month_sum_03m']=sum(list(map(sum,loan_second['overdue_month'][(loan_second['businessType']=='个人经营性贷款') & (loan_second['month_Desc_report']<3) & (loan_second['overdue_class']==1)])))#最近3个月轻度逾期个人经营性贷款逾期月数求和

        #现行最近3个月轻度逾期个人经营性贷款
        dict_out['business_loan_slight_overdue_account_count_03m_now']=loan_second['overdue_month'][(loan_second['businessType']=='个人经营性贷款') & (loan_second['month_Desc_report']<3) & (loan_second['overdue_class']==1) & (loan_second['is_now']==1)].sum()#现行最近3个月轻度逾期个人经营性贷款账户计数
        dict_out['business_loan_slight_overdue_month_sum_03m_now']=sum(list(map(sum,loan_second['overdue_month'][(loan_second['businessType']=='个人经营性贷款') & (loan_second['month_Desc_report']<3) & (loan_second['overdue_class']==1) & (loan_second['is_now']==1)])))#现行最近3个月轻度逾期个人经营性贷款逾期月数求和
        dict_out['business_loan_slight_overdue_planRepayAmount_sum_03m_now']=loan_second['planRepayAmount'][(loan_second['businessType']=='个人经营性贷款') & (loan_second['month_Desc_report']<3) & (loan_second['overdue_class']==1) & (loan_second['is_now']==1)].sum()#现行最近3个月轻度逾期个人经营性贷款本月应还款求和
        dict_out['business_loan_slight_overdue_RepayedAmount_sum_03m_now']=loan_second['RepayedAmount'][(loan_second['businessType']=='个人经营性贷款') & (loan_second['month_Desc_report']<3) & (loan_second['overdue_class']==1) & (loan_second['is_now']==1)].sum()#现行最近3个月轻度逾期个人经营性贷款本月实还款求和
        dict_out['business_loan_slight_overdue_planRepayAmount_max_03m_now']=loan_second['planRepayAmount'][(loan_second['businessType']=='个人经营性贷款') & (loan_second['month_Desc_report']<3) & (loan_second['overdue_class']==1) & (loan_second['is_now']==1)].max()#现行最近3个月轻度逾期个人经营性贷款本月应还款最大
        dict_out['business_loan_slight_overdue_RepayedAmount_man_03m_now']=loan_second['RepayedAmount'][(loan_second['businessType']=='个人经营性贷款') & (loan_second['month_Desc_report']<3) & (loan_second['overdue_class']==1) & (loan_second['is_now']==1)].max()#现行最近3个月轻度逾期个人经营性贷款本月实还款最大

        #最近3个月轻度逾期有担保个人经营性贷款
        dict_out['business_loan_is_vouch_slight_overdue_account_count_03m']=len(loan_second[(loan_second['businessType']=='个人经营性贷款') & (loan_second['month_Desc_report']<3) & (loan_second['overdue_class']==1) & (loan_second['is_vouch']==1)])#最近3个月轻度逾期有担保个人经营性贷款账户计数
        dict_out['business_loan_is_vouch_slight_overdue_month_sum_03m']=sum(list(map(sum,loan_second['overdue_month'][(loan_second['businessType']=='个人经营性贷款') & (loan_second['month_Desc_report']<3) & (loan_second['overdue_class']==1) & (loan_second['is_vouch']==1)])))#最近3个月轻度逾期有担保个人经营性贷款逾期月数求和

        #现行最近3个月轻度逾期有担保个人经营性贷款
        dict_out['business_loan_is_vouch_slight_overdue_account_count_03m_now']=len(loan_second[(loan_second['businessType']=='个人经营性贷款') & (loan_second['month_Desc_report']<3) & (loan_second['overdue_class']==1) & (loan_second['is_vouch']==1) & (loan_second['is_now']==1)])#现行最近3个月轻度逾期有担保个人经营性贷款账户计数
        dict_out['business_loan_is_vouch_slight_overdue_month_sum_03m_now']=sum(list(map(sum,loan_second['overdue_month'][(loan_second['businessType']=='个人经营性贷款') & (loan_second['month_Desc_report']<3) & (loan_second['overdue_class']==1) & (loan_second['is_vouch']==1) & (loan_second['is_now']==1)])))#现行最近3个月轻度逾期有担保个人经营性贷款逾期月数求和
        dict_out['business_loan_is_vouch_slight_overdue_planRepayAmount_sum_03m_now']=loan_second['planRepayAmount'][(loan_second['businessType']=='个人经营性贷款') & (loan_second['month_Desc_report']<3) & (loan_second['overdue_class']==1) & (loan_second['is_vouch']==1) & (loan_second['is_now']==1)].sum()#现行最近3个月轻度逾期有担保个人经营性贷款本月应还款求和
        dict_out['business_loan_is_vouch_slight_overdue_RepayedAmount_sum_03m_now']=loan_second['RepayedAmount'][(loan_second['businessType']=='个人经营性贷款') & (loan_second['month_Desc_report']<3) & (loan_second['overdue_class']==1) & (loan_second['is_vouch']==1) & (loan_second['is_now']==1)].sum()#现行最近3个月轻度逾期有担保个人经营性贷款本月实还款求和
        dict_out['business_loan_is_vouch_slight_overdue_planRepayAmount_max_03m_now']=loan_second['planRepayAmount'][(loan_second['businessType']=='个人经营性贷款') & (loan_second['month_Desc_report']<3) & (loan_second['overdue_class']==1) & (loan_second['is_vouch']==1) & (loan_second['is_now']==1)].max()#现行最近3个月轻度逾期有担保个人经营性贷款本月应还款最大
        dict_out['business_loan_is_vouch_slight_overdue_RepayedAmount_max_03m_now']=loan_second['RepayedAmount'][(loan_second['businessType']=='个人经营性贷款') & (loan_second['month_Desc_report']<3) & (loan_second['overdue_class']==1) & (loan_second['is_vouch']==1) & (loan_second['is_now']==1)].max()#现行最近3个月轻度逾期有担保个人经营性贷款本月实还款最大

        #最近6个月轻度逾期个人经营性贷款
        dict_out['business_loan_slight_overdue_account_count_06m']=len(loan_second[(loan_second['businessType']=='个人经营性贷款') & (loan_second['month_Desc_report']<6) & (loan_second['overdue_class']==1)])#最近6个月轻度逾期个人经营性贷款账户计数
        dict_out['business_loan_slight_overdue_month_sum_06m']=sum(list(map(sum,loan_second['overdue_month'][(loan_second['businessType']=='个人经营性贷款') & (loan_second['month_Desc_report']<6) & (loan_second['overdue_class']==1)])))#最近6个月轻度逾期个人经营性贷款逾期月数求和

        #现行最近6个月轻度逾期个人经营性贷款
        dict_out['business_loan_slight_overdue_account_count_06m_now']=loan_second['overdue_month'][(loan_second['businessType']=='个人经营性贷款') & (loan_second['month_Desc_report']<6) & (loan_second['overdue_class']==1) & (loan_second['is_now']==1)].sum()#现行最近6个月轻度逾期个人经营性贷款账户计数
        dict_out['business_loan_slight_overdue_month_sum_06m_now']=sum(list(map(sum,loan_second['overdue_month'][(loan_second['businessType']=='个人经营性贷款') & (loan_second['month_Desc_report']<6) & (loan_second['overdue_class']==1) & (loan_second['is_now']==1)])))#现行最近6个月轻度逾期个人经营性贷款逾期月数求和
        dict_out['business_loan_slight_overdue_planRepayAmount_sum_06m_now']=loan_second['planRepayAmount'][(loan_second['businessType']=='个人经营性贷款') & (loan_second['month_Desc_report']<6) & (loan_second['overdue_class']==1) & (loan_second['is_now']==1)].sum()#现行最近6个月轻度逾期个人经营性贷款本月应还款求和
        dict_out['business_loan_slight_overdue_RepayedAmount_sum_06m_now']=loan_second['RepayedAmount'][(loan_second['businessType']=='个人经营性贷款') & (loan_second['month_Desc_report']<6) & (loan_second['overdue_class']==1) & (loan_second['is_now']==1)].sum()#现行最近6个月轻度逾期个人经营性贷款本月实还款求和
        dict_out['business_loan_slight_overdue_planRepayAmount_max_06m_now']=loan_second['planRepayAmount'][(loan_second['businessType']=='个人经营性贷款') & (loan_second['month_Desc_report']<6) & (loan_second['overdue_class']==1) & (loan_second['is_now']==1)].max()#现行最近6个月轻度逾期个人经营性贷款本月应还款最大
        dict_out['business_loan_slight_overdue_RepayedAmount_man_06m_now']=loan_second['RepayedAmount'][(loan_second['businessType']=='个人经营性贷款') & (loan_second['month_Desc_report']<6) & (loan_second['overdue_class']==1) & (loan_second['is_now']==1)].max()#现行最近6个月轻度逾期个人经营性贷款本月实还款最大

        #最近6个月轻度逾期有担保个人经营性贷款
        dict_out['business_loan_is_vouch_slight_overdue_account_count_06m']=len(loan_second[(loan_second['businessType']=='个人经营性贷款') & (loan_second['month_Desc_report']<6) & (loan_second['overdue_class']==1) & (loan_second['is_vouch']==1)])#最近6个月轻度逾期有担保个人经营性贷款账户计数
        dict_out['business_loan_is_vouch_slight_overdue_month_sum_06m']=sum(list(map(sum,loan_second['overdue_month'][(loan_second['businessType']=='个人经营性贷款') & (loan_second['month_Desc_report']<6) & (loan_second['overdue_class']==1) & (loan_second['is_vouch']==1)])))#最近6个月轻度逾期有担保个人经营性贷款逾期月数求和

        #现行最近6个月轻度逾期有担保个人经营性贷款
        dict_out['business_loan_is_vouch_slight_overdue_account_count_06m_now']=len(loan_second[(loan_second['businessType']=='个人经营性贷款') & (loan_second['month_Desc_report']<6) & (loan_second['overdue_class']==1) & (loan_second['is_vouch']==1) & (loan_second['is_now']==1)])#现行最近6个月轻度逾期有担保个人经营性贷款账户计数
        dict_out['business_loan_is_vouch_slight_overdue_month_sum_06m_now']=sum(list(map(sum,loan_second['overdue_month'][(loan_second['businessType']=='个人经营性贷款') & (loan_second['month_Desc_report']<6) & (loan_second['overdue_class']==1) & (loan_second['is_vouch']==1) & (loan_second['is_now']==1)])))#现行最近6个月轻度逾期有担保个人经营性贷款逾期月数求和
        dict_out['business_loan_is_vouch_slight_overdue_planRepayAmount_sum_06m_now']=loan_second['planRepayAmount'][(loan_second['businessType']=='个人经营性贷款') & (loan_second['month_Desc_report']<6) & (loan_second['overdue_class']==1) & (loan_second['is_vouch']==1) & (loan_second['is_now']==1)].sum()#现行最近6个月轻度逾期有担保个人经营性贷款本月应还款求和
        dict_out['business_loan_is_vouch_slight_overdue_RepayedAmount_sum_06m_now']=loan_second['RepayedAmount'][(loan_second['businessType']=='个人经营性贷款') & (loan_second['month_Desc_report']<6) & (loan_second['overdue_class']==1) & (loan_second['is_vouch']==1) & (loan_second['is_now']==1)].sum()#现行最近6个月轻度逾期有担保个人经营性贷款本月实还款求和
        dict_out['business_loan_is_vouch_slight_overdue_planRepayAmount_max_06m_now']=loan_second['planRepayAmount'][(loan_second['businessType']=='个人经营性贷款') & (loan_second['month_Desc_report']<6) & (loan_second['overdue_class']==1) & (loan_second['is_vouch']==1) & (loan_second['is_now']==1)].max()#现行最近6个月轻度逾期有担保个人经营性贷款本月应还款最大
        dict_out['business_loan_is_vouch_slight_overdue_RepayedAmount_max_06m_now']=loan_second['RepayedAmount'][(loan_second['businessType']=='个人经营性贷款') & (loan_second['month_Desc_report']<6) & (loan_second['overdue_class']==1) & (loan_second['is_vouch']==1) & (loan_second['is_now']==1)].max()#现行最近6个月轻度逾期有担保个人经营性贷款本月实还款最大

        #最近12个月轻度逾期个人经营性贷款
        dict_out['business_loan_slight_overdue_account_count_12m']=len(loan_second[(loan_second['businessType']=='个人经营性贷款') & (loan_second['month_Desc_report']<12) & (loan_second['overdue_class']==1)])#最近12个月轻度逾期个人经营性贷款账户计数
        dict_out['business_loan_slight_overdue_month_sum_12m']=sum(list(map(sum,loan_second['overdue_month'][(loan_second['businessType']=='个人经营性贷款') & (loan_second['month_Desc_report']<12) & (loan_second['overdue_class']==1)])))#最近12个月轻度逾期个人经营性贷款逾期月数求和

        #现行最近12个月轻度逾期个人经营性贷款
        dict_out['business_loan_slight_overdue_account_count_12m_now']=loan_second['overdue_month'][(loan_second['businessType']=='个人经营性贷款') & (loan_second['month_Desc_report']<12) & (loan_second['overdue_class']==1) & (loan_second['is_now']==1)].sum()#现行最近12个月轻度逾期个人经营性贷款账户计数
        dict_out['business_loan_slight_overdue_month_sum_12m_now']=sum(list(map(sum,loan_second['overdue_month'][(loan_second['businessType']=='个人经营性贷款') & (loan_second['month_Desc_report']<12) & (loan_second['overdue_class']==1) & (loan_second['is_now']==1)])))#现行最近12个月轻度逾期个人经营性贷款逾期月数求和
        dict_out['business_loan_slight_overdue_planRepayAmount_sum_12m_now']=loan_second['planRepayAmount'][(loan_second['businessType']=='个人经营性贷款') & (loan_second['month_Desc_report']<12) & (loan_second['overdue_class']==1) & (loan_second['is_now']==1)].sum()#现行最近12个月轻度逾期个人经营性贷款本月应还款求和
        dict_out['business_loan_slight_overdue_RepayedAmount_sum_12m_now']=loan_second['RepayedAmount'][(loan_second['businessType']=='个人经营性贷款') & (loan_second['month_Desc_report']<12) & (loan_second['overdue_class']==1) & (loan_second['is_now']==1)].sum()#现行最近12个月轻度逾期个人经营性贷款本月实还款求和
        dict_out['business_loan_slight_overdue_planRepayAmount_max_12m_now']=loan_second['planRepayAmount'][(loan_second['businessType']=='个人经营性贷款') & (loan_second['month_Desc_report']<12) & (loan_second['overdue_class']==1) & (loan_second['is_now']==1)].max()#现行最近12个月轻度逾期个人经营性贷款本月应还款最大
        dict_out['business_loan_slight_overdue_RepayedAmount_man_12m_now']=loan_second['RepayedAmount'][(loan_second['businessType']=='个人经营性贷款') & (loan_second['month_Desc_report']<12) & (loan_second['overdue_class']==1) & (loan_second['is_now']==1)].max()#现行最近12个月轻度逾期个人经营性贷款本月实还款最大

        #最近12个月轻度逾期有担保个人经营性贷款
        dict_out['business_loan_is_vouch_slight_overdue_account_count_12m']=len(loan_second[(loan_second['businessType']=='个人经营性贷款') & (loan_second['month_Desc_report']<12) & (loan_second['overdue_class']==1) & (loan_second['is_vouch']==1)])#最近12个月轻度逾期有担保个人经营性贷款账户计数
        dict_out['business_loan_is_vouch_slight_overdue_month_sum_12m']=sum(list(map(sum,loan_second['overdue_month'][(loan_second['businessType']=='个人经营性贷款') & (loan_second['month_Desc_report']<12) & (loan_second['overdue_class']==1) & (loan_second['is_vouch']==1)])))#最近12个月轻度逾期有担保个人经营性贷款逾期月数求和

        #现行最近12个月轻度逾期有担保个人经营性贷款
        dict_out['business_loan_is_vouch_slight_overdue_account_count_12m_now']=len(loan_second[(loan_second['businessType']=='个人经营性贷款') & (loan_second['month_Desc_report']<12) & (loan_second['overdue_class']==1) & (loan_second['is_vouch']==1) & (loan_second['is_now']==1)])#现行最近12个月轻度逾期有担保个人经营性贷款账户计数
        dict_out['business_loan_is_vouch_slight_overdue_month_sum_12m_now']=sum(list(map(sum,loan_second['overdue_month'][(loan_second['businessType']=='个人经营性贷款') & (loan_second['month_Desc_report']<12) & (loan_second['overdue_class']==1) & (loan_second['is_vouch']==1) & (loan_second['is_now']==1)])))#现行最近12个月轻度逾期有担保个人经营性贷款逾期月数求和
        dict_out['business_loan_is_vouch_slight_overdue_planRepayAmount_sum_12m_now']=loan_second['planRepayAmount'][(loan_second['businessType']=='个人经营性贷款') & (loan_second['month_Desc_report']<12) & (loan_second['overdue_class']==1) & (loan_second['is_vouch']==1) & (loan_second['is_now']==1)].sum()#现行最近12个月轻度逾期有担保个人经营性贷款本月应还款求和
        dict_out['business_loan_is_vouch_slight_overdue_RepayedAmount_sum_12m_now']=loan_second['RepayedAmount'][(loan_second['businessType']=='个人经营性贷款') & (loan_second['month_Desc_report']<12) & (loan_second['overdue_class']==1) & (loan_second['is_vouch']==1) & (loan_second['is_now']==1)].sum()#现行最近12个月轻度逾期有担保个人经营性贷款本月实还款求和
        dict_out['business_loan_is_vouch_slight_overdue_planRepayAmount_max_12m_now']=loan_second['planRepayAmount'][(loan_second['businessType']=='个人经营性贷款') & (loan_second['month_Desc_report']<12) & (loan_second['overdue_class']==1) & (loan_second['is_vouch']==1) & (loan_second['is_now']==1)].max()#现行最近12个月轻度逾期有担保个人经营性贷款本月应还款最大
        dict_out['business_loan_is_vouch_slight_overdue_RepayedAmount_max_12m_now']=loan_second['RepayedAmount'][(loan_second['businessType']=='个人经营性贷款') & (loan_second['month_Desc_report']<12) & (loan_second['overdue_class']==1) & (loan_second['is_vouch']==1) & (loan_second['is_now']==1)].max()#现行最近12个月轻度逾期有担保个人经营性贷款本月实还款最大

        #最近24个月轻度逾期个人经营性贷款
        dict_out['business_loan_slight_overdue_account_count_24m']=len(loan_second[(loan_second['businessType']=='个人经营性贷款') & (loan_second['month_Desc_report']<24) & (loan_second['overdue_class']==1)])#最近24个月轻度逾期个人经营性贷款账户计数
        dict_out['business_loan_slight_overdue_month_sum_24m']=sum(list(map(sum,loan_second['overdue_month'][(loan_second['businessType']=='个人经营性贷款') & (loan_second['month_Desc_report']<24) & (loan_second['overdue_class']==1)])))#最近24个月轻度逾期个人经营性贷款逾期月数求和

        #现行最近24个月轻度逾期个人经营性贷款
        dict_out['business_loan_slight_overdue_account_count_24m_now']=loan_second['overdue_month'][(loan_second['businessType']=='个人经营性贷款') & (loan_second['month_Desc_report']<24) & (loan_second['overdue_class']==1) & (loan_second['is_now']==1)].sum()#现行最近24个月轻度逾期个人经营性贷款账户计数
        dict_out['business_loan_slight_overdue_month_sum_24m_now']=sum(list(map(sum,loan_second['overdue_month'][(loan_second['businessType']=='个人经营性贷款') & (loan_second['month_Desc_report']<24) & (loan_second['overdue_class']==1) & (loan_second['is_now']==1)])))#现行最近24个月轻度逾期个人经营性贷款逾期月数求和
        dict_out['business_loan_slight_overdue_planRepayAmount_sum_24m_now']=loan_second['planRepayAmount'][(loan_second['businessType']=='个人经营性贷款') & (loan_second['month_Desc_report']<24) & (loan_second['overdue_class']==1) & (loan_second['is_now']==1)].sum()#现行最近24个月轻度逾期个人经营性贷款本月应还款求和
        dict_out['business_loan_slight_overdue_RepayedAmount_sum_24m_now']=loan_second['RepayedAmount'][(loan_second['businessType']=='个人经营性贷款') & (loan_second['month_Desc_report']<24) & (loan_second['overdue_class']==1) & (loan_second['is_now']==1)].sum()#现行最近24个月轻度逾期个人经营性贷款本月实还款求和
        dict_out['business_loan_slight_overdue_planRepayAmount_max_24m_now']=loan_second['planRepayAmount'][(loan_second['businessType']=='个人经营性贷款') & (loan_second['month_Desc_report']<24) & (loan_second['overdue_class']==1) & (loan_second['is_now']==1)].max()#现行最近24个月轻度逾期个人经营性贷款本月应还款最大
        dict_out['business_loan_slight_overdue_RepayedAmount_man_24m_now']=loan_second['RepayedAmount'][(loan_second['businessType']=='个人经营性贷款') & (loan_second['month_Desc_report']<24) & (loan_second['overdue_class']==1) & (loan_second['is_now']==1)].max()#现行最近24个月轻度逾期个人经营性贷款本月实还款最大

        #最近24个月轻度逾期有担保个人经营性贷款
        dict_out['business_loan_is_vouch_slight_overdue_account_count_24m']=len(loan_second[(loan_second['businessType']=='个人经营性贷款') & (loan_second['month_Desc_report']<24) & (loan_second['overdue_class']==1) & (loan_second['is_vouch']==1)])#最近24个月轻度逾期有担保个人经营性贷款账户计数
        dict_out['business_loan_is_vouch_slight_overdue_month_sum_24m']=sum(list(map(sum,loan_second['overdue_month'][(loan_second['businessType']=='个人经营性贷款') & (loan_second['month_Desc_report']<24) & (loan_second['overdue_class']==1) & (loan_second['is_vouch']==1)])))#最近24个月轻度逾期有担保个人经营性贷款逾期月数求和

        #现行最近24个月轻度逾期有担保个人经营性贷款
        dict_out['business_loan_is_vouch_slight_overdue_account_count_24m_now']=len(loan_second[(loan_second['businessType']=='个人经营性贷款') & (loan_second['month_Desc_report']<24) & (loan_second['overdue_class']==1) & (loan_second['is_vouch']==1) & (loan_second['is_now']==1)])#现行最近24个月轻度逾期有担保个人经营性贷款账户计数
        dict_out['business_loan_is_vouch_slight_overdue_month_sum_24m_now']=sum(list(map(sum,loan_second['overdue_month'][(loan_second['businessType']=='个人经营性贷款') & (loan_second['month_Desc_report']<24) & (loan_second['overdue_class']==1) & (loan_second['is_vouch']==1) & (loan_second['is_now']==1)])))#现行最近24个月轻度逾期有担保个人经营性贷款逾期月数求和
        dict_out['business_loan_is_vouch_slight_overdue_planRepayAmount_sum_24m_now']=loan_second['planRepayAmount'][(loan_second['businessType']=='个人经营性贷款') & (loan_second['month_Desc_report']<24) & (loan_second['overdue_class']==1) & (loan_second['is_vouch']==1) & (loan_second['is_now']==1)].sum()#现行最近24个月轻度逾期有担保个人经营性贷款本月应还款求和
        dict_out['business_loan_is_vouch_slight_overdue_RepayedAmount_sum_24m_now']=loan_second['RepayedAmount'][(loan_second['businessType']=='个人经营性贷款') & (loan_second['month_Desc_report']<24) & (loan_second['overdue_class']==1) & (loan_second['is_vouch']==1) & (loan_second['is_now']==1)].sum()#现行最近24个月轻度逾期有担保个人经营性贷款本月实还款求和
        dict_out['business_loan_is_vouch_slight_overdue_planRepayAmount_max_24m_now']=loan_second['planRepayAmount'][(loan_second['businessType']=='个人经营性贷款') & (loan_second['month_Desc_report']<24) & (loan_second['overdue_class']==1) & (loan_second['is_vouch']==1) & (loan_second['is_now']==1)].max()#现行最近24个月轻度逾期有担保个人经营性贷款本月应还款最大
        dict_out['business_loan_is_vouch_slight_overdue_RepayedAmount_max_24m_now']=loan_second['RepayedAmount'][(loan_second['businessType']=='个人经营性贷款') & (loan_second['month_Desc_report']<24) & (loan_second['overdue_class']==1) & (loan_second['is_vouch']==1) & (loan_second['is_now']==1)].max()#现行最近24个月轻度逾期有担保个人经营性贷款本月实还款最大

        #严重逾期个人经营性贷款
        dict_out['business_loan_serious_overdue_account_count']=len(loan_second[(loan_second['businessType']=='个人经营性贷款') & (loan_second['overdue_class']==3)])#严重逾期个人经营性贷款账户计数
        dict_out['business_loan_serious_overdue_month_sum']=sum(list(map(sum,loan_second['overdue_month'][(loan_second['businessType']=='个人经营性贷款') & (loan_second['overdue_class']==3)])))#严重逾期个人经营性贷款逾期月数求和
        dict_out['business_loan_serious_overdue_amount_sum']=loan_second['loanAmount'][(loan_second['businessType']=='个人经营性贷款') & (loan_second['overdue_class']==3)].sum()#严重逾期个人经营性贷款本金求和
        dict_out['business_loan_serious_overdue_amount_max']=loan_second['loanAmount'][(loan_second['businessType']=='个人经营性贷款') & (loan_second['overdue_class']==3)].max()#严重逾期个人经营性贷款本金最大

        #现行严重逾期个人经营性贷款
        dict_out['business_loan_serious_overdue_account_count_now']=len(loan_second[(loan_second['businessType']=='个人经营性贷款') & (loan_second['overdue_class']==3) & (loan_second['is_now']==1)])#现行严重逾期个人经营性贷款账户计数
        dict_out['business_loan_serious_overdue_month_sum_now']=sum(list(map(sum,loan_second['overdue_month'][(loan_second['businessType']=='个人经营性贷款') & (loan_second['overdue_class']==3) & (loan_second['is_now']==1)])))#现行严重逾期个人经营性贷款逾期月数求和
        dict_out['business_loan_serious_overdue_amount_sum_now']=loan_second['loanAmount'][(loan_second['businessType']=='个人经营性贷款') & (loan_second['overdue_class']==3) & (loan_second['is_now']==1)].sum()#现行严重逾期个人经营性贷款本金求和
        dict_out['business_loan_serious_overdue_balance_sum_now']=loan_second['balance'][(loan_second['businessType']=='个人经营性贷款') & (loan_second['overdue_class']==3) & (loan_second['is_now']==1)].sum()#现行严重逾期个人经营性贷款本金余额求和
        dict_out['business_loan_serious_overdue_planRepayAmount_sum_now']=loan_second['planRepayAmount'][(loan_second['businessType']=='个人经营性贷款') & (loan_second['overdue_class']==3) & (loan_second['is_now']==1)].sum()#现行严重逾期个人经营性贷款本月应还款求和
        dict_out['business_loan_serious_overdue_RepayedAmount_sum_now']=loan_second['RepayedAmount'][(loan_second['businessType']=='个人经营性贷款') & (loan_second['overdue_class']==3) & (loan_second['is_now']==1)].sum()#现行严重逾期个人经营性贷款本月实还款求和
        dict_out['business_loan_serious_overdue_currentOverdueAmount_sum_now']=loan_second['currentOverdueAmount'][(loan_second['businessType']=='个人经营性贷款') & (loan_second['overdue_class']==3) & (loan_second['is_now']==1)].sum()#现行严重逾期个人经营性贷款当前逾期金额求和
        dict_out['business_loan_serious_overdue_amount_max_now']=loan_second['loanAmount'][(loan_second['businessType']=='个人经营性贷款') & (loan_second['overdue_class']==3) & (loan_second['is_now']==1)].max()#现行严重逾期个人经营性贷款本金最大
        dict_out['business_loan_serious_overdue_balance_max_now']=loan_second['balance'][(loan_second['businessType']=='个人经营性贷款') & (loan_second['overdue_class']==3) & (loan_second['is_now']==1)].max()#现行严重逾期个人经营性贷款本金余额最大
        dict_out['business_loan_serious_overdue_planRepayAmount_max_now']=loan_second['planRepayAmount'][(loan_second['businessType']=='个人经营性贷款') & (loan_second['overdue_class']==3) & (loan_second['is_now']==1)].max()#现行严重逾期个人经营性贷款本月应还款最大
        dict_out['business_loan_serious_overdue_RepayedAmount_max_now']=loan_second['RepayedAmount'][(loan_second['businessType']=='个人经营性贷款') & (loan_second['overdue_class']==3) & (loan_second['is_now']==1)].max()#现行严重逾期个人经营性贷款本月实还款最大
        dict_out['business_loan_serious_overdue_currentOverdueAmount_max_now']=loan_second['currentOverdueAmount'][(loan_second['businessType']=='个人经营性贷款') & (loan_second['overdue_class']==3) & (loan_second['is_now']==1)].max()#现行严重逾期个人经营性贷款当前逾期金额最大

        #严重逾期有担保个人经营性贷款
        dict_out['business_loan_is_vouch_serious_overdue_account_count']=len(loan_second[(loan_second['businessType']=='个人经营性贷款') & (loan_second['overdue_class']==3) & (loan_second['is_vouch']==1)])#严重逾期有担保个人经营性贷款账户计数
        dict_out['business_loan_is_vouch_serious_overdue_month_sum']=sum(list(map(sum,loan_second['overdue_month'][(loan_second['businessType']=='个人经营性贷款') & (loan_second['overdue_class']==3) & (loan_second['is_vouch']==1)])))#严重逾期有担保个人经营性贷款逾期月数求和
        dict_out['business_loan_is_vouch_serious_overdue_amount_sum']=loan_second['loanAmount'][(loan_second['businessType']=='个人经营性贷款') & (loan_second['overdue_class']==3) & (loan_second['is_vouch']==1)].sum()#严重逾期有担保个人经营性贷款本金求和
        dict_out['business_loan_is_vouch_serious_overdue_amount_max']=loan_second['loanAmount'][(loan_second['businessType']=='个人经营性贷款') & (loan_second['overdue_class']==3) & (loan_second['is_vouch']==1)].max()#严重逾期有担保个人经营性贷款本金最大

        #现行严重逾期有担保个人经营性贷款
        dict_out['business_loan_is_vouch_serious_overdue_account_count_now']=len(loan_second[(loan_second['businessType']=='个人经营性贷款') & (loan_second['overdue_class']==3) & (loan_second['is_vouch']==1) & (loan_second['is_now']==1)])#现行严重逾期有担保个人经营性贷款账户计数
        dict_out['business_loan_is_vouch_serious_overdue_month_sum_now']=sum(list(map(sum,loan_second['overdue_month'][(loan_second['businessType']=='个人经营性贷款') & (loan_second['overdue_class']==3) & (loan_second['is_vouch']==1) & (loan_second['is_now']==1)])))#现行严重逾期有担保个人经营性贷款逾期月数求和
        dict_out['business_loan_is_vouch_serious_overdue_amount_sum_now']=loan_second['loanAmount'][(loan_second['businessType']=='个人经营性贷款') & (loan_second['overdue_class']==3) & (loan_second['is_vouch']==1) & (loan_second['is_now']==1)].sum()#现行严重逾期有担保个人经营性贷款本金求和
        dict_out['business_loan_is_vouch_serious_overdue_balance_sum_now']=loan_second['balance'][(loan_second['businessType']=='个人经营性贷款') & (loan_second['overdue_class']==3) & (loan_second['is_vouch']==1) & (loan_second['is_now']==1)].sum()#现行严重逾期有担保个人经营性贷款本金余额求和
        dict_out['business_loan_is_vouch_serious_overdue_planRepayAmount_sum_now']=loan_second['planRepayAmount'][(loan_second['businessType']=='个人经营性贷款') & (loan_second['overdue_class']==3) & (loan_second['is_vouch']==1) & (loan_second['is_now']==1)].sum()#现行严重逾期有担保个人经营性贷款本月应还款求和
        dict_out['business_loan_is_vouch_serious_overdue_RepayedAmount_sum_now']=loan_second['RepayedAmount'][(loan_second['businessType']=='个人经营性贷款') & (loan_second['overdue_class']==3) & (loan_second['is_vouch']==1) & (loan_second['is_now']==1)].sum()#现行严重逾期有担保个人经营性贷款本月实还款求和
        dict_out['business_loan_is_vouch_serious_overdue_currentOverdueAmount_sum_now']=loan_second['currentOverdueAmount'][(loan_second['businessType']=='个人经营性贷款') & (loan_second['overdue_class']==3) & (loan_second['is_vouch']==1) & (loan_second['is_now']==1)].sum()#现行严重逾期有担保个人经营性贷款当前逾期金额求和
        dict_out['business_loan_is_vouch_serious_overdue_amount_max_now']=loan_second['loanAmount'][(loan_second['businessType']=='个人经营性贷款') & (loan_second['overdue_class']==3) & (loan_second['is_vouch']==1) & (loan_second['is_now']==1)].max()#现行严重逾期有担保个人经营性贷款本金最大
        dict_out['business_loan_is_vouch_serious_overdue_balance_max_now']=loan_second['balance'][(loan_second['businessType']=='个人经营性贷款') & (loan_second['overdue_class']==3) & (loan_second['is_vouch']==1) & (loan_second['is_now']==1)].max()#现行严重逾期有担保个人经营性贷款本金余额最大
        dict_out['business_loan_is_vouch_serious_overdue_planRepayAmount_max_now']=loan_second['planRepayAmount'][(loan_second['businessType']=='个人经营性贷款') & (loan_second['overdue_class']==3) & (loan_second['is_vouch']==1) & (loan_second['is_now']==1)].max()#现行严重逾期有担保个人经营性贷款本月应还款最大
        dict_out['business_loan_is_vouch_serious_overdue_RepayedAmount_max_now']=loan_second['RepayedAmount'][(loan_second['businessType']=='个人经营性贷款') & (loan_second['overdue_class']==3) & (loan_second['is_vouch']==1) & (loan_second['is_now']==1)].max()#现行严重逾期有担保个人经营性贷款本月实还款最大
        dict_out['business_loan_is_vouch_serious_overdue_currentOverdueAmount_max_now']=loan_second['currentOverdueAmount'][(loan_second['businessType']=='个人经营性贷款') & (loan_second['overdue_class']==3) & (loan_second['is_vouch']==1) & (loan_second['is_now']==1)].max()#现行严重逾期有担保个人经营性贷款当前逾期金额最大

        #最近3个月轻度逾期个人消费贷款
        dict_out['consume_loan_slight_overdue_account_count_03m']=len(loan_second[(loan_second['businessType']=='其他个人消费贷款') & (loan_second['month_Desc_report']<3) & (loan_second['overdue_class']==1)])#最近3个月轻度逾期个人消费贷款账户计数
        dict_out['consume_loan_slight_overdue_month_sum_03m']=sum(list(map(sum,loan_second['overdue_month'][(loan_second['businessType']=='其他个人消费贷款') & (loan_second['month_Desc_report']<3) & (loan_second['overdue_class']==1)])))#最近3个月轻度逾期个人消费贷款逾期月数求和

        #现行最近3个月轻度逾期个人消费贷款
        dict_out['consume_loan_slight_overdue_account_count_03m_now']=loan_second['overdue_month'][(loan_second['businessType']=='其他个人消费贷款款') & (loan_second['month_Desc_report']<3) & (loan_second['overdue_class']==1) & (loan_second['is_now']==1)].sum()#现行最近3个月轻度逾期个人消费贷款账户计数
        dict_out['consume_loan_slight_overdue_month_sum_03m_now']=sum(list(map(sum,loan_second['overdue_month'][(loan_second['businessType']=='其他个人消费贷款') & (loan_second['month_Desc_report']<3) & (loan_second['overdue_class']==1) & (loan_second['is_now']==1)])))#现行最近3个月轻度逾期个人消费贷款逾期月数求和
        dict_out['consume_loan_slight_overdue_planRepayAmount_sum_03m_now']=loan_second['planRepayAmount'][(loan_second['businessType']=='其他个人消费贷款') & (loan_second['month_Desc_report']<3) & (loan_second['overdue_class']==1) & (loan_second['is_now']==1)].sum()#现行最近3个月轻度逾期个人消费贷款本月应还款求和
        dict_out['consume_loan_slight_overdue_RepayedAmount_sum_03m_now']=loan_second['RepayedAmount'][(loan_second['businessType']=='其他个人消费贷款') & (loan_second['month_Desc_report']<3) & (loan_second['overdue_class']==1) & (loan_second['is_now']==1)].sum()#现行最近3个月轻度逾期个人消费贷款本月实还款求和
        dict_out['consume_loan_slight_overdue_planRepayAmount_max_03m_now']=loan_second['planRepayAmount'][(loan_second['businessType']=='其他个人消费贷款') & (loan_second['month_Desc_report']<3) & (loan_second['overdue_class']==1) & (loan_second['is_now']==1)].max()#现行最近3个月轻度逾期个人消费贷款本月应还款最大
        dict_out['consume_loan_slight_overdue_RepayedAmount_man_03m_now']=loan_second['RepayedAmount'][(loan_second['businessType']=='其他个人消费贷款') & (loan_second['month_Desc_report']<3) & (loan_second['overdue_class']==1) & (loan_second['is_now']==1)].max()#现行最近3个月轻度逾期个人消费贷款本月实还款最大

        #最近3个月轻度逾期有担保个人消费贷款
        dict_out['consume_loan_is_vouch_slight_overdue_account_count_03m']=len(loan_second[(loan_second['businessType']=='其他个人消费贷款') & (loan_second['month_Desc_report']<3) & (loan_second['overdue_class']==1) & (loan_second['is_vouch']==1)])#最近3个月轻度逾期有担保个人消费贷款账户计数
        dict_out['consume_loan_is_vouch_slight_overdue_month_sum_03m']=sum(list(map(sum,loan_second['overdue_month'][(loan_second['businessType']=='其他个人消费贷款') & (loan_second['month_Desc_report']<3) & (loan_second['overdue_class']==1) & (loan_second['is_vouch']==1)])))#最近3个月轻度逾期有担保个人消费贷款逾期月数求和

        #现行最近3个月轻度逾期有担保个人消费贷款
        dict_out['consume_loan_is_vouch_slight_overdue_account_count_03m_now']=len(loan_second[(loan_second['businessType']=='其他个人消费贷款') & (loan_second['month_Desc_report']<3) & (loan_second['overdue_class']==1) & (loan_second['is_vouch']==1) & (loan_second['is_now']==1)])#现行最近3个月轻度逾期有担保个人消费贷款账户计数
        dict_out['consume_loan_is_vouch_slight_overdue_month_sum_03m_now']=sum(list(map(sum,loan_second['overdue_month'][(loan_second['businessType']=='其他个人消费贷款') & (loan_second['month_Desc_report']<3) & (loan_second['overdue_class']==1) & (loan_second['is_vouch']==1) & (loan_second['is_now']==1)])))#现行最近3个月轻度逾期有担保个人消费贷款逾期月数求和
        dict_out['consume_loan_is_vouch_slight_overdue_planRepayAmount_sum_03m_now']=loan_second['planRepayAmount'][(loan_second['businessType']=='其他个人消费贷款') & (loan_second['month_Desc_report']<3) & (loan_second['overdue_class']==1) & (loan_second['is_vouch']==1) & (loan_second['is_now']==1)].sum()#现行最近3个月轻度逾期有担保个人消费贷款本月应还款求和
        dict_out['consume_loan_is_vouch_slight_overdue_RepayedAmount_sum_03m_now']=loan_second['RepayedAmount'][(loan_second['businessType']=='其他个人消费贷款') & (loan_second['month_Desc_report']<3) & (loan_second['overdue_class']==1) & (loan_second['is_vouch']==1) & (loan_second['is_now']==1)].sum()#现行最近3个月轻度逾期有担保个人消费贷款本月实还款求和
        dict_out['consume_loan_is_vouch_slight_overdue_planRepayAmount_max_03m_now']=loan_second['planRepayAmount'][(loan_second['businessType']=='其他个人消费贷款') & (loan_second['month_Desc_report']<3) & (loan_second['overdue_class']==1) & (loan_second['is_vouch']==1) & (loan_second['is_now']==1)].max()#现行最近3个月轻度逾期有担保个人消费贷款本月应还款最大
        dict_out['consume_loan_is_vouch_slight_overdue_RepayedAmount_max_03m_now']=loan_second['RepayedAmount'][(loan_second['businessType']=='其他个人消费贷款') & (loan_second['month_Desc_report']<3) & (loan_second['overdue_class']==1) & (loan_second['is_vouch']==1) & (loan_second['is_now']==1)].max()#现行最近3个月轻度逾期有担保个人消费贷款本月实还款最大

        #最近6个月轻度逾期个人消费贷款
        dict_out['consume_loan_slight_overdue_account_count_06m']=len(loan_second[(loan_second['businessType']=='其他个人消费贷款') & (loan_second['month_Desc_report']<6) & (loan_second['overdue_class']==1)])#最近6个月轻度逾期个人消费贷款账户计数
        dict_out['consume_loan_slight_overdue_month_sum_06m']=sum(list(map(sum,loan_second['overdue_month'][(loan_second['businessType']=='其他个人消费贷款') & (loan_second['month_Desc_report']<6) & (loan_second['overdue_class']==1)])))#最近6个月轻度逾期个人消费贷款逾期月数求和

        #现行最近6个月轻度逾期个人消费贷款
        dict_out['consume_loan_slight_overdue_account_count_06m_now']=loan_second['overdue_month'][(loan_second['businessType']=='其他个人消费贷款款') & (loan_second['month_Desc_report']<6) & (loan_second['overdue_class']==1) & (loan_second['is_now']==1)].sum()#现行最近6个月轻度逾期个人消费贷款账户计数
        dict_out['consume_loan_slight_overdue_month_sum_06m_now']=sum(list(map(sum,loan_second['overdue_month'][(loan_second['businessType']=='其他个人消费贷款') & (loan_second['month_Desc_report']<6) & (loan_second['overdue_class']==1) & (loan_second['is_now']==1)])))#现行最近6个月轻度逾期个人消费贷款逾期月数求和
        dict_out['consume_loan_slight_overdue_planRepayAmount_sum_06m_now']=loan_second['planRepayAmount'][(loan_second['businessType']=='其他个人消费贷款') & (loan_second['month_Desc_report']<6) & (loan_second['overdue_class']==1) & (loan_second['is_now']==1)].sum()#现行最近6个月轻度逾期个人消费贷款本月应还款求和
        dict_out['consume_loan_slight_overdue_RepayedAmount_sum_06m_now']=loan_second['RepayedAmount'][(loan_second['businessType']=='其他个人消费贷款') & (loan_second['month_Desc_report']<6) & (loan_second['overdue_class']==1) & (loan_second['is_now']==1)].sum()#现行最近6个月轻度逾期个人消费贷款本月实还款求和
        dict_out['consume_loan_slight_overdue_planRepayAmount_max_06m_now']=loan_second['planRepayAmount'][(loan_second['businessType']=='其他个人消费贷款') & (loan_second['month_Desc_report']<6) & (loan_second['overdue_class']==1) & (loan_second['is_now']==1)].max()#现行最近6个月轻度逾期个人消费贷款本月应还款最大
        dict_out['consume_loan_slight_overdue_RepayedAmount_man_06m_now']=loan_second['RepayedAmount'][(loan_second['businessType']=='其他个人消费贷款') & (loan_second['month_Desc_report']<6) & (loan_second['overdue_class']==1) & (loan_second['is_now']==1)].max()#现行最近6个月轻度逾期个人消费贷款本月实还款最大

        #最近6个月轻度逾期有担保个人消费贷款
        dict_out['consume_loan_is_vouch_slight_overdue_account_count_06m']=len(loan_second[(loan_second['businessType']=='其他个人消费贷款') & (loan_second['month_Desc_report']<6) & (loan_second['overdue_class']==1) & (loan_second['is_vouch']==1)])#最近6个月轻度逾期有担保个人消费贷款账户计数
        dict_out['consume_loan_is_vouch_slight_overdue_month_sum_06m']=sum(list(map(sum,loan_second['overdue_month'][(loan_second['businessType']=='其他个人消费贷款') & (loan_second['month_Desc_report']<6) & (loan_second['overdue_class']==1) & (loan_second['is_vouch']==1)])))#最近6个月轻度逾期有担保个人消费贷款逾期月数求和

        #现行最近6个月轻度逾期有担保个人消费贷款
        dict_out['consume_loan_is_vouch_slight_overdue_account_count_06m_now']=len(loan_second[(loan_second['businessType']=='其他个人消费贷款') & (loan_second['month_Desc_report']<6) & (loan_second['overdue_class']==1) & (loan_second['is_vouch']==1) & (loan_second['is_now']==1)])#现行最近6个月轻度逾期有担保个人消费贷款账户计数
        dict_out['consume_loan_is_vouch_slight_overdue_month_sum_06m_now']=sum(list(map(sum,loan_second['overdue_month'][(loan_second['businessType']=='其他个人消费贷款') & (loan_second['month_Desc_report']<6) & (loan_second['overdue_class']==1) & (loan_second['is_vouch']==1) & (loan_second['is_now']==1)])))#现行最近6个月轻度逾期有担保个人消费贷款逾期月数求和
        dict_out['consume_loan_is_vouch_slight_overdue_planRepayAmount_sum_06m_now']=loan_second['planRepayAmount'][(loan_second['businessType']=='其他个人消费贷款') & (loan_second['month_Desc_report']<6) & (loan_second['overdue_class']==1) & (loan_second['is_vouch']==1) & (loan_second['is_now']==1)].sum()#现行最近6个月轻度逾期有担保个人消费贷款本月应还款求和
        dict_out['consume_loan_is_vouch_slight_overdue_RepayedAmount_sum_06m_now']=loan_second['RepayedAmount'][(loan_second['businessType']=='其他个人消费贷款') & (loan_second['month_Desc_report']<6) & (loan_second['overdue_class']==1) & (loan_second['is_vouch']==1) & (loan_second['is_now']==1)].sum()#现行最近6个月轻度逾期有担保个人消费贷款本月实还款求和
        dict_out['consume_loan_is_vouch_slight_overdue_planRepayAmount_max_06m_now']=loan_second['planRepayAmount'][(loan_second['businessType']=='其他个人消费贷款') & (loan_second['month_Desc_report']<6) & (loan_second['overdue_class']==1) & (loan_second['is_vouch']==1) & (loan_second['is_now']==1)].max()#现行最近6个月轻度逾期有担保个人消费贷款本月应还款最大
        dict_out['consume_loan_is_vouch_slight_overdue_RepayedAmount_max_06m_now']=loan_second['RepayedAmount'][(loan_second['businessType']=='其他个人消费贷款') & (loan_second['month_Desc_report']<6) & (loan_second['overdue_class']==1) & (loan_second['is_vouch']==1) & (loan_second['is_now']==1)].max()#现行最近6个月轻度逾期有担保个人消费贷款本月实还款最大

        #最近12个月轻度逾期个人消费贷款
        dict_out['consume_loan_slight_overdue_account_count_12m']=len(loan_second[(loan_second['businessType']=='其他个人消费贷款') & (loan_second['month_Desc_report']<12) & (loan_second['overdue_class']==1)])#最近12个月轻度逾期个人消费贷款账户计数
        dict_out['consume_loan_slight_overdue_month_sum_12m']=sum(list(map(sum,loan_second['overdue_month'][(loan_second['businessType']=='其他个人消费贷款') & (loan_second['month_Desc_report']<12) & (loan_second['overdue_class']==1)])))#最近12个月轻度逾期个人消费贷款逾期月数求和

        #现行最近12个月轻度逾期个人消费贷款
        dict_out['consume_loan_slight_overdue_account_count_12m_now']=loan_second['overdue_month'][(loan_second['businessType']=='其他个人消费贷款款') & (loan_second['month_Desc_report']<12) & (loan_second['overdue_class']==1) & (loan_second['is_now']==1)].sum()#现行最近12个月轻度逾期个人消费贷款账户计数
        dict_out['consume_loan_slight_overdue_month_sum_12m_now']=sum(list(map(sum,loan_second['overdue_month'][(loan_second['businessType']=='其他个人消费贷款') & (loan_second['month_Desc_report']<12) & (loan_second['overdue_class']==1) & (loan_second['is_now']==1)])))#现行最近12个月轻度逾期个人消费贷款逾期月数求和
        dict_out['consume_loan_slight_overdue_planRepayAmount_sum_12m_now']=loan_second['planRepayAmount'][(loan_second['businessType']=='其他个人消费贷款') & (loan_second['month_Desc_report']<12) & (loan_second['overdue_class']==1) & (loan_second['is_now']==1)].sum()#现行最近12个月轻度逾期个人消费贷款本月应还款求和
        dict_out['consume_loan_slight_overdue_RepayedAmount_sum_12m_now']=loan_second['RepayedAmount'][(loan_second['businessType']=='其他个人消费贷款') & (loan_second['month_Desc_report']<12) & (loan_second['overdue_class']==1) & (loan_second['is_now']==1)].sum()#现行最近12个月轻度逾期个人消费贷款本月实还款求和
        dict_out['consume_loan_slight_overdue_planRepayAmount_max_12m_now']=loan_second['planRepayAmount'][(loan_second['businessType']=='其他个人消费贷款') & (loan_second['month_Desc_report']<12) & (loan_second['overdue_class']==1) & (loan_second['is_now']==1)].max()#现行最近12个月轻度逾期个人消费贷款本月应还款最大
        dict_out['consume_loan_slight_overdue_RepayedAmount_man_12m_now']=loan_second['RepayedAmount'][(loan_second['businessType']=='其他个人消费贷款') & (loan_second['month_Desc_report']<12) & (loan_second['overdue_class']==1) & (loan_second['is_now']==1)].max()#现行最近12个月轻度逾期个人消费贷款本月实还款最大

        #最近12个月轻度逾期有担保个人消费贷款
        dict_out['consume_loan_is_vouch_slight_overdue_account_count_12m']=len(loan_second[(loan_second['businessType']=='其他个人消费贷款') & (loan_second['month_Desc_report']<12) & (loan_second['overdue_class']==1) & (loan_second['is_vouch']==1)])#最近12个月轻度逾期有担保个人消费贷款账户计数
        dict_out['consume_loan_is_vouch_slight_overdue_month_sum_12m']=sum(list(map(sum,loan_second['overdue_month'][(loan_second['businessType']=='其他个人消费贷款') & (loan_second['month_Desc_report']<12) & (loan_second['overdue_class']==1) & (loan_second['is_vouch']==1)])))#最近12个月轻度逾期有担保个人消费贷款逾期月数求和

        #现行最近12个月轻度逾期有担保个人消费贷款
        dict_out['consume_loan_is_vouch_slight_overdue_account_count_12m_now']=len(loan_second[(loan_second['businessType']=='其他个人消费贷款') & (loan_second['month_Desc_report']<12) & (loan_second['overdue_class']==1) & (loan_second['is_vouch']==1) & (loan_second['is_now']==1)])#现行最近12个月轻度逾期有担保个人消费贷款账户计数
        dict_out['consume_loan_is_vouch_slight_overdue_month_sum_12m_now']=sum(list(map(sum,loan_second['overdue_month'][(loan_second['businessType']=='其他个人消费贷款') & (loan_second['month_Desc_report']<12) & (loan_second['overdue_class']==1) & (loan_second['is_vouch']==1) & (loan_second['is_now']==1)])))#现行最近12个月轻度逾期有担保个人消费贷款逾期月数求和
        dict_out['consume_loan_is_vouch_slight_overdue_planRepayAmount_sum_12m_now']=loan_second['planRepayAmount'][(loan_second['businessType']=='其他个人消费贷款') & (loan_second['month_Desc_report']<12) & (loan_second['overdue_class']==1) & (loan_second['is_vouch']==1) & (loan_second['is_now']==1)].sum()#现行最近12个月轻度逾期有担保个人消费贷款本月应还款求和
        dict_out['consume_loan_is_vouch_slight_overdue_RepayedAmount_sum_12m_now']=loan_second['RepayedAmount'][(loan_second['businessType']=='其他个人消费贷款') & (loan_second['month_Desc_report']<12) & (loan_second['overdue_class']==1) & (loan_second['is_vouch']==1) & (loan_second['is_now']==1)].sum()#现行最近12个月轻度逾期有担保个人消费贷款本月实还款求和
        dict_out['consume_loan_is_vouch_slight_overdue_planRepayAmount_max_12m_now']=loan_second['planRepayAmount'][(loan_second['businessType']=='其他个人消费贷款') & (loan_second['month_Desc_report']<12) & (loan_second['overdue_class']==1) & (loan_second['is_vouch']==1) & (loan_second['is_now']==1)].max()#现行最近12个月轻度逾期有担保个人消费贷款本月应还款最大
        dict_out['consume_loan_is_vouch_slight_overdue_RepayedAmount_max_12m_now']=loan_second['RepayedAmount'][(loan_second['businessType']=='其他个人消费贷款') & (loan_second['month_Desc_report']<12) & (loan_second['overdue_class']==1) & (loan_second['is_vouch']==1) & (loan_second['is_now']==1)].max()#现行最近12个月轻度逾期有担保个人消费贷款本月实还款最大

        #最近24个月轻度逾期个人消费贷款
        dict_out['consume_loan_slight_overdue_account_count_24m']=len(loan_second[(loan_second['businessType']=='其他个人消费贷款') & (loan_second['month_Desc_report']<24) & (loan_second['overdue_class']==1)])#最近24个月轻度逾期个人消费贷款账户计数
        dict_out['consume_loan_slight_overdue_month_sum_24m']=sum(list(map(sum,loan_second['overdue_month'][(loan_second['businessType']=='其他个人消费贷款') & (loan_second['month_Desc_report']<24) & (loan_second['overdue_class']==1)])))#最近24个月轻度逾期个人消费贷款逾期月数求和

        #现行最近24个月轻度逾期个人消费贷款
        dict_out['consume_loan_slight_overdue_account_count_24m_now']=loan_second['overdue_month'][(loan_second['businessType']=='其他个人消费贷款款') & (loan_second['month_Desc_report']<24) & (loan_second['overdue_class']==1) & (loan_second['is_now']==1)].sum()#现行最近24个月轻度逾期个人消费贷款账户计数
        dict_out['consume_loan_slight_overdue_month_sum_24m_now']=sum(list(map(sum,loan_second['overdue_month'][(loan_second['businessType']=='其他个人消费贷款') & (loan_second['month_Desc_report']<24) & (loan_second['overdue_class']==1) & (loan_second['is_now']==1)])))#现行最近24个月轻度逾期个人消费贷款逾期月数求和
        dict_out['consume_loan_slight_overdue_planRepayAmount_sum_24m_now']=loan_second['planRepayAmount'][(loan_second['businessType']=='其他个人消费贷款') & (loan_second['month_Desc_report']<24) & (loan_second['overdue_class']==1) & (loan_second['is_now']==1)].sum()#现行最近24个月轻度逾期个人消费贷款本月应还款求和
        dict_out['consume_loan_slight_overdue_RepayedAmount_sum_24m_now']=loan_second['RepayedAmount'][(loan_second['businessType']=='其他个人消费贷款') & (loan_second['month_Desc_report']<24) & (loan_second['overdue_class']==1) & (loan_second['is_now']==1)].sum()#现行最近24个月轻度逾期个人消费贷款本月实还款求和
        dict_out['consume_loan_slight_overdue_planRepayAmount_max_24m_now']=loan_second['planRepayAmount'][(loan_second['businessType']=='其他个人消费贷款') & (loan_second['month_Desc_report']<24) & (loan_second['overdue_class']==1) & (loan_second['is_now']==1)].max()#现行最近24个月轻度逾期个人消费贷款本月应还款最大
        dict_out['consume_loan_slight_overdue_RepayedAmount_man_24m_now']=loan_second['RepayedAmount'][(loan_second['businessType']=='其他个人消费贷款') & (loan_second['month_Desc_report']<24) & (loan_second['overdue_class']==1) & (loan_second['is_now']==1)].max()#现行最近24个月轻度逾期个人消费贷款本月实还款最大

        #最近24个月轻度逾期有担保个人消费贷款
        dict_out['consume_loan_is_vouch_slight_overdue_account_count_24m']=len(loan_second[(loan_second['businessType']=='其他个人消费贷款') & (loan_second['month_Desc_report']<24) & (loan_second['overdue_class']==1) & (loan_second['is_vouch']==1)])#最近24个月轻度逾期有担保个人消费贷款账户计数
        dict_out['consume_loan_is_vouch_slight_overdue_month_sum_24m']=sum(list(map(sum,loan_second['overdue_month'][(loan_second['businessType']=='其他个人消费贷款') & (loan_second['month_Desc_report']<24) & (loan_second['overdue_class']==1) & (loan_second['is_vouch']==1)])))#最近24个月轻度逾期有担保个人消费贷款逾期月数求和

        #现行最近24个月轻度逾期有担保个人消费贷款
        dict_out['consume_loan_is_vouch_slight_overdue_account_count_24m_now']=len(loan_second[(loan_second['businessType']=='其他个人消费贷款') & (loan_second['month_Desc_report']<24) & (loan_second['overdue_class']==1) & (loan_second['is_vouch']==1) & (loan_second['is_now']==1)])#现行最近24个月轻度逾期有担保个人消费贷款账户计数
        dict_out['consume_loan_is_vouch_slight_overdue_month_sum_24m_now']=sum(list(map(sum,loan_second['overdue_month'][(loan_second['businessType']=='其他个人消费贷款') & (loan_second['month_Desc_report']<24) & (loan_second['overdue_class']==1) & (loan_second['is_vouch']==1) & (loan_second['is_now']==1)])))#现行最近24个月轻度逾期有担保个人消费贷款逾期月数求和
        dict_out['consume_loan_is_vouch_slight_overdue_planRepayAmount_sum_24m_now']=loan_second['planRepayAmount'][(loan_second['businessType']=='其他个人消费贷款') & (loan_second['month_Desc_report']<24) & (loan_second['overdue_class']==1) & (loan_second['is_vouch']==1) & (loan_second['is_now']==1)].sum()#现行最近24个月轻度逾期有担保个人消费贷款本月应还款求和
        dict_out['consume_loan_is_vouch_slight_overdue_RepayedAmount_sum_24m_now']=loan_second['RepayedAmount'][(loan_second['businessType']=='其他个人消费贷款') & (loan_second['month_Desc_report']<24) & (loan_second['overdue_class']==1) & (loan_second['is_vouch']==1) & (loan_second['is_now']==1)].sum()#现行最近24个月轻度逾期有担保个人消费贷款本月实还款求和
        dict_out['consume_loan_is_vouch_slight_overdue_planRepayAmount_max_24m_now']=loan_second['planRepayAmount'][(loan_second['businessType']=='其他个人消费贷款') & (loan_second['month_Desc_report']<24) & (loan_second['overdue_class']==1) & (loan_second['is_vouch']==1) & (loan_second['is_now']==1)].max()#现行最近24个月轻度逾期有担保个人消费贷款本月应还款最大
        dict_out['consume_loan_is_vouch_slight_overdue_RepayedAmount_max_24m_now']=loan_second['RepayedAmount'][(loan_second['businessType']=='其他个人消费贷款') & (loan_second['month_Desc_report']<24) & (loan_second['overdue_class']==1) & (loan_second['is_vouch']==1) & (loan_second['is_now']==1)].max()#现行最近24个月轻度逾期有担保个人消费贷款本月实还款最大

        #严重逾期个人消费贷款
        dict_out['consume_loan_serious_overdue_account_count']=len(loan_second[(loan_second['businessType']=='其他个人消费贷款') & (loan_second['overdue_class']==3)])#严重逾期个人消费贷款账户计数
        dict_out['consume_loan_serious_overdue_month_sum']=sum(list(map(sum,loan_second['overdue_month'][(loan_second['businessType']=='其他个人消费贷款') & (loan_second['overdue_class']==3)])))#严重逾期个人消费贷款逾期月数求和
        dict_out['consume_loan_serious_overdue_amount_sum']=loan_second['loanAmount'][(loan_second['businessType']=='其他个人消费贷款') & (loan_second['overdue_class']==3)].sum()#严重逾期个人消费贷款本金求和
        dict_out['consume_loan_serious_overdue_amount_max']=loan_second['loanAmount'][(loan_second['businessType']=='其他个人消费贷款') & (loan_second['overdue_class']==3)].max()#严重逾期个人消费贷款本金最大

        #现行严重逾期个人消费贷款
        dict_out['consume_loan_serious_overdue_account_count_now']=len(loan_second[(loan_second['businessType']=='其他个人消费贷款') & (loan_second['overdue_class']==3) & (loan_second['is_now']==1)])#现行严重逾期个人消费贷款账户计数
        dict_out['consume_loan_serious_overdue_month_sum_now']=sum(list(map(sum,loan_second['overdue_month'][(loan_second['businessType']=='其他个人消费贷款') & (loan_second['overdue_class']==3) & (loan_second['is_now']==1)])))#现行严重逾期个人消费贷款逾期月数求和
        dict_out['consume_loan_serious_overdue_amount_sum_now']=loan_second['loanAmount'][(loan_second['businessType']=='其他个人消费贷款') & (loan_second['overdue_class']==3) & (loan_second['is_now']==1)].sum()#现行严重逾期个人消费贷款本金求和
        dict_out['consume_loan_serious_overdue_balance_sum_now']=loan_second['balance'][(loan_second['businessType']=='其他个人消费贷款') & (loan_second['overdue_class']==3) & (loan_second['is_now']==1)].sum()#现行严重逾期个人消费贷款本金余额求和
        dict_out['consume_loan_serious_overdue_planRepayAmount_sum_now']=loan_second['planRepayAmount'][(loan_second['businessType']=='其他个人消费贷款') & (loan_second['overdue_class']==3) & (loan_second['is_now']==1)].sum()#现行严重逾期个人消费贷款本月应还款求和
        dict_out['consume_loan_serious_overdue_RepayedAmount_sum_now']=loan_second['RepayedAmount'][(loan_second['businessType']=='其他个人消费贷款') & (loan_second['overdue_class']==3) & (loan_second['is_now']==1)].sum()#现行严重逾期个人消费贷款本月实还款求和
        dict_out['consume_loan_serious_overdue_currentOverdueAmount_sum_now']=loan_second['currentOverdueAmount'][(loan_second['businessType']=='其他个人消费贷款') & (loan_second['overdue_class']==3) & (loan_second['is_now']==1)].sum()#现行严重逾期个人消费贷款当前逾期金额求和
        dict_out['consume_loan_serious_overdue_amount_max_now']=loan_second['loanAmount'][(loan_second['businessType']=='其他个人消费贷款') & (loan_second['overdue_class']==3) & (loan_second['is_now']==1)].max()#现行严重逾期个人消费贷款本金最大
        dict_out['consume_loan_serious_overdue_balance_max_now']=loan_second['balance'][(loan_second['businessType']=='其他个人消费贷款') & (loan_second['overdue_class']==3) & (loan_second['is_now']==1)].max()#现行严重逾期个人消费贷款本金余额最大
        dict_out['consume_loan_serious_overdue_planRepayAmount_max_now']=loan_second['planRepayAmount'][(loan_second['businessType']=='其他个人消费贷款') & (loan_second['overdue_class']==3) & (loan_second['is_now']==1)].max()#现行严重逾期个人消费贷款本月应还款最大
        dict_out['consume_loan_serious_overdue_RepayedAmount_max_now']=loan_second['RepayedAmount'][(loan_second['businessType']=='其他个人消费贷款') & (loan_second['overdue_class']==3) & (loan_second['is_now']==1)].max()#现行严重逾期个人消费贷款本月实还款最大
        dict_out['consume_loan_serious_overdue_currentOverdueAmount_max_now']=loan_second['currentOverdueAmount'][(loan_second['businessType']=='其他个人消费贷款') & (loan_second['overdue_class']==3) & (loan_second['is_now']==1)].max()#现行严重逾期个人消费贷款当前逾期金额最大


        #严重逾期有担保个人消费贷款
        dict_out['consume_loan_is_vouch_serious_overdue_account_count']=len(loan_second[(loan_second['businessType']=='其他个人消费贷款') & (loan_second['overdue_class']==3) & (loan_second['is_vouch']==1)])#严重逾期有担保个人消费贷款账户计数
        dict_out['consume_loan_is_vouch_serious_overdue_month_sum']=sum(list(map(sum,loan_second['overdue_month'][(loan_second['businessType']=='其他个人消费贷款') & (loan_second['overdue_class']==3) & (loan_second['is_vouch']==1)])))#严重逾期有担保个人消费贷款逾期月数求和
        dict_out['consume_loan_is_vouch_serious_overdue_amount_sum']=loan_second['loanAmount'][(loan_second['businessType']=='其他个人消费贷款') & (loan_second['overdue_class']==3) & (loan_second['is_vouch']==1)].sum()#严重逾期有担保个人消费贷款本金求和
        dict_out['consume_loan_is_vouch_serious_overdue_amount_max']=loan_second['loanAmount'][(loan_second['businessType']=='其他个人消费贷款') & (loan_second['overdue_class']==3) & (loan_second['is_vouch']==1)].max()#严重逾期有担保个人消费贷款本金最大

        #现行严重逾期有担保个人消费贷款
        dict_out['consume_loan_is_vouch_serious_overdue_account_count_now']=len(loan_second[(loan_second['businessType']=='其他个人消费贷款') & (loan_second['overdue_class']==3) & (loan_second['is_vouch']==1) & (loan_second['is_now']==1)])#现行严重逾期有担保个人消费贷款账户计数
        dict_out['consume_loan_is_vouch_serious_overdue_month_sum_now']=sum(list(map(sum,loan_second['overdue_month'][(loan_second['businessType']=='其他个人消费贷款') & (loan_second['overdue_class']==3) & (loan_second['is_vouch']==1) & (loan_second['is_now']==1)])))#现行严重逾期有担保个人消费贷款逾期月数求和
        dict_out['consume_loan_is_vouch_serious_overdue_amount_sum_now']=loan_second['loanAmount'][(loan_second['businessType']=='其他个人消费贷款') & (loan_second['overdue_class']==3) & (loan_second['is_vouch']==1) & (loan_second['is_now']==1)].sum()#现行严重逾期有担保个人消费贷款本金求和
        dict_out['consume_loan_is_vouch_serious_overdue_balance_sum_now']=loan_second['balance'][(loan_second['businessType']=='其他个人消费贷款') & (loan_second['overdue_class']==3) & (loan_second['is_vouch']==1) & (loan_second['is_now']==1)].sum()#现行严重逾期有担保个人消费贷款本金余额求和
        dict_out['consume_loan_is_vouch_serious_overdue_planRepayAmount_sum_now']=loan_second['planRepayAmount'][(loan_second['businessType']=='其他个人消费贷款') & (loan_second['overdue_class']==3) & (loan_second['is_vouch']==1) & (loan_second['is_now']==1)].sum()#现行严重逾期有担保个人消费贷款本月应还款求和
        dict_out['consume_loan_is_vouch_serious_overdue_RepayedAmount_sum_now']=loan_second['RepayedAmount'][(loan_second['businessType']=='其他个人消费贷款') & (loan_second['overdue_class']==3) & (loan_second['is_vouch']==1) & (loan_second['is_now']==1)].sum()#现行严重逾期有担保个人消费贷款本月实还款求和
        dict_out['consume_loan_is_vouch_serious_overdue_currentOverdueAmount_sum_now']=loan_second['currentOverdueAmount'][(loan_second['businessType']=='其他个人消费贷款') & (loan_second['overdue_class']==3) & (loan_second['is_vouch']==1) & (loan_second['is_now']==1)].sum()#现行严重逾期有担保个人消费贷款当前逾期金额求和
        dict_out['consume_loan_is_vouch_serious_overdue_amount_max_now']=loan_second['loanAmount'][(loan_second['businessType']=='其他个人消费贷款') & (loan_second['overdue_class']==3) & (loan_second['is_vouch']==1) & (loan_second['is_now']==1)].max()#现行严重逾期有担保个人消费贷款本金最大
        dict_out['consume_loan_is_vouch_serious_overdue_balance_max_now']=loan_second['balance'][(loan_second['businessType']=='其他个人消费贷款') & (loan_second['overdue_class']==3) & (loan_second['is_vouch']==1) & (loan_second['is_now']==1)].max()#现行严重逾期有担保个人消费贷款本金余额最大
        dict_out['consume_loan_is_vouch_serious_overdue_planRepayAmount_max_now']=loan_second['planRepayAmount'][(loan_second['businessType']=='其他个人消费贷款') & (loan_second['overdue_class']==3) & (loan_second['is_vouch']==1) & (loan_second['is_now']==1)].max()#现行严重逾期有担保个人消费贷款本月应还款最大
        dict_out['consume_loan_is_vouch_serious_overdue_RepayedAmount_max_now']=loan_second['RepayedAmount'][(loan_second['businessType']=='其他个人消费贷款') & (loan_second['overdue_class']==3) & (loan_second['is_vouch']==1) & (loan_second['is_now']==1)].max()#现行严重逾期有担保个人消费贷款本月实还款最大
        dict_out['consume_loan_is_vouch_serious_overdue_currentOverdueAmount_max_now']=loan_second['currentOverdueAmount'][(loan_second['businessType']=='其他个人消费贷款') & (loan_second['overdue_class']==3) & (loan_second['is_vouch']==1) & (loan_second['is_now']==1)].max()#现行严重逾期有担保个人消费贷款当前逾期金额最大

        #最近3个月严重逾期贷款
        dict_out['loan_serious_overdue_account_count_03m']=len(loan_second[(loan_second['month_Desc_report']<3) & (loan_second['overdue_class']==3)])#最近3个月严重逾期贷款账户计数
        dict_out['loan_serious_overdue_month_sum_03m']=sum(list(map(sum,loan_second['overdue_month'][(loan_second['month_Desc_report']<3) & (loan_second['overdue_class']==3)])))#最近3个月严重逾期贷款逾期月数求和

        #现行最近3个月严重逾期贷款
        dict_out['loan_serious_overdue_account_count_03m_now']=len(loan_second[(loan_second['month_Desc_report']<3) & (loan_second['overdue_class']==3) & (loan_second['is_now']==1)])#现行最近3个月严重逾期贷款账户计数
        dict_out['loan_serious_overdue_month_sum_03m_now']=sum(list(map(sum,loan_second['overdue_month'][(loan_second['month_Desc_report']<3) & (loan_second['overdue_class']==3) & (loan_second['is_now']==1)])))#现行最近3个月严重逾期贷款逾期月数求和
        dict_out['loan_serious_overdue_planRepayAmount_sum_03m_now']=loan_second['planRepayAmount'][(loan_second['month_Desc_report']<3) & (loan_second['overdue_class']==3) & (loan_second['is_now']==1)].sum()#现行最近3个月严重逾期贷款本月应还款求和
        dict_out['loan_serious_overdue_RepayedAmount_sum_03m_now']=loan_second['RepayedAmount'][(loan_second['month_Desc_report']<3) & (loan_second['overdue_class']==3) & (loan_second['is_now']==1)].sum()#现行最近3个月严重逾期贷款本月实还款求和
        dict_out['loan_serious_overdue_planRepayAmount_max_03m_now']=loan_second['planRepayAmount'][(loan_second['month_Desc_report']<3) & (loan_second['overdue_class']==3) & (loan_second['is_now']==1)].max()#现行最近3个月严重逾期贷款本月应还款最大
        dict_out['loan_serious_overdue_RepayedAmount_max_03m_now']=loan_second['RepayedAmount'][(loan_second['month_Desc_report']<3) & (loan_second['overdue_class']==3) & (loan_second['is_now']==1)].max()#现行最近3个月严重逾期贷款本月实还款最大

        #最近3个月严重逾期有担保贷款
        dict_out['loan_is_vouch_serious_overdue_account_count_03m']=len(loan_second[(loan_second['month_Desc_report']<3) & (loan_second['overdue_class']==3) & (loan_second['is_vouch']==1)])#最近3个月严重逾期有担保贷款账户计数
        dict_out['loan_is_vouch_serious_overdue_month_sum_03m']=sum(list(map(sum,loan_second['overdue_month'][(loan_second['month_Desc_report']<3) & (loan_second['overdue_class']==3) & (loan_second['is_vouch']==1)])))#最近3个月严重逾期有担保贷款逾期月数求和

        #现行最近3个月严重逾期有担保贷款
        dict_out['loan_is_vouch_serious_overdue_account_count_03m_now']=len(loan_second[(loan_second['month_Desc_report']<3) & (loan_second['overdue_class']==3) & (loan_second['is_vouch']==1) & (loan_second['is_now']==1)])#现行最近3个月严重逾期有担保贷款账户计数
        dict_out['loan_is_vouch_serious_overdue_month_sum_03m_now']=sum(list(map(sum,loan_second['overdue_month'][(loan_second['month_Desc_report']<3) & (loan_second['overdue_class']==3) & (loan_second['is_vouch']==1) & (loan_second['is_now']==1)])))#现行最近3个月严重逾期有担保贷款逾期月数求和
        dict_out['loan_is_vouch_serious_overdue_planRepayAmount_sum_03m_now']=loan_second['planRepayAmount'][(loan_second['month_Desc_report']<3) & (loan_second['overdue_class']==3) & (loan_second['is_vouch']==1) & (loan_second['is_now']==1)].sum()#现行最近3个月严重逾期有担保贷款本月应还款求和
        dict_out['loan_is_vouch_serious_overdue_RepayedAmount_sum_03m_now']=loan_second['RepayedAmount'][(loan_second['month_Desc_report']<3) & (loan_second['overdue_class']==3) & (loan_second['is_vouch']==1) & (loan_second['is_now']==1)].sum()#现行最近3个月严重逾期有担保贷款本月实还款求和
        dict_out['loan_is_vouch_serious_overdue_planRepayAmount_max_03m_now']=loan_second['planRepayAmount'][(loan_second['month_Desc_report']<3) & (loan_second['overdue_class']==3) & (loan_second['is_vouch']==1) & (loan_second['is_now']==1)].max()#现行最近3个月严重逾期有担保贷款本月应还款最大
        dict_out['loan_is_vouch_serious_overdue_RepayedAmount_max_03m_now']=loan_second['RepayedAmount'][(loan_second['month_Desc_report']<3) & (loan_second['overdue_class']==3) & (loan_second['is_vouch']==1) & (loan_second['is_now']==1)].max()#现行最近3个月严重逾期有担保贷款本月实还款最大

        #最近6个月严重逾期贷款
        dict_out['loan_serious_overdue_account_count_06m']=len(loan_second[(loan_second['month_Desc_report']<6) & (loan_second['overdue_class']==3)])#最近6个月严重逾期贷款账户计数
        dict_out['loan_serious_overdue_month_sum_06m']=sum(list(map(sum,loan_second['overdue_month'][(loan_second['month_Desc_report']<6) & (loan_second['overdue_class']==3)])))#最近6个月严重逾期贷款逾期月数求和

        #现行最近6个月严重逾期贷款
        dict_out['loan_serious_overdue_account_count_06m_now']=len(loan_second[(loan_second['month_Desc_report']<6) & (loan_second['overdue_class']==3) & (loan_second['is_now']==1)])#现行最近6个月严重逾期贷款账户计数
        dict_out['loan_serious_overdue_month_sum_06m_now']=sum(list(map(sum,loan_second['overdue_month'][(loan_second['month_Desc_report']<6) & (loan_second['overdue_class']==3) & (loan_second['is_now']==1)])))#现行最近6个月严重逾期贷款逾期月数求和
        dict_out['loan_serious_overdue_planRepayAmount_sum_06m_now']=loan_second['planRepayAmount'][(loan_second['month_Desc_report']<6) & (loan_second['overdue_class']==3) & (loan_second['is_now']==1)].sum()#现行最近6个月严重逾期贷款本月应还款求和
        dict_out['loan_serious_overdue_RepayedAmount_sum_06m_now']=loan_second['RepayedAmount'][(loan_second['month_Desc_report']<6) & (loan_second['overdue_class']==3) & (loan_second['is_now']==1)].sum()#现行最近6个月严重逾期贷款本月实还款求和
        dict_out['loan_serious_overdue_planRepayAmount_max_06m_now']=loan_second['planRepayAmount'][(loan_second['month_Desc_report']<6) & (loan_second['overdue_class']==3) & (loan_second['is_now']==1)].max()#现行最近6个月严重逾期贷款本月应还款最大
        dict_out['loan_serious_overdue_RepayedAmount_max_06m_now']=loan_second['RepayedAmount'][(loan_second['month_Desc_report']<6) & (loan_second['overdue_class']==3) & (loan_second['is_now']==1)].max()#现行最近6个月严重逾期贷款本月实还款最大

        #最近6个月严重逾期有担保贷款
        dict_out['loan_is_vouch_serious_overdue_account_count_06m']=len(loan_second[(loan_second['month_Desc_report']<6) & (loan_second['overdue_class']==3) & (loan_second['is_vouch']==1)])#最近6个月严重逾期有担保贷款账户计数
        dict_out['loan_is_vouch_serious_overdue_month_sum_06m']=sum(list(map(sum,loan_second['overdue_month'][(loan_second['month_Desc_report']<6) & (loan_second['overdue_class']==3) & (loan_second['is_vouch']==1)])))#最近6个月严重逾期有担保贷款逾期月数求和

        #现行最近6个月严重逾期有担保贷款
        dict_out['loan_is_vouch_serious_overdue_account_count_06m_now']=len(loan_second[(loan_second['month_Desc_report']<6) & (loan_second['overdue_class']==3) & (loan_second['is_vouch']==1) & (loan_second['is_now']==1)])#现行最近6个月严重逾期有担保贷款账户计数
        dict_out['loan_is_vouch_serious_overdue_month_sum_06m_now']=sum(list(map(sum,loan_second['overdue_month'][(loan_second['month_Desc_report']<6) & (loan_second['overdue_class']==3) & (loan_second['is_vouch']==1) & (loan_second['is_now']==1)])))#现行最近6个月严重逾期有担保贷款逾期月数求和
        dict_out['loan_is_vouch_serious_overdue_planRepayAmount_sum_06m_now']=loan_second['planRepayAmount'][(loan_second['month_Desc_report']<6) & (loan_second['overdue_class']==3) & (loan_second['is_vouch']==1) & (loan_second['is_now']==1)].sum()#现行最近6个月严重逾期有担保贷款本月应还款求和
        dict_out['loan_is_vouch_serious_overdue_RepayedAmount_sum_06m_now']=loan_second['RepayedAmount'][(loan_second['month_Desc_report']<6) & (loan_second['overdue_class']==3) & (loan_second['is_vouch']==1) & (loan_second['is_now']==1)].sum()#现行最近6个月严重逾期有担保贷款本月实还款求和
        dict_out['loan_is_vouch_serious_overdue_planRepayAmount_max_06m_now']=loan_second['planRepayAmount'][(loan_second['month_Desc_report']<6) & (loan_second['overdue_class']==3) & (loan_second['is_vouch']==1) & (loan_second['is_now']==1)].max()#现行最近6个月严重逾期有担保贷款本月应还款最大
        dict_out['loan_is_vouch_serious_overdue_RepayedAmount_max_06m_now']=loan_second['RepayedAmount'][(loan_second['month_Desc_report']<6) & (loan_second['overdue_class']==3) & (loan_second['is_vouch']==1) & (loan_second['is_now']==1)].max()#现行最近6个月严重逾期有担保贷款本月实还款最大

        #最近12个月严重逾期贷款
        dict_out['loan_serious_overdue_account_count_12m']=len(loan_second[(loan_second['month_Desc_report']<12) & (loan_second['overdue_class']==3)])#最近12个月严重逾期贷款账户计数
        dict_out['loan_serious_overdue_month_sum_12m']=sum(list(map(sum,loan_second['overdue_month'][(loan_second['month_Desc_report']<12) & (loan_second['overdue_class']==3)])))#最近12个月严重逾期贷款逾期月数求和

        #现行最近12个月严重逾期贷款
        dict_out['loan_serious_overdue_account_count_12m_now']=len(loan_second[(loan_second['month_Desc_report']<12) & (loan_second['overdue_class']==3) & (loan_second['is_now']==1)])#现行最近12个月严重逾期贷款账户计数
        dict_out['loan_serious_overdue_month_sum_12m_now']=sum(list(map(sum,loan_second['overdue_month'][(loan_second['month_Desc_report']<12) & (loan_second['overdue_class']==3) & (loan_second['is_now']==1)])))#现行最近12个月严重逾期贷款逾期月数求和
        dict_out['loan_serious_overdue_planRepayAmount_sum_12m_now']=loan_second['planRepayAmount'][(loan_second['month_Desc_report']<12) & (loan_second['overdue_class']==3) & (loan_second['is_now']==1)].sum()#现行最近12个月严重逾期贷款本月应还款求和
        dict_out['loan_serious_overdue_RepayedAmount_sum_12m_now']=loan_second['RepayedAmount'][(loan_second['month_Desc_report']<12) & (loan_second['overdue_class']==3) & (loan_second['is_now']==1)].sum()#现行最近12个月严重逾期贷款本月实还款求和
        dict_out['loan_serious_overdue_planRepayAmount_max_12m_now']=loan_second['planRepayAmount'][(loan_second['month_Desc_report']<12) & (loan_second['overdue_class']==3) & (loan_second['is_now']==1)].max()#现行最近12个月严重逾期贷款本月应还款最大
        dict_out['loan_serious_overdue_RepayedAmount_max_12m_now']=loan_second['RepayedAmount'][(loan_second['month_Desc_report']<12) & (loan_second['overdue_class']==3) & (loan_second['is_now']==1)].max()#现行最近12个月严重逾期贷款本月实还款最大

        #最近12个月严重逾期有担保贷款
        dict_out['loan_is_vouch_serious_overdue_account_count_12m']=len(loan_second[(loan_second['month_Desc_report']<12) & (loan_second['overdue_class']==3) & (loan_second['is_vouch']==1)])#最近12个月严重逾期有担保贷款账户计数
        dict_out['loan_is_vouch_serious_overdue_month_sum_12m']=sum(list(map(sum,loan_second['overdue_month'][(loan_second['month_Desc_report']<12) & (loan_second['overdue_class']==3) & (loan_second['is_vouch']==1)])))#最近12个月严重逾期有担保贷款逾期月数求和

        #现行最近12个月严重逾期有担保贷款
        dict_out['loan_is_vouch_serious_overdue_account_count_12m_now']=len(loan_second[(loan_second['month_Desc_report']<12) & (loan_second['overdue_class']==3) & (loan_second['is_vouch']==1) & (loan_second['is_now']==1)])#现行最近12个月严重逾期有担保贷款账户计数
        dict_out['loan_is_vouch_serious_overdue_month_sum_12m_now']=sum(list(map(sum,loan_second['overdue_month'][(loan_second['month_Desc_report']<12) & (loan_second['overdue_class']==3) & (loan_second['is_vouch']==1) & (loan_second['is_now']==1)])))#现行最近12个月严重逾期有担保贷款逾期月数求和
        dict_out['loan_is_vouch_serious_overdue_planRepayAmount_sum_12m_now']=loan_second['planRepayAmount'][(loan_second['month_Desc_report']<12) & (loan_second['overdue_class']==3) & (loan_second['is_vouch']==1) & (loan_second['is_now']==1)].sum()#现行最近12个月严重逾期有担保贷款本月应还款求和
        dict_out['loan_is_vouch_serious_overdue_RepayedAmount_sum_12m_now']=loan_second['RepayedAmount'][(loan_second['month_Desc_report']<12) & (loan_second['overdue_class']==3) & (loan_second['is_vouch']==1) & (loan_second['is_now']==1)].sum()#现行最近12个月严重逾期有担保贷款本月实还款求和
        dict_out['loan_is_vouch_serious_overdue_planRepayAmount_max_12m_now']=loan_second['planRepayAmount'][(loan_second['month_Desc_report']<12) & (loan_second['overdue_class']==3) & (loan_second['is_vouch']==1) & (loan_second['is_now']==1)].max()#现行最近12个月严重逾期有担保贷款本月应还款最大
        dict_out['loan_is_vouch_serious_overdue_RepayedAmount_max_12m_now']=loan_second['RepayedAmount'][(loan_second['month_Desc_report']<12) & (loan_second['overdue_class']==3) & (loan_second['is_vouch']==1) & (loan_second['is_now']==1)].max()#现行最近12个月严重逾期有担保贷款本月实还款最大

        #最近24个月严重逾期贷款
        dict_out['loan_serious_overdue_account_count_24m']=len(loan_second[(loan_second['month_Desc_report']<24) & (loan_second['overdue_class']==3)])#最近24个月严重逾期贷款账户计数
        dict_out['loan_serious_overdue_month_sum_24m']=sum(list(map(sum,loan_second['overdue_month'][(loan_second['month_Desc_report']<24) & (loan_second['overdue_class']==3)])))#最近24个月严重逾期贷款逾期月数求和

        #现行最近24个月严重逾期贷款
        dict_out['loan_serious_overdue_account_count_24m_now']=len(loan_second[(loan_second['month_Desc_report']<24) & (loan_second['overdue_class']==3) & (loan_second['is_now']==1)])#现行最近24个月严重逾期贷款账户计数
        dict_out['loan_serious_overdue_month_sum_24m_now']=sum(list(map(sum,loan_second['overdue_month'][(loan_second['month_Desc_report']<24) & (loan_second['overdue_class']==3) & (loan_second['is_now']==1)])))#现行最近24个月严重逾期贷款逾期月数求和
        dict_out['loan_serious_overdue_planRepayAmount_sum_24m_now']=loan_second['planRepayAmount'][(loan_second['month_Desc_report']<24) & (loan_second['overdue_class']==3) & (loan_second['is_now']==1)].sum()#现行最近24个月严重逾期贷款本月应还款求和
        dict_out['loan_serious_overdue_RepayedAmount_sum_24m_now']=loan_second['RepayedAmount'][(loan_second['month_Desc_report']<24) & (loan_second['overdue_class']==3) & (loan_second['is_now']==1)].sum()#现行最近24个月严重逾期贷款本月实还款求和
        dict_out['loan_serious_overdue_planRepayAmount_max_24m_now']=loan_second['planRepayAmount'][(loan_second['month_Desc_report']<24) & (loan_second['overdue_class']==3) & (loan_second['is_now']==1)].max()#现行最近24个月严重逾期贷款本月应还款最大
        dict_out['loan_serious_overdue_RepayedAmount_max_24m_now']=loan_second['RepayedAmount'][(loan_second['month_Desc_report']<24) & (loan_second['overdue_class']==3) & (loan_second['is_now']==1)].max()#现行最近24个月严重逾期贷款本月实还款最大

        #最近24个月严重逾期有担保贷款
        dict_out['loan_is_vouch_serious_overdue_account_count_24m']=len(loan_second[(loan_second['month_Desc_report']<24) & (loan_second['overdue_class']==3) & (loan_second['is_vouch']==1)])#最近24个月严重逾期有担保贷款账户计数
        dict_out['loan_is_vouch_serious_overdue_month_sum_24m']=sum(list(map(sum,loan_second['overdue_month'][(loan_second['month_Desc_report']<24) & (loan_second['overdue_class']==3) & (loan_second['is_vouch']==1)])))#最近24个月严重逾期有担保贷款逾期月数求和

        #现行最近24个月严重逾期有担保贷款
        dict_out['loan_is_vouch_serious_overdue_account_count_24m_now']=len(loan_second[(loan_second['month_Desc_report']<24) & (loan_second['overdue_class']==3) & (loan_second['is_vouch']==1) & (loan_second['is_now']==1)])#现行最近24个月严重逾期有担保贷款账户计数
        dict_out['loan_is_vouch_serious_overdue_month_sum_24m_now']=sum(list(map(sum,loan_second['overdue_month'][(loan_second['month_Desc_report']<24) & (loan_second['overdue_class']==3) & (loan_second['is_vouch']==1) & (loan_second['is_now']==1)])))#现行最近24个月严重逾期有担保贷款逾期月数求和
        dict_out['loan_is_vouch_serious_overdue_planRepayAmount_sum_24m_now']=loan_second['planRepayAmount'][(loan_second['month_Desc_report']<24) & (loan_second['overdue_class']==3) & (loan_second['is_vouch']==1) & (loan_second['is_now']==1)].sum()#现行最近24个月严重逾期有担保贷款本月应还款求和
        dict_out['loan_is_vouch_serious_overdue_RepayedAmount_sum_24m_now']=loan_second['RepayedAmount'][(loan_second['month_Desc_report']<24) & (loan_second['overdue_class']==3) & (loan_second['is_vouch']==1) & (loan_second['is_now']==1)].sum()#现行最近24个月严重逾期有担保贷款本月实还款求和
        dict_out['loan_is_vouch_serious_overdue_planRepayAmount_max_24m_now']=loan_second['planRepayAmount'][(loan_second['month_Desc_report']<24) & (loan_second['overdue_class']==3) & (loan_second['is_vouch']==1) & (loan_second['is_now']==1)].max()#现行最近24个月严重逾期有担保贷款本月应还款最大
        dict_out['loan_is_vouch_serious_overdue_RepayedAmount_max_24m_now']=loan_second['RepayedAmount'][(loan_second['month_Desc_report']<24) & (loan_second['overdue_class']==3) & (loan_second['is_vouch']==1) & (loan_second['is_now']==1)].max()#现行最近24个月严重逾期有担保贷款本月实还款最大

        #违约贷款
        dict_out['loan_is_break_account_count']=len(loan_second[(loan_second['is_break']==1)])#违约贷款账户计数
        dict_out['loan_is_break_amount_sum']=loan_second['loanAmount'][(loan_second['is_break']==1)].sum()#违约贷款本金求和
        dict_out['loan_is_break_amount_max']=loan_second['loanAmount'][(loan_second['is_break']==1)].max()#违约贷款本金最大

        #违约有担保贷款
        dict_out['loan_is_vouch_is_break_account_count']=len(loan_second[(loan_second['is_break']==1) & (loan_second['is_vouch']==1)])#违约有担保贷款账户计数
        dict_out['loan_is_vouch_is_break_amount_sum']=loan_second['loanAmount'][(loan_second['is_break']==1) & (loan_second['is_vouch']==1)].sum()#违约有担保贷款本金求和
        dict_out['loan_is_vouch_is_break_amount_max']=loan_second['loanAmount'][(loan_second['is_break']==1) & (loan_second['is_vouch']==1)].max()#违约有担保贷款本金最大

        #违约房贷
        dict_out['house_loan_is_break_account_count']=len(loan_second[(loan_second['is_break']==1) & (loan_second['businessType']=='个人住房商业贷款')])#违约房贷账户计数
        dict_out['house_loan_is_break_amount_sum']=loan_second['loanAmount'][(loan_second['is_break']==1) & (loan_second['businessType']=='个人住房商业贷款')].sum()#违约房贷本金求和
        dict_out['house_loan_is_break_amount_max']=loan_second['loanAmount'][(loan_second['is_break']==1) & (loan_second['businessType']=='个人住房商业贷款')].max()#违约房贷本金最大

        #违约个人汽车贷款
        dict_out['car_loan_is_break_account_count']=len(loan_second[(loan_second['is_break']==1) & (loan_second['businessType']=='个人汽车消费贷款')])#违约个人汽车贷款账户计数
        dict_out['car_loan_is_break_amount_sum']=loan_second['loanAmount'][(loan_second['is_break']==1) & (loan_second['businessType']=='个人汽车消费贷款')].sum()#违约个人汽车贷款本金求和
        dict_out['car_loan_is_break_amount_max']=loan_second['loanAmount'][(loan_second['is_break']==1) & (loan_second['businessType']=='个人汽车消费贷款')].max()#违约个人汽车贷款本金最大

        #违约个人助学贷款
        dict_out['stu_loan_is_break_account_count']=len(loan_second[(loan_second['is_break']==1) & (loan_second['businessType']=='个人助学贷款')])#违约个人助学贷款账户计数
        dict_out['stu_loan_is_break_amount_sum']=loan_second['loanAmount'][(loan_second['is_break']==1) & (loan_second['businessType']=='个人助学贷款')].sum()#违约个人助学贷款本金求和
        dict_out['stu_loan_is_break_amount_max']=loan_second['loanAmount'][(loan_second['is_break']==1) & (loan_second['businessType']=='个人助学贷款')].max()#违约个人助学贷款本金最大

        #最近3个月严重逾期个人经营性贷款
        dict_out['business_loan_serious_overdue_account_count_03m']=len(loan_second[(loan_second['businessType']=='个人经营性贷款') & (loan_second['overdue_class']==3) & (loan_second['month_Desc_report']<3)])#最近3个月严重逾期个人经营性贷款账户计数
        dict_out['business_loan_serious_overdue_month_sum_03m']=sum(list(map(sum,loan_second['overdue_month'][(loan_second['businessType']=='个人经营性贷款') & (loan_second['overdue_class']==3) & (loan_second['month_Desc_report']<3)])))#最近3个月严重逾期个人经营性贷款逾期月数求和

        #现行最近3个月严重逾期个人经营性贷款
        dict_out['business_loan_serious_overdue_account_count_03m_now']=len(loan_second[(loan_second['businessType']=='个人经营性贷款') & (loan_second['overdue_class']==3) & (loan_second['month_Desc_report']<3) & (loan_second['is_now']==1)])#现行最近3个月严重逾期个人经营性贷款账户计数
        dict_out['business_loan_serious_overdue_month_sum_03m_now']=sum(list(map(sum,loan_second['overdue_month'][(loan_second['businessType']=='个人经营性贷款') & (loan_second['overdue_class']==3) & (loan_second['month_Desc_report']<3) & (loan_second['is_now']==1)])))#现行最近3个月严重逾期个人经营性贷款逾期月数求和
        dict_out['business_loan_serious_overdue_planRepayAmount_sum_03m_now']=loan_second['planRepayAmount'][(loan_second['businessType']=='个人经营性贷款') & (loan_second['overdue_class']==3) & (loan_second['month_Desc_report']<3) & (loan_second['is_now']==1)].sum()#现行最近3个月严重逾期个人经营性贷款本月应还款求和
        dict_out['business_loan_serious_overdue_RepayedAmount_sum_03m_now']=loan_second['RepayedAmount'][(loan_second['businessType']=='个人经营性贷款') & (loan_second['overdue_class']==3) & (loan_second['month_Desc_report']<3) & (loan_second['is_now']==1)].sum()#现行最近3个月严重逾期个人经营性贷款本月实还款求和
        dict_out['business_loan_serious_overdue_planRepayAmount_max_03m_now']=loan_second['planRepayAmount'][(loan_second['businessType']=='个人经营性贷款') & (loan_second['overdue_class']==3) & (loan_second['month_Desc_report']<3) & (loan_second['is_now']==1)].max()#现行最近3个月严重逾期个人经营性贷款本月应还款最大
        dict_out['business_loan_serious_overdue_RepayedAmount_max_03m_now']=loan_second['RepayedAmount'][(loan_second['businessType']=='个人经营性贷款') & (loan_second['overdue_class']==3) & (loan_second['month_Desc_report']<3) & (loan_second['is_now']==1)].max()#现行最近3个月严重逾期个人经营性贷款本月实还款最大

        #最近3个月严重逾期有担保个人经营性贷款
        dict_out['business_loan_is_vouch_serious_overdue_account_count_03m']=len(loan_second[(loan_second['businessType']=='个人经营性贷款') & (loan_second['overdue_class']==3) & (loan_second['month_Desc_report']<3) & (loan_second['is_vouch']==1)])#最近3个月严重逾期有担保个人经营性贷款账户计数
        dict_out['business_loan_is_vouch_serious_overdue_month_sum_03m']=sum(list(map(sum,loan_second['overdue_month'][(loan_second['businessType']=='个人经营性贷款') & (loan_second['overdue_class']==3) & (loan_second['month_Desc_report']<3) & (loan_second['is_vouch']==1)])))#最近3个月严重逾期有担保个人经营性贷款逾期月数求和

        #现行最近3个月严重逾期有担保个人经营性贷款
        dict_out['business_loan_is_vouch_serious_overdue_account_count_03m_now']=len(loan_second[(loan_second['businessType']=='个人经营性贷款') & (loan_second['overdue_class']==3) & (loan_second['month_Desc_report']<3) & (loan_second['is_vouch']==1) & (loan_second['is_now']==1)])#现行最近3个月严重逾期有担保个人经营性贷款账户计数
        dict_out['business_loan_is_vouch_serious_overdue_month_sum_03m_now']=sum(list(map(sum,loan_second['overdue_month'][(loan_second['businessType']=='个人经营性贷款') & (loan_second['overdue_class']==3) & (loan_second['month_Desc_report']<3) & (loan_second['is_vouch']==1) & (loan_second['is_now']==1)])))#现行最近3个月严重逾期有担保个人经营性贷款逾期月数求和
        dict_out['business_loan_is_vouch_serious_overdue_planRepayAmount_sum_03m_now']=loan_second['planRepayAmount'][(loan_second['businessType']=='个人经营性贷款') & (loan_second['overdue_class']==3) & (loan_second['month_Desc_report']<3) & (loan_second['is_vouch']==1) & (loan_second['is_now']==1)].sum()#现行最近3个月严重逾期有担保个人经营性贷款本月应还款求和
        dict_out['business_loan_is_vouch_serious_overdue_RepayedAmount_sum_03m_now']=loan_second['RepayedAmount'][(loan_second['businessType']=='个人经营性贷款') & (loan_second['overdue_class']==3) & (loan_second['month_Desc_report']<3) & (loan_second['is_vouch']==1) & (loan_second['is_now']==1)].sum()#现行最近3个月严重逾期有担保个人经营性贷款本月实还款求和
        dict_out['business_loan_is_vouch_serious_overdue_planRepayAmount_max_03m_now']=loan_second['planRepayAmount'][(loan_second['businessType']=='个人经营性贷款') & (loan_second['overdue_class']==3) & (loan_second['month_Desc_report']<3) & (loan_second['is_vouch']==1) & (loan_second['is_now']==1)].max()#现行最近3个月严重逾期有担保个人经营性贷款本月应还款最大
        dict_out['business_loan_is_vouch_serious_overdue_RepayedAmount_max_03m_now']=loan_second['RepayedAmount'][(loan_second['businessType']=='个人经营性贷款') & (loan_second['overdue_class']==3) & (loan_second['month_Desc_report']<3) & (loan_second['is_vouch']==1) & (loan_second['is_now']==1)].max()#现行最近3个月严重逾期有担保个人经营性贷款本月实还款最大

        #最近6个月严重逾期个人经营性贷款
        dict_out['business_loan_serious_overdue_account_count_06m']=len(loan_second[(loan_second['businessType']=='个人经营性贷款') & (loan_second['overdue_class']==3) & (loan_second['month_Desc_report']<6)])#最近6个月严重逾期个人经营性贷款账户计数
        dict_out['business_loan_serious_overdue_month_sum_06m']=sum(list(map(sum,loan_second['overdue_month'][(loan_second['businessType']=='个人经营性贷款') & (loan_second['overdue_class']==3) & (loan_second['month_Desc_report']<6)])))#最近6个月严重逾期个人经营性贷款逾期月数求和

        #现行最近6个月严重逾期个人经营性贷款
        dict_out['business_loan_serious_overdue_account_count_06m_now']=len(loan_second[(loan_second['businessType']=='个人经营性贷款') & (loan_second['overdue_class']==3) & (loan_second['month_Desc_report']<6) & (loan_second['is_now']==1)])#现行最近6个月严重逾期个人经营性贷款账户计数
        dict_out['business_loan_serious_overdue_month_sum_06m_now']=sum(list(map(sum,loan_second['overdue_month'][(loan_second['businessType']=='个人经营性贷款') & (loan_second['overdue_class']==3) & (loan_second['month_Desc_report']<6) & (loan_second['is_now']==1)])))#现行最近6个月严重逾期个人经营性贷款逾期月数求和
        dict_out['business_loan_serious_overdue_planRepayAmount_sum_06m_now']=loan_second['planRepayAmount'][(loan_second['businessType']=='个人经营性贷款') & (loan_second['overdue_class']==3) & (loan_second['month_Desc_report']<6) & (loan_second['is_now']==1)].sum()#现行最近6个月严重逾期个人经营性贷款本月应还款求和
        dict_out['business_loan_serious_overdue_RepayedAmount_sum_06m_now']=loan_second['RepayedAmount'][(loan_second['businessType']=='个人经营性贷款') & (loan_second['overdue_class']==3) & (loan_second['month_Desc_report']<6) & (loan_second['is_now']==1)].sum()#现行最近6个月严重逾期个人经营性贷款本月实还款求和
        dict_out['business_loan_serious_overdue_planRepayAmount_max_06m_now']=loan_second['planRepayAmount'][(loan_second['businessType']=='个人经营性贷款') & (loan_second['overdue_class']==3) & (loan_second['month_Desc_report']<6) & (loan_second['is_now']==1)].max()#现行最近6个月严重逾期个人经营性贷款本月应还款最大
        dict_out['business_loan_serious_overdue_RepayedAmount_max_06m_now']=loan_second['RepayedAmount'][(loan_second['businessType']=='个人经营性贷款') & (loan_second['overdue_class']==3) & (loan_second['month_Desc_report']<6) & (loan_second['is_now']==1)].max()#现行最近6个月严重逾期个人经营性贷款本月实还款最大

        #最近6月严重逾期有担保个人经营性贷款
        dict_out['business_loan_is_vouch_serious_overdue_account_count_06m']=len(loan_second[(loan_second['businessType']=='个人经营性贷款') & (loan_second['overdue_class']==3) & (loan_second['month_Desc_report']<6) & (loan_second['is_vouch']==1)])#最近6个月严重逾期有担保个人经营性贷款账户计数
        dict_out['business_loan_is_vouch_serious_overdue_month_sum_06m']=sum(list(map(sum,loan_second['overdue_month'][(loan_second['businessType']=='个人经营性贷款') & (loan_second['overdue_class']==3) & (loan_second['month_Desc_report']<6) & (loan_second['is_vouch']==1)])))#最近6个月严重逾期有担保个人经营性贷款逾期月数求和

        #现行最近6个月严重逾期有担保个人经营性贷款
        dict_out['business_loan_is_vouch_serious_overdue_account_count_06m_now']=len(loan_second[(loan_second['businessType']=='个人经营性贷款') & (loan_second['overdue_class']==3) & (loan_second['month_Desc_report']<6) & (loan_second['is_vouch']==1) & (loan_second['is_now']==1)])#现行最近6个月严重逾期有担保个人经营性贷款账户计数
        dict_out['business_loan_is_vouch_serious_overdue_month_sum_06m_now']=sum(list(map(sum,loan_second['overdue_month'][(loan_second['businessType']=='个人经营性贷款') & (loan_second['overdue_class']==3) & (loan_second['month_Desc_report']<6) & (loan_second['is_vouch']==1) & (loan_second['is_now']==1)])))#现行最近6个月严重逾期有担保个人经营性贷款逾期月数求和
        dict_out['business_loan_is_vouch_serious_overdue_planRepayAmount_sum_06m_now']=loan_second['planRepayAmount'][(loan_second['businessType']=='个人经营性贷款') & (loan_second['overdue_class']==3) & (loan_second['month_Desc_report']<6) & (loan_second['is_vouch']==1) & (loan_second['is_now']==1)].sum()#现行最近6个月严重逾期有担保个人经营性贷款本月应还款求和
        dict_out['business_loan_is_vouch_serious_overdue_RepayedAmount_sum_06m_now']=loan_second['RepayedAmount'][(loan_second['businessType']=='个人经营性贷款') & (loan_second['overdue_class']==3) & (loan_second['month_Desc_report']<6) & (loan_second['is_vouch']==1) & (loan_second['is_now']==1)].sum()#现行最近6个月严重逾期有担保个人经营性贷款本月实还款求和
        dict_out['business_loan_is_vouch_serious_overdue_planRepayAmount_max_06m_now']=loan_second['planRepayAmount'][(loan_second['businessType']=='个人经营性贷款') & (loan_second['overdue_class']==3) & (loan_second['month_Desc_report']<6) & (loan_second['is_vouch']==1) & (loan_second['is_now']==1)].max()#现行最近6个月严重逾期有担保个人经营性贷款本月应还款最大
        dict_out['business_loan_is_vouch_serious_overdue_RepayedAmount_max_06m_now']=loan_second['RepayedAmount'][(loan_second['businessType']=='个人经营性贷款') & (loan_second['overdue_class']==3) & (loan_second['month_Desc_report']<6) & (loan_second['is_vouch']==1) & (loan_second['is_now']==1)].max()#现行最近6个月严重逾期有担保个人经营性贷款本月实还款最大

        #最近12个月严重逾期个人经营性贷款
        dict_out['business_loan_serious_overdue_account_count_12m']=len(loan_second[(loan_second['businessType']=='个人经营性贷款') & (loan_second['overdue_class']==3) & (loan_second['month_Desc_report']<12)])#最近12个月严重逾期个人经营性贷款账户计数
        dict_out['business_loan_serious_overdue_month_sum_12m']=sum(list(map(sum,loan_second['overdue_month'][(loan_second['businessType']=='个人经营性贷款') & (loan_second['overdue_class']==3) & (loan_second['month_Desc_report']<12)])))#最近12个月严重逾期个人经营性贷款逾期月数求和

        #现行最近12个月严重逾期个人经营性贷款
        dict_out['business_loan_serious_overdue_account_count_12m_now']=len(loan_second[(loan_second['businessType']=='个人经营性贷款') & (loan_second['overdue_class']==3) & (loan_second['month_Desc_report']<12) & (loan_second['is_now']==1)])#现行最近12个月严重逾期个人经营性贷款账户计数
        dict_out['business_loan_serious_overdue_month_sum_12m_now']=sum(list(map(sum,loan_second['overdue_month'][(loan_second['businessType']=='个人经营性贷款') & (loan_second['overdue_class']==3) & (loan_second['month_Desc_report']<12) & (loan_second['is_now']==1)])))#现行最近12个月严重逾期个人经营性贷款逾期月数求和
        dict_out['business_loan_serious_overdue_planRepayAmount_sum_12m_now']=loan_second['planRepayAmount'][(loan_second['businessType']=='个人经营性贷款') & (loan_second['overdue_class']==3) & (loan_second['month_Desc_report']<12) & (loan_second['is_now']==1)].sum()#现行最近12个月严重逾期个人经营性贷款本月应还款求和
        dict_out['business_loan_serious_overdue_RepayedAmount_sum_12m_now']=loan_second['RepayedAmount'][(loan_second['businessType']=='个人经营性贷款') & (loan_second['overdue_class']==3) & (loan_second['month_Desc_report']<12) & (loan_second['is_now']==1)].sum()#现行最近12个月严重逾期个人经营性贷款本月实还款求和
        dict_out['business_loan_serious_overdue_planRepayAmount_max_12m_now']=loan_second['planRepayAmount'][(loan_second['businessType']=='个人经营性贷款') & (loan_second['overdue_class']==3) & (loan_second['month_Desc_report']<12) & (loan_second['is_now']==1)].max()#现行最近12个月严重逾期个人经营性贷款本月应还款最大
        dict_out['business_loan_serious_overdue_RepayedAmount_max_12m_now']=loan_second['RepayedAmount'][(loan_second['businessType']=='个人经营性贷款') & (loan_second['overdue_class']==3) & (loan_second['month_Desc_report']<12) & (loan_second['is_now']==1)].max()#现行最近12个月严重逾期个人经营性贷款本月实还款最大

        #最近12个月严重逾期有担保个人经营性贷款
        dict_out['business_loan_is_vouch_serious_overdue_account_count_12m']=len(loan_second[(loan_second['businessType']=='个人经营性贷款') & (loan_second['overdue_class']==3) & (loan_second['month_Desc_report']<12) & (loan_second['is_vouch']==1)])#最近12个月严重逾期有担保个人经营性贷款账户计数
        dict_out['business_loan_is_vouch_serious_overdue_month_sum_12m']=sum(list(map(sum,loan_second['overdue_month'][(loan_second['businessType']=='个人经营性贷款') & (loan_second['overdue_class']==3) & (loan_second['month_Desc_report']<12) & (loan_second['is_vouch']==1)])))#最近12个月严重逾期有担保个人经营性贷款逾期月数求和

        #现行最近12个月严重逾期有担保个人经营性贷款
        dict_out['business_loan_is_vouch_serious_overdue_account_count_12m_now']=len(loan_second[(loan_second['businessType']=='个人经营性贷款') & (loan_second['overdue_class']==3) & (loan_second['month_Desc_report']<12) & (loan_second['is_vouch']==1) & (loan_second['is_now']==1)])#现行最近12个月严重逾期有担保个人经营性贷款账户计数
        dict_out['business_loan_is_vouch_serious_overdue_month_sum_12m_now']=sum(list(map(sum,loan_second['overdue_month'][(loan_second['businessType']=='个人经营性贷款') & (loan_second['overdue_class']==3) & (loan_second['month_Desc_report']<12) & (loan_second['is_vouch']==1) & (loan_second['is_now']==1)])))#现行最近12个月严重逾期有担保个人经营性贷款逾期月数求和
        dict_out['business_loan_is_vouch_serious_overdue_planRepayAmount_sum_12m_now']=loan_second['planRepayAmount'][(loan_second['businessType']=='个人经营性贷款') & (loan_second['overdue_class']==3) & (loan_second['month_Desc_report']<12) & (loan_second['is_vouch']==1) & (loan_second['is_now']==1)].sum()#现行最近12个月严重逾期有担保个人经营性贷款本月应还款求和
        dict_out['business_loan_is_vouch_serious_overdue_RepayedAmount_sum_12m_now']=loan_second['RepayedAmount'][(loan_second['businessType']=='个人经营性贷款') & (loan_second['overdue_class']==3) & (loan_second['month_Desc_report']<12) & (loan_second['is_vouch']==1) & (loan_second['is_now']==1)].sum()#现行最近12个月严重逾期有担保个人经营性贷款本月实还款求和
        dict_out['business_loan_is_vouch_serious_overdue_planRepayAmount_max_12m_now']=loan_second['planRepayAmount'][(loan_second['businessType']=='个人经营性贷款') & (loan_second['overdue_class']==3) & (loan_second['month_Desc_report']<12) & (loan_second['is_vouch']==1) & (loan_second['is_now']==1)].max()#现行最近12个月严重逾期有担保个人经营性贷款本月应还款最大
        dict_out['business_loan_is_vouch_serious_overdue_RepayedAmount_max_12m_now']=loan_second['RepayedAmount'][(loan_second['businessType']=='个人经营性贷款') & (loan_second['overdue_class']==3) & (loan_second['month_Desc_report']<12) & (loan_second['is_vouch']==1) & (loan_second['is_now']==1)].max()#现行最近12个月严重逾期有担保个人经营性贷款本月实还款最大

        #最近24个月严重逾期个人经营性贷款
        dict_out['business_loan_serious_overdue_account_count_24m']=len(loan_second[(loan_second['businessType']=='个人经营性贷款') & (loan_second['overdue_class']==3) & (loan_second['month_Desc_report']<24)])#最近24个月严重逾期个人经营性贷款账户计数
        dict_out['business_loan_serious_overdue_month_sum_24m']=sum(list(map(sum,loan_second['overdue_month'][(loan_second['businessType']=='个人经营性贷款') & (loan_second['overdue_class']==3) & (loan_second['month_Desc_report']<24)])))#最近24个月严重逾期个人经营性贷款逾期月数求和

        #现行最近24个月严重逾期个人经营性贷款
        dict_out['business_loan_serious_overdue_account_count_24m_now']=len(loan_second[(loan_second['businessType']=='个人经营性贷款') & (loan_second['overdue_class']==3) & (loan_second['month_Desc_report']<24) & (loan_second['is_now']==1)])#现行最近24个月严重逾期个人经营性贷款账户计数
        dict_out['business_loan_serious_overdue_month_sum_24m_now']=sum(list(map(sum,loan_second['overdue_month'][(loan_second['businessType']=='个人经营性贷款') & (loan_second['overdue_class']==3) & (loan_second['month_Desc_report']<24) & (loan_second['is_now']==1)])))#现行最近24个月严重逾期个人经营性贷款逾期月数求和
        dict_out['business_loan_serious_overdue_planRepayAmount_sum_24m_now']=loan_second['planRepayAmount'][(loan_second['businessType']=='个人经营性贷款') & (loan_second['overdue_class']==3) & (loan_second['month_Desc_report']<24) & (loan_second['is_now']==1)].sum()#现行最近24个月严重逾期个人经营性贷款本月应还款求和
        dict_out['business_loan_serious_overdue_RepayedAmount_sum_24m_now']=loan_second['RepayedAmount'][(loan_second['businessType']=='个人经营性贷款') & (loan_second['overdue_class']==3) & (loan_second['month_Desc_report']<24) & (loan_second['is_now']==1)].sum()#现行最近24个月严重逾期个人经营性贷款本月实还款求和
        dict_out['business_loan_serious_overdue_planRepayAmount_max_24m_now']=loan_second['planRepayAmount'][(loan_second['businessType']=='个人经营性贷款') & (loan_second['overdue_class']==3) & (loan_second['month_Desc_report']<24) & (loan_second['is_now']==1)].max()#现行最近24个月严重逾期个人经营性贷款本月应还款最大
        dict_out['business_loan_serious_overdue_RepayedAmount_max_24m_now']=loan_second['RepayedAmount'][(loan_second['businessType']=='个人经营性贷款') & (loan_second['overdue_class']==3) & (loan_second['month_Desc_report']<24) & (loan_second['is_now']==1)].max()#现行最近24个月严重逾期个人经营性贷款本月实还款最大

        #最近24个月严重逾期有担保个人经营性贷款
        dict_out['business_loan_is_vouch_serious_overdue_account_count_24m']=len(loan_second[(loan_second['businessType']=='个人经营性贷款') & (loan_second['overdue_class']==3) & (loan_second['month_Desc_report']<24) & (loan_second['is_vouch']==1)])#最近24个月严重逾期有担保个人经营性贷款账户计数
        dict_out['business_loan_is_vouch_serious_overdue_month_sum_24m']=sum(list(map(sum,loan_second['overdue_month'][(loan_second['businessType']=='个人经营性贷款') & (loan_second['overdue_class']==3) & (loan_second['month_Desc_report']<24) & (loan_second['is_vouch']==1)])))#最近24个月严重逾期有担保个人经营性贷款逾期月数求和

        #现行最近24个月严重逾期有担保个人经营性贷款
        dict_out['business_loan_is_vouch_serious_overdue_account_count_24m_now']=len(loan_second[(loan_second['businessType']=='个人经营性贷款') & (loan_second['overdue_class']==3) & (loan_second['month_Desc_report']<24) & (loan_second['is_vouch']==1) & (loan_second['is_now']==1)])#现行最近24个月严重逾期有担保个人经营性贷款账户计数
        dict_out['business_loan_is_vouch_serious_overdue_month_sum_24m_now']=sum(list(map(sum,loan_second['overdue_month'][(loan_second['businessType']=='个人经营性贷款') & (loan_second['overdue_class']==3) & (loan_second['month_Desc_report']<24) & (loan_second['is_vouch']==1) & (loan_second['is_now']==1)])))#现行最近24个月严重逾期有担保个人经营性贷款逾期月数求和
        dict_out['business_loan_is_vouch_serious_overdue_planRepayAmount_sum_24m_now']=loan_second['planRepayAmount'][(loan_second['businessType']=='个人经营性贷款') & (loan_second['overdue_class']==3) & (loan_second['month_Desc_report']<24) & (loan_second['is_vouch']==1) & (loan_second['is_now']==1)].sum()#现行最近24个月严重逾期有担保个人经营性贷款本月应还款求和
        dict_out['business_loan_is_vouch_serious_overdue_RepayedAmount_sum_24m_now']=loan_second['RepayedAmount'][(loan_second['businessType']=='个人经营性贷款') & (loan_second['overdue_class']==3) & (loan_second['month_Desc_report']<24) & (loan_second['is_vouch']==1) & (loan_second['is_now']==1)].sum()#现行最近24个月严重逾期有担保个人经营性贷款本月实还款求和
        dict_out['business_loan_is_vouch_serious_overdue_planRepayAmount_max_24m_now']=loan_second['planRepayAmount'][(loan_second['businessType']=='个人经营性贷款') & (loan_second['overdue_class']==3) & (loan_second['month_Desc_report']<24) & (loan_second['is_vouch']==1) & (loan_second['is_now']==1)].max()#现行最近24月严重逾期有担保个人经营性贷款本月应还款最大
        dict_out['business_loan_is_vouch_serious_overdue_RepayedAmount_max_24m_now']=loan_second['RepayedAmount'][(loan_second['businessType']=='个人经营性贷款') & (loan_second['overdue_class']==3) & (loan_second['month_Desc_report']<24) & (loan_second['is_vouch']==1) & (loan_second['is_now']==1)].max()#现行最近24个月严重逾期有担保个人经营性贷款本月实还款最大

        #违约个人经营性贷款
        dict_out['business_loan_is_break_account_count']=len(loan_second[(loan_second['businessType']=='个人经营性贷款') & (loan_second['is_break']==1)])#违约个人经营性贷款账户计数
        dict_out['business_loan_is_break_amount_sum']=loan_second['loanAmount'][(loan_second['businessType']=='个人经营性贷款') & (loan_second['is_break']==1)].sum()#违约个人经营性贷款本金求和
        dict_out['business_loan_is_break_amount_max']=loan_second['loanAmount'][(loan_second['businessType']=='个人经营性贷款') & (loan_second['is_break']==1)].max()#违约个人经营性贷款本金最大

        #违约有担保个人经营性贷款
        dict_out['business_loan_is_vouch_is_break_account_count']=len(loan_second[(loan_second['businessType']=='个人经营性贷款') & (loan_second['is_break']==1) & (loan_second['is_vouch']==1)])#违约有担保个人经营性贷款账户计数
        dict_out['business_loan_is_vouch_is_break_amount_sum']=loan_second['loanAmount'][(loan_second['businessType']=='个人经营性贷款') & (loan_second['is_break']==1) & (loan_second['is_vouch']==1)].sum()#违约有担保个人经营性贷款本金求和
        dict_out['business_loan_is_vouch_is_break_amount_max']=loan_second['loanAmount'][(loan_second['businessType']=='个人经营性贷款') & (loan_second['is_break']==1) & (loan_second['is_vouch']==1)].max()#违约有担保个人经营性贷款本金最大

        #最近3个月严重逾期个人消费贷款
        dict_out['consume_loan_serious_overdue_account_count_03m']=len(loan_second[(loan_second['businessType']=='其他个人消费贷款') & (loan_second['overdue_class']==3) & (loan_second['month_Desc_report']<3)])#最近3个月严重逾期个人消费贷款账户计数
        dict_out['consume_loan_serious_overdue_month_sum_03m']=sum(list(map(sum,loan_second['overdue_month'][(loan_second['businessType']=='其他个人消费贷款') & (loan_second['overdue_class']==3) & (loan_second['month_Desc_report']<3)])))#最近3个月严重逾期个人消费贷款逾期月数求和

        #现行最近3个月严重逾期个人消费贷款
        dict_out['consume_loan_serious_overdue_account_count_03m_now']=len(loan_second[(loan_second['businessType']=='其他个人消费贷款') & (loan_second['overdue_class']==3) & (loan_second['month_Desc_report']<3) & (loan_second['is_now']==1)])#现行最近3个月严重逾期个人消费贷款账户计数
        dict_out['consume_loan_serious_overdue_month_sum_03m_now']=sum(list(map(sum,loan_second['overdue_month'][(loan_second['businessType']=='其他个人消费贷款') & (loan_second['overdue_class']==3) & (loan_second['month_Desc_report']<3) & (loan_second['is_now']==1)])))#现行最近3个月严重逾期个人消费贷款逾期月数求和
        dict_out['consume_loan_serious_overdue_planRepayAmount_sum_03m_now']=loan_second['planRepayAmount'][(loan_second['businessType']=='其他个人消费贷款') & (loan_second['overdue_class']==3) & (loan_second['month_Desc_report']<3) & (loan_second['is_now']==1)].sum()#现行最近3个月严重逾期个人消费贷款本月应还款求和
        dict_out['consume_loan_serious_overdue_RepayedAmount_sum_03m_now']=loan_second['RepayedAmount'][(loan_second['businessType']=='其他个人消费贷款') & (loan_second['overdue_class']==3) & (loan_second['month_Desc_report']<3) & (loan_second['is_now']==1)].sum()#现行最近3个月严重逾期个人消费贷款本月实还款求和
        dict_out['consume_loan_serious_overdue_planRepayAmount_max_03m_now']=loan_second['planRepayAmount'][(loan_second['businessType']=='其他个人消费贷款') & (loan_second['overdue_class']==3) & (loan_second['month_Desc_report']<3) & (loan_second['is_now']==1)].max()#现行最近3个月严重逾期个人消费贷款本月应还款最大
        dict_out['consume_loan_serious_overdue_RepayedAmount_max_03m_now']=loan_second['RepayedAmount'][(loan_second['businessType']=='其他个人消费贷款') & (loan_second['overdue_class']==3) & (loan_second['month_Desc_report']<3) & (loan_second['is_now']==1)].max()#现行最近3个月严重逾期个人消费贷款本月实还款最大

        #最近3个月严重逾期有担保个人消费贷款
        dict_out['consume_loan_is_vouch_serious_overdue_account_count_03m']=len(loan_second[(loan_second['businessType']=='其他个人消费贷款') & (loan_second['overdue_class']==3) & (loan_second['month_Desc_report']<3) & (loan_second['is_vouch']==1)])#最近3个月严重逾期有担保个人消费贷款账户计数
        dict_out['consume_loan_is_vouch_serious_overdue_month_sum_03m']=sum(list(map(sum,loan_second['overdue_month'][(loan_second['businessType']=='其他个人消费贷款') & (loan_second['overdue_class']==3) & (loan_second['month_Desc_report']<3) & (loan_second['is_vouch']==1)])))#最近3个月严重逾期有担保个人消费贷款逾期月数求和

        #现行最近3个月严重逾期有担保个人消费贷款
        dict_out['consume_loan_is_vouch_serious_overdue_account_count_03m_now']=len(loan_second[(loan_second['businessType']=='其他个人消费贷款') & (loan_second['overdue_class']==3) & (loan_second['month_Desc_report']<3) & (loan_second['is_vouch']==1) & (loan_second['is_now']==1)])#现行最近3个月严重逾期有担保个人消费贷款账户计数
        dict_out['consume_loan_is_vouch_serious_overdue_month_sum_03m_now']=sum(list(map(sum,loan_second['overdue_month'][(loan_second['businessType']=='其他个人消费贷款') & (loan_second['overdue_class']==3) & (loan_second['month_Desc_report']<3) & (loan_second['is_vouch']==1) & (loan_second['is_now']==1)])))#现行最近3个月严重逾期有担保个人消费贷款逾期月数求和
        dict_out['consume_loan_is_vouch_serious_overdue_planRepayAmount_sum_03m_now']=loan_second['planRepayAmount'][(loan_second['businessType']=='其他个人消费贷款') & (loan_second['overdue_class']==3) & (loan_second['month_Desc_report']<3) & (loan_second['is_vouch']==1) & (loan_second['is_now']==1)].sum()#现行最近3个月严重逾期有担保个人消费贷款本月应还款求和
        dict_out['consume_loan_is_vouch_serious_overdue_RepayedAmount_sum_03m_now']=loan_second['RepayedAmount'][(loan_second['businessType']=='其他个人消费贷款') & (loan_second['overdue_class']==3) & (loan_second['month_Desc_report']<3) & (loan_second['is_vouch']==1) & (loan_second['is_now']==1)].sum()#现行最近3个月严重逾期有担保个人消费贷款本月实还款求和
        dict_out['consume_loan_is_vouch_serious_overdue_planRepayAmount_max_03m_now']=loan_second['planRepayAmount'][(loan_second['businessType']=='其他个人消费贷款') & (loan_second['overdue_class']==3) & (loan_second['month_Desc_report']<3) & (loan_second['is_vouch']==1) & (loan_second['is_now']==1)].max()#现行最近3个月严重逾期有担保个人消费贷款本月应还款最大
        dict_out['consume_loan_is_vouch_serious_overdue_RepayedAmount_max_03m_now']=loan_second['RepayedAmount'][(loan_second['businessType']=='其他个人消费贷款') & (loan_second['overdue_class']==3) & (loan_second['month_Desc_report']<3) & (loan_second['is_vouch']==1) & (loan_second['is_now']==1)].max()#现行最近3个月严重逾期有担保个人消费贷款本月实还款最大

        #最近6个月严重逾期个人消费贷款
        dict_out['consume_loan_serious_overdue_account_count_06m']=len(loan_second[(loan_second['businessType']=='其他个人消费贷款') & (loan_second['overdue_class']==3) & (loan_second['month_Desc_report']<6)])#最近6个月严重逾期个人消费贷款账户计数
        dict_out['consume_loan_serious_overdue_month_sum_06m']=sum(list(map(sum,loan_second['overdue_month'][(loan_second['businessType']=='其他个人消费贷款') & (loan_second['overdue_class']==3) & (loan_second['month_Desc_report']<6)])))#最近6个月严重逾期个人消费贷款逾期月数求和

        #现行最近6个月严重逾期个人消费贷款
        dict_out['consume_loan_serious_overdue_account_count_06m_now']=len(loan_second[(loan_second['businessType']=='其他个人消费贷款') & (loan_second['overdue_class']==3) & (loan_second['month_Desc_report']<6) & (loan_second['is_now']==1)])#现行最近6个月严重逾期个人消费贷款账户计数
        dict_out['consume_loan_serious_overdue_month_sum_06m_now']=sum(list(map(sum,loan_second['overdue_month'][(loan_second['businessType']=='其他个人消费贷款') & (loan_second['overdue_class']==3) & (loan_second['month_Desc_report']<6) & (loan_second['is_now']==1)])))#现行最近6个月严重逾期个人消费贷款逾期月数求和
        dict_out['consume_loan_serious_overdue_planRepayAmount_sum_06m_now']=loan_second['planRepayAmount'][(loan_second['businessType']=='其他个人消费贷款') & (loan_second['overdue_class']==3) & (loan_second['month_Desc_report']<6) & (loan_second['is_now']==1)].sum()#现行最近6个月严重逾期个人消费贷款本月应还款求和
        dict_out['consume_loan_serious_overdue_RepayedAmount_sum_06m_now']=loan_second['RepayedAmount'][(loan_second['businessType']=='其他个人消费贷款') & (loan_second['overdue_class']==3) & (loan_second['month_Desc_report']<6) & (loan_second['is_now']==1)].sum()#现行最近6个月严重逾期个人消费贷款本月实还款求和
        dict_out['consume_loan_serious_overdue_planRepayAmount_max_06m_now']=loan_second['planRepayAmount'][(loan_second['businessType']=='其他个人消费贷款') & (loan_second['overdue_class']==3) & (loan_second['month_Desc_report']<6) & (loan_second['is_now']==1)].max()#现行最近6个月严重逾期个人消费贷款本月应还款最大
        dict_out['consume_loan_serious_overdue_RepayedAmount_max_06m_now']=loan_second['RepayedAmount'][(loan_second['businessType']=='其他个人消费贷款') & (loan_second['overdue_class']==3) & (loan_second['month_Desc_report']<6) & (loan_second['is_now']==1)].max()#现行最近6个月严重逾期个人消费贷款本月实还款最大

        #最近6个月严重逾期有担保个人消费贷款
        dict_out['consume_loan_is_vouch_serious_overdue_account_count_06m']=len(loan_second[(loan_second['businessType']=='其他个人消费贷款') & (loan_second['overdue_class']==3) & (loan_second['month_Desc_report']<6) & (loan_second['is_vouch']==1)])#最近6个月严重逾期有担保个人消费贷款账户计数
        dict_out['consume_loan_is_vouch_serious_overdue_month_sum_06m']=sum(list(map(sum,loan_second['overdue_month'][(loan_second['businessType']=='其他个人消费贷款') & (loan_second['overdue_class']==3) & (loan_second['month_Desc_report']<6) & (loan_second['is_vouch']==1)])))#最近6个月严重逾期有担保个人消费贷款逾期月数求和

        #现行最近6个月严重逾期有担保个人消费贷款
        dict_out['consume_loan_is_vouch_serious_overdue_account_count_06m_now']=len(loan_second[(loan_second['businessType']=='其他个人消费贷款') & (loan_second['overdue_class']==3) & (loan_second['month_Desc_report']<6) & (loan_second['is_vouch']==1) & (loan_second['is_now']==1)])#现行最近6个月严重逾期有担保个人消费贷款账户计数
        dict_out['consume_loan_is_vouch_serious_overdue_month_sum_06m_now']=sum(list(map(sum,loan_second['overdue_month'][(loan_second['businessType']=='其他个人消费贷款') & (loan_second['overdue_class']==3) & (loan_second['month_Desc_report']<6) & (loan_second['is_vouch']==1) & (loan_second['is_now']==1)])))#现行最近6个月严重逾期有担保个人消费贷款逾期月数求和
        dict_out['consume_loan_is_vouch_serious_overdue_planRepayAmount_sum_06m_now']=loan_second['planRepayAmount'][(loan_second['businessType']=='其他个人消费贷款') & (loan_second['overdue_class']==3) & (loan_second['month_Desc_report']<6) & (loan_second['is_vouch']==1) & (loan_second['is_now']==1)].sum()#现行最近6个月严重逾期有担保个人消费贷款本月应还款求和
        dict_out['consume_loan_is_vouch_serious_overdue_RepayedAmount_sum_06m_now']=loan_second['RepayedAmount'][(loan_second['businessType']=='其他个人消费贷款') & (loan_second['overdue_class']==3) & (loan_second['month_Desc_report']<6) & (loan_second['is_vouch']==1) & (loan_second['is_now']==1)].sum()#现行最近6个月严重逾期有担保个人消费贷款本月实还款求和
        dict_out['consume_loan_is_vouch_serious_overdue_planRepayAmount_max_06m_now']=loan_second['planRepayAmount'][(loan_second['businessType']=='其他个人消费贷款') & (loan_second['overdue_class']==3) & (loan_second['month_Desc_report']<6) & (loan_second['is_vouch']==1) & (loan_second['is_now']==1)].max()#现行最近6个月严重逾期有担保个人消费贷款本月应还款最大
        dict_out['consume_loan_is_vouch_serious_overdue_RepayedAmount_max_06m_now']=loan_second['RepayedAmount'][(loan_second['businessType']=='其他个人消费贷款') & (loan_second['overdue_class']==3) & (loan_second['month_Desc_report']<6) & (loan_second['is_vouch']==1) & (loan_second['is_now']==1)].max()#现行最近6个月严重逾期有担保个人消费贷款本月实还款最大

        #最近12个月严重逾期个人消费贷款
        dict_out['consume_loan_serious_overdue_account_count_12m']=len(loan_second[(loan_second['businessType']=='其他个人消费贷款') & (loan_second['overdue_class']==3) & (loan_second['month_Desc_report']<12)])#最近12个月严重逾期个人消费贷款账户计数
        dict_out['consume_loan_serious_overdue_month_sum_12m']=sum(list(map(sum,loan_second['overdue_month'][(loan_second['businessType']=='其他个人消费贷款') & (loan_second['overdue_class']==3) & (loan_second['month_Desc_report']<12)])))#最近12个月严重逾期个人消费贷款逾期月数求和

        #现行最近12个月严重逾期个人消费贷款
        dict_out['consume_loan_serious_overdue_account_count_12m_now']=len(loan_second[(loan_second['businessType']=='其他个人消费贷款') & (loan_second['overdue_class']==3) & (loan_second['month_Desc_report']<12) & (loan_second['is_now']==1)])#现行最近12个月严重逾期个人消费贷款账户计数
        dict_out['consume_loan_serious_overdue_month_sum_12m_now']=sum(list(map(sum,loan_second['overdue_month'][(loan_second['businessType']=='其他个人消费贷款') & (loan_second['overdue_class']==3) & (loan_second['month_Desc_report']<12) & (loan_second['is_now']==1)])))#现行最近12个月严重逾期个人消费贷款逾期月数求和
        dict_out['consume_loan_serious_overdue_planRepayAmount_sum_12m_now']=loan_second['planRepayAmount'][(loan_second['businessType']=='其他个人消费贷款') & (loan_second['overdue_class']==3) & (loan_second['month_Desc_report']<12) & (loan_second['is_now']==1)].sum()#现行最近12个月严重逾期个人消费贷款本月应还款求和
        dict_out['consume_loan_serious_overdue_RepayedAmount_sum_12m_now']=loan_second['RepayedAmount'][(loan_second['businessType']=='其他个人消费贷款') & (loan_second['overdue_class']==3) & (loan_second['month_Desc_report']<12) & (loan_second['is_now']==1)].sum()#现行最近12个月严重逾期个人消费贷款本月实还款求和
        dict_out['consume_loan_serious_overdue_planRepayAmount_max_12m_now']=loan_second['planRepayAmount'][(loan_second['businessType']=='其他个人消费贷款') & (loan_second['overdue_class']==3) & (loan_second['month_Desc_report']<12) & (loan_second['is_now']==1)].max()#现行最近12个月严重逾期个人消费贷款本月应还款最大
        dict_out['consume_loan_serious_overdue_RepayedAmount_max_12m_now']=loan_second['RepayedAmount'][(loan_second['businessType']=='其他个人消费贷款') & (loan_second['overdue_class']==3) & (loan_second['month_Desc_report']<12) & (loan_second['is_now']==1)].max()#现行最近12个月严重逾期个人消费贷款本月实还款最大

        #最近12个月严重逾期有担保个人消费贷款
        dict_out['consume_loan_is_vouch_serious_overdue_account_count_12m']=len(loan_second[(loan_second['businessType']=='其他个人消费贷款') & (loan_second['overdue_class']==3) & (loan_second['month_Desc_report']<12) & (loan_second['is_vouch']==1)])#最近12个月严重逾期有担保个人消费贷款账户计数
        dict_out['consume_loan_is_vouch_serious_overdue_month_sum_12m']=sum(list(map(sum,loan_second['overdue_month'][(loan_second['businessType']=='其他个人消费贷款') & (loan_second['overdue_class']==3) & (loan_second['month_Desc_report']<12) & (loan_second['is_vouch']==1)])))#最近12个月严重逾期有担保个人消费贷款逾期月数求和

        #现行最近12个月严重逾期有担保个人消费贷款
        dict_out['consume_loan_is_vouch_serious_overdue_account_count_12m_now']=len(loan_second[(loan_second['businessType']=='其他个人消费贷款') & (loan_second['overdue_class']==3) & (loan_second['month_Desc_report']<12) & (loan_second['is_vouch']==1) & (loan_second['is_now']==1)])#现行最近12个月严重逾期有担保个人消费贷款账户计数
        dict_out['consume_loan_is_vouch_serious_overdue_month_sum_12m_now']=sum(list(map(sum,loan_second['overdue_month'][(loan_second['businessType']=='其他个人消费贷款') & (loan_second['overdue_class']==3) & (loan_second['month_Desc_report']<12) & (loan_second['is_vouch']==1) & (loan_second['is_now']==1)])))#现行最近12个月严重逾期有担保个人消费贷款逾期月数求和
        dict_out['consume_loan_is_vouch_serious_overdue_planRepayAmount_sum_12m_now']=loan_second['planRepayAmount'][(loan_second['businessType']=='其他个人消费贷款') & (loan_second['overdue_class']==3) & (loan_second['month_Desc_report']<12) & (loan_second['is_vouch']==1) & (loan_second['is_now']==1)].sum()#现行最近12个月严重逾期有担保个人消费贷款本月应还款求和
        dict_out['consume_loan_is_vouch_serious_overdue_RepayedAmount_sum_12m_now']=loan_second['RepayedAmount'][(loan_second['businessType']=='其他个人消费贷款') & (loan_second['overdue_class']==3) & (loan_second['month_Desc_report']<12) & (loan_second['is_vouch']==1) & (loan_second['is_now']==1)].sum()#现行最近12个月严重逾期有担保个人消费贷款本月实还款求和
        dict_out['consume_loan_is_vouch_serious_overdue_planRepayAmount_max_12m_now']=loan_second['planRepayAmount'][(loan_second['businessType']=='其他个人消费贷款') & (loan_second['overdue_class']==3) & (loan_second['month_Desc_report']<12) & (loan_second['is_vouch']==1) & (loan_second['is_now']==1)].max()#现行最近12个月严重逾期有担保个人消费贷款本月应还款最大
        dict_out['consume_loan_is_vouch_serious_overdue_RepayedAmount_max_12m_now']=loan_second['RepayedAmount'][(loan_second['businessType']=='其他个人消费贷款') & (loan_second['overdue_class']==3) & (loan_second['month_Desc_report']<12) & (loan_second['is_vouch']==1) & (loan_second['is_now']==1)].max()#现行最近12个月严重逾期有担保个人消费贷款本月实还款最大

        #最近24个月严重逾期个人消费贷款
        dict_out['consume_loan_serious_overdue_account_count_24m']=len(loan_second[(loan_second['businessType']=='其他个人消费贷款') & (loan_second['overdue_class']==3) & (loan_second['month_Desc_report']<24)])#最近24个月严重逾期个人消费贷款账户计数
        dict_out['consume_loan_serious_overdue_month_sum_24m']=sum(list(map(sum,loan_second['overdue_month'][(loan_second['businessType']=='其他个人消费贷款') & (loan_second['overdue_class']==3) & (loan_second['month_Desc_report']<24)])))#最近24个月严重逾期个人消费贷款逾期月数求和

        #现行最近24个月严重逾期个人消费贷款
        dict_out['consume_loan_serious_overdue_account_count_24m_now']=len(loan_second[(loan_second['businessType']=='其他个人消费贷款') & (loan_second['overdue_class']==3) & (loan_second['month_Desc_report']<24) & (loan_second['is_now']==1)])#现行最近24个月严重逾期个人消费贷款账户计数
        dict_out['consume_loan_serious_overdue_month_sum_24m_now']=sum(list(map(sum,loan_second['overdue_month'][(loan_second['businessType']=='其他个人消费贷款') & (loan_second['overdue_class']==3) & (loan_second['month_Desc_report']<24) & (loan_second['is_now']==1)])))#现行最近24个月严重逾期个人消费贷款逾期月数求和
        dict_out['consume_loan_serious_overdue_planRepayAmount_sum_24m_now']=loan_second['planRepayAmount'][(loan_second['businessType']=='其他个人消费贷款') & (loan_second['overdue_class']==3) & (loan_second['month_Desc_report']<24) & (loan_second['is_now']==1)].sum()#现行最近24个月严重逾期个人消费贷款本月应还款求和
        dict_out['consume_loan_serious_overdue_RepayedAmount_sum_24m_now']=loan_second['RepayedAmount'][(loan_second['businessType']=='其他个人消费贷款') & (loan_second['overdue_class']==3) & (loan_second['month_Desc_report']<24) & (loan_second['is_now']==1)].sum()#现行最近24个月严重逾期个人消费贷款本月实还款求和
        dict_out['consume_loan_serious_overdue_planRepayAmount_max_24m_now']=loan_second['planRepayAmount'][(loan_second['businessType']=='其他个人消费贷款') & (loan_second['overdue_class']==3) & (loan_second['month_Desc_report']<24) & (loan_second['is_now']==1)].max()#现行最近24个月严重逾期个人消费贷款本月应还款最大
        dict_out['consume_loan_serious_overdue_RepayedAmount_max_24m_now']=loan_second['RepayedAmount'][(loan_second['businessType']=='其他个人消费贷款') & (loan_second['overdue_class']==3) & (loan_second['month_Desc_report']<24) & (loan_second['is_now']==1)].max()#现行最近24个月严重逾期个人消费贷款本月实还款最大

        #最近24个月严重逾期有担保个人消费贷款
        dict_out['consume_loan_is_vouch_serious_overdue_account_count_24m']=len(loan_second[(loan_second['businessType']=='其他个人消费贷款') & (loan_second['overdue_class']==3) & (loan_second['month_Desc_report']<24) & (loan_second['is_vouch']==1)])#最近24个月严重逾期有担保个人消费贷款账户计数
        dict_out['consume_loan_is_vouch_serious_overdue_month_sum_24m']=sum(list(map(sum,loan_second['overdue_month'][(loan_second['businessType']=='其他个人消费贷款') & (loan_second['overdue_class']==3) & (loan_second['month_Desc_report']<24) & (loan_second['is_vouch']==1)])))#最近24个月严重逾期有担保个人消费贷款逾期月数求和

        #现行最近24个月严重逾期有担保个人消费贷款
        dict_out['consume_loan_is_vouch_serious_overdue_account_count_24m_now']=len(loan_second[(loan_second['businessType']=='其他个人消费贷款') & (loan_second['overdue_class']==3) & (loan_second['month_Desc_report']<24) & (loan_second['is_vouch']==1) & (loan_second['is_now']==1)])#现行最近24个月严重逾期有担保个人消费贷款账户计数
        dict_out['consume_loan_is_vouch_serious_overdue_month_sum_24m_now']=sum(list(map(sum,loan_second['overdue_month'][(loan_second['businessType']=='其他个人消费贷款') & (loan_second['overdue_class']==3) & (loan_second['month_Desc_report']<24) & (loan_second['is_vouch']==1) & (loan_second['is_now']==1)])))#现行最近24个月严重逾期有担保个人消费贷款逾期月数求和
        dict_out['consume_loan_is_vouch_serious_overdue_planRepayAmount_sum_24m_now']=loan_second['planRepayAmount'][(loan_second['businessType']=='其他个人消费贷款') & (loan_second['overdue_class']==3) & (loan_second['month_Desc_report']<24) & (loan_second['is_vouch']==1) & (loan_second['is_now']==1)].sum()#现行最近24个月严重逾期有担保个人消费贷款本月应还款求和
        dict_out['consume_loan_is_vouch_serious_overdue_RepayedAmount_sum_24m_now']=loan_second['RepayedAmount'][(loan_second['businessType']=='其他个人消费贷款') & (loan_second['overdue_class']==3) & (loan_second['month_Desc_report']<24) & (loan_second['is_vouch']==1) & (loan_second['is_now']==1)].sum()#现行最近24个月严重逾期有担保个人消费贷款本月实还款求和
        dict_out['consume_loan_is_vouch_serious_overdue_planRepayAmount_max_24m_now']=loan_second['planRepayAmount'][(loan_second['businessType']=='其他个人消费贷款') & (loan_second['overdue_class']==3) & (loan_second['month_Desc_report']<24) & (loan_second['is_vouch']==1) & (loan_second['is_now']==1)].max()#现行最近24个月严重逾期有担保个人消费贷款本月应还款最大
        dict_out['consume_loan_is_vouch_serious_overdue_RepayedAmount_max_24m_now']=loan_second['RepayedAmount'][(loan_second['businessType']=='其他个人消费贷款') & (loan_second['overdue_class']==3) & (loan_second['month_Desc_report']<24) & (loan_second['is_vouch']==1) & (loan_second['is_now']==1)].max()#现行最近24个月严重逾期有担保个人消费贷款本月实还款最大

        #违约个人消费贷款
        dict_out['consume_loan_is_break_account_count']=len(loan_second[(loan_second['is_break']==1) & (loan_second['businessType']=='其他个人消费贷款')])#违约个人消费贷款账户计数
        dict_out['consume_loan_is_break_amount_sum']=loan_second['loanAmount'][(loan_second['is_break']==1) & (loan_second['businessType']=='其他个人消费贷款')].sum()#违约个人消费贷款本金求和
        dict_out['consume_loan_is_break_amount_max']=loan_second['loanAmount'][(loan_second['is_break']==1) & (loan_second['businessType']=='其他个人消费贷款')].max()#违约个人消费贷款本金最大

        #违约有担保个人消费贷款
        dict_out['consume_loan_is_vouch_is_break_account_count']=len(loan_second[(loan_second['is_break']==1) & (loan_second['businessType']=='其他个人消费贷款') & (loan_second['is_vouch']==1)])#违约有担保个人消费贷款账户计数
        dict_out['consume_loan_is_vouch_is_break_amount_sum']=loan_second['loanAmount'][(loan_second['is_break']==1) & (loan_second['businessType']=='其他个人消费贷款') & (loan_second['is_vouch']==1)].sum()#违约有担保个人消费贷款本金求和
        dict_out['consume_loan_is_vouch_is_break_amount_max']=loan_second['loanAmount'][(loan_second['is_break']==1) & (loan_second['businessType']=='其他个人消费贷款') & (loan_second['is_vouch']==1)].max()#违约有担保个人消费贷款本金最大


        #比率
        if dict_out['loan_account_count_now'] != 0:
            dict_out['lnacn_lacn']=dict_out['loan_ndue_account_count_now'] / dict_out['loan_account_count_now']#现行无逾期贷款账户计数占现行贷款账户计数比率
            dict_out['lnivacn_lacn']=dict_out['loan_ndue_is_vouch_account_count_now'] / dict_out['loan_account_count_now']#现行无逾期有担保贷款账户计数占现行贷款账户计数比率
            dict_out['blnacn_lacn']=dict_out['business_loan_ndue_account_count_now'] / dict_out['loan_account_count_now']#现行无逾期个人经营性贷款账户计数占现行贷款账户计数比率
            dict_out['blivnacn_lacn']=dict_out['business_loan_is_vouch_ndue_account_count_now'] / dict_out['loan_account_count_now']#现行无逾期有担保个人经营性贷款账户计数占现行贷款账户计数比率
            dict_out['clnacn_lacn']=dict_out['consume_loan_ndue_account_count_now'] / dict_out['loan_account_count_now']#现行无逾期个人消费贷款账户计数占现行贷款账户计数比率
            dict_out['clnivacn_lacn']=dict_out['consume_loan_ndue_is_vouch_account_count_now'] / dict_out['loan_account_count_now']#现行无逾期有担保个人消费贷款账户计数占现行贷款账户计数比率
            dict_out['lsoacn_lacn']=dict_out['loan_serious_overdue_account_count_now'] / dict_out['loan_account_count_now']#现行严重逾期贷款账户计数占现行贷款账户计数比率
            dict_out['livsoacn_lacn']=dict_out['loan_is_vouch_serious_overdue_account_count_now'] / dict_out['loan_account_count_now']#现行严重逾期有担保贷款账户计数占现行贷款账户计数比率
            dict_out['blsoacn_lacn']=dict_out['business_loan_serious_overdue_account_count_now'] / dict_out['loan_account_count_now']#现行严重逾期个人经营性贷款账户计数占现行贷款账户计数比率
            dict_out['blivsoacn_lacn']=dict_out['business_loan_is_vouch_serious_overdue_account_count_now'] / dict_out['loan_account_count_now']#现行严重逾期有担保个人经营性贷款账户计数占现行贷款账户计数比率
            dict_out['clsoacn_lacn']=dict_out['consume_loan_serious_overdue_account_count_now'] / dict_out['loan_account_count_now']#现行严重逾期个人消费贷款账户计数占现行贷款账户计数比率
            dict_out['clivsoacn_lacn']=dict_out['consume_loan_is_vouch_serious_overdue_account_count_now'] / dict_out['loan_account_count_now']#现行严重逾期有担保个人消费贷款账户计数占现行贷款账户计数比率
            dict_out['livacn_lacn']=dict_out['loan_is_vouch_account_count_now'] / dict_out['loan_account_count_now']#现行有担保贷款账户计数占现行贷款账户计数比率
            dict_out['blivacn_lacn']=dict_out['business_loan_is_vouch_account_count_now'] / dict_out['loan_account_count_now']#现行有担保个人经营性贷款账户计数占现行贷款账户计数比率
            dict_out['clivacn_lacn']=dict_out['consume_loan_is_vouch_account_count_now'] / dict_out['loan_account_count_now']#现行有担保个人消费贷款账户计数占现行贷款账户计数比率
            dict_out['laco03_lacn']=dict_out['loan_account_count_open_03m'] / dict_out['loan_account_count_now']#最近3个月开户贷款账户计数占现行贷款账户计数比率
            dict_out['laco06_lacn']=dict_out['loan_account_count_open_06m'] / dict_out['loan_account_count_now']#最近6个月开户贷款账户计数占现行贷款账户计数比率
            dict_out['blacn_lacn']=dict_out['business_loan_account_count_now'] / dict_out['loan_account_count_now']#现行个人经营性贷款账户计数占现行贷款账户计数比率
            dict_out['clacn_lacn']=dict_out['consume_loan_account_count_now'] / dict_out['loan_account_count_now']#现行个人消费贷款账户计数占现行贷款账户计数比率
        else:  # 现行贷款账户计数为零
            dict_out['lnacn_lacn'] = 0  # 现行无逾期贷款账户计数占现行贷款账户计数比率
            dict_out['lnivacn_lacn'] = 0  # 现行无逾期有担保贷款账户计数占现行贷款账户计数比率
            dict_out['blnacn_lacn'] = 0  # 现行无逾期个人经营性贷款账户计数占现行贷款账户计数比率
            dict_out['blivnacn_lacn'] = 0  # 现行无逾期有担保个人经营性贷款账户计数占现行贷款账户计数比率
            dict_out['clnacn_lacn'] = 0  # 现行无逾期个人消费贷款账户计数占现行贷款账户计数比率
            dict_out['clnivacn_lacn'] = 0  # 现行无逾期有担保个人消费贷款账户计数占现行贷款账户计数比率
            dict_out['lsoacn_lacn'] = 0  # 现行严重逾期贷款账户计数占现行贷款账户计数比率
            dict_out['livsoacn_lacn'] = 0  # 现行严重逾期有担保贷款账户计数占现行贷款账户计数比率
            dict_out['blsoacn_lacn'] = 0  # 现行严重逾期个人经营性贷款账户计数占现行贷款账户计数比率
            dict_out['blivsoacn_lacn'] = 0  # 现行严重逾期有担保个人经营性贷款账户计数占现行贷款账户计数比率
            dict_out['clsoacn_lacn'] = 0  # 现行严重逾期个人消费贷款账户计数占现行贷款账户计数比率
            dict_out['clivsoacn_lacn'] = 0  # 现行严重逾期有担保个人消费贷款账户计数占现行贷款账户计数比率
            dict_out['livacn_lacn'] = 0  # 现行有担保贷款账户计数占现行贷款账户计数比率
            dict_out['blivacn_lacn'] = 0  # 现行有担保个人经营性贷款账户计数占现行贷款账户计数比率
            dict_out['clivacn_lacn'] = 0  # 现行有担保个人消费贷款账户计数占现行贷款账户计数比率
            dict_out['laco03_lacn'] = 0  # 最近3个月开户贷款账户计数占现行贷款账户计数比率
            dict_out['laco06_lacn'] = 0  # 最近6个月开户贷款账户计数占现行贷款账户计数比率
            dict_out['blacn_lacn'] = 0  # 现行个人经营性贷款账户计数占现行贷款账户计数比率
            dict_out['clacn_lacn'] = 0  # 现行个人消费贷款账户计数占现行贷款账户计数比率

        if dict_out['loan_account_count'] != 0:
            dict_out['lacn_lac']=dict_out['loan_account_count_now'] / dict_out['loan_account_count']#现行贷款账户计数占贷款账户计数比率
            dict_out['lnacn_lac']=dict_out['loan_ndue_account_count_now'] / dict_out['loan_account_count']#现行无逾期贷款账户计数占贷款账户计数比率
            dict_out['lnivacn_lac']=dict_out['loan_ndue_is_vouch_account_count_now'] / dict_out['loan_account_count']#现行无逾期有担保贷款账户计数占贷款账户计数比率
            dict_out['blnacn_lac']=dict_out['business_loan_ndue_account_count_now'] / dict_out['loan_account_count']#现行无逾期个人经营性贷款账户计数占贷款账户计数比率
            dict_out['blivnacn_lac']=dict_out['business_loan_is_vouch_ndue_account_count_now'] / dict_out['loan_account_count']#现行无逾期有担保个人经营性贷款账户计数占贷款账户计数比率
            dict_out['clnacn_lac']=dict_out['consume_loan_ndue_account_count_now'] / dict_out['loan_account_count']#现行无逾期个人消费贷款账户计数占贷款账户计数比率
            dict_out['clnivacn_lac']=dict_out['consume_loan_ndue_is_vouch_account_count_now'] / dict_out['loan_account_count']#现行无逾期有担保个人消费贷款账户计数占贷款账户计数比率
            dict_out['lsoacn_lac']=dict_out['loan_serious_overdue_account_count_now'] / dict_out['loan_account_count']#现行严重逾期贷款账户计数占贷款账户计数比率
            dict_out['livsoacn_lac']=dict_out['loan_is_vouch_serious_overdue_account_count_now'] / dict_out['loan_account_count']#现行严重逾期有担保贷款账户计数占贷款账户计数比率
            dict_out['blsoacn_lac']=dict_out['business_loan_serious_overdue_account_count_now'] / dict_out['loan_account_count']#现行严重逾期个人经营性贷款账户计数占贷款账户计数比率
            dict_out['blivsoacn_lac']=dict_out['business_loan_is_vouch_serious_overdue_account_count_now'] / dict_out['loan_account_count']#现行严重逾期有担保个人经营性贷款账户计数占贷款账户计数比率
            dict_out['clsoacn_lac']=dict_out['consume_loan_serious_overdue_account_count_now'] / dict_out['loan_account_count']#现行严重逾期个人消费贷款账户计数占贷款账户计数比率
            dict_out['clivsoacn_lac']=dict_out['consume_loan_is_vouch_serious_overdue_account_count_now'] / dict_out['loan_account_count']#现行严重逾期有担保个人消费贷款账户计数占贷款账户计数比率
            dict_out['livacn_lac']=dict_out['loan_is_vouch_account_count_now'] / dict_out['loan_account_count']#现行有担保贷款账户计数占贷款账户计数比率
            dict_out['blivacn_lac']=dict_out['business_loan_is_vouch_account_count_now'] / dict_out['loan_account_count']#现行有担保个人经营性贷款账户计数占贷款账户计数比率
            dict_out['clivacn_lac']=dict_out['consume_loan_is_vouch_account_count_now'] / dict_out['loan_account_count']#现行有担保个人消费贷款账户计数占贷款账户计数比率
            dict_out['laco03_lac']=dict_out['loan_account_count_open_03m'] / dict_out['loan_account_count']#最近3个月开户贷款账户计数占贷款账户计数比率
            dict_out['laco06_lac']=dict_out['loan_account_count_open_06m'] / dict_out['loan_account_count']#最近6个月开户贷款账户计数占贷款账户计数比率
            dict_out['blacn_lac']=dict_out['business_loan_account_count_now'] / dict_out['loan_account_count']#现行个人经营性贷款账户计数占贷款账户计数比率
            dict_out['clacn_lac']=dict_out['consume_loan_account_count_now'] / dict_out['loan_account_count']#现行个人消费贷款账户计数占贷款账户计数比率
            dict_out['libac_lac']=dict_out['loan_is_break_account_count'] / dict_out['loan_account_count']#违约贷款账户计数占贷款账户计数比率
            dict_out['blibac_lac']=dict_out['business_loan_is_break_account_count'] / dict_out['loan_account_count']#违约个人经营性贷款账户计数占贷款账户计数比率
            dict_out['clibac_lac']=dict_out['consume_loan_is_break_account_count'] / dict_out['loan_account_count']#违约个人消费贷款账户计数占贷款账户计数比率
        else:  # 贷款账户计数为零
            dict_out['lacn_lac'] = 0  # 现行贷款账户计数占贷款账户计数比率
            dict_out['lnacn_lac'] = 0  # 现行无逾期贷款账户计数占贷款账户计数比率
            dict_out['lnivacn_lac'] = 0  # 现行无逾期有担保贷款账户计数占贷款账户计数比率
            dict_out['blnacn_lac'] = 0  # 现行无逾期个人经营性贷款账户计数占贷款账户计数比率
            dict_out['blivnacn_lac'] = 0  # 现行无逾期有担保个人经营性贷款账户计数占贷款账户计数比率
            dict_out['clnacn_lac'] = 0  # 现行无逾期个人消费贷款账户计数占贷款账户计数比率
            dict_out['clnivacn_lac'] = 0  # 现行无逾期有担保个人消费贷款账户计数占贷款账户计数比率
            dict_out['lsoacn_lac'] = 0  # 现行严重逾期贷款账户计数占贷款账户计数比率
            dict_out['livsoacn_lac'] = 0  # 现行严重逾期有担保贷款账户计数占贷款账户计数比率
            dict_out['blsoacn_lac'] = 0  # 现行严重逾期个人经营性贷款账户计数占贷款账户计数比率
            dict_out['blivsoacn_lac'] = 0  # 现行严重逾期有担保个人经营性贷款账户计数占贷款账户计数比率
            dict_out['clsoacn_lac'] = 0  # 现行严重逾期个人消费贷款账户计数占贷款账户计数比率
            dict_out['clivsoacn_lac'] = 0  # 现行严重逾期有担保个人消费贷款账户计数占贷款账户计数比率
            dict_out['livacn_lac'] = 0  # 现行有担保贷款账户计数占贷款账户计数比率
            dict_out['blivacn_lac'] = 0  # 现行有担保个人经营性贷款账户计数占贷款账户计数比率
            dict_out['clivacn_lac'] = 0  # 现行有担保个人消费贷款账户计数占贷款账户计数比率
            dict_out['laco03_lac'] = 0  # 最近3个月开户贷款账户计数占贷款账户计数比率
            dict_out['laco06_lac'] = 0  # 最近6个月开户贷款账户计数占贷款账户计数比率
            dict_out['blacn_lac'] = 0  # 现行个人经营性贷款账户计数占贷款账户计数比率
            dict_out['clacn_lac'] = 0  # 现行个人消费贷款账户计数占贷款账户计数比率
            dict_out['libac_lac'] = 0  # 违约贷款账户计数占贷款账户计数比率
            dict_out['blibac_lac'] = 0  # 违约个人经营性贷款账户计数占贷款账户计数比率
            dict_out['clibac_lac'] = 0  # 违约个人消费贷款账户计数占贷款账户计数比率

        if dict_out['loan_amount_sum_now'] != 0:
            dict_out['lbsn_lasn']=dict_out['loan_balance_sum_now'] / dict_out['loan_amount_sum_now']#现行贷款本金余额求和占现行贷款本金求和比率
            dict_out['lnasn_lasn']=dict_out['loan_ndue_amount_sum_now'] / dict_out['loan_amount_sum_now']#现行无逾期贷款本金求和占现行贷款本金求和比率
            dict_out['blnasn_lasn']=dict_out['business_loan_ndue_amount_sum_now'] / dict_out['loan_amount_sum_now']#现行无逾期个人经营性贷款本金求和占现行贷款本金求和比率
            dict_out['clnasn_lasn']=dict_out['consume_loan_ndue_amount_sum_now'] / dict_out['loan_amount_sum_now']#现行无逾期个人消费贷款本金求和占现行贷款本金求和比率
            dict_out['lsoasn_lasn']=dict_out['loan_serious_overdue_amount_sum_now'] / dict_out['loan_amount_sum_now']#现行严重逾期贷款本金求和占现行贷款本金求和比率
            dict_out['blsoasn_lasn']=dict_out['business_loan_serious_overdue_amount_sum_now'] / dict_out['loan_amount_sum_now']#现行严重逾期个人经营性贷款本金求和占现行贷款本金求和比率
            dict_out['clsoasn_lasn']=dict_out['consume_loan_serious_overdue_amount_sum_now'] / dict_out['loan_amount_sum_now']#现行严重逾期个人消费贷款本金求和占现行贷款本金求和比率
            dict_out['blasn_lasn'] = dict_out['business_loan_amount_sum_now'] / dict_out['loan_amount_sum_now']  # 现行个人经营性贷款本金求和占现行贷款本金求和比率
            dict_out['blivasn_lasn'] = dict_out['business_loan_is_vouch_amount_sum_now'] / dict_out['loan_amount_sum_now']  # 现行有担保个人经营性贷款本金求和占现行贷款本金求和比率
            dict_out['clasn_lasn'] = dict_out['consume_loan_amount_sum_now'] / dict_out['loan_amount_sum_now']  # 现行个人消费贷款本金求和占现行贷款本金求和比率
            dict_out['clivasn_lasn'] = dict_out['consume_loan_is_vouch_amount_sum_now'] / dict_out['loan_amount_sum_now']  # 现行有担保个人消费贷款本金求和占现行贷款本金求和比率
            dict_out['blbsn_blasn'] = dict_out['business_loan_balance_sum_now'] / dict_out['business_loan_amount_sum_now']  # 现行个人经营性贷款本金余额求和占现行个人经营性贷款本金求和比率
            dict_out['clbsn_clasn'] = dict_out['consume_loan_balance_sum_now'] / dict_out['consume_loan_amount_sum_now']  # 现行个人消费贷款本金余额求和占现行个人消费贷款本金求和比率
        else:  # 现行贷款本金求和为零
            dict_out['lbsn_lasn'] = 0  # 现行贷款本金余额求和占现行贷款本金求和比率
            dict_out['lnasn_lasn'] = 0  # 现行无逾期贷款本金求和占现行贷款本金求和比率
            dict_out['blnasn_lasn'] = 0  # 现行无逾期个人经营性贷款本金求和占现行贷款本金求和比率
            dict_out['clnasn_lasn'] = 0  # 现行无逾期个人消费贷款本金求和占现行贷款本金求和比率
            dict_out['lsoasn_lasn'] = 0  # 现行严重逾期贷款本金求和占现行贷款本金求和比率
            dict_out['blsoasn_lasn'] = 0  # 现行严重逾期个人经营性贷款本金求和占现行贷款本金求和比率
            dict_out['clsoasn_lasn'] = 0  # 现行严重逾期个人消费贷款本金求和占现行贷款本金求和比率
            dict_out['blasn_lasn'] = 0  # 现行个人经营性贷款本金求和占现行贷款本金求和比率
            dict_out['blivasn_lasn'] = 0  # 现行有担保个人经营性贷款本金求和占现行贷款本金求和比率
            dict_out['clasn_lasn'] = 0  # 现行个人消费贷款本金求和占现行贷款本金求和比率
            dict_out['clivasn_lasn'] = 0  # 现行有担保个人消费贷款本金求和占现行贷款本金求和比率
            dict_out['blbsn_blasn'] = 0  # 现行个人经营性贷款本金余额求和占现行个人经营性贷款本金求和比率
            dict_out['clbsn_clasn'] = 0  # 现行个人消费贷款本金余额求和占现行个人消费贷款本金求和比率

        if dict_out['loan_ndue_amount_sum_now'] != 0:
            dict_out['lnbsn_lnasn'] = dict_out['loan_ndue_balance_sum_now'] / dict_out['loan_ndue_amount_sum_now']  # 现行无逾期贷款本金余额求和占现行无逾期贷款本金求和比率
        else:
            dict_out['lnbsn_lnasn'] = 0  # 现行无逾期贷款本金余额求和占现行无逾期贷款本金求和比率

        if dict_out['business_loan_ndue_amount_sum_now'] != 0:
            dict_out['blnbsn_blnasn'] = dict_out['business_loan_ndue_balance_sum_now'] / dict_out['business_loan_ndue_amount_sum_now']  # 现行无逾期个人经营性贷款本金余额求和占现行无逾期个人经营性贷款本金求和比率
        else:
            dict_out['blnbsn_blnasn'] = 0  # 现行无逾期个人经营性贷款本金余额求和占现行无逾期个人经营性贷款本金求和比率

        if dict_out['consume_loan_ndue_amount_sum_now'] != 0:
            dict_out['clnbsn_clnasn'] = dict_out['consume_loan_ndue_balance_sum_now'] / dict_out['consume_loan_ndue_amount_sum_now']  # 现行无逾期个人消费贷款本金余额求和占现行无逾期个人消费贷款本金求和比率
        else:
            dict_out['clnbsn_clnasn'] = 0  # 现行无逾期个人消费贷款本金余额求和占现行无逾期个人消费贷款本金求和比率

        if dict_out['loan_serious_overdue_amount_sum_now'] != 0:
            dict_out['lsobsn_lsoasn'] = dict_out['loan_serious_overdue_balance_sum_now'] / dict_out['loan_serious_overdue_amount_sum_now']  # 现行严重逾期贷款本金余额求和占现行严重逾期贷款本金求和比率
        else:
            dict_out['lsobsn_lsoasn'] = 0  # 现行严重逾期贷款本金余额求和占现行严重逾期贷款本金求和比率

        if dict_out['business_loan_serious_overdue_amount_sum_now'] != 0:
            dict_out['blsobsn_blsoasn'] = dict_out['business_loan_serious_overdue_balance_sum_now'] / dict_out['business_loan_serious_overdue_amount_sum_now']  # 现行严重逾期个人经营性贷款本金余额求和占现行严重逾期个人经营性贷款本金求和比率
        else:
            dict_out['blsobsn_blsoasn'] = 0  # 现行严重逾期个人经营性贷款本金余额求和占现行严重逾期个人经营性贷款本金求和比率

        if dict_out['consume_loan_serious_overdue_amount_sum_now'] != 0:
            dict_out['clsobsn_clsoasn'] = dict_out['consume_loan_serious_overdue_balance_sum_now'] / dict_out['consume_loan_serious_overdue_amount_sum_now']  # 现行严重逾期个人消费贷款本金余额求和占现行严重逾期个人消费贷款本金求和比率
        else:
            dict_out['clsobsn_clsoasn'] = 0  # 现行严重逾期个人消费贷款本金余额求和占现行严重逾期个人消费贷款本金求和比率

        if dict_out['loan_amount_sum'] != 0:
            dict_out['lnas_las']=dict_out['loan_ndue_amount_sum'] / dict_out['loan_amount_sum']#无逾期贷款本金求和占贷款本金求和比率
            dict_out['blnas_las']=dict_out['business_loan_ndue_amount_sum'] / dict_out['loan_amount_sum']#无逾期个人经营性贷款本金求和占贷款本金求和比率
            dict_out['clnas_las']=dict_out['consume_loan_ndue_amount_sum'] / dict_out['loan_amount_sum']#无逾期个人消费贷款本金求和占贷款本金求和比率
            dict_out['lsoas_las']=dict_out['loan_serious_overdue_amount_sum'] / dict_out['loan_amount_sum']#严重逾期贷款本金求和占贷款本金求和比率
            dict_out['blsoas_las']=dict_out['business_loan_serious_overdue_amount_sum'] / dict_out['loan_amount_sum']#严重逾期个人经营性贷款本金求和占贷款本金求和比率
            dict_out['clsoas_las']=dict_out['consume_loan_serious_overdue_amount_sum'] / dict_out['loan_amount_sum']#严重逾期个人消费贷款本金求和占贷款本金求和比率
            dict_out['blnas_blas']=dict_out['business_loan_ndue_amount_sum'] / dict_out['business_loan_amount_sum']#无逾期个人经营性贷款本金求和占个人经营性贷款本金求和比率
            dict_out['clnas_clas']=dict_out['consume_loan_ndue_amount_sum'] / dict_out['consume_loan_amount_sum']#无逾期个人消费贷款本金求和占个人消费贷款本金求和比率
            dict_out['blsoas_blas']=dict_out['business_loan_serious_overdue_amount_sum'] / dict_out['business_loan_amount_sum']#严重逾期个人经营性贷款本金求和占个人经营性贷款本金求和比率
            dict_out['clsoas_clas']=dict_out['consume_loan_serious_overdue_amount_sum'] / dict_out['consume_loan_amount_sum']#严重逾期个人消费贷款本金求和占个人消费贷款本金求和比率
        else:
            dict_out['lnas_las'] = 0  # 无逾期贷款本金求和占贷款本金求和比率
            dict_out['blnas_las'] = 0  # 无逾期个人经营性贷款本金求和占贷款本金求和比率
            dict_out['clnas_las'] = 0  # 无逾期个人消费贷款本金求和占贷款本金求和比率
            dict_out['lsoas_las'] = 0  # 严重逾期贷款本金求和占贷款本金求和比率
            dict_out['blsoas_las'] = 0  # 严重逾期个人经营性贷款本金求和占贷款本金求和比率
            dict_out['clsoas_las'] = 0  # 严重逾期个人消费贷款本金求和占贷款本金求和比率
            dict_out['blnas_blas'] = 0  # 无逾期个人经营性贷款本金求和占个人经营性贷款本金求和比率
            dict_out['clnas_clas'] = 0  # 无逾期个人消费贷款本金求和占个人消费贷款本金求和比率
            dict_out['blsoas_blas'] = 0  # 严重逾期个人经营性贷款本金求和占个人经营性贷款本金求和比率
            dict_out['clsoas_clas'] = 0  # 严重逾期个人消费贷款本金求和占个人消费贷款本金求和比率

        dict_out['first_loan_overdue_month_sum']=loan_second['overdue_month_sum'][loan_second['startDate']==loan_second['startDate'].min()].values[0]#第一笔贷款总逾期月数
        dict_out['recent_loan_overdue_month_sum']=loan_second['overdue_month_sum'][loan_second['startDate']==loan_second['startDate'].max()].values[0]#最近一笔贷款总逾期月数
        dict_out['loan_amount_max_overdue_month_sum']=loan_second['overdue_month_sum'][loan_second['loanAmount']==loan_second['loanAmount'].max()].values[0]#本金最大一笔贷款总逾期月数
        dict_out['first_loan_overdue_month_max']=loan_second['overdue_month_max'][loan_second['startDate']==loan_second['startDate'].min()].values[0]#第一笔贷款最大持续逾期月数
        dict_out['recent_loan_overdue_month_max']=loan_second['overdue_month_max'][loan_second['startDate']==loan_second['startDate'].max()].values[0]#最近一笔贷款最大持续逾期月数
        dict_out['loan_amount_max_overdue_month_max']=loan_second['overdue_month_max'][loan_second['loanAmount']==loan_second['loanAmount'].max()].values[0]#本金最大一笔贷款最大持续逾期月数
        dict_out['first_loan_rating_worst']={'1.0':'损失', '2.0':'可疑', '3.0':'关注', '4.0':'次级', '5.0':'正常','nan':"无"}[str(loan_second['classify5_num'][loan_second['startDate']==loan_second['startDate'].min()].min())]#第一笔贷款最差Rating
        dict_out['recent_loan_rating_worst']={'1.0':'损失', '2.0':'可疑', '3.0':'关注', '4.0':'次级', '5.0':'正常','nan':"无"}[str(loan_second['classify5_num'][loan_second['startDate']==loan_second['startDate'].max()].min())]#最近一笔贷款最差Rating
        dict_out['loan_amount_max_rating_worst']={'1.0':'损失', '2.0':'可疑', '3.0':'关注', '4.0':'次级', '5.0':'正常','nan':"无"}[str(loan_second['classify5_num'][loan_second['loanAmount']==loan_second['loanAmount'].max()].min())]#本金最大一笔贷款最差Rating
        dict_out['first_loan_amount']=loan_second['loanAmount'][loan_second['startDate']==loan_second['startDate'].min()].values[0]#第一笔贷款本金
        dict_out['recent_loan_amount']=loan_second['loanAmount'][loan_second['startDate']==loan_second['startDate'].max()].values[0]#最近一笔贷款本金
        dict_out['first_loan_time_till_now']=time_del(dict_out['reportTime'],loan_second['startDate'].min())#第一笔贷款距现在的时间间隔
        dict_out['recent_loan_time_till_now']=time_del(dict_out['reportTime'],loan_second['startDate'].max())#最近一笔贷款距现在的时间间隔
        dict_out['loan_amount_max_time_till_now']=time_del(dict_out['reportTime'],loan_second['startDate'][loan_second['loanAmount']==loan_second['loanAmount'].max()].values[0])#本金最大一笔贷款距现在的时间间隔
        return dict_out
    else:
        return dict_out






