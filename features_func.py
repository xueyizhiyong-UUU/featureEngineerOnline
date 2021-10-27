import numpy as np
import pandas as pd 
from scipy.stats import chi2
# import scorecardpy as sc
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
import os
import math
from scipy import stats
from multiprocessing import Pool
import multiprocessing
import sys
import requests
from dateutil.relativedelta import relativedelta
import json
from itertools import chain

#函数 两个日期的月份差 
#import datetime
#from dateutil.relativedelta import relativedelta
def monthdelta(startdate, enddate):
    startdate1=re.findall(r'[1-9][0-9]{3} *?[0-9]{1,2} *?[0-9]{1,2}|[1-9][0-9]{3} *?[-/.] *?[0-9]{1,2} *?[-/.] *?[0-9]{1,2}',str(startdate))
    startdate2=re.findall('[1-9][0-9]{3} *?[-/.]? *?[0-9]{1,2} *?',str(startdate))
    
    if  (len(startdate1)==0) & (len(startdate2)==0) :
        return np.nan
    elif (len(startdate1)>0) :
        startdate=startdate1[0]
        startdate=re.sub('[-/.]','-',re.sub(' ','',startdate))
        sub1=re.findall('-([0-9]{1}-)',startdate)
        startdate=re.sub('-'+sub1[0],'-0'+str(sub1[0]),startdate)  if len(sub1)>0 else startdate
        sub2=re.findall('-([0-9]{1}$)',startdate)
        startdate=re.sub('-'+sub2[0],'-0'+str(sub2[0]),startdate)  if len(sub2)>0 else startdate
        startdate=re.sub('[-/.]','',startdate)
        d1=datetime.datetime.strptime(startdate[0:10],'%Y%m%d')
    elif len(startdate2)>0:
        startdate=startdate2[0]
        startdate=re.sub('[-/.]','-',re.sub(' ','',startdate))
        sub2=re.findall('-([0-9]{1}$)',startdate)
        startdate=re.sub('-'+sub2[0],'-0'+str(sub2[0]),startdate)  if len(sub2)>0 else startdate
        startdate=re.sub('[-/.]','',startdate)
        d1=datetime.datetime.strptime(startdate[0:10],'%Y%m')
    else :
        return np.nan
    
    
    enddate1=re.findall(r'[1-9][0-9]{3} *?[0-9]{1,2} *?[0-9]{1,2}|[1-9][0-9]{3} *?[-/.] *?[0-9]{1,2} *?[-/.] *?[0-9]{1,2}',str(enddate))
    enddate2=re.findall('[1-9][0-9]{3} *?[-/.]? *?[0-9]{1,2} *?',str(enddate))
    if  (len(enddate1)==0) & (len(enddate2)==0) :
        return np.nan
    elif (len(enddate1)>0):
        enddate=enddate1[0]
        enddate=re.sub('[-/.]','-',re.sub(' ','',enddate))
        sub1=re.findall('-([0-9]{1}-)',enddate)
        enddate=re.sub('-'+sub1[0],'-0'+str(sub1[0]),enddate)  if len(sub1)>0 else enddate
        sub2=re.findall('-([0-9]{1}$)',enddate)
        enddate=re.sub('-'+sub2[0],'-0'+str(sub2[0]),enddate)  if len(sub2)>0 else enddate
        enddate=re.sub('[-/.]','',enddate)
        d2=datetime.datetime.strptime(enddate[0:10],'%Y%m%d')
    elif len(enddate2)>0:
        enddate=enddate2[0]
        enddate=re.sub('[-/.]','-',re.sub(' ','',enddate))
        sub2=re.findall('-([0-9]{1}$)',enddate)
        enddate=re.sub('-'+sub2[0],'-0'+str(sub2[0]),enddate)  if len(sub2)>0 else enddate
        enddate=re.sub('[-/.]','',enddate)
        d2=datetime.datetime.strptime(enddate[0:10],'%Y%m')
    else :
        return np.nan
    
    delta = 0
    if d1<=d2:
        while True:
            d1_tmp = d1+ relativedelta(months=delta+1)
            if d1_tmp <= d2:
                delta += 1
            else:
                break
        return delta
    else:
        while True:
            d1_tmp = d1+ relativedelta(months=delta-1)
            if d1_tmp >= d2:
                delta -= 1
            else:
                break
        return delta
#monthdelta('2019.1.30', '2019.3.30')


#函数 两个日期的天数差 
def daysdelta(startdate, enddate):
    startdate=re.findall(r'[1-9][0-9]{3} *?[0-9]{1,2} *?[0-9]{1,2}|[1-9][0-9]{3} *?[-/.] *?[0-9]{1,2} *?[-/.] *?[0-9]{1,2}',str(startdate))
    if len(startdate)>0:
        startdate=startdate[0]
        startdate=re.sub('[-/.]','-',re.sub(' ','',startdate))
        sub1=re.findall('-([0-9]{1}-)',startdate)   
        if len(sub1)>0:
            sub1_is_0='1' if str(sub1[0])=='0' else sub1[0]
            startdate=re.sub('-'+sub1[0],'-0'+str(sub1_is_0),startdate)
        
        sub2=re.findall('-([0-9]{1}$)',startdate)
        if len(sub2)>0:
            sub2_is_0='1' if str(sub2[0])=='0' else sub2[0]
            startdate=re.sub('-'+sub2[0]+'$','-0'+str(sub2_is_0),startdate)
        startdate=re.sub('[-/.]','',startdate)
        d1=datetime.datetime.strptime(startdate[0:10],'%Y%m%d')
    else :
        return np.nan
    
    enddate=re.findall(r'[1-9][0-9]{3} *?[0-9]{1,2} *?[0-9]{1,2}|[1-9][0-9]{3} *?[-/.] *?[0-9]{1,2} *?[-/.] *?[0-9]{1,2}',str(enddate))
    if len(enddate)>0:
        enddate=enddate[0]
        enddate=re.sub('[-/.]','-',re.sub(' ','',enddate))  #把间隔替换为-
        sub1=re.findall('-([0-9]{1}-)',enddate)       #把间2050-1-1隔替换为2050-01-1
        if len(sub1)>0:
            sub1_is_0='1' if str(sub1[0])=='0' else sub1[0]
            enddate=re.sub('-'+sub1[0],'-0'+str(sub1_is_0),enddate)
        sub2=re.findall('-([0-9]{1}$)',enddate)       #把间2050-01-1隔替换为2050-01-01
        if len(sub2)>0:
            sub2_is_0='1' if str(sub2[0])=='0' else sub2[0]
            enddate=re.sub('-'+sub2[0]+'$','-0'+str(sub2_is_0),enddate)
        enddate=re.sub('[-/.]','',enddate)
        d2=datetime.datetime.strptime(enddate[0:10],'%Y%m%d')
    else :
        return np.nan
    return (d2-d1).days
#daysdelta('2019.1.30', '2019.3.30')

def dict_generator(indict, pre=None):
    pre = pre[:] if pre else []
    if isinstance(indict, dict):
        for key, value in indict.items():
            if isinstance(value, dict):
                if len(value) == 0:
                    yield pre+[key, '{}']
                else:
                    for d in dict_generator(value, pre + [key]):
                        yield d
            elif isinstance(value, list):
                if len(value) == 0:
                    yield pre + [key, '[]']
                else:
                    for v in range(len(value)):
                        for d in dict_generator(value[v], pre + [key, v]):
                            yield d
            elif isinstance(value, tuple):
                if len(value) == 0:
                    yield pre+[key, '()']
                else:
                    for v in value:
                        for d in dict_generator(v, pre + [key]):
                            yield d
            else:
                yield pre + [key, value]
    else:
        yield indict
        
def json_to_dict(json_str, path_pre=None,path_layer=1,connector='.'):
    #path_layer=0 if path_layer==0 else -1-path_layer
    if type(json_str)==str:
        sValue = json.loads(json_str)
    else:
        sValue = json_str
    tmp_dict=dict()
    for i in dict_generator(sValue, path_pre):
        if len(connector.join(i[path_layer:-1]))>0 :
            if (path_layer!=0) & pd.notnull(path_pre):
                tmp_dict.update({i[0]+connector+connector.join(i[path_layer:-1]):i[-1]}) #选路径作为变量名（i[0:-1]):i[-1] 为完整路径）  tmp_dict.update({'.'.join(i[-2:-1]):i[-1]}) 为取最后一层
            else:
                tmp_dict.update({connector.join(i[path_layer:-1]):i[-1]})
        #print('.'.join(i[0:-1]), ':', i[-1])
    return tmp_dict 

def lower_to_capital(dict_info):
    new_dict = {}
    for i, j in dict_info.items():
        new_dict[i.upper()] = j
    return new_dict

#BAIRONG_APPLY_LOAN	208     百融-借贷意向验证(0.25) ApplyLoanStr
#json_dict=json.loads(br_df_respBody)
def BAIRONG_APPLY_LOAN_feature_online(dict_out,json_dict):
    ApplyLoanStr=json_dict['applyLoanStr'] if 'applyLoanStr' in json_dict.keys() and type(json_dict['applyLoanStr'])==dict else {'dateSecType':{'getQueryType':{}}}
    d07=ApplyLoanStr['m1'] if 'd7' in ApplyLoanStr.keys() and type(ApplyLoanStr['d7'])==dict else dict()
    d15=ApplyLoanStr['m1'] if 'd15' in ApplyLoanStr.keys() and type(ApplyLoanStr['d15'])==dict else dict()
    m01=ApplyLoanStr['m1'] if 'm1' in ApplyLoanStr.keys() and type(ApplyLoanStr['m1'])==dict else dict()
    m03=ApplyLoanStr['m3'] if 'm3' in ApplyLoanStr.keys() and type(ApplyLoanStr['m3'])==dict else dict()
    m06=ApplyLoanStr['m6'] if 'm6' in ApplyLoanStr.keys() and type(ApplyLoanStr['m6'])==dict else dict()
    m12=ApplyLoanStr['m12'] if 'm12' in ApplyLoanStr.keys() and type(ApplyLoanStr['m12'])==dict else dict()

    d07_id=json_to_dict(d07['id'], path_pre=None,path_layer=0,connector='_') if 'id' in d07.keys() else dict()
    d15_id=json_to_dict(d15['id'], path_pre=None,path_layer=0,connector='_') if 'id' in d15.keys() else dict()
    m01_id=json_to_dict(m01['id'], path_pre=None,path_layer=0,connector='_') if 'id' in m01.keys() else dict()
    m03_id=json_to_dict(m03['id'], path_pre=None,path_layer=0,connector='_') if 'id' in m03.keys() else dict()
    m06_id=json_to_dict(m06['id'], path_pre=None,path_layer=0,connector='_') if 'id' in m06.keys() else dict()
    m12_id=json_to_dict(m12['id'], path_pre=None,path_layer=0,connector='_') if 'id' in m12.keys() else dict()

    d07_cell=json_to_dict(d07['cell'], path_pre=None,path_layer=0,connector='_') if 'cell' in d07.keys() else dict()
    d15_cell=json_to_dict(d15['cell'], path_pre=None,path_layer=0,connector='_') if 'cell' in d15.keys() else dict()
    m01_cell=json_to_dict(m01['cell'], path_pre=None,path_layer=0,connector='_') if 'cell' in m01.keys() else dict()
    m03_cell=json_to_dict(m03['cell'], path_pre=None,path_layer=0,connector='_') if 'cell' in m03.keys() else dict()
    m06_cell=json_to_dict(m06['cell'], path_pre=None,path_layer=0,connector='_') if 'cell' in m06.keys() else dict()
    m12_cell=json_to_dict(m12['cell'], path_pre=None,path_layer=0,connector='_') if 'cell' in m12.keys() else dict()

    dict_out['BAIRONG_APPLY_LOAN_nbank_else_orgnum_m12_cell']=float(m12_cell['nbank_else_orgnum']) if 'nbank_else_orgnum' in m12_cell.keys() else np.nan
    dict_out['BAIRONG_APPLY_LOAN_pdl_orgnum_m03_id']=float(m03_id['pdl_orgnum']) if 'pdl_orgnum' in m03_id.keys() else np.nan
    dict_out['BAIRONG_APPLY_LOAN_min_monnum_m06_id']=float(m06_id['min_monnum']) if 'min_monnum' in m06_id.keys() else np.nan
    dict_out['BAIRONG_APPLY_LOAN_af_allnum_m03_cell']=float(m03_cell['af_allnum']) if 'af_allnum' in m03_cell.keys() else np.nan
    dict_out['BAIRONG_APPLY_LOAN_nbank_min_monnum_m06_id']=float(m06_id['nbank_min_monnum']) if 'nbank_min_monnum' in m06_id.keys() else np.nan
    dict_out['BAIRONG_APPLY_LOAN_bank_tra_allnum_m06_cell']=float(m06_cell['bank_tra_allnum']) if 'bank_tra_allnum' in m06_cell.keys() else np.nan
    dict_out['BAIRONG_APPLY_LOAN_nbank_nsloan_allnum_m03_cell']=float(m03_cell['nbank_nsloan_allnum']) if 'nbank_nsloan_allnum' in m03_cell.keys() else np.nan
    dict_out['BAIRONG_APPLY_LOAN_oth_orgnum_m12_id']=float(m12_id['oth_orgnum']) if 'oth_orgnum' in m12_id.keys() else np.nan
    dict_out['BAIRONG_APPLY_LOAN_pdl_orgnum_m12_cell']=float(m12_cell['pdl_orgnum']) if 'pdl_orgnum' in m12_cell.keys() else np.nan
    dict_out['BAIRONG_APPLY_LOAN_nbank_allnum_m06_cell']=float(m06_cell['nbank_allnum']) if 'nbank_allnum' in m06_cell.keys() else np.nan
    dict_out['BAIRONG_APPLY_LOAN_nbank_oth_allnum_d07_id']=float(d07_id['nbank_oth_allnum']) if 'nbank_oth_allnum' in d07_id.keys() else np.nan
    dict_out['BAIRONG_APPLY_LOAN_nbank_night_allnum_m01_cell']=float(m01_cell['nbank_night_allnum']) if 'nbank_night_allnum' in m01_cell.keys() else np.nan
    dict_out['BAIRONG_APPLY_LOAN_nbank_avg_monnum_m06_id']=float(m06_id['nbank_avg_monnum']) if 'nbank_avg_monnum' in m06_id.keys() else np.nan
    dict_out['BAIRONG_APPLY_LOAN_oth_allnum_m06_cell']=float(m06_cell['oth_allnum']) if 'oth_allnum' in m06_cell.keys() else np.nan
    dict_out['BAIRONG_APPLY_LOAN_rel_orgnum_d15_id']=float(d15_id['rel_orgnum']) if 'rel_orgnum' in d15_id.keys() else np.nan
    dict_out['BAIRONG_APPLY_LOAN_bank_week_allnum_m12_id']=float(m12_id['bank_week_allnum']) if 'bank_week_allnum' in m12_id.keys() else np.nan
    dict_out['BAIRONG_APPLY_LOAN_nbank_week_allnum_d15_id']=float(d15_id['nbank_week_allnum']) if 'nbank_week_allnum' in d15_id.keys() else np.nan
    dict_out['BAIRONG_APPLY_LOAN_max_inteday_m12_id']=float(m12_id['max_inteday']) if 'max_inteday' in m12_id.keys() else np.nan
    dict_out['BAIRONG_APPLY_LOAN_nbank_cf_orgnum_d07_id']=float(d07_id['nbank_cf_orgnum']) if 'nbank_cf_orgnum' in d07_id.keys() else np.nan
    dict_out['BAIRONG_APPLY_LOAN_rel_allnum_m03_cell']=float(m03_cell['rel_allnum']) if 'rel_allnum' in m03_cell.keys() else np.nan
    dict_out['BAIRONG_APPLY_LOAN_nbank_p2p_allnum_d15_cell']=float(d15_cell['nbank_p2p_allnum']) if 'nbank_p2p_allnum' in d15_cell.keys() else np.nan
    dict_out['BAIRONG_APPLY_LOAN_nbank_night_allnum_m03_cell']=float(m03_cell['nbank_night_allnum']) if 'nbank_night_allnum' in m03_cell.keys() else np.nan
    dict_out['BAIRONG_APPLY_LOAN_nbank_night_orgnum_m01_cell']=float(m01_cell['nbank_night_orgnum']) if 'nbank_night_orgnum' in m01_cell.keys() else np.nan
    dict_out['BAIRONG_APPLY_LOAN_nbank_finlea_allnum_d15_cell']=float(d15_cell['nbank_finlea_allnum']) if 'nbank_finlea_allnum' in d15_cell.keys() else np.nan
    dict_out['BAIRONG_APPLY_LOAN_nbank_p2p_orgnum_m12_cell']=float(m12_cell['nbank_p2p_orgnum']) if 'nbank_p2p_orgnum' in m12_cell.keys() else np.nan
    dict_out['BAIRONG_APPLY_LOAN_nbank_week_orgnum_m06_cell']=float(m06_cell['nbank_week_orgnum']) if 'nbank_week_orgnum' in m06_cell.keys() else np.nan
    dict_out['BAIRONG_APPLY_LOAN_bank_tra_orgnum_m01_cell']=float(m01_cell['bank_tra_orgnum']) if 'bank_tra_orgnum' in m01_cell.keys() else np.nan
    dict_out['BAIRONG_APPLY_LOAN_nbank_cf_allnum_m03_cell']=float(m03_cell['nbank_cf_allnum']) if 'nbank_cf_allnum' in m03_cell.keys() else np.nan
    dict_out['BAIRONG_APPLY_LOAN_bank_allnum_m03_id']=float(m03_id['bank_allnum']) if 'bank_allnum' in m03_id.keys() else np.nan
    dict_out['BAIRONG_APPLY_LOAN_pdl_orgnum_d07_id']=float(d07_id['pdl_orgnum']) if 'pdl_orgnum' in d07_id.keys() else np.nan
    dict_out['BAIRONG_APPLY_LOAN_bank_tot_mons_m06_cell']=float(m06_cell['bank_tot_mons']) if 'bank_tot_mons' in m06_cell.keys() else np.nan
    dict_out['BAIRONG_APPLY_LOAN_nbank_else_orgnum_m03_id']=float(m03_id['nbank_else_orgnum']) if 'nbank_else_orgnum' in m03_id.keys() else np.nan
    dict_out['BAIRONG_APPLY_LOAN_nbank_sloan_orgnum_m03_cell']=float(m03_cell['nbank_sloan_orgnum']) if 'nbank_sloan_orgnum' in m03_cell.keys() else np.nan
    dict_out['BAIRONG_APPLY_LOAN_nbank_p2p_orgnum_m06_cell']=float(m06_cell['nbank_p2p_orgnum']) if 'nbank_p2p_orgnum' in m06_cell.keys() else np.nan
    dict_out['BAIRONG_APPLY_LOAN_pdl_allnum_d15_cell']=float(d15_cell['pdl_allnum']) if 'pdl_allnum' in d15_cell.keys() else np.nan
    dict_out['BAIRONG_APPLY_LOAN_nbank_ca_allnum_m12_cell']=float(m12_cell['nbank_ca_allnum']) if 'nbank_ca_allnum' in m12_cell.keys() else np.nan
    dict_out['BAIRONG_APPLY_LOAN_bank_orgnum_m03_cell']=float(m03_cell['bank_orgnum']) if 'bank_orgnum' in m03_cell.keys() else np.nan
    dict_out['BAIRONG_APPLY_LOAN_nbank_orgnum_d15_cell']=float(d15_cell['nbank_orgnum']) if 'nbank_orgnum' in d15_cell.keys() else np.nan
    dict_out['BAIRONG_APPLY_LOAN_nbank_p2p_orgnum_m12_id']=float(m12_id['nbank_p2p_orgnum']) if 'nbank_p2p_orgnum' in m12_id.keys() else np.nan
    dict_out['BAIRONG_APPLY_LOAN_nbank_week_allnum_m06_cell']=float(m06_cell['nbank_week_allnum']) if 'nbank_week_allnum' in m06_cell.keys() else np.nan
    dict_out['BAIRONG_APPLY_LOAN_cooff_orgnum_m03_id']=float(m03_id['cooff_orgnum']) if 'cooff_orgnum' in m03_id.keys() else np.nan
    dict_out['BAIRONG_APPLY_LOAN_nbank_max_inteday_m03_id']=float(m03_id['nbank_max_inteday']) if 'nbank_max_inteday' in m03_id.keys() else np.nan
    dict_out['BAIRONG_APPLY_LOAN_cooff_allnum_d15_cell']=float(d15_cell['cooff_allnum']) if 'cooff_allnum' in d15_cell.keys() else np.nan
    dict_out['BAIRONG_APPLY_LOAN_nbank_night_orgnum_m12_cell']=float(m12_cell['nbank_night_orgnum']) if 'nbank_night_orgnum' in m12_cell.keys() else np.nan
    dict_out['BAIRONG_APPLY_LOAN_nbank_mc_allnum_m12_id']=float(m12_id['nbank_mc_allnum']) if 'nbank_mc_allnum' in m12_id.keys() else np.nan
    dict_out['BAIRONG_APPLY_LOAN_nbank_week_orgnum_m12_id']=float(m12_id['nbank_week_orgnum']) if 'nbank_week_orgnum' in m12_id.keys() else np.nan
    dict_out['BAIRONG_APPLY_LOAN_nbank_else_allnum_d15_id']=float(d15_id['nbank_else_allnum']) if 'nbank_else_allnum' in d15_id.keys() else np.nan
    dict_out['BAIRONG_APPLY_LOAN_pdl_orgnum_d15_cell']=float(d15_cell['pdl_orgnum']) if 'pdl_orgnum' in d15_cell.keys() else np.nan
    dict_out['BAIRONG_APPLY_LOAN_nbank_p2p_orgnum_m03_id']=float(m03_id['nbank_p2p_orgnum']) if 'nbank_p2p_orgnum' in m03_id.keys() else np.nan
    dict_out['BAIRONG_APPLY_LOAN_nbank_else_allnum_d15_cell']=float(d15_cell['nbank_else_allnum']) if 'nbank_else_allnum' in d15_cell.keys() else np.nan
    dict_out['BAIRONG_APPLY_LOAN_caoff_orgnum_m12_cell']=float(m12_cell['caoff_orgnum']) if 'caoff_orgnum' in m12_cell.keys() else np.nan
    dict_out['BAIRONG_APPLY_LOAN_oth_allnum_m03_id']=float(m03_id['oth_allnum']) if 'oth_allnum' in m03_id.keys() else np.nan
    dict_out['BAIRONG_APPLY_LOAN_nbank_nsloan_allnum_m12_cell']=float(m12_cell['nbank_nsloan_allnum']) if 'nbank_nsloan_allnum' in m12_cell.keys() else np.nan
    dict_out['BAIRONG_APPLY_LOAN_nbank_else_allnum_m06_id']=float(m06_id['nbank_else_allnum']) if 'nbank_else_allnum' in m06_id.keys() else np.nan
    dict_out['BAIRONG_APPLY_LOAN_avg_monnum_m12_cell']=float(m12_cell['avg_monnum']) if 'avg_monnum' in m12_cell.keys() else np.nan
    dict_out['BAIRONG_APPLY_LOAN_nbank_nsloan_orgnum_d15_cell']=float(d15_cell['nbank_nsloan_orgnum']) if 'nbank_nsloan_orgnum' in d15_cell.keys() else np.nan
    dict_out['BAIRONG_APPLY_LOAN_nbank_autofin_allnum_m12_cell']=float(m12_cell['nbank_autofin_allnum']) if 'nbank_autofin_allnum' in m12_cell.keys() else np.nan
    dict_out['BAIRONG_APPLY_LOAN_bank_avg_monnum_m06_id']=float(m06_id['bank_avg_monnum']) if 'bank_avg_monnum' in m06_id.keys() else np.nan
    dict_out['BAIRONG_APPLY_LOAN_nbank_night_orgnum_m06_id']=float(m06_id['nbank_night_orgnum']) if 'nbank_night_orgnum' in m06_id.keys() else np.nan
    dict_out['BAIRONG_APPLY_LOAN_nbank_ca_orgnum_d07_cell']=float(d07_cell['nbank_ca_orgnum']) if 'nbank_ca_orgnum' in d07_cell.keys() else np.nan
    dict_out['BAIRONG_APPLY_LOAN_bank_ret_allnum_m03_id']=float(m03_id['bank_ret_allnum']) if 'bank_ret_allnum' in m03_id.keys() else np.nan
    dict_out['BAIRONG_APPLY_LOAN_nbank_min_inteday_m03_id']=float(m03_id['nbank_min_inteday']) if 'nbank_min_inteday' in m03_id.keys() else np.nan
    dict_out['BAIRONG_APPLY_LOAN_nbank_cf_orgnum_m12_cell']=float(m12_cell['nbank_cf_orgnum']) if 'nbank_cf_orgnum' in m12_cell.keys() else np.nan
    dict_out['BAIRONG_APPLY_LOAN_af_allnum_m01_cell']=float(m01_cell['af_allnum']) if 'af_allnum' in m01_cell.keys() else np.nan
    dict_out['BAIRONG_APPLY_LOAN_nbank_night_allnum_m06_id']=float(m06_id['nbank_night_allnum']) if 'nbank_night_allnum' in m06_id.keys() else np.nan
    dict_out['BAIRONG_APPLY_LOAN_caoff_allnum_d07_cell']=float(d07_cell['caoff_allnum']) if 'caoff_allnum' in d07_cell.keys() else np.nan
    dict_out['BAIRONG_APPLY_LOAN_bank_max_inteday_m06_cell']=float(m06_cell['bank_max_inteday']) if 'bank_max_inteday' in m06_cell.keys() else np.nan
    dict_out['BAIRONG_APPLY_LOAN_pdl_allnum_d15_id']=float(d15_id['pdl_allnum']) if 'pdl_allnum' in d15_id.keys() else np.nan
    dict_out['BAIRONG_APPLY_LOAN_avg_monnum_m06_id']=float(m06_id['avg_monnum']) if 'avg_monnum' in m06_id.keys() else np.nan
    dict_out['BAIRONG_APPLY_LOAN_nbank_finlea_allnum_m06_id']=float(m06_id['nbank_finlea_allnum']) if 'nbank_finlea_allnum' in m06_id.keys() else np.nan
    dict_out['BAIRONG_APPLY_LOAN_nbank_else_orgnum_m06_cell']=float(m06_cell['nbank_else_orgnum']) if 'nbank_else_orgnum' in m06_cell.keys() else np.nan
    dict_out['BAIRONG_APPLY_LOAN_cooff_orgnum_m01_id']=float(m01_id['cooff_orgnum']) if 'cooff_orgnum' in m01_id.keys() else np.nan
    dict_out['BAIRONG_APPLY_LOAN_nbank_mc_allnum_m06_id']=float(m06_id['nbank_mc_allnum']) if 'nbank_mc_allnum' in m06_id.keys() else np.nan
    dict_out['BAIRONG_APPLY_LOAN_bank_week_allnum_m03_id']=float(m03_id['bank_week_allnum']) if 'bank_week_allnum' in m03_id.keys() else np.nan
    dict_out['BAIRONG_APPLY_LOAN_max_inteday_m06_id']=float(m06_id['max_inteday']) if 'max_inteday' in m06_id.keys() else np.nan
    dict_out['BAIRONG_APPLY_LOAN_caon_allnum_m12_cell']=float(m12_cell['caon_allnum']) if 'caon_allnum' in m12_cell.keys() else np.nan
    dict_out['BAIRONG_APPLY_LOAN_nbank_nsloan_allnum_m06_id']=float(m06_id['nbank_nsloan_allnum']) if 'nbank_nsloan_allnum' in m06_id.keys() else np.nan
    dict_out['BAIRONG_APPLY_LOAN_nbank_cons_orgnum_m01_cell']=float(m01_cell['nbank_cons_orgnum']) if 'nbank_cons_orgnum' in m01_cell.keys() else np.nan
    dict_out['BAIRONG_APPLY_LOAN_nbank_sloan_orgnum_m12_cell']=float(m12_cell['nbank_sloan_orgnum']) if 'nbank_sloan_orgnum' in m12_cell.keys() else np.nan
    dict_out['BAIRONG_APPLY_LOAN_af_orgnum_m01_id']=float(m01_id['af_orgnum']) if 'af_orgnum' in m01_id.keys() else np.nan
    dict_out['BAIRONG_APPLY_LOAN_nbank_oth_orgnum_m06_id']=float(m06_id['nbank_oth_orgnum']) if 'nbank_oth_orgnum' in m06_id.keys() else np.nan
    dict_out['BAIRONG_APPLY_LOAN_bank_ret_orgnum_m12_id']=float(m12_id['bank_ret_orgnum']) if 'bank_ret_orgnum' in m12_id.keys() else np.nan
    dict_out['BAIRONG_APPLY_LOAN_nbank_finlea_allnum_m12_id']=float(m12_id['nbank_finlea_allnum']) if 'nbank_finlea_allnum' in m12_id.keys() else np.nan
    dict_out['BAIRONG_APPLY_LOAN_nbank_week_orgnum_d07_cell']=float(d07_cell['nbank_week_orgnum']) if 'nbank_week_orgnum' in d07_cell.keys() else np.nan
    dict_out['BAIRONG_APPLY_LOAN_coon_allnum_d07_id']=float(d07_id['coon_allnum']) if 'coon_allnum' in d07_id.keys() else np.nan
    dict_out['BAIRONG_APPLY_LOAN_nbank_mc_allnum_m06_cell']=float(m06_cell['nbank_mc_allnum']) if 'nbank_mc_allnum' in m06_cell.keys() else np.nan
    dict_out['BAIRONG_APPLY_LOAN_nbank_week_orgnum_m01_cell']=float(m01_cell['nbank_week_orgnum']) if 'nbank_week_orgnum' in m01_cell.keys() else np.nan
    dict_out['BAIRONG_APPLY_LOAN_caon_orgnum_m06_cell']=float(m06_cell['caon_orgnum']) if 'caon_orgnum' in m06_cell.keys() else np.nan
    dict_out['BAIRONG_APPLY_LOAN_bank_orgnum_m03_id']=float(m03_id['bank_orgnum']) if 'bank_orgnum' in m03_id.keys() else np.nan
    dict_out['BAIRONG_APPLY_LOAN_nbank_tot_mons_m12_cell']=float(m12_cell['nbank_tot_mons']) if 'nbank_tot_mons' in m12_cell.keys() else np.nan
    dict_out['BAIRONG_APPLY_LOAN_caoff_orgnum_m01_cell']=float(m01_cell['caoff_orgnum']) if 'caoff_orgnum' in m01_cell.keys() else np.nan
    dict_out['BAIRONG_APPLY_LOAN_nbank_finlea_allnum_d07_id']=float(d07_id['nbank_finlea_allnum']) if 'nbank_finlea_allnum' in d07_id.keys() else np.nan
    dict_out['BAIRONG_APPLY_LOAN_nbank_nsloan_allnum_d07_id']=float(d07_id['nbank_nsloan_allnum']) if 'nbank_nsloan_allnum' in d07_id.keys() else np.nan
    dict_out['BAIRONG_APPLY_LOAN_nbank_mc_allnum_m12_cell']=float(m12_cell['nbank_mc_allnum']) if 'nbank_mc_allnum' in m12_cell.keys() else np.nan
    dict_out['BAIRONG_APPLY_LOAN_nbank_avg_monnum_m03_id']=float(m03_id['nbank_avg_monnum']) if 'nbank_avg_monnum' in m03_id.keys() else np.nan
    dict_out['BAIRONG_APPLY_LOAN_coon_orgnum_m06_cell']=float(m06_cell['coon_orgnum']) if 'coon_orgnum' in m06_cell.keys() else np.nan
    dict_out['BAIRONG_APPLY_LOAN_rel_allnum_m03_id']=float(m03_id['rel_allnum']) if 'rel_allnum' in m03_id.keys() else np.nan
    dict_out['BAIRONG_APPLY_LOAN_oth_allnum_m12_id']=float(m12_id['oth_allnum']) if 'oth_allnum' in m12_id.keys() else np.nan
    dict_out['BAIRONG_APPLY_LOAN_caon_allnum_m01_cell']=float(m01_cell['caon_allnum']) if 'caon_allnum' in m01_cell.keys() else np.nan
    dict_out['BAIRONG_APPLY_LOAN_oth_allnum_d07_id']=float(d07_id['oth_allnum']) if 'oth_allnum' in d07_id.keys() else np.nan
    dict_out['BAIRONG_APPLY_LOAN_nbank_cons_allnum_m03_id']=float(m03_id['nbank_cons_allnum']) if 'nbank_cons_allnum' in m03_id.keys() else np.nan
    dict_out['BAIRONG_APPLY_LOAN_nbank_mc_allnum_m03_cell']=float(m03_cell['nbank_mc_allnum']) if 'nbank_mc_allnum' in m03_cell.keys() else np.nan
    dict_out['BAIRONG_APPLY_LOAN_nbank_sloan_orgnum_m06_cell']=float(m06_cell['nbank_sloan_orgnum']) if 'nbank_sloan_orgnum' in m06_cell.keys() else np.nan
    dict_out['BAIRONG_APPLY_LOAN_nbank_else_orgnum_m01_id']=float(m01_id['nbank_else_orgnum']) if 'nbank_else_orgnum' in m01_id.keys() else np.nan
    dict_out['BAIRONG_APPLY_LOAN_nbank_week_allnum_m01_cell']=float(m01_cell['nbank_week_allnum']) if 'nbank_week_allnum' in m01_cell.keys() else np.nan
    dict_out['BAIRONG_APPLY_LOAN_bank_max_monnum_m06_cell']=float(m06_cell['bank_max_monnum']) if 'bank_max_monnum' in m06_cell.keys() else np.nan
    dict_out['BAIRONG_APPLY_LOAN_af_orgnum_m03_cell']=float(m03_cell['af_orgnum']) if 'af_orgnum' in m03_cell.keys() else np.nan
    dict_out['BAIRONG_APPLY_LOAN_bank_night_allnum_m12_cell']=float(m12_cell['bank_night_allnum']) if 'bank_night_allnum' in m12_cell.keys() else np.nan
    dict_out['BAIRONG_APPLY_LOAN_caon_orgnum_m12_id']=float(m12_id['caon_orgnum']) if 'caon_orgnum' in m12_id.keys() else np.nan
    dict_out['BAIRONG_APPLY_LOAN_bank_week_orgnum_m06_cell']=float(m06_cell['bank_week_orgnum']) if 'bank_week_orgnum' in m06_cell.keys() else np.nan
    dict_out['BAIRONG_APPLY_LOAN_caoff_allnum_m12_id']=float(m12_id['caoff_allnum']) if 'caoff_allnum' in m12_id.keys() else np.nan
    dict_out['BAIRONG_APPLY_LOAN_nbank_ca_allnum_d15_id']=float(d15_id['nbank_ca_allnum']) if 'nbank_ca_allnum' in d15_id.keys() else np.nan
    dict_out['BAIRONG_APPLY_LOAN_caoff_orgnum_m03_cell']=float(m03_cell['caoff_orgnum']) if 'caoff_orgnum' in m03_cell.keys() else np.nan
    dict_out['BAIRONG_APPLY_LOAN_oth_allnum_d07_cell']=float(d07_cell['oth_allnum']) if 'oth_allnum' in d07_cell.keys() else np.nan
    dict_out['BAIRONG_APPLY_LOAN_caon_orgnum_m03_id']=float(m03_id['caon_orgnum']) if 'caon_orgnum' in m03_id.keys() else np.nan
    dict_out['BAIRONG_APPLY_LOAN_min_inteday_m12_cell']=float(m12_cell['min_inteday']) if 'min_inteday' in m12_cell.keys() else np.nan
    dict_out['BAIRONG_APPLY_LOAN_nbank_ca_allnum_d07_id']=float(d07_id['nbank_ca_allnum']) if 'nbank_ca_allnum' in d07_id.keys() else np.nan
    dict_out['BAIRONG_APPLY_LOAN_pdl_allnum_d07_cell']=float(d07_cell['pdl_allnum']) if 'pdl_allnum' in d07_cell.keys() else np.nan
    dict_out['BAIRONG_APPLY_LOAN_rel_orgnum_m12_id']=float(m12_id['rel_orgnum']) if 'rel_orgnum' in m12_id.keys() else np.nan
    dict_out['BAIRONG_APPLY_LOAN_nbank_allnum_m01_id']=float(m01_id['nbank_allnum']) if 'nbank_allnum' in m01_id.keys() else np.nan
    dict_out['BAIRONG_APPLY_LOAN_nbank_ca_orgnum_m03_cell']=float(m03_cell['nbank_ca_orgnum']) if 'nbank_ca_orgnum' in m03_cell.keys() else np.nan
    dict_out['BAIRONG_APPLY_LOAN_oth_allnum_m01_cell']=float(m01_cell['oth_allnum']) if 'oth_allnum' in m01_cell.keys() else np.nan
    dict_out['BAIRONG_APPLY_LOAN_nbank_orgnum_m12_cell']=float(m12_cell['nbank_orgnum']) if 'nbank_orgnum' in m12_cell.keys() else np.nan
    dict_out['BAIRONG_APPLY_LOAN_cooff_allnum_m03_id']=float(m03_id['cooff_allnum']) if 'cooff_allnum' in m03_id.keys() else np.nan
    dict_out['BAIRONG_APPLY_LOAN_cooff_allnum_m12_id']=float(m12_id['cooff_allnum']) if 'cooff_allnum' in m12_id.keys() else np.nan
    dict_out['BAIRONG_APPLY_LOAN_cooff_allnum_m01_id']=float(m01_id['cooff_allnum']) if 'cooff_allnum' in m01_id.keys() else np.nan
    dict_out['BAIRONG_APPLY_LOAN_bank_allnum_d15_id']=float(d15_id['bank_allnum']) if 'bank_allnum' in d15_id.keys() else np.nan
    dict_out['BAIRONG_APPLY_LOAN_rel_allnum_d07_id']=float(d07_id['rel_allnum']) if 'rel_allnum' in d07_id.keys() else np.nan
    dict_out['BAIRONG_APPLY_LOAN_nbank_sloan_orgnum_m01_cell']=float(m01_cell['nbank_sloan_orgnum']) if 'nbank_sloan_orgnum' in m01_cell.keys() else np.nan
    dict_out['BAIRONG_APPLY_LOAN_caoff_orgnum_d15_id']=float(d15_id['caoff_orgnum']) if 'caoff_orgnum' in d15_id.keys() else np.nan
    dict_out['BAIRONG_APPLY_LOAN_pdl_orgnum_m06_id']=float(m06_id['pdl_orgnum']) if 'pdl_orgnum' in m06_id.keys() else np.nan
    dict_out['BAIRONG_APPLY_LOAN_nbank_p2p_allnum_m06_cell']=float(m06_cell['nbank_p2p_allnum']) if 'nbank_p2p_allnum' in m06_cell.keys() else np.nan
    dict_out['BAIRONG_APPLY_LOAN_pdl_allnum_m01_cell']=float(m01_cell['pdl_allnum']) if 'pdl_allnum' in m01_cell.keys() else np.nan
    dict_out['BAIRONG_APPLY_LOAN_nbank_orgnum_m03_cell']=float(m03_cell['nbank_orgnum']) if 'nbank_orgnum' in m03_cell.keys() else np.nan
    dict_out['BAIRONG_APPLY_LOAN_rel_orgnum_m01_id']=float(m01_id['rel_orgnum']) if 'rel_orgnum' in m01_id.keys() else np.nan
    dict_out['BAIRONG_APPLY_LOAN_rel_orgnum_m03_cell']=float(m03_cell['rel_orgnum']) if 'rel_orgnum' in m03_cell.keys() else np.nan
    dict_out['BAIRONG_APPLY_LOAN_oth_orgnum_d07_cell']=float(d07_cell['oth_orgnum']) if 'oth_orgnum' in d07_cell.keys() else np.nan
    dict_out['BAIRONG_APPLY_LOAN_nbank_ca_allnum_m12_id']=float(m12_id['nbank_ca_allnum']) if 'nbank_ca_allnum' in m12_id.keys() else np.nan
    dict_out['BAIRONG_APPLY_LOAN_caon_orgnum_d15_cell']=float(d15_cell['caon_orgnum']) if 'caon_orgnum' in d15_cell.keys() else np.nan
    dict_out['BAIRONG_APPLY_LOAN_caoff_orgnum_m03_id']=float(m03_id['caoff_orgnum']) if 'caoff_orgnum' in m03_id.keys() else np.nan
    dict_out['BAIRONG_APPLY_LOAN_nbank_mc_orgnum_m06_id']=float(m06_id['nbank_mc_orgnum']) if 'nbank_mc_orgnum' in m06_id.keys() else np.nan
    dict_out['BAIRONG_APPLY_LOAN_bank_week_allnum_m06_id']=float(m06_id['bank_week_allnum']) if 'bank_week_allnum' in m06_id.keys() else np.nan
    dict_out['BAIRONG_APPLY_LOAN_bank_week_orgnum_m03_id']=float(m03_id['bank_week_orgnum']) if 'bank_week_orgnum' in m03_id.keys() else np.nan
    dict_out['BAIRONG_APPLY_LOAN_nbank_p2p_orgnum_m01_cell']=float(m01_cell['nbank_p2p_orgnum']) if 'nbank_p2p_orgnum' in m01_cell.keys() else np.nan
    dict_out['BAIRONG_APPLY_LOAN_nbank_cons_allnum_d07_cell']=float(d07_cell['nbank_cons_allnum']) if 'nbank_cons_allnum' in d07_cell.keys() else np.nan
    dict_out['BAIRONG_APPLY_LOAN_cooff_orgnum_m06_cell']=float(m06_cell['cooff_orgnum']) if 'cooff_orgnum' in m06_cell.keys() else np.nan
    dict_out['BAIRONG_APPLY_LOAN_nbank_else_orgnum_m01_cell']=float(m01_cell['nbank_else_orgnum']) if 'nbank_else_orgnum' in m01_cell.keys() else np.nan
    dict_out['BAIRONG_APPLY_LOAN_caoff_orgnum_d07_id']=float(d07_id['caoff_orgnum']) if 'caoff_orgnum' in d07_id.keys() else np.nan
    dict_out['BAIRONG_APPLY_LOAN_af_orgnum_m12_id']=float(m12_id['af_orgnum']) if 'af_orgnum' in m12_id.keys() else np.nan
    dict_out['BAIRONG_APPLY_LOAN_nbank_else_orgnum_d15_cell']=float(d15_cell['nbank_else_orgnum']) if 'nbank_else_orgnum' in d15_cell.keys() else np.nan
    dict_out['BAIRONG_APPLY_LOAN_rel_allnum_d07_cell']=float(d07_cell['rel_allnum']) if 'rel_allnum' in d07_cell.keys() else np.nan
    dict_out['BAIRONG_APPLY_LOAN_nbank_week_allnum_m03_cell']=float(m03_cell['nbank_week_allnum']) if 'nbank_week_allnum' in m03_cell.keys() else np.nan
    dict_out['BAIRONG_APPLY_LOAN_nbank_max_monnum_m03_id']=float(m03_id['nbank_max_monnum']) if 'nbank_max_monnum' in m03_id.keys() else np.nan
    dict_out['BAIRONG_APPLY_LOAN_nbank_cf_allnum_m12_id']=float(m12_id['nbank_cf_allnum']) if 'nbank_cf_allnum' in m12_id.keys() else np.nan
    dict_out['BAIRONG_APPLY_LOAN_nbank_cf_orgnum_m03_cell']=float(m03_cell['nbank_cf_orgnum']) if 'nbank_cf_orgnum' in m03_cell.keys() else np.nan
    dict_out['BAIRONG_APPLY_LOAN_oth_allnum_m06_id']=float(m06_id['oth_allnum']) if 'oth_allnum' in m06_id.keys() else np.nan
    dict_out['BAIRONG_APPLY_LOAN_bank_orgnum_m12_cell']=float(m12_cell['bank_orgnum']) if 'bank_orgnum' in m12_cell.keys() else np.nan
    dict_out['BAIRONG_APPLY_LOAN_nbank_mc_orgnum_m03_id']=float(m03_id['nbank_mc_orgnum']) if 'nbank_mc_orgnum' in m03_id.keys() else np.nan
    dict_out['BAIRONG_APPLY_LOAN_nbank_cf_orgnum_m06_id']=float(m06_id['nbank_cf_orgnum']) if 'nbank_cf_orgnum' in m06_id.keys() else np.nan
    dict_out['BAIRONG_APPLY_LOAN_nbank_cf_orgnum_m01_cell']=float(m01_cell['nbank_cf_orgnum']) if 'nbank_cf_orgnum' in m01_cell.keys() else np.nan
    dict_out['BAIRONG_APPLY_LOAN_nbank_max_monnum_m03_cell']=float(m03_cell['nbank_max_monnum']) if 'nbank_max_monnum' in m03_cell.keys() else np.nan
    dict_out['BAIRONG_APPLY_LOAN_caon_allnum_d15_id']=float(d15_id['caon_allnum']) if 'caon_allnum' in d15_id.keys() else np.nan
    dict_out['BAIRONG_APPLY_LOAN_cooff_allnum_d15_id']=float(d15_id['cooff_allnum']) if 'cooff_allnum' in d15_id.keys() else np.nan
    dict_out['BAIRONG_APPLY_LOAN_nbank_finlea_allnum_m06_cell']=float(m06_cell['nbank_finlea_allnum']) if 'nbank_finlea_allnum' in m06_cell.keys() else np.nan
    dict_out['BAIRONG_APPLY_LOAN_min_monnum_m03_cell']=float(m03_cell['min_monnum']) if 'min_monnum' in m03_cell.keys() else np.nan
    dict_out['BAIRONG_APPLY_LOAN_oth_orgnum_m01_cell']=float(m01_cell['oth_orgnum']) if 'oth_orgnum' in m01_cell.keys() else np.nan
    dict_out['BAIRONG_APPLY_LOAN_bank_ret_allnum_m12_id']=float(m12_id['bank_ret_allnum']) if 'bank_ret_allnum' in m12_id.keys() else np.nan
    dict_out['BAIRONG_APPLY_LOAN_max_inteday_m06_cell']=float(m06_cell['max_inteday']) if 'max_inteday' in m06_cell.keys() else np.nan
    dict_out['BAIRONG_APPLY_LOAN_nbank_else_allnum_m03_cell']=float(m03_cell['nbank_else_allnum']) if 'nbank_else_allnum' in m03_cell.keys() else np.nan
    dict_out['BAIRONG_APPLY_LOAN_max_inteday_m03_id']=float(m03_id['max_inteday']) if 'max_inteday' in m03_id.keys() else np.nan
    dict_out['BAIRONG_APPLY_LOAN_pdl_orgnum_m12_id']=float(m12_id['pdl_orgnum']) if 'pdl_orgnum' in m12_id.keys() else np.nan
    dict_out['BAIRONG_APPLY_LOAN_nbank_oth_orgnum_d15_id']=float(d15_id['nbank_oth_orgnum']) if 'nbank_oth_orgnum' in d15_id.keys() else np.nan
    dict_out['BAIRONG_APPLY_LOAN_nbank_orgnum_m06_id']=float(m06_id['nbank_orgnum']) if 'nbank_orgnum' in m06_id.keys() else np.nan
    dict_out['BAIRONG_APPLY_LOAN_af_orgnum_m06_cell']=float(m06_cell['af_orgnum']) if 'af_orgnum' in m06_cell.keys() else np.nan
    dict_out['BAIRONG_APPLY_LOAN_nbank_p2p_allnum_m12_id']=float(m12_id['nbank_p2p_allnum']) if 'nbank_p2p_allnum' in m12_id.keys() else np.nan
    dict_out['BAIRONG_APPLY_LOAN_bank_tra_allnum_d15_cell']=float(d15_cell['bank_tra_allnum']) if 'bank_tra_allnum' in d15_cell.keys() else np.nan
    dict_out['BAIRONG_APPLY_LOAN_bank_tra_allnum_m06_id']=float(m06_id['bank_tra_allnum']) if 'bank_tra_allnum' in m06_id.keys() else np.nan
    dict_out['BAIRONG_APPLY_LOAN_nbank_else_orgnum_m06_id']=float(m06_id['nbank_else_orgnum']) if 'nbank_else_orgnum' in m06_id.keys() else np.nan
    dict_out['BAIRONG_APPLY_LOAN_nbank_tot_mons_m06_cell']=float(m06_cell['nbank_tot_mons']) if 'nbank_tot_mons' in m06_cell.keys() else np.nan
    dict_out['BAIRONG_APPLY_LOAN_nbank_week_orgnum_d15_id']=float(d15_id['nbank_week_orgnum']) if 'nbank_week_orgnum' in d15_id.keys() else np.nan
    dict_out['BAIRONG_APPLY_LOAN_bank_night_allnum_m12_id']=float(m12_id['bank_night_allnum']) if 'bank_night_allnum' in m12_id.keys() else np.nan
    dict_out['BAIRONG_APPLY_LOAN_nbank_week_allnum_m06_id']=float(m06_id['nbank_week_allnum']) if 'nbank_week_allnum' in m06_id.keys() else np.nan
    dict_out['BAIRONG_APPLY_LOAN_nbank_nsloan_allnum_m12_id']=float(m12_id['nbank_nsloan_allnum']) if 'nbank_nsloan_allnum' in m12_id.keys() else np.nan
    dict_out['BAIRONG_APPLY_LOAN_nbank_oth_orgnum_m03_id']=float(m03_id['nbank_oth_orgnum']) if 'nbank_oth_orgnum' in m03_id.keys() else np.nan
    dict_out['BAIRONG_APPLY_LOAN_nbank_orgnum_m01_cell']=float(m01_cell['nbank_orgnum']) if 'nbank_orgnum' in m01_cell.keys() else np.nan
    dict_out['BAIRONG_APPLY_LOAN_nbank_p2p_allnum_d07_cell']=float(d07_cell['nbank_p2p_allnum']) if 'nbank_p2p_allnum' in d07_cell.keys() else np.nan
    dict_out['BAIRONG_APPLY_LOAN_coon_orgnum_m12_cell']=float(m12_cell['coon_orgnum']) if 'coon_orgnum' in m12_cell.keys() else np.nan
    dict_out['BAIRONG_APPLY_LOAN_nbank_ca_orgnum_m12_id']=float(m12_id['nbank_ca_orgnum']) if 'nbank_ca_orgnum' in m12_id.keys() else np.nan
    dict_out['BAIRONG_APPLY_LOAN_nbank_sloan_allnum_m06_cell']=float(m06_cell['nbank_sloan_allnum']) if 'nbank_sloan_allnum' in m06_cell.keys() else np.nan
    dict_out['BAIRONG_APPLY_LOAN_caon_allnum_d15_cell']=float(d15_cell['caon_allnum']) if 'caon_allnum' in d15_cell.keys() else np.nan
    dict_out['BAIRONG_APPLY_LOAN_caoff_orgnum_m06_id']=float(m06_id['caoff_orgnum']) if 'caoff_orgnum' in m06_id.keys() else np.nan
    dict_out['BAIRONG_APPLY_LOAN_nbank_ca_orgnum_m03_id']=float(m03_id['nbank_ca_orgnum']) if 'nbank_ca_orgnum' in m03_id.keys() else np.nan
    dict_out['BAIRONG_APPLY_LOAN_nbank_cf_orgnum_d15_cell']=float(d15_cell['nbank_cf_orgnum']) if 'nbank_cf_orgnum' in d15_cell.keys() else np.nan
    dict_out['BAIRONG_APPLY_LOAN_nbank_cf_allnum_d15_id']=float(d15_id['nbank_cf_allnum']) if 'nbank_cf_allnum' in d15_id.keys() else np.nan
    dict_out['BAIRONG_APPLY_LOAN_pdl_allnum_m06_id']=float(m06_id['pdl_allnum']) if 'pdl_allnum' in m06_id.keys() else np.nan
    dict_out['BAIRONG_APPLY_LOAN_nbank_night_orgnum_m03_cell']=float(m03_cell['nbank_night_orgnum']) if 'nbank_night_orgnum' in m03_cell.keys() else np.nan
    dict_out['BAIRONG_APPLY_LOAN_nbank_cons_allnum_m06_id']=float(m06_id['nbank_cons_allnum']) if 'nbank_cons_allnum' in m06_id.keys() else np.nan
    dict_out['BAIRONG_APPLY_LOAN_caoff_allnum_m01_id']=float(m01_id['caoff_allnum']) if 'caoff_allnum' in m01_id.keys() else np.nan
    dict_out['BAIRONG_APPLY_LOAN_min_monnum_m12_id']=float(m12_id['min_monnum']) if 'min_monnum' in m12_id.keys() else np.nan
    dict_out['BAIRONG_APPLY_LOAN_bank_max_inteday_m12_id']=float(m12_id['bank_max_inteday']) if 'bank_max_inteday' in m12_id.keys() else np.nan
    dict_out['BAIRONG_APPLY_LOAN_nbank_cf_orgnum_d07_cell']=float(d07_cell['nbank_cf_orgnum']) if 'nbank_cf_orgnum' in d07_cell.keys() else np.nan
    dict_out['BAIRONG_APPLY_LOAN_caoff_allnum_m03_id']=float(m03_id['caoff_allnum']) if 'caoff_allnum' in m03_id.keys() else np.nan
    dict_out['BAIRONG_APPLY_LOAN_nbank_else_allnum_m01_id']=float(m01_id['nbank_else_allnum']) if 'nbank_else_allnum' in m01_id.keys() else np.nan
    dict_out['BAIRONG_APPLY_LOAN_min_monnum_m03_id']=float(m03_id['min_monnum']) if 'min_monnum' in m03_id.keys() else np.nan
    dict_out['BAIRONG_APPLY_LOAN_nbank_tot_mons_m12_id']=float(m12_id['nbank_tot_mons']) if 'nbank_tot_mons' in m12_id.keys() else np.nan
    dict_out['BAIRONG_APPLY_LOAN_nbank_p2p_allnum_d15_id']=float(d15_id['nbank_p2p_allnum']) if 'nbank_p2p_allnum' in d15_id.keys() else np.nan
    dict_out['BAIRONG_APPLY_LOAN_nbank_tot_mons_m06_id']=float(m06_id['nbank_tot_mons']) if 'nbank_tot_mons' in m06_id.keys() else np.nan
    dict_out['BAIRONG_APPLY_LOAN_nbank_week_orgnum_d07_id']=float(d07_id['nbank_week_orgnum']) if 'nbank_week_orgnum' in d07_id.keys() else np.nan
    dict_out['BAIRONG_APPLY_LOAN_caon_allnum_m06_cell']=float(m06_cell['caon_allnum']) if 'caon_allnum' in m06_cell.keys() else np.nan
    dict_out['BAIRONG_APPLY_LOAN_rel_orgnum_m01_cell']=float(m01_cell['rel_orgnum']) if 'rel_orgnum' in m01_cell.keys() else np.nan
    dict_out['BAIRONG_APPLY_LOAN_nbank_oth_orgnum_d07_cell']=float(d07_cell['nbank_oth_orgnum']) if 'nbank_oth_orgnum' in d07_cell.keys() else np.nan
    dict_out['BAIRONG_APPLY_LOAN_cooff_allnum_d07_cell']=float(d07_cell['cooff_allnum']) if 'cooff_allnum' in d07_cell.keys() else np.nan
    dict_out['BAIRONG_APPLY_LOAN_pdl_allnum_m01_id']=float(m01_id['pdl_allnum']) if 'pdl_allnum' in m01_id.keys() else np.nan
    dict_out['BAIRONG_APPLY_LOAN_coon_allnum_m06_cell']=float(m06_cell['coon_allnum']) if 'coon_allnum' in m06_cell.keys() else np.nan
    dict_out['BAIRONG_APPLY_LOAN_bank_min_inteday_m06_id']=float(m06_id['bank_min_inteday']) if 'bank_min_inteday' in m06_id.keys() else np.nan
    dict_out['BAIRONG_APPLY_LOAN_nbank_night_allnum_m01_id']=float(m01_id['nbank_night_allnum']) if 'nbank_night_allnum' in m01_id.keys() else np.nan
    dict_out['BAIRONG_APPLY_LOAN_max_monnum_m12_id']=float(m12_id['max_monnum']) if 'max_monnum' in m12_id.keys() else np.nan
    dict_out['BAIRONG_APPLY_LOAN_cooff_orgnum_m03_cell']=float(m03_cell['cooff_orgnum']) if 'cooff_orgnum' in m03_cell.keys() else np.nan
    dict_out['BAIRONG_APPLY_LOAN_nbank_p2p_orgnum_m03_cell']=float(m03_cell['nbank_p2p_orgnum']) if 'nbank_p2p_orgnum' in m03_cell.keys() else np.nan
    dict_out['BAIRONG_APPLY_LOAN_caon_allnum_m01_id']=float(m01_id['caon_allnum']) if 'caon_allnum' in m01_id.keys() else np.nan
    dict_out['BAIRONG_APPLY_LOAN_nbank_cons_orgnum_m03_id']=float(m03_id['nbank_cons_orgnum']) if 'nbank_cons_orgnum' in m03_id.keys() else np.nan
    dict_out['BAIRONG_APPLY_LOAN_caon_allnum_m03_cell']=float(m03_cell['caon_allnum']) if 'caon_allnum' in m03_cell.keys() else np.nan
    dict_out['BAIRONG_APPLY_LOAN_coon_allnum_m12_id']=float(m12_id['coon_allnum']) if 'coon_allnum' in m12_id.keys() else np.nan
    dict_out['BAIRONG_APPLY_LOAN_nbank_max_inteday_m03_cell']=float(m03_cell['nbank_max_inteday']) if 'nbank_max_inteday' in m03_cell.keys() else np.nan
    dict_out['BAIRONG_APPLY_LOAN_rel_allnum_m12_id']=float(m12_id['rel_allnum']) if 'rel_allnum' in m12_id.keys() else np.nan
    dict_out['BAIRONG_APPLY_LOAN_nbank_allnum_m12_cell']=float(m12_cell['nbank_allnum']) if 'nbank_allnum' in m12_cell.keys() else np.nan
    dict_out['BAIRONG_APPLY_LOAN_caon_orgnum_m03_cell']=float(m03_cell['caon_orgnum']) if 'caon_orgnum' in m03_cell.keys() else np.nan
    dict_out['BAIRONG_APPLY_LOAN_nbank_cf_orgnum_d15_id']=float(d15_id['nbank_cf_orgnum']) if 'nbank_cf_orgnum' in d15_id.keys() else np.nan
    dict_out['BAIRONG_APPLY_LOAN_nbank_sloan_orgnum_m01_id']=float(m01_id['nbank_sloan_orgnum']) if 'nbank_sloan_orgnum' in m01_id.keys() else np.nan
    dict_out['BAIRONG_APPLY_LOAN_nbank_ca_allnum_m03_id']=float(m03_id['nbank_ca_allnum']) if 'nbank_ca_allnum' in m03_id.keys() else np.nan
    dict_out['BAIRONG_APPLY_LOAN_bank_tot_mons_m03_id']=float(m03_id['bank_tot_mons']) if 'bank_tot_mons' in m03_id.keys() else np.nan
    dict_out['BAIRONG_APPLY_LOAN_nbank_oth_allnum_m12_id']=float(m12_id['nbank_oth_allnum']) if 'nbank_oth_allnum' in m12_id.keys() else np.nan
    dict_out['BAIRONG_APPLY_LOAN_nbank_cf_allnum_d07_id']=float(d07_id['nbank_cf_allnum']) if 'nbank_cf_allnum' in d07_id.keys() else np.nan
    dict_out['BAIRONG_APPLY_LOAN_nbank_night_orgnum_m06_cell']=float(m06_cell['nbank_night_orgnum']) if 'nbank_night_orgnum' in m06_cell.keys() else np.nan
    dict_out['BAIRONG_APPLY_LOAN_caoff_orgnum_m12_id']=float(m12_id['caoff_orgnum']) if 'caoff_orgnum' in m12_id.keys() else np.nan
    dict_out['BAIRONG_APPLY_LOAN_caoff_allnum_m12_cell']=float(m12_cell['caoff_allnum']) if 'caoff_allnum' in m12_cell.keys() else np.nan
    dict_out['BAIRONG_APPLY_LOAN_nbank_cf_orgnum_m03_id']=float(m03_id['nbank_cf_orgnum']) if 'nbank_cf_orgnum' in m03_id.keys() else np.nan
    dict_out['BAIRONG_APPLY_LOAN_bank_max_inteday_m12_cell']=float(m12_cell['bank_max_inteday']) if 'bank_max_inteday' in m12_cell.keys() else np.nan
    dict_out['BAIRONG_APPLY_LOAN_pdl_allnum_m06_cell']=float(m06_cell['pdl_allnum']) if 'pdl_allnum' in m06_cell.keys() else np.nan
    dict_out['BAIRONG_APPLY_LOAN_nbank_allnum_d07_id']=float(d07_id['nbank_allnum']) if 'nbank_allnum' in d07_id.keys() else np.nan
    dict_out['BAIRONG_APPLY_LOAN_nbank_mc_orgnum_m06_cell']=float(m06_cell['nbank_mc_orgnum']) if 'nbank_mc_orgnum' in m06_cell.keys() else np.nan
    dict_out['BAIRONG_APPLY_LOAN_af_orgnum_m03_id']=float(m03_id['af_orgnum']) if 'af_orgnum' in m03_id.keys() else np.nan
    dict_out['BAIRONG_APPLY_LOAN_af_allnum_m06_cell']=float(m06_cell['af_allnum']) if 'af_allnum' in m06_cell.keys() else np.nan
    dict_out['BAIRONG_APPLY_LOAN_pdl_allnum_m12_id']=float(m12_id['pdl_allnum']) if 'pdl_allnum' in m12_id.keys() else np.nan
    dict_out['BAIRONG_APPLY_LOAN_nbank_orgnum_m01_id']=float(m01_id['nbank_orgnum']) if 'nbank_orgnum' in m01_id.keys() else np.nan
    dict_out['BAIRONG_APPLY_LOAN_nbank_week_orgnum_m03_cell']=float(m03_cell['nbank_week_orgnum']) if 'nbank_week_orgnum' in m03_cell.keys() else np.nan
    dict_out['BAIRONG_APPLY_LOAN_bank_night_allnum_m06_id']=float(m06_id['bank_night_allnum']) if 'bank_night_allnum' in m06_id.keys() else np.nan
    dict_out['BAIRONG_APPLY_LOAN_bank_night_orgnum_m12_id']=float(m12_id['bank_night_orgnum']) if 'bank_night_orgnum' in m12_id.keys() else np.nan
    dict_out['BAIRONG_APPLY_LOAN_coon_allnum_m03_id']=float(m03_id['coon_allnum']) if 'coon_allnum' in m03_id.keys() else np.nan
    dict_out['BAIRONG_APPLY_LOAN_nbank_allnum_m03_cell']=float(m03_cell['nbank_allnum']) if 'nbank_allnum' in m03_cell.keys() else np.nan
    dict_out['BAIRONG_APPLY_LOAN_nbank_cons_orgnum_m01_id']=float(m01_id['nbank_cons_orgnum']) if 'nbank_cons_orgnum' in m01_id.keys() else np.nan
    dict_out['BAIRONG_APPLY_LOAN_caoff_allnum_m01_cell']=float(m01_cell['caoff_allnum']) if 'caoff_allnum' in m01_cell.keys() else np.nan
    dict_out['BAIRONG_APPLY_LOAN_nbank_ca_orgnum_m06_cell']=float(m06_cell['nbank_ca_orgnum']) if 'nbank_ca_orgnum' in m06_cell.keys() else np.nan
    dict_out['BAIRONG_APPLY_LOAN_bank_allnum_m01_id']=float(m01_id['bank_allnum']) if 'bank_allnum' in m01_id.keys() else np.nan
    dict_out['BAIRONG_APPLY_LOAN_nbank_sloan_orgnum_m12_id']=float(m12_id['nbank_sloan_orgnum']) if 'nbank_sloan_orgnum' in m12_id.keys() else np.nan
    dict_out['BAIRONG_APPLY_LOAN_bank_allnum_m06_cell']=float(m06_cell['bank_allnum']) if 'bank_allnum' in m06_cell.keys() else np.nan
    dict_out['BAIRONG_APPLY_LOAN_bank_orgnum_m01_cell']=float(m01_cell['bank_orgnum']) if 'bank_orgnum' in m01_cell.keys() else np.nan
    dict_out['BAIRONG_APPLY_LOAN_nbank_week_orgnum_m06_id']=float(m06_id['nbank_week_orgnum']) if 'nbank_week_orgnum' in m06_id.keys() else np.nan
    dict_out['BAIRONG_APPLY_LOAN_bank_avg_monnum_m06_cell']=float(m06_cell['bank_avg_monnum']) if 'bank_avg_monnum' in m06_cell.keys() else np.nan
    dict_out['BAIRONG_APPLY_LOAN_af_allnum_m06_id']=float(m06_id['af_allnum']) if 'af_allnum' in m06_id.keys() else np.nan
    dict_out['BAIRONG_APPLY_LOAN_nbank_cons_orgnum_m12_cell']=float(m12_cell['nbank_cons_orgnum']) if 'nbank_cons_orgnum' in m12_cell.keys() else np.nan
    dict_out['BAIRONG_APPLY_LOAN_caoff_allnum_m03_cell']=float(m03_cell['caoff_allnum']) if 'caoff_allnum' in m03_cell.keys() else np.nan
    dict_out['BAIRONG_APPLY_LOAN_caoff_allnum_m06_id']=float(m06_id['caoff_allnum']) if 'caoff_allnum' in m06_id.keys() else np.nan
    dict_out['BAIRONG_APPLY_LOAN_nbank_nsloan_orgnum_d15_id']=float(d15_id['nbank_nsloan_orgnum']) if 'nbank_nsloan_orgnum' in d15_id.keys() else np.nan
    dict_out['BAIRONG_APPLY_LOAN_coon_orgnum_m01_id']=float(m01_id['coon_orgnum']) if 'coon_orgnum' in m01_id.keys() else np.nan
    dict_out['BAIRONG_APPLY_LOAN_nbank_sloan_allnum_m06_id']=float(m06_id['nbank_sloan_allnum']) if 'nbank_sloan_allnum' in m06_id.keys() else np.nan
    dict_out['BAIRONG_APPLY_LOAN_nbank_oth_orgnum_m01_cell']=float(m01_cell['nbank_oth_orgnum']) if 'nbank_oth_orgnum' in m01_cell.keys() else np.nan
    dict_out['BAIRONG_APPLY_LOAN_rel_allnum_m06_cell']=float(m06_cell['rel_allnum']) if 'rel_allnum' in m06_cell.keys() else np.nan
    dict_out['BAIRONG_APPLY_LOAN_bank_tra_orgnum_d15_id']=float(d15_id['bank_tra_orgnum']) if 'bank_tra_orgnum' in d15_id.keys() else np.nan
    dict_out['BAIRONG_APPLY_LOAN_bank_allnum_m12_id']=float(m12_id['bank_allnum']) if 'bank_allnum' in m12_id.keys() else np.nan
    dict_out['BAIRONG_APPLY_LOAN_nbank_cons_allnum_d15_cell']=float(d15_cell['nbank_cons_allnum']) if 'nbank_cons_allnum' in d15_cell.keys() else np.nan
    dict_out['BAIRONG_APPLY_LOAN_nbank_sloan_allnum_m12_cell']=float(m12_cell['nbank_sloan_allnum']) if 'nbank_sloan_allnum' in m12_cell.keys() else np.nan
    dict_out['BAIRONG_APPLY_LOAN_nbank_else_allnum_m06_cell']=float(m06_cell['nbank_else_allnum']) if 'nbank_else_allnum' in m06_cell.keys() else np.nan
    dict_out['BAIRONG_APPLY_LOAN_pdl_orgnum_m06_cell']=float(m06_cell['pdl_orgnum']) if 'pdl_orgnum' in m06_cell.keys() else np.nan
    dict_out['BAIRONG_APPLY_LOAN_bank_avg_monnum_m12_id']=float(m12_id['bank_avg_monnum']) if 'bank_avg_monnum' in m12_id.keys() else np.nan
    dict_out['BAIRONG_APPLY_LOAN_af_orgnum_m01_cell']=float(m01_cell['af_orgnum']) if 'af_orgnum' in m01_cell.keys() else np.nan
    dict_out['BAIRONG_APPLY_LOAN_nbank_else_orgnum_m12_id']=float(m12_id['nbank_else_orgnum']) if 'nbank_else_orgnum' in m12_id.keys() else np.nan
    dict_out['BAIRONG_APPLY_LOAN_nbank_cons_allnum_m12_cell']=float(m12_cell['nbank_cons_allnum']) if 'nbank_cons_allnum' in m12_cell.keys() else np.nan
    dict_out['BAIRONG_APPLY_LOAN_nbank_sloan_orgnum_d15_id']=float(d15_id['nbank_sloan_orgnum']) if 'nbank_sloan_orgnum' in d15_id.keys() else np.nan
    dict_out['BAIRONG_APPLY_LOAN_nbank_p2p_allnum_m06_id']=float(m06_id['nbank_p2p_allnum']) if 'nbank_p2p_allnum' in m06_id.keys() else np.nan
    dict_out['BAIRONG_APPLY_LOAN_nbank_ca_orgnum_m01_cell']=float(m01_cell['nbank_ca_orgnum']) if 'nbank_ca_orgnum' in m01_cell.keys() else np.nan
    dict_out['BAIRONG_APPLY_LOAN_coon_allnum_m12_cell']=float(m12_cell['coon_allnum']) if 'coon_allnum' in m12_cell.keys() else np.nan
    dict_out['BAIRONG_APPLY_LOAN_nbank_week_orgnum_m03_id']=float(m03_id['nbank_week_orgnum']) if 'nbank_week_orgnum' in m03_id.keys() else np.nan
    dict_out['BAIRONG_APPLY_LOAN_coon_allnum_m01_cell']=float(m01_cell['coon_allnum']) if 'coon_allnum' in m01_cell.keys() else np.nan
    dict_out['BAIRONG_APPLY_LOAN_avg_monnum_m03_id']=float(m03_id['avg_monnum']) if 'avg_monnum' in m03_id.keys() else np.nan
    dict_out['BAIRONG_APPLY_LOAN_tot_mons_m12_cell']=float(m12_cell['tot_mons']) if 'tot_mons' in m12_cell.keys() else np.nan
    dict_out['BAIRONG_APPLY_LOAN_nbank_max_monnum_m12_cell']=float(m12_cell['nbank_max_monnum']) if 'nbank_max_monnum' in m12_cell.keys() else np.nan
    dict_out['BAIRONG_APPLY_LOAN_rel_orgnum_d07_cell']=float(d07_cell['rel_orgnum']) if 'rel_orgnum' in d07_cell.keys() else np.nan
    dict_out['BAIRONG_APPLY_LOAN_af_orgnum_m06_id']=float(m06_id['af_orgnum']) if 'af_orgnum' in m06_id.keys() else np.nan
    dict_out['BAIRONG_APPLY_LOAN_nbank_cons_orgnum_m06_id']=float(m06_id['nbank_cons_orgnum']) if 'nbank_cons_orgnum' in m06_id.keys() else np.nan
    dict_out['BAIRONG_APPLY_LOAN_bank_ret_orgnum_m06_id']=float(m06_id['bank_ret_orgnum']) if 'bank_ret_orgnum' in m06_id.keys() else np.nan
    dict_out['BAIRONG_APPLY_LOAN_nbank_ca_orgnum_d15_id']=float(d15_id['nbank_ca_orgnum']) if 'nbank_ca_orgnum' in d15_id.keys() else np.nan
    dict_out['BAIRONG_APPLY_LOAN_avg_monnum_m12_id']=float(m12_id['avg_monnum']) if 'avg_monnum' in m12_id.keys() else np.nan
    dict_out['BAIRONG_APPLY_LOAN_nbank_cf_allnum_m06_id']=float(m06_id['nbank_cf_allnum']) if 'nbank_cf_allnum' in m06_id.keys() else np.nan
    dict_out['BAIRONG_APPLY_LOAN_nbank_nsloan_orgnum_m03_cell']=float(m03_cell['nbank_nsloan_orgnum']) if 'nbank_nsloan_orgnum' in m03_cell.keys() else np.nan
    dict_out['BAIRONG_APPLY_LOAN_cooff_allnum_m06_cell']=float(m06_cell['cooff_allnum']) if 'cooff_allnum' in m06_cell.keys() else np.nan
    dict_out['BAIRONG_APPLY_LOAN_nbank_else_orgnum_m03_cell']=float(m03_cell['nbank_else_orgnum']) if 'nbank_else_orgnum' in m03_cell.keys() else np.nan
    dict_out['BAIRONG_APPLY_LOAN_nbank_ca_allnum_m01_cell']=float(m01_cell['nbank_ca_allnum']) if 'nbank_ca_allnum' in m01_cell.keys() else np.nan
    dict_out['BAIRONG_APPLY_LOAN_bank_tot_mons_m03_cell']=float(m03_cell['bank_tot_mons']) if 'bank_tot_mons' in m03_cell.keys() else np.nan
    dict_out['BAIRONG_APPLY_LOAN_nbank_night_allnum_m12_id']=float(m12_id['nbank_night_allnum']) if 'nbank_night_allnum' in m12_id.keys() else np.nan
    dict_out['BAIRONG_APPLY_LOAN_nbank_cf_orgnum_m12_id']=float(m12_id['nbank_cf_orgnum']) if 'nbank_cf_orgnum' in m12_id.keys() else np.nan
    dict_out['BAIRONG_APPLY_LOAN_nbank_else_allnum_m03_id']=float(m03_id['nbank_else_allnum']) if 'nbank_else_allnum' in m03_id.keys() else np.nan
    dict_out['BAIRONG_APPLY_LOAN_nbank_p2p_allnum_m12_cell']=float(m12_cell['nbank_p2p_allnum']) if 'nbank_p2p_allnum' in m12_cell.keys() else np.nan
    dict_out['BAIRONG_APPLY_LOAN_oth_orgnum_m06_cell']=float(m06_cell['oth_orgnum']) if 'oth_orgnum' in m06_cell.keys() else np.nan
    dict_out['BAIRONG_APPLY_LOAN_oth_orgnum_d15_cell']=float(d15_cell['oth_orgnum']) if 'oth_orgnum' in d15_cell.keys() else np.nan
    dict_out['BAIRONG_APPLY_LOAN_coon_allnum_m06_id']=float(m06_id['coon_allnum']) if 'coon_allnum' in m06_id.keys() else np.nan
    dict_out['BAIRONG_APPLY_LOAN_tot_mons_m06_id']=float(m06_id['tot_mons']) if 'tot_mons' in m06_id.keys() else np.nan
    dict_out['BAIRONG_APPLY_LOAN_nbank_cons_orgnum_m12_id']=float(m12_id['nbank_cons_orgnum']) if 'nbank_cons_orgnum' in m12_id.keys() else np.nan
    dict_out['BAIRONG_APPLY_LOAN_nbank_night_allnum_m12_cell']=float(m12_cell['nbank_night_allnum']) if 'nbank_night_allnum' in m12_cell.keys() else np.nan
    dict_out['BAIRONG_APPLY_LOAN_bank_orgnum_m01_id']=float(m01_id['bank_orgnum']) if 'bank_orgnum' in m01_id.keys() else np.nan
    dict_out['BAIRONG_APPLY_LOAN_nbank_max_monnum_m12_id']=float(m12_id['nbank_max_monnum']) if 'nbank_max_monnum' in m12_id.keys() else np.nan
    dict_out['BAIRONG_APPLY_LOAN_cooff_orgnum_m06_id']=float(m06_id['cooff_orgnum']) if 'cooff_orgnum' in m06_id.keys() else np.nan
    dict_out['BAIRONG_APPLY_LOAN_min_monnum_m06_cell']=float(m06_cell['min_monnum']) if 'min_monnum' in m06_cell.keys() else np.nan
    dict_out['BAIRONG_APPLY_LOAN_af_allnum_m03_id']=float(m03_id['af_allnum']) if 'af_allnum' in m03_id.keys() else np.nan
    dict_out['BAIRONG_APPLY_LOAN_oth_orgnum_d07_id']=float(d07_id['oth_orgnum']) if 'oth_orgnum' in d07_id.keys() else np.nan
    dict_out['BAIRONG_APPLY_LOAN_nbank_cons_allnum_m01_id']=float(m01_id['nbank_cons_allnum']) if 'nbank_cons_allnum' in m01_id.keys() else np.nan
    dict_out['BAIRONG_APPLY_LOAN_nbank_nsloan_orgnum_m12_id']=float(m12_id['nbank_nsloan_orgnum']) if 'nbank_nsloan_orgnum' in m12_id.keys() else np.nan
    dict_out['BAIRONG_APPLY_LOAN_caon_orgnum_m12_cell']=float(m12_cell['caon_orgnum']) if 'caon_orgnum' in m12_cell.keys() else np.nan
    dict_out['BAIRONG_APPLY_LOAN_bank_ret_allnum_m12_cell']=float(m12_cell['bank_ret_allnum']) if 'bank_ret_allnum' in m12_cell.keys() else np.nan
    dict_out['BAIRONG_APPLY_LOAN_bank_week_orgnum_m12_id']=float(m12_id['bank_week_orgnum']) if 'bank_week_orgnum' in m12_id.keys() else np.nan
    dict_out['BAIRONG_APPLY_LOAN_nbank_night_allnum_d07_id']=float(d07_id['nbank_night_allnum']) if 'nbank_night_allnum' in d07_id.keys() else np.nan
    dict_out['BAIRONG_APPLY_LOAN_nbank_nsloan_orgnum_m01_id']=float(m01_id['nbank_nsloan_orgnum']) if 'nbank_nsloan_orgnum' in m01_id.keys() else np.nan
    dict_out['BAIRONG_APPLY_LOAN_bank_week_orgnum_m03_cell']=float(m03_cell['bank_week_orgnum']) if 'bank_week_orgnum' in m03_cell.keys() else np.nan
    dict_out['BAIRONG_APPLY_LOAN_rel_orgnum_m12_cell']=float(m12_cell['rel_orgnum']) if 'rel_orgnum' in m12_cell.keys() else np.nan
    dict_out['BAIRONG_APPLY_LOAN_nbank_p2p_allnum_m03_id']=float(m03_id['nbank_p2p_allnum']) if 'nbank_p2p_allnum' in m03_id.keys() else np.nan
    dict_out['BAIRONG_APPLY_LOAN_caoff_allnum_d15_cell']=float(d15_cell['caoff_allnum']) if 'caoff_allnum' in d15_cell.keys() else np.nan
    dict_out['BAIRONG_APPLY_LOAN_caoff_orgnum_d07_cell']=float(d07_cell['caoff_orgnum']) if 'caoff_orgnum' in d07_cell.keys() else np.nan
    dict_out['BAIRONG_APPLY_LOAN_nbank_week_orgnum_d15_cell']=float(d15_cell['nbank_week_orgnum']) if 'nbank_week_orgnum' in d15_cell.keys() else np.nan
    dict_out['BAIRONG_APPLY_LOAN_nbank_ca_allnum_m01_id']=float(m01_id['nbank_ca_allnum']) if 'nbank_ca_allnum' in m01_id.keys() else np.nan
    dict_out['BAIRONG_APPLY_LOAN_nbank_max_inteday_m06_id']=float(m06_id['nbank_max_inteday']) if 'nbank_max_inteday' in m06_id.keys() else np.nan
    dict_out['BAIRONG_APPLY_LOAN_nbank_finlea_orgnum_m12_cell']=float(m12_cell['nbank_finlea_orgnum']) if 'nbank_finlea_orgnum' in m12_cell.keys() else np.nan
    dict_out['BAIRONG_APPLY_LOAN_nbank_sloan_allnum_m01_cell']=float(m01_cell['nbank_sloan_allnum']) if 'nbank_sloan_allnum' in m01_cell.keys() else np.nan
    dict_out['BAIRONG_APPLY_LOAN_nbank_nsloan_orgnum_m03_id']=float(m03_id['nbank_nsloan_orgnum']) if 'nbank_nsloan_orgnum' in m03_id.keys() else np.nan
    dict_out['BAIRONG_APPLY_LOAN_nbank_allnum_m03_id']=float(m03_id['nbank_allnum']) if 'nbank_allnum' in m03_id.keys() else np.nan
    dict_out['BAIRONG_APPLY_LOAN_caon_orgnum_d07_id']=float(d07_id['caon_orgnum']) if 'caon_orgnum' in d07_id.keys() else np.nan
    dict_out['BAIRONG_APPLY_LOAN_nbank_allnum_m01_cell']=float(m01_cell['nbank_allnum']) if 'nbank_allnum' in m01_cell.keys() else np.nan
    dict_out['BAIRONG_APPLY_LOAN_nbank_sloan_orgnum_d15_cell']=float(d15_cell['nbank_sloan_orgnum']) if 'nbank_sloan_orgnum' in d15_cell.keys() else np.nan
    dict_out['BAIRONG_APPLY_LOAN_pdl_allnum_d07_id']=float(d07_id['pdl_allnum']) if 'pdl_allnum' in d07_id.keys() else np.nan
    dict_out['BAIRONG_APPLY_LOAN_bank_avg_monnum_m12_cell']=float(m12_cell['bank_avg_monnum']) if 'bank_avg_monnum' in m12_cell.keys() else np.nan
    dict_out['BAIRONG_APPLY_LOAN_nbank_mc_allnum_m03_id']=float(m03_id['nbank_mc_allnum']) if 'nbank_mc_allnum' in m03_id.keys() else np.nan
    dict_out['BAIRONG_APPLY_LOAN_caon_orgnum_d15_id']=float(d15_id['caon_orgnum']) if 'caon_orgnum' in d15_id.keys() else np.nan
    dict_out['BAIRONG_APPLY_LOAN_nbank_avg_monnum_m12_id']=float(m12_id['nbank_avg_monnum']) if 'nbank_avg_monnum' in m12_id.keys() else np.nan
    dict_out['BAIRONG_APPLY_LOAN_nbank_cf_allnum_m01_cell']=float(m01_cell['nbank_cf_allnum']) if 'nbank_cf_allnum' in m01_cell.keys() else np.nan
    dict_out['BAIRONG_APPLY_LOAN_nbank_nsloan_allnum_m01_id']=float(m01_id['nbank_nsloan_allnum']) if 'nbank_nsloan_allnum' in m01_id.keys() else np.nan
    dict_out['BAIRONG_APPLY_LOAN_nbank_sloan_allnum_d15_cell']=float(d15_cell['nbank_sloan_allnum']) if 'nbank_sloan_allnum' in d15_cell.keys() else np.nan
    dict_out['BAIRONG_APPLY_LOAN_avg_monnum_m06_cell']=float(m06_cell['avg_monnum']) if 'avg_monnum' in m06_cell.keys() else np.nan
    dict_out['BAIRONG_APPLY_LOAN_nbank_night_orgnum_m03_id']=float(m03_id['nbank_night_orgnum']) if 'nbank_night_orgnum' in m03_id.keys() else np.nan
    dict_out['BAIRONG_APPLY_LOAN_nbank_oth_orgnum_d07_id']=float(d07_id['nbank_oth_orgnum']) if 'nbank_oth_orgnum' in d07_id.keys() else np.nan
    dict_out['BAIRONG_APPLY_LOAN_coon_allnum_m03_cell']=float(m03_cell['coon_allnum']) if 'coon_allnum' in m03_cell.keys() else np.nan
    dict_out['BAIRONG_APPLY_LOAN_af_orgnum_m12_cell']=float(m12_cell['af_orgnum']) if 'af_orgnum' in m12_cell.keys() else np.nan
    dict_out['BAIRONG_APPLY_LOAN_nbank_night_allnum_m06_cell']=float(m06_cell['nbank_night_allnum']) if 'nbank_night_allnum' in m06_cell.keys() else np.nan
    dict_out['BAIRONG_APPLY_LOAN_bank_night_allnum_m01_id']=float(m01_id['bank_night_allnum']) if 'bank_night_allnum' in m01_id.keys() else np.nan
    dict_out['BAIRONG_APPLY_LOAN_bank_tot_mons_m12_id']=float(m12_id['bank_tot_mons']) if 'bank_tot_mons' in m12_id.keys() else np.nan
    dict_out['BAIRONG_APPLY_LOAN_bank_orgnum_m06_cell']=float(m06_cell['bank_orgnum']) if 'bank_orgnum' in m06_cell.keys() else np.nan
    dict_out['BAIRONG_APPLY_LOAN_bank_tra_allnum_m12_id']=float(m12_id['bank_tra_allnum']) if 'bank_tra_allnum' in m12_id.keys() else np.nan
    dict_out['BAIRONG_APPLY_LOAN_nbank_min_monnum_m03_id']=float(m03_id['nbank_min_monnum']) if 'nbank_min_monnum' in m03_id.keys() else np.nan
    dict_out['BAIRONG_APPLY_LOAN_af_allnum_m12_cell']=float(m12_cell['af_allnum']) if 'af_allnum' in m12_cell.keys() else np.nan
    dict_out['BAIRONG_APPLY_LOAN_coon_orgnum_m06_id']=float(m06_id['coon_orgnum']) if 'coon_orgnum' in m06_id.keys() else np.nan
    dict_out['BAIRONG_APPLY_LOAN_nbank_nsloan_orgnum_m06_cell']=float(m06_cell['nbank_nsloan_orgnum']) if 'nbank_nsloan_orgnum' in m06_cell.keys() else np.nan
    dict_out['BAIRONG_APPLY_LOAN_rel_allnum_d15_cell']=float(d15_cell['rel_allnum']) if 'rel_allnum' in d15_cell.keys() else np.nan
    dict_out['BAIRONG_APPLY_LOAN_pdl_orgnum_m03_cell']=float(m03_cell['pdl_orgnum']) if 'pdl_orgnum' in m03_cell.keys() else np.nan
    dict_out['BAIRONG_APPLY_LOAN_nbank_cons_orgnum_m03_cell']=float(m03_cell['nbank_cons_orgnum']) if 'nbank_cons_orgnum' in m03_cell.keys() else np.nan
    dict_out['BAIRONG_APPLY_LOAN_pdl_orgnum_d15_id']=float(d15_id['pdl_orgnum']) if 'pdl_orgnum' in d15_id.keys() else np.nan
    dict_out['BAIRONG_APPLY_LOAN_nbank_night_orgnum_m12_id']=float(m12_id['nbank_night_orgnum']) if 'nbank_night_orgnum' in m12_id.keys() else np.nan
    dict_out['BAIRONG_APPLY_LOAN_nbank_sloan_orgnum_d07_cell']=float(d07_cell['nbank_sloan_orgnum']) if 'nbank_sloan_orgnum' in d07_cell.keys() else np.nan
    dict_out['BAIRONG_APPLY_LOAN_nbank_oth_orgnum_m01_id']=float(m01_id['nbank_oth_orgnum']) if 'nbank_oth_orgnum' in m01_id.keys() else np.nan
    dict_out['BAIRONG_APPLY_LOAN_nbank_tot_mons_m03_cell']=float(m03_cell['nbank_tot_mons']) if 'nbank_tot_mons' in m03_cell.keys() else np.nan
    dict_out['BAIRONG_APPLY_LOAN_nbank_finlea_allnum_m01_id']=float(m01_id['nbank_finlea_allnum']) if 'nbank_finlea_allnum' in m01_id.keys() else np.nan
    dict_out['BAIRONG_APPLY_LOAN_caon_allnum_d07_id']=float(d07_id['caon_allnum']) if 'caon_allnum' in d07_id.keys() else np.nan
    dict_out['BAIRONG_APPLY_LOAN_nbank_avg_monnum_m06_cell']=float(m06_cell['nbank_avg_monnum']) if 'nbank_avg_monnum' in m06_cell.keys() else np.nan
    dict_out['BAIRONG_APPLY_LOAN_nbank_cf_allnum_d07_cell']=float(d07_cell['nbank_cf_allnum']) if 'nbank_cf_allnum' in d07_cell.keys() else np.nan
    dict_out['BAIRONG_APPLY_LOAN_nbank_week_allnum_d07_id']=float(d07_id['nbank_week_allnum']) if 'nbank_week_allnum' in d07_id.keys() else np.nan
    dict_out['BAIRONG_APPLY_LOAN_oth_orgnum_m06_id']=float(m06_id['oth_orgnum']) if 'oth_orgnum' in m06_id.keys() else np.nan
    dict_out['BAIRONG_APPLY_LOAN_nbank_nsloan_orgnum_d07_id']=float(d07_id['nbank_nsloan_orgnum']) if 'nbank_nsloan_orgnum' in d07_id.keys() else np.nan
    dict_out['BAIRONG_APPLY_LOAN_nbank_night_orgnum_d15_id']=float(d15_id['nbank_night_orgnum']) if 'nbank_night_orgnum' in d15_id.keys() else np.nan
    dict_out['BAIRONG_APPLY_LOAN_caon_allnum_d07_cell']=float(d07_cell['caon_allnum']) if 'caon_allnum' in d07_cell.keys() else np.nan
    dict_out['BAIRONG_APPLY_LOAN_bank_avg_monnum_m03_cell']=float(m03_cell['bank_avg_monnum']) if 'bank_avg_monnum' in m03_cell.keys() else np.nan
    dict_out['BAIRONG_APPLY_LOAN_nbank_cf_allnum_m03_id']=float(m03_id['nbank_cf_allnum']) if 'nbank_cf_allnum' in m03_id.keys() else np.nan
    dict_out['BAIRONG_APPLY_LOAN_nbank_else_orgnum_d07_id']=float(d07_id['nbank_else_orgnum']) if 'nbank_else_orgnum' in d07_id.keys() else np.nan
    dict_out['BAIRONG_APPLY_LOAN_nbank_finlea_allnum_m03_cell']=float(m03_cell['nbank_finlea_allnum']) if 'nbank_finlea_allnum' in m03_cell.keys() else np.nan
    dict_out['BAIRONG_APPLY_LOAN_oth_allnum_m12_cell']=float(m12_cell['oth_allnum']) if 'oth_allnum' in m12_cell.keys() else np.nan
    dict_out['BAIRONG_APPLY_LOAN_pdl_orgnum_d07_cell']=float(d07_cell['pdl_orgnum']) if 'pdl_orgnum' in d07_cell.keys() else np.nan
    dict_out['BAIRONG_APPLY_LOAN_bank_ret_orgnum_m03_id']=float(m03_id['bank_ret_orgnum']) if 'bank_ret_orgnum' in m03_id.keys() else np.nan
    dict_out['BAIRONG_APPLY_LOAN_nbank_oth_allnum_d15_id']=float(d15_id['nbank_oth_allnum']) if 'nbank_oth_allnum' in d15_id.keys() else np.nan
    dict_out['BAIRONG_APPLY_LOAN_nbank_sloan_allnum_m12_id']=float(m12_id['nbank_sloan_allnum']) if 'nbank_sloan_allnum' in m12_id.keys() else np.nan
    dict_out['BAIRONG_APPLY_LOAN_nbank_p2p_allnum_m03_cell']=float(m03_cell['nbank_p2p_allnum']) if 'nbank_p2p_allnum' in m03_cell.keys() else np.nan
    dict_out['BAIRONG_APPLY_LOAN_caoff_allnum_d15_id']=float(d15_id['caoff_allnum']) if 'caoff_allnum' in d15_id.keys() else np.nan
    dict_out['BAIRONG_APPLY_LOAN_nbank_allnum_d07_cell']=float(d07_cell['nbank_allnum']) if 'nbank_allnum' in d07_cell.keys() else np.nan
    dict_out['BAIRONG_APPLY_LOAN_nbank_min_monnum_m12_cell']=float(m12_cell['nbank_min_monnum']) if 'nbank_min_monnum' in m12_cell.keys() else np.nan
    dict_out['BAIRONG_APPLY_LOAN_nbank_week_allnum_m12_id']=float(m12_id['nbank_week_allnum']) if 'nbank_week_allnum' in m12_id.keys() else np.nan
    dict_out['BAIRONG_APPLY_LOAN_nbank_orgnum_m03_id']=float(m03_id['nbank_orgnum']) if 'nbank_orgnum' in m03_id.keys() else np.nan
    dict_out['BAIRONG_APPLY_LOAN_bank_tra_orgnum_m03_id']=float(m03_id['bank_tra_orgnum']) if 'bank_tra_orgnum' in m03_id.keys() else np.nan
    dict_out['BAIRONG_APPLY_LOAN_nbank_nsloan_allnum_m01_cell']=float(m01_cell['nbank_nsloan_allnum']) if 'nbank_nsloan_allnum' in m01_cell.keys() else np.nan
    dict_out['BAIRONG_APPLY_LOAN_nbank_nsloan_allnum_d07_cell']=float(d07_cell['nbank_nsloan_allnum']) if 'nbank_nsloan_allnum' in d07_cell.keys() else np.nan
    dict_out['BAIRONG_APPLY_LOAN_caon_orgnum_m01_cell']=float(m01_cell['caon_orgnum']) if 'caon_orgnum' in m01_cell.keys() else np.nan
    dict_out['BAIRONG_APPLY_LOAN_nbank_week_orgnum_m12_cell']=float(m12_cell['nbank_week_orgnum']) if 'nbank_week_orgnum' in m12_cell.keys() else np.nan
    dict_out['BAIRONG_APPLY_LOAN_caoff_orgnum_d15_cell']=float(d15_cell['caoff_orgnum']) if 'caoff_orgnum' in d15_cell.keys() else np.nan
    dict_out['BAIRONG_APPLY_LOAN_nbank_allnum_m12_id']=float(m12_id['nbank_allnum']) if 'nbank_allnum' in m12_id.keys() else np.nan
    dict_out['BAIRONG_APPLY_LOAN_nbank_cf_orgnum_m06_cell']=float(m06_cell['nbank_cf_orgnum']) if 'nbank_cf_orgnum' in m06_cell.keys() else np.nan
    dict_out['BAIRONG_APPLY_LOAN_nbank_nsloan_orgnum_m01_cell']=float(m01_cell['nbank_nsloan_orgnum']) if 'nbank_nsloan_orgnum' in m01_cell.keys() else np.nan
    dict_out['BAIRONG_APPLY_LOAN_nbank_cons_orgnum_d15_cell']=float(d15_cell['nbank_cons_orgnum']) if 'nbank_cons_orgnum' in d15_cell.keys() else np.nan
    dict_out['BAIRONG_APPLY_LOAN_nbank_orgnum_d15_id']=float(d15_id['nbank_orgnum']) if 'nbank_orgnum' in d15_id.keys() else np.nan
    dict_out['BAIRONG_APPLY_LOAN_nbank_finlea_allnum_d15_id']=float(d15_id['nbank_finlea_allnum']) if 'nbank_finlea_allnum' in d15_id.keys() else np.nan
    dict_out['BAIRONG_APPLY_LOAN_nbank_nsloan_orgnum_m06_id']=float(m06_id['nbank_nsloan_orgnum']) if 'nbank_nsloan_orgnum' in m06_id.keys() else np.nan
    dict_out['BAIRONG_APPLY_LOAN_nbank_night_orgnum_m01_id']=float(m01_id['nbank_night_orgnum']) if 'nbank_night_orgnum' in m01_id.keys() else np.nan
    dict_out['BAIRONG_APPLY_LOAN_bank_tra_orgnum_d15_cell']=float(d15_cell['bank_tra_orgnum']) if 'bank_tra_orgnum' in d15_cell.keys() else np.nan
    dict_out['BAIRONG_APPLY_LOAN_nbank_ca_allnum_d15_cell']=float(d15_cell['nbank_ca_allnum']) if 'nbank_ca_allnum' in d15_cell.keys() else np.nan
    dict_out['BAIRONG_APPLY_LOAN_avg_monnum_m03_cell']=float(m03_cell['avg_monnum']) if 'avg_monnum' in m03_cell.keys() else np.nan
    dict_out['BAIRONG_APPLY_LOAN_caon_allnum_m03_id']=float(m03_id['caon_allnum']) if 'caon_allnum' in m03_id.keys() else np.nan
    dict_out['BAIRONG_APPLY_LOAN_coon_allnum_d15_id']=float(d15_id['coon_allnum']) if 'coon_allnum' in d15_id.keys() else np.nan
    dict_out['BAIRONG_APPLY_LOAN_af_allnum_m12_id']=float(m12_id['af_allnum']) if 'af_allnum' in m12_id.keys() else np.nan
    dict_out['BAIRONG_APPLY_LOAN_oth_orgnum_d15_id']=float(d15_id['oth_orgnum']) if 'oth_orgnum' in d15_id.keys() else np.nan
    dict_out['BAIRONG_APPLY_LOAN_bank_tra_allnum_d07_id']=float(d07_id['bank_tra_allnum']) if 'bank_tra_allnum' in d07_id.keys() else np.nan
    dict_out['BAIRONG_APPLY_LOAN_nbank_cons_allnum_m01_cell']=float(m01_cell['nbank_cons_allnum']) if 'nbank_cons_allnum' in m01_cell.keys() else np.nan
    dict_out['BAIRONG_APPLY_LOAN_cooff_allnum_m01_cell']=float(m01_cell['cooff_allnum']) if 'cooff_allnum' in m01_cell.keys() else np.nan
    dict_out['BAIRONG_APPLY_LOAN_oth_allnum_d15_cell']=float(d15_cell['oth_allnum']) if 'oth_allnum' in d15_cell.keys() else np.nan
    dict_out['BAIRONG_APPLY_LOAN_cooff_orgnum_m01_cell']=float(m01_cell['cooff_orgnum']) if 'cooff_orgnum' in m01_cell.keys() else np.nan
    dict_out['BAIRONG_APPLY_LOAN_nbank_cons_allnum_d07_id']=float(d07_id['nbank_cons_allnum']) if 'nbank_cons_allnum' in d07_id.keys() else np.nan
    dict_out['BAIRONG_APPLY_LOAN_min_inteday_m06_id']=float(m06_id['min_inteday']) if 'min_inteday' in m06_id.keys() else np.nan
    dict_out['BAIRONG_APPLY_LOAN_nbank_ca_allnum_m06_cell']=float(m06_cell['nbank_ca_allnum']) if 'nbank_ca_allnum' in m06_cell.keys() else np.nan
    dict_out['BAIRONG_APPLY_LOAN_nbank_nsloan_orgnum_d07_cell']=float(d07_cell['nbank_nsloan_orgnum']) if 'nbank_nsloan_orgnum' in d07_cell.keys() else np.nan
    dict_out['BAIRONG_APPLY_LOAN_rel_allnum_m06_id']=float(m06_id['rel_allnum']) if 'rel_allnum' in m06_id.keys() else np.nan
    dict_out['BAIRONG_APPLY_LOAN_nbank_cons_orgnum_m06_cell']=float(m06_cell['nbank_cons_orgnum']) if 'nbank_cons_orgnum' in m06_cell.keys() else np.nan
    dict_out['BAIRONG_APPLY_LOAN_nbank_week_orgnum_m01_id']=float(m01_id['nbank_week_orgnum']) if 'nbank_week_orgnum' in m01_id.keys() else np.nan
    dict_out['BAIRONG_APPLY_LOAN_bank_night_orgnum_m12_cell']=float(m12_cell['bank_night_orgnum']) if 'bank_night_orgnum' in m12_cell.keys() else np.nan
    dict_out['BAIRONG_APPLY_LOAN_nbank_max_inteday_m12_id']=float(m12_id['nbank_max_inteday']) if 'nbank_max_inteday' in m12_id.keys() else np.nan
    dict_out['BAIRONG_APPLY_LOAN_nbank_oth_orgnum_m06_cell']=float(m06_cell['nbank_oth_orgnum']) if 'nbank_oth_orgnum' in m06_cell.keys() else np.nan
    dict_out['BAIRONG_APPLY_LOAN_cooff_orgnum_m12_cell']=float(m12_cell['cooff_orgnum']) if 'cooff_orgnum' in m12_cell.keys() else np.nan
    dict_out['BAIRONG_APPLY_LOAN_bank_tra_allnum_m03_id']=float(m03_id['bank_tra_allnum']) if 'bank_tra_allnum' in m03_id.keys() else np.nan
    dict_out['BAIRONG_APPLY_LOAN_nbank_oth_allnum_m03_id']=float(m03_id['nbank_oth_allnum']) if 'nbank_oth_allnum' in m03_id.keys() else np.nan
    dict_out['BAIRONG_APPLY_LOAN_nbank_avg_monnum_m12_cell']=float(m12_cell['nbank_avg_monnum']) if 'nbank_avg_monnum' in m12_cell.keys() else np.nan
    dict_out['BAIRONG_APPLY_LOAN_bank_tra_orgnum_m06_id']=float(m06_id['bank_tra_orgnum']) if 'bank_tra_orgnum' in m06_id.keys() else np.nan
    dict_out['BAIRONG_APPLY_LOAN_max_monnum_m03_id']=float(m03_id['max_monnum']) if 'max_monnum' in m03_id.keys() else np.nan
    dict_out['BAIRONG_APPLY_LOAN_bank_tra_allnum_m01_id']=float(m01_id['bank_tra_allnum']) if 'bank_tra_allnum' in m01_id.keys() else np.nan
    dict_out['BAIRONG_APPLY_LOAN_bank_orgnum_d15_id']=float(d15_id['bank_orgnum']) if 'bank_orgnum' in d15_id.keys() else np.nan
    dict_out['BAIRONG_APPLY_LOAN_bank_ret_allnum_m03_cell']=float(m03_cell['bank_ret_allnum']) if 'bank_ret_allnum' in m03_cell.keys() else np.nan
    dict_out['BAIRONG_APPLY_LOAN_nbank_oth_allnum_d07_cell']=float(d07_cell['nbank_oth_allnum']) if 'nbank_oth_allnum' in d07_cell.keys() else np.nan
    dict_out['BAIRONG_APPLY_LOAN_nbank_cf_orgnum_m01_id']=float(m01_id['nbank_cf_orgnum']) if 'nbank_cf_orgnum' in m01_id.keys() else np.nan
    dict_out['BAIRONG_APPLY_LOAN_nbank_else_allnum_m01_cell']=float(m01_cell['nbank_else_allnum']) if 'nbank_else_allnum' in m01_cell.keys() else np.nan
    dict_out['BAIRONG_APPLY_LOAN_nbank_oth_orgnum_m12_id']=float(m12_id['nbank_oth_orgnum']) if 'nbank_oth_orgnum' in m12_id.keys() else np.nan
    dict_out['BAIRONG_APPLY_LOAN_max_inteday_m12_cell']=float(m12_cell['max_inteday']) if 'max_inteday' in m12_cell.keys() else np.nan
    dict_out['BAIRONG_APPLY_LOAN_bank_ret_allnum_m06_id']=float(m06_id['bank_ret_allnum']) if 'bank_ret_allnum' in m06_id.keys() else np.nan
    dict_out['BAIRONG_APPLY_LOAN_min_inteday_m12_id']=float(m12_id['min_inteday']) if 'min_inteday' in m12_id.keys() else np.nan
    dict_out['BAIRONG_APPLY_LOAN_bank_allnum_m01_cell']=float(m01_cell['bank_allnum']) if 'bank_allnum' in m01_cell.keys() else np.nan
    dict_out['BAIRONG_APPLY_LOAN_nbank_orgnum_d07_cell']=float(d07_cell['nbank_orgnum']) if 'nbank_orgnum' in d07_cell.keys() else np.nan
    dict_out['BAIRONG_APPLY_LOAN_nbank_min_monnum_m06_cell']=float(m06_cell['nbank_min_monnum']) if 'nbank_min_monnum' in m06_cell.keys() else np.nan
    dict_out['BAIRONG_APPLY_LOAN_bank_night_allnum_m06_cell']=float(m06_cell['bank_night_allnum']) if 'bank_night_allnum' in m06_cell.keys() else np.nan
    dict_out['BAIRONG_APPLY_LOAN_nbank_p2p_orgnum_m06_id']=float(m06_id['nbank_p2p_orgnum']) if 'nbank_p2p_orgnum' in m06_id.keys() else np.nan
    dict_out['BAIRONG_APPLY_LOAN_nbank_night_allnum_m03_id']=float(m03_id['nbank_night_allnum']) if 'nbank_night_allnum' in m03_id.keys() else np.nan
    dict_out['BAIRONG_APPLY_LOAN_max_monnum_m06_cell']=float(m06_cell['max_monnum']) if 'max_monnum' in m06_cell.keys() else np.nan
    dict_out['BAIRONG_APPLY_LOAN_nbank_oth_allnum_m06_cell']=float(m06_cell['nbank_oth_allnum']) if 'nbank_oth_allnum' in m06_cell.keys() else np.nan
    dict_out['BAIRONG_APPLY_LOAN_nbank_min_inteday_m12_id']=float(m12_id['nbank_min_inteday']) if 'nbank_min_inteday' in m12_id.keys() else np.nan
    dict_out['BAIRONG_APPLY_LOAN_bank_orgnum_d07_id']=float(d07_id['bank_orgnum']) if 'bank_orgnum' in d07_id.keys() else np.nan
    dict_out['BAIRONG_APPLY_LOAN_rel_orgnum_m06_cell']=float(m06_cell['rel_orgnum']) if 'rel_orgnum' in m06_cell.keys() else np.nan
    dict_out['BAIRONG_APPLY_LOAN_rel_allnum_m01_id']=float(m01_id['rel_allnum']) if 'rel_allnum' in m01_id.keys() else np.nan
    dict_out['BAIRONG_APPLY_LOAN_nbank_ca_orgnum_d15_cell']=float(d15_cell['nbank_ca_orgnum']) if 'nbank_ca_orgnum' in d15_cell.keys() else np.nan
    dict_out['BAIRONG_APPLY_LOAN_bank_min_inteday_m12_id']=float(m12_id['bank_min_inteday']) if 'bank_min_inteday' in m12_id.keys() else np.nan
    dict_out['BAIRONG_APPLY_LOAN_pdl_allnum_m03_cell']=float(m03_cell['pdl_allnum']) if 'pdl_allnum' in m03_cell.keys() else np.nan
    dict_out['BAIRONG_APPLY_LOAN_nbank_ca_allnum_m03_cell']=float(m03_cell['nbank_ca_allnum']) if 'nbank_ca_allnum' in m03_cell.keys() else np.nan
    dict_out['BAIRONG_APPLY_LOAN_nbank_p2p_allnum_m01_id']=float(m01_id['nbank_p2p_allnum']) if 'nbank_p2p_allnum' in m01_id.keys() else np.nan
    dict_out['BAIRONG_APPLY_LOAN_nbank_week_allnum_d15_cell']=float(d15_cell['nbank_week_allnum']) if 'nbank_week_allnum' in d15_cell.keys() else np.nan
    dict_out['BAIRONG_APPLY_LOAN_rel_allnum_m01_cell']=float(m01_cell['rel_allnum']) if 'rel_allnum' in m01_cell.keys() else np.nan
    dict_out['BAIRONG_APPLY_LOAN_tot_mons_m03_cell']=float(m03_cell['tot_mons']) if 'tot_mons' in m03_cell.keys() else np.nan
    dict_out['BAIRONG_APPLY_LOAN_nbank_ca_orgnum_m12_cell']=float(m12_cell['nbank_ca_orgnum']) if 'nbank_ca_orgnum' in m12_cell.keys() else np.nan
    dict_out['BAIRONG_APPLY_LOAN_nbank_nsloan_allnum_m06_cell']=float(m06_cell['nbank_nsloan_allnum']) if 'nbank_nsloan_allnum' in m06_cell.keys() else np.nan
    dict_out['BAIRONG_APPLY_LOAN_bank_tra_orgnum_m03_cell']=float(m03_cell['bank_tra_orgnum']) if 'bank_tra_orgnum' in m03_cell.keys() else np.nan
    dict_out['BAIRONG_APPLY_LOAN_nbank_oth_allnum_m06_id']=float(m06_id['nbank_oth_allnum']) if 'nbank_oth_allnum' in m06_id.keys() else np.nan
    dict_out['BAIRONG_APPLY_LOAN_nbank_nsloan_allnum_m03_id']=float(m03_id['nbank_nsloan_allnum']) if 'nbank_nsloan_allnum' in m03_id.keys() else np.nan
    dict_out['BAIRONG_APPLY_LOAN_caon_orgnum_m01_id']=float(m01_id['caon_orgnum']) if 'caon_orgnum' in m01_id.keys() else np.nan
    dict_out['BAIRONG_APPLY_LOAN_nbank_oth_allnum_m01_id']=float(m01_id['nbank_oth_allnum']) if 'nbank_oth_allnum' in m01_id.keys() else np.nan
    dict_out['BAIRONG_APPLY_LOAN_caoff_orgnum_m01_id']=float(m01_id['caoff_orgnum']) if 'caoff_orgnum' in m01_id.keys() else np.nan
    dict_out['BAIRONG_APPLY_LOAN_nbank_min_inteday_m06_id']=float(m06_id['nbank_min_inteday']) if 'nbank_min_inteday' in m06_id.keys() else np.nan
    dict_out['BAIRONG_APPLY_LOAN_bank_ret_orgnum_m12_cell']=float(m12_cell['bank_ret_orgnum']) if 'bank_ret_orgnum' in m12_cell.keys() else np.nan
    dict_out['BAIRONG_APPLY_LOAN_bank_avg_monnum_m03_id']=float(m03_id['bank_avg_monnum']) if 'bank_avg_monnum' in m03_id.keys() else np.nan
    dict_out['BAIRONG_APPLY_LOAN_bank_ret_orgnum_m03_cell']=float(m03_cell['bank_ret_orgnum']) if 'bank_ret_orgnum' in m03_cell.keys() else np.nan
    dict_out['BAIRONG_APPLY_LOAN_cooff_orgnum_d15_id']=float(d15_id['cooff_orgnum']) if 'cooff_orgnum' in d15_id.keys() else np.nan
    dict_out['BAIRONG_APPLY_LOAN_max_inteday_m03_cell']=float(m03_cell['max_inteday']) if 'max_inteday' in m03_cell.keys() else np.nan
    dict_out['BAIRONG_APPLY_LOAN_oth_orgnum_m12_cell']=float(m12_cell['oth_orgnum']) if 'oth_orgnum' in m12_cell.keys() else np.nan
    dict_out['BAIRONG_APPLY_LOAN_rel_orgnum_m06_id']=float(m06_id['rel_orgnum']) if 'rel_orgnum' in m06_id.keys() else np.nan
    dict_out['BAIRONG_APPLY_LOAN_nbank_cf_allnum_d15_cell']=float(d15_cell['nbank_cf_allnum']) if 'nbank_cf_allnum' in d15_cell.keys() else np.nan
    dict_out['BAIRONG_APPLY_LOAN_nbank_sloan_allnum_d15_id']=float(d15_id['nbank_sloan_allnum']) if 'nbank_sloan_allnum' in d15_id.keys() else np.nan
    dict_out['BAIRONG_APPLY_LOAN_nbank_sloan_allnum_d07_cell']=float(d07_cell['nbank_sloan_allnum']) if 'nbank_sloan_allnum' in d07_cell.keys() else np.nan
    dict_out['BAIRONG_APPLY_LOAN_caoff_orgnum_m06_cell']=float(m06_cell['caoff_orgnum']) if 'caoff_orgnum' in m06_cell.keys() else np.nan
    dict_out['BAIRONG_APPLY_LOAN_nbank_ca_orgnum_m01_id']=float(m01_id['nbank_ca_orgnum']) if 'nbank_ca_orgnum' in m01_id.keys() else np.nan
    dict_out['BAIRONG_APPLY_LOAN_nbank_oth_allnum_m12_cell']=float(m12_cell['nbank_oth_allnum']) if 'nbank_oth_allnum' in m12_cell.keys() else np.nan
    dict_out['BAIRONG_APPLY_LOAN_nbank_cons_orgnum_d15_id']=float(d15_id['nbank_cons_orgnum']) if 'nbank_cons_orgnum' in d15_id.keys() else np.nan
    dict_out['BAIRONG_APPLY_LOAN_nbank_oth_orgnum_m12_cell']=float(m12_cell['nbank_oth_orgnum']) if 'nbank_oth_orgnum' in m12_cell.keys() else np.nan
    dict_out['BAIRONG_APPLY_LOAN_bank_max_inteday_m03_cell']=float(m03_cell['bank_max_inteday']) if 'bank_max_inteday' in m03_cell.keys() else np.nan
    dict_out['BAIRONG_APPLY_LOAN_oth_allnum_m03_cell']=float(m03_cell['oth_allnum']) if 'oth_allnum' in m03_cell.keys() else np.nan
    dict_out['BAIRONG_APPLY_LOAN_bank_tra_orgnum_m12_cell']=float(m12_cell['bank_tra_orgnum']) if 'bank_tra_orgnum' in m12_cell.keys() else np.nan
    dict_out['BAIRONG_APPLY_LOAN_rel_allnum_m12_cell']=float(m12_cell['rel_allnum']) if 'rel_allnum' in m12_cell.keys() else np.nan
    dict_out['BAIRONG_APPLY_LOAN_nbank_max_monnum_m06_cell']=float(m06_cell['nbank_max_monnum']) if 'nbank_max_monnum' in m06_cell.keys() else np.nan
    dict_out['BAIRONG_APPLY_LOAN_coon_orgnum_m12_id']=float(m12_id['coon_orgnum']) if 'coon_orgnum' in m12_id.keys() else np.nan
    dict_out['BAIRONG_APPLY_LOAN_nbank_cf_allnum_m06_cell']=float(m06_cell['nbank_cf_allnum']) if 'nbank_cf_allnum' in m06_cell.keys() else np.nan
    dict_out['BAIRONG_APPLY_LOAN_nbank_allnum_d15_cell']=float(d15_cell['nbank_allnum']) if 'nbank_allnum' in d15_cell.keys() else np.nan
    dict_out['BAIRONG_APPLY_LOAN_cooff_allnum_d07_id']=float(d07_id['cooff_allnum']) if 'cooff_allnum' in d07_id.keys() else np.nan
    dict_out['BAIRONG_APPLY_LOAN_caon_orgnum_m06_id']=float(m06_id['caon_orgnum']) if 'caon_orgnum' in m06_id.keys() else np.nan
    dict_out['BAIRONG_APPLY_LOAN_caon_orgnum_d07_cell']=float(d07_cell['caon_orgnum']) if 'caon_orgnum' in d07_cell.keys() else np.nan
    dict_out['BAIRONG_APPLY_LOAN_nbank_orgnum_m12_id']=float(m12_id['nbank_orgnum']) if 'nbank_orgnum' in m12_id.keys() else np.nan
    dict_out['BAIRONG_APPLY_LOAN_oth_allnum_d15_id']=float(d15_id['oth_allnum']) if 'oth_allnum' in d15_id.keys() else np.nan
    dict_out['BAIRONG_APPLY_LOAN_rel_orgnum_d15_cell']=float(d15_cell['rel_orgnum']) if 'rel_orgnum' in d15_cell.keys() else np.nan
    dict_out['BAIRONG_APPLY_LOAN_bank_tra_allnum_m03_cell']=float(m03_cell['bank_tra_allnum']) if 'bank_tra_allnum' in m03_cell.keys() else np.nan
    dict_out['BAIRONG_APPLY_LOAN_nbank_allnum_m06_id']=float(m06_id['nbank_allnum']) if 'nbank_allnum' in m06_id.keys() else np.nan
    dict_out['BAIRONG_APPLY_LOAN_nbank_cf_allnum_m01_id']=float(m01_id['nbank_cf_allnum']) if 'nbank_cf_allnum' in m01_id.keys() else np.nan
    dict_out['BAIRONG_APPLY_LOAN_bank_tot_mons_m06_id']=float(m06_id['bank_tot_mons']) if 'bank_tot_mons' in m06_id.keys() else np.nan
    dict_out['BAIRONG_APPLY_LOAN_bank_orgnum_m12_id']=float(m12_id['bank_orgnum']) if 'bank_orgnum' in m12_id.keys() else np.nan
    dict_out['BAIRONG_APPLY_LOAN_caon_allnum_m12_id']=float(m12_id['caon_allnum']) if 'caon_allnum' in m12_id.keys() else np.nan
    dict_out['BAIRONG_APPLY_LOAN_nbank_sloan_orgnum_d07_id']=float(d07_id['nbank_sloan_orgnum']) if 'nbank_sloan_orgnum' in d07_id.keys() else np.nan
    dict_out['BAIRONG_APPLY_LOAN_cooff_orgnum_d07_cell']=float(d07_cell['cooff_orgnum']) if 'cooff_orgnum' in d07_cell.keys() else np.nan
    dict_out['BAIRONG_APPLY_LOAN_nbank_week_allnum_d07_cell']=float(d07_cell['nbank_week_allnum']) if 'nbank_week_allnum' in d07_cell.keys() else np.nan
    dict_out['BAIRONG_APPLY_LOAN_nbank_sloan_allnum_m01_id']=float(m01_id['nbank_sloan_allnum']) if 'nbank_sloan_allnum' in m01_id.keys() else np.nan
    dict_out['BAIRONG_APPLY_LOAN_nbank_max_inteday_m12_cell']=float(m12_cell['nbank_max_inteday']) if 'nbank_max_inteday' in m12_cell.keys() else np.nan
    dict_out['BAIRONG_APPLY_LOAN_nbank_oth_allnum_m01_cell']=float(m01_cell['nbank_oth_allnum']) if 'nbank_oth_allnum' in m01_cell.keys() else np.nan
    dict_out['BAIRONG_APPLY_LOAN_nbank_min_monnum_m03_cell']=float(m03_cell['nbank_min_monnum']) if 'nbank_min_monnum' in m03_cell.keys() else np.nan
    dict_out['BAIRONG_APPLY_LOAN_nbank_cons_orgnum_d07_id']=float(d07_id['nbank_cons_orgnum']) if 'nbank_cons_orgnum' in d07_id.keys() else np.nan
    dict_out['BAIRONG_APPLY_LOAN_nbank_else_allnum_d07_id']=float(d07_id['nbank_else_allnum']) if 'nbank_else_allnum' in d07_id.keys() else np.nan
    dict_out['BAIRONG_APPLY_LOAN_bank_min_inteday_m12_cell']=float(m12_cell['bank_min_inteday']) if 'bank_min_inteday' in m12_cell.keys() else np.nan
    dict_out['BAIRONG_APPLY_LOAN_min_inteday_m06_cell']=float(m06_cell['min_inteday']) if 'min_inteday' in m06_cell.keys() else np.nan
    dict_out['BAIRONG_APPLY_LOAN_nbank_sloan_allnum_d07_id']=float(d07_id['nbank_sloan_allnum']) if 'nbank_sloan_allnum' in d07_id.keys() else np.nan
    dict_out['BAIRONG_APPLY_LOAN_nbank_allnum_d15_id']=float(d15_id['nbank_allnum']) if 'nbank_allnum' in d15_id.keys() else np.nan
    dict_out['BAIRONG_APPLY_LOAN_oth_orgnum_m03_cell']=float(m03_cell['oth_orgnum']) if 'oth_orgnum' in m03_cell.keys() else np.nan
    dict_out['BAIRONG_APPLY_LOAN_nbank_oth_orgnum_m03_cell']=float(m03_cell['nbank_oth_orgnum']) if 'nbank_oth_orgnum' in m03_cell.keys() else np.nan
    dict_out['BAIRONG_APPLY_LOAN_nbank_max_monnum_m06_id']=float(m06_id['nbank_max_monnum']) if 'nbank_max_monnum' in m06_id.keys() else np.nan
    dict_out['BAIRONG_APPLY_LOAN_nbank_ca_orgnum_m06_id']=float(m06_id['nbank_ca_orgnum']) if 'nbank_ca_orgnum' in m06_id.keys() else np.nan
    dict_out['BAIRONG_APPLY_LOAN_bank_week_orgnum_m12_cell']=float(m12_cell['bank_week_orgnum']) if 'bank_week_orgnum' in m12_cell.keys() else np.nan
    dict_out['BAIRONG_APPLY_LOAN_nbank_sloan_orgnum_m06_id']=float(m06_id['nbank_sloan_orgnum']) if 'nbank_sloan_orgnum' in m06_id.keys() else np.nan
    dict_out['BAIRONG_APPLY_LOAN_nbank_finlea_allnum_m12_cell']=float(m12_cell['nbank_finlea_allnum']) if 'nbank_finlea_allnum' in m12_cell.keys() else np.nan
    dict_out['BAIRONG_APPLY_LOAN_oth_allnum_m01_id']=float(m01_id['oth_allnum']) if 'oth_allnum' in m01_id.keys() else np.nan
    dict_out['BAIRONG_APPLY_LOAN_caoff_allnum_m06_cell']=float(m06_cell['caoff_allnum']) if 'caoff_allnum' in m06_cell.keys() else np.nan
    dict_out['BAIRONG_APPLY_LOAN_nbank_finlea_allnum_m01_cell']=float(m01_cell['nbank_finlea_allnum']) if 'nbank_finlea_allnum' in m01_cell.keys() else np.nan
    dict_out['BAIRONG_APPLY_LOAN_nbank_ca_allnum_m06_id']=float(m06_id['nbank_ca_allnum']) if 'nbank_ca_allnum' in m06_id.keys() else np.nan
    dict_out['BAIRONG_APPLY_LOAN_nbank_nsloan_orgnum_m12_cell']=float(m12_cell['nbank_nsloan_orgnum']) if 'nbank_nsloan_orgnum' in m12_cell.keys() else np.nan
    dict_out['BAIRONG_APPLY_LOAN_nbank_oth_allnum_m03_cell']=float(m03_cell['nbank_oth_allnum']) if 'nbank_oth_allnum' in m03_cell.keys() else np.nan
    dict_out['BAIRONG_APPLY_LOAN_af_allnum_m01_id']=float(m01_id['af_allnum']) if 'af_allnum' in m01_id.keys() else np.nan
    dict_out['BAIRONG_APPLY_LOAN_bank_max_monnum_m03_cell']=float(m03_cell['bank_max_monnum']) if 'bank_max_monnum' in m03_cell.keys() else np.nan
    dict_out['BAIRONG_APPLY_LOAN_nbank_else_allnum_d07_cell']=float(d07_cell['nbank_else_allnum']) if 'nbank_else_allnum' in d07_cell.keys() else np.nan
    dict_out['BAIRONG_APPLY_LOAN_nbank_avg_monnum_m03_cell']=float(m03_cell['nbank_avg_monnum']) if 'nbank_avg_monnum' in m03_cell.keys() else np.nan
    dict_out['BAIRONG_APPLY_LOAN_nbank_min_monnum_m12_id']=float(m12_id['nbank_min_monnum']) if 'nbank_min_monnum' in m12_id.keys() else np.nan
    dict_out['BAIRONG_APPLY_LOAN_nbank_orgnum_d07_id']=float(d07_id['nbank_orgnum']) if 'nbank_orgnum' in d07_id.keys() else np.nan
    dict_out['BAIRONG_APPLY_LOAN_pdl_orgnum_m01_id']=float(m01_id['pdl_orgnum']) if 'pdl_orgnum' in m01_id.keys() else np.nan
    dict_out['BAIRONG_APPLY_LOAN_nbank_nsloan_allnum_d15_cell']=float(d15_cell['nbank_nsloan_allnum']) if 'nbank_nsloan_allnum' in d15_cell.keys() else np.nan
    dict_out['BAIRONG_APPLY_LOAN_nbank_cf_allnum_m12_cell']=float(m12_cell['nbank_cf_allnum']) if 'nbank_cf_allnum' in m12_cell.keys() else np.nan
    dict_out['BAIRONG_APPLY_LOAN_nbank_mc_orgnum_m03_cell']=float(m03_cell['nbank_mc_orgnum']) if 'nbank_mc_orgnum' in m03_cell.keys() else np.nan
    dict_out['BAIRONG_APPLY_LOAN_nbank_week_allnum_m01_id']=float(m01_id['nbank_week_allnum']) if 'nbank_week_allnum' in m01_id.keys() else np.nan
    dict_out['BAIRONG_APPLY_LOAN_nbank_cons_allnum_m06_cell']=float(m06_cell['nbank_cons_allnum']) if 'nbank_cons_allnum' in m06_cell.keys() else np.nan
    dict_out['BAIRONG_APPLY_LOAN_bank_max_inteday_m03_id']=float(m03_id['bank_max_inteday']) if 'bank_max_inteday' in m03_id.keys() else np.nan
    dict_out['BAIRONG_APPLY_LOAN_bank_max_inteday_m06_id']=float(m06_id['bank_max_inteday']) if 'bank_max_inteday' in m06_id.keys() else np.nan
    dict_out['BAIRONG_APPLY_LOAN_nbank_else_orgnum_d15_id']=float(d15_id['nbank_else_orgnum']) if 'nbank_else_orgnum' in d15_id.keys() else np.nan
    dict_out['BAIRONG_APPLY_LOAN_nbank_cons_orgnum_d07_cell']=float(d07_cell['nbank_cons_orgnum']) if 'nbank_cons_orgnum' in d07_cell.keys() else np.nan
    dict_out['BAIRONG_APPLY_LOAN_max_monnum_m12_cell']=float(m12_cell['max_monnum']) if 'max_monnum' in m12_cell.keys() else np.nan
    dict_out['BAIRONG_APPLY_LOAN_bank_min_inteday_m06_cell']=float(m06_cell['bank_min_inteday']) if 'bank_min_inteday' in m06_cell.keys() else np.nan
    dict_out['BAIRONG_APPLY_LOAN_bank_allnum_m06_id']=float(m06_id['bank_allnum']) if 'bank_allnum' in m06_id.keys() else np.nan
    dict_out['BAIRONG_APPLY_LOAN_nbank_cons_allnum_d15_id']=float(d15_id['nbank_cons_allnum']) if 'nbank_cons_allnum' in d15_id.keys() else np.nan
    dict_out['BAIRONG_APPLY_LOAN_pdl_allnum_m03_id']=float(m03_id['pdl_allnum']) if 'pdl_allnum' in m03_id.keys() else np.nan
    dict_out['BAIRONG_APPLY_LOAN_pdl_orgnum_m01_cell']=float(m01_cell['pdl_orgnum']) if 'pdl_orgnum' in m01_cell.keys() else np.nan
    dict_out['BAIRONG_APPLY_LOAN_nbank_week_allnum_m03_id']=float(m03_id['nbank_week_allnum']) if 'nbank_week_allnum' in m03_id.keys() else np.nan
    dict_out['BAIRONG_APPLY_LOAN_bank_allnum_m03_cell']=float(m03_cell['bank_allnum']) if 'bank_allnum' in m03_cell.keys() else np.nan
    dict_out['BAIRONG_APPLY_LOAN_bank_tra_allnum_m01_cell']=float(m01_cell['bank_tra_allnum']) if 'bank_tra_allnum' in m01_cell.keys() else np.nan
    dict_out['BAIRONG_APPLY_LOAN_nbank_p2p_allnum_m01_cell']=float(m01_cell['nbank_p2p_allnum']) if 'nbank_p2p_allnum' in m01_cell.keys() else np.nan
    dict_out['BAIRONG_APPLY_LOAN_caon_allnum_m06_id']=float(m06_id['caon_allnum']) if 'caon_allnum' in m06_id.keys() else np.nan
    dict_out['BAIRONG_APPLY_LOAN_bank_tot_mons_m12_cell']=float(m12_cell['bank_tot_mons']) if 'bank_tot_mons' in m12_cell.keys() else np.nan
    dict_out['BAIRONG_APPLY_LOAN_bank_tra_allnum_m12_cell']=float(m12_cell['bank_tra_allnum']) if 'bank_tra_allnum' in m12_cell.keys() else np.nan
    dict_out['BAIRONG_APPLY_LOAN_nbank_max_inteday_m06_cell']=float(m06_cell['nbank_max_inteday']) if 'nbank_max_inteday' in m06_cell.keys() else np.nan
    dict_out['BAIRONG_APPLY_LOAN_nbank_else_orgnum_d07_cell']=float(d07_cell['nbank_else_orgnum']) if 'nbank_else_orgnum' in d07_cell.keys() else np.nan
    dict_out['BAIRONG_APPLY_LOAN_nbank_ca_allnum_d07_cell']=float(d07_cell['nbank_ca_allnum']) if 'nbank_ca_allnum' in d07_cell.keys() else np.nan
    dict_out['BAIRONG_APPLY_LOAN_nbank_sloan_allnum_m03_id']=float(m03_id['nbank_sloan_allnum']) if 'nbank_sloan_allnum' in m03_id.keys() else np.nan
    dict_out['BAIRONG_APPLY_LOAN_tot_mons_m06_cell']=float(m06_cell['tot_mons']) if 'tot_mons' in m06_cell.keys() else np.nan
    dict_out['BAIRONG_APPLY_LOAN_bank_ret_allnum_m06_cell']=float(m06_cell['bank_ret_allnum']) if 'bank_ret_allnum' in m06_cell.keys() else np.nan
    dict_out['BAIRONG_APPLY_LOAN_nbank_night_orgnum_d07_id']=float(d07_id['nbank_night_orgnum']) if 'nbank_night_orgnum' in d07_id.keys() else np.nan
    dict_out['BAIRONG_APPLY_LOAN_nbank_nsloan_allnum_d15_id']=float(d15_id['nbank_nsloan_allnum']) if 'nbank_nsloan_allnum' in d15_id.keys() else np.nan
    dict_out['BAIRONG_APPLY_LOAN_cooff_orgnum_m12_id']=float(m12_id['cooff_orgnum']) if 'cooff_orgnum' in m12_id.keys() else np.nan
    dict_out['BAIRONG_APPLY_LOAN_nbank_else_allnum_m12_id']=float(m12_id['nbank_else_allnum']) if 'nbank_else_allnum' in m12_id.keys() else np.nan
    dict_out['BAIRONG_APPLY_LOAN_nbank_sloan_allnum_m03_cell']=float(m03_cell['nbank_sloan_allnum']) if 'nbank_sloan_allnum' in m03_cell.keys() else np.nan
    dict_out['BAIRONG_APPLY_LOAN_nbank_cons_allnum_m12_id']=float(m12_id['nbank_cons_allnum']) if 'nbank_cons_allnum' in m12_id.keys() else np.nan
    dict_out['BAIRONG_APPLY_LOAN_cooff_allnum_m06_id']=float(m06_id['cooff_allnum']) if 'cooff_allnum' in m06_id.keys() else np.nan
    dict_out['BAIRONG_APPLY_LOAN_nbank_cons_allnum_m03_cell']=float(m03_cell['nbank_cons_allnum']) if 'nbank_cons_allnum' in m03_cell.keys() else np.nan
    dict_out['BAIRONG_APPLY_LOAN_bank_tra_orgnum_m12_id']=float(m12_id['bank_tra_orgnum']) if 'bank_tra_orgnum' in m12_id.keys() else np.nan
    dict_out['BAIRONG_APPLY_LOAN_bank_night_allnum_m03_cell']=float(m03_cell['bank_night_allnum']) if 'bank_night_allnum' in m03_cell.keys() else np.nan
    dict_out['BAIRONG_APPLY_LOAN_nbank_night_orgnum_d07_cell']=float(d07_cell['nbank_night_orgnum']) if 'nbank_night_orgnum' in d07_cell.keys() else np.nan
    dict_out['BAIRONG_APPLY_LOAN_bank_ret_orgnum_m06_cell']=float(m06_cell['bank_ret_orgnum']) if 'bank_ret_orgnum' in m06_cell.keys() else np.nan
    dict_out['BAIRONG_APPLY_LOAN_max_monnum_m03_cell']=float(m03_cell['max_monnum']) if 'max_monnum' in m03_cell.keys() else np.nan
    dict_out['BAIRONG_APPLY_LOAN_cooff_allnum_m12_cell']=float(m12_cell['cooff_allnum']) if 'cooff_allnum' in m12_cell.keys() else np.nan
    dict_out['BAIRONG_APPLY_LOAN_bank_allnum_m12_cell']=float(m12_cell['bank_allnum']) if 'bank_allnum' in m12_cell.keys() else np.nan
    dict_out['BAIRONG_APPLY_LOAN_nbank_else_allnum_m12_cell']=float(m12_cell['nbank_else_allnum']) if 'nbank_else_allnum' in m12_cell.keys() else np.nan
    dict_out['BAIRONG_APPLY_LOAN_bank_tra_orgnum_m01_id']=float(m01_id['bank_tra_orgnum']) if 'bank_tra_orgnum' in m01_id.keys() else np.nan
    dict_out['BAIRONG_APPLY_LOAN_rel_allnum_d15_id']=float(d15_id['rel_allnum']) if 'rel_allnum' in d15_id.keys() else np.nan
    dict_out['BAIRONG_APPLY_LOAN_nbank_mc_orgnum_m12_id']=float(m12_id['nbank_mc_orgnum']) if 'nbank_mc_orgnum' in m12_id.keys() else np.nan
    dict_out['BAIRONG_APPLY_LOAN_nbank_mc_orgnum_m12_cell']=float(m12_cell['nbank_mc_orgnum']) if 'nbank_mc_orgnum' in m12_cell.keys() else np.nan
    dict_out['BAIRONG_APPLY_LOAN_nbank_orgnum_m06_cell']=float(m06_cell['nbank_orgnum']) if 'nbank_orgnum' in m06_cell.keys() else np.nan
    dict_out['BAIRONG_APPLY_LOAN_cooff_allnum_m03_cell']=float(m03_cell['cooff_allnum']) if 'cooff_allnum' in m03_cell.keys() else np.nan
    dict_out['BAIRONG_APPLY_LOAN_bank_night_orgnum_m03_id']=float(m03_id['bank_night_orgnum']) if 'bank_night_orgnum' in m03_id.keys() else np.nan
    dict_out['BAIRONG_APPLY_LOAN_cooff_orgnum_d15_cell']=float(d15_cell['cooff_orgnum']) if 'cooff_orgnum' in d15_cell.keys() else np.nan
    dict_out['BAIRONG_APPLY_LOAN_pdl_allnum_m12_cell']=float(m12_cell['pdl_allnum']) if 'pdl_allnum' in m12_cell.keys() else np.nan
    dict_out['BAIRONG_APPLY_LOAN_nbank_week_allnum_m12_cell']=float(m12_cell['nbank_week_allnum']) if 'nbank_week_allnum' in m12_cell.keys() else np.nan
    dict_out['BAIRONG_APPLY_LOAN_bank_orgnum_m06_id']=float(m06_id['bank_orgnum']) if 'bank_orgnum' in m06_id.keys() else np.nan
    dict_out['BAIRONG_APPLY_LOAN_bank_tra_orgnum_m06_cell']=float(m06_cell['bank_tra_orgnum']) if 'bank_tra_orgnum' in m06_cell.keys() else np.nan


    dict_out['BAIRONG_APPLY_LOAN_oth_allnum_m03vm12_cell']= dict_out['BAIRONG_APPLY_LOAN_oth_allnum_m03_cell']/dict_out['BAIRONG_APPLY_LOAN_oth_allnum_m12_cell'] if dict_out['BAIRONG_APPLY_LOAN_oth_allnum_m12_cell']>0 else np.nan
    dict_out['BAIRONG_APPLY_LOAN_nbank_cf_orgnum_m03vm06_cell']= dict_out['BAIRONG_APPLY_LOAN_nbank_cf_orgnum_m03_cell']/dict_out['BAIRONG_APPLY_LOAN_nbank_cf_orgnum_m06_cell'] if dict_out['BAIRONG_APPLY_LOAN_nbank_cf_orgnum_m06_cell']>0 else np.nan
    dict_out['BAIRONG_APPLY_LOAN_rel_allnum_d15vm06_id']= dict_out['BAIRONG_APPLY_LOAN_rel_allnum_d15_id']/dict_out['BAIRONG_APPLY_LOAN_rel_allnum_m06_id'] if dict_out['BAIRONG_APPLY_LOAN_rel_allnum_m06_id']>0 else np.nan
    dict_out['BAIRONG_APPLY_LOAN_avg_monnum_m12_idsubcell']= dict_out['BAIRONG_APPLY_LOAN_avg_monnum_m12_id']-dict_out['BAIRONG_APPLY_LOAN_avg_monnum_m12_cell']
    dict_out['BAIRONG_APPLY_LOAN_caon_orgnum_d07vd15_cell']= dict_out['BAIRONG_APPLY_LOAN_caon_orgnum_d07_cell']/dict_out['BAIRONG_APPLY_LOAN_caon_orgnum_d15_cell'] if dict_out['BAIRONG_APPLY_LOAN_caon_orgnum_d15_cell']>0 else np.nan
    dict_out['BAIRONG_APPLY_LOAN_nbank_night_orgnum_d15vm12_id']= dict_out['BAIRONG_APPLY_LOAN_nbank_night_orgnum_d15_id']/dict_out['BAIRONG_APPLY_LOAN_nbank_night_orgnum_m12_id'] if dict_out['BAIRONG_APPLY_LOAN_nbank_night_orgnum_m12_id']>0 else np.nan
    dict_out['BAIRONG_APPLY_LOAN_caon_allnum_d15vm01_cell']= dict_out['BAIRONG_APPLY_LOAN_caon_allnum_d15_cell']/dict_out['BAIRONG_APPLY_LOAN_caon_allnum_m01_cell'] if dict_out['BAIRONG_APPLY_LOAN_caon_allnum_m01_cell']>0 else np.nan
    dict_out['BAIRONG_APPLY_LOAN_oth_allnum_m06vm12_id']= dict_out['BAIRONG_APPLY_LOAN_oth_allnum_m06_id']/dict_out['BAIRONG_APPLY_LOAN_oth_allnum_m12_id'] if dict_out['BAIRONG_APPLY_LOAN_oth_allnum_m12_id']>0 else np.nan
    dict_out['BAIRONG_APPLY_LOAN_bank_tra_allnum_d15vm06_cell']= dict_out['BAIRONG_APPLY_LOAN_bank_tra_allnum_d15_cell']/dict_out['BAIRONG_APPLY_LOAN_bank_tra_allnum_m06_cell'] if dict_out['BAIRONG_APPLY_LOAN_bank_tra_allnum_m06_cell']>0 else np.nan
    dict_out['BAIRONG_APPLY_LOAN_nbank_cons_allnum_m03vm12_id']= dict_out['BAIRONG_APPLY_LOAN_nbank_cons_allnum_m03_id']/dict_out['BAIRONG_APPLY_LOAN_nbank_cons_allnum_m12_id'] if dict_out['BAIRONG_APPLY_LOAN_nbank_cons_allnum_m12_id']>0 else np.nan
    dict_out['BAIRONG_APPLY_LOAN_caon_allnum_d07vm12_cell']= dict_out['BAIRONG_APPLY_LOAN_caon_allnum_d07_cell']/dict_out['BAIRONG_APPLY_LOAN_caon_allnum_m12_cell'] if dict_out['BAIRONG_APPLY_LOAN_caon_allnum_m12_cell']>0 else np.nan
    dict_out['BAIRONG_APPLY_LOAN_nbank_cf_orgnum_m03vm12_cell']= dict_out['BAIRONG_APPLY_LOAN_nbank_cf_orgnum_m03_cell']/dict_out['BAIRONG_APPLY_LOAN_nbank_cf_orgnum_m12_cell'] if dict_out['BAIRONG_APPLY_LOAN_nbank_cf_orgnum_m12_cell']>0 else np.nan
    dict_out['BAIRONG_APPLY_LOAN_nbank_orgnum_m01vm12_id']= dict_out['BAIRONG_APPLY_LOAN_nbank_orgnum_m01_id']/dict_out['BAIRONG_APPLY_LOAN_nbank_orgnum_m12_id'] if dict_out['BAIRONG_APPLY_LOAN_nbank_orgnum_m12_id']>0 else np.nan
    dict_out['BAIRONG_APPLY_LOAN_nbank_cf_orgnum_d07vd15_id']= dict_out['BAIRONG_APPLY_LOAN_nbank_cf_orgnum_d07_id']/dict_out['BAIRONG_APPLY_LOAN_nbank_cf_orgnum_d15_id'] if dict_out['BAIRONG_APPLY_LOAN_nbank_cf_orgnum_d15_id']>0 else np.nan
    dict_out['BAIRONG_APPLY_LOAN_nbank_mc_allnum_m06vm12_id']= dict_out['BAIRONG_APPLY_LOAN_nbank_mc_allnum_m06_id']/dict_out['BAIRONG_APPLY_LOAN_nbank_mc_allnum_m12_id'] if dict_out['BAIRONG_APPLY_LOAN_nbank_mc_allnum_m12_id']>0 else np.nan
    dict_out['BAIRONG_APPLY_LOAN_nbank_mc_allnum_m03vm12_cell']= dict_out['BAIRONG_APPLY_LOAN_nbank_mc_allnum_m03_cell']/dict_out['BAIRONG_APPLY_LOAN_nbank_mc_allnum_m12_cell'] if dict_out['BAIRONG_APPLY_LOAN_nbank_mc_allnum_m12_cell']>0 else np.nan
    dict_out['BAIRONG_APPLY_LOAN_oth_allnum_m03vm12_id']= dict_out['BAIRONG_APPLY_LOAN_oth_allnum_m03_id']/dict_out['BAIRONG_APPLY_LOAN_oth_allnum_m12_id'] if dict_out['BAIRONG_APPLY_LOAN_oth_allnum_m12_id']>0 else np.nan
    dict_out['BAIRONG_APPLY_LOAN_nbank_nsloan_allnum_m03vm06_id']= dict_out['BAIRONG_APPLY_LOAN_nbank_nsloan_allnum_m03_id']/dict_out['BAIRONG_APPLY_LOAN_nbank_nsloan_allnum_m06_id'] if dict_out['BAIRONG_APPLY_LOAN_nbank_nsloan_allnum_m06_id']>0 else np.nan
    dict_out['BAIRONG_APPLY_LOAN_coon_allnum_m06vm12_id']= dict_out['BAIRONG_APPLY_LOAN_coon_allnum_m06_id']/dict_out['BAIRONG_APPLY_LOAN_coon_allnum_m12_id'] if dict_out['BAIRONG_APPLY_LOAN_coon_allnum_m12_id']>0 else np.nan
    dict_out['BAIRONG_APPLY_LOAN_nbank_p2p_allnum_d15vm12_cell']= dict_out['BAIRONG_APPLY_LOAN_nbank_p2p_allnum_d15_cell']/dict_out['BAIRONG_APPLY_LOAN_nbank_p2p_allnum_m12_cell'] if dict_out['BAIRONG_APPLY_LOAN_nbank_p2p_allnum_m12_cell']>0 else np.nan
    dict_out['BAIRONG_APPLY_LOAN_caoff_allnum_m03vm06_cell']= dict_out['BAIRONG_APPLY_LOAN_caoff_allnum_m03_cell']/dict_out['BAIRONG_APPLY_LOAN_caoff_allnum_m06_cell'] if dict_out['BAIRONG_APPLY_LOAN_caoff_allnum_m06_cell']>0 else np.nan
    dict_out['BAIRONG_APPLY_LOAN_cooff_allnum_d15vm06_id']= dict_out['BAIRONG_APPLY_LOAN_cooff_allnum_d15_id']/dict_out['BAIRONG_APPLY_LOAN_cooff_allnum_m06_id'] if dict_out['BAIRONG_APPLY_LOAN_cooff_allnum_m06_id']>0 else np.nan
    dict_out['BAIRONG_APPLY_LOAN_nbank_ca_orgnum_m12_idsubcell']= dict_out['BAIRONG_APPLY_LOAN_nbank_ca_orgnum_m12_id']-dict_out['BAIRONG_APPLY_LOAN_nbank_ca_orgnum_m12_cell']
    dict_out['BAIRONG_APPLY_LOAN_bank_ret_orgnum_m03vm12_id']= dict_out['BAIRONG_APPLY_LOAN_bank_ret_orgnum_m03_id']/dict_out['BAIRONG_APPLY_LOAN_bank_ret_orgnum_m12_id'] if dict_out['BAIRONG_APPLY_LOAN_bank_ret_orgnum_m12_id']>0 else np.nan
    dict_out['BAIRONG_APPLY_LOAN_nbank_cf_orgnum_m01vm12_cell']= dict_out['BAIRONG_APPLY_LOAN_nbank_cf_orgnum_m01_cell']/dict_out['BAIRONG_APPLY_LOAN_nbank_cf_orgnum_m12_cell'] if dict_out['BAIRONG_APPLY_LOAN_nbank_cf_orgnum_m12_cell']>0 else np.nan
    dict_out['BAIRONG_APPLY_LOAN_nbank_cf_orgnum_d07vm06_id']= dict_out['BAIRONG_APPLY_LOAN_nbank_cf_orgnum_d07_id']/dict_out['BAIRONG_APPLY_LOAN_nbank_cf_orgnum_m06_id'] if dict_out['BAIRONG_APPLY_LOAN_nbank_cf_orgnum_m06_id']>0 else np.nan
    dict_out['BAIRONG_APPLY_LOAN_bank_orgnum_m01vm03_id']= dict_out['BAIRONG_APPLY_LOAN_bank_orgnum_m01_id']/dict_out['BAIRONG_APPLY_LOAN_bank_orgnum_m03_id'] if dict_out['BAIRONG_APPLY_LOAN_bank_orgnum_m03_id']>0 else np.nan
    dict_out['BAIRONG_APPLY_LOAN_nbank_p2p_allnum_m12_idsubcell']= dict_out['BAIRONG_APPLY_LOAN_nbank_p2p_allnum_m12_id']-dict_out['BAIRONG_APPLY_LOAN_nbank_p2p_allnum_m12_cell']
    dict_out['BAIRONG_APPLY_LOAN_nbank_week_allnum_d15vm06_cell']= dict_out['BAIRONG_APPLY_LOAN_nbank_week_allnum_d15_cell']/dict_out['BAIRONG_APPLY_LOAN_nbank_week_allnum_m06_cell'] if dict_out['BAIRONG_APPLY_LOAN_nbank_week_allnum_m06_cell']>0 else np.nan
    dict_out['BAIRONG_APPLY_LOAN_rel_allnum_d07vm12_cell']= dict_out['BAIRONG_APPLY_LOAN_rel_allnum_d07_cell']/dict_out['BAIRONG_APPLY_LOAN_rel_allnum_m12_cell'] if dict_out['BAIRONG_APPLY_LOAN_rel_allnum_m12_cell']>0 else np.nan
    dict_out['BAIRONG_APPLY_LOAN_nbank_tot_mons_m06vm12_cell']= dict_out['BAIRONG_APPLY_LOAN_nbank_tot_mons_m06_cell']/dict_out['BAIRONG_APPLY_LOAN_nbank_tot_mons_m12_cell'] if dict_out['BAIRONG_APPLY_LOAN_nbank_tot_mons_m12_cell']>0 else np.nan
    dict_out['BAIRONG_APPLY_LOAN_caoff_allnum_m06vm12_id']= dict_out['BAIRONG_APPLY_LOAN_caoff_allnum_m06_id']/dict_out['BAIRONG_APPLY_LOAN_caoff_allnum_m12_id'] if dict_out['BAIRONG_APPLY_LOAN_caoff_allnum_m12_id']>0 else np.nan
    dict_out['BAIRONG_APPLY_LOAN_caon_orgnum_m01vm06_cell']= dict_out['BAIRONG_APPLY_LOAN_caon_orgnum_m01_cell']/dict_out['BAIRONG_APPLY_LOAN_caon_orgnum_m06_cell'] if dict_out['BAIRONG_APPLY_LOAN_caon_orgnum_m06_cell']>0 else np.nan
    dict_out['BAIRONG_APPLY_LOAN_cooff_allnum_m06vm12_id']= dict_out['BAIRONG_APPLY_LOAN_cooff_allnum_m06_id']/dict_out['BAIRONG_APPLY_LOAN_cooff_allnum_m12_id'] if dict_out['BAIRONG_APPLY_LOAN_cooff_allnum_m12_id']>0 else np.nan
    dict_out['BAIRONG_APPLY_LOAN_nbank_sloan_allnum_m01vm06_cell']= dict_out['BAIRONG_APPLY_LOAN_nbank_sloan_allnum_m01_cell']/dict_out['BAIRONG_APPLY_LOAN_nbank_sloan_allnum_m06_cell'] if dict_out['BAIRONG_APPLY_LOAN_nbank_sloan_allnum_m06_cell']>0 else np.nan
    dict_out['BAIRONG_APPLY_LOAN_caoff_allnum_d15vm06_id']= dict_out['BAIRONG_APPLY_LOAN_caoff_allnum_d15_id']/dict_out['BAIRONG_APPLY_LOAN_caoff_allnum_m06_id'] if dict_out['BAIRONG_APPLY_LOAN_caoff_allnum_m06_id']>0 else np.nan
    dict_out['BAIRONG_APPLY_LOAN_nbank_nsloan_allnum_d07vm06_cell']= dict_out['BAIRONG_APPLY_LOAN_nbank_nsloan_allnum_d07_cell']/dict_out['BAIRONG_APPLY_LOAN_nbank_nsloan_allnum_m06_cell'] if dict_out['BAIRONG_APPLY_LOAN_nbank_nsloan_allnum_m06_cell']>0 else np.nan
    dict_out['BAIRONG_APPLY_LOAN_pdl_allnum_d15vm06_cell']= dict_out['BAIRONG_APPLY_LOAN_pdl_allnum_d15_cell']/dict_out['BAIRONG_APPLY_LOAN_pdl_allnum_m06_cell'] if dict_out['BAIRONG_APPLY_LOAN_pdl_allnum_m06_cell']>0 else np.nan
    dict_out['BAIRONG_APPLY_LOAN_nbank_cf_allnum_d07vm12_cell']= dict_out['BAIRONG_APPLY_LOAN_nbank_cf_allnum_d07_cell']/dict_out['BAIRONG_APPLY_LOAN_nbank_cf_allnum_m12_cell'] if dict_out['BAIRONG_APPLY_LOAN_nbank_cf_allnum_m12_cell']>0 else np.nan
    dict_out['BAIRONG_APPLY_LOAN_nbank_cons_allnum_m01vm12_id']= dict_out['BAIRONG_APPLY_LOAN_nbank_cons_allnum_m01_id']/dict_out['BAIRONG_APPLY_LOAN_nbank_cons_allnum_m12_id'] if dict_out['BAIRONG_APPLY_LOAN_nbank_cons_allnum_m12_id']>0 else np.nan
    dict_out['BAIRONG_APPLY_LOAN_coon_allnum_d15vm12_id']= dict_out['BAIRONG_APPLY_LOAN_coon_allnum_d15_id']/dict_out['BAIRONG_APPLY_LOAN_coon_allnum_m12_id'] if dict_out['BAIRONG_APPLY_LOAN_coon_allnum_m12_id']>0 else np.nan
    dict_out['BAIRONG_APPLY_LOAN_nbank_nsloan_orgnum_m06_idsubcell']= dict_out['BAIRONG_APPLY_LOAN_nbank_nsloan_orgnum_m06_id']-dict_out['BAIRONG_APPLY_LOAN_nbank_nsloan_orgnum_m06_cell']
    dict_out['BAIRONG_APPLY_LOAN_nbank_orgnum_d15vm01_cell']= dict_out['BAIRONG_APPLY_LOAN_nbank_orgnum_d15_cell']/dict_out['BAIRONG_APPLY_LOAN_nbank_orgnum_m01_cell'] if dict_out['BAIRONG_APPLY_LOAN_nbank_orgnum_m01_cell']>0 else np.nan
    dict_out['BAIRONG_APPLY_LOAN_nbank_cons_allnum_m01vm03_cell']= dict_out['BAIRONG_APPLY_LOAN_nbank_cons_allnum_m01_cell']/dict_out['BAIRONG_APPLY_LOAN_nbank_cons_allnum_m03_cell'] if dict_out['BAIRONG_APPLY_LOAN_nbank_cons_allnum_m03_cell']>0 else np.nan
    dict_out['BAIRONG_APPLY_LOAN_caoff_orgnum_m03vm06_id']= dict_out['BAIRONG_APPLY_LOAN_caoff_orgnum_m03_id']/dict_out['BAIRONG_APPLY_LOAN_caoff_orgnum_m06_id'] if dict_out['BAIRONG_APPLY_LOAN_caoff_orgnum_m06_id']>0 else np.nan
    dict_out['BAIRONG_APPLY_LOAN_nbank_week_orgnum_m03vm06_id']= dict_out['BAIRONG_APPLY_LOAN_nbank_week_orgnum_m03_id']/dict_out['BAIRONG_APPLY_LOAN_nbank_week_orgnum_m06_id'] if dict_out['BAIRONG_APPLY_LOAN_nbank_week_orgnum_m06_id']>0 else np.nan
    dict_out['BAIRONG_APPLY_LOAN_coon_allnum_d15vm06_id']= dict_out['BAIRONG_APPLY_LOAN_coon_allnum_d15_id']/dict_out['BAIRONG_APPLY_LOAN_coon_allnum_m06_id'] if dict_out['BAIRONG_APPLY_LOAN_coon_allnum_m06_id']>0 else np.nan
    dict_out['BAIRONG_APPLY_LOAN_nbank_week_orgnum_d07vm12_cell']= dict_out['BAIRONG_APPLY_LOAN_nbank_week_orgnum_d07_cell']/dict_out['BAIRONG_APPLY_LOAN_nbank_week_orgnum_m12_cell'] if dict_out['BAIRONG_APPLY_LOAN_nbank_week_orgnum_m12_cell']>0 else np.nan
    dict_out['BAIRONG_APPLY_LOAN_coon_allnum_m01vm06_cell']= dict_out['BAIRONG_APPLY_LOAN_coon_allnum_m01_cell']/dict_out['BAIRONG_APPLY_LOAN_coon_allnum_m06_cell'] if dict_out['BAIRONG_APPLY_LOAN_coon_allnum_m06_cell']>0 else np.nan
    dict_out['BAIRONG_APPLY_LOAN_caon_allnum_m01vm03_cell']= dict_out['BAIRONG_APPLY_LOAN_caon_allnum_m01_cell']/dict_out['BAIRONG_APPLY_LOAN_caon_allnum_m03_cell'] if dict_out['BAIRONG_APPLY_LOAN_caon_allnum_m03_cell']>0 else np.nan
    dict_out['BAIRONG_APPLY_LOAN_nbank_ca_allnum_m01vm12_id']= dict_out['BAIRONG_APPLY_LOAN_nbank_ca_allnum_m01_id']/dict_out['BAIRONG_APPLY_LOAN_nbank_ca_allnum_m12_id'] if dict_out['BAIRONG_APPLY_LOAN_nbank_ca_allnum_m12_id']>0 else np.nan
    dict_out['BAIRONG_APPLY_LOAN_cooff_allnum_m06vm12_cell']= dict_out['BAIRONG_APPLY_LOAN_cooff_allnum_m06_cell']/dict_out['BAIRONG_APPLY_LOAN_cooff_allnum_m12_cell'] if dict_out['BAIRONG_APPLY_LOAN_cooff_allnum_m12_cell']>0 else np.nan
    dict_out['BAIRONG_APPLY_LOAN_nbank_week_orgnum_d07vm06_cell']= dict_out['BAIRONG_APPLY_LOAN_nbank_week_orgnum_d07_cell']/dict_out['BAIRONG_APPLY_LOAN_nbank_week_orgnum_m06_cell'] if dict_out['BAIRONG_APPLY_LOAN_nbank_week_orgnum_m06_cell']>0 else np.nan
    dict_out['BAIRONG_APPLY_LOAN_cooff_allnum_m01vm06_id']= dict_out['BAIRONG_APPLY_LOAN_cooff_allnum_m01_id']/dict_out['BAIRONG_APPLY_LOAN_cooff_allnum_m06_id'] if dict_out['BAIRONG_APPLY_LOAN_cooff_allnum_m06_id']>0 else np.nan
    dict_out['BAIRONG_APPLY_LOAN_cooff_allnum_d07vm01_cell']= dict_out['BAIRONG_APPLY_LOAN_cooff_allnum_d07_cell']/dict_out['BAIRONG_APPLY_LOAN_cooff_allnum_m01_cell'] if dict_out['BAIRONG_APPLY_LOAN_cooff_allnum_m01_cell']>0 else np.nan
    dict_out['BAIRONG_APPLY_LOAN_nbank_p2p_allnum_d15vm12_id']= dict_out['BAIRONG_APPLY_LOAN_nbank_p2p_allnum_d15_id']/dict_out['BAIRONG_APPLY_LOAN_nbank_p2p_allnum_m12_id'] if dict_out['BAIRONG_APPLY_LOAN_nbank_p2p_allnum_m12_id']>0 else np.nan
    dict_out['BAIRONG_APPLY_LOAN_nbank_mc_allnum_m12_idsubcell']= dict_out['BAIRONG_APPLY_LOAN_nbank_mc_allnum_m12_id']-dict_out['BAIRONG_APPLY_LOAN_nbank_mc_allnum_m12_cell']
    dict_out['BAIRONG_APPLY_LOAN_nbank_else_orgnum_m06vm12_cell']= dict_out['BAIRONG_APPLY_LOAN_nbank_else_orgnum_m06_cell']/dict_out['BAIRONG_APPLY_LOAN_nbank_else_orgnum_m12_cell'] if dict_out['BAIRONG_APPLY_LOAN_nbank_else_orgnum_m12_cell']>0 else np.nan
    dict_out['BAIRONG_APPLY_LOAN_rel_allnum_m03vm12_id']= dict_out['BAIRONG_APPLY_LOAN_rel_allnum_m03_id']/dict_out['BAIRONG_APPLY_LOAN_rel_allnum_m12_id'] if dict_out['BAIRONG_APPLY_LOAN_rel_allnum_m12_id']>0 else np.nan
    dict_out['BAIRONG_APPLY_LOAN_oth_allnum_m06_idsubcell']= dict_out['BAIRONG_APPLY_LOAN_oth_allnum_m06_id']-dict_out['BAIRONG_APPLY_LOAN_oth_allnum_m06_cell']
    dict_out['BAIRONG_APPLY_LOAN_nbank_cons_orgnum_d07vd15_id']= dict_out['BAIRONG_APPLY_LOAN_nbank_cons_orgnum_d07_id']/dict_out['BAIRONG_APPLY_LOAN_nbank_cons_orgnum_d15_id'] if dict_out['BAIRONG_APPLY_LOAN_nbank_cons_orgnum_d15_id']>0 else np.nan
    dict_out['BAIRONG_APPLY_LOAN_nbank_allnum_m12_idsubcell']= dict_out['BAIRONG_APPLY_LOAN_nbank_allnum_m12_id']-dict_out['BAIRONG_APPLY_LOAN_nbank_allnum_m12_cell']
    dict_out['BAIRONG_APPLY_LOAN_cooff_allnum_d15vm12_cell']= dict_out['BAIRONG_APPLY_LOAN_cooff_allnum_d15_cell']/dict_out['BAIRONG_APPLY_LOAN_cooff_allnum_m12_cell'] if dict_out['BAIRONG_APPLY_LOAN_cooff_allnum_m12_cell']>0 else np.nan
    dict_out['BAIRONG_APPLY_LOAN_nbank_week_allnum_m01vm03_cell']= dict_out['BAIRONG_APPLY_LOAN_nbank_week_allnum_m01_cell']/dict_out['BAIRONG_APPLY_LOAN_nbank_week_allnum_m03_cell'] if dict_out['BAIRONG_APPLY_LOAN_nbank_week_allnum_m03_cell']>0 else np.nan
    dict_out['BAIRONG_APPLY_LOAN_nbank_finlea_allnum_d15vm01_id']= dict_out['BAIRONG_APPLY_LOAN_nbank_finlea_allnum_d15_id']/dict_out['BAIRONG_APPLY_LOAN_nbank_finlea_allnum_m01_id'] if dict_out['BAIRONG_APPLY_LOAN_nbank_finlea_allnum_m01_id']>0 else np.nan
    dict_out['BAIRONG_APPLY_LOAN_coon_orgnum_m06vm12_id']= dict_out['BAIRONG_APPLY_LOAN_coon_orgnum_m06_id']/dict_out['BAIRONG_APPLY_LOAN_coon_orgnum_m12_id'] if dict_out['BAIRONG_APPLY_LOAN_coon_orgnum_m12_id']>0 else np.nan
    dict_out['BAIRONG_APPLY_LOAN_nbank_cons_orgnum_m06vm12_cell']= dict_out['BAIRONG_APPLY_LOAN_nbank_cons_orgnum_m06_cell']/dict_out['BAIRONG_APPLY_LOAN_nbank_cons_orgnum_m12_cell'] if dict_out['BAIRONG_APPLY_LOAN_nbank_cons_orgnum_m12_cell']>0 else np.nan
    dict_out['BAIRONG_APPLY_LOAN_nbank_nsloan_allnum_m01vm12_cell']= dict_out['BAIRONG_APPLY_LOAN_nbank_nsloan_allnum_m01_cell']/dict_out['BAIRONG_APPLY_LOAN_nbank_nsloan_allnum_m12_cell'] if dict_out['BAIRONG_APPLY_LOAN_nbank_nsloan_allnum_m12_cell']>0 else np.nan
    dict_out['BAIRONG_APPLY_LOAN_nbank_week_allnum_m01vm12_cell']= dict_out['BAIRONG_APPLY_LOAN_nbank_week_allnum_m01_cell']/dict_out['BAIRONG_APPLY_LOAN_nbank_week_allnum_m12_cell'] if dict_out['BAIRONG_APPLY_LOAN_nbank_week_allnum_m12_cell']>0 else np.nan
    dict_out['BAIRONG_APPLY_LOAN_nbank_night_allnum_m06vm12_id']= dict_out['BAIRONG_APPLY_LOAN_nbank_night_allnum_m06_id']/dict_out['BAIRONG_APPLY_LOAN_nbank_night_allnum_m12_id'] if dict_out['BAIRONG_APPLY_LOAN_nbank_night_allnum_m12_id']>0 else np.nan
    dict_out['BAIRONG_APPLY_LOAN_nbank_else_allnum_d07vm01_cell']= dict_out['BAIRONG_APPLY_LOAN_nbank_else_allnum_d07_cell']/dict_out['BAIRONG_APPLY_LOAN_nbank_else_allnum_m01_cell'] if dict_out['BAIRONG_APPLY_LOAN_nbank_else_allnum_m01_cell']>0 else np.nan
    dict_out['BAIRONG_APPLY_LOAN_nbank_min_monnum_m03_idsubcell']= dict_out['BAIRONG_APPLY_LOAN_nbank_min_monnum_m03_id']-dict_out['BAIRONG_APPLY_LOAN_nbank_min_monnum_m03_cell']
    dict_out['BAIRONG_APPLY_LOAN_bank_week_orgnum_m03vm12_id']= dict_out['BAIRONG_APPLY_LOAN_bank_week_orgnum_m03_id']/dict_out['BAIRONG_APPLY_LOAN_bank_week_orgnum_m12_id'] if dict_out['BAIRONG_APPLY_LOAN_bank_week_orgnum_m12_id']>0 else np.nan
    dict_out['BAIRONG_APPLY_LOAN_nbank_else_allnum_d07vd15_cell']= dict_out['BAIRONG_APPLY_LOAN_nbank_else_allnum_d07_cell']/dict_out['BAIRONG_APPLY_LOAN_nbank_else_allnum_d15_cell'] if dict_out['BAIRONG_APPLY_LOAN_nbank_else_allnum_d15_cell']>0 else np.nan
    dict_out['BAIRONG_APPLY_LOAN_bank_allnum_m01vm03_id']= dict_out['BAIRONG_APPLY_LOAN_bank_allnum_m01_id']/dict_out['BAIRONG_APPLY_LOAN_bank_allnum_m03_id'] if dict_out['BAIRONG_APPLY_LOAN_bank_allnum_m03_id']>0 else np.nan
    dict_out['BAIRONG_APPLY_LOAN_nbank_sloan_orgnum_m03vm12_cell']= dict_out['BAIRONG_APPLY_LOAN_nbank_sloan_orgnum_m03_cell']/dict_out['BAIRONG_APPLY_LOAN_nbank_sloan_orgnum_m12_cell'] if dict_out['BAIRONG_APPLY_LOAN_nbank_sloan_orgnum_m12_cell']>0 else np.nan
    dict_out['BAIRONG_APPLY_LOAN_pdl_allnum_m06vm12_cell']= dict_out['BAIRONG_APPLY_LOAN_pdl_allnum_m06_cell']/dict_out['BAIRONG_APPLY_LOAN_pdl_allnum_m12_cell'] if dict_out['BAIRONG_APPLY_LOAN_pdl_allnum_m12_cell']>0 else np.nan
    dict_out['BAIRONG_APPLY_LOAN_nbank_cf_allnum_m12_idsubcell']= dict_out['BAIRONG_APPLY_LOAN_nbank_cf_allnum_m12_id']-dict_out['BAIRONG_APPLY_LOAN_nbank_cf_allnum_m12_cell']
    dict_out['BAIRONG_APPLY_LOAN_rel_orgnum_m01vm12_id']= dict_out['BAIRONG_APPLY_LOAN_rel_orgnum_m01_id']/dict_out['BAIRONG_APPLY_LOAN_rel_orgnum_m12_id'] if dict_out['BAIRONG_APPLY_LOAN_rel_orgnum_m12_id']>0 else np.nan
    dict_out['BAIRONG_APPLY_LOAN_nbank_nsloan_allnum_d15vm06_id']= dict_out['BAIRONG_APPLY_LOAN_nbank_nsloan_allnum_d15_id']/dict_out['BAIRONG_APPLY_LOAN_nbank_nsloan_allnum_m06_id'] if dict_out['BAIRONG_APPLY_LOAN_nbank_nsloan_allnum_m06_id']>0 else np.nan
    dict_out['BAIRONG_APPLY_LOAN_af_orgnum_m01vm06_cell']= dict_out['BAIRONG_APPLY_LOAN_af_orgnum_m01_cell']/dict_out['BAIRONG_APPLY_LOAN_af_orgnum_m06_cell'] if dict_out['BAIRONG_APPLY_LOAN_af_orgnum_m06_cell']>0 else np.nan
    dict_out['BAIRONG_APPLY_LOAN_nbank_oth_orgnum_d07vm01_id']= dict_out['BAIRONG_APPLY_LOAN_nbank_oth_orgnum_d07_id']/dict_out['BAIRONG_APPLY_LOAN_nbank_oth_orgnum_m01_id'] if dict_out['BAIRONG_APPLY_LOAN_nbank_oth_orgnum_m01_id']>0 else np.nan
    dict_out['BAIRONG_APPLY_LOAN_bank_orgnum_m01vm06_cell']= dict_out['BAIRONG_APPLY_LOAN_bank_orgnum_m01_cell']/dict_out['BAIRONG_APPLY_LOAN_bank_orgnum_m06_cell'] if dict_out['BAIRONG_APPLY_LOAN_bank_orgnum_m06_cell']>0 else np.nan
    dict_out['BAIRONG_APPLY_LOAN_nbank_week_allnum_m03vm12_cell']= dict_out['BAIRONG_APPLY_LOAN_nbank_week_allnum_m03_cell']/dict_out['BAIRONG_APPLY_LOAN_nbank_week_allnum_m12_cell'] if dict_out['BAIRONG_APPLY_LOAN_nbank_week_allnum_m12_cell']>0 else np.nan
    dict_out['BAIRONG_APPLY_LOAN_nbank_p2p_allnum_d15vm06_id']= dict_out['BAIRONG_APPLY_LOAN_nbank_p2p_allnum_d15_id']/dict_out['BAIRONG_APPLY_LOAN_nbank_p2p_allnum_m06_id'] if dict_out['BAIRONG_APPLY_LOAN_nbank_p2p_allnum_m06_id']>0 else np.nan
    dict_out['BAIRONG_APPLY_LOAN_nbank_night_orgnum_d07vm06_cell']= dict_out['BAIRONG_APPLY_LOAN_nbank_night_orgnum_d07_cell']/dict_out['BAIRONG_APPLY_LOAN_nbank_night_orgnum_m06_cell'] if dict_out['BAIRONG_APPLY_LOAN_nbank_night_orgnum_m06_cell']>0 else np.nan
    dict_out['BAIRONG_APPLY_LOAN_nbank_cons_allnum_d15vm12_cell']= dict_out['BAIRONG_APPLY_LOAN_nbank_cons_allnum_d15_cell']/dict_out['BAIRONG_APPLY_LOAN_nbank_cons_allnum_m12_cell'] if dict_out['BAIRONG_APPLY_LOAN_nbank_cons_allnum_m12_cell']>0 else np.nan
    dict_out['BAIRONG_APPLY_LOAN_caon_allnum_d07vm12_id']= dict_out['BAIRONG_APPLY_LOAN_caon_allnum_d07_id']/dict_out['BAIRONG_APPLY_LOAN_caon_allnum_m12_id'] if dict_out['BAIRONG_APPLY_LOAN_caon_allnum_m12_id']>0 else np.nan
    dict_out['BAIRONG_APPLY_LOAN_nbank_cons_orgnum_m01vm12_cell']= dict_out['BAIRONG_APPLY_LOAN_nbank_cons_orgnum_m01_cell']/dict_out['BAIRONG_APPLY_LOAN_nbank_cons_orgnum_m12_cell'] if dict_out['BAIRONG_APPLY_LOAN_nbank_cons_orgnum_m12_cell']>0 else np.nan
    dict_out['BAIRONG_APPLY_LOAN_nbank_cons_allnum_m01vm03_id']= dict_out['BAIRONG_APPLY_LOAN_nbank_cons_allnum_m01_id']/dict_out['BAIRONG_APPLY_LOAN_nbank_cons_allnum_m03_id'] if dict_out['BAIRONG_APPLY_LOAN_nbank_cons_allnum_m03_id']>0 else np.nan
    dict_out['BAIRONG_APPLY_LOAN_nbank_oth_allnum_d07vm06_id']= dict_out['BAIRONG_APPLY_LOAN_nbank_oth_allnum_d07_id']/dict_out['BAIRONG_APPLY_LOAN_nbank_oth_allnum_m06_id'] if dict_out['BAIRONG_APPLY_LOAN_nbank_oth_allnum_m06_id']>0 else np.nan
    dict_out['BAIRONG_APPLY_LOAN_nbank_ca_orgnum_d07vm12_cell']= dict_out['BAIRONG_APPLY_LOAN_nbank_ca_orgnum_d07_cell']/dict_out['BAIRONG_APPLY_LOAN_nbank_ca_orgnum_m12_cell'] if dict_out['BAIRONG_APPLY_LOAN_nbank_ca_orgnum_m12_cell']>0 else np.nan
    dict_out['BAIRONG_APPLY_LOAN_nbank_ca_orgnum_d07vm06_cell']= dict_out['BAIRONG_APPLY_LOAN_nbank_ca_orgnum_d07_cell']/dict_out['BAIRONG_APPLY_LOAN_nbank_ca_orgnum_m06_cell'] if dict_out['BAIRONG_APPLY_LOAN_nbank_ca_orgnum_m06_cell']>0 else np.nan
    dict_out['BAIRONG_APPLY_LOAN_nbank_mc_allnum_m03vm06_id']= dict_out['BAIRONG_APPLY_LOAN_nbank_mc_allnum_m03_id']/dict_out['BAIRONG_APPLY_LOAN_nbank_mc_allnum_m06_id'] if dict_out['BAIRONG_APPLY_LOAN_nbank_mc_allnum_m06_id']>0 else np.nan
    dict_out['BAIRONG_APPLY_LOAN_nbank_mc_orgnum_m03vm06_id']= dict_out['BAIRONG_APPLY_LOAN_nbank_mc_orgnum_m03_id']/dict_out['BAIRONG_APPLY_LOAN_nbank_mc_orgnum_m06_id'] if dict_out['BAIRONG_APPLY_LOAN_nbank_mc_orgnum_m06_id']>0 else np.nan
    dict_out['BAIRONG_APPLY_LOAN_nbank_mc_orgnum_m06_idsubcell']= dict_out['BAIRONG_APPLY_LOAN_nbank_mc_orgnum_m06_id']-dict_out['BAIRONG_APPLY_LOAN_nbank_mc_orgnum_m06_cell']
    dict_out['BAIRONG_APPLY_LOAN_rel_orgnum_d15vm06_id']= dict_out['BAIRONG_APPLY_LOAN_rel_orgnum_d15_id']/dict_out['BAIRONG_APPLY_LOAN_rel_orgnum_m06_id'] if dict_out['BAIRONG_APPLY_LOAN_rel_orgnum_m06_id']>0 else np.nan
    dict_out['BAIRONG_APPLY_LOAN_oth_allnum_d07vm06_cell']= dict_out['BAIRONG_APPLY_LOAN_oth_allnum_d07_cell']/dict_out['BAIRONG_APPLY_LOAN_oth_allnum_m06_cell'] if dict_out['BAIRONG_APPLY_LOAN_oth_allnum_m06_cell']>0 else np.nan
    dict_out['BAIRONG_APPLY_LOAN_nbank_avg_monnum_m03vm12_cell']= dict_out['BAIRONG_APPLY_LOAN_nbank_avg_monnum_m03_cell']/dict_out['BAIRONG_APPLY_LOAN_nbank_avg_monnum_m12_cell'] if dict_out['BAIRONG_APPLY_LOAN_nbank_avg_monnum_m12_cell']>0 else np.nan
    dict_out['BAIRONG_APPLY_LOAN_nbank_cf_orgnum_d07vm12_cell']= dict_out['BAIRONG_APPLY_LOAN_nbank_cf_orgnum_d07_cell']/dict_out['BAIRONG_APPLY_LOAN_nbank_cf_orgnum_m12_cell'] if dict_out['BAIRONG_APPLY_LOAN_nbank_cf_orgnum_m12_cell']>0 else np.nan
    dict_out['BAIRONG_APPLY_LOAN_rel_orgnum_m01vm12_cell']= dict_out['BAIRONG_APPLY_LOAN_rel_orgnum_m01_cell']/dict_out['BAIRONG_APPLY_LOAN_rel_orgnum_m12_cell'] if dict_out['BAIRONG_APPLY_LOAN_rel_orgnum_m12_cell']>0 else np.nan
    dict_out['BAIRONG_APPLY_LOAN_caon_allnum_m06vm12_id']= dict_out['BAIRONG_APPLY_LOAN_caon_allnum_m06_id']/dict_out['BAIRONG_APPLY_LOAN_caon_allnum_m12_id'] if dict_out['BAIRONG_APPLY_LOAN_caon_allnum_m12_id']>0 else np.nan
    dict_out['BAIRONG_APPLY_LOAN_nbank_ca_allnum_m01vm03_id']= dict_out['BAIRONG_APPLY_LOAN_nbank_ca_allnum_m01_id']/dict_out['BAIRONG_APPLY_LOAN_nbank_ca_allnum_m03_id'] if dict_out['BAIRONG_APPLY_LOAN_nbank_ca_allnum_m03_id']>0 else np.nan
    dict_out['BAIRONG_APPLY_LOAN_nbank_ca_allnum_m06_idsubcell']= dict_out['BAIRONG_APPLY_LOAN_nbank_ca_allnum_m06_id']-dict_out['BAIRONG_APPLY_LOAN_nbank_ca_allnum_m06_cell']
    dict_out['BAIRONG_APPLY_LOAN_nbank_ca_orgnum_d15vm01_cell']= dict_out['BAIRONG_APPLY_LOAN_nbank_ca_orgnum_d15_cell']/dict_out['BAIRONG_APPLY_LOAN_nbank_ca_orgnum_m01_cell'] if dict_out['BAIRONG_APPLY_LOAN_nbank_ca_orgnum_m01_cell']>0 else np.nan
    dict_out['BAIRONG_APPLY_LOAN_pdl_allnum_m06vm12_id']= dict_out['BAIRONG_APPLY_LOAN_pdl_allnum_m06_id']/dict_out['BAIRONG_APPLY_LOAN_pdl_allnum_m12_id'] if dict_out['BAIRONG_APPLY_LOAN_pdl_allnum_m12_id']>0 else np.nan
    dict_out['BAIRONG_APPLY_LOAN_nbank_else_orgnum_m01vm03_cell']= dict_out['BAIRONG_APPLY_LOAN_nbank_else_orgnum_m01_cell']/dict_out['BAIRONG_APPLY_LOAN_nbank_else_orgnum_m03_cell'] if dict_out['BAIRONG_APPLY_LOAN_nbank_else_orgnum_m03_cell']>0 else np.nan
    dict_out['BAIRONG_APPLY_LOAN_oth_orgnum_d07vm12_id']= dict_out['BAIRONG_APPLY_LOAN_oth_orgnum_d07_id']/dict_out['BAIRONG_APPLY_LOAN_oth_orgnum_m12_id'] if dict_out['BAIRONG_APPLY_LOAN_oth_orgnum_m12_id']>0 else np.nan
    dict_out['BAIRONG_APPLY_LOAN_bank_tra_orgnum_m06vm12_id']= dict_out['BAIRONG_APPLY_LOAN_bank_tra_orgnum_m06_id']/dict_out['BAIRONG_APPLY_LOAN_bank_tra_orgnum_m12_id'] if dict_out['BAIRONG_APPLY_LOAN_bank_tra_orgnum_m12_id']>0 else np.nan
    dict_out['BAIRONG_APPLY_LOAN_caoff_allnum_m03vm12_cell']= dict_out['BAIRONG_APPLY_LOAN_caoff_allnum_m03_cell']/dict_out['BAIRONG_APPLY_LOAN_caoff_allnum_m12_cell'] if dict_out['BAIRONG_APPLY_LOAN_caoff_allnum_m12_cell']>0 else np.nan
    dict_out['BAIRONG_APPLY_LOAN_bank_ret_allnum_m06vm12_id']= dict_out['BAIRONG_APPLY_LOAN_bank_ret_allnum_m06_id']/dict_out['BAIRONG_APPLY_LOAN_bank_ret_allnum_m12_id'] if dict_out['BAIRONG_APPLY_LOAN_bank_ret_allnum_m12_id']>0 else np.nan
    dict_out['BAIRONG_APPLY_LOAN_nbank_oth_orgnum_m03vm06_cell']= dict_out['BAIRONG_APPLY_LOAN_nbank_oth_orgnum_m03_cell']/dict_out['BAIRONG_APPLY_LOAN_nbank_oth_orgnum_m06_cell'] if dict_out['BAIRONG_APPLY_LOAN_nbank_oth_orgnum_m06_cell']>0 else np.nan
    dict_out['BAIRONG_APPLY_LOAN_bank_allnum_m03vm06_id']= dict_out['BAIRONG_APPLY_LOAN_bank_allnum_m03_id']/dict_out['BAIRONG_APPLY_LOAN_bank_allnum_m06_id'] if dict_out['BAIRONG_APPLY_LOAN_bank_allnum_m06_id']>0 else np.nan
    dict_out['BAIRONG_APPLY_LOAN_bank_tra_orgnum_d15vm12_id']= dict_out['BAIRONG_APPLY_LOAN_bank_tra_orgnum_d15_id']/dict_out['BAIRONG_APPLY_LOAN_bank_tra_orgnum_m12_id'] if dict_out['BAIRONG_APPLY_LOAN_bank_tra_orgnum_m12_id']>0 else np.nan
    dict_out['BAIRONG_APPLY_LOAN_nbank_p2p_orgnum_m01vm12_cell']= dict_out['BAIRONG_APPLY_LOAN_nbank_p2p_orgnum_m01_cell']/dict_out['BAIRONG_APPLY_LOAN_nbank_p2p_orgnum_m12_cell'] if dict_out['BAIRONG_APPLY_LOAN_nbank_p2p_orgnum_m12_cell']>0 else np.nan
    dict_out['BAIRONG_APPLY_LOAN_nbank_nsloan_orgnum_m03vm06_id']= dict_out['BAIRONG_APPLY_LOAN_nbank_nsloan_orgnum_m03_id']/dict_out['BAIRONG_APPLY_LOAN_nbank_nsloan_orgnum_m06_id'] if dict_out['BAIRONG_APPLY_LOAN_nbank_nsloan_orgnum_m06_id']>0 else np.nan
    dict_out['BAIRONG_APPLY_LOAN_caon_orgnum_m01vm03_cell']= dict_out['BAIRONG_APPLY_LOAN_caon_orgnum_m01_cell']/dict_out['BAIRONG_APPLY_LOAN_caon_orgnum_m03_cell'] if dict_out['BAIRONG_APPLY_LOAN_caon_orgnum_m03_cell']>0 else np.nan
    dict_out['BAIRONG_APPLY_LOAN_pdl_allnum_d15vm12_id']= dict_out['BAIRONG_APPLY_LOAN_pdl_allnum_d15_id']/dict_out['BAIRONG_APPLY_LOAN_pdl_allnum_m12_id'] if dict_out['BAIRONG_APPLY_LOAN_pdl_allnum_m12_id']>0 else np.nan
    dict_out['BAIRONG_APPLY_LOAN_nbank_week_allnum_d07vm12_id']= dict_out['BAIRONG_APPLY_LOAN_nbank_week_allnum_d07_id']/dict_out['BAIRONG_APPLY_LOAN_nbank_week_allnum_m12_id'] if dict_out['BAIRONG_APPLY_LOAN_nbank_week_allnum_m12_id']>0 else np.nan
    dict_out['BAIRONG_APPLY_LOAN_bank_tra_orgnum_m03vm12_cell']= dict_out['BAIRONG_APPLY_LOAN_bank_tra_orgnum_m03_cell']/dict_out['BAIRONG_APPLY_LOAN_bank_tra_orgnum_m12_cell'] if dict_out['BAIRONG_APPLY_LOAN_bank_tra_orgnum_m12_cell']>0 else np.nan
    dict_out['BAIRONG_APPLY_LOAN_cooff_allnum_m01vm06_cell']= dict_out['BAIRONG_APPLY_LOAN_cooff_allnum_m01_cell']/dict_out['BAIRONG_APPLY_LOAN_cooff_allnum_m06_cell'] if dict_out['BAIRONG_APPLY_LOAN_cooff_allnum_m06_cell']>0 else np.nan
    dict_out['BAIRONG_APPLY_LOAN_bank_ret_orgnum_m03_idsubcell']= dict_out['BAIRONG_APPLY_LOAN_bank_ret_orgnum_m03_id']-dict_out['BAIRONG_APPLY_LOAN_bank_ret_orgnum_m03_cell']
    dict_out['BAIRONG_APPLY_LOAN_nbank_finlea_allnum_m12_idsubcell']= dict_out['BAIRONG_APPLY_LOAN_nbank_finlea_allnum_m12_id']-dict_out['BAIRONG_APPLY_LOAN_nbank_finlea_allnum_m12_cell']
    dict_out['BAIRONG_APPLY_LOAN_pdl_orgnum_m03vm06_id']= dict_out['BAIRONG_APPLY_LOAN_pdl_orgnum_m03_id']/dict_out['BAIRONG_APPLY_LOAN_pdl_orgnum_m06_id'] if dict_out['BAIRONG_APPLY_LOAN_pdl_orgnum_m06_id']>0 else np.nan
    dict_out['BAIRONG_APPLY_LOAN_bank_max_inteday_m03vm06_cell']= dict_out['BAIRONG_APPLY_LOAN_bank_max_inteday_m03_cell']/dict_out['BAIRONG_APPLY_LOAN_bank_max_inteday_m06_cell'] if dict_out['BAIRONG_APPLY_LOAN_bank_max_inteday_m06_cell']>0 else np.nan
    dict_out['BAIRONG_APPLY_LOAN_bank_allnum_m03vm12_cell']= dict_out['BAIRONG_APPLY_LOAN_bank_allnum_m03_cell']/dict_out['BAIRONG_APPLY_LOAN_bank_allnum_m12_cell'] if dict_out['BAIRONG_APPLY_LOAN_bank_allnum_m12_cell']>0 else np.nan
    dict_out['BAIRONG_APPLY_LOAN_nbank_cons_orgnum_m06vm12_id']= dict_out['BAIRONG_APPLY_LOAN_nbank_cons_orgnum_m06_id']/dict_out['BAIRONG_APPLY_LOAN_nbank_cons_orgnum_m12_id'] if dict_out['BAIRONG_APPLY_LOAN_nbank_cons_orgnum_m12_id']>0 else np.nan
    dict_out['BAIRONG_APPLY_LOAN_max_inteday_m06_idsubcell']= dict_out['BAIRONG_APPLY_LOAN_max_inteday_m06_id']-dict_out['BAIRONG_APPLY_LOAN_max_inteday_m06_cell']
    dict_out['BAIRONG_APPLY_LOAN_nbank_cons_orgnum_d15vm12_id']= dict_out['BAIRONG_APPLY_LOAN_nbank_cons_orgnum_d15_id']/dict_out['BAIRONG_APPLY_LOAN_nbank_cons_orgnum_m12_id'] if dict_out['BAIRONG_APPLY_LOAN_nbank_cons_orgnum_m12_id']>0 else np.nan
    dict_out['BAIRONG_APPLY_LOAN_bank_max_inteday_m03vm12_cell']= dict_out['BAIRONG_APPLY_LOAN_bank_max_inteday_m03_cell']/dict_out['BAIRONG_APPLY_LOAN_bank_max_inteday_m12_cell'] if dict_out['BAIRONG_APPLY_LOAN_bank_max_inteday_m12_cell']>0 else np.nan
    dict_out['BAIRONG_APPLY_LOAN_nbank_oth_allnum_m06vm12_cell']= dict_out['BAIRONG_APPLY_LOAN_nbank_oth_allnum_m06_cell']/dict_out['BAIRONG_APPLY_LOAN_nbank_oth_allnum_m12_cell'] if dict_out['BAIRONG_APPLY_LOAN_nbank_oth_allnum_m12_cell']>0 else np.nan
    dict_out['BAIRONG_APPLY_LOAN_nbank_allnum_d15vm12_cell']= dict_out['BAIRONG_APPLY_LOAN_nbank_allnum_d15_cell']/dict_out['BAIRONG_APPLY_LOAN_nbank_allnum_m12_cell'] if dict_out['BAIRONG_APPLY_LOAN_nbank_allnum_m12_cell']>0 else np.nan
    dict_out['BAIRONG_APPLY_LOAN_rel_allnum_m06_idsubcell']= dict_out['BAIRONG_APPLY_LOAN_rel_allnum_m06_id']-dict_out['BAIRONG_APPLY_LOAN_rel_allnum_m06_cell']
    dict_out['BAIRONG_APPLY_LOAN_coon_allnum_m06vm12_cell']= dict_out['BAIRONG_APPLY_LOAN_coon_allnum_m06_cell']/dict_out['BAIRONG_APPLY_LOAN_coon_allnum_m12_cell'] if dict_out['BAIRONG_APPLY_LOAN_coon_allnum_m12_cell']>0 else np.nan
    dict_out['BAIRONG_APPLY_LOAN_nbank_nsloan_allnum_m01vm03_cell']= dict_out['BAIRONG_APPLY_LOAN_nbank_nsloan_allnum_m01_cell']/dict_out['BAIRONG_APPLY_LOAN_nbank_nsloan_allnum_m03_cell'] if dict_out['BAIRONG_APPLY_LOAN_nbank_nsloan_allnum_m03_cell']>0 else np.nan
    dict_out['BAIRONG_APPLY_LOAN_pdl_allnum_d07vm06_id']= dict_out['BAIRONG_APPLY_LOAN_pdl_allnum_d07_id']/dict_out['BAIRONG_APPLY_LOAN_pdl_allnum_m06_id'] if dict_out['BAIRONG_APPLY_LOAN_pdl_allnum_m06_id']>0 else np.nan
    dict_out['BAIRONG_APPLY_LOAN_nbank_ca_allnum_d07vm12_id']= dict_out['BAIRONG_APPLY_LOAN_nbank_ca_allnum_d07_id']/dict_out['BAIRONG_APPLY_LOAN_nbank_ca_allnum_m12_id'] if dict_out['BAIRONG_APPLY_LOAN_nbank_ca_allnum_m12_id']>0 else np.nan
    dict_out['BAIRONG_APPLY_LOAN_pdl_orgnum_d07vm12_id']= dict_out['BAIRONG_APPLY_LOAN_pdl_orgnum_d07_id']/dict_out['BAIRONG_APPLY_LOAN_pdl_orgnum_m12_id'] if dict_out['BAIRONG_APPLY_LOAN_pdl_orgnum_m12_id']>0 else np.nan
    dict_out['BAIRONG_APPLY_LOAN_nbank_mc_allnum_m06vm12_cell']= dict_out['BAIRONG_APPLY_LOAN_nbank_mc_allnum_m06_cell']/dict_out['BAIRONG_APPLY_LOAN_nbank_mc_allnum_m12_cell'] if dict_out['BAIRONG_APPLY_LOAN_nbank_mc_allnum_m12_cell']>0 else np.nan
    dict_out['BAIRONG_APPLY_LOAN_cooff_orgnum_d07vm01_cell']= dict_out['BAIRONG_APPLY_LOAN_cooff_orgnum_d07_cell']/dict_out['BAIRONG_APPLY_LOAN_cooff_orgnum_m01_cell'] if dict_out['BAIRONG_APPLY_LOAN_cooff_orgnum_m01_cell']>0 else np.nan
    dict_out['BAIRONG_APPLY_LOAN_nbank_p2p_allnum_m01vm12_id']= dict_out['BAIRONG_APPLY_LOAN_nbank_p2p_allnum_m01_id']/dict_out['BAIRONG_APPLY_LOAN_nbank_p2p_allnum_m12_id'] if dict_out['BAIRONG_APPLY_LOAN_nbank_p2p_allnum_m12_id']>0 else np.nan
    dict_out['BAIRONG_APPLY_LOAN_rel_orgnum_d07vm12_cell']= dict_out['BAIRONG_APPLY_LOAN_rel_orgnum_d07_cell']/dict_out['BAIRONG_APPLY_LOAN_rel_orgnum_m12_cell'] if dict_out['BAIRONG_APPLY_LOAN_rel_orgnum_m12_cell']>0 else np.nan
    dict_out['BAIRONG_APPLY_LOAN_nbank_sloan_orgnum_d07vm06_cell']= dict_out['BAIRONG_APPLY_LOAN_nbank_sloan_orgnum_d07_cell']/dict_out['BAIRONG_APPLY_LOAN_nbank_sloan_orgnum_m06_cell'] if dict_out['BAIRONG_APPLY_LOAN_nbank_sloan_orgnum_m06_cell']>0 else np.nan
    dict_out['BAIRONG_APPLY_LOAN_bank_avg_monnum_m03vm06_cell']= dict_out['BAIRONG_APPLY_LOAN_bank_avg_monnum_m03_cell']/dict_out['BAIRONG_APPLY_LOAN_bank_avg_monnum_m06_cell'] if dict_out['BAIRONG_APPLY_LOAN_bank_avg_monnum_m06_cell']>0 else np.nan
    dict_out['BAIRONG_APPLY_LOAN_nbank_ca_allnum_d07vd15_cell']= dict_out['BAIRONG_APPLY_LOAN_nbank_ca_allnum_d07_cell']/dict_out['BAIRONG_APPLY_LOAN_nbank_ca_allnum_d15_cell'] if dict_out['BAIRONG_APPLY_LOAN_nbank_ca_allnum_d15_cell']>0 else np.nan
    dict_out['BAIRONG_APPLY_LOAN_nbank_cons_allnum_d07vm12_cell']= dict_out['BAIRONG_APPLY_LOAN_nbank_cons_allnum_d07_cell']/dict_out['BAIRONG_APPLY_LOAN_nbank_cons_allnum_m12_cell'] if dict_out['BAIRONG_APPLY_LOAN_nbank_cons_allnum_m12_cell']>0 else np.nan
    dict_out['BAIRONG_APPLY_LOAN_af_allnum_m01vm12_id']= dict_out['BAIRONG_APPLY_LOAN_af_allnum_m01_id']/dict_out['BAIRONG_APPLY_LOAN_af_allnum_m12_id'] if dict_out['BAIRONG_APPLY_LOAN_af_allnum_m12_id']>0 else np.nan
    dict_out['BAIRONG_APPLY_LOAN_rel_orgnum_d15vm06_cell']= dict_out['BAIRONG_APPLY_LOAN_rel_orgnum_d15_cell']/dict_out['BAIRONG_APPLY_LOAN_rel_orgnum_m06_cell'] if dict_out['BAIRONG_APPLY_LOAN_rel_orgnum_m06_cell']>0 else np.nan
    dict_out['BAIRONG_APPLY_LOAN_nbank_finlea_allnum_m01vm06_cell']= dict_out['BAIRONG_APPLY_LOAN_nbank_finlea_allnum_m01_cell']/dict_out['BAIRONG_APPLY_LOAN_nbank_finlea_allnum_m06_cell'] if dict_out['BAIRONG_APPLY_LOAN_nbank_finlea_allnum_m06_cell']>0 else np.nan
    dict_out['BAIRONG_APPLY_LOAN_nbank_cf_allnum_d07vm01_cell']= dict_out['BAIRONG_APPLY_LOAN_nbank_cf_allnum_d07_cell']/dict_out['BAIRONG_APPLY_LOAN_nbank_cf_allnum_m01_cell'] if dict_out['BAIRONG_APPLY_LOAN_nbank_cf_allnum_m01_cell']>0 else np.nan
    dict_out['BAIRONG_APPLY_LOAN_bank_orgnum_m01vm03_cell']= dict_out['BAIRONG_APPLY_LOAN_bank_orgnum_m01_cell']/dict_out['BAIRONG_APPLY_LOAN_bank_orgnum_m03_cell'] if dict_out['BAIRONG_APPLY_LOAN_bank_orgnum_m03_cell']>0 else np.nan
    dict_out['BAIRONG_APPLY_LOAN_nbank_night_orgnum_m03vm12_cell']= dict_out['BAIRONG_APPLY_LOAN_nbank_night_orgnum_m03_cell']/dict_out['BAIRONG_APPLY_LOAN_nbank_night_orgnum_m12_cell'] if dict_out['BAIRONG_APPLY_LOAN_nbank_night_orgnum_m12_cell']>0 else np.nan
    dict_out['BAIRONG_APPLY_LOAN_oth_allnum_m12_idsubcell']= dict_out['BAIRONG_APPLY_LOAN_oth_allnum_m12_id']-dict_out['BAIRONG_APPLY_LOAN_oth_allnum_m12_cell']
    dict_out['BAIRONG_APPLY_LOAN_nbank_ca_allnum_d15vm12_cell']= dict_out['BAIRONG_APPLY_LOAN_nbank_ca_allnum_d15_cell']/dict_out['BAIRONG_APPLY_LOAN_nbank_ca_allnum_m12_cell'] if dict_out['BAIRONG_APPLY_LOAN_nbank_ca_allnum_m12_cell']>0 else np.nan
    dict_out['BAIRONG_APPLY_LOAN_caon_orgnum_m01vm12_cell']= dict_out['BAIRONG_APPLY_LOAN_caon_orgnum_m01_cell']/dict_out['BAIRONG_APPLY_LOAN_caon_orgnum_m12_cell'] if dict_out['BAIRONG_APPLY_LOAN_caon_orgnum_m12_cell']>0 else np.nan
    dict_out['BAIRONG_APPLY_LOAN_nbank_cf_allnum_m01vm06_cell']= dict_out['BAIRONG_APPLY_LOAN_nbank_cf_allnum_m01_cell']/dict_out['BAIRONG_APPLY_LOAN_nbank_cf_allnum_m06_cell'] if dict_out['BAIRONG_APPLY_LOAN_nbank_cf_allnum_m06_cell']>0 else np.nan
    dict_out['BAIRONG_APPLY_LOAN_nbank_nsloan_orgnum_d07vm01_cell']= dict_out['BAIRONG_APPLY_LOAN_nbank_nsloan_orgnum_d07_cell']/dict_out['BAIRONG_APPLY_LOAN_nbank_nsloan_orgnum_m01_cell'] if dict_out['BAIRONG_APPLY_LOAN_nbank_nsloan_orgnum_m01_cell']>0 else np.nan
    dict_out['BAIRONG_APPLY_LOAN_nbank_ca_allnum_m06vm12_id']= dict_out['BAIRONG_APPLY_LOAN_nbank_ca_allnum_m06_id']/dict_out['BAIRONG_APPLY_LOAN_nbank_ca_allnum_m12_id'] if dict_out['BAIRONG_APPLY_LOAN_nbank_ca_allnum_m12_id']>0 else np.nan
    dict_out['BAIRONG_APPLY_LOAN_nbank_ca_orgnum_d07vd15_cell']= dict_out['BAIRONG_APPLY_LOAN_nbank_ca_orgnum_d07_cell']/dict_out['BAIRONG_APPLY_LOAN_nbank_ca_orgnum_d15_cell'] if dict_out['BAIRONG_APPLY_LOAN_nbank_ca_orgnum_d15_cell']>0 else np.nan
    dict_out['BAIRONG_APPLY_LOAN_oth_allnum_m01vm03_id']= dict_out['BAIRONG_APPLY_LOAN_oth_allnum_m01_id']/dict_out['BAIRONG_APPLY_LOAN_oth_allnum_m03_id'] if dict_out['BAIRONG_APPLY_LOAN_oth_allnum_m03_id']>0 else np.nan
    dict_out['BAIRONG_APPLY_LOAN_nbank_night_allnum_m01vm03_id']= dict_out['BAIRONG_APPLY_LOAN_nbank_night_allnum_m01_id']/dict_out['BAIRONG_APPLY_LOAN_nbank_night_allnum_m03_id'] if dict_out['BAIRONG_APPLY_LOAN_nbank_night_allnum_m03_id']>0 else np.nan
    dict_out['BAIRONG_APPLY_LOAN_nbank_ca_orgnum_d07vm01_cell']= dict_out['BAIRONG_APPLY_LOAN_nbank_ca_orgnum_d07_cell']/dict_out['BAIRONG_APPLY_LOAN_nbank_ca_orgnum_m01_cell'] if dict_out['BAIRONG_APPLY_LOAN_nbank_ca_orgnum_m01_cell']>0 else np.nan
    dict_out['BAIRONG_APPLY_LOAN_nbank_sloan_orgnum_m01vm12_id']= dict_out['BAIRONG_APPLY_LOAN_nbank_sloan_orgnum_m01_id']/dict_out['BAIRONG_APPLY_LOAN_nbank_sloan_orgnum_m12_id'] if dict_out['BAIRONG_APPLY_LOAN_nbank_sloan_orgnum_m12_id']>0 else np.nan
    dict_out['BAIRONG_APPLY_LOAN_nbank_avg_monnum_m03vm06_cell']= dict_out['BAIRONG_APPLY_LOAN_nbank_avg_monnum_m03_cell']/dict_out['BAIRONG_APPLY_LOAN_nbank_avg_monnum_m06_cell'] if dict_out['BAIRONG_APPLY_LOAN_nbank_avg_monnum_m06_cell']>0 else np.nan
    dict_out['BAIRONG_APPLY_LOAN_caoff_orgnum_d15vm06_cell']= dict_out['BAIRONG_APPLY_LOAN_caoff_orgnum_d15_cell']/dict_out['BAIRONG_APPLY_LOAN_caoff_orgnum_m06_cell'] if dict_out['BAIRONG_APPLY_LOAN_caoff_orgnum_m06_cell']>0 else np.nan
    dict_out['BAIRONG_APPLY_LOAN_nbank_ca_allnum_m01vm06_id']= dict_out['BAIRONG_APPLY_LOAN_nbank_ca_allnum_m01_id']/dict_out['BAIRONG_APPLY_LOAN_nbank_ca_allnum_m06_id'] if dict_out['BAIRONG_APPLY_LOAN_nbank_ca_allnum_m06_id']>0 else np.nan
    dict_out['BAIRONG_APPLY_LOAN_max_inteday_m12_idsubcell']= dict_out['BAIRONG_APPLY_LOAN_max_inteday_m12_id']-dict_out['BAIRONG_APPLY_LOAN_max_inteday_m12_cell']
    dict_out['BAIRONG_APPLY_LOAN_nbank_finlea_allnum_m06vm12_id']= dict_out['BAIRONG_APPLY_LOAN_nbank_finlea_allnum_m06_id']/dict_out['BAIRONG_APPLY_LOAN_nbank_finlea_allnum_m12_id'] if dict_out['BAIRONG_APPLY_LOAN_nbank_finlea_allnum_m12_id']>0 else np.nan
    dict_out['BAIRONG_APPLY_LOAN_bank_night_allnum_m06vm12_id']= dict_out['BAIRONG_APPLY_LOAN_bank_night_allnum_m06_id']/dict_out['BAIRONG_APPLY_LOAN_bank_night_allnum_m12_id'] if dict_out['BAIRONG_APPLY_LOAN_bank_night_allnum_m12_id']>0 else np.nan
    dict_out['BAIRONG_APPLY_LOAN_nbank_else_allnum_m01vm12_cell']= dict_out['BAIRONG_APPLY_LOAN_nbank_else_allnum_m01_cell']/dict_out['BAIRONG_APPLY_LOAN_nbank_else_allnum_m12_cell'] if dict_out['BAIRONG_APPLY_LOAN_nbank_else_allnum_m12_cell']>0 else np.nan
    dict_out['BAIRONG_APPLY_LOAN_nbank_else_orgnum_m03vm12_id']= dict_out['BAIRONG_APPLY_LOAN_nbank_else_orgnum_m03_id']/dict_out['BAIRONG_APPLY_LOAN_nbank_else_orgnum_m12_id'] if dict_out['BAIRONG_APPLY_LOAN_nbank_else_orgnum_m12_id']>0 else np.nan
    dict_out['BAIRONG_APPLY_LOAN_nbank_sloan_allnum_m01vm03_cell']= dict_out['BAIRONG_APPLY_LOAN_nbank_sloan_allnum_m01_cell']/dict_out['BAIRONG_APPLY_LOAN_nbank_sloan_allnum_m03_cell'] if dict_out['BAIRONG_APPLY_LOAN_nbank_sloan_allnum_m03_cell']>0 else np.nan
    dict_out['BAIRONG_APPLY_LOAN_nbank_cf_orgnum_d07vm12_id']= dict_out['BAIRONG_APPLY_LOAN_nbank_cf_orgnum_d07_id']/dict_out['BAIRONG_APPLY_LOAN_nbank_cf_orgnum_m12_id'] if dict_out['BAIRONG_APPLY_LOAN_nbank_cf_orgnum_m12_id']>0 else np.nan
    dict_out['BAIRONG_APPLY_LOAN_nbank_night_orgnum_m06vm12_cell']= dict_out['BAIRONG_APPLY_LOAN_nbank_night_orgnum_m06_cell']/dict_out['BAIRONG_APPLY_LOAN_nbank_night_orgnum_m12_cell'] if dict_out['BAIRONG_APPLY_LOAN_nbank_night_orgnum_m12_cell']>0 else np.nan
    dict_out['BAIRONG_APPLY_LOAN_caon_orgnum_m03vm12_cell']= dict_out['BAIRONG_APPLY_LOAN_caon_orgnum_m03_cell']/dict_out['BAIRONG_APPLY_LOAN_caon_orgnum_m12_cell'] if dict_out['BAIRONG_APPLY_LOAN_caon_orgnum_m12_cell']>0 else np.nan
    dict_out['BAIRONG_APPLY_LOAN_nbank_nsloan_orgnum_m03_idsubcell']= dict_out['BAIRONG_APPLY_LOAN_nbank_nsloan_orgnum_m03_id']-dict_out['BAIRONG_APPLY_LOAN_nbank_nsloan_orgnum_m03_cell']
    dict_out['BAIRONG_APPLY_LOAN_caoff_orgnum_d07vm12_cell']= dict_out['BAIRONG_APPLY_LOAN_caoff_orgnum_d07_cell']/dict_out['BAIRONG_APPLY_LOAN_caoff_orgnum_m12_cell'] if dict_out['BAIRONG_APPLY_LOAN_caoff_orgnum_m12_cell']>0 else np.nan
    dict_out['BAIRONG_APPLY_LOAN_nbank_allnum_d07vm12_cell']= dict_out['BAIRONG_APPLY_LOAN_nbank_allnum_d07_cell']/dict_out['BAIRONG_APPLY_LOAN_nbank_allnum_m12_cell'] if dict_out['BAIRONG_APPLY_LOAN_nbank_allnum_m12_cell']>0 else np.nan
    dict_out['BAIRONG_APPLY_LOAN_caoff_allnum_m12_idsubcell']= dict_out['BAIRONG_APPLY_LOAN_caoff_allnum_m12_id']-dict_out['BAIRONG_APPLY_LOAN_caoff_allnum_m12_cell']
    dict_out['BAIRONG_APPLY_LOAN_nbank_nsloan_allnum_m12_idsubcell']= dict_out['BAIRONG_APPLY_LOAN_nbank_nsloan_allnum_m12_id']-dict_out['BAIRONG_APPLY_LOAN_nbank_nsloan_allnum_m12_cell']
    dict_out['BAIRONG_APPLY_LOAN_nbank_night_allnum_m01vm06_cell']= dict_out['BAIRONG_APPLY_LOAN_nbank_night_allnum_m01_cell']/dict_out['BAIRONG_APPLY_LOAN_nbank_night_allnum_m06_cell'] if dict_out['BAIRONG_APPLY_LOAN_nbank_night_allnum_m06_cell']>0 else np.nan
    dict_out['BAIRONG_APPLY_LOAN_nbank_sloan_orgnum_d07vd15_cell']= dict_out['BAIRONG_APPLY_LOAN_nbank_sloan_orgnum_d07_cell']/dict_out['BAIRONG_APPLY_LOAN_nbank_sloan_orgnum_d15_cell'] if dict_out['BAIRONG_APPLY_LOAN_nbank_sloan_orgnum_d15_cell']>0 else np.nan
    dict_out['BAIRONG_APPLY_LOAN_nbank_week_allnum_d15vm01_id']= dict_out['BAIRONG_APPLY_LOAN_nbank_week_allnum_d15_id']/dict_out['BAIRONG_APPLY_LOAN_nbank_week_allnum_m01_id'] if dict_out['BAIRONG_APPLY_LOAN_nbank_week_allnum_m01_id']>0 else np.nan
    dict_out['BAIRONG_APPLY_LOAN_nbank_nsloan_orgnum_d07vm06_cell']= dict_out['BAIRONG_APPLY_LOAN_nbank_nsloan_orgnum_d07_cell']/dict_out['BAIRONG_APPLY_LOAN_nbank_nsloan_orgnum_m06_cell'] if dict_out['BAIRONG_APPLY_LOAN_nbank_nsloan_orgnum_m06_cell']>0 else np.nan
    dict_out['BAIRONG_APPLY_LOAN_min_monnum_m03vm06_cell']= dict_out['BAIRONG_APPLY_LOAN_min_monnum_m03_cell']/dict_out['BAIRONG_APPLY_LOAN_min_monnum_m06_cell'] if dict_out['BAIRONG_APPLY_LOAN_min_monnum_m06_cell']>0 else np.nan
    dict_out['BAIRONG_APPLY_LOAN_bank_tot_mons_m12_idsubcell']= dict_out['BAIRONG_APPLY_LOAN_bank_tot_mons_m12_id']-dict_out['BAIRONG_APPLY_LOAN_bank_tot_mons_m12_cell']
    dict_out['BAIRONG_APPLY_LOAN_oth_allnum_d07vm12_id']= dict_out['BAIRONG_APPLY_LOAN_oth_allnum_d07_id']/dict_out['BAIRONG_APPLY_LOAN_oth_allnum_m12_id'] if dict_out['BAIRONG_APPLY_LOAN_oth_allnum_m12_id']>0 else np.nan
    dict_out['BAIRONG_APPLY_LOAN_nbank_oth_allnum_d07vm12_cell']= dict_out['BAIRONG_APPLY_LOAN_nbank_oth_allnum_d07_cell']/dict_out['BAIRONG_APPLY_LOAN_nbank_oth_allnum_m12_cell'] if dict_out['BAIRONG_APPLY_LOAN_nbank_oth_allnum_m12_cell']>0 else np.nan
    dict_out['BAIRONG_APPLY_LOAN_oth_allnum_d15vm01_cell']= dict_out['BAIRONG_APPLY_LOAN_oth_allnum_d15_cell']/dict_out['BAIRONG_APPLY_LOAN_oth_allnum_m01_cell'] if dict_out['BAIRONG_APPLY_LOAN_oth_allnum_m01_cell']>0 else np.nan
    dict_out['BAIRONG_APPLY_LOAN_pdl_orgnum_d07vm06_cell']= dict_out['BAIRONG_APPLY_LOAN_pdl_orgnum_d07_cell']/dict_out['BAIRONG_APPLY_LOAN_pdl_orgnum_m06_cell'] if dict_out['BAIRONG_APPLY_LOAN_pdl_orgnum_m06_cell']>0 else np.nan
    dict_out['BAIRONG_APPLY_LOAN_nbank_sloan_orgnum_m01vm06_cell']= dict_out['BAIRONG_APPLY_LOAN_nbank_sloan_orgnum_m01_cell']/dict_out['BAIRONG_APPLY_LOAN_nbank_sloan_orgnum_m06_cell'] if dict_out['BAIRONG_APPLY_LOAN_nbank_sloan_orgnum_m06_cell']>0 else np.nan
    dict_out['BAIRONG_APPLY_LOAN_nbank_nsloan_orgnum_d15vm06_id']= dict_out['BAIRONG_APPLY_LOAN_nbank_nsloan_orgnum_d15_id']/dict_out['BAIRONG_APPLY_LOAN_nbank_nsloan_orgnum_m06_id'] if dict_out['BAIRONG_APPLY_LOAN_nbank_nsloan_orgnum_m06_id']>0 else np.nan
    dict_out['BAIRONG_APPLY_LOAN_nbank_night_orgnum_m01vm06_cell']= dict_out['BAIRONG_APPLY_LOAN_nbank_night_orgnum_m01_cell']/dict_out['BAIRONG_APPLY_LOAN_nbank_night_orgnum_m06_cell'] if dict_out['BAIRONG_APPLY_LOAN_nbank_night_orgnum_m06_cell']>0 else np.nan
    dict_out['BAIRONG_APPLY_LOAN_caoff_allnum_m01vm12_cell']= dict_out['BAIRONG_APPLY_LOAN_caoff_allnum_m01_cell']/dict_out['BAIRONG_APPLY_LOAN_caoff_allnum_m12_cell'] if dict_out['BAIRONG_APPLY_LOAN_caoff_allnum_m12_cell']>0 else np.nan
    dict_out['BAIRONG_APPLY_LOAN_nbank_cf_allnum_m01vm12_id']= dict_out['BAIRONG_APPLY_LOAN_nbank_cf_allnum_m01_id']/dict_out['BAIRONG_APPLY_LOAN_nbank_cf_allnum_m12_id'] if dict_out['BAIRONG_APPLY_LOAN_nbank_cf_allnum_m12_id']>0 else np.nan
    dict_out['BAIRONG_APPLY_LOAN_nbank_cf_orgnum_d15vm01_id']= dict_out['BAIRONG_APPLY_LOAN_nbank_cf_orgnum_d15_id']/dict_out['BAIRONG_APPLY_LOAN_nbank_cf_orgnum_m01_id'] if dict_out['BAIRONG_APPLY_LOAN_nbank_cf_orgnum_m01_id']>0 else np.nan
    dict_out['BAIRONG_APPLY_LOAN_nbank_else_allnum_m01vm06_id']= dict_out['BAIRONG_APPLY_LOAN_nbank_else_allnum_m01_id']/dict_out['BAIRONG_APPLY_LOAN_nbank_else_allnum_m06_id'] if dict_out['BAIRONG_APPLY_LOAN_nbank_else_allnum_m06_id']>0 else np.nan
    dict_out['BAIRONG_APPLY_LOAN_nbank_p2p_allnum_d07vm12_cell']= dict_out['BAIRONG_APPLY_LOAN_nbank_p2p_allnum_d07_cell']/dict_out['BAIRONG_APPLY_LOAN_nbank_p2p_allnum_m12_cell'] if dict_out['BAIRONG_APPLY_LOAN_nbank_p2p_allnum_m12_cell']>0 else np.nan
    dict_out['BAIRONG_APPLY_LOAN_nbank_nsloan_allnum_d15vm01_cell']= dict_out['BAIRONG_APPLY_LOAN_nbank_nsloan_allnum_d15_cell']/dict_out['BAIRONG_APPLY_LOAN_nbank_nsloan_allnum_m01_cell'] if dict_out['BAIRONG_APPLY_LOAN_nbank_nsloan_allnum_m01_cell']>0 else np.nan
    dict_out['BAIRONG_APPLY_LOAN_nbank_mc_orgnum_m03vm12_cell']= dict_out['BAIRONG_APPLY_LOAN_nbank_mc_orgnum_m03_cell']/dict_out['BAIRONG_APPLY_LOAN_nbank_mc_orgnum_m12_cell'] if dict_out['BAIRONG_APPLY_LOAN_nbank_mc_orgnum_m12_cell']>0 else np.nan
    dict_out['BAIRONG_APPLY_LOAN_nbank_night_allnum_m01vm03_cell']= dict_out['BAIRONG_APPLY_LOAN_nbank_night_allnum_m01_cell']/dict_out['BAIRONG_APPLY_LOAN_nbank_night_allnum_m03_cell'] if dict_out['BAIRONG_APPLY_LOAN_nbank_night_allnum_m03_cell']>0 else np.nan
    dict_out['BAIRONG_APPLY_LOAN_caoff_orgnum_m01vm03_cell']= dict_out['BAIRONG_APPLY_LOAN_caoff_orgnum_m01_cell']/dict_out['BAIRONG_APPLY_LOAN_caoff_orgnum_m03_cell'] if dict_out['BAIRONG_APPLY_LOAN_caoff_orgnum_m03_cell']>0 else np.nan
    dict_out['BAIRONG_APPLY_LOAN_pdl_allnum_d07vm06_cell']= dict_out['BAIRONG_APPLY_LOAN_pdl_allnum_d07_cell']/dict_out['BAIRONG_APPLY_LOAN_pdl_allnum_m06_cell'] if dict_out['BAIRONG_APPLY_LOAN_pdl_allnum_m06_cell']>0 else np.nan
    dict_out['BAIRONG_APPLY_LOAN_nbank_sloan_orgnum_m06vm12_id']= dict_out['BAIRONG_APPLY_LOAN_nbank_sloan_orgnum_m06_id']/dict_out['BAIRONG_APPLY_LOAN_nbank_sloan_orgnum_m12_id'] if dict_out['BAIRONG_APPLY_LOAN_nbank_sloan_orgnum_m12_id']>0 else np.nan
    dict_out['BAIRONG_APPLY_LOAN_nbank_allnum_m01vm12_cell']= dict_out['BAIRONG_APPLY_LOAN_nbank_allnum_m01_cell']/dict_out['BAIRONG_APPLY_LOAN_nbank_allnum_m12_cell'] if dict_out['BAIRONG_APPLY_LOAN_nbank_allnum_m12_cell']>0 else np.nan
    dict_out['BAIRONG_APPLY_LOAN_avg_monnum_m03vm12_cell']= dict_out['BAIRONG_APPLY_LOAN_avg_monnum_m03_cell']/dict_out['BAIRONG_APPLY_LOAN_avg_monnum_m12_cell'] if dict_out['BAIRONG_APPLY_LOAN_avg_monnum_m12_cell']>0 else np.nan
    dict_out['BAIRONG_APPLY_LOAN_rel_allnum_d15vm12_id']= dict_out['BAIRONG_APPLY_LOAN_rel_allnum_d15_id']/dict_out['BAIRONG_APPLY_LOAN_rel_allnum_m12_id'] if dict_out['BAIRONG_APPLY_LOAN_rel_allnum_m12_id']>0 else np.nan
    dict_out['BAIRONG_APPLY_LOAN_bank_ret_orgnum_m06_idsubcell']= dict_out['BAIRONG_APPLY_LOAN_bank_ret_orgnum_m06_id']-dict_out['BAIRONG_APPLY_LOAN_bank_ret_orgnum_m06_cell']
    dict_out['BAIRONG_APPLY_LOAN_nbank_max_monnum_m03vm12_cell']= dict_out['BAIRONG_APPLY_LOAN_nbank_max_monnum_m03_cell']/dict_out['BAIRONG_APPLY_LOAN_nbank_max_monnum_m12_cell'] if dict_out['BAIRONG_APPLY_LOAN_nbank_max_monnum_m12_cell']>0 else np.nan
    dict_out['BAIRONG_APPLY_LOAN_nbank_ca_allnum_m03vm06_cell']= dict_out['BAIRONG_APPLY_LOAN_nbank_ca_allnum_m03_cell']/dict_out['BAIRONG_APPLY_LOAN_nbank_ca_allnum_m06_cell'] if dict_out['BAIRONG_APPLY_LOAN_nbank_ca_allnum_m06_cell']>0 else np.nan
    dict_out['BAIRONG_APPLY_LOAN_nbank_week_orgnum_d07vm01_id']= dict_out['BAIRONG_APPLY_LOAN_nbank_week_orgnum_d07_id']/dict_out['BAIRONG_APPLY_LOAN_nbank_week_orgnum_m01_id'] if dict_out['BAIRONG_APPLY_LOAN_nbank_week_orgnum_m01_id']>0 else np.nan
    dict_out['BAIRONG_APPLY_LOAN_nbank_week_allnum_d15vm12_id']= dict_out['BAIRONG_APPLY_LOAN_nbank_week_allnum_d15_id']/dict_out['BAIRONG_APPLY_LOAN_nbank_week_allnum_m12_id'] if dict_out['BAIRONG_APPLY_LOAN_nbank_week_allnum_m12_id']>0 else np.nan
    dict_out['BAIRONG_APPLY_LOAN_nbank_p2p_allnum_m06vm12_id']= dict_out['BAIRONG_APPLY_LOAN_nbank_p2p_allnum_m06_id']/dict_out['BAIRONG_APPLY_LOAN_nbank_p2p_allnum_m12_id'] if dict_out['BAIRONG_APPLY_LOAN_nbank_p2p_allnum_m12_id']>0 else np.nan
    dict_out['BAIRONG_APPLY_LOAN_af_allnum_m01vm06_id']= dict_out['BAIRONG_APPLY_LOAN_af_allnum_m01_id']/dict_out['BAIRONG_APPLY_LOAN_af_allnum_m06_id'] if dict_out['BAIRONG_APPLY_LOAN_af_allnum_m06_id']>0 else np.nan
    dict_out['BAIRONG_APPLY_LOAN_pdl_orgnum_m01vm06_id']= dict_out['BAIRONG_APPLY_LOAN_pdl_orgnum_m01_id']/dict_out['BAIRONG_APPLY_LOAN_pdl_orgnum_m06_id'] if dict_out['BAIRONG_APPLY_LOAN_pdl_orgnum_m06_id']>0 else np.nan
    dict_out['BAIRONG_APPLY_LOAN_nbank_avg_monnum_m03_idsubcell']= dict_out['BAIRONG_APPLY_LOAN_nbank_avg_monnum_m03_id']-dict_out['BAIRONG_APPLY_LOAN_nbank_avg_monnum_m03_cell']
    dict_out['BAIRONG_APPLY_LOAN_coon_allnum_m03vm06_cell']= dict_out['BAIRONG_APPLY_LOAN_coon_allnum_m03_cell']/dict_out['BAIRONG_APPLY_LOAN_coon_allnum_m06_cell'] if dict_out['BAIRONG_APPLY_LOAN_coon_allnum_m06_cell']>0 else np.nan
    dict_out['BAIRONG_APPLY_LOAN_oth_allnum_m01vm06_id']= dict_out['BAIRONG_APPLY_LOAN_oth_allnum_m01_id']/dict_out['BAIRONG_APPLY_LOAN_oth_allnum_m06_id'] if dict_out['BAIRONG_APPLY_LOAN_oth_allnum_m06_id']>0 else np.nan
    dict_out['BAIRONG_APPLY_LOAN_bank_week_allnum_m03vm12_id']= dict_out['BAIRONG_APPLY_LOAN_bank_week_allnum_m03_id']/dict_out['BAIRONG_APPLY_LOAN_bank_week_allnum_m12_id'] if dict_out['BAIRONG_APPLY_LOAN_bank_week_allnum_m12_id']>0 else np.nan
    dict_out['BAIRONG_APPLY_LOAN_nbank_max_monnum_m06vm12_cell']= dict_out['BAIRONG_APPLY_LOAN_nbank_max_monnum_m06_cell']/dict_out['BAIRONG_APPLY_LOAN_nbank_max_monnum_m12_cell'] if dict_out['BAIRONG_APPLY_LOAN_nbank_max_monnum_m12_cell']>0 else np.nan
    dict_out['BAIRONG_APPLY_LOAN_nbank_p2p_allnum_m03vm06_id']= dict_out['BAIRONG_APPLY_LOAN_nbank_p2p_allnum_m03_id']/dict_out['BAIRONG_APPLY_LOAN_nbank_p2p_allnum_m06_id'] if dict_out['BAIRONG_APPLY_LOAN_nbank_p2p_allnum_m06_id']>0 else np.nan
    dict_out['BAIRONG_APPLY_LOAN_nbank_nsloan_allnum_d15vm01_id']= dict_out['BAIRONG_APPLY_LOAN_nbank_nsloan_allnum_d15_id']/dict_out['BAIRONG_APPLY_LOAN_nbank_nsloan_allnum_m01_id'] if dict_out['BAIRONG_APPLY_LOAN_nbank_nsloan_allnum_m01_id']>0 else np.nan
    dict_out['BAIRONG_APPLY_LOAN_nbank_nsloan_allnum_d15vm12_cell']= dict_out['BAIRONG_APPLY_LOAN_nbank_nsloan_allnum_d15_cell']/dict_out['BAIRONG_APPLY_LOAN_nbank_nsloan_allnum_m12_cell'] if dict_out['BAIRONG_APPLY_LOAN_nbank_nsloan_allnum_m12_cell']>0 else np.nan
    dict_out['BAIRONG_APPLY_LOAN_nbank_else_orgnum_d07vm12_cell']= dict_out['BAIRONG_APPLY_LOAN_nbank_else_orgnum_d07_cell']/dict_out['BAIRONG_APPLY_LOAN_nbank_else_orgnum_m12_cell'] if dict_out['BAIRONG_APPLY_LOAN_nbank_else_orgnum_m12_cell']>0 else np.nan
    dict_out['BAIRONG_APPLY_LOAN_max_inteday_m03vm12_id']= dict_out['BAIRONG_APPLY_LOAN_max_inteday_m03_id']/dict_out['BAIRONG_APPLY_LOAN_max_inteday_m12_id'] if dict_out['BAIRONG_APPLY_LOAN_max_inteday_m12_id']>0 else np.nan
    dict_out['BAIRONG_APPLY_LOAN_bank_avg_monnum_m03vm12_cell']= dict_out['BAIRONG_APPLY_LOAN_bank_avg_monnum_m03_cell']/dict_out['BAIRONG_APPLY_LOAN_bank_avg_monnum_m12_cell'] if dict_out['BAIRONG_APPLY_LOAN_bank_avg_monnum_m12_cell']>0 else np.nan
    dict_out['BAIRONG_APPLY_LOAN_bank_tra_allnum_d15vm01_cell']= dict_out['BAIRONG_APPLY_LOAN_bank_tra_allnum_d15_cell']/dict_out['BAIRONG_APPLY_LOAN_bank_tra_allnum_m01_cell'] if dict_out['BAIRONG_APPLY_LOAN_bank_tra_allnum_m01_cell']>0 else np.nan
    dict_out['BAIRONG_APPLY_LOAN_nbank_ca_orgnum_m06vm12_id']= dict_out['BAIRONG_APPLY_LOAN_nbank_ca_orgnum_m06_id']/dict_out['BAIRONG_APPLY_LOAN_nbank_ca_orgnum_m12_id'] if dict_out['BAIRONG_APPLY_LOAN_nbank_ca_orgnum_m12_id']>0 else np.nan
    dict_out['BAIRONG_APPLY_LOAN_rel_allnum_d07vm12_id']= dict_out['BAIRONG_APPLY_LOAN_rel_allnum_d07_id']/dict_out['BAIRONG_APPLY_LOAN_rel_allnum_m12_id'] if dict_out['BAIRONG_APPLY_LOAN_rel_allnum_m12_id']>0 else np.nan
    dict_out['BAIRONG_APPLY_LOAN_bank_night_allnum_m03vm06_cell']= dict_out['BAIRONG_APPLY_LOAN_bank_night_allnum_m03_cell']/dict_out['BAIRONG_APPLY_LOAN_bank_night_allnum_m06_cell'] if dict_out['BAIRONG_APPLY_LOAN_bank_night_allnum_m06_cell']>0 else np.nan
    dict_out['BAIRONG_APPLY_LOAN_nbank_ca_orgnum_m03vm06_cell']= dict_out['BAIRONG_APPLY_LOAN_nbank_ca_orgnum_m03_cell']/dict_out['BAIRONG_APPLY_LOAN_nbank_ca_orgnum_m06_cell'] if dict_out['BAIRONG_APPLY_LOAN_nbank_ca_orgnum_m06_cell']>0 else np.nan
    dict_out['BAIRONG_APPLY_LOAN_nbank_sloan_allnum_m03vm12_cell']= dict_out['BAIRONG_APPLY_LOAN_nbank_sloan_allnum_m03_cell']/dict_out['BAIRONG_APPLY_LOAN_nbank_sloan_allnum_m12_cell'] if dict_out['BAIRONG_APPLY_LOAN_nbank_sloan_allnum_m12_cell']>0 else np.nan
    dict_out['BAIRONG_APPLY_LOAN_pdl_allnum_m03vm06_id']= dict_out['BAIRONG_APPLY_LOAN_pdl_allnum_m03_id']/dict_out['BAIRONG_APPLY_LOAN_pdl_allnum_m06_id'] if dict_out['BAIRONG_APPLY_LOAN_pdl_allnum_m06_id']>0 else np.nan
    dict_out['BAIRONG_APPLY_LOAN_nbank_oth_allnum_d15vm01_id']= dict_out['BAIRONG_APPLY_LOAN_nbank_oth_allnum_d15_id']/dict_out['BAIRONG_APPLY_LOAN_nbank_oth_allnum_m01_id'] if dict_out['BAIRONG_APPLY_LOAN_nbank_oth_allnum_m01_id']>0 else np.nan
    dict_out['BAIRONG_APPLY_LOAN_nbank_ca_allnum_m03vm12_cell']= dict_out['BAIRONG_APPLY_LOAN_nbank_ca_allnum_m03_cell']/dict_out['BAIRONG_APPLY_LOAN_nbank_ca_allnum_m12_cell'] if dict_out['BAIRONG_APPLY_LOAN_nbank_ca_allnum_m12_cell']>0 else np.nan
    dict_out['BAIRONG_APPLY_LOAN_bank_avg_monnum_m03vm12_id']= dict_out['BAIRONG_APPLY_LOAN_bank_avg_monnum_m03_id']/dict_out['BAIRONG_APPLY_LOAN_bank_avg_monnum_m12_id'] if dict_out['BAIRONG_APPLY_LOAN_bank_avg_monnum_m12_id']>0 else np.nan
    dict_out['BAIRONG_APPLY_LOAN_nbank_cf_allnum_d15vm06_cell']= dict_out['BAIRONG_APPLY_LOAN_nbank_cf_allnum_d15_cell']/dict_out['BAIRONG_APPLY_LOAN_nbank_cf_allnum_m06_cell'] if dict_out['BAIRONG_APPLY_LOAN_nbank_cf_allnum_m06_cell']>0 else np.nan
    dict_out['BAIRONG_APPLY_LOAN_nbank_else_allnum_d07vm06_id']= dict_out['BAIRONG_APPLY_LOAN_nbank_else_allnum_d07_id']/dict_out['BAIRONG_APPLY_LOAN_nbank_else_allnum_m06_id'] if dict_out['BAIRONG_APPLY_LOAN_nbank_else_allnum_m06_id']>0 else np.nan
    dict_out['BAIRONG_APPLY_LOAN_nbank_p2p_allnum_m03vm06_cell']= dict_out['BAIRONG_APPLY_LOAN_nbank_p2p_allnum_m03_cell']/dict_out['BAIRONG_APPLY_LOAN_nbank_p2p_allnum_m06_cell'] if dict_out['BAIRONG_APPLY_LOAN_nbank_p2p_allnum_m06_cell']>0 else np.nan
    dict_out['BAIRONG_APPLY_LOAN_nbank_night_orgnum_d07vm12_id']= dict_out['BAIRONG_APPLY_LOAN_nbank_night_orgnum_d07_id']/dict_out['BAIRONG_APPLY_LOAN_nbank_night_orgnum_m12_id'] if dict_out['BAIRONG_APPLY_LOAN_nbank_night_orgnum_m12_id']>0 else np.nan
    dict_out['BAIRONG_APPLY_LOAN_nbank_nsloan_allnum_m01vm06_id']= dict_out['BAIRONG_APPLY_LOAN_nbank_nsloan_allnum_m01_id']/dict_out['BAIRONG_APPLY_LOAN_nbank_nsloan_allnum_m06_id'] if dict_out['BAIRONG_APPLY_LOAN_nbank_nsloan_allnum_m06_id']>0 else np.nan
    dict_out['BAIRONG_APPLY_LOAN_nbank_sloan_orgnum_m01vm06_id']= dict_out['BAIRONG_APPLY_LOAN_nbank_sloan_orgnum_m01_id']/dict_out['BAIRONG_APPLY_LOAN_nbank_sloan_orgnum_m06_id'] if dict_out['BAIRONG_APPLY_LOAN_nbank_sloan_orgnum_m06_id']>0 else np.nan
    dict_out['BAIRONG_APPLY_LOAN_nbank_p2p_allnum_m06vm12_cell']= dict_out['BAIRONG_APPLY_LOAN_nbank_p2p_allnum_m06_cell']/dict_out['BAIRONG_APPLY_LOAN_nbank_p2p_allnum_m12_cell'] if dict_out['BAIRONG_APPLY_LOAN_nbank_p2p_allnum_m12_cell']>0 else np.nan
    dict_out['BAIRONG_APPLY_LOAN_bank_tra_allnum_d07vm01_id']= dict_out['BAIRONG_APPLY_LOAN_bank_tra_allnum_d07_id']/dict_out['BAIRONG_APPLY_LOAN_bank_tra_allnum_m01_id'] if dict_out['BAIRONG_APPLY_LOAN_bank_tra_allnum_m01_id']>0 else np.nan
    dict_out['BAIRONG_APPLY_LOAN_nbank_orgnum_m01vm06_cell']= dict_out['BAIRONG_APPLY_LOAN_nbank_orgnum_m01_cell']/dict_out['BAIRONG_APPLY_LOAN_nbank_orgnum_m06_cell'] if dict_out['BAIRONG_APPLY_LOAN_nbank_orgnum_m06_cell']>0 else np.nan
    dict_out['BAIRONG_APPLY_LOAN_bank_avg_monnum_m06vm12_cell']= dict_out['BAIRONG_APPLY_LOAN_bank_avg_monnum_m06_cell']/dict_out['BAIRONG_APPLY_LOAN_bank_avg_monnum_m12_cell'] if dict_out['BAIRONG_APPLY_LOAN_bank_avg_monnum_m12_cell']>0 else np.nan
    dict_out['BAIRONG_APPLY_LOAN_nbank_nsloan_allnum_d07vd15_cell']= dict_out['BAIRONG_APPLY_LOAN_nbank_nsloan_allnum_d07_cell']/dict_out['BAIRONG_APPLY_LOAN_nbank_nsloan_allnum_d15_cell'] if dict_out['BAIRONG_APPLY_LOAN_nbank_nsloan_allnum_d15_cell']>0 else np.nan
    dict_out['BAIRONG_APPLY_LOAN_nbank_ca_orgnum_m01vm12_cell']= dict_out['BAIRONG_APPLY_LOAN_nbank_ca_orgnum_m01_cell']/dict_out['BAIRONG_APPLY_LOAN_nbank_ca_orgnum_m12_cell'] if dict_out['BAIRONG_APPLY_LOAN_nbank_ca_orgnum_m12_cell']>0 else np.nan
    dict_out['BAIRONG_APPLY_LOAN_nbank_cf_allnum_d07vd15_cell']= dict_out['BAIRONG_APPLY_LOAN_nbank_cf_allnum_d07_cell']/dict_out['BAIRONG_APPLY_LOAN_nbank_cf_allnum_d15_cell'] if dict_out['BAIRONG_APPLY_LOAN_nbank_cf_allnum_d15_cell']>0 else np.nan
    dict_out['BAIRONG_APPLY_LOAN_nbank_allnum_m06vm12_id']= dict_out['BAIRONG_APPLY_LOAN_nbank_allnum_m06_id']/dict_out['BAIRONG_APPLY_LOAN_nbank_allnum_m12_id'] if dict_out['BAIRONG_APPLY_LOAN_nbank_allnum_m12_id']>0 else np.nan
    dict_out['BAIRONG_APPLY_LOAN_oth_orgnum_d07vm06_cell']= dict_out['BAIRONG_APPLY_LOAN_oth_orgnum_d07_cell']/dict_out['BAIRONG_APPLY_LOAN_oth_orgnum_m06_cell'] if dict_out['BAIRONG_APPLY_LOAN_oth_orgnum_m06_cell']>0 else np.nan
    dict_out['BAIRONG_APPLY_LOAN_oth_allnum_d07vm12_cell']= dict_out['BAIRONG_APPLY_LOAN_oth_allnum_d07_cell']/dict_out['BAIRONG_APPLY_LOAN_oth_allnum_m12_cell'] if dict_out['BAIRONG_APPLY_LOAN_oth_allnum_m12_cell']>0 else np.nan
    dict_out['BAIRONG_APPLY_LOAN_rel_allnum_m06vm12_cell']= dict_out['BAIRONG_APPLY_LOAN_rel_allnum_m06_cell']/dict_out['BAIRONG_APPLY_LOAN_rel_allnum_m12_cell'] if dict_out['BAIRONG_APPLY_LOAN_rel_allnum_m12_cell']>0 else np.nan
    dict_out['BAIRONG_APPLY_LOAN_caoff_orgnum_m06vm12_id']= dict_out['BAIRONG_APPLY_LOAN_caoff_orgnum_m06_id']/dict_out['BAIRONG_APPLY_LOAN_caoff_orgnum_m12_id'] if dict_out['BAIRONG_APPLY_LOAN_caoff_orgnum_m12_id']>0 else np.nan
    dict_out['BAIRONG_APPLY_LOAN_cooff_allnum_m03vm06_id']= dict_out['BAIRONG_APPLY_LOAN_cooff_allnum_m03_id']/dict_out['BAIRONG_APPLY_LOAN_cooff_allnum_m06_id'] if dict_out['BAIRONG_APPLY_LOAN_cooff_allnum_m06_id']>0 else np.nan
    dict_out['BAIRONG_APPLY_LOAN_caoff_orgnum_m03vm12_cell']= dict_out['BAIRONG_APPLY_LOAN_caoff_orgnum_m03_cell']/dict_out['BAIRONG_APPLY_LOAN_caoff_orgnum_m12_cell'] if dict_out['BAIRONG_APPLY_LOAN_caoff_orgnum_m12_cell']>0 else np.nan
    dict_out['BAIRONG_APPLY_LOAN_nbank_else_orgnum_d15vm01_id']= dict_out['BAIRONG_APPLY_LOAN_nbank_else_orgnum_d15_id']/dict_out['BAIRONG_APPLY_LOAN_nbank_else_orgnum_m01_id'] if dict_out['BAIRONG_APPLY_LOAN_nbank_else_orgnum_m01_id']>0 else np.nan
    dict_out['BAIRONG_APPLY_LOAN_nbank_cf_orgnum_m06vm12_id']= dict_out['BAIRONG_APPLY_LOAN_nbank_cf_orgnum_m06_id']/dict_out['BAIRONG_APPLY_LOAN_nbank_cf_orgnum_m12_id'] if dict_out['BAIRONG_APPLY_LOAN_nbank_cf_orgnum_m12_id']>0 else np.nan
    dict_out['BAIRONG_APPLY_LOAN_af_allnum_m06vm12_cell']= dict_out['BAIRONG_APPLY_LOAN_af_allnum_m06_cell']/dict_out['BAIRONG_APPLY_LOAN_af_allnum_m12_cell'] if dict_out['BAIRONG_APPLY_LOAN_af_allnum_m12_cell']>0 else np.nan
    dict_out['BAIRONG_APPLY_LOAN_cooff_allnum_d15vm12_id']= dict_out['BAIRONG_APPLY_LOAN_cooff_allnum_d15_id']/dict_out['BAIRONG_APPLY_LOAN_cooff_allnum_m12_id'] if dict_out['BAIRONG_APPLY_LOAN_cooff_allnum_m12_id']>0 else np.nan
    dict_out['BAIRONG_APPLY_LOAN_nbank_finlea_allnum_m01vm12_id']= dict_out['BAIRONG_APPLY_LOAN_nbank_finlea_allnum_m01_id']/dict_out['BAIRONG_APPLY_LOAN_nbank_finlea_allnum_m12_id'] if dict_out['BAIRONG_APPLY_LOAN_nbank_finlea_allnum_m12_id']>0 else np.nan
    dict_out['BAIRONG_APPLY_LOAN_nbank_allnum_m01vm03_cell']= dict_out['BAIRONG_APPLY_LOAN_nbank_allnum_m01_cell']/dict_out['BAIRONG_APPLY_LOAN_nbank_allnum_m03_cell'] if dict_out['BAIRONG_APPLY_LOAN_nbank_allnum_m03_cell']>0 else np.nan
    dict_out['BAIRONG_APPLY_LOAN_nbank_nsloan_allnum_d07vm12_cell']= dict_out['BAIRONG_APPLY_LOAN_nbank_nsloan_allnum_d07_cell']/dict_out['BAIRONG_APPLY_LOAN_nbank_nsloan_allnum_m12_cell'] if dict_out['BAIRONG_APPLY_LOAN_nbank_nsloan_allnum_m12_cell']>0 else np.nan
    dict_out['BAIRONG_APPLY_LOAN_nbank_sloan_allnum_m01vm06_id']= dict_out['BAIRONG_APPLY_LOAN_nbank_sloan_allnum_m01_id']/dict_out['BAIRONG_APPLY_LOAN_nbank_sloan_allnum_m06_id'] if dict_out['BAIRONG_APPLY_LOAN_nbank_sloan_allnum_m06_id']>0 else np.nan
    dict_out['BAIRONG_APPLY_LOAN_rel_allnum_m01vm06_cell']= dict_out['BAIRONG_APPLY_LOAN_rel_allnum_m01_cell']/dict_out['BAIRONG_APPLY_LOAN_rel_allnum_m06_cell'] if dict_out['BAIRONG_APPLY_LOAN_rel_allnum_m06_cell']>0 else np.nan
    dict_out['BAIRONG_APPLY_LOAN_rel_allnum_m12_idsubcell']= dict_out['BAIRONG_APPLY_LOAN_rel_allnum_m12_id']-dict_out['BAIRONG_APPLY_LOAN_rel_allnum_m12_cell']
    dict_out['BAIRONG_APPLY_LOAN_nbank_allnum_m01vm03_id']= dict_out['BAIRONG_APPLY_LOAN_nbank_allnum_m01_id']/dict_out['BAIRONG_APPLY_LOAN_nbank_allnum_m03_id'] if dict_out['BAIRONG_APPLY_LOAN_nbank_allnum_m03_id']>0 else np.nan
    dict_out['BAIRONG_APPLY_LOAN_nbank_mc_orgnum_m03vm12_id']= dict_out['BAIRONG_APPLY_LOAN_nbank_mc_orgnum_m03_id']/dict_out['BAIRONG_APPLY_LOAN_nbank_mc_orgnum_m12_id'] if dict_out['BAIRONG_APPLY_LOAN_nbank_mc_orgnum_m12_id']>0 else np.nan
    dict_out['BAIRONG_APPLY_LOAN_nbank_allnum_m06_idsubcell']= dict_out['BAIRONG_APPLY_LOAN_nbank_allnum_m06_id']-dict_out['BAIRONG_APPLY_LOAN_nbank_allnum_m06_cell']
    dict_out['BAIRONG_APPLY_LOAN_nbank_oth_allnum_d07vm06_cell']= dict_out['BAIRONG_APPLY_LOAN_nbank_oth_allnum_d07_cell']/dict_out['BAIRONG_APPLY_LOAN_nbank_oth_allnum_m06_cell'] if dict_out['BAIRONG_APPLY_LOAN_nbank_oth_allnum_m06_cell']>0 else np.nan
    dict_out['BAIRONG_APPLY_LOAN_cooff_allnum_m01vm03_cell']= dict_out['BAIRONG_APPLY_LOAN_cooff_allnum_m01_cell']/dict_out['BAIRONG_APPLY_LOAN_cooff_allnum_m03_cell'] if dict_out['BAIRONG_APPLY_LOAN_cooff_allnum_m03_cell']>0 else np.nan
    dict_out['BAIRONG_APPLY_LOAN_nbank_allnum_d07vm01_id']= dict_out['BAIRONG_APPLY_LOAN_nbank_allnum_d07_id']/dict_out['BAIRONG_APPLY_LOAN_nbank_allnum_m01_id'] if dict_out['BAIRONG_APPLY_LOAN_nbank_allnum_m01_id']>0 else np.nan
    dict_out['BAIRONG_APPLY_LOAN_af_allnum_m01vm12_cell']= dict_out['BAIRONG_APPLY_LOAN_af_allnum_m01_cell']/dict_out['BAIRONG_APPLY_LOAN_af_allnum_m12_cell'] if dict_out['BAIRONG_APPLY_LOAN_af_allnum_m12_cell']>0 else np.nan
    dict_out['BAIRONG_APPLY_LOAN_nbank_nsloan_orgnum_d15vm06_cell']= dict_out['BAIRONG_APPLY_LOAN_nbank_nsloan_orgnum_d15_cell']/dict_out['BAIRONG_APPLY_LOAN_nbank_nsloan_orgnum_m06_cell'] if dict_out['BAIRONG_APPLY_LOAN_nbank_nsloan_orgnum_m06_cell']>0 else np.nan
    dict_out['BAIRONG_APPLY_LOAN_bank_min_inteday_m12_idsubcell']= dict_out['BAIRONG_APPLY_LOAN_bank_min_inteday_m12_id']-dict_out['BAIRONG_APPLY_LOAN_bank_min_inteday_m12_cell']
    dict_out['BAIRONG_APPLY_LOAN_af_orgnum_m03_idsubcell']= dict_out['BAIRONG_APPLY_LOAN_af_orgnum_m03_id']-dict_out['BAIRONG_APPLY_LOAN_af_orgnum_m03_cell']
    dict_out['BAIRONG_APPLY_LOAN_rel_allnum_m01vm03_cell']= dict_out['BAIRONG_APPLY_LOAN_rel_allnum_m01_cell']/dict_out['BAIRONG_APPLY_LOAN_rel_allnum_m03_cell'] if dict_out['BAIRONG_APPLY_LOAN_rel_allnum_m03_cell']>0 else np.nan
    dict_out['BAIRONG_APPLY_LOAN_nbank_allnum_m03_idsubcell']= dict_out['BAIRONG_APPLY_LOAN_nbank_allnum_m03_id']-dict_out['BAIRONG_APPLY_LOAN_nbank_allnum_m03_cell']
    dict_out['BAIRONG_APPLY_LOAN_nbank_cons_allnum_d07vm06_id']= dict_out['BAIRONG_APPLY_LOAN_nbank_cons_allnum_d07_id']/dict_out['BAIRONG_APPLY_LOAN_nbank_cons_allnum_m06_id'] if dict_out['BAIRONG_APPLY_LOAN_nbank_cons_allnum_m06_id']>0 else np.nan
    dict_out['BAIRONG_APPLY_LOAN_nbank_cf_orgnum_m01vm03_cell']= dict_out['BAIRONG_APPLY_LOAN_nbank_cf_orgnum_m01_cell']/dict_out['BAIRONG_APPLY_LOAN_nbank_cf_orgnum_m03_cell'] if dict_out['BAIRONG_APPLY_LOAN_nbank_cf_orgnum_m03_cell']>0 else np.nan
    dict_out['BAIRONG_APPLY_LOAN_rel_orgnum_d15vm12_id']= dict_out['BAIRONG_APPLY_LOAN_rel_orgnum_d15_id']/dict_out['BAIRONG_APPLY_LOAN_rel_orgnum_m12_id'] if dict_out['BAIRONG_APPLY_LOAN_rel_orgnum_m12_id']>0 else np.nan
    dict_out['BAIRONG_APPLY_LOAN_nbank_nsloan_orgnum_m06vm12_id']= dict_out['BAIRONG_APPLY_LOAN_nbank_nsloan_orgnum_m06_id']/dict_out['BAIRONG_APPLY_LOAN_nbank_nsloan_orgnum_m12_id'] if dict_out['BAIRONG_APPLY_LOAN_nbank_nsloan_orgnum_m12_id']>0 else np.nan
    dict_out['BAIRONG_APPLY_LOAN_nbank_cf_orgnum_d15vm12_id']= dict_out['BAIRONG_APPLY_LOAN_nbank_cf_orgnum_d15_id']/dict_out['BAIRONG_APPLY_LOAN_nbank_cf_orgnum_m12_id'] if dict_out['BAIRONG_APPLY_LOAN_nbank_cf_orgnum_m12_id']>0 else np.nan
    dict_out['BAIRONG_APPLY_LOAN_cooff_allnum_d07vm12_id']= dict_out['BAIRONG_APPLY_LOAN_cooff_allnum_d07_id']/dict_out['BAIRONG_APPLY_LOAN_cooff_allnum_m12_id'] if dict_out['BAIRONG_APPLY_LOAN_cooff_allnum_m12_id']>0 else np.nan
    dict_out['BAIRONG_APPLY_LOAN_nbank_oth_orgnum_d07vm01_cell']= dict_out['BAIRONG_APPLY_LOAN_nbank_oth_orgnum_d07_cell']/dict_out['BAIRONG_APPLY_LOAN_nbank_oth_orgnum_m01_cell'] if dict_out['BAIRONG_APPLY_LOAN_nbank_oth_orgnum_m01_cell']>0 else np.nan
    dict_out['BAIRONG_APPLY_LOAN_bank_tot_mons_m03vm12_cell']= dict_out['BAIRONG_APPLY_LOAN_bank_tot_mons_m03_cell']/dict_out['BAIRONG_APPLY_LOAN_bank_tot_mons_m12_cell'] if dict_out['BAIRONG_APPLY_LOAN_bank_tot_mons_m12_cell']>0 else np.nan
    dict_out['BAIRONG_APPLY_LOAN_nbank_cf_allnum_d15vm12_cell']= dict_out['BAIRONG_APPLY_LOAN_nbank_cf_allnum_d15_cell']/dict_out['BAIRONG_APPLY_LOAN_nbank_cf_allnum_m12_cell'] if dict_out['BAIRONG_APPLY_LOAN_nbank_cf_allnum_m12_cell']>0 else np.nan
    dict_out['BAIRONG_APPLY_LOAN_nbank_oth_orgnum_d15vm01_id']= dict_out['BAIRONG_APPLY_LOAN_nbank_oth_orgnum_d15_id']/dict_out['BAIRONG_APPLY_LOAN_nbank_oth_orgnum_m01_id'] if dict_out['BAIRONG_APPLY_LOAN_nbank_oth_orgnum_m01_id']>0 else np.nan
    dict_out['BAIRONG_APPLY_LOAN_oth_orgnum_m06vm12_id']= dict_out['BAIRONG_APPLY_LOAN_oth_orgnum_m06_id']/dict_out['BAIRONG_APPLY_LOAN_oth_orgnum_m12_id'] if dict_out['BAIRONG_APPLY_LOAN_oth_orgnum_m12_id']>0 else np.nan
    dict_out['BAIRONG_APPLY_LOAN_bank_tot_mons_m06vm12_cell']= dict_out['BAIRONG_APPLY_LOAN_bank_tot_mons_m06_cell']/dict_out['BAIRONG_APPLY_LOAN_bank_tot_mons_m12_cell'] if dict_out['BAIRONG_APPLY_LOAN_bank_tot_mons_m12_cell']>0 else np.nan
    dict_out['BAIRONG_APPLY_LOAN_nbank_ca_allnum_d07vm12_cell']= dict_out['BAIRONG_APPLY_LOAN_nbank_ca_allnum_d07_cell']/dict_out['BAIRONG_APPLY_LOAN_nbank_ca_allnum_m12_cell'] if dict_out['BAIRONG_APPLY_LOAN_nbank_ca_allnum_m12_cell']>0 else np.nan
    dict_out['BAIRONG_APPLY_LOAN_bank_tra_orgnum_m03vm12_id']= dict_out['BAIRONG_APPLY_LOAN_bank_tra_orgnum_m03_id']/dict_out['BAIRONG_APPLY_LOAN_bank_tra_orgnum_m12_id'] if dict_out['BAIRONG_APPLY_LOAN_bank_tra_orgnum_m12_id']>0 else np.nan
    dict_out['BAIRONG_APPLY_LOAN_af_allnum_m03_idsubcell']= dict_out['BAIRONG_APPLY_LOAN_af_allnum_m03_id']-dict_out['BAIRONG_APPLY_LOAN_af_allnum_m03_cell']
    dict_out['BAIRONG_APPLY_LOAN_nbank_cf_orgnum_d15vm01_cell']= dict_out['BAIRONG_APPLY_LOAN_nbank_cf_orgnum_d15_cell']/dict_out['BAIRONG_APPLY_LOAN_nbank_cf_orgnum_m01_cell'] if dict_out['BAIRONG_APPLY_LOAN_nbank_cf_orgnum_m01_cell']>0 else np.nan
    dict_out['BAIRONG_APPLY_LOAN_nbank_cf_orgnum_d15vm06_cell']= dict_out['BAIRONG_APPLY_LOAN_nbank_cf_orgnum_d15_cell']/dict_out['BAIRONG_APPLY_LOAN_nbank_cf_orgnum_m06_cell'] if dict_out['BAIRONG_APPLY_LOAN_nbank_cf_orgnum_m06_cell']>0 else np.nan
    dict_out['BAIRONG_APPLY_LOAN_bank_tra_allnum_m06vm12_id']= dict_out['BAIRONG_APPLY_LOAN_bank_tra_allnum_m06_id']/dict_out['BAIRONG_APPLY_LOAN_bank_tra_allnum_m12_id'] if dict_out['BAIRONG_APPLY_LOAN_bank_tra_allnum_m12_id']>0 else np.nan
    dict_out['BAIRONG_APPLY_LOAN_nbank_sloan_allnum_d07vm12_cell']= dict_out['BAIRONG_APPLY_LOAN_nbank_sloan_allnum_d07_cell']/dict_out['BAIRONG_APPLY_LOAN_nbank_sloan_allnum_m12_cell'] if dict_out['BAIRONG_APPLY_LOAN_nbank_sloan_allnum_m12_cell']>0 else np.nan
    dict_out['BAIRONG_APPLY_LOAN_nbank_cf_orgnum_m06vm12_cell']= dict_out['BAIRONG_APPLY_LOAN_nbank_cf_orgnum_m06_cell']/dict_out['BAIRONG_APPLY_LOAN_nbank_cf_orgnum_m12_cell'] if dict_out['BAIRONG_APPLY_LOAN_nbank_cf_orgnum_m12_cell']>0 else np.nan
    dict_out['BAIRONG_APPLY_LOAN_coon_orgnum_m01vm12_id']= dict_out['BAIRONG_APPLY_LOAN_coon_orgnum_m01_id']/dict_out['BAIRONG_APPLY_LOAN_coon_orgnum_m12_id'] if dict_out['BAIRONG_APPLY_LOAN_coon_orgnum_m12_id']>0 else np.nan
    dict_out['BAIRONG_APPLY_LOAN_nbank_else_allnum_d15vm06_id']= dict_out['BAIRONG_APPLY_LOAN_nbank_else_allnum_d15_id']/dict_out['BAIRONG_APPLY_LOAN_nbank_else_allnum_m06_id'] if dict_out['BAIRONG_APPLY_LOAN_nbank_else_allnum_m06_id']>0 else np.nan
    dict_out['BAIRONG_APPLY_LOAN_avg_monnum_m06vm12_cell']= dict_out['BAIRONG_APPLY_LOAN_avg_monnum_m06_cell']/dict_out['BAIRONG_APPLY_LOAN_avg_monnum_m12_cell'] if dict_out['BAIRONG_APPLY_LOAN_avg_monnum_m12_cell']>0 else np.nan
    dict_out['BAIRONG_APPLY_LOAN_nbank_ca_allnum_m01vm12_cell']= dict_out['BAIRONG_APPLY_LOAN_nbank_ca_allnum_m01_cell']/dict_out['BAIRONG_APPLY_LOAN_nbank_ca_allnum_m12_cell'] if dict_out['BAIRONG_APPLY_LOAN_nbank_ca_allnum_m12_cell']>0 else np.nan
    dict_out['BAIRONG_APPLY_LOAN_nbank_oth_allnum_d07vd15_id']= dict_out['BAIRONG_APPLY_LOAN_nbank_oth_allnum_d07_id']/dict_out['BAIRONG_APPLY_LOAN_nbank_oth_allnum_d15_id'] if dict_out['BAIRONG_APPLY_LOAN_nbank_oth_allnum_d15_id']>0 else np.nan
    dict_out['BAIRONG_APPLY_LOAN_nbank_night_orgnum_m01vm03_id']= dict_out['BAIRONG_APPLY_LOAN_nbank_night_orgnum_m01_id']/dict_out['BAIRONG_APPLY_LOAN_nbank_night_orgnum_m03_id'] if dict_out['BAIRONG_APPLY_LOAN_nbank_night_orgnum_m03_id']>0 else np.nan
    dict_out['BAIRONG_APPLY_LOAN_max_monnum_m03vm06_cell']= dict_out['BAIRONG_APPLY_LOAN_max_monnum_m03_cell']/dict_out['BAIRONG_APPLY_LOAN_max_monnum_m06_cell'] if dict_out['BAIRONG_APPLY_LOAN_max_monnum_m06_cell']>0 else np.nan
    dict_out['BAIRONG_APPLY_LOAN_nbank_allnum_d15vm01_cell']= dict_out['BAIRONG_APPLY_LOAN_nbank_allnum_d15_cell']/dict_out['BAIRONG_APPLY_LOAN_nbank_allnum_m01_cell'] if dict_out['BAIRONG_APPLY_LOAN_nbank_allnum_m01_cell']>0 else np.nan
    dict_out['BAIRONG_APPLY_LOAN_nbank_nsloan_allnum_d07vm06_id']= dict_out['BAIRONG_APPLY_LOAN_nbank_nsloan_allnum_d07_id']/dict_out['BAIRONG_APPLY_LOAN_nbank_nsloan_allnum_m06_id'] if dict_out['BAIRONG_APPLY_LOAN_nbank_nsloan_allnum_m06_id']>0 else np.nan
    dict_out['BAIRONG_APPLY_LOAN_nbank_max_inteday_m03vm06_cell']= dict_out['BAIRONG_APPLY_LOAN_nbank_max_inteday_m03_cell']/dict_out['BAIRONG_APPLY_LOAN_nbank_max_inteday_m06_cell'] if dict_out['BAIRONG_APPLY_LOAN_nbank_max_inteday_m06_cell']>0 else np.nan
    dict_out['BAIRONG_APPLY_LOAN_nbank_week_orgnum_m06vm12_id']= dict_out['BAIRONG_APPLY_LOAN_nbank_week_orgnum_m06_id']/dict_out['BAIRONG_APPLY_LOAN_nbank_week_orgnum_m12_id'] if dict_out['BAIRONG_APPLY_LOAN_nbank_week_orgnum_m12_id']>0 else np.nan
    dict_out['BAIRONG_APPLY_LOAN_nbank_cf_allnum_d07vm12_id']= dict_out['BAIRONG_APPLY_LOAN_nbank_cf_allnum_d07_id']/dict_out['BAIRONG_APPLY_LOAN_nbank_cf_allnum_m12_id'] if dict_out['BAIRONG_APPLY_LOAN_nbank_cf_allnum_m12_id']>0 else np.nan
    dict_out['BAIRONG_APPLY_LOAN_nbank_else_orgnum_d07vd15_cell']= dict_out['BAIRONG_APPLY_LOAN_nbank_else_orgnum_d07_cell']/dict_out['BAIRONG_APPLY_LOAN_nbank_else_orgnum_d15_cell'] if dict_out['BAIRONG_APPLY_LOAN_nbank_else_orgnum_d15_cell']>0 else np.nan
    dict_out['BAIRONG_APPLY_LOAN_nbank_cf_orgnum_m01vm12_id']= dict_out['BAIRONG_APPLY_LOAN_nbank_cf_orgnum_m01_id']/dict_out['BAIRONG_APPLY_LOAN_nbank_cf_orgnum_m12_id'] if dict_out['BAIRONG_APPLY_LOAN_nbank_cf_orgnum_m12_id']>0 else np.nan
    dict_out['BAIRONG_APPLY_LOAN_nbank_allnum_d15vm01_id']= dict_out['BAIRONG_APPLY_LOAN_nbank_allnum_d15_id']/dict_out['BAIRONG_APPLY_LOAN_nbank_allnum_m01_id'] if dict_out['BAIRONG_APPLY_LOAN_nbank_allnum_m01_id']>0 else np.nan
    dict_out['BAIRONG_APPLY_LOAN_nbank_finlea_allnum_d15vm12_cell']= dict_out['BAIRONG_APPLY_LOAN_nbank_finlea_allnum_d15_cell']/dict_out['BAIRONG_APPLY_LOAN_nbank_finlea_allnum_m12_cell'] if dict_out['BAIRONG_APPLY_LOAN_nbank_finlea_allnum_m12_cell']>0 else np.nan
    dict_out['BAIRONG_APPLY_LOAN_af_orgnum_m01vm12_id']= dict_out['BAIRONG_APPLY_LOAN_af_orgnum_m01_id']/dict_out['BAIRONG_APPLY_LOAN_af_orgnum_m12_id'] if dict_out['BAIRONG_APPLY_LOAN_af_orgnum_m12_id']>0 else np.nan
    dict_out['BAIRONG_APPLY_LOAN_pdl_orgnum_m03vm12_cell']= dict_out['BAIRONG_APPLY_LOAN_pdl_orgnum_m03_cell']/dict_out['BAIRONG_APPLY_LOAN_pdl_orgnum_m12_cell'] if dict_out['BAIRONG_APPLY_LOAN_pdl_orgnum_m12_cell']>0 else np.nan
    dict_out['BAIRONG_APPLY_LOAN_nbank_week_allnum_m03vm12_id']= dict_out['BAIRONG_APPLY_LOAN_nbank_week_allnum_m03_id']/dict_out['BAIRONG_APPLY_LOAN_nbank_week_allnum_m12_id'] if dict_out['BAIRONG_APPLY_LOAN_nbank_week_allnum_m12_id']>0 else np.nan
    dict_out['BAIRONG_APPLY_LOAN_caon_allnum_m01vm03_id']= dict_out['BAIRONG_APPLY_LOAN_caon_allnum_m01_id']/dict_out['BAIRONG_APPLY_LOAN_caon_allnum_m03_id'] if dict_out['BAIRONG_APPLY_LOAN_caon_allnum_m03_id']>0 else np.nan
    dict_out['BAIRONG_APPLY_LOAN_caon_allnum_d07vd15_id']= dict_out['BAIRONG_APPLY_LOAN_caon_allnum_d07_id']/dict_out['BAIRONG_APPLY_LOAN_caon_allnum_d15_id'] if dict_out['BAIRONG_APPLY_LOAN_caon_allnum_d15_id']>0 else np.nan
    dict_out['BAIRONG_APPLY_LOAN_nbank_avg_monnum_m03vm12_id']= dict_out['BAIRONG_APPLY_LOAN_nbank_avg_monnum_m03_id']/dict_out['BAIRONG_APPLY_LOAN_nbank_avg_monnum_m12_id'] if dict_out['BAIRONG_APPLY_LOAN_nbank_avg_monnum_m12_id']>0 else np.nan
    dict_out['BAIRONG_APPLY_LOAN_nbank_orgnum_m01vm03_id']= dict_out['BAIRONG_APPLY_LOAN_nbank_orgnum_m01_id']/dict_out['BAIRONG_APPLY_LOAN_nbank_orgnum_m03_id'] if dict_out['BAIRONG_APPLY_LOAN_nbank_orgnum_m03_id']>0 else np.nan
    dict_out['BAIRONG_APPLY_LOAN_caoff_allnum_d15vm06_cell']= dict_out['BAIRONG_APPLY_LOAN_caoff_allnum_d15_cell']/dict_out['BAIRONG_APPLY_LOAN_caoff_allnum_m06_cell'] if dict_out['BAIRONG_APPLY_LOAN_caoff_allnum_m06_cell']>0 else np.nan
    dict_out['BAIRONG_APPLY_LOAN_nbank_allnum_m03vm12_cell']= dict_out['BAIRONG_APPLY_LOAN_nbank_allnum_m03_cell']/dict_out['BAIRONG_APPLY_LOAN_nbank_allnum_m12_cell'] if dict_out['BAIRONG_APPLY_LOAN_nbank_allnum_m12_cell']>0 else np.nan
    dict_out['BAIRONG_APPLY_LOAN_bank_tra_orgnum_m01vm03_cell']= dict_out['BAIRONG_APPLY_LOAN_bank_tra_orgnum_m01_cell']/dict_out['BAIRONG_APPLY_LOAN_bank_tra_orgnum_m03_cell'] if dict_out['BAIRONG_APPLY_LOAN_bank_tra_orgnum_m03_cell']>0 else np.nan
    dict_out['BAIRONG_APPLY_LOAN_rel_allnum_m01vm06_id']= dict_out['BAIRONG_APPLY_LOAN_rel_allnum_m01_id']/dict_out['BAIRONG_APPLY_LOAN_rel_allnum_m06_id'] if dict_out['BAIRONG_APPLY_LOAN_rel_allnum_m06_id']>0 else np.nan
    dict_out['BAIRONG_APPLY_LOAN_nbank_else_orgnum_d07vd15_id']= dict_out['BAIRONG_APPLY_LOAN_nbank_else_orgnum_d07_id']/dict_out['BAIRONG_APPLY_LOAN_nbank_else_orgnum_d15_id'] if dict_out['BAIRONG_APPLY_LOAN_nbank_else_orgnum_d15_id']>0 else np.nan
    dict_out['BAIRONG_APPLY_LOAN_rel_orgnum_m03vm06_cell']= dict_out['BAIRONG_APPLY_LOAN_rel_orgnum_m03_cell']/dict_out['BAIRONG_APPLY_LOAN_rel_orgnum_m06_cell'] if dict_out['BAIRONG_APPLY_LOAN_rel_orgnum_m06_cell']>0 else np.nan
    dict_out['BAIRONG_APPLY_LOAN_max_monnum_m03vm12_cell']= dict_out['BAIRONG_APPLY_LOAN_max_monnum_m03_cell']/dict_out['BAIRONG_APPLY_LOAN_max_monnum_m12_cell'] if dict_out['BAIRONG_APPLY_LOAN_max_monnum_m12_cell']>0 else np.nan
    dict_out['BAIRONG_APPLY_LOAN_bank_max_inteday_m03vm06_id']= dict_out['BAIRONG_APPLY_LOAN_bank_max_inteday_m03_id']/dict_out['BAIRONG_APPLY_LOAN_bank_max_inteday_m06_id'] if dict_out['BAIRONG_APPLY_LOAN_bank_max_inteday_m06_id']>0 else np.nan
    dict_out['BAIRONG_APPLY_LOAN_nbank_ca_orgnum_d15vm06_cell']= dict_out['BAIRONG_APPLY_LOAN_nbank_ca_orgnum_d15_cell']/dict_out['BAIRONG_APPLY_LOAN_nbank_ca_orgnum_m06_cell'] if dict_out['BAIRONG_APPLY_LOAN_nbank_ca_orgnum_m06_cell']>0 else np.nan
    dict_out['BAIRONG_APPLY_LOAN_bank_max_inteday_m06vm12_id']= dict_out['BAIRONG_APPLY_LOAN_bank_max_inteday_m06_id']/dict_out['BAIRONG_APPLY_LOAN_bank_max_inteday_m12_id'] if dict_out['BAIRONG_APPLY_LOAN_bank_max_inteday_m12_id']>0 else np.nan
    dict_out['BAIRONG_APPLY_LOAN_caoff_allnum_m03vm06_id']= dict_out['BAIRONG_APPLY_LOAN_caoff_allnum_m03_id']/dict_out['BAIRONG_APPLY_LOAN_caoff_allnum_m06_id'] if dict_out['BAIRONG_APPLY_LOAN_caoff_allnum_m06_id']>0 else np.nan
    dict_out['BAIRONG_APPLY_LOAN_nbank_else_allnum_d15vm12_id']= dict_out['BAIRONG_APPLY_LOAN_nbank_else_allnum_d15_id']/dict_out['BAIRONG_APPLY_LOAN_nbank_else_allnum_m12_id'] if dict_out['BAIRONG_APPLY_LOAN_nbank_else_allnum_m12_id']>0 else np.nan
    dict_out['BAIRONG_APPLY_LOAN_caon_orgnum_d07vm06_id']= dict_out['BAIRONG_APPLY_LOAN_caon_orgnum_d07_id']/dict_out['BAIRONG_APPLY_LOAN_caon_orgnum_m06_id'] if dict_out['BAIRONG_APPLY_LOAN_caon_orgnum_m06_id']>0 else np.nan
    dict_out['BAIRONG_APPLY_LOAN_bank_ret_orgnum_m12_idsubcell']= dict_out['BAIRONG_APPLY_LOAN_bank_ret_orgnum_m12_id']-dict_out['BAIRONG_APPLY_LOAN_bank_ret_orgnum_m12_cell']
    dict_out['BAIRONG_APPLY_LOAN_oth_allnum_d15vm06_id']= dict_out['BAIRONG_APPLY_LOAN_oth_allnum_d15_id']/dict_out['BAIRONG_APPLY_LOAN_oth_allnum_m06_id'] if dict_out['BAIRONG_APPLY_LOAN_oth_allnum_m06_id']>0 else np.nan
    dict_out['BAIRONG_APPLY_LOAN_nbank_sloan_allnum_m03vm12_id']= dict_out['BAIRONG_APPLY_LOAN_nbank_sloan_allnum_m03_id']/dict_out['BAIRONG_APPLY_LOAN_nbank_sloan_allnum_m12_id'] if dict_out['BAIRONG_APPLY_LOAN_nbank_sloan_allnum_m12_id']>0 else np.nan
    dict_out['BAIRONG_APPLY_LOAN_nbank_sloan_allnum_d15vm01_id']= dict_out['BAIRONG_APPLY_LOAN_nbank_sloan_allnum_d15_id']/dict_out['BAIRONG_APPLY_LOAN_nbank_sloan_allnum_m01_id'] if dict_out['BAIRONG_APPLY_LOAN_nbank_sloan_allnum_m01_id']>0 else np.nan
    dict_out['BAIRONG_APPLY_LOAN_caon_allnum_m03vm06_id']= dict_out['BAIRONG_APPLY_LOAN_caon_allnum_m03_id']/dict_out['BAIRONG_APPLY_LOAN_caon_allnum_m06_id'] if dict_out['BAIRONG_APPLY_LOAN_caon_allnum_m06_id']>0 else np.nan
    dict_out['BAIRONG_APPLY_LOAN_caon_allnum_m01vm12_cell']= dict_out['BAIRONG_APPLY_LOAN_caon_allnum_m01_cell']/dict_out['BAIRONG_APPLY_LOAN_caon_allnum_m12_cell'] if dict_out['BAIRONG_APPLY_LOAN_caon_allnum_m12_cell']>0 else np.nan
    dict_out['BAIRONG_APPLY_LOAN_caon_orgnum_m06vm12_cell']= dict_out['BAIRONG_APPLY_LOAN_caon_orgnum_m06_cell']/dict_out['BAIRONG_APPLY_LOAN_caon_orgnum_m12_cell'] if dict_out['BAIRONG_APPLY_LOAN_caon_orgnum_m12_cell']>0 else np.nan
    dict_out['BAIRONG_APPLY_LOAN_rel_orgnum_m03vm12_cell']= dict_out['BAIRONG_APPLY_LOAN_rel_orgnum_m03_cell']/dict_out['BAIRONG_APPLY_LOAN_rel_orgnum_m12_cell'] if dict_out['BAIRONG_APPLY_LOAN_rel_orgnum_m12_cell']>0 else np.nan
    dict_out['BAIRONG_APPLY_LOAN_nbank_cons_allnum_d07vm01_id']= dict_out['BAIRONG_APPLY_LOAN_nbank_cons_allnum_d07_id']/dict_out['BAIRONG_APPLY_LOAN_nbank_cons_allnum_m01_id'] if dict_out['BAIRONG_APPLY_LOAN_nbank_cons_allnum_m01_id']>0 else np.nan
    dict_out['BAIRONG_APPLY_LOAN_caoff_allnum_m01vm03_id']= dict_out['BAIRONG_APPLY_LOAN_caoff_allnum_m01_id']/dict_out['BAIRONG_APPLY_LOAN_caoff_allnum_m03_id'] if dict_out['BAIRONG_APPLY_LOAN_caoff_allnum_m03_id']>0 else np.nan
    dict_out['BAIRONG_APPLY_LOAN_cooff_orgnum_d15vm06_id']= dict_out['BAIRONG_APPLY_LOAN_cooff_orgnum_d15_id']/dict_out['BAIRONG_APPLY_LOAN_cooff_orgnum_m06_id'] if dict_out['BAIRONG_APPLY_LOAN_cooff_orgnum_m06_id']>0 else np.nan
    dict_out['BAIRONG_APPLY_LOAN_nbank_ca_allnum_m12_idsubcell']= dict_out['BAIRONG_APPLY_LOAN_nbank_ca_allnum_m12_id']-dict_out['BAIRONG_APPLY_LOAN_nbank_ca_allnum_m12_cell']
    dict_out['BAIRONG_APPLY_LOAN_bank_allnum_d15vm12_id']= dict_out['BAIRONG_APPLY_LOAN_bank_allnum_d15_id']/dict_out['BAIRONG_APPLY_LOAN_bank_allnum_m12_id'] if dict_out['BAIRONG_APPLY_LOAN_bank_allnum_m12_id']>0 else np.nan
    dict_out['BAIRONG_APPLY_LOAN_nbank_cf_allnum_m03vm12_id']= dict_out['BAIRONG_APPLY_LOAN_nbank_cf_allnum_m03_id']/dict_out['BAIRONG_APPLY_LOAN_nbank_cf_allnum_m12_id'] if dict_out['BAIRONG_APPLY_LOAN_nbank_cf_allnum_m12_id']>0 else np.nan
    dict_out['BAIRONG_APPLY_LOAN_nbank_cons_orgnum_m03vm12_cell']= dict_out['BAIRONG_APPLY_LOAN_nbank_cons_orgnum_m03_cell']/dict_out['BAIRONG_APPLY_LOAN_nbank_cons_orgnum_m12_cell'] if dict_out['BAIRONG_APPLY_LOAN_nbank_cons_orgnum_m12_cell']>0 else np.nan
    dict_out['BAIRONG_APPLY_LOAN_bank_ret_allnum_m12_idsubcell']= dict_out['BAIRONG_APPLY_LOAN_bank_ret_allnum_m12_id']-dict_out['BAIRONG_APPLY_LOAN_bank_ret_allnum_m12_cell']
    dict_out['BAIRONG_APPLY_LOAN_nbank_max_inteday_m03vm06_id']= dict_out['BAIRONG_APPLY_LOAN_nbank_max_inteday_m03_id']/dict_out['BAIRONG_APPLY_LOAN_nbank_max_inteday_m06_id'] if dict_out['BAIRONG_APPLY_LOAN_nbank_max_inteday_m06_id']>0 else np.nan
    dict_out['BAIRONG_APPLY_LOAN_nbank_nsloan_allnum_m03vm06_cell']= dict_out['BAIRONG_APPLY_LOAN_nbank_nsloan_allnum_m03_cell']/dict_out['BAIRONG_APPLY_LOAN_nbank_nsloan_allnum_m06_cell'] if dict_out['BAIRONG_APPLY_LOAN_nbank_nsloan_allnum_m06_cell']>0 else np.nan
    dict_out['BAIRONG_APPLY_LOAN_nbank_else_allnum_d07vm06_cell']= dict_out['BAIRONG_APPLY_LOAN_nbank_else_allnum_d07_cell']/dict_out['BAIRONG_APPLY_LOAN_nbank_else_allnum_m06_cell'] if dict_out['BAIRONG_APPLY_LOAN_nbank_else_allnum_m06_cell']>0 else np.nan
    dict_out['BAIRONG_APPLY_LOAN_pdl_orgnum_d07vm06_id']= dict_out['BAIRONG_APPLY_LOAN_pdl_orgnum_d07_id']/dict_out['BAIRONG_APPLY_LOAN_pdl_orgnum_m06_id'] if dict_out['BAIRONG_APPLY_LOAN_pdl_orgnum_m06_id']>0 else np.nan
    dict_out['BAIRONG_APPLY_LOAN_nbank_orgnum_d15vm06_id']= dict_out['BAIRONG_APPLY_LOAN_nbank_orgnum_d15_id']/dict_out['BAIRONG_APPLY_LOAN_nbank_orgnum_m06_id'] if dict_out['BAIRONG_APPLY_LOAN_nbank_orgnum_m06_id']>0 else np.nan
    dict_out['BAIRONG_APPLY_LOAN_nbank_cf_allnum_d15vm06_id']= dict_out['BAIRONG_APPLY_LOAN_nbank_cf_allnum_d15_id']/dict_out['BAIRONG_APPLY_LOAN_nbank_cf_allnum_m06_id'] if dict_out['BAIRONG_APPLY_LOAN_nbank_cf_allnum_m06_id']>0 else np.nan
    dict_out['BAIRONG_APPLY_LOAN_nbank_cf_allnum_m06vm12_id']= dict_out['BAIRONG_APPLY_LOAN_nbank_cf_allnum_m06_id']/dict_out['BAIRONG_APPLY_LOAN_nbank_cf_allnum_m12_id'] if dict_out['BAIRONG_APPLY_LOAN_nbank_cf_allnum_m12_id']>0 else np.nan
    dict_out['BAIRONG_APPLY_LOAN_nbank_p2p_orgnum_m03vm06_cell']= dict_out['BAIRONG_APPLY_LOAN_nbank_p2p_orgnum_m03_cell']/dict_out['BAIRONG_APPLY_LOAN_nbank_p2p_orgnum_m06_cell'] if dict_out['BAIRONG_APPLY_LOAN_nbank_p2p_orgnum_m06_cell']>0 else np.nan
    dict_out['BAIRONG_APPLY_LOAN_nbank_ca_orgnum_m01vm06_cell']= dict_out['BAIRONG_APPLY_LOAN_nbank_ca_orgnum_m01_cell']/dict_out['BAIRONG_APPLY_LOAN_nbank_ca_orgnum_m06_cell'] if dict_out['BAIRONG_APPLY_LOAN_nbank_ca_orgnum_m06_cell']>0 else np.nan
    dict_out['BAIRONG_APPLY_LOAN_bank_min_inteday_m06vm12_cell']= dict_out['BAIRONG_APPLY_LOAN_bank_min_inteday_m06_cell']/dict_out['BAIRONG_APPLY_LOAN_bank_min_inteday_m12_cell'] if dict_out['BAIRONG_APPLY_LOAN_bank_min_inteday_m12_cell']>0 else np.nan
    dict_out['BAIRONG_APPLY_LOAN_bank_tra_orgnum_m01vm03_id']= dict_out['BAIRONG_APPLY_LOAN_bank_tra_orgnum_m01_id']/dict_out['BAIRONG_APPLY_LOAN_bank_tra_orgnum_m03_id'] if dict_out['BAIRONG_APPLY_LOAN_bank_tra_orgnum_m03_id']>0 else np.nan
    dict_out['BAIRONG_APPLY_LOAN_nbank_orgnum_d07vm01_cell']= dict_out['BAIRONG_APPLY_LOAN_nbank_orgnum_d07_cell']/dict_out['BAIRONG_APPLY_LOAN_nbank_orgnum_m01_cell'] if dict_out['BAIRONG_APPLY_LOAN_nbank_orgnum_m01_cell']>0 else np.nan
    dict_out['BAIRONG_APPLY_LOAN_caon_orgnum_d15vm06_id']= dict_out['BAIRONG_APPLY_LOAN_caon_orgnum_d15_id']/dict_out['BAIRONG_APPLY_LOAN_caon_orgnum_m06_id'] if dict_out['BAIRONG_APPLY_LOAN_caon_orgnum_m06_id']>0 else np.nan
    dict_out['BAIRONG_APPLY_LOAN_bank_tot_mons_m03vm06_id']= dict_out['BAIRONG_APPLY_LOAN_bank_tot_mons_m03_id']/dict_out['BAIRONG_APPLY_LOAN_bank_tot_mons_m06_id'] if dict_out['BAIRONG_APPLY_LOAN_bank_tot_mons_m06_id']>0 else np.nan
    dict_out['BAIRONG_APPLY_LOAN_nbank_nsloan_allnum_d15vm12_id']= dict_out['BAIRONG_APPLY_LOAN_nbank_nsloan_allnum_d15_id']/dict_out['BAIRONG_APPLY_LOAN_nbank_nsloan_allnum_m12_id'] if dict_out['BAIRONG_APPLY_LOAN_nbank_nsloan_allnum_m12_id']>0 else np.nan
    dict_out['BAIRONG_APPLY_LOAN_bank_ret_allnum_m06_idsubcell']= dict_out['BAIRONG_APPLY_LOAN_bank_ret_allnum_m06_id']-dict_out['BAIRONG_APPLY_LOAN_bank_ret_allnum_m06_cell']
    dict_out['BAIRONG_APPLY_LOAN_cooff_orgnum_m01vm06_cell']= dict_out['BAIRONG_APPLY_LOAN_cooff_orgnum_m01_cell']/dict_out['BAIRONG_APPLY_LOAN_cooff_orgnum_m06_cell'] if dict_out['BAIRONG_APPLY_LOAN_cooff_orgnum_m06_cell']>0 else np.nan
    dict_out['BAIRONG_APPLY_LOAN_nbank_oth_orgnum_d15vm06_id']= dict_out['BAIRONG_APPLY_LOAN_nbank_oth_orgnum_d15_id']/dict_out['BAIRONG_APPLY_LOAN_nbank_oth_orgnum_m06_id'] if dict_out['BAIRONG_APPLY_LOAN_nbank_oth_orgnum_m06_id']>0 else np.nan
    dict_out['BAIRONG_APPLY_LOAN_bank_ret_orgnum_m03vm06_id']= dict_out['BAIRONG_APPLY_LOAN_bank_ret_orgnum_m03_id']/dict_out['BAIRONG_APPLY_LOAN_bank_ret_orgnum_m06_id'] if dict_out['BAIRONG_APPLY_LOAN_bank_ret_orgnum_m06_id']>0 else np.nan
    dict_out['BAIRONG_APPLY_LOAN_nbank_p2p_allnum_m03vm12_cell']= dict_out['BAIRONG_APPLY_LOAN_nbank_p2p_allnum_m03_cell']/dict_out['BAIRONG_APPLY_LOAN_nbank_p2p_allnum_m12_cell'] if dict_out['BAIRONG_APPLY_LOAN_nbank_p2p_allnum_m12_cell']>0 else np.nan
    dict_out['BAIRONG_APPLY_LOAN_bank_ret_orgnum_m06vm12_id']= dict_out['BAIRONG_APPLY_LOAN_bank_ret_orgnum_m06_id']/dict_out['BAIRONG_APPLY_LOAN_bank_ret_orgnum_m12_id'] if dict_out['BAIRONG_APPLY_LOAN_bank_ret_orgnum_m12_id']>0 else np.nan
    dict_out['BAIRONG_APPLY_LOAN_nbank_allnum_m01vm12_id']= dict_out['BAIRONG_APPLY_LOAN_nbank_allnum_m01_id']/dict_out['BAIRONG_APPLY_LOAN_nbank_allnum_m12_id'] if dict_out['BAIRONG_APPLY_LOAN_nbank_allnum_m12_id']>0 else np.nan
    dict_out['BAIRONG_APPLY_LOAN_caon_orgnum_d07vm12_cell']= dict_out['BAIRONG_APPLY_LOAN_caon_orgnum_d07_cell']/dict_out['BAIRONG_APPLY_LOAN_caon_orgnum_m12_cell'] if dict_out['BAIRONG_APPLY_LOAN_caon_orgnum_m12_cell']>0 else np.nan
    dict_out['BAIRONG_APPLY_LOAN_nbank_cf_allnum_m01vm03_id']= dict_out['BAIRONG_APPLY_LOAN_nbank_cf_allnum_m01_id']/dict_out['BAIRONG_APPLY_LOAN_nbank_cf_allnum_m03_id'] if dict_out['BAIRONG_APPLY_LOAN_nbank_cf_allnum_m03_id']>0 else np.nan
    dict_out['BAIRONG_APPLY_LOAN_caoff_orgnum_d07vm06_cell']= dict_out['BAIRONG_APPLY_LOAN_caoff_orgnum_d07_cell']/dict_out['BAIRONG_APPLY_LOAN_caoff_orgnum_m06_cell'] if dict_out['BAIRONG_APPLY_LOAN_caoff_orgnum_m06_cell']>0 else np.nan
    dict_out['BAIRONG_APPLY_LOAN_caoff_allnum_d15vm12_cell']= dict_out['BAIRONG_APPLY_LOAN_caoff_allnum_d15_cell']/dict_out['BAIRONG_APPLY_LOAN_caoff_allnum_m12_cell'] if dict_out['BAIRONG_APPLY_LOAN_caoff_allnum_m12_cell']>0 else np.nan
    dict_out['BAIRONG_APPLY_LOAN_caoff_orgnum_m01vm12_id']= dict_out['BAIRONG_APPLY_LOAN_caoff_orgnum_m01_id']/dict_out['BAIRONG_APPLY_LOAN_caoff_orgnum_m12_id'] if dict_out['BAIRONG_APPLY_LOAN_caoff_orgnum_m12_id']>0 else np.nan
    dict_out['BAIRONG_APPLY_LOAN_nbank_ca_orgnum_m03vm12_cell']= dict_out['BAIRONG_APPLY_LOAN_nbank_ca_orgnum_m03_cell']/dict_out['BAIRONG_APPLY_LOAN_nbank_ca_orgnum_m12_cell'] if dict_out['BAIRONG_APPLY_LOAN_nbank_ca_orgnum_m12_cell']>0 else np.nan
    dict_out['BAIRONG_APPLY_LOAN_nbank_week_allnum_d15vm12_cell']= dict_out['BAIRONG_APPLY_LOAN_nbank_week_allnum_d15_cell']/dict_out['BAIRONG_APPLY_LOAN_nbank_week_allnum_m12_cell'] if dict_out['BAIRONG_APPLY_LOAN_nbank_week_allnum_m12_cell']>0 else np.nan
    dict_out['BAIRONG_APPLY_LOAN_nbank_nsloan_allnum_m06vm12_id']= dict_out['BAIRONG_APPLY_LOAN_nbank_nsloan_allnum_m06_id']/dict_out['BAIRONG_APPLY_LOAN_nbank_nsloan_allnum_m12_id'] if dict_out['BAIRONG_APPLY_LOAN_nbank_nsloan_allnum_m12_id']>0 else np.nan
    dict_out['BAIRONG_APPLY_LOAN_nbank_nsloan_allnum_m03vm12_id']= dict_out['BAIRONG_APPLY_LOAN_nbank_nsloan_allnum_m03_id']/dict_out['BAIRONG_APPLY_LOAN_nbank_nsloan_allnum_m12_id'] if dict_out['BAIRONG_APPLY_LOAN_nbank_nsloan_allnum_m12_id']>0 else np.nan
    dict_out['BAIRONG_APPLY_LOAN_nbank_night_allnum_m01vm06_id']= dict_out['BAIRONG_APPLY_LOAN_nbank_night_allnum_m01_id']/dict_out['BAIRONG_APPLY_LOAN_nbank_night_allnum_m06_id'] if dict_out['BAIRONG_APPLY_LOAN_nbank_night_allnum_m06_id']>0 else np.nan
    dict_out['BAIRONG_APPLY_LOAN_nbank_week_orgnum_d07vm06_id']= dict_out['BAIRONG_APPLY_LOAN_nbank_week_orgnum_d07_id']/dict_out['BAIRONG_APPLY_LOAN_nbank_week_orgnum_m06_id'] if dict_out['BAIRONG_APPLY_LOAN_nbank_week_orgnum_m06_id']>0 else np.nan
    dict_out['BAIRONG_APPLY_LOAN_nbank_cons_orgnum_m01vm06_id']= dict_out['BAIRONG_APPLY_LOAN_nbank_cons_orgnum_m01_id']/dict_out['BAIRONG_APPLY_LOAN_nbank_cons_orgnum_m06_id'] if dict_out['BAIRONG_APPLY_LOAN_nbank_cons_orgnum_m06_id']>0 else np.nan
    dict_out['BAIRONG_APPLY_LOAN_nbank_cons_orgnum_d15vm01_cell']= dict_out['BAIRONG_APPLY_LOAN_nbank_cons_orgnum_d15_cell']/dict_out['BAIRONG_APPLY_LOAN_nbank_cons_orgnum_m01_cell'] if dict_out['BAIRONG_APPLY_LOAN_nbank_cons_orgnum_m01_cell']>0 else np.nan
    dict_out['BAIRONG_APPLY_LOAN_caoff_allnum_d07vm12_cell']= dict_out['BAIRONG_APPLY_LOAN_caoff_allnum_d07_cell']/dict_out['BAIRONG_APPLY_LOAN_caoff_allnum_m12_cell'] if dict_out['BAIRONG_APPLY_LOAN_caoff_allnum_m12_cell']>0 else np.nan
    dict_out['BAIRONG_APPLY_LOAN_nbank_cf_allnum_m01vm03_cell']= dict_out['BAIRONG_APPLY_LOAN_nbank_cf_allnum_m01_cell']/dict_out['BAIRONG_APPLY_LOAN_nbank_cf_allnum_m03_cell'] if dict_out['BAIRONG_APPLY_LOAN_nbank_cf_allnum_m03_cell']>0 else np.nan
    dict_out['BAIRONG_APPLY_LOAN_caoff_allnum_m01vm06_cell']= dict_out['BAIRONG_APPLY_LOAN_caoff_allnum_m01_cell']/dict_out['BAIRONG_APPLY_LOAN_caoff_allnum_m06_cell'] if dict_out['BAIRONG_APPLY_LOAN_caoff_allnum_m06_cell']>0 else np.nan
    dict_out['BAIRONG_APPLY_LOAN_bank_night_allnum_m06vm12_cell']= dict_out['BAIRONG_APPLY_LOAN_bank_night_allnum_m06_cell']/dict_out['BAIRONG_APPLY_LOAN_bank_night_allnum_m12_cell'] if dict_out['BAIRONG_APPLY_LOAN_bank_night_allnum_m12_cell']>0 else np.nan
    dict_out['BAIRONG_APPLY_LOAN_cooff_allnum_d07vm12_cell']= dict_out['BAIRONG_APPLY_LOAN_cooff_allnum_d07_cell']/dict_out['BAIRONG_APPLY_LOAN_cooff_allnum_m12_cell'] if dict_out['BAIRONG_APPLY_LOAN_cooff_allnum_m12_cell']>0 else np.nan
    dict_out['BAIRONG_APPLY_LOAN_nbank_cons_orgnum_m01vm03_id']= dict_out['BAIRONG_APPLY_LOAN_nbank_cons_orgnum_m01_id']/dict_out['BAIRONG_APPLY_LOAN_nbank_cons_orgnum_m03_id'] if dict_out['BAIRONG_APPLY_LOAN_nbank_cons_orgnum_m03_id']>0 else np.nan
    dict_out['BAIRONG_APPLY_LOAN_nbank_oth_allnum_m03vm12_id']= dict_out['BAIRONG_APPLY_LOAN_nbank_oth_allnum_m03_id']/dict_out['BAIRONG_APPLY_LOAN_nbank_oth_allnum_m12_id'] if dict_out['BAIRONG_APPLY_LOAN_nbank_oth_allnum_m12_id']>0 else np.nan
    dict_out['BAIRONG_APPLY_LOAN_nbank_cf_allnum_m03vm06_cell']= dict_out['BAIRONG_APPLY_LOAN_nbank_cf_allnum_m03_cell']/dict_out['BAIRONG_APPLY_LOAN_nbank_cf_allnum_m06_cell'] if dict_out['BAIRONG_APPLY_LOAN_nbank_cf_allnum_m06_cell']>0 else np.nan
    dict_out['BAIRONG_APPLY_LOAN_nbank_nsloan_orgnum_d15vm12_cell']= dict_out['BAIRONG_APPLY_LOAN_nbank_nsloan_orgnum_d15_cell']/dict_out['BAIRONG_APPLY_LOAN_nbank_nsloan_orgnum_m12_cell'] if dict_out['BAIRONG_APPLY_LOAN_nbank_nsloan_orgnum_m12_cell']>0 else np.nan
    dict_out['BAIRONG_APPLY_LOAN_nbank_ca_allnum_d15vm12_id']= dict_out['BAIRONG_APPLY_LOAN_nbank_ca_allnum_d15_id']/dict_out['BAIRONG_APPLY_LOAN_nbank_ca_allnum_m12_id'] if dict_out['BAIRONG_APPLY_LOAN_nbank_ca_allnum_m12_id']>0 else np.nan
    dict_out['BAIRONG_APPLY_LOAN_caon_allnum_d15vm06_cell']= dict_out['BAIRONG_APPLY_LOAN_caon_allnum_d15_cell']/dict_out['BAIRONG_APPLY_LOAN_caon_allnum_m06_cell'] if dict_out['BAIRONG_APPLY_LOAN_caon_allnum_m06_cell']>0 else np.nan
    dict_out['BAIRONG_APPLY_LOAN_nbank_cf_orgnum_m03vm06_id']= dict_out['BAIRONG_APPLY_LOAN_nbank_cf_orgnum_m03_id']/dict_out['BAIRONG_APPLY_LOAN_nbank_cf_orgnum_m06_id'] if dict_out['BAIRONG_APPLY_LOAN_nbank_cf_orgnum_m06_id']>0 else np.nan
    dict_out['BAIRONG_APPLY_LOAN_nbank_sloan_allnum_d07vm06_id']= dict_out['BAIRONG_APPLY_LOAN_nbank_sloan_allnum_d07_id']/dict_out['BAIRONG_APPLY_LOAN_nbank_sloan_allnum_m06_id'] if dict_out['BAIRONG_APPLY_LOAN_nbank_sloan_allnum_m06_id']>0 else np.nan
    dict_out['BAIRONG_APPLY_LOAN_nbank_nsloan_orgnum_m03vm06_cell']= dict_out['BAIRONG_APPLY_LOAN_nbank_nsloan_orgnum_m03_cell']/dict_out['BAIRONG_APPLY_LOAN_nbank_nsloan_orgnum_m06_cell'] if dict_out['BAIRONG_APPLY_LOAN_nbank_nsloan_orgnum_m06_cell']>0 else np.nan
    dict_out['BAIRONG_APPLY_LOAN_nbank_sloan_allnum_d15vm12_cell']= dict_out['BAIRONG_APPLY_LOAN_nbank_sloan_allnum_d15_cell']/dict_out['BAIRONG_APPLY_LOAN_nbank_sloan_allnum_m12_cell'] if dict_out['BAIRONG_APPLY_LOAN_nbank_sloan_allnum_m12_cell']>0 else np.nan
    dict_out['BAIRONG_APPLY_LOAN_nbank_week_orgnum_d07vd15_id']= dict_out['BAIRONG_APPLY_LOAN_nbank_week_orgnum_d07_id']/dict_out['BAIRONG_APPLY_LOAN_nbank_week_orgnum_d15_id'] if dict_out['BAIRONG_APPLY_LOAN_nbank_week_orgnum_d15_id']>0 else np.nan
    dict_out['BAIRONG_APPLY_LOAN_min_inteday_m12_idsubcell']= dict_out['BAIRONG_APPLY_LOAN_min_inteday_m12_id']-dict_out['BAIRONG_APPLY_LOAN_min_inteday_m12_cell']
    dict_out['BAIRONG_APPLY_LOAN_caon_allnum_m01vm12_id']= dict_out['BAIRONG_APPLY_LOAN_caon_allnum_m01_id']/dict_out['BAIRONG_APPLY_LOAN_caon_allnum_m12_id'] if dict_out['BAIRONG_APPLY_LOAN_caon_allnum_m12_id']>0 else np.nan
    dict_out['BAIRONG_APPLY_LOAN_caoff_orgnum_m12_idsubcell']= dict_out['BAIRONG_APPLY_LOAN_caoff_orgnum_m12_id']-dict_out['BAIRONG_APPLY_LOAN_caoff_orgnum_m12_cell']
    dict_out['BAIRONG_APPLY_LOAN_caon_allnum_d15vm12_id']= dict_out['BAIRONG_APPLY_LOAN_caon_allnum_d15_id']/dict_out['BAIRONG_APPLY_LOAN_caon_allnum_m12_id'] if dict_out['BAIRONG_APPLY_LOAN_caon_allnum_m12_id']>0 else np.nan
    dict_out['BAIRONG_APPLY_LOAN_pdl_allnum_d07vm01_cell']= dict_out['BAIRONG_APPLY_LOAN_pdl_allnum_d07_cell']/dict_out['BAIRONG_APPLY_LOAN_pdl_allnum_m01_cell'] if dict_out['BAIRONG_APPLY_LOAN_pdl_allnum_m01_cell']>0 else np.nan
    dict_out['BAIRONG_APPLY_LOAN_nbank_week_orgnum_m01vm03_cell']= dict_out['BAIRONG_APPLY_LOAN_nbank_week_orgnum_m01_cell']/dict_out['BAIRONG_APPLY_LOAN_nbank_week_orgnum_m03_cell'] if dict_out['BAIRONG_APPLY_LOAN_nbank_week_orgnum_m03_cell']>0 else np.nan
    dict_out['BAIRONG_APPLY_LOAN_nbank_ca_orgnum_m01vm03_cell']= dict_out['BAIRONG_APPLY_LOAN_nbank_ca_orgnum_m01_cell']/dict_out['BAIRONG_APPLY_LOAN_nbank_ca_orgnum_m03_cell'] if dict_out['BAIRONG_APPLY_LOAN_nbank_ca_orgnum_m03_cell']>0 else np.nan
    dict_out['BAIRONG_APPLY_LOAN_cooff_allnum_d15vm06_cell']= dict_out['BAIRONG_APPLY_LOAN_cooff_allnum_d15_cell']/dict_out['BAIRONG_APPLY_LOAN_cooff_allnum_m06_cell'] if dict_out['BAIRONG_APPLY_LOAN_cooff_allnum_m06_cell']>0 else np.nan
    dict_out['BAIRONG_APPLY_LOAN_nbank_week_orgnum_d15vm01_id']= dict_out['BAIRONG_APPLY_LOAN_nbank_week_orgnum_d15_id']/dict_out['BAIRONG_APPLY_LOAN_nbank_week_orgnum_m01_id'] if dict_out['BAIRONG_APPLY_LOAN_nbank_week_orgnum_m01_id']>0 else np.nan
    dict_out['BAIRONG_APPLY_LOAN_bank_tra_orgnum_d15vm06_cell']= dict_out['BAIRONG_APPLY_LOAN_bank_tra_orgnum_d15_cell']/dict_out['BAIRONG_APPLY_LOAN_bank_tra_orgnum_m06_cell'] if dict_out['BAIRONG_APPLY_LOAN_bank_tra_orgnum_m06_cell']>0 else np.nan
    dict_out['BAIRONG_APPLY_LOAN_nbank_finlea_allnum_m03vm12_cell']= dict_out['BAIRONG_APPLY_LOAN_nbank_finlea_allnum_m03_cell']/dict_out['BAIRONG_APPLY_LOAN_nbank_finlea_allnum_m12_cell'] if dict_out['BAIRONG_APPLY_LOAN_nbank_finlea_allnum_m12_cell']>0 else np.nan
    dict_out['BAIRONG_APPLY_LOAN_af_allnum_m03vm12_cell']= dict_out['BAIRONG_APPLY_LOAN_af_allnum_m03_cell']/dict_out['BAIRONG_APPLY_LOAN_af_allnum_m12_cell'] if dict_out['BAIRONG_APPLY_LOAN_af_allnum_m12_cell']>0 else np.nan
    dict_out['BAIRONG_APPLY_LOAN_nbank_night_orgnum_d15vm06_id']= dict_out['BAIRONG_APPLY_LOAN_nbank_night_orgnum_d15_id']/dict_out['BAIRONG_APPLY_LOAN_nbank_night_orgnum_m06_id'] if dict_out['BAIRONG_APPLY_LOAN_nbank_night_orgnum_m06_id']>0 else np.nan
    dict_out['BAIRONG_APPLY_LOAN_nbank_allnum_d15vm06_cell']= dict_out['BAIRONG_APPLY_LOAN_nbank_allnum_d15_cell']/dict_out['BAIRONG_APPLY_LOAN_nbank_allnum_m06_cell'] if dict_out['BAIRONG_APPLY_LOAN_nbank_allnum_m06_cell']>0 else np.nan
    dict_out['BAIRONG_APPLY_LOAN_tot_mons_m06vm12_cell']= dict_out['BAIRONG_APPLY_LOAN_tot_mons_m06_cell']/dict_out['BAIRONG_APPLY_LOAN_tot_mons_m12_cell'] if dict_out['BAIRONG_APPLY_LOAN_tot_mons_m12_cell']>0 else np.nan
    dict_out['BAIRONG_APPLY_LOAN_oth_orgnum_d15_idsubcell']= dict_out['BAIRONG_APPLY_LOAN_oth_orgnum_d15_id']-dict_out['BAIRONG_APPLY_LOAN_oth_orgnum_d15_cell']
    dict_out['BAIRONG_APPLY_LOAN_nbank_week_orgnum_d15vm06_id']= dict_out['BAIRONG_APPLY_LOAN_nbank_week_orgnum_d15_id']/dict_out['BAIRONG_APPLY_LOAN_nbank_week_orgnum_m06_id'] if dict_out['BAIRONG_APPLY_LOAN_nbank_week_orgnum_m06_id']>0 else np.nan
    dict_out['BAIRONG_APPLY_LOAN_nbank_orgnum_d07vm12_id']= dict_out['BAIRONG_APPLY_LOAN_nbank_orgnum_d07_id']/dict_out['BAIRONG_APPLY_LOAN_nbank_orgnum_m12_id'] if dict_out['BAIRONG_APPLY_LOAN_nbank_orgnum_m12_id']>0 else np.nan
    dict_out['BAIRONG_APPLY_LOAN_bank_orgnum_m03vm06_id']= dict_out['BAIRONG_APPLY_LOAN_bank_orgnum_m03_id']/dict_out['BAIRONG_APPLY_LOAN_bank_orgnum_m06_id'] if dict_out['BAIRONG_APPLY_LOAN_bank_orgnum_m06_id']>0 else np.nan
    dict_out['BAIRONG_APPLY_LOAN_nbank_nsloan_orgnum_m01vm06_cell']= dict_out['BAIRONG_APPLY_LOAN_nbank_nsloan_orgnum_m01_cell']/dict_out['BAIRONG_APPLY_LOAN_nbank_nsloan_orgnum_m06_cell'] if dict_out['BAIRONG_APPLY_LOAN_nbank_nsloan_orgnum_m06_cell']>0 else np.nan
    dict_out['BAIRONG_APPLY_LOAN_nbank_min_monnum_m03vm06_id']= dict_out['BAIRONG_APPLY_LOAN_nbank_min_monnum_m03_id']/dict_out['BAIRONG_APPLY_LOAN_nbank_min_monnum_m06_id'] if dict_out['BAIRONG_APPLY_LOAN_nbank_min_monnum_m06_id']>0 else np.nan
    dict_out['BAIRONG_APPLY_LOAN_bank_ret_allnum_m03vm12_id']= dict_out['BAIRONG_APPLY_LOAN_bank_ret_allnum_m03_id']/dict_out['BAIRONG_APPLY_LOAN_bank_ret_allnum_m12_id'] if dict_out['BAIRONG_APPLY_LOAN_bank_ret_allnum_m12_id']>0 else np.nan
    dict_out['BAIRONG_APPLY_LOAN_nbank_min_monnum_m06_idsubcell']= dict_out['BAIRONG_APPLY_LOAN_nbank_min_monnum_m06_id']-dict_out['BAIRONG_APPLY_LOAN_nbank_min_monnum_m06_cell']
    dict_out['BAIRONG_APPLY_LOAN_max_inteday_m03vm12_cell']= dict_out['BAIRONG_APPLY_LOAN_max_inteday_m03_cell']/dict_out['BAIRONG_APPLY_LOAN_max_inteday_m12_cell'] if dict_out['BAIRONG_APPLY_LOAN_max_inteday_m12_cell']>0 else np.nan
    dict_out['BAIRONG_APPLY_LOAN_nbank_cons_allnum_d15vm06_id']= dict_out['BAIRONG_APPLY_LOAN_nbank_cons_allnum_d15_id']/dict_out['BAIRONG_APPLY_LOAN_nbank_cons_allnum_m06_id'] if dict_out['BAIRONG_APPLY_LOAN_nbank_cons_allnum_m06_id']>0 else np.nan
    dict_out['BAIRONG_APPLY_LOAN_nbank_min_monnum_m03vm06_cell']= dict_out['BAIRONG_APPLY_LOAN_nbank_min_monnum_m03_cell']/dict_out['BAIRONG_APPLY_LOAN_nbank_min_monnum_m06_cell'] if dict_out['BAIRONG_APPLY_LOAN_nbank_min_monnum_m06_cell']>0 else np.nan
    dict_out['BAIRONG_APPLY_LOAN_nbank_cf_allnum_d15vm01_cell']= dict_out['BAIRONG_APPLY_LOAN_nbank_cf_allnum_d15_cell']/dict_out['BAIRONG_APPLY_LOAN_nbank_cf_allnum_m01_cell'] if dict_out['BAIRONG_APPLY_LOAN_nbank_cf_allnum_m01_cell']>0 else np.nan
    dict_out['BAIRONG_APPLY_LOAN_caon_orgnum_m01vm03_id']= dict_out['BAIRONG_APPLY_LOAN_caon_orgnum_m01_id']/dict_out['BAIRONG_APPLY_LOAN_caon_orgnum_m03_id'] if dict_out['BAIRONG_APPLY_LOAN_caon_orgnum_m03_id']>0 else np.nan
    dict_out['BAIRONG_APPLY_LOAN_nbank_mc_allnum_m03vm06_cell']= dict_out['BAIRONG_APPLY_LOAN_nbank_mc_allnum_m03_cell']/dict_out['BAIRONG_APPLY_LOAN_nbank_mc_allnum_m06_cell'] if dict_out['BAIRONG_APPLY_LOAN_nbank_mc_allnum_m06_cell']>0 else np.nan
    dict_out['BAIRONG_APPLY_LOAN_nbank_nsloan_orgnum_d07vm06_id']= dict_out['BAIRONG_APPLY_LOAN_nbank_nsloan_orgnum_d07_id']/dict_out['BAIRONG_APPLY_LOAN_nbank_nsloan_orgnum_m06_id'] if dict_out['BAIRONG_APPLY_LOAN_nbank_nsloan_orgnum_m06_id']>0 else np.nan
    dict_out['BAIRONG_APPLY_LOAN_nbank_allnum_d07vm06_cell']= dict_out['BAIRONG_APPLY_LOAN_nbank_allnum_d07_cell']/dict_out['BAIRONG_APPLY_LOAN_nbank_allnum_m06_cell'] if dict_out['BAIRONG_APPLY_LOAN_nbank_allnum_m06_cell']>0 else np.nan
    dict_out['BAIRONG_APPLY_LOAN_oth_allnum_d15vm06_cell']= dict_out['BAIRONG_APPLY_LOAN_oth_allnum_d15_cell']/dict_out['BAIRONG_APPLY_LOAN_oth_allnum_m06_cell'] if dict_out['BAIRONG_APPLY_LOAN_oth_allnum_m06_cell']>0 else np.nan
    dict_out['BAIRONG_APPLY_LOAN_nbank_night_allnum_m06vm12_cell']= dict_out['BAIRONG_APPLY_LOAN_nbank_night_allnum_m06_cell']/dict_out['BAIRONG_APPLY_LOAN_nbank_night_allnum_m12_cell'] if dict_out['BAIRONG_APPLY_LOAN_nbank_night_allnum_m12_cell']>0 else np.nan
    dict_out['BAIRONG_APPLY_LOAN_nbank_mc_orgnum_m03_idsubcell']= dict_out['BAIRONG_APPLY_LOAN_nbank_mc_orgnum_m03_id']-dict_out['BAIRONG_APPLY_LOAN_nbank_mc_orgnum_m03_cell']
    dict_out['BAIRONG_APPLY_LOAN_nbank_p2p_allnum_m06_idsubcell']= dict_out['BAIRONG_APPLY_LOAN_nbank_p2p_allnum_m06_id']-dict_out['BAIRONG_APPLY_LOAN_nbank_p2p_allnum_m06_cell']
    dict_out['BAIRONG_APPLY_LOAN_oth_allnum_m03vm06_id']= dict_out['BAIRONG_APPLY_LOAN_oth_allnum_m03_id']/dict_out['BAIRONG_APPLY_LOAN_oth_allnum_m06_id'] if dict_out['BAIRONG_APPLY_LOAN_oth_allnum_m06_id']>0 else np.nan
    dict_out['BAIRONG_APPLY_LOAN_pdl_allnum_d07vd15_cell']= dict_out['BAIRONG_APPLY_LOAN_pdl_allnum_d07_cell']/dict_out['BAIRONG_APPLY_LOAN_pdl_allnum_d15_cell'] if dict_out['BAIRONG_APPLY_LOAN_pdl_allnum_d15_cell']>0 else np.nan
    dict_out['BAIRONG_APPLY_LOAN_nbank_else_allnum_m01vm06_cell']= dict_out['BAIRONG_APPLY_LOAN_nbank_else_allnum_m01_cell']/dict_out['BAIRONG_APPLY_LOAN_nbank_else_allnum_m06_cell'] if dict_out['BAIRONG_APPLY_LOAN_nbank_else_allnum_m06_cell']>0 else np.nan
    dict_out['BAIRONG_APPLY_LOAN_nbank_cf_allnum_d07vm06_id']= dict_out['BAIRONG_APPLY_LOAN_nbank_cf_allnum_d07_id']/dict_out['BAIRONG_APPLY_LOAN_nbank_cf_allnum_m06_id'] if dict_out['BAIRONG_APPLY_LOAN_nbank_cf_allnum_m06_id']>0 else np.nan
    dict_out['BAIRONG_APPLY_LOAN_nbank_else_allnum_m01vm12_id']= dict_out['BAIRONG_APPLY_LOAN_nbank_else_allnum_m01_id']/dict_out['BAIRONG_APPLY_LOAN_nbank_else_allnum_m12_id'] if dict_out['BAIRONG_APPLY_LOAN_nbank_else_allnum_m12_id']>0 else np.nan
    dict_out['BAIRONG_APPLY_LOAN_af_allnum_m03vm06_cell']= dict_out['BAIRONG_APPLY_LOAN_af_allnum_m03_cell']/dict_out['BAIRONG_APPLY_LOAN_af_allnum_m06_cell'] if dict_out['BAIRONG_APPLY_LOAN_af_allnum_m06_cell']>0 else np.nan
    dict_out['BAIRONG_APPLY_LOAN_cooff_orgnum_m01vm12_id']= dict_out['BAIRONG_APPLY_LOAN_cooff_orgnum_m01_id']/dict_out['BAIRONG_APPLY_LOAN_cooff_orgnum_m12_id'] if dict_out['BAIRONG_APPLY_LOAN_cooff_orgnum_m12_id']>0 else np.nan
    dict_out['BAIRONG_APPLY_LOAN_nbank_nsloan_allnum_m01vm03_id']= dict_out['BAIRONG_APPLY_LOAN_nbank_nsloan_allnum_m01_id']/dict_out['BAIRONG_APPLY_LOAN_nbank_nsloan_allnum_m03_id'] if dict_out['BAIRONG_APPLY_LOAN_nbank_nsloan_allnum_m03_id']>0 else np.nan
    dict_out['BAIRONG_APPLY_LOAN_nbank_night_allnum_m03vm12_cell']= dict_out['BAIRONG_APPLY_LOAN_nbank_night_allnum_m03_cell']/dict_out['BAIRONG_APPLY_LOAN_nbank_night_allnum_m12_cell'] if dict_out['BAIRONG_APPLY_LOAN_nbank_night_allnum_m12_cell']>0 else np.nan
    dict_out['BAIRONG_APPLY_LOAN_rel_orgnum_d15vm12_cell']= dict_out['BAIRONG_APPLY_LOAN_rel_orgnum_d15_cell']/dict_out['BAIRONG_APPLY_LOAN_rel_orgnum_m12_cell'] if dict_out['BAIRONG_APPLY_LOAN_rel_orgnum_m12_cell']>0 else np.nan
    dict_out['BAIRONG_APPLY_LOAN_bank_week_allnum_m03vm06_id']= dict_out['BAIRONG_APPLY_LOAN_bank_week_allnum_m03_id']/dict_out['BAIRONG_APPLY_LOAN_bank_week_allnum_m06_id'] if dict_out['BAIRONG_APPLY_LOAN_bank_week_allnum_m06_id']>0 else np.nan
    dict_out['BAIRONG_APPLY_LOAN_nbank_allnum_d07vm06_id']= dict_out['BAIRONG_APPLY_LOAN_nbank_allnum_d07_id']/dict_out['BAIRONG_APPLY_LOAN_nbank_allnum_m06_id'] if dict_out['BAIRONG_APPLY_LOAN_nbank_allnum_m06_id']>0 else np.nan
    dict_out['BAIRONG_APPLY_LOAN_nbank_oth_orgnum_d07vm06_id']= dict_out['BAIRONG_APPLY_LOAN_nbank_oth_orgnum_d07_id']/dict_out['BAIRONG_APPLY_LOAN_nbank_oth_orgnum_m06_id'] if dict_out['BAIRONG_APPLY_LOAN_nbank_oth_orgnum_m06_id']>0 else np.nan
    dict_out['BAIRONG_APPLY_LOAN_nbank_nsloan_allnum_d07vm01_cell']= dict_out['BAIRONG_APPLY_LOAN_nbank_nsloan_allnum_d07_cell']/dict_out['BAIRONG_APPLY_LOAN_nbank_nsloan_allnum_m01_cell'] if dict_out['BAIRONG_APPLY_LOAN_nbank_nsloan_allnum_m01_cell']>0 else np.nan
    dict_out['BAIRONG_APPLY_LOAN_pdl_allnum_m01vm12_cell']= dict_out['BAIRONG_APPLY_LOAN_pdl_allnum_m01_cell']/dict_out['BAIRONG_APPLY_LOAN_pdl_allnum_m12_cell'] if dict_out['BAIRONG_APPLY_LOAN_pdl_allnum_m12_cell']>0 else np.nan
    dict_out['BAIRONG_APPLY_LOAN_nbank_cons_allnum_m06vm12_cell']= dict_out['BAIRONG_APPLY_LOAN_nbank_cons_allnum_m06_cell']/dict_out['BAIRONG_APPLY_LOAN_nbank_cons_allnum_m12_cell'] if dict_out['BAIRONG_APPLY_LOAN_nbank_cons_allnum_m12_cell']>0 else np.nan
    dict_out['BAIRONG_APPLY_LOAN_nbank_orgnum_m03vm06_id']= dict_out['BAIRONG_APPLY_LOAN_nbank_orgnum_m03_id']/dict_out['BAIRONG_APPLY_LOAN_nbank_orgnum_m06_id'] if dict_out['BAIRONG_APPLY_LOAN_nbank_orgnum_m06_id']>0 else np.nan
    dict_out['BAIRONG_APPLY_LOAN_nbank_oth_allnum_m01vm03_cell']= dict_out['BAIRONG_APPLY_LOAN_nbank_oth_allnum_m01_cell']/dict_out['BAIRONG_APPLY_LOAN_nbank_oth_allnum_m03_cell'] if dict_out['BAIRONG_APPLY_LOAN_nbank_oth_allnum_m03_cell']>0 else np.nan
    dict_out['BAIRONG_APPLY_LOAN_avg_monnum_m06_idsubcell']= dict_out['BAIRONG_APPLY_LOAN_avg_monnum_m06_id']-dict_out['BAIRONG_APPLY_LOAN_avg_monnum_m06_cell']
    dict_out['BAIRONG_APPLY_LOAN_nbank_cf_allnum_d15vm12_id']= dict_out['BAIRONG_APPLY_LOAN_nbank_cf_allnum_d15_id']/dict_out['BAIRONG_APPLY_LOAN_nbank_cf_allnum_m12_id'] if dict_out['BAIRONG_APPLY_LOAN_nbank_cf_allnum_m12_id']>0 else np.nan
    dict_out['BAIRONG_APPLY_LOAN_nbank_p2p_orgnum_m03vm12_id']= dict_out['BAIRONG_APPLY_LOAN_nbank_p2p_orgnum_m03_id']/dict_out['BAIRONG_APPLY_LOAN_nbank_p2p_orgnum_m12_id'] if dict_out['BAIRONG_APPLY_LOAN_nbank_p2p_orgnum_m12_id']>0 else np.nan
    dict_out['BAIRONG_APPLY_LOAN_tot_mons_m03vm06_cell']= dict_out['BAIRONG_APPLY_LOAN_tot_mons_m03_cell']/dict_out['BAIRONG_APPLY_LOAN_tot_mons_m06_cell'] if dict_out['BAIRONG_APPLY_LOAN_tot_mons_m06_cell']>0 else np.nan
    dict_out['BAIRONG_APPLY_LOAN_pdl_allnum_d07vm12_cell']= dict_out['BAIRONG_APPLY_LOAN_pdl_allnum_d07_cell']/dict_out['BAIRONG_APPLY_LOAN_pdl_allnum_m12_cell'] if dict_out['BAIRONG_APPLY_LOAN_pdl_allnum_m12_cell']>0 else np.nan
    dict_out['BAIRONG_APPLY_LOAN_rel_orgnum_m01vm03_cell']= dict_out['BAIRONG_APPLY_LOAN_rel_orgnum_m01_cell']/dict_out['BAIRONG_APPLY_LOAN_rel_orgnum_m03_cell'] if dict_out['BAIRONG_APPLY_LOAN_rel_orgnum_m03_cell']>0 else np.nan
    dict_out['BAIRONG_APPLY_LOAN_af_orgnum_m03vm06_cell']= dict_out['BAIRONG_APPLY_LOAN_af_orgnum_m03_cell']/dict_out['BAIRONG_APPLY_LOAN_af_orgnum_m06_cell'] if dict_out['BAIRONG_APPLY_LOAN_af_orgnum_m06_cell']>0 else np.nan
    dict_out['BAIRONG_APPLY_LOAN_nbank_sloan_orgnum_d15vm12_cell']= dict_out['BAIRONG_APPLY_LOAN_nbank_sloan_orgnum_d15_cell']/dict_out['BAIRONG_APPLY_LOAN_nbank_sloan_orgnum_m12_cell'] if dict_out['BAIRONG_APPLY_LOAN_nbank_sloan_orgnum_m12_cell']>0 else np.nan
    dict_out['BAIRONG_APPLY_LOAN_caoff_orgnum_m01vm12_cell']= dict_out['BAIRONG_APPLY_LOAN_caoff_orgnum_m01_cell']/dict_out['BAIRONG_APPLY_LOAN_caoff_orgnum_m12_cell'] if dict_out['BAIRONG_APPLY_LOAN_caoff_orgnum_m12_cell']>0 else np.nan
    dict_out['BAIRONG_APPLY_LOAN_nbank_orgnum_d07vm01_id']= dict_out['BAIRONG_APPLY_LOAN_nbank_orgnum_d07_id']/dict_out['BAIRONG_APPLY_LOAN_nbank_orgnum_m01_id'] if dict_out['BAIRONG_APPLY_LOAN_nbank_orgnum_m01_id']>0 else np.nan
    dict_out['BAIRONG_APPLY_LOAN_nbank_cons_allnum_m01vm12_cell']= dict_out['BAIRONG_APPLY_LOAN_nbank_cons_allnum_m01_cell']/dict_out['BAIRONG_APPLY_LOAN_nbank_cons_allnum_m12_cell'] if dict_out['BAIRONG_APPLY_LOAN_nbank_cons_allnum_m12_cell']>0 else np.nan
    dict_out['BAIRONG_APPLY_LOAN_rel_allnum_m03vm06_id']= dict_out['BAIRONG_APPLY_LOAN_rel_allnum_m03_id']/dict_out['BAIRONG_APPLY_LOAN_rel_allnum_m06_id'] if dict_out['BAIRONG_APPLY_LOAN_rel_allnum_m06_id']>0 else np.nan
    dict_out['BAIRONG_APPLY_LOAN_pdl_orgnum_m03_idsubcell']= dict_out['BAIRONG_APPLY_LOAN_pdl_orgnum_m03_id']-dict_out['BAIRONG_APPLY_LOAN_pdl_orgnum_m03_cell']
    dict_out['BAIRONG_APPLY_LOAN_min_monnum_m03_idsubcell']= dict_out['BAIRONG_APPLY_LOAN_min_monnum_m03_id']-dict_out['BAIRONG_APPLY_LOAN_min_monnum_m03_cell']
    dict_out['BAIRONG_APPLY_LOAN_nbank_cons_allnum_d07vm01_cell']= dict_out['BAIRONG_APPLY_LOAN_nbank_cons_allnum_d07_cell']/dict_out['BAIRONG_APPLY_LOAN_nbank_cons_allnum_m01_cell'] if dict_out['BAIRONG_APPLY_LOAN_nbank_cons_allnum_m01_cell']>0 else np.nan
    dict_out['BAIRONG_APPLY_LOAN_caon_orgnum_d15vm06_cell']= dict_out['BAIRONG_APPLY_LOAN_caon_orgnum_d15_cell']/dict_out['BAIRONG_APPLY_LOAN_caon_orgnum_m06_cell'] if dict_out['BAIRONG_APPLY_LOAN_caon_orgnum_m06_cell']>0 else np.nan
    dict_out['BAIRONG_APPLY_LOAN_nbank_finlea_allnum_d15vm06_id']= dict_out['BAIRONG_APPLY_LOAN_nbank_finlea_allnum_d15_id']/dict_out['BAIRONG_APPLY_LOAN_nbank_finlea_allnum_m06_id'] if dict_out['BAIRONG_APPLY_LOAN_nbank_finlea_allnum_m06_id']>0 else np.nan
    dict_out['BAIRONG_APPLY_LOAN_bank_tra_orgnum_m01vm12_id']= dict_out['BAIRONG_APPLY_LOAN_bank_tra_orgnum_m01_id']/dict_out['BAIRONG_APPLY_LOAN_bank_tra_orgnum_m12_id'] if dict_out['BAIRONG_APPLY_LOAN_bank_tra_orgnum_m12_id']>0 else np.nan
    dict_out['BAIRONG_APPLY_LOAN_cooff_allnum_m03vm12_id']= dict_out['BAIRONG_APPLY_LOAN_cooff_allnum_m03_id']/dict_out['BAIRONG_APPLY_LOAN_cooff_allnum_m12_id'] if dict_out['BAIRONG_APPLY_LOAN_cooff_allnum_m12_id']>0 else np.nan
    dict_out['BAIRONG_APPLY_LOAN_bank_ret_allnum_m03vm12_cell']= dict_out['BAIRONG_APPLY_LOAN_bank_ret_allnum_m03_cell']/dict_out['BAIRONG_APPLY_LOAN_bank_ret_allnum_m12_cell'] if dict_out['BAIRONG_APPLY_LOAN_bank_ret_allnum_m12_cell']>0 else np.nan
    dict_out['BAIRONG_APPLY_LOAN_cooff_orgnum_m06vm12_id']= dict_out['BAIRONG_APPLY_LOAN_cooff_orgnum_m06_id']/dict_out['BAIRONG_APPLY_LOAN_cooff_orgnum_m12_id'] if dict_out['BAIRONG_APPLY_LOAN_cooff_orgnum_m12_id']>0 else np.nan
    dict_out['BAIRONG_APPLY_LOAN_nbank_min_monnum_m12_idsubcell']= dict_out['BAIRONG_APPLY_LOAN_nbank_min_monnum_m12_id']-dict_out['BAIRONG_APPLY_LOAN_nbank_min_monnum_m12_cell']
    dict_out['BAIRONG_APPLY_LOAN_max_inteday_m06vm12_cell']= dict_out['BAIRONG_APPLY_LOAN_max_inteday_m06_cell']/dict_out['BAIRONG_APPLY_LOAN_max_inteday_m12_cell'] if dict_out['BAIRONG_APPLY_LOAN_max_inteday_m12_cell']>0 else np.nan
    dict_out['BAIRONG_APPLY_LOAN_pdl_allnum_m01vm06_id']= dict_out['BAIRONG_APPLY_LOAN_pdl_allnum_m01_id']/dict_out['BAIRONG_APPLY_LOAN_pdl_allnum_m06_id'] if dict_out['BAIRONG_APPLY_LOAN_pdl_allnum_m06_id']>0 else np.nan
    dict_out['BAIRONG_APPLY_LOAN_caoff_allnum_m03_idsubcell']= dict_out['BAIRONG_APPLY_LOAN_caoff_allnum_m03_id']-dict_out['BAIRONG_APPLY_LOAN_caoff_allnum_m03_cell']
    dict_out['BAIRONG_APPLY_LOAN_nbank_sloan_orgnum_d07vm12_cell']= dict_out['BAIRONG_APPLY_LOAN_nbank_sloan_orgnum_d07_cell']/dict_out['BAIRONG_APPLY_LOAN_nbank_sloan_orgnum_m12_cell'] if dict_out['BAIRONG_APPLY_LOAN_nbank_sloan_orgnum_m12_cell']>0 else np.nan
    dict_out['BAIRONG_APPLY_LOAN_oth_allnum_m01vm12_cell']= dict_out['BAIRONG_APPLY_LOAN_oth_allnum_m01_cell']/dict_out['BAIRONG_APPLY_LOAN_oth_allnum_m12_cell'] if dict_out['BAIRONG_APPLY_LOAN_oth_allnum_m12_cell']>0 else np.nan
    dict_out['BAIRONG_APPLY_LOAN_nbank_night_orgnum_m01vm12_id']= dict_out['BAIRONG_APPLY_LOAN_nbank_night_orgnum_m01_id']/dict_out['BAIRONG_APPLY_LOAN_nbank_night_orgnum_m12_id'] if dict_out['BAIRONG_APPLY_LOAN_nbank_night_orgnum_m12_id']>0 else np.nan
    dict_out['BAIRONG_APPLY_LOAN_pdl_orgnum_d15vm12_cell']= dict_out['BAIRONG_APPLY_LOAN_pdl_orgnum_d15_cell']/dict_out['BAIRONG_APPLY_LOAN_pdl_orgnum_m12_cell'] if dict_out['BAIRONG_APPLY_LOAN_pdl_orgnum_m12_cell']>0 else np.nan
    dict_out['BAIRONG_APPLY_LOAN_cooff_orgnum_d07vm06_cell']= dict_out['BAIRONG_APPLY_LOAN_cooff_orgnum_d07_cell']/dict_out['BAIRONG_APPLY_LOAN_cooff_orgnum_m06_cell'] if dict_out['BAIRONG_APPLY_LOAN_cooff_orgnum_m06_cell']>0 else np.nan
    dict_out['BAIRONG_APPLY_LOAN_nbank_week_orgnum_m01vm12_cell']= dict_out['BAIRONG_APPLY_LOAN_nbank_week_orgnum_m01_cell']/dict_out['BAIRONG_APPLY_LOAN_nbank_week_orgnum_m12_cell'] if dict_out['BAIRONG_APPLY_LOAN_nbank_week_orgnum_m12_cell']>0 else np.nan
    dict_out['BAIRONG_APPLY_LOAN_bank_avg_monnum_m12_idsubcell']= dict_out['BAIRONG_APPLY_LOAN_bank_avg_monnum_m12_id']-dict_out['BAIRONG_APPLY_LOAN_bank_avg_monnum_m12_cell']
    dict_out['BAIRONG_APPLY_LOAN_nbank_week_orgnum_d15vm01_cell']= dict_out['BAIRONG_APPLY_LOAN_nbank_week_orgnum_d15_cell']/dict_out['BAIRONG_APPLY_LOAN_nbank_week_orgnum_m01_cell'] if dict_out['BAIRONG_APPLY_LOAN_nbank_week_orgnum_m01_cell']>0 else np.nan
    dict_out['BAIRONG_APPLY_LOAN_nbank_cons_allnum_d15vm01_cell']= dict_out['BAIRONG_APPLY_LOAN_nbank_cons_allnum_d15_cell']/dict_out['BAIRONG_APPLY_LOAN_nbank_cons_allnum_m01_cell'] if dict_out['BAIRONG_APPLY_LOAN_nbank_cons_allnum_m01_cell']>0 else np.nan
    dict_out['BAIRONG_APPLY_LOAN_nbank_cf_orgnum_d15vm12_cell']= dict_out['BAIRONG_APPLY_LOAN_nbank_cf_orgnum_d15_cell']/dict_out['BAIRONG_APPLY_LOAN_nbank_cf_orgnum_m12_cell'] if dict_out['BAIRONG_APPLY_LOAN_nbank_cf_orgnum_m12_cell']>0 else np.nan
    dict_out['BAIRONG_APPLY_LOAN_nbank_week_allnum_d07vm06_cell']= dict_out['BAIRONG_APPLY_LOAN_nbank_week_allnum_d07_cell']/dict_out['BAIRONG_APPLY_LOAN_nbank_week_allnum_m06_cell'] if dict_out['BAIRONG_APPLY_LOAN_nbank_week_allnum_m06_cell']>0 else np.nan
    dict_out['BAIRONG_APPLY_LOAN_af_allnum_m01vm06_cell']= dict_out['BAIRONG_APPLY_LOAN_af_allnum_m01_cell']/dict_out['BAIRONG_APPLY_LOAN_af_allnum_m06_cell'] if dict_out['BAIRONG_APPLY_LOAN_af_allnum_m06_cell']>0 else np.nan
    dict_out['BAIRONG_APPLY_LOAN_nbank_max_monnum_m06_idsubcell']= dict_out['BAIRONG_APPLY_LOAN_nbank_max_monnum_m06_id']-dict_out['BAIRONG_APPLY_LOAN_nbank_max_monnum_m06_cell']
    dict_out['BAIRONG_APPLY_LOAN_nbank_cf_allnum_m03vm12_cell']= dict_out['BAIRONG_APPLY_LOAN_nbank_cf_allnum_m03_cell']/dict_out['BAIRONG_APPLY_LOAN_nbank_cf_allnum_m12_cell'] if dict_out['BAIRONG_APPLY_LOAN_nbank_cf_allnum_m12_cell']>0 else np.nan
    dict_out['BAIRONG_APPLY_LOAN_pdl_allnum_m01vm03_id']= dict_out['BAIRONG_APPLY_LOAN_pdl_allnum_m01_id']/dict_out['BAIRONG_APPLY_LOAN_pdl_allnum_m03_id'] if dict_out['BAIRONG_APPLY_LOAN_pdl_allnum_m03_id']>0 else np.nan
    dict_out['BAIRONG_APPLY_LOAN_rel_allnum_d07vm06_cell']= dict_out['BAIRONG_APPLY_LOAN_rel_allnum_d07_cell']/dict_out['BAIRONG_APPLY_LOAN_rel_allnum_m06_cell'] if dict_out['BAIRONG_APPLY_LOAN_rel_allnum_m06_cell']>0 else np.nan
    dict_out['BAIRONG_APPLY_LOAN_af_orgnum_m01vm12_cell']= dict_out['BAIRONG_APPLY_LOAN_af_orgnum_m01_cell']/dict_out['BAIRONG_APPLY_LOAN_af_orgnum_m12_cell'] if dict_out['BAIRONG_APPLY_LOAN_af_orgnum_m12_cell']>0 else np.nan
    dict_out['BAIRONG_APPLY_LOAN_cooff_orgnum_d15vm01_cell']= dict_out['BAIRONG_APPLY_LOAN_cooff_orgnum_d15_cell']/dict_out['BAIRONG_APPLY_LOAN_cooff_orgnum_m01_cell'] if dict_out['BAIRONG_APPLY_LOAN_cooff_orgnum_m01_cell']>0 else np.nan
    dict_out['BAIRONG_APPLY_LOAN_bank_max_inteday_m03vm12_id']= dict_out['BAIRONG_APPLY_LOAN_bank_max_inteday_m03_id']/dict_out['BAIRONG_APPLY_LOAN_bank_max_inteday_m12_id'] if dict_out['BAIRONG_APPLY_LOAN_bank_max_inteday_m12_id']>0 else np.nan
    dict_out['BAIRONG_APPLY_LOAN_pdl_allnum_m01vm12_id']= dict_out['BAIRONG_APPLY_LOAN_pdl_allnum_m01_id']/dict_out['BAIRONG_APPLY_LOAN_pdl_allnum_m12_id'] if dict_out['BAIRONG_APPLY_LOAN_pdl_allnum_m12_id']>0 else np.nan
    dict_out['BAIRONG_APPLY_LOAN_pdl_allnum_d07vm12_id']= dict_out['BAIRONG_APPLY_LOAN_pdl_allnum_d07_id']/dict_out['BAIRONG_APPLY_LOAN_pdl_allnum_m12_id'] if dict_out['BAIRONG_APPLY_LOAN_pdl_allnum_m12_id']>0 else np.nan
    dict_out['BAIRONG_APPLY_LOAN_bank_orgnum_m03vm12_id']= dict_out['BAIRONG_APPLY_LOAN_bank_orgnum_m03_id']/dict_out['BAIRONG_APPLY_LOAN_bank_orgnum_m12_id'] if dict_out['BAIRONG_APPLY_LOAN_bank_orgnum_m12_id']>0 else np.nan
    dict_out['BAIRONG_APPLY_LOAN_nbank_orgnum_d15vm12_cell']= dict_out['BAIRONG_APPLY_LOAN_nbank_orgnum_d15_cell']/dict_out['BAIRONG_APPLY_LOAN_nbank_orgnum_m12_cell'] if dict_out['BAIRONG_APPLY_LOAN_nbank_orgnum_m12_cell']>0 else np.nan
    dict_out['BAIRONG_APPLY_LOAN_max_inteday_m03vm06_id']= dict_out['BAIRONG_APPLY_LOAN_max_inteday_m03_id']/dict_out['BAIRONG_APPLY_LOAN_max_inteday_m06_id'] if dict_out['BAIRONG_APPLY_LOAN_max_inteday_m06_id']>0 else np.nan
    dict_out['BAIRONG_APPLY_LOAN_bank_week_orgnum_m03vm12_cell']= dict_out['BAIRONG_APPLY_LOAN_bank_week_orgnum_m03_cell']/dict_out['BAIRONG_APPLY_LOAN_bank_week_orgnum_m12_cell'] if dict_out['BAIRONG_APPLY_LOAN_bank_week_orgnum_m12_cell']>0 else np.nan
    dict_out['BAIRONG_APPLY_LOAN_nbank_allnum_d07vd15_cell']= dict_out['BAIRONG_APPLY_LOAN_nbank_allnum_d07_cell']/dict_out['BAIRONG_APPLY_LOAN_nbank_allnum_d15_cell'] if dict_out['BAIRONG_APPLY_LOAN_nbank_allnum_d15_cell']>0 else np.nan
    dict_out['BAIRONG_APPLY_LOAN_nbank_sloan_allnum_d15vm06_cell']= dict_out['BAIRONG_APPLY_LOAN_nbank_sloan_allnum_d15_cell']/dict_out['BAIRONG_APPLY_LOAN_nbank_sloan_allnum_m06_cell'] if dict_out['BAIRONG_APPLY_LOAN_nbank_sloan_allnum_m06_cell']>0 else np.nan
    dict_out['BAIRONG_APPLY_LOAN_nbank_oth_allnum_d07vm12_id']= dict_out['BAIRONG_APPLY_LOAN_nbank_oth_allnum_d07_id']/dict_out['BAIRONG_APPLY_LOAN_nbank_oth_allnum_m12_id'] if dict_out['BAIRONG_APPLY_LOAN_nbank_oth_allnum_m12_id']>0 else np.nan
    dict_out['BAIRONG_APPLY_LOAN_nbank_else_allnum_d07vm12_cell']= dict_out['BAIRONG_APPLY_LOAN_nbank_else_allnum_d07_cell']/dict_out['BAIRONG_APPLY_LOAN_nbank_else_allnum_m12_cell'] if dict_out['BAIRONG_APPLY_LOAN_nbank_else_allnum_m12_cell']>0 else np.nan
    dict_out['BAIRONG_APPLY_LOAN_caon_orgnum_d15vm12_cell']= dict_out['BAIRONG_APPLY_LOAN_caon_orgnum_d15_cell']/dict_out['BAIRONG_APPLY_LOAN_caon_orgnum_m12_cell'] if dict_out['BAIRONG_APPLY_LOAN_caon_orgnum_m12_cell']>0 else np.nan
    dict_out['BAIRONG_APPLY_LOAN_nbank_else_orgnum_m01vm06_id']= dict_out['BAIRONG_APPLY_LOAN_nbank_else_orgnum_m01_id']/dict_out['BAIRONG_APPLY_LOAN_nbank_else_orgnum_m06_id'] if dict_out['BAIRONG_APPLY_LOAN_nbank_else_orgnum_m06_id']>0 else np.nan
    dict_out['BAIRONG_APPLY_LOAN_nbank_else_allnum_d15vm06_cell']= dict_out['BAIRONG_APPLY_LOAN_nbank_else_allnum_d15_cell']/dict_out['BAIRONG_APPLY_LOAN_nbank_else_allnum_m06_cell'] if dict_out['BAIRONG_APPLY_LOAN_nbank_else_allnum_m06_cell']>0 else np.nan
    dict_out['BAIRONG_APPLY_LOAN_max_monnum_m03vm12_id']= dict_out['BAIRONG_APPLY_LOAN_max_monnum_m03_id']/dict_out['BAIRONG_APPLY_LOAN_max_monnum_m12_id'] if dict_out['BAIRONG_APPLY_LOAN_max_monnum_m12_id']>0 else np.nan
    dict_out['BAIRONG_APPLY_LOAN_nbank_max_inteday_m06vm12_cell']= dict_out['BAIRONG_APPLY_LOAN_nbank_max_inteday_m06_cell']/dict_out['BAIRONG_APPLY_LOAN_nbank_max_inteday_m12_cell'] if dict_out['BAIRONG_APPLY_LOAN_nbank_max_inteday_m12_cell']>0 else np.nan
    dict_out['BAIRONG_APPLY_LOAN_af_allnum_m01vm03_cell']= dict_out['BAIRONG_APPLY_LOAN_af_allnum_m01_cell']/dict_out['BAIRONG_APPLY_LOAN_af_allnum_m03_cell'] if dict_out['BAIRONG_APPLY_LOAN_af_allnum_m03_cell']>0 else np.nan
    dict_out['BAIRONG_APPLY_LOAN_bank_orgnum_m01vm12_cell']= dict_out['BAIRONG_APPLY_LOAN_bank_orgnum_m01_cell']/dict_out['BAIRONG_APPLY_LOAN_bank_orgnum_m12_cell'] if dict_out['BAIRONG_APPLY_LOAN_bank_orgnum_m12_cell']>0 else np.nan
    dict_out['BAIRONG_APPLY_LOAN_caon_allnum_d07vm06_id']= dict_out['BAIRONG_APPLY_LOAN_caon_allnum_d07_id']/dict_out['BAIRONG_APPLY_LOAN_caon_allnum_m06_id'] if dict_out['BAIRONG_APPLY_LOAN_caon_allnum_m06_id']>0 else np.nan
    dict_out['BAIRONG_APPLY_LOAN_nbank_orgnum_m01vm03_cell']= dict_out['BAIRONG_APPLY_LOAN_nbank_orgnum_m01_cell']/dict_out['BAIRONG_APPLY_LOAN_nbank_orgnum_m03_cell'] if dict_out['BAIRONG_APPLY_LOAN_nbank_orgnum_m03_cell']>0 else np.nan
    dict_out['BAIRONG_APPLY_LOAN_nbank_nsloan_orgnum_d07vm12_cell']= dict_out['BAIRONG_APPLY_LOAN_nbank_nsloan_orgnum_d07_cell']/dict_out['BAIRONG_APPLY_LOAN_nbank_nsloan_orgnum_m12_cell'] if dict_out['BAIRONG_APPLY_LOAN_nbank_nsloan_orgnum_m12_cell']>0 else np.nan
    dict_out['BAIRONG_APPLY_LOAN_nbank_week_allnum_m01vm03_id']= dict_out['BAIRONG_APPLY_LOAN_nbank_week_allnum_m01_id']/dict_out['BAIRONG_APPLY_LOAN_nbank_week_allnum_m03_id'] if dict_out['BAIRONG_APPLY_LOAN_nbank_week_allnum_m03_id']>0 else np.nan
    dict_out['BAIRONG_APPLY_LOAN_nbank_nsloan_orgnum_d07vd15_cell']= dict_out['BAIRONG_APPLY_LOAN_nbank_nsloan_orgnum_d07_cell']/dict_out['BAIRONG_APPLY_LOAN_nbank_nsloan_orgnum_d15_cell'] if dict_out['BAIRONG_APPLY_LOAN_nbank_nsloan_orgnum_d15_cell']>0 else np.nan
    dict_out['BAIRONG_APPLY_LOAN_nbank_nsloan_allnum_m03_idsubcell']= dict_out['BAIRONG_APPLY_LOAN_nbank_nsloan_allnum_m03_id']-dict_out['BAIRONG_APPLY_LOAN_nbank_nsloan_allnum_m03_cell']
    dict_out['BAIRONG_APPLY_LOAN_nbank_week_allnum_m06vm12_cell']= dict_out['BAIRONG_APPLY_LOAN_nbank_week_allnum_m06_cell']/dict_out['BAIRONG_APPLY_LOAN_nbank_week_allnum_m12_cell'] if dict_out['BAIRONG_APPLY_LOAN_nbank_week_allnum_m12_cell']>0 else np.nan
    dict_out['BAIRONG_APPLY_LOAN_caon_allnum_m01vm06_id']= dict_out['BAIRONG_APPLY_LOAN_caon_allnum_m01_id']/dict_out['BAIRONG_APPLY_LOAN_caon_allnum_m06_id'] if dict_out['BAIRONG_APPLY_LOAN_caon_allnum_m06_id']>0 else np.nan
    dict_out['BAIRONG_APPLY_LOAN_nbank_avg_monnum_m06vm12_cell']= dict_out['BAIRONG_APPLY_LOAN_nbank_avg_monnum_m06_cell']/dict_out['BAIRONG_APPLY_LOAN_nbank_avg_monnum_m12_cell'] if dict_out['BAIRONG_APPLY_LOAN_nbank_avg_monnum_m12_cell']>0 else np.nan
    dict_out['BAIRONG_APPLY_LOAN_nbank_else_orgnum_d07vm06_id']= dict_out['BAIRONG_APPLY_LOAN_nbank_else_orgnum_d07_id']/dict_out['BAIRONG_APPLY_LOAN_nbank_else_orgnum_m06_id'] if dict_out['BAIRONG_APPLY_LOAN_nbank_else_orgnum_m06_id']>0 else np.nan
    dict_out['BAIRONG_APPLY_LOAN_nbank_night_allnum_d07vm12_id']= dict_out['BAIRONG_APPLY_LOAN_nbank_night_allnum_d07_id']/dict_out['BAIRONG_APPLY_LOAN_nbank_night_allnum_m12_id'] if dict_out['BAIRONG_APPLY_LOAN_nbank_night_allnum_m12_id']>0 else np.nan
    dict_out['BAIRONG_APPLY_LOAN_nbank_week_orgnum_m03vm06_cell']= dict_out['BAIRONG_APPLY_LOAN_nbank_week_orgnum_m03_cell']/dict_out['BAIRONG_APPLY_LOAN_nbank_week_orgnum_m06_cell'] if dict_out['BAIRONG_APPLY_LOAN_nbank_week_orgnum_m06_cell']>0 else np.nan
    dict_out['BAIRONG_APPLY_LOAN_caon_allnum_d15vm06_id']= dict_out['BAIRONG_APPLY_LOAN_caon_allnum_d15_id']/dict_out['BAIRONG_APPLY_LOAN_caon_allnum_m06_id'] if dict_out['BAIRONG_APPLY_LOAN_caon_allnum_m06_id']>0 else np.nan
    dict_out['BAIRONG_APPLY_LOAN_nbank_cons_orgnum_m01vm06_cell']= dict_out['BAIRONG_APPLY_LOAN_nbank_cons_orgnum_m01_cell']/dict_out['BAIRONG_APPLY_LOAN_nbank_cons_orgnum_m06_cell'] if dict_out['BAIRONG_APPLY_LOAN_nbank_cons_orgnum_m06_cell']>0 else np.nan
    dict_out['BAIRONG_APPLY_LOAN_nbank_avg_monnum_m06vm12_id']= dict_out['BAIRONG_APPLY_LOAN_nbank_avg_monnum_m06_id']/dict_out['BAIRONG_APPLY_LOAN_nbank_avg_monnum_m12_id'] if dict_out['BAIRONG_APPLY_LOAN_nbank_avg_monnum_m12_id']>0 else np.nan
    dict_out['BAIRONG_APPLY_LOAN_caoff_orgnum_d15vm01_id']= dict_out['BAIRONG_APPLY_LOAN_caoff_orgnum_d15_id']/dict_out['BAIRONG_APPLY_LOAN_caoff_orgnum_m01_id'] if dict_out['BAIRONG_APPLY_LOAN_caoff_orgnum_m01_id']>0 else np.nan
    dict_out['BAIRONG_APPLY_LOAN_nbank_cf_allnum_d07vm01_id']= dict_out['BAIRONG_APPLY_LOAN_nbank_cf_allnum_d07_id']/dict_out['BAIRONG_APPLY_LOAN_nbank_cf_allnum_m01_id'] if dict_out['BAIRONG_APPLY_LOAN_nbank_cf_allnum_m01_id']>0 else np.nan
    dict_out['BAIRONG_APPLY_LOAN_caoff_orgnum_m03vm06_cell']= dict_out['BAIRONG_APPLY_LOAN_caoff_orgnum_m03_cell']/dict_out['BAIRONG_APPLY_LOAN_caoff_orgnum_m06_cell'] if dict_out['BAIRONG_APPLY_LOAN_caoff_orgnum_m06_cell']>0 else np.nan
    dict_out['BAIRONG_APPLY_LOAN_rel_allnum_m03vm06_cell']= dict_out['BAIRONG_APPLY_LOAN_rel_allnum_m03_cell']/dict_out['BAIRONG_APPLY_LOAN_rel_allnum_m06_cell'] if dict_out['BAIRONG_APPLY_LOAN_rel_allnum_m06_cell']>0 else np.nan
    dict_out['BAIRONG_APPLY_LOAN_nbank_nsloan_orgnum_m01vm03_id']= dict_out['BAIRONG_APPLY_LOAN_nbank_nsloan_orgnum_m01_id']/dict_out['BAIRONG_APPLY_LOAN_nbank_nsloan_orgnum_m03_id'] if dict_out['BAIRONG_APPLY_LOAN_nbank_nsloan_orgnum_m03_id']>0 else np.nan
    dict_out['BAIRONG_APPLY_LOAN_nbank_oth_orgnum_m01vm03_id']= dict_out['BAIRONG_APPLY_LOAN_nbank_oth_orgnum_m01_id']/dict_out['BAIRONG_APPLY_LOAN_nbank_oth_orgnum_m03_id'] if dict_out['BAIRONG_APPLY_LOAN_nbank_oth_orgnum_m03_id']>0 else np.nan
    dict_out['BAIRONG_APPLY_LOAN_nbank_nsloan_orgnum_d07vm12_id']= dict_out['BAIRONG_APPLY_LOAN_nbank_nsloan_orgnum_d07_id']/dict_out['BAIRONG_APPLY_LOAN_nbank_nsloan_orgnum_m12_id'] if dict_out['BAIRONG_APPLY_LOAN_nbank_nsloan_orgnum_m12_id']>0 else np.nan
    dict_out['BAIRONG_APPLY_LOAN_cooff_allnum_d07vm06_cell']= dict_out['BAIRONG_APPLY_LOAN_cooff_allnum_d07_cell']/dict_out['BAIRONG_APPLY_LOAN_cooff_allnum_m06_cell'] if dict_out['BAIRONG_APPLY_LOAN_cooff_allnum_m06_cell']>0 else np.nan
    dict_out['BAIRONG_APPLY_LOAN_bank_tra_allnum_m06vm12_cell']= dict_out['BAIRONG_APPLY_LOAN_bank_tra_allnum_m06_cell']/dict_out['BAIRONG_APPLY_LOAN_bank_tra_allnum_m12_cell'] if dict_out['BAIRONG_APPLY_LOAN_bank_tra_allnum_m12_cell']>0 else np.nan
    dict_out['BAIRONG_APPLY_LOAN_pdl_orgnum_d07vm12_cell']= dict_out['BAIRONG_APPLY_LOAN_pdl_orgnum_d07_cell']/dict_out['BAIRONG_APPLY_LOAN_pdl_orgnum_m12_cell'] if dict_out['BAIRONG_APPLY_LOAN_pdl_orgnum_m12_cell']>0 else np.nan
    dict_out['BAIRONG_APPLY_LOAN_oth_allnum_d15vm12_id']= dict_out['BAIRONG_APPLY_LOAN_oth_allnum_d15_id']/dict_out['BAIRONG_APPLY_LOAN_oth_allnum_m12_id'] if dict_out['BAIRONG_APPLY_LOAN_oth_allnum_m12_id']>0 else np.nan
    dict_out['BAIRONG_APPLY_LOAN_nbank_else_orgnum_m01vm12_id']= dict_out['BAIRONG_APPLY_LOAN_nbank_else_orgnum_m01_id']/dict_out['BAIRONG_APPLY_LOAN_nbank_else_orgnum_m12_id'] if dict_out['BAIRONG_APPLY_LOAN_nbank_else_orgnum_m12_id']>0 else np.nan
    dict_out['BAIRONG_APPLY_LOAN_nbank_week_allnum_m01vm06_cell']= dict_out['BAIRONG_APPLY_LOAN_nbank_week_allnum_m01_cell']/dict_out['BAIRONG_APPLY_LOAN_nbank_week_allnum_m06_cell'] if dict_out['BAIRONG_APPLY_LOAN_nbank_week_allnum_m06_cell']>0 else np.nan
    dict_out['BAIRONG_APPLY_LOAN_nbank_cf_allnum_m01vm12_cell']= dict_out['BAIRONG_APPLY_LOAN_nbank_cf_allnum_m01_cell']/dict_out['BAIRONG_APPLY_LOAN_nbank_cf_allnum_m12_cell'] if dict_out['BAIRONG_APPLY_LOAN_nbank_cf_allnum_m12_cell']>0 else np.nan
    dict_out['BAIRONG_APPLY_LOAN_nbank_else_allnum_m06vm12_id']= dict_out['BAIRONG_APPLY_LOAN_nbank_else_allnum_m06_id']/dict_out['BAIRONG_APPLY_LOAN_nbank_else_allnum_m12_id'] if dict_out['BAIRONG_APPLY_LOAN_nbank_else_allnum_m12_id']>0 else np.nan
    dict_out['BAIRONG_APPLY_LOAN_nbank_sloan_allnum_d07vd15_cell']= dict_out['BAIRONG_APPLY_LOAN_nbank_sloan_allnum_d07_cell']/dict_out['BAIRONG_APPLY_LOAN_nbank_sloan_allnum_d15_cell'] if dict_out['BAIRONG_APPLY_LOAN_nbank_sloan_allnum_d15_cell']>0 else np.nan
    dict_out['BAIRONG_APPLY_LOAN_nbank_allnum_m03vm06_cell']= dict_out['BAIRONG_APPLY_LOAN_nbank_allnum_m03_cell']/dict_out['BAIRONG_APPLY_LOAN_nbank_allnum_m06_cell'] if dict_out['BAIRONG_APPLY_LOAN_nbank_allnum_m06_cell']>0 else np.nan
    dict_out['BAIRONG_APPLY_LOAN_caon_allnum_d15vm12_cell']= dict_out['BAIRONG_APPLY_LOAN_caon_allnum_d15_cell']/dict_out['BAIRONG_APPLY_LOAN_caon_allnum_m12_cell'] if dict_out['BAIRONG_APPLY_LOAN_caon_allnum_m12_cell']>0 else np.nan
    dict_out['BAIRONG_APPLY_LOAN_nbank_ca_orgnum_m01vm03_id']= dict_out['BAIRONG_APPLY_LOAN_nbank_ca_orgnum_m01_id']/dict_out['BAIRONG_APPLY_LOAN_nbank_ca_orgnum_m03_id'] if dict_out['BAIRONG_APPLY_LOAN_nbank_ca_orgnum_m03_id']>0 else np.nan
    dict_out['BAIRONG_APPLY_LOAN_pdl_allnum_m03vm12_cell']= dict_out['BAIRONG_APPLY_LOAN_pdl_allnum_m03_cell']/dict_out['BAIRONG_APPLY_LOAN_pdl_allnum_m12_cell'] if dict_out['BAIRONG_APPLY_LOAN_pdl_allnum_m12_cell']>0 else np.nan
    dict_out['BAIRONG_APPLY_LOAN_nbank_sloan_orgnum_m03vm06_cell']= dict_out['BAIRONG_APPLY_LOAN_nbank_sloan_orgnum_m03_cell']/dict_out['BAIRONG_APPLY_LOAN_nbank_sloan_orgnum_m06_cell'] if dict_out['BAIRONG_APPLY_LOAN_nbank_sloan_orgnum_m06_cell']>0 else np.nan
    dict_out['BAIRONG_APPLY_LOAN_caon_orgnum_m01vm06_id']= dict_out['BAIRONG_APPLY_LOAN_caon_orgnum_m01_id']/dict_out['BAIRONG_APPLY_LOAN_caon_orgnum_m06_id'] if dict_out['BAIRONG_APPLY_LOAN_caon_orgnum_m06_id']>0 else np.nan
    dict_out['BAIRONG_APPLY_LOAN_nbank_p2p_allnum_d07vm06_cell']= dict_out['BAIRONG_APPLY_LOAN_nbank_p2p_allnum_d07_cell']/dict_out['BAIRONG_APPLY_LOAN_nbank_p2p_allnum_m06_cell'] if dict_out['BAIRONG_APPLY_LOAN_nbank_p2p_allnum_m06_cell']>0 else np.nan
    dict_out['BAIRONG_APPLY_LOAN_nbank_week_orgnum_d07vm12_id']= dict_out['BAIRONG_APPLY_LOAN_nbank_week_orgnum_d07_id']/dict_out['BAIRONG_APPLY_LOAN_nbank_week_orgnum_m12_id'] if dict_out['BAIRONG_APPLY_LOAN_nbank_week_orgnum_m12_id']>0 else np.nan
    dict_out['BAIRONG_APPLY_LOAN_nbank_nsloan_allnum_d15vm06_cell']= dict_out['BAIRONG_APPLY_LOAN_nbank_nsloan_allnum_d15_cell']/dict_out['BAIRONG_APPLY_LOAN_nbank_nsloan_allnum_m06_cell'] if dict_out['BAIRONG_APPLY_LOAN_nbank_nsloan_allnum_m06_cell']>0 else np.nan
    dict_out['BAIRONG_APPLY_LOAN_cooff_orgnum_d07vm12_cell']= dict_out['BAIRONG_APPLY_LOAN_cooff_orgnum_d07_cell']/dict_out['BAIRONG_APPLY_LOAN_cooff_orgnum_m12_cell'] if dict_out['BAIRONG_APPLY_LOAN_cooff_orgnum_m12_cell']>0 else np.nan
    dict_out['BAIRONG_APPLY_LOAN_caon_allnum_d07vm06_cell']= dict_out['BAIRONG_APPLY_LOAN_caon_allnum_d07_cell']/dict_out['BAIRONG_APPLY_LOAN_caon_allnum_m06_cell'] if dict_out['BAIRONG_APPLY_LOAN_caon_allnum_m06_cell']>0 else np.nan
    dict_out['BAIRONG_APPLY_LOAN_nbank_mc_allnum_m03_idsubcell']= dict_out['BAIRONG_APPLY_LOAN_nbank_mc_allnum_m03_id']-dict_out['BAIRONG_APPLY_LOAN_nbank_mc_allnum_m03_cell']
    dict_out['BAIRONG_APPLY_LOAN_nbank_max_inteday_m03vm12_id']= dict_out['BAIRONG_APPLY_LOAN_nbank_max_inteday_m03_id']/dict_out['BAIRONG_APPLY_LOAN_nbank_max_inteday_m12_id'] if dict_out['BAIRONG_APPLY_LOAN_nbank_max_inteday_m12_id']>0 else np.nan
    dict_out['BAIRONG_APPLY_LOAN_nbank_orgnum_m01vm12_cell']= dict_out['BAIRONG_APPLY_LOAN_nbank_orgnum_m01_cell']/dict_out['BAIRONG_APPLY_LOAN_nbank_orgnum_m12_cell'] if dict_out['BAIRONG_APPLY_LOAN_nbank_orgnum_m12_cell']>0 else np.nan
    dict_out['BAIRONG_APPLY_LOAN_nbank_sloan_allnum_d15vm06_id']= dict_out['BAIRONG_APPLY_LOAN_nbank_sloan_allnum_d15_id']/dict_out['BAIRONG_APPLY_LOAN_nbank_sloan_allnum_m06_id'] if dict_out['BAIRONG_APPLY_LOAN_nbank_sloan_allnum_m06_id']>0 else np.nan
    dict_out['BAIRONG_APPLY_LOAN_nbank_oth_orgnum_m01vm12_id']= dict_out['BAIRONG_APPLY_LOAN_nbank_oth_orgnum_m01_id']/dict_out['BAIRONG_APPLY_LOAN_nbank_oth_orgnum_m12_id'] if dict_out['BAIRONG_APPLY_LOAN_nbank_oth_orgnum_m12_id']>0 else np.nan
    dict_out['BAIRONG_APPLY_LOAN_caoff_orgnum_m03_idsubcell']= dict_out['BAIRONG_APPLY_LOAN_caoff_orgnum_m03_id']-dict_out['BAIRONG_APPLY_LOAN_caoff_orgnum_m03_cell']
    dict_out['BAIRONG_APPLY_LOAN_caoff_allnum_m01vm03_cell']= dict_out['BAIRONG_APPLY_LOAN_caoff_allnum_m01_cell']/dict_out['BAIRONG_APPLY_LOAN_caoff_allnum_m03_cell'] if dict_out['BAIRONG_APPLY_LOAN_caoff_allnum_m03_cell']>0 else np.nan
    dict_out['BAIRONG_APPLY_LOAN_caoff_allnum_m01vm12_id']= dict_out['BAIRONG_APPLY_LOAN_caoff_allnum_m01_id']/dict_out['BAIRONG_APPLY_LOAN_caoff_allnum_m12_id'] if dict_out['BAIRONG_APPLY_LOAN_caoff_allnum_m12_id']>0 else np.nan
    dict_out['BAIRONG_APPLY_LOAN_cooff_allnum_m01_idsubcell']= dict_out['BAIRONG_APPLY_LOAN_cooff_allnum_m01_id']-dict_out['BAIRONG_APPLY_LOAN_cooff_allnum_m01_cell']
    dict_out['BAIRONG_APPLY_LOAN_af_orgnum_m01vm06_id']= dict_out['BAIRONG_APPLY_LOAN_af_orgnum_m01_id']/dict_out['BAIRONG_APPLY_LOAN_af_orgnum_m06_id'] if dict_out['BAIRONG_APPLY_LOAN_af_orgnum_m06_id']>0 else np.nan
    dict_out['BAIRONG_APPLY_LOAN_nbank_sloan_allnum_m01vm12_cell']= dict_out['BAIRONG_APPLY_LOAN_nbank_sloan_allnum_m01_cell']/dict_out['BAIRONG_APPLY_LOAN_nbank_sloan_allnum_m12_cell'] if dict_out['BAIRONG_APPLY_LOAN_nbank_sloan_allnum_m12_cell']>0 else np.nan
    dict_out['BAIRONG_APPLY_LOAN_rel_orgnum_m06vm12_id']= dict_out['BAIRONG_APPLY_LOAN_rel_orgnum_m06_id']/dict_out['BAIRONG_APPLY_LOAN_rel_orgnum_m12_id'] if dict_out['BAIRONG_APPLY_LOAN_rel_orgnum_m12_id']>0 else np.nan
    dict_out['BAIRONG_APPLY_LOAN_nbank_max_monnum_m06vm12_id']= dict_out['BAIRONG_APPLY_LOAN_nbank_max_monnum_m06_id']/dict_out['BAIRONG_APPLY_LOAN_nbank_max_monnum_m12_id'] if dict_out['BAIRONG_APPLY_LOAN_nbank_max_monnum_m12_id']>0 else np.nan
    dict_out['BAIRONG_APPLY_LOAN_nbank_nsloan_orgnum_m03vm12_id']= dict_out['BAIRONG_APPLY_LOAN_nbank_nsloan_orgnum_m03_id']/dict_out['BAIRONG_APPLY_LOAN_nbank_nsloan_orgnum_m12_id'] if dict_out['BAIRONG_APPLY_LOAN_nbank_nsloan_orgnum_m12_id']>0 else np.nan
    dict_out['BAIRONG_APPLY_LOAN_nbank_mc_orgnum_m06vm12_id']= dict_out['BAIRONG_APPLY_LOAN_nbank_mc_orgnum_m06_id']/dict_out['BAIRONG_APPLY_LOAN_nbank_mc_orgnum_m12_id'] if dict_out['BAIRONG_APPLY_LOAN_nbank_mc_orgnum_m12_id']>0 else np.nan
    dict_out['BAIRONG_APPLY_LOAN_nbank_night_orgnum_m01vm03_cell']= dict_out['BAIRONG_APPLY_LOAN_nbank_night_orgnum_m01_cell']/dict_out['BAIRONG_APPLY_LOAN_nbank_night_orgnum_m03_cell'] if dict_out['BAIRONG_APPLY_LOAN_nbank_night_orgnum_m03_cell']>0 else np.nan
    dict_out['BAIRONG_APPLY_LOAN_rel_orgnum_m01vm06_cell']= dict_out['BAIRONG_APPLY_LOAN_rel_orgnum_m01_cell']/dict_out['BAIRONG_APPLY_LOAN_rel_orgnum_m06_cell'] if dict_out['BAIRONG_APPLY_LOAN_rel_orgnum_m06_cell']>0 else np.nan
    dict_out['BAIRONG_APPLY_LOAN_nbank_cons_allnum_d07vm12_id']= dict_out['BAIRONG_APPLY_LOAN_nbank_cons_allnum_d07_id']/dict_out['BAIRONG_APPLY_LOAN_nbank_cons_allnum_m12_id'] if dict_out['BAIRONG_APPLY_LOAN_nbank_cons_allnum_m12_id']>0 else np.nan
    dict_out['BAIRONG_APPLY_LOAN_pdl_allnum_m01vm06_cell']= dict_out['BAIRONG_APPLY_LOAN_pdl_allnum_m01_cell']/dict_out['BAIRONG_APPLY_LOAN_pdl_allnum_m06_cell'] if dict_out['BAIRONG_APPLY_LOAN_pdl_allnum_m06_cell']>0 else np.nan
    dict_out['BAIRONG_APPLY_LOAN_oth_orgnum_d15vm12_cell']= dict_out['BAIRONG_APPLY_LOAN_oth_orgnum_d15_cell']/dict_out['BAIRONG_APPLY_LOAN_oth_orgnum_m12_cell'] if dict_out['BAIRONG_APPLY_LOAN_oth_orgnum_m12_cell']>0 else np.nan
    dict_out['BAIRONG_APPLY_LOAN_af_orgnum_m01vm03_id']= dict_out['BAIRONG_APPLY_LOAN_af_orgnum_m01_id']/dict_out['BAIRONG_APPLY_LOAN_af_orgnum_m03_id'] if dict_out['BAIRONG_APPLY_LOAN_af_orgnum_m03_id']>0 else np.nan
    dict_out['BAIRONG_APPLY_LOAN_caoff_orgnum_d07vm06_id']= dict_out['BAIRONG_APPLY_LOAN_caoff_orgnum_d07_id']/dict_out['BAIRONG_APPLY_LOAN_caoff_orgnum_m06_id'] if dict_out['BAIRONG_APPLY_LOAN_caoff_orgnum_m06_id']>0 else np.nan
    dict_out['BAIRONG_APPLY_LOAN_nbank_else_allnum_m06vm12_cell']= dict_out['BAIRONG_APPLY_LOAN_nbank_else_allnum_m06_cell']/dict_out['BAIRONG_APPLY_LOAN_nbank_else_allnum_m12_cell'] if dict_out['BAIRONG_APPLY_LOAN_nbank_else_allnum_m12_cell']>0 else np.nan
    dict_out['BAIRONG_APPLY_LOAN_nbank_nsloan_orgnum_d07vm01_id']= dict_out['BAIRONG_APPLY_LOAN_nbank_nsloan_orgnum_d07_id']/dict_out['BAIRONG_APPLY_LOAN_nbank_nsloan_orgnum_m01_id'] if dict_out['BAIRONG_APPLY_LOAN_nbank_nsloan_orgnum_m01_id']>0 else np.nan
    dict_out['BAIRONG_APPLY_LOAN_nbank_sloan_allnum_m03vm06_cell']= dict_out['BAIRONG_APPLY_LOAN_nbank_sloan_allnum_m03_cell']/dict_out['BAIRONG_APPLY_LOAN_nbank_sloan_allnum_m06_cell'] if dict_out['BAIRONG_APPLY_LOAN_nbank_sloan_allnum_m06_cell']>0 else np.nan
    dict_out['BAIRONG_APPLY_LOAN_nbank_else_allnum_m01vm03_cell']= dict_out['BAIRONG_APPLY_LOAN_nbank_else_allnum_m01_cell']/dict_out['BAIRONG_APPLY_LOAN_nbank_else_allnum_m03_cell'] if dict_out['BAIRONG_APPLY_LOAN_nbank_else_allnum_m03_cell']>0 else np.nan
    dict_out['BAIRONG_APPLY_LOAN_nbank_else_allnum_m03vm12_cell']= dict_out['BAIRONG_APPLY_LOAN_nbank_else_allnum_m03_cell']/dict_out['BAIRONG_APPLY_LOAN_nbank_else_allnum_m12_cell'] if dict_out['BAIRONG_APPLY_LOAN_nbank_else_allnum_m12_cell']>0 else np.nan
    dict_out['BAIRONG_APPLY_LOAN_nbank_oth_allnum_d15vm06_id']= dict_out['BAIRONG_APPLY_LOAN_nbank_oth_allnum_d15_id']/dict_out['BAIRONG_APPLY_LOAN_nbank_oth_allnum_m06_id'] if dict_out['BAIRONG_APPLY_LOAN_nbank_oth_allnum_m06_id']>0 else np.nan
    dict_out['BAIRONG_APPLY_LOAN_nbank_avg_monnum_m03vm06_id']= dict_out['BAIRONG_APPLY_LOAN_nbank_avg_monnum_m03_id']/dict_out['BAIRONG_APPLY_LOAN_nbank_avg_monnum_m06_id'] if dict_out['BAIRONG_APPLY_LOAN_nbank_avg_monnum_m06_id']>0 else np.nan
    dict_out['BAIRONG_APPLY_LOAN_bank_allnum_m01vm03_cell']= dict_out['BAIRONG_APPLY_LOAN_bank_allnum_m01_cell']/dict_out['BAIRONG_APPLY_LOAN_bank_allnum_m03_cell'] if dict_out['BAIRONG_APPLY_LOAN_bank_allnum_m03_cell']>0 else np.nan
    dict_out['BAIRONG_APPLY_LOAN_nbank_cf_orgnum_m01vm06_id']= dict_out['BAIRONG_APPLY_LOAN_nbank_cf_orgnum_m01_id']/dict_out['BAIRONG_APPLY_LOAN_nbank_cf_orgnum_m06_id'] if dict_out['BAIRONG_APPLY_LOAN_nbank_cf_orgnum_m06_id']>0 else np.nan
    dict_out['BAIRONG_APPLY_LOAN_nbank_cons_allnum_d15vm06_cell']= dict_out['BAIRONG_APPLY_LOAN_nbank_cons_allnum_d15_cell']/dict_out['BAIRONG_APPLY_LOAN_nbank_cons_allnum_m06_cell'] if dict_out['BAIRONG_APPLY_LOAN_nbank_cons_allnum_m06_cell']>0 else np.nan
    dict_out['BAIRONG_APPLY_LOAN_nbank_p2p_orgnum_m03vm06_id']= dict_out['BAIRONG_APPLY_LOAN_nbank_p2p_orgnum_m03_id']/dict_out['BAIRONG_APPLY_LOAN_nbank_p2p_orgnum_m06_id'] if dict_out['BAIRONG_APPLY_LOAN_nbank_p2p_orgnum_m06_id']>0 else np.nan
    dict_out['BAIRONG_APPLY_LOAN_nbank_cf_orgnum_d15vm06_id']= dict_out['BAIRONG_APPLY_LOAN_nbank_cf_orgnum_d15_id']/dict_out['BAIRONG_APPLY_LOAN_nbank_cf_orgnum_m06_id'] if dict_out['BAIRONG_APPLY_LOAN_nbank_cf_orgnum_m06_id']>0 else np.nan
    dict_out['BAIRONG_APPLY_LOAN_nbank_nsloan_allnum_d07vm12_id']= dict_out['BAIRONG_APPLY_LOAN_nbank_nsloan_allnum_d07_id']/dict_out['BAIRONG_APPLY_LOAN_nbank_nsloan_allnum_m12_id'] if dict_out['BAIRONG_APPLY_LOAN_nbank_nsloan_allnum_m12_id']>0 else np.nan
    dict_out['BAIRONG_APPLY_LOAN_nbank_sloan_orgnum_d15vm12_id']= dict_out['BAIRONG_APPLY_LOAN_nbank_sloan_orgnum_d15_id']/dict_out['BAIRONG_APPLY_LOAN_nbank_sloan_orgnum_m12_id'] if dict_out['BAIRONG_APPLY_LOAN_nbank_sloan_orgnum_m12_id']>0 else np.nan
    dict_out['BAIRONG_APPLY_LOAN_nbank_sloan_allnum_d15vm12_id']= dict_out['BAIRONG_APPLY_LOAN_nbank_sloan_allnum_d15_id']/dict_out['BAIRONG_APPLY_LOAN_nbank_sloan_allnum_m12_id'] if dict_out['BAIRONG_APPLY_LOAN_nbank_sloan_allnum_m12_id']>0 else np.nan
    dict_out['BAIRONG_APPLY_LOAN_rel_orgnum_m06vm12_cell']= dict_out['BAIRONG_APPLY_LOAN_rel_orgnum_m06_cell']/dict_out['BAIRONG_APPLY_LOAN_rel_orgnum_m12_cell'] if dict_out['BAIRONG_APPLY_LOAN_rel_orgnum_m12_cell']>0 else np.nan
    dict_out['BAIRONG_APPLY_LOAN_nbank_sloan_allnum_d07vm12_id']= dict_out['BAIRONG_APPLY_LOAN_nbank_sloan_allnum_d07_id']/dict_out['BAIRONG_APPLY_LOAN_nbank_sloan_allnum_m12_id'] if dict_out['BAIRONG_APPLY_LOAN_nbank_sloan_allnum_m12_id']>0 else np.nan
    dict_out['BAIRONG_APPLY_LOAN_nbank_ca_allnum_m01vm06_cell']= dict_out['BAIRONG_APPLY_LOAN_nbank_ca_allnum_m01_cell']/dict_out['BAIRONG_APPLY_LOAN_nbank_ca_allnum_m06_cell'] if dict_out['BAIRONG_APPLY_LOAN_nbank_ca_allnum_m06_cell']>0 else np.nan
    dict_out['BAIRONG_APPLY_LOAN_nbank_cf_orgnum_m06_idsubcell']= dict_out['BAIRONG_APPLY_LOAN_nbank_cf_orgnum_m06_id']-dict_out['BAIRONG_APPLY_LOAN_nbank_cf_orgnum_m06_cell']
    dict_out['BAIRONG_APPLY_LOAN_cooff_allnum_m01vm12_id']= dict_out['BAIRONG_APPLY_LOAN_cooff_allnum_m01_id']/dict_out['BAIRONG_APPLY_LOAN_cooff_allnum_m12_id'] if dict_out['BAIRONG_APPLY_LOAN_cooff_allnum_m12_id']>0 else np.nan
    dict_out['BAIRONG_APPLY_LOAN_oth_orgnum_d15vm12_id']= dict_out['BAIRONG_APPLY_LOAN_oth_orgnum_d15_id']/dict_out['BAIRONG_APPLY_LOAN_oth_orgnum_m12_id'] if dict_out['BAIRONG_APPLY_LOAN_oth_orgnum_m12_id']>0 else np.nan
    dict_out['BAIRONG_APPLY_LOAN_caon_orgnum_m06_idsubcell']= dict_out['BAIRONG_APPLY_LOAN_caon_orgnum_m06_id']-dict_out['BAIRONG_APPLY_LOAN_caon_orgnum_m06_cell']
    dict_out['BAIRONG_APPLY_LOAN_coon_allnum_d07vm06_id']= dict_out['BAIRONG_APPLY_LOAN_coon_allnum_d07_id']/dict_out['BAIRONG_APPLY_LOAN_coon_allnum_m06_id'] if dict_out['BAIRONG_APPLY_LOAN_coon_allnum_m06_id']>0 else np.nan
    dict_out['BAIRONG_APPLY_LOAN_nbank_finlea_allnum_m06vm12_cell']= dict_out['BAIRONG_APPLY_LOAN_nbank_finlea_allnum_m06_cell']/dict_out['BAIRONG_APPLY_LOAN_nbank_finlea_allnum_m12_cell'] if dict_out['BAIRONG_APPLY_LOAN_nbank_finlea_allnum_m12_cell']>0 else np.nan
    dict_out['BAIRONG_APPLY_LOAN_nbank_mc_allnum_m03vm12_id']= dict_out['BAIRONG_APPLY_LOAN_nbank_mc_allnum_m03_id']/dict_out['BAIRONG_APPLY_LOAN_nbank_mc_allnum_m12_id'] if dict_out['BAIRONG_APPLY_LOAN_nbank_mc_allnum_m12_id']>0 else np.nan
    dict_out['BAIRONG_APPLY_LOAN_pdl_orgnum_m06vm12_id']= dict_out['BAIRONG_APPLY_LOAN_pdl_orgnum_m06_id']/dict_out['BAIRONG_APPLY_LOAN_pdl_orgnum_m12_id'] if dict_out['BAIRONG_APPLY_LOAN_pdl_orgnum_m12_id']>0 else np.nan
    dict_out['BAIRONG_APPLY_LOAN_cooff_allnum_d07vd15_cell']= dict_out['BAIRONG_APPLY_LOAN_cooff_allnum_d07_cell']/dict_out['BAIRONG_APPLY_LOAN_cooff_allnum_d15_cell'] if dict_out['BAIRONG_APPLY_LOAN_cooff_allnum_d15_cell']>0 else np.nan
    dict_out['BAIRONG_APPLY_LOAN_nbank_tot_mons_m03vm06_cell']= dict_out['BAIRONG_APPLY_LOAN_nbank_tot_mons_m03_cell']/dict_out['BAIRONG_APPLY_LOAN_nbank_tot_mons_m06_cell'] if dict_out['BAIRONG_APPLY_LOAN_nbank_tot_mons_m06_cell']>0 else np.nan
    dict_out['BAIRONG_APPLY_LOAN_nbank_allnum_m06vm12_cell']= dict_out['BAIRONG_APPLY_LOAN_nbank_allnum_m06_cell']/dict_out['BAIRONG_APPLY_LOAN_nbank_allnum_m12_cell'] if dict_out['BAIRONG_APPLY_LOAN_nbank_allnum_m12_cell']>0 else np.nan
    dict_out['BAIRONG_APPLY_LOAN_oth_orgnum_m06vm12_cell']= dict_out['BAIRONG_APPLY_LOAN_oth_orgnum_m06_cell']/dict_out['BAIRONG_APPLY_LOAN_oth_orgnum_m12_cell'] if dict_out['BAIRONG_APPLY_LOAN_oth_orgnum_m12_cell']>0 else np.nan
    dict_out['BAIRONG_APPLY_LOAN_nbank_cf_orgnum_m03vm12_id']= dict_out['BAIRONG_APPLY_LOAN_nbank_cf_orgnum_m03_id']/dict_out['BAIRONG_APPLY_LOAN_nbank_cf_orgnum_m12_id'] if dict_out['BAIRONG_APPLY_LOAN_nbank_cf_orgnum_m12_id']>0 else np.nan
    dict_out['BAIRONG_APPLY_LOAN_af_allnum_m01vm03_id']= dict_out['BAIRONG_APPLY_LOAN_af_allnum_m01_id']/dict_out['BAIRONG_APPLY_LOAN_af_allnum_m03_id'] if dict_out['BAIRONG_APPLY_LOAN_af_allnum_m03_id']>0 else np.nan
    dict_out['BAIRONG_APPLY_LOAN_max_inteday_m03vm06_cell']= dict_out['BAIRONG_APPLY_LOAN_max_inteday_m03_cell']/dict_out['BAIRONG_APPLY_LOAN_max_inteday_m06_cell'] if dict_out['BAIRONG_APPLY_LOAN_max_inteday_m06_cell']>0 else np.nan
    dict_out['BAIRONG_APPLY_LOAN_rel_allnum_m01vm12_id']= dict_out['BAIRONG_APPLY_LOAN_rel_allnum_m01_id']/dict_out['BAIRONG_APPLY_LOAN_rel_allnum_m12_id'] if dict_out['BAIRONG_APPLY_LOAN_rel_allnum_m12_id']>0 else np.nan
    dict_out['BAIRONG_APPLY_LOAN_nbank_sloan_allnum_d07vm06_cell']= dict_out['BAIRONG_APPLY_LOAN_nbank_sloan_allnum_d07_cell']/dict_out['BAIRONG_APPLY_LOAN_nbank_sloan_allnum_m06_cell'] if dict_out['BAIRONG_APPLY_LOAN_nbank_sloan_allnum_m06_cell']>0 else np.nan
    dict_out['BAIRONG_APPLY_LOAN_bank_orgnum_d07vm12_id']= dict_out['BAIRONG_APPLY_LOAN_bank_orgnum_d07_id']/dict_out['BAIRONG_APPLY_LOAN_bank_orgnum_m12_id'] if dict_out['BAIRONG_APPLY_LOAN_bank_orgnum_m12_id']>0 else np.nan
    dict_out['BAIRONG_APPLY_LOAN_nbank_else_allnum_m06_idsubcell']= dict_out['BAIRONG_APPLY_LOAN_nbank_else_allnum_m06_id']-dict_out['BAIRONG_APPLY_LOAN_nbank_else_allnum_m06_cell']
    dict_out['BAIRONG_APPLY_LOAN_nbank_cons_orgnum_m03vm12_id']= dict_out['BAIRONG_APPLY_LOAN_nbank_cons_orgnum_m03_id']/dict_out['BAIRONG_APPLY_LOAN_nbank_cons_orgnum_m12_id'] if dict_out['BAIRONG_APPLY_LOAN_nbank_cons_orgnum_m12_id']>0 else np.nan
    dict_out['BAIRONG_APPLY_LOAN_oth_orgnum_m01vm12_cell']= dict_out['BAIRONG_APPLY_LOAN_oth_orgnum_m01_cell']/dict_out['BAIRONG_APPLY_LOAN_oth_orgnum_m12_cell'] if dict_out['BAIRONG_APPLY_LOAN_oth_orgnum_m12_cell']>0 else np.nan
    dict_out['BAIRONG_APPLY_LOAN_nbank_orgnum_m01vm06_id']= dict_out['BAIRONG_APPLY_LOAN_nbank_orgnum_m01_id']/dict_out['BAIRONG_APPLY_LOAN_nbank_orgnum_m06_id'] if dict_out['BAIRONG_APPLY_LOAN_nbank_orgnum_m06_id']>0 else np.nan
    dict_out['BAIRONG_APPLY_LOAN_caon_orgnum_m03vm06_cell']= dict_out['BAIRONG_APPLY_LOAN_caon_orgnum_m03_cell']/dict_out['BAIRONG_APPLY_LOAN_caon_orgnum_m06_cell'] if dict_out['BAIRONG_APPLY_LOAN_caon_orgnum_m06_cell']>0 else np.nan
    dict_out['BAIRONG_APPLY_LOAN_rel_orgnum_d07vd15_cell']= dict_out['BAIRONG_APPLY_LOAN_rel_orgnum_d07_cell']/dict_out['BAIRONG_APPLY_LOAN_rel_orgnum_d15_cell'] if dict_out['BAIRONG_APPLY_LOAN_rel_orgnum_d15_cell']>0 else np.nan
    dict_out['BAIRONG_APPLY_LOAN_pdl_orgnum_d07vm01_cell']= dict_out['BAIRONG_APPLY_LOAN_pdl_orgnum_d07_cell']/dict_out['BAIRONG_APPLY_LOAN_pdl_orgnum_m01_cell'] if dict_out['BAIRONG_APPLY_LOAN_pdl_orgnum_m01_cell']>0 else np.nan
    dict_out['BAIRONG_APPLY_LOAN_nbank_week_allnum_m01vm06_id']= dict_out['BAIRONG_APPLY_LOAN_nbank_week_allnum_m01_id']/dict_out['BAIRONG_APPLY_LOAN_nbank_week_allnum_m06_id'] if dict_out['BAIRONG_APPLY_LOAN_nbank_week_allnum_m06_id']>0 else np.nan
    dict_out['BAIRONG_APPLY_LOAN_nbank_nsloan_orgnum_m12_idsubcell']= dict_out['BAIRONG_APPLY_LOAN_nbank_nsloan_orgnum_m12_id']-dict_out['BAIRONG_APPLY_LOAN_nbank_nsloan_orgnum_m12_cell']
    dict_out['BAIRONG_APPLY_LOAN_nbank_finlea_allnum_d07vm12_id']= dict_out['BAIRONG_APPLY_LOAN_nbank_finlea_allnum_d07_id']/dict_out['BAIRONG_APPLY_LOAN_nbank_finlea_allnum_m12_id'] if dict_out['BAIRONG_APPLY_LOAN_nbank_finlea_allnum_m12_id']>0 else np.nan
    dict_out['BAIRONG_APPLY_LOAN_caon_orgnum_m06vm12_id']= dict_out['BAIRONG_APPLY_LOAN_caon_orgnum_m06_id']/dict_out['BAIRONG_APPLY_LOAN_caon_orgnum_m12_id'] if dict_out['BAIRONG_APPLY_LOAN_caon_orgnum_m12_id']>0 else np.nan
    dict_out['BAIRONG_APPLY_LOAN_nbank_nsloan_orgnum_m01vm03_cell']= dict_out['BAIRONG_APPLY_LOAN_nbank_nsloan_orgnum_m01_cell']/dict_out['BAIRONG_APPLY_LOAN_nbank_nsloan_orgnum_m03_cell'] if dict_out['BAIRONG_APPLY_LOAN_nbank_nsloan_orgnum_m03_cell']>0 else np.nan
    dict_out['BAIRONG_APPLY_LOAN_oth_orgnum_m03vm12_cell']= dict_out['BAIRONG_APPLY_LOAN_oth_orgnum_m03_cell']/dict_out['BAIRONG_APPLY_LOAN_oth_orgnum_m12_cell'] if dict_out['BAIRONG_APPLY_LOAN_oth_orgnum_m12_cell']>0 else np.nan
    dict_out['BAIRONG_APPLY_LOAN_nbank_else_orgnum_m03vm06_cell']= dict_out['BAIRONG_APPLY_LOAN_nbank_else_orgnum_m03_cell']/dict_out['BAIRONG_APPLY_LOAN_nbank_else_orgnum_m06_cell'] if dict_out['BAIRONG_APPLY_LOAN_nbank_else_orgnum_m06_cell']>0 else np.nan
    dict_out['BAIRONG_APPLY_LOAN_caoff_allnum_m06vm12_cell']= dict_out['BAIRONG_APPLY_LOAN_caoff_allnum_m06_cell']/dict_out['BAIRONG_APPLY_LOAN_caoff_allnum_m12_cell'] if dict_out['BAIRONG_APPLY_LOAN_caoff_allnum_m12_cell']>0 else np.nan
    dict_out['BAIRONG_APPLY_LOAN_nbank_cons_allnum_d07vd15_cell']= dict_out['BAIRONG_APPLY_LOAN_nbank_cons_allnum_d07_cell']/dict_out['BAIRONG_APPLY_LOAN_nbank_cons_allnum_d15_cell'] if dict_out['BAIRONG_APPLY_LOAN_nbank_cons_allnum_d15_cell']>0 else np.nan
    dict_out['BAIRONG_APPLY_LOAN_caon_allnum_d07vm01_id']= dict_out['BAIRONG_APPLY_LOAN_caon_allnum_d07_id']/dict_out['BAIRONG_APPLY_LOAN_caon_allnum_m01_id'] if dict_out['BAIRONG_APPLY_LOAN_caon_allnum_m01_id']>0 else np.nan
    dict_out['BAIRONG_APPLY_LOAN_nbank_nsloan_orgnum_d15vm01_cell']= dict_out['BAIRONG_APPLY_LOAN_nbank_nsloan_orgnum_d15_cell']/dict_out['BAIRONG_APPLY_LOAN_nbank_nsloan_orgnum_m01_cell'] if dict_out['BAIRONG_APPLY_LOAN_nbank_nsloan_orgnum_m01_cell']>0 else np.nan
    dict_out['BAIRONG_APPLY_LOAN_nbank_night_allnum_m01vm12_id']= dict_out['BAIRONG_APPLY_LOAN_nbank_night_allnum_m01_id']/dict_out['BAIRONG_APPLY_LOAN_nbank_night_allnum_m12_id'] if dict_out['BAIRONG_APPLY_LOAN_nbank_night_allnum_m12_id']>0 else np.nan
    dict_out['BAIRONG_APPLY_LOAN_nbank_p2p_allnum_m01vm12_cell']= dict_out['BAIRONG_APPLY_LOAN_nbank_p2p_allnum_m01_cell']/dict_out['BAIRONG_APPLY_LOAN_nbank_p2p_allnum_m12_cell'] if dict_out['BAIRONG_APPLY_LOAN_nbank_p2p_allnum_m12_cell']>0 else np.nan
    dict_out['BAIRONG_APPLY_LOAN_bank_orgnum_d15vm12_id']= dict_out['BAIRONG_APPLY_LOAN_bank_orgnum_d15_id']/dict_out['BAIRONG_APPLY_LOAN_bank_orgnum_m12_id'] if dict_out['BAIRONG_APPLY_LOAN_bank_orgnum_m12_id']>0 else np.nan
    dict_out['BAIRONG_APPLY_LOAN_nbank_nsloan_orgnum_m06vm12_cell']= dict_out['BAIRONG_APPLY_LOAN_nbank_nsloan_orgnum_m06_cell']/dict_out['BAIRONG_APPLY_LOAN_nbank_nsloan_orgnum_m12_cell'] if dict_out['BAIRONG_APPLY_LOAN_nbank_nsloan_orgnum_m12_cell']>0 else np.nan
    dict_out['BAIRONG_APPLY_LOAN_bank_ret_allnum_m03vm06_id']= dict_out['BAIRONG_APPLY_LOAN_bank_ret_allnum_m03_id']/dict_out['BAIRONG_APPLY_LOAN_bank_ret_allnum_m06_id'] if dict_out['BAIRONG_APPLY_LOAN_bank_ret_allnum_m06_id']>0 else np.nan
    dict_out['BAIRONG_APPLY_LOAN_bank_tra_allnum_m03vm12_cell']= dict_out['BAIRONG_APPLY_LOAN_bank_tra_allnum_m03_cell']/dict_out['BAIRONG_APPLY_LOAN_bank_tra_allnum_m12_cell'] if dict_out['BAIRONG_APPLY_LOAN_bank_tra_allnum_m12_cell']>0 else np.nan
    dict_out['BAIRONG_APPLY_LOAN_max_monnum_m06vm12_cell']= dict_out['BAIRONG_APPLY_LOAN_max_monnum_m06_cell']/dict_out['BAIRONG_APPLY_LOAN_max_monnum_m12_cell'] if dict_out['BAIRONG_APPLY_LOAN_max_monnum_m12_cell']>0 else np.nan
    dict_out['BAIRONG_APPLY_LOAN_nbank_sloan_orgnum_d07vm06_id']= dict_out['BAIRONG_APPLY_LOAN_nbank_sloan_orgnum_d07_id']/dict_out['BAIRONG_APPLY_LOAN_nbank_sloan_orgnum_m06_id'] if dict_out['BAIRONG_APPLY_LOAN_nbank_sloan_orgnum_m06_id']>0 else np.nan
    dict_out['BAIRONG_APPLY_LOAN_pdl_orgnum_d07vm01_id']= dict_out['BAIRONG_APPLY_LOAN_pdl_orgnum_d07_id']/dict_out['BAIRONG_APPLY_LOAN_pdl_orgnum_m01_id'] if dict_out['BAIRONG_APPLY_LOAN_pdl_orgnum_m01_id']>0 else np.nan
    dict_out['BAIRONG_APPLY_LOAN_nbank_orgnum_m06vm12_cell']= dict_out['BAIRONG_APPLY_LOAN_nbank_orgnum_m06_cell']/dict_out['BAIRONG_APPLY_LOAN_nbank_orgnum_m12_cell'] if dict_out['BAIRONG_APPLY_LOAN_nbank_orgnum_m12_cell']>0 else np.nan
    dict_out['BAIRONG_APPLY_LOAN_nbank_cons_orgnum_d07vm01_id']= dict_out['BAIRONG_APPLY_LOAN_nbank_cons_orgnum_d07_id']/dict_out['BAIRONG_APPLY_LOAN_nbank_cons_orgnum_m01_id'] if dict_out['BAIRONG_APPLY_LOAN_nbank_cons_orgnum_m01_id']>0 else np.nan
    dict_out['BAIRONG_APPLY_LOAN_cooff_orgnum_m06vm12_cell']= dict_out['BAIRONG_APPLY_LOAN_cooff_orgnum_m06_cell']/dict_out['BAIRONG_APPLY_LOAN_cooff_orgnum_m12_cell'] if dict_out['BAIRONG_APPLY_LOAN_cooff_orgnum_m12_cell']>0 else np.nan
    dict_out['BAIRONG_APPLY_LOAN_bank_allnum_m01vm06_cell']= dict_out['BAIRONG_APPLY_LOAN_bank_allnum_m01_cell']/dict_out['BAIRONG_APPLY_LOAN_bank_allnum_m06_cell'] if dict_out['BAIRONG_APPLY_LOAN_bank_allnum_m06_cell']>0 else np.nan
    dict_out['BAIRONG_APPLY_LOAN_nbank_allnum_d15vm06_id']= dict_out['BAIRONG_APPLY_LOAN_nbank_allnum_d15_id']/dict_out['BAIRONG_APPLY_LOAN_nbank_allnum_m06_id'] if dict_out['BAIRONG_APPLY_LOAN_nbank_allnum_m06_id']>0 else np.nan
    dict_out['BAIRONG_APPLY_LOAN_bank_tra_allnum_m03vm12_id']= dict_out['BAIRONG_APPLY_LOAN_bank_tra_allnum_m03_id']/dict_out['BAIRONG_APPLY_LOAN_bank_tra_allnum_m12_id'] if dict_out['BAIRONG_APPLY_LOAN_bank_tra_allnum_m12_id']>0 else np.nan
    dict_out['BAIRONG_APPLY_LOAN_min_inteday_m06_idsubcell']= dict_out['BAIRONG_APPLY_LOAN_min_inteday_m06_id']-dict_out['BAIRONG_APPLY_LOAN_min_inteday_m06_cell']
    dict_out['BAIRONG_APPLY_LOAN_nbank_oth_orgnum_d07vm12_cell']= dict_out['BAIRONG_APPLY_LOAN_nbank_oth_orgnum_d07_cell']/dict_out['BAIRONG_APPLY_LOAN_nbank_oth_orgnum_m12_cell'] if dict_out['BAIRONG_APPLY_LOAN_nbank_oth_orgnum_m12_cell']>0 else np.nan
    dict_out['BAIRONG_APPLY_LOAN_nbank_week_orgnum_d15vm12_id']= dict_out['BAIRONG_APPLY_LOAN_nbank_week_orgnum_d15_id']/dict_out['BAIRONG_APPLY_LOAN_nbank_week_orgnum_m12_id'] if dict_out['BAIRONG_APPLY_LOAN_nbank_week_orgnum_m12_id']>0 else np.nan
    dict_out['BAIRONG_APPLY_LOAN_nbank_orgnum_d07vd15_id']= dict_out['BAIRONG_APPLY_LOAN_nbank_orgnum_d07_id']/dict_out['BAIRONG_APPLY_LOAN_nbank_orgnum_d15_id'] if dict_out['BAIRONG_APPLY_LOAN_nbank_orgnum_d15_id']>0 else np.nan
    dict_out['BAIRONG_APPLY_LOAN_nbank_cf_orgnum_d07vm01_cell']= dict_out['BAIRONG_APPLY_LOAN_nbank_cf_orgnum_d07_cell']/dict_out['BAIRONG_APPLY_LOAN_nbank_cf_orgnum_m01_cell'] if dict_out['BAIRONG_APPLY_LOAN_nbank_cf_orgnum_m01_cell']>0 else np.nan
    dict_out['BAIRONG_APPLY_LOAN_nbank_else_orgnum_m01vm06_cell']= dict_out['BAIRONG_APPLY_LOAN_nbank_else_orgnum_m01_cell']/dict_out['BAIRONG_APPLY_LOAN_nbank_else_orgnum_m06_cell'] if dict_out['BAIRONG_APPLY_LOAN_nbank_else_orgnum_m06_cell']>0 else np.nan
    dict_out['BAIRONG_APPLY_LOAN_nbank_oth_orgnum_m06vm12_id']= dict_out['BAIRONG_APPLY_LOAN_nbank_oth_orgnum_m06_id']/dict_out['BAIRONG_APPLY_LOAN_nbank_oth_orgnum_m12_id'] if dict_out['BAIRONG_APPLY_LOAN_nbank_oth_orgnum_m12_id']>0 else np.nan
    dict_out['BAIRONG_APPLY_LOAN_coon_allnum_m03vm12_id']= dict_out['BAIRONG_APPLY_LOAN_coon_allnum_m03_id']/dict_out['BAIRONG_APPLY_LOAN_coon_allnum_m12_id'] if dict_out['BAIRONG_APPLY_LOAN_coon_allnum_m12_id']>0 else np.nan
    dict_out['BAIRONG_APPLY_LOAN_nbank_min_inteday_m06vm12_id']= dict_out['BAIRONG_APPLY_LOAN_nbank_min_inteday_m06_id']/dict_out['BAIRONG_APPLY_LOAN_nbank_min_inteday_m12_id'] if dict_out['BAIRONG_APPLY_LOAN_nbank_min_inteday_m12_id']>0 else np.nan
    dict_out['BAIRONG_APPLY_LOAN_pdl_orgnum_d15vm06_id']= dict_out['BAIRONG_APPLY_LOAN_pdl_orgnum_d15_id']/dict_out['BAIRONG_APPLY_LOAN_pdl_orgnum_m06_id'] if dict_out['BAIRONG_APPLY_LOAN_pdl_orgnum_m06_id']>0 else np.nan
    dict_out['BAIRONG_APPLY_LOAN_nbank_oth_allnum_m01vm03_id']= dict_out['BAIRONG_APPLY_LOAN_nbank_oth_allnum_m01_id']/dict_out['BAIRONG_APPLY_LOAN_nbank_oth_allnum_m03_id'] if dict_out['BAIRONG_APPLY_LOAN_nbank_oth_allnum_m03_id']>0 else np.nan
    dict_out['BAIRONG_APPLY_LOAN_pdl_allnum_m01vm03_cell']= dict_out['BAIRONG_APPLY_LOAN_pdl_allnum_m01_cell']/dict_out['BAIRONG_APPLY_LOAN_pdl_allnum_m03_cell'] if dict_out['BAIRONG_APPLY_LOAN_pdl_allnum_m03_cell']>0 else np.nan
    dict_out['BAIRONG_APPLY_LOAN_nbank_ca_allnum_m03vm12_id']= dict_out['BAIRONG_APPLY_LOAN_nbank_ca_allnum_m03_id']/dict_out['BAIRONG_APPLY_LOAN_nbank_ca_allnum_m12_id'] if dict_out['BAIRONG_APPLY_LOAN_nbank_ca_allnum_m12_id']>0 else np.nan
    dict_out['BAIRONG_APPLY_LOAN_nbank_cons_orgnum_m01vm12_id']= dict_out['BAIRONG_APPLY_LOAN_nbank_cons_orgnum_m01_id']/dict_out['BAIRONG_APPLY_LOAN_nbank_cons_orgnum_m12_id'] if dict_out['BAIRONG_APPLY_LOAN_nbank_cons_orgnum_m12_id']>0 else np.nan
    dict_out['BAIRONG_APPLY_LOAN_nbank_allnum_d15vm12_id']= dict_out['BAIRONG_APPLY_LOAN_nbank_allnum_d15_id']/dict_out['BAIRONG_APPLY_LOAN_nbank_allnum_m12_id'] if dict_out['BAIRONG_APPLY_LOAN_nbank_allnum_m12_id']>0 else np.nan
    dict_out['BAIRONG_APPLY_LOAN_cooff_allnum_m01vm12_cell']= dict_out['BAIRONG_APPLY_LOAN_cooff_allnum_m01_cell']/dict_out['BAIRONG_APPLY_LOAN_cooff_allnum_m12_cell'] if dict_out['BAIRONG_APPLY_LOAN_cooff_allnum_m12_cell']>0 else np.nan
    dict_out['BAIRONG_APPLY_LOAN_avg_monnum_m03vm06_id']= dict_out['BAIRONG_APPLY_LOAN_avg_monnum_m03_id']/dict_out['BAIRONG_APPLY_LOAN_avg_monnum_m06_id'] if dict_out['BAIRONG_APPLY_LOAN_avg_monnum_m06_id']>0 else np.nan
    dict_out['BAIRONG_APPLY_LOAN_nbank_sloan_orgnum_m06vm12_cell']= dict_out['BAIRONG_APPLY_LOAN_nbank_sloan_orgnum_m06_cell']/dict_out['BAIRONG_APPLY_LOAN_nbank_sloan_orgnum_m12_cell'] if dict_out['BAIRONG_APPLY_LOAN_nbank_sloan_orgnum_m12_cell']>0 else np.nan
    dict_out['BAIRONG_APPLY_LOAN_nbank_max_monnum_m03vm06_id']= dict_out['BAIRONG_APPLY_LOAN_nbank_max_monnum_m03_id']/dict_out['BAIRONG_APPLY_LOAN_nbank_max_monnum_m06_id'] if dict_out['BAIRONG_APPLY_LOAN_nbank_max_monnum_m06_id']>0 else np.nan
    dict_out['BAIRONG_APPLY_LOAN_nbank_p2p_allnum_m01vm03_cell']= dict_out['BAIRONG_APPLY_LOAN_nbank_p2p_allnum_m01_cell']/dict_out['BAIRONG_APPLY_LOAN_nbank_p2p_allnum_m03_cell'] if dict_out['BAIRONG_APPLY_LOAN_nbank_p2p_allnum_m03_cell']>0 else np.nan
    dict_out['BAIRONG_APPLY_LOAN_rel_allnum_d07vm06_id']= dict_out['BAIRONG_APPLY_LOAN_rel_allnum_d07_id']/dict_out['BAIRONG_APPLY_LOAN_rel_allnum_m06_id'] if dict_out['BAIRONG_APPLY_LOAN_rel_allnum_m06_id']>0 else np.nan
    dict_out['BAIRONG_APPLY_LOAN_nbank_oth_allnum_d15vm12_id']= dict_out['BAIRONG_APPLY_LOAN_nbank_oth_allnum_d15_id']/dict_out['BAIRONG_APPLY_LOAN_nbank_oth_allnum_m12_id'] if dict_out['BAIRONG_APPLY_LOAN_nbank_oth_allnum_m12_id']>0 else np.nan
    dict_out['BAIRONG_APPLY_LOAN_pdl_orgnum_m03vm12_id']= dict_out['BAIRONG_APPLY_LOAN_pdl_orgnum_m03_id']/dict_out['BAIRONG_APPLY_LOAN_pdl_orgnum_m12_id'] if dict_out['BAIRONG_APPLY_LOAN_pdl_orgnum_m12_id']>0 else np.nan
    dict_out['BAIRONG_APPLY_LOAN_nbank_oth_allnum_m12_idsubcell']= dict_out['BAIRONG_APPLY_LOAN_nbank_oth_allnum_m12_id']-dict_out['BAIRONG_APPLY_LOAN_nbank_oth_allnum_m12_cell']
    dict_out['BAIRONG_APPLY_LOAN_nbank_ca_allnum_d15vm01_cell']= dict_out['BAIRONG_APPLY_LOAN_nbank_ca_allnum_d15_cell']/dict_out['BAIRONG_APPLY_LOAN_nbank_ca_allnum_m01_cell'] if dict_out['BAIRONG_APPLY_LOAN_nbank_ca_allnum_m01_cell']>0 else np.nan
    dict_out['BAIRONG_APPLY_LOAN_bank_allnum_m06vm12_id']= dict_out['BAIRONG_APPLY_LOAN_bank_allnum_m06_id']/dict_out['BAIRONG_APPLY_LOAN_bank_allnum_m12_id'] if dict_out['BAIRONG_APPLY_LOAN_bank_allnum_m12_id']>0 else np.nan
    dict_out['BAIRONG_APPLY_LOAN_caoff_allnum_m01vm06_id']= dict_out['BAIRONG_APPLY_LOAN_caoff_allnum_m01_id']/dict_out['BAIRONG_APPLY_LOAN_caoff_allnum_m06_id'] if dict_out['BAIRONG_APPLY_LOAN_caoff_allnum_m06_id']>0 else np.nan
    dict_out['BAIRONG_APPLY_LOAN_nbank_else_allnum_m01vm03_id']= dict_out['BAIRONG_APPLY_LOAN_nbank_else_allnum_m01_id']/dict_out['BAIRONG_APPLY_LOAN_nbank_else_allnum_m03_id'] if dict_out['BAIRONG_APPLY_LOAN_nbank_else_allnum_m03_id']>0 else np.nan
    dict_out['BAIRONG_APPLY_LOAN_nbank_max_monnum_m03vm06_cell']= dict_out['BAIRONG_APPLY_LOAN_nbank_max_monnum_m03_cell']/dict_out['BAIRONG_APPLY_LOAN_nbank_max_monnum_m06_cell'] if dict_out['BAIRONG_APPLY_LOAN_nbank_max_monnum_m06_cell']>0 else np.nan
    dict_out['BAIRONG_APPLY_LOAN_pdl_orgnum_m01vm03_id']= dict_out['BAIRONG_APPLY_LOAN_pdl_orgnum_m01_id']/dict_out['BAIRONG_APPLY_LOAN_pdl_orgnum_m03_id'] if dict_out['BAIRONG_APPLY_LOAN_pdl_orgnum_m03_id']>0 else np.nan
    dict_out['BAIRONG_APPLY_LOAN_af_allnum_m06vm12_id']= dict_out['BAIRONG_APPLY_LOAN_af_allnum_m06_id']/dict_out['BAIRONG_APPLY_LOAN_af_allnum_m12_id'] if dict_out['BAIRONG_APPLY_LOAN_af_allnum_m12_id']>0 else np.nan
    dict_out['BAIRONG_APPLY_LOAN_pdl_orgnum_d07vd15_cell']= dict_out['BAIRONG_APPLY_LOAN_pdl_orgnum_d07_cell']/dict_out['BAIRONG_APPLY_LOAN_pdl_orgnum_d15_cell'] if dict_out['BAIRONG_APPLY_LOAN_pdl_orgnum_d15_cell']>0 else np.nan
    dict_out['BAIRONG_APPLY_LOAN_nbank_cons_allnum_d15vm12_id']= dict_out['BAIRONG_APPLY_LOAN_nbank_cons_allnum_d15_id']/dict_out['BAIRONG_APPLY_LOAN_nbank_cons_allnum_m12_id'] if dict_out['BAIRONG_APPLY_LOAN_nbank_cons_allnum_m12_id']>0 else np.nan
    dict_out['BAIRONG_APPLY_LOAN_af_orgnum_m06vm12_id']= dict_out['BAIRONG_APPLY_LOAN_af_orgnum_m06_id']/dict_out['BAIRONG_APPLY_LOAN_af_orgnum_m12_id'] if dict_out['BAIRONG_APPLY_LOAN_af_orgnum_m12_id']>0 else np.nan
    dict_out['BAIRONG_APPLY_LOAN_nbank_oth_orgnum_m06vm12_cell']= dict_out['BAIRONG_APPLY_LOAN_nbank_oth_orgnum_m06_cell']/dict_out['BAIRONG_APPLY_LOAN_nbank_oth_orgnum_m12_cell'] if dict_out['BAIRONG_APPLY_LOAN_nbank_oth_orgnum_m12_cell']>0 else np.nan
    dict_out['BAIRONG_APPLY_LOAN_bank_ret_allnum_m06vm12_cell']= dict_out['BAIRONG_APPLY_LOAN_bank_ret_allnum_m06_cell']/dict_out['BAIRONG_APPLY_LOAN_bank_ret_allnum_m12_cell'] if dict_out['BAIRONG_APPLY_LOAN_bank_ret_allnum_m12_cell']>0 else np.nan
    dict_out['BAIRONG_APPLY_LOAN_bank_avg_monnum_m03vm06_id']= dict_out['BAIRONG_APPLY_LOAN_bank_avg_monnum_m03_id']/dict_out['BAIRONG_APPLY_LOAN_bank_avg_monnum_m06_id'] if dict_out['BAIRONG_APPLY_LOAN_bank_avg_monnum_m06_id']>0 else np.nan
    dict_out['BAIRONG_APPLY_LOAN_max_inteday_m06vm12_id']= dict_out['BAIRONG_APPLY_LOAN_max_inteday_m06_id']/dict_out['BAIRONG_APPLY_LOAN_max_inteday_m12_id'] if dict_out['BAIRONG_APPLY_LOAN_max_inteday_m12_id']>0 else np.nan
    dict_out['BAIRONG_APPLY_LOAN_nbank_mc_allnum_m06_idsubcell']= dict_out['BAIRONG_APPLY_LOAN_nbank_mc_allnum_m06_id']-dict_out['BAIRONG_APPLY_LOAN_nbank_mc_allnum_m06_cell']
    dict_out['BAIRONG_APPLY_LOAN_caoff_orgnum_d15vm12_id']= dict_out['BAIRONG_APPLY_LOAN_caoff_orgnum_d15_id']/dict_out['BAIRONG_APPLY_LOAN_caoff_orgnum_m12_id'] if dict_out['BAIRONG_APPLY_LOAN_caoff_orgnum_m12_id']>0 else np.nan
    dict_out['BAIRONG_APPLY_LOAN_bank_tra_orgnum_m06vm12_cell']= dict_out['BAIRONG_APPLY_LOAN_bank_tra_orgnum_m06_cell']/dict_out['BAIRONG_APPLY_LOAN_bank_tra_orgnum_m12_cell'] if dict_out['BAIRONG_APPLY_LOAN_bank_tra_orgnum_m12_cell']>0 else np.nan
    dict_out['BAIRONG_APPLY_LOAN_nbank_orgnum_m03vm06_cell']= dict_out['BAIRONG_APPLY_LOAN_nbank_orgnum_m03_cell']/dict_out['BAIRONG_APPLY_LOAN_nbank_orgnum_m06_cell'] if dict_out['BAIRONG_APPLY_LOAN_nbank_orgnum_m06_cell']>0 else np.nan
    dict_out['BAIRONG_APPLY_LOAN_nbank_else_orgnum_d07vm12_id']= dict_out['BAIRONG_APPLY_LOAN_nbank_else_orgnum_d07_id']/dict_out['BAIRONG_APPLY_LOAN_nbank_else_orgnum_m12_id'] if dict_out['BAIRONG_APPLY_LOAN_nbank_else_orgnum_m12_id']>0 else np.nan
    dict_out['BAIRONG_APPLY_LOAN_nbank_ca_orgnum_m01vm12_id']= dict_out['BAIRONG_APPLY_LOAN_nbank_ca_orgnum_m01_id']/dict_out['BAIRONG_APPLY_LOAN_nbank_ca_orgnum_m12_id'] if dict_out['BAIRONG_APPLY_LOAN_nbank_ca_orgnum_m12_id']>0 else np.nan
    dict_out['BAIRONG_APPLY_LOAN_nbank_nsloan_orgnum_d15vm01_id']= dict_out['BAIRONG_APPLY_LOAN_nbank_nsloan_orgnum_d15_id']/dict_out['BAIRONG_APPLY_LOAN_nbank_nsloan_orgnum_m01_id'] if dict_out['BAIRONG_APPLY_LOAN_nbank_nsloan_orgnum_m01_id']>0 else np.nan
    dict_out['BAIRONG_APPLY_LOAN_nbank_cf_allnum_d07vm06_cell']= dict_out['BAIRONG_APPLY_LOAN_nbank_cf_allnum_d07_cell']/dict_out['BAIRONG_APPLY_LOAN_nbank_cf_allnum_m06_cell'] if dict_out['BAIRONG_APPLY_LOAN_nbank_cf_allnum_m06_cell']>0 else np.nan
    dict_out['BAIRONG_APPLY_LOAN_pdl_allnum_d15vm01_cell']= dict_out['BAIRONG_APPLY_LOAN_pdl_allnum_d15_cell']/dict_out['BAIRONG_APPLY_LOAN_pdl_allnum_m01_cell'] if dict_out['BAIRONG_APPLY_LOAN_pdl_allnum_m01_cell']>0 else np.nan
    dict_out['BAIRONG_APPLY_LOAN_bank_ret_allnum_m03_idsubcell']= dict_out['BAIRONG_APPLY_LOAN_bank_ret_allnum_m03_id']-dict_out['BAIRONG_APPLY_LOAN_bank_ret_allnum_m03_cell']
    dict_out['BAIRONG_APPLY_LOAN_caon_allnum_d07vd15_cell']= dict_out['BAIRONG_APPLY_LOAN_caon_allnum_d07_cell']/dict_out['BAIRONG_APPLY_LOAN_caon_allnum_d15_cell'] if dict_out['BAIRONG_APPLY_LOAN_caon_allnum_d15_cell']>0 else np.nan
    dict_out['BAIRONG_APPLY_LOAN_max_monnum_m12_idsubcell']= dict_out['BAIRONG_APPLY_LOAN_max_monnum_m12_id']-dict_out['BAIRONG_APPLY_LOAN_max_monnum_m12_cell']
    dict_out['BAIRONG_APPLY_LOAN_nbank_ca_orgnum_d15vm12_cell']= dict_out['BAIRONG_APPLY_LOAN_nbank_ca_orgnum_d15_cell']/dict_out['BAIRONG_APPLY_LOAN_nbank_ca_orgnum_m12_cell'] if dict_out['BAIRONG_APPLY_LOAN_nbank_ca_orgnum_m12_cell']>0 else np.nan
    dict_out['BAIRONG_APPLY_LOAN_pdl_orgnum_m01vm03_cell']= dict_out['BAIRONG_APPLY_LOAN_pdl_orgnum_m01_cell']/dict_out['BAIRONG_APPLY_LOAN_pdl_orgnum_m03_cell'] if dict_out['BAIRONG_APPLY_LOAN_pdl_orgnum_m03_cell']>0 else np.nan
    dict_out['BAIRONG_APPLY_LOAN_rel_allnum_d15vm12_cell']= dict_out['BAIRONG_APPLY_LOAN_rel_allnum_d15_cell']/dict_out['BAIRONG_APPLY_LOAN_rel_allnum_m12_cell'] if dict_out['BAIRONG_APPLY_LOAN_rel_allnum_m12_cell']>0 else np.nan
    dict_out['BAIRONG_APPLY_LOAN_max_inteday_m03_idsubcell']= dict_out['BAIRONG_APPLY_LOAN_max_inteday_m03_id']-dict_out['BAIRONG_APPLY_LOAN_max_inteday_m03_cell']
    dict_out['BAIRONG_APPLY_LOAN_nbank_oth_orgnum_m06_idsubcell']= dict_out['BAIRONG_APPLY_LOAN_nbank_oth_orgnum_m06_id']-dict_out['BAIRONG_APPLY_LOAN_nbank_oth_orgnum_m06_cell']
    dict_out['BAIRONG_APPLY_LOAN_pdl_orgnum_d15vm06_cell']= dict_out['BAIRONG_APPLY_LOAN_pdl_orgnum_d15_cell']/dict_out['BAIRONG_APPLY_LOAN_pdl_orgnum_m06_cell'] if dict_out['BAIRONG_APPLY_LOAN_pdl_orgnum_m06_cell']>0 else np.nan
    dict_out['BAIRONG_APPLY_LOAN_pdl_allnum_m03vm12_id']= dict_out['BAIRONG_APPLY_LOAN_pdl_allnum_m03_id']/dict_out['BAIRONG_APPLY_LOAN_pdl_allnum_m12_id'] if dict_out['BAIRONG_APPLY_LOAN_pdl_allnum_m12_id']>0 else np.nan
    dict_out['BAIRONG_APPLY_LOAN_nbank_cons_allnum_d07vm06_cell']= dict_out['BAIRONG_APPLY_LOAN_nbank_cons_allnum_d07_cell']/dict_out['BAIRONG_APPLY_LOAN_nbank_cons_allnum_m06_cell'] if dict_out['BAIRONG_APPLY_LOAN_nbank_cons_allnum_m06_cell']>0 else np.nan
    dict_out['BAIRONG_APPLY_LOAN_nbank_else_orgnum_d15vm01_cell']= dict_out['BAIRONG_APPLY_LOAN_nbank_else_orgnum_d15_cell']/dict_out['BAIRONG_APPLY_LOAN_nbank_else_orgnum_m01_cell'] if dict_out['BAIRONG_APPLY_LOAN_nbank_else_orgnum_m01_cell']>0 else np.nan
    dict_out['BAIRONG_APPLY_LOAN_nbank_allnum_d07vm12_id']= dict_out['BAIRONG_APPLY_LOAN_nbank_allnum_d07_id']/dict_out['BAIRONG_APPLY_LOAN_nbank_allnum_m12_id'] if dict_out['BAIRONG_APPLY_LOAN_nbank_allnum_m12_id']>0 else np.nan
    dict_out['BAIRONG_APPLY_LOAN_nbank_nsloan_orgnum_m01vm06_id']= dict_out['BAIRONG_APPLY_LOAN_nbank_nsloan_orgnum_m01_id']/dict_out['BAIRONG_APPLY_LOAN_nbank_nsloan_orgnum_m06_id'] if dict_out['BAIRONG_APPLY_LOAN_nbank_nsloan_orgnum_m06_id']>0 else np.nan
    dict_out['BAIRONG_APPLY_LOAN_nbank_night_orgnum_m03vm06_cell']= dict_out['BAIRONG_APPLY_LOAN_nbank_night_orgnum_m03_cell']/dict_out['BAIRONG_APPLY_LOAN_nbank_night_orgnum_m06_cell'] if dict_out['BAIRONG_APPLY_LOAN_nbank_night_orgnum_m06_cell']>0 else np.nan
    dict_out['BAIRONG_APPLY_LOAN_caon_allnum_m03vm06_cell']= dict_out['BAIRONG_APPLY_LOAN_caon_allnum_m03_cell']/dict_out['BAIRONG_APPLY_LOAN_caon_allnum_m06_cell'] if dict_out['BAIRONG_APPLY_LOAN_caon_allnum_m06_cell']>0 else np.nan
    dict_out['BAIRONG_APPLY_LOAN_caon_orgnum_m03vm06_id']= dict_out['BAIRONG_APPLY_LOAN_caon_orgnum_m03_id']/dict_out['BAIRONG_APPLY_LOAN_caon_orgnum_m06_id'] if dict_out['BAIRONG_APPLY_LOAN_caon_orgnum_m06_id']>0 else np.nan
    dict_out['BAIRONG_APPLY_LOAN_nbank_ca_allnum_d07vm06_id']= dict_out['BAIRONG_APPLY_LOAN_nbank_ca_allnum_d07_id']/dict_out['BAIRONG_APPLY_LOAN_nbank_ca_allnum_m06_id'] if dict_out['BAIRONG_APPLY_LOAN_nbank_ca_allnum_m06_id']>0 else np.nan
    dict_out['BAIRONG_APPLY_LOAN_nbank_allnum_d07vd15_id']= dict_out['BAIRONG_APPLY_LOAN_nbank_allnum_d07_id']/dict_out['BAIRONG_APPLY_LOAN_nbank_allnum_d15_id'] if dict_out['BAIRONG_APPLY_LOAN_nbank_allnum_d15_id']>0 else np.nan
    dict_out['BAIRONG_APPLY_LOAN_rel_allnum_d15vm06_cell']= dict_out['BAIRONG_APPLY_LOAN_rel_allnum_d15_cell']/dict_out['BAIRONG_APPLY_LOAN_rel_allnum_m06_cell'] if dict_out['BAIRONG_APPLY_LOAN_rel_allnum_m06_cell']>0 else np.nan
    dict_out['BAIRONG_APPLY_LOAN_nbank_cons_orgnum_m03vm06_cell']= dict_out['BAIRONG_APPLY_LOAN_nbank_cons_orgnum_m03_cell']/dict_out['BAIRONG_APPLY_LOAN_nbank_cons_orgnum_m06_cell'] if dict_out['BAIRONG_APPLY_LOAN_nbank_cons_orgnum_m06_cell']>0 else np.nan
    dict_out['BAIRONG_APPLY_LOAN_nbank_else_allnum_d15vm12_cell']= dict_out['BAIRONG_APPLY_LOAN_nbank_else_allnum_d15_cell']/dict_out['BAIRONG_APPLY_LOAN_nbank_else_allnum_m12_cell'] if dict_out['BAIRONG_APPLY_LOAN_nbank_else_allnum_m12_cell']>0 else np.nan
    dict_out['BAIRONG_APPLY_LOAN_nbank_cons_orgnum_d07vm01_cell']= dict_out['BAIRONG_APPLY_LOAN_nbank_cons_orgnum_d07_cell']/dict_out['BAIRONG_APPLY_LOAN_nbank_cons_orgnum_m01_cell'] if dict_out['BAIRONG_APPLY_LOAN_nbank_cons_orgnum_m01_cell']>0 else np.nan
    dict_out['BAIRONG_APPLY_LOAN_nbank_allnum_m03vm06_id']= dict_out['BAIRONG_APPLY_LOAN_nbank_allnum_m03_id']/dict_out['BAIRONG_APPLY_LOAN_nbank_allnum_m06_id'] if dict_out['BAIRONG_APPLY_LOAN_nbank_allnum_m06_id']>0 else np.nan
    dict_out['BAIRONG_APPLY_LOAN_caon_orgnum_d07vd15_id']= dict_out['BAIRONG_APPLY_LOAN_caon_orgnum_d07_id']/dict_out['BAIRONG_APPLY_LOAN_caon_orgnum_d15_id'] if dict_out['BAIRONG_APPLY_LOAN_caon_orgnum_d15_id']>0 else np.nan
    dict_out['BAIRONG_APPLY_LOAN_nbank_mc_orgnum_m12_idsubcell']= dict_out['BAIRONG_APPLY_LOAN_nbank_mc_orgnum_m12_id']-dict_out['BAIRONG_APPLY_LOAN_nbank_mc_orgnum_m12_cell']
    dict_out['BAIRONG_APPLY_LOAN_rel_allnum_m03vm12_cell']= dict_out['BAIRONG_APPLY_LOAN_rel_allnum_m03_cell']/dict_out['BAIRONG_APPLY_LOAN_rel_allnum_m12_cell'] if dict_out['BAIRONG_APPLY_LOAN_rel_allnum_m12_cell']>0 else np.nan
    dict_out['BAIRONG_APPLY_LOAN_nbank_p2p_orgnum_m06vm12_cell']= dict_out['BAIRONG_APPLY_LOAN_nbank_p2p_orgnum_m06_cell']/dict_out['BAIRONG_APPLY_LOAN_nbank_p2p_orgnum_m12_cell'] if dict_out['BAIRONG_APPLY_LOAN_nbank_p2p_orgnum_m12_cell']>0 else np.nan
    dict_out['BAIRONG_APPLY_LOAN_nbank_else_orgnum_d15vm06_cell']= dict_out['BAIRONG_APPLY_LOAN_nbank_else_orgnum_d15_cell']/dict_out['BAIRONG_APPLY_LOAN_nbank_else_orgnum_m06_cell'] if dict_out['BAIRONG_APPLY_LOAN_nbank_else_orgnum_m06_cell']>0 else np.nan
    dict_out['BAIRONG_APPLY_LOAN_nbank_mc_orgnum_m06vm12_cell']= dict_out['BAIRONG_APPLY_LOAN_nbank_mc_orgnum_m06_cell']/dict_out['BAIRONG_APPLY_LOAN_nbank_mc_orgnum_m12_cell'] if dict_out['BAIRONG_APPLY_LOAN_nbank_mc_orgnum_m12_cell']>0 else np.nan
    dict_out['BAIRONG_APPLY_LOAN_bank_week_orgnum_m06vm12_cell']= dict_out['BAIRONG_APPLY_LOAN_bank_week_orgnum_m06_cell']/dict_out['BAIRONG_APPLY_LOAN_bank_week_orgnum_m12_cell'] if dict_out['BAIRONG_APPLY_LOAN_bank_week_orgnum_m12_cell']>0 else np.nan
    dict_out['BAIRONG_APPLY_LOAN_nbank_night_orgnum_m03vm12_id']= dict_out['BAIRONG_APPLY_LOAN_nbank_night_orgnum_m03_id']/dict_out['BAIRONG_APPLY_LOAN_nbank_night_orgnum_m12_id'] if dict_out['BAIRONG_APPLY_LOAN_nbank_night_orgnum_m12_id']>0 else np.nan
    dict_out['BAIRONG_APPLY_LOAN_nbank_nsloan_orgnum_d07vd15_id']= dict_out['BAIRONG_APPLY_LOAN_nbank_nsloan_orgnum_d07_id']/dict_out['BAIRONG_APPLY_LOAN_nbank_nsloan_orgnum_d15_id'] if dict_out['BAIRONG_APPLY_LOAN_nbank_nsloan_orgnum_d15_id']>0 else np.nan
    dict_out['BAIRONG_APPLY_LOAN_nbank_oth_orgnum_m01vm06_cell']= dict_out['BAIRONG_APPLY_LOAN_nbank_oth_orgnum_m01_cell']/dict_out['BAIRONG_APPLY_LOAN_nbank_oth_orgnum_m06_cell'] if dict_out['BAIRONG_APPLY_LOAN_nbank_oth_orgnum_m06_cell']>0 else np.nan
    dict_out['BAIRONG_APPLY_LOAN_caon_orgnum_d07vm12_id']= dict_out['BAIRONG_APPLY_LOAN_caon_orgnum_d07_id']/dict_out['BAIRONG_APPLY_LOAN_caon_orgnum_m12_id'] if dict_out['BAIRONG_APPLY_LOAN_caon_orgnum_m12_id']>0 else np.nan
    dict_out['BAIRONG_APPLY_LOAN_rel_allnum_m01vm12_cell']= dict_out['BAIRONG_APPLY_LOAN_rel_allnum_m01_cell']/dict_out['BAIRONG_APPLY_LOAN_rel_allnum_m12_cell'] if dict_out['BAIRONG_APPLY_LOAN_rel_allnum_m12_cell']>0 else np.nan
    dict_out['BAIRONG_APPLY_LOAN_pdl_orgnum_m03vm06_cell']= dict_out['BAIRONG_APPLY_LOAN_pdl_orgnum_m03_cell']/dict_out['BAIRONG_APPLY_LOAN_pdl_orgnum_m06_cell'] if dict_out['BAIRONG_APPLY_LOAN_pdl_orgnum_m06_cell']>0 else np.nan
    dict_out['BAIRONG_APPLY_LOAN_min_inteday_m06vm12_id']= dict_out['BAIRONG_APPLY_LOAN_min_inteday_m06_id']/dict_out['BAIRONG_APPLY_LOAN_min_inteday_m12_id'] if dict_out['BAIRONG_APPLY_LOAN_min_inteday_m12_id']>0 else np.nan
    dict_out['BAIRONG_APPLY_LOAN_coon_orgnum_m06vm12_cell']= dict_out['BAIRONG_APPLY_LOAN_coon_orgnum_m06_cell']/dict_out['BAIRONG_APPLY_LOAN_coon_orgnum_m12_cell'] if dict_out['BAIRONG_APPLY_LOAN_coon_orgnum_m12_cell']>0 else np.nan
    dict_out['BAIRONG_APPLY_LOAN_nbank_sloan_allnum_m12_idsubcell']= dict_out['BAIRONG_APPLY_LOAN_nbank_sloan_allnum_m12_id']-dict_out['BAIRONG_APPLY_LOAN_nbank_sloan_allnum_m12_cell']
    dict_out['BAIRONG_APPLY_LOAN_bank_allnum_m01vm12_id']= dict_out['BAIRONG_APPLY_LOAN_bank_allnum_m01_id']/dict_out['BAIRONG_APPLY_LOAN_bank_allnum_m12_id'] if dict_out['BAIRONG_APPLY_LOAN_bank_allnum_m12_id']>0 else np.nan
    dict_out['BAIRONG_APPLY_LOAN_caoff_allnum_m03vm12_id']= dict_out['BAIRONG_APPLY_LOAN_caoff_allnum_m03_id']/dict_out['BAIRONG_APPLY_LOAN_caoff_allnum_m12_id'] if dict_out['BAIRONG_APPLY_LOAN_caoff_allnum_m12_id']>0 else np.nan
    dict_out['BAIRONG_APPLY_LOAN_caon_orgnum_d15vm01_id']= dict_out['BAIRONG_APPLY_LOAN_caon_orgnum_d15_id']/dict_out['BAIRONG_APPLY_LOAN_caon_orgnum_m01_id'] if dict_out['BAIRONG_APPLY_LOAN_caon_orgnum_m01_id']>0 else np.nan
    dict_out['BAIRONG_APPLY_LOAN_nbank_cons_allnum_d15vm01_id']= dict_out['BAIRONG_APPLY_LOAN_nbank_cons_allnum_d15_id']/dict_out['BAIRONG_APPLY_LOAN_nbank_cons_allnum_m01_id'] if dict_out['BAIRONG_APPLY_LOAN_nbank_cons_allnum_m01_id']>0 else np.nan
    dict_out['BAIRONG_APPLY_LOAN_nbank_sloan_allnum_m06vm12_cell']= dict_out['BAIRONG_APPLY_LOAN_nbank_sloan_allnum_m06_cell']/dict_out['BAIRONG_APPLY_LOAN_nbank_sloan_allnum_m12_cell'] if dict_out['BAIRONG_APPLY_LOAN_nbank_sloan_allnum_m12_cell']>0 else np.nan
    dict_out['BAIRONG_APPLY_LOAN_nbank_cons_allnum_m01vm06_cell']= dict_out['BAIRONG_APPLY_LOAN_nbank_cons_allnum_m01_cell']/dict_out['BAIRONG_APPLY_LOAN_nbank_cons_allnum_m06_cell'] if dict_out['BAIRONG_APPLY_LOAN_nbank_cons_allnum_m06_cell']>0 else np.nan
    dict_out['BAIRONG_APPLY_LOAN_nbank_mc_orgnum_m03vm06_cell']= dict_out['BAIRONG_APPLY_LOAN_nbank_mc_orgnum_m03_cell']/dict_out['BAIRONG_APPLY_LOAN_nbank_mc_orgnum_m06_cell'] if dict_out['BAIRONG_APPLY_LOAN_nbank_mc_orgnum_m06_cell']>0 else np.nan
    dict_out['BAIRONG_APPLY_LOAN_nbank_sloan_orgnum_m01vm12_cell']= dict_out['BAIRONG_APPLY_LOAN_nbank_sloan_orgnum_m01_cell']/dict_out['BAIRONG_APPLY_LOAN_nbank_sloan_orgnum_m12_cell'] if dict_out['BAIRONG_APPLY_LOAN_nbank_sloan_orgnum_m12_cell']>0 else np.nan
    dict_out['BAIRONG_APPLY_LOAN_nbank_oth_allnum_m03vm06_cell']= dict_out['BAIRONG_APPLY_LOAN_nbank_oth_allnum_m03_cell']/dict_out['BAIRONG_APPLY_LOAN_nbank_oth_allnum_m06_cell'] if dict_out['BAIRONG_APPLY_LOAN_nbank_oth_allnum_m06_cell']>0 else np.nan
    dict_out['BAIRONG_APPLY_LOAN_nbank_week_allnum_m03vm06_cell']= dict_out['BAIRONG_APPLY_LOAN_nbank_week_allnum_m03_cell']/dict_out['BAIRONG_APPLY_LOAN_nbank_week_allnum_m06_cell'] if dict_out['BAIRONG_APPLY_LOAN_nbank_week_allnum_m06_cell']>0 else np.nan
    dict_out['BAIRONG_APPLY_LOAN_caoff_allnum_m06_idsubcell']= dict_out['BAIRONG_APPLY_LOAN_caoff_allnum_m06_id']-dict_out['BAIRONG_APPLY_LOAN_caoff_allnum_m06_cell']
    dict_out['BAIRONG_APPLY_LOAN_nbank_max_inteday_m03vm12_cell']= dict_out['BAIRONG_APPLY_LOAN_nbank_max_inteday_m03_cell']/dict_out['BAIRONG_APPLY_LOAN_nbank_max_inteday_m12_cell'] if dict_out['BAIRONG_APPLY_LOAN_nbank_max_inteday_m12_cell']>0 else np.nan
    dict_out['BAIRONG_APPLY_LOAN_nbank_avg_monnum_m12_idsubcell']= dict_out['BAIRONG_APPLY_LOAN_nbank_avg_monnum_m12_id']-dict_out['BAIRONG_APPLY_LOAN_nbank_avg_monnum_m12_cell']
    dict_out['BAIRONG_APPLY_LOAN_nbank_week_allnum_m12_idsubcell']= dict_out['BAIRONG_APPLY_LOAN_nbank_week_allnum_m12_id']-dict_out['BAIRONG_APPLY_LOAN_nbank_week_allnum_m12_cell']
    dict_out['BAIRONG_APPLY_LOAN_coon_allnum_m01vm12_cell']= dict_out['BAIRONG_APPLY_LOAN_coon_allnum_m01_cell']/dict_out['BAIRONG_APPLY_LOAN_coon_allnum_m12_cell'] if dict_out['BAIRONG_APPLY_LOAN_coon_allnum_m12_cell']>0 else np.nan
    dict_out['BAIRONG_APPLY_LOAN_nbank_else_allnum_d07vm12_id']= dict_out['BAIRONG_APPLY_LOAN_nbank_else_allnum_d07_id']/dict_out['BAIRONG_APPLY_LOAN_nbank_else_allnum_m12_id'] if dict_out['BAIRONG_APPLY_LOAN_nbank_else_allnum_m12_id']>0 else np.nan
    dict_out['BAIRONG_APPLY_LOAN_caon_orgnum_d07vm01_id']= dict_out['BAIRONG_APPLY_LOAN_caon_orgnum_d07_id']/dict_out['BAIRONG_APPLY_LOAN_caon_orgnum_m01_id'] if dict_out['BAIRONG_APPLY_LOAN_caon_orgnum_m01_id']>0 else np.nan
    dict_out['BAIRONG_APPLY_LOAN_nbank_else_allnum_m03vm12_id']= dict_out['BAIRONG_APPLY_LOAN_nbank_else_allnum_m03_id']/dict_out['BAIRONG_APPLY_LOAN_nbank_else_allnum_m12_id'] if dict_out['BAIRONG_APPLY_LOAN_nbank_else_allnum_m12_id']>0 else np.nan
    dict_out['BAIRONG_APPLY_LOAN_oth_allnum_d15vm12_cell']= dict_out['BAIRONG_APPLY_LOAN_oth_allnum_d15_cell']/dict_out['BAIRONG_APPLY_LOAN_oth_allnum_m12_cell'] if dict_out['BAIRONG_APPLY_LOAN_oth_allnum_m12_cell']>0 else np.nan
    dict_out['BAIRONG_APPLY_LOAN_pdl_orgnum_m01vm12_id']= dict_out['BAIRONG_APPLY_LOAN_pdl_orgnum_m01_id']/dict_out['BAIRONG_APPLY_LOAN_pdl_orgnum_m12_id'] if dict_out['BAIRONG_APPLY_LOAN_pdl_orgnum_m12_id']>0 else np.nan
    dict_out['BAIRONG_APPLY_LOAN_nbank_orgnum_d07vm12_cell']= dict_out['BAIRONG_APPLY_LOAN_nbank_orgnum_d07_cell']/dict_out['BAIRONG_APPLY_LOAN_nbank_orgnum_m12_cell'] if dict_out['BAIRONG_APPLY_LOAN_nbank_orgnum_m12_cell']>0 else np.nan
    dict_out['BAIRONG_APPLY_LOAN_caon_orgnum_d07vm06_cell']= dict_out['BAIRONG_APPLY_LOAN_caon_orgnum_d07_cell']/dict_out['BAIRONG_APPLY_LOAN_caon_orgnum_m06_cell'] if dict_out['BAIRONG_APPLY_LOAN_caon_orgnum_m06_cell']>0 else np.nan
    dict_out['BAIRONG_APPLY_LOAN_bank_tot_mons_m03vm12_id']= dict_out['BAIRONG_APPLY_LOAN_bank_tot_mons_m03_id']/dict_out['BAIRONG_APPLY_LOAN_bank_tot_mons_m12_id'] if dict_out['BAIRONG_APPLY_LOAN_bank_tot_mons_m12_id']>0 else np.nan
    dict_out['BAIRONG_APPLY_LOAN_rel_allnum_m01vm03_id']= dict_out['BAIRONG_APPLY_LOAN_rel_allnum_m01_id']/dict_out['BAIRONG_APPLY_LOAN_rel_allnum_m03_id'] if dict_out['BAIRONG_APPLY_LOAN_rel_allnum_m03_id']>0 else np.nan
    dict_out['BAIRONG_APPLY_LOAN_nbank_cf_allnum_m06vm12_cell']= dict_out['BAIRONG_APPLY_LOAN_nbank_cf_allnum_m06_cell']/dict_out['BAIRONG_APPLY_LOAN_nbank_cf_allnum_m12_cell'] if dict_out['BAIRONG_APPLY_LOAN_nbank_cf_allnum_m12_cell']>0 else np.nan
    dict_out['BAIRONG_APPLY_LOAN_nbank_cf_allnum_m01vm06_id']= dict_out['BAIRONG_APPLY_LOAN_nbank_cf_allnum_m01_id']/dict_out['BAIRONG_APPLY_LOAN_nbank_cf_allnum_m06_id'] if dict_out['BAIRONG_APPLY_LOAN_nbank_cf_allnum_m06_id']>0 else np.nan
    dict_out['BAIRONG_APPLY_LOAN_nbank_sloan_allnum_m06vm12_id']= dict_out['BAIRONG_APPLY_LOAN_nbank_sloan_allnum_m06_id']/dict_out['BAIRONG_APPLY_LOAN_nbank_sloan_allnum_m12_id'] if dict_out['BAIRONG_APPLY_LOAN_nbank_sloan_allnum_m12_id']>0 else np.nan
    dict_out['BAIRONG_APPLY_LOAN_cooff_allnum_m01vm03_id']= dict_out['BAIRONG_APPLY_LOAN_cooff_allnum_m01_id']/dict_out['BAIRONG_APPLY_LOAN_cooff_allnum_m03_id'] if dict_out['BAIRONG_APPLY_LOAN_cooff_allnum_m03_id']>0 else np.nan
    dict_out['BAIRONG_APPLY_LOAN_pdl_allnum_d15vm06_id']= dict_out['BAIRONG_APPLY_LOAN_pdl_allnum_d15_id']/dict_out['BAIRONG_APPLY_LOAN_pdl_allnum_m06_id'] if dict_out['BAIRONG_APPLY_LOAN_pdl_allnum_m06_id']>0 else np.nan
    dict_out['BAIRONG_APPLY_LOAN_cooff_orgnum_d07vd15_cell']= dict_out['BAIRONG_APPLY_LOAN_cooff_orgnum_d07_cell']/dict_out['BAIRONG_APPLY_LOAN_cooff_orgnum_d15_cell'] if dict_out['BAIRONG_APPLY_LOAN_cooff_orgnum_d15_cell']>0 else np.nan
    dict_out['BAIRONG_APPLY_LOAN_nbank_ca_allnum_d07vm01_cell']= dict_out['BAIRONG_APPLY_LOAN_nbank_ca_allnum_d07_cell']/dict_out['BAIRONG_APPLY_LOAN_nbank_ca_allnum_m01_cell'] if dict_out['BAIRONG_APPLY_LOAN_nbank_ca_allnum_m01_cell']>0 else np.nan
    dict_out['BAIRONG_APPLY_LOAN_nbank_p2p_allnum_m03vm12_id']= dict_out['BAIRONG_APPLY_LOAN_nbank_p2p_allnum_m03_id']/dict_out['BAIRONG_APPLY_LOAN_nbank_p2p_allnum_m12_id'] if dict_out['BAIRONG_APPLY_LOAN_nbank_p2p_allnum_m12_id']>0 else np.nan
    dict_out['BAIRONG_APPLY_LOAN_nbank_allnum_m03vm12_id']= dict_out['BAIRONG_APPLY_LOAN_nbank_allnum_m03_id']/dict_out['BAIRONG_APPLY_LOAN_nbank_allnum_m12_id'] if dict_out['BAIRONG_APPLY_LOAN_nbank_allnum_m12_id']>0 else np.nan
    dict_out['BAIRONG_APPLY_LOAN_oth_allnum_m01vm12_id']= dict_out['BAIRONG_APPLY_LOAN_oth_allnum_m01_id']/dict_out['BAIRONG_APPLY_LOAN_oth_allnum_m12_id'] if dict_out['BAIRONG_APPLY_LOAN_oth_allnum_m12_id']>0 else np.nan
    dict_out['BAIRONG_APPLY_LOAN_nbank_sloan_allnum_d07vm01_id']= dict_out['BAIRONG_APPLY_LOAN_nbank_sloan_allnum_d07_id']/dict_out['BAIRONG_APPLY_LOAN_nbank_sloan_allnum_m01_id'] if dict_out['BAIRONG_APPLY_LOAN_nbank_sloan_allnum_m01_id']>0 else np.nan
    dict_out['BAIRONG_APPLY_LOAN_nbank_sloan_allnum_m01vm12_id']= dict_out['BAIRONG_APPLY_LOAN_nbank_sloan_allnum_m01_id']/dict_out['BAIRONG_APPLY_LOAN_nbank_sloan_allnum_m12_id'] if dict_out['BAIRONG_APPLY_LOAN_nbank_sloan_allnum_m12_id']>0 else np.nan
    dict_out['BAIRONG_APPLY_LOAN_cooff_allnum_d15vm01_cell']= dict_out['BAIRONG_APPLY_LOAN_cooff_allnum_d15_cell']/dict_out['BAIRONG_APPLY_LOAN_cooff_allnum_m01_cell'] if dict_out['BAIRONG_APPLY_LOAN_cooff_allnum_m01_cell']>0 else np.nan
    dict_out['BAIRONG_APPLY_LOAN_nbank_ca_orgnum_d15vm12_id']= dict_out['BAIRONG_APPLY_LOAN_nbank_ca_orgnum_d15_id']/dict_out['BAIRONG_APPLY_LOAN_nbank_ca_orgnum_m12_id'] if dict_out['BAIRONG_APPLY_LOAN_nbank_ca_orgnum_m12_id']>0 else np.nan
    dict_out['BAIRONG_APPLY_LOAN_avg_monnum_m03vm06_cell']= dict_out['BAIRONG_APPLY_LOAN_avg_monnum_m03_cell']/dict_out['BAIRONG_APPLY_LOAN_avg_monnum_m06_cell'] if dict_out['BAIRONG_APPLY_LOAN_avg_monnum_m06_cell']>0 else np.nan
    dict_out['BAIRONG_APPLY_LOAN_nbank_cons_allnum_m03vm12_cell']= dict_out['BAIRONG_APPLY_LOAN_nbank_cons_allnum_m03_cell']/dict_out['BAIRONG_APPLY_LOAN_nbank_cons_allnum_m12_cell'] if dict_out['BAIRONG_APPLY_LOAN_nbank_cons_allnum_m12_cell']>0 else np.nan
    dict_out['BAIRONG_APPLY_LOAN_nbank_oth_orgnum_d07vm12_id']= dict_out['BAIRONG_APPLY_LOAN_nbank_oth_orgnum_d07_id']/dict_out['BAIRONG_APPLY_LOAN_nbank_oth_orgnum_m12_id'] if dict_out['BAIRONG_APPLY_LOAN_nbank_oth_orgnum_m12_id']>0 else np.nan
    dict_out['BAIRONG_APPLY_LOAN_caoff_allnum_d15vm12_id']= dict_out['BAIRONG_APPLY_LOAN_caoff_allnum_d15_id']/dict_out['BAIRONG_APPLY_LOAN_caoff_allnum_m12_id'] if dict_out['BAIRONG_APPLY_LOAN_caoff_allnum_m12_id']>0 else np.nan
    dict_out['BAIRONG_APPLY_LOAN_bank_orgnum_m06vm12_cell']= dict_out['BAIRONG_APPLY_LOAN_bank_orgnum_m06_cell']/dict_out['BAIRONG_APPLY_LOAN_bank_orgnum_m12_cell'] if dict_out['BAIRONG_APPLY_LOAN_bank_orgnum_m12_cell']>0 else np.nan
    dict_out['BAIRONG_APPLY_LOAN_nbank_cons_allnum_m06_idsubcell']= dict_out['BAIRONG_APPLY_LOAN_nbank_cons_allnum_m06_id']-dict_out['BAIRONG_APPLY_LOAN_nbank_cons_allnum_m06_cell']
    dict_out['BAIRONG_APPLY_LOAN_nbank_cons_orgnum_d15vm01_id']= dict_out['BAIRONG_APPLY_LOAN_nbank_cons_orgnum_d15_id']/dict_out['BAIRONG_APPLY_LOAN_nbank_cons_orgnum_m01_id'] if dict_out['BAIRONG_APPLY_LOAN_nbank_cons_orgnum_m01_id']>0 else np.nan
    dict_out['BAIRONG_APPLY_LOAN_af_orgnum_m01vm03_cell']= dict_out['BAIRONG_APPLY_LOAN_af_orgnum_m01_cell']/dict_out['BAIRONG_APPLY_LOAN_af_orgnum_m03_cell'] if dict_out['BAIRONG_APPLY_LOAN_af_orgnum_m03_cell']>0 else np.nan

    return dict_out




##########################
def huadao_feature_func_online(dict_add,json_dict,appl_time):
    json_dict=lower_to_capital(json_dict)
    data=json_dict['DATA'] if 'DATA' in json_dict.keys() and type(json_dict['DATA'])==dict  else dict()
    data=lower_to_capital(data)
    for key,value in data.items():
        data[key]=np.nan if data[key]=='none' else data[key]
        if len(re.findall('-|t',str(data[key])))>0:
            data[key]=np.nan if len(re.findall('1970-01',str(data[key])))>0 else abs(monthdelta(data[key],appl_time))
            
    dict_add['D001'] = data['D001'] if 'D001' in data.keys() else np.nan 
    dict_add['D002'] = float(data['D002']) if 'D002' in data.keys() else np.nan 
    dict_add['D003'] = float(data['D003']) if 'D003' in data.keys() else np.nan 
    dict_add['D004'] = float(data['D004']) if 'D004' in data.keys() else np.nan 
    dict_add['D005'] = float(data['D005']) if 'D005' in data.keys() else np.nan 
    dict_add['D006'] = float(data['D006']) if 'D006' in data.keys() else np.nan 
    dict_add['D007'] = float(data['D007']) if 'D007' in data.keys() else np.nan 
    dict_add['D008'] = float(data['D008']) if 'D008' in data.keys() else np.nan 
    dict_add['D009'] = float(data['D009']) if 'D009' in data.keys() else np.nan 
    dict_add['D010'] = float(data['D010']) if 'D010' in data.keys() else np.nan 
    dict_add['D011'] = float(data['D011']) if 'D011' in data.keys() else np.nan 
    dict_add['D012'] = float(data['D012']) if 'D012' in data.keys() else np.nan 
    dict_add['D013'] = float(data['D013']) if 'D013' in data.keys() else np.nan 
    dict_add['D014'] = float(data['D014']) if 'D014' in data.keys() else np.nan 
    dict_add['D015'] = float(data['D015']) if 'D015' in data.keys() else np.nan 
    dict_add['D016'] = float(data['D016']) if 'D016' in data.keys() else np.nan 
    dict_add['D017'] = float(data['D017']) if 'D017' in data.keys() else np.nan 
    dict_add['D018'] = float(data['D018']) if 'D018' in data.keys() else np.nan 
    dict_add['D019'] = float(data['D019']) if 'D019' in data.keys() else np.nan 
    dict_add['D020'] = float(data['D020']) if 'D020' in data.keys() else np.nan 
    dict_add['D021'] = float(data['D021']) if 'D021' in data.keys() else np.nan 
    dict_add['D022'] = float(data['D022']) if 'D022' in data.keys() else np.nan 
    dict_add['D023'] = float(data['D023']) if 'D023' in data.keys() else np.nan 
    dict_add['D024'] = float(data['D024']) if 'D024' in data.keys() else np.nan 
    dict_add['D025'] = float(data['D025']) if 'D025' in data.keys() else np.nan 
    dict_add['D026'] = float(data['D026']) if 'D026' in data.keys() else np.nan 
    dict_add['D027'] = float(data['D027']) if 'D027' in data.keys() else np.nan 
    dict_add['D028'] = float(data['D028']) if 'D028' in data.keys() else np.nan 
    dict_add['D029'] = float(data['D029']) if 'D029' in data.keys() else np.nan 
    dict_add['D030'] = float(data['D030']) if 'D030' in data.keys() else np.nan 
    dict_add['D031'] = float(data['D031']) if 'D031' in data.keys() else np.nan 
    dict_add['D032'] = float(data['D032']) if 'D032' in data.keys() else np.nan 
    dict_add['D033'] = float(data['D033']) if 'D033' in data.keys() else np.nan 
    dict_add['D034'] = float(data['D034']) if 'D034' in data.keys() else np.nan 
    dict_add['D035'] = float(data['D035']) if 'D035' in data.keys() else np.nan 
    dict_add['D036'] = float(data['D036']) if 'D036' in data.keys() else np.nan 
    dict_add['D037'] = float(data['D037']) if 'D037' in data.keys() else np.nan 
    dict_add['D038'] = float(data['D038']) if 'D038' in data.keys() else np.nan 
    dict_add['D039'] = float(data['D039']) if 'D039' in data.keys() else np.nan 
    dict_add['D040'] = float(data['D040']) if 'D040' in data.keys() else np.nan 
    dict_add['D041'] = float(data['D041']) if 'D041' in data.keys() else np.nan 
    dict_add['D042'] = float(data['D042']) if 'D042' in data.keys() else np.nan 
    dict_add['D043'] = float(data['D043']) if 'D043' in data.keys() else np.nan 
    dict_add['D044'] = float(data['D044']) if 'D044' in data.keys() else np.nan 
    dict_add['D045'] = float(data['D045']) if 'D045' in data.keys() else np.nan 
    dict_add['D046'] = float(data['D046']) if 'D046' in data.keys() else np.nan 
    dict_add['D047'] = float(data['D047']) if 'D047' in data.keys() else np.nan 
    dict_add['D048'] = float(data['D048']) if 'D048' in data.keys() else np.nan 
    dict_add['D049'] = float(data['D049']) if 'D049' in data.keys() else np.nan 
    dict_add['D050'] = data['D050'] if 'D050' in data.keys() else np.nan 
    dict_add['D051'] = data['D051'] if 'D051' in data.keys() else np.nan 
    dict_add['D052'] = float(data['D052']) if 'D052' in data.keys() else np.nan 
    dict_add['D053'] = float(data['D053']) if 'D053' in data.keys() else np.nan 
    dict_add['D054'] = float(data['D054']) if 'D054' in data.keys() else np.nan 
    dict_add['D055'] = float(data['D055']) if 'D055' in data.keys() else np.nan 
    dict_add['D056'] = float(data['D056']) if 'D056' in data.keys() else np.nan 
    dict_add['D057'] = float(data['D057']) if 'D057' in data.keys() else np.nan 
    dict_add['D058'] = float(data['D058']) if 'D058' in data.keys() else np.nan 
    dict_add['D059'] = float(data['D059']) if 'D059' in data.keys() else np.nan 
    dict_add['D060'] = float(data['D060']) if 'D060' in data.keys() else np.nan 
    dict_add['D061'] = float(data['D061']) if 'D061' in data.keys() else np.nan 
    dict_add['D062'] = float(data['D062']) if 'D062' in data.keys() else np.nan 
    dict_add['D063'] = float(data['D063']) if 'D063' in data.keys() else np.nan 
    dict_add['D064'] = float(data['D064']) if 'D064' in data.keys() else np.nan 
    dict_add['D065'] = float(data['D065']) if 'D065' in data.keys() else np.nan 
    dict_add['D066'] = float(data['D066']) if 'D066' in data.keys() else np.nan 
    dict_add['D067'] = float(data['D067']) if 'D067' in data.keys() else np.nan 
    dict_add['D068'] = float(data['D068']) if 'D068' in data.keys() else np.nan 
    dict_add['D069'] = float(data['D069']) if 'D069' in data.keys() else np.nan 
    dict_add['D070'] = float(data['D070']) if 'D070' in data.keys() else np.nan 
    dict_add['D071'] = float(data['D071']) if 'D071' in data.keys() else np.nan 
    dict_add['D072'] = float(data['D072']) if 'D072' in data.keys() else np.nan 
    dict_add['D073'] = float(data['D073']) if 'D073' in data.keys() else np.nan 
    dict_add['D074'] = float(data['D074']) if 'D074' in data.keys() else np.nan 
    dict_add['D075'] = float(data['D075']) if 'D075' in data.keys() else np.nan 
    dict_add['D076'] = float(data['D076']) if 'D076' in data.keys() else np.nan 
    dict_add['D077'] = float(data['D077']) if 'D077' in data.keys() else np.nan 
    dict_add['D078'] = float(data['D078']) if 'D078' in data.keys() else np.nan 
    dict_add['D079'] = float(data['D079']) if 'D079' in data.keys() else np.nan 
    dict_add['D080'] = float(data['D080']) if 'D080' in data.keys() else np.nan 
    dict_add['D081'] = float(data['D081']) if 'D081' in data.keys() else np.nan 
    dict_add['D082'] = float(data['D082']) if 'D082' in data.keys() else np.nan 
    dict_add['D083'] = float(data['D083']) if 'D083' in data.keys() else np.nan 
    dict_add['D084'] = float(data['D084']) if 'D084' in data.keys() else np.nan 
    dict_add['D085'] = float(data['D085']) if 'D085' in data.keys() else np.nan 
    dict_add['D086'] = float(data['D086']) if 'D086' in data.keys() else np.nan 
    dict_add['D087'] = float(data['D087']) if 'D087' in data.keys() else np.nan 
    dict_add['D088'] = float(data['D088']) if 'D088' in data.keys() else np.nan 
    dict_add['D089'] = float(data['D089']) if 'D089' in data.keys() else np.nan 
    dict_add['D090'] = float(data['D090']) if 'D090' in data.keys() else np.nan 
    dict_add['D091'] = float(data['D091']) if 'D091' in data.keys() else np.nan 
    dict_add['D092'] = float(data['D092']) if 'D092' in data.keys() else np.nan 
    dict_add['D093'] = float(data['D093']) if 'D093' in data.keys() else np.nan 
    dict_add['D094'] = float(data['D094']) if 'D094' in data.keys() else np.nan 
    dict_add['D095'] = float(data['D095']) if 'D095' in data.keys() else np.nan 
    dict_add['D096'] = float(data['D096']) if 'D096' in data.keys() else np.nan 
    dict_add['D097'] = float(data['D097']) if 'D097' in data.keys() else np.nan 
    dict_add['D098'] = float(data['D098']) if 'D098' in data.keys() else np.nan 
    dict_add['D099'] = float(data['D099']) if 'D099' in data.keys() else np.nan 
    dict_add['D100'] = float(data['D100']) if 'D100' in data.keys() else np.nan 
    dict_add['D101'] = float(data['D101']) if 'D101' in data.keys() else np.nan 
    dict_add['D102'] = float(data['D102']) if 'D102' in data.keys() else np.nan 
    dict_add['D103'] = float(data['D103']) if 'D103' in data.keys() else np.nan 
    dict_add['D104'] = data['D104'] if 'D104' in data.keys() else np.nan 
    dict_add['D105'] = data['D105'] if 'D105' in data.keys() else np.nan 
    dict_add['D106'] = float(data['D106']) if 'D106' in data.keys() else np.nan 
    dict_add['D107'] = float(data['D107']) if 'D107' in data.keys() else np.nan 
    dict_add['D108'] = float(data['D108']) if 'D108' in data.keys() else np.nan 
    dict_add['D109'] = float(data['D109']) if 'D109' in data.keys() else np.nan 
    dict_add['D110'] = float(data['D110']) if 'D110' in data.keys() else np.nan 
    dict_add['D111'] = float(data['D111']) if 'D111' in data.keys() else np.nan 
    dict_add['D112'] = float(data['D112']) if 'D112' in data.keys() else np.nan 
    dict_add['D113'] = float(data['D113']) if 'D113' in data.keys() else np.nan 
    dict_add['D114'] = float(data['D114']) if 'D114' in data.keys() else np.nan 
    dict_add['D115'] = float(data['D115']) if 'D115' in data.keys() else np.nan 
    dict_add['D116'] = float(data['D116']) if 'D116' in data.keys() else np.nan 
    dict_add['D117'] = float(data['D117']) if 'D117' in data.keys() else np.nan 
    dict_add['D118'] = float(data['D118']) if 'D118' in data.keys() else np.nan 
    dict_add['D119'] = float(data['D119']) if 'D119' in data.keys() else np.nan 
    dict_add['D120'] = float(data['D120']) if 'D120' in data.keys() else np.nan 
    dict_add['D121'] = float(data['D121']) if 'D121' in data.keys() else np.nan 
    dict_add['D122'] = float(data['D122']) if 'D122' in data.keys() else np.nan 
    dict_add['D123'] = float(data['D123']) if 'D123' in data.keys() else np.nan 
    dict_add['D124'] = float(data['D124']) if 'D124' in data.keys() else np.nan 
    dict_add['D125'] = float(data['D125']) if 'D125' in data.keys() else np.nan 
    dict_add['D126'] = float(data['D126']) if 'D126' in data.keys() else np.nan 
    dict_add['D127'] = float(data['D127']) if 'D127' in data.keys() else np.nan 
    dict_add['D128'] = float(data['D128']) if 'D128' in data.keys() else np.nan 
    dict_add['D129'] = float(data['D129']) if 'D129' in data.keys() else np.nan 
    dict_add['D130'] = float(data['D130']) if 'D130' in data.keys() else np.nan 
    dict_add['D131'] = float(data['D131']) if 'D131' in data.keys() else np.nan 
    dict_add['D132'] = float(data['D132']) if 'D132' in data.keys() else np.nan 
    dict_add['D133'] = float(data['D133']) if 'D133' in data.keys() else np.nan 
    dict_add['D134'] = float(data['D134']) if 'D134' in data.keys() else np.nan 
    dict_add['D135'] = float(data['D135']) if 'D135' in data.keys() else np.nan 
    dict_add['D136'] = float(data['D136']) if 'D136' in data.keys() else np.nan 
    dict_add['D137'] = float(data['D137']) if 'D137' in data.keys() else np.nan 
    dict_add['D138'] = float(data['D138']) if 'D138' in data.keys() else np.nan 
    dict_add['D139'] = float(data['D139']) if 'D139' in data.keys() else np.nan 
    dict_add['D140'] = float(data['D140']) if 'D140' in data.keys() else np.nan 
    dict_add['D141'] = float(data['D141']) if 'D141' in data.keys() else np.nan 
    dict_add['D142'] = float(data['D142']) if 'D142' in data.keys() else np.nan 
    dict_add['D143'] = float(data['D143']) if 'D143' in data.keys() else np.nan 
    dict_add['D144'] = float(data['D144']) if 'D144' in data.keys() else np.nan 
    dict_add['D145'] = float(data['D145']) if 'D145' in data.keys() else np.nan 
    dict_add['D146'] = float(data['D146']) if 'D146' in data.keys() else np.nan 
    dict_add['D147'] = float(data['D147']) if 'D147' in data.keys() else np.nan 
    dict_add['D148'] = float(data['D148']) if 'D148' in data.keys() else np.nan 
    dict_add['D149'] = float(data['D149']) if 'D149' in data.keys() else np.nan 
    dict_add['D150'] = float(data['D150']) if 'D150' in data.keys() else np.nan 
    dict_add['D151'] = float(data['D151']) if 'D151' in data.keys() else np.nan 
    dict_add['D152'] = float(data['D152']) if 'D152' in data.keys() else np.nan 
    dict_add['D153'] = float(data['D153']) if 'D153' in data.keys() else np.nan 
    dict_add['D154'] = data['D154'] if 'D154' in data.keys() else np.nan 
    dict_add['D155'] = data['D155'] if 'D155' in data.keys() else np.nan 
    dict_add['D156'] = float(data['D156']) if 'D156' in data.keys() else np.nan 
    dict_add['D157'] = float(data['D157']) if 'D157' in data.keys() else np.nan 
    dict_add['D158'] = float(data['D158']) if 'D158' in data.keys() else np.nan 
    dict_add['D159'] = float(data['D159']) if 'D159' in data.keys() else np.nan 
    dict_add['D160'] = data['D160'] if 'D160' in data.keys() else np.nan 
    dict_add['D161'] = float(data['D161']) if 'D161' in data.keys() else np.nan 
    dict_add['D162'] = float(data['D162']) if 'D162' in data.keys() else np.nan 
    dict_add['D163'] = float(data['D163']) if 'D163' in data.keys() else np.nan 
    dict_add['D164'] = float(data['D164']) if 'D164' in data.keys() else np.nan 
    dict_add['D165'] = float(data['D165']) if 'D165' in data.keys() else np.nan 
    dict_add['D166'] = float(data['D166']) if 'D166' in data.keys() else np.nan 
    dict_add['D167'] = float(data['D167']) if 'D167' in data.keys() else np.nan 
    dict_add['D168'] = float(data['D168']) if 'D168' in data.keys() else np.nan 
    dict_add['D169'] = float(data['D169']) if 'D169' in data.keys() else np.nan 
    dict_add['D170'] = float(data['D170']) if 'D170' in data.keys() else np.nan 
    dict_add['D171'] = float(data['D171']) if 'D171' in data.keys() else np.nan 
    dict_add['D172'] = float(data['D172']) if 'D172' in data.keys() else np.nan 
    dict_add['D173'] = float(data['D173']) if 'D173' in data.keys() else np.nan 
    dict_add['D174'] = float(data['D174']) if 'D174' in data.keys() else np.nan 
    dict_add['D175'] = float(data['D175']) if 'D175' in data.keys() else np.nan 
    dict_add['D176'] = float(data['D176']) if 'D176' in data.keys() else np.nan 
#     dict_add['D177'] = float(data['D177']) if 'D177' in data.keys() else np.nan 
#     dict_add['D178'] = float(data['D178']) if 'D178' in data.keys() else np.nan 
#     dict_add['D179'] = float(data['D179']) if 'D179' in data.keys() else np.nan 
#     dict_add['D180'] = float(data['D180']) if 'D180' in data.keys() else np.nan 
#     dict_add['D181'] = float(data['D181']) if 'D181' in data.keys() else np.nan 
#     dict_add['D182'] = float(data['D182']) if 'D182' in data.keys() else np.nan 
#     dict_add['D183'] = float(data['D183']) if 'D183' in data.keys() else np.nan 
#     dict_add['D184'] = float(data['D184']) if 'D184' in data.keys() else np.nan 
#     dict_add['D185'] = float(data['D185']) if 'D185' in data.keys() else np.nan 
#     dict_add['D186'] = float(data['D186']) if 'D186' in data.keys() else np.nan 
#     dict_add['D187'] = float(data['D187']) if 'D187' in data.keys() else np.nan 
#     dict_add['D188'] = float(data['D188']) if 'D188' in data.keys() else np.nan 
#     dict_add['D189'] = float(data['D189']) if 'D189' in data.keys() else np.nan 
#     dict_add['D190'] = float(data['D190']) if 'D190' in data.keys() else np.nan 
#     dict_add['D191'] = float(data['D191']) if 'D191' in data.keys() else np.nan 
#     dict_add['D192'] = float(data['D192']) if 'D192' in data.keys() else np.nan 
#     dict_add['D193'] = float(data['D193']) if 'D193' in data.keys() else np.nan 
#     dict_add['D194'] = float(data['D194']) if 'D194' in data.keys() else np.nan 
#     dict_add['D195'] = float(data['D195']) if 'D195' in data.keys() else np.nan 
#     dict_add['D196'] = float(data['D196']) if 'D196' in data.keys() else np.nan 
#     dict_add['D197'] = float(data['D197']) if 'D197' in data.keys() else np.nan 
#     dict_add['D198'] = float(data['D198']) if 'D198' in data.keys() else np.nan 
#     dict_add['D199'] = float(data['D199']) if 'D199' in data.keys() else np.nan 
#     dict_add['D200'] = float(data['D200']) if 'D200' in data.keys() else np.nan 
#     dict_add['D201'] = float(data['D201']) if 'D201' in data.keys() else np.nan 
#     dict_add['D202'] = float(data['D202']) if 'D202' in data.keys() else np.nan 
#     dict_add['D203'] = float(data['D203']) if 'D203' in data.keys() else np.nan 
#     dict_add['D204'] = float(data['D204']) if 'D204' in data.keys() else np.nan 
#     dict_add['D205'] = float(data['D205']) if 'D205' in data.keys() else np.nan 
#     dict_add['D206'] = float(data['D206']) if 'D206' in data.keys() else np.nan 
#     dict_add['D207'] = float(data['D207']) if 'D207' in data.keys() else np.nan 
#     dict_add['D208'] = float(data['D208']) if 'D208' in data.keys() else np.nan 
#     dict_add['D209'] = float(data['D209']) if 'D209' in data.keys() else np.nan 
#     dict_add['D210'] = float(data['D210']) if 'D210' in data.keys() else np.nan 
#     dict_add['D211'] = float(data['D211']) if 'D211' in data.keys() else np.nan 
#     dict_add['D212'] = float(data['D212']) if 'D212' in data.keys() else np.nan 
#     dict_add['D213'] = float(data['D213']) if 'D213' in data.keys() else np.nan 
#     dict_add['D214'] = float(data['D214']) if 'D214' in data.keys() else np.nan 
#     dict_add['D215'] = float(data['D215']) if 'D215' in data.keys() else np.nan 
#     dict_add['D216'] = float(data['D216']) if 'D216' in data.keys() else np.nan 
#     dict_add['D217'] = float(data['D217']) if 'D217' in data.keys() else np.nan 
#     dict_add['D218'] = float(data['D218']) if 'D218' in data.keys() else np.nan 
#     dict_add['D219'] = float(data['D219']) if 'D219' in data.keys() else np.nan 
#     dict_add['D220'] = float(data['D220']) if 'D220' in data.keys() else np.nan 
#     dict_add['D221'] = float(data['D221']) if 'D221' in data.keys() else np.nan 
#     dict_add['D222'] = float(data['D222']) if 'D222' in data.keys() else np.nan 
    dict_add['D223'] = float(data['D223']) if 'D223' in data.keys() else np.nan 
    dict_add['D224'] = float(data['D224']) if 'D224' in data.keys() else np.nan 
    dict_add['D225'] = float(data['D225']) if 'D225' in data.keys() else np.nan 
    dict_add['D226'] = float(data['D226']) if 'D226' in data.keys() else np.nan 
    dict_add['D227'] = float(data['D227']) if 'D227' in data.keys() else np.nan 
    dict_add['D228'] = float(data['D228']) if 'D228' in data.keys() else np.nan 
    dict_add['D229'] = float(data['D229']) if 'D229' in data.keys() else np.nan 
    dict_add['D230'] = float(data['D230']) if 'D230' in data.keys() else np.nan 
    dict_add['D231'] = float(data['D231']) if 'D231' in data.keys() else np.nan 
    dict_add['D232'] = float(data['D232']) if 'D232' in data.keys() else np.nan 
    dict_add['D233'] = float(data['D233']) if 'D233' in data.keys() else np.nan 
    dict_add['D234'] = float(data['D234']) if 'D234' in data.keys() else np.nan 
    dict_add['D235'] = float(data['D235']) if 'D235' in data.keys() else np.nan 
    dict_add['D236'] = float(data['D236']) if 'D236' in data.keys() else np.nan 
    dict_add['D237'] = float(data['D237']) if 'D237' in data.keys() else np.nan 
    dict_add['D238'] = float(data['D238']) if 'D238' in data.keys() else np.nan 
    dict_add['D239'] = float(data['D239']) if 'D239' in data.keys() else np.nan 
    dict_add['D240'] = float(data['D240']) if 'D240' in data.keys() else np.nan 
    dict_add['D241'] = float(data['D241']) if 'D241' in data.keys() else np.nan 
    dict_add['D242'] = float(data['D242']) if 'D242' in data.keys() else np.nan 
    dict_add['D243'] = float(data['D243']) if 'D243' in data.keys() else np.nan 
    dict_add['D244'] = float(data['D244']) if 'D244' in data.keys() else np.nan 
    dict_add['D245'] = float(data['D245']) if 'D245' in data.keys() else np.nan 
    dict_add['D246'] = float(data['D246']) if 'D246' in data.keys() else np.nan 
    dict_add['D247'] = float(data['D247']) if 'D247' in data.keys() else np.nan 
    dict_add['D248'] = float(data['D248']) if 'D248' in data.keys() else np.nan 
    dict_add['D249'] = float(data['D249']) if 'D249' in data.keys() else np.nan 
    dict_add['D250'] = float(data['D250']) if 'D250' in data.keys() else np.nan 
    dict_add['D251'] = float(data['D251']) if 'D251' in data.keys() else np.nan 
    dict_add['D252'] = float(data['D252']) if 'D252' in data.keys() else np.nan 
    dict_add['D253'] = float(data['D253']) if 'D253' in data.keys() else np.nan 
    dict_add['D254'] = float(data['D254']) if 'D254' in data.keys() else np.nan 
    dict_add['D255'] = float(data['D255']) if 'D255' in data.keys() else np.nan 
    dict_add['D256'] = float(data['D256']) if 'D256' in data.keys() else np.nan 
    dict_add['D257'] = float(data['D257']) if 'D257' in data.keys() else np.nan 
    dict_add['D258'] = float(data['D258']) if 'D258' in data.keys() else np.nan 
    dict_add['D259'] = float(data['D259']) if 'D259' in data.keys() else np.nan 
    dict_add['D260'] = float(data['D260']) if 'D260' in data.keys() else np.nan 
    dict_add['D261'] = float(data['D261']) if 'D261' in data.keys() else np.nan 
    dict_add['D262'] = float(data['D262']) if 'D262' in data.keys() else np.nan 
    dict_add['D263'] = float(data['D263']) if 'D263' in data.keys() else np.nan 
    dict_add['D264'] = float(data['D264']) if 'D264' in data.keys() else np.nan 
    dict_add['D265'] = float(data['D265']) if 'D265' in data.keys() else np.nan 
    dict_add['D266'] = float(data['D266']) if 'D266' in data.keys() else np.nan 
    dict_add['D267'] = float(data['D267']) if 'D267' in data.keys() else np.nan 
    dict_add['D268'] = float(data['D268']) if 'D268' in data.keys() else np.nan 
    dict_add['D269'] = float(data['D269']) if 'D269' in data.keys() else np.nan 
    dict_add['D270'] = float(data['D270']) if 'D270' in data.keys() else np.nan 
    dict_add['D271'] = float(data['D271']) if 'D271' in data.keys() else np.nan 
    dict_add['D272'] = float(data['D272']) if 'D272' in data.keys() else np.nan 
    dict_add['D273'] = float(data['D273']) if 'D273' in data.keys() else np.nan 
    dict_add['D274'] = float(data['D274']) if 'D274' in data.keys() else np.nan 
    dict_add['D275'] = float(data['D275']) if 'D275' in data.keys() else np.nan 
    dict_add['D276'] = float(data['D276']) if 'D276' in data.keys() else np.nan 
    dict_add['D277'] = float(data['D277']) if 'D277' in data.keys() else np.nan 
    dict_add['D278'] = float(data['D278']) if 'D278' in data.keys() else np.nan 
    dict_add['D279'] = float(data['D279']) if 'D279' in data.keys() else np.nan 
    dict_add['D280'] = float(data['D280']) if 'D280' in data.keys() else np.nan 
    dict_add['D281'] = float(data['D281']) if 'D281' in data.keys() else np.nan 
    dict_add['D282'] = float(data['D282']) if 'D282' in data.keys() else np.nan 
    dict_add['D283'] = float(data['D283']) if 'D283' in data.keys() else np.nan 
    dict_add['D284'] = float(data['D284']) if 'D284' in data.keys() else np.nan 
    dict_add['D285'] = float(data['D285']) if 'D285' in data.keys() else np.nan 
    dict_add['D286'] = float(data['D286']) if 'D286' in data.keys() else np.nan 
    dict_add['D287'] = float(data['D287']) if 'D287' in data.keys() else np.nan 
    dict_add['D288'] = float(data['D288']) if 'D288' in data.keys() else np.nan 
    dict_add['D289'] = float(data['D289']) if 'D289' in data.keys() else np.nan 
    dict_add['D290'] = float(data['D290']) if 'D290' in data.keys() else np.nan 
    dict_add['D291'] = float(data['D291']) if 'D291' in data.keys() else np.nan 
    dict_add['D292'] = float(data['D292']) if 'D292' in data.keys() else np.nan 
    dict_add['D293'] = float(data['D293']) if 'D293' in data.keys() else np.nan 
    dict_add['D294'] = float(data['D294']) if 'D294' in data.keys() else np.nan 
    dict_add['D295'] = float(data['D295']) if 'D295' in data.keys() else np.nan 
    dict_add['D296'] = float(data['D296']) if 'D296' in data.keys() else np.nan 
    dict_add['D297'] = float(data['D297']) if 'D297' in data.keys() else np.nan 
    dict_add['D298'] = float(data['D298']) if 'D298' in data.keys() else np.nan 
    dict_add['D299'] = float(data['D299']) if 'D299' in data.keys() else np.nan 
    dict_add['D300'] = float(data['D300']) if 'D300' in data.keys() else np.nan 
    dict_add['D301'] = float(data['D301']) if 'D301' in data.keys() else np.nan 
    dict_add['D302'] = float(data['D302']) if 'D302' in data.keys() else np.nan 
    dict_add['D303'] = float(data['D303']) if 'D303' in data.keys() else np.nan 
    dict_add['D304'] = float(data['D304']) if 'D304' in data.keys() else np.nan 
    dict_add['D305'] = float(data['D305']) if 'D305' in data.keys() else np.nan 
    dict_add['D306'] = float(data['D306']) if 'D306' in data.keys() else np.nan 
    dict_add['D307'] = float(data['D307']) if 'D307' in data.keys() else np.nan 
    dict_add['D308'] = float(data['D308']) if 'D308' in data.keys() else np.nan 
    dict_add['D309'] = float(data['D309']) if 'D309' in data.keys() else np.nan 
    dict_add['D310'] = float(data['D310']) if 'D310' in data.keys() else np.nan 
    dict_add['D311'] = float(data['D311']) if 'D311' in data.keys() else np.nan 
    dict_add['D312'] = float(data['D312']) if 'D312' in data.keys() else np.nan 
    dict_add['D313'] = float(data['D313']) if 'D313' in data.keys() else np.nan 
    dict_add['D314'] = float(data['D314']) if 'D314' in data.keys() else np.nan 
    dict_add['D315'] = float(data['D315']) if 'D315' in data.keys() else np.nan 
    dict_add['D316'] = float(data['D316']) if 'D316' in data.keys() else np.nan 
    dict_add['D317'] = float(data['D317']) if 'D317' in data.keys() else np.nan 
    dict_add['D318'] = float(data['D318']) if 'D318' in data.keys() else np.nan 
    dict_add['D319'] = float(data['D319']) if 'D319' in data.keys() else np.nan 
    dict_add['D320'] = float(data['D320']) if 'D320' in data.keys() else np.nan 
    dict_add['D321'] = float(data['D321']) if 'D321' in data.keys() else np.nan 
    dict_add['D322'] = float(data['D322']) if 'D322' in data.keys() else np.nan 
    dict_add['D323'] = float(data['D323']) if 'D323' in data.keys() else np.nan 
    dict_add['D324'] = float(data['D324']) if 'D324' in data.keys() else np.nan 
    dict_add['D325'] = float(data['D325']) if 'D325' in data.keys() else np.nan 
    dict_add['D326'] = float(data['D326']) if 'D326' in data.keys() else np.nan 
    dict_add['D327'] = float(data['D327']) if 'D327' in data.keys() else np.nan 
    dict_add['D328'] = float(data['D328']) if 'D328' in data.keys() else np.nan 
    dict_add['D329'] = float(data['D329']) if 'D329' in data.keys() else np.nan 
    dict_add['D330'] = float(data['D330']) if 'D330' in data.keys() else np.nan 
    dict_add['D331'] = float(data['D331']) if 'D331' in data.keys() else np.nan 
    dict_add['D332'] = float(data['D332']) if 'D332' in data.keys() else np.nan 
    dict_add['D333'] = float(data['D333']) if 'D333' in data.keys() else np.nan 
    dict_add['D334'] = float(data['D334']) if 'D334' in data.keys() else np.nan 
    dict_add['D335'] = float(data['D335']) if 'D335' in data.keys() else np.nan 
    dict_add['D336'] = float(data['D336']) if 'D336' in data.keys() else np.nan 
    dict_add['D337'] = float(data['D337']) if 'D337' in data.keys() else np.nan 
    dict_add['D338'] = float(data['D338']) if 'D338' in data.keys() else np.nan 
    dict_add['D339'] = float(data['D339']) if 'D339' in data.keys() else np.nan 
    dict_add['D340'] = float(data['D340']) if 'D340' in data.keys() else np.nan 
    dict_add['D341'] = float(data['D341']) if 'D341' in data.keys() else np.nan 
    dict_add['D342'] = float(data['D342']) if 'D342' in data.keys() else np.nan 
    dict_add['D343'] = float(data['D343']) if 'D343' in data.keys() else np.nan 
    dict_add['D344'] = float(data['D344']) if 'D344' in data.keys() else np.nan 
    dict_add['D345'] = float(data['D345']) if 'D345' in data.keys() else np.nan 
    dict_add['D346'] = float(data['D346']) if 'D346' in data.keys() else np.nan 
    dict_add['D347'] = float(data['D347']) if 'D347' in data.keys() else np.nan 
    dict_add['D348'] = float(data['D348']) if 'D348' in data.keys() else np.nan 
    dict_add['D349'] = float(data['D349']) if 'D349' in data.keys() else np.nan 
    dict_add['D350'] = float(data['D350']) if 'D350' in data.keys() else np.nan 
    dict_add['D351'] = float(data['D351']) if 'D351' in data.keys() else np.nan 
    dict_add['D352'] = float(data['D352']) if 'D352' in data.keys() else np.nan 
    dict_add['D353'] = float(data['D353']) if 'D353' in data.keys() else np.nan 
    dict_add['D354'] = float(data['D354']) if 'D354' in data.keys() else np.nan 
    dict_add['D355'] = float(data['D355']) if 'D355' in data.keys() else np.nan 
    dict_add['D356'] = float(data['D356']) if 'D356' in data.keys() else np.nan 
    dict_add['D357'] = float(data['D357']) if 'D357' in data.keys() else np.nan 
    dict_add['D358'] = float(data['D358']) if 'D358' in data.keys() else np.nan 
    dict_add['D359'] = float(data['D359']) if 'D359' in data.keys() else np.nan 
    dict_add['D360'] = float(data['D360']) if 'D360' in data.keys() else np.nan 
    dict_add['D361'] = float(data['D361']) if 'D361' in data.keys() else np.nan 
    dict_add['D362'] = float(data['D362']) if 'D362' in data.keys() else np.nan 
    dict_add['D363'] = float(data['D363']) if 'D363' in data.keys() else np.nan 
    dict_add['D364'] = float(data['D364']) if 'D364' in data.keys() else np.nan 
    dict_add['D365'] = float(data['D365']) if 'D365' in data.keys() else np.nan 
    dict_add['D366'] = float(data['D366']) if 'D366' in data.keys() else np.nan 
    dict_add['D367'] = float(data['D367']) if 'D367' in data.keys() else np.nan 
    dict_add['D368'] = float(data['D368']) if 'D368' in data.keys() else np.nan 
    dict_add['D369'] = float(data['D369']) if 'D369' in data.keys() else np.nan 
    dict_add['D370'] = float(data['D370']) if 'D370' in data.keys() else np.nan 
    dict_add['D371'] = float(data['D371']) if 'D371' in data.keys() else np.nan 
    dict_add['D372'] = float(data['D372']) if 'D372' in data.keys() else np.nan 
    dict_add['D373'] = float(data['D373']) if 'D373' in data.keys() else np.nan 
    dict_add['D374'] = float(data['D374']) if 'D374' in data.keys() else np.nan 
    dict_add['D375'] = float(data['D375']) if 'D375' in data.keys() else np.nan 
    dict_add['D376'] = float(data['D376']) if 'D376' in data.keys() else np.nan 
    dict_add['D377'] = float(data['D377']) if 'D377' in data.keys() else np.nan 
    dict_add['D378'] = float(data['D378']) if 'D378' in data.keys() else np.nan 
    dict_add['D379'] = float(data['D379']) if 'D379' in data.keys() else np.nan 
    dict_add['D380'] = float(data['D380']) if 'D380' in data.keys() else np.nan 
    dict_add['D381'] = float(data['D381']) if 'D381' in data.keys() else np.nan 
    dict_add['D382'] = float(data['D382']) if 'D382' in data.keys() else np.nan 
    dict_add['D383'] = float(data['D383']) if 'D383' in data.keys() else np.nan 
    dict_add['D384'] = float(data['D384']) if 'D384' in data.keys() else np.nan 
    dict_add['D385'] = float(data['D385']) if 'D385' in data.keys() else np.nan 
    dict_add['D386'] = float(data['D386']) if 'D386' in data.keys() else np.nan 
    dict_add['D387'] = float(data['D387']) if 'D387' in data.keys() else np.nan 
    dict_add['D388'] = float(data['D388']) if 'D388' in data.keys() else np.nan 
    dict_add['D389'] = float(data['D389']) if 'D389' in data.keys() else np.nan 
    dict_add['D390'] = float(data['D390']) if 'D390' in data.keys() else np.nan 
    dict_add['D391'] = float(data['D391']) if 'D391' in data.keys() else np.nan 
    dict_add['D392'] = float(data['D392']) if 'D392' in data.keys() else np.nan 
    dict_add['D393'] = float(data['D393']) if 'D393' in data.keys() else np.nan 
    dict_add['D394'] = float(data['D394']) if 'D394' in data.keys() else np.nan 
    dict_add['D395'] = float(data['D395']) if 'D395' in data.keys() else np.nan 
    dict_add['D396'] = float(data['D396']) if 'D396' in data.keys() else np.nan 
    dict_add['D397'] = float(data['D397']) if 'D397' in data.keys() else np.nan 
    dict_add['D398'] = float(data['D398']) if 'D398' in data.keys() else np.nan 
    dict_add['D399'] = float(data['D399']) if 'D399' in data.keys() else np.nan 
    dict_add['D400'] = float(data['D400']) if 'D400' in data.keys() else np.nan 
    dict_add['D401'] = float(data['D401']) if 'D401' in data.keys() else np.nan 
#     dict_add['D402'] = float(data['D402']) if 'D402' in data.keys() else np.nan 
#     dict_add['D403'] = float(data['D403']) if 'D403' in data.keys() else np.nan 
#     dict_add['D404'] = float(data['D404']) if 'D404' in data.keys() else np.nan 
#     dict_add['D405'] = float(data['D405']) if 'D405' in data.keys() else np.nan 
#     dict_add['D406'] = float(data['D406']) if 'D406' in data.keys() else np.nan 
#     dict_add['D407'] = float(data['D407']) if 'D407' in data.keys() else np.nan 
#     dict_add['D408'] = float(data['D408']) if 'D408' in data.keys() else np.nan 
#     dict_add['D409'] = float(data['D409']) if 'D409' in data.keys() else np.nan 
#     dict_add['D410'] = float(data['D410']) if 'D410' in data.keys() else np.nan 
#     dict_add['D411'] = float(data['D411']) if 'D411' in data.keys() else np.nan 
#     dict_add['D412'] = float(data['D412']) if 'D412' in data.keys() else np.nan 
#     dict_add['D413'] = float(data['D413']) if 'D413' in data.keys() else np.nan 
#     dict_add['D414'] = float(data['D414']) if 'D414' in data.keys() else np.nan 
#     dict_add['D415'] = float(data['D415']) if 'D415' in data.keys() else np.nan 
#     dict_add['D416'] = float(data['D416']) if 'D416' in data.keys() else np.nan 
    dict_add['D417'] = data['D417'] if 'D417' in data.keys() else np.nan 
    dict_add['D418'] = data['D418'] if 'D418' in data.keys() else np.nan 
    dict_add['D419'] = data['D419'] if 'D419' in data.keys() else np.nan 
    dict_add['D420'] = data['D420'] if 'D420' in data.keys() else np.nan 
    dict_add['D421'] = data['D421'] if 'D421' in data.keys() else np.nan 
    dict_add['D422'] = data['D422'] if 'D422' in data.keys() else np.nan 
    dict_add['D423'] = data['D423'] if 'D423' in data.keys() else np.nan 
    dict_add['D424'] = data['D424'] if 'D424' in data.keys() else np.nan 
    dict_add['D425'] = data['D425'] if 'D425' in data.keys() else np.nan 
    dict_add['D426'] = float(data['D426']) if 'D426' in data.keys() else np.nan 
    dict_add['D427'] = float(data['D427']) if 'D427' in data.keys() else np.nan 
    dict_add['D428'] = float(data['D428']) if 'D428' in data.keys() else np.nan 
    dict_add['D429'] = float(data['D429']) if 'D429' in data.keys() else np.nan 
    dict_add['D430'] = float(data['D430']) if 'D430' in data.keys() else np.nan 
    dict_add['D431'] = float(data['D431']) if 'D431' in data.keys() else np.nan 
    dict_add['D432'] = float(data['D432']) if 'D432' in data.keys() else np.nan 
    dict_add['D433'] = float(data['D433']) if 'D433' in data.keys() else np.nan 
    dict_add['D434'] = float(data['D434']) if 'D434' in data.keys() else np.nan 
    dict_add['D435'] = float(data['D435']) if 'D435' in data.keys() else np.nan 
    dict_add['D436'] = float(data['D436']) if 'D436' in data.keys() else np.nan 
    dict_add['D437'] = float(data['D437']) if 'D437' in data.keys() else np.nan 
    dict_add['D438'] = float(data['D438']) if 'D438' in data.keys() else np.nan 
    dict_add['D439'] = float(data['D439']) if 'D439' in data.keys() else np.nan 

    dict_add['D173vsD165']=dict_add['D173']/dict_add['D165'] if dict_add['D165']>0 else np.nan
    dict_add['D399vsD175']=dict_add['D399']/dict_add['D175'] if dict_add['D175']>0 else np.nan
    dict_add['D399vsD167']=dict_add['D399']/dict_add['D167'] if dict_add['D167']>0 else np.nan
    dict_add['D389vsD175']=dict_add['D389']/dict_add['D175'] if dict_add['D175']>0 else np.nan
    dict_add['D389vsD171']=dict_add['D389']/dict_add['D171'] if dict_add['D171']>0 else np.nan
    dict_add['D175vsD171']=dict_add['D175']/dict_add['D171'] if dict_add['D171']>0 else np.nan
    dict_add['D171vsD163']=dict_add['D171']/dict_add['D163'] if dict_add['D163']>0 else np.nan
    dict_add['D167vsD163']=dict_add['D167']/dict_add['D163'] if dict_add['D163']>0 else np.nan
    dict_add['D388vsD170']=dict_add['D388']/dict_add['D170'] if dict_add['D170']>0 else np.nan
    dict_add['D397vsD161']=dict_add['D397']/dict_add['D161'] if dict_add['D161']>0 else np.nan
    dict_add['D173vsD165']=dict_add['D173']/dict_add['D165'] if dict_add['D165']>0 else np.nan
    dict_add['D173vsD161']=dict_add['D173']/dict_add['D161'] if dict_add['D161']>0 else np.nan
    dict_add['D169vsD161']=dict_add['D169']/dict_add['D161'] if dict_add['D161']>0 else np.nan
    dict_add['D165vsD161']=dict_add['D165']/dict_add['D161'] if dict_add['D161']>0 else np.nan
    dict_add['D425subD155']=dict_add['D425']-dict_add['D155']
    dict_add['D425subD154']=dict_add['D425']-dict_add['D154']
    dict_add['D383vsD153']=dict_add['D383']/dict_add['D153'] if dict_add['D153']>0 else np.nan
    dict_add['D383vsD152']=dict_add['D383']/dict_add['D152'] if dict_add['D152']>0 else np.nan
    dict_add['D383vsD150']=dict_add['D383']/dict_add['D150'] if dict_add['D150']>0 else np.nan
    dict_add['D382vsD381']=dict_add['D382']/dict_add['D381'] if dict_add['D381']>0 else np.nan
    dict_add['D382vsD153']=dict_add['D382']/dict_add['D153'] if dict_add['D153']>0 else np.nan
    dict_add['D382vsD152']=dict_add['D382']/dict_add['D152'] if dict_add['D152']>0 else np.nan
    dict_add['D382vsD151']=dict_add['D382']/dict_add['D151'] if dict_add['D151']>0 else np.nan
    dict_add['D382vsD150']=dict_add['D382']/dict_add['D150'] if dict_add['D150']>0 else np.nan
    dict_add['D381vsD152']=dict_add['D381']/dict_add['D152'] if dict_add['D152']>0 else np.nan
    dict_add['D381vsD151']=dict_add['D381']/dict_add['D151'] if dict_add['D151']>0 else np.nan
    dict_add['D381vsD150']=dict_add['D381']/dict_add['D150'] if dict_add['D150']>0 else np.nan
    dict_add['D374vsD143']=dict_add['D374']/dict_add['D143'] if dict_add['D143']>0 else np.nan
    dict_add['D374vsD142']=dict_add['D374']/dict_add['D142'] if dict_add['D142']>0 else np.nan
    dict_add['D373vsD143']=dict_add['D373']/dict_add['D143'] if dict_add['D143']>0 else np.nan
    dict_add['D373vsD142']=dict_add['D373']/dict_add['D142'] if dict_add['D142']>0 else np.nan
    dict_add['D144vsD142']=dict_add['D144']/dict_add['D142'] if dict_add['D142']>0 else np.nan
    dict_add['D143vsD142']=dict_add['D143']/dict_add['D142'] if dict_add['D142']>0 else np.nan
    dict_add['D371vsD139']=dict_add['D371']/dict_add['D139'] if dict_add['D139']>0 else np.nan
    dict_add['D141vsD138']=dict_add['D141']/dict_add['D138'] if dict_add['D138']>0 else np.nan
    dict_add['D140vsD138']=dict_add['D140']/dict_add['D138'] if dict_add['D138']>0 else np.nan
    dict_add['D139vsD138']=dict_add['D139']/dict_add['D138'] if dict_add['D138']>0 else np.nan
    dict_add['D367vsD134']=dict_add['D367']/dict_add['D134'] if dict_add['D134']>0 else np.nan
    dict_add['D137vsD136']=dict_add['D137']/dict_add['D136'] if dict_add['D136']>0 else np.nan
    dict_add['D136vsD135']=dict_add['D136']/dict_add['D135'] if dict_add['D135']>0 else np.nan
    dict_add['D135vsD134']=dict_add['D135']/dict_add['D134'] if dict_add['D134']>0 else np.nan
    dict_add['D365vsD133']=dict_add['D365']/dict_add['D133'] if dict_add['D133']>0 else np.nan
    dict_add['D365vsD132']=dict_add['D365']/dict_add['D132'] if dict_add['D132']>0 else np.nan
    dict_add['D365vsD130']=dict_add['D365']/dict_add['D130'] if dict_add['D130']>0 else np.nan
    dict_add['D364vsD363']=dict_add['D364']/dict_add['D363'] if dict_add['D363']>0 else np.nan
    dict_add['D364vsD133']=dict_add['D364']/dict_add['D133'] if dict_add['D133']>0 else np.nan
    dict_add['D364vsD132']=dict_add['D364']/dict_add['D132'] if dict_add['D132']>0 else np.nan
    dict_add['D364vsD131']=dict_add['D364']/dict_add['D131'] if dict_add['D131']>0 else np.nan
    dict_add['D364vsD130']=dict_add['D364']/dict_add['D130'] if dict_add['D130']>0 else np.nan
    dict_add['D363vsD130']=dict_add['D363']/dict_add['D130'] if dict_add['D130']>0 else np.nan
    dict_add['D133vsD131']=dict_add['D133']/dict_add['D131'] if dict_add['D131']>0 else np.nan
    dict_add['D133vsD130']=dict_add['D133']/dict_add['D130'] if dict_add['D130']>0 else np.nan
    dict_add['D131vsD130']=dict_add['D131']/dict_add['D130'] if dict_add['D130']>0 else np.nan
    dict_add['D362vsD129']=dict_add['D362']/dict_add['D129'] if dict_add['D129']>0 else np.nan
    dict_add['D362vsD128']=dict_add['D362']/dict_add['D128'] if dict_add['D128']>0 else np.nan
    dict_add['D362vsD127']=dict_add['D362']/dict_add['D127'] if dict_add['D127']>0 else np.nan
    dict_add['D362vsD126']=dict_add['D362']/dict_add['D126'] if dict_add['D126']>0 else np.nan
    dict_add['D361vsD360']=dict_add['D361']/dict_add['D360'] if dict_add['D360']>0 else np.nan
    dict_add['D361vsD129']=dict_add['D361']/dict_add['D129'] if dict_add['D129']>0 else np.nan
    dict_add['D361vsD128']=dict_add['D361']/dict_add['D128'] if dict_add['D128']>0 else np.nan
    dict_add['D361vsD127']=dict_add['D361']/dict_add['D127'] if dict_add['D127']>0 else np.nan
    dict_add['D361vsD126']=dict_add['D361']/dict_add['D126'] if dict_add['D126']>0 else np.nan
    dict_add['D360vsD129']=dict_add['D360']/dict_add['D129'] if dict_add['D129']>0 else np.nan
    dict_add['D360vsD126']=dict_add['D360']/dict_add['D126'] if dict_add['D126']>0 else np.nan
    dict_add['D129vsD126']=dict_add['D129']/dict_add['D126'] if dict_add['D126']>0 else np.nan
    dict_add['D128vsD126']=dict_add['D128']/dict_add['D126'] if dict_add['D126']>0 else np.nan
    dict_add['D127vsD126']=dict_add['D127']/dict_add['D126'] if dict_add['D126']>0 else np.nan
    dict_add['D353vsD121']=dict_add['D353']/dict_add['D121'] if dict_add['D121']>0 else np.nan
    dict_add['D353vsD120']=dict_add['D353']/dict_add['D120'] if dict_add['D120']>0 else np.nan
    dict_add['D353vsD119']=dict_add['D353']/dict_add['D119'] if dict_add['D119']>0 else np.nan
    dict_add['D353vsD118']=dict_add['D353']/dict_add['D118'] if dict_add['D118']>0 else np.nan
    dict_add['D352vsD121']=dict_add['D352']/dict_add['D121'] if dict_add['D121']>0 else np.nan
    dict_add['D352vsD120']=dict_add['D352']/dict_add['D120'] if dict_add['D120']>0 else np.nan
    dict_add['D352vsD119']=dict_add['D352']/dict_add['D119'] if dict_add['D119']>0 else np.nan
    dict_add['D352vsD118']=dict_add['D352']/dict_add['D118'] if dict_add['D118']>0 else np.nan
    dict_add['D351vsD120']=dict_add['D351']/dict_add['D120'] if dict_add['D120']>0 else np.nan
    dict_add['D351vsD119']=dict_add['D351']/dict_add['D119'] if dict_add['D119']>0 else np.nan
    dict_add['D121vsD120']=dict_add['D121']/dict_add['D120'] if dict_add['D120']>0 else np.nan
    dict_add['D121vsD118']=dict_add['D121']/dict_add['D118'] if dict_add['D118']>0 else np.nan
    dict_add['D120vsD119']=dict_add['D120']/dict_add['D119'] if dict_add['D119']>0 else np.nan
    dict_add['D119vsD118']=dict_add['D119']/dict_add['D118'] if dict_add['D118']>0 else np.nan
    dict_add['D349vsD114']=dict_add['D349']/dict_add['D114'] if dict_add['D114']>0 else np.nan
    dict_add['D348vsD116']=dict_add['D348']/dict_add['D116'] if dict_add['D116']>0 else np.nan
    dict_add['D116vsD114']=dict_add['D116']/dict_add['D114'] if dict_add['D114']>0 else np.nan
    dict_add['D115vsD114']=dict_add['D115']/dict_add['D114'] if dict_add['D114']>0 else np.nan
    dict_add['D347vsD112']=dict_add['D347']/dict_add['D112'] if dict_add['D112']>0 else np.nan
    dict_add['D347vsD111']=dict_add['D347']/dict_add['D111'] if dict_add['D111']>0 else np.nan
    dict_add['D347vsD110']=dict_add['D347']/dict_add['D110'] if dict_add['D110']>0 else np.nan
    dict_add['D346vsD110']=dict_add['D346']/dict_add['D110'] if dict_add['D110']>0 else np.nan
    dict_add['D113vsD112']=dict_add['D113']/dict_add['D112'] if dict_add['D112']>0 else np.nan
    dict_add['D113vsD111']=dict_add['D113']/dict_add['D111'] if dict_add['D111']>0 else np.nan
    dict_add['D112vsD110']=dict_add['D112']/dict_add['D110'] if dict_add['D110']>0 else np.nan
    dict_add['D111vsD110']=dict_add['D111']/dict_add['D110'] if dict_add['D110']>0 else np.nan
    dict_add['D344vsD109']=dict_add['D344']/dict_add['D109'] if dict_add['D109']>0 else np.nan
    dict_add['D344vsD107']=dict_add['D344']/dict_add['D107'] if dict_add['D107']>0 else np.nan
    dict_add['D344vsD106']=dict_add['D344']/dict_add['D106'] if dict_add['D106']>0 else np.nan
    dict_add['D340vsD339']=dict_add['D340']/dict_add['D339'] if dict_add['D339']>0 else np.nan
    dict_add['D340vsD109']=dict_add['D340']/dict_add['D109'] if dict_add['D109']>0 else np.nan
    dict_add['D340vsD108']=dict_add['D340']/dict_add['D108'] if dict_add['D108']>0 else np.nan
    dict_add['D340vsD107']=dict_add['D340']/dict_add['D107'] if dict_add['D107']>0 else np.nan
    dict_add['D340vsD106']=dict_add['D340']/dict_add['D106'] if dict_add['D106']>0 else np.nan
    dict_add['D339vsD109']=dict_add['D339']/dict_add['D109'] if dict_add['D109']>0 else np.nan
    dict_add['D339vsD108']=dict_add['D339']/dict_add['D108'] if dict_add['D108']>0 else np.nan
    dict_add['D339vsD107']=dict_add['D339']/dict_add['D107'] if dict_add['D107']>0 else np.nan
    dict_add['D339vsD106']=dict_add['D339']/dict_add['D106'] if dict_add['D106']>0 else np.nan
    dict_add['D109vsD108']=dict_add['D109']/dict_add['D108'] if dict_add['D108']>0 else np.nan
    dict_add['D109vsD106']=dict_add['D109']/dict_add['D106'] if dict_add['D106']>0 else np.nan
    dict_add['D108vsD107']=dict_add['D108']/dict_add['D107'] if dict_add['D107']>0 else np.nan
    dict_add['D108vsD106']=dict_add['D108']/dict_add['D106'] if dict_add['D106']>0 else np.nan
    dict_add['D107vsD106']=dict_add['D107']/dict_add['D106'] if dict_add['D106']>0 else np.nan
    dict_add['D424subD105']=dict_add['D424']-dict_add['D105']
    dict_add['D424subD104']=dict_add['D424']-dict_add['D104']
    dict_add['D329vsD094']=dict_add['D329']/dict_add['D094'] if dict_add['D094']>0 else np.nan
    dict_add['D328vsD095']=dict_add['D328']/dict_add['D095'] if dict_add['D095']>0 else np.nan
    dict_add['D328vsD094']=dict_add['D328']/dict_add['D094'] if dict_add['D094']>0 else np.nan
    dict_add['D095vsD094']=dict_add['D095']/dict_add['D094'] if dict_add['D094']>0 else np.nan
    dict_add['D093vsD092']=dict_add['D093']/dict_add['D092'] if dict_add['D092']>0 else np.nan
    dict_add['D091vsD090']=dict_add['D091']/dict_add['D090'] if dict_add['D090']>0 else np.nan
    dict_add['D090vsD088']=dict_add['D090']/dict_add['D088'] if dict_add['D088']>0 else np.nan
    dict_add['D320vsD318']=dict_add['D320']/dict_add['D318'] if dict_add['D318']>0 else np.nan
    dict_add['D320vsD080']=dict_add['D320']/dict_add['D080'] if dict_add['D080']>0 else np.nan
    dict_add['D081vsD080']=dict_add['D081']/dict_add['D080'] if dict_add['D080']>0 else np.nan
    dict_add['D079vsD078']=dict_add['D079']/dict_add['D078'] if dict_add['D078']>0 else np.nan
    dict_add['D078vsD077']=dict_add['D078']/dict_add['D077'] if dict_add['D077']>0 else np.nan
    dict_add['D077vsD076']=dict_add['D077']/dict_add['D076'] if dict_add['D076']>0 else np.nan
    dict_add['D308vsD071']=dict_add['D308']/dict_add['D071'] if dict_add['D071']>0 else np.nan
    dict_add['D308vsD070']=dict_add['D308']/dict_add['D070'] if dict_add['D070']>0 else np.nan
    dict_add['D307vsD071']=dict_add['D307']/dict_add['D071'] if dict_add['D071']>0 else np.nan
    dict_add['D307vsD070']=dict_add['D307']/dict_add['D070'] if dict_add['D070']>0 else np.nan
    dict_add['D070vsD069']=dict_add['D070']/dict_add['D069'] if dict_add['D069']>0 else np.nan
    dict_add['D070vsD068']=dict_add['D070']/dict_add['D068'] if dict_add['D068']>0 else np.nan
    dict_add['D069vsD068']=dict_add['D069']/dict_add['D068'] if dict_add['D068']>0 else np.nan
    dict_add['D065vsD064']=dict_add['D065']/dict_add['D064'] if dict_add['D064']>0 else np.nan
    dict_add['D061vsD060']=dict_add['D061']/dict_add['D060'] if dict_add['D060']>0 else np.nan
    dict_add['D057vsD056']=dict_add['D057']/dict_add['D056'] if dict_add['D056']>0 else np.nan
    dict_add['D051subD050']=dict_add['D051']-dict_add['D050']
    dict_add['D284vsD040']=dict_add['D284']/dict_add['D040'] if dict_add['D040']>0 else np.nan
    dict_add['D283vsD040']=dict_add['D283']/dict_add['D040'] if dict_add['D040']>0 else np.nan
    dict_add['D041vsD040']=dict_add['D041']/dict_add['D040'] if dict_add['D040']>0 else np.nan
    dict_add['D040vsD038']=dict_add['D040']/dict_add['D038'] if dict_add['D038']>0 else np.nan
    dict_add['D039vsD038']=dict_add['D039']/dict_add['D038'] if dict_add['D038']>0 else np.nan
    dict_add['D036vsD035']=dict_add['D036']/dict_add['D035'] if dict_add['D035']>0 else np.nan
    dict_add['D036vsD034']=dict_add['D036']/dict_add['D034'] if dict_add['D034']>0 else np.nan
    dict_add['D035vsD034']=dict_add['D035']/dict_add['D034'] if dict_add['D034']>0 else np.nan
    dict_add['D029vsD028']=dict_add['D029']/dict_add['D028'] if dict_add['D028']>0 else np.nan
    dict_add['D028vsD027']=dict_add['D028']/dict_add['D027'] if dict_add['D027']>0 else np.nan
    dict_add['D271vsD024']=dict_add['D271']/dict_add['D024'] if dict_add['D024']>0 else np.nan
    dict_add['D023vsD022']=dict_add['D023']/dict_add['D022'] if dict_add['D022']>0 else np.nan
    dict_add['D263vsD017']=dict_add['D263']/dict_add['D017'] if dict_add['D017']>0 else np.nan
    dict_add['D263vsD016']=dict_add['D263']/dict_add['D016'] if dict_add['D016']>0 else np.nan
    dict_add['D262vsD017']=dict_add['D262']/dict_add['D017'] if dict_add['D017']>0 else np.nan
    dict_add['D262vsD016']=dict_add['D262']/dict_add['D016'] if dict_add['D016']>0 else np.nan
    dict_add['D017vsD014']=dict_add['D017']/dict_add['D014'] if dict_add['D014']>0 else np.nan
    dict_add['D016vsD015']=dict_add['D016']/dict_add['D015'] if dict_add['D015']>0 else np.nan
    dict_add['D016vsD014']=dict_add['D016']/dict_add['D014'] if dict_add['D014']>0 else np.nan
    dict_add['D015vsD014']=dict_add['D015']/dict_add['D014'] if dict_add['D014']>0 else np.nan
    dict_add['D007vsD006']=dict_add['D007']/dict_add['D006'] if dict_add['D006']>0 else np.nan
    dict_add['D253vsD002']=dict_add['D253']/dict_add['D002'] if dict_add['D002']>0 else np.nan
    dict_add['D004vsD003']=dict_add['D004']/dict_add['D003'] if dict_add['D003']>0 else np.nan
    dict_add['D003vsD002']=dict_add['D003']/dict_add['D002'] if dict_add['D002']>0 else np.nan
    dict_add['D001subD160']=dict_add['D001']-dict_add['D160']
    dict_add['D001subD417']=dict_add['D001']-dict_add['D417']
    dict_add['D001subD418']=dict_add['D001']-dict_add['D418']
    dict_add['D001subD419']=dict_add['D009']-dict_add['D419']
    dict_add['D001subD420']=dict_add['D001']-dict_add['D420']
    dict_add['D001subD421']=dict_add['D001']-dict_add['D421']
    dict_add['D001subD422']=dict_add['D001']-dict_add['D422']
    dict_add['D001subD423']=dict_add['D001']-dict_add['D423']
    dict_add['D417subD160']=dict_add['D417']-dict_add['D160']
    dict_add['D419subD418']=dict_add['D419']-dict_add['D418']
    dict_add['D421subD420']=dict_add['D421']-dict_add['D420']
    dict_add['D423subD422']=dict_add['D423']-dict_add['D422']
    dict_add['D417subD419']=dict_add['D417']-dict_add['D419']
    dict_add['D417subD421']=dict_add['D417']-dict_add['D421']
    dict_add['D417subD423']=dict_add['D417']-dict_add['D423']
    dict_add['D160subD418']=dict_add['D160']-dict_add['D418']
    dict_add['D160subD420']=dict_add['D160']-dict_add['D420']
    dict_add['D160subD422']=dict_add['D160']-dict_add['D422']
    dict_add['D439vsD438']=dict_add['D439']/dict_add['D438'] if dict_add['D438']>0 else np.nan
    dict_add['D439vsD159']=dict_add['D439']/dict_add['D159'] if dict_add['D159']>0 else np.nan
    dict_add['D438vsD158']=dict_add['D438']/dict_add['D158'] if dict_add['D158']>0 else np.nan
    dict_add['D437vsD157']=dict_add['D437']/dict_add['D157'] if dict_add['D157']>0 else np.nan
    dict_add['D436vsD156']=dict_add['D436']/dict_add['D156'] if dict_add['D156']>0 else np.nan
    dict_add['D435vsD433']=dict_add['D435']/dict_add['D433'] if dict_add['D433']>0 else np.nan
    dict_add['D435vsD432']=dict_add['D435']/dict_add['D432'] if dict_add['D432']>0 else np.nan
    dict_add['D434vsD433']=dict_add['D434']/dict_add['D433'] if dict_add['D433']>0 else np.nan
    dict_add['D432vsD156']=dict_add['D432']/dict_add['D156'] if dict_add['D156']>0 else np.nan
    dict_add['D430vsD429']=dict_add['D430']/dict_add['D429'] if dict_add['D429']>0 else np.nan
    dict_add['D430vsD428']=dict_add['D430']/dict_add['D428'] if dict_add['D428']>0 else np.nan
    dict_add['D429vsD428']=dict_add['D429']/dict_add['D428'] if dict_add['D428']>0 else np.nan
    dict_add['D431vsD159']=dict_add['D431']/dict_add['D159'] if dict_add['D159']>0 else np.nan
    dict_add['D430vsD158']=dict_add['D430']/dict_add['D158'] if dict_add['D158']>0 else np.nan
    dict_add['D429vsD157']=dict_add['D429']/dict_add['D157'] if dict_add['D157']>0 else np.nan
    dict_add['D428vsD156']=dict_add['D428']/dict_add['D156'] if dict_add['D156']>0 else np.nan
    dict_add['D386vsD385']=dict_add['D386']/dict_add['D385'] if dict_add['D385']>0 else np.nan
    dict_add['D386vsD384']=dict_add['D386']/dict_add['D384'] if dict_add['D384']>0 else np.nan
    dict_add['D386vsD159']=dict_add['D386']/dict_add['D159'] if dict_add['D159']>0 else np.nan
    dict_add['D385vsD384']=dict_add['D385']/dict_add['D384'] if dict_add['D384']>0 else np.nan
    dict_add['D385vsD159']=dict_add['D385']/dict_add['D159'] if dict_add['D159']>0 else np.nan
    dict_add['D385vsD158']=dict_add['D385']/dict_add['D158'] if dict_add['D158']>0 else np.nan
    dict_add['D385vsD157']=dict_add['D385']/dict_add['D157'] if dict_add['D157']>0 else np.nan
    dict_add['D384vsD158']=dict_add['D384']/dict_add['D158'] if dict_add['D158']>0 else np.nan
    dict_add['D384vsD156']=dict_add['D384']/dict_add['D156'] if dict_add['D156']>0 else np.nan
    dict_add['D384vsD158']=dict_add['D384']/dict_add['D158'] if dict_add['D158']>0 else np.nan
    dict_add['D384vsD156']=dict_add['D384']/dict_add['D156'] if dict_add['D156']>0 else np.nan



    return dict_add
#huadao_feature_func_excel({},huadao_data.iloc[[4]])



def XINYAN_RADAR_feature_func_online(dict_add,json_dict,appl_time):
    data=json_dict['data'] if 'data' in json_dict.keys() and type(json_dict['data'])==dict  else dict()
    result_detail=data['result_detail'] if 'result_detail' in data.keys() and type(data['result_detail'])==dict  else dict()
    apply_report_detail=lower_to_capital(result_detail['apply_report_detail']) if 'apply_report_detail' in result_detail.keys() and type(result_detail['apply_report_detail'])==dict  else dict()
    behavior_report_detail=lower_to_capital(result_detail['behavior_report_detail']) if 'behavior_report_detail' in result_detail.keys() and type(result_detail['behavior_report_detail'])==dict  else dict()
    current_report_detail=lower_to_capital(result_detail['current_report_detail']) if 'current_report_detail' in result_detail.keys() and type(result_detail['current_report_detail'])==dict  else dict()

    dict_add['XINYAN_RADAR_A22160001'] = float(apply_report_detail['A22160001']) if 'A22160001' in apply_report_detail.keys() else np.nan  #申请准入分
    dict_add['XINYAN_RADAR_A22160002'] = float(apply_report_detail['A22160002']) if 'A22160002' in apply_report_detail.keys() else np.nan  #申请准入置信度
    dict_add['XINYAN_RADAR_A22160003'] = float(apply_report_detail['A22160003']) if 'A22160003' in apply_report_detail.keys() else np.nan  #查询机构数
    dict_add['XINYAN_RADAR_A22160004'] = float(apply_report_detail['A22160004']) if 'A22160004' in apply_report_detail.keys() else np.nan  #查询消费金融类机构数
    dict_add['XINYAN_RADAR_A22160005'] = float(apply_report_detail['A22160005']) if 'A22160005' in apply_report_detail.keys() else np.nan  #查询网络贷款类机构数
    dict_add['XINYAN_RADAR_A22160006'] = float(apply_report_detail['A22160006']) if 'A22160006' in apply_report_detail.keys() else np.nan  #总查询次数
    dict_add['XINYAN_RADAR_A22160007'] = apply_report_detail['A22160007'] if 'A22160007' in apply_report_detail.keys() else np.nan  #最近查询时间
    dict_add['XINYAN_RADAR_A22160008'] = float(apply_report_detail['A22160008']) if 'A22160008' in apply_report_detail.keys() else np.nan  #近1个月总查询笔数
    dict_add['XINYAN_RADAR_A22160009'] = float(apply_report_detail['A22160009']) if 'A22160009' in apply_report_detail.keys() else np.nan  #近3个月总查询笔数
    dict_add['XINYAN_RADAR_A22160010'] = float(apply_report_detail['A22160010']) if 'A22160010' in apply_report_detail.keys() else np.nan  #近6个月总查询笔数
    
    dict_add['XINYAN_RADAR_B22170001'] = float(behavior_report_detail['B22170001']) if 'B22170001' in behavior_report_detail.keys() else np.nan  #贷款行为分
    dict_add['XINYAN_RADAR_B22170002'] = float(behavior_report_detail['B22170002']) if 'B22170002' in behavior_report_detail.keys() else np.nan  #近1个月贷款笔数
    dict_add['XINYAN_RADAR_B22170003'] = float(behavior_report_detail['B22170003']) if 'B22170003' in behavior_report_detail.keys() else np.nan  #近3个月贷款笔数
    dict_add['XINYAN_RADAR_B22170004'] = float(behavior_report_detail['B22170004']) if 'B22170004' in behavior_report_detail.keys() else np.nan  #近6个月贷款笔数
    dict_add['XINYAN_RADAR_B22170005'] = float(behavior_report_detail['B22170005']) if 'B22170005' in behavior_report_detail.keys() else np.nan  #近12个月贷款笔数
    dict_add['XINYAN_RADAR_B22170006'] = float(behavior_report_detail['B22170006']) if 'B22170006' in behavior_report_detail.keys() else np.nan  #近24个月贷款笔数
    dict_add['XINYAN_RADAR_B22170007'] = behavior_report_detail['B22170007'] if 'B22170007' in behavior_report_detail.keys() else np.nan  #近1个月贷款总金额
    dict_add['XINYAN_RADAR_B22170008'] = behavior_report_detail['B22170008'] if 'B22170008' in behavior_report_detail.keys() else np.nan  #近3个月贷款总金额
    dict_add['XINYAN_RADAR_B22170009'] = behavior_report_detail['B22170009'] if 'B22170009' in behavior_report_detail.keys() else np.nan  #近6个月贷款总金额
    dict_add['XINYAN_RADAR_B22170010'] = behavior_report_detail['B22170010'] if 'B22170010' in behavior_report_detail.keys() else np.nan  #近12个月贷款总金额
    dict_add['XINYAN_RADAR_B22170011'] = behavior_report_detail['B22170011'] if 'B22170011' in behavior_report_detail.keys() else np.nan  #近24个月贷款总金额
    dict_add['XINYAN_RADAR_B22170012'] = float(behavior_report_detail['B22170012']) if 'B22170012' in behavior_report_detail.keys() else np.nan  #近12个月贷款金额在1k及以下的笔数
    dict_add['XINYAN_RADAR_B22170013'] = float(behavior_report_detail['B22170013']) if 'B22170013' in behavior_report_detail.keys() else np.nan  #近12个月贷款金额在1k-3k的笔数
    dict_add['XINYAN_RADAR_B22170014'] = float(behavior_report_detail['B22170014']) if 'B22170014' in behavior_report_detail.keys() else np.nan  #近12个月贷款金额在3k-10k的笔数
    dict_add['XINYAN_RADAR_B22170015'] = float(behavior_report_detail['B22170015']) if 'B22170015' in behavior_report_detail.keys() else np.nan  #近12个月贷款金额在1w以上的笔数
    dict_add['XINYAN_RADAR_B22170016'] = float(behavior_report_detail['B22170016']) if 'B22170016' in behavior_report_detail.keys() else np.nan  #近1个月贷款机构数
    dict_add['XINYAN_RADAR_B22170017'] = float(behavior_report_detail['B22170017']) if 'B22170017' in behavior_report_detail.keys() else np.nan  #近3个月贷款机构数
    dict_add['XINYAN_RADAR_B22170018'] = float(behavior_report_detail['B22170018']) if 'B22170018' in behavior_report_detail.keys() else np.nan  #近6个月贷款机构数
    dict_add['XINYAN_RADAR_B22170019'] = float(behavior_report_detail['B22170019']) if 'B22170019' in behavior_report_detail.keys() else np.nan  #近12个月贷款机构数
    dict_add['XINYAN_RADAR_B22170020'] = float(behavior_report_detail['B22170020']) if 'B22170020' in behavior_report_detail.keys() else np.nan  #近24个月贷款机构数
    dict_add['XINYAN_RADAR_B22170021'] = float(behavior_report_detail['B22170021']) if 'B22170021' in behavior_report_detail.keys() else np.nan  #近12个月消金类贷款机构数
    dict_add['XINYAN_RADAR_B22170022'] = float(behavior_report_detail['B22170022']) if 'B22170022' in behavior_report_detail.keys() else np.nan  #近24个月消金类贷款机构数
    dict_add['XINYAN_RADAR_B22170023'] = float(behavior_report_detail['B22170023']) if 'B22170023' in behavior_report_detail.keys() else np.nan  #近12个月网贷类贷款机构数
    dict_add['XINYAN_RADAR_B22170024'] = float(behavior_report_detail['B22170024']) if 'B22170024' in behavior_report_detail.keys() else np.nan  #近24个月网贷类贷款机构数
    dict_add['XINYAN_RADAR_B22170025'] = float(behavior_report_detail['B22170025']) if 'B22170025' in behavior_report_detail.keys() else np.nan  #近6个月M0+逾期贷款笔数
    dict_add['XINYAN_RADAR_B22170026'] = float(behavior_report_detail['B22170026']) if 'B22170026' in behavior_report_detail.keys() else np.nan  #近12个月M0+逾期贷款笔数
    dict_add['XINYAN_RADAR_B22170027'] = float(behavior_report_detail['B22170027']) if 'B22170027' in behavior_report_detail.keys() else np.nan  #近24个月M0+逾期贷款笔数
    dict_add['XINYAN_RADAR_B22170028'] = float(behavior_report_detail['B22170028']) if 'B22170028' in behavior_report_detail.keys() else np.nan  #近6个月M1+逾期贷款笔数
    dict_add['XINYAN_RADAR_B22170029'] = float(behavior_report_detail['B22170029']) if 'B22170029' in behavior_report_detail.keys() else np.nan  #近12个月M1+逾期贷款笔数
    dict_add['XINYAN_RADAR_B22170030'] = float(behavior_report_detail['B22170030']) if 'B22170030' in behavior_report_detail.keys() else np.nan  #近24个月M1+逾期贷款笔数
    dict_add['XINYAN_RADAR_B22170031'] = behavior_report_detail['B22170031'] if 'B22170031' in behavior_report_detail.keys() else np.nan  #近6个月累计逾期金额
    dict_add['XINYAN_RADAR_B22170032'] = behavior_report_detail['B22170032'] if 'B22170032' in behavior_report_detail.keys() else np.nan  #近12个月累计逾期金额
    dict_add['XINYAN_RADAR_B22170033'] = behavior_report_detail['B22170033'] if 'B22170033' in behavior_report_detail.keys() else np.nan  #近24个月累计逾期金额
    dict_add['XINYAN_RADAR_B22170034'] = float(re.findall('([0-9]*)%',behavior_report_detail['B22170034'])[0]) if 'B22170034' in behavior_report_detail.keys() and len(re.findall('([0-9]*)%',behavior_report_detail['B22170034']))>0 else np.nan  #正常还款订单数占贷款总订单数比例
    dict_add['XINYAN_RADAR_B22170035'] = float(behavior_report_detail['B22170035']) if 'B22170035' in behavior_report_detail.keys() else np.nan  #近1个月失败扣款笔数
    dict_add['XINYAN_RADAR_B22170036'] = float(behavior_report_detail['B22170036']) if 'B22170036' in behavior_report_detail.keys() else np.nan  #近3个月失败扣款笔数
    dict_add['XINYAN_RADAR_B22170037'] = float(behavior_report_detail['B22170037']) if 'B22170037' in behavior_report_detail.keys() else np.nan  #近6个月失败扣款笔数
    dict_add['XINYAN_RADAR_B22170038'] = float(behavior_report_detail['B22170038']) if 'B22170038' in behavior_report_detail.keys() else np.nan  #近12个月失败扣款笔数
    dict_add['XINYAN_RADAR_B22170039'] = float(behavior_report_detail['B22170039']) if 'B22170039' in behavior_report_detail.keys() else np.nan  #近24个月失败扣款笔数
    dict_add['XINYAN_RADAR_B22170040'] = behavior_report_detail['B22170040'] if 'B22170040' in behavior_report_detail.keys() else np.nan  #近1个月履约贷款总金额
    dict_add['XINYAN_RADAR_B22170041'] = behavior_report_detail['B22170041'] if 'B22170041' in behavior_report_detail.keys() else np.nan  #近3个月履约贷款总金额
    dict_add['XINYAN_RADAR_B22170042'] = behavior_report_detail['B22170042'] if 'B22170042' in behavior_report_detail.keys() else np.nan  #近6个月履约贷款总金额
    dict_add['XINYAN_RADAR_B22170043'] = behavior_report_detail['B22170043'] if 'B22170043' in behavior_report_detail.keys() else np.nan  #近12个月履约贷款总金额
    dict_add['XINYAN_RADAR_B22170044'] = behavior_report_detail['B22170044'] if 'B22170044' in behavior_report_detail.keys() else np.nan  #近24个月履约贷款总金额
    dict_add['XINYAN_RADAR_B22170045'] = float(behavior_report_detail['B22170045']) if 'B22170045' in behavior_report_detail.keys() else np.nan  #近1个月履约贷款次数
    dict_add['XINYAN_RADAR_B22170046'] = float(behavior_report_detail['B22170046']) if 'B22170046' in behavior_report_detail.keys() else np.nan  #近3个月履约贷款次数
    dict_add['XINYAN_RADAR_B22170047'] = float(behavior_report_detail['B22170047']) if 'B22170047' in behavior_report_detail.keys() else np.nan  #近6个月履约贷款次数
    dict_add['XINYAN_RADAR_B22170048'] = float(behavior_report_detail['B22170048']) if 'B22170048' in behavior_report_detail.keys() else np.nan  #近12个月履约贷款次数
    dict_add['XINYAN_RADAR_B22170049'] = float(behavior_report_detail['B22170049']) if 'B22170049' in behavior_report_detail.keys() else np.nan  #近24个月履约贷款次数
    dict_add['XINYAN_RADAR_B22170050'] = behavior_report_detail['B22170050'] if 'B22170050' in behavior_report_detail.keys() else np.nan  #最近一次履约距今天数
    dict_add['XINYAN_RADAR_B22170051'] = behavior_report_detail['B22170051'] if 'B22170051' in behavior_report_detail.keys() else np.nan  #贷款行为置信度
    dict_add['XINYAN_RADAR_B22170052'] = float(behavior_report_detail['B22170052']) if 'B22170052' in behavior_report_detail.keys() else np.nan  #贷款已结清订单数
    dict_add['XINYAN_RADAR_B22170053'] = float(behavior_report_detail['B22170053']) if 'B22170053' in behavior_report_detail.keys() else np.nan  #信用贷款时长
    dict_add['XINYAN_RADAR_B22170054'] = behavior_report_detail['B22170054'] if 'B22170054' in behavior_report_detail.keys() else np.nan  #最近一次贷款时间
    
    dict_add['XINYAN_RADAR_C22180001'] = float(current_report_detail['C22180001']) if 'C22180001' in current_report_detail.keys() else np.nan  #网贷建议授信额度
    dict_add['XINYAN_RADAR_C22180002'] = float(current_report_detail['C22180002']) if 'C22180002' in current_report_detail.keys() else np.nan  #网贷额度置信度
    dict_add['XINYAN_RADAR_C22180003'] = float(current_report_detail['C22180003']) if 'C22180003' in current_report_detail.keys() else np.nan  #网络贷款类机构数
    dict_add['XINYAN_RADAR_C22180004'] = float(current_report_detail['C22180004']) if 'C22180004' in current_report_detail.keys() else np.nan  #网络贷款类产品数
    dict_add['XINYAN_RADAR_C22180005'] = float(current_report_detail['C22180005']) if 'C22180005' in current_report_detail.keys() else np.nan  #网络贷款机构最大授信额度
    dict_add['XINYAN_RADAR_C22180006'] = float(current_report_detail['C22180006']) if 'C22180006' in current_report_detail.keys() else np.nan  #网络贷款机构平均授信额度
    dict_add['XINYAN_RADAR_C22180007'] = float(current_report_detail['C22180007']) if 'C22180007' in current_report_detail.keys() else np.nan  #消金贷款类机构数
    dict_add['XINYAN_RADAR_C22180008'] = float(current_report_detail['C22180008']) if 'C22180008' in current_report_detail.keys() else np.nan  #消金贷款类产品数
    dict_add['XINYAN_RADAR_C22180009'] = float(current_report_detail['C22180009']) if 'C22180009' in current_report_detail.keys() else np.nan  #消金贷款类机构最大授信额度
    dict_add['XINYAN_RADAR_C22180010'] = float(current_report_detail['C22180010']) if 'C22180010' in current_report_detail.keys() else np.nan  #消金贷款类机构平均授信额度
    dict_add['XINYAN_RADAR_C22180011'] = float(current_report_detail['C22180011']) if 'C22180011' in current_report_detail.keys() else np.nan  #消金建议授信额度
    dict_add['XINYAN_RADAR_C22180012'] = float(current_report_detail['C22180012']) if 'C22180012' in current_report_detail.keys() else np.nan  #消金额度置信度
    
    dict_add['XINYAN_RADAR_monthdelta_lastquery_apply']=monthdelta(dict_add['XINYAN_RADAR_A22160007'],appl_time)
    dict_add['XINYAN_RADAR_monthdelta_lastloan_apply']=monthdelta(dict_add['XINYAN_RADAR_B22170054'],appl_time)
    
    dict_add['XINYAN_RADAR_daysdelta_lastquery_apply']=daysdelta(dict_add['XINYAN_RADAR_A22160007'],appl_time)
    dict_add['XINYAN_RADAR_daysdelta_lastloan_apply']=daysdelta(dict_add['XINYAN_RADAR_B22170054'],appl_time)
    
    amt_dict=dict({'0':0,'(0,500)':100,'[500,1000)':500,'[1000,2000)':1000,'[2000,3000)':2000,'[3000,5000)':3000,
          '[5000,10000)':5000, '[10000,20000)':10000,'[20000,30000)':20000,'[30000,50000)':30000,'[50000,+)':50000})
    dict_add['XINYAN_RADAR_B22170007_num']=amt_dict[dict_add['XINYAN_RADAR_B22170007']] if dict_add['XINYAN_RADAR_B22170007'] in amt_dict.keys() else np.nan
    dict_add['XINYAN_RADAR_B22170008_num']=amt_dict[dict_add['XINYAN_RADAR_B22170008']] if dict_add['XINYAN_RADAR_B22170008'] in amt_dict.keys() else np.nan
    dict_add['XINYAN_RADAR_B22170009_num']=amt_dict[dict_add['XINYAN_RADAR_B22170009']] if dict_add['XINYAN_RADAR_B22170009'] in amt_dict.keys() else np.nan
    dict_add['XINYAN_RADAR_B22170010_num']=amt_dict[dict_add['XINYAN_RADAR_B22170010']] if dict_add['XINYAN_RADAR_B22170010'] in amt_dict.keys() else np.nan
    dict_add['XINYAN_RADAR_B22170011_num']=amt_dict[dict_add['XINYAN_RADAR_B22170011']] if dict_add['XINYAN_RADAR_B22170011'] in amt_dict.keys() else np.nan
    
    dict_add['XINYAN_RADAR_B22170031_num']=amt_dict[dict_add['XINYAN_RADAR_B22170031']] if dict_add['XINYAN_RADAR_B22170031'] in amt_dict.keys() else np.nan
    dict_add['XINYAN_RADAR_B22170032_num']=amt_dict[dict_add['XINYAN_RADAR_B22170032']] if dict_add['XINYAN_RADAR_B22170032'] in amt_dict.keys() else np.nan
    dict_add['XINYAN_RADAR_B22170033_num']=amt_dict[dict_add['XINYAN_RADAR_B22170033']] if dict_add['XINYAN_RADAR_B22170033'] in amt_dict.keys() else np.nan
    
    dict_add['XINYAN_RADAR_B22170040_num']=amt_dict[dict_add['XINYAN_RADAR_B22170040']] if dict_add['XINYAN_RADAR_B22170040'] in amt_dict.keys() else np.nan
    dict_add['XINYAN_RADAR_B22170041_num']=amt_dict[dict_add['XINYAN_RADAR_B22170041']] if dict_add['XINYAN_RADAR_B22170041'] in amt_dict.keys() else np.nan
    dict_add['XINYAN_RADAR_B22170042_num']=amt_dict[dict_add['XINYAN_RADAR_B22170042']] if dict_add['XINYAN_RADAR_B22170042'] in amt_dict.keys() else np.nan
    dict_add['XINYAN_RADAR_B22170043_num']=amt_dict[dict_add['XINYAN_RADAR_B22170043']] if dict_add['XINYAN_RADAR_B22170043'] in amt_dict.keys() else np.nan
    dict_add['XINYAN_RADAR_B22170044_num']=amt_dict[dict_add['XINYAN_RADAR_B22170044']] if dict_add['XINYAN_RADAR_B22170044'] in amt_dict.keys() else np.nan
    

    

    days_dict=dict({'0':0,'(0,7]':7,'(7,15]':15,'(15,30]':30,'(30,60]':60,'(60,90]':90,
          '(90,120]':120, '(120,150]':150,'(150,180]':180,'(180,360]':360,'(360,+)':720})
    dict_add['XINYAN_RADAR_B22170050_num']=days_dict[dict_add['XINYAN_RADAR_B22170050']] if dict_add['XINYAN_RADAR_B22170050'] in days_dict.keys() else np.nan
    
    
    
    #信用贷款时长-最近一次履约距今天数
    dict_add['XINYAN_RADAR_daysdelta_credit_to_pay']=float(dict_add['XINYAN_RADAR_B22170053'])-float(dict_add['XINYAN_RADAR_B22170050_num'])
    #信用贷款时长-最近一次贷款时间距今天数
    dict_add['XINYAN_RADAR_daysdelta_credit_to_loan']=float(dict_add['XINYAN_RADAR_B22170053'])-float(dict_add['XINYAN_RADAR_daysdelta_lastloan_apply'])
    #信用贷款时长-最近一次贷款时间距今天数
    dict_add['XINYAN_RADAR_daysdelta_credit_to_query']=float(dict_add['XINYAN_RADAR_B22170053'])-float(dict_add['XINYAN_RADAR_daysdelta_lastquery_apply'])
    #最近一次履约距今天数-最近一次贷款时间距今天数
    dict_add['XINYAN_RADAR_daysdelta_pay_to_loan']=float(dict_add['XINYAN_RADAR_B22170050_num'])-float(dict_add['XINYAN_RADAR_daysdelta_lastloan_apply'])
    #最近一次履约距今天数-最近查询时间距今天数
    dict_add['XINYAN_RADAR_daysdelta_pay_to_query']=float(dict_add['XINYAN_RADAR_B22170050_num'])-float(dict_add['XINYAN_RADAR_daysdelta_lastquery_apply'])
    #最近一次贷款时间距今天数-最近查询时间距今天数
    dict_add['XINYAN_RADAR_daysdelta_query_to_loan']=float(dict_add['XINYAN_RADAR_daysdelta_lastloan_apply'])-float(dict_add['XINYAN_RADAR_daysdelta_lastquery_apply'])
    

    #信用贷款时长/最近一次履约距今天数
    dict_add['XINYAN_RADAR_daysdelta_credit_to_pay']=dict_add['XINYAN_RADAR_B22170053']/dict_add['XINYAN_RADAR_B22170050_num'] if dict_add['XINYAN_RADAR_B22170050_num']>0 else np.nan
    #信用贷款时长/最近一次贷款时间距今天数
    dict_add['XINYAN_RADAR_daysdelta_credit_to_loan']=dict_add['XINYAN_RADAR_B22170053']/dict_add['XINYAN_RADAR_daysdelta_lastloan_apply'] if dict_add['XINYAN_RADAR_daysdelta_lastloan_apply']>0 else np.nan
    #信用贷款时长/最近一次贷款时间距今天数
    dict_add['XINYAN_RADAR_daysdelta_credit_to_query']=dict_add['XINYAN_RADAR_B22170053']/dict_add['XINYAN_RADAR_daysdelta_lastquery_apply']  if dict_add['XINYAN_RADAR_daysdelta_lastquery_apply']>0 else np.nan
    #最近一次履约距今天数/最近一次贷款时间距今天数
    dict_add['XINYAN_RADAR_daysdelta_pay_vs_loan']=dict_add['XINYAN_RADAR_B22170050_num']/dict_add['XINYAN_RADAR_daysdelta_lastloan_apply'] if dict_add['XINYAN_RADAR_daysdelta_lastloan_apply']>0 else np.nan
    #最近一次履约距今天数/最近查询时间距今天数
    dict_add['XINYAN_RADAR_daysdelta_pay_vs_query']=dict_add['XINYAN_RADAR_B22170050_num']/dict_add['XINYAN_RADAR_daysdelta_lastquery_apply'] if dict_add['XINYAN_RADAR_daysdelta_lastquery_apply']>0 else np.nan
    #最近一次贷款时间距今天数/最近查询时间距今天数
    dict_add['XINYAN_RADAR_daysdelta_query_vs_loan']=dict_add['XINYAN_RADAR_daysdelta_lastloan_apply']/dict_add['XINYAN_RADAR_daysdelta_lastquery_apply'] if dict_add['XINYAN_RADAR_daysdelta_lastloan_apply']>0 else np.nan
    
    
    
    #查询消费金融类机构数/查询机构数
    dict_add['XINYAN_RADAR_A22160004vsA22160003']=dict_add['XINYAN_RADAR_A22160004']/dict_add['XINYAN_RADAR_A22160003'] if dict_add['XINYAN_RADAR_A22160003']>0 else np.nan
    #查询网络贷款类机构数/查询机构数
    dict_add['XINYAN_RADAR_A22160005vsA22160003']=dict_add['XINYAN_RADAR_A22160005']/dict_add['XINYAN_RADAR_A22160003'] if dict_add['XINYAN_RADAR_A22160003']>0 else np.nan
    
    #近*个月总查询笔数/总查询次数
    dict_add['XINYAN_RADAR_A22160008vsA22160006']=dict_add['XINYAN_RADAR_A22160008']/dict_add['XINYAN_RADAR_A22160006'] if dict_add['XINYAN_RADAR_A22160006']>0 else np.nan
    dict_add['XINYAN_RADAR_A22160009vsA22160006']=dict_add['XINYAN_RADAR_A22160009']/dict_add['XINYAN_RADAR_A22160006'] if dict_add['XINYAN_RADAR_A22160006']>0 else np.nan
    dict_add['XINYAN_RADAR_A22160010vsA22160006']=dict_add['XINYAN_RADAR_A22160010']/dict_add['XINYAN_RADAR_A22160006'] if dict_add['XINYAN_RADAR_A22160006']>0 else np.nan
    #近*个月总查询笔数/近6个月总查询笔数
    dict_add['XINYAN_RADAR_A22160008vsA22160010']=dict_add['XINYAN_RADAR_A22160008']/dict_add['XINYAN_RADAR_A22160010'] if dict_add['XINYAN_RADAR_A22160010']>0 else np.nan
    dict_add['XINYAN_RADAR_A22160009vsA22160010']=dict_add['XINYAN_RADAR_A22160009']/dict_add['XINYAN_RADAR_A22160010'] if dict_add['XINYAN_RADAR_A22160010']>0 else np.nan
    #近*个月总查询笔数/近3个月总查询笔数
    dict_add['XINYAN_RADAR_A22160008vsA22160009']=dict_add['XINYAN_RADAR_A22160008']/dict_add['XINYAN_RADAR_A22160009'] if dict_add['XINYAN_RADAR_A22160009']>0 else np.nan
    
    #查询机构数/总查询次数
    dict_add['XINYAN_RADAR_A22160003vsA22160006']=dict_add['XINYAN_RADAR_A22160003']/dict_add['XINYAN_RADAR_A22160006'] if dict_add['XINYAN_RADAR_A22160006']>0 else np.nan
    
    #近*个月贷款笔数/近24个月贷款笔数
    dict_add['XINYAN_RADAR_B22170002vsB22170006']=dict_add['XINYAN_RADAR_B22170002']/dict_add['XINYAN_RADAR_B22170006'] if dict_add['XINYAN_RADAR_B22170006']>0 else np.nan
    dict_add['XINYAN_RADAR_B22170003vsB22170006']=dict_add['XINYAN_RADAR_B22170003']/dict_add['XINYAN_RADAR_B22170006'] if dict_add['XINYAN_RADAR_B22170006']>0 else np.nan
    dict_add['XINYAN_RADAR_B22170004vsB22170006']=dict_add['XINYAN_RADAR_B22170004']/dict_add['XINYAN_RADAR_B22170006'] if dict_add['XINYAN_RADAR_B22170006']>0 else np.nan
    dict_add['XINYAN_RADAR_B22170005vsB22170006']=dict_add['XINYAN_RADAR_B22170005']/dict_add['XINYAN_RADAR_B22170006'] if dict_add['XINYAN_RADAR_B22170006']>0 else np.nan
    #近*个月贷款笔数/近12个月贷款笔数
    dict_add['XINYAN_RADAR_B22170002vsB22170005']=dict_add['XINYAN_RADAR_B22170002']/dict_add['XINYAN_RADAR_B22170005'] if dict_add['XINYAN_RADAR_B22170005']>0 else np.nan
    dict_add['XINYAN_RADAR_B22170003vsB22170005']=dict_add['XINYAN_RADAR_B22170003']/dict_add['XINYAN_RADAR_B22170005'] if dict_add['XINYAN_RADAR_B22170005']>0 else np.nan
    dict_add['XINYAN_RADAR_B22170004vsB22170005']=dict_add['XINYAN_RADAR_B22170004']/dict_add['XINYAN_RADAR_B22170005'] if dict_add['XINYAN_RADAR_B22170005']>0 else np.nan
    #近*个月贷款笔数/近6个月贷款笔数
    dict_add['XINYAN_RADAR_B22170002vsB22170004']=dict_add['XINYAN_RADAR_B22170002']/dict_add['XINYAN_RADAR_B22170004'] if dict_add['XINYAN_RADAR_B22170004']>0 else np.nan
    dict_add['XINYAN_RADAR_B22170003vsB22170004']=dict_add['XINYAN_RADAR_B22170003']/dict_add['XINYAN_RADAR_B22170004'] if dict_add['XINYAN_RADAR_B22170004']>0 else np.nan
    #近*个月贷款笔数/近3个月贷款笔数
    dict_add['XINYAN_RADAR_B22170002vsB22170003']=dict_add['XINYAN_RADAR_B22170002']/dict_add['XINYAN_RADAR_B22170003'] if dict_add['XINYAN_RADAR_B22170003']>0 else np.nan
    
    #近*个月贷款金额/近24个月贷款金额
    dict_add['XINYAN_RADAR_B22170007_numvsB22170011_num']=dict_add['XINYAN_RADAR_B22170007_num']/dict_add['XINYAN_RADAR_B22170011_num'] if dict_add['XINYAN_RADAR_B22170011_num']>0 else np.nan
    dict_add['XINYAN_RADAR_B22170008_numvsB22170011_num']=dict_add['XINYAN_RADAR_B22170008_num']/dict_add['XINYAN_RADAR_B22170011_num'] if dict_add['XINYAN_RADAR_B22170011_num']>0 else np.nan
    dict_add['XINYAN_RADAR_B22170009_numvsB22170011_num']=dict_add['XINYAN_RADAR_B22170009_num']/dict_add['XINYAN_RADAR_B22170011_num'] if dict_add['XINYAN_RADAR_B22170011_num']>0 else np.nan
    dict_add['XINYAN_RADAR_B22170010_numvsB22170011_num']=dict_add['XINYAN_RADAR_B22170010_num']/dict_add['XINYAN_RADAR_B22170011_num'] if dict_add['XINYAN_RADAR_B22170011_num']>0 else np.nan
    #近*个月贷款金额/近12个月贷款金额
    dict_add['XINYAN_RADAR_B22170007_numvsB22170010_num']=dict_add['XINYAN_RADAR_B22170007_num']/dict_add['XINYAN_RADAR_B22170010_num'] if dict_add['XINYAN_RADAR_B22170010_num']>0 else np.nan
    dict_add['XINYAN_RADAR_B22170008_numvsB22170010_num']=dict_add['XINYAN_RADAR_B22170008_num']/dict_add['XINYAN_RADAR_B22170010_num'] if dict_add['XINYAN_RADAR_B22170010_num']>0 else np.nan
    dict_add['XINYAN_RADAR_B22170009_numvsB22170010_num']=dict_add['XINYAN_RADAR_B22170009_num']/dict_add['XINYAN_RADAR_B22170010_num'] if dict_add['XINYAN_RADAR_B22170010_num']>0 else np.nan
    #近*个月贷款金额/近6个月贷款金额
    dict_add['XINYAN_RADAR_B22170007_numvsB22170009_num']=dict_add['XINYAN_RADAR_B22170007_num']/dict_add['XINYAN_RADAR_B22170009_num'] if dict_add['XINYAN_RADAR_B22170009_num']>0 else np.nan
    dict_add['XINYAN_RADAR_B22170008_numvsB22170009_num']=dict_add['XINYAN_RADAR_B22170008_num']/dict_add['XINYAN_RADAR_B22170009_num'] if dict_add['XINYAN_RADAR_B22170009_num']>0 else np.nan
    #近*个月贷款金额/近3个月贷款金额
    dict_add['XINYAN_RADAR_B22170007_numvsB22170008_num']=dict_add['XINYAN_RADAR_B22170007_num']/dict_add['XINYAN_RADAR_B22170008_num'] if dict_add['XINYAN_RADAR_B22170008_num']>0 else np.nan
    
    #近1个月件均
    dict_add['XINYAN_RADAR_amt_per_m01']=dict_add['XINYAN_RADAR_B22170007_num']/dict_add['XINYAN_RADAR_B22170002'] if dict_add['XINYAN_RADAR_B22170002']>0 else np.nan
    #近3个月件均
    dict_add['XINYAN_RADAR_amt_per_m03']=dict_add['XINYAN_RADAR_B22170008_num']/dict_add['XINYAN_RADAR_B22170003'] if dict_add['XINYAN_RADAR_B22170003']>0 else np.nan
    #近6个月件均
    dict_add['XINYAN_RADAR_amt_per_m06']=dict_add['XINYAN_RADAR_B22170009_num']/dict_add['XINYAN_RADAR_B22170004'] if dict_add['XINYAN_RADAR_B22170004']>0 else np.nan
    #近12个月件均
    dict_add['XINYAN_RADAR_amt_per_m12']=dict_add['XINYAN_RADAR_B22170010_num']/dict_add['XINYAN_RADAR_B22170005'] if dict_add['XINYAN_RADAR_B22170005']>0 else np.nan
    #近24个月件均
    dict_add['XINYAN_RADAR_amt_per_m24']=dict_add['XINYAN_RADAR_B22170011_num']/dict_add['XINYAN_RADAR_B22170006'] if dict_add['XINYAN_RADAR_B22170006']>0 else np.nan
    
    #近1个月件均/近3个月件均
    dict_add['XINYAN_RADAR_amt_per_m01vm03']=dict_add['XINYAN_RADAR_amt_per_m01']/dict_add['XINYAN_RADAR_amt_per_m03'] if dict_add['XINYAN_RADAR_amt_per_m03']>0 else np.nan
    #近1个月件均/近6个月件均
    dict_add['XINYAN_RADAR_amt_per_m01vm06']=dict_add['XINYAN_RADAR_amt_per_m01']/dict_add['XINYAN_RADAR_amt_per_m06'] if dict_add['XINYAN_RADAR_amt_per_m06']>0 else np.nan
    #近1个月件均/近12个月件均
    dict_add['XINYAN_RADAR_amt_per_m01vm12']=dict_add['XINYAN_RADAR_amt_per_m01']/dict_add['XINYAN_RADAR_amt_per_m12'] if dict_add['XINYAN_RADAR_amt_per_m12']>0 else np.nan
    #近1个月件均/近24个月件均
    dict_add['XINYAN_RADAR_amt_per_m01vm24']=dict_add['XINYAN_RADAR_amt_per_m01']/dict_add['XINYAN_RADAR_amt_per_m24'] if dict_add['XINYAN_RADAR_amt_per_m24']>0 else np.nan
    #近3个月件均/近6个月件均
    dict_add['XINYAN_RADAR_amt_per_m03vm06']=dict_add['XINYAN_RADAR_amt_per_m03']/dict_add['XINYAN_RADAR_amt_per_m06'] if dict_add['XINYAN_RADAR_amt_per_m06']>0 else np.nan
    #近3个月件均/近12个月件均
    dict_add['XINYAN_RADAR_amt_per_m03vm12']=dict_add['XINYAN_RADAR_amt_per_m03']/dict_add['XINYAN_RADAR_amt_per_m12'] if dict_add['XINYAN_RADAR_amt_per_m12']>0 else np.nan
    #近3个月件均/近24个月件均
    dict_add['XINYAN_RADAR_amt_per_m03vm24']=dict_add['XINYAN_RADAR_amt_per_m03']/dict_add['XINYAN_RADAR_amt_per_m24'] if dict_add['XINYAN_RADAR_amt_per_m24']>0 else np.nan
    #近6个月件均/近12个月件均
    dict_add['XINYAN_RADAR_amt_per_m06vm12']=dict_add['XINYAN_RADAR_amt_per_m06']/dict_add['XINYAN_RADAR_amt_per_m12'] if dict_add['XINYAN_RADAR_amt_per_m12']>0 else np.nan
    #近6个月件均/近24个月件均
    dict_add['XINYAN_RADAR_amt_per_m06vm24']=dict_add['XINYAN_RADAR_amt_per_m06']/dict_add['XINYAN_RADAR_amt_per_m24'] if dict_add['XINYAN_RADAR_amt_per_m24']>0 else np.nan
    #近12个月件均/近24个月件均
    dict_add['XINYAN_RADAR_amt_per_m12vm24']=dict_add['XINYAN_RADAR_amt_per_m12']/dict_add['XINYAN_RADAR_amt_per_m24'] if dict_add['XINYAN_RADAR_amt_per_m24']>0 else np.nan
    
    #近12个月贷款金额在1k及以下的笔数/近12个月贷款笔数
    dict_add['XINYAN_RADAR_B22170012vsB22170005']=dict_add['XINYAN_RADAR_B22170012']/dict_add['XINYAN_RADAR_B22170005'] if dict_add['XINYAN_RADAR_B22170005']>0 else np.nan
    #近12个月贷款金额在1k-3k的笔数/近12个月贷款笔数
    dict_add['XINYAN_RADAR_B22170013vsB22170005']=dict_add['XINYAN_RADAR_B22170013']/dict_add['XINYAN_RADAR_B22170005'] if dict_add['XINYAN_RADAR_B22170005']>0 else np.nan
    #近12个月贷款金额在3k-10k的笔数
    dict_add['XINYAN_RADAR_B22170014vsB22170005']=dict_add['XINYAN_RADAR_B22170014']/dict_add['XINYAN_RADAR_B22170005'] if dict_add['XINYAN_RADAR_B22170005']>0 else np.nan
    #近12个月贷款金额在1w以上的笔数
    dict_add['XINYAN_RADAR_B22170015vsB22170005']=dict_add['XINYAN_RADAR_B22170015']/dict_add['XINYAN_RADAR_B22170005'] if dict_add['XINYAN_RADAR_B22170005']>0 else np.nan
    
    #近1个月贷款机构数/近3个月贷款机构数
    dict_add['XINYAN_RADAR_B22170016vsB22170017']=dict_add['XINYAN_RADAR_B22170016']/dict_add['XINYAN_RADAR_B22170017'] if dict_add['XINYAN_RADAR_B22170017']>0 else np.nan
    #近1个月贷款机构数/近6个月贷款机构数
    dict_add['XINYAN_RADAR_B22170016vsB22170018']=dict_add['XINYAN_RADAR_B22170016']/dict_add['XINYAN_RADAR_B22170018'] if dict_add['XINYAN_RADAR_B22170018']>0 else np.nan
    #近1个月贷款机构数/近12个月贷款机构数
    dict_add['XINYAN_RADAR_B22170016vsB22170019']=dict_add['XINYAN_RADAR_B22170016']/dict_add['XINYAN_RADAR_B22170019'] if dict_add['XINYAN_RADAR_B22170019']>0 else np.nan
    #近1个月贷款机构数/近24个月贷款机构数
    dict_add['XINYAN_RADAR_B22170016vsB22170020']=dict_add['XINYAN_RADAR_B22170016']/dict_add['XINYAN_RADAR_B22170020'] if dict_add['XINYAN_RADAR_B22170020']>0 else np.nan
    #近3个月贷款机构数/近6个月贷款机构数
    dict_add['XINYAN_RADAR_B22170017vsB22170018']=dict_add['XINYAN_RADAR_B22170017']/dict_add['XINYAN_RADAR_B22170018'] if dict_add['XINYAN_RADAR_B22170018']>0 else np.nan
    #近3个月贷款机构数/近12个月贷款机构数
    dict_add['XINYAN_RADAR_B22170017vsB22170019']=dict_add['XINYAN_RADAR_B22170017']/dict_add['XINYAN_RADAR_B22170019'] if dict_add['XINYAN_RADAR_B22170019']>0 else np.nan
    #近3个月贷款机构数/近24个月贷款机构数
    dict_add['XINYAN_RADAR_B22170017vsB22170020']=dict_add['XINYAN_RADAR_B22170017']/dict_add['XINYAN_RADAR_B22170020'] if dict_add['XINYAN_RADAR_B22170020']>0 else np.nan
    #近6个月贷款机构数/近12个月贷款机构数
    dict_add['XINYAN_RADAR_B22170018vsB22170019']=dict_add['XINYAN_RADAR_B22170018']/dict_add['XINYAN_RADAR_B22170019'] if dict_add['XINYAN_RADAR_B22170019']>0 else np.nan
    #近6个月贷款机构数/近24个月贷款机构数
    dict_add['XINYAN_RADAR_B22170018vsB22170020']=dict_add['XINYAN_RADAR_B22170018']/dict_add['XINYAN_RADAR_B22170020'] if dict_add['XINYAN_RADAR_B22170020']>0 else np.nan
    #近12个月贷款机构数/近24个月贷款机构数
    dict_add['XINYAN_RADAR_B22170019vsB22170020']=dict_add['XINYAN_RADAR_B22170019']/dict_add['XINYAN_RADAR_B22170020'] if dict_add['XINYAN_RADAR_B22170020']>0 else np.nan
    
    #近12个月消金类贷款机构数/近24个月消金类贷款机构数
    dict_add['XINYAN_RADAR_B22170021vsB22170022']=dict_add['XINYAN_RADAR_B22170021']/dict_add['XINYAN_RADAR_B22170022'] if dict_add['XINYAN_RADAR_B22170022']>0 else np.nan
    #近12个月消金类贷款机构数/近12个月贷款机构数
    dict_add['XINYAN_RADAR_B22170021vsB22170019']=dict_add['XINYAN_RADAR_B22170021']/dict_add['XINYAN_RADAR_B22170019'] if dict_add['XINYAN_RADAR_B22170019']>0 else np.nan
    #近24个月消金类贷款机构数/近24个月贷款机构数
    dict_add['XINYAN_RADAR_B22170022vsB22170020']=dict_add['XINYAN_RADAR_B22170022']/dict_add['XINYAN_RADAR_B22170020'] if dict_add['XINYAN_RADAR_B22170020']>0 else np.nan
    
    #近12个月网贷类贷款机构数/近24个月网贷类贷款机构数
    dict_add['XINYAN_RADAR_B22170023vsB22170024']=dict_add['XINYAN_RADAR_B22170023']/dict_add['XINYAN_RADAR_B22170024'] if dict_add['XINYAN_RADAR_B22170024']>0 else np.nan
    #近12个月网贷类贷款机构数/近12个月贷款机构数
    dict_add['XINYAN_RADAR_B22170023vsB22170019']=dict_add['XINYAN_RADAR_B22170023']/dict_add['XINYAN_RADAR_B22170019'] if dict_add['XINYAN_RADAR_B22170019']>0 else np.nan
    #近24个月网贷类贷款机构数/近24个月贷款机构数
    dict_add['XINYAN_RADAR_B22170024vsB22170020']=dict_add['XINYAN_RADAR_B22170024']/dict_add['XINYAN_RADAR_B22170020'] if dict_add['XINYAN_RADAR_B22170020']>0 else np.nan
    
    #近6个月M0+逾期贷款笔数/近12个月M0+逾期贷款笔数
    dict_add['XINYAN_RADAR_B22170025vsB22170026']=dict_add['XINYAN_RADAR_B22170025']/dict_add['XINYAN_RADAR_B22170026'] if dict_add['XINYAN_RADAR_B22170026']>0 else np.nan
    #近6个月M0+逾期贷款笔数/近24个月M0+逾期贷款笔数
    dict_add['XINYAN_RADAR_B22170025vsB22170027']=dict_add['XINYAN_RADAR_B22170025']/dict_add['XINYAN_RADAR_B22170027'] if dict_add['XINYAN_RADAR_B22170027']>0 else np.nan
    #近12个月M0+逾期贷款笔数/近24个月M0+逾期贷款笔数
    dict_add['XINYAN_RADAR_B22170026vsB22170027']=dict_add['XINYAN_RADAR_B22170026']/dict_add['XINYAN_RADAR_B22170027'] if dict_add['XINYAN_RADAR_B22170027']>0 else np.nan
    
    #近6个月M1+逾期贷款笔数/近12个月M1+逾期贷款笔数
    dict_add['XINYAN_RADAR_B22170028vsB22170029']=dict_add['XINYAN_RADAR_B22170028']/dict_add['XINYAN_RADAR_B22170029'] if dict_add['XINYAN_RADAR_B22170029']>0 else np.nan
    #近6个月M1+逾期贷款笔数/近24个月M1+逾期贷款笔数
    dict_add['XINYAN_RADAR_B22170028vsB22170030']=dict_add['XINYAN_RADAR_B22170028']/dict_add['XINYAN_RADAR_B22170030'] if dict_add['XINYAN_RADAR_B22170030']>0 else np.nan
    #近12个月M1+逾期贷款笔数/近24个月M1+逾期贷款笔数
    dict_add['XINYAN_RADAR_B22170029vsB22170030']=dict_add['XINYAN_RADAR_B22170029']/dict_add['XINYAN_RADAR_B22170030'] if dict_add['XINYAN_RADAR_B22170030']>0 else np.nan
    
    #近6个月M1+逾期贷款笔数/近6个月M0+逾期贷款笔数
    dict_add['XINYAN_RADAR_B22170028vsB22170025']=dict_add['XINYAN_RADAR_B22170028']/dict_add['XINYAN_RADAR_B22170025'] if dict_add['XINYAN_RADAR_B22170025']>0 else np.nan
    #近12个月M1+逾期贷款笔数/近12个月M0+逾期贷款笔数
    dict_add['XINYAN_RADAR_B22170029vsB22170026']=dict_add['XINYAN_RADAR_B22170029']/dict_add['XINYAN_RADAR_B22170026'] if dict_add['XINYAN_RADAR_B22170026']>0 else np.nan
    #近24个月M1+逾期贷款笔数/近24个月M0+逾期贷款笔数
    dict_add['XINYAN_RADAR_B22170030vsB22170027']=dict_add['XINYAN_RADAR_B22170030']/dict_add['XINYAN_RADAR_B22170027'] if dict_add['XINYAN_RADAR_B22170027']>0 else np.nan
    
    #近6个月累计逾期金额/近12个月累计逾期金额
    dict_add['XINYAN_RADAR_B22170031vsB22170032']=dict_add['XINYAN_RADAR_B22170031_num']/dict_add['XINYAN_RADAR_B22170032_num'] if dict_add['XINYAN_RADAR_B22170032_num']>0 else np.nan
    #近6个月累计逾期金额/近24个月累计逾期金额
    dict_add['XINYAN_RADAR_B22170031vsB22170033']=dict_add['XINYAN_RADAR_B22170031_num']/dict_add['XINYAN_RADAR_B22170033_num'] if dict_add['XINYAN_RADAR_B22170033_num']>0 else np.nan
    #近12个月累计逾期金额/近24个月累计逾期金额
    dict_add['XINYAN_RADAR_B22170032vsB22170033']=dict_add['XINYAN_RADAR_B22170032_num']/dict_add['XINYAN_RADAR_B22170033_num'] if dict_add['XINYAN_RADAR_B22170033_num']>0 else np.nan
    
    #近6个月逾期件均（近6个月累计逾期金额/近6个月M0+逾期贷款笔数）
    dict_add['XINYAN_RADAR_amt_per_ove_m06']=dict_add['XINYAN_RADAR_B22170031_num']/dict_add['XINYAN_RADAR_B22170025'] if dict_add['XINYAN_RADAR_B22170025']>0 else np.nan
    #近12个月逾期件均（近12个月累计逾期金额/近12个月M0+逾期贷款笔数）
    dict_add['XINYAN_RADAR_amt_per_ove_m12']=dict_add['XINYAN_RADAR_B22170032_num']/dict_add['XINYAN_RADAR_B22170026'] if dict_add['XINYAN_RADAR_B22170026']>0 else np.nan
    #近24个月逾期件均（近24个月累计逾期金额/近24个月M0+逾期贷款笔数）
    dict_add['XINYAN_RADAR_amt_per_ove_m24']=dict_add['XINYAN_RADAR_B22170033_num']/dict_add['XINYAN_RADAR_B22170027'] if dict_add['XINYAN_RADAR_B22170027']>0 else np.nan
    
    #近6个月逾期件均/近12个月逾期件均
    dict_add['XINYAN_RADAR_amt_per_ove_m06vm12']=dict_add['XINYAN_RADAR_amt_per_ove_m06']/dict_add['XINYAN_RADAR_amt_per_ove_m12'] if dict_add['XINYAN_RADAR_amt_per_ove_m12']>0 else np.nan
    #近6个月逾期件均/近24个月逾期件均
    dict_add['XINYAN_RADAR_amt_per_ove_m06vm24']=dict_add['XINYAN_RADAR_amt_per_ove_m06']/dict_add['XINYAN_RADAR_amt_per_ove_m24'] if dict_add['XINYAN_RADAR_amt_per_ove_m24']>0 else np.nan
    #近12个月逾期件均/近24个月逾期件均
    dict_add['XINYAN_RADAR_amt_per_ove_m12vm24']=dict_add['XINYAN_RADAR_amt_per_ove_m12']/dict_add['XINYAN_RADAR_amt_per_ove_m24'] if dict_add['XINYAN_RADAR_amt_per_ove_m24']>0 else np.nan
    
    #近6个月逾期件均/#近6个月件均
    dict_add['XINYAN_RADAR_amt_per_ove_m06vm06']=dict_add['XINYAN_RADAR_amt_per_ove_m06']/dict_add['XINYAN_RADAR_amt_per_m06'] if dict_add['XINYAN_RADAR_amt_per_m06']>0 else np.nan
    #近12个月逾期件均/#近12个月件均
    dict_add['XINYAN_RADAR_amt_per_ove_m12vm12']=dict_add['XINYAN_RADAR_amt_per_ove_m12']/dict_add['XINYAN_RADAR_amt_per_m12'] if dict_add['XINYAN_RADAR_amt_per_m12']>0 else np.nan
    #近24个月逾期件均/#近24个月件均
    dict_add['XINYAN_RADAR_amt_per_ove_m24vm24']=dict_add['XINYAN_RADAR_amt_per_ove_m24']/dict_add['XINYAN_RADAR_amt_per_m24'] if dict_add['XINYAN_RADAR_amt_per_m24']>0 else np.nan
    
    #近1个月失败扣款笔数/近3个月失败扣款笔数
    dict_add['XINYAN_RADAR_B22170035vsB22170036']=dict_add['XINYAN_RADAR_B22170035']/dict_add['XINYAN_RADAR_B22170036'] if dict_add['XINYAN_RADAR_B22170036']>0 else np.nan
    #近1个月失败扣款笔数/近6个月失败扣款笔数
    dict_add['XINYAN_RADAR_B22170035vsB22170037']=dict_add['XINYAN_RADAR_B22170035']/dict_add['XINYAN_RADAR_B22170037'] if dict_add['XINYAN_RADAR_B22170037']>0 else np.nan
    #近1个月失败扣款笔数/近12个月失败扣款笔数
    dict_add['XINYAN_RADAR_B22170035vsB22170038']=dict_add['XINYAN_RADAR_B22170035']/dict_add['XINYAN_RADAR_B22170038'] if dict_add['XINYAN_RADAR_B22170038']>0 else np.nan
    #近1个月失败扣款笔数/近24个月失败扣款笔数
    dict_add['XINYAN_RADAR_B22170035vsB22170039']=dict_add['XINYAN_RADAR_B22170035']/dict_add['XINYAN_RADAR_B22170039'] if dict_add['XINYAN_RADAR_B22170039']>0 else np.nan
    #近3个月失败扣款笔数/近6个月失败扣款笔数
    dict_add['XINYAN_RADAR_B22170036vsB22170037']=dict_add['XINYAN_RADAR_B22170036']/dict_add['XINYAN_RADAR_B22170037'] if dict_add['XINYAN_RADAR_B22170037']>0 else np.nan
    #近3个月失败扣款笔数/近12个月失败扣款笔数
    dict_add['XINYAN_RADAR_B22170036vsB22170038']=dict_add['XINYAN_RADAR_B22170036']/dict_add['XINYAN_RADAR_B22170038'] if dict_add['XINYAN_RADAR_B22170038']>0 else np.nan
    #近3个月失败扣款笔数/近24个月失败扣款笔数
    dict_add['XINYAN_RADAR_B22170036vsB22170039']=dict_add['XINYAN_RADAR_B22170036']/dict_add['XINYAN_RADAR_B22170039'] if dict_add['XINYAN_RADAR_B22170039']>0 else np.nan
    #近6个月失败扣款笔数/近12个月失败扣款笔数
    dict_add['XINYAN_RADAR_B22170037vsB22170038']=dict_add['XINYAN_RADAR_B22170037']/dict_add['XINYAN_RADAR_B22170038'] if dict_add['XINYAN_RADAR_B22170038']>0 else np.nan
    #近6个月失败扣款笔数/近24个月失败扣款笔数
    dict_add['XINYAN_RADAR_B22170037vsB22170039']=dict_add['XINYAN_RADAR_B22170037']/dict_add['XINYAN_RADAR_B22170039'] if dict_add['XINYAN_RADAR_B22170039']>0 else np.nan
    #近12个月失败扣款笔数/近24个月失败扣款笔数
    dict_add['XINYAN_RADAR_B22170038vsB22170039']=dict_add['XINYAN_RADAR_B22170038']/dict_add['XINYAN_RADAR_B22170039'] if dict_add['XINYAN_RADAR_B22170039']>0 else np.nan
    
    #近1个月失败扣款笔数/近1个月贷款笔数
    dict_add['XINYAN_RADAR_B22170035vsB22170002']=dict_add['XINYAN_RADAR_B22170035']/dict_add['XINYAN_RADAR_B22170002'] if dict_add['XINYAN_RADAR_B22170002']>0 else np.nan
    #近3个月失败扣款笔数/近3个月贷款笔数
    dict_add['XINYAN_RADAR_B22170036vsB22170003']=dict_add['XINYAN_RADAR_B22170036']/dict_add['XINYAN_RADAR_B22170003'] if dict_add['XINYAN_RADAR_B22170003']>0 else np.nan
    #近6个月失败扣款笔数/近6个月贷款笔数
    dict_add['XINYAN_RADAR_B22170037vsB22170004']=dict_add['XINYAN_RADAR_B22170037']/dict_add['XINYAN_RADAR_B22170004'] if dict_add['XINYAN_RADAR_B22170004']>0 else np.nan
    #近12个月失败扣款笔数/近12个月贷款笔数
    dict_add['XINYAN_RADAR_B22170038vsB22170005']=dict_add['XINYAN_RADAR_B22170038']/dict_add['XINYAN_RADAR_B22170005'] if dict_add['XINYAN_RADAR_B22170005']>0 else np.nan
    #近24个月失败扣款笔数/近24个月贷款笔数
    dict_add['XINYAN_RADAR_B22170039vsB22170006']=dict_add['XINYAN_RADAR_B22170039']/dict_add['XINYAN_RADAR_B22170006'] if dict_add['XINYAN_RADAR_B22170006']>0 else np.nan
    
    #近1个月履约贷款总金额/近3个月履约贷款总金额
    dict_add['XINYAN_RADAR_B22170040vsB22170041']=dict_add['XINYAN_RADAR_B22170040_num']/dict_add['XINYAN_RADAR_B22170041_num'] if dict_add['XINYAN_RADAR_B22170041_num']>0 else np.nan
    #近1个月履约贷款总金额/近6个月履约贷款总金额
    dict_add['XINYAN_RADAR_B22170040vsB22170042']=dict_add['XINYAN_RADAR_B22170040_num']/dict_add['XINYAN_RADAR_B22170042_num'] if dict_add['XINYAN_RADAR_B22170042_num']>0 else np.nan
    #近1个月履约贷款总金额/近12个月履约贷款总金额
    dict_add['XINYAN_RADAR_B22170040vsB22170043']=dict_add['XINYAN_RADAR_B22170040_num']/dict_add['XINYAN_RADAR_B22170043_num'] if dict_add['XINYAN_RADAR_B22170043_num']>0 else np.nan
    #近1个月履约贷款总金额/近24个月履约贷款总金额
    dict_add['XINYAN_RADAR_B22170040vsB22170044']=dict_add['XINYAN_RADAR_B22170040_num']/dict_add['XINYAN_RADAR_B22170044_num'] if dict_add['XINYAN_RADAR_B22170044_num']>0 else np.nan
    #近3个月履约贷款总金额/近6个月履约贷款总金额
    dict_add['XINYAN_RADAR_B22170041vsB22170042']=dict_add['XINYAN_RADAR_B22170041_num']/dict_add['XINYAN_RADAR_B22170042_num'] if dict_add['XINYAN_RADAR_B22170042_num']>0 else np.nan
    #近3个月履约贷款总金额/近12个月履约贷款总金额
    dict_add['XINYAN_RADAR_B22170041vsB22170043']=dict_add['XINYAN_RADAR_B22170041_num']/dict_add['XINYAN_RADAR_B22170043_num'] if dict_add['XINYAN_RADAR_B22170043_num']>0 else np.nan
    #近3个月履约贷款总金额/近24个月履约贷款总金额
    dict_add['XINYAN_RADAR_B22170041vsB22170044']=dict_add['XINYAN_RADAR_B22170041_num']/dict_add['XINYAN_RADAR_B22170044_num'] if dict_add['XINYAN_RADAR_B22170044_num']>0 else np.nan
    #近6个月履约贷款总金额/近12个月履约贷款总金额
    dict_add['XINYAN_RADAR_B22170042vsB22170043']=dict_add['XINYAN_RADAR_B22170042_num']/dict_add['XINYAN_RADAR_B22170043_num'] if dict_add['XINYAN_RADAR_B22170043_num']>0 else np.nan
    #近6个月履约贷款总金额/近24个月履约贷款总金额
    dict_add['XINYAN_RADAR_B22170042vsB22170044']=dict_add['XINYAN_RADAR_B22170042_num']/dict_add['XINYAN_RADAR_B22170044_num'] if dict_add['XINYAN_RADAR_B22170044_num']>0 else np.nan
    #近12个月履约贷款总金额/近24个月履约贷款总金额
    dict_add['XINYAN_RADAR_B22170043vsB22170044']=dict_add['XINYAN_RADAR_B22170043_num']/dict_add['XINYAN_RADAR_B22170044_num'] if dict_add['XINYAN_RADAR_B22170044_num']>0 else np.nan
    
    #近1个月履约贷款次数/近3个月履约贷款次数
    dict_add['XINYAN_RADAR_B22170045vsB22170046']=dict_add['XINYAN_RADAR_B22170045']/dict_add['XINYAN_RADAR_B22170046'] if dict_add['XINYAN_RADAR_B22170046']>0 else np.nan
    #近1个月履约贷款次数/近6个月履约贷款次数
    dict_add['XINYAN_RADAR_B22170045vsB22170047']=dict_add['XINYAN_RADAR_B22170045']/dict_add['XINYAN_RADAR_B22170047'] if dict_add['XINYAN_RADAR_B22170047']>0 else np.nan
    #近1个月履约贷款次数/近12个月履约贷款次数
    dict_add['XINYAN_RADAR_B22170045vsB22170048']=dict_add['XINYAN_RADAR_B22170045']/dict_add['XINYAN_RADAR_B22170048'] if dict_add['XINYAN_RADAR_B22170048']>0 else np.nan
    #近1个月履约贷款次数/近24个月履约贷款次数
    dict_add['XINYAN_RADAR_B22170045vsB22170049']=dict_add['XINYAN_RADAR_B22170045']/dict_add['XINYAN_RADAR_B22170049'] if dict_add['XINYAN_RADAR_B22170049']>0 else np.nan
    #近3个月履约贷款次数/近6个月履约贷款次数
    dict_add['XINYAN_RADAR_B22170046vsB22170047']=dict_add['XINYAN_RADAR_B22170046']/dict_add['XINYAN_RADAR_B22170047'] if dict_add['XINYAN_RADAR_B22170047']>0 else np.nan
    #近3个月履约贷款次数/近12个月履约贷款次数
    dict_add['XINYAN_RADAR_B22170046vsB22170048']=dict_add['XINYAN_RADAR_B22170046']/dict_add['XINYAN_RADAR_B22170048'] if dict_add['XINYAN_RADAR_B22170048']>0 else np.nan
    #近3个月履约贷款次数/近24个月履约贷款次数
    dict_add['XINYAN_RADAR_B22170046vsB22170049']=dict_add['XINYAN_RADAR_B22170046']/dict_add['XINYAN_RADAR_B22170049'] if dict_add['XINYAN_RADAR_B22170049']>0 else np.nan
    #近6个月履约贷款次数/近12个月履约贷款次数
    dict_add['XINYAN_RADAR_B22170047vsB22170048']=dict_add['XINYAN_RADAR_B22170047']/dict_add['XINYAN_RADAR_B22170048'] if dict_add['XINYAN_RADAR_B22170048']>0 else np.nan
    #近6个月履约贷款次数/近24个月履约贷款次数
    dict_add['XINYAN_RADAR_B22170047vsB22170049']=dict_add['XINYAN_RADAR_B22170047']/dict_add['XINYAN_RADAR_B22170049'] if dict_add['XINYAN_RADAR_B22170049']>0 else np.nan
    #近12个月履约贷款次数/近24个月履约贷款次数
    dict_add['XINYAN_RADAR_B22170048vsB22170049']=dict_add['XINYAN_RADAR_B22170048']/dict_add['XINYAN_RADAR_B22170049'] if dict_add['XINYAN_RADAR_B22170049']>0 else np.nan
    
    #近1个月履约贷款次均（近1个月履约贷款总金额/近1个月履约贷款次数）
    dict_add['XINYAN_RADAR_amt_per_pay_m01']=dict_add['XINYAN_RADAR_B22170040_num']/dict_add['XINYAN_RADAR_B22170045'] if dict_add['XINYAN_RADAR_B22170045']>0 else np.nan
    #近3个月履约贷款次均（近3个月履约贷款总金额/近3个月履约贷款次数）
    dict_add['XINYAN_RADAR_amt_per_pay_m03']=dict_add['XINYAN_RADAR_B22170041_num']/dict_add['XINYAN_RADAR_B22170046'] if dict_add['XINYAN_RADAR_B22170046']>0 else np.nan
    #近6个月履约贷款次均（近6个月履约贷款总金额/近6个月履约贷款次数）
    dict_add['XINYAN_RADAR_amt_per_pay_m06']=dict_add['XINYAN_RADAR_B22170042_num']/dict_add['XINYAN_RADAR_B22170047'] if dict_add['XINYAN_RADAR_B22170047']>0 else np.nan
    #近12个月履约贷款次均（近12个月履约贷款总金额/近12个月履约贷款次数）
    dict_add['XINYAN_RADAR_amt_per_pay_m12']=dict_add['XINYAN_RADAR_B22170043_num']/dict_add['XINYAN_RADAR_B22170048'] if dict_add['XINYAN_RADAR_B22170048']>0 else np.nan
    #近24个月履约贷款次均（近24个月履约贷款总金额/近24个月履约贷款次数）
    dict_add['XINYAN_RADAR_amt_per_pay_m24']=dict_add['XINYAN_RADAR_B22170044_num']/dict_add['XINYAN_RADAR_B22170049'] if dict_add['XINYAN_RADAR_B22170049']>0 else np.nan
    
    #近1个月履约贷款次均/近3个月履约贷款次均
    dict_add['XINYAN_RADAR_amt_per_pay_m01vm03']=dict_add['XINYAN_RADAR_amt_per_pay_m01']/dict_add['XINYAN_RADAR_amt_per_pay_m03'] if dict_add['XINYAN_RADAR_amt_per_pay_m03']>0 else np.nan
    #近1个月履约贷款次均/近6个月履约贷款次均
    dict_add['XINYAN_RADAR_amt_per_pay_m01vm06']=dict_add['XINYAN_RADAR_amt_per_pay_m01']/dict_add['XINYAN_RADAR_amt_per_pay_m06'] if dict_add['XINYAN_RADAR_amt_per_pay_m06']>0 else np.nan
    #近1个月履约贷款次均/近12个月履约贷款次均
    dict_add['XINYAN_RADAR_amt_per_pay_m01vm12']=dict_add['XINYAN_RADAR_amt_per_pay_m01']/dict_add['XINYAN_RADAR_amt_per_pay_m12'] if dict_add['XINYAN_RADAR_amt_per_pay_m12']>0 else np.nan
    #近1个月履约贷款次均/近24个月履约贷款次均
    dict_add['XINYAN_RADAR_amt_per_pay_m01vm24']=dict_add['XINYAN_RADAR_amt_per_pay_m01']/dict_add['XINYAN_RADAR_amt_per_pay_m24'] if dict_add['XINYAN_RADAR_amt_per_pay_m24']>0 else np.nan
    #近3个月履约贷款次均/近6个月履约贷款次均
    dict_add['XINYAN_RADAR_amt_per_pay_m03vm06']=dict_add['XINYAN_RADAR_amt_per_pay_m03']/dict_add['XINYAN_RADAR_amt_per_pay_m06'] if dict_add['XINYAN_RADAR_amt_per_pay_m06']>0 else np.nan
    #近3个月履约贷款次均/近12个月履约贷款次均
    dict_add['XINYAN_RADAR_amt_per_pay_m03vm12']=dict_add['XINYAN_RADAR_amt_per_pay_m03']/dict_add['XINYAN_RADAR_amt_per_pay_m12'] if dict_add['XINYAN_RADAR_amt_per_pay_m12']>0 else np.nan
    #近3个月履约贷款次均/近24个月履约贷款次均
    dict_add['XINYAN_RADAR_amt_per_pay_m03vm24']=dict_add['XINYAN_RADAR_amt_per_pay_m03']/dict_add['XINYAN_RADAR_amt_per_pay_m24'] if dict_add['XINYAN_RADAR_amt_per_pay_m24']>0 else np.nan
    #近6个月履约贷款次均/近12个月履约贷款次均
    dict_add['XINYAN_RADAR_amt_per_pay_m06vm12']=dict_add['XINYAN_RADAR_amt_per_pay_m06']/dict_add['XINYAN_RADAR_amt_per_pay_m12'] if dict_add['XINYAN_RADAR_amt_per_pay_m12']>0 else np.nan
    #近6个月履约贷款次均/近24个月履约贷款次均
    dict_add['XINYAN_RADAR_amt_per_pay_m06vm24']=dict_add['XINYAN_RADAR_amt_per_pay_m06']/dict_add['XINYAN_RADAR_amt_per_pay_m24'] if dict_add['XINYAN_RADAR_amt_per_pay_m24']>0 else np.nan
    #近12个月履约贷款次均/近24个月履约贷款次均
    dict_add['XINYAN_RADAR_amt_per_pay_m12vm24']=dict_add['XINYAN_RADAR_amt_per_pay_m12']/dict_add['XINYAN_RADAR_amt_per_pay_m24'] if dict_add['XINYAN_RADAR_amt_per_pay_m24']>0 else np.nan
    
    #近1个月履约贷款次数-近1个月失败扣款笔数
    dict_add['XINYAN_RADAR_B22170045subB22170035']=dict_add['XINYAN_RADAR_B22170045']-dict_add['XINYAN_RADAR_B22170035']
    #近3个月履约贷款次数-近3个月失败扣款笔数
    dict_add['XINYAN_RADAR_B22170046subB22170036']=dict_add['XINYAN_RADAR_B22170046']-dict_add['XINYAN_RADAR_B22170036']
    #近6个月履约贷款次数-近6个月失败扣款笔数
    dict_add['XINYAN_RADAR_B22170047subB22170037']=dict_add['XINYAN_RADAR_B22170047']-dict_add['XINYAN_RADAR_B22170037']
    #近12个月履约贷款次数-近12个月失败扣款笔数
    dict_add['XINYAN_RADAR_B22170048subB22170038']=dict_add['XINYAN_RADAR_B22170048']-dict_add['XINYAN_RADAR_B22170038']
    #近24个月履约贷款次数-近24个月失败扣款笔数
    dict_add['XINYAN_RADAR_B22170049subB22170039']=dict_add['XINYAN_RADAR_B22170049']-dict_add['XINYAN_RADAR_B22170039']
    
    #近1个月失败扣款笔数/近1个月履约贷款次数
    dict_add['XINYAN_RADAR_B22170035subB22170045']=dict_add['XINYAN_RADAR_B22170035']/dict_add['XINYAN_RADAR_B22170045'] if dict_add['XINYAN_RADAR_B22170045']>0 else np.nan
    #近3个月失败扣款笔数/近3个月履约贷款次数
    dict_add['XINYAN_RADAR_B22170036subB22170046']=dict_add['XINYAN_RADAR_B22170036']/dict_add['XINYAN_RADAR_B22170046'] if dict_add['XINYAN_RADAR_B22170046']>0 else np.nan
    #近6个月失败扣款笔数/近6个月履约贷款次数
    dict_add['XINYAN_RADAR_B22170037subB22170047']=dict_add['XINYAN_RADAR_B22170037']/dict_add['XINYAN_RADAR_B22170047'] if dict_add['XINYAN_RADAR_B22170047']>0 else np.nan
    #近12个月失败扣款笔数/近12个月履约贷款次数
    dict_add['XINYAN_RADAR_B22170038subB22170048']=dict_add['XINYAN_RADAR_B22170038']/dict_add['XINYAN_RADAR_B22170048'] if dict_add['XINYAN_RADAR_B22170048']>0 else np.nan
    #近24个月失败扣款笔数/近24个月履约贷款次数
    dict_add['XINYAN_RADAR_B22170039subB22170049']=dict_add['XINYAN_RADAR_B22170039']/dict_add['XINYAN_RADAR_B22170049'] if dict_add['XINYAN_RADAR_B22170049']>0 else np.nan
    
    #近1个月履约贷款总金额/近1个月贷款总金额
    dict_add['XINYAN_RADAR_B22170040vsB22170007']=dict_add['XINYAN_RADAR_B22170040_num']/dict_add['XINYAN_RADAR_B22170007_num'] if dict_add['XINYAN_RADAR_B22170007_num']>0 else np.nan
    #近3个月履约贷款总金额/近3个月贷款总金额
    dict_add['XINYAN_RADAR_B22170041vsB22170008']=dict_add['XINYAN_RADAR_B22170041_num']/dict_add['XINYAN_RADAR_B22170008_num'] if dict_add['XINYAN_RADAR_B22170008_num']>0 else np.nan
    #近6个月履约贷款总金额/近6个月贷款总金额
    dict_add['XINYAN_RADAR_B22170042vsB22170009']=dict_add['XINYAN_RADAR_B22170042_num']/dict_add['XINYAN_RADAR_B22170009_num'] if dict_add['XINYAN_RADAR_B22170009_num']>0 else np.nan
    #近12个月履约贷款总金额/近12个月贷款总金额
    dict_add['XINYAN_RADAR_B22170043vsB22170010']=dict_add['XINYAN_RADAR_B22170043_num']/dict_add['XINYAN_RADAR_B22170010_num'] if dict_add['XINYAN_RADAR_B22170010_num']>0 else np.nan
    #近24个月履约贷款总金额/近24个月贷款总金额
    dict_add['XINYAN_RADAR_B22170044vsB22170011']=dict_add['XINYAN_RADAR_B22170044_num']/dict_add['XINYAN_RADAR_B22170011_num'] if dict_add['XINYAN_RADAR_B22170011_num']>0 else np.nan
    
    #近1个月履约贷款次均/近1个月件均
    dict_add['XINYAN_RADAR_amt_per_pay_m01vs01']=dict_add['XINYAN_RADAR_amt_per_pay_m01']/dict_add['XINYAN_RADAR_amt_per_m01'] if dict_add['XINYAN_RADAR_amt_per_m01']>0 else np.nan
    #近3个月履约贷款次均/近3个月件均
    dict_add['XINYAN_RADAR_amt_per_pay_m03vs03']=dict_add['XINYAN_RADAR_amt_per_pay_m03']/dict_add['XINYAN_RADAR_amt_per_m03'] if dict_add['XINYAN_RADAR_amt_per_m03']>0 else np.nan
    #近6个月履约贷款次均/近6个月件均
    dict_add['XINYAN_RADAR_amt_per_pay_m06vs06']=dict_add['XINYAN_RADAR_amt_per_pay_m06']/dict_add['XINYAN_RADAR_amt_per_m06'] if dict_add['XINYAN_RADAR_amt_per_m06']>0 else np.nan
    #近12个月履约贷款次均/近12个月件均
    dict_add['XINYAN_RADAR_amt_per_pay_m12vs12']=dict_add['XINYAN_RADAR_amt_per_pay_m12']/dict_add['XINYAN_RADAR_amt_per_m12'] if dict_add['XINYAN_RADAR_amt_per_m12']>0 else np.nan
    #近24个月履约贷款次均/近24个月件均
    dict_add['XINYAN_RADAR_amt_per_pay_m24vs24']=dict_add['XINYAN_RADAR_amt_per_pay_m24']/dict_add['XINYAN_RADAR_amt_per_m24'] if dict_add['XINYAN_RADAR_amt_per_m24']>0 else np.nan
  
    #近1个月失败扣款笔数/近1个月总查询笔数
    dict_add['XINYAN_RADAR_B22170035vsA22160008']=dict_add['XINYAN_RADAR_B22170035']/dict_add['XINYAN_RADAR_A22160008'] if dict_add['XINYAN_RADAR_A22160008']>0 else np.nan
    #近3个月失败扣款笔数/近3个月总查询笔数
    dict_add['XINYAN_RADAR_B22170036vsA22160009']=dict_add['XINYAN_RADAR_B22170036']/dict_add['XINYAN_RADAR_A22160009'] if dict_add['XINYAN_RADAR_A22160009']>0 else np.nan
    #近6个月失败扣款笔数/近6个月总查询笔数
    dict_add['XINYAN_RADAR_B22170037vsA22160010']=dict_add['XINYAN_RADAR_B22170037']/dict_add['XINYAN_RADAR_A22160010'] if dict_add['XINYAN_RADAR_A22160010']>0 else np.nan
    
    #近1个月通过率（近1个月贷款笔数/近1个月总查询笔数）
    dict_add['XINYAN_RADAR_approve_rate_m01']=dict_add['XINYAN_RADAR_B22170002']/dict_add['XINYAN_RADAR_A22160008'] if dict_add['XINYAN_RADAR_A22160008']>0 else np.nan
    #近3个月通过率（近3个月贷款笔数/近3个月总查询笔数）
    dict_add['XINYAN_RADAR_approve_rate_m03']=dict_add['XINYAN_RADAR_B22170003']/dict_add['XINYAN_RADAR_A22160009'] if dict_add['XINYAN_RADAR_A22160009']>0 else np.nan
    #近6个月通过率（近6个月贷款笔数/近6个月总查询笔数）
    dict_add['XINYAN_RADAR_approve_rate_m06']=dict_add['XINYAN_RADAR_B22170004']/dict_add['XINYAN_RADAR_A22160010'] if dict_add['XINYAN_RADAR_A22160010']>0 else np.nan
    
    #近1个月通过率/近3个月通过率
    dict_add['XINYAN_RADAR_approve_rate_m01vsm03']=dict_add['XINYAN_RADAR_approve_rate_m01']/dict_add['XINYAN_RADAR_approve_rate_m03'] if dict_add['XINYAN_RADAR_approve_rate_m03']>0 else np.nan
    #近1个月通过率/近6个月通过率
    dict_add['XINYAN_RADAR_approve_rate_m01vsm06']=dict_add['XINYAN_RADAR_approve_rate_m01']/dict_add['XINYAN_RADAR_approve_rate_m06'] if dict_add['XINYAN_RADAR_approve_rate_m06']>0 else np.nan
    #近3个月通过率/近6个月通过率
    dict_add['XINYAN_RADAR_approve_rate_m03vsm06']=dict_add['XINYAN_RADAR_approve_rate_m03']/dict_add['XINYAN_RADAR_approve_rate_m06'] if dict_add['XINYAN_RADAR_approve_rate_m06']>0 else np.nan
    
    #近6个月M0+逾期率（近6个月M0+逾期贷款笔数/近6个月贷款笔数）
    dict_add['XINYAN_RADAR_m0_overdue_rate_m06']=dict_add['XINYAN_RADAR_B22170025']/dict_add['XINYAN_RADAR_B22170004'] if dict_add['XINYAN_RADAR_B22170004']>0 else np.nan
    #近12个月M0+逾期率（近12个月M0+逾期贷款笔数/近12个月贷款笔数）
    dict_add['XINYAN_RADAR_m0_overdue_rate_m12']=dict_add['XINYAN_RADAR_B22170026']/dict_add['XINYAN_RADAR_B22170005'] if dict_add['XINYAN_RADAR_B22170005']>0 else np.nan
    #近24个月M0+逾期率（近24个月M0+逾期贷款笔数/近24个月贷款笔数）
    dict_add['XINYAN_RADAR_m0_overdue_rate_m24']=dict_add['XINYAN_RADAR_B22170027']/dict_add['XINYAN_RADAR_B22170006'] if dict_add['XINYAN_RADAR_B22170006']>0 else np.nan
    
    #近6个月M1+逾期率（近6个月M1+逾期贷款笔数/近6个月贷款笔数）
    dict_add['XINYAN_RADAR_m1_overdue_rate_m06']=dict_add['XINYAN_RADAR_B22170028']/dict_add['XINYAN_RADAR_B22170004'] if dict_add['XINYAN_RADAR_B22170004']>0 else np.nan
    #近12个月M1+逾期率（近12个月M1+逾期贷款笔数/近12个月贷款笔数）
    dict_add['XINYAN_RADAR_m1_overdue_rate_m12']=dict_add['XINYAN_RADAR_B22170029']/dict_add['XINYAN_RADAR_B22170005'] if dict_add['XINYAN_RADAR_B22170005']>0 else np.nan
    #近24个月M1+逾期率（近24个月M1+逾期贷款笔数/近24个月贷款笔数）
    dict_add['XINYAN_RADAR_m1_overdue_rate_m24']=dict_add['XINYAN_RADAR_B22170030']/dict_add['XINYAN_RADAR_B22170006'] if dict_add['XINYAN_RADAR_B22170006']>0 else np.nan
    
    #近6个月M0+逾期率/近12个月M0+逾期率
    dict_add['XINYAN_RADAR_m0_overdue_rate_m06vsm12']=dict_add['XINYAN_RADAR_m0_overdue_rate_m06']/dict_add['XINYAN_RADAR_m0_overdue_rate_m12'] if dict_add['XINYAN_RADAR_m0_overdue_rate_m12']>0 else np.nan
    #近6个月M0+逾期率/近24个月M0+逾期率
    dict_add['XINYAN_RADAR_m0_overdue_rate_m06vsm24']=dict_add['XINYAN_RADAR_m0_overdue_rate_m06']/dict_add['XINYAN_RADAR_m0_overdue_rate_m24'] if dict_add['XINYAN_RADAR_m0_overdue_rate_m24']>0 else np.nan
    #近12个月M0+逾期率/近24个月M0+逾期率
    dict_add['XINYAN_RADAR_m0_overdue_rate_m12vsm24']=dict_add['XINYAN_RADAR_m0_overdue_rate_m12']/dict_add['XINYAN_RADAR_m0_overdue_rate_m24'] if dict_add['XINYAN_RADAR_m0_overdue_rate_m24']>0 else np.nan
    
    #近6个月M1+逾期率/近12个月M1+逾期率
    dict_add['XINYAN_RADAR_m1_overdue_rate_m06vsm12']=dict_add['XINYAN_RADAR_m1_overdue_rate_m06']/dict_add['XINYAN_RADAR_m1_overdue_rate_m12'] if dict_add['XINYAN_RADAR_m1_overdue_rate_m12']>0 else np.nan
    #近6个月M1+逾期率/近24个月M1+逾期率
    dict_add['XINYAN_RADAR_m1_overdue_rate_m06vsm24']=dict_add['XINYAN_RADAR_m1_overdue_rate_m06']/dict_add['XINYAN_RADAR_m1_overdue_rate_m24'] if dict_add['XINYAN_RADAR_m1_overdue_rate_m24']>0 else np.nan
    #近12个月M1+逾期率/近24个月M1+逾期率
    dict_add['XINYAN_RADAR_m1_overdue_rate_m12vsm24']=dict_add['XINYAN_RADAR_m1_overdue_rate_m12']/dict_add['XINYAN_RADAR_m1_overdue_rate_m24'] if dict_add['XINYAN_RADAR_m1_overdue_rate_m24']>0 else np.nan
    
    #近6个月M0+逾期率/近6个月M1+逾期率
    dict_add['XINYAN_RADAR_m0_overdue_rate_m06vsm1m06']=dict_add['XINYAN_RADAR_m0_overdue_rate_m06']/dict_add['XINYAN_RADAR_m1_overdue_rate_m06'] if dict_add['XINYAN_RADAR_m1_overdue_rate_m06']>0 else np.nan
    #近12个月M0+逾期率/近12个月M1+逾期率
    dict_add['XINYAN_RADAR_m0_overdue_rate_m12vsm1m12']=dict_add['XINYAN_RADAR_m0_overdue_rate_m12']/dict_add['XINYAN_RADAR_m1_overdue_rate_m12'] if dict_add['XINYAN_RADAR_m1_overdue_rate_m12']>0 else np.nan
    #近24个月M0+逾期率/近24个月M1+逾期率
    dict_add['XINYAN_RADAR_m0_overdue_rate_m24vsm1m24']=dict_add['XINYAN_RADAR_m0_overdue_rate_m24']/dict_add['XINYAN_RADAR_m1_overdue_rate_m24'] if dict_add['XINYAN_RADAR_m1_overdue_rate_m24']>0 else np.nan
    
    
    #贷款已结清订单数/近24个月贷款笔数
    dict_add['XINYAN_RADAR_B22170052vsB22170006']=dict_add['XINYAN_RADAR_B22170052']/dict_add['XINYAN_RADAR_B22170006'] if dict_add['XINYAN_RADAR_B22170006']>0 else np.nan
    #贷款已结清订单数/近24个月贷款机构数
    dict_add['XINYAN_RADAR_B22170052vsB22170020']=dict_add['XINYAN_RADAR_B22170052']/dict_add['XINYAN_RADAR_B22170020'] if dict_add['XINYAN_RADAR_B22170020']>0 else np.nan
    #贷款已结清订单数/近24个月M0+逾期贷款笔数
    dict_add['XINYAN_RADAR_B22170052vsB22170027']=dict_add['XINYAN_RADAR_B22170052']/dict_add['XINYAN_RADAR_B22170027'] if dict_add['XINYAN_RADAR_B22170027']>0 else np.nan
    #贷款已结清订单数/近24个月M1+逾期贷款笔数
    dict_add['XINYAN_RADAR_B22170052vsB22170030']=dict_add['XINYAN_RADAR_B22170052']/dict_add['XINYAN_RADAR_B22170030'] if dict_add['XINYAN_RADAR_B22170030']>0 else np.nan
    #贷款已结清订单数/近24个月失败扣款笔数
    dict_add['XINYAN_RADAR_B22170052vsB22170039']=dict_add['XINYAN_RADAR_B22170052']/dict_add['XINYAN_RADAR_B22170039'] if dict_add['XINYAN_RADAR_B22170039']>0 else np.nan
    #贷款已结清订单数/近24个月履约贷款次数
    dict_add['XINYAN_RADAR_B22170052vsB22170049']=dict_add['XINYAN_RADAR_B22170052']/dict_add['XINYAN_RADAR_B22170049'] if dict_add['XINYAN_RADAR_B22170049']>0 else np.nan
    
    
    #近24个月履约贷款总金额/网贷建议授信额度
    dict_add['XINYAN_RADAR_B22170044vsC22180001']=dict_add['XINYAN_RADAR_B22170044_num']/dict_add['XINYAN_RADAR_C22180001'] if dict_add['XINYAN_RADAR_C22180001']>0 else np.nan
    #近12个月履约贷款总金额/网贷建议授信额度
    dict_add['XINYAN_RADAR_B22170043vsC22180001']=dict_add['XINYAN_RADAR_B22170043_num']/dict_add['XINYAN_RADAR_C22180001'] if dict_add['XINYAN_RADAR_C22180001']>0 else np.nan
    #近6个月履约贷款总金额/网贷建议授信额度
    dict_add['XINYAN_RADAR_B22170042vsC22180001']=dict_add['XINYAN_RADAR_B22170042_num']/dict_add['XINYAN_RADAR_C22180001'] if dict_add['XINYAN_RADAR_C22180001']>0 else np.nan
    #近3个月履约贷款总金额/网贷建议授信额度
    dict_add['XINYAN_RADAR_B22170041vsC22180001']=dict_add['XINYAN_RADAR_B22170041_num']/dict_add['XINYAN_RADAR_C22180001'] if dict_add['XINYAN_RADAR_C22180001']>0 else np.nan
    #近1个月履约贷款总金额/网贷建议授信额度
    dict_add['XINYAN_RADAR_B22170040vsC22180001']=dict_add['XINYAN_RADAR_B22170040_num']/dict_add['XINYAN_RADAR_C22180001'] if dict_add['XINYAN_RADAR_C22180001']>0 else np.nan
    #近24个月累计逾期金额/网贷建议授信额度
    dict_add['XINYAN_RADAR_B22170033vsC22180001']=dict_add['XINYAN_RADAR_B22170033_num']/dict_add['XINYAN_RADAR_C22180001'] if dict_add['XINYAN_RADAR_C22180001']>0 else np.nan
    #近12个月累计逾期金额/网贷建议授信额度
    dict_add['XINYAN_RADAR_B22170032vsC22180001']=dict_add['XINYAN_RADAR_B22170032_num']/dict_add['XINYAN_RADAR_C22180001'] if dict_add['XINYAN_RADAR_C22180001']>0 else np.nan
    #近06个月累计逾期金额/网贷建议授信额度
    dict_add['XINYAN_RADAR_B22170031vsC22180001']=dict_add['XINYAN_RADAR_B22170031_num']/dict_add['XINYAN_RADAR_C22180001'] if dict_add['XINYAN_RADAR_C22180001']>0 else np.nan
    #近24个月贷款总金额/网贷建议授信额度
    dict_add['XINYAN_RADAR_B22170011vsC22180001']=dict_add['XINYAN_RADAR_B22170011_num']/dict_add['XINYAN_RADAR_C22180001'] if dict_add['XINYAN_RADAR_C22180001']>0 else np.nan
    #近12个月贷款总金额/网贷建议授信额度
    dict_add['XINYAN_RADAR_B22170010vsC22180001']=dict_add['XINYAN_RADAR_B22170010_num']/dict_add['XINYAN_RADAR_C22180001'] if dict_add['XINYAN_RADAR_C22180001']>0 else np.nan
    #近6个月贷款总金额/网贷建议授信额度
    dict_add['XINYAN_RADAR_B22170009vsC22180001']=dict_add['XINYAN_RADAR_B22170009_num']/dict_add['XINYAN_RADAR_C22180001'] if dict_add['XINYAN_RADAR_C22180001']>0 else np.nan
    #近3个月贷款总金额/网贷建议授信额度
    dict_add['XINYAN_RADAR_B22170008vsC22180001']=dict_add['XINYAN_RADAR_B22170008_num']/dict_add['XINYAN_RADAR_C22180001'] if dict_add['XINYAN_RADAR_C22180001']>0 else np.nan
    #近1个月贷款总金额/网贷建议授信额度
    dict_add['XINYAN_RADAR_B22170007vsC22180001']=dict_add['XINYAN_RADAR_B22170007_num']/dict_add['XINYAN_RADAR_C22180001'] if dict_add['XINYAN_RADAR_C22180001']>0 else np.nan
    #近1个月件均/网贷建议授信额度
    dict_add['XINYAN_RADAR_amt_per_m01vsC22180001']=dict_add['XINYAN_RADAR_amt_per_m01']/dict_add['XINYAN_RADAR_C22180001'] if dict_add['XINYAN_RADAR_C22180001']>0 else np.nan
    #近3个月件均/网贷建议授信额度
    dict_add['XINYAN_RADAR_amt_per_m03vsC22180001']=dict_add['XINYAN_RADAR_amt_per_m03']/dict_add['XINYAN_RADAR_C22180001'] if dict_add['XINYAN_RADAR_C22180001']>0 else np.nan
    #近6个月件均/网贷建议授信额度
    dict_add['XINYAN_RADAR_amt_per_m06vsC22180001']=dict_add['XINYAN_RADAR_amt_per_m06']/dict_add['XINYAN_RADAR_C22180001'] if dict_add['XINYAN_RADAR_C22180001']>0 else np.nan
    #近12个月件均/网贷建议授信额度
    dict_add['XINYAN_RADAR_amt_per_m12vsC22180001']=dict_add['XINYAN_RADAR_amt_per_m12']/dict_add['XINYAN_RADAR_C22180001'] if dict_add['XINYAN_RADAR_C22180001']>0 else np.nan
    #近24个月件均/网贷建议授信额度
    dict_add['XINYAN_RADAR_amt_per_m24vsC22180001']=dict_add['XINYAN_RADAR_amt_per_m24']/dict_add['XINYAN_RADAR_C22180001'] if dict_add['XINYAN_RADAR_C22180001']>0 else np.nan
    #近1个月履约贷款次均/网贷建议授信额度
    dict_add['XINYAN_RADAR_amt_per_pay_m01vsC22180001']=dict_add['XINYAN_RADAR_amt_per_pay_m01']/dict_add['XINYAN_RADAR_C22180001'] if dict_add['XINYAN_RADAR_C22180001']>0 else np.nan
    #近3个月履约贷款次均/网贷建议授信额度
    dict_add['XINYAN_RADAR_amt_per_pay_m03vsC22180001']=dict_add['XINYAN_RADAR_amt_per_pay_m03']/dict_add['XINYAN_RADAR_C22180001'] if dict_add['XINYAN_RADAR_C22180001']>0 else np.nan
    #近6个月履约贷款次均/网贷建议授信额度
    dict_add['XINYAN_RADAR_amt_per_pay_m06vsC22180001']=dict_add['XINYAN_RADAR_amt_per_pay_m06']/dict_add['XINYAN_RADAR_C22180001'] if dict_add['XINYAN_RADAR_C22180001']>0 else np.nan
    #近12个月履约贷款次均/网贷建议授信额度
    dict_add['XINYAN_RADAR_amt_per_pay_m12vsC22180001']=dict_add['XINYAN_RADAR_amt_per_pay_m12']/dict_add['XINYAN_RADAR_C22180001'] if dict_add['XINYAN_RADAR_C22180001']>0 else np.nan
    #近24个月履约贷款次均/网贷建议授信额度
    dict_add['XINYAN_RADAR_amt_per_pay_m24vsC22180001']=dict_add['XINYAN_RADAR_amt_per_pay_m24']/dict_add['XINYAN_RADAR_C22180001'] if dict_add['XINYAN_RADAR_C22180001']>0 else np.nan
    #近6个月逾期件均/网贷建议授信额度
    dict_add['XINYAN_RADAR_amt_per_ove_m06vsC22180001']=dict_add['XINYAN_RADAR_amt_per_ove_m06']/dict_add['XINYAN_RADAR_C22180001'] if dict_add['XINYAN_RADAR_C22180001']>0 else np.nan
    #近12个月逾期件均/网贷建议授信额度
    dict_add['XINYAN_RADAR_amt_per_ove_m12vsC22180001']=dict_add['XINYAN_RADAR_amt_per_ove_m12']/dict_add['XINYAN_RADAR_C22180001'] if dict_add['XINYAN_RADAR_C22180001']>0 else np.nan
    #近24个月逾期件均/网贷建议授信额度
    dict_add['XINYAN_RADAR_amt_per_ove_m24vsC22180001']=dict_add['XINYAN_RADAR_amt_per_ove_m24']/dict_add['XINYAN_RADAR_C22180001'] if dict_add['XINYAN_RADAR_C22180001']>0 else np.nan
    
    #网贷建议授信额度*网贷额度置信度/100
    dict_add['XINYAN_RADAR_credit_amt']=dict_add['XINYAN_RADAR_C22180001']*dict_add['XINYAN_RADAR_C22180002']/100
    
    #近24个月履约贷款总金额/网贷置信授信额度
    dict_add['XINYAN_RADAR_B22170044vscredit_amt']=dict_add['XINYAN_RADAR_B22170044_num']/dict_add['XINYAN_RADAR_credit_amt'] if dict_add['XINYAN_RADAR_credit_amt']>0 else np.nan
    #近12个月履约贷款总金额/网贷置信授信额度
    dict_add['XINYAN_RADAR_B22170043vscredit_amt']=dict_add['XINYAN_RADAR_B22170043_num']/dict_add['XINYAN_RADAR_credit_amt'] if dict_add['XINYAN_RADAR_credit_amt']>0 else np.nan
    #近6个月履约贷款总金额/网贷置信授信额度
    dict_add['XINYAN_RADAR_B22170042vscredit_amt']=dict_add['XINYAN_RADAR_B22170042_num']/dict_add['XINYAN_RADAR_credit_amt'] if dict_add['XINYAN_RADAR_credit_amt']>0 else np.nan
    #近3个月履约贷款总金额/网贷置信授信额度
    dict_add['XINYAN_RADAR_B22170041vscredit_amt']=dict_add['XINYAN_RADAR_B22170041_num']/dict_add['XINYAN_RADAR_credit_amt'] if dict_add['XINYAN_RADAR_credit_amt']>0 else np.nan
    #近1个月履约贷款总金额/网贷置信授信额度
    dict_add['XINYAN_RADAR_B22170040vscredit_amt']=dict_add['XINYAN_RADAR_B22170040_num']/dict_add['XINYAN_RADAR_credit_amt'] if dict_add['XINYAN_RADAR_credit_amt']>0 else np.nan
    #近24个月累计逾期金额/网贷置信授信额度
    dict_add['XINYAN_RADAR_B22170033vscredit_amt']=dict_add['XINYAN_RADAR_B22170033_num']/dict_add['XINYAN_RADAR_credit_amt'] if dict_add['XINYAN_RADAR_credit_amt']>0 else np.nan
    #近12个月累计逾期金额/网贷置信授信额度
    dict_add['XINYAN_RADAR_B22170032vscredit_amt']=dict_add['XINYAN_RADAR_B22170032_num']/dict_add['XINYAN_RADAR_credit_amt'] if dict_add['XINYAN_RADAR_credit_amt']>0 else np.nan
    #近06个月累计逾期金额/网贷置信授信额度
    dict_add['XINYAN_RADAR_B22170031vscredit_amt']=dict_add['XINYAN_RADAR_B22170031_num']/dict_add['XINYAN_RADAR_credit_amt'] if dict_add['XINYAN_RADAR_credit_amt']>0 else np.nan
    #近24个月贷款总金额/网贷置信授信额度
    dict_add['XINYAN_RADAR_B22170011vscredit_amt']=dict_add['XINYAN_RADAR_B22170011_num']/dict_add['XINYAN_RADAR_credit_amt'] if dict_add['XINYAN_RADAR_credit_amt']>0 else np.nan
    #近12个月贷款总金额/网贷置信授信额度
    dict_add['XINYAN_RADAR_B22170010vscredit_amt']=dict_add['XINYAN_RADAR_B22170010_num']/dict_add['XINYAN_RADAR_credit_amt'] if dict_add['XINYAN_RADAR_credit_amt']>0 else np.nan
    #近6个月贷款总金额/网贷置信授信额度
    dict_add['XINYAN_RADAR_B22170009vscredit_amt']=dict_add['XINYAN_RADAR_B22170009_num']/dict_add['XINYAN_RADAR_credit_amt'] if dict_add['XINYAN_RADAR_credit_amt']>0 else np.nan
    #近3个月贷款总金额/网贷置信授信额度
    dict_add['XINYAN_RADAR_B22170008vscredit_amt']=dict_add['XINYAN_RADAR_B22170008_num']/dict_add['XINYAN_RADAR_credit_amt'] if dict_add['XINYAN_RADAR_credit_amt']>0 else np.nan
    #近1个月贷款总金额/网贷置信授信额度
    dict_add['XINYAN_RADAR_B22170007vscredit_amt']=dict_add['XINYAN_RADAR_B22170007_num']/dict_add['XINYAN_RADAR_credit_amt'] if dict_add['XINYAN_RADAR_credit_amt']>0 else np.nan
    #近1个月件均/网贷置信授信额度
    dict_add['XINYAN_RADAR_amt_per_m01vscredit_amt']=dict_add['XINYAN_RADAR_amt_per_m01']/dict_add['XINYAN_RADAR_credit_amt'] if dict_add['XINYAN_RADAR_credit_amt']>0 else np.nan
    #近3个月件均/网贷置信授信额度
    dict_add['XINYAN_RADAR_amt_per_m03vscredit_amt']=dict_add['XINYAN_RADAR_amt_per_m03']/dict_add['XINYAN_RADAR_credit_amt'] if dict_add['XINYAN_RADAR_credit_amt']>0 else np.nan
    #近6个月件均/网贷置信授信额度
    dict_add['XINYAN_RADAR_amt_per_m06vscredit_amt']=dict_add['XINYAN_RADAR_amt_per_m06']/dict_add['XINYAN_RADAR_credit_amt'] if dict_add['XINYAN_RADAR_credit_amt']>0 else np.nan
    #近12个月件均/网贷置信授信额度
    dict_add['XINYAN_RADAR_amt_per_m12vscredit_amt']=dict_add['XINYAN_RADAR_amt_per_m12']/dict_add['XINYAN_RADAR_credit_amt'] if dict_add['XINYAN_RADAR_credit_amt']>0 else np.nan
    #近24个月件均/网贷置信授信额度
    dict_add['XINYAN_RADAR_amt_per_m24vscredit_amt']=dict_add['XINYAN_RADAR_amt_per_m24']/dict_add['XINYAN_RADAR_credit_amt'] if dict_add['XINYAN_RADAR_credit_amt']>0 else np.nan
    #近1个月履约贷款次均/网贷置信授信额度
    dict_add['XINYAN_RADAR_amt_per_pay_m01vscredit_amt']=dict_add['XINYAN_RADAR_amt_per_pay_m01']/dict_add['XINYAN_RADAR_credit_amt'] if dict_add['XINYAN_RADAR_credit_amt']>0 else np.nan
    #近3个月履约贷款次均/网贷置信授信额度
    dict_add['XINYAN_RADAR_amt_per_pay_m03vscredit_amt']=dict_add['XINYAN_RADAR_amt_per_pay_m03']/dict_add['XINYAN_RADAR_credit_amt'] if dict_add['XINYAN_RADAR_credit_amt']>0 else np.nan
    #近6个月履约贷款次均/网贷置信授信额度
    dict_add['XINYAN_RADAR_amt_per_pay_m06vscredit_amt']=dict_add['XINYAN_RADAR_amt_per_pay_m06']/dict_add['XINYAN_RADAR_credit_amt'] if dict_add['XINYAN_RADAR_credit_amt']>0 else np.nan
    #近12个月履约贷款次均/网贷置信授信额度
    dict_add['XINYAN_RADAR_amt_per_pay_m12vscredit_amt']=dict_add['XINYAN_RADAR_amt_per_pay_m12']/dict_add['XINYAN_RADAR_credit_amt'] if dict_add['XINYAN_RADAR_credit_amt']>0 else np.nan
    #近24个月履约贷款次均/网贷置信授信额度
    dict_add['XINYAN_RADAR_amt_per_pay_m24vscredit_amt']=dict_add['XINYAN_RADAR_amt_per_pay_m24']/dict_add['XINYAN_RADAR_credit_amt'] if dict_add['XINYAN_RADAR_credit_amt']>0 else np.nan
    #近6个月逾期件均/网贷置信授信额度
    dict_add['XINYAN_RADAR_amt_per_ove_m06vscredit_amt']=dict_add['XINYAN_RADAR_amt_per_ove_m06']/dict_add['XINYAN_RADAR_credit_amt'] if dict_add['XINYAN_RADAR_credit_amt']>0 else np.nan
    #近12个月逾期件均/网贷置信授信额度
    dict_add['XINYAN_RADAR_amt_per_ove_m12vscredit_amt']=dict_add['XINYAN_RADAR_amt_per_ove_m12']/dict_add['XINYAN_RADAR_credit_amt'] if dict_add['XINYAN_RADAR_credit_amt']>0 else np.nan
    #近24个月逾期件均/网贷置信授信额度
    dict_add['XINYAN_RADAR_amt_per_ove_m24vscredit_amt']=dict_add['XINYAN_RADAR_amt_per_ove_m24']/dict_add['XINYAN_RADAR_credit_amt'] if dict_add['XINYAN_RADAR_credit_amt']>0 else np.nan
    
    #网络贷款类机构数/网络贷款类产品数
    dict_add['XINYAN_RADAR_C22180003vsC22180004']=dict_add['XINYAN_RADAR_C22180003']/dict_add['XINYAN_RADAR_C22180004'] if dict_add['XINYAN_RADAR_C22180004']>0 else np.nan
    
    #查询网络贷款类机构数/网络贷款类机构数
    dict_add['XINYAN_RADAR_A22160005vsC22180003']=dict_add['XINYAN_RADAR_A22160005']/dict_add['XINYAN_RADAR_C22180003'] if dict_add['XINYAN_RADAR_C22180003']>0 else np.nan
    #近1个月贷款机构数/网络贷款类机构数
    dict_add['XINYAN_RADAR_B22170016vsC22180003']=dict_add['XINYAN_RADAR_B22170016']/dict_add['XINYAN_RADAR_C22180003'] if dict_add['XINYAN_RADAR_C22180003']>0 else np.nan
    #近3个月贷款机构数/网络贷款类机构数
    dict_add['XINYAN_RADAR_B22170017vsC22180003']=dict_add['XINYAN_RADAR_B22170017']/dict_add['XINYAN_RADAR_C22180003'] if dict_add['XINYAN_RADAR_C22180003']>0 else np.nan
    #近6个月贷款机构数/网络贷款类机构数
    dict_add['XINYAN_RADAR_B22170018vsC22180003']=dict_add['XINYAN_RADAR_B22170018']/dict_add['XINYAN_RADAR_C22180003'] if dict_add['XINYAN_RADAR_C22180003']>0 else np.nan
    #近12个月贷款机构数/网络贷款类机构数
    dict_add['XINYAN_RADAR_B22170019vsC22180003']=dict_add['XINYAN_RADAR_B22170019']/dict_add['XINYAN_RADAR_C22180003'] if dict_add['XINYAN_RADAR_C22180003']>0 else np.nan
    #近24个月贷款机构数/网络贷款类机构数
    dict_add['XINYAN_RADAR_B22170020vsC22180003']=dict_add['XINYAN_RADAR_B22170020']/dict_add['XINYAN_RADAR_C22180003'] if dict_add['XINYAN_RADAR_C22180003']>0 else np.nan
    
    #近12个月网贷类贷款机构数/网络贷款类机构数
    dict_add['XINYAN_RADAR_B22170023vsC22180003']=dict_add['XINYAN_RADAR_B22170023']/dict_add['XINYAN_RADAR_C22180003'] if dict_add['XINYAN_RADAR_C22180003']>0 else np.nan
    #近24个月网贷类贷款机构数/网络贷款类机构数
    dict_add['XINYAN_RADAR_B22170024vsC22180003']=dict_add['XINYAN_RADAR_B22170024']/dict_add['XINYAN_RADAR_C22180003'] if dict_add['XINYAN_RADAR_C22180003']>0 else np.nan
    
    #网贷建议授信额度/网络贷款机构最大授信额度
    dict_add['XINYAN_RADAR_C22180001vsC22180005']=dict_add['XINYAN_RADAR_C22180001']/dict_add['XINYAN_RADAR_C22180005'] if dict_add['XINYAN_RADAR_C22180005']>0 else np.nan
    
    
    
    #消金贷款类机构数/消金贷款类产品数
    dict_add['XINYAN_RADAR_C22180007vsC22180008']=dict_add['XINYAN_RADAR_C22180007']/dict_add['XINYAN_RADAR_C22180008'] if dict_add['XINYAN_RADAR_C22180008']>0 else np.nan
    
    #查询网络贷款类机构数/消金贷款类机构数
    dict_add['XINYAN_RADAR_A22160004vsC22180007']=dict_add['XINYAN_RADAR_A22160004']/dict_add['XINYAN_RADAR_C22180007'] if dict_add['XINYAN_RADAR_C22180007']>0 else np.nan
    #近1个月贷款机构数/消金贷款类机构数
    dict_add['XINYAN_RADAR_B22170016vsC22180007']=dict_add['XINYAN_RADAR_B22170016']/dict_add['XINYAN_RADAR_C22180007'] if dict_add['XINYAN_RADAR_C22180007']>0 else np.nan
    #近3个月贷款机构数/消金贷款类机构数
    dict_add['XINYAN_RADAR_B22170017vsC22180007']=dict_add['XINYAN_RADAR_B22170017']/dict_add['XINYAN_RADAR_C22180007'] if dict_add['XINYAN_RADAR_C22180007']>0 else np.nan
    #近6个月贷款机构数/消金贷款类机构数
    dict_add['XINYAN_RADAR_B22170018vsC22180007']=dict_add['XINYAN_RADAR_B22170018']/dict_add['XINYAN_RADAR_C22180007'] if dict_add['XINYAN_RADAR_C22180007']>0 else np.nan
    #近12个月贷款机构数/消金贷款类机构数
    dict_add['XINYAN_RADAR_B22170019vsC22180007']=dict_add['XINYAN_RADAR_B22170019']/dict_add['XINYAN_RADAR_C22180007'] if dict_add['XINYAN_RADAR_C22180007']>0 else np.nan
    #近24个月贷款机构数/消金贷款类机构数
    dict_add['XINYAN_RADAR_B22170020vsC22180007']=dict_add['XINYAN_RADAR_B22170020']/dict_add['XINYAN_RADAR_C22180007'] if dict_add['XINYAN_RADAR_C22180007']>0 else np.nan

    #近12个月消金贷款机构数/消金贷款类机构数
    dict_add['XINYAN_RADAR_B22170021vsC22180007']=dict_add['XINYAN_RADAR_B22170021']/dict_add['XINYAN_RADAR_C22180007'] if dict_add['XINYAN_RADAR_C22180007']>0 else np.nan
    #近24个月消金贷款机构数/消金贷款类机构数
    dict_add['XINYAN_RADAR_B22170022vsC22180007']=dict_add['XINYAN_RADAR_B22170022']/dict_add['XINYAN_RADAR_C22180007'] if dict_add['XINYAN_RADAR_C22180007']>0 else np.nan
    
    
    
    
    
    
    return dict_add 


def XINYAN_RADAR_feature_func(dict_add,json_dict,appl_time):
    data=json_dict['data'] if 'data' in json_dict.keys() and type(json_dict['data'])==dict  else dict()
    result_detail=data['result_detail'] if 'result_detail' in data.keys() and type(data['result_detail'])==dict  else dict()
    apply_report_detail=result_detail['apply_report_detail'] if 'apply_report_detail' in result_detail.keys() and type(result_detail['apply_report_detail'])==dict  else dict()
    behavior_report_detail=result_detail['behavior_report_detail'] if 'behavior_report_detail' in result_detail.keys() and type(result_detail['behavior_report_detail'])==dict  else dict()
    current_report_detail=result_detail['current_report_detail'] if 'current_report_detail' in result_detail.keys() and type(result_detail['current_report_detail'])==dict  else dict()
    dict_add['XINYAN_RADAR_A22160001'] = float(apply_report_detail['A22160001']) if 'A22160001' in apply_report_detail.keys() else np.nan  #申请准入分
    dict_add['XINYAN_RADAR_A22160002'] = float(apply_report_detail['A22160002']) if 'A22160002' in apply_report_detail.keys() else np.nan  #申请准入置信度
    dict_add['XINYAN_RADAR_A22160003'] = float(apply_report_detail['A22160003']) if 'A22160003' in apply_report_detail.keys() else np.nan  #查询机构数
    dict_add['XINYAN_RADAR_A22160004'] = float(apply_report_detail['A22160004']) if 'A22160004' in apply_report_detail.keys() else np.nan  #查询消费金融类机构数
    dict_add['XINYAN_RADAR_A22160005'] = float(apply_report_detail['A22160005']) if 'A22160005' in apply_report_detail.keys() else np.nan  #查询网络贷款类机构数
    dict_add['XINYAN_RADAR_A22160006'] = float(apply_report_detail['A22160006']) if 'A22160006' in apply_report_detail.keys() else np.nan  #总查询次数
    dict_add['XINYAN_RADAR_A22160007'] = apply_report_detail['A22160007'] if 'A22160007' in apply_report_detail.keys() else np.nan  #最近查询时间
    dict_add['XINYAN_RADAR_A22160008'] = float(apply_report_detail['A22160008']) if 'A22160008' in apply_report_detail.keys() else np.nan  #近1个月总查询笔数
    dict_add['XINYAN_RADAR_A22160009'] = float(apply_report_detail['A22160009']) if 'A22160009' in apply_report_detail.keys() else np.nan  #近3个月总查询笔数
    dict_add['XINYAN_RADAR_A22160010'] = float(apply_report_detail['A22160010']) if 'A22160010' in apply_report_detail.keys() else np.nan  #近6个月总查询笔数
    
    dict_add['XINYAN_RADAR_B22170001'] = float(behavior_report_detail['B22170001']) if 'B22170001' in behavior_report_detail.keys() else np.nan  #贷款行为分
    dict_add['XINYAN_RADAR_B22170002'] = float(behavior_report_detail['B22170002']) if 'B22170002' in behavior_report_detail.keys() else np.nan  #近1个月贷款笔数
    dict_add['XINYAN_RADAR_B22170003'] = float(behavior_report_detail['B22170003']) if 'B22170003' in behavior_report_detail.keys() else np.nan  #近3个月贷款笔数
    dict_add['XINYAN_RADAR_B22170004'] = float(behavior_report_detail['B22170004']) if 'B22170004' in behavior_report_detail.keys() else np.nan  #近6个月贷款笔数
    dict_add['XINYAN_RADAR_B22170005'] = float(behavior_report_detail['B22170005']) if 'B22170005' in behavior_report_detail.keys() else np.nan  #近12个月贷款笔数
    dict_add['XINYAN_RADAR_B22170006'] = float(behavior_report_detail['B22170006']) if 'B22170006' in behavior_report_detail.keys() else np.nan  #近24个月贷款笔数
    dict_add['XINYAN_RADAR_B22170007'] = behavior_report_detail['B22170007'] if 'B22170007' in behavior_report_detail.keys() else np.nan  #近1个月贷款总金额
    dict_add['XINYAN_RADAR_B22170008'] = behavior_report_detail['B22170008'] if 'B22170008' in behavior_report_detail.keys() else np.nan  #近3个月贷款总金额
    dict_add['XINYAN_RADAR_B22170009'] = behavior_report_detail['B22170009'] if 'B22170009' in behavior_report_detail.keys() else np.nan  #近6个月贷款总金额
    dict_add['XINYAN_RADAR_B22170010'] = behavior_report_detail['B22170010'] if 'B22170010' in behavior_report_detail.keys() else np.nan  #近12个月贷款总金额
    dict_add['XINYAN_RADAR_B22170011'] = behavior_report_detail['B22170011'] if 'B22170011' in behavior_report_detail.keys() else np.nan  #近24个月贷款总金额
    dict_add['XINYAN_RADAR_B22170012'] = float(behavior_report_detail['B22170012']) if 'B22170012' in behavior_report_detail.keys() else np.nan  #近12个月贷款金额在1k及以下的笔数
    dict_add['XINYAN_RADAR_B22170013'] = float(behavior_report_detail['B22170013']) if 'B22170013' in behavior_report_detail.keys() else np.nan  #近12个月贷款金额在1k-3k的笔数
    dict_add['XINYAN_RADAR_B22170014'] = float(behavior_report_detail['B22170014']) if 'B22170014' in behavior_report_detail.keys() else np.nan  #近12个月贷款金额在3k-10k的笔数
    dict_add['XINYAN_RADAR_B22170015'] = float(behavior_report_detail['B22170015']) if 'B22170015' in behavior_report_detail.keys() else np.nan  #近12个月贷款金额在1w以上的笔数
    dict_add['XINYAN_RADAR_B22170016'] = float(behavior_report_detail['B22170016']) if 'B22170016' in behavior_report_detail.keys() else np.nan  #近1个月贷款机构数
    dict_add['XINYAN_RADAR_B22170017'] = float(behavior_report_detail['B22170017']) if 'B22170017' in behavior_report_detail.keys() else np.nan  #近3个月贷款机构数
    dict_add['XINYAN_RADAR_B22170018'] = float(behavior_report_detail['B22170018']) if 'B22170018' in behavior_report_detail.keys() else np.nan  #近6个月贷款机构数
    dict_add['XINYAN_RADAR_B22170019'] = float(behavior_report_detail['B22170019']) if 'B22170019' in behavior_report_detail.keys() else np.nan  #近12个月贷款机构数
    dict_add['XINYAN_RADAR_B22170020'] = float(behavior_report_detail['B22170020']) if 'B22170020' in behavior_report_detail.keys() else np.nan  #近24个月贷款机构数
    dict_add['XINYAN_RADAR_B22170021'] = float(behavior_report_detail['B22170021']) if 'B22170021' in behavior_report_detail.keys() else np.nan  #近12个月消金类贷款机构数
    dict_add['XINYAN_RADAR_B22170022'] = float(behavior_report_detail['B22170022']) if 'B22170022' in behavior_report_detail.keys() else np.nan  #近24个月消金类贷款机构数
    dict_add['XINYAN_RADAR_B22170023'] = float(behavior_report_detail['B22170023']) if 'B22170023' in behavior_report_detail.keys() else np.nan  #近12个月网贷类贷款机构数
    dict_add['XINYAN_RADAR_B22170024'] = float(behavior_report_detail['B22170024']) if 'B22170024' in behavior_report_detail.keys() else np.nan  #近24个月网贷类贷款机构数
    dict_add['XINYAN_RADAR_B22170025'] = float(behavior_report_detail['B22170025']) if 'B22170025' in behavior_report_detail.keys() else np.nan  #近6个月M0+逾期贷款笔数
    dict_add['XINYAN_RADAR_B22170026'] = float(behavior_report_detail['B22170026']) if 'B22170026' in behavior_report_detail.keys() else np.nan  #近12个月M0+逾期贷款笔数
    dict_add['XINYAN_RADAR_B22170027'] = float(behavior_report_detail['B22170027']) if 'B22170027' in behavior_report_detail.keys() else np.nan  #近24个月M0+逾期贷款笔数
    dict_add['XINYAN_RADAR_B22170028'] = float(behavior_report_detail['B22170028']) if 'B22170028' in behavior_report_detail.keys() else np.nan  #近6个月M1+逾期贷款笔数
    dict_add['XINYAN_RADAR_B22170029'] = float(behavior_report_detail['B22170029']) if 'B22170029' in behavior_report_detail.keys() else np.nan  #近12个月M1+逾期贷款笔数
    dict_add['XINYAN_RADAR_B22170030'] = float(behavior_report_detail['B22170030']) if 'B22170030' in behavior_report_detail.keys() else np.nan  #近24个月M1+逾期贷款笔数
    dict_add['XINYAN_RADAR_B22170031'] = behavior_report_detail['B22170031'] if 'B22170031' in behavior_report_detail.keys() else np.nan  #近6个月累计逾期金额
    dict_add['XINYAN_RADAR_B22170032'] = behavior_report_detail['B22170032'] if 'B22170032' in behavior_report_detail.keys() else np.nan  #近12个月累计逾期金额
    dict_add['XINYAN_RADAR_B22170033'] = behavior_report_detail['B22170033'] if 'B22170033' in behavior_report_detail.keys() else np.nan  #近24个月累计逾期金额
    dict_add['XINYAN_RADAR_B22170034'] = float(re.findall('([0-9]*)%',behavior_report_detail['B22170034'])[0]) if 'B22170034' in behavior_report_detail.keys() and len(re.findall('([0-9]*)%',behavior_report_detail['B22170034']))>0 else np.nan  #正常还款订单数占贷款总订单数比例
    dict_add['XINYAN_RADAR_B22170035'] = float(behavior_report_detail['B22170035']) if 'B22170035' in behavior_report_detail.keys() else np.nan  #近1个月失败扣款笔数
    dict_add['XINYAN_RADAR_B22170036'] = float(behavior_report_detail['B22170036']) if 'B22170036' in behavior_report_detail.keys() else np.nan  #近3个月失败扣款笔数
    dict_add['XINYAN_RADAR_B22170037'] = float(behavior_report_detail['B22170037']) if 'B22170037' in behavior_report_detail.keys() else np.nan  #近6个月失败扣款笔数
    dict_add['XINYAN_RADAR_B22170038'] = float(behavior_report_detail['B22170038']) if 'B22170038' in behavior_report_detail.keys() else np.nan  #近12个月失败扣款笔数
    dict_add['XINYAN_RADAR_B22170039'] = float(behavior_report_detail['B22170039']) if 'B22170039' in behavior_report_detail.keys() else np.nan  #近24个月失败扣款笔数
    dict_add['XINYAN_RADAR_B22170040'] = behavior_report_detail['B22170040'] if 'B22170040' in behavior_report_detail.keys() else np.nan  #近1个月履约贷款总金额
    dict_add['XINYAN_RADAR_B22170041'] = behavior_report_detail['B22170041'] if 'B22170041' in behavior_report_detail.keys() else np.nan  #近3个月履约贷款总金额
    dict_add['XINYAN_RADAR_B22170042'] = behavior_report_detail['B22170042'] if 'B22170042' in behavior_report_detail.keys() else np.nan  #近6个月履约贷款总金额
    dict_add['XINYAN_RADAR_B22170043'] = behavior_report_detail['B22170043'] if 'B22170043' in behavior_report_detail.keys() else np.nan  #近12个月履约贷款总金额
    dict_add['XINYAN_RADAR_B22170044'] = behavior_report_detail['B22170044'] if 'B22170044' in behavior_report_detail.keys() else np.nan  #近24个月履约贷款总金额
    dict_add['XINYAN_RADAR_B22170045'] = float(behavior_report_detail['B22170045']) if 'B22170045' in behavior_report_detail.keys() else np.nan  #近1个月履约贷款次数
    dict_add['XINYAN_RADAR_B22170046'] = float(behavior_report_detail['B22170046']) if 'B22170046' in behavior_report_detail.keys() else np.nan  #近3个月履约贷款次数
    dict_add['XINYAN_RADAR_B22170047'] = float(behavior_report_detail['B22170047']) if 'B22170047' in behavior_report_detail.keys() else np.nan  #近6个月履约贷款次数
    dict_add['XINYAN_RADAR_B22170048'] = float(behavior_report_detail['B22170048']) if 'B22170048' in behavior_report_detail.keys() else np.nan  #近12个月履约贷款次数
    dict_add['XINYAN_RADAR_B22170049'] = float(behavior_report_detail['B22170049']) if 'B22170049' in behavior_report_detail.keys() else np.nan  #近24个月履约贷款次数
    dict_add['XINYAN_RADAR_B22170050'] = behavior_report_detail['B22170050'] if 'B22170050' in behavior_report_detail.keys() else np.nan  #最近一次履约距今天数
    dict_add['XINYAN_RADAR_B22170051'] = behavior_report_detail['B22170051'] if 'B22170051' in behavior_report_detail.keys() else np.nan  #贷款行为置信度
    dict_add['XINYAN_RADAR_B22170052'] = float(behavior_report_detail['B22170052']) if 'B22170052' in behavior_report_detail.keys() else np.nan  #贷款已结清订单数
    dict_add['XINYAN_RADAR_B22170053'] = float(behavior_report_detail['B22170053']) if 'B22170053' in behavior_report_detail.keys() else np.nan  #信用贷款时长
    dict_add['XINYAN_RADAR_B22170054'] = behavior_report_detail['B22170054'] if 'B22170054' in behavior_report_detail.keys() else np.nan  #最近一次贷款时间
    
    dict_add['XINYAN_RADAR_C22180001'] = float(current_report_detail['C22180001']) if 'C22180001' in current_report_detail.keys() else np.nan  #网贷建议授信额度
    dict_add['XINYAN_RADAR_C22180002'] = float(current_report_detail['C22180002']) if 'C22180002' in current_report_detail.keys() else np.nan  #网贷额度置信度
    dict_add['XINYAN_RADAR_C22180003'] = float(current_report_detail['C22180003']) if 'C22180003' in current_report_detail.keys() else np.nan  #网络贷款类机构数
    dict_add['XINYAN_RADAR_C22180004'] = float(current_report_detail['C22180004']) if 'C22180004' in current_report_detail.keys() else np.nan  #网络贷款类产品数
    dict_add['XINYAN_RADAR_C22180005'] = float(current_report_detail['C22180005']) if 'C22180005' in current_report_detail.keys() else np.nan  #网络贷款机构最大授信额度
    dict_add['XINYAN_RADAR_C22180006'] = float(current_report_detail['C22180006']) if 'C22180006' in current_report_detail.keys() else np.nan  #网络贷款机构平均授信额度
    dict_add['XINYAN_RADAR_C22180007'] = float(current_report_detail['C22180007']) if 'C22180007' in current_report_detail.keys() else np.nan  #消金贷款类机构数
    dict_add['XINYAN_RADAR_C22180008'] = float(current_report_detail['C22180008']) if 'C22180008' in current_report_detail.keys() else np.nan  #消金贷款类产品数
    dict_add['XINYAN_RADAR_C22180009'] = float(current_report_detail['C22180009']) if 'C22180009' in current_report_detail.keys() else np.nan  #消金贷款类机构最大授信额度
    dict_add['XINYAN_RADAR_C22180010'] = float(current_report_detail['C22180010']) if 'C22180010' in current_report_detail.keys() else np.nan  #消金贷款类机构平均授信额度
    dict_add['XINYAN_RADAR_C22180011'] = float(current_report_detail['C22180011']) if 'C22180011' in current_report_detail.keys() else np.nan  #消金建议授信额度
    dict_add['XINYAN_RADAR_C22180012'] = float(current_report_detail['C22180012']) if 'C22180012' in current_report_detail.keys() else np.nan  #消金额度置信度
    
    dict_add['XINYAN_RADAR_monthdelta_lastquery_apply']=monthdelta(dict_add['XINYAN_RADAR_A22160007'],appl_time)
    dict_add['XINYAN_RADAR_monthdelta_lastloan_apply']=monthdelta(dict_add['XINYAN_RADAR_B22170054'],appl_time)
    
    dict_add['XINYAN_RADAR_daysdelta_lastquery_apply']=daysdelta(dict_add['XINYAN_RADAR_A22160007'],appl_time)
    dict_add['XINYAN_RADAR_daysdelta_lastloan_apply']=daysdelta(dict_add['XINYAN_RADAR_B22170054'],appl_time)
    
    amt_dict=dict({'0':0,'(0,500)':100,'[500,1000)':500,'[1000,2000)':1000,'[2000,3000)':2000,'[3000,5000)':3000,
          '[5000,10000)':5000, '[10000,20000)':10000,'[20000,30000)':20000,'[30000,50000)':30000,'[50000,+)':50000})
    dict_add['XINYAN_RADAR_B22170007_num']=amt_dict[dict_add['XINYAN_RADAR_B22170007']] if dict_add['XINYAN_RADAR_B22170007'] in amt_dict.keys() else np.nan
    dict_add['XINYAN_RADAR_B22170008_num']=amt_dict[dict_add['XINYAN_RADAR_B22170008']] if dict_add['XINYAN_RADAR_B22170008'] in amt_dict.keys() else np.nan
    dict_add['XINYAN_RADAR_B22170009_num']=amt_dict[dict_add['XINYAN_RADAR_B22170009']] if dict_add['XINYAN_RADAR_B22170009'] in amt_dict.keys() else np.nan
    dict_add['XINYAN_RADAR_B22170010_num']=amt_dict[dict_add['XINYAN_RADAR_B22170010']] if dict_add['XINYAN_RADAR_B22170010'] in amt_dict.keys() else np.nan
    dict_add['XINYAN_RADAR_B22170011_num']=amt_dict[dict_add['XINYAN_RADAR_B22170011']] if dict_add['XINYAN_RADAR_B22170011'] in amt_dict.keys() else np.nan
    
    dict_add['XINYAN_RADAR_B22170031_num']=amt_dict[dict_add['XINYAN_RADAR_B22170031']] if dict_add['XINYAN_RADAR_B22170031'] in amt_dict.keys() else np.nan
    dict_add['XINYAN_RADAR_B22170032_num']=amt_dict[dict_add['XINYAN_RADAR_B22170032']] if dict_add['XINYAN_RADAR_B22170032'] in amt_dict.keys() else np.nan
    dict_add['XINYAN_RADAR_B22170033_num']=amt_dict[dict_add['XINYAN_RADAR_B22170033']] if dict_add['XINYAN_RADAR_B22170033'] in amt_dict.keys() else np.nan
    
    dict_add['XINYAN_RADAR_B22170040_num']=amt_dict[dict_add['XINYAN_RADAR_B22170040']] if dict_add['XINYAN_RADAR_B22170040'] in amt_dict.keys() else np.nan
    dict_add['XINYAN_RADAR_B22170041_num']=amt_dict[dict_add['XINYAN_RADAR_B22170041']] if dict_add['XINYAN_RADAR_B22170041'] in amt_dict.keys() else np.nan
    dict_add['XINYAN_RADAR_B22170042_num']=amt_dict[dict_add['XINYAN_RADAR_B22170042']] if dict_add['XINYAN_RADAR_B22170042'] in amt_dict.keys() else np.nan
    dict_add['XINYAN_RADAR_B22170043_num']=amt_dict[dict_add['XINYAN_RADAR_B22170043']] if dict_add['XINYAN_RADAR_B22170043'] in amt_dict.keys() else np.nan
    dict_add['XINYAN_RADAR_B22170044_num']=amt_dict[dict_add['XINYAN_RADAR_B22170044']] if dict_add['XINYAN_RADAR_B22170044'] in amt_dict.keys() else np.nan
    

    

    days_dict=dict({'0':0,'(0,7]':7,'(7,15]':15,'(15,30]':30,'(30,60]':60,'(60,90]':90,
          '(90,120]':120, '(120,150]':150,'(150,180]':180,'(180,360]':360,'(360,+)':720})
    dict_add['XINYAN_RADAR_B22170050_num']=days_dict[dict_add['XINYAN_RADAR_B22170050']] if dict_add['XINYAN_RADAR_B22170050'] in days_dict.keys() else np.nan
    
    
    
    #信用贷款时长-最近一次履约距今天数
    dict_add['XINYAN_RADAR_daysdelta_credit_to_pay']=float(dict_add['XINYAN_RADAR_B22170053'])-float(dict_add['XINYAN_RADAR_B22170050_num'])
    #信用贷款时长-最近一次贷款时间距今天数
    dict_add['XINYAN_RADAR_daysdelta_credit_to_loan']=float(dict_add['XINYAN_RADAR_B22170053'])-float(dict_add['XINYAN_RADAR_daysdelta_lastloan_apply'])
    #信用贷款时长-最近一次贷款时间距今天数
    dict_add['XINYAN_RADAR_daysdelta_credit_to_query']=float(dict_add['XINYAN_RADAR_B22170053'])-float(dict_add['XINYAN_RADAR_daysdelta_lastquery_apply'])
    #最近一次履约距今天数-最近一次贷款时间距今天数
    dict_add['XINYAN_RADAR_daysdelta_pay_to_loan']=float(dict_add['XINYAN_RADAR_B22170050_num'])-float(dict_add['XINYAN_RADAR_daysdelta_lastloan_apply'])
    #最近一次履约距今天数-最近查询时间距今天数
    dict_add['XINYAN_RADAR_daysdelta_pay_to_query']=float(dict_add['XINYAN_RADAR_B22170050_num'])-float(dict_add['XINYAN_RADAR_daysdelta_lastquery_apply'])
    #最近一次贷款时间距今天数-最近查询时间距今天数
    dict_add['XINYAN_RADAR_daysdelta_query_to_loan']=float(dict_add['XINYAN_RADAR_daysdelta_lastloan_apply'])-float(dict_add['XINYAN_RADAR_daysdelta_lastquery_apply'])
    

    #信用贷款时长/最近一次履约距今天数
    dict_add['XINYAN_RADAR_daysdelta_credit_to_pay']=dict_add['XINYAN_RADAR_B22170053']/dict_add['XINYAN_RADAR_B22170050_num'] if dict_add['XINYAN_RADAR_B22170050_num']>0 else np.nan
    #信用贷款时长/最近一次贷款时间距今天数
    dict_add['XINYAN_RADAR_daysdelta_credit_to_loan']=dict_add['XINYAN_RADAR_B22170053']/dict_add['XINYAN_RADAR_daysdelta_lastloan_apply'] if dict_add['XINYAN_RADAR_daysdelta_lastloan_apply']>0 else np.nan
    #信用贷款时长/最近一次贷款时间距今天数
    dict_add['XINYAN_RADAR_daysdelta_credit_to_query']=dict_add['XINYAN_RADAR_B22170053']/dict_add['XINYAN_RADAR_daysdelta_lastquery_apply']  if dict_add['XINYAN_RADAR_daysdelta_lastquery_apply']>0 else np.nan
    #最近一次履约距今天数/最近一次贷款时间距今天数
    dict_add['XINYAN_RADAR_daysdelta_pay_vs_loan']=dict_add['XINYAN_RADAR_B22170050_num']/dict_add['XINYAN_RADAR_daysdelta_lastloan_apply'] if dict_add['XINYAN_RADAR_daysdelta_lastloan_apply']>0 else np.nan
    #最近一次履约距今天数/最近查询时间距今天数
    dict_add['XINYAN_RADAR_daysdelta_pay_vs_query']=dict_add['XINYAN_RADAR_B22170050_num']/dict_add['XINYAN_RADAR_daysdelta_lastquery_apply'] if dict_add['XINYAN_RADAR_daysdelta_lastquery_apply']>0 else np.nan
    #最近一次贷款时间距今天数/最近查询时间距今天数
    dict_add['XINYAN_RADAR_daysdelta_query_vs_loan']=dict_add['XINYAN_RADAR_daysdelta_lastloan_apply']/dict_add['XINYAN_RADAR_daysdelta_lastquery_apply'] if dict_add['XINYAN_RADAR_daysdelta_lastloan_apply']>0 else np.nan
    
    
    
    #查询消费金融类机构数/查询机构数
    dict_add['XINYAN_RADAR_A22160004vsA22160003']=dict_add['XINYAN_RADAR_A22160004']/dict_add['XINYAN_RADAR_A22160003'] if dict_add['XINYAN_RADAR_A22160003']>0 else np.nan
    #查询网络贷款类机构数/查询机构数
    dict_add['XINYAN_RADAR_A22160005vsA22160003']=dict_add['XINYAN_RADAR_A22160005']/dict_add['XINYAN_RADAR_A22160003'] if dict_add['XINYAN_RADAR_A22160003']>0 else np.nan
    
    #近*个月总查询笔数/总查询次数
    dict_add['XINYAN_RADAR_A22160008vsA22160006']=dict_add['XINYAN_RADAR_A22160008']/dict_add['XINYAN_RADAR_A22160006'] if dict_add['XINYAN_RADAR_A22160006']>0 else np.nan
    dict_add['XINYAN_RADAR_A22160009vsA22160006']=dict_add['XINYAN_RADAR_A22160009']/dict_add['XINYAN_RADAR_A22160006'] if dict_add['XINYAN_RADAR_A22160006']>0 else np.nan
    dict_add['XINYAN_RADAR_A22160010vsA22160006']=dict_add['XINYAN_RADAR_A22160010']/dict_add['XINYAN_RADAR_A22160006'] if dict_add['XINYAN_RADAR_A22160006']>0 else np.nan
    #近*个月总查询笔数/近6个月总查询笔数
    dict_add['XINYAN_RADAR_A22160008vsA22160010']=dict_add['XINYAN_RADAR_A22160008']/dict_add['XINYAN_RADAR_A22160010'] if dict_add['XINYAN_RADAR_A22160010']>0 else np.nan
    dict_add['XINYAN_RADAR_A22160009vsA22160010']=dict_add['XINYAN_RADAR_A22160009']/dict_add['XINYAN_RADAR_A22160010'] if dict_add['XINYAN_RADAR_A22160010']>0 else np.nan
    #近*个月总查询笔数/近3个月总查询笔数
    dict_add['XINYAN_RADAR_A22160008vsA22160009']=dict_add['XINYAN_RADAR_A22160008']/dict_add['XINYAN_RADAR_A22160009'] if dict_add['XINYAN_RADAR_A22160009']>0 else np.nan
    
    #查询机构数/总查询次数
    dict_add['XINYAN_RADAR_A22160003vsA22160006']=dict_add['XINYAN_RADAR_A22160003']/dict_add['XINYAN_RADAR_A22160006'] if dict_add['XINYAN_RADAR_A22160006']>0 else np.nan
    
    #近*个月贷款笔数/近24个月贷款笔数
    dict_add['XINYAN_RADAR_B22170002vsB22170006']=dict_add['XINYAN_RADAR_B22170002']/dict_add['XINYAN_RADAR_B22170006'] if dict_add['XINYAN_RADAR_B22170006']>0 else np.nan
    dict_add['XINYAN_RADAR_B22170003vsB22170006']=dict_add['XINYAN_RADAR_B22170003']/dict_add['XINYAN_RADAR_B22170006'] if dict_add['XINYAN_RADAR_B22170006']>0 else np.nan
    dict_add['XINYAN_RADAR_B22170004vsB22170006']=dict_add['XINYAN_RADAR_B22170004']/dict_add['XINYAN_RADAR_B22170006'] if dict_add['XINYAN_RADAR_B22170006']>0 else np.nan
    dict_add['XINYAN_RADAR_B22170005vsB22170006']=dict_add['XINYAN_RADAR_B22170005']/dict_add['XINYAN_RADAR_B22170006'] if dict_add['XINYAN_RADAR_B22170006']>0 else np.nan
    #近*个月贷款笔数/近12个月贷款笔数
    dict_add['XINYAN_RADAR_B22170002vsB22170005']=dict_add['XINYAN_RADAR_B22170002']/dict_add['XINYAN_RADAR_B22170005'] if dict_add['XINYAN_RADAR_B22170005']>0 else np.nan
    dict_add['XINYAN_RADAR_B22170003vsB22170005']=dict_add['XINYAN_RADAR_B22170003']/dict_add['XINYAN_RADAR_B22170005'] if dict_add['XINYAN_RADAR_B22170005']>0 else np.nan
    dict_add['XINYAN_RADAR_B22170004vsB22170005']=dict_add['XINYAN_RADAR_B22170004']/dict_add['XINYAN_RADAR_B22170005'] if dict_add['XINYAN_RADAR_B22170005']>0 else np.nan
    #近*个月贷款笔数/近6个月贷款笔数
    dict_add['XINYAN_RADAR_B22170002vsB22170004']=dict_add['XINYAN_RADAR_B22170002']/dict_add['XINYAN_RADAR_B22170004'] if dict_add['XINYAN_RADAR_B22170004']>0 else np.nan
    dict_add['XINYAN_RADAR_B22170003vsB22170004']=dict_add['XINYAN_RADAR_B22170003']/dict_add['XINYAN_RADAR_B22170004'] if dict_add['XINYAN_RADAR_B22170004']>0 else np.nan
    #近*个月贷款笔数/近3个月贷款笔数
    dict_add['XINYAN_RADAR_B22170002vsB22170003']=dict_add['XINYAN_RADAR_B22170002']/dict_add['XINYAN_RADAR_B22170003'] if dict_add['XINYAN_RADAR_B22170003']>0 else np.nan
    
    #近*个月贷款金额/近24个月贷款金额
    dict_add['XINYAN_RADAR_B22170007_numvsB22170011_num']=dict_add['XINYAN_RADAR_B22170007_num']/dict_add['XINYAN_RADAR_B22170011_num'] if dict_add['XINYAN_RADAR_B22170011_num']>0 else np.nan
    dict_add['XINYAN_RADAR_B22170008_numvsB22170011_num']=dict_add['XINYAN_RADAR_B22170008_num']/dict_add['XINYAN_RADAR_B22170011_num'] if dict_add['XINYAN_RADAR_B22170011_num']>0 else np.nan
    dict_add['XINYAN_RADAR_B22170009_numvsB22170011_num']=dict_add['XINYAN_RADAR_B22170009_num']/dict_add['XINYAN_RADAR_B22170011_num'] if dict_add['XINYAN_RADAR_B22170011_num']>0 else np.nan
    dict_add['XINYAN_RADAR_B22170010_numvsB22170011_num']=dict_add['XINYAN_RADAR_B22170010_num']/dict_add['XINYAN_RADAR_B22170011_num'] if dict_add['XINYAN_RADAR_B22170011_num']>0 else np.nan
    #近*个月贷款金额/近12个月贷款金额
    dict_add['XINYAN_RADAR_B22170007_numvsB22170010_num']=dict_add['XINYAN_RADAR_B22170007_num']/dict_add['XINYAN_RADAR_B22170010_num'] if dict_add['XINYAN_RADAR_B22170010_num']>0 else np.nan
    dict_add['XINYAN_RADAR_B22170008_numvsB22170010_num']=dict_add['XINYAN_RADAR_B22170008_num']/dict_add['XINYAN_RADAR_B22170010_num'] if dict_add['XINYAN_RADAR_B22170010_num']>0 else np.nan
    dict_add['XINYAN_RADAR_B22170009_numvsB22170010_num']=dict_add['XINYAN_RADAR_B22170009_num']/dict_add['XINYAN_RADAR_B22170010_num'] if dict_add['XINYAN_RADAR_B22170010_num']>0 else np.nan
    #近*个月贷款金额/近6个月贷款金额
    dict_add['XINYAN_RADAR_B22170007_numvsB22170009_num']=dict_add['XINYAN_RADAR_B22170007_num']/dict_add['XINYAN_RADAR_B22170009_num'] if dict_add['XINYAN_RADAR_B22170009_num']>0 else np.nan
    dict_add['XINYAN_RADAR_B22170008_numvsB22170009_num']=dict_add['XINYAN_RADAR_B22170008_num']/dict_add['XINYAN_RADAR_B22170009_num'] if dict_add['XINYAN_RADAR_B22170009_num']>0 else np.nan
    #近*个月贷款金额/近3个月贷款金额
    dict_add['XINYAN_RADAR_B22170007_numvsB22170008_num']=dict_add['XINYAN_RADAR_B22170007_num']/dict_add['XINYAN_RADAR_B22170008_num'] if dict_add['XINYAN_RADAR_B22170008_num']>0 else np.nan
    
    #近1个月件均
    dict_add['XINYAN_RADAR_amt_per_m01']=dict_add['XINYAN_RADAR_B22170007_num']/dict_add['XINYAN_RADAR_B22170002'] if dict_add['XINYAN_RADAR_B22170002']>0 else np.nan
    #近3个月件均
    dict_add['XINYAN_RADAR_amt_per_m03']=dict_add['XINYAN_RADAR_B22170008_num']/dict_add['XINYAN_RADAR_B22170003'] if dict_add['XINYAN_RADAR_B22170003']>0 else np.nan
    #近6个月件均
    dict_add['XINYAN_RADAR_amt_per_m06']=dict_add['XINYAN_RADAR_B22170009_num']/dict_add['XINYAN_RADAR_B22170004'] if dict_add['XINYAN_RADAR_B22170004']>0 else np.nan
    #近12个月件均
    dict_add['XINYAN_RADAR_amt_per_m12']=dict_add['XINYAN_RADAR_B22170010_num']/dict_add['XINYAN_RADAR_B22170005'] if dict_add['XINYAN_RADAR_B22170005']>0 else np.nan
    #近24个月件均
    dict_add['XINYAN_RADAR_amt_per_m24']=dict_add['XINYAN_RADAR_B22170011_num']/dict_add['XINYAN_RADAR_B22170006'] if dict_add['XINYAN_RADAR_B22170006']>0 else np.nan
    
    #近1个月件均/近3个月件均
    dict_add['XINYAN_RADAR_amt_per_m01vm03']=dict_add['XINYAN_RADAR_amt_per_m01']/dict_add['XINYAN_RADAR_amt_per_m03'] if dict_add['XINYAN_RADAR_amt_per_m03']>0 else np.nan
    #近1个月件均/近6个月件均
    dict_add['XINYAN_RADAR_amt_per_m01vm06']=dict_add['XINYAN_RADAR_amt_per_m01']/dict_add['XINYAN_RADAR_amt_per_m06'] if dict_add['XINYAN_RADAR_amt_per_m06']>0 else np.nan
    #近1个月件均/近12个月件均
    dict_add['XINYAN_RADAR_amt_per_m01vm12']=dict_add['XINYAN_RADAR_amt_per_m01']/dict_add['XINYAN_RADAR_amt_per_m12'] if dict_add['XINYAN_RADAR_amt_per_m12']>0 else np.nan
    #近1个月件均/近24个月件均
    dict_add['XINYAN_RADAR_amt_per_m01vm24']=dict_add['XINYAN_RADAR_amt_per_m01']/dict_add['XINYAN_RADAR_amt_per_m24'] if dict_add['XINYAN_RADAR_amt_per_m24']>0 else np.nan
    #近3个月件均/近6个月件均
    dict_add['XINYAN_RADAR_amt_per_m03vm06']=dict_add['XINYAN_RADAR_amt_per_m03']/dict_add['XINYAN_RADAR_amt_per_m06'] if dict_add['XINYAN_RADAR_amt_per_m06']>0 else np.nan
    #近3个月件均/近12个月件均
    dict_add['XINYAN_RADAR_amt_per_m03vm12']=dict_add['XINYAN_RADAR_amt_per_m03']/dict_add['XINYAN_RADAR_amt_per_m12'] if dict_add['XINYAN_RADAR_amt_per_m12']>0 else np.nan
    #近3个月件均/近24个月件均
    dict_add['XINYAN_RADAR_amt_per_m03vm24']=dict_add['XINYAN_RADAR_amt_per_m03']/dict_add['XINYAN_RADAR_amt_per_m24'] if dict_add['XINYAN_RADAR_amt_per_m24']>0 else np.nan
    #近6个月件均/近12个月件均
    dict_add['XINYAN_RADAR_amt_per_m06vm12']=dict_add['XINYAN_RADAR_amt_per_m06']/dict_add['XINYAN_RADAR_amt_per_m12'] if dict_add['XINYAN_RADAR_amt_per_m12']>0 else np.nan
    #近6个月件均/近24个月件均
    dict_add['XINYAN_RADAR_amt_per_m06vm24']=dict_add['XINYAN_RADAR_amt_per_m06']/dict_add['XINYAN_RADAR_amt_per_m24'] if dict_add['XINYAN_RADAR_amt_per_m24']>0 else np.nan
    #近12个月件均/近24个月件均
    dict_add['XINYAN_RADAR_amt_per_m12vm24']=dict_add['XINYAN_RADAR_amt_per_m12']/dict_add['XINYAN_RADAR_amt_per_m24'] if dict_add['XINYAN_RADAR_amt_per_m24']>0 else np.nan
    
    #近12个月贷款金额在1k及以下的笔数/近12个月贷款笔数
    dict_add['XINYAN_RADAR_B22170012vsB22170005']=dict_add['XINYAN_RADAR_B22170012']/dict_add['XINYAN_RADAR_B22170005'] if dict_add['XINYAN_RADAR_B22170005']>0 else np.nan
    #近12个月贷款金额在1k-3k的笔数/近12个月贷款笔数
    dict_add['XINYAN_RADAR_B22170013vsB22170005']=dict_add['XINYAN_RADAR_B22170013']/dict_add['XINYAN_RADAR_B22170005'] if dict_add['XINYAN_RADAR_B22170005']>0 else np.nan
    #近12个月贷款金额在3k-10k的笔数
    dict_add['XINYAN_RADAR_B22170014vsB22170005']=dict_add['XINYAN_RADAR_B22170014']/dict_add['XINYAN_RADAR_B22170005'] if dict_add['XINYAN_RADAR_B22170005']>0 else np.nan
    #近12个月贷款金额在1w以上的笔数
    dict_add['XINYAN_RADAR_B22170015vsB22170005']=dict_add['XINYAN_RADAR_B22170015']/dict_add['XINYAN_RADAR_B22170005'] if dict_add['XINYAN_RADAR_B22170005']>0 else np.nan
    
    #近1个月贷款机构数/近3个月贷款机构数
    dict_add['XINYAN_RADAR_B22170016vsB22170017']=dict_add['XINYAN_RADAR_B22170016']/dict_add['XINYAN_RADAR_B22170017'] if dict_add['XINYAN_RADAR_B22170017']>0 else np.nan
    #近1个月贷款机构数/近6个月贷款机构数
    dict_add['XINYAN_RADAR_B22170016vsB22170018']=dict_add['XINYAN_RADAR_B22170016']/dict_add['XINYAN_RADAR_B22170018'] if dict_add['XINYAN_RADAR_B22170018']>0 else np.nan
    #近1个月贷款机构数/近12个月贷款机构数
    dict_add['XINYAN_RADAR_B22170016vsB22170019']=dict_add['XINYAN_RADAR_B22170016']/dict_add['XINYAN_RADAR_B22170019'] if dict_add['XINYAN_RADAR_B22170019']>0 else np.nan
    #近1个月贷款机构数/近24个月贷款机构数
    dict_add['XINYAN_RADAR_B22170016vsB22170020']=dict_add['XINYAN_RADAR_B22170016']/dict_add['XINYAN_RADAR_B22170020'] if dict_add['XINYAN_RADAR_B22170020']>0 else np.nan
    #近3个月贷款机构数/近6个月贷款机构数
    dict_add['XINYAN_RADAR_B22170017vsB22170018']=dict_add['XINYAN_RADAR_B22170017']/dict_add['XINYAN_RADAR_B22170018'] if dict_add['XINYAN_RADAR_B22170018']>0 else np.nan
    #近3个月贷款机构数/近12个月贷款机构数
    dict_add['XINYAN_RADAR_B22170017vsB22170019']=dict_add['XINYAN_RADAR_B22170017']/dict_add['XINYAN_RADAR_B22170019'] if dict_add['XINYAN_RADAR_B22170019']>0 else np.nan
    #近3个月贷款机构数/近24个月贷款机构数
    dict_add['XINYAN_RADAR_B22170017vsB22170020']=dict_add['XINYAN_RADAR_B22170017']/dict_add['XINYAN_RADAR_B22170020'] if dict_add['XINYAN_RADAR_B22170020']>0 else np.nan
    #近6个月贷款机构数/近12个月贷款机构数
    dict_add['XINYAN_RADAR_B22170018vsB22170019']=dict_add['XINYAN_RADAR_B22170018']/dict_add['XINYAN_RADAR_B22170019'] if dict_add['XINYAN_RADAR_B22170019']>0 else np.nan
    #近6个月贷款机构数/近24个月贷款机构数
    dict_add['XINYAN_RADAR_B22170018vsB22170020']=dict_add['XINYAN_RADAR_B22170018']/dict_add['XINYAN_RADAR_B22170020'] if dict_add['XINYAN_RADAR_B22170020']>0 else np.nan
    #近12个月贷款机构数/近24个月贷款机构数
    dict_add['XINYAN_RADAR_B22170019vsB22170020']=dict_add['XINYAN_RADAR_B22170019']/dict_add['XINYAN_RADAR_B22170020'] if dict_add['XINYAN_RADAR_B22170020']>0 else np.nan
    
    #近12个月消金类贷款机构数/近24个月消金类贷款机构数
    dict_add['XINYAN_RADAR_B22170021vsB22170022']=dict_add['XINYAN_RADAR_B22170021']/dict_add['XINYAN_RADAR_B22170022'] if dict_add['XINYAN_RADAR_B22170022']>0 else np.nan
    #近12个月消金类贷款机构数/近12个月贷款机构数
    dict_add['XINYAN_RADAR_B22170021vsB22170019']=dict_add['XINYAN_RADAR_B22170021']/dict_add['XINYAN_RADAR_B22170019'] if dict_add['XINYAN_RADAR_B22170019']>0 else np.nan
    #近24个月消金类贷款机构数/近24个月贷款机构数
    dict_add['XINYAN_RADAR_B22170022vsB22170020']=dict_add['XINYAN_RADAR_B22170022']/dict_add['XINYAN_RADAR_B22170020'] if dict_add['XINYAN_RADAR_B22170020']>0 else np.nan
    
    #近12个月网贷类贷款机构数/近24个月网贷类贷款机构数
    dict_add['XINYAN_RADAR_B22170023vsB22170024']=dict_add['XINYAN_RADAR_B22170023']/dict_add['XINYAN_RADAR_B22170024'] if dict_add['XINYAN_RADAR_B22170024']>0 else np.nan
    #近12个月网贷类贷款机构数/近12个月贷款机构数
    dict_add['XINYAN_RADAR_B22170023vsB22170019']=dict_add['XINYAN_RADAR_B22170023']/dict_add['XINYAN_RADAR_B22170019'] if dict_add['XINYAN_RADAR_B22170019']>0 else np.nan
    #近24个月网贷类贷款机构数/近24个月贷款机构数
    dict_add['XINYAN_RADAR_B22170024vsB22170020']=dict_add['XINYAN_RADAR_B22170024']/dict_add['XINYAN_RADAR_B22170020'] if dict_add['XINYAN_RADAR_B22170020']>0 else np.nan
    
    #近6个月M0+逾期贷款笔数/近12个月M0+逾期贷款笔数
    dict_add['XINYAN_RADAR_B22170025vsB22170026']=dict_add['XINYAN_RADAR_B22170025']/dict_add['XINYAN_RADAR_B22170026'] if dict_add['XINYAN_RADAR_B22170026']>0 else np.nan
    #近6个月M0+逾期贷款笔数/近24个月M0+逾期贷款笔数
    dict_add['XINYAN_RADAR_B22170025vsB22170027']=dict_add['XINYAN_RADAR_B22170025']/dict_add['XINYAN_RADAR_B22170027'] if dict_add['XINYAN_RADAR_B22170027']>0 else np.nan
    #近12个月M0+逾期贷款笔数/近24个月M0+逾期贷款笔数
    dict_add['XINYAN_RADAR_B22170026vsB22170027']=dict_add['XINYAN_RADAR_B22170026']/dict_add['XINYAN_RADAR_B22170027'] if dict_add['XINYAN_RADAR_B22170027']>0 else np.nan
    
    #近6个月M1+逾期贷款笔数/近12个月M1+逾期贷款笔数
    dict_add['XINYAN_RADAR_B22170028vsB22170029']=dict_add['XINYAN_RADAR_B22170028']/dict_add['XINYAN_RADAR_B22170029'] if dict_add['XINYAN_RADAR_B22170029']>0 else np.nan
    #近6个月M1+逾期贷款笔数/近24个月M1+逾期贷款笔数
    dict_add['XINYAN_RADAR_B22170028vsB22170030']=dict_add['XINYAN_RADAR_B22170028']/dict_add['XINYAN_RADAR_B22170030'] if dict_add['XINYAN_RADAR_B22170030']>0 else np.nan
    #近12个月M1+逾期贷款笔数/近24个月M1+逾期贷款笔数
    dict_add['XINYAN_RADAR_B22170029vsB22170030']=dict_add['XINYAN_RADAR_B22170029']/dict_add['XINYAN_RADAR_B22170030'] if dict_add['XINYAN_RADAR_B22170030']>0 else np.nan
    
    #近6个月M1+逾期贷款笔数/近6个月M0+逾期贷款笔数
    dict_add['XINYAN_RADAR_B22170028vsB22170025']=dict_add['XINYAN_RADAR_B22170028']/dict_add['XINYAN_RADAR_B22170025'] if dict_add['XINYAN_RADAR_B22170025']>0 else np.nan
    #近12个月M1+逾期贷款笔数/近12个月M0+逾期贷款笔数
    dict_add['XINYAN_RADAR_B22170029vsB22170026']=dict_add['XINYAN_RADAR_B22170029']/dict_add['XINYAN_RADAR_B22170026'] if dict_add['XINYAN_RADAR_B22170026']>0 else np.nan
    #近24个月M1+逾期贷款笔数/近24个月M0+逾期贷款笔数
    dict_add['XINYAN_RADAR_B22170030vsB22170027']=dict_add['XINYAN_RADAR_B22170030']/dict_add['XINYAN_RADAR_B22170027'] if dict_add['XINYAN_RADAR_B22170027']>0 else np.nan
    
    #近6个月累计逾期金额/近12个月累计逾期金额
    dict_add['XINYAN_RADAR_B22170031vsB22170032']=dict_add['XINYAN_RADAR_B22170031_num']/dict_add['XINYAN_RADAR_B22170032_num'] if dict_add['XINYAN_RADAR_B22170032_num']>0 else np.nan
    #近6个月累计逾期金额/近24个月累计逾期金额
    dict_add['XINYAN_RADAR_B22170031vsB22170033']=dict_add['XINYAN_RADAR_B22170031_num']/dict_add['XINYAN_RADAR_B22170033_num'] if dict_add['XINYAN_RADAR_B22170033_num']>0 else np.nan
    #近12个月累计逾期金额/近24个月累计逾期金额
    dict_add['XINYAN_RADAR_B22170032vsB22170033']=dict_add['XINYAN_RADAR_B22170032_num']/dict_add['XINYAN_RADAR_B22170033_num'] if dict_add['XINYAN_RADAR_B22170033_num']>0 else np.nan
    
    #近6个月逾期件均（近6个月累计逾期金额/近6个月M0+逾期贷款笔数）
    dict_add['XINYAN_RADAR_amt_per_ove_m06']=dict_add['XINYAN_RADAR_B22170031_num']/dict_add['XINYAN_RADAR_B22170025'] if dict_add['XINYAN_RADAR_B22170025']>0 else np.nan
    #近12个月逾期件均（近12个月累计逾期金额/近12个月M0+逾期贷款笔数）
    dict_add['XINYAN_RADAR_amt_per_ove_m12']=dict_add['XINYAN_RADAR_B22170032_num']/dict_add['XINYAN_RADAR_B22170026'] if dict_add['XINYAN_RADAR_B22170026']>0 else np.nan
    #近24个月逾期件均（近24个月累计逾期金额/近24个月M0+逾期贷款笔数）
    dict_add['XINYAN_RADAR_amt_per_ove_m24']=dict_add['XINYAN_RADAR_B22170033_num']/dict_add['XINYAN_RADAR_B22170027'] if dict_add['XINYAN_RADAR_B22170027']>0 else np.nan
    
    #近6个月逾期件均/近12个月逾期件均
    dict_add['XINYAN_RADAR_amt_per_ove_m06vm12']=dict_add['XINYAN_RADAR_amt_per_ove_m06']/dict_add['XINYAN_RADAR_amt_per_ove_m12'] if dict_add['XINYAN_RADAR_amt_per_ove_m12']>0 else np.nan
    #近6个月逾期件均/近24个月逾期件均
    dict_add['XINYAN_RADAR_amt_per_ove_m06vm24']=dict_add['XINYAN_RADAR_amt_per_ove_m06']/dict_add['XINYAN_RADAR_amt_per_ove_m24'] if dict_add['XINYAN_RADAR_amt_per_ove_m24']>0 else np.nan
    #近12个月逾期件均/近24个月逾期件均
    dict_add['XINYAN_RADAR_amt_per_ove_m12vm24']=dict_add['XINYAN_RADAR_amt_per_ove_m12']/dict_add['XINYAN_RADAR_amt_per_ove_m24'] if dict_add['XINYAN_RADAR_amt_per_ove_m24']>0 else np.nan
    
    #近6个月逾期件均/#近6个月件均
    dict_add['XINYAN_RADAR_amt_per_ove_m06vm06']=dict_add['XINYAN_RADAR_amt_per_ove_m06']/dict_add['XINYAN_RADAR_amt_per_m06'] if dict_add['XINYAN_RADAR_amt_per_m06']>0 else np.nan
    #近12个月逾期件均/#近12个月件均
    dict_add['XINYAN_RADAR_amt_per_ove_m12vm12']=dict_add['XINYAN_RADAR_amt_per_ove_m12']/dict_add['XINYAN_RADAR_amt_per_m12'] if dict_add['XINYAN_RADAR_amt_per_m12']>0 else np.nan
    #近24个月逾期件均/#近24个月件均
    dict_add['XINYAN_RADAR_amt_per_ove_m24vm24']=dict_add['XINYAN_RADAR_amt_per_ove_m24']/dict_add['XINYAN_RADAR_amt_per_m24'] if dict_add['XINYAN_RADAR_amt_per_m24']>0 else np.nan
    
    #近1个月失败扣款笔数/近3个月失败扣款笔数
    dict_add['XINYAN_RADAR_B22170035vsB22170036']=dict_add['XINYAN_RADAR_B22170035']/dict_add['XINYAN_RADAR_B22170036'] if dict_add['XINYAN_RADAR_B22170036']>0 else np.nan
    #近1个月失败扣款笔数/近6个月失败扣款笔数
    dict_add['XINYAN_RADAR_B22170035vsB22170037']=dict_add['XINYAN_RADAR_B22170035']/dict_add['XINYAN_RADAR_B22170037'] if dict_add['XINYAN_RADAR_B22170037']>0 else np.nan
    #近1个月失败扣款笔数/近12个月失败扣款笔数
    dict_add['XINYAN_RADAR_B22170035vsB22170038']=dict_add['XINYAN_RADAR_B22170035']/dict_add['XINYAN_RADAR_B22170038'] if dict_add['XINYAN_RADAR_B22170038']>0 else np.nan
    #近1个月失败扣款笔数/近24个月失败扣款笔数
    dict_add['XINYAN_RADAR_B22170035vsB22170039']=dict_add['XINYAN_RADAR_B22170035']/dict_add['XINYAN_RADAR_B22170039'] if dict_add['XINYAN_RADAR_B22170039']>0 else np.nan
    #近3个月失败扣款笔数/近6个月失败扣款笔数
    dict_add['XINYAN_RADAR_B22170036vsB22170037']=dict_add['XINYAN_RADAR_B22170036']/dict_add['XINYAN_RADAR_B22170037'] if dict_add['XINYAN_RADAR_B22170037']>0 else np.nan
    #近3个月失败扣款笔数/近12个月失败扣款笔数
    dict_add['XINYAN_RADAR_B22170036vsB22170038']=dict_add['XINYAN_RADAR_B22170036']/dict_add['XINYAN_RADAR_B22170038'] if dict_add['XINYAN_RADAR_B22170038']>0 else np.nan
    #近3个月失败扣款笔数/近24个月失败扣款笔数
    dict_add['XINYAN_RADAR_B22170036vsB22170039']=dict_add['XINYAN_RADAR_B22170036']/dict_add['XINYAN_RADAR_B22170039'] if dict_add['XINYAN_RADAR_B22170039']>0 else np.nan
    #近6个月失败扣款笔数/近12个月失败扣款笔数
    dict_add['XINYAN_RADAR_B22170037vsB22170038']=dict_add['XINYAN_RADAR_B22170037']/dict_add['XINYAN_RADAR_B22170038'] if dict_add['XINYAN_RADAR_B22170038']>0 else np.nan
    #近6个月失败扣款笔数/近24个月失败扣款笔数
    dict_add['XINYAN_RADAR_B22170037vsB22170039']=dict_add['XINYAN_RADAR_B22170037']/dict_add['XINYAN_RADAR_B22170039'] if dict_add['XINYAN_RADAR_B22170039']>0 else np.nan
    #近12个月失败扣款笔数/近24个月失败扣款笔数
    dict_add['XINYAN_RADAR_B22170038vsB22170039']=dict_add['XINYAN_RADAR_B22170038']/dict_add['XINYAN_RADAR_B22170039'] if dict_add['XINYAN_RADAR_B22170039']>0 else np.nan
    
    #近1个月失败扣款笔数/近1个月贷款笔数
    dict_add['XINYAN_RADAR_B22170035vsB22170002']=dict_add['XINYAN_RADAR_B22170035']/dict_add['XINYAN_RADAR_B22170002'] if dict_add['XINYAN_RADAR_B22170002']>0 else np.nan
    #近3个月失败扣款笔数/近3个月贷款笔数
    dict_add['XINYAN_RADAR_B22170036vsB22170003']=dict_add['XINYAN_RADAR_B22170036']/dict_add['XINYAN_RADAR_B22170003'] if dict_add['XINYAN_RADAR_B22170003']>0 else np.nan
    #近6个月失败扣款笔数/近6个月贷款笔数
    dict_add['XINYAN_RADAR_B22170037vsB22170004']=dict_add['XINYAN_RADAR_B22170037']/dict_add['XINYAN_RADAR_B22170004'] if dict_add['XINYAN_RADAR_B22170004']>0 else np.nan
    #近12个月失败扣款笔数/近12个月贷款笔数
    dict_add['XINYAN_RADAR_B22170038vsB22170005']=dict_add['XINYAN_RADAR_B22170038']/dict_add['XINYAN_RADAR_B22170005'] if dict_add['XINYAN_RADAR_B22170005']>0 else np.nan
    #近24个月失败扣款笔数/近24个月贷款笔数
    dict_add['XINYAN_RADAR_B22170039vsB22170006']=dict_add['XINYAN_RADAR_B22170039']/dict_add['XINYAN_RADAR_B22170006'] if dict_add['XINYAN_RADAR_B22170006']>0 else np.nan
    
    #近1个月履约贷款总金额/近3个月履约贷款总金额
    dict_add['XINYAN_RADAR_B22170040vsB22170041']=dict_add['XINYAN_RADAR_B22170040_num']/dict_add['XINYAN_RADAR_B22170041_num'] if dict_add['XINYAN_RADAR_B22170041_num']>0 else np.nan
    #近1个月履约贷款总金额/近6个月履约贷款总金额
    dict_add['XINYAN_RADAR_B22170040vsB22170042']=dict_add['XINYAN_RADAR_B22170040_num']/dict_add['XINYAN_RADAR_B22170042_num'] if dict_add['XINYAN_RADAR_B22170042_num']>0 else np.nan
    #近1个月履约贷款总金额/近12个月履约贷款总金额
    dict_add['XINYAN_RADAR_B22170040vsB22170043']=dict_add['XINYAN_RADAR_B22170040_num']/dict_add['XINYAN_RADAR_B22170043_num'] if dict_add['XINYAN_RADAR_B22170043_num']>0 else np.nan
    #近1个月履约贷款总金额/近24个月履约贷款总金额
    dict_add['XINYAN_RADAR_B22170040vsB22170044']=dict_add['XINYAN_RADAR_B22170040_num']/dict_add['XINYAN_RADAR_B22170044_num'] if dict_add['XINYAN_RADAR_B22170044_num']>0 else np.nan
    #近3个月履约贷款总金额/近6个月履约贷款总金额
    dict_add['XINYAN_RADAR_B22170041vsB22170042']=dict_add['XINYAN_RADAR_B22170041_num']/dict_add['XINYAN_RADAR_B22170042_num'] if dict_add['XINYAN_RADAR_B22170042_num']>0 else np.nan
    #近3个月履约贷款总金额/近12个月履约贷款总金额
    dict_add['XINYAN_RADAR_B22170041vsB22170043']=dict_add['XINYAN_RADAR_B22170041_num']/dict_add['XINYAN_RADAR_B22170043_num'] if dict_add['XINYAN_RADAR_B22170043_num']>0 else np.nan
    #近3个月履约贷款总金额/近24个月履约贷款总金额
    dict_add['XINYAN_RADAR_B22170041vsB22170044']=dict_add['XINYAN_RADAR_B22170041_num']/dict_add['XINYAN_RADAR_B22170044_num'] if dict_add['XINYAN_RADAR_B22170044_num']>0 else np.nan
    #近6个月履约贷款总金额/近12个月履约贷款总金额
    dict_add['XINYAN_RADAR_B22170042vsB22170043']=dict_add['XINYAN_RADAR_B22170042_num']/dict_add['XINYAN_RADAR_B22170043_num'] if dict_add['XINYAN_RADAR_B22170043_num']>0 else np.nan
    #近6个月履约贷款总金额/近24个月履约贷款总金额
    dict_add['XINYAN_RADAR_B22170042vsB22170044']=dict_add['XINYAN_RADAR_B22170042_num']/dict_add['XINYAN_RADAR_B22170044_num'] if dict_add['XINYAN_RADAR_B22170044_num']>0 else np.nan
    #近12个月履约贷款总金额/近24个月履约贷款总金额
    dict_add['XINYAN_RADAR_B22170043vsB22170044']=dict_add['XINYAN_RADAR_B22170043_num']/dict_add['XINYAN_RADAR_B22170044_num'] if dict_add['XINYAN_RADAR_B22170044_num']>0 else np.nan
    
    #近1个月履约贷款次数/近3个月履约贷款次数
    dict_add['XINYAN_RADAR_B22170045vsB22170046']=dict_add['XINYAN_RADAR_B22170045']/dict_add['XINYAN_RADAR_B22170046'] if dict_add['XINYAN_RADAR_B22170046']>0 else np.nan
    #近1个月履约贷款次数/近6个月履约贷款次数
    dict_add['XINYAN_RADAR_B22170045vsB22170047']=dict_add['XINYAN_RADAR_B22170045']/dict_add['XINYAN_RADAR_B22170047'] if dict_add['XINYAN_RADAR_B22170047']>0 else np.nan
    #近1个月履约贷款次数/近12个月履约贷款次数
    dict_add['XINYAN_RADAR_B22170045vsB22170048']=dict_add['XINYAN_RADAR_B22170045']/dict_add['XINYAN_RADAR_B22170048'] if dict_add['XINYAN_RADAR_B22170048']>0 else np.nan
    #近1个月履约贷款次数/近24个月履约贷款次数
    dict_add['XINYAN_RADAR_B22170045vsB22170049']=dict_add['XINYAN_RADAR_B22170045']/dict_add['XINYAN_RADAR_B22170049'] if dict_add['XINYAN_RADAR_B22170049']>0 else np.nan
    #近3个月履约贷款次数/近6个月履约贷款次数
    dict_add['XINYAN_RADAR_B22170046vsB22170047']=dict_add['XINYAN_RADAR_B22170046']/dict_add['XINYAN_RADAR_B22170047'] if dict_add['XINYAN_RADAR_B22170047']>0 else np.nan
    #近3个月履约贷款次数/近12个月履约贷款次数
    dict_add['XINYAN_RADAR_B22170046vsB22170048']=dict_add['XINYAN_RADAR_B22170046']/dict_add['XINYAN_RADAR_B22170048'] if dict_add['XINYAN_RADAR_B22170048']>0 else np.nan
    #近3个月履约贷款次数/近24个月履约贷款次数
    dict_add['XINYAN_RADAR_B22170046vsB22170049']=dict_add['XINYAN_RADAR_B22170046']/dict_add['XINYAN_RADAR_B22170049'] if dict_add['XINYAN_RADAR_B22170049']>0 else np.nan
    #近6个月履约贷款次数/近12个月履约贷款次数
    dict_add['XINYAN_RADAR_B22170047vsB22170048']=dict_add['XINYAN_RADAR_B22170047']/dict_add['XINYAN_RADAR_B22170048'] if dict_add['XINYAN_RADAR_B22170048']>0 else np.nan
    #近6个月履约贷款次数/近24个月履约贷款次数
    dict_add['XINYAN_RADAR_B22170047vsB22170049']=dict_add['XINYAN_RADAR_B22170047']/dict_add['XINYAN_RADAR_B22170049'] if dict_add['XINYAN_RADAR_B22170049']>0 else np.nan
    #近12个月履约贷款次数/近24个月履约贷款次数
    dict_add['XINYAN_RADAR_B22170048vsB22170049']=dict_add['XINYAN_RADAR_B22170048']/dict_add['XINYAN_RADAR_B22170049'] if dict_add['XINYAN_RADAR_B22170049']>0 else np.nan
    
    #近1个月履约贷款次均（近1个月履约贷款总金额/近1个月履约贷款次数）
    dict_add['XINYAN_RADAR_amt_per_pay_m01']=dict_add['XINYAN_RADAR_B22170040_num']/dict_add['XINYAN_RADAR_B22170045'] if dict_add['XINYAN_RADAR_B22170045']>0 else np.nan
    #近3个月履约贷款次均（近3个月履约贷款总金额/近3个月履约贷款次数）
    dict_add['XINYAN_RADAR_amt_per_pay_m03']=dict_add['XINYAN_RADAR_B22170041_num']/dict_add['XINYAN_RADAR_B22170046'] if dict_add['XINYAN_RADAR_B22170046']>0 else np.nan
    #近6个月履约贷款次均（近6个月履约贷款总金额/近6个月履约贷款次数）
    dict_add['XINYAN_RADAR_amt_per_pay_m06']=dict_add['XINYAN_RADAR_B22170042_num']/dict_add['XINYAN_RADAR_B22170047'] if dict_add['XINYAN_RADAR_B22170047']>0 else np.nan
    #近12个月履约贷款次均（近12个月履约贷款总金额/近12个月履约贷款次数）
    dict_add['XINYAN_RADAR_amt_per_pay_m12']=dict_add['XINYAN_RADAR_B22170043_num']/dict_add['XINYAN_RADAR_B22170048'] if dict_add['XINYAN_RADAR_B22170048']>0 else np.nan
    #近24个月履约贷款次均（近24个月履约贷款总金额/近24个月履约贷款次数）
    dict_add['XINYAN_RADAR_amt_per_pay_m24']=dict_add['XINYAN_RADAR_B22170044_num']/dict_add['XINYAN_RADAR_B22170049'] if dict_add['XINYAN_RADAR_B22170049']>0 else np.nan
    
    #近1个月履约贷款次均/近3个月履约贷款次均
    dict_add['XINYAN_RADAR_amt_per_pay_m01vm03']=dict_add['XINYAN_RADAR_amt_per_pay_m01']/dict_add['XINYAN_RADAR_amt_per_pay_m03'] if dict_add['XINYAN_RADAR_amt_per_pay_m03']>0 else np.nan
    #近1个月履约贷款次均/近6个月履约贷款次均
    dict_add['XINYAN_RADAR_amt_per_pay_m01vm06']=dict_add['XINYAN_RADAR_amt_per_pay_m01']/dict_add['XINYAN_RADAR_amt_per_pay_m06'] if dict_add['XINYAN_RADAR_amt_per_pay_m06']>0 else np.nan
    #近1个月履约贷款次均/近12个月履约贷款次均
    dict_add['XINYAN_RADAR_amt_per_pay_m01vm12']=dict_add['XINYAN_RADAR_amt_per_pay_m01']/dict_add['XINYAN_RADAR_amt_per_pay_m12'] if dict_add['XINYAN_RADAR_amt_per_pay_m12']>0 else np.nan
    #近1个月履约贷款次均/近24个月履约贷款次均
    dict_add['XINYAN_RADAR_amt_per_pay_m01vm24']=dict_add['XINYAN_RADAR_amt_per_pay_m01']/dict_add['XINYAN_RADAR_amt_per_pay_m24'] if dict_add['XINYAN_RADAR_amt_per_pay_m24']>0 else np.nan
    #近3个月履约贷款次均/近6个月履约贷款次均
    dict_add['XINYAN_RADAR_amt_per_pay_m03vm06']=dict_add['XINYAN_RADAR_amt_per_pay_m03']/dict_add['XINYAN_RADAR_amt_per_pay_m06'] if dict_add['XINYAN_RADAR_amt_per_pay_m06']>0 else np.nan
    #近3个月履约贷款次均/近12个月履约贷款次均
    dict_add['XINYAN_RADAR_amt_per_pay_m03vm12']=dict_add['XINYAN_RADAR_amt_per_pay_m03']/dict_add['XINYAN_RADAR_amt_per_pay_m12'] if dict_add['XINYAN_RADAR_amt_per_pay_m12']>0 else np.nan
    #近3个月履约贷款次均/近24个月履约贷款次均
    dict_add['XINYAN_RADAR_amt_per_pay_m03vm24']=dict_add['XINYAN_RADAR_amt_per_pay_m03']/dict_add['XINYAN_RADAR_amt_per_pay_m24'] if dict_add['XINYAN_RADAR_amt_per_pay_m24']>0 else np.nan
    #近6个月履约贷款次均/近12个月履约贷款次均
    dict_add['XINYAN_RADAR_amt_per_pay_m06vm12']=dict_add['XINYAN_RADAR_amt_per_pay_m06']/dict_add['XINYAN_RADAR_amt_per_pay_m12'] if dict_add['XINYAN_RADAR_amt_per_pay_m12']>0 else np.nan
    #近6个月履约贷款次均/近24个月履约贷款次均
    dict_add['XINYAN_RADAR_amt_per_pay_m06vm24']=dict_add['XINYAN_RADAR_amt_per_pay_m06']/dict_add['XINYAN_RADAR_amt_per_pay_m24'] if dict_add['XINYAN_RADAR_amt_per_pay_m24']>0 else np.nan
    #近12个月履约贷款次均/近24个月履约贷款次均
    dict_add['XINYAN_RADAR_amt_per_pay_m12vm24']=dict_add['XINYAN_RADAR_amt_per_pay_m12']/dict_add['XINYAN_RADAR_amt_per_pay_m24'] if dict_add['XINYAN_RADAR_amt_per_pay_m24']>0 else np.nan
    
    #近1个月履约贷款次数-近1个月失败扣款笔数
    dict_add['XINYAN_RADAR_B22170045subB22170035']=dict_add['XINYAN_RADAR_B22170045']-dict_add['XINYAN_RADAR_B22170035']
    #近3个月履约贷款次数-近3个月失败扣款笔数
    dict_add['XINYAN_RADAR_B22170046subB22170036']=dict_add['XINYAN_RADAR_B22170046']-dict_add['XINYAN_RADAR_B22170036']
    #近6个月履约贷款次数-近6个月失败扣款笔数
    dict_add['XINYAN_RADAR_B22170047subB22170037']=dict_add['XINYAN_RADAR_B22170047']-dict_add['XINYAN_RADAR_B22170037']
    #近12个月履约贷款次数-近12个月失败扣款笔数
    dict_add['XINYAN_RADAR_B22170048subB22170038']=dict_add['XINYAN_RADAR_B22170048']-dict_add['XINYAN_RADAR_B22170038']
    #近24个月履约贷款次数-近24个月失败扣款笔数
    dict_add['XINYAN_RADAR_B22170049subB22170039']=dict_add['XINYAN_RADAR_B22170049']-dict_add['XINYAN_RADAR_B22170039']
    
    #近1个月失败扣款笔数/近1个月履约贷款次数
    dict_add['XINYAN_RADAR_B22170035subB22170045']=dict_add['XINYAN_RADAR_B22170035']/dict_add['XINYAN_RADAR_B22170045'] if dict_add['XINYAN_RADAR_B22170045']>0 else np.nan
    #近3个月失败扣款笔数/近3个月履约贷款次数
    dict_add['XINYAN_RADAR_B22170036subB22170046']=dict_add['XINYAN_RADAR_B22170036']/dict_add['XINYAN_RADAR_B22170046'] if dict_add['XINYAN_RADAR_B22170046']>0 else np.nan
    #近6个月失败扣款笔数/近6个月履约贷款次数
    dict_add['XINYAN_RADAR_B22170037subB22170047']=dict_add['XINYAN_RADAR_B22170037']/dict_add['XINYAN_RADAR_B22170047'] if dict_add['XINYAN_RADAR_B22170047']>0 else np.nan
    #近12个月失败扣款笔数/近12个月履约贷款次数
    dict_add['XINYAN_RADAR_B22170038subB22170048']=dict_add['XINYAN_RADAR_B22170038']/dict_add['XINYAN_RADAR_B22170048'] if dict_add['XINYAN_RADAR_B22170048']>0 else np.nan
    #近24个月失败扣款笔数/近24个月履约贷款次数
    dict_add['XINYAN_RADAR_B22170039subB22170049']=dict_add['XINYAN_RADAR_B22170039']/dict_add['XINYAN_RADAR_B22170049'] if dict_add['XINYAN_RADAR_B22170049']>0 else np.nan
    
    #近1个月履约贷款总金额/近1个月贷款总金额
    dict_add['XINYAN_RADAR_B22170040vsB22170007']=dict_add['XINYAN_RADAR_B22170040_num']/dict_add['XINYAN_RADAR_B22170007_num'] if dict_add['XINYAN_RADAR_B22170007_num']>0 else np.nan
    #近3个月履约贷款总金额/近3个月贷款总金额
    dict_add['XINYAN_RADAR_B22170041vsB22170008']=dict_add['XINYAN_RADAR_B22170041_num']/dict_add['XINYAN_RADAR_B22170008_num'] if dict_add['XINYAN_RADAR_B22170008_num']>0 else np.nan
    #近6个月履约贷款总金额/近6个月贷款总金额
    dict_add['XINYAN_RADAR_B22170042vsB22170009']=dict_add['XINYAN_RADAR_B22170042_num']/dict_add['XINYAN_RADAR_B22170009_num'] if dict_add['XINYAN_RADAR_B22170009_num']>0 else np.nan
    #近12个月履约贷款总金额/近12个月贷款总金额
    dict_add['XINYAN_RADAR_B22170043vsB22170010']=dict_add['XINYAN_RADAR_B22170043_num']/dict_add['XINYAN_RADAR_B22170010_num'] if dict_add['XINYAN_RADAR_B22170010_num']>0 else np.nan
    #近24个月履约贷款总金额/近24个月贷款总金额
    dict_add['XINYAN_RADAR_B22170044vsB22170011']=dict_add['XINYAN_RADAR_B22170044_num']/dict_add['XINYAN_RADAR_B22170011_num'] if dict_add['XINYAN_RADAR_B22170011_num']>0 else np.nan
    
    #近1个月履约贷款次均/近1个月件均
    dict_add['XINYAN_RADAR_amt_per_pay_m01vs01']=dict_add['XINYAN_RADAR_amt_per_pay_m01']/dict_add['XINYAN_RADAR_amt_per_m01'] if dict_add['XINYAN_RADAR_amt_per_m01']>0 else np.nan
    #近3个月履约贷款次均/近3个月件均
    dict_add['XINYAN_RADAR_amt_per_pay_m03vs03']=dict_add['XINYAN_RADAR_amt_per_pay_m03']/dict_add['XINYAN_RADAR_amt_per_m03'] if dict_add['XINYAN_RADAR_amt_per_m03']>0 else np.nan
    #近6个月履约贷款次均/近6个月件均
    dict_add['XINYAN_RADAR_amt_per_pay_m06vs06']=dict_add['XINYAN_RADAR_amt_per_pay_m06']/dict_add['XINYAN_RADAR_amt_per_m06'] if dict_add['XINYAN_RADAR_amt_per_m06']>0 else np.nan
    #近12个月履约贷款次均/近12个月件均
    dict_add['XINYAN_RADAR_amt_per_pay_m12vs12']=dict_add['XINYAN_RADAR_amt_per_pay_m12']/dict_add['XINYAN_RADAR_amt_per_m12'] if dict_add['XINYAN_RADAR_amt_per_m12']>0 else np.nan
    #近24个月履约贷款次均/近24个月件均
    dict_add['XINYAN_RADAR_amt_per_pay_m24vs24']=dict_add['XINYAN_RADAR_amt_per_pay_m24']/dict_add['XINYAN_RADAR_amt_per_m24'] if dict_add['XINYAN_RADAR_amt_per_m24']>0 else np.nan
  
    #近1个月失败扣款笔数/近1个月总查询笔数
    dict_add['XINYAN_RADAR_B22170035vsA22160008']=dict_add['XINYAN_RADAR_B22170035']/dict_add['XINYAN_RADAR_A22160008'] if dict_add['XINYAN_RADAR_A22160008']>0 else np.nan
    #近3个月失败扣款笔数/近3个月总查询笔数
    dict_add['XINYAN_RADAR_B22170036vsA22160009']=dict_add['XINYAN_RADAR_B22170036']/dict_add['XINYAN_RADAR_A22160009'] if dict_add['XINYAN_RADAR_A22160009']>0 else np.nan
    #近6个月失败扣款笔数/近6个月总查询笔数
    dict_add['XINYAN_RADAR_B22170037vsA22160010']=dict_add['XINYAN_RADAR_B22170037']/dict_add['XINYAN_RADAR_A22160010'] if dict_add['XINYAN_RADAR_A22160010']>0 else np.nan
    
    #近1个月通过率（近1个月贷款笔数/近1个月总查询笔数）
    dict_add['XINYAN_RADAR_approve_rate_m01']=dict_add['XINYAN_RADAR_B22170002']/dict_add['XINYAN_RADAR_A22160008'] if dict_add['XINYAN_RADAR_A22160008']>0 else np.nan
    #近3个月通过率（近3个月贷款笔数/近3个月总查询笔数）
    dict_add['XINYAN_RADAR_approve_rate_m03']=dict_add['XINYAN_RADAR_B22170003']/dict_add['XINYAN_RADAR_A22160009'] if dict_add['XINYAN_RADAR_A22160009']>0 else np.nan
    #近6个月通过率（近6个月贷款笔数/近6个月总查询笔数）
    dict_add['XINYAN_RADAR_approve_rate_m06']=dict_add['XINYAN_RADAR_B22170004']/dict_add['XINYAN_RADAR_A22160010'] if dict_add['XINYAN_RADAR_A22160010']>0 else np.nan
    
    #近1个月通过率/近3个月通过率
    dict_add['XINYAN_RADAR_approve_rate_m01vsm03']=dict_add['XINYAN_RADAR_approve_rate_m01']/dict_add['XINYAN_RADAR_approve_rate_m03'] if dict_add['XINYAN_RADAR_approve_rate_m03']>0 else np.nan
    #近1个月通过率/近6个月通过率
    dict_add['XINYAN_RADAR_approve_rate_m01vsm06']=dict_add['XINYAN_RADAR_approve_rate_m01']/dict_add['XINYAN_RADAR_approve_rate_m06'] if dict_add['XINYAN_RADAR_approve_rate_m06']>0 else np.nan
    #近3个月通过率/近6个月通过率
    dict_add['XINYAN_RADAR_approve_rate_m03vsm06']=dict_add['XINYAN_RADAR_approve_rate_m03']/dict_add['XINYAN_RADAR_approve_rate_m06'] if dict_add['XINYAN_RADAR_approve_rate_m06']>0 else np.nan
    
    #近6个月M0+逾期率（近6个月M0+逾期贷款笔数/近6个月贷款笔数）
    dict_add['XINYAN_RADAR_m0_overdue_rate_m06']=dict_add['XINYAN_RADAR_B22170025']/dict_add['XINYAN_RADAR_B22170004'] if dict_add['XINYAN_RADAR_B22170004']>0 else np.nan
    #近12个月M0+逾期率（近12个月M0+逾期贷款笔数/近12个月贷款笔数）
    dict_add['XINYAN_RADAR_m0_overdue_rate_m12']=dict_add['XINYAN_RADAR_B22170026']/dict_add['XINYAN_RADAR_B22170005'] if dict_add['XINYAN_RADAR_B22170005']>0 else np.nan
    #近24个月M0+逾期率（近24个月M0+逾期贷款笔数/近24个月贷款笔数）
    dict_add['XINYAN_RADAR_m0_overdue_rate_m24']=dict_add['XINYAN_RADAR_B22170027']/dict_add['XINYAN_RADAR_B22170006'] if dict_add['XINYAN_RADAR_B22170006']>0 else np.nan
    
    #近6个月M1+逾期率（近6个月M1+逾期贷款笔数/近6个月贷款笔数）
    dict_add['XINYAN_RADAR_m1_overdue_rate_m06']=dict_add['XINYAN_RADAR_B22170028']/dict_add['XINYAN_RADAR_B22170004'] if dict_add['XINYAN_RADAR_B22170004']>0 else np.nan
    #近12个月M1+逾期率（近12个月M1+逾期贷款笔数/近12个月贷款笔数）
    dict_add['XINYAN_RADAR_m1_overdue_rate_m12']=dict_add['XINYAN_RADAR_B22170029']/dict_add['XINYAN_RADAR_B22170005'] if dict_add['XINYAN_RADAR_B22170005']>0 else np.nan
    #近24个月M1+逾期率（近24个月M1+逾期贷款笔数/近24个月贷款笔数）
    dict_add['XINYAN_RADAR_m1_overdue_rate_m24']=dict_add['XINYAN_RADAR_B22170030']/dict_add['XINYAN_RADAR_B22170006'] if dict_add['XINYAN_RADAR_B22170006']>0 else np.nan
    
    #近6个月M0+逾期率/近12个月M0+逾期率
    dict_add['XINYAN_RADAR_m0_overdue_rate_m06vsm12']=dict_add['XINYAN_RADAR_m0_overdue_rate_m06']/dict_add['XINYAN_RADAR_m0_overdue_rate_m12'] if dict_add['XINYAN_RADAR_m0_overdue_rate_m12']>0 else np.nan
    #近6个月M0+逾期率/近24个月M0+逾期率
    dict_add['XINYAN_RADAR_m0_overdue_rate_m06vsm24']=dict_add['XINYAN_RADAR_m0_overdue_rate_m06']/dict_add['XINYAN_RADAR_m0_overdue_rate_m24'] if dict_add['XINYAN_RADAR_m0_overdue_rate_m24']>0 else np.nan
    #近12个月M0+逾期率/近24个月M0+逾期率
    dict_add['XINYAN_RADAR_m0_overdue_rate_m12vsm24']=dict_add['XINYAN_RADAR_m0_overdue_rate_m12']/dict_add['XINYAN_RADAR_m0_overdue_rate_m24'] if dict_add['XINYAN_RADAR_m0_overdue_rate_m24']>0 else np.nan
    
    #近6个月M1+逾期率/近12个月M1+逾期率
    dict_add['XINYAN_RADAR_m1_overdue_rate_m06vsm12']=dict_add['XINYAN_RADAR_m1_overdue_rate_m06']/dict_add['XINYAN_RADAR_m1_overdue_rate_m12'] if dict_add['XINYAN_RADAR_m1_overdue_rate_m12']>0 else np.nan
    #近6个月M1+逾期率/近24个月M1+逾期率
    dict_add['XINYAN_RADAR_m1_overdue_rate_m06vsm24']=dict_add['XINYAN_RADAR_m1_overdue_rate_m06']/dict_add['XINYAN_RADAR_m1_overdue_rate_m24'] if dict_add['XINYAN_RADAR_m1_overdue_rate_m24']>0 else np.nan
    #近12个月M1+逾期率/近24个月M1+逾期率
    dict_add['XINYAN_RADAR_m1_overdue_rate_m12vsm24']=dict_add['XINYAN_RADAR_m1_overdue_rate_m12']/dict_add['XINYAN_RADAR_m1_overdue_rate_m24'] if dict_add['XINYAN_RADAR_m1_overdue_rate_m24']>0 else np.nan
    
    #近6个月M0+逾期率/近6个月M1+逾期率
    dict_add['XINYAN_RADAR_m0_overdue_rate_m06vsm1m06']=dict_add['XINYAN_RADAR_m0_overdue_rate_m06']/dict_add['XINYAN_RADAR_m1_overdue_rate_m06'] if dict_add['XINYAN_RADAR_m1_overdue_rate_m06']>0 else np.nan
    #近12个月M0+逾期率/近12个月M1+逾期率
    dict_add['XINYAN_RADAR_m0_overdue_rate_m12vsm1m12']=dict_add['XINYAN_RADAR_m0_overdue_rate_m12']/dict_add['XINYAN_RADAR_m1_overdue_rate_m12'] if dict_add['XINYAN_RADAR_m1_overdue_rate_m12']>0 else np.nan
    #近24个月M0+逾期率/近24个月M1+逾期率
    dict_add['XINYAN_RADAR_m0_overdue_rate_m24vsm1m24']=dict_add['XINYAN_RADAR_m0_overdue_rate_m24']/dict_add['XINYAN_RADAR_m1_overdue_rate_m24'] if dict_add['XINYAN_RADAR_m1_overdue_rate_m24']>0 else np.nan
    
    
    #贷款已结清订单数/近24个月贷款笔数
    dict_add['XINYAN_RADAR_B22170052vsB22170006']=dict_add['XINYAN_RADAR_B22170052']/dict_add['XINYAN_RADAR_B22170006'] if dict_add['XINYAN_RADAR_B22170006']>0 else np.nan
    #贷款已结清订单数/近24个月贷款机构数
    dict_add['XINYAN_RADAR_B22170052vsB22170020']=dict_add['XINYAN_RADAR_B22170052']/dict_add['XINYAN_RADAR_B22170020'] if dict_add['XINYAN_RADAR_B22170020']>0 else np.nan
    #贷款已结清订单数/近24个月M0+逾期贷款笔数
    dict_add['XINYAN_RADAR_B22170052vsB22170027']=dict_add['XINYAN_RADAR_B22170052']/dict_add['XINYAN_RADAR_B22170027'] if dict_add['XINYAN_RADAR_B22170027']>0 else np.nan
    #贷款已结清订单数/近24个月M1+逾期贷款笔数
    dict_add['XINYAN_RADAR_B22170052vsB22170030']=dict_add['XINYAN_RADAR_B22170052']/dict_add['XINYAN_RADAR_B22170030'] if dict_add['XINYAN_RADAR_B22170030']>0 else np.nan
    #贷款已结清订单数/近24个月失败扣款笔数
    dict_add['XINYAN_RADAR_B22170052vsB22170039']=dict_add['XINYAN_RADAR_B22170052']/dict_add['XINYAN_RADAR_B22170039'] if dict_add['XINYAN_RADAR_B22170039']>0 else np.nan
    #贷款已结清订单数/近24个月履约贷款次数
    dict_add['XINYAN_RADAR_B22170052vsB22170049']=dict_add['XINYAN_RADAR_B22170052']/dict_add['XINYAN_RADAR_B22170049'] if dict_add['XINYAN_RADAR_B22170049']>0 else np.nan
    
    
    #近24个月履约贷款总金额/网贷建议授信额度
    dict_add['XINYAN_RADAR_B22170044vsC22180001']=dict_add['XINYAN_RADAR_B22170044_num']/dict_add['XINYAN_RADAR_C22180001'] if dict_add['XINYAN_RADAR_C22180001']>0 else np.nan
    #近12个月履约贷款总金额/网贷建议授信额度
    dict_add['XINYAN_RADAR_B22170043vsC22180001']=dict_add['XINYAN_RADAR_B22170043_num']/dict_add['XINYAN_RADAR_C22180001'] if dict_add['XINYAN_RADAR_C22180001']>0 else np.nan
    #近6个月履约贷款总金额/网贷建议授信额度
    dict_add['XINYAN_RADAR_B22170042vsC22180001']=dict_add['XINYAN_RADAR_B22170042_num']/dict_add['XINYAN_RADAR_C22180001'] if dict_add['XINYAN_RADAR_C22180001']>0 else np.nan
    #近3个月履约贷款总金额/网贷建议授信额度
    dict_add['XINYAN_RADAR_B22170041vsC22180001']=dict_add['XINYAN_RADAR_B22170041_num']/dict_add['XINYAN_RADAR_C22180001'] if dict_add['XINYAN_RADAR_C22180001']>0 else np.nan
    #近1个月履约贷款总金额/网贷建议授信额度
    dict_add['XINYAN_RADAR_B22170040vsC22180001']=dict_add['XINYAN_RADAR_B22170040_num']/dict_add['XINYAN_RADAR_C22180001'] if dict_add['XINYAN_RADAR_C22180001']>0 else np.nan
    #近24个月累计逾期金额/网贷建议授信额度
    dict_add['XINYAN_RADAR_B22170033vsC22180001']=dict_add['XINYAN_RADAR_B22170033_num']/dict_add['XINYAN_RADAR_C22180001'] if dict_add['XINYAN_RADAR_C22180001']>0 else np.nan
    #近12个月累计逾期金额/网贷建议授信额度
    dict_add['XINYAN_RADAR_B22170032vsC22180001']=dict_add['XINYAN_RADAR_B22170032_num']/dict_add['XINYAN_RADAR_C22180001'] if dict_add['XINYAN_RADAR_C22180001']>0 else np.nan
    #近06个月累计逾期金额/网贷建议授信额度
    dict_add['XINYAN_RADAR_B22170031vsC22180001']=dict_add['XINYAN_RADAR_B22170031_num']/dict_add['XINYAN_RADAR_C22180001'] if dict_add['XINYAN_RADAR_C22180001']>0 else np.nan
    #近24个月贷款总金额/网贷建议授信额度
    dict_add['XINYAN_RADAR_B22170011vsC22180001']=dict_add['XINYAN_RADAR_B22170011_num']/dict_add['XINYAN_RADAR_C22180001'] if dict_add['XINYAN_RADAR_C22180001']>0 else np.nan
    #近12个月贷款总金额/网贷建议授信额度
    dict_add['XINYAN_RADAR_B22170010vsC22180001']=dict_add['XINYAN_RADAR_B22170010_num']/dict_add['XINYAN_RADAR_C22180001'] if dict_add['XINYAN_RADAR_C22180001']>0 else np.nan
    #近6个月贷款总金额/网贷建议授信额度
    dict_add['XINYAN_RADAR_B22170009vsC22180001']=dict_add['XINYAN_RADAR_B22170009_num']/dict_add['XINYAN_RADAR_C22180001'] if dict_add['XINYAN_RADAR_C22180001']>0 else np.nan
    #近3个月贷款总金额/网贷建议授信额度
    dict_add['XINYAN_RADAR_B22170008vsC22180001']=dict_add['XINYAN_RADAR_B22170008_num']/dict_add['XINYAN_RADAR_C22180001'] if dict_add['XINYAN_RADAR_C22180001']>0 else np.nan
    #近1个月贷款总金额/网贷建议授信额度
    dict_add['XINYAN_RADAR_B22170007vsC22180001']=dict_add['XINYAN_RADAR_B22170007_num']/dict_add['XINYAN_RADAR_C22180001'] if dict_add['XINYAN_RADAR_C22180001']>0 else np.nan
    #近1个月件均/网贷建议授信额度
    dict_add['XINYAN_RADAR_amt_per_m01vsC22180001']=dict_add['XINYAN_RADAR_amt_per_m01']/dict_add['XINYAN_RADAR_C22180001'] if dict_add['XINYAN_RADAR_C22180001']>0 else np.nan
    #近3个月件均/网贷建议授信额度
    dict_add['XINYAN_RADAR_amt_per_m03vsC22180001']=dict_add['XINYAN_RADAR_amt_per_m03']/dict_add['XINYAN_RADAR_C22180001'] if dict_add['XINYAN_RADAR_C22180001']>0 else np.nan
    #近6个月件均/网贷建议授信额度
    dict_add['XINYAN_RADAR_amt_per_m06vsC22180001']=dict_add['XINYAN_RADAR_amt_per_m06']/dict_add['XINYAN_RADAR_C22180001'] if dict_add['XINYAN_RADAR_C22180001']>0 else np.nan
    #近12个月件均/网贷建议授信额度
    dict_add['XINYAN_RADAR_amt_per_m12vsC22180001']=dict_add['XINYAN_RADAR_amt_per_m12']/dict_add['XINYAN_RADAR_C22180001'] if dict_add['XINYAN_RADAR_C22180001']>0 else np.nan
    #近24个月件均/网贷建议授信额度
    dict_add['XINYAN_RADAR_amt_per_m24vsC22180001']=dict_add['XINYAN_RADAR_amt_per_m24']/dict_add['XINYAN_RADAR_C22180001'] if dict_add['XINYAN_RADAR_C22180001']>0 else np.nan
    #近1个月履约贷款次均/网贷建议授信额度
    dict_add['XINYAN_RADAR_amt_per_pay_m01vsC22180001']=dict_add['XINYAN_RADAR_amt_per_pay_m01']/dict_add['XINYAN_RADAR_C22180001'] if dict_add['XINYAN_RADAR_C22180001']>0 else np.nan
    #近3个月履约贷款次均/网贷建议授信额度
    dict_add['XINYAN_RADAR_amt_per_pay_m03vsC22180001']=dict_add['XINYAN_RADAR_amt_per_pay_m03']/dict_add['XINYAN_RADAR_C22180001'] if dict_add['XINYAN_RADAR_C22180001']>0 else np.nan
    #近6个月履约贷款次均/网贷建议授信额度
    dict_add['XINYAN_RADAR_amt_per_pay_m06vsC22180001']=dict_add['XINYAN_RADAR_amt_per_pay_m06']/dict_add['XINYAN_RADAR_C22180001'] if dict_add['XINYAN_RADAR_C22180001']>0 else np.nan
    #近12个月履约贷款次均/网贷建议授信额度
    dict_add['XINYAN_RADAR_amt_per_pay_m12vsC22180001']=dict_add['XINYAN_RADAR_amt_per_pay_m12']/dict_add['XINYAN_RADAR_C22180001'] if dict_add['XINYAN_RADAR_C22180001']>0 else np.nan
    #近24个月履约贷款次均/网贷建议授信额度
    dict_add['XINYAN_RADAR_amt_per_pay_m24vsC22180001']=dict_add['XINYAN_RADAR_amt_per_pay_m24']/dict_add['XINYAN_RADAR_C22180001'] if dict_add['XINYAN_RADAR_C22180001']>0 else np.nan
    #近6个月逾期件均/网贷建议授信额度
    dict_add['XINYAN_RADAR_amt_per_ove_m06vsC22180001']=dict_add['XINYAN_RADAR_amt_per_ove_m06']/dict_add['XINYAN_RADAR_C22180001'] if dict_add['XINYAN_RADAR_C22180001']>0 else np.nan
    #近12个月逾期件均/网贷建议授信额度
    dict_add['XINYAN_RADAR_amt_per_ove_m12vsC22180001']=dict_add['XINYAN_RADAR_amt_per_ove_m12']/dict_add['XINYAN_RADAR_C22180001'] if dict_add['XINYAN_RADAR_C22180001']>0 else np.nan
    #近24个月逾期件均/网贷建议授信额度
    dict_add['XINYAN_RADAR_amt_per_ove_m24vsC22180001']=dict_add['XINYAN_RADAR_amt_per_ove_m24']/dict_add['XINYAN_RADAR_C22180001'] if dict_add['XINYAN_RADAR_C22180001']>0 else np.nan
    
    #网贷建议授信额度*网贷额度置信度/100
    dict_add['XINYAN_RADAR_credit_amt']=dict_add['XINYAN_RADAR_C22180001']*dict_add['XINYAN_RADAR_C22180002']/100
    
    #近24个月履约贷款总金额/网贷置信授信额度
    dict_add['XINYAN_RADAR_B22170044vscredit_amt']=dict_add['XINYAN_RADAR_B22170044_num']/dict_add['XINYAN_RADAR_credit_amt'] if dict_add['XINYAN_RADAR_credit_amt']>0 else np.nan
    #近12个月履约贷款总金额/网贷置信授信额度
    dict_add['XINYAN_RADAR_B22170043vscredit_amt']=dict_add['XINYAN_RADAR_B22170043_num']/dict_add['XINYAN_RADAR_credit_amt'] if dict_add['XINYAN_RADAR_credit_amt']>0 else np.nan
    #近6个月履约贷款总金额/网贷置信授信额度
    dict_add['XINYAN_RADAR_B22170042vscredit_amt']=dict_add['XINYAN_RADAR_B22170042_num']/dict_add['XINYAN_RADAR_credit_amt'] if dict_add['XINYAN_RADAR_credit_amt']>0 else np.nan
    #近3个月履约贷款总金额/网贷置信授信额度
    dict_add['XINYAN_RADAR_B22170041vscredit_amt']=dict_add['XINYAN_RADAR_B22170041_num']/dict_add['XINYAN_RADAR_credit_amt'] if dict_add['XINYAN_RADAR_credit_amt']>0 else np.nan
    #近1个月履约贷款总金额/网贷置信授信额度
    dict_add['XINYAN_RADAR_B22170040vscredit_amt']=dict_add['XINYAN_RADAR_B22170040_num']/dict_add['XINYAN_RADAR_credit_amt'] if dict_add['XINYAN_RADAR_credit_amt']>0 else np.nan
    #近24个月累计逾期金额/网贷置信授信额度
    dict_add['XINYAN_RADAR_B22170033vscredit_amt']=dict_add['XINYAN_RADAR_B22170033_num']/dict_add['XINYAN_RADAR_credit_amt'] if dict_add['XINYAN_RADAR_credit_amt']>0 else np.nan
    #近12个月累计逾期金额/网贷置信授信额度
    dict_add['XINYAN_RADAR_B22170032vscredit_amt']=dict_add['XINYAN_RADAR_B22170032_num']/dict_add['XINYAN_RADAR_credit_amt'] if dict_add['XINYAN_RADAR_credit_amt']>0 else np.nan
    #近06个月累计逾期金额/网贷置信授信额度
    dict_add['XINYAN_RADAR_B22170031vscredit_amt']=dict_add['XINYAN_RADAR_B22170031_num']/dict_add['XINYAN_RADAR_credit_amt'] if dict_add['XINYAN_RADAR_credit_amt']>0 else np.nan
    #近24个月贷款总金额/网贷置信授信额度
    dict_add['XINYAN_RADAR_B22170011vscredit_amt']=dict_add['XINYAN_RADAR_B22170011_num']/dict_add['XINYAN_RADAR_credit_amt'] if dict_add['XINYAN_RADAR_credit_amt']>0 else np.nan
    #近12个月贷款总金额/网贷置信授信额度
    dict_add['XINYAN_RADAR_B22170010vscredit_amt']=dict_add['XINYAN_RADAR_B22170010_num']/dict_add['XINYAN_RADAR_credit_amt'] if dict_add['XINYAN_RADAR_credit_amt']>0 else np.nan
    #近6个月贷款总金额/网贷置信授信额度
    dict_add['XINYAN_RADAR_B22170009vscredit_amt']=dict_add['XINYAN_RADAR_B22170009_num']/dict_add['XINYAN_RADAR_credit_amt'] if dict_add['XINYAN_RADAR_credit_amt']>0 else np.nan
    #近3个月贷款总金额/网贷置信授信额度
    dict_add['XINYAN_RADAR_B22170008vscredit_amt']=dict_add['XINYAN_RADAR_B22170008_num']/dict_add['XINYAN_RADAR_credit_amt'] if dict_add['XINYAN_RADAR_credit_amt']>0 else np.nan
    #近1个月贷款总金额/网贷置信授信额度
    dict_add['XINYAN_RADAR_B22170007vscredit_amt']=dict_add['XINYAN_RADAR_B22170007_num']/dict_add['XINYAN_RADAR_credit_amt'] if dict_add['XINYAN_RADAR_credit_amt']>0 else np.nan
    #近1个月件均/网贷置信授信额度
    dict_add['XINYAN_RADAR_amt_per_m01vscredit_amt']=dict_add['XINYAN_RADAR_amt_per_m01']/dict_add['XINYAN_RADAR_credit_amt'] if dict_add['XINYAN_RADAR_credit_amt']>0 else np.nan
    #近3个月件均/网贷置信授信额度
    dict_add['XINYAN_RADAR_amt_per_m03vscredit_amt']=dict_add['XINYAN_RADAR_amt_per_m03']/dict_add['XINYAN_RADAR_credit_amt'] if dict_add['XINYAN_RADAR_credit_amt']>0 else np.nan
    #近6个月件均/网贷置信授信额度
    dict_add['XINYAN_RADAR_amt_per_m06vscredit_amt']=dict_add['XINYAN_RADAR_amt_per_m06']/dict_add['XINYAN_RADAR_credit_amt'] if dict_add['XINYAN_RADAR_credit_amt']>0 else np.nan
    #近12个月件均/网贷置信授信额度
    dict_add['XINYAN_RADAR_amt_per_m12vscredit_amt']=dict_add['XINYAN_RADAR_amt_per_m12']/dict_add['XINYAN_RADAR_credit_amt'] if dict_add['XINYAN_RADAR_credit_amt']>0 else np.nan
    #近24个月件均/网贷置信授信额度
    dict_add['XINYAN_RADAR_amt_per_m24vscredit_amt']=dict_add['XINYAN_RADAR_amt_per_m24']/dict_add['XINYAN_RADAR_credit_amt'] if dict_add['XINYAN_RADAR_credit_amt']>0 else np.nan
    #近1个月履约贷款次均/网贷置信授信额度
    dict_add['XINYAN_RADAR_amt_per_pay_m01vscredit_amt']=dict_add['XINYAN_RADAR_amt_per_pay_m01']/dict_add['XINYAN_RADAR_credit_amt'] if dict_add['XINYAN_RADAR_credit_amt']>0 else np.nan
    #近3个月履约贷款次均/网贷置信授信额度
    dict_add['XINYAN_RADAR_amt_per_pay_m03vscredit_amt']=dict_add['XINYAN_RADAR_amt_per_pay_m03']/dict_add['XINYAN_RADAR_credit_amt'] if dict_add['XINYAN_RADAR_credit_amt']>0 else np.nan
    #近6个月履约贷款次均/网贷置信授信额度
    dict_add['XINYAN_RADAR_amt_per_pay_m06vscredit_amt']=dict_add['XINYAN_RADAR_amt_per_pay_m06']/dict_add['XINYAN_RADAR_credit_amt'] if dict_add['XINYAN_RADAR_credit_amt']>0 else np.nan
    #近12个月履约贷款次均/网贷置信授信额度
    dict_add['XINYAN_RADAR_amt_per_pay_m12vscredit_amt']=dict_add['XINYAN_RADAR_amt_per_pay_m12']/dict_add['XINYAN_RADAR_credit_amt'] if dict_add['XINYAN_RADAR_credit_amt']>0 else np.nan
    #近24个月履约贷款次均/网贷置信授信额度
    dict_add['XINYAN_RADAR_amt_per_pay_m24vscredit_amt']=dict_add['XINYAN_RADAR_amt_per_pay_m24']/dict_add['XINYAN_RADAR_credit_amt'] if dict_add['XINYAN_RADAR_credit_amt']>0 else np.nan
    #近6个月逾期件均/网贷置信授信额度
    dict_add['XINYAN_RADAR_amt_per_ove_m06vscredit_amt']=dict_add['XINYAN_RADAR_amt_per_ove_m06']/dict_add['XINYAN_RADAR_credit_amt'] if dict_add['XINYAN_RADAR_credit_amt']>0 else np.nan
    #近12个月逾期件均/网贷置信授信额度
    dict_add['XINYAN_RADAR_amt_per_ove_m12vscredit_amt']=dict_add['XINYAN_RADAR_amt_per_ove_m12']/dict_add['XINYAN_RADAR_credit_amt'] if dict_add['XINYAN_RADAR_credit_amt']>0 else np.nan
    #近24个月逾期件均/网贷置信授信额度
    dict_add['XINYAN_RADAR_amt_per_ove_m24vscredit_amt']=dict_add['XINYAN_RADAR_amt_per_ove_m24']/dict_add['XINYAN_RADAR_credit_amt'] if dict_add['XINYAN_RADAR_credit_amt']>0 else np.nan
    
    #网络贷款类机构数/网络贷款类产品数
    dict_add['XINYAN_RADAR_C22180003vsC22180004']=dict_add['XINYAN_RADAR_C22180003']/dict_add['XINYAN_RADAR_C22180004'] if dict_add['XINYAN_RADAR_C22180004']>0 else np.nan
    
    #查询网络贷款类机构数/网络贷款类机构数
    dict_add['XINYAN_RADAR_A22160005vsC22180003']=dict_add['XINYAN_RADAR_A22160005']/dict_add['XINYAN_RADAR_C22180003'] if dict_add['XINYAN_RADAR_C22180003']>0 else np.nan
    #近1个月贷款机构数/网络贷款类机构数
    dict_add['XINYAN_RADAR_B22170016vsC22180003']=dict_add['XINYAN_RADAR_B22170016']/dict_add['XINYAN_RADAR_C22180003'] if dict_add['XINYAN_RADAR_C22180003']>0 else np.nan
    #近3个月贷款机构数/网络贷款类机构数
    dict_add['XINYAN_RADAR_B22170017vsC22180003']=dict_add['XINYAN_RADAR_B22170017']/dict_add['XINYAN_RADAR_C22180003'] if dict_add['XINYAN_RADAR_C22180003']>0 else np.nan
    #近6个月贷款机构数/网络贷款类机构数
    dict_add['XINYAN_RADAR_B22170018vsC22180003']=dict_add['XINYAN_RADAR_B22170018']/dict_add['XINYAN_RADAR_C22180003'] if dict_add['XINYAN_RADAR_C22180003']>0 else np.nan
    #近12个月贷款机构数/网络贷款类机构数
    dict_add['XINYAN_RADAR_B22170019vsC22180003']=dict_add['XINYAN_RADAR_B22170019']/dict_add['XINYAN_RADAR_C22180003'] if dict_add['XINYAN_RADAR_C22180003']>0 else np.nan
    #近24个月贷款机构数/网络贷款类机构数
    dict_add['XINYAN_RADAR_B22170020vsC22180003']=dict_add['XINYAN_RADAR_B22170020']/dict_add['XINYAN_RADAR_C22180003'] if dict_add['XINYAN_RADAR_C22180003']>0 else np.nan
    
    #近12个月网贷类贷款机构数/网络贷款类机构数
    dict_add['XINYAN_RADAR_B22170023vsC22180003']=dict_add['XINYAN_RADAR_B22170023']/dict_add['XINYAN_RADAR_C22180003'] if dict_add['XINYAN_RADAR_C22180003']>0 else np.nan
    #近24个月网贷类贷款机构数/网络贷款类机构数
    dict_add['XINYAN_RADAR_B22170024vsC22180003']=dict_add['XINYAN_RADAR_B22170024']/dict_add['XINYAN_RADAR_C22180003'] if dict_add['XINYAN_RADAR_C22180003']>0 else np.nan
    
    #网贷建议授信额度/网络贷款机构最大授信额度
    dict_add['XINYAN_RADAR_C22180001vsC22180005']=dict_add['XINYAN_RADAR_C22180001']/dict_add['XINYAN_RADAR_C22180005'] if dict_add['XINYAN_RADAR_C22180005']>0 else np.nan
    
    
    
    #消金贷款类机构数/消金贷款类产品数
    dict_add['XINYAN_RADAR_C22180007vsC22180008']=dict_add['XINYAN_RADAR_C22180007']/dict_add['XINYAN_RADAR_C22180008'] if dict_add['XINYAN_RADAR_C22180008']>0 else np.nan
    
    #查询网络贷款类机构数/消金贷款类机构数
    dict_add['XINYAN_RADAR_A22160004vsC22180007']=dict_add['XINYAN_RADAR_A22160004']/dict_add['XINYAN_RADAR_C22180007'] if dict_add['XINYAN_RADAR_C22180007']>0 else np.nan
    #近1个月贷款机构数/消金贷款类机构数
    dict_add['XINYAN_RADAR_B22170016vsC22180007']=dict_add['XINYAN_RADAR_B22170016']/dict_add['XINYAN_RADAR_C22180007'] if dict_add['XINYAN_RADAR_C22180007']>0 else np.nan
    #近3个月贷款机构数/消金贷款类机构数
    dict_add['XINYAN_RADAR_B22170017vsC22180007']=dict_add['XINYAN_RADAR_B22170017']/dict_add['XINYAN_RADAR_C22180007'] if dict_add['XINYAN_RADAR_C22180007']>0 else np.nan
    #近6个月贷款机构数/消金贷款类机构数
    dict_add['XINYAN_RADAR_B22170018vsC22180007']=dict_add['XINYAN_RADAR_B22170018']/dict_add['XINYAN_RADAR_C22180007'] if dict_add['XINYAN_RADAR_C22180007']>0 else np.nan
    #近12个月贷款机构数/消金贷款类机构数
    dict_add['XINYAN_RADAR_B22170019vsC22180007']=dict_add['XINYAN_RADAR_B22170019']/dict_add['XINYAN_RADAR_C22180007'] if dict_add['XINYAN_RADAR_C22180007']>0 else np.nan
    #近24个月贷款机构数/消金贷款类机构数
    dict_add['XINYAN_RADAR_B22170020vsC22180007']=dict_add['XINYAN_RADAR_B22170020']/dict_add['XINYAN_RADAR_C22180007'] if dict_add['XINYAN_RADAR_C22180007']>0 else np.nan

    #近12个月消金贷款机构数/消金贷款类机构数
    dict_add['XINYAN_RADAR_B22170021vsC22180007']=dict_add['XINYAN_RADAR_B22170021']/dict_add['XINYAN_RADAR_C22180007'] if dict_add['XINYAN_RADAR_C22180007']>0 else np.nan
    #近24个月消金贷款机构数/消金贷款类机构数
    dict_add['XINYAN_RADAR_B22170022vsC22180007']=dict_add['XINYAN_RADAR_B22170022']/dict_add['XINYAN_RADAR_C22180007'] if dict_add['XINYAN_RADAR_C22180007']>0 else np.nan
    

    
    
    return dict_add 

def JD_feature_func_online(dict_add,json_dict):
    #json_dict=excel_row.iloc[0].to_dict()

#     dict_add={}
#     json_dict=jd_data.iloc[0].to_dict()

    dict_add['jd_stability'] = float(json_dict['stability']) if 'stability' in json_dict.keys() else np.nan  #消费水平稳定性
    dict_add['jd_buyingIndex'] = float(json_dict['buyingIndex']) if 'buyingIndex' in json_dict.keys() else np.nan  #购买力指数
    dict_add['jd_riskIndex'] = float(json_dict['riskIndex']) if 'riskIndex' in json_dict.keys() else np.nan  #交易风险指数
    dict_add['jd_resonableConsuming'] = float(json_dict['resonableConsuming']) if 'resonableConsuming' in json_dict.keys() else np.nan  #理性消费指数
    dict_add['jd_city'] = json_dict['city'] if 'city' in json_dict.keys() else np.nan  #常驻城市
    dict_add['jd_comsumingSocial'] = float(json_dict['comsumingSocial']) if 'comsumingSocial' in json_dict.keys() else np.nan  #消费社交影响力
    dict_add['jd_riskPeriodConsuming'] = float(json_dict['riskPeriodConsuming']) if 'riskPeriodConsuming' in json_dict.keys() else np.nan  #高危时段消费指数
    dict_add['jd_riskCategoryConsuming'] = float(json_dict['riskCategoryConsuming']) if 'riskCategoryConsuming' in json_dict.keys() else np.nan  #高危品类消费指数
    dict_add['jd_worktimeShopping'] = float(json_dict['worktimeShopping']) if 'worktimeShopping' in json_dict.keys() else np.nan  #工作时间网购偏好
    dict_add['jd_cellphonePreference'] = float(json_dict['cellphonePreference']) if 'cellphonePreference' in json_dict.keys() else np.nan  #手机依赖度
    dict_add['jd_ecommerceAddressStability'] = float(json_dict['ecommerceAddressStability']) if 'ecommerceAddressStability' in json_dict.keys() else np.nan  #电商收货地址稳定性
    dict_add['jd_ecommercecellphoneStability'] = float(json_dict['ecommercecellphoneStability']) if 'ecommercecellphoneStability' in json_dict.keys() else np.nan  #电商联系方式稳定性
    dict_add['jd_asset_score'] = float(json_dict['asset_score']) if 'asset_score' in json_dict.keys() else np.nan  #综合资产指数
    dict_add['jd_gd_level'] = float(json_dict['gd_level']) if 'gd_level' in json_dict.keys() else np.nan  #实物资产等级
    dict_add['jd_cd_level'] = float(json_dict['cd_level']) if 'cd_level' in json_dict.keys() else np.nan  #信用资产等级
    dict_add['jd_income_score'] = float(json_dict['income_score']) if 'income_score' in json_dict.keys() else np.nan  #收入水平指数
    dict_add['jd_car_score'] = float(json_dict['car_score']) if 'car_score' in json_dict.keys() else np.nan  #有车指数
    dict_add['jd_house_score'] = float(json_dict['house_score']) if 'house_score' in json_dict.keys() else np.nan  #有房指数
    dict_add['jd_tob_rank'] = float(json_dict['tob_rank']) if 'tob_rank' in json_dict.keys() else np.nan  #网购账龄等级
    dict_add['jd_consume_m_freq'] = float(json_dict['consume_m_freq']) if 'consume_m_freq' in json_dict.keys() else np.nan  #消费活跃度等级
    dict_add['jd_consume_cnt_freq'] = float(json_dict['consume_cnt_freq']) if 'consume_cnt_freq' in json_dict.keys() else np.nan  #消费频繁度等级
    dict_add['jd_pur_preference'] = json_dict['pur_preference'] if 'pur_preference' in json_dict.keys() else np.nan  #消费品类偏好
    dict_add['jd_pay_preference'] = json_dict['pay_preference'] if 'pay_preference' in json_dict.keys() else np.nan  #支付方式偏好
    dict_add['jd_liability_level'] = float(json_dict['liability_level']) if 'liability_level' in json_dict.keys() else np.nan  #债务等级
    dict_add['jd_credit_consume_level'] = float(json_dict['credit_consume_level']) if 'credit_consume_level' in json_dict.keys() else np.nan  #信用消费等级
    dict_add['jd_debt_bearing_level'] = float(json_dict['debt_bearing_level']) if 'debt_bearing_level' in json_dict.keys() else np.nan  #债偿能力等级
    dict_add['jd_risk_pre_score'] = float(json_dict['risk_pre_score']) if 'risk_pre_score' in json_dict.keys() else np.nan  #投资风险偏好指数
    dict_add['jd_risk_pre_level'] = float(json_dict['risk_pre_level']) if 'risk_pre_level' in json_dict.keys() else np.nan  #投资风险偏好预测
    dict_add['jd_business_travel'] = float(json_dict['business_travel']) if 'business_travel' in json_dict.keys() else np.nan  #商旅人士指数
    dict_add['jd_have_child'] = float(json_dict['have_child']) if 'have_child' in json_dict.keys() else np.nan  #有无子女指数
    dict_add['jd_performance_score'] = float(json_dict['performance_score']) if 'performance_score' in json_dict.keys() else np.nan  #履约指数
    dict_add['jd_dec_ord_freq'] = float(json_dict['dec_ord_freq']) if 'dec_ord_freq' in json_dict.keys() else np.nan  #拒收订单等级
    dict_add['jd_mal_ord_freq'] = float(json_dict['mal_ord_freq']) if 'mal_ord_freq' in json_dict.keys() else np.nan  #恶意订单等级
    dict_add['jd_max_dlq_days_all_30_level'] = float(json_dict['max_dlq_days_all_30_level']) if 'max_dlq_days_all_30_level' in json_dict.keys() else np.nan  #历史30天内信用行为等级
    dict_add['jd_max_dlq_days_all_60_level'] = float(json_dict['max_dlq_days_all_60_level']) if 'max_dlq_days_all_60_level' in json_dict.keys() else np.nan  #历史60天内信用行为等级
    dict_add['jd_max_dlq_days_all_90_level'] = float(json_dict['max_dlq_days_all_90_level']) if 'max_dlq_days_all_90_level' in json_dict.keys() else np.nan  #历史90天内信用行为等级
    dict_add['jd_max_dlq_days_all_180_level'] = float(json_dict['max_dlq_days_all_180_level']) if 'max_dlq_days_all_180_level' in json_dict.keys() else np.nan  #历史180天内信用行为等级
    dict_add['jd_max_dlq_days_all_365_level'] = float(json_dict['max_dlq_days_all_365_level']) if 'max_dlq_days_all_365_level' in json_dict.keys() else np.nan  #历史365天内信用行为等级
    dict_add['jd_tot_dlq_days_all_30_level'] = float(json_dict['tot_dlq_days_all_30_level']) if 'tot_dlq_days_all_30_level' in json_dict.keys() else np.nan  #历史30天内履约等级
    dict_add['jd_tot_dlq_days_all_60_level'] = float(json_dict['tot_dlq_days_all_60_level']) if 'tot_dlq_days_all_60_level' in json_dict.keys() else np.nan  #历史60天内履约等级
    dict_add['jd_tot_dlq_days_all_90_level'] = float(json_dict['tot_dlq_days_all_90_level']) if 'tot_dlq_days_all_90_level' in json_dict.keys() else np.nan  #历史90天内履约等级
    dict_add['jd_tot_dlq_days_all_180_level'] = float(json_dict['tot_dlq_days_all_180_level']) if 'tot_dlq_days_all_180_level' in json_dict.keys() else np.nan  #历史180天内履约等级
    dict_add['jd_tot_dlq_days_all_365_level'] = float(json_dict['tot_dlq_days_all_365_level']) if 'tot_dlq_days_all_365_level' in json_dict.keys() else np.nan  #历史365天内履约等级
    dict_add['jd_time_on_book'] = float(json_dict['time_on_book']) if 'time_on_book' in json_dict.keys() else np.nan  #信用账龄等级
    dict_add['jd_tot_30_amt_level'] = float(json_dict['tot_30_amt_level']) if 'tot_30_amt_level' in json_dict.keys() else np.nan  #历史30天内信用消费金额等级
    dict_add['jd_tot_60_amt_level'] = float(json_dict['tot_60_amt_level']) if 'tot_60_amt_level' in json_dict.keys() else np.nan  #历史60天内信用消费金额等级
    dict_add['jd_tot_90_amt_level'] = float(json_dict['tot_90_amt_level']) if 'tot_90_amt_level' in json_dict.keys() else np.nan  #历史90天内信用消费金额等级
    dict_add['jd_tot_180_amt_level'] = float(json_dict['tot_180_amt_level']) if 'tot_180_amt_level' in json_dict.keys() else np.nan  #历史180天内信用消费金额等级
    dict_add['jd_tot_365_amt_level'] = float(json_dict['tot_365_amt_level']) if 'tot_365_amt_level' in json_dict.keys() else np.nan  #历史365天内信用消费金额等级
    dict_add['jd_tot_30_cnt_level'] = float(json_dict['tot_30_cnt_level']) if 'tot_30_cnt_level' in json_dict.keys() else np.nan  #历史30天内信用消费次数等级
    dict_add['jd_tot_60_cnt_level'] = float(json_dict['tot_60_cnt_level']) if 'tot_60_cnt_level' in json_dict.keys() else np.nan  #历史60天内信用消费次数等级
    dict_add['jd_tot_90_cnt_level'] = float(json_dict['tot_90_cnt_level']) if 'tot_90_cnt_level' in json_dict.keys() else np.nan  #历史90天内信用消费次数等级
    dict_add['jd_tot_180_cnt_level'] = float(json_dict['tot_180_cnt_level']) if 'tot_180_cnt_level' in json_dict.keys() else np.nan  #历史180天内信用消费次数等级
    dict_add['jd_tot_365_cnt_level'] = float(json_dict['tot_365_cnt_level']) if 'tot_365_cnt_level' in json_dict.keys() else np.nan  #历史365天内信用消费次数等级
    dict_add['jd_tot_charge_30_amt_level'] = float(json_dict['tot_charge_30_amt_level']) if 'tot_charge_30_amt_level' in json_dict.keys() else np.nan  #历史30天内(非免息)信用消费金额等级
    dict_add['jd_tot_charge_60_amt_level'] = float(json_dict['tot_charge_60_amt_level']) if 'tot_charge_60_amt_level' in json_dict.keys() else np.nan  #历史60天内(非免息)信用消费金额等级
    dict_add['jd_tot_charge_90_amt_level'] = float(json_dict['tot_charge_90_amt_level']) if 'tot_charge_90_amt_level' in json_dict.keys() else np.nan  #历史90天内(非免息)信用消费金额等级
    dict_add['jd_tot_charge_180_amt_level'] = float(json_dict['tot_charge_180_amt_level']) if 'tot_charge_180_amt_level' in json_dict.keys() else np.nan  #历史180天内(非免息)信用消费金额等级
    dict_add['jd_tot_charge_365_amt_level'] = float(json_dict['tot_charge_365_amt_level']) if 'tot_charge_365_amt_level' in json_dict.keys() else np.nan  #历史365天内(非免息)信用消费金额等级
    dict_add['jd_tot_charge_30_cnt_level'] = float(json_dict['tot_charge_30_cnt_level']) if 'tot_charge_30_cnt_level' in json_dict.keys() else np.nan  #历史30天内(非免息)信用消费次数等级
    dict_add['jd_tot_charge_60_cnt_level'] = float(json_dict['tot_charge_60_cnt_level']) if 'tot_charge_60_cnt_level' in json_dict.keys() else np.nan  #历史60天内(非免息)信用消费次数等级
    dict_add['jd_tot_charge_90_cnt_level'] = float(json_dict['tot_charge_90_cnt_level']) if 'tot_charge_90_cnt_level' in json_dict.keys() else np.nan  #历史90天内(非免息)信用消费次数等级
    dict_add['jd_tot_charge_180_cnt_level'] = float(json_dict['tot_charge_180_cnt_level']) if 'tot_charge_180_cnt_level' in json_dict.keys() else np.nan  #历史180天内(非免息)信用消费次数等级
    dict_add['jd_tot_charge_365_cnt_level'] = float(json_dict['tot_charge_365_cnt_level']) if 'tot_charge_365_cnt_level' in json_dict.keys() else np.nan  #历史365天内(非免息)信用消费次数等级
    dict_add['jd_tot_180_m_cnt_level'] = float(json_dict['tot_180_m_cnt_level']) if 'tot_180_m_cnt_level' in json_dict.keys() else np.nan  #历史180天内信用消费月份数
    dict_add['jd_tot_365_m_cnt_level'] = float(json_dict['tot_365_m_cnt_level']) if 'tot_365_m_cnt_level' in json_dict.keys() else np.nan  #历史365天内信用消费月份数

    #消费品类偏好dict
    pur_preference_dict={1:'数码产品',2:'居家日用',3:'服饰箱包',4:'个护化妆',5:'家电厨房',6:'运动户外',7:'图书影音',8:'母婴用品',
                        9:'旅游出行',10:'珠宝首饰',11:'汽车相关',12:'食品生鲜',13:'宠物饲养',14:'其他',-1:'未查得',
                        '1':'数码产品','2':'居家日用','3':'服饰箱包','4':'个护化妆','5':'家电厨房','6':'运动户外','7':'图书影音','8':'母婴用品',
                        '9':'旅游出行','10':'珠宝首饰','11':'汽车相关','12':'食品生鲜','13':'宠物饲养','14':'其他','-1':'未查得'}

    dict_add['jd_pur_preference_dict'] = pur_preference_dict[dict_add['jd_pur_preference']] if dict_add['jd_pur_preference'] in pur_preference_dict.keys() else np.nan  #消费品类偏好dict

    dict_add['jd_pur_preference_is_digital']=1 if '1' in re.sub(" ","",str(dict_add['jd_pur_preference'])).split(",") else 0
    dict_add['jd_pur_preference_is_household']=1 if '2' in re.sub(" ","",str(dict_add['jd_pur_preference'])).split(",") else 0
    dict_add['jd_pur_preference_is_clothing']=1 if '3' in re.sub(" ","",str(dict_add['jd_pur_preference'])).split(",") else 0
    dict_add['jd_pur_preference_is_makeup']=1 if '4' in re.sub(" ","",str(dict_add['jd_pur_preference'])).split(",") else 0
    dict_add['jd_pur_preference_is_kitchen']=1 if '5' in re.sub(" ","",str(dict_add['jd_pur_preference'])).split(",") else 0
    dict_add['jd_pur_preference_is_outdoor']=1 if '6' in re.sub(" ","",str(dict_add['jd_pur_preference'])).split(",") else 0
    dict_add['jd_pur_preference_is_books']=1 if '7' in re.sub(" ","",str(dict_add['jd_pur_preference'])).split(",") else 0
    dict_add['jd_pur_preference_is_baby']=1 if '8' in re.sub(" ","",str(dict_add['jd_pur_preference'])).split(",") else 0
    dict_add['jd_pur_preference_is_traveling']=1 if '9' in re.sub(" ","",str(dict_add['jd_pur_preference'])).split(",") else 0
    dict_add['jd_pur_preference_is_jewelry']=1 if '10' in re.sub(" ","",str(dict_add['jd_pur_preference'])).split(",") else 0
    dict_add['jd_pur_preference_is_car']=1 if '11' in re.sub(" ","",str(dict_add['jd_pur_preference'])).split(",") else 0
    dict_add['jd_pur_preference_is_food']=1 if '12' in re.sub(" ","",str(dict_add['jd_pur_preference'])).split(",") else 0
    dict_add['jd_pur_preference_is_pet']=1 if '13' in re.sub(" ","",str(dict_add['jd_pur_preference'])).split(",") else 0
    dict_add['jd_pur_preference_is_others']=1 if '14' in re.sub(" ","",str(dict_add['jd_pur_preference'])).split(",") else 0
    dict_add['jd_pur_preference_is_unknow']=1 if '-1' in re.sub(" ","",str(dict_add['jd_pur_preference'])).split(",") else 0

    
    #支付方式偏好dict
    pay_preference_dict={1:'线下支付',2:'三方支付',3:'线上信用付',4:'其他',-1:'未查得',
                        '1':'线下支付','2':'三方支付','3':'线上信用付','4':'其他','-1':'未查得'}
    dict_add['jd_pay_preference_dict'] = pay_preference_dict[dict_add['jd_pay_preference']] if dict_add['jd_pay_preference'] in pur_preference_dict.keys() else np.nan  #支付方式偏好dict

    dict_add['jd_pay_preference_is_offline']=1 if len(re.findall('1',str(dict_add['jd_pay_preference'])))>0 else 0
    dict_add['jd_pay_preference_is_thirdpay']=1 if len(re.findall('2',str(dict_add['jd_pay_preference'])))>0 else 0
    dict_add['jd_pay_preference_is_onlinecard']=1 if len(re.findall('3',str(dict_add['jd_pay_preference'])))>0 else 0
    dict_add['jd_pay_preference_is_others']=1 if len(re.findall('4',str(dict_add['jd_pay_preference'])))>0 else 0
    


    dict_add['jd_ecommerceAddressStability_DIV_jd_tot_charge_90_cnt_level']=dict_add['jd_ecommerceAddressStability']/dict_add['jd_tot_charge_90_cnt_level'] if dict_add['jd_tot_charge_90_cnt_level'] > 0 else np.nan
    dict_add['jd_ecommerceAddressStability_MUL_jd_business_travel']=dict_add['jd_ecommerceAddressStability']*dict_add['jd_business_travel']
    dict_add['jd_ecommerceAddressStability_DIV_jd_business_travel']=dict_add['jd_ecommerceAddressStability']/dict_add['jd_business_travel'] if dict_add['jd_business_travel'] > 0 else np.nan
    dict_add['jd_ecommerceAddressStability_MIN_jd_tot_charge_365_amt_level']=dict_add['jd_ecommerceAddressStability']-dict_add['jd_tot_charge_365_amt_level']
    dict_add['jd_ecommerceAddressStability_MIN_jd_buyingIndex']=dict_add['jd_ecommerceAddressStability']-dict_add['jd_buyingIndex']
    dict_add['jd_ecommerceAddressStability_DIV_jd_house_score']=dict_add['jd_ecommerceAddressStability']/dict_add['jd_house_score'] if dict_add['jd_house_score'] > 0 else np.nan
    dict_add['jd_ecommerceAddressStability_MIN_jd_cellphonePreference']=dict_add['jd_ecommerceAddressStability']-dict_add['jd_cellphonePreference']
    dict_add['jd_ecommerceAddressStability_MIN_jd_tot_30_cnt_level']=dict_add['jd_ecommerceAddressStability']-dict_add['jd_tot_30_cnt_level']
    dict_add['jd_ecommerceAddressStability_MIN_jd_tob_rank']=dict_add['jd_ecommerceAddressStability']-dict_add['jd_tob_rank']
    dict_add['jd_ecommerceAddressStability_MIN_jd_tot_180_amt_level']=dict_add['jd_ecommerceAddressStability']-dict_add['jd_tot_180_amt_level']
    dict_add['jd_ecommerceAddressStability_MUL_jd_riskIndex']=dict_add['jd_ecommerceAddressStability']*dict_add['jd_riskIndex']
    dict_add['jd_ecommerceAddressStability_MIN_jd_tot_60_amt_level']=dict_add['jd_ecommerceAddressStability']-dict_add['jd_tot_60_amt_level']
    dict_add['jd_ecommerceAddressStability_MUL_jd_worktimeShopping']=dict_add['jd_ecommerceAddressStability']*dict_add['jd_worktimeShopping']
    dict_add['jd_ecommerceAddressStability_DIV_jd_worktimeShopping']=dict_add['jd_ecommerceAddressStability']/dict_add['jd_worktimeShopping'] if dict_add['jd_worktimeShopping'] > 0 else np.nan
    dict_add['jd_ecommerceAddressStability_MIN_jd_time_on_book']=dict_add['jd_ecommerceAddressStability']-dict_add['jd_time_on_book']
    dict_add['jd_ecommerceAddressStability_MUL_jd_time_on_book']=dict_add['jd_ecommerceAddressStability']*dict_add['jd_time_on_book']
    dict_add['jd_ecommerceAddressStability_DIV_jd_time_on_book']=dict_add['jd_ecommerceAddressStability']/dict_add['jd_time_on_book'] if dict_add['jd_time_on_book'] > 0 else np.nan
    dict_add['jd_ecommerceAddressStability_DIV_jd_car_score']=dict_add['jd_ecommerceAddressStability']/dict_add['jd_car_score'] if dict_add['jd_car_score'] > 0 else np.nan
    dict_add['jd_ecommerceAddressStability_DIV_jd_max_dlq_days_all_180_level']=dict_add['jd_ecommerceAddressStability']/dict_add['jd_max_dlq_days_all_180_level'] if dict_add['jd_max_dlq_days_all_180_level'] > 0 else np.nan
    dict_add['jd_ecommerceAddressStability_MUL_jd_income_score']=dict_add['jd_ecommerceAddressStability']*dict_add['jd_income_score']
    dict_add['jd_ecommerceAddressStability_MIN_jd_tot_charge_60_amt_level']=dict_add['jd_ecommerceAddressStability']-dict_add['jd_tot_charge_60_amt_level']
    dict_add['jd_ecommerceAddressStability_MIN_jd_tot_charge_180_amt_level']=dict_add['jd_ecommerceAddressStability']-dict_add['jd_tot_charge_180_amt_level']
    dict_add['jd_ecommerceAddressStability_MUL_jd_tot_charge_30_cnt_level']=dict_add['jd_ecommerceAddressStability']*dict_add['jd_tot_charge_30_cnt_level']
    dict_add['jd_ecommerceAddressStability_DIV_jd_riskCategoryConsuming']=dict_add['jd_ecommerceAddressStability']/dict_add['jd_riskCategoryConsuming'] if dict_add['jd_riskCategoryConsuming'] > 0 else np.nan
    dict_add['jd_ecommerceAddressStability_DIV_jd_riskPeriodConsuming']=dict_add['jd_ecommerceAddressStability']/dict_add['jd_riskPeriodConsuming'] if dict_add['jd_riskPeriodConsuming'] > 0 else np.nan
    dict_add['jd_ecommerceAddressStability_MIN_jd_liability_level']=dict_add['jd_ecommerceAddressStability']-dict_add['jd_liability_level']
    dict_add['jd_ecommerceAddressStability_MUL_jd_risk_pre_score']=dict_add['jd_ecommerceAddressStability']*dict_add['jd_risk_pre_score']
    dict_add['jd_ecommerceAddressStability_DIV_jd_risk_pre_score']=dict_add['jd_ecommerceAddressStability']/dict_add['jd_risk_pre_score'] if dict_add['jd_risk_pre_score'] > 0 else np.nan
    dict_add['jd_ecommerceAddressStability_MIN_jd_stability']=dict_add['jd_ecommerceAddressStability']-dict_add['jd_stability']
    dict_add['jd_ecommerceAddressStability_MIN_jd_tot_30_amt_level']=dict_add['jd_ecommerceAddressStability']-dict_add['jd_tot_30_amt_level']
    dict_add['jd_ecommerceAddressStability_DIV_jd_tot_dlq_days_all_365_level']=dict_add['jd_ecommerceAddressStability']/dict_add['jd_tot_dlq_days_all_365_level'] if dict_add['jd_tot_dlq_days_all_365_level'] > 0 else np.nan
    dict_add['jd_tot_charge_90_cnt_level_DIV_jd_business_travel']=dict_add['jd_tot_charge_90_cnt_level']/dict_add['jd_business_travel'] if dict_add['jd_business_travel'] > 0 else np.nan
    dict_add['jd_tot_charge_90_cnt_level_MIN_jd_consume_m_freq']=dict_add['jd_tot_charge_90_cnt_level']-dict_add['jd_consume_m_freq']
    dict_add['jd_tot_charge_90_cnt_level_DIV_jd_tot_dlq_days_all_30_level']=dict_add['jd_tot_charge_90_cnt_level']/dict_add['jd_tot_dlq_days_all_30_level'] if dict_add['jd_tot_dlq_days_all_30_level'] > 0 else np.nan
    dict_add['jd_tot_charge_90_cnt_level_MIN_jd_tot_charge_365_amt_level']=dict_add['jd_tot_charge_90_cnt_level']-dict_add['jd_tot_charge_365_amt_level']
    dict_add['jd_tot_charge_90_cnt_level_MUL_jd_tot_dlq_days_all_180_level']=dict_add['jd_tot_charge_90_cnt_level']*dict_add['jd_tot_dlq_days_all_180_level']
    dict_add['jd_tot_charge_90_cnt_level_ADD_jd_tot_180_m_cnt_level']=dict_add['jd_tot_charge_90_cnt_level']+dict_add['jd_tot_180_m_cnt_level']
    dict_add['jd_tot_charge_90_cnt_level_MIN_jd_tot_90_cnt_level']=dict_add['jd_tot_charge_90_cnt_level']-dict_add['jd_tot_90_cnt_level']
    dict_add['jd_tot_charge_90_cnt_level_DIV_jd_tot_90_cnt_level']=dict_add['jd_tot_charge_90_cnt_level']/dict_add['jd_tot_90_cnt_level'] if dict_add['jd_tot_90_cnt_level'] > 0 else np.nan
    dict_add['jd_tot_charge_90_cnt_level_MIN_jd_consume_cnt_freq']=dict_add['jd_tot_charge_90_cnt_level']-dict_add['jd_consume_cnt_freq']
    dict_add['jd_tot_charge_90_cnt_level_DIV_jd_house_score']=dict_add['jd_tot_charge_90_cnt_level']/dict_add['jd_house_score'] if dict_add['jd_house_score'] > 0 else np.nan
    dict_add['jd_tot_charge_90_cnt_level_MIN_jd_tot_30_cnt_level']=dict_add['jd_tot_charge_90_cnt_level']-dict_add['jd_tot_30_cnt_level']
    dict_add['jd_tot_charge_90_cnt_level_MUL_jd_performance_score']=dict_add['jd_tot_charge_90_cnt_level']*dict_add['jd_performance_score']
    dict_add['jd_tot_charge_90_cnt_level_DIV_jd_performance_score']=dict_add['jd_tot_charge_90_cnt_level']/dict_add['jd_performance_score'] if dict_add['jd_performance_score'] > 0 else np.nan
    dict_add['jd_tot_charge_90_cnt_level_MIN_jd_tob_rank']=dict_add['jd_tot_charge_90_cnt_level']-dict_add['jd_tob_rank']
    dict_add['jd_tot_charge_90_cnt_level_MIN_jd_riskIndex']=dict_add['jd_tot_charge_90_cnt_level']-dict_add['jd_riskIndex']
    dict_add['jd_tot_charge_90_cnt_level_MIN_jd_tot_charge_180_cnt_level']=dict_add['jd_tot_charge_90_cnt_level']-dict_add['jd_tot_charge_180_cnt_level']
    dict_add['jd_tot_charge_90_cnt_level_MIN_jd_tot_60_cnt_level']=dict_add['jd_tot_charge_90_cnt_level']-dict_add['jd_tot_60_cnt_level']
    dict_add['jd_tot_charge_90_cnt_level_MUL_jd_worktimeShopping']=dict_add['jd_tot_charge_90_cnt_level']*dict_add['jd_worktimeShopping']
    dict_add['jd_tot_charge_90_cnt_level_MUL_jd_max_dlq_days_all_60_level']=dict_add['jd_tot_charge_90_cnt_level']*dict_add['jd_max_dlq_days_all_60_level']
    dict_add['jd_tot_charge_90_cnt_level_DIV_jd_car_score']=dict_add['jd_tot_charge_90_cnt_level']/dict_add['jd_car_score'] if dict_add['jd_car_score'] > 0 else np.nan
    dict_add['jd_tot_charge_90_cnt_level_MIN_jd_tot_charge_365_cnt_level']=dict_add['jd_tot_charge_90_cnt_level']-dict_add['jd_tot_charge_365_cnt_level']
    dict_add['jd_tot_charge_90_cnt_level_MUL_jd_have_child']=dict_add['jd_tot_charge_90_cnt_level']*dict_add['jd_have_child']
    dict_add['jd_tot_charge_90_cnt_level_MUL_jd_debt_bearing_level']=dict_add['jd_tot_charge_90_cnt_level']*dict_add['jd_debt_bearing_level']
    dict_add['jd_tot_charge_90_cnt_level_MIN_jd_max_dlq_days_all_180_level']=dict_add['jd_tot_charge_90_cnt_level']-dict_add['jd_max_dlq_days_all_180_level']
    dict_add['jd_tot_charge_90_cnt_level_MIN_jd_tot_charge_60_cnt_level']=dict_add['jd_tot_charge_90_cnt_level']-dict_add['jd_tot_charge_60_cnt_level']
    dict_add['jd_tot_charge_90_cnt_level_DIV_jd_income_score']=dict_add['jd_tot_charge_90_cnt_level']/dict_add['jd_income_score'] if dict_add['jd_income_score'] > 0 else np.nan
    dict_add['jd_tot_charge_90_cnt_level_MIN_jd_tot_365_amt_level']=dict_add['jd_tot_charge_90_cnt_level']-dict_add['jd_tot_365_amt_level']
    dict_add['jd_tot_charge_90_cnt_level_MIN_jd_tot_charge_180_amt_level']=dict_add['jd_tot_charge_90_cnt_level']-dict_add['jd_tot_charge_180_amt_level']
    dict_add['jd_tot_charge_90_cnt_level_MIN_jd_tot_charge_90_amt_level']=dict_add['jd_tot_charge_90_cnt_level']-dict_add['jd_tot_charge_90_amt_level']
    dict_add['jd_tot_charge_90_cnt_level_MIN_jd_tot_charge_30_cnt_level']=dict_add['jd_tot_charge_90_cnt_level']-dict_add['jd_tot_charge_30_cnt_level']
    dict_add['jd_tot_charge_90_cnt_level_MIN_jd_riskCategoryConsuming']=dict_add['jd_tot_charge_90_cnt_level']-dict_add['jd_riskCategoryConsuming']
    dict_add['jd_tot_charge_90_cnt_level_DIV_jd_riskCategoryConsuming']=dict_add['jd_tot_charge_90_cnt_level']/dict_add['jd_riskCategoryConsuming'] if dict_add['jd_riskCategoryConsuming'] > 0 else np.nan
    dict_add['jd_tot_charge_90_cnt_level_DIV_jd_risk_pre_score']=dict_add['jd_tot_charge_90_cnt_level']/dict_add['jd_risk_pre_score'] if dict_add['jd_risk_pre_score'] > 0 else np.nan
    dict_add['jd_tot_charge_90_cnt_level_DIV_jd_stability']=dict_add['jd_tot_charge_90_cnt_level']/dict_add['jd_stability'] if dict_add['jd_stability'] > 0 else np.nan
    dict_add['jd_tot_charge_90_cnt_level_MIN_jd_credit_consume_level']=dict_add['jd_tot_charge_90_cnt_level']-dict_add['jd_credit_consume_level']
    dict_add['jd_tot_charge_90_cnt_level_MIN_jd_tot_dlq_days_all_365_level']=dict_add['jd_tot_charge_90_cnt_level']-dict_add['jd_tot_dlq_days_all_365_level']
    dict_add['jd_business_travel_MUL_jd_tot_dlq_days_all_60_level']=dict_add['jd_business_travel']*dict_add['jd_tot_dlq_days_all_60_level']
    dict_add['jd_business_travel_MUL_jd_comsumingSocial']=dict_add['jd_business_travel']*dict_add['jd_comsumingSocial']
    dict_add['jd_business_travel_MUL_jd_consume_m_freq']=dict_add['jd_business_travel']*dict_add['jd_consume_m_freq']
    dict_add['jd_business_travel_DIV_jd_tot_charge_365_amt_level']=dict_add['jd_business_travel']/dict_add['jd_tot_charge_365_amt_level'] if dict_add['jd_tot_charge_365_amt_level'] > 0 else np.nan
    dict_add['jd_business_travel_MUL_jd_tot_dlq_days_all_180_level']=dict_add['jd_business_travel']*dict_add['jd_tot_dlq_days_all_180_level']
    dict_add['jd_business_travel_MUL_jd_asset_score']=dict_add['jd_business_travel']*dict_add['jd_asset_score']
    dict_add['jd_business_travel_ADD_jd_performance_score']=dict_add['jd_business_travel']+dict_add['jd_performance_score']
    dict_add['jd_business_travel_MUL_jd_performance_score']=dict_add['jd_business_travel']*dict_add['jd_performance_score']
    dict_add['jd_business_travel_ADD_jd_tob_rank']=dict_add['jd_business_travel']+dict_add['jd_tob_rank']
    dict_add['jd_business_travel_MIN_jd_tob_rank']=dict_add['jd_business_travel']-dict_add['jd_tob_rank']
    dict_add['jd_business_travel_MUL_jd_tot_365_cnt_level']=dict_add['jd_business_travel']*dict_add['jd_tot_365_cnt_level']
    dict_add['jd_business_travel_MIN_jd_riskIndex']=dict_add['jd_business_travel']-dict_add['jd_riskIndex']
    dict_add['jd_business_travel_DIV_jd_riskIndex']=dict_add['jd_business_travel']/dict_add['jd_riskIndex'] if dict_add['jd_riskIndex'] > 0 else np.nan
    dict_add['jd_business_travel_ADD_jd_car_score']=dict_add['jd_business_travel']+dict_add['jd_car_score']
    dict_add['jd_business_travel_DIV_jd_car_score']=dict_add['jd_business_travel']/dict_add['jd_car_score'] if dict_add['jd_car_score'] > 0 else np.nan
    dict_add['jd_business_travel_ADD_jd_have_child']=dict_add['jd_business_travel']+dict_add['jd_have_child']
    dict_add['jd_business_travel_MIN_jd_have_child']=dict_add['jd_business_travel']-dict_add['jd_have_child']
    dict_add['jd_business_travel_DIV_jd_debt_bearing_level']=dict_add['jd_business_travel']/dict_add['jd_debt_bearing_level'] if dict_add['jd_debt_bearing_level'] > 0 else np.nan
    dict_add['jd_business_travel_DIV_jd_tot_charge_180_amt_level']=dict_add['jd_business_travel']/dict_add['jd_tot_charge_180_amt_level'] if dict_add['jd_tot_charge_180_amt_level'] > 0 else np.nan
    dict_add['jd_business_travel_DIV_jd_riskCategoryConsuming']=dict_add['jd_business_travel']/dict_add['jd_riskCategoryConsuming'] if dict_add['jd_riskCategoryConsuming'] > 0 else np.nan
    dict_add['jd_business_travel_ADD_jd_tot_90_amt_level']=dict_add['jd_business_travel']+dict_add['jd_tot_90_amt_level']
    dict_add['jd_business_travel_MUL_jd_risk_pre_score']=dict_add['jd_business_travel']*dict_add['jd_risk_pre_score']
    dict_add['jd_tot_dlq_days_all_60_level_MUL_jd_risk_pre_level']=dict_add['jd_tot_dlq_days_all_60_level']*dict_add['jd_risk_pre_level']
    dict_add['jd_tot_dlq_days_all_60_level_MUL_jd_tot_charge_365_amt_level']=dict_add['jd_tot_dlq_days_all_60_level']*dict_add['jd_tot_charge_365_amt_level']
    dict_add['jd_tot_dlq_days_all_60_level_MUL_jd_tot_90_cnt_level']=dict_add['jd_tot_dlq_days_all_60_level']*dict_add['jd_tot_90_cnt_level']
    dict_add['jd_tot_dlq_days_all_60_level_MIN_jd_tob_rank']=dict_add['jd_tot_dlq_days_all_60_level']-dict_add['jd_tob_rank']
    dict_add['jd_tot_dlq_days_all_60_level_MUL_jd_car_score']=dict_add['jd_tot_dlq_days_all_60_level']*dict_add['jd_car_score']
    dict_add['jd_tot_dlq_days_all_60_level_DIV_jd_income_score']=dict_add['jd_tot_dlq_days_all_60_level']/dict_add['jd_income_score'] if dict_add['jd_income_score'] > 0 else np.nan
    dict_add['jd_tot_dlq_days_all_60_level_DIV_jd_stability']=dict_add['jd_tot_dlq_days_all_60_level']/dict_add['jd_stability'] if dict_add['jd_stability'] > 0 else np.nan
    dict_add['jd_tot_dlq_days_all_60_level_MUL_jd_tot_dlq_days_all_365_level']=dict_add['jd_tot_dlq_days_all_60_level']*dict_add['jd_tot_dlq_days_all_365_level']
    dict_add['jd_risk_pre_level_MUL_jd_tot_dlq_days_all_90_level']=dict_add['jd_risk_pre_level']*dict_add['jd_tot_dlq_days_all_90_level']
    dict_add['jd_risk_pre_level_MIN_jd_tot_dlq_days_all_180_level']=dict_add['jd_risk_pre_level']-dict_add['jd_tot_dlq_days_all_180_level']
    dict_add['jd_risk_pre_level_DIV_jd_max_dlq_days_all_30_level']=dict_add['jd_risk_pre_level']/dict_add['jd_max_dlq_days_all_30_level'] if dict_add['jd_max_dlq_days_all_30_level'] > 0 else np.nan
    dict_add['jd_risk_pre_level_MIN_jd_max_dlq_days_all_365_level']=dict_add['jd_risk_pre_level']-dict_add['jd_max_dlq_days_all_365_level']
    dict_add['jd_risk_pre_level_MUL_jd_max_dlq_days_all_365_level']=dict_add['jd_risk_pre_level']*dict_add['jd_max_dlq_days_all_365_level']
    dict_add['jd_risk_pre_level_MIN_jd_max_dlq_days_all_90_level']=dict_add['jd_risk_pre_level']-dict_add['jd_max_dlq_days_all_90_level']
    dict_add['jd_risk_pre_level_MUL_jd_max_dlq_days_all_90_level']=dict_add['jd_risk_pre_level']*dict_add['jd_max_dlq_days_all_90_level']
    dict_add['jd_risk_pre_level_MUL_jd_performance_score']=dict_add['jd_risk_pre_level']*dict_add['jd_performance_score']
    dict_add['jd_risk_pre_level_MIN_jd_tot_charge_180_cnt_level']=dict_add['jd_risk_pre_level']-dict_add['jd_tot_charge_180_cnt_level']
    dict_add['jd_risk_pre_level_MIN_jd_cd_level']=dict_add['jd_risk_pre_level']-dict_add['jd_cd_level']
    dict_add['jd_risk_pre_level_MUL_jd_max_dlq_days_all_60_level']=dict_add['jd_risk_pre_level']*dict_add['jd_max_dlq_days_all_60_level']
    dict_add['jd_risk_pre_level_ADD_jd_car_score']=dict_add['jd_risk_pre_level']+dict_add['jd_car_score']
    dict_add['jd_risk_pre_level_MIN_jd_car_score']=dict_add['jd_risk_pre_level']-dict_add['jd_car_score']
    dict_add['jd_risk_pre_level_DIV_jd_car_score']=dict_add['jd_risk_pre_level']/dict_add['jd_car_score'] if dict_add['jd_car_score'] > 0 else np.nan
    dict_add['jd_risk_pre_level_MIN_jd_max_dlq_days_all_180_level']=dict_add['jd_risk_pre_level']-dict_add['jd_max_dlq_days_all_180_level']
    dict_add['jd_risk_pre_level_DIV_jd_riskCategoryConsuming']=dict_add['jd_risk_pre_level']/dict_add['jd_riskCategoryConsuming'] if dict_add['jd_riskCategoryConsuming'] > 0 else np.nan
    dict_add['jd_risk_pre_level_DIV_jd_riskPeriodConsuming']=dict_add['jd_risk_pre_level']/dict_add['jd_riskPeriodConsuming'] if dict_add['jd_riskPeriodConsuming'] > 0 else np.nan
    dict_add['jd_risk_pre_level_MIN_jd_stability']=dict_add['jd_risk_pre_level']-dict_add['jd_stability']
    dict_add['jd_risk_pre_level_MIN_jd_tot_dlq_days_all_365_level']=dict_add['jd_risk_pre_level']-dict_add['jd_tot_dlq_days_all_365_level']
    dict_add['jd_risk_pre_level_MUL_jd_tot_dlq_days_all_365_level']=dict_add['jd_risk_pre_level']*dict_add['jd_tot_dlq_days_all_365_level']
    dict_add['jd_dec_ord_freq_MIN_jd_tot_dlq_days_all_30_level']=dict_add['jd_dec_ord_freq']-dict_add['jd_tot_dlq_days_all_30_level']
    dict_add['jd_dec_ord_freq_DIV_jd_asset_score']=dict_add['jd_dec_ord_freq']/dict_add['jd_asset_score'] if dict_add['jd_asset_score'] > 0 else np.nan
    dict_add['jd_dec_ord_freq_MUL_jd_max_dlq_days_all_30_level']=dict_add['jd_dec_ord_freq']*dict_add['jd_max_dlq_days_all_30_level']
    dict_add['jd_dec_ord_freq_MIN_jd_tob_rank']=dict_add['jd_dec_ord_freq']-dict_add['jd_tob_rank']
    dict_add['jd_dec_ord_freq_DIV_jd_riskIndex']=dict_add['jd_dec_ord_freq']/dict_add['jd_riskIndex'] if dict_add['jd_riskIndex'] > 0 else np.nan
    dict_add['jd_dec_ord_freq_MIN_jd_tot_charge_30_cnt_level']=dict_add['jd_dec_ord_freq']-dict_add['jd_tot_charge_30_cnt_level']
    dict_add['jd_dec_ord_freq_DIV_jd_risk_pre_score']=dict_add['jd_dec_ord_freq']/dict_add['jd_risk_pre_score'] if dict_add['jd_risk_pre_score'] > 0 else np.nan
    dict_add['jd_dec_ord_freq_MIN_jd_tot_30_amt_level']=dict_add['jd_dec_ord_freq']-dict_add['jd_tot_30_amt_level']
    dict_add['jd_comsumingSocial_MIN_jd_asset_score']=dict_add['jd_comsumingSocial']-dict_add['jd_asset_score']
    dict_add['jd_comsumingSocial_MIN_jd_performance_score']=dict_add['jd_comsumingSocial']-dict_add['jd_performance_score']
    dict_add['jd_comsumingSocial_MUL_jd_performance_score']=dict_add['jd_comsumingSocial']*dict_add['jd_performance_score']
    dict_add['jd_comsumingSocial_DIV_jd_tob_rank']=dict_add['jd_comsumingSocial']/dict_add['jd_tob_rank'] if dict_add['jd_tob_rank'] > 0 else np.nan
    dict_add['jd_comsumingSocial_MIN_jd_worktimeShopping']=dict_add['jd_comsumingSocial']-dict_add['jd_worktimeShopping']
    dict_add['jd_comsumingSocial_ADD_jd_income_score']=dict_add['jd_comsumingSocial']+dict_add['jd_income_score']
    dict_add['jd_comsumingSocial_ADD_jd_tot_charge_90_amt_level']=dict_add['jd_comsumingSocial']+dict_add['jd_tot_charge_90_amt_level']
    dict_add['jd_comsumingSocial_MIN_jd_tot_charge_90_amt_level']=dict_add['jd_comsumingSocial']-dict_add['jd_tot_charge_90_amt_level']
    dict_add['jd_comsumingSocial_ADD_jd_tot_charge_30_cnt_level']=dict_add['jd_comsumingSocial']+dict_add['jd_tot_charge_30_cnt_level']
    dict_add['jd_comsumingSocial_MIN_jd_riskCategoryConsuming']=dict_add['jd_comsumingSocial']-dict_add['jd_riskCategoryConsuming']
    dict_add['jd_comsumingSocial_DIV_jd_riskCategoryConsuming']=dict_add['jd_comsumingSocial']/dict_add['jd_riskCategoryConsuming'] if dict_add['jd_riskCategoryConsuming'] > 0 else np.nan
    dict_add['jd_comsumingSocial_MIN_jd_riskPeriodConsuming']=dict_add['jd_comsumingSocial']-dict_add['jd_riskPeriodConsuming']
    dict_add['jd_comsumingSocial_DIV_jd_risk_pre_score']=dict_add['jd_comsumingSocial']/dict_add['jd_risk_pre_score'] if dict_add['jd_risk_pre_score'] > 0 else np.nan
    dict_add['jd_tot_180_cnt_level_MIN_jd_consume_m_freq']=dict_add['jd_tot_180_cnt_level']-dict_add['jd_consume_m_freq']
    dict_add['jd_tot_180_cnt_level_MIN_jd_tot_180_m_cnt_level']=dict_add['jd_tot_180_cnt_level']-dict_add['jd_tot_180_m_cnt_level']
    dict_add['jd_tot_180_cnt_level_MIN_jd_tot_90_cnt_level']=dict_add['jd_tot_180_cnt_level']-dict_add['jd_tot_90_cnt_level']
    dict_add['jd_tot_180_cnt_level_MUL_jd_tot_90_cnt_level']=dict_add['jd_tot_180_cnt_level']*dict_add['jd_tot_90_cnt_level']
    dict_add['jd_tot_180_cnt_level_MIN_jd_consume_cnt_freq']=dict_add['jd_tot_180_cnt_level']-dict_add['jd_consume_cnt_freq']
    dict_add['jd_tot_180_cnt_level_MIN_jd_tot_365_cnt_level']=dict_add['jd_tot_180_cnt_level']-dict_add['jd_tot_365_cnt_level']
    dict_add['jd_tot_180_cnt_level_MIN_jd_ecommercecellphoneStability']=dict_add['jd_tot_180_cnt_level']-dict_add['jd_ecommercecellphoneStability']
    dict_add['jd_tot_180_cnt_level_MIN_jd_time_on_book']=dict_add['jd_tot_180_cnt_level']-dict_add['jd_time_on_book']
    dict_add['jd_tot_180_cnt_level_DIV_jd_car_score']=dict_add['jd_tot_180_cnt_level']/dict_add['jd_car_score'] if dict_add['jd_car_score'] > 0 else np.nan
    dict_add['jd_tot_180_cnt_level_MIN_jd_gd_level']=dict_add['jd_tot_180_cnt_level']-dict_add['jd_gd_level']
    dict_add['jd_tot_180_cnt_level_MIN_jd_tot_charge_180_amt_level']=dict_add['jd_tot_180_cnt_level']-dict_add['jd_tot_charge_180_amt_level']
    dict_add['jd_tot_180_cnt_level_MIN_jd_riskCategoryConsuming']=dict_add['jd_tot_180_cnt_level']-dict_add['jd_riskCategoryConsuming']
    dict_add['jd_tot_180_cnt_level_MIN_jd_tot_365_m_cnt_level']=dict_add['jd_tot_180_cnt_level']-dict_add['jd_tot_365_m_cnt_level']
    dict_add['jd_tot_180_cnt_level_DIV_jd_risk_pre_score']=dict_add['jd_tot_180_cnt_level']/dict_add['jd_risk_pre_score'] if dict_add['jd_risk_pre_score'] > 0 else np.nan
    dict_add['jd_tot_180_cnt_level_MIN_jd_credit_consume_level']=dict_add['jd_tot_180_cnt_level']-dict_add['jd_credit_consume_level']
    dict_add['jd_consume_m_freq_MIN_jd_tot_charge_365_amt_level']=dict_add['jd_consume_m_freq']-dict_add['jd_tot_charge_365_amt_level']
    dict_add['jd_consume_m_freq_MIN_jd_resonableConsuming']=dict_add['jd_consume_m_freq']-dict_add['jd_resonableConsuming']
    dict_add['jd_consume_m_freq_MIN_jd_tot_180_m_cnt_level']=dict_add['jd_consume_m_freq']-dict_add['jd_tot_180_m_cnt_level']
    dict_add['jd_consume_m_freq_DIV_jd_consume_cnt_freq']=dict_add['jd_consume_m_freq']/dict_add['jd_consume_cnt_freq'] if dict_add['jd_consume_cnt_freq'] > 0 else np.nan
    dict_add['jd_consume_m_freq_MIN_jd_cellphonePreference']=dict_add['jd_consume_m_freq']-dict_add['jd_cellphonePreference']
    dict_add['jd_consume_m_freq_MUL_jd_performance_score']=dict_add['jd_consume_m_freq']*dict_add['jd_performance_score']
    dict_add['jd_consume_m_freq_DIV_jd_performance_score']=dict_add['jd_consume_m_freq']/dict_add['jd_performance_score'] if dict_add['jd_performance_score'] > 0 else np.nan
    dict_add['jd_consume_m_freq_ADD_jd_tob_rank']=dict_add['jd_consume_m_freq']+dict_add['jd_tob_rank']
    dict_add['jd_consume_m_freq_DIV_jd_tob_rank']=dict_add['jd_consume_m_freq']/dict_add['jd_tob_rank'] if dict_add['jd_tob_rank'] > 0 else np.nan
    dict_add['jd_consume_m_freq_MIN_jd_tot_180_amt_level']=dict_add['jd_consume_m_freq']-dict_add['jd_tot_180_amt_level']
    dict_add['jd_consume_m_freq_MIN_jd_tot_365_cnt_level']=dict_add['jd_consume_m_freq']-dict_add['jd_tot_365_cnt_level']
    dict_add['jd_consume_m_freq_DIV_jd_riskIndex']=dict_add['jd_consume_m_freq']/dict_add['jd_riskIndex'] if dict_add['jd_riskIndex'] > 0 else np.nan
    dict_add['jd_consume_m_freq_MIN_jd_tot_charge_180_cnt_level']=dict_add['jd_consume_m_freq']-dict_add['jd_tot_charge_180_cnt_level']
    dict_add['jd_consume_m_freq_MIN_jd_cd_level']=dict_add['jd_consume_m_freq']-dict_add['jd_cd_level']
    dict_add['jd_consume_m_freq_DIV_jd_car_score']=dict_add['jd_consume_m_freq']/dict_add['jd_car_score'] if dict_add['jd_car_score'] > 0 else np.nan
    dict_add['jd_consume_m_freq_MIN_jd_tot_charge_365_cnt_level']=dict_add['jd_consume_m_freq']-dict_add['jd_tot_charge_365_cnt_level']
    dict_add['jd_consume_m_freq_MIN_jd_debt_bearing_level']=dict_add['jd_consume_m_freq']-dict_add['jd_debt_bearing_level']
    dict_add['jd_consume_m_freq_MIN_jd_tot_charge_60_cnt_level']=dict_add['jd_consume_m_freq']-dict_add['jd_tot_charge_60_cnt_level']
    dict_add['jd_consume_m_freq_DIV_jd_tot_charge_60_cnt_level']=dict_add['jd_consume_m_freq']/dict_add['jd_tot_charge_60_cnt_level'] if dict_add['jd_tot_charge_60_cnt_level'] > 0 else np.nan
    dict_add['jd_consume_m_freq_MIN_jd_gd_level']=dict_add['jd_consume_m_freq']-dict_add['jd_gd_level']
    dict_add['jd_consume_m_freq_DIV_jd_income_score']=dict_add['jd_consume_m_freq']/dict_add['jd_income_score'] if dict_add['jd_income_score'] > 0 else np.nan
    dict_add['jd_consume_m_freq_MIN_jd_tot_365_amt_level']=dict_add['jd_consume_m_freq']-dict_add['jd_tot_365_amt_level']
    dict_add['jd_consume_m_freq_MIN_jd_tot_charge_180_amt_level']=dict_add['jd_consume_m_freq']-dict_add['jd_tot_charge_180_amt_level']
    dict_add['jd_consume_m_freq_DIV_jd_riskCategoryConsuming']=dict_add['jd_consume_m_freq']/dict_add['jd_riskCategoryConsuming'] if dict_add['jd_riskCategoryConsuming'] > 0 else np.nan
    dict_add['jd_consume_m_freq_DIV_jd_riskPeriodConsuming']=dict_add['jd_consume_m_freq']/dict_add['jd_riskPeriodConsuming'] if dict_add['jd_riskPeriodConsuming'] > 0 else np.nan
    dict_add['jd_consume_m_freq_MUL_jd_risk_pre_score']=dict_add['jd_consume_m_freq']*dict_add['jd_risk_pre_score']
    dict_add['jd_consume_m_freq_MIN_jd_credit_consume_level']=dict_add['jd_consume_m_freq']-dict_add['jd_credit_consume_level']
    dict_add['jd_tot_dlq_days_all_30_level_MUL_jd_resonableConsuming']=dict_add['jd_tot_dlq_days_all_30_level']*dict_add['jd_resonableConsuming']
    dict_add['jd_tot_dlq_days_all_30_level_MUL_jd_max_dlq_days_all_30_level']=dict_add['jd_tot_dlq_days_all_30_level']*dict_add['jd_max_dlq_days_all_30_level']
    dict_add['jd_tot_dlq_days_all_30_level_MUL_jd_consume_cnt_freq']=dict_add['jd_tot_dlq_days_all_30_level']*dict_add['jd_consume_cnt_freq']
    dict_add['jd_tot_dlq_days_all_30_level_ADD_jd_cellphonePreference']=dict_add['jd_tot_dlq_days_all_30_level']+dict_add['jd_cellphonePreference']
    dict_add['jd_tot_dlq_days_all_30_level_MUL_jd_tot_30_cnt_level']=dict_add['jd_tot_dlq_days_all_30_level']*dict_add['jd_tot_30_cnt_level']
    dict_add['jd_tot_dlq_days_all_30_level_MIN_jd_tob_rank']=dict_add['jd_tot_dlq_days_all_30_level']-dict_add['jd_tob_rank']
    dict_add['jd_tot_dlq_days_all_30_level_MIN_jd_tot_charge_60_amt_level']=dict_add['jd_tot_dlq_days_all_30_level']-dict_add['jd_tot_charge_60_amt_level']
    dict_add['jd_tot_dlq_days_all_30_level_MUL_jd_tot_charge_30_cnt_level']=dict_add['jd_tot_dlq_days_all_30_level']*dict_add['jd_tot_charge_30_cnt_level']
    dict_add['jd_tot_dlq_days_all_30_level_DIV_jd_risk_pre_score']=dict_add['jd_tot_dlq_days_all_30_level']/dict_add['jd_risk_pre_score'] if dict_add['jd_risk_pre_score'] > 0 else np.nan
    dict_add['jd_tot_dlq_days_all_90_level_ADD_jd_house_score']=dict_add['jd_tot_dlq_days_all_90_level']+dict_add['jd_house_score']
    dict_add['jd_tot_dlq_days_all_90_level_MIN_jd_tob_rank']=dict_add['jd_tot_dlq_days_all_90_level']-dict_add['jd_tob_rank']
    dict_add['jd_tot_dlq_days_all_90_level_MIN_jd_riskPeriodConsuming']=dict_add['jd_tot_dlq_days_all_90_level']-dict_add['jd_riskPeriodConsuming']
    dict_add['jd_tot_charge_365_amt_level_MUL_jd_tot_dlq_days_all_180_level']=dict_add['jd_tot_charge_365_amt_level']*dict_add['jd_tot_dlq_days_all_180_level']
    dict_add['jd_tot_charge_365_amt_level_MIN_jd_house_score']=dict_add['jd_tot_charge_365_amt_level']-dict_add['jd_house_score']
    dict_add['jd_tot_charge_365_amt_level_MIN_jd_tot_30_cnt_level']=dict_add['jd_tot_charge_365_amt_level']-dict_add['jd_tot_30_cnt_level']
    dict_add['jd_tot_charge_365_amt_level_MIN_jd_performance_score']=dict_add['jd_tot_charge_365_amt_level']-dict_add['jd_performance_score']
    dict_add['jd_tot_charge_365_amt_level_MIN_jd_tob_rank']=dict_add['jd_tot_charge_365_amt_level']-dict_add['jd_tob_rank']
    dict_add['jd_tot_charge_365_amt_level_MIN_jd_tot_180_amt_level']=dict_add['jd_tot_charge_365_amt_level']-dict_add['jd_tot_180_amt_level']
    dict_add['jd_tot_charge_365_amt_level_DIV_jd_tot_180_amt_level']=dict_add['jd_tot_charge_365_amt_level']/dict_add['jd_tot_180_amt_level'] if dict_add['jd_tot_180_amt_level'] > 0 else np.nan
    dict_add['jd_tot_charge_365_amt_level_MIN_jd_tot_365_cnt_level']=dict_add['jd_tot_charge_365_amt_level']-dict_add['jd_tot_365_cnt_level']
    dict_add['jd_tot_charge_365_amt_level_MIN_jd_riskIndex']=dict_add['jd_tot_charge_365_amt_level']-dict_add['jd_riskIndex']
    dict_add['jd_tot_charge_365_amt_level_MIN_jd_tot_60_amt_level']=dict_add['jd_tot_charge_365_amt_level']-dict_add['jd_tot_60_amt_level']
    dict_add['jd_tot_charge_365_amt_level_MIN_jd_tot_60_cnt_level']=dict_add['jd_tot_charge_365_amt_level']-dict_add['jd_tot_60_cnt_level']
    dict_add['jd_tot_charge_365_amt_level_MUL_jd_worktimeShopping']=dict_add['jd_tot_charge_365_amt_level']*dict_add['jd_worktimeShopping']
    dict_add['jd_tot_charge_365_amt_level_MIN_jd_tot_charge_365_cnt_level']=dict_add['jd_tot_charge_365_amt_level']-dict_add['jd_tot_charge_365_cnt_level']
    dict_add['jd_tot_charge_365_amt_level_MIN_jd_debt_bearing_level']=dict_add['jd_tot_charge_365_amt_level']-dict_add['jd_debt_bearing_level']
    dict_add['jd_tot_charge_365_amt_level_MIN_jd_tot_charge_60_cnt_level']=dict_add['jd_tot_charge_365_amt_level']-dict_add['jd_tot_charge_60_cnt_level']
    dict_add['jd_tot_charge_365_amt_level_MIN_jd_gd_level']=dict_add['jd_tot_charge_365_amt_level']-dict_add['jd_gd_level']
    dict_add['jd_tot_charge_365_amt_level_MIN_jd_tot_charge_60_amt_level']=dict_add['jd_tot_charge_365_amt_level']-dict_add['jd_tot_charge_60_amt_level']
    dict_add['jd_tot_charge_365_amt_level_MIN_jd_tot_charge_180_amt_level']=dict_add['jd_tot_charge_365_amt_level']-dict_add['jd_tot_charge_180_amt_level']
    dict_add['jd_tot_charge_365_amt_level_MIN_jd_tot_charge_90_amt_level']=dict_add['jd_tot_charge_365_amt_level']-dict_add['jd_tot_charge_90_amt_level']
    dict_add['jd_tot_charge_365_amt_level_MIN_jd_tot_charge_30_cnt_level']=dict_add['jd_tot_charge_365_amt_level']-dict_add['jd_tot_charge_30_cnt_level']
    dict_add['jd_tot_charge_365_amt_level_MIN_jd_tot_365_m_cnt_level']=dict_add['jd_tot_charge_365_amt_level']-dict_add['jd_tot_365_m_cnt_level']
    dict_add['jd_tot_charge_365_amt_level_DIV_jd_tot_365_m_cnt_level']=dict_add['jd_tot_charge_365_amt_level']/dict_add['jd_tot_365_m_cnt_level'] if dict_add['jd_tot_365_m_cnt_level'] > 0 else np.nan
    dict_add['jd_tot_charge_365_amt_level_DIV_jd_riskPeriodConsuming']=dict_add['jd_tot_charge_365_amt_level']/dict_add['jd_riskPeriodConsuming'] if dict_add['jd_riskPeriodConsuming'] > 0 else np.nan
    dict_add['jd_tot_charge_365_amt_level_MIN_jd_tot_90_amt_level']=dict_add['jd_tot_charge_365_amt_level']-dict_add['jd_tot_90_amt_level']
    dict_add['jd_tot_charge_365_amt_level_DIV_jd_risk_pre_score']=dict_add['jd_tot_charge_365_amt_level']/dict_add['jd_risk_pre_score'] if dict_add['jd_risk_pre_score'] > 0 else np.nan
    dict_add['jd_tot_charge_365_amt_level_MUL_jd_stability']=dict_add['jd_tot_charge_365_amt_level']*dict_add['jd_stability']
    dict_add['jd_tot_charge_365_amt_level_MIN_jd_credit_consume_level']=dict_add['jd_tot_charge_365_amt_level']-dict_add['jd_credit_consume_level']
    dict_add['jd_tot_charge_365_amt_level_DIV_jd_credit_consume_level']=dict_add['jd_tot_charge_365_amt_level']/dict_add['jd_credit_consume_level'] if dict_add['jd_credit_consume_level'] > 0 else np.nan
    dict_add['jd_tot_charge_365_amt_level_MIN_jd_tot_30_amt_level']=dict_add['jd_tot_charge_365_amt_level']-dict_add['jd_tot_30_amt_level']
    dict_add['jd_tot_dlq_days_all_180_level_MIN_jd_tob_rank']=dict_add['jd_tot_dlq_days_all_180_level']-dict_add['jd_tob_rank']
    dict_add['jd_tot_dlq_days_all_180_level_DIV_jd_riskIndex']=dict_add['jd_tot_dlq_days_all_180_level']/dict_add['jd_riskIndex'] if dict_add['jd_riskIndex'] > 0 else np.nan
    dict_add['jd_tot_dlq_days_all_180_level_ADD_jd_max_dlq_days_all_60_level']=dict_add['jd_tot_dlq_days_all_180_level']+dict_add['jd_max_dlq_days_all_60_level']
    dict_add['jd_tot_dlq_days_all_180_level_MUL_jd_max_dlq_days_all_60_level']=dict_add['jd_tot_dlq_days_all_180_level']*dict_add['jd_max_dlq_days_all_60_level']
    dict_add['jd_tot_dlq_days_all_180_level_MUL_jd_tot_charge_60_cnt_level']=dict_add['jd_tot_dlq_days_all_180_level']*dict_add['jd_tot_charge_60_cnt_level']
    dict_add['jd_tot_dlq_days_all_180_level_MUL_jd_riskPeriodConsuming']=dict_add['jd_tot_dlq_days_all_180_level']*dict_add['jd_riskPeriodConsuming']
    dict_add['jd_tot_dlq_days_all_180_level_DIV_jd_risk_pre_score']=dict_add['jd_tot_dlq_days_all_180_level']/dict_add['jd_risk_pre_score'] if dict_add['jd_risk_pre_score'] > 0 else np.nan
    dict_add['jd_tot_dlq_days_all_180_level_MIN_jd_tot_30_amt_level']=dict_add['jd_tot_dlq_days_all_180_level']-dict_add['jd_tot_30_amt_level']
    dict_add['jd_buyingIndex_MIN_jd_tob_rank']=dict_add['jd_buyingIndex']-dict_add['jd_tob_rank']
    dict_add['jd_buyingIndex_DIV_jd_tot_180_amt_level']=dict_add['jd_buyingIndex']/dict_add['jd_tot_180_amt_level'] if dict_add['jd_tot_180_amt_level'] > 0 else np.nan
    dict_add['jd_buyingIndex_DIV_jd_car_score']=dict_add['jd_buyingIndex']/dict_add['jd_car_score'] if dict_add['jd_car_score'] > 0 else np.nan
    dict_add['jd_buyingIndex_DIV_jd_debt_bearing_level']=dict_add['jd_buyingIndex']/dict_add['jd_debt_bearing_level'] if dict_add['jd_debt_bearing_level'] > 0 else np.nan
    dict_add['jd_buyingIndex_MIN_jd_income_score']=dict_add['jd_buyingIndex']-dict_add['jd_income_score']
    dict_add['jd_buyingIndex_ADD_jd_riskCategoryConsuming']=dict_add['jd_buyingIndex']+dict_add['jd_riskCategoryConsuming']
    dict_add['jd_buyingIndex_MIN_jd_riskCategoryConsuming']=dict_add['jd_buyingIndex']-dict_add['jd_riskCategoryConsuming']
    dict_add['jd_buyingIndex_ADD_jd_riskPeriodConsuming']=dict_add['jd_buyingIndex']+dict_add['jd_riskPeriodConsuming']
    dict_add['jd_buyingIndex_MUL_jd_risk_pre_score']=dict_add['jd_buyingIndex']*dict_add['jd_risk_pre_score']
    dict_add['jd_buyingIndex_DIV_jd_tot_30_amt_level']=dict_add['jd_buyingIndex']/dict_add['jd_tot_30_amt_level'] if dict_add['jd_tot_30_amt_level'] > 0 else np.nan
    dict_add['jd_resonableConsuming_MIN_jd_performance_score']=dict_add['jd_resonableConsuming']-dict_add['jd_performance_score']
    dict_add['jd_resonableConsuming_DIV_jd_tot_365_cnt_level']=dict_add['jd_resonableConsuming']/dict_add['jd_tot_365_cnt_level'] if dict_add['jd_tot_365_cnt_level'] > 0 else np.nan
    dict_add['jd_resonableConsuming_ADD_jd_riskIndex']=dict_add['jd_resonableConsuming']+dict_add['jd_riskIndex']
    dict_add['jd_resonableConsuming_MIN_jd_tot_60_cnt_level']=dict_add['jd_resonableConsuming']-dict_add['jd_tot_60_cnt_level']
    dict_add['jd_resonableConsuming_MUL_jd_ecommercecellphoneStability']=dict_add['jd_resonableConsuming']*dict_add['jd_ecommercecellphoneStability']
    dict_add['jd_resonableConsuming_MUL_jd_cd_level']=dict_add['jd_resonableConsuming']*dict_add['jd_cd_level']
    dict_add['jd_resonableConsuming_MUL_jd_max_dlq_days_all_60_level']=dict_add['jd_resonableConsuming']*dict_add['jd_max_dlq_days_all_60_level']
    dict_add['jd_resonableConsuming_MIN_jd_car_score']=dict_add['jd_resonableConsuming']-dict_add['jd_car_score']
    dict_add['jd_resonableConsuming_DIV_jd_car_score']=dict_add['jd_resonableConsuming']/dict_add['jd_car_score'] if dict_add['jd_car_score'] > 0 else np.nan
    dict_add['jd_resonableConsuming_ADD_jd_have_child']=dict_add['jd_resonableConsuming']+dict_add['jd_have_child']
    dict_add['jd_resonableConsuming_MUL_jd_debt_bearing_level']=dict_add['jd_resonableConsuming']*dict_add['jd_debt_bearing_level']
    dict_add['jd_resonableConsuming_DIV_jd_debt_bearing_level']=dict_add['jd_resonableConsuming']/dict_add['jd_debt_bearing_level'] if dict_add['jd_debt_bearing_level'] > 0 else np.nan
    dict_add['jd_resonableConsuming_MUL_jd_tot_charge_60_cnt_level']=dict_add['jd_resonableConsuming']*dict_add['jd_tot_charge_60_cnt_level']
    dict_add['jd_resonableConsuming_MUL_jd_gd_level']=dict_add['jd_resonableConsuming']*dict_add['jd_gd_level']
    dict_add['jd_resonableConsuming_MIN_jd_income_score']=dict_add['jd_resonableConsuming']-dict_add['jd_income_score']
    dict_add['jd_resonableConsuming_MUL_jd_tot_365_amt_level']=dict_add['jd_resonableConsuming']*dict_add['jd_tot_365_amt_level']
    dict_add['jd_resonableConsuming_DIV_jd_tot_charge_180_amt_level']=dict_add['jd_resonableConsuming']/dict_add['jd_tot_charge_180_amt_level'] if dict_add['jd_tot_charge_180_amt_level'] > 0 else np.nan
    dict_add['jd_resonableConsuming_ADD_jd_riskCategoryConsuming']=dict_add['jd_resonableConsuming']+dict_add['jd_riskCategoryConsuming']
    dict_add['jd_resonableConsuming_MIN_jd_riskCategoryConsuming']=dict_add['jd_resonableConsuming']-dict_add['jd_riskCategoryConsuming']
    dict_add['jd_resonableConsuming_MUL_jd_riskCategoryConsuming']=dict_add['jd_resonableConsuming']*dict_add['jd_riskCategoryConsuming']
    dict_add['jd_resonableConsuming_DIV_jd_riskCategoryConsuming']=dict_add['jd_resonableConsuming']/dict_add['jd_riskCategoryConsuming'] if dict_add['jd_riskCategoryConsuming'] > 0 else np.nan
    dict_add['jd_resonableConsuming_ADD_jd_risk_pre_score']=dict_add['jd_resonableConsuming']+dict_add['jd_risk_pre_score']
    dict_add['jd_resonableConsuming_MIN_jd_risk_pre_score']=dict_add['jd_resonableConsuming']-dict_add['jd_risk_pre_score']
    dict_add['jd_resonableConsuming_DIV_jd_tot_30_amt_level']=dict_add['jd_resonableConsuming']/dict_add['jd_tot_30_amt_level'] if dict_add['jd_tot_30_amt_level'] > 0 else np.nan
    dict_add['jd_asset_score_DIV_jd_tot_90_cnt_level']=dict_add['jd_asset_score']/dict_add['jd_tot_90_cnt_level'] if dict_add['jd_tot_90_cnt_level'] > 0 else np.nan
    dict_add['jd_asset_score_ADD_jd_consume_cnt_freq']=dict_add['jd_asset_score']+dict_add['jd_consume_cnt_freq']
    dict_add['jd_asset_score_MIN_jd_house_score']=dict_add['jd_asset_score']-dict_add['jd_house_score']
    dict_add['jd_asset_score_DIV_jd_tot_30_cnt_level']=dict_add['jd_asset_score']/dict_add['jd_tot_30_cnt_level'] if dict_add['jd_tot_30_cnt_level'] > 0 else np.nan
    dict_add['jd_asset_score_ADD_jd_performance_score']=dict_add['jd_asset_score']+dict_add['jd_performance_score']
    dict_add['jd_asset_score_MIN_jd_performance_score']=dict_add['jd_asset_score']-dict_add['jd_performance_score']
    dict_add['jd_asset_score_MUL_jd_tob_rank']=dict_add['jd_asset_score']*dict_add['jd_tob_rank']
    dict_add['jd_asset_score_DIV_jd_tot_180_amt_level']=dict_add['jd_asset_score']/dict_add['jd_tot_180_amt_level'] if dict_add['jd_tot_180_amt_level'] > 0 else np.nan
    dict_add['jd_asset_score_MIN_jd_riskIndex']=dict_add['jd_asset_score']-dict_add['jd_riskIndex']
    dict_add['jd_asset_score_DIV_jd_tot_60_amt_level']=dict_add['jd_asset_score']/dict_add['jd_tot_60_amt_level'] if dict_add['jd_tot_60_amt_level'] > 0 else np.nan
    dict_add['jd_asset_score_DIV_jd_tot_60_cnt_level']=dict_add['jd_asset_score']/dict_add['jd_tot_60_cnt_level'] if dict_add['jd_tot_60_cnt_level'] > 0 else np.nan
    dict_add['jd_asset_score_MIN_jd_worktimeShopping']=dict_add['jd_asset_score']-dict_add['jd_worktimeShopping']
    dict_add['jd_asset_score_DIV_jd_worktimeShopping']=dict_add['jd_asset_score']/dict_add['jd_worktimeShopping'] if dict_add['jd_worktimeShopping'] > 0 else np.nan
    dict_add['jd_asset_score_MIN_jd_cd_level']=dict_add['jd_asset_score']-dict_add['jd_cd_level']
    dict_add['jd_asset_score_ADD_jd_time_on_book']=dict_add['jd_asset_score']+dict_add['jd_time_on_book']
    dict_add['jd_asset_score_MUL_jd_time_on_book']=dict_add['jd_asset_score']*dict_add['jd_time_on_book']
    dict_add['jd_asset_score_DIV_jd_time_on_book']=dict_add['jd_asset_score']/dict_add['jd_time_on_book'] if dict_add['jd_time_on_book'] > 0 else np.nan
    dict_add['jd_asset_score_MUL_jd_car_score']=dict_add['jd_asset_score']*dict_add['jd_car_score']
    dict_add['jd_asset_score_DIV_jd_car_score']=dict_add['jd_asset_score']/dict_add['jd_car_score'] if dict_add['jd_car_score'] > 0 else np.nan
    dict_add['jd_asset_score_ADD_jd_tot_charge_365_cnt_level']=dict_add['jd_asset_score']+dict_add['jd_tot_charge_365_cnt_level']
    dict_add['jd_asset_score_MIN_jd_have_child']=dict_add['jd_asset_score']-dict_add['jd_have_child']
    dict_add['jd_asset_score_DIV_jd_gd_level']=dict_add['jd_asset_score']/dict_add['jd_gd_level'] if dict_add['jd_gd_level'] > 0 else np.nan
    dict_add['jd_asset_score_DIV_jd_income_score']=dict_add['jd_asset_score']/dict_add['jd_income_score'] if dict_add['jd_income_score'] > 0 else np.nan
    dict_add['jd_asset_score_ADD_jd_tot_365_amt_level']=dict_add['jd_asset_score']+dict_add['jd_tot_365_amt_level']
    dict_add['jd_asset_score_DIV_jd_tot_365_amt_level']=dict_add['jd_asset_score']/dict_add['jd_tot_365_amt_level'] if dict_add['jd_tot_365_amt_level'] > 0 else np.nan
    dict_add['jd_asset_score_DIV_jd_tot_charge_180_amt_level']=dict_add['jd_asset_score']/dict_add['jd_tot_charge_180_amt_level'] if dict_add['jd_tot_charge_180_amt_level'] > 0 else np.nan
    dict_add['jd_asset_score_ADD_jd_riskCategoryConsuming']=dict_add['jd_asset_score']+dict_add['jd_riskCategoryConsuming']
    dict_add['jd_asset_score_MIN_jd_riskCategoryConsuming']=dict_add['jd_asset_score']-dict_add['jd_riskCategoryConsuming']
    dict_add['jd_asset_score_ADD_jd_tot_365_m_cnt_level']=dict_add['jd_asset_score']+dict_add['jd_tot_365_m_cnt_level']
    dict_add['jd_asset_score_DIV_jd_liability_level']=dict_add['jd_asset_score']/dict_add['jd_liability_level'] if dict_add['jd_liability_level'] > 0 else np.nan
    dict_add['jd_asset_score_DIV_jd_risk_pre_score']=dict_add['jd_asset_score']/dict_add['jd_risk_pre_score'] if dict_add['jd_risk_pre_score'] > 0 else np.nan
    dict_add['jd_asset_score_DIV_jd_credit_consume_level']=dict_add['jd_asset_score']/dict_add['jd_credit_consume_level'] if dict_add['jd_credit_consume_level'] > 0 else np.nan
    dict_add['jd_tot_180_m_cnt_level_MIN_jd_tot_90_cnt_level']=dict_add['jd_tot_180_m_cnt_level']-dict_add['jd_tot_90_cnt_level']
    dict_add['jd_tot_180_m_cnt_level_MIN_jd_consume_cnt_freq']=dict_add['jd_tot_180_m_cnt_level']-dict_add['jd_consume_cnt_freq']
    dict_add['jd_tot_180_m_cnt_level_MUL_jd_performance_score']=dict_add['jd_tot_180_m_cnt_level']*dict_add['jd_performance_score']
    dict_add['jd_tot_180_m_cnt_level_MIN_jd_tot_180_amt_level']=dict_add['jd_tot_180_m_cnt_level']-dict_add['jd_tot_180_amt_level']
    dict_add['jd_tot_180_m_cnt_level_MIN_jd_riskIndex']=dict_add['jd_tot_180_m_cnt_level']-dict_add['jd_riskIndex']
    dict_add['jd_tot_180_m_cnt_level_MIN_jd_tot_charge_180_cnt_level']=dict_add['jd_tot_180_m_cnt_level']-dict_add['jd_tot_charge_180_cnt_level']
    dict_add['jd_tot_180_m_cnt_level_MIN_jd_tot_60_cnt_level']=dict_add['jd_tot_180_m_cnt_level']-dict_add['jd_tot_60_cnt_level']
    dict_add['jd_tot_180_m_cnt_level_MIN_jd_ecommercecellphoneStability']=dict_add['jd_tot_180_m_cnt_level']-dict_add['jd_ecommercecellphoneStability']
    dict_add['jd_tot_180_m_cnt_level_MIN_jd_time_on_book']=dict_add['jd_tot_180_m_cnt_level']-dict_add['jd_time_on_book']
    dict_add['jd_tot_180_m_cnt_level_DIV_jd_car_score']=dict_add['jd_tot_180_m_cnt_level']/dict_add['jd_car_score'] if dict_add['jd_car_score'] > 0 else np.nan
    dict_add['jd_tot_180_m_cnt_level_MIN_jd_tot_charge_365_cnt_level']=dict_add['jd_tot_180_m_cnt_level']-dict_add['jd_tot_charge_365_cnt_level']
    dict_add['jd_tot_180_m_cnt_level_ADD_jd_have_child']=dict_add['jd_tot_180_m_cnt_level']+dict_add['jd_have_child']
    dict_add['jd_tot_180_m_cnt_level_MIN_jd_tot_charge_60_cnt_level']=dict_add['jd_tot_180_m_cnt_level']-dict_add['jd_tot_charge_60_cnt_level']
    dict_add['jd_tot_180_m_cnt_level_MUL_jd_income_score']=dict_add['jd_tot_180_m_cnt_level']*dict_add['jd_income_score']
    dict_add['jd_tot_180_m_cnt_level_MIN_jd_tot_365_amt_level']=dict_add['jd_tot_180_m_cnt_level']-dict_add['jd_tot_365_amt_level']
    dict_add['jd_tot_180_m_cnt_level_MIN_jd_tot_charge_180_amt_level']=dict_add['jd_tot_180_m_cnt_level']-dict_add['jd_tot_charge_180_amt_level']
    dict_add['jd_tot_180_m_cnt_level_DIV_jd_tot_charge_180_amt_level']=dict_add['jd_tot_180_m_cnt_level']/dict_add['jd_tot_charge_180_amt_level'] if dict_add['jd_tot_charge_180_amt_level'] > 0 else np.nan
    dict_add['jd_tot_180_m_cnt_level_MIN_jd_riskCategoryConsuming']=dict_add['jd_tot_180_m_cnt_level']-dict_add['jd_riskCategoryConsuming']
    dict_add['jd_tot_180_m_cnt_level_MIN_jd_riskPeriodConsuming']=dict_add['jd_tot_180_m_cnt_level']-dict_add['jd_riskPeriodConsuming']
    dict_add['jd_tot_180_m_cnt_level_MUL_jd_liability_level']=dict_add['jd_tot_180_m_cnt_level']*dict_add['jd_liability_level']
    dict_add['jd_tot_180_m_cnt_level_DIV_jd_tot_90_amt_level']=dict_add['jd_tot_180_m_cnt_level']/dict_add['jd_tot_90_amt_level'] if dict_add['jd_tot_90_amt_level'] > 0 else np.nan
    dict_add['jd_tot_90_cnt_level_MUL_jd_house_score']=dict_add['jd_tot_90_cnt_level']*dict_add['jd_house_score']
    dict_add['jd_tot_90_cnt_level_DIV_jd_performance_score']=dict_add['jd_tot_90_cnt_level']/dict_add['jd_performance_score'] if dict_add['jd_performance_score'] > 0 else np.nan
    dict_add['jd_tot_90_cnt_level_MIN_jd_tob_rank']=dict_add['jd_tot_90_cnt_level']-dict_add['jd_tob_rank']
    dict_add['jd_tot_90_cnt_level_MIN_jd_tot_180_amt_level']=dict_add['jd_tot_90_cnt_level']-dict_add['jd_tot_180_amt_level']
    dict_add['jd_tot_90_cnt_level_MIN_jd_tot_365_cnt_level']=dict_add['jd_tot_90_cnt_level']-dict_add['jd_tot_365_cnt_level']
    dict_add['jd_tot_90_cnt_level_DIV_jd_riskIndex']=dict_add['jd_tot_90_cnt_level']/dict_add['jd_riskIndex'] if dict_add['jd_riskIndex'] > 0 else np.nan
    dict_add['jd_tot_90_cnt_level_MIN_jd_tot_60_amt_level']=dict_add['jd_tot_90_cnt_level']-dict_add['jd_tot_60_amt_level']
    dict_add['jd_tot_90_cnt_level_MIN_jd_tot_charge_180_cnt_level']=dict_add['jd_tot_90_cnt_level']-dict_add['jd_tot_charge_180_cnt_level']
    dict_add['jd_tot_90_cnt_level_MIN_jd_tot_60_cnt_level']=dict_add['jd_tot_90_cnt_level']-dict_add['jd_tot_60_cnt_level']
    dict_add['jd_tot_90_cnt_level_MIN_jd_ecommercecellphoneStability']=dict_add['jd_tot_90_cnt_level']-dict_add['jd_ecommercecellphoneStability']
    dict_add['jd_tot_90_cnt_level_MIN_jd_tot_charge_365_cnt_level']=dict_add['jd_tot_90_cnt_level']-dict_add['jd_tot_charge_365_cnt_level']
    dict_add['jd_tot_90_cnt_level_MIN_jd_tot_charge_60_cnt_level']=dict_add['jd_tot_90_cnt_level']-dict_add['jd_tot_charge_60_cnt_level']
    dict_add['jd_tot_90_cnt_level_MIN_jd_gd_level']=dict_add['jd_tot_90_cnt_level']-dict_add['jd_gd_level']
    dict_add['jd_tot_90_cnt_level_DIV_jd_tot_365_amt_level']=dict_add['jd_tot_90_cnt_level']/dict_add['jd_tot_365_amt_level'] if dict_add['jd_tot_365_amt_level'] > 0 else np.nan
    dict_add['jd_tot_90_cnt_level_MIN_jd_tot_charge_90_amt_level']=dict_add['jd_tot_90_cnt_level']-dict_add['jd_tot_charge_90_amt_level']
    dict_add['jd_tot_90_cnt_level_MIN_jd_tot_charge_30_cnt_level']=dict_add['jd_tot_90_cnt_level']-dict_add['jd_tot_charge_30_cnt_level']
    dict_add['jd_tot_90_cnt_level_MIN_jd_riskCategoryConsuming']=dict_add['jd_tot_90_cnt_level']-dict_add['jd_riskCategoryConsuming']
    dict_add['jd_tot_90_cnt_level_MIN_jd_tot_365_m_cnt_level']=dict_add['jd_tot_90_cnt_level']-dict_add['jd_tot_365_m_cnt_level']
    dict_add['jd_tot_90_cnt_level_MIN_jd_tot_90_amt_level']=dict_add['jd_tot_90_cnt_level']-dict_add['jd_tot_90_amt_level']
    dict_add['jd_tot_90_cnt_level_DIV_jd_risk_pre_score']=dict_add['jd_tot_90_cnt_level']/dict_add['jd_risk_pre_score'] if dict_add['jd_risk_pre_score'] > 0 else np.nan
    dict_add['jd_tot_90_cnt_level_MIN_jd_credit_consume_level']=dict_add['jd_tot_90_cnt_level']-dict_add['jd_credit_consume_level']
    dict_add['jd_tot_90_cnt_level_MIN_jd_tot_dlq_days_all_365_level']=dict_add['jd_tot_90_cnt_level']-dict_add['jd_tot_dlq_days_all_365_level']
    dict_add['jd_tot_90_cnt_level_DIV_jd_tot_dlq_days_all_365_level']=dict_add['jd_tot_90_cnt_level']/dict_add['jd_tot_dlq_days_all_365_level'] if dict_add['jd_tot_dlq_days_all_365_level'] > 0 else np.nan
    dict_add['jd_max_dlq_days_all_30_level_MIN_jd_mal_ord_freq']=dict_add['jd_max_dlq_days_all_30_level']-dict_add['jd_mal_ord_freq']
    dict_add['jd_max_dlq_days_all_30_level_ADD_jd_tob_rank']=dict_add['jd_max_dlq_days_all_30_level']+dict_add['jd_tob_rank']
    dict_add['jd_max_dlq_days_all_30_level_MIN_jd_tob_rank']=dict_add['jd_max_dlq_days_all_30_level']-dict_add['jd_tob_rank']
    dict_add['jd_max_dlq_days_all_30_level_DIV_jd_tob_rank']=dict_add['jd_max_dlq_days_all_30_level']/dict_add['jd_tob_rank'] if dict_add['jd_tob_rank'] > 0 else np.nan
    dict_add['jd_max_dlq_days_all_30_level_MUL_jd_tot_180_amt_level']=dict_add['jd_max_dlq_days_all_30_level']*dict_add['jd_tot_180_amt_level']
    dict_add['jd_max_dlq_days_all_30_level_MIN_jd_riskIndex']=dict_add['jd_max_dlq_days_all_30_level']-dict_add['jd_riskIndex']
    dict_add['jd_max_dlq_days_all_30_level_MUL_jd_riskIndex']=dict_add['jd_max_dlq_days_all_30_level']*dict_add['jd_riskIndex']
    dict_add['jd_max_dlq_days_all_30_level_MUL_jd_debt_bearing_level']=dict_add['jd_max_dlq_days_all_30_level']*dict_add['jd_debt_bearing_level']
    dict_add['jd_max_dlq_days_all_30_level_MUL_jd_max_dlq_days_all_180_level']=dict_add['jd_max_dlq_days_all_30_level']*dict_add['jd_max_dlq_days_all_180_level']
    dict_add['jd_max_dlq_days_all_30_level_ADD_jd_gd_level']=dict_add['jd_max_dlq_days_all_30_level']+dict_add['jd_gd_level']
    dict_add['jd_max_dlq_days_all_30_level_MUL_jd_income_score']=dict_add['jd_max_dlq_days_all_30_level']*dict_add['jd_income_score']
    dict_add['jd_max_dlq_days_all_30_level_MUL_jd_tot_charge_60_amt_level']=dict_add['jd_max_dlq_days_all_30_level']*dict_add['jd_tot_charge_60_amt_level']
    dict_add['jd_max_dlq_days_all_30_level_MUL_jd_tot_charge_180_amt_level']=dict_add['jd_max_dlq_days_all_30_level']*dict_add['jd_tot_charge_180_amt_level']
    dict_add['jd_max_dlq_days_all_30_level_MUL_jd_tot_charge_90_amt_level']=dict_add['jd_max_dlq_days_all_30_level']*dict_add['jd_tot_charge_90_amt_level']
    dict_add['jd_max_dlq_days_all_30_level_MUL_jd_tot_30_amt_level']=dict_add['jd_max_dlq_days_all_30_level']*dict_add['jd_tot_30_amt_level']
    dict_add['jd_max_dlq_days_all_365_level_MIN_jd_consume_cnt_freq']=dict_add['jd_max_dlq_days_all_365_level']-dict_add['jd_consume_cnt_freq']
    dict_add['jd_max_dlq_days_all_365_level_DIV_jd_consume_cnt_freq']=dict_add['jd_max_dlq_days_all_365_level']/dict_add['jd_consume_cnt_freq'] if dict_add['jd_consume_cnt_freq'] > 0 else np.nan
    dict_add['jd_max_dlq_days_all_365_level_MUL_jd_performance_score']=dict_add['jd_max_dlq_days_all_365_level']*dict_add['jd_performance_score']
    dict_add['jd_max_dlq_days_all_365_level_MIN_jd_tob_rank']=dict_add['jd_max_dlq_days_all_365_level']-dict_add['jd_tob_rank']
    dict_add['jd_max_dlq_days_all_365_level_MUL_jd_tot_180_amt_level']=dict_add['jd_max_dlq_days_all_365_level']*dict_add['jd_tot_180_amt_level']
    dict_add['jd_max_dlq_days_all_365_level_MUL_jd_riskIndex']=dict_add['jd_max_dlq_days_all_365_level']*dict_add['jd_riskIndex']
    dict_add['jd_max_dlq_days_all_365_level_DIV_jd_time_on_book']=dict_add['jd_max_dlq_days_all_365_level']/dict_add['jd_time_on_book'] if dict_add['jd_time_on_book'] > 0 else np.nan
    dict_add['jd_max_dlq_days_all_365_level_MIN_jd_have_child']=dict_add['jd_max_dlq_days_all_365_level']-dict_add['jd_have_child']
    dict_add['jd_max_dlq_days_all_365_level_MUL_jd_income_score']=dict_add['jd_max_dlq_days_all_365_level']*dict_add['jd_income_score']
    dict_add['jd_max_dlq_days_all_365_level_MUL_jd_tot_charge_60_amt_level']=dict_add['jd_max_dlq_days_all_365_level']*dict_add['jd_tot_charge_60_amt_level']
    dict_add['jd_max_dlq_days_all_365_level_MUL_jd_tot_charge_180_amt_level']=dict_add['jd_max_dlq_days_all_365_level']*dict_add['jd_tot_charge_180_amt_level']
    dict_add['jd_max_dlq_days_all_365_level_MIN_jd_tot_365_m_cnt_level']=dict_add['jd_max_dlq_days_all_365_level']-dict_add['jd_tot_365_m_cnt_level']
    dict_add['jd_max_dlq_days_all_365_level_MUL_jd_tot_30_amt_level']=dict_add['jd_max_dlq_days_all_365_level']*dict_add['jd_tot_30_amt_level']
    dict_add['jd_max_dlq_days_all_365_level_MUL_jd_tot_dlq_days_all_365_level']=dict_add['jd_max_dlq_days_all_365_level']*dict_add['jd_tot_dlq_days_all_365_level']
    dict_add['jd_consume_cnt_freq_DIV_jd_performance_score']=dict_add['jd_consume_cnt_freq']/dict_add['jd_performance_score'] if dict_add['jd_performance_score'] > 0 else np.nan
    dict_add['jd_consume_cnt_freq_MIN_jd_tot_180_amt_level']=dict_add['jd_consume_cnt_freq']-dict_add['jd_tot_180_amt_level']
    dict_add['jd_consume_cnt_freq_MIN_jd_tot_365_cnt_level']=dict_add['jd_consume_cnt_freq']-dict_add['jd_tot_365_cnt_level']
    dict_add['jd_consume_cnt_freq_ADD_jd_riskIndex']=dict_add['jd_consume_cnt_freq']+dict_add['jd_riskIndex']
    dict_add['jd_consume_cnt_freq_MIN_jd_tot_60_amt_level']=dict_add['jd_consume_cnt_freq']-dict_add['jd_tot_60_amt_level']
    dict_add['jd_consume_cnt_freq_MIN_jd_tot_charge_180_cnt_level']=dict_add['jd_consume_cnt_freq']-dict_add['jd_tot_charge_180_cnt_level']
    dict_add['jd_consume_cnt_freq_MIN_jd_tot_60_cnt_level']=dict_add['jd_consume_cnt_freq']-dict_add['jd_tot_60_cnt_level']
    dict_add['jd_consume_cnt_freq_MIN_jd_ecommercecellphoneStability']=dict_add['jd_consume_cnt_freq']-dict_add['jd_ecommercecellphoneStability']
    dict_add['jd_consume_cnt_freq_MUL_jd_car_score']=dict_add['jd_consume_cnt_freq']*dict_add['jd_car_score']
    dict_add['jd_consume_cnt_freq_DIV_jd_car_score']=dict_add['jd_consume_cnt_freq']/dict_add['jd_car_score'] if dict_add['jd_car_score'] > 0 else np.nan
    dict_add['jd_consume_cnt_freq_MIN_jd_tot_charge_365_cnt_level']=dict_add['jd_consume_cnt_freq']-dict_add['jd_tot_charge_365_cnt_level']
    dict_add['jd_consume_cnt_freq_ADD_jd_have_child']=dict_add['jd_consume_cnt_freq']+dict_add['jd_have_child']
    dict_add['jd_consume_cnt_freq_MIN_jd_gd_level']=dict_add['jd_consume_cnt_freq']-dict_add['jd_gd_level']
    dict_add['jd_consume_cnt_freq_DIV_jd_riskPeriodConsuming']=dict_add['jd_consume_cnt_freq']/dict_add['jd_riskPeriodConsuming'] if dict_add['jd_riskPeriodConsuming'] > 0 else np.nan
    dict_add['jd_consume_cnt_freq_DIV_jd_risk_pre_score']=dict_add['jd_consume_cnt_freq']/dict_add['jd_risk_pre_score'] if dict_add['jd_risk_pre_score'] > 0 else np.nan
    dict_add['jd_consume_cnt_freq_MIN_jd_credit_consume_level']=dict_add['jd_consume_cnt_freq']-dict_add['jd_credit_consume_level']
    dict_add['jd_consume_cnt_freq_MIN_jd_tot_30_amt_level']=dict_add['jd_consume_cnt_freq']-dict_add['jd_tot_30_amt_level']
    dict_add['jd_house_score_ADD_jd_performance_score']=dict_add['jd_house_score']+dict_add['jd_performance_score']
    dict_add['jd_house_score_MIN_jd_performance_score']=dict_add['jd_house_score']-dict_add['jd_performance_score']
    dict_add['jd_house_score_MUL_jd_performance_score']=dict_add['jd_house_score']*dict_add['jd_performance_score']
    dict_add['jd_house_score_ADD_jd_have_child']=dict_add['jd_house_score']+dict_add['jd_have_child']
    dict_add['jd_house_score_DIV_jd_debt_bearing_level']=dict_add['jd_house_score']/dict_add['jd_debt_bearing_level'] if dict_add['jd_debt_bearing_level'] > 0 else np.nan
    dict_add['jd_house_score_DIV_jd_max_dlq_days_all_180_level']=dict_add['jd_house_score']/dict_add['jd_max_dlq_days_all_180_level'] if dict_add['jd_max_dlq_days_all_180_level'] > 0 else np.nan
    dict_add['jd_house_score_MIN_jd_tot_365_amt_level']=dict_add['jd_house_score']-dict_add['jd_tot_365_amt_level']
    dict_add['jd_house_score_ADD_jd_risk_pre_score']=dict_add['jd_house_score']+dict_add['jd_risk_pre_score']
    dict_add['jd_house_score_MUL_jd_risk_pre_score']=dict_add['jd_house_score']*dict_add['jd_risk_pre_score']
    dict_add['jd_house_score_MUL_jd_stability']=dict_add['jd_house_score']*dict_add['jd_stability']
    dict_add['jd_house_score_DIV_jd_tot_30_amt_level']=dict_add['jd_house_score']/dict_add['jd_tot_30_amt_level'] if dict_add['jd_tot_30_amt_level'] > 0 else np.nan
    dict_add['jd_mal_ord_freq_MIN_jd_tob_rank']=dict_add['jd_mal_ord_freq']-dict_add['jd_tob_rank']
    dict_add['jd_mal_ord_freq_MIN_jd_have_child']=dict_add['jd_mal_ord_freq']-dict_add['jd_have_child']
    dict_add['jd_mal_ord_freq_DIV_jd_have_child']=dict_add['jd_mal_ord_freq']/dict_add['jd_have_child'] if dict_add['jd_have_child'] > 0 else np.nan
    dict_add['jd_mal_ord_freq_MIN_jd_tot_charge_90_amt_level']=dict_add['jd_mal_ord_freq']-dict_add['jd_tot_charge_90_amt_level']
    dict_add['jd_mal_ord_freq_MIN_jd_tot_charge_30_cnt_level']=dict_add['jd_mal_ord_freq']-dict_add['jd_tot_charge_30_cnt_level']
    dict_add['jd_max_dlq_days_all_90_level_MIN_jd_tob_rank']=dict_add['jd_max_dlq_days_all_90_level']-dict_add['jd_tob_rank']
    dict_add['jd_max_dlq_days_all_90_level_DIV_jd_car_score']=dict_add['jd_max_dlq_days_all_90_level']/dict_add['jd_car_score'] if dict_add['jd_car_score'] > 0 else np.nan
    dict_add['jd_max_dlq_days_all_90_level_MUL_jd_tot_charge_60_amt_level']=dict_add['jd_max_dlq_days_all_90_level']*dict_add['jd_tot_charge_60_amt_level']
    dict_add['jd_max_dlq_days_all_90_level_MUL_jd_tot_charge_90_amt_level']=dict_add['jd_max_dlq_days_all_90_level']*dict_add['jd_tot_charge_90_amt_level']
    dict_add['jd_max_dlq_days_all_90_level_MIN_jd_tot_365_m_cnt_level']=dict_add['jd_max_dlq_days_all_90_level']-dict_add['jd_tot_365_m_cnt_level']
    dict_add['jd_max_dlq_days_all_90_level_DIV_jd_risk_pre_score']=dict_add['jd_max_dlq_days_all_90_level']/dict_add['jd_risk_pre_score'] if dict_add['jd_risk_pre_score'] > 0 else np.nan
    dict_add['jd_max_dlq_days_all_90_level_MUL_jd_tot_30_amt_level']=dict_add['jd_max_dlq_days_all_90_level']*dict_add['jd_tot_30_amt_level']
    dict_add['jd_cellphonePreference_MIN_jd_performance_score']=dict_add['jd_cellphonePreference']-dict_add['jd_performance_score']
    dict_add['jd_cellphonePreference_MUL_jd_performance_score']=dict_add['jd_cellphonePreference']*dict_add['jd_performance_score']
    dict_add['jd_cellphonePreference_DIV_jd_performance_score']=dict_add['jd_cellphonePreference']/dict_add['jd_performance_score'] if dict_add['jd_performance_score'] > 0 else np.nan
    dict_add['jd_cellphonePreference_MIN_jd_tob_rank']=dict_add['jd_cellphonePreference']-dict_add['jd_tob_rank']
    dict_add['jd_cellphonePreference_MIN_jd_tot_180_amt_level']=dict_add['jd_cellphonePreference']-dict_add['jd_tot_180_amt_level']
    dict_add['jd_cellphonePreference_MIN_jd_tot_365_cnt_level']=dict_add['jd_cellphonePreference']-dict_add['jd_tot_365_cnt_level']
    dict_add['jd_cellphonePreference_MIN_jd_tot_60_amt_level']=dict_add['jd_cellphonePreference']-dict_add['jd_tot_60_amt_level']
    dict_add['jd_cellphonePreference_MIN_jd_tot_charge_180_cnt_level']=dict_add['jd_cellphonePreference']-dict_add['jd_tot_charge_180_cnt_level']
    dict_add['jd_cellphonePreference_MIN_jd_tot_60_cnt_level']=dict_add['jd_cellphonePreference']-dict_add['jd_tot_60_cnt_level']
    dict_add['jd_cellphonePreference_MIN_jd_cd_level']=dict_add['jd_cellphonePreference']-dict_add['jd_cd_level']
    dict_add['jd_cellphonePreference_MIN_jd_time_on_book']=dict_add['jd_cellphonePreference']-dict_add['jd_time_on_book']
    dict_add['jd_cellphonePreference_MIN_jd_car_score']=dict_add['jd_cellphonePreference']-dict_add['jd_car_score']
    dict_add['jd_cellphonePreference_DIV_jd_car_score']=dict_add['jd_cellphonePreference']/dict_add['jd_car_score'] if dict_add['jd_car_score'] > 0 else np.nan
    dict_add['jd_cellphonePreference_MIN_jd_tot_charge_365_cnt_level']=dict_add['jd_cellphonePreference']-dict_add['jd_tot_charge_365_cnt_level']
    dict_add['jd_cellphonePreference_MIN_jd_debt_bearing_level']=dict_add['jd_cellphonePreference']-dict_add['jd_debt_bearing_level']
    dict_add['jd_cellphonePreference_ADD_jd_tot_charge_60_cnt_level']=dict_add['jd_cellphonePreference']+dict_add['jd_tot_charge_60_cnt_level']
    dict_add['jd_cellphonePreference_MUL_jd_income_score']=dict_add['jd_cellphonePreference']*dict_add['jd_income_score']
    dict_add['jd_cellphonePreference_MIN_jd_tot_charge_60_amt_level']=dict_add['jd_cellphonePreference']-dict_add['jd_tot_charge_60_amt_level']
    dict_add['jd_cellphonePreference_MIN_jd_tot_charge_180_amt_level']=dict_add['jd_cellphonePreference']-dict_add['jd_tot_charge_180_amt_level']
    dict_add['jd_cellphonePreference_MUL_jd_riskPeriodConsuming']=dict_add['jd_cellphonePreference']*dict_add['jd_riskPeriodConsuming']
    dict_add['jd_cellphonePreference_MIN_jd_tot_90_amt_level']=dict_add['jd_cellphonePreference']-dict_add['jd_tot_90_amt_level']
    dict_add['jd_cellphonePreference_ADD_jd_risk_pre_score']=dict_add['jd_cellphonePreference']+dict_add['jd_risk_pre_score']
    dict_add['jd_cellphonePreference_DIV_jd_risk_pre_score']=dict_add['jd_cellphonePreference']/dict_add['jd_risk_pre_score'] if dict_add['jd_risk_pre_score'] > 0 else np.nan
    dict_add['jd_cellphonePreference_MIN_jd_stability']=dict_add['jd_cellphonePreference']-dict_add['jd_stability']
    dict_add['jd_cellphonePreference_MUL_jd_stability']=dict_add['jd_cellphonePreference']*dict_add['jd_stability']
    dict_add['jd_cellphonePreference_MIN_jd_credit_consume_level']=dict_add['jd_cellphonePreference']-dict_add['jd_credit_consume_level']
    dict_add['jd_cellphonePreference_MIN_jd_tot_30_amt_level']=dict_add['jd_cellphonePreference']-dict_add['jd_tot_30_amt_level']
    dict_add['jd_tot_30_cnt_level_MUL_jd_performance_score']=dict_add['jd_tot_30_cnt_level']*dict_add['jd_performance_score']
    dict_add['jd_tot_30_cnt_level_DIV_jd_performance_score']=dict_add['jd_tot_30_cnt_level']/dict_add['jd_performance_score'] if dict_add['jd_performance_score'] > 0 else np.nan
    dict_add['jd_tot_30_cnt_level_MIN_jd_tot_180_amt_level']=dict_add['jd_tot_30_cnt_level']-dict_add['jd_tot_180_amt_level']
    dict_add['jd_tot_30_cnt_level_MIN_jd_tot_365_cnt_level']=dict_add['jd_tot_30_cnt_level']-dict_add['jd_tot_365_cnt_level']
    dict_add['jd_tot_30_cnt_level_ADD_jd_riskIndex']=dict_add['jd_tot_30_cnt_level']+dict_add['jd_riskIndex']
    dict_add['jd_tot_30_cnt_level_MIN_jd_tot_60_cnt_level']=dict_add['jd_tot_30_cnt_level']-dict_add['jd_tot_60_cnt_level']
    dict_add['jd_tot_30_cnt_level_DIV_jd_debt_bearing_level']=dict_add['jd_tot_30_cnt_level']/dict_add['jd_debt_bearing_level'] if dict_add['jd_debt_bearing_level'] > 0 else np.nan
    dict_add['jd_tot_30_cnt_level_DIV_jd_tot_charge_60_cnt_level']=dict_add['jd_tot_30_cnt_level']/dict_add['jd_tot_charge_60_cnt_level'] if dict_add['jd_tot_charge_60_cnt_level'] > 0 else np.nan
    dict_add['jd_tot_30_cnt_level_MIN_jd_gd_level']=dict_add['jd_tot_30_cnt_level']-dict_add['jd_gd_level']
    dict_add['jd_tot_30_cnt_level_MIN_jd_tot_charge_60_amt_level']=dict_add['jd_tot_30_cnt_level']-dict_add['jd_tot_charge_60_amt_level']
    dict_add['jd_tot_30_cnt_level_MIN_jd_tot_365_amt_level']=dict_add['jd_tot_30_cnt_level']-dict_add['jd_tot_365_amt_level']
    dict_add['jd_tot_30_cnt_level_MIN_jd_tot_charge_180_amt_level']=dict_add['jd_tot_30_cnt_level']-dict_add['jd_tot_charge_180_amt_level']
    dict_add['jd_tot_30_cnt_level_DIV_jd_tot_charge_180_amt_level']=dict_add['jd_tot_30_cnt_level']/dict_add['jd_tot_charge_180_amt_level'] if dict_add['jd_tot_charge_180_amt_level'] > 0 else np.nan
    dict_add['jd_tot_30_cnt_level_MIN_jd_riskCategoryConsuming']=dict_add['jd_tot_30_cnt_level']-dict_add['jd_riskCategoryConsuming']
    dict_add['jd_tot_30_cnt_level_DIV_jd_riskCategoryConsuming']=dict_add['jd_tot_30_cnt_level']/dict_add['jd_riskCategoryConsuming'] if dict_add['jd_riskCategoryConsuming'] > 0 else np.nan
    dict_add['jd_tot_30_cnt_level_MIN_jd_riskPeriodConsuming']=dict_add['jd_tot_30_cnt_level']-dict_add['jd_riskPeriodConsuming']
    dict_add['jd_tot_30_cnt_level_MIN_jd_tot_90_amt_level']=dict_add['jd_tot_30_cnt_level']-dict_add['jd_tot_90_amt_level']
    dict_add['jd_tot_30_cnt_level_MIN_jd_stability']=dict_add['jd_tot_30_cnt_level']-dict_add['jd_stability']
    dict_add['jd_tot_30_cnt_level_MIN_jd_credit_consume_level']=dict_add['jd_tot_30_cnt_level']-dict_add['jd_credit_consume_level']
    dict_add['jd_performance_score_MUL_jd_tob_rank']=dict_add['jd_performance_score']*dict_add['jd_tob_rank']
    dict_add['jd_performance_score_DIV_jd_tob_rank']=dict_add['jd_performance_score']/dict_add['jd_tob_rank'] if dict_add['jd_tob_rank'] > 0 else np.nan
    dict_add['jd_performance_score_MUL_jd_tot_365_cnt_level']=dict_add['jd_performance_score']*dict_add['jd_tot_365_cnt_level']
    dict_add['jd_performance_score_DIV_jd_tot_365_cnt_level']=dict_add['jd_performance_score']/dict_add['jd_tot_365_cnt_level'] if dict_add['jd_tot_365_cnt_level'] > 0 else np.nan
    dict_add['jd_performance_score_ADD_jd_riskIndex']=dict_add['jd_performance_score']+dict_add['jd_riskIndex']
    dict_add['jd_performance_score_MUL_jd_riskIndex']=dict_add['jd_performance_score']*dict_add['jd_riskIndex']
    dict_add['jd_performance_score_MIN_jd_tot_charge_180_cnt_level']=dict_add['jd_performance_score']-dict_add['jd_tot_charge_180_cnt_level']
    dict_add['jd_performance_score_ADD_jd_tot_60_cnt_level']=dict_add['jd_performance_score']+dict_add['jd_tot_60_cnt_level']
    dict_add['jd_performance_score_DIV_jd_tot_60_cnt_level']=dict_add['jd_performance_score']/dict_add['jd_tot_60_cnt_level'] if dict_add['jd_tot_60_cnt_level'] > 0 else np.nan
    dict_add['jd_performance_score_MUL_jd_ecommercecellphoneStability']=dict_add['jd_performance_score']*dict_add['jd_ecommercecellphoneStability']
    dict_add['jd_performance_score_ADD_jd_worktimeShopping']=dict_add['jd_performance_score']+dict_add['jd_worktimeShopping']
    dict_add['jd_performance_score_MIN_jd_worktimeShopping']=dict_add['jd_performance_score']-dict_add['jd_worktimeShopping']
    dict_add['jd_performance_score_MUL_jd_time_on_book']=dict_add['jd_performance_score']*dict_add['jd_time_on_book']
    dict_add['jd_performance_score_ADD_jd_car_score']=dict_add['jd_performance_score']+dict_add['jd_car_score']
    dict_add['jd_performance_score_MIN_jd_car_score']=dict_add['jd_performance_score']-dict_add['jd_car_score']
    dict_add['jd_performance_score_DIV_jd_car_score']=dict_add['jd_performance_score']/dict_add['jd_car_score'] if dict_add['jd_car_score'] > 0 else np.nan
    dict_add['jd_performance_score_ADD_jd_have_child']=dict_add['jd_performance_score']+dict_add['jd_have_child']
    dict_add['jd_performance_score_MIN_jd_have_child']=dict_add['jd_performance_score']-dict_add['jd_have_child']
    dict_add['jd_performance_score_MUL_jd_have_child']=dict_add['jd_performance_score']*dict_add['jd_have_child']
    dict_add['jd_performance_score_DIV_jd_have_child']=dict_add['jd_performance_score']/dict_add['jd_have_child'] if dict_add['jd_have_child'] > 0 else np.nan
    dict_add['jd_performance_score_MUL_jd_debt_bearing_level']=dict_add['jd_performance_score']*dict_add['jd_debt_bearing_level']
    dict_add['jd_performance_score_DIV_jd_debt_bearing_level']=dict_add['jd_performance_score']/dict_add['jd_debt_bearing_level'] if dict_add['jd_debt_bearing_level'] > 0 else np.nan
    dict_add['jd_performance_score_DIV_jd_tot_charge_60_cnt_level']=dict_add['jd_performance_score']/dict_add['jd_tot_charge_60_cnt_level'] if dict_add['jd_tot_charge_60_cnt_level'] > 0 else np.nan
    dict_add['jd_performance_score_MUL_jd_gd_level']=dict_add['jd_performance_score']*dict_add['jd_gd_level']
    dict_add['jd_performance_score_ADD_jd_income_score']=dict_add['jd_performance_score']+dict_add['jd_income_score']
    dict_add['jd_performance_score_MIN_jd_income_score']=dict_add['jd_performance_score']-dict_add['jd_income_score']
    dict_add['jd_performance_score_MUL_jd_income_score']=dict_add['jd_performance_score']*dict_add['jd_income_score']
    dict_add['jd_performance_score_DIV_jd_tot_charge_60_amt_level']=dict_add['jd_performance_score']/dict_add['jd_tot_charge_60_amt_level'] if dict_add['jd_tot_charge_60_amt_level'] > 0 else np.nan
    dict_add['jd_performance_score_DIV_jd_tot_charge_90_amt_level']=dict_add['jd_performance_score']/dict_add['jd_tot_charge_90_amt_level'] if dict_add['jd_tot_charge_90_amt_level'] > 0 else np.nan
    dict_add['jd_performance_score_MUL_jd_tot_charge_30_cnt_level']=dict_add['jd_performance_score']*dict_add['jd_tot_charge_30_cnt_level']
    dict_add['jd_performance_score_DIV_jd_tot_charge_30_cnt_level']=dict_add['jd_performance_score']/dict_add['jd_tot_charge_30_cnt_level'] if dict_add['jd_tot_charge_30_cnt_level'] > 0 else np.nan
    dict_add['jd_performance_score_MIN_jd_riskCategoryConsuming']=dict_add['jd_performance_score']-dict_add['jd_riskCategoryConsuming']
    dict_add['jd_performance_score_MUL_jd_riskCategoryConsuming']=dict_add['jd_performance_score']*dict_add['jd_riskCategoryConsuming']
    dict_add['jd_performance_score_MUL_jd_tot_365_m_cnt_level']=dict_add['jd_performance_score']*dict_add['jd_tot_365_m_cnt_level']
    dict_add['jd_performance_score_DIV_jd_tot_365_m_cnt_level']=dict_add['jd_performance_score']/dict_add['jd_tot_365_m_cnt_level'] if dict_add['jd_tot_365_m_cnt_level'] > 0 else np.nan
    dict_add['jd_performance_score_DIV_jd_riskPeriodConsuming']=dict_add['jd_performance_score']/dict_add['jd_riskPeriodConsuming'] if dict_add['jd_riskPeriodConsuming'] > 0 else np.nan
    dict_add['jd_performance_score_DIV_jd_tot_90_amt_level']=dict_add['jd_performance_score']/dict_add['jd_tot_90_amt_level'] if dict_add['jd_tot_90_amt_level'] > 0 else np.nan
    dict_add['jd_performance_score_ADD_jd_risk_pre_score']=dict_add['jd_performance_score']+dict_add['jd_risk_pre_score']
    dict_add['jd_performance_score_MIN_jd_risk_pre_score']=dict_add['jd_performance_score']-dict_add['jd_risk_pre_score']
    dict_add['jd_performance_score_DIV_jd_risk_pre_score']=dict_add['jd_performance_score']/dict_add['jd_risk_pre_score'] if dict_add['jd_risk_pre_score'] > 0 else np.nan
    dict_add['jd_performance_score_ADD_jd_stability']=dict_add['jd_performance_score']+dict_add['jd_stability']
    dict_add['jd_performance_score_MUL_jd_stability']=dict_add['jd_performance_score']*dict_add['jd_stability']
    dict_add['jd_performance_score_DIV_jd_stability']=dict_add['jd_performance_score']/dict_add['jd_stability'] if dict_add['jd_stability'] > 0 else np.nan
    dict_add['jd_performance_score_DIV_jd_credit_consume_level']=dict_add['jd_performance_score']/dict_add['jd_credit_consume_level'] if dict_add['jd_credit_consume_level'] > 0 else np.nan
    dict_add['jd_performance_score_MUL_jd_tot_30_amt_level']=dict_add['jd_performance_score']*dict_add['jd_tot_30_amt_level']
    dict_add['jd_tob_rank_MIN_jd_tot_180_amt_level']=dict_add['jd_tob_rank']-dict_add['jd_tot_180_amt_level']
    dict_add['jd_tob_rank_DIV_jd_tot_180_amt_level']=dict_add['jd_tob_rank']/dict_add['jd_tot_180_amt_level'] if dict_add['jd_tot_180_amt_level'] > 0 else np.nan
    dict_add['jd_tob_rank_MIN_jd_tot_365_cnt_level']=dict_add['jd_tob_rank']-dict_add['jd_tot_365_cnt_level']
    dict_add['jd_tob_rank_ADD_jd_riskIndex']=dict_add['jd_tob_rank']+dict_add['jd_riskIndex']
    dict_add['jd_tob_rank_MIN_jd_riskIndex']=dict_add['jd_tob_rank']-dict_add['jd_riskIndex']
    dict_add['jd_tob_rank_MIN_jd_tot_60_amt_level']=dict_add['jd_tob_rank']-dict_add['jd_tot_60_amt_level']
    dict_add['jd_tob_rank_MIN_jd_tot_charge_180_cnt_level']=dict_add['jd_tob_rank']-dict_add['jd_tot_charge_180_cnt_level']
    dict_add['jd_tob_rank_MIN_jd_ecommercecellphoneStability']=dict_add['jd_tob_rank']-dict_add['jd_ecommercecellphoneStability']
    dict_add['jd_tob_rank_MIN_jd_cd_level']=dict_add['jd_tob_rank']-dict_add['jd_cd_level']
    dict_add['jd_tob_rank_DIV_jd_cd_level']=dict_add['jd_tob_rank']/dict_add['jd_cd_level'] if dict_add['jd_cd_level'] > 0 else np.nan
    dict_add['jd_tob_rank_MIN_jd_max_dlq_days_all_60_level']=dict_add['jd_tob_rank']-dict_add['jd_max_dlq_days_all_60_level']
    dict_add['jd_tob_rank_MIN_jd_time_on_book']=dict_add['jd_tob_rank']-dict_add['jd_time_on_book']
    dict_add['jd_tob_rank_MIN_jd_car_score']=dict_add['jd_tob_rank']-dict_add['jd_car_score']
    dict_add['jd_tob_rank_DIV_jd_car_score']=dict_add['jd_tob_rank']/dict_add['jd_car_score'] if dict_add['jd_car_score'] > 0 else np.nan
    dict_add['jd_tob_rank_ADD_jd_tot_charge_365_cnt_level']=dict_add['jd_tob_rank']+dict_add['jd_tot_charge_365_cnt_level']
    dict_add['jd_tob_rank_MIN_jd_tot_charge_365_cnt_level']=dict_add['jd_tob_rank']-dict_add['jd_tot_charge_365_cnt_level']
    dict_add['jd_tob_rank_MUL_jd_have_child']=dict_add['jd_tob_rank']*dict_add['jd_have_child']
    dict_add['jd_tob_rank_MIN_jd_debt_bearing_level']=dict_add['jd_tob_rank']-dict_add['jd_debt_bearing_level']
    dict_add['jd_tob_rank_MUL_jd_debt_bearing_level']=dict_add['jd_tob_rank']*dict_add['jd_debt_bearing_level']
    dict_add['jd_tob_rank_MIN_jd_max_dlq_days_all_180_level']=dict_add['jd_tob_rank']-dict_add['jd_max_dlq_days_all_180_level']
    dict_add['jd_tob_rank_MIN_jd_tot_charge_60_cnt_level']=dict_add['jd_tob_rank']-dict_add['jd_tot_charge_60_cnt_level']
    dict_add['jd_tob_rank_MUL_jd_gd_level']=dict_add['jd_tob_rank']*dict_add['jd_gd_level']
    dict_add['jd_tob_rank_MIN_jd_tot_charge_60_amt_level']=dict_add['jd_tob_rank']-dict_add['jd_tot_charge_60_amt_level']
    dict_add['jd_tob_rank_MIN_jd_tot_365_amt_level']=dict_add['jd_tob_rank']-dict_add['jd_tot_365_amt_level']
    dict_add['jd_tob_rank_MIN_jd_tot_charge_180_amt_level']=dict_add['jd_tob_rank']-dict_add['jd_tot_charge_180_amt_level']
    dict_add['jd_tob_rank_MIN_jd_tot_charge_90_amt_level']=dict_add['jd_tob_rank']-dict_add['jd_tot_charge_90_amt_level']
    dict_add['jd_tob_rank_MIN_jd_tot_charge_30_cnt_level']=dict_add['jd_tob_rank']-dict_add['jd_tot_charge_30_cnt_level']
    dict_add['jd_tob_rank_MUL_jd_tot_charge_30_cnt_level']=dict_add['jd_tob_rank']*dict_add['jd_tot_charge_30_cnt_level']
    dict_add['jd_tob_rank_MIN_jd_riskCategoryConsuming']=dict_add['jd_tob_rank']-dict_add['jd_riskCategoryConsuming']
    dict_add['jd_tob_rank_DIV_jd_riskCategoryConsuming']=dict_add['jd_tob_rank']/dict_add['jd_riskCategoryConsuming'] if dict_add['jd_riskCategoryConsuming'] > 0 else np.nan
    dict_add['jd_tob_rank_MIN_jd_tot_365_m_cnt_level']=dict_add['jd_tob_rank']-dict_add['jd_tot_365_m_cnt_level']
    dict_add['jd_tob_rank_MIN_jd_riskPeriodConsuming']=dict_add['jd_tob_rank']-dict_add['jd_riskPeriodConsuming']
    dict_add['jd_tob_rank_MUL_jd_riskPeriodConsuming']=dict_add['jd_tob_rank']*dict_add['jd_riskPeriodConsuming']
    dict_add['jd_tob_rank_DIV_jd_riskPeriodConsuming']=dict_add['jd_tob_rank']/dict_add['jd_riskPeriodConsuming'] if dict_add['jd_riskPeriodConsuming'] > 0 else np.nan
    dict_add['jd_tob_rank_MIN_jd_liability_level']=dict_add['jd_tob_rank']-dict_add['jd_liability_level']
    dict_add['jd_tob_rank_MIN_jd_tot_90_amt_level']=dict_add['jd_tob_rank']-dict_add['jd_tot_90_amt_level']
    dict_add['jd_tob_rank_DIV_jd_risk_pre_score']=dict_add['jd_tob_rank']/dict_add['jd_risk_pre_score'] if dict_add['jd_risk_pre_score'] > 0 else np.nan
    dict_add['jd_tob_rank_MIN_jd_stability']=dict_add['jd_tob_rank']-dict_add['jd_stability']
    dict_add['jd_tob_rank_MUL_jd_stability']=dict_add['jd_tob_rank']*dict_add['jd_stability']
    dict_add['jd_tob_rank_MIN_jd_credit_consume_level']=dict_add['jd_tob_rank']-dict_add['jd_credit_consume_level']
    dict_add['jd_tob_rank_MIN_jd_tot_30_amt_level']=dict_add['jd_tob_rank']-dict_add['jd_tot_30_amt_level']
    dict_add['jd_tob_rank_MIN_jd_tot_dlq_days_all_365_level']=dict_add['jd_tob_rank']-dict_add['jd_tot_dlq_days_all_365_level']
    dict_add['jd_tot_180_amt_level_MIN_jd_tot_365_cnt_level']=dict_add['jd_tot_180_amt_level']-dict_add['jd_tot_365_cnt_level']
    dict_add['jd_tot_180_amt_level_MUL_jd_tot_365_cnt_level']=dict_add['jd_tot_180_amt_level']*dict_add['jd_tot_365_cnt_level']
    dict_add['jd_tot_180_amt_level_MIN_jd_riskIndex']=dict_add['jd_tot_180_amt_level']-dict_add['jd_riskIndex']
    dict_add['jd_tot_180_amt_level_MIN_jd_tot_60_amt_level']=dict_add['jd_tot_180_amt_level']-dict_add['jd_tot_60_amt_level']
    dict_add['jd_tot_180_amt_level_MIN_jd_tot_charge_180_cnt_level']=dict_add['jd_tot_180_amt_level']-dict_add['jd_tot_charge_180_cnt_level']
    dict_add['jd_tot_180_amt_level_MIN_jd_tot_60_cnt_level']=dict_add['jd_tot_180_amt_level']-dict_add['jd_tot_60_cnt_level']
    dict_add['jd_tot_180_amt_level_MIN_jd_ecommercecellphoneStability']=dict_add['jd_tot_180_amt_level']-dict_add['jd_ecommercecellphoneStability']
    dict_add['jd_tot_180_amt_level_MUL_jd_worktimeShopping']=dict_add['jd_tot_180_amt_level']*dict_add['jd_worktimeShopping']
    dict_add['jd_tot_180_amt_level_MIN_jd_time_on_book']=dict_add['jd_tot_180_amt_level']-dict_add['jd_time_on_book']
    dict_add['jd_tot_180_amt_level_MIN_jd_debt_bearing_level']=dict_add['jd_tot_180_amt_level']-dict_add['jd_debt_bearing_level']
    dict_add['jd_tot_180_amt_level_MIN_jd_tot_charge_60_cnt_level']=dict_add['jd_tot_180_amt_level']-dict_add['jd_tot_charge_60_cnt_level']
    dict_add['jd_tot_180_amt_level_MIN_jd_gd_level']=dict_add['jd_tot_180_amt_level']-dict_add['jd_gd_level']
    dict_add['jd_tot_180_amt_level_MIN_jd_income_score']=dict_add['jd_tot_180_amt_level']-dict_add['jd_income_score']
    dict_add['jd_tot_180_amt_level_DIV_jd_income_score']=dict_add['jd_tot_180_amt_level']/dict_add['jd_income_score'] if dict_add['jd_income_score'] > 0 else np.nan
    dict_add['jd_tot_180_amt_level_MIN_jd_tot_charge_60_amt_level']=dict_add['jd_tot_180_amt_level']-dict_add['jd_tot_charge_60_amt_level']
    dict_add['jd_tot_180_amt_level_MIN_jd_tot_365_amt_level']=dict_add['jd_tot_180_amt_level']-dict_add['jd_tot_365_amt_level']
    dict_add['jd_tot_180_amt_level_MIN_jd_tot_charge_30_cnt_level']=dict_add['jd_tot_180_amt_level']-dict_add['jd_tot_charge_30_cnt_level']
    dict_add['jd_tot_180_amt_level_DIV_jd_tot_charge_30_cnt_level']=dict_add['jd_tot_180_amt_level']/dict_add['jd_tot_charge_30_cnt_level'] if dict_add['jd_tot_charge_30_cnt_level'] > 0 else np.nan
    dict_add['jd_tot_180_amt_level_MIN_jd_riskCategoryConsuming']=dict_add['jd_tot_180_amt_level']-dict_add['jd_riskCategoryConsuming']
    dict_add['jd_tot_180_amt_level_MUL_jd_liability_level']=dict_add['jd_tot_180_amt_level']*dict_add['jd_liability_level']
    dict_add['jd_tot_180_amt_level_MIN_jd_tot_90_amt_level']=dict_add['jd_tot_180_amt_level']-dict_add['jd_tot_90_amt_level']
    dict_add['jd_tot_180_amt_level_MUL_jd_stability']=dict_add['jd_tot_180_amt_level']*dict_add['jd_stability']
    dict_add['jd_tot_180_amt_level_MIN_jd_credit_consume_level']=dict_add['jd_tot_180_amt_level']-dict_add['jd_credit_consume_level']
    dict_add['jd_tot_180_amt_level_MIN_jd_tot_30_amt_level']=dict_add['jd_tot_180_amt_level']-dict_add['jd_tot_30_amt_level']
    dict_add['jd_tot_365_cnt_level_MIN_jd_riskIndex']=dict_add['jd_tot_365_cnt_level']-dict_add['jd_riskIndex']
    dict_add['jd_tot_365_cnt_level_DIV_jd_riskIndex']=dict_add['jd_tot_365_cnt_level']/dict_add['jd_riskIndex'] if dict_add['jd_riskIndex'] > 0 else np.nan
    dict_add['jd_tot_365_cnt_level_MIN_jd_tot_charge_180_cnt_level']=dict_add['jd_tot_365_cnt_level']-dict_add['jd_tot_charge_180_cnt_level']
    dict_add['jd_tot_365_cnt_level_MIN_jd_tot_60_cnt_level']=dict_add['jd_tot_365_cnt_level']-dict_add['jd_tot_60_cnt_level']
    dict_add['jd_tot_365_cnt_level_MIN_jd_ecommercecellphoneStability']=dict_add['jd_tot_365_cnt_level']-dict_add['jd_ecommercecellphoneStability']
    dict_add['jd_tot_365_cnt_level_MUL_jd_worktimeShopping']=dict_add['jd_tot_365_cnt_level']*dict_add['jd_worktimeShopping']
    dict_add['jd_tot_365_cnt_level_MIN_jd_tot_charge_365_cnt_level']=dict_add['jd_tot_365_cnt_level']-dict_add['jd_tot_charge_365_cnt_level']
    dict_add['jd_tot_365_cnt_level_MIN_jd_debt_bearing_level']=dict_add['jd_tot_365_cnt_level']-dict_add['jd_debt_bearing_level']
    dict_add['jd_tot_365_cnt_level_MIN_jd_tot_charge_60_cnt_level']=dict_add['jd_tot_365_cnt_level']-dict_add['jd_tot_charge_60_cnt_level']
    dict_add['jd_tot_365_cnt_level_MIN_jd_gd_level']=dict_add['jd_tot_365_cnt_level']-dict_add['jd_gd_level']
    dict_add['jd_tot_365_cnt_level_MUL_jd_tot_365_amt_level']=dict_add['jd_tot_365_cnt_level']*dict_add['jd_tot_365_amt_level']
    dict_add['jd_tot_365_cnt_level_DIV_jd_tot_365_amt_level']=dict_add['jd_tot_365_cnt_level']/dict_add['jd_tot_365_amt_level'] if dict_add['jd_tot_365_amt_level'] > 0 else np.nan
    dict_add['jd_tot_365_cnt_level_MIN_jd_tot_charge_90_amt_level']=dict_add['jd_tot_365_cnt_level']-dict_add['jd_tot_charge_90_amt_level']
    dict_add['jd_tot_365_cnt_level_ADD_jd_riskCategoryConsuming']=dict_add['jd_tot_365_cnt_level']+dict_add['jd_riskCategoryConsuming']
    dict_add['jd_tot_365_cnt_level_MIN_jd_riskCategoryConsuming']=dict_add['jd_tot_365_cnt_level']-dict_add['jd_riskCategoryConsuming']
    dict_add['jd_tot_365_cnt_level_MIN_jd_riskPeriodConsuming']=dict_add['jd_tot_365_cnt_level']-dict_add['jd_riskPeriodConsuming']
    dict_add['jd_tot_365_cnt_level_MIN_jd_tot_90_amt_level']=dict_add['jd_tot_365_cnt_level']-dict_add['jd_tot_90_amt_level']
    dict_add['jd_tot_365_cnt_level_DIV_jd_tot_90_amt_level']=dict_add['jd_tot_365_cnt_level']/dict_add['jd_tot_90_amt_level'] if dict_add['jd_tot_90_amt_level'] > 0 else np.nan
    dict_add['jd_tot_365_cnt_level_MIN_jd_stability']=dict_add['jd_tot_365_cnt_level']-dict_add['jd_stability']
    dict_add['jd_tot_365_cnt_level_MIN_jd_credit_consume_level']=dict_add['jd_tot_365_cnt_level']-dict_add['jd_credit_consume_level']
    dict_add['jd_tot_365_cnt_level_MIN_jd_tot_30_amt_level']=dict_add['jd_tot_365_cnt_level']-dict_add['jd_tot_30_amt_level']
    dict_add['jd_tot_365_cnt_level_MIN_jd_tot_dlq_days_all_365_level']=dict_add['jd_tot_365_cnt_level']-dict_add['jd_tot_dlq_days_all_365_level']
    dict_add['jd_riskIndex_MIN_jd_tot_60_amt_level']=dict_add['jd_riskIndex']-dict_add['jd_tot_60_amt_level']
    dict_add['jd_riskIndex_MIN_jd_tot_charge_180_cnt_level']=dict_add['jd_riskIndex']-dict_add['jd_tot_charge_180_cnt_level']
    dict_add['jd_riskIndex_MIN_jd_worktimeShopping']=dict_add['jd_riskIndex']-dict_add['jd_worktimeShopping']
    dict_add['jd_riskIndex_ADD_jd_cd_level']=dict_add['jd_riskIndex']+dict_add['jd_cd_level']
    dict_add['jd_riskIndex_MUL_jd_cd_level']=dict_add['jd_riskIndex']*dict_add['jd_cd_level']
    dict_add['jd_riskIndex_ADD_jd_time_on_book']=dict_add['jd_riskIndex']+dict_add['jd_time_on_book']
    dict_add['jd_riskIndex_MIN_jd_time_on_book']=dict_add['jd_riskIndex']-dict_add['jd_time_on_book']
    dict_add['jd_riskIndex_MUL_jd_car_score']=dict_add['jd_riskIndex']*dict_add['jd_car_score']
    dict_add['jd_riskIndex_DIV_jd_car_score']=dict_add['jd_riskIndex']/dict_add['jd_car_score'] if dict_add['jd_car_score'] > 0 else np.nan
    dict_add['jd_riskIndex_ADD_jd_tot_charge_365_cnt_level']=dict_add['jd_riskIndex']+dict_add['jd_tot_charge_365_cnt_level']
    dict_add['jd_riskIndex_MIN_jd_tot_charge_365_cnt_level']=dict_add['jd_riskIndex']-dict_add['jd_tot_charge_365_cnt_level']
    dict_add['jd_riskIndex_DIV_jd_tot_charge_365_cnt_level']=dict_add['jd_riskIndex']/dict_add['jd_tot_charge_365_cnt_level'] if dict_add['jd_tot_charge_365_cnt_level'] > 0 else np.nan
    dict_add['jd_riskIndex_MIN_jd_have_child']=dict_add['jd_riskIndex']-dict_add['jd_have_child']
    dict_add['jd_riskIndex_MUL_jd_have_child']=dict_add['jd_riskIndex']*dict_add['jd_have_child']
    dict_add['jd_riskIndex_MIN_jd_tot_charge_60_cnt_level']=dict_add['jd_riskIndex']-dict_add['jd_tot_charge_60_cnt_level']
    dict_add['jd_riskIndex_ADD_jd_gd_level']=dict_add['jd_riskIndex']+dict_add['jd_gd_level']
    dict_add['jd_riskIndex_MIN_jd_income_score']=dict_add['jd_riskIndex']-dict_add['jd_income_score']
    dict_add['jd_riskIndex_ADD_jd_tot_charge_90_amt_level']=dict_add['jd_riskIndex']+dict_add['jd_tot_charge_90_amt_level']
    dict_add['jd_riskIndex_MIN_jd_tot_charge_90_amt_level']=dict_add['jd_riskIndex']-dict_add['jd_tot_charge_90_amt_level']
    dict_add['jd_riskIndex_DIV_jd_riskCategoryConsuming']=dict_add['jd_riskIndex']/dict_add['jd_riskCategoryConsuming'] if dict_add['jd_riskCategoryConsuming'] > 0 else np.nan
    dict_add['jd_riskIndex_MIN_jd_riskPeriodConsuming']=dict_add['jd_riskIndex']-dict_add['jd_riskPeriodConsuming']
    dict_add['jd_riskIndex_MUL_jd_riskPeriodConsuming']=dict_add['jd_riskIndex']*dict_add['jd_riskPeriodConsuming']
    dict_add['jd_riskIndex_MUL_jd_tot_90_amt_level']=dict_add['jd_riskIndex']*dict_add['jd_tot_90_amt_level']
    dict_add['jd_riskIndex_DIV_jd_risk_pre_score']=dict_add['jd_riskIndex']/dict_add['jd_risk_pre_score'] if dict_add['jd_risk_pre_score'] > 0 else np.nan
    dict_add['jd_riskIndex_MUL_jd_stability']=dict_add['jd_riskIndex']*dict_add['jd_stability']
    dict_add['jd_riskIndex_MIN_jd_tot_30_amt_level']=dict_add['jd_riskIndex']-dict_add['jd_tot_30_amt_level']
    dict_add['jd_riskIndex_MUL_jd_tot_dlq_days_all_365_level']=dict_add['jd_riskIndex']*dict_add['jd_tot_dlq_days_all_365_level']
    dict_add['jd_tot_60_amt_level_MIN_jd_tot_60_cnt_level']=dict_add['jd_tot_60_amt_level']-dict_add['jd_tot_60_cnt_level']
    dict_add['jd_tot_60_amt_level_MUL_jd_worktimeShopping']=dict_add['jd_tot_60_amt_level']*dict_add['jd_worktimeShopping']
    dict_add['jd_tot_60_amt_level_DIV_jd_car_score']=dict_add['jd_tot_60_amt_level']/dict_add['jd_car_score'] if dict_add['jd_car_score'] > 0 else np.nan
    dict_add['jd_tot_60_amt_level_MIN_jd_tot_charge_365_cnt_level']=dict_add['jd_tot_60_amt_level']-dict_add['jd_tot_charge_365_cnt_level']
    dict_add['jd_tot_60_amt_level_MUL_jd_tot_charge_365_cnt_level']=dict_add['jd_tot_60_amt_level']*dict_add['jd_tot_charge_365_cnt_level']
    dict_add['jd_tot_60_amt_level_MUL_jd_have_child']=dict_add['jd_tot_60_amt_level']*dict_add['jd_have_child']
    dict_add['jd_tot_60_amt_level_DIV_jd_income_score']=dict_add['jd_tot_60_amt_level']/dict_add['jd_income_score'] if dict_add['jd_income_score'] > 0 else np.nan
    dict_add['jd_tot_60_amt_level_MIN_jd_tot_365_amt_level']=dict_add['jd_tot_60_amt_level']-dict_add['jd_tot_365_amt_level']
    dict_add['jd_tot_60_amt_level_MUL_jd_tot_365_amt_level']=dict_add['jd_tot_60_amt_level']*dict_add['jd_tot_365_amt_level']
    dict_add['jd_tot_60_amt_level_MIN_jd_tot_charge_180_amt_level']=dict_add['jd_tot_60_amt_level']-dict_add['jd_tot_charge_180_amt_level']
    dict_add['jd_tot_60_amt_level_MIN_jd_tot_charge_90_amt_level']=dict_add['jd_tot_60_amt_level']-dict_add['jd_tot_charge_90_amt_level']
    dict_add['jd_tot_60_amt_level_MIN_jd_tot_charge_30_cnt_level']=dict_add['jd_tot_60_amt_level']-dict_add['jd_tot_charge_30_cnt_level']
    dict_add['jd_tot_60_amt_level_DIV_jd_riskCategoryConsuming']=dict_add['jd_tot_60_amt_level']/dict_add['jd_riskCategoryConsuming'] if dict_add['jd_riskCategoryConsuming'] > 0 else np.nan
    dict_add['jd_tot_60_amt_level_MIN_jd_tot_365_m_cnt_level']=dict_add['jd_tot_60_amt_level']-dict_add['jd_tot_365_m_cnt_level']
    dict_add['jd_tot_60_amt_level_DIV_jd_riskPeriodConsuming']=dict_add['jd_tot_60_amt_level']/dict_add['jd_riskPeriodConsuming'] if dict_add['jd_riskPeriodConsuming'] > 0 else np.nan
    dict_add['jd_tot_60_amt_level_MIN_jd_liability_level']=dict_add['jd_tot_60_amt_level']-dict_add['jd_liability_level']
    dict_add['jd_tot_60_amt_level_DIV_jd_tot_90_amt_level']=dict_add['jd_tot_60_amt_level']/dict_add['jd_tot_90_amt_level'] if dict_add['jd_tot_90_amt_level'] > 0 else np.nan
    dict_add['jd_tot_60_amt_level_MIN_jd_stability']=dict_add['jd_tot_60_amt_level']-dict_add['jd_stability']
    dict_add['jd_tot_60_amt_level_DIV_jd_credit_consume_level']=dict_add['jd_tot_60_amt_level']/dict_add['jd_credit_consume_level'] if dict_add['jd_credit_consume_level'] > 0 else np.nan
    dict_add['jd_tot_60_amt_level_MIN_jd_tot_30_amt_level']=dict_add['jd_tot_60_amt_level']-dict_add['jd_tot_30_amt_level']
    dict_add['jd_tot_charge_180_cnt_level_MIN_jd_ecommercecellphoneStability']=dict_add['jd_tot_charge_180_cnt_level']-dict_add['jd_ecommercecellphoneStability']
    dict_add['jd_tot_charge_180_cnt_level_MIN_jd_tot_charge_365_cnt_level']=dict_add['jd_tot_charge_180_cnt_level']-dict_add['jd_tot_charge_365_cnt_level']
    dict_add['jd_tot_charge_180_cnt_level_ADD_jd_debt_bearing_level']=dict_add['jd_tot_charge_180_cnt_level']+dict_add['jd_debt_bearing_level']
    dict_add['jd_tot_charge_180_cnt_level_MUL_jd_debt_bearing_level']=dict_add['jd_tot_charge_180_cnt_level']*dict_add['jd_debt_bearing_level']
    dict_add['jd_tot_charge_180_cnt_level_DIV_jd_debt_bearing_level']=dict_add['jd_tot_charge_180_cnt_level']/dict_add['jd_debt_bearing_level'] if dict_add['jd_debt_bearing_level'] > 0 else np.nan
    dict_add['jd_tot_charge_180_cnt_level_MIN_jd_tot_charge_60_cnt_level']=dict_add['jd_tot_charge_180_cnt_level']-dict_add['jd_tot_charge_60_cnt_level']
    dict_add['jd_tot_charge_180_cnt_level_MIN_jd_income_score']=dict_add['jd_tot_charge_180_cnt_level']-dict_add['jd_income_score']
    dict_add['jd_tot_charge_180_cnt_level_MIN_jd_tot_charge_60_amt_level']=dict_add['jd_tot_charge_180_cnt_level']-dict_add['jd_tot_charge_60_amt_level']
    dict_add['jd_tot_charge_180_cnt_level_MIN_jd_tot_365_amt_level']=dict_add['jd_tot_charge_180_cnt_level']-dict_add['jd_tot_365_amt_level']
    dict_add['jd_tot_charge_180_cnt_level_DIV_jd_tot_charge_180_amt_level']=dict_add['jd_tot_charge_180_cnt_level']/dict_add['jd_tot_charge_180_amt_level'] if dict_add['jd_tot_charge_180_amt_level'] > 0 else np.nan
    dict_add['jd_tot_charge_180_cnt_level_MIN_jd_riskCategoryConsuming']=dict_add['jd_tot_charge_180_cnt_level']-dict_add['jd_riskCategoryConsuming']
    dict_add['jd_tot_charge_180_cnt_level_DIV_jd_riskCategoryConsuming']=dict_add['jd_tot_charge_180_cnt_level']/dict_add['jd_riskCategoryConsuming'] if dict_add['jd_riskCategoryConsuming'] > 0 else np.nan
    dict_add['jd_tot_charge_180_cnt_level_MIN_jd_tot_365_m_cnt_level']=dict_add['jd_tot_charge_180_cnt_level']-dict_add['jd_tot_365_m_cnt_level']
    dict_add['jd_tot_charge_180_cnt_level_DIV_jd_tot_365_m_cnt_level']=dict_add['jd_tot_charge_180_cnt_level']/dict_add['jd_tot_365_m_cnt_level'] if dict_add['jd_tot_365_m_cnt_level'] > 0 else np.nan
    dict_add['jd_tot_charge_180_cnt_level_MIN_jd_riskPeriodConsuming']=dict_add['jd_tot_charge_180_cnt_level']-dict_add['jd_riskPeriodConsuming']
    dict_add['jd_tot_charge_180_cnt_level_MUL_jd_liability_level']=dict_add['jd_tot_charge_180_cnt_level']*dict_add['jd_liability_level']
    dict_add['jd_tot_charge_180_cnt_level_ADD_jd_risk_pre_score']=dict_add['jd_tot_charge_180_cnt_level']+dict_add['jd_risk_pre_score']
    dict_add['jd_tot_charge_180_cnt_level_MIN_jd_risk_pre_score']=dict_add['jd_tot_charge_180_cnt_level']-dict_add['jd_risk_pre_score']
    dict_add['jd_tot_charge_180_cnt_level_DIV_jd_risk_pre_score']=dict_add['jd_tot_charge_180_cnt_level']/dict_add['jd_risk_pre_score'] if dict_add['jd_risk_pre_score'] > 0 else np.nan
    dict_add['jd_tot_charge_180_cnt_level_MIN_jd_stability']=dict_add['jd_tot_charge_180_cnt_level']-dict_add['jd_stability']
    dict_add['jd_tot_charge_180_cnt_level_MIN_jd_credit_consume_level']=dict_add['jd_tot_charge_180_cnt_level']-dict_add['jd_credit_consume_level']
    dict_add['jd_tot_charge_180_cnt_level_DIV_jd_credit_consume_level']=dict_add['jd_tot_charge_180_cnt_level']/dict_add['jd_credit_consume_level'] if dict_add['jd_credit_consume_level'] > 0 else np.nan
    dict_add['jd_tot_60_cnt_level_MIN_jd_time_on_book']=dict_add['jd_tot_60_cnt_level']-dict_add['jd_time_on_book']
    dict_add['jd_tot_60_cnt_level_MIN_jd_tot_charge_60_cnt_level']=dict_add['jd_tot_60_cnt_level']-dict_add['jd_tot_charge_60_cnt_level']
    dict_add['jd_tot_60_cnt_level_MIN_jd_tot_charge_60_amt_level']=dict_add['jd_tot_60_cnt_level']-dict_add['jd_tot_charge_60_amt_level']
    dict_add['jd_tot_60_cnt_level_MIN_jd_tot_365_amt_level']=dict_add['jd_tot_60_cnt_level']-dict_add['jd_tot_365_amt_level']
    dict_add['jd_tot_60_cnt_level_MIN_jd_tot_charge_30_cnt_level']=dict_add['jd_tot_60_cnt_level']-dict_add['jd_tot_charge_30_cnt_level']
    dict_add['jd_tot_60_cnt_level_MIN_jd_riskCategoryConsuming']=dict_add['jd_tot_60_cnt_level']-dict_add['jd_riskCategoryConsuming']
    dict_add['jd_tot_60_cnt_level_ADD_jd_risk_pre_score']=dict_add['jd_tot_60_cnt_level']+dict_add['jd_risk_pre_score']
    dict_add['jd_tot_60_cnt_level_MUL_jd_risk_pre_score']=dict_add['jd_tot_60_cnt_level']*dict_add['jd_risk_pre_score']
    dict_add['jd_tot_60_cnt_level_DIV_jd_risk_pre_score']=dict_add['jd_tot_60_cnt_level']/dict_add['jd_risk_pre_score'] if dict_add['jd_risk_pre_score'] > 0 else np.nan
    dict_add['jd_tot_60_cnt_level_MIN_jd_credit_consume_level']=dict_add['jd_tot_60_cnt_level']-dict_add['jd_credit_consume_level']
    dict_add['jd_tot_60_cnt_level_MIN_jd_tot_30_amt_level']=dict_add['jd_tot_60_cnt_level']-dict_add['jd_tot_30_amt_level']
    dict_add['jd_ecommercecellphoneStability_MUL_jd_worktimeShopping']=dict_add['jd_ecommercecellphoneStability']*dict_add['jd_worktimeShopping']
    dict_add['jd_ecommercecellphoneStability_MIN_jd_cd_level']=dict_add['jd_ecommercecellphoneStability']-dict_add['jd_cd_level']
    dict_add['jd_ecommercecellphoneStability_MUL_jd_car_score']=dict_add['jd_ecommercecellphoneStability']*dict_add['jd_car_score']
    dict_add['jd_ecommercecellphoneStability_DIV_jd_car_score']=dict_add['jd_ecommercecellphoneStability']/dict_add['jd_car_score'] if dict_add['jd_car_score'] > 0 else np.nan
    dict_add['jd_ecommercecellphoneStability_MIN_jd_tot_charge_365_cnt_level']=dict_add['jd_ecommercecellphoneStability']-dict_add['jd_tot_charge_365_cnt_level']
    dict_add['jd_ecommercecellphoneStability_MIN_jd_debt_bearing_level']=dict_add['jd_ecommercecellphoneStability']-dict_add['jd_debt_bearing_level']
    dict_add['jd_ecommercecellphoneStability_MIN_jd_gd_level']=dict_add['jd_ecommercecellphoneStability']-dict_add['jd_gd_level']
    dict_add['jd_ecommercecellphoneStability_MIN_jd_tot_charge_180_amt_level']=dict_add['jd_ecommercecellphoneStability']-dict_add['jd_tot_charge_180_amt_level']
    dict_add['jd_ecommercecellphoneStability_DIV_jd_riskCategoryConsuming']=dict_add['jd_ecommercecellphoneStability']/dict_add['jd_riskCategoryConsuming'] if dict_add['jd_riskCategoryConsuming'] > 0 else np.nan
    dict_add['jd_ecommercecellphoneStability_MUL_jd_tot_365_m_cnt_level']=dict_add['jd_ecommercecellphoneStability']*dict_add['jd_tot_365_m_cnt_level']
    dict_add['jd_ecommercecellphoneStability_DIV_jd_riskPeriodConsuming']=dict_add['jd_ecommercecellphoneStability']/dict_add['jd_riskPeriodConsuming'] if dict_add['jd_riskPeriodConsuming'] > 0 else np.nan
    dict_add['jd_ecommercecellphoneStability_DIV_jd_risk_pre_score']=dict_add['jd_ecommercecellphoneStability']/dict_add['jd_risk_pre_score'] if dict_add['jd_risk_pre_score'] > 0 else np.nan
    dict_add['jd_ecommercecellphoneStability_MIN_jd_credit_consume_level']=dict_add['jd_ecommercecellphoneStability']-dict_add['jd_credit_consume_level']
    dict_add['jd_ecommercecellphoneStability_DIV_jd_credit_consume_level']=dict_add['jd_ecommercecellphoneStability']/dict_add['jd_credit_consume_level'] if dict_add['jd_credit_consume_level'] > 0 else np.nan
    dict_add['jd_worktimeShopping_MUL_jd_cd_level']=dict_add['jd_worktimeShopping']*dict_add['jd_cd_level']
    dict_add['jd_worktimeShopping_MUL_jd_max_dlq_days_all_60_level']=dict_add['jd_worktimeShopping']*dict_add['jd_max_dlq_days_all_60_level']
    dict_add['jd_worktimeShopping_ADD_jd_time_on_book']=dict_add['jd_worktimeShopping']+dict_add['jd_time_on_book']
    dict_add['jd_worktimeShopping_MIN_jd_car_score']=dict_add['jd_worktimeShopping']-dict_add['jd_car_score']
    dict_add['jd_worktimeShopping_DIV_jd_car_score']=dict_add['jd_worktimeShopping']/dict_add['jd_car_score'] if dict_add['jd_car_score'] > 0 else np.nan
    dict_add['jd_worktimeShopping_ADD_jd_gd_level']=dict_add['jd_worktimeShopping']+dict_add['jd_gd_level']
    dict_add['jd_worktimeShopping_ADD_jd_tot_charge_180_amt_level']=dict_add['jd_worktimeShopping']+dict_add['jd_tot_charge_180_amt_level']
    dict_add['jd_worktimeShopping_DIV_jd_riskCategoryConsuming']=dict_add['jd_worktimeShopping']/dict_add['jd_riskCategoryConsuming'] if dict_add['jd_riskCategoryConsuming'] > 0 else np.nan
    dict_add['jd_worktimeShopping_MUL_jd_riskPeriodConsuming']=dict_add['jd_worktimeShopping']*dict_add['jd_riskPeriodConsuming']
    dict_add['jd_worktimeShopping_MUL_jd_tot_90_amt_level']=dict_add['jd_worktimeShopping']*dict_add['jd_tot_90_amt_level']
    dict_add['jd_worktimeShopping_DIV_jd_tot_90_amt_level']=dict_add['jd_worktimeShopping']/dict_add['jd_tot_90_amt_level'] if dict_add['jd_tot_90_amt_level'] > 0 else np.nan
    dict_add['jd_worktimeShopping_DIV_jd_risk_pre_score']=dict_add['jd_worktimeShopping']/dict_add['jd_risk_pre_score'] if dict_add['jd_risk_pre_score'] > 0 else np.nan
    dict_add['jd_worktimeShopping_MUL_jd_stability']=dict_add['jd_worktimeShopping']*dict_add['jd_stability']
    dict_add['jd_worktimeShopping_DIV_jd_stability']=dict_add['jd_worktimeShopping']/dict_add['jd_stability'] if dict_add['jd_stability'] > 0 else np.nan
    dict_add['jd_worktimeShopping_MIN_jd_tot_30_amt_level']=dict_add['jd_worktimeShopping']-dict_add['jd_tot_30_amt_level']
    dict_add['jd_cd_level_MIN_jd_time_on_book']=dict_add['jd_cd_level']-dict_add['jd_time_on_book']
    dict_add['jd_cd_level_DIV_jd_time_on_book']=dict_add['jd_cd_level']/dict_add['jd_time_on_book'] if dict_add['jd_time_on_book'] > 0 else np.nan
    dict_add['jd_cd_level_DIV_jd_have_child']=dict_add['jd_cd_level']/dict_add['jd_have_child'] if dict_add['jd_have_child'] > 0 else np.nan
    dict_add['jd_cd_level_MIN_jd_debt_bearing_level']=dict_add['jd_cd_level']-dict_add['jd_debt_bearing_level']
    dict_add['jd_cd_level_DIV_jd_debt_bearing_level']=dict_add['jd_cd_level']/dict_add['jd_debt_bearing_level'] if dict_add['jd_debt_bearing_level'] > 0 else np.nan
    dict_add['jd_cd_level_DIV_jd_income_score']=dict_add['jd_cd_level']/dict_add['jd_income_score'] if dict_add['jd_income_score'] > 0 else np.nan
    dict_add['jd_cd_level_DIV_jd_riskCategoryConsuming']=dict_add['jd_cd_level']/dict_add['jd_riskCategoryConsuming'] if dict_add['jd_riskCategoryConsuming'] > 0 else np.nan
    dict_add['jd_cd_level_MIN_jd_riskPeriodConsuming']=dict_add['jd_cd_level']-dict_add['jd_riskPeriodConsuming']
    dict_add['jd_cd_level_MIN_jd_liability_level']=dict_add['jd_cd_level']-dict_add['jd_liability_level']
    dict_add['jd_cd_level_MIN_jd_tot_90_amt_level']=dict_add['jd_cd_level']-dict_add['jd_tot_90_amt_level']
    dict_add['jd_cd_level_MIN_jd_stability']=dict_add['jd_cd_level']-dict_add['jd_stability']
    dict_add['jd_cd_level_MIN_jd_tot_30_amt_level']=dict_add['jd_cd_level']-dict_add['jd_tot_30_amt_level']
    dict_add['jd_cd_level_DIV_jd_tot_dlq_days_all_365_level']=dict_add['jd_cd_level']/dict_add['jd_tot_dlq_days_all_365_level'] if dict_add['jd_tot_dlq_days_all_365_level'] > 0 else np.nan
    dict_add['jd_max_dlq_days_all_60_level_DIV_jd_debt_bearing_level']=dict_add['jd_max_dlq_days_all_60_level']/dict_add['jd_debt_bearing_level'] if dict_add['jd_debt_bearing_level'] > 0 else np.nan
    dict_add['jd_max_dlq_days_all_60_level_MUL_jd_max_dlq_days_all_180_level']=dict_add['jd_max_dlq_days_all_60_level']*dict_add['jd_max_dlq_days_all_180_level']
    dict_add['jd_max_dlq_days_all_60_level_MUL_jd_tot_charge_60_amt_level']=dict_add['jd_max_dlq_days_all_60_level']*dict_add['jd_tot_charge_60_amt_level']
    dict_add['jd_max_dlq_days_all_60_level_MUL_jd_tot_charge_90_amt_level']=dict_add['jd_max_dlq_days_all_60_level']*dict_add['jd_tot_charge_90_amt_level']
    dict_add['jd_max_dlq_days_all_60_level_MUL_jd_tot_30_amt_level']=dict_add['jd_max_dlq_days_all_60_level']*dict_add['jd_tot_30_amt_level']
    dict_add['jd_max_dlq_days_all_60_level_MUL_jd_tot_dlq_days_all_365_level']=dict_add['jd_max_dlq_days_all_60_level']*dict_add['jd_tot_dlq_days_all_365_level']
    dict_add['jd_time_on_book_DIV_jd_car_score']=dict_add['jd_time_on_book']/dict_add['jd_car_score'] if dict_add['jd_car_score'] > 0 else np.nan
    dict_add['jd_time_on_book_MIN_jd_debt_bearing_level']=dict_add['jd_time_on_book']-dict_add['jd_debt_bearing_level']
    dict_add['jd_time_on_book_DIV_jd_max_dlq_days_all_180_level']=dict_add['jd_time_on_book']/dict_add['jd_max_dlq_days_all_180_level'] if dict_add['jd_max_dlq_days_all_180_level'] > 0 else np.nan
    dict_add['jd_time_on_book_DIV_jd_income_score']=dict_add['jd_time_on_book']/dict_add['jd_income_score'] if dict_add['jd_income_score'] > 0 else np.nan
    dict_add['jd_time_on_book_MIN_jd_tot_365_amt_level']=dict_add['jd_time_on_book']-dict_add['jd_tot_365_amt_level']
    dict_add['jd_time_on_book_MIN_jd_tot_charge_180_amt_level']=dict_add['jd_time_on_book']-dict_add['jd_tot_charge_180_amt_level']
    dict_add['jd_time_on_book_MUL_jd_tot_charge_30_cnt_level']=dict_add['jd_time_on_book']*dict_add['jd_tot_charge_30_cnt_level']
    dict_add['jd_time_on_book_DIV_jd_riskPeriodConsuming']=dict_add['jd_time_on_book']/dict_add['jd_riskPeriodConsuming'] if dict_add['jd_riskPeriodConsuming'] > 0 else np.nan
    dict_add['jd_time_on_book_MIN_jd_liability_level']=dict_add['jd_time_on_book']-dict_add['jd_liability_level']
    dict_add['jd_time_on_book_DIV_jd_tot_90_amt_level']=dict_add['jd_time_on_book']/dict_add['jd_tot_90_amt_level'] if dict_add['jd_tot_90_amt_level'] > 0 else np.nan
    dict_add['jd_time_on_book_MIN_jd_stability']=dict_add['jd_time_on_book']-dict_add['jd_stability']
    dict_add['jd_time_on_book_MUL_jd_stability']=dict_add['jd_time_on_book']*dict_add['jd_stability']
    dict_add['jd_time_on_book_DIV_jd_stability']=dict_add['jd_time_on_book']/dict_add['jd_stability'] if dict_add['jd_stability'] > 0 else np.nan
    dict_add['jd_time_on_book_MIN_jd_credit_consume_level']=dict_add['jd_time_on_book']-dict_add['jd_credit_consume_level']
    dict_add['jd_car_score_MUL_jd_max_dlq_days_all_180_level']=dict_add['jd_car_score']*dict_add['jd_max_dlq_days_all_180_level']
    dict_add['jd_car_score_DIV_jd_max_dlq_days_all_180_level']=dict_add['jd_car_score']/dict_add['jd_max_dlq_days_all_180_level'] if dict_add['jd_max_dlq_days_all_180_level'] > 0 else np.nan
    dict_add['jd_car_score_MIN_jd_gd_level']=dict_add['jd_car_score']-dict_add['jd_gd_level']
    dict_add['jd_car_score_DIV_jd_riskCategoryConsuming']=dict_add['jd_car_score']/dict_add['jd_riskCategoryConsuming'] if dict_add['jd_riskCategoryConsuming'] > 0 else np.nan
    dict_add['jd_car_score_MIN_jd_tot_365_m_cnt_level']=dict_add['jd_car_score']-dict_add['jd_tot_365_m_cnt_level']
    dict_add['jd_car_score_DIV_jd_risk_pre_score']=dict_add['jd_car_score']/dict_add['jd_risk_pre_score'] if dict_add['jd_risk_pre_score'] > 0 else np.nan
    dict_add['jd_tot_charge_365_cnt_level_DIV_jd_debt_bearing_level']=dict_add['jd_tot_charge_365_cnt_level']/dict_add['jd_debt_bearing_level'] if dict_add['jd_debt_bearing_level'] > 0 else np.nan
    dict_add['jd_tot_charge_365_cnt_level_MIN_jd_tot_charge_60_cnt_level']=dict_add['jd_tot_charge_365_cnt_level']-dict_add['jd_tot_charge_60_cnt_level']
    dict_add['jd_tot_charge_365_cnt_level_MIN_jd_gd_level']=dict_add['jd_tot_charge_365_cnt_level']-dict_add['jd_gd_level']
    dict_add['jd_tot_charge_365_cnt_level_MIN_jd_tot_365_amt_level']=dict_add['jd_tot_charge_365_cnt_level']-dict_add['jd_tot_365_amt_level']
    dict_add['jd_tot_charge_365_cnt_level_DIV_jd_tot_365_amt_level']=dict_add['jd_tot_charge_365_cnt_level']/dict_add['jd_tot_365_amt_level'] if dict_add['jd_tot_365_amt_level'] > 0 else np.nan
    dict_add['jd_tot_charge_365_cnt_level_MIN_jd_tot_charge_90_amt_level']=dict_add['jd_tot_charge_365_cnt_level']-dict_add['jd_tot_charge_90_amt_level']
    dict_add['jd_tot_charge_365_cnt_level_MIN_jd_tot_365_m_cnt_level']=dict_add['jd_tot_charge_365_cnt_level']-dict_add['jd_tot_365_m_cnt_level']
    dict_add['jd_tot_charge_365_cnt_level_MIN_jd_riskPeriodConsuming']=dict_add['jd_tot_charge_365_cnt_level']-dict_add['jd_riskPeriodConsuming']
    dict_add['jd_tot_charge_365_cnt_level_DIV_jd_liability_level']=dict_add['jd_tot_charge_365_cnt_level']/dict_add['jd_liability_level'] if dict_add['jd_liability_level'] > 0 else np.nan
    dict_add['jd_tot_charge_365_cnt_level_MUL_jd_tot_90_amt_level']=dict_add['jd_tot_charge_365_cnt_level']*dict_add['jd_tot_90_amt_level']
    dict_add['jd_tot_charge_365_cnt_level_DIV_jd_tot_90_amt_level']=dict_add['jd_tot_charge_365_cnt_level']/dict_add['jd_tot_90_amt_level'] if dict_add['jd_tot_90_amt_level'] > 0 else np.nan
    dict_add['jd_tot_charge_365_cnt_level_MIN_jd_credit_consume_level']=dict_add['jd_tot_charge_365_cnt_level']-dict_add['jd_credit_consume_level']
    dict_add['jd_tot_charge_365_cnt_level_DIV_jd_credit_consume_level']=dict_add['jd_tot_charge_365_cnt_level']/dict_add['jd_credit_consume_level'] if dict_add['jd_credit_consume_level'] > 0 else np.nan
    dict_add['jd_have_child_MUL_jd_debt_bearing_level']=dict_add['jd_have_child']*dict_add['jd_debt_bearing_level']
    dict_add['jd_have_child_DIV_jd_max_dlq_days_all_180_level']=dict_add['jd_have_child']/dict_add['jd_max_dlq_days_all_180_level'] if dict_add['jd_max_dlq_days_all_180_level'] > 0 else np.nan
    dict_add['jd_have_child_MIN_jd_income_score']=dict_add['jd_have_child']-dict_add['jd_income_score']
    dict_add['jd_have_child_DIV_jd_tot_365_amt_level']=dict_add['jd_have_child']/dict_add['jd_tot_365_amt_level'] if dict_add['jd_tot_365_amt_level'] > 0 else np.nan
    dict_add['jd_have_child_MIN_jd_riskCategoryConsuming']=dict_add['jd_have_child']-dict_add['jd_riskCategoryConsuming']
    dict_add['jd_have_child_DIV_jd_riskCategoryConsuming']=dict_add['jd_have_child']/dict_add['jd_riskCategoryConsuming'] if dict_add['jd_riskCategoryConsuming'] > 0 else np.nan
    dict_add['jd_have_child_DIV_jd_riskPeriodConsuming']=dict_add['jd_have_child']/dict_add['jd_riskPeriodConsuming'] if dict_add['jd_riskPeriodConsuming'] > 0 else np.nan
    dict_add['jd_have_child_DIV_jd_tot_90_amt_level']=dict_add['jd_have_child']/dict_add['jd_tot_90_amt_level'] if dict_add['jd_tot_90_amt_level'] > 0 else np.nan
    dict_add['jd_have_child_ADD_jd_risk_pre_score']=dict_add['jd_have_child']+dict_add['jd_risk_pre_score']
    dict_add['jd_have_child_MIN_jd_risk_pre_score']=dict_add['jd_have_child']-dict_add['jd_risk_pre_score']
    dict_add['jd_have_child_MUL_jd_risk_pre_score']=dict_add['jd_have_child']*dict_add['jd_risk_pre_score']
    dict_add['jd_have_child_MUL_jd_stability']=dict_add['jd_have_child']*dict_add['jd_stability']
    dict_add['jd_have_child_DIV_jd_stability']=dict_add['jd_have_child']/dict_add['jd_stability'] if dict_add['jd_stability'] > 0 else np.nan
    dict_add['jd_have_child_DIV_jd_tot_30_amt_level']=dict_add['jd_have_child']/dict_add['jd_tot_30_amt_level'] if dict_add['jd_tot_30_amt_level'] > 0 else np.nan
    dict_add['jd_debt_bearing_level_MUL_jd_income_score']=dict_add['jd_debt_bearing_level']*dict_add['jd_income_score']
    dict_add['jd_debt_bearing_level_MIN_jd_tot_365_amt_level']=dict_add['jd_debt_bearing_level']-dict_add['jd_tot_365_amt_level']
    dict_add['jd_debt_bearing_level_MUL_jd_tot_charge_30_cnt_level']=dict_add['jd_debt_bearing_level']*dict_add['jd_tot_charge_30_cnt_level']
    dict_add['jd_debt_bearing_level_MIN_jd_tot_365_m_cnt_level']=dict_add['jd_debt_bearing_level']-dict_add['jd_tot_365_m_cnt_level']
    dict_add['jd_debt_bearing_level_DIV_jd_riskPeriodConsuming']=dict_add['jd_debt_bearing_level']/dict_add['jd_riskPeriodConsuming'] if dict_add['jd_riskPeriodConsuming'] > 0 else np.nan
    dict_add['jd_debt_bearing_level_MIN_jd_tot_90_amt_level']=dict_add['jd_debt_bearing_level']-dict_add['jd_tot_90_amt_level']
    dict_add['jd_debt_bearing_level_MUL_jd_risk_pre_score']=dict_add['jd_debt_bearing_level']*dict_add['jd_risk_pre_score']
    dict_add['jd_debt_bearing_level_MIN_jd_credit_consume_level']=dict_add['jd_debt_bearing_level']-dict_add['jd_credit_consume_level']
    dict_add['jd_debt_bearing_level_MIN_jd_tot_30_amt_level']=dict_add['jd_debt_bearing_level']-dict_add['jd_tot_30_amt_level']
    dict_add['jd_max_dlq_days_all_180_level_MIN_jd_tot_charge_30_cnt_level']=dict_add['jd_max_dlq_days_all_180_level']-dict_add['jd_tot_charge_30_cnt_level']
    dict_add['jd_max_dlq_days_all_180_level_MUL_jd_stability']=dict_add['jd_max_dlq_days_all_180_level']*dict_add['jd_stability']
    dict_add['jd_max_dlq_days_all_180_level_MUL_jd_tot_30_amt_level']=dict_add['jd_max_dlq_days_all_180_level']*dict_add['jd_tot_30_amt_level']
    dict_add['jd_tot_charge_60_cnt_level_MIN_jd_tot_365_amt_level']=dict_add['jd_tot_charge_60_cnt_level']-dict_add['jd_tot_365_amt_level']
    dict_add['jd_tot_charge_60_cnt_level_MIN_jd_tot_charge_30_cnt_level']=dict_add['jd_tot_charge_60_cnt_level']-dict_add['jd_tot_charge_30_cnt_level']
    dict_add['jd_tot_charge_60_cnt_level_MIN_jd_riskCategoryConsuming']=dict_add['jd_tot_charge_60_cnt_level']-dict_add['jd_riskCategoryConsuming']
    dict_add['jd_tot_charge_60_cnt_level_DIV_jd_riskCategoryConsuming']=dict_add['jd_tot_charge_60_cnt_level']/dict_add['jd_riskCategoryConsuming'] if dict_add['jd_riskCategoryConsuming'] > 0 else np.nan
    dict_add['jd_tot_charge_60_cnt_level_ADD_jd_tot_365_m_cnt_level']=dict_add['jd_tot_charge_60_cnt_level']+dict_add['jd_tot_365_m_cnt_level']
    dict_add['jd_tot_charge_60_cnt_level_DIV_jd_riskPeriodConsuming']=dict_add['jd_tot_charge_60_cnt_level']/dict_add['jd_riskPeriodConsuming'] if dict_add['jd_riskPeriodConsuming'] > 0 else np.nan
    dict_add['jd_tot_charge_60_cnt_level_MIN_jd_stability']=dict_add['jd_tot_charge_60_cnt_level']-dict_add['jd_stability']
    dict_add['jd_tot_charge_60_cnt_level_MIN_jd_tot_30_amt_level']=dict_add['jd_tot_charge_60_cnt_level']-dict_add['jd_tot_30_amt_level']
    dict_add['jd_tot_charge_60_cnt_level_MUL_jd_tot_dlq_days_all_365_level']=dict_add['jd_tot_charge_60_cnt_level']*dict_add['jd_tot_dlq_days_all_365_level']
    dict_add['jd_gd_level_MIN_jd_tot_charge_60_amt_level']=dict_add['jd_gd_level']-dict_add['jd_tot_charge_60_amt_level']
    dict_add['jd_gd_level_MIN_jd_tot_charge_180_amt_level']=dict_add['jd_gd_level']-dict_add['jd_tot_charge_180_amt_level']
    dict_add['jd_gd_level_MIN_jd_tot_charge_90_amt_level']=dict_add['jd_gd_level']-dict_add['jd_tot_charge_90_amt_level']
    dict_add['jd_gd_level_MIN_jd_tot_charge_30_cnt_level']=dict_add['jd_gd_level']-dict_add['jd_tot_charge_30_cnt_level']
    dict_add['jd_gd_level_ADD_jd_riskCategoryConsuming']=dict_add['jd_gd_level']+dict_add['jd_riskCategoryConsuming']
    dict_add['jd_gd_level_MIN_jd_riskCategoryConsuming']=dict_add['jd_gd_level']-dict_add['jd_riskCategoryConsuming']
    dict_add['jd_gd_level_MIN_jd_tot_365_m_cnt_level']=dict_add['jd_gd_level']-dict_add['jd_tot_365_m_cnt_level']
    dict_add['jd_income_score_MUL_jd_tot_365_amt_level']=dict_add['jd_income_score']*dict_add['jd_tot_365_amt_level']
    dict_add['jd_income_score_DIV_jd_tot_365_amt_level']=dict_add['jd_income_score']/dict_add['jd_tot_365_amt_level'] if dict_add['jd_tot_365_amt_level'] > 0 else np.nan
    dict_add['jd_income_score_MIN_jd_riskCategoryConsuming']=dict_add['jd_income_score']-dict_add['jd_riskCategoryConsuming']
    dict_add['jd_income_score_DIV_jd_tot_365_m_cnt_level']=dict_add['jd_income_score']/dict_add['jd_tot_365_m_cnt_level'] if dict_add['jd_tot_365_m_cnt_level'] > 0 else np.nan
    dict_add['jd_income_score_ADD_jd_riskPeriodConsuming']=dict_add['jd_income_score']+dict_add['jd_riskPeriodConsuming']
    dict_add['jd_income_score_DIV_jd_riskPeriodConsuming']=dict_add['jd_income_score']/dict_add['jd_riskPeriodConsuming'] if dict_add['jd_riskPeriodConsuming'] > 0 else np.nan
    dict_add['jd_income_score_DIV_jd_tot_90_amt_level']=dict_add['jd_income_score']/dict_add['jd_tot_90_amt_level'] if dict_add['jd_tot_90_amt_level'] > 0 else np.nan
    dict_add['jd_income_score_MIN_jd_risk_pre_score']=dict_add['jd_income_score']-dict_add['jd_risk_pre_score']
    dict_add['jd_income_score_ADD_jd_credit_consume_level']=dict_add['jd_income_score']+dict_add['jd_credit_consume_level']
    dict_add['jd_income_score_DIV_jd_tot_30_amt_level']=dict_add['jd_income_score']/dict_add['jd_tot_30_amt_level'] if dict_add['jd_tot_30_amt_level'] > 0 else np.nan
    dict_add['jd_income_score_DIV_jd_tot_dlq_days_all_365_level']=dict_add['jd_income_score']/dict_add['jd_tot_dlq_days_all_365_level'] if dict_add['jd_tot_dlq_days_all_365_level'] > 0 else np.nan
    dict_add['jd_tot_charge_60_amt_level_MIN_jd_tot_365_amt_level']=dict_add['jd_tot_charge_60_amt_level']-dict_add['jd_tot_365_amt_level']
    dict_add['jd_tot_charge_60_amt_level_MIN_jd_tot_charge_180_amt_level']=dict_add['jd_tot_charge_60_amt_level']-dict_add['jd_tot_charge_180_amt_level']
    dict_add['jd_tot_charge_60_amt_level_MIN_jd_tot_charge_90_amt_level']=dict_add['jd_tot_charge_60_amt_level']-dict_add['jd_tot_charge_90_amt_level']
    dict_add['jd_tot_charge_60_amt_level_MIN_jd_tot_charge_30_cnt_level']=dict_add['jd_tot_charge_60_amt_level']-dict_add['jd_tot_charge_30_cnt_level']
    dict_add['jd_tot_charge_60_amt_level_MUL_jd_tot_365_m_cnt_level']=dict_add['jd_tot_charge_60_amt_level']*dict_add['jd_tot_365_m_cnt_level']
    dict_add['jd_tot_charge_60_amt_level_DIV_jd_tot_365_m_cnt_level']=dict_add['jd_tot_charge_60_amt_level']/dict_add['jd_tot_365_m_cnt_level'] if dict_add['jd_tot_365_m_cnt_level'] > 0 else np.nan
    dict_add['jd_tot_charge_60_amt_level_MIN_jd_riskPeriodConsuming']=dict_add['jd_tot_charge_60_amt_level']-dict_add['jd_riskPeriodConsuming']
    dict_add['jd_tot_charge_60_amt_level_MIN_jd_liability_level']=dict_add['jd_tot_charge_60_amt_level']-dict_add['jd_liability_level']
    dict_add['jd_tot_charge_60_amt_level_MIN_jd_tot_90_amt_level']=dict_add['jd_tot_charge_60_amt_level']-dict_add['jd_tot_90_amt_level']
    dict_add['jd_tot_charge_60_amt_level_MIN_jd_stability']=dict_add['jd_tot_charge_60_amt_level']-dict_add['jd_stability']
    dict_add['jd_tot_charge_60_amt_level_DIV_jd_stability']=dict_add['jd_tot_charge_60_amt_level']/dict_add['jd_stability'] if dict_add['jd_stability'] > 0 else np.nan
    dict_add['jd_tot_365_amt_level_MIN_jd_tot_charge_180_amt_level']=dict_add['jd_tot_365_amt_level']-dict_add['jd_tot_charge_180_amt_level']
    dict_add['jd_tot_365_amt_level_MIN_jd_tot_charge_30_cnt_level']=dict_add['jd_tot_365_amt_level']-dict_add['jd_tot_charge_30_cnt_level']
    dict_add['jd_tot_365_amt_level_MIN_jd_riskCategoryConsuming']=dict_add['jd_tot_365_amt_level']-dict_add['jd_riskCategoryConsuming']
    dict_add['jd_tot_365_amt_level_MIN_jd_tot_365_m_cnt_level']=dict_add['jd_tot_365_amt_level']-dict_add['jd_tot_365_m_cnt_level']
    dict_add['jd_tot_365_amt_level_MIN_jd_riskPeriodConsuming']=dict_add['jd_tot_365_amt_level']-dict_add['jd_riskPeriodConsuming']
    dict_add['jd_tot_365_amt_level_MIN_jd_tot_90_amt_level']=dict_add['jd_tot_365_amt_level']-dict_add['jd_tot_90_amt_level']
    dict_add['jd_tot_365_amt_level_MIN_jd_credit_consume_level']=dict_add['jd_tot_365_amt_level']-dict_add['jd_credit_consume_level']
    dict_add['jd_tot_charge_180_amt_level_MIN_jd_tot_charge_90_amt_level']=dict_add['jd_tot_charge_180_amt_level']-dict_add['jd_tot_charge_90_amt_level']
    dict_add['jd_tot_charge_180_amt_level_MIN_jd_riskCategoryConsuming']=dict_add['jd_tot_charge_180_amt_level']-dict_add['jd_riskCategoryConsuming']
    dict_add['jd_tot_charge_180_amt_level_DIV_jd_riskCategoryConsuming']=dict_add['jd_tot_charge_180_amt_level']/dict_add['jd_riskCategoryConsuming'] if dict_add['jd_riskCategoryConsuming'] > 0 else np.nan
    dict_add['jd_tot_charge_180_amt_level_MIN_jd_tot_90_amt_level']=dict_add['jd_tot_charge_180_amt_level']-dict_add['jd_tot_90_amt_level']
    dict_add['jd_tot_charge_180_amt_level_DIV_jd_risk_pre_score']=dict_add['jd_tot_charge_180_amt_level']/dict_add['jd_risk_pre_score'] if dict_add['jd_risk_pre_score'] > 0 else np.nan
    dict_add['jd_tot_charge_180_amt_level_MIN_jd_stability']=dict_add['jd_tot_charge_180_amt_level']-dict_add['jd_stability']
    dict_add['jd_tot_charge_180_amt_level_MIN_jd_credit_consume_level']=dict_add['jd_tot_charge_180_amt_level']-dict_add['jd_credit_consume_level']
    dict_add['jd_tot_charge_180_amt_level_MIN_jd_tot_30_amt_level']=dict_add['jd_tot_charge_180_amt_level']-dict_add['jd_tot_30_amt_level']
    dict_add['jd_tot_charge_180_amt_level_DIV_jd_tot_dlq_days_all_365_level']=dict_add['jd_tot_charge_180_amt_level']/dict_add['jd_tot_dlq_days_all_365_level'] if dict_add['jd_tot_dlq_days_all_365_level'] > 0 else np.nan
    dict_add['jd_tot_charge_90_amt_level_MIN_jd_tot_charge_30_cnt_level']=dict_add['jd_tot_charge_90_amt_level']-dict_add['jd_tot_charge_30_cnt_level']
    dict_add['jd_tot_charge_90_amt_level_MIN_jd_riskCategoryConsuming']=dict_add['jd_tot_charge_90_amt_level']-dict_add['jd_riskCategoryConsuming']
    dict_add['jd_tot_charge_90_amt_level_MIN_jd_tot_365_m_cnt_level']=dict_add['jd_tot_charge_90_amt_level']-dict_add['jd_tot_365_m_cnt_level']
    dict_add['jd_tot_charge_90_amt_level_MIN_jd_stability']=dict_add['jd_tot_charge_90_amt_level']-dict_add['jd_stability']
    dict_add['jd_tot_charge_90_amt_level_MIN_jd_tot_30_amt_level']=dict_add['jd_tot_charge_90_amt_level']-dict_add['jd_tot_30_amt_level']
    dict_add['jd_tot_charge_30_cnt_level_DIV_jd_riskCategoryConsuming']=dict_add['jd_tot_charge_30_cnt_level']/dict_add['jd_riskCategoryConsuming'] if dict_add['jd_riskCategoryConsuming'] > 0 else np.nan
    dict_add['jd_tot_charge_30_cnt_level_ADD_jd_tot_365_m_cnt_level']=dict_add['jd_tot_charge_30_cnt_level']+dict_add['jd_tot_365_m_cnt_level']
    dict_add['jd_tot_charge_30_cnt_level_DIV_jd_riskPeriodConsuming']=dict_add['jd_tot_charge_30_cnt_level']/dict_add['jd_riskPeriodConsuming'] if dict_add['jd_riskPeriodConsuming'] > 0 else np.nan
    dict_add['jd_tot_charge_30_cnt_level_MUL_jd_risk_pre_score']=dict_add['jd_tot_charge_30_cnt_level']*dict_add['jd_risk_pre_score']
    dict_add['jd_tot_charge_30_cnt_level_MIN_jd_credit_consume_level']=dict_add['jd_tot_charge_30_cnt_level']-dict_add['jd_credit_consume_level']
    dict_add['jd_tot_charge_30_cnt_level_MIN_jd_tot_30_amt_level']=dict_add['jd_tot_charge_30_cnt_level']-dict_add['jd_tot_30_amt_level']
    dict_add['jd_riskCategoryConsuming_MIN_jd_tot_365_m_cnt_level']=dict_add['jd_riskCategoryConsuming']-dict_add['jd_tot_365_m_cnt_level']
    dict_add['jd_riskCategoryConsuming_MIN_jd_riskPeriodConsuming']=dict_add['jd_riskCategoryConsuming']-dict_add['jd_riskPeriodConsuming']
    dict_add['jd_riskCategoryConsuming_MUL_jd_riskPeriodConsuming']=dict_add['jd_riskCategoryConsuming']*dict_add['jd_riskPeriodConsuming']
    dict_add['jd_riskCategoryConsuming_DIV_jd_riskPeriodConsuming']=dict_add['jd_riskCategoryConsuming']/dict_add['jd_riskPeriodConsuming'] if dict_add['jd_riskPeriodConsuming'] > 0 else np.nan
    dict_add['jd_riskCategoryConsuming_MIN_jd_liability_level']=dict_add['jd_riskCategoryConsuming']-dict_add['jd_liability_level']
    dict_add['jd_riskCategoryConsuming_MIN_jd_tot_90_amt_level']=dict_add['jd_riskCategoryConsuming']-dict_add['jd_tot_90_amt_level']
    dict_add['jd_riskCategoryConsuming_MUL_jd_tot_90_amt_level']=dict_add['jd_riskCategoryConsuming']*dict_add['jd_tot_90_amt_level']
    dict_add['jd_riskCategoryConsuming_DIV_jd_risk_pre_score']=dict_add['jd_riskCategoryConsuming']/dict_add['jd_risk_pre_score'] if dict_add['jd_risk_pre_score'] > 0 else np.nan
    dict_add['jd_riskCategoryConsuming_DIV_jd_stability']=dict_add['jd_riskCategoryConsuming']/dict_add['jd_stability'] if dict_add['jd_stability'] > 0 else np.nan
    dict_add['jd_riskCategoryConsuming_MIN_jd_credit_consume_level']=dict_add['jd_riskCategoryConsuming']-dict_add['jd_credit_consume_level']
    dict_add['jd_riskCategoryConsuming_MIN_jd_tot_30_amt_level']=dict_add['jd_riskCategoryConsuming']-dict_add['jd_tot_30_amt_level']
    dict_add['jd_tot_365_m_cnt_level_MIN_jd_riskPeriodConsuming']=dict_add['jd_tot_365_m_cnt_level']-dict_add['jd_riskPeriodConsuming']
    dict_add['jd_tot_365_m_cnt_level_ADD_jd_risk_pre_score']=dict_add['jd_tot_365_m_cnt_level']+dict_add['jd_risk_pre_score']
    dict_add['jd_tot_365_m_cnt_level_MIN_jd_credit_consume_level']=dict_add['jd_tot_365_m_cnt_level']-dict_add['jd_credit_consume_level']
    dict_add['jd_riskPeriodConsuming_DIV_jd_risk_pre_score']=dict_add['jd_riskPeriodConsuming']/dict_add['jd_risk_pre_score'] if dict_add['jd_risk_pre_score'] > 0 else np.nan
    dict_add['jd_riskPeriodConsuming_MUL_jd_tot_dlq_days_all_365_level']=dict_add['jd_riskPeriodConsuming']*dict_add['jd_tot_dlq_days_all_365_level']
    dict_add['jd_liability_level_DIV_jd_risk_pre_score']=dict_add['jd_liability_level']/dict_add['jd_risk_pre_score'] if dict_add['jd_risk_pre_score'] > 0 else np.nan
    dict_add['jd_liability_level_MUL_jd_credit_consume_level']=dict_add['jd_liability_level']*dict_add['jd_credit_consume_level']
    dict_add['jd_liability_level_MIN_jd_tot_30_amt_level']=dict_add['jd_liability_level']-dict_add['jd_tot_30_amt_level']
    dict_add['jd_tot_90_amt_level_ADD_jd_risk_pre_score']=dict_add['jd_tot_90_amt_level']+dict_add['jd_risk_pre_score']
    dict_add['jd_tot_90_amt_level_DIV_jd_risk_pre_score']=dict_add['jd_tot_90_amt_level']/dict_add['jd_risk_pre_score'] if dict_add['jd_risk_pre_score'] > 0 else np.nan
    dict_add['jd_tot_90_amt_level_MIN_jd_tot_30_amt_level']=dict_add['jd_tot_90_amt_level']-dict_add['jd_tot_30_amt_level']
    dict_add['jd_risk_pre_score_ADD_jd_credit_consume_level']=dict_add['jd_risk_pre_score']+dict_add['jd_credit_consume_level']
    dict_add['jd_risk_pre_score_ADD_jd_tot_30_amt_level']=dict_add['jd_risk_pre_score']+dict_add['jd_tot_30_amt_level']
    dict_add['jd_stability_MIN_jd_tot_30_amt_level']=dict_add['jd_stability']-dict_add['jd_tot_30_amt_level']
    dict_add['jd_stability_DIV_jd_tot_dlq_days_all_365_level']=dict_add['jd_stability']/dict_add['jd_tot_dlq_days_all_365_level'] if dict_add['jd_tot_dlq_days_all_365_level'] > 0 else np.nan
    dict_add['jd_credit_consume_level_MUL_jd_tot_30_amt_level']=dict_add['jd_credit_consume_level']*dict_add['jd_tot_30_amt_level']






    return dict_add