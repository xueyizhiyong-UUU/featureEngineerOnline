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

#高德详细地址转经纬度API
#申请的key,请大家自己申请哈，原来给了我的Ak，结果有人给我把一天的配额用完了
#ak='5ba9f811bdfa620cb8946cc833075155'
#传入地址，返回对应地址的经纬度信息
def address(address):
    url="http://restapi.amap.com/v3/geocode/geo?key=%s&address=%s"%('5ba9f811bdfa620cb8946cc833075155',address)
    data=requests.get(url).json()
    if data['info']=='OK':
        contest=data['geocodes'][0]
    else:
        contest={'formatted_address': [],
   'country': [],
   'province': [],
   'citycode': [],
   'city': [],
   'district': [],
   'township': [],
   'neighborhood': {'name': [], 'type': []},
   'building': {'name': [], 'type': []},
   'adcode': [],
   'street': [],
   'number': [],
   'location': [],
   'level': []}
    return contest

def address_1(address):
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




def row_address2gps(num,total,address_row,address_vars,prov=True,city=True,longi=True,lati=True):
    address_row=address_row.reset_index(drop=True)
    #地址转经纬度
    for var in address_vars:
        try:
            address_i=address(address_row.loc[0,var])
            if prov:
                address_row.loc[0,var+'_prov']=(len(address_i['province'])>0 and address_i['province'] or np.nan)
            if city:
                address_row.loc[0,var+'_city']=(len(address_i['city'])>0 and address_i['city'] or np.nan)
            if longi:
                address_row.loc[0,var+'_longi']=(len(address_i['location'])>0 and float(address_i['location'].split(',')[0]) or np.nan)
            if lati:
                address_row.loc[0,var+'_lati']=(len(address_i['location'])>0 and float(address_i['location'].split(',')[1]) or np.nan)
        except:
            if prov:
                address_row.loc[0,var+'_prov']=np.nan
            if city:
                address_row.loc[0,var+'_city']=np.nan
            if longi:
                address_row.loc[0,var+'_longi']=np.nan
            if lati:
                address_row.loc[0,var+'_lati']=np.nan
    return list([address_row,num,total])

def address2gps(address_table,address_vars,prov=True,city=True,longi=True,lati=True):
    address_table=address_table.reset_index(drop=True)
    combin_list=list()
    address_table_new =None

    total=len(address_table)


    for i in range(total):
        # print(i)
        list_a = row_address2gps(i,total,address_table.iloc[[i],],address_vars,prov,city,longi,lati,)
        address_table_new = pd.concat([address_table_new, list_a[0]])

    # address_table_new=pd.concat(combin_list)
    # print(address_table_new)
    return address_table_new

def address2gps2(address_table,address_vars,prov=True,city=True,longi=True,lati=True):
    address_table=address_table.reset_index(drop=True)
    combin_list=list()
    total=len(address_table)
    for i in range(total):
        address_row=address_table.iloc[[i],].reset_index(drop=True)
        for var in address_vars:
            try:
                address_i=address(address_row.loc[0,var])
                if prov:
                    address_row.loc[0,var+'_prov']=(len(address_i['province'])>0 and address_i['province'] or np.nan)
                if city:
                    address_row.loc[0,var+'_city']=(len(address_i['city'])>0 and address_i['city'] or np.nan)
                if longi:
                    address_row.loc[0,var+'_longi']=(len(address_i['location'])>0 and float(address_i['location'].split(',')[0]) or np.nan)
                if lati:
                    address_row.loc[0,var+'_lati']=(len(address_i['location'])>0 and float(address_i['location'].split(',')[1]) or np.nan)
            except:
                if prov:
                    address_row.loc[0,var+'_prov']=np.nan
                if city:
                    address_row.loc[0,var+'_city']=np.nan
                if longi:
                    address_row.loc[0,var+'_longi']=np.nan
                if lati:
                    address_row.loc[0,var+'_lati']=np.nan
        combin_list.append(address_row)
    address_table_new=pd.concat(combin_list)
    return address_table_new


#经纬度直线距离  返回Km
def distance_of_stations(point1, point2):
    long1, lat1 = point1
    long2, lat2 = point2
    delta_long = math.radians(long2 - long1)
    delta_lat  = math.radians(lat2 - lat1)
    s = 2 * math.asin(math.sqrt(math.pow(math.sin(delta_lat / 2), 2) + math.cos(math.radians(lat1)) * math.cos(math.radians(lat2)) * math.pow(math.sin(delta_long / 2), 2)))
    s = s * 6378.2
    return s


#经纬度转距离和时间
def origin_destination(origin,destination):
    url="https://restapi.amap.com/v3/direction/driving?origin=%s&destination=%s&strategy=0&extensions=all&output=json&key=%s"%(origin,destination,'5ba9f811bdfa620cb8946cc833075155')
    data=requests.get(url)
    contest=data.json()
    contest=contest['route']['paths'][0]
    return contest
def row_gps2distance(num,total,address_row,address_var1,address_var2):
    address_row=address_row.reset_index(drop=True)
    #经纬度转距离和时间
    try:
        contest=origin_destination(str(address_row.loc[0,address_var1+'_longi'])+','+str(address_row.loc[0,address_var1+'_lati']),
                             str(address_row.loc[0,address_var2+'_longi'])+','+str(address_row.loc[0,address_var2+'_lati']))
        address_row.loc[0,address_var1+'_2_'+address_var2+'_distance']=(float(contest['distance']) if contest['distance'] else np.nan)
        address_row.loc[0,address_var1+'_2_'+address_var2+'_time']=(float(contest['duration']) if contest['duration'] else np.nan)
        address_row.loc[0,address_var1+'_2_'+address_var2+'_traffic']=(float(contest['traffic_lights'])/float(contest['distance']) if contest['distance'] else np.nan)

    except:
        address_row.loc[0,address_var1+'_2_'+address_var2+'_distance']=np.nan
        address_row.loc[0,address_var1+'_2_'+address_var2+'_time']=np.nan
        address_row.loc[0,address_var1+'_2_'+address_var2+'_traffic']=np.nan
    return list([address_row,num,total])
def gps2distance(address_table,address_var1,address_var2):
    address_table=address_table.reset_index(drop=True)
    combin_list=list()
    def call_append (y):  # 回调函数
        num=y[1];total=y[2];
        f = num / total
        p = f*100
        r = '\r%s%d%%' %(min(round(p)+1,100)*'='+'>',min(round(p)+1,100))    #\r 光标返回行首
        sys.stdout.write(r)
        sys.stdout.flush()
        combin_list.append(y[0])
    if True: #if __name__ == "__main__"
        num_processes =4
        pool = multiprocessing.Pool(processes=num_processes)
        total=len(address_table)

        for i in range(total):
            pool.apply_async(row_gps2distance, args=(i,total,address_table.iloc[[i],],address_var1,address_var2,), callback=call_append)

        pool.close()
        pool.join()
    address_table_new=pd.concat(combin_list)
    return address_table_new

def row_gps2distance1(x,address_var1,address_var2,distance=True,time=False,traffic=False):
    #经纬度转距离和时间
    try:
        
        contest=origin_destination(str(x[address_var1+'_longi'])+','+str(x[address_var1+'_lati']),
                             str(x[address_var2+'_longi'])+','+str(x[address_var2+'_lati']))
        if distance:
            return (float(contest['distance']) if contest['distance'] else np.nan)
            
        elif time:
            return (float(contest['duration']) if contest['duration'] else np.nan)
        elif traffic:
            return (float(contest['traffic_lights'])/float(contest['distance']) if contest['distance'] else np.nan)

    except:
        if distance:
            return np.nan
        elif time:
            return np.nan
        elif traffic:
            return np.nan









def address2gps01(address_table,address_vars,prov=True,city=True,longi=True,lati=True):
    address_table=address_table.reset_index(drop=True)
    from tqdm import tqdm
    import time
    #地址转经纬度
    for var in address_vars:
        print(var)
        for i in tqdm(range(0,len(address_table))):
            try:
                address_i=address(address_table.loc[i,var])
                if prov:
                    address_table.loc[i,var+'_prov']=(len(address_i['province'])>0 and address_i['province'] or '')
                if city:
                    address_table.loc[i,var+'_city']=(len(address_i['city'])>0 and address_i['city'] or '')
                if longi:
                    address_table.loc[i,var+'_longi']=(len(address_i['location'])>0 and address_i['location'].split(',')[0] or '')
                if lati:
                    address_table.loc[i,var+'_lati']=(len(address_i['location'])>0 and address_i['location'].split(',')[1] or '')
            except:
                if prov:
                    address_table.loc[i,var+'_prov']=''
                if city:
                    address_table.loc[i,var+'_city']=''
                if longi:
                    address_table.loc[i,var+'_longi']=''
                if lati:
                    address_table.loc[i,var+'_lati']=''
                #print(i,'   messageAddress:',address_table.loc[i,'messageAddress'])
    return address_table

#手机号归属地API
#https://gitee.com/oss/phonedata/attach_files
#1.自研api(精确到地市,邮编等支持jsonp).已开源:https://my.oschina.net/xiaogg/blog/2990766
#https://tool.bitefu.net/shouji/?mobile=手机号码
#2.淘宝网（(精确到省份）
#API地址： http://tcc.taobao.com/cc/json/mobile_tel_segment.htm?tel=手机号码
#3.百度(精确到地市)
#API地址：http://mobsec-dianhua.baidu.com/dianhua_api/open/location?tel=手机号码
#4.百度(精确到地市)
#https://sp0.baidu.com/8aQDcjqpAAV3otqbppnN2DJv/api.php?resource_name=guishudi&query=手机号码
#5.sogou(精确到地市)
#https://www.sogou.com/websearch/phoneAddress.jsp?phoneNumber=手机号码




def getStrAsMD5(parmStr):
    #1、参数必须是utf8
    #2、python3所有字符都是unicode形式，已经不存在unicode关键字
    #3、python3 str 实质上就是unicode
    if isinstance(parmStr,str):
        # 如果是unicode先转utf-8
        parmStr=parmStr.encode("utf-8")
    m = hashlib.md5()
    m.update(parmStr)
    return m.hexdigest()



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
        startdate=re.sub('-'+sub1[0],'-0'+str(sub1[0]),startdate)  if len(sub1)>0 else startdate
        sub2=re.findall('-([0-9]{1}$)',startdate)
        startdate=re.sub('-'+sub2[0],'-0'+str(sub2[0]),startdate)  if len(sub2)>0 else startdate
        startdate=re.sub('[-/.]','',startdate)
        d1=datetime.datetime.strptime(startdate[0:10],'%Y%m%d')
    else :
        return np.nan
    
    enddate=re.findall(r'[1-9][0-9]{3} *?[0-9]{1,2} *?[0-9]{1,2}|[1-9][0-9]{3} *?[-/.] *?[0-9]{1,2} *?[-/.] *?[0-9]{1,2}',str(enddate))
    if len(enddate)>0:
        enddate=enddate[0]
        enddate=re.sub('[-/.]','-',re.sub(' ','',enddate))
        sub1=re.findall('-([0-9]{1}-)',enddate)
        enddate=re.sub('-'+sub1[0],'-0'+str(sub1[0]),enddate)  if len(sub1)>0 else enddate
        sub2=re.findall('-([0-9]{1}$)',enddate)
        enddate=re.sub('-'+sub2[0],'-0'+str(sub2[0]),enddate)  if len(sub2)>0 else enddate
        enddate=re.sub('[-/.]','',enddate)
        d2=datetime.datetime.strptime(enddate[0:10],'%Y%m%d')
    else :
        return np.nan
    return (d2-d1).days
#daysdelta('2019.1.30', '2019.3.30')

#########################################
# UTF8成ANSI
#########################################

import os
import codecs

def removeBom(file):
    '''移除UTF-8文件的BOM字节'''
    BOM=b'\xef\xbb\xbf'
    existBom = lambda s: True if s==BOM else False
    f = open(file, 'rb')
    if existBom(f.read(3)):
        fbody = f.read()
        #f.close()
        with open(file,'wb') as f:
            f.write(fbody)

#oldfile:UTF8文件的路径
#newfile:要保存的ANSI文件的路径
def convertUTF8ToANSI(oldfile,newfile):
    #打开UTF8文本文件
    f = codecs.open(oldfile,'r','utf8')
    utfstr = f.read()
    f.close()

    #把UTF8字符串转码成ANSI字符串
    outansestr = utfstr.encode('mbcs',errors = 'ignore')
    #加上ignore就会自动忽略无法编码的字符，否则遇到不能编码成ascii的字符就会抛异常干掉程序
    #需要的原因：部分utf8编码的字符无法转换为ascii，只能丢弃

    #使用二进制格式保存转码后的文本
    f = open(newfile,'wb')
    f.write(outansestr)
    f.close()
    
#########################################
# json转dataframe
#########################################

#import json
#from __future__ import print_function
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
def json_to_dict(json_str, pre=None):
    sValue = json.loads(json_str)
    tmp_dict=dict()
    for i in dict_generator(sValue, pre=None):
        tmp_dict.update({'.'.join(i[-2:-1]):i[-1]}) #选路径作为变量名（i[0:-1]):i[-1] 为完整路径）
        #print('.'.join(i[0:-1]), ':', i[-1])
    return tmp_dict
#from itertools import chain
def datajson_to_frame(data_json,json_var_name):
    data=copy.copy(data_json)
    var_name=json_var_name
    data['columns'] = data[str(var_name)].map(lambda x:list(json_to_dict(x).keys()))       #新增一列'columns'用于存储每一列的json串的字段名
    add_columns_list =list(set( list(chain(*data['columns'] ))))                         #获取json串中的所有字段名称
    for columns in add_columns_list:
        data[str(columns)] = data[str(var_name)].map(lambda x:json_to_dict(x).get(str(columns)))  #将json串展开
    del data['columns']
    return data


#########################################
# 手机号信息验证和提取
#########################################
def phone_num_class(mobile_4):
    #8"*AAAA" 尾数4连号，尾号不等于4; 
    #7" *000、*666、*888 、*999 、*4444"
    #6" *1588 ，*1688 、*AAA 、*A8A8 、*88AA 、*8A8A 、*AA88" A 不等4; X、 Y 不相同; 
    #5" *0001 、*444 、*AB88、*88AB、*AABB 、*888A 、*A888 、*AAA8、*8AAA'" A 不等于B; A 、B 均不等于4
    #4" *ABCD 、*X168 、*X158 、*X518 、*8YY8"  ABCD 为累加数或递减数、X、Y 均不等于4
    #3" *ABBA、*AAAB 、*AB99 、*ABAB"  A、B、C 均不等于4
    #2" *ABC 、*XX 、*X88Y 、*8"  ABC 为累加数或递减数 X不等于Y；X、Y 均不等于4
    #1" 普通号 " 除以上级别号外，号码最后三位均不等于4 的号码
    #0" 差号 " 除以上级如同号外，号码最后三位中任意一位等于4 的号
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

#########################################
# 身份证号信息验证和提取
#########################################

'''
python判断真假身份证号
识别一串身份证是否是真实的身份证号码: 公民身份号码是特征组合码，共18位，由十七位数字本体码和一位数字校验码组成。 排列顺序从左至右依次为：六位数字地址码，八位数字出生日期码，三位数字顺序码和一位数字校验码。
作为尾号的校验码，是由号码编制单位按统一的公式计算出来的。 身份证第18位（校验码）的计算方法:
1、将前面的身份证号码17位数分别乘以不同的 系数。 从第一位到第十七位的系数分别为： [7,9,10,5,8,4,2,1,6,3,7,9,10,5,8,4,2]。
2、将身份证前17位数字和系数相乘的结果相加。 
3、用加出来和除以11，看余数是多少？ 
4、余数只可能有[0,1,2,3,4,5,6,7,8,9,10]这11个数字。 其分别对应的最后 一位身份证的号码为 [1,0,X,9,8,7,6,5,4,3,2]。 
5、通过上面得知如果余数是2，就会在身份证的第18位数字上出现罗马数字的Ⅹ。如果余数是10，身份证的最后一位号码是2。
实验身份证号：'110000198003198182'/'440204199406184727'
'''
def is_ture_id(id):
    id=str(id)
    if len(id) == 18:   #校验省份证长度是否是18位
        num17 = id[0:17]
        last_num = id[-1]  #截取前17位和最后一位
        moduls = [7,9,10,5,8,4,2,1,6,3,7,9,10,5,8,4,2]
        num17 = map(int,num17)
        num_tuple = zip(num17,moduls)  # [(1, 4), (2, 5), (3, 6)]
        num = map(lambda x:x[0]*x[1],num_tuple)
        mod = sum(num)%11
        yushu1 = [0,1,2,3,4,5,6,7,8,9,10]
        yushu2 = [1,0,'X',9,8,7,6,5,4,3,2]
        last_yushu = dict(zip(yushu1,yushu2))
        if last_num == str(last_yushu[mod]):
            return True
        else:
            return False
    else:
        return False
    
class GetIdInformation(object):

    def __init__(self,id):
        self.id = id
        self.birth_year = int(self.id[6:10])
        self.birth_month = int(self.id[10:12])
        self.birth_day = int(self.id[12:14])
        self.address_code = int(self.id[0:6])

    def get_birthday(self):
        """通过身份证号获取出生日期"""
        birthday = "{0}-{1}-{2}".format(self.birth_year, self.birth_month, self.birth_day)
        return birthday

    def get_sex(self):
        """男生：1 女生：0"""
        num = int(self.id[16:17])
        if num % 2 == 0:
            return 0
        else:
            return 1

    def get_age(self):
        """通过身份证号获取年龄"""
        now = (datetime.datetime.now() + datetime.timedelta(days=1))
        year = now.year
        month = now.month
        day = now.day

        if year == self.birth_year:
            return 0
        else:
            if self.birth_month > month or (self.birth_month == month and self.birth_day > day):
                return year - self.birth_year - 1
            else:
                return year - self.birth_year


    def get_zodiac(self):  
        """通过身份证号获取生肖"""  
        start_year = 1900
        zodiac_interval = (int(self.birth_year) - start_year) % 12
        if zodiac_interval == 0 or zodiac_interval == -12:
            return '鼠'
        if zodiac_interval == 1 or zodiac_interval == -11:
            return '牛'
        if zodiac_interval == 2 or zodiac_interval == -10:
            return '虎'
        if zodiac_interval == 3 or zodiac_interval == -9:
            return '兔'
        if zodiac_interval == 4 or zodiac_interval == -8:
            return '龙'
        if zodiac_interval == 5 or zodiac_interval == -7:
            return '蛇'
        if zodiac_interval == 6 or zodiac_interval == -6:
            return '马'
        if zodiac_interval == 7 or zodiac_interval == -5:
            return '羊'
        if zodiac_interval == 8 or zodiac_interval == -4:
            return '猴'
        if zodiac_interval == 9 or zodiac_interval == -3:
            return '鸡'
        if zodiac_interval == 10 or zodiac_interval == -2:
            return '狗'
        if zodiac_interval == 11 or zodiac_interval == -1:
            return '猪'
        else:
            return np.nan

    def get_starsign(self):  
        """通过身份证号获取星座"""    
        if((self.birth_month== 1 and self.birth_day > 19) or (self.birth_month== 2 and self.birth_day <= 18)):
            return "水瓶座"
        if ((self.birth_month== 2 and self.birth_day > 18) or (self.birth_month== 3 and self.birth_day <= 20)):
            return "双鱼座"
        if ((self.birth_month== 3 and self.birth_day > 20) or (self.birth_month== 4 and self.birth_day <= 19)):
            return "白羊座"
        if ((self.birth_month== 4 and self.birth_day > 19) or (self.birth_month== 5 and self.birth_day <= 20)):
            return "金牛座"
        if ((self.birth_month== 5 and self.birth_day > 20) or (self.birth_month== 6 and self.birth_day <= 21)):
            return "双子座"
        if ((self.birth_month== 6 and self.birth_day > 21) or (self.birth_month== 7 and self.birth_day <= 22)):
            return "巨蟹座"
        if ((self.birth_month== 7 and self.birth_day > 22) or (self.birth_month== 8 and self.birth_day <= 22)):
            return "狮子座"
        if ((self.birth_month== 8 and self.birth_day > 22) or (self.birth_month== 9 and self.birth_day <= 22)):
            return "处女座"
        if ((self.birth_month== 9 and self.birth_day > 22) or (self.birth_month== 10 and self.birth_day <= 23)):
            return "天秤座"
        if ((self.birth_month== 10 and self.birth_day > 23) or (self.birth_month== 11 and self.birth_day <= 22)):
            return "天蝎座"
        if ((self.birth_month== 11 and self.birth_day > 22) or (self.birth_month== 12 and self.birth_day <= 21)):
            return "射手座"
        if ((self.birth_month== 12 and self.birth_day > 21) or (self.birth_month== 1 and self.birth_day <= 19)):
            return "魔羯座"
        else:
            return np.nan 

    def get_province_only(self):  
        """通过身份证号获取省份"""    
        province_code = int(self.id[0:2])
        province_dict = {
        11 : "北京",
        12 : "天津",
        13 : "河北",
        14 : "山西",
        15 : "内蒙古",
        21 : "辽宁",
        22 : "吉林",
        23 : "黑龙江",
        31 : "上海",
        32 : "江苏",
        33 : "浙江",
        34 : "安徽",
        35 : "福建",
        36 : "江西",
        37 : "山东",
        41 : "河南",
        42 : "湖北",
        43 : "湖南",
        44 : "广东",
        45 : "广西",
        46 : "海南",
        50 : "重庆",
        51 : "四川",
        52 : "贵州",
        53 : "云南",
        54 : "西藏",
        61 : "陕西",
        62 : "甘肃",
        63 : "青海",
        64 : "宁夏",
        65 : "新疆",
        71 : "台湾",
        81 : "香港",
        82 : "澳门",
        91 : "国外"
        }
        if province_dict[province_code] is not None:
            return province_dict[province_code]
        else:
            return np.nan



    def get_province(self):  
        """通过身份证号获取省份"""    
        province_dict = area_info.to_dict()['province']

        if self.address_code in  list(province_dict.keys()):
            return province_dict[self.address_code]
        else:
            return np.nan
        



    def get_city(self):  
        """通过身份证号获取城市"""    
        city_dict = area_info.to_dict()['city']

        if self.address_code in  list(city_dict.keys()):
            return city_dict[self.address_code]
        else:
            return np.nan
        


    def get_district(self):  
        """通过身份证号获取区/县"""    
        district_dict = area_info.to_dict()['district']
        
        if self.address_code in  list(district_dict.keys()):
            return district_dict[self.address_code]
        else:
            return np.nan
        
        


    def get_address(self):  
        """通过身份证号获取完整归属地"""    
        address_dict = area_info.to_dict()['all']
        
        if self.address_code in  list(address_dict.keys()):
            return address_dict[self.address_code]
        else:
            return np.nan

###########################################
# 概论转分数  分数转决策表
###########################################        
        
def p_to_score(p, PDO=75.0, Base=660, Ratio=1.0/15.0):
    """
    逾期概率转换分数

    Args:
    p (float): 逾期概率
    PDO (float): points double odds. default = 75
    Base (int): base points. default = 660
    Ratio (float): bad:good ratio. default = 1.0/15.0

    Returns:
    化整后的模型分数
    """
    B = 1.0*PDO/math.log(2)
    A = Base+B*math.log(Ratio)
    score=A-B*math.log(p/(1-p))
    return round(score,0)

def score_to_p(score, PDO=75.0, Base=660, Ratio=1.0/15.0):
    """
    分数转换逾期概率

    Args:
    score (float): 模型分数
    PDO (float): points double odds. default = 75
    Base (int): base points. default = 660
    Ratio (float): bad:good ratio. default = 1.0/15.0

    Returns:
    转化后的逾期概率
    """
    B = 1.0*PDO/math.log(2)
    A = Base+B*math.log(Ratio)
    alpha = (A - score) / B
    p = np.exp(alpha) / (1+np.exp(alpha))
    return p


def calculate_ks_by_decile(score, y, job, q=10, score_bin_size = 25, manual_cut_bounds=[], score_type='raw'):
    """
    可同时计算decile analysis和Runbook analysis

    Args:
    score (pd.Series): 计算好的模型分
    y (pd.Series): 逾期事件label
    job (str): ['decile', 'runbook'], decile时，将会把score平分成q份， runkbook时，
    将会把平分分成25分一档的区间。有一种runbook是decile时，q=20
    q (int): default = 10, 将score分成等分的数量
    manual_cut_bounds (list): default = [], 当需要手动切分箱的时候，可以将分箱的bounds
        传入。
    score_type (str): ['raw', 'binned']. default='raw'. if 'raw', 传入的 score 是原始分数, if 'binned',传入的是分箱好的数据

    Returns:
    r (pd.DataFrame): 按照score分箱计算的EventRate, CumBadPct, CumGoodPct等，用来
    放置于model evaluation结果里面。
    """
    if score_type == 'raw':
        score = np.round(score)
        if job == 'decile':
            if len(manual_cut_bounds) == 0:
                decile_score = pd.qcut(score, q=q, duplicates='drop', precision=0).astype(str) #, labels=range(1,11))
            else:
                decile_score = pd.cut(score, manual_cut_bounds, precision=0).astype(str)

        if job == 'runbook':
            if len(manual_cut_bounds) == 0:
                min_score = int(np.round(min(score)))
                max_score = int(np.round(max(score)))
                score_bin_bounardies = list(range(min_score, max_score, score_bin_size))
                score_bin_bounardies[0] = min_score - 0.001
                score_bin_bounardies[-1] = max_score
                decile_score = pd.cut(score, score_bin_bounardies, precision=0).astype(str)
            else:
                decile_score = pd.cut(score, manual_cut_bounds, precision=0).astype(str)
    else:
        decile_score = score.astype(str)

    r = pd.crosstab(decile_score, y).rename(columns={0: 'N_nonEvent', 1: 'N_Event'})
    if 'N_Event' not in r.columns:
        r.loc[:, 'N_Event'] = 0
    if 'N_nonEvent' not in r.columns:
        r.loc[:, 'N_nonEvent'] = 0
    r.index.name = None
    r['rank'] = r.index
    r['rank'] = r['rank'].apply(lambda x:float(x[1:].split(',')[0]))
    r.sort_values(by='rank',inplace=True)
    r.drop(['rank'],1,inplace=True)
    r.loc[:, 'N_sample'] = decile_score.value_counts()
    r.loc[:, 'EventRate'] = r.N_Event * 1.0 / r.N_sample
    r.loc[:, 'Distribution'] = r.N_sample * 1.0 / r.N_sample.sum()
    r.loc[:, 'BadPct'] = r.N_Event * 1.0 / sum(r.N_Event)
    r.loc[:, 'GoodPct'] = r.N_nonEvent * 1.0 / sum(r.N_nonEvent)
    r.loc[:, 'CumBadPct'] = r.BadPct.cumsum()
    r.loc[:, 'CumGoodPct'] = r.GoodPct.cumsum()
    r.loc[:, 'KS'] = np.round(r.CumBadPct - r.CumGoodPct, 4)
    r.loc[:, 'odds(good:bad)'] = np.round(r.N_nonEvent*1.0 / r.N_Event, 1)
    r = r.reset_index().rename(columns={'index': '分箱',
                                        'N_sample': '样本数',
                                        'N_nonEvent': '好样本数',
                                        'N_Event': '坏样本数',
                                        'EventRate': '逾期率',
                                        'Distribution':'分布占比',
                                        'BadPct': 'Bad分布占比',
                                        'GoodPct': 'Good分布占比',
                                        'CumBadPct': '累积Bad占比',
                                        'CumGoodPct': '累积Good占比'
                                        })
    reorder_cols = ['分箱', '样本数', '好样本数', '坏样本数', '逾期率','分布占比',\
                    'Bad分布占比', 'Good分布占比', '累积Bad占比', '累积Good占比',\
                    'KS', 'odds(good:bad)']
    result = r[reorder_cols]
    result.loc[:, '分箱'] = result.loc[:, '分箱'].astype(str)
    return result        
        
        
        
        
        


###########################################
# 3.0 根据最优分箱生成WOE数据集主函数
###########################################
#' apply_mapping
#' 映射成第几箱及WOE数据集
#' 应用分箱的MAP映射数据集在原数据集上加一列第几箱的变量(例如_B结尾),和一列WOE的变量(例如_WOE结尾)
#' 输入为：
#' df           ：原数据集，包含目标变量y和若干自变量变量x.
#' x            ：需要处理的变量.
#' maps         ：optimal_binning分箱后产生的MAP映射数据集
#' NewVarBin    ：存放12345...第几箱的变量名后缀，默认原变量加_B结尾，如果为NULL则不生成变量
#' NewVarWOE    ：存放WOE的变量名后后缀，默认原变量加_WOE结尾，如果为NULL则不生成变量
#' 
#' 输出为：
#' df_all         ：在原数据集上加第几箱的变量，和WOE的变量
#' woe_var         ：woe变量
#'
#' example:
#' library(scorecard)
#' data(germancredit)
#' tmp=data_cleaning(df=germancredit,y='creditability')
#' df_list = data_splitting(df=tmp$df, y="creditability")
#' train = df_list$train
#' test = df_list$test
#' opt_bin=optimal_binning(df=train,y='creditability',x=NULL,Method=4,MMax=7,MinPercent=0.01,Mono=2,Acc=NULL,Disc=1)
#' train_woe=apply_mapping(df=train,y='creditability',maps=opt_bin$mapping,NewVarBin='_B',NewVarWOE='_WOE')
#' test_woe=apply_mapping(df=test,y='creditability',maps=opt_bin$mapping,NewVarBin='_B',NewVarWOE='_WOE')
#' 
def apply_mapping(df,maps,NewVarBin='_B',NewVarWOE='_WOE',NewVarScore=np.nan):
    temp_df=pd.DataFrame(df).copy()
    # 2.1.2.找出所有需要映射的连续变量和分类变量
    Continuous_vars=list(set(maps[maps.var_type=='continuous']['var_name'])  & set(temp_df.columns))
    Categorical_vars=list(set(maps[maps.var_type=='categorical']['var_name'])  & set(temp_df.columns))
    # 2.1.3.遍历所有连续变量
    df_out_cont=temp_df.copy()
    df_out_cont_woe=temp_df.copy()
    if len(Continuous_vars)>=1:    
        for i in range(0,len(Continuous_vars)):
            NewVarBin_p=(np.nan if pd.isnull(NewVarBin) else Continuous_vars[i]+NewVarBin)
            NewVarWOE_p=(np.nan if pd.isnull(NewVarWOE) else Continuous_vars[i]+NewVarWOE)
            NewVarScore_p=(np.nan if pd.isnull(NewVarScore) else Continuous_vars[i]+NewVarScore)
            df_out_cont[Continuous_vars[i]]=df_out_cont.apply(lambda x:float(x[Continuous_vars[i]]) if len(re.findall('^[0-9.eE-]*$',str(x[Continuous_vars[i]])))>0 else np.nan,axis=1)
            df_out_cont=ApplyMap_cont(DSin=df_out_cont, VarX=Continuous_vars[i], DSVarMap=maps, 
                                NewVarBin=NewVarBin_p, 
                                NewVarWOE=NewVarWOE_p,
                                NewVarScore=NewVarScore_p)
#     else:
#         print('Calculate WOE : no continuous varibles!')

    # 2.1.4.遍历所有分类变量
    df_out_cont_cat=df_out_cont.copy()
    if len(Categorical_vars)>=1:    
        for i in range(0,len(Categorical_vars)):
            NewVarBin_p=(np.nan if pd.isnull(NewVarBin) else Categorical_vars[i]+NewVarBin)
            NewVarWOE_p=(np.nan if pd.isnull(NewVarWOE) else Categorical_vars[i]+NewVarWOE)
            NewVarScore_p=(np.nan if pd.isnull(NewVarScore) else Categorical_vars[i]+NewVarScore)
            df_out_cont_cat[Categorical_vars[i]]=df_out_cont_cat[Categorical_vars[i]].astype(str)
            df_out_cont_cat=ApplyMap_cat(DSin=df_out_cont_cat, VarX=Categorical_vars[i], DSVarMap=maps, 
                                NewVarBin=NewVarBin_p, 
                                NewVarWOE=NewVarWOE_p,
                                NewVarScore=NewVarScore_p)
#     else:
#         print('Calculate WOE : no categorical varibles!')
    #woe_vars=[x for x in df_out_cont_cat.columns  if len(re.findall(NewVarWOE+'$',x))>0 ]

    return df_out_cont_cat
###########################################
# 3.1 连续变量映射成WOE数据集函数
###########################################
#' ApplyMap_cont 
#' 连续变量映射成第几箱
#' 应用分箱的MAP映射数据集在原数据集上加一列第几箱的变量(例如_B结尾),和一列WOE的变量(例如_WOE结尾)
#' 输入为：
#' DSin         ：原数据集，包含目标变量y和若干自变量变量x.
#' VarX         : 需要处理的变量.
#' DSVarMap     ：contbinning分箱后产生的MAP映射数据集
#' NewVarBin    ：存放12345...第几箱的变量名，例如原变量加_B结尾(可以为空)
#' NewVarWOE    ：存放WOE的变量名，例如原变量加_WOE结尾(可以为空)
#' 
#' 输出为：
#' DSin         ：在原数据集上加一列第几箱的变量，和一列WOE的变量
#' @examples
#' 
#' 
def ApplyMap_cont(DSin, VarX, DSVarMap, NewVarBin,NewVarWOE,NewVarScore=np.nan):
    DSVarMap=DSVarMap[DSVarMap['var_name']==VarX].copy()
    DSVarMap=DSVarMap.reset_index(drop=True)
    if len(DSVarMap)<2:
        print('The varible ',VarX,' is not in the mapping dataset.')
        return DSin
    # Applying a mapping scheme; to be used with function contbinning
    tempds=DSin[[VarX]].copy()
    # Generating variable to replace the cetgories with their bins 
    if pd.notnull(NewVarBin):
        tempds[NewVarBin]=np.nan
        for i in range(0,max(DSVarMap.bin)):
            if pd.isnull(DSVarMap.bin_limit[i]):
                tempds.loc[tempds[pd.isnull(tempds[VarX])].index,NewVarBin]=DSVarMap.bin[i]
            else:
                irows=(tempds[VarX] >=(float("-inf") if len(re.findall('-Inf,',str(DSVarMap.bin_limit[i])))>0 else DSVarMap.Bin_LowerLimit[i])) &  (tempds[VarX] <=(float("inf") if len(re.findall('Inf\)',str(DSVarMap.bin_limit[i])))>0  else DSVarMap.Bin_UpperLimit[i]))
                tempds.loc[tempds[irows].index,NewVarBin]=DSVarMap.bin[i]
            #如果NA和数字合并到了一组，则把NA放到这一组的范围里
            if len(re.findall("NA",str(DSVarMap.bin_limit[i])))>0:
                tempds.loc[tempds[pd.isnull(tempds[VarX])].index,NewVarBin]=DSVarMap.bin[i]
        DSin[NewVarBin]=tempds[NewVarBin]
    if pd.notnull(NewVarWOE):
        tempds[NewVarWOE]=np.nan
        for i in range(0,max(DSVarMap.bin)):
            if pd.isnull(DSVarMap.bin_limit[i]):
                tempds.loc[tempds[pd.isnull(tempds[VarX])].index,NewVarWOE]=DSVarMap.woe[i]
            else:
                irows=(tempds[VarX] >=(float("-inf") if len(re.findall('-Inf,',str(DSVarMap.bin_limit[i])))>0 else DSVarMap.Bin_LowerLimit[i])) &  (tempds[VarX] <=(float("inf") if len(re.findall('Inf\)',str(DSVarMap.bin_limit[i])))>0  else DSVarMap.Bin_UpperLimit[i]))
                tempds.loc[tempds[irows].index,NewVarWOE]=DSVarMap.woe[i]
            #如果NA和数字合并到了一组，则把NA放到这一组的范围里
            if len(re.findall("NA",str(DSVarMap.bin_limit[i])))>0:
                tempds.loc[tempds[pd.isnull(tempds[VarX])].index,NewVarWOE]=DSVarMap.woe[i]
        DSin[NewVarWOE]=tempds[NewVarWOE]
    if pd.notnull(NewVarScore):
        tempds[NewVarWOE]=np.nan
        for i in range(0,max(DSVarMap.bin)):
            if pd.isnull(DSVarMap.bin_limit[i]):
                tempds.loc[tempds[pd.isnull(tempds[VarX])].index,NewVarScore]=DSVarMap.score[i]
            else:
                irows=(tempds[VarX] >=(float("-inf") if len(re.findall('-Inf,',str(DSVarMap.bin_limit[i])))>0 else DSVarMap.Bin_LowerLimit[i])) &  (tempds[VarX] <=(float("inf") if len(re.findall('Inf\)',str(DSVarMap.bin_limit[i])))>0  else DSVarMap.Bin_UpperLimit[i]))
                tempds.loc[tempds[irows].index,NewVarScore]=DSVarMap.score[i]
            #如果NA和数字合并到了一组，则把NA放到这一组的范围里
            if len(re.findall("NA",str(DSVarMap.bin_limit[i])))>0:
                tempds.loc[tempds[pd.isnull(tempds[VarX])].index,NewVarScore]=DSVarMap.score[i]
        DSin[NewVarScore]=tempds[NewVarScore]
    return DSin

###########################################
# 3.2 分类变量映射成WOE函数
###########################################
#' ApplyMap_cat 
#' 分类变量映射成第几箱
#' 应用分箱的MAP映射数据集在原数据集上加一列第几箱的变量(例如_B结尾),和一列WOE的变量(例如_WOE结尾)
#' 输入为：
#' DSin         ：原数据集，包含目标变量y和若干自变量变量x.
#' VarX         : 需要处理的变量.
#' DSVarMap     ：catbinning分箱后产生的MAP映射数据集
#' NewVarBin    ：存放12345...第几箱的变量名，例如原变量加_B结尾(可以为空)
#' NewVarWOE    ：存放WOE的变量名，例如原变量加_WOE结尾(可以为空)
#' 
#' 输出为：
#' DSin         ：在原数据集上加一列第几箱的变量，和一列WOE的变量
#' 
#' @examples  credit.history
#' library(scorecard)
#' data(germancredit)
#' tmp=data_cleaning(df=germancredit,y='creditability')
#' tmp_bin=catbinning(df=tmp$df,y='creditability',x='credit.history',Method=4,MMax=7,MinPercent=0.01,Mono=1)
#' tmp_df=ApplyMap_cat(DSin=tmp$df,VarX='credit.history',DSVarMap=tmp_bin,NewVarBin='credit.history_B',NewVarWOE='credit.history_WOE')
#' tmp_df[,grep(pattern = '^credit.history',colnames(tmp_df))]
#' 
#' ApplyMap_cat(DSin=df_out_cont_cat, VarX=Categorical_vars[i], DSVarMap=maps, NewVarBin=ifelse(is.null(NewVarBin),NULL,paste0(Categorical_vars[i],NewVarBin)), NewVarWOE=ifelse(is.null(NewVarWOE),NULL,paste0(Categorical_vars[i],NewVarWOE)))

#' 
def ApplyMap_cat(DSin, VarX, DSVarMap, NewVarBin,NewVarWOE,NewVarScore=np.nan):
    DSVarMap=DSVarMap[DSVarMap['var_name']==VarX].copy()
    DSVarMap=DSVarMap.reset_index(drop=True)
    if len(DSVarMap)<2:
        print('The varible ',VarX,' is not in the mapping dataset.')
        return DSin
    # Applying a mapping scheme; to be used with function contbinning
    tempds=DSin[[VarX]].copy()
    # Generating variable to replace the cetgories with their bins 

    if pd.notnull(NewVarBin):
        tempds[NewVarBin]=np.nan
        for i in range(0,max(DSVarMap.bin)):
            if pd.isnull(DSVarMap.bin_limit[i]) | len(re.findall("^NA *,|, *NA$|, *NA *,",str(DSVarMap.bin_limit[i])))>0:
                tempds.loc[tempds[pd.isnull(tempds[VarX])].index,NewVarBin]=DSVarMap.bin.iloc[i]
            irows=tempds[VarX].apply(lambda x:x.strip() in [y.strip() for y in str(DSVarMap.bin_limit[i]).split(',')] ) 
            tempds.loc[tempds[irows].index,NewVarBin]=DSVarMap.bin.iloc[i]
        #把映射文件中没有出现的类别归到最多的一个组
        if sum(pd.isnull(tempds[NewVarBin]))>0:
            max_bin=DSVarMap[DSVarMap.n_accts==max(DSVarMap.n_accts)]['bin'].values[0]
            tempds.loc[tempds[pd.isnull(tempds[NewVarBin])].index,NewVarBin]=max_bin
        DSin[NewVarBin]=tempds[NewVarBin]
    if pd.notnull(NewVarWOE):
        tempds[NewVarWOE]=np.nan
        for i in range(0,max(DSVarMap.bin)):
            if pd.isnull(DSVarMap.bin_limit[i]) | len(re.findall("^NA *,|, *NA$|, *NA *,",str(DSVarMap.bin_limit[i])))>0:
                tempds.loc[tempds[pd.isnull(tempds[VarX])].index,NewVarWOE]=DSVarMap.woe.iloc[i]
            irows=tempds[VarX].apply(lambda x:x.strip() in [y.strip() for y in str(DSVarMap.bin_limit[i]).split(',')] ) 
            tempds.loc[tempds[irows].index,NewVarWOE]=DSVarMap.woe.iloc[i]
        #把映射文件中没有出现的类别归到最多的一个组
        if sum(pd.isnull(tempds[NewVarWOE]))>0:
            max_bin_woe=DSVarMap[DSVarMap.n_accts==max(DSVarMap.n_accts)]['woe'].values[0]
            tempds.loc[tempds[pd.isnull(tempds[NewVarWOE])].index,NewVarWOE]=max_bin_woe
        DSin[NewVarWOE]=tempds[NewVarWOE]    
    if pd.notnull(NewVarScore):
        tempds[NewVarScore]=np.nan
        for i in range(0,max(DSVarMap.bin)):
            if pd.isnull(DSVarMap.bin_limit[i]) | len(re.findall("^NA *,|, *NA$|, *NA *,",str(DSVarMap.bin_limit[i])))>0:
                tempds.loc[tempds[pd.isnull(tempds[VarX])].index,NewVarScore]=DSVarMap.score.iloc[i]
            irows=tempds[VarX].apply(lambda x:x.strip() in [y.strip() for y in str(DSVarMap.bin_limit[i]).split(',')] ) 
            tempds.loc[tempds[irows].index,NewVarScore]=DSVarMap.score.iloc[i]
        #把映射文件中没有出现的类别归到最多的一个组
        if sum(pd.isnull(tempds[NewVarScore]))>0:
            max_bin_score=DSVarMap[DSVarMap.n_accts==max(DSVarMap.n_accts)]['score'].values[0]
            tempds.loc[tempds[pd.isnull(tempds[NewVarScore])].index,NewVarScore]=max_bin_score
        DSin[NewVarScore]=tempds[NewVarScore]  
    return DSin