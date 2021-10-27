#!/usr/bin/python
# -*- coding: UTF-8 -*-

"""
@file:feature_merge.py
@Function:
"""
import json
from model_utils.bh_feature import *
from model_utils.rh_feature import *
from model_utils.customerlog_feature import *
from model_utils.customerlog_feature_utils import *
from model_utils.hd_feature import *
from model_utils.br_feature import *
from model_utils.xy_feature import *
from model_utils.jd_feature import *
from model_utils.jd_feature_utils import *




def get_all_raw_data_offline(loan_id):
    model_data_loan_one = read_data('model_data_loan_one', './data/raw_data/')

    model_data_loan_one['apply_info'] = model_data_loan_one['resultData'].apply(lambda x: resultData_analyze(x, 'apply_info'))
    model_data_loan_one['pboc_info'] = model_data_loan_one['resultData'].apply(lambda x: resultData_analyze(x, 'pboc_info'))
    model_data_loan_one['baihang_info'] = model_data_loan_one['resultData'].apply(lambda x: resultData_analyze(x, 'baihang_info'))

    return model_data_loan_one




def handle_local(loanNo):
    data = {'loanNos': loanNo}
    headers = {'content-type': 'application/json',
               'User-Agent': 'Mozilla/5.0 (X11; Ubuntu; Linux x86_64; rv:22.0) Gecko/20100101 Firefox/22.0'}

    r = requests.post('http://eigen.test.qnvipfs.com/test/model', data=data, timeout=5)
    status_code = r.status_code
    return status_code






def handle_local(loanNo):
    data = {'loanNos': loanNo}
    headers = {'content-type': 'application/json',
               'User-Agent': 'Mozilla/5.0 (X11; Ubuntu; Linux x86_64; rv:22.0) Gecko/20100101 Firefox/22.0'}

    r = requests.post('http://eigen.test.qnvipfs.com/test/customerLog', data=data, timeout=5)
    status_code = r.status_code
    return status_code


def deal_json_to_dict(new_baihang_info):
    resultData = {}
    loan_id = new_baihang_info['apply_info']['loanNo']
    order_time = new_baihang_info['apply_info']['loan_time']

    # apply_info
    if 'apply_info' in new_baihang_info:
        # resultData['apply_info'] = json.dumps(new_baihang_info['apply_info'], ensure_ascii=False)
        apply_info = json.dumps(new_baihang_info['apply_info'], ensure_ascii=False)
    else:
        apply_info = np.nan


    # mongoInfo
    if 'mongoInfo' in  new_baihang_info:
        mongoInfo = json.dumps(new_baihang_info['mongoInfo'], ensure_ascii=False)
    else:
        mongoInfo = np.nan

    # baihang_info
    if 'baihang_info' in new_baihang_info:
        baihang_info = json.dumps(new_baihang_info['baihang_info'], ensure_ascii=False)
    else:
        baihang_info = np.nan

    # pboc_info
    if 'pboc_info' in new_baihang_info:
        pboc_info =json.dumps(new_baihang_info['pboc_info'], ensure_ascii=False)
    else:
        pboc_info = np.nan

    # huadao_respBody
    if 'huadao_respBody' in new_baihang_info:
        huadao_info = json.dumps(new_baihang_info['huadao_respBody'], ensure_ascii=False)
    else:
        huadao_info = np.nan

    # jingdong_respBody
    if 'jingdong_respBody' in new_baihang_info:
        jingdong_info = json.dumps(new_baihang_info['jingdong_respBody'], ensure_ascii=False)
    else:
        jingdong_info = np.nan

    # xinyan_respBody
    if 'xinyan_respBody' in new_baihang_info:
        xinyan_info = json.dumps(new_baihang_info['xinyan_respBody'], ensure_ascii=False)
    else:
        xinyan_info = np.nan

    # bairong_info
    if 'bairong_info' in new_baihang_info:
        bairong_info = json.dumps(new_baihang_info['bairong_info'], ensure_ascii=False)
    else:
        bairong_info = np.nan

    # resultData = json.dumps(resultData, ensure_ascii=False)
    df_dict = {'loan_id': loan_id, 'order_time':order_time,
               'apply_info':apply_info, 'mongoInfo':mongoInfo,
               'baihang_info':baihang_info, 'pboc_info':pboc_info,
               'huadao_info':huadao_info, 'jingdong_info':jingdong_info,
               'xinyan_info':xinyan_info, 'bairong_info':bairong_info}
    df = pd.DataFrame.from_dict(df_dict, orient='index').T
    return df

# 根据订单获取数据包
def handle_local_online(request_body):
    # url = 'http://eigen.test.qnvipfs.com/test/customerLog?loanNo={}'.format(str(loanNo))
    # payload = {}
    # headers = {}
    # response = requests.request("POST", url, headers=headers, data=payload)
    # content = json.loads(response.text)
    # save_pickle(content, 'test_json_data','./data/pkl/')
    content = request_body
    content_df = deal_json_to_dict(content)
    return content_df



def get_all_raw_data_online(loan_id):
    model_data_loan_one = handle_local_online(loan_id)
    return model_data_loan_one


def get_bh_data(all_raw_data):
    all_raw_data_one = all_raw_data[['loan_id', 'baihang_info', 'apply_info', 'order_time']]
    return all_raw_data_one


def get_rh_data(all_raw_data):
    all_raw_data_one = all_raw_data[['loan_id', 'pboc_info', 'apply_info', 'order_time']]
    return all_raw_data_one

# mongoInfo
def get_mongoInfo_raw_data(all_raw_data):
    # 选取目标数据
    all_raw_data_one = all_raw_data[['loan_id', 'order_time', 'mongoInfo']]
    all_raw_data_one = all_raw_data_one[all_raw_data_one['mongoInfo'].notnull()]
    if all_raw_data_one.shape[0]>0:
        # 提取公共部分
        loan_id = list(all_raw_data_one['loan_id'])[0]
        order_time = list(all_raw_data_one['order_time'])[0]

        # 专有部分
        all_raw_data_unique = list(all_raw_data_one['mongoInfo'])[0]
        all_raw_data_unique = json.loads(all_raw_data_unique)
        all_raw_data_unique = deal_json_to_df_unique(all_raw_data_unique)

        # 数据合并
        all_raw_data_unique.loc[:, 'loan_id'] = str(loan_id)
        all_raw_data_unique.loc[:, 'order_time'] = str(order_time)
        # 输出
        all_raw_data_one = all_raw_data_unique
    return all_raw_data_one


# bh_Info
def get_public_raw_data(all_raw_data, col):
    # 选取目标数据
    all_raw_data_one = all_raw_data[['loan_id', 'order_time', col]]
    all_raw_data_one = all_raw_data_one[all_raw_data_one[col].notnull()]
    return all_raw_data_one


def data_analysis_processing(loan_id, request_body):
    # 根据订单获取数据
    all_raw_data = handle_local_online(request_body)


    # 进行逐个数据解析
    all_deal_data_dict = {}
    all_deal_data_dict['loan_id'] = str(loan_id)

    # 1 mongoInfo
    mongoInfo_df = get_mongoInfo_raw_data(all_raw_data)
    if mongoInfo_df.shape[0]>0:
        all_deal_data_dict['mongoInfo_df'] = mongoInfo_df

    # 2 bh
    bh_df = get_public_raw_data(all_raw_data, 'baihang_info')
    if bh_df.shape[0]>0:
        all_deal_data_dict['bh_df'] = bh_df

    # 3 rh
    rh_df = get_public_raw_data(all_raw_data, 'pboc_info')
    if rh_df.shape[0]>0:
        all_deal_data_dict['rh_df'] = rh_df

    # 4 hd
    hd_df = get_public_raw_data(all_raw_data, 'huadao_info')
    if hd_df.shape[0]>0:
        all_deal_data_dict['hd_info'] = hd_df

    # 5 jd
    jd_df = get_public_raw_data(all_raw_data, 'jingdong_info')
    if jd_df.shape[0]>0:
        all_deal_data_dict['jd_df'] = jd_df

    # 6 xy
    xy_df = get_public_raw_data(all_raw_data, 'xinyan_info')
    if xy_df.shape[0]>0:
        all_deal_data_dict['xy_df'] = xy_df

    # 7 br
    br_df = get_public_raw_data(all_raw_data, 'bairong_info')
    if br_df.shape[0]>0:
        all_deal_data_dict['br_df'] = br_df

    return all_deal_data_dict



def get_all_feature_deal(all_deal_data_dict, loan_id, run_mode, model_data_dict):
    all_feature_data_dict = {}
    # 需要删除的列
    del_list = ['loan_id']
    # 1 mongoInfo
    if 'mongoInfo_df' in all_deal_data_dict:
        for _ in range(1):
            try:
                # start_time = time.time()
                customerlog_feature = get_all_customerlog_feature(all_deal_data_dict, loan_id, run_mode, model_data_dict['mongoInfo'])
                # end_time = time.time()
                # print('get_all_customerlog_feature:', end_time - start_time)
                # save_data(customerlog_feature, str(loan_id), './data/feature_data/mongoInfo/')
                if 'loan_id' in list(customerlog_feature.columns):
                    customerlog_feature = customerlog_feature.drop(del_list, axis=1)
                all_feature_data_dict['customerlog_feature'] = customerlog_feature
            except Exception as e:
                print(e)
                pass

    # 2 bh
    if 'bh_df' in all_deal_data_dict:
        for _ in range(1):
            try:
                # start_time = time.time()
                bh_feature = get_all_bh_feature(all_deal_data_dict, loan_id, run_mode, model_data_dict['bh'])
                # end_time = time.time()
                # print('get_all_bh_feature:', end_time - start_time)

                if 'loan_id' in list(bh_feature.columns):
                    bh_feature = bh_feature.drop(del_list, axis=1)
                all_feature_data_dict['bh_feature'] = bh_feature
            except Exception as e:
                print(e)
                pass

    # 3 rh
    if 'rh_df' in all_deal_data_dict:
        for _ in range(1):
            try:
                # start_time = time.time()
                rh_feature = get_all_rh_feature(all_deal_data_dict, loan_id, run_mode, model_data_dict['rh'])
                # end_time = time.time()
                # print('get_all_rh_feature:', end_time - start_time)

                if 'loan_id' in list(rh_feature.columns):
                    rh_feature = rh_feature.drop(del_list, axis=1)
                all_feature_data_dict['rh_feature'] = rh_feature
            except Exception as e:
                print(e)
                pass

    # 4 hd
    if 'hd_info' in all_deal_data_dict:
        for _ in range(1):
            try:
                # start_time = time.time()
                hd_feature = get_all_hd_feature(all_deal_data_dict, loan_id, run_mode, model_data_dict['hd'])
                # end_time = time.time()
                # print('get_all_hdlog_feature:', end_time - start_time)

                if 'loan_id' in list(hd_feature.columns):
                    hd_feature = hd_feature.drop(del_list, axis=1)
                all_feature_data_dict['hd_feature'] = hd_feature
            except Exception as e:
                print(e)
                pass


    # 5 xy
    if 'xy_df' in all_deal_data_dict:
        for _ in range(1):
            try:
                # start_time = time.time()
                xy_feature = get_all_xy_feature(all_deal_data_dict, loan_id, run_mode, model_data_dict['xy'])
                # end_time = time.time()
                # print('get_all_xy_feature:', end_time - start_time)

                if 'loan_id' in list(xy_feature.columns):
                    xy_feature = xy_feature.drop(del_list, axis=1)
                all_feature_data_dict['xy_feature'] = xy_feature
            except Exception as e:
                print(e)
                pass


    # 6 br
    if 'br_df' in all_deal_data_dict:
        for _ in range(1):
            try:
                # start_time = time.time()
                br_feature = get_all_br_feature(all_deal_data_dict, loan_id, run_mode, model_data_dict['br'])
                # end_time = time.time()
                # print('get_all_br_feature:', end_time - start_time)

                if 'loan_id' in list(br_feature.columns):
                    br_feature = br_feature.drop(del_list, axis=1)
                all_feature_data_dict['br_feature'] = br_feature
            except Exception as e:
                print(e)
                pass


    # 7 jd
    if 'jd_df' in all_deal_data_dict:
        for _ in range(1):
            try:
                # start_time = time.time()
                jd_feature = get_all_jd_feature(all_deal_data_dict, loan_id, run_mode, model_data_dict['jd'])
                # end_time = time.time()
                # print('get_all_jd_feature:', end_time - start_time)

                if 'loan_id' in list(jd_feature.columns):
                    jd_feature = jd_feature.drop(del_list, axis=1)
                all_feature_data_dict['jd_feature'] = jd_feature
            except Exception as e:
                print(e)
                pass
    #
    # save_pickle(all_feature_data_dict, 'all_feature_full_source_data_dict','./data/pkl/')
    # all_feature_data_dict = read_pickle('all_feature_full_source_data_dict','./data/pkl/')
    return all_feature_data_dict






def get_df_from_db(SQL_execute, variable_list):
    import pymysql

    conn = pymysql.connect(
        host='121.199.3.118',
        user='root',
        password='Cq9NYpAc0ydxOj22N2NB',
        database='ods_credit',
        port=3306,
        charset="utf8"
    )
    cursor = conn.cursor()

    cursor.execute(SQL_execute, variable_list)
    data = cursor.fetchall()
    columnDes = cursor.description

    columnNames = [columnDes[i][0] for i in range(len(columnDes))]
    df = pd.DataFrame([list(i) for i in data], columns=columnNames)
    return df



def get_report_data(loan_id):
    variable_list = [loan_id]
    SQL_execute = """SELECT loanNo as loan_id, applyInfo, rhInfo, bhInfo from ods_alchemist_temp.model_data where loanNo=%s"""

    report_data_df = get_df_from_db(SQL_execute, variable_list)
    return report_data_df



def get_df_from_db_mongo(SQL_execute, variable_list):
    import pymysql

    conn = pymysql.connect(
        host='192.168.1.201',
        user='root',
        password='qnroot2022',
        database='ods_credit',
        port=3306,
        charset="utf8"
    )
    cursor = conn.cursor()

    cursor.execute(SQL_execute, variable_list)
    data = cursor.fetchall()
    columnDes = cursor.description

    columnNames = [columnDes[i][0] for i in range(len(columnDes))]
    df = pd.DataFrame([list(i) for i in data], columns=columnNames)
    return df

def get_mongoInfo_data(loan_id):
    variable_list = [loan_id]
    SQL_execute = """SELECT mongoInfo from ods_alchemist.model_data where loanNo=%s"""

    report_data_df = get_df_from_db_mongo(SQL_execute, variable_list)
    return report_data_df





