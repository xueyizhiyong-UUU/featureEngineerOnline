#!/usr/bin/python
# -*- coding: UTF-8 -*-
from multiprocessing import Pool
from model_utils.feature_merge import *
from model_utils.model import *
from tqdm import *

def main(request_body, model_data_dict):

    for _ in range(1):
        try:
            run_mode = 'online_mode'
            # request_body = request_body['data']
            loan_id = str(request_body['apply_info']['loanNo'])
            all_deal_data_dict = data_analysis_processing(loan_id, request_body)

            all_feature_data_dict = get_all_feature_deal(all_deal_data_dict, loan_id, run_mode, model_data_dict)
            result_score = get_mode_sore(all_feature_data_dict, model_data_dict)
        except Exception as e:
            print(e)
            result_score = {'bh_score':'bh_score is mising', 'bh_rh_score': 'bh_rh_score is mising'}
            pass
    return result_score


def get_model_data():
    model_data_dict = {}
    all_col_list = read_pickle('all_model_col_list','./data/pkl/')

    mongoInfo_feature_list = []
    bh_feature_list = []
    rh_feature_list = []
    for col in all_col_list:
        if 'mongoInfo' in col:
            mongoInfo_feature_list.append(col)
        elif  'bh_' in col:
            bh_feature_list.append(col)
        elif  'rh_' in col:
            rh_feature_list.append(col)


    predictor_bh = TabularPredictor.load('./data/model/riskModels_Infer_bh')
    predictor_bh_rh = TabularPredictor.load('./data/model/riskModels_Infer_bh_rh')

    model_data_dict['mongoInfo'] = mongoInfo_feature_list
    model_data_dict['bh'] = bh_feature_list
    model_data_dict['rh'] = rh_feature_list
    model_data_dict['predictor_bh'] = predictor_bh
    model_data_dict['predictor_bh_rh'] = predictor_bh_rh
    return model_data_dict

def zgl_main(request_body):
    model_data_dict = get_model_data()

    result_score = main(request_body, model_data_dict)
    print(result_score)
    return result_score

# if __name__ == '__main__':
#     request_body = read_pickle('test_json_data', './data/pkl/')
#     # print(request_body)
#     zgl_main(request_body)









