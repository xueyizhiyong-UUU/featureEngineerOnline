#!/usr/bin/python
# -*- coding: UTF-8 -*-
from multiprocessing import Pool
from model_utils.feature_merge import *
from model_utils.model import *
from model_utils.deployment_util import *
from tqdm import *
import time

def main(request_body, model_data_dict):

    for _ in range(1):
        try:
            run_mode = 'online_mode'
            # request_body = request_body['data']
            loan_id = str(request_body['apply_info']['loanNo'])

            start_time = time.time()
            all_deal_data_dict = data_analysis_processing(loan_id, request_body)

            all_feature_data_dict = get_all_feature_deal(all_deal_data_dict, loan_id, run_mode, model_data_dict)


            result_score = get_mode_sore(all_feature_data_dict, model_data_dict)


        except Exception as e:
            print(e)
            result_score = {'bh_fspd20_2021_10':'bh is mising',
                            'bh_rh_fspd20_2021_10':'bh_rh is mising',
                            'full_source_fspd20_2021_10':'full_source is mising'}
            pass
    return result_score


def get_model_data():

    all_col_list = read_pickle('all_full_souce_feature_col_list_new_label','./data/pkl/')
    model_data_dict = get_feature_name_specified_all(all_col_list)

    bh_feature_list = read_pickle('bh_model_feature_v3', './data/pkl/')
    bh_rh_feature_list = read_pickle('rh_model_feature_v3', './data/pkl/')
    full_source_feature_list = read_pickle('full_source_feature_v3', './data/pkl/')
    model_data_dict['bh_feature_list'] = bh_feature_list
    model_data_dict['bh_rh_feature_list'] = bh_rh_feature_list
    model_data_dict['full_source_feature_list'] = full_source_feature_list

    predictor_bh = TabularPredictor.load('./data/model/riskModels_Infer_bh')
    predictor_bh_rh = TabularPredictor.load('./data/model/riskModels_Infer_bh_rh')
    predictor_full_source = TabularPredictor.load('./data/model/riskModels_Infer_full_source')

    model_data_dict['predictor_bh'] = predictor_bh
    model_data_dict['predictor_bh_rh'] = predictor_bh_rh
    model_data_dict['predictor_full_source'] = predictor_full_source
    return model_data_dict

def zgl_main(request_body):
    model_data_dict = get_model_data()
    result_score = main(request_body, model_data_dict)
    print(result_score)
    return result_score

if __name__ == '__main__':

    start_time = time.time()
    request_body = read_pickle('test_json_data', './data/pkl/')
    zgl_main(request_body)
    end_time = time.time()
    print('run_time',end_time-start_time)










