#!/usr/bin/python
# -*- coding: UTF-8 -*-
"""
@file:model.py
@Function:
"""


import pandas as pd
import numpy as np
import math
from sklearn import metrics
import pickle
from sklearn.metrics import roc_auc_score,roc_curve,auc
from autogluon.tabular import TabularDataset, TabularPredictor
# from special_tools import *
# from config import *


import warnings
warnings.filterwarnings('ignore')



from sympy import *

def model_score_coefficientp(ratio = 1/7, basescore=600, double=50):
    x = Symbol('x')
    y = Symbol('y')
    out = solve([basescore -(x-y * np.log(ratio)), basescore -(x-y * np.log(2*ratio) + double)], [x, y])
    return out

def prod_to_score(xbeta, out):
    x = Symbol('x')
    y = Symbol('y')
    score_sig = out.get(x) - out.get(y) * np.log(xbeta / (1 - xbeta))
    if score_sig > 900:
        score_sig = 900
    elif score_sig < 300:
        score_sig = 300
    else:
        score_sig = score_sig
    score_sig = int(score_sig)
    return score_sig



def read_pickle(file_name, path_file):
    fr = open(path_file + file_name + '.pkl', 'rb')
    dataset_pickle = pickle.load(fr)
    fr.close()
    return dataset_pickle

def ks_eva(y_label,y_pred):
    tpr,fpr,threshold = metrics.roc_curve(y_label,y_pred)
    KS = abs(tpr - fpr).max()
    return KS


def bh_feature_select(feature_data_bh):
    all_columns_list = list(feature_data_bh.columns)
    new_df = feature_data_bh[['loan_id']]
    bh_feature_name_list = read_pickle('bh_model_feature', './data/pkl/')
    for col in bh_feature_name_list:
        if col in all_columns_list:
            new_df[col] = feature_data_bh[col]
        else:
            # new_df[col] = None
            new_df.loc[:, col] = np.nan
    return new_df


def bh_rh_feature_select(feature_data_bh_rh):
    all_columns_list = list(feature_data_bh_rh.columns)
    new_df = feature_data_bh_rh[['loan_id']]
    bh_rh_feature_name_list = read_pickle('bh_rh_model_feature', './data/pkl/')
    for col in bh_rh_feature_name_list:
        if col in all_columns_list:
            new_df[col] = feature_data_bh_rh[col]
        else:
            # new_df[col] = None
            new_df.loc[:, col] = np.nan

    return new_df

def bh_model(feature_data_bh, model_data_dict):
    predictor = model_data_dict['predictor_bh']
    proba_df = predictor.predict_proba(feature_data_bh).reset_index(drop=True)
    proba_df = proba_df.rename(columns={1: 'proba'})
    y_pred = proba_df['proba'][0]
    return y_pred

def bh_rh_model(feature_data_bh_rh, model_data_dict):
    predictor = model_data_dict['predictor_bh_rh']
    proba_df = predictor.predict_proba(feature_data_bh_rh).reset_index(drop=True)
    proba_df = proba_df.rename(columns={1: 'proba'})
    y_pred = proba_df['proba'][0]
    return y_pred


def full_source_model(feature_data_bh, model_data_dict):
    predictor = model_data_dict['predictor_full_source']
    proba_df = predictor.predict_proba(feature_data_bh).reset_index(drop=True)
    proba_df = proba_df.rename(columns={1: 'proba'})
    y_pred = proba_df['proba'][0]
    return y_pred



def get_mode_sore(all_feature_data_dict, model_data_dict):


    score_dict = {}


    # bh_data
    # customerlog
    if 'customerlog_feature' in all_feature_data_dict and 'bh_feature' in all_feature_data_dict:
        feature_df_list = []
        # try:
        feature_df_list.append(all_feature_data_dict['customerlog_feature'])
        feature_df_list.append(all_feature_data_dict['bh_feature'])

        feature_data_bh = pd.concat(feature_df_list,axis=1)

        all_feature_df_list = list(feature_data_bh.columns)

        bh_feature_dict = model_data_dict['bh_feature_list']

        new_all_feature_df = pd.DataFrame({'loan_id': ['111']})
        for col in bh_feature_dict:
            if col in all_feature_df_list:
                new_all_feature_df[col] = feature_data_bh[col]
            else:
                new_all_feature_df[col] = np.nan
        del new_all_feature_df['loan_id']
        bh_prob = bh_model(new_all_feature_df, model_data_dict)
        out = model_score_coefficientp()
        bh_score = prod_to_score(bh_prob, out)
        score_dict['bh_fspd20_2021_10'] = bh_score
        # except Exception as e:
        #     score_dict['bh_fspd20_2021_10'] = 'bh_mising'
        #     print(e)
        #     pass
    else:
        score_dict['bh_fspd20_2021_10'] = 'bh_mising'

    # bh_rh_data
    # customerlog
    if 'customerlog_feature' in all_feature_data_dict and 'bh_feature' in all_feature_data_dict and 'rh_feature' in all_feature_data_dict:
        # try:
        feature_df_list = []
        feature_df_list.append(all_feature_data_dict['customerlog_feature'])
        feature_df_list.append(all_feature_data_dict['bh_feature'])
        feature_df_list.append(all_feature_data_dict['rh_feature'])

        feature_data_bh_rh = pd.concat(feature_df_list,axis=1)

        all_feature_df_list = list(feature_data_bh_rh.columns)

        bh_rh_feature_dict = model_data_dict['bh_rh_feature_list']

        new_all_feature_df = pd.DataFrame({'loan_id': ['111']})
        for col in bh_rh_feature_dict:
            if col in all_feature_df_list:
                # try:
                new_all_feature_df[col] = feature_data_bh_rh[col]
                # except Exception as e:
                #     print(col)
                #     new_all_feature_df[col] = np.nan
            else:
                new_all_feature_df[col] = np.nan

        del new_all_feature_df['loan_id']
        bh_rh_prob = bh_rh_model(new_all_feature_df, model_data_dict)
        out = model_score_coefficientp()
        bh_rh_score = prod_to_score(bh_rh_prob, out)
        score_dict['bh_rh_fspd20_2021_10'] = bh_rh_score
        # except Exception as e:
        #     print(e)
        #     score_dict['bh_rh_fspd20_2021_10'] = 'bh_rh_mising'
        #     pass
    else:
        score_dict['bh_rh_fspd20_2021_10'] = 'bh_rh_mising'


    # full_source_data
    # customerlog
    if 'customerlog_feature' in all_feature_data_dict and 'bh_feature' in all_feature_data_dict and 'rh_feature' in all_feature_data_dict:
        # try:
        feature_df_list = []
        feature_df_list.append(all_feature_data_dict['customerlog_feature'])
        feature_df_list.append(all_feature_data_dict['bh_feature'])
        feature_df_list.append(all_feature_data_dict['rh_feature'])

        # hd
        if 'hd_feature' in all_feature_data_dict:
            feature_df_list.append(all_feature_data_dict['hd_feature'])
        # else:
        #     hd_feature_df_empty = read_pickle('hd_feature_empty_df', './data/pkl/')
        #     feature_df_list.append(hd_feature_df_empty)

        # xy
        if 'xy_feature' in all_feature_data_dict:
            feature_df_list.append(all_feature_data_dict['xy_feature'])
        # else:
        #     xy_feature_df_empty = read_pickle('xy_feature_empty_df', './data/pkl/')
        #     feature_df_list.append(xy_feature_df_empty)

        # br
        if 'br_feature' in all_feature_data_dict:
            feature_df_list.append(all_feature_data_dict['br_feature'])
        # else:
        #     br_feature_df_empty = read_pickle('br_feature_empty_df', './data/pkl/')
        #     feature_df_list.append(br_feature_df_empty)

        # jd
        if 'jd_feature' in all_feature_data_dict:
            feature_df_list.append(all_feature_data_dict['jd_feature'])
        # else:
        #     jd_feature_df_empty = read_pickle('jd_feature_empty_df', './data/pkl/')
        #     feature_df_list.append(jd_feature_df_empty)

        feature_data_full_source = pd.concat(feature_df_list,axis=1)

        all_feature_df_list = list(feature_data_full_source.columns)

        full_source_feature_dict = model_data_dict['full_source_feature_list']

        new_all_feature_df = pd.DataFrame({'loan_id': ['111']})
        for col in full_source_feature_dict:
            if col in all_feature_df_list:
                new_all_feature_df[col] = feature_data_full_source[col]
            else:
                new_all_feature_df[col] = np.nan

        del new_all_feature_df['loan_id']

        full_source_prob = full_source_model(new_all_feature_df, model_data_dict)
        out = model_score_coefficientp()
        full_source_score = prod_to_score(full_source_prob, out)
        score_dict['full_source_fspd20_2021_10'] = full_source_score
        # except Exception as e:
        #     print(e)
        #     score_dict['full_source_fspd20_2021_10'] = 'full_source_mising'
        #     pass
    else:
        score_dict['full_source_fspd20_2021_10'] = 'full_source_mising'
    return score_dict









if __name__ == '__main__':
    pass




