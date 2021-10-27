from lgh_rh_202111001_func import *
from joblib import Parallel, delayed, load, dump
from optbinning import BinningProcess
from catboost import CatBoostClassifier, Pool, cv
from time import *
import math


# 人行模型
def lgh_rh_20211001(train_df):
    """
    lgh人行模型 日期版本：lgh_rh_20211001
    :param train_df: 上方 rhDataOnline函数 加工完毕后的 特征数据
    :return: 模型：y_pred
    """

    memoryKeys = {'rh_dpd60_raw_cross_model_cat_fold', 'rh_dpd60_raw_cross_feature',
                  'pboc2_fpd20_raw_cross_model_cat_fold_coor','pboc2_fpd20_raw_cross_feature_coor'}

    # 判断变量有无加载到内存中
    if memoryKeys.issubset(locals().keys()):
        pass
    else:

        rh_dpd60_raw_cross_model_cat_fold = load('./rh_model/rh_dpd60_raw_cross_model_cat_fold.joblib')
        rh_dpd60_raw_cross_feature = load('./rh_model/rh_dpd60_raw_cross_feature.joblib')


        pboc2_fpd20_raw_cross_feature_coor = load('./rh_model/pboc2_fpd20_raw_cross_feature_coor.joblib')
        pboc2_fpd20_raw_cross_model_cat_fold_coor = load('./rh_model/pboc2_fpd20_raw_cross_model_cat_fold_coor.joblib')

    dict_in = train_df[pboc2_fpd20_raw_cross_feature_coor].iloc[0].to_dict()
    pboc2_fpd20_raw_coor_score = get_raw_cross_score(dict_in, pboc2_fpd20_raw_cross_feature_coor, pboc2_fpd20_raw_cross_model_cat_fold_coor)


    dict_in = train_df[rh_dpd60_raw_cross_feature].iloc[0].to_dict()
    rh_dpd60_raw_score = get_raw_cross_score(dict_in, rh_dpd60_raw_cross_feature, rh_dpd60_raw_cross_model_cat_fold)


    data = {'rh_dpd60_raw_score': rh_dpd60_raw_score,
            'pboc2_fpd20_raw_coor_score': pboc2_fpd20_raw_coor_score}

    return data


# 百融模型
def lgh_br_20211001(train_df):
    memoryKeys = {'bairong_dpd60_raw_cross_model_cat_fold', 'bairong_dpd60_raw_cross_feature'}

    # 判断变量有无加载到内存中
    if memoryKeys.issubset(locals().keys()):
        pass
    else:
        bairong_dpd60_raw_cross_model_cat_fold = load('./br_model/bairong_dpd60_raw_cross_model_cat_fold.joblib')
        bairong_dpd60_raw_cross_feature = load('./br_model/bairong_dpd60_raw_cross_feature.joblib')


    dict_in = train_df[bairong_dpd60_raw_cross_feature].iloc[0].to_dict()
    bairong_dpd60_raw_score = get_raw_cross_score(dict_in, bairong_dpd60_raw_cross_feature,bairong_dpd60_raw_cross_model_cat_fold)

    dict_out = {'bairong_dpd60_raw_score': bairong_dpd60_raw_score}

    return dict_out



# 京东模型
def lgh_jd_20211001(train_df):
    memoryKeys = {'jingdong_fpd20_raw_cross_feature_block3', 'jingdong_fpd20_raw_cross_model_cat_fold_block3'}

    # 判断变量有无加载到内存中
    if memoryKeys.issubset(locals().keys()):
        pass
    else:
        jingdong_fpd20_raw_cross_feature_block3 = load('./jd_model/jingdong_fpd20_raw_cross_feature_block3.joblib')
        jingdong_fpd20_raw_cross_model_cat_fold_block3 = load('./jd_model/jingdong_fpd20_raw_cross_model_cat_fold_block3.joblib')


    dict_in = train_df[jingdong_fpd20_raw_cross_feature_block3].iloc[0].to_dict()
    jingdong_fpd20_raw_block3_score = get_raw_cross_score(dict_in, jingdong_fpd20_raw_cross_feature_block3,
                                                   jingdong_fpd20_raw_cross_model_cat_fold_block3)


    dict_out = {'jingdong_fpd20_raw_block3_score': jingdong_fpd20_raw_block3_score}

    return dict_out


# 大数据模型
def lgh_big_20211001(train_df):

    big_time = time()

    memoryKeys = {'cross_feature_big3_dpd60_raw_block3', 'cross_model_big3_dpd60_raw_block3',
                  'cross_feature_big3_dpd60_raw_n3', 'cross_model_big3_dpd60_raw_n3',
                  'cross_feature_big3_fpd20_raw_coor', 'cross_model_big3_fpd20_raw_coor',
                  'big_fpd20_raw_cross_feature', 'big_fpd20_raw_cross_model_cat_fold',
                  'big_fpd20_raw_cross_model_cat_fold_coor_1', 'big_fpd20_raw_cross_feature_coor_1'}

    # 判断变量有无加载到内存中
    if memoryKeys.issubset(locals().keys()):
        pass
    else:

        cross_feature_big3_dpd60_raw_block3 = load('./big_model/cross_feature_big3_dpd60_raw_block3.joblib')
        cross_model_big3_dpd60_raw_block3 = load('./big_model/cross_model_big3_dpd60_raw_block3.joblib')

        cross_feature_big3_dpd60_raw_n3 = load('./big_model/cross_feature_big3_dpd60_raw_n3.joblib')
        cross_model_big3_dpd60_raw_n3 = load('./big_model/cross_model_big3_dpd60_raw_n3.joblib')

        cross_feature_big3_fpd20_raw_coor = load('./big_model/cross_feature_big3_fpd20_raw_coor.joblib')
        cross_model_big3_fpd20_raw_coor = load('./big_model/cross_model_big3_fpd20_raw_coor.joblib')

        big_fpd20_raw_cross_model_cat_fold = load('./big_model/big_fpd20_raw_cross_model_cat_fold.joblib')
        big_fpd20_raw_cross_feature = load('./big_model/big_fpd20_raw_cross_feature.joblib')

        big_fpd20_raw_cross_model_cat_fold_coor_1 = load('./big_model/big_fpd20_raw_cross_model_cat_fold_coor_1.joblib')
        big_fpd20_raw_cross_feature_coor_1 = load('./big_model/big_fpd20_raw_cross_feature_coor_1.joblib')

    big1_time = time()
    print("big模型变量加载时间:", big1_time - big_time)

    dict_in = train_df[big_fpd20_raw_cross_feature].iloc[0].to_dict()
    big_fpd20_raw_score = get_raw_cross_score(dict_in, big_fpd20_raw_cross_feature, big_fpd20_raw_cross_model_cat_fold)

    time1 = time()
    print("big_fpd20_raw_score模型计算时间:", time1 - big1_time)

    dict_in = train_df[cross_feature_big3_dpd60_raw_block3].iloc[0].to_dict()
    big3_dpd60_raw_block3_score = get_raw_cross_score(dict_in, cross_feature_big3_dpd60_raw_block3, cross_model_big3_dpd60_raw_block3)

    time2 = time()
    print("big3_dpd60_raw_block3_score模型计算时间:", time2 - time1)

    dict_in = train_df[cross_feature_big3_dpd60_raw_n3].iloc[0].to_dict()
    big3_dpd60_raw_n3_score = get_raw_cross_score(dict_in, cross_feature_big3_dpd60_raw_n3,cross_model_big3_dpd60_raw_n3)

    time3 = time()
    print("big3_dpd60_raw_n3_score模型计算时间:", time3 - time2)

    dict_in = train_df[cross_feature_big3_fpd20_raw_coor].iloc[0].to_dict()
    big3_fpd20_raw_coor_score = get_raw_cross_score(dict_in, cross_feature_big3_fpd20_raw_coor, cross_model_big3_fpd20_raw_coor)

    time4 = time()
    print("big3_fpd20_raw_coor_score模型计算时间:", time4 - time3)

    dict_in = train_df[big_fpd20_raw_cross_feature_coor_1].iloc[0].to_dict()
    big_fpd20_raw_coor_1_score = get_raw_cross_score(dict_in, big_fpd20_raw_cross_feature_coor_1, big_fpd20_raw_cross_model_cat_fold_coor_1)

    time5 = time()
    print("big_fpd20_raw_coor_1_score模型计算时间:", time5 - time4)

    dict_out = {'big3_dpd60_raw_block3_score': big3_dpd60_raw_block3_score,
                'big3_dpd60_raw_n3_score': big3_dpd60_raw_n3_score,
                'big3_fpd20_raw_coor_score': big3_fpd20_raw_coor_score,
                'big_fpd20_raw_coor_1_score': big_fpd20_raw_coor_1_score,
                'big_fpd20_raw_score': big_fpd20_raw_score}

    return dict_out


# 全数据模型
def lgh_all_20211001(train_df):

    all_time = time()

    memoryKeys = {'cross_feature_all3_dpd60_raw_block3', 'cross_model_all3_dpd60_raw_block3',
                  'cross_feature_all3_fpd20_raw_block3', 'cross_model_all3_fpd20_raw_block3',
                  'cross_feature_all3_fpd20_raw_coor', 'cross_model_all3_fpd20_raw_coor',
                  'all_dpd60_raw_cross_model_cat_fold', 'all_dpd60_raw_cross_feature'}

    # 判断变量有无加载到内存中
    if memoryKeys.issubset(locals().keys()):
        pass
    else:
        cross_model_all3_dpd60_raw_block3 = load('./all_model/cross_model_all3_dpd60_raw_block3.joblib')
        cross_feature_all3_dpd60_raw_block3 = load('./all_model/cross_feature_all3_dpd60_raw_block3.joblib')

        cross_feature_all3_fpd20_raw_block3 = load('./all_model/cross_feature_all3_fpd20_raw_block3.joblib')
        cross_model_all3_fpd20_raw_block3 = load('./all_model/cross_model_all3_fpd20_raw_block3.joblib')

        cross_feature_all3_fpd20_raw_coor = load('./all_model/cross_feature_all3_fpd20_raw_coor.joblib')
        cross_model_all3_fpd20_raw_coor = load('./all_model/cross_model_all3_fpd20_raw_coor.joblib')

        all_dpd60_raw_cross_model_cat_fold = load('./all_model/all_dpd60_raw_cross_model_cat_fold.joblib')
        all_dpd60_raw_cross_feature = load('./all_model/all_dpd60_raw_cross_feature.joblib')

    time1 = time()
    print("all模型变量加载时间:", time1 - all_time)

    dict_in = train_df[cross_feature_all3_dpd60_raw_block3].iloc[0].to_dict()
    all3_dpd60_raw_block3_score = get_raw_cross_score(dict_in, cross_feature_all3_dpd60_raw_block3,cross_model_all3_dpd60_raw_block3)

    time2 = time()
    print("all3_dpd60_raw_block3_score计算时间:", time2 - time1)


    dict_in = train_df[cross_feature_all3_fpd20_raw_block3].iloc[0].to_dict()
    all3_fpd20_raw_block3_score = get_raw_cross_score(dict_in, cross_feature_all3_fpd20_raw_block3,cross_model_all3_fpd20_raw_block3)

    time3 = time()
    print("all3_fpd20_raw_block3_score计算时间:", time3 - time2)

    dict_in = train_df[cross_feature_all3_fpd20_raw_coor].iloc[0].to_dict()
    all3_fpd20_raw_coor_score = get_raw_cross_score(dict_in, cross_feature_all3_fpd20_raw_coor,cross_model_all3_fpd20_raw_coor)

    time4 = time()
    print("all3_fpd20_raw_coor_score计算时间:", time4 - time3)

    dict_in = train_df[all_dpd60_raw_cross_feature].iloc[0].to_dict()
    all_dpd60_raw_score = get_raw_cross_score(dict_in, all_dpd60_raw_cross_feature,all_dpd60_raw_cross_model_cat_fold)

    time5 = time()
    print("all_dpd60_raw_score计算时间:", time5 - time4)


    dict_out = {'all3_dpd60_raw_block3_score': all3_dpd60_raw_block3_score,
                'all3_fpd20_raw_block3_score': all3_fpd20_raw_block3_score,
                'all3_fpd20_raw_coor_score': all3_fpd20_raw_coor_score,
                'all_dpd60_raw_score': all_dpd60_raw_score}

    return dict_out


# 优分数据模型
def lgh_youfen_20211001(train_df):
    memoryKeys = {'youfen_fpd20_raw_cross_feature_coor3', 'youfen_fpd20_raw_cross_model_cat_fold_coor3'}

    # 判断变量有无加载到内存中
    if memoryKeys.issubset(locals().keys()):
        pass
    else:

        youfen_fpd20_raw_cross_model_cat_fold_coor3 = load('./youfen_model/youfen_fpd20_raw_cross_model_cat_fold_coor3.joblib')
        youfen_fpd20_raw_cross_feature_coor3 = load('./youfen_model/youfen_fpd20_raw_cross_feature_coor3.joblib')


    dict_in = train_df[youfen_fpd20_raw_cross_feature_coor3].iloc[0].to_dict()
    youfen_fpd20_raw_coor3_score = get_raw_cross_score(dict_in, youfen_fpd20_raw_cross_feature_coor3,youfen_fpd20_raw_cross_model_cat_fold_coor3)



    dict_out = {'youfen_fpd20_raw_coor3_score': youfen_fpd20_raw_coor3_score}

    return dict_out


# yzj全数据模型
def yzj_all_20211001(train_df):

    memoryKeys = {'yzj_binning5','yzj_model5','yzj_feature5'}

    # 判断变量有无加载到内存中
    if memoryKeys.issubset(locals().keys()):
        pass
    else:
        # 分箱表和模型和特征加载
        # yzj_binning1 = BinningProcess.load("./yzj_model/binning1.pkl")
        # yzj_binning2 = BinningProcess.load("./yzj_model/binning2.pkl")
        # yzj_binning3 = BinningProcess.load("./yzj_model/binning3.pkl")
        # yzj_binning4 = BinningProcess.load("./yzj_model/binning4.pkl")
        yzj_binning5 = BinningProcess.load("./yzj_model/binning5.pkl")

        # model1 = CatBoostClassifier()
        # yzj_model1 = model1.load_model('./yzj_model/catboost_model1.dump')
        #
        # model2 = CatBoostClassifier()
        # yzj_model2 = model2.load_model('./yzj_model/catboost_model2.dump')
        #
        # model3 = CatBoostClassifier()
        # yzj_model3 = model3.load_model('./yzj_model/catboost_model3.dump')
        #
        # model4 = CatBoostClassifier()
        # yzj_model4 = model4.load_model('./yzj_model/catboost_model4.dump')

        model5 = CatBoostClassifier()
        yzj_model5 = model5.load_model('./yzj_model/catboost_model5.dump')

        # yzj_feature1 = load('./yzj_model/feature_model1.joblib')
        # yzj_feature2 = load('./yzj_model/feature_model2.joblib')
        # yzj_feature3 = load('./yzj_model/feature_model3.joblib')
        # yzj_feature4 = load('./yzj_model/feature_model4.joblib')
        yzj_feature5 = load('./yzj_model/feature_model5.joblib')

    # 特征加工
    train_df = train_df.replace('--', np.nan).reset_index(drop=True)

    # test1_woe = yzj_binning1.transform(train_df[yzj_feature1], metric="woe")
    # y_pred_1 = yzj_model1.predict_proba(test1_woe[yzj_feature1])[:, 1]
    # # score1 = p_to_score(y_pred_1)
    #
    # test2_woe = yzj_binning2.transform(train_df[yzj_feature2], metric="woe")
    # y_pred_2 = yzj_model2.predict_proba(test2_woe[yzj_feature2])[:, 1]
    # # score2 = p_to_score(y_pred_2)
    #
    # test3_woe = yzj_binning3.transform(train_df[yzj_feature3], metric="woe")
    # y_pred_3 = yzj_model3.predict_proba(test3_woe[yzj_feature3])[:, 1]
    # # score3 = p_to_score(y_pred_3)
    #
    # test4_woe = yzj_binning4.transform(train_df[yzj_feature4], metric="woe")
    # y_pred_4 = yzj_model4.predict_proba(test4_woe[yzj_feature4])[:, 1]
    # score4 = p_to_score(y_pred_4)

    test5_woe = yzj_binning5.transform(train_df[yzj_feature5], metric="woe")
    y_pred_5 = yzj_model5.predict_proba(test5_woe[yzj_feature5])[:, 1]
    # score5 = p_to_score(y_pred_5)

    # score = (score1 + score2 + score3 + score4 + score5)/5

    # 测试返回 dataframe
    #  y_pred = pd.DataFrame({"y_pred_1":y_pred_1,"y_pred_2":y_pred_2,"y_pred_3":y_pred_3, "y_pred_4":y_pred_4, "y_pred_5":y_pred_5})
    # y_pred = (y_pred_1 + y_pred_2 + y_pred_3 + y_pred_4 + y_pred_5) / 5
    y_pred = y_pred_5[0]
    return {'yzj_all_model_score': y_pred}


# cxt全数据模型
def cxt_all_20211001(train_df):

    memoryKeys = {'cxt_model', 'cxt_binning_process'}

    # 判断变量有无加载到内存中
    if memoryKeys.issubset(locals().keys()):
        pass
    else:
        model = CatBoostClassifier()
        cxt_model = model.load_model('./cxt_model/cxt_model_01.bin')
        cxt_binning_process = BinningProcess.load('./cxt_model/binning_process_01.pkl')

    cxt_features = cxt_model.feature_names_
    data_woe = cxt_binning_process.transform(train_df[cxt_features], metric="woe")
    proba = cxt_model.predict_proba(data_woe)[:, 1]

    return {'cxt_all_model_score': proba[0]}


# 综合决策数据模型
def lgh_final_20211001(train_df):
    memoryKeys = {'final_cross_feature', 'final_cross_model_cat_fold'}

    # 判断变量有无加载到内存中
    if memoryKeys.issubset(locals().keys()):
        pass
    else:

        final_cross_feature = load('./final_model/final_cross_feature.joblib')
        final_cross_model_cat_fold = load('./final_model/final_cross_model_cat_fold.joblib')


    dict_in = train_df[final_cross_feature].iloc[0].to_dict()
    final_score = get_raw_cross_score(dict_in, final_cross_feature,final_cross_model_cat_fold)



    dict_out = {'final_score': final_score}

    return dict_out