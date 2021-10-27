import pandas as pd
import re
import numpy as np


def duotou_money_handle(x):
    if x == "0W~0.2W":
        return 1
    elif x == "0.2W~0.5W":
        return 2
    elif x == "0.5W~1W":
        return 3
    elif x == "1W~3W":
        return 4
    elif x == "3W~5W":
        return 5
    elif x == "5W~10W":
        return 5
    elif x == "10W以上":
        return 5
    else:
        return x


def shebao_handle(x):
    if x == "A":
        return 0
    elif x == "B":
        return 1
    elif x == "C":
        return 2
    elif x == "D":
        return 3
    elif x == "E":
        return 4
    elif x == "F":
        return 5
    elif x == "G":
        return 6
    elif x == "H":
        return 7
    elif x == "I":
        return 8
    elif x == "J":
        return 9
    elif x == "K":
        return 10
    else:
        return x


def black_Condition(x):
    if x == "M1":
        return 1
    elif x == "M2":
        return 2
    elif x == "M3":
        return 3
    elif x == "M4":
        return 4
    elif x == "M5":
        return 5
    elif x == "M6":
        return 6
    elif x == "M6+":
        return 7
    elif x == "Related risks":
        return 8
    elif x == "suspected_fraud":
        return 9
    elif x == "fraud":
        return 10
    else:
        return x


def black_seriousCondition(x):
    hit = ["M2", 'M3', 'M4', 'M5', 'M6', 'M6+',"Related risks", "suspected_fraud", "fraud"]
    hit_true = 5
    hit_false = 10
    if x in hit:
        x = hit_true
    else:
        x = hit_false
    return x


def handle_new_third(finnal_data):

    # 多头特征工程
    finnal_data['duotou_app_biggest_money'] = finnal_data['duotou_app_biggest_money'].apply(lambda x: duotou_money_handle(x))
    finnal_data['duotou_app_small_money'] = finnal_data['duotou_app_small_money'].apply(lambda x: duotou_money_handle(x))
    finnal_data['duotou_app_bank_biggest_money'] = finnal_data['duotou_app_bank_biggest_money'].apply(lambda x: duotou_money_handle(x))

    finnal_data['duotou_app_bank_small_money'] = finnal_data['duotou_app_bank_small_money'].apply(lambda x: duotou_money_handle(x))

    finnal_data['duotou_app_unbank_biggest_money'] = finnal_data['duotou_app_unbank_biggest_money'].apply(lambda x: duotou_money_handle(x))
    finnal_data['duotou_app_unbank_small_money'] = finnal_data['duotou_app_unbank_small_money'].apply(lambda x: duotou_money_handle(x))
    finnal_data['duotou_loan_biggest_money'] = finnal_data['duotou_loan_biggest_money'].apply(lambda x: duotou_money_handle(x))
    finnal_data['duotou_loan_bank_biggest_money'] = finnal_data['duotou_loan_bank_biggest_money'].apply(lambda x: duotou_money_handle(x))

    finnal_data['duotou_loan_bank_small_money'] = finnal_data['duotou_loan_bank_small_money'].apply(lambda x: duotou_money_handle(x))
    finnal_data['duotou_loan_unbank_biggest_money'] = finnal_data['duotou_loan_unbank_biggest_money'].apply(lambda x: duotou_money_handle(x))

    finnal_data['duotou_loan_unbank_small_money'] = finnal_data['duotou_loan_unbank_small_money'].apply(lambda x: duotou_money_handle(x))

    finnal_data['duotou_overdue_biggest_money'] = finnal_data['duotou_overdue_biggest_money'].apply(lambda x: duotou_money_handle(x))
    finnal_data['duotou_overdue_small_money'] = finnal_data['duotou_overdue_small_money'].apply(lambda x: duotou_money_handle(x))
    finnal_data['duotou_arrearage_biggest_money'] = finnal_data['duotou_arrearage_biggest_money'].apply(lambda x: duotou_money_handle(x))
    finnal_data['duotou_arrearage_small_money'] = finnal_data['duotou_arrearage_small_money'].apply(lambda x: duotou_money_handle(x))

    finnal_data['duotou_app_small_money'] = finnal_data['duotou_app_small_money'].apply(lambda x: duotou_money_handle(x))
    finnal_data['duotou_loan_small_money'] = finnal_data['duotou_loan_small_money'].apply(lambda x: duotou_money_handle(x))

    # 社保特征工程
    finnal_data['shebao_IncomeLevel'] = finnal_data['shebao_incomeLevel'].apply(lambda x: shebao_handle(x))
    finnal_data['shebao_StabilityLevel'] = finnal_data['shebao_stabilityLevel'].apply(lambda x: shebao_handle(x))
    finnal_data['shebao_CreditLevel'] = finnal_data['shebao_creditLevel'].apply(lambda x: shebao_handle(x))

    # 黑名单特征工程
    finnal_data['black_idcardDimension_lastCondition_new'] = finnal_data['black_idcardDimension_lastCondition'].apply(lambda x: black_Condition(x))
    finnal_data['black_cellphoneDimension_lastCondition_new'] = finnal_data['black_cellphoneDimension_lastCondition'].apply(lambda x: black_Condition(x))
    finnal_data['black_cellphoneDimension_seriousCondition_new'] = finnal_data['black_cellphoneDimension_seriousCondition'].apply(lambda x: black_Condition(x))
    finnal_data['black_idcardDimension_seriousCondition_new'] = finnal_data['black_idcardDimension_seriousCondition'].apply(lambda x: black_Condition(x))

    finnal_data['black_cellphoneDimension_seriousCondition_hit'] = finnal_data['black_cellphoneDimension_seriousCondition'].apply(lambda x: black_seriousCondition(x))
    finnal_data['black_idcardDimension_seriousCondition_hit'] = finnal_data['black_idcardDimension_seriousCondition'].apply(lambda x: black_seriousCondition(x))

    finnal_data['black_cellphoneDimension_loanTypes_is_payday_loan'] = 1 if 'payday_loan' in re.sub(" ", "", str(finnal_data['black_cellphoneDimension_loanTypes'])).split(",") else 0
    finnal_data['black_cellphoneDimension_loanTypes_is_micro_loan'] = 1 if 'micro_loan' in re.sub(" ", "", str(finnal_data['black_cellphoneDimension_loanTypes'])).split(",") else 0
    finnal_data['black_cellphoneDimension_loanTypes_is_p2p'] = 1 if 'p2p' in re.sub(" ", "", str(finnal_data['black_cellphoneDimension_loanTypes'])).split(",") else 0
    finnal_data['black_cellphoneDimension_loanTypes_is_cf_edu'] = 1 if 'cf_edu' in re.sub(" ", "", str(finnal_data['black_cellphoneDimension_loanTypes'])).split(",") else 0
    finnal_data['black_cellphoneDimension_loanTypes_is_cf_medical'] = 1 if 'cf_medical' in re.sub(" ", "", str(finnal_data['black_cellphoneDimension_loanTypes'])).split(",") else 0
    finnal_data['black_cellphoneDimension_loanTypes_is_cf_rent'] = 1 if 'cf_rent' in re.sub(" ", "", str(finnal_data['black_cellphoneDimension_loanTypes'])).split(",") else 0
    finnal_data['black_cellphoneDimension_loanTypes_is_cf_digital'] = 1 if 'cf_digital' in re.sub(" ", "", str(finnal_data['black_cellphoneDimension_loanTypes'])).split(",") else 0
    finnal_data['black_cellphoneDimension_loanTypes_is_cf_car'] = 1 if 'cf_car' in re.sub(" ", "", str(finnal_data['black_cellphoneDimension_loanTypes'])).split(",") else 0
    finnal_data['black_cellphoneDimension_loanTypes_is_cf_deco'] = 1 if 'cf_deco' in re.sub(" ", "", str(finnal_data['black_cellphoneDimension_loanTypes'])).split(",") else 0
    finnal_data['black_cellphoneDimension_loanTypes_is_cf_tour'] = 1 if 'cf_tour' in re.sub(" ", "", str(finnal_data['black_cellphoneDimension_loanTypes'])).split(",") else 0
    finnal_data['black_cellphoneDimension_loanTypes_is_cf_agri'] = 1 if 'cf_agri' in re.sub(" ", "", str(finnal_data['black_cellphoneDimension_loanTypes'])).split(",") else 0
    finnal_data['black_cellphoneDimension_loanTypes_is_cf_others'] = 1 if 'cf_others' in re.sub(" ", "", str(finnal_data['black_cellphoneDimension_loanTypes'])).split(",") else 0
    finnal_data['black_cellphoneDimension_loanTypes_is_supply_chain'] = 1 if 'supply_chain' in re.sub(" ", "", str(finnal_data['black_cellphoneDimension_loanTypes'])).split(",") else 0
    finnal_data['black_cellphoneDimension_loanTypes_is_leasing_company'] = 1 if 'leasing_company' in re.sub(" ", "",str(finnal_data['black_cellphoneDimension_loanTypes'])).split(",") else 0
    finnal_data['black_cellphoneDimension_loanTypes_is_car_mortgage'] = 1 if 'car_mortgage' in re.sub(" ", "", str(finnal_data['black_cellphoneDimension_loanTypes'])).split(",") else 0
    finnal_data['black_cellphoneDimension_loanTypes_is_house_mortgage'] = 1 if 'house_mortgage' in re.sub(" ", "", str(finnal_data['black_cellphoneDimension_loanTypes'])).split(",") else 0
    finnal_data['black_cellphoneDimension_loanTypes_is_credit_card'] = 1 if 'credit_card' in re.sub(" ", "", str(finnal_data['black_cellphoneDimension_loanTypes'])).split(",") else 0
    finnal_data['black_cellphoneDimension_loanTypes_is_balance_transfer'] = 1 if 'balance_transfer' in re.sub(" ", "",str(finnal_data['black_cellphoneDimension_loanTypes'])).split(",") else 0
    finnal_data['black_cellphoneDimension_loanTypes_is_others'] = 1 if 'others' in re.sub(" ", "", str(finnal_data['black_cellphoneDimension_loanTypes'])).split(",") else 0
    finnal_data['black_cellphoneDimension_loanTypes_is_IOU'] = 1 if 'IOU' in re.sub(" ", "", str(finnal_data['black_cellphoneDimension_loanTypes'])).split(",") else 0

    finnal_data['black_idcardDimension_loanTypes_is_payday_loan'] = 1 if 'payday_loan' in re.sub(" ", "", str(finnal_data['black_idcardDimension_loanTypes'])).split(",") else 0
    finnal_data['black_idcardDimension_loanTypes_is_micro_loan'] = 1 if 'micro_loan' in re.sub(" ", "", str(finnal_data['black_idcardDimension_loanTypes'])).split(",") else 0
    finnal_data['black_idcardDimension_loanTypes_is_p2p'] = 1 if 'p2p' in re.sub(" ", "", str(finnal_data['black_idcardDimension_loanTypes'])).split(",") else 0
    finnal_data['black_idcardDimension_loanTypes_is_cf_edu'] = 1 if 'cf_edu' in re.sub(" ", "", str(finnal_data['black_idcardDimension_loanTypes'])).split(",") else 0
    finnal_data['black_idcardDimension_loanTypes_is_cf_medical'] = 1 if 'cf_medical' in re.sub(" ", "", str(finnal_data['black_idcardDimension_loanTypes'])).split(",") else 0
    finnal_data['black_idcardDimension_loanTypes_is_cf_rent'] = 1 if 'cf_rent' in re.sub(" ", "", str(finnal_data['black_idcardDimension_loanTypes'])).split(",") else 0
    finnal_data['black_idcardDimension_loanTypes_is_cf_digital'] = 1 if 'cf_digital' in re.sub(" ", "", str(finnal_data['black_idcardDimension_loanTypes'])).split(",") else 0
    finnal_data['black_idcardDimension_loanTypes_is_cf_car'] = 1 if 'cf_car' in re.sub(" ", "", str(finnal_data['black_idcardDimension_loanTypes'])).split(",") else 0
    finnal_data['black_idcardDimension_loanTypes_is_cf_deco'] = 1 if 'cf_deco' in re.sub(" ", "", str(finnal_data['black_idcardDimension_loanTypes'])).split(",") else 0
    finnal_data['black_idcardDimension_loanTypes_is_cf_tour'] = 1 if 'cf_tour' in re.sub(" ", "", str(finnal_data['black_idcardDimension_loanTypes'])).split(",") else 0
    finnal_data['black_idcardDimension_loanTypes_is_cf_agri'] = 1 if 'cf_agri' in re.sub(" ", "", str(finnal_data['black_idcardDimension_loanTypes'])).split(",") else 0
    finnal_data['black_idcardDimension_loanTypes_is_cf_others'] = 1 if 'cf_others' in re.sub(" ", "", str(finnal_data['black_idcardDimension_loanTypes'])).split(",") else 0
    finnal_data['black_idcardDimension_loanTypes_is_supply_chain'] = 1 if 'supply_chain' in re.sub(" ", "", str(finnal_data['black_idcardDimension_loanTypes'])).split(",") else 0
    finnal_data['black_idcardDimension_loanTypes_is_leasing_company'] = 1 if 'leasing_company' in re.sub(" ", "", str(finnal_data['black_idcardDimension_loanTypes'])).split(",") else 0
    finnal_data['black_idcardDimension_loanTypes_is_car_mortgage'] = 1 if 'car_mortgage' in re.sub(" ", "", str(finnal_data['black_idcardDimension_loanTypes'])).split(",") else 0
    finnal_data['black_idcardDimension_loanTypes_is_house_mortgage'] = 1 if 'house_mortgage' in re.sub(" ", "", str(finnal_data['black_idcardDimension_loanTypes'])).split(",") else 0
    finnal_data['black_idcardDimension_loanTypes_is_credit_card'] = 1 if 'credit_card' in re.sub(" ", "", str(finnal_data['black_idcardDimension_loanTypes'])).split(",") else 0
    finnal_data['black_idcardDimension_loanTypes_is_balance_transfer'] = 1 if 'balance_transfer' in re.sub(" ", "", str(finnal_data['black_idcardDimension_loanTypes'])).split(",") else 0
    finnal_data['black_idcardDimension_loanTypes_is_others'] = 1 if 'others' in re.sub(" ", "", str(finnal_data['black_idcardDimension_loanTypes'])).split(",") else 0
    finnal_data['black_idcardDimension_loanTypes_is_IOU'] = 1 if 'IOU' in re.sub(" ", "", str(finnal_data['black_idcardDimension_loanTypes'])).split(",") else 0

    return finnal_data


def intal_init_bhph(finnal_data):
    finnal_data['bhph_score'] = np.nan
    return finnal_data

def intal_init_shebao(finnal_data):
    finnal_data['shebao_incomeLevel'] = np.nan
    finnal_data['shebao_stabilityLevel'] = np.nan
    finnal_data['shebao_creditLevel'] = np.nan
    return finnal_data


def intal_init_black(finnal_data):

    finnal_data['black_idcardDimension_lastCondition'] = np.nan
    finnal_data['black_idcardDimension_loanTypes'] = np.nan
    finnal_data['black_idcardDimension_level'] = np.nan
    finnal_data['black_idcardDimension_seriousCondition'] = np.nan
    finnal_data['black_idcardDimension_blackOrgNum'] = np.nan
    finnal_data['black_cellphoneDimension_lastCondition'] = np.nan
    finnal_data['black_cellphoneDimension_loanTypes'] = np.nan
    finnal_data['black_cellphoneDimension_level'] = np.nan
    finnal_data['black_cellphoneDimension_seriousCondition'] = np.nan
    finnal_data['black_cellphoneDimension_blackOrgNum'] = np.nan
    finnal_data['black_idcardDimension_lastCondition_new'] = np.nan
    finnal_data['black_cellphoneDimension_lastCondition_new'] = np.nan
    finnal_data['black_cellphoneDimension_seriousCondition_new'] = np.nan
    finnal_data['black_idcardDimension_seriousCondition_new'] = np.nan
    finnal_data['black_cellphoneDimension_loanTypes_is_payday_loan'] = np.nan
    finnal_data['black_cellphoneDimension_loanTypes_is_micro_loan'] = np.nan
    finnal_data['black_cellphoneDimension_loanTypes_is_p2p'] = np.nan
    finnal_data['black_cellphoneDimension_loanTypes_is_cf_edu'] = np.nan
    finnal_data['black_cellphoneDimension_loanTypes_is_cf_medical'] = np.nan
    finnal_data['black_cellphoneDimension_loanTypes_is_cf_rent'] = np.nan
    finnal_data['black_cellphoneDimension_loanTypes_is_cf_digital'] = np.nan
    finnal_data['black_cellphoneDimension_loanTypes_is_cf_car'] = np.nan
    finnal_data['black_cellphoneDimension_loanTypes_is_cf_deco'] = np.nan
    finnal_data['black_cellphoneDimension_loanTypes_is_cf_tour'] = np.nan
    finnal_data['black_cellphoneDimension_loanTypes_is_cf_agri'] = np.nan
    finnal_data['black_cellphoneDimension_loanTypes_is_cf_others'] = np.nan
    finnal_data['black_cellphoneDimension_loanTypes_is_supply_chain'] = np.nan
    finnal_data['black_cellphoneDimension_loanTypes_is_leasing_company'] = np.nan
    finnal_data['black_cellphoneDimension_loanTypes_is_car_mortgage'] = np.nan
    finnal_data['black_cellphoneDimension_loanTypes_is_house_mortgage'] = np.nan
    finnal_data['black_cellphoneDimension_loanTypes_is_credit_card'] = np.nan
    finnal_data['black_cellphoneDimension_loanTypes_is_balance_transfer'] = np.nan
    finnal_data['black_cellphoneDimension_loanTypes_is_others'] = np.nan
    finnal_data['black_cellphoneDimension_loanTypes_is_IOU'] = np.nan
    finnal_data['black_idcardDimension_loanTypes_is_payday_loan'] = np.nan
    finnal_data['black_idcardDimension_loanTypes_is_micro_loan'] = np.nan
    finnal_data['black_idcardDimension_loanTypes_is_p2p'] = np.nan
    finnal_data['black_idcardDimension_loanTypes_is_cf_edu'] = np.nan
    finnal_data['black_idcardDimension_loanTypes_is_cf_medical'] = np.nan
    finnal_data['black_idcardDimension_loanTypes_is_cf_rent'] = np.nan
    finnal_data['black_idcardDimension_loanTypes_is_cf_digital'] = np.nan
    finnal_data['black_idcardDimension_loanTypes_is_cf_car'] = np.nan
    finnal_data['black_idcardDimension_loanTypes_is_cf_deco'] = np.nan
    finnal_data['black_idcardDimension_loanTypes_is_cf_tour'] = np.nan
    finnal_data['black_idcardDimension_loanTypes_is_cf_agri'] = np.nan
    finnal_data['black_idcardDimension_loanTypes_is_cf_others'] = np.nan
    finnal_data['black_idcardDimension_loanTypes_is_supply_chain'] = np.nan
    finnal_data['black_idcardDimension_loanTypes_is_leasing_company'] = np.nan
    finnal_data['black_idcardDimension_loanTypes_is_car_mortgage'] = np.nan
    finnal_data['black_idcardDimension_loanTypes_is_house_mortgage'] = np.nan
    finnal_data['black_idcardDimension_loanTypes_is_credit_card'] = np.nan
    finnal_data['black_idcardDimension_loanTypes_is_balance_transfer'] = np.nan
    finnal_data['black_idcardDimension_loanTypes_is_others'] = np.nan
    finnal_data['black_idcardDimension_loanTypes_is_IOU'] = np.nan

    return finnal_data


def intal_init_duotou(finnal_data):

    finnal_data['duotou_reg_platform_counts'] = np.nan
    finnal_data['duotou_reg_recently_day'] = np.nan
    finnal_data['duotou_reg_history_day'] = np.nan
    finnal_data['duotou_reg_bank_recently_day'] = np.nan
    finnal_data['duotou_reg_bank_history_day'] = np.nan
    finnal_data['duotou_reg_unbank_recently_day'] = np.nan
    finnal_data['duotou_reg_unbank_history_day'] = np.nan
    finnal_data['duotou_app_recently_day'] = np.nan
    finnal_data['duotou_app_history_day'] = np.nan
    finnal_data['duotou_app_counts'] = np.nan
    finnal_data['duotou_app_platform_counts'] = np.nan
    finnal_data['duotou_app_biggest_money'] = np.nan
    finnal_data['duotou_app_biggest_money_recently_day'] = np.nan
    finnal_data['duotou_app_biggest_money_history_day'] = np.nan
    finnal_data['duotou_app_small_money'] = np.nan
    finnal_data['duotou_app_small_money_recently_day'] = np.nan
    finnal_data['duotou_app_small_money_history_day'] = np.nan
    finnal_data['duotou_app_bank_recently_day'] = np.nan
    finnal_data['duotou_app_bank_history_day'] = np.nan
    finnal_data['duotou_app_bank_counts'] = np.nan
    finnal_data['duotou_app_bank_biggest_money'] = np.nan
    finnal_data['duotou_app_bank_biggest_money_recently_day'] = np.nan
    finnal_data['duotou_app_bank_biggest_money_history_day'] = np.nan
    finnal_data['duotou_app_bank_small_money'] = np.nan
    finnal_data['duotou_app_bank_small_money_recently_day'] = np.nan
    finnal_data['duotou_app_bank_small_money_history_day'] = np.nan
    finnal_data['duotou_app_unbank_counts'] = np.nan
    finnal_data['duotou_app_unbank_biggest_money'] = np.nan
    finnal_data['duotou_app_unbank_biggest_money_recently_day'] = np.nan
    finnal_data['duotou_app_unbank_biggest_money_history_day'] = np.nan
    finnal_data['duotou_app_unbank_small_money'] = np.nan
    finnal_data['duotou_app_unbank_small_money_recently_day'] = np.nan
    finnal_data['duotou_app_unbank_small_money_history_day'] = np.nan
    finnal_data['duotou_app_unbank_recently_day'] = np.nan
    finnal_data['duotou_app_unbank_history_day'] = np.nan
    finnal_data['duotou_loan_counts'] = np.nan
    finnal_data['duotou_loan_platform_counts'] = np.nan
    finnal_data['duotou_loan_biggest_money'] = np.nan
    finnal_data['duotou_loan_biggest_money_recently_day'] = np.nan
    finnal_data['duotou_loan_biggest_money_history_day'] = np.nan
    finnal_data['duotou_loan_small_money'] = np.nan
    finnal_data['duotou_loan_small_money_recently_day'] = np.nan
    finnal_data['duotou_loan_small_money_history_day'] = np.nan
    finnal_data['duotou_loan_recently_day'] = np.nan
    finnal_data['duotou_loan_history_day'] = np.nan
    finnal_data['duotou_loan_bank_recently_day'] = np.nan
    finnal_data['duotou_loan_bank_history_day'] = np.nan
    finnal_data['duotou_loan_bank_counts'] = np.nan
    finnal_data['duotou_loan_bank_biggest_money'] = np.nan
    finnal_data['duotou_loan_bank_biggest_money_recently_day'] = np.nan
    finnal_data['duotou_loan_bank_biggest_money_history_day'] = np.nan
    finnal_data['duotou_loan_bank_small_money'] = np.nan
    finnal_data['duotou_loan_bank_small_money_recently_day'] = np.nan
    finnal_data['duotou_loan_bank_small_money_history_day'] = np.nan
    finnal_data['duotou_loan_unbank_counts'] = np.nan
    finnal_data['duotou_loan_unbank_biggest_money'] = np.nan
    finnal_data['duotou_loan_unbank_biggest_money_recently_day'] = np.nan
    finnal_data['duotou_loan_unbank_biggest_money_history_day'] = np.nan
    finnal_data['duotou_loan_unbank_small_money'] = np.nan
    finnal_data['duotou_loan_unbank_small_money_recently_day'] = np.nan
    finnal_data['duotou_loan_unbank_small_money_history_day'] = np.nan
    finnal_data['duotou_loan_unbank_recently_day'] = np.nan
    finnal_data['duotou_loan_unbank_history_day'] = np.nan
    finnal_data['duotou_reject_counts'] = np.nan
    finnal_data['duotou_reject_platform_counts'] = np.nan
    finnal_data['duotou_reject_recently_day'] = np.nan
    finnal_data['duotou_reject_history_day'] = np.nan
    finnal_data['duotou_reject_bank_recently_day'] = np.nan
    finnal_data['duotou_reject_bank_history_day'] = np.nan
    finnal_data['duotou_reject_bank_counts'] = np.nan
    finnal_data['duotou_reject_unbank_counts'] = np.nan
    finnal_data['duotou_reject_unbank_recently_day'] = np.nan
    finnal_data['duotou_reject_unbank_history_day'] = np.nan
    finnal_data['duotou_overdue_recently_day'] = np.nan
    finnal_data['duotou_overdue_history_day'] = np.nan
    finnal_data['duotou_overdue_counts'] = np.nan
    finnal_data['duotou_overdue_platform_counts'] = np.nan
    finnal_data['duotou_overdue_biggest_money'] = np.nan
    finnal_data['duotou_overdue_biggest_money_recently_day'] = np.nan
    finnal_data['duotou_overdue_biggest_money_history_day'] = np.nan
    finnal_data['duotou_overdue_small_money'] = np.nan
    finnal_data['duotou_overdue_small_money_recently_day'] = np.nan
    finnal_data['duotou_overdue_small_money_history_day'] = np.nan
    finnal_data['duotou_arrearage_counts'] = np.nan
    finnal_data['duotou_arrearage_platform_counts'] = np.nan
    finnal_data['duotou_arrearage_biggest_money'] = np.nan
    finnal_data['duotou_arrearage_small_money'] = np.nan
    finnal_data['duotou_reg_platform_week'] = np.nan
    finnal_data['duotou_reg_platform_month'] = np.nan
    finnal_data['duotou_reg_platform_month3'] = np.nan
    finnal_data['duotou_reg_platform_month6'] = np.nan
    finnal_data['duotou_reg_platform_month12'] = np.nan
    finnal_data['duotou_reg_platform_month18'] = np.nan
    finnal_data['duotou_reg_platform_month24'] = np.nan
    finnal_data['duotou_app_week'] = np.nan
    finnal_data['duotou_app_month'] = np.nan
    finnal_data['duotou_app_month3'] = np.nan
    finnal_data['duotou_app_month6'] = np.nan
    finnal_data['duotou_app_month12'] = np.nan
    finnal_data['duotou_app_month18'] = np.nan
    finnal_data['duotou_app_month24'] = np.nan
    finnal_data['duotou_app_platform_week'] = np.nan
    finnal_data['duotou_app_platform_month'] = np.nan
    finnal_data['duotou_app_platform_month3'] = np.nan
    finnal_data['duotou_app_platform_month6'] = np.nan
    finnal_data['duotou_app_platform_month12'] = np.nan
    finnal_data['duotou_app_platform_month18'] = np.nan
    finnal_data['duotou_app_platform_month24'] = np.nan
    finnal_data['duotou_loan_counts_week'] = np.nan
    finnal_data['duotou_loan_counts_month'] = np.nan
    finnal_data['duotou_loan_counts_month3'] = np.nan
    finnal_data['duotou_loan_counts_month6'] = np.nan
    finnal_data['duotou_loan_counts_month12'] = np.nan
    finnal_data['duotou_loan_counts_month18'] = np.nan
    finnal_data['duotou_loan_counts_month24'] = np.nan
    finnal_data['duotou_loan_platform_week'] = np.nan
    finnal_data['duotou_loan_platform_month'] = np.nan
    finnal_data['duotou_loan_platform_month3'] = np.nan
    finnal_data['duotou_loan_platform_month6'] = np.nan
    finnal_data['duotou_loan_platform_month12'] = np.nan
    finnal_data['duotou_loan_platform_month18'] = np.nan
    finnal_data['duotou_loan_platform_month24'] = np.nan
    finnal_data['duotou_reject_week'] = np.nan
    finnal_data['duotou_reject_month'] = np.nan
    finnal_data['duotou_reject_month3'] = np.nan
    finnal_data['duotou_reject_month6'] = np.nan
    finnal_data['duotou_reject_month12'] = np.nan
    finnal_data['duotou_reject_month18'] = np.nan
    finnal_data['duotou_reject_month24'] = np.nan
    finnal_data['duotou_reject_platform_week'] = np.nan
    finnal_data['duotou_reject_platform_month'] = np.nan
    finnal_data['duotou_reject_platform_month3'] = np.nan
    finnal_data['duotou_reject_platform_month6'] = np.nan
    finnal_data['duotou_reject_platform_month12'] = np.nan
    finnal_data['duotou_reject_platform_month18'] = np.nan
    finnal_data['duotou_reject_platform_month24'] = np.nan
    finnal_data['duotou_overdue_week'] = np.nan
    finnal_data['duotou_overdue_month'] = np.nan
    finnal_data['duotou_overdue_month3'] = np.nan
    finnal_data['duotou_overdue_month6'] = np.nan
    finnal_data['duotou_overdue_month12'] = np.nan
    finnal_data['duotou_overdue_month18'] = np.nan
    finnal_data['duotou_overdue_month24'] = np.nan
    finnal_data['duotou_overdue_platform_week'] = np.nan
    finnal_data['duotou_overdue_platform_month'] = np.nan
    finnal_data['duotou_overdue_platform_month3'] = np.nan
    finnal_data['duotou_overdue_platform_month6'] = np.nan
    finnal_data['duotou_overdue_platform_month12'] = np.nan
    finnal_data['duotou_overdue_platform_month18'] = np.nan
    finnal_data['duotou_overdue_platform_month24'] = np.nan

    return finnal_data







