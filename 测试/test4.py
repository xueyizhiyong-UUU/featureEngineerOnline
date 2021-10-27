import re
from joblib import Parallel, delayed, load, dump
str_aa = """
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
    finnal_data['shebao_IncomeLevel'] = finnal_data['shebao_IncomeLevel'].apply(lambda x: shebao_handle(x))
    finnal_data['shebao_StabilityLevel'] = finnal_data['shebao_StabilityLevel'].apply(lambda x: shebao_handle(x))
    finnal_data['shebao_CreditLevel'] = finnal_data['shebao_CreditLevel'].apply(lambda x: shebao_handle(x))

    # 黑名单特征工程
    finnal_data['black_idcardDimension_lastCondition_new'] = finnal_data['black_idcardDimension_lastCondition'].apply(lambda x: black_Condition(x))
    finnal_data['black_cellphoneDimension_lastCondition_new'] = finnal_data['black_cellphoneDimension_lastCondition'].apply(lambda x: black_Condition(x))
    finnal_data['black_cellphoneDimension_seriousCondition_new'] = finnal_data['black_cellphoneDimension_seriousCondition'].apply(lambda x: black_Condition(x))
    finnal_data['black_idcardDimension_seriousCondition_new'] = finnal_data['black_idcardDimension_seriousCondition'].apply(lambda x: black_Condition(x))

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

"""



data = load('./New_Third_Service_Data_6month.joblib')
col = data.columns.tolist()
for i in col:
    print("finnal_data['%s'] = np.nan" %i)