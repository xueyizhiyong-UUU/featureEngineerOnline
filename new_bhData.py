from my_func_0_1 import *


def newBhData(dict_in, dict_out):
    result = dict()
    bh_bh_report = dict_in['bh_bh_report']

    bh_bh_report_customer_home = dict_in['bh_bh_report_customer_home']


    bh_bh_report_customer_work = dict_in['bh_bh_report_customer_work']


    bh_bh_report_loan_non_revolving = dict_in['bh_bh_report_loan_non_revolving']

    bh_bh_report_loan_non_revolving_day_summary = dict_in['bh_bh_report_loan_non_revolving_day_summary']
    bh_bh_report_loan_revolving = dict_in['bh_bh_report_loan_revolving']
    bh_bh_report_loan_revolving_day_summary = dict_in['bh_bh_report_loan_revolving_day_summary']
    bh_bh_report_query_history = dict_in['bh_bh_report_query_history']


    if bh_bh_report_customer_home.empty:
        result['homeAddress_prov'] = np.nan
        result['homeAddress_city'] = np.nan
        result['homeAddress_longi'] = np.nan
        result['homeAddress_lati'] = np.nan
        result['homeAddress_cnt'] = np.nan
        result['homeAddress_prov'] = np.nan
        result['homeAddress_lati'] = np.nan

        result['homeAddress_CHANGE'] = np.nan
        result['homeAddress_prov_CHANGE'] = np.nan
        result['homeAddress_city_CHANGE'] = np.nan
        result['address_cnt_03m'] = np.nan
        result['distinct_longi_03m'] = np.nan
        result['distinct_prov_03m'] = np.nan
        result['distinct_city_03m'] = np.nan
        result['address_cnt_06m'] = np.nan
        result['distinct_longi_06m'] = np.nan
        result['distinct_prov_06m'] = np.nan
        result['distinct_city_06m'] = np.nan
        result['address_cnt_12m'] = np.nan
        result['distinct_longi_12m'] = np.nan
        result['distinct_prov_12m'] = np.nan
        result['distinct_city_12m'] = np.nan
        result['address_cnt_24m'] = np.nan
        result['distinct_longi_24m'] = np.nan
        result['distinct_prov_24m'] = np.nan
        result['distinct_city_24m'] = np.nan

    else:
        bh_bh_report_customer_home = pd.merge(bh_bh_report[['id', 'name', 'idcardNo', 'createTime']].rename(
            columns={'id': 'reportId', 'createTime': 'reportTime'}), bh_bh_report_customer_home, on='reportId',
                                              how='inner')

        bh_bh_report_customer_home = address2gps(address_table=bh_bh_report_customer_home,address_vars=list(['homeAddress']))

        # 注意：month_updateTime_report 这个是个serise
        bh_bh_report_customer_home['month_updateTime_report'] = bh_bh_report_customer_home.apply(lambda x: monthdelta(x['date'], x['reportTime']), axis=1)

        # 家庭住址个数
        result['homeAddress_cnt'] = bh_bh_report_customer_home['homeAddress'].count()

        # 最近家庭住址省份
        result['homeAddress_prov'] =bh_bh_report_customer_home.sort_values(['reportId', 'date'], ascending=[True, False]).groupby('reportId').first()['homeAddress_prov'].values[0]

        # 最近家庭住址城市
        result['homeAddress_city'] = bh_bh_report_customer_home.sort_values(['reportId', 'date'], ascending=[True, False]).groupby('reportId').first()['homeAddress_city'].values[0]

        # 最近家庭住址经度
        result['homeAddress_longi'] = bh_bh_report_customer_home.sort_values(['reportId', 'date'], ascending=[True, False]).groupby('reportId').first()['homeAddress_longi'].values[0]
        # 最近家庭住址纬度
        result['homeAddress_lati'] = bh_bh_report_customer_home.sort_values(['reportId', 'date'], ascending=[True, False]).groupby('reportId').first()['homeAddress_lati'].values[0]
        # 不同家庭住址个数
        result['homeAddress_CHANGE'] = bh_bh_report_customer_home['homeAddress'].nunique()
        # 不同家庭住址省份个数
        result['homeAddress_prov_CHANGE'] = bh_bh_report_customer_home['homeAddress_prov'].nunique()
        # 不同家庭住址城市个数
        result['homeAddress_city_CHANGE'] = bh_bh_report_customer_home['homeAddress_city'].nunique()

        # 近3个月地址记录数
        result['address_cnt_03m'] = bh_bh_report_customer_home['homeAddress'][bh_bh_report_customer_home['month_updateTime_report'] < 3].count()
        # 近3个月不同有效地址数
        result['distinct_longi_03m'] = bh_bh_report_customer_home['homeAddress_longi'][bh_bh_report_customer_home['month_updateTime_report'] < 3].nunique()
        # 近3个月不同省份数
        result['distinct_prov_03m'] = bh_bh_report_customer_home['homeAddress_prov'][bh_bh_report_customer_home['month_updateTime_report'] < 3].nunique()
        # 近3个月不同城市数
        result['distinct_city_03m'] = bh_bh_report_customer_home['homeAddress_city'][bh_bh_report_customer_home['month_updateTime_report'] < 3].nunique()

        # 近6个月地址记录数
        result['address_cnt_06m'] = bh_bh_report_customer_home['homeAddress'][bh_bh_report_customer_home['month_updateTime_report'] < 6].count()
        # 近6个月不同有效地址数
        result['distinct_longi_06m'] = bh_bh_report_customer_home['homeAddress_longi'][bh_bh_report_customer_home['month_updateTime_report'] < 6].nunique()
        # 近6个月不同省份数
        result['distinct_prov_06m'] = bh_bh_report_customer_home['homeAddress_prov'][bh_bh_report_customer_home['month_updateTime_report'] < 6].nunique()
        # 近6个月不同城市数
        result['distinct_city_06m'] = bh_bh_report_customer_home['homeAddress_city'][bh_bh_report_customer_home['month_updateTime_report'] < 6].nunique()

        # 近12个月地址记录数
        result['address_cnt_12m'] = bh_bh_report_customer_home['homeAddress'][bh_bh_report_customer_home['month_updateTime_report'] < 12].count()
        # 近12个月不同有效地址数
        result['distinct_longi_12m'] = bh_bh_report_customer_home['homeAddress_longi'][bh_bh_report_customer_home['month_updateTime_report'] < 12].nunique()
        # 近12个月不同省份数
        result['distinct_prov_12m'] = bh_bh_report_customer_home['homeAddress_prov'][bh_bh_report_customer_home['month_updateTime_report'] < 12].nunique()
        # 近12个月不同城市数
        result['distinct_city_12m'] = bh_bh_report_customer_home['homeAddress_city'][bh_bh_report_customer_home['month_updateTime_report'] < 12].nunique()

        # 近24个月地址记录数
        result['address_cnt_24m'] = bh_bh_report_customer_home['homeAddress'][bh_bh_report_customer_home['month_updateTime_report'] < 24].count()
        # 近24个月不同有效地址数
        result['distinct_longi_24m'] = bh_bh_report_customer_home['homeAddress_longi'][bh_bh_report_customer_home['month_updateTime_report'] < 24].nunique()
        # 近24个月不同省份数
        result['distinct_prov_24m'] = bh_bh_report_customer_home['homeAddress_prov'][bh_bh_report_customer_home['month_updateTime_report'] < 24].nunique()
        # 近24个月不同城市数
        result['distinct_city_24m'] = bh_bh_report_customer_home['homeAddress_city'][bh_bh_report_customer_home['month_updateTime_report'] < 24].nunique()


    if bh_bh_report_customer_work.empty:
        # result['month_updateTime_report'] = np.nan
        result['workName_cnt'] = np.nan
        result['workAddress_prov'] = np.nan
        result['workAddress_city'] = np.nan
        result['workAddress_longi'] = np.nan
        result['workAddress_lati'] = np.nan
        result['workName_CHANGE'] = np.nan
        result['workAddress_prov_CHANGE'] = np.nan
        result['workAddress_city_CHANGE'] = np.nan
        result['workName_prov_CHANGE'] = np.nan
        result['workName_city_CHANGE'] = np.nan
        result['work_month_last'] = np.nan
        result['work_month_all'] = np.nan
        result['first_work_date'] = np.nan
        result['workAddress_cnt_03m'] = np.nan
        result['distinct_workAddress_03m'] = np.nan
        result['distinct_workAddProv_03m'] = np.nan
        result['distinct_workAddCity_03m'] = np.nan
        result['workName_cnt_03m'] = np.nan
        result['distinct_workName_03m'] = np.nan
        result['distinct_workNameProv_03m'] = np.nan
        result['distinct_workNameCity_03m'] = np.nan
        result['workAddress_cnt_06m'] = np.nan
        result['distinct_workAddress_06m'] = np.nan
        result['distinct_workAddProv_06m'] = np.nan
        result['distinct_workAddCity_06m'] = np.nan
        result['workName_cnt_06m'] = np.nan
        result['distinct_workName_06m'] = np.nan
        result['distinct_workNameProv_06m'] = np.nan
        result['distinct_workNameCity_06m'] = np.nan
        result['workAddress_cnt_12m'] = np.nan
        result['distinct_workAddress_12m'] = np.nan
        result['distinct_workAddProv_12m'] = np.nan
        result['distinct_workAddCity_12m'] = np.nan
        result['workName_cnt_12m'] = np.nan
        result['distinct_workName_12m'] = np.nan
        result['distinct_workNameProv_12m'] = np.nan
        result['distinct_workNameCity_12m'] = np.nan
        result['workAddress_cnt_24m'] = np.nan
        result['distinct_workAddress_24m'] = np.nan
        result['distinct_workAddProv_24m'] = np.nan
        result['distinct_workAddCity_24m'] = np.nan
        result['workName_cnt_24m'] = np.nan
        result['distinct_workName_24m'] = np.nan
        result['distinct_workNameProv_24m'] = np.nan
        result['distinct_workNameCity_24m'] = np.nan

    else:
        bh_bh_report_customer_work = address2gps(address_table=bh_bh_report_customer_work,
                                                     address_vars=list(['workName', 'workAddress']))
        bh_bh_report_customer_work = pd.merge(bh_bh_report[['id', 'name', 'idcardNo', 'createTime']].rename(
            columns={'id': 'reportId', 'createTime': 'reportTime'}), bh_bh_report_customer_work, on='reportId', how='inner')
        bh_bh_report_customer_work['month_updateTime_report'] = bh_bh_report_customer_work.apply(
            lambda x: monthdelta(x['date'], x['reportTime']), axis=1)

        # 公司个数
        result['workName_cnt'] = bh_bh_report_customer_work['workName'].count()
        # 最近公司省份
        result['workAddress_prov'] = bh_bh_report_customer_work.sort_values(['reportId', 'date'], ascending=[True, False]).groupby('reportId').first()['workAddress_prov'].values[0]
        # 最近公司城市
        result['workAddress_city'] = bh_bh_report_customer_work.sort_values(['reportId', 'date'], ascending=[True, False]).groupby('reportId').first()['workAddress_city'].values[0]
        # 最近公司经度
        result['workAddress_longi'] = bh_bh_report_customer_work.sort_values(['reportId', 'date'], ascending=[True, False]).groupby('reportId').first()['workAddress_longi'].values[0]
        # 最近公司纬度
        result['workAddress_lati'] = bh_bh_report_customer_work.sort_values(['reportId', 'date'], ascending=[True, False]).groupby('reportId').first()['workAddress_lati'].values[0]
        # 不同公司个数
        result['workName_CHANGE'] = bh_bh_report_customer_work['workName'].nunique()
        # 不同公司省份个数
        result['workAddress_prov_CHANGE'] = bh_bh_report_customer_work['workAddress_prov'].nunique()
        # 不同公司城市个数
        result['workAddress_city_CHANGE'] = bh_bh_report_customer_work['workAddress_city'].nunique()
        # 不同公司name省份个数
        result['workName_prov_CHANGE'] = bh_bh_report_customer_work['workName_prov'].nunique()
        # 不同公司name城市个数
        result['workName_city_CHANGE'] = bh_bh_report_customer_work['workName_city'].nunique()
        # 最近一份工作距报告月份数
        result['work_month_last'] = bh_bh_report_customer_work['month_updateTime_report'].min()
        # 最早一份工作距报告月份数
        result['work_month_all'] = bh_bh_report_customer_work['month_updateTime_report'].max()
        # 最早一份工作日期
        result['first_work_date'] = bh_bh_report_customer_work['date'].min()

        # 近3个月工作地址记录数
        result['workAddress_cnt_03m'] = bh_bh_report_customer_work['workAddress'][bh_bh_report_customer_work['month_updateTime_report'] < 3].count()
        # 近3个月不同有效工作地址数
        result['distinct_workAddress_03m'] = bh_bh_report_customer_work['workAddress'][bh_bh_report_customer_work['month_updateTime_report'] < 3].nunique()
        # 近3个月不同工作省份数
        result['distinct_workAddProv_03m'] = bh_bh_report_customer_work['workAddress_prov'][bh_bh_report_customer_work['month_updateTime_report'] < 3].nunique()
        # 近3个月不同工作城市数
        result['distinct_workAddCity_03m'] = bh_bh_report_customer_work['workAddress_city'][bh_bh_report_customer_work['month_updateTime_report'] < 3].nunique()
        # 近3个月公司记录数
        result['workName_cnt_03m'] = bh_bh_report_customer_work['workName'][bh_bh_report_customer_work['month_updateTime_report'] < 3].count()
        # 近3个月不同有效公司数
        result['distinct_workName_03m'] = bh_bh_report_customer_work['workName'][bh_bh_report_customer_work['month_updateTime_report'] < 3].nunique()
        # 近3个月不同公司省份数
        result['distinct_workNameProv_03m'] = bh_bh_report_customer_work['workName_prov'][bh_bh_report_customer_work['month_updateTime_report'] < 3].nunique()
        # 近3个月不同公司城市数
        result['distinct_workNameCity_03m'] = bh_bh_report_customer_work['workName_city'][bh_bh_report_customer_work['month_updateTime_report'] < 3].nunique()

        # 近6个月工作地址记录数
        result['workAddress_cnt_06m'] = bh_bh_report_customer_work['workAddress'][bh_bh_report_customer_work['month_updateTime_report'] < 6].count()
        # 近6个月不同有效工作地址数
        result['distinct_workAddress_06m'] = bh_bh_report_customer_work['workAddress'][bh_bh_report_customer_work['month_updateTime_report'] < 6].nunique()
        # 近6个月不同工作省份数
        result['distinct_workAddProv_06m'] = bh_bh_report_customer_work['workAddress_prov'][bh_bh_report_customer_work['month_updateTime_report'] < 6].nunique()
        # 近6个月不同工作城市数
        result['distinct_workAddCity_06m'] = bh_bh_report_customer_work['workAddress_city'][bh_bh_report_customer_work['month_updateTime_report'] < 6].nunique()
        # 近6个月公司记录数
        result['workName_cnt_06m'] = bh_bh_report_customer_work['workName'][bh_bh_report_customer_work['month_updateTime_report'] < 6].count()
        # 近6个月不同有效公司数
        result['distinct_workName_06m'] = bh_bh_report_customer_work['workName'][bh_bh_report_customer_work['month_updateTime_report'] < 6].nunique()
        # 近6个月不同公司省份数
        result['distinct_workNameProv_06m'] = bh_bh_report_customer_work['workName_prov'][bh_bh_report_customer_work['month_updateTime_report'] < 6].nunique()
        # 近6个月不同公司城市数
        result['distinct_workNameCity_06m'] = bh_bh_report_customer_work['workName_city'][bh_bh_report_customer_work['month_updateTime_report'] < 6].nunique()

        # 近12个月工作地址记录数
        result['workAddress_cnt_12m'] = bh_bh_report_customer_work['workAddress'][bh_bh_report_customer_work['month_updateTime_report'] < 12].count()
        # 近12个月不同有效工作地址数
        result['distinct_workAddress_12m'] = bh_bh_report_customer_work['workAddress'][bh_bh_report_customer_work['month_updateTime_report'] < 12].nunique()
        # 近12个月不同工作省份数
        result['distinct_workAddProv_12m'] = bh_bh_report_customer_work['workAddress_prov'][bh_bh_report_customer_work['month_updateTime_report'] < 12].nunique()
        # 近12个月不同工作城市数
        result['distinct_workAddCity_12m'] = bh_bh_report_customer_work['workAddress_city'][bh_bh_report_customer_work['month_updateTime_report'] < 12].nunique()
        # 近12个月公司记录数
        result['workName_cnt_12m'] = bh_bh_report_customer_work['workName'][bh_bh_report_customer_work['month_updateTime_report'] < 12].count()
        # 近12个月不同有效公司数
        result['distinct_workName_12m'] = bh_bh_report_customer_work['workName'][bh_bh_report_customer_work['month_updateTime_report'] < 12].nunique()
        # 近12个月不同公司省份数
        result['distinct_workNameProv_12m'] = bh_bh_report_customer_work['workName_prov'][bh_bh_report_customer_work['month_updateTime_report'] < 12].nunique()
        # 近12个月不同公司城市数
        result['distinct_workNameCity_12m'] = bh_bh_report_customer_work['workName_city'][bh_bh_report_customer_work['month_updateTime_report'] < 12].nunique()

        # 近24个月工作地址记录数
        result['workAddress_cnt_24m'] = bh_bh_report_customer_work['workAddress'][bh_bh_report_customer_work['month_updateTime_report'] < 24].count()
        # 近24个月不同有效工作地址数
        result['distinct_workAddress_24m'] = bh_bh_report_customer_work['workAddress'][bh_bh_report_customer_work['month_updateTime_report'] < 24].nunique()
        # 近24个月不同工作省份数
        result['distinct_workAddProv_24m'] = bh_bh_report_customer_work['workAddress_prov'][bh_bh_report_customer_work['month_updateTime_report'] < 24].nunique()
        # 近24个月不同工作城市数
        result['distinct_workAddCity_24m'] = bh_bh_report_customer_work['workAddress_city'][bh_bh_report_customer_work['month_updateTime_report'] < 24].nunique()
        # 近24个月公司记录数
        result['workName_cnt_24m'] = bh_bh_report_customer_work['workName'][bh_bh_report_customer_work['month_updateTime_report'] < 24].count()
        # 近24个月不同有效公司数
        result['distinct_workName_24m'] = bh_bh_report_customer_work['workName'][bh_bh_report_customer_work['month_updateTime_report'] < 24].nunique()
        # 近24个月不同公司省份数
        result['distinct_workNameProv_24m'] = bh_bh_report_customer_work['workName_prov'][bh_bh_report_customer_work['month_updateTime_report'] < 24].nunique()
        # 近24个月不同公司城市数
        result['distinct_workNameCity_24m'] = bh_bh_report_customer_work['workName_city'][bh_bh_report_customer_work['month_updateTime_report'] < 24].nunique()


    bh_bh_report_loan_non_revolving = bh_bh_report_loan_non_revolving.rename(columns={'lastCompensationDate':'lastCompensationDate_nonrev','loanCount':'loanCount_nonrev',"maxOverdueStatus": 'maxOverdueStatus_nonrev',
    "openLoanCount":'openLoanCount_nonrev', "overdueCount":'overdueCount_nonrev',
    "remainingAmount": 'remainingAmount_nonrev', "remainingMaxOverdueStatus":'remainingMaxOverdueStatus_nonrev',"remainingOverdueAmount": 'remainingOverdueAmount_nonrev',
     "remainingOverdueLoanCount":'remainingOverdueLoanCount_nonrev'})


    bh_bh_report_loan_revolving = bh_bh_report_loan_revolving.rename(columns={"accountCount":'accountCount_revol', "creditLimitSum":'creditLimitSum_revol',
                                                                              "maxCreditLimitPerTenant":'maxCreditLimitPerTenant_revol', "maxOverdueStatus":'maxOverdueStatus_revol', "overdueCount":'overdueCount_revol',"remainingAmount": 'remainingAmount_revol', "remainingMaxOverdueStatus":'remainingMaxOverdueStatus_revol', "remainingOverdueAccountCount":'remainingOverdueAccountCount_revol', "remainingOverdueAmount":'remainingOverdueAmount_revol',"revolvingLastCompensationDate":'revolvingLastCompensationDate_revol', "validAccountCount":'validAccountCount_revol'})

    non_revolving_summary = bh_bh_report_loan_non_revolving_day_summary.copy()
    # non_revolving_summary.to_excel('test.xlsx')

    if non_revolving_summary.empty:
        result['non_revolving_loanCount30'] = np.nan
        result['non_revolving_loanTenantCount30'] = np.nan
        result['non_revolving_maxLoanAmount30'] = np.nan
        result['non_revolving_averageLoanAmount30'] = np.nan
        result['non_revolving_overdueLoanCount30'] = np.nan
        result['non_revolving_loanCount90'] = np.nan
        result['non_revolving_loanTenantCount90'] = np.nan
        result['non_revolving_maxLoanAmount90'] = np.nan
        result['non_revolving_averageLoanAmount90'] = np.nan
        result['non_revolving_overdueLoanCount90'] = np.nan
        result['non_revolving_loanCount180'] = np.nan
        result['non_revolving_loanTenantCount180'] = np.nan
        result['non_revolving_maxLoanAmount180'] = np.nan
        result['non_revolving_averageLoanAmount180'] = np.nan
        result['non_revolving_overdueLoanCount180'] = np.nan
        result['non_revolving_loanCount360'] = np.nan
        result['non_revolving_loanTenantCount360'] = np.nan
        result['non_revolving_maxLoanAmount360'] = np.nan
        result['non_revolving_averageLoanAmount360'] = np.nan
        result['non_revolving_overdueLoanCount360'] = np.nan

    else:
        result['non_revolving_loanCount30'] = non_revolving_summary[non_revolving_summary.type == 1]['loanCount'].max()
        result['non_revolving_loanTenantCount30'] = non_revolving_summary[non_revolving_summary.type == 1]['loanTenantCount'].max()
        result['non_revolving_maxLoanAmount30'] = non_revolving_summary[non_revolving_summary.type == 1]['maxLoanAmount'].max()
        result['non_revolving_averageLoanAmount30'] = non_revolving_summary[non_revolving_summary.type == 1]['averageLoanAmount'].max()
        result['non_revolving_overdueLoanCount30'] = non_revolving_summary[non_revolving_summary.type == 1]['overdueLoanCount'].max()
        result['non_revolving_loanCount90'] = non_revolving_summary[non_revolving_summary.type == 2]['loanCount'].max()
        result['non_revolving_loanTenantCount90'] = non_revolving_summary[non_revolving_summary.type == 2]['loanTenantCount'].max()
        result['non_revolving_maxLoanAmount90'] = non_revolving_summary[non_revolving_summary.type == 2]['maxLoanAmount'].max()
        result['non_revolving_averageLoanAmount90'] = non_revolving_summary[non_revolving_summary.type == 2]['averageLoanAmount'].max()
        result['non_revolving_overdueLoanCount90'] = non_revolving_summary[non_revolving_summary.type == 2]['overdueLoanCount'].max()
        result['non_revolving_loanCount180'] = non_revolving_summary[non_revolving_summary.type == 3]['loanCount'].max()
        result['non_revolving_loanTenantCount180'] = non_revolving_summary[non_revolving_summary.type == 3]['loanTenantCount'].max()
        result['non_revolving_maxLoanAmount180'] = non_revolving_summary[non_revolving_summary.type == 3]['maxLoanAmount'].max()
        result['non_revolving_averageLoanAmount180'] = non_revolving_summary[non_revolving_summary.type == 3]['averageLoanAmount'].max()
        result['non_revolving_overdueLoanCount180'] = non_revolving_summary[non_revolving_summary.type == 3]['overdueLoanCount'].max()
        result['non_revolving_loanCount360'] = non_revolving_summary[non_revolving_summary.type == 4]['loanCount'].max()
        result['non_revolving_loanTenantCount360'] = non_revolving_summary[non_revolving_summary.type == 4]['loanTenantCount'].max()
        result['non_revolving_maxLoanAmount360'] = non_revolving_summary[non_revolving_summary.type == 4]['maxLoanAmount'].max()
        result['non_revolving_averageLoanAmount360'] = non_revolving_summary[non_revolving_summary.type == 4]['averageLoanAmount'].max()
        result['non_revolving_overdueLoanCount360'] = non_revolving_summary[non_revolving_summary.type == 4]['overdueLoanCount'].max()

    revolving_summary = bh_bh_report_loan_revolving_day_summary.copy()

    if revolving_summary.empty:

        result['revolving_accountCount30'] = np.nan
        result['revolving_creditLimitSum30'] = np.nan
        result['revolving_lendingAmount30'] = np.nan
        result['revolving_overdueAccountCount30'] = np.nan
        result['revolving_accountCount90'] = np.nan
        result['revolving_creditLimitSum90'] = np.nan
        result['revolving_lendingAmount90'] = np.nan
        result['revolving_overdueAccountCount90'] = np.nan
        result['revolving_accountCount180'] = np.nan
        result['revolving_creditLimitSum180'] = np.nan
        result['revolving_lendingAmount180'] = np.nan
        result['revolving_overdueAccountCount180'] = np.nan
        result['revolving_accountCount360'] = np.nan
        result['revolving_creditLimitSum360'] = np.nan
        result['revolving_lendingAmount360'] = np.nan
        result['revolving_overdueAccountCount360'] = np.nan

    else:
        result['revolving_accountCount30'] = revolving_summary[revolving_summary.type == 1]['accountCount'].max()
        result['revolving_creditLimitSum30'] = revolving_summary[revolving_summary.type == 1]['creditLimitSum'].max()
        result['revolving_lendingAmount30'] = revolving_summary[revolving_summary.type == 1]['lendingAmount'].max()
        result['revolving_overdueAccountCount30'] = revolving_summary[revolving_summary.type == 1]['overdueAccountCount'].max()
        result['revolving_accountCount90'] = revolving_summary[revolving_summary.type == 2]['accountCount'].max()
        result['revolving_creditLimitSum90'] = revolving_summary[revolving_summary.type == 2]['creditLimitSum'].max()
        result['revolving_lendingAmount90'] = revolving_summary[revolving_summary.type == 2]['lendingAmount'].max()
        result['revolving_overdueAccountCount90'] = revolving_summary[revolving_summary.type == 2]['overdueAccountCount'].max()
        result['revolving_accountCount180'] = revolving_summary[revolving_summary.type == 3]['accountCount'].max()
        result['revolving_creditLimitSum180'] = revolving_summary[revolving_summary.type == 3]['creditLimitSum'].max()
        result['revolving_lendingAmount180'] = revolving_summary[revolving_summary.type == 3]['lendingAmount'].max()
        result['revolving_overdueAccountCount180'] = revolving_summary[revolving_summary.type == 3]['overdueAccountCount'].max()
        result['revolving_accountCount360'] = revolving_summary[revolving_summary.type == 4]['accountCount'].max()
        result['revolving_creditLimitSum360'] = revolving_summary[revolving_summary.type == 4]['creditLimitSum'].max()
        result['revolving_lendingAmount360'] = revolving_summary[revolving_summary.type == 4]['lendingAmount'].max()
        result['revolving_overdueAccountCount360'] = revolving_summary[revolving_summary.type == 4]['overdueAccountCount'].max()

    # 1.剔除dataframe形式, 做成字典形式单个加工

    query_history = bh_bh_report_query_history.copy()
    if query_history.empty:

        result['query_6_SUM_24M'] = np.nan
        result['query_1_SUM_12M_ratio_1'] = np.nan
        result['query_SUM_1M_ratio'] = np.nan
        result['query_2_SUM_1M_ratio_1M'] = np.nan
        result['query_3_SUM_3M_ratio_3M'] = np.nan
        result['query_3_SUM_3M'] = np.nan
        result['query_3_SUM'] = np.nan
        result['query_4_SUM_6M_ratio_4'] = np.nan
        result['query_3_SUM_6M_ratio_3'] = np.nan
        result['query_3_SUM_6M'] = np.nan
        result['query_4_SUM_24M_ratio_24M'] = np.nan
        result['query_6_SUM_12M_ratio_6'] = np.nan
        result['query_6_SUM_12M'] = np.nan
        result['query_1_SUM_3M_ratio_3M'] = np.nan
        result['query_2_SUM_6M_ratio_6M'] = np.nan
        result['query_6_SUM_1M_ratio_6'] = np.nan
        result['query_2_SUM_1M'] = np.nan
        result['query_6_SUM_24M_ratio_24M'] = np.nan
        result['query_6_SUM_1M'] = np.nan
        result['query_4_SUM_12M'] = np.nan
        result['query_4_SUM_1M_ratio_4'] = np.nan
        result['query_4_SUM_24M_ratio_4'] = np.nan
        result['query_6_SUM_3M_ratio_6'] = np.nan
        result['query_1_SUM_6M_ratio_6M'] = np.nan
        result['query_3_SUM_24M_ratio_3'] = np.nan
        result['query_1_SUM_3M'] = np.nan
        result['query_2_SUM_24M_ratio_24M'] = np.nan
        result['query_2_SUM_3M_ratio_3M'] = np.nan
        result['query_2_SUM_12M_ratio_12M'] = np.nan
        result['query_2_SUM_3M'] = np.nan
        result['query_1_SUM_24M_ratio_24M'] = np.nan
        result['query_1_SUM_ratio'] = np.nan
        result['query_6_SUM_3M'] = np.nan
        result['query_SUM_12M'] = np.nan
        result['query_4_SUM_12M_ratio_12M'] = np.nan
        result['query_6_SUM_6M_ratio_6'] = np.nan
        result['query_1_SUM_1M'] = np.nan
        result['query_4_SUM_1M_ratio_1M'] = np.nan
        result['query_1_SUM_12M_ratio_12M'] = np.nan
        result['query_SUM_3M'] = np.nan
        result['query_3_SUM_12M_ratio_12M'] = np.nan
        result['query_SUM_24M_ratio'] = np.nan
        result['query_3_SUM_24M'] = np.nan
        result['query_4_SUM_6M_ratio_6M'] = np.nan
        result['query_3_SUM_1M_ratio_3'] = np.nan
        result['query_4_SUM_3M'] = np.nan
        result['query_SUM_24M'] = np.nan
        result['query_1_SUM_3M_ratio_1'] = np.nan
        result['query_1_SUM_24M_ratio_1'] = np.nan
        result['query_2_SUM_3M_ratio_2'] = np.nan
        result['query_2_SUM'] = np.nan
        result['query_2_SUM_12M_ratio_2'] = np.nan
        result['query_3_SUM_6M_ratio_6M'] = np.nan
        result['query_4_SUM_3M_ratio_3M'] = np.nan
        result['query_2_SUM_24M'] = np.nan
        result['query_3_SUM_24M_ratio_24M'] = np.nan
        result['query_6_SUM_ratio'] = np.nan
        result['query_3_SUM_12M'] = np.nan
        result['query_1_SUM_24M'] = np.nan
        result['query_4_SUM_3M_ratio_4'] = np.nan
        result['query_SUM_12M_ratio'] = np.nan
        result['query_3_SUM_1M'] = np.nan
        result['query_4_SUM_ratio'] = np.nan
        result['query_6_SUM'] = np.nan
        result['query_4_SUM_6M'] = np.nan
        result['query_1_SUM_12M'] = np.nan
        result['query_SUM'] = np.nan
        result['query_SUM_6M_ratio'] = np.nan
        result['query_2_SUM_24M_ratio_2'] = np.nan
        result['query_1_SUM_6M'] = np.nan
        result['query_6_SUM_6M'] = np.nan
        result['query_4_SUM_1M'] = np.nan
        result['query_2_SUM_ratio'] = np.nan
        result['query_4_SUM_24M'] = np.nan
        result['query_1_SUM_1M_ratio_1'] = np.nan
        result['query_2_SUM_6M_ratio_2'] = np.nan
        result['query_3_SUM_3M_ratio_3'] = np.nan
        result['query_6_SUM_12M_ratio_12M'] = np.nan
        result['query_2_SUM_6M'] = np.nan
        result['query_3_SUM_1M_ratio_1M'] = np.nan
        result['query_6_SUM_1M_ratio_1M'] = np.nan
        result['query_SUM_1M'] = np.nan
        result['query_3_SUM_ratio'] = np.nan
        result['query_SUM_3M_ratio'] = np.nan
        result['query_1_SUM_6M_ratio_1'] = np.nan
        result['query_6_SUM_24M_ratio_6'] = np.nan
        result['query_2_SUM_12M'] = np.nan
        result['query_6_SUM_6M_ratio_6M'] = np.nan
        result['query_4_SUM_12M_ratio_4'] = np.nan
        result['query_1_SUM'] = np.nan
        result['query_6_SUM_3M_ratio_3M'] = np.nan
        result['query_3_SUM_12M_ratio_3'] = np.nan
        result['query_SUM_6M'] = np.nan
        result['query_1_SUM_1M_ratio_1M'] = np.nan
        result['query_2_SUM_1M_ratio_2'] = np.nan
        result['query_4_SUM'] = np.nan

    else:
        query_history = pd.merge(bh_bh_report[['id', 'name', 'idcardNo', 'createTime']].rename(
            columns={'id': 'reportId', 'createTime': 'reportTime'}), query_history, on='reportId', how='inner')

        query_history['mon_delta'] = query_history.apply(lambda x: monthdelta(x['date'], x['reportTime']), axis=1)

        # query_SUM
        result['query_SUM'] = query_history['reason'].count()

        if 1 in query_history['reason'].unique():

            result['query_1_SUM'] = query_history[query_history.reason == 1]['reason'].count()
        else:

            result['query_1_SUM'] = np.nan

        if 2 in query_history['reason'].unique():
            result['query_2_SUM'] = query_history[query_history.reason == 2]['reason'].count()
        else:
            result['query_2_SUM'] = np.nan

        if 3 in query_history['reason'].unique():
            result['query_3_SUM'] = query_history[query_history.reason == 3]['reason'].count()

        else:
            result['query_3_SUM'] = np.nan

        if 4 in query_history['reason'].unique():
            result['query_4_SUM'] = query_history[query_history.reason == 4]['reason'].count()
        else:
            result['query_4_SUM'] = np.nan

        if 6 in query_history['reason'].unique():
            result['query_6_SUM'] = query_history[query_history.reason == 6]['reason'].count()
        else:
            result['query_6_SUM'] = np.nan

        result['query_1_SUM_ratio'] = result['query_1_SUM'] / result['query_SUM'] if result['query_SUM'] > 0 else np.nan
        result['query_2_SUM_ratio'] = result['query_2_SUM'] / result['query_SUM'] if result['query_SUM'] > 0 else np.nan
        result['query_3_SUM_ratio'] = result['query_3_SUM'] / result['query_SUM'] if result['query_SUM'] > 0 else np.nan
        result['query_4_SUM_ratio'] = result['query_4_SUM'] / result['query_SUM'] if result['query_SUM'] > 0 else np.nan
        result['query_6_SUM_ratio'] = result['query_6_SUM'] / result['query_SUM'] if result['query_SUM'] > 0 else np.nan

        # query_SUM_m
        result['query_SUM_1M'] = query_history[query_history.mon_delta < 1]['reason'].count()
        result['query_SUM_3M'] = query_history[query_history.mon_delta < 3]['reason'].count()
        result['query_SUM_6M'] = query_history[query_history.mon_delta < 6]['reason'].count()
        result['query_SUM_12M'] = query_history[query_history.mon_delta < 12]['reason'].count()
        result['query_SUM_24M'] = query_history[query_history.mon_delta < 24]['reason'].count()

        result['query_SUM_1M_ratio'] = result['query_SUM_1M'] / result['query_SUM'] if result['query_SUM'] > 0 else np.nan
        result['query_SUM_3M_ratio'] = result['query_SUM_3M'] / result['query_SUM'] if result['query_SUM'] > 0 else np.nan
        result['query_SUM_6M_ratio'] = result['query_SUM_6M'] / result['query_SUM'] if result['query_SUM'] > 0 else np.nan
        result['query_SUM_12M_ratio'] = result['query_SUM_12M'] / result['query_SUM'] if result['query_SUM'] > 0 else np.nan
        result['query_SUM_24M_ratio'] = result['query_SUM_24M'] / result['query_SUM'] if result['query_SUM'] > 0 else np.nan

        # query_1
        result['query_1_SUM_1M'] = query_history[(query_history.reason == 1) | (query_history.mon_delta < 1)]['reason'].count()
        result['query_1_SUM_3M'] = query_history[(query_history.reason == 1) | (query_history.mon_delta < 3)]['reason'].count()
        result['query_1_SUM_6M'] = query_history[(query_history.reason == 1) | (query_history.mon_delta < 6)]['reason'].count()
        result['query_1_SUM_12M'] = query_history[(query_history.reason == 1) | (query_history.mon_delta < 12)]['reason'].count()
        result['query_1_SUM_24M'] = query_history[(query_history.reason == 1) | (query_history.mon_delta < 24)]['reason'].count()

        result['query_1_SUM_1M_ratio_1'] = result['query_1_SUM_1M'] / result['query_SUM'] if result['query_SUM'] > 0 else np.nan
        result['query_1_SUM_3M_ratio_1'] = result['query_1_SUM_3M'] / result['query_SUM'] if result['query_SUM'] > 0 else np.nan
        result['query_1_SUM_6M_ratio_1'] = result['query_1_SUM_6M'] / result['query_SUM'] if result['query_SUM'] > 0 else np.nan
        result['query_1_SUM_12M_ratio_1'] = result['query_1_SUM_12M'] / result['query_SUM'] if result['query_SUM'] > 0 else np.nan
        result['query_1_SUM_24M_ratio_1'] = result['query_1_SUM_24M'] / result['query_SUM'] if result['query_SUM'] > 0 else np.nan

        result['query_1_SUM_1M_ratio_1M'] = result['query_1_SUM_1M'] / result['query_SUM_1M'] if result['query_SUM_1M'] > 0 else np.nan
        result['query_1_SUM_3M_ratio_3M'] = result['query_1_SUM_3M'] / result['query_SUM_3M'] if result['query_SUM_3M'] > 0 else np.nan
        result['query_1_SUM_6M_ratio_6M'] = result['query_1_SUM_6M'] / result['query_SUM_6M'] if result['query_SUM_6M'] > 0 else np.nan
        result['query_1_SUM_12M_ratio_12M'] = result['query_1_SUM_12M'] / result['query_SUM_12M'] if result['query_SUM_12M'] > 0 else np.nan
        result['query_1_SUM_24M_ratio_24M'] = result['query_1_SUM_24M'] / result['query_SUM_24M'] if result['query_SUM_24M'] > 0 else np.nan

        # query_2
        result['query_2_SUM_1M'] = query_history[(query_history.reason == 2) | (query_history.mon_delta < 1)]['reason'].count()
        result['query_2_SUM_3M'] = query_history[(query_history.reason == 2) | (query_history.mon_delta < 3)]['reason'].count()
        result['query_2_SUM_6M'] = query_history[(query_history.reason == 2) | (query_history.mon_delta < 6)]['reason'].count()
        result['query_2_SUM_12M'] = query_history[(query_history.reason == 2) | (query_history.mon_delta < 12)]['reason'].count()
        result['query_2_SUM_24M'] = query_history[(query_history.reason == 2) | (query_history.mon_delta < 24)]['reason'].count()

        result['query_2_SUM_1M_ratio_2'] = result['query_2_SUM_1M'] / result['query_2_SUM'] if result['query_2_SUM'] > 0 else np.nan
        result['query_2_SUM_3M_ratio_2'] = result['query_2_SUM_3M'] / result['query_2_SUM'] if result['query_2_SUM'] > 0 else np.nan
        result['query_2_SUM_6M_ratio_2'] = result['query_2_SUM_6M'] / result['query_2_SUM'] if result['query_2_SUM'] > 0 else np.nan
        result['query_2_SUM_12M_ratio_2'] = result['query_2_SUM_12M'] / result['query_2_SUM'] if result['query_2_SUM'] > 0 else np.nan
        result['query_2_SUM_24M_ratio_2'] = result['query_2_SUM_24M'] / result['query_2_SUM'] if result['query_2_SUM'] > 0 else np.nan

        result['query_2_SUM_1M_ratio_1M'] = result['query_2_SUM_1M'] / result['query_SUM_1M'] if result['query_SUM_1M'] > 0 else np.nan
        result['query_2_SUM_3M_ratio_3M'] = result['query_2_SUM_3M'] / result['query_SUM_3M'] if result['query_SUM_3M'] > 0 else np.nan
        result['query_2_SUM_6M_ratio_6M'] = result['query_2_SUM_6M'] / result['query_SUM_6M'] if result['query_SUM_6M'] > 0 else np.nan
        result['query_2_SUM_12M_ratio_12M'] = result['query_2_SUM_12M'] / result['query_SUM_12M'] if result['query_SUM_12M'] > 0 else np.nan
        result['query_2_SUM_24M_ratio_24M'] = result['query_2_SUM_24M'] / result['query_SUM_24M'] if result['query_SUM_24M'] > 0 else np.nan

        # query_3
        result['query_3_SUM_1M'] = query_history[(query_history.reason == 3) | (query_history.mon_delta < 1)]['reason'].count()
        result['query_3_SUM_3M'] = query_history[(query_history.reason == 3) | (query_history.mon_delta < 3)]['reason'].count()
        result['query_3_SUM_6M'] = query_history[(query_history.reason == 3) | (query_history.mon_delta < 6)]['reason'].count()
        result['query_3_SUM_12M'] = query_history[(query_history.reason == 3) | (query_history.mon_delta < 12)]['reason'].count()
        result['query_3_SUM_24M'] = query_history[(query_history.reason == 3) | (query_history.mon_delta < 24)]['reason'].count()

        result['query_3_SUM_1M_ratio_3'] = result['query_3_SUM_1M'] / result['query_3_SUM'] if result['query_3_SUM'] > 0 else np.nan
        result['query_3_SUM_3M_ratio_3'] = result['query_3_SUM_3M'] / result['query_3_SUM'] if result['query_3_SUM'] > 0 else np.nan
        result['query_3_SUM_6M_ratio_3'] = result['query_3_SUM_6M'] / result['query_3_SUM'] if result['query_3_SUM'] > 0 else np.nan
        result['query_3_SUM_12M_ratio_3'] = result['query_3_SUM_12M'] / result['query_3_SUM'] if result['query_3_SUM'] > 0 else np.nan
        result['query_3_SUM_24M_ratio_3'] = result['query_3_SUM_24M'] / result['query_3_SUM'] if result['query_3_SUM'] > 0 else np.nan

        result['query_3_SUM_1M_ratio_1M'] = result['query_3_SUM_1M'] / result['query_SUM_1M'] if result['query_SUM_1M'] > 0 else np.nan
        result['query_3_SUM_3M_ratio_3M'] = result['query_3_SUM_3M'] / result['query_SUM_3M'] if result['query_SUM_3M'] > 0 else np.nan
        result['query_3_SUM_6M_ratio_6M'] = result['query_3_SUM_6M'] / result['query_SUM_6M'] if result['query_SUM_6M'] > 0 else np.nan
        result['query_3_SUM_12M_ratio_12M'] = result['query_3_SUM_12M'] / result['query_SUM_12M'] if result['query_SUM_12M'] > 0 else np.nan
        result['query_3_SUM_24M_ratio_24M'] = result['query_3_SUM_24M'] / result['query_SUM_24M'] if result['query_SUM_24M'] > 0 else np.nan

        # query_4
        result['query_4_SUM_1M'] = query_history[(query_history.reason == 4) | (query_history.mon_delta < 1)]['reason'].count()
        result['query_4_SUM_3M'] = query_history[(query_history.reason == 4) | (query_history.mon_delta < 3)]['reason'].count()
        result['query_4_SUM_6M'] = query_history[(query_history.reason == 4) | (query_history.mon_delta < 6)]['reason'].count()
        result['query_4_SUM_12M'] = query_history[(query_history.reason == 4) | (query_history.mon_delta < 12)]['reason'].count()
        result['query_4_SUM_24M'] = query_history[(query_history.reason == 4) | (query_history.mon_delta < 24)]['reason'].count()

        result['query_4_SUM_1M_ratio_4'] = result['query_4_SUM_1M'] / result['query_4_SUM'] if result['query_4_SUM'] > 0 else np.nan
        result['query_4_SUM_3M_ratio_4'] = result['query_4_SUM_3M'] / result['query_4_SUM'] if result['query_4_SUM'] > 0 else np.nan
        result['query_4_SUM_6M_ratio_4'] = result['query_4_SUM_6M'] / result['query_4_SUM'] if result['query_4_SUM'] > 0 else np.nan
        result['query_4_SUM_12M_ratio_4'] = result['query_4_SUM_12M'] / result['query_4_SUM'] if result['query_4_SUM'] > 0 else np.nan
        result['query_4_SUM_24M_ratio_4'] = result['query_4_SUM_24M'] / result['query_4_SUM'] if result['query_4_SUM'] > 0 else np.nan

        result['query_4_SUM_1M_ratio_1M'] = result['query_4_SUM_1M'] / result['query_SUM_1M'] if result['query_SUM_1M'] > 0 else np.nan
        result['query_4_SUM_3M_ratio_3M'] = result['query_4_SUM_3M'] / result['query_SUM_3M'] if result['query_SUM_3M'] > 0 else np.nan
        result['query_4_SUM_6M_ratio_6M'] = result['query_4_SUM_6M'] / result['query_SUM_6M'] if result['query_SUM_6M'] > 0 else np.nan
        result['query_4_SUM_12M_ratio_12M'] = result['query_4_SUM_12M'] / result['query_SUM_12M'] if result['query_SUM_12M'] > 0 else np.nan
        result['query_4_SUM_24M_ratio_24M'] = result['query_4_SUM_24M'] / result['query_SUM_24M'] if result['query_SUM_24M'] > 0 else np.nan

        # query_6
        result['query_6_SUM_1M'] = query_history[(query_history.reason == 6) | (query_history.mon_delta < 1)]['reason'].count()
        result['query_6_SUM_3M'] = query_history[(query_history.reason == 6) | (query_history.mon_delta < 3)]['reason'].count()
        result['query_6_SUM_6M'] = query_history[(query_history.reason == 6) | (query_history.mon_delta < 6)]['reason'].count()
        result['query_6_SUM_12M'] = query_history[(query_history.reason == 6) | (query_history.mon_delta < 12)]['reason'].count()
        result['query_6_SUM_24M'] = query_history[(query_history.reason == 6) | (query_history.mon_delta < 24)]['reason'].count()

        result['query_6_SUM_1M_ratio_6'] = result['query_6_SUM_1M'] / result['query_6_SUM'] if result['query_6_SUM'] > 0 else np.nan
        result['query_6_SUM_3M_ratio_6'] = result['query_6_SUM_3M'] / result['query_6_SUM'] if result['query_6_SUM'] > 0 else np.nan
        result['query_6_SUM_6M_ratio_6'] = result['query_6_SUM_6M'] / result['query_6_SUM'] if result['query_6_SUM'] > 0 else np.nan
        result['query_6_SUM_12M_ratio_6'] = result['query_6_SUM_12M'] / result['query_6_SUM'] if result['query_6_SUM'] > 0 else np.nan
        result['query_6_SUM_24M_ratio_6'] = result['query_6_SUM_24M'] / result['query_6_SUM'] if result['query_6_SUM'] > 0 else np.nan

        result['query_6_SUM_1M_ratio_1M'] = result['query_6_SUM_1M'] / result['query_SUM_1M'] if result['query_SUM_1M'] > 0 else np.nan
        result['query_6_SUM_3M_ratio_3M'] = result['query_6_SUM_3M'] / result['query_SUM_3M'] if result['query_SUM_3M'] > 0 else np.nan
        result['query_6_SUM_6M_ratio_6M'] = result['query_6_SUM_6M'] / result['query_SUM_6M'] if result['query_SUM_6M'] > 0 else np.nan
        result['query_6_SUM_12M_ratio_12M'] = result['query_6_SUM_12M'] / result['query_SUM_12M'] if result['query_SUM_12M'] > 0 else np.nan
        result['query_6_SUM_24M_ratio_24M'] = result['query_6_SUM_24M'] / result['query_SUM_24M'] if result['query_SUM_24M'] > 0 else np.nan

    return result
