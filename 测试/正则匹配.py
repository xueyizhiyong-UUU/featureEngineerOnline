str_loan = """
            if len(loan_second_by12_hdue1)>0:
                for var in numeric_vers:
                    dict_out['loan_second_by12_hdue1_'+var+'_max'] = loan_second_by12_hdue1[var].max() #近12个月截至 历史逾期var最大值
                    dict_out['loan_second_by12_hdue1_'+var+'_min'] = loan_second_by12_hdue1[var].min() #近12个月截至 历史逾期var最小值
                    dict_out['loan_second_by12_hdue1_'+var+'_mean'] = loan_second_by12_hdue1[var].mean() #近12个月截至 历史逾期var平均值
                    dict_out['loan_second_by12_hdue1_'+var+'_sum'] = loan_second_by12_hdue1[var].sum() #近12个月截至 历史逾期var求和
                    dict_out['loan_second_by12_hdue1_'+var+'_range'] = (dict_out['loan_second_by12_hdue1_'+var+'_max']-dict_out['loan_second_by12_hdue1_'+var+'_min'])/dict_out['loan_second_by12_hdue1_'+var+'_max'] if dict_out['loan_second_by12_hdue1_'+var+'_max']>0 else np.nan #近12个月截至 历史逾期var区间率

                dict_out['loan_second_by12_hdue1_loanGrantOrg_giniimpurity'] = giniimpurity(loan_second_by12_hdue1['loanGrantOrg']) #近12个月截至 历史逾期贷款账户管理机构详细基尼不纯度
                dict_out['loan_second_by12_hdue1_loanGrantOrg_nunique'] = loan_second_by12_hdue1['loanGrantOrg'].nunique() #近12个月截至 历史逾期贷款账户管理机构详细nunique

                dict_out['loan_second_by12_hdue1_org_giniimpurity'] = giniimpurity(loan_second_by12_hdue1['org']) #近12个月截至 历史逾期贷款账户管理机构基尼不纯度
                dict_out['loan_second_by12_hdue1_org_commercial_bank_count'] = loan_second_by12_hdue1[loan_second_by12_hdue1['org']=='商业银行'].shape[0] #近12个月截至 历史逾期贷款账户管理机构为商业银行 计数
                dict_out['loan_second_by12_hdue1_org_commercial_bank_ratio'] = loan_second_by12_hdue1[loan_second_by12_hdue1['org']=='商业银行'].shape[0]/len(loan_second_by12_hdue1) #近12个月截至 历史逾期贷款账户管理机构为商业银行 占比
                dict_out['loan_second_by12_hdue1_org_consumer_finance_count'] = loan_second_by12_hdue1[loan_second_by12_hdue1['org']=='消费金融公司'].shape[0] #近12个月截至 历史逾期贷款账户管理机构为消费金融公司 计数
                dict_out['loan_second_by12_hdue1_org_consumer_finance_ratio'] = loan_second_by12_hdue1[loan_second_by12_hdue1['org']=='消费金融公司'].shape[0]/len(loan_second_by12_hdue1) #近12个月截至 历史逾期贷款账户管理机构为消费金融公司 占比
                dict_out['loan_second_by12_hdue1_org_micro_loan_count'] = loan_second_by12_hdue1[loan_second_by12_hdue1['org']=='小额贷款公司'].shape[0] #近12个月截至 历史逾期贷款账户管理机构为小额贷款公司 计数
                dict_out['loan_second_by12_hdue1_org_micro_loan_ratio'] = loan_second_by12_hdue1[loan_second_by12_hdue1['org']=='小额贷款公司'].shape[0]/len(loan_second_by12_hdue1) #近12个月截至 历史逾期贷款账户管理机构为小额贷款公司 占比
                dict_out['loan_second_by12_hdue1_org_other_count'] = loan_second_by12_hdue1[loan_second_by12_hdue1['org']=='其他机构'].shape[0] #近12个月截至 历史逾期贷款账户管理机构为其他机构 计数
                dict_out['loan_second_by12_hdue1_org_other_ratio'] = loan_second_by12_hdue1[loan_second_by12_hdue1['org']=='其他机构'].shape[0]/len(loan_second_by12_hdue1) #近12个月截至 历史逾期贷款账户管理机构为其他机构 占比
                dict_out['loan_second_by12_hdue1_org_trust_company_count'] = loan_second_by12_hdue1[loan_second_by12_hdue1['org']=='信托公司'].shape[0] #近12个月截至 历史逾期贷款账户管理机构为信托公司 计数
                dict_out['loan_second_by12_hdue1_org_trust_company_ratio'] = loan_second_by12_hdue1[loan_second_by12_hdue1['org']=='信托公司'].shape[0]/len(loan_second_by12_hdue1) #近12个月截至 历史逾期贷款账户管理机构为信托公司 占比
                dict_out['loan_second_by12_hdue1_org_car_finance_count'] = loan_second_by12_hdue1[loan_second_by12_hdue1['org']=='汽车金融公司'].shape[0] #近12个月截至 历史逾期贷款账户管理机构为汽车金融公司 计数
                dict_out['loan_second_by12_hdue1_org_car_finance_ratio'] = loan_second_by12_hdue1[loan_second_by12_hdue1['org']=='汽车金融公司'].shape[0]/len(loan_second_by12_hdue1) #近12个月截至 历史逾期贷款账户管理机构为汽车金融公司 占比
                dict_out['loan_second_by12_hdue1_org_lease_finance_count'] = loan_second_by12_hdue1[loan_second_by12_hdue1['org']=='融资租赁公司'].shape[0] #近12个月截至 历史逾期贷款账户管理机构为融资租赁公司 计数
                dict_out['loan_second_by12_hdue1_org_lease_finance_ratio'] = loan_second_by12_hdue1[loan_second_by12_hdue1['org']=='融资租赁公司'].shape[0]/len(loan_second_by12_hdue1) #近12个月截至 历史逾期贷款账户管理机构为融资租赁公司 占比
                dict_out['loan_second_by12_hdue1_org_myself_count'] = loan_second_by12_hdue1[loan_second_by12_hdue1['org']==dict_in['cc_rh_report'].queryOperator.values[0]].shape[0] #近12个月截至 历史逾期贷款账户管理机构为本机构 计数
                dict_out['loan_second_by12_hdue1_org_myself_ratio'] = loan_second_by12_hdue1[loan_second_by12_hdue1['org']==dict_in['cc_rh_report'].queryOperator.values[0]].shape[0]/len(loan_second_by12_hdue1) #近12个月截至 历史逾期贷款账户管理机构为本机构 占比
                dict_out['loan_second_by12_hdue1_org_village_banks_count'] = loan_second_by12_hdue1[loan_second_by12_hdue1['org']=='村镇银行'].shape[0] #近12个月截至 历史逾期贷款账户管理机构为村镇银行 计数
                dict_out['loan_second_by12_hdue1_org_village_banks_ratio'] = loan_second_by12_hdue1[loan_second_by12_hdue1['org']=='村镇银行'].shape[0]/len(loan_second_by12_hdue1) #近12个月截至 历史逾期贷款账户管理机构为村镇银行 占比
                dict_out['loan_second_by12_hdue1_org_finance_company_count'] = loan_second_by12_hdue1[loan_second_by12_hdue1['org']=='财务公司'].shape[0] #近12个月截至 历史逾期贷款账户管理机构为财务公司 计数
                dict_out['loan_second_by12_hdue1_org_finance_company_ratio'] = loan_second_by12_hdue1[loan_second_by12_hdue1['org']=='财务公司'].shape[0]/len(loan_second_by12_hdue1) #近12个月截至 历史逾期贷款账户管理机构为财务公司 占比
                dict_out['loan_second_by12_hdue1_org_foreign_banks_count'] = loan_second_by12_hdue1[loan_second_by12_hdue1['org']=='外资银行'].shape[0] #近12个月截至 历史逾期贷款账户管理机构为外资银行 计数
                dict_out['loan_second_by12_hdue1_org_foreign_banks_ratio'] = loan_second_by12_hdue1[loan_second_by12_hdue1['org']=='外资银行'].shape[0]/len(loan_second_by12_hdue1) #近12个月截至 历史逾期贷款账户管理机构为外资银行 占比
                dict_out['loan_second_by12_hdue1_org_provident_fund_count'] = loan_second_by12_hdue1[loan_second_by12_hdue1['org']=='公积金管理中心'].shape[0] #近12个月截至 历史逾期贷款账户管理机构为公积金管理中心 计数
                dict_out['loan_second_by12_hdue1_org_provident_fund_ratio'] = loan_second_by12_hdue1[loan_second_by12_hdue1['org']=='公积金管理中心'].shape[0]/len(loan_second_by12_hdue1) #近12个月截至 历史逾期贷款账户管理机构为公积金管理中心 占比
                dict_out['loan_second_by12_hdue1_org_securities_firm_count'] = loan_second_by12_hdue1[loan_second_by12_hdue1['org']=='证券公司'].shape[0] #近12个月截至 历史逾期贷款账户管理机构为证券公司 计数
                dict_out['loan_second_by12_hdue1_org_securities_firm_ratio'] = loan_second_by12_hdue1[loan_second_by12_hdue1['org']=='证券公司'].shape[0]/len(loan_second_by12_hdue1) #近12个月截至 历史逾期贷款账户管理机构为证券公司 占比

                dict_out['loan_second_by12_hdue1_class_giniimpurity'] = giniimpurity(loan_second_by12_hdue1['class']) #近12个月截至 历史逾期贷款账户账户类别基尼不纯度
                dict_out['loan_second_by12_hdue1_class_ncycle_count'] = loan_second_by12_hdue1[loan_second_by12_hdue1['class']=='非循环贷账户'].shape[0] #近12个月截至 历史逾期贷款账户账户类别为非循环贷账户 计数
                dict_out['loan_second_by12_hdue1_class_ncycle_ratio'] = loan_second_by12_hdue1[loan_second_by12_hdue1['class']=='非循环贷账户'].shape[0]/len(loan_second_by12_hdue1) #近12个月截至 历史逾期贷款账户账户类别为非循环贷账户 占比
                dict_out['loan_second_by12_hdue1_class_cycle_sub_count'] = loan_second_by12_hdue1[loan_second_by12_hdue1['class']=='循环额度下分账户'].shape[0] #近12个月截至 历史逾期贷款账户账户类别为循环额度下分账户 计数
                dict_out['loan_second_by12_hdue1_class_cycle_sub_ratio'] = loan_second_by12_hdue1[loan_second_by12_hdue1['class']=='循环额度下分账户'].shape[0]/len(loan_second_by12_hdue1) #近12个月截至 历史逾期贷款账户账户类别为循环额度下分账户 占比
                dict_out['loan_second_by12_hdue1_class_cycle_count'] = loan_second_by12_hdue1[loan_second_by12_hdue1['class']=='循环贷账户'].shape[0] #近12个月截至 历史逾期贷款账户账户类别为循环贷账户 计数
                dict_out['loan_second_by12_hdue1_class_cycle_ratio'] = loan_second_by12_hdue1[loan_second_by12_hdue1['class']=='循环贷账户'].shape[0]/len(loan_second_by12_hdue1) #近12个月截至 历史逾期贷款账户账户类别为循环贷账户 占比

                dict_out['loan_second_by12_hdue1_classify5_giniimpurity'] = giniimpurity(loan_second_by12_hdue1['classify5']) #近12个月截至 历史逾期贷款账户五级分类基尼不纯度
                dict_out['loan_second_by12_hdue1_c5_unknow_count'] = loan_second_by12_hdue1[loan_second_by12_hdue1['classify5']==''].shape[0] #近12个月截至 历史逾期贷款账户五级分类为'' 计数
                dict_out['loan_second_by12_hdue1_c5_unknow_ratio'] = loan_second_by12_hdue1[loan_second_by12_hdue1['classify5']==''].shape[0]/len(loan_second_by12_hdue1) #近12个月截至 历史逾期贷款账户五级分类为'' 占比
                dict_out['loan_second_by12_hdue1_c5_normal_count'] = loan_second_by12_hdue1[loan_second_by12_hdue1['classify5']=='正常'].shape[0] #近12个月截至 历史逾期贷款账户五级分类为正常 计数
                dict_out['loan_second_by12_hdue1_c5_normal_ratio'] = loan_second_by12_hdue1[loan_second_by12_hdue1['classify5']=='正常'].shape[0]/len(loan_second_by12_hdue1) #近12个月截至 历史逾期贷款账户五级分类为正常 占比
                dict_out['loan_second_by12_hdue1_c5_loss_count'] = loan_second_by12_hdue1[loan_second_by12_hdue1['classify5']=='损失'].shape[0] #近12个月截至 历史逾期贷款账户五级分类为损失 计数
                dict_out['loan_second_by12_hdue1_c5_loss_ratio'] = loan_second_by12_hdue1[loan_second_by12_hdue1['classify5']=='损失'].shape[0]/len(loan_second_by12_hdue1) #近12个月截至 历史逾期贷款账户五级分类为损失 占比
                dict_out['loan_second_by12_hdue1_c5_attention_count'] = loan_second_by12_hdue1[loan_second_by12_hdue1['classify5']=='关注'].shape[0] #近12个月截至 历史逾期贷款账户五级分类为关注 计数
                dict_out['loan_second_by12_hdue1_c5_attention_ratio'] = loan_second_by12_hdue1[loan_second_by12_hdue1['classify5']=='关注'].shape[0]/len(loan_second_by12_hdue1) #近12个月截至 历史逾期贷款账户五级分类为关注 占比
                dict_out['loan_second_by12_hdue1_c5_suspicious_count'] = loan_second_by12_hdue1[loan_second_by12_hdue1['classify5']=='可疑'].shape[0] #近12个月截至 历史逾期贷款账户五级分类为可疑 计数
                dict_out['loan_second_by12_hdue1_c5_suspicious_ratio'] = loan_second_by12_hdue1[loan_second_by12_hdue1['classify5']=='可疑'].shape[0]/len(loan_second_by12_hdue1) #近12个月截至 历史逾期贷款账户五级分类为可疑 占比
                dict_out['loan_second_by12_hdue1_c5_secondary_count'] = loan_second_by12_hdue1[loan_second_by12_hdue1['classify5']=='次级'].shape[0] #近12个月截至 历史逾期贷款账户五级分类为次级 计数
                dict_out['loan_second_by12_hdue1_c5_secondary_ratio'] = loan_second_by12_hdue1[loan_second_by12_hdue1['classify5']=='次级'].shape[0]/len(loan_second_by12_hdue1) #近12个月截至 历史逾期贷款账户五级分类为次级 占比
                dict_out['loan_second_by12_hdue1_c5_noclass_count'] = loan_second_by12_hdue1[loan_second_by12_hdue1['classify5']=='未分类'].shape[0] #近12个月截至 历史逾期贷款账户五级分类为未分类 计数
                dict_out['loan_second_by12_hdue1_c5_noclass_ratio'] = loan_second_by12_hdue1[loan_second_by12_hdue1['classify5']=='未分类'].shape[0]/len(loan_second_by12_hdue1) #近12个月截至 历史逾期贷款账户五级分类为未分类 占比

                dict_out['loan_second_by12_hdue1_accountStatus_giniimpurity'] = giniimpurity(loan_second_by12_hdue1['accountStatus']) #近12个月截至 历史逾期贷款账户账户状态基尼不纯度
                dict_out['loan_second_by12_hdue1_as_settle_count'] = loan_second_by12_hdue1[loan_second_by12_hdue1['accountStatus']=='结清'].shape[0] #近12个月截至 历史逾期贷款账户账户状态为结清 计数
                dict_out['loan_second_by12_hdue1_as_settle_ratio'] = loan_second_by12_hdue1[loan_second_by12_hdue1['accountStatus']=='结清'].shape[0]/len(loan_second_by12_hdue1) #近12个月截至 历史逾期贷款账户账户状态为结清 占比
                dict_out['loan_second_by12_hdue1_as_normal_count'] = loan_second_by12_hdue1[loan_second_by12_hdue1['accountStatus']=='正常'].shape[0] #近12个月截至 历史逾期贷款账户账户状态为正常 计数
                dict_out['loan_second_by12_hdue1_as_normal_ratio'] = loan_second_by12_hdue1[loan_second_by12_hdue1['accountStatus']=='正常'].shape[0]/len(loan_second_by12_hdue1) #近12个月截至 历史逾期贷款账户账户状态为正常 占比
                dict_out['loan_second_by12_hdue1_as_overdue_count'] = loan_second_by12_hdue1[loan_second_by12_hdue1['accountStatus']=='逾期'].shape[0] #近12个月截至 历史逾期贷款账户账户状态为逾期 计数
                dict_out['loan_second_by12_hdue1_as_overdue_ratio'] = loan_second_by12_hdue1[loan_second_by12_hdue1['accountStatus']=='逾期'].shape[0]/len(loan_second_by12_hdue1) #近12个月截至 历史逾期贷款账户账户状态为逾期 占比
                dict_out['loan_second_by12_hdue1_as_bad_debts_count'] = loan_second_by12_hdue1[loan_second_by12_hdue1['accountStatus']=='呆账'].shape[0] #近12个月截至 历史逾期贷款账户账户状态为呆账 计数
                dict_out['loan_second_by12_hdue1_as_bad_debts_ratio'] = loan_second_by12_hdue1[loan_second_by12_hdue1['accountStatus']=='呆账'].shape[0]/len(loan_second_by12_hdue1) #近12个月截至 历史逾期贷款账户账户状态为呆账 占比
                dict_out['loan_second_by12_hdue1_as_unknow_count'] = loan_second_by12_hdue1[loan_second_by12_hdue1['accountStatus']==''].shape[0] #近12个月截至 历史逾期贷款账户账户状态为'' 计数
                dict_out['loan_second_by12_hdue1_as_unknow_ratio'] = loan_second_by12_hdue1[loan_second_by12_hdue1['accountStatus']==''].shape[0]/len(loan_second_by12_hdue1) #近12个月截至 历史逾期贷款账户账户状态为'' 占比
                dict_out['loan_second_by12_hdue1_as_roll_out_count'] = loan_second_by12_hdue1[loan_second_by12_hdue1['accountStatus']=='转出'].shape[0] #近12个月截至 历史逾期贷款账户账户状态为转出 计数
                dict_out['loan_second_by12_hdue1_as_roll_out_ratio'] = loan_second_by12_hdue1[loan_second_by12_hdue1['accountStatus']=='转出'].shape[0]/len(loan_second_by12_hdue1) #近12个月截至 历史逾期贷款账户账户状态为转出 占比

                dict_out['loan_second_by12_hdue1_repayType_giniimpurity'] = giniimpurity(loan_second_by12_hdue1['repayType']) #近12个月截至 历史逾期贷款账户还款方式基尼不纯度
                dict_out['loan_second_by12_hdue1_rt_unknow_count'] = loan_second_by12_hdue1[loan_second_by12_hdue1['repayType']=='--'].shape[0] #近12个月截至 历史逾期贷款账户还款方式为-- 计数
                dict_out['loan_second_by12_hdue1_rt_unknow_ratio'] = loan_second_by12_hdue1[loan_second_by12_hdue1['repayType']=='--'].shape[0]/len(loan_second_by12_hdue1) #近12个月截至 历史逾期贷款账户还款方式为-- 占比
                dict_out['loan_second_by12_hdue1_rt_equality_count'] = loan_second_by12_hdue1[loan_second_by12_hdue1['repayType']=='分期等额本息'].shape[0] #近12个月截至 历史逾期贷款账户还款方式为分期等额本息 计数
                dict_out['loan_second_by12_hdue1_rt_equality_ratio'] = loan_second_by12_hdue1[loan_second_by12_hdue1['repayType']=='分期等额本息'].shape[0]/len(loan_second_by12_hdue1) #近12个月截至 历史逾期贷款账户还款方式为分期等额本息 占比
                dict_out['loan_second_by12_hdue1_rt_onschedule_count'] = loan_second_by12_hdue1[loan_second_by12_hdue1['repayType']=='按期计算还本付息'].shape[0] #近12个月截至 历史逾期贷款账户还款方式为按期计算还本付息 计数
                dict_out['loan_second_by12_hdue1_rt_onschedule_ratio'] = loan_second_by12_hdue1[loan_second_by12_hdue1['repayType']=='按期计算还本付息'].shape[0]/len(loan_second_by12_hdue1) #近12个月截至 历史逾期贷款账户还款方式为按期计算还本付息 占比
                dict_out['loan_second_by12_hdue1_rt_not_distinguish_count'] = loan_second_by12_hdue1[loan_second_by12_hdue1['repayType']=='不区分还款方式'].shape[0] #近12个月截至 历史逾期贷款账户还款方式为不区分还款方式 计数
                dict_out['loan_second_by12_hdue1_rt_not_distinguish_ratio'] = loan_second_by12_hdue1[loan_second_by12_hdue1['repayType']=='不区分还款方式'].shape[0]/len(loan_second_by12_hdue1) #近12个月截至 历史逾期贷款账户还款方式为不区分还款方式 占比
                dict_out['loan_second_by12_hdue1_rt_circulation_count'] = loan_second_by12_hdue1[loan_second_by12_hdue1['repayType']=='循环贷款下其他还款方式'].shape[0] #近12个月截至 历史逾期贷款账户还款方式为循环贷款下其他还款方式 计数
                dict_out['loan_second_by12_hdue1_rt_circulation_ratio'] = loan_second_by12_hdue1[loan_second_by12_hdue1['repayType']=='循环贷款下其他还款方式'].shape[0]/len(loan_second_by12_hdue1) #近12个月截至 历史逾期贷款账户还款方式为循环贷款下其他还款方式 占比
                dict_out['loan_second_by12_hdue1_rt_once_count'] = loan_second_by12_hdue1[loan_second_by12_hdue1['repayType']=='到期一次还本付息'].shape[0] #近12个月截至 历史逾期贷款账户还款方式为到期一次还本付息 计数
                dict_out['loan_second_by12_hdue1_rt_once_ratio'] = loan_second_by12_hdue1[loan_second_by12_hdue1['repayType']=='到期一次还本付息'].shape[0]/len(loan_second_by12_hdue1) #近12个月截至 历史逾期贷款账户还款方式为到期一次还本付息 占比

                dict_out['loan_second_by12_hdue1_repayFrequency_giniimpurity'] = giniimpurity(loan_second_by12_hdue1['repayFrequency']) #近12个月截至 历史逾期贷款账户还款频率基尼不纯度
                dict_out['loan_second_by12_hdue1_rf_month_count'] = loan_second_by12_hdue1[loan_second_by12_hdue1['repayFrequency']=='月'].shape[0] #近12个月截至 历史逾期贷款账户还款频率为月 计数
                dict_out['loan_second_by12_hdue1_rf_month_ratio'] = loan_second_by12_hdue1[loan_second_by12_hdue1['repayFrequency']=='月'].shape[0]/len(loan_second_by12_hdue1) #近12个月截至 历史逾期贷款账户还款频率为月 占比
                dict_out['loan_second_by12_hdue1_rf_once_count'] = loan_second_by12_hdue1[loan_second_by12_hdue1['repayFrequency']=='一次性'].shape[0] #近12个月截至 历史逾期贷款账户还款频率为一次性 计数
                dict_out['loan_second_by12_hdue1_rf_once_ratio'] = loan_second_by12_hdue1[loan_second_by12_hdue1['repayFrequency']=='一次性'].shape[0]/len(loan_second_by12_hdue1) #近12个月截至 历史逾期贷款账户还款频率为一次性 占比
                dict_out['loan_second_by12_hdue1_rf_other_count'] = loan_second_by12_hdue1[loan_second_by12_hdue1['repayFrequency']=='其他'].shape[0] #近12个月截至 历史逾期贷款账户还款频率为其他 计数
                dict_out['loan_second_by12_hdue1_rf_other_ratio'] = loan_second_by12_hdue1[loan_second_by12_hdue1['repayFrequency']=='其他'].shape[0]/len(loan_second_by12_hdue1) #近12个月截至 历史逾期贷款账户还款频率为其他 占比
                dict_out['loan_second_by12_hdue1_rf_irregular_count'] = loan_second_by12_hdue1[loan_second_by12_hdue1['repayFrequency']=='不定期'].shape[0] #近12个月截至 历史逾期贷款账户还款频率为不定期 计数
                dict_out['loan_second_by12_hdue1_rf_irregular_ratio'] = loan_second_by12_hdue1[loan_second_by12_hdue1['repayFrequency']=='不定期'].shape[0]/len(loan_second_by12_hdue1) #近12个月截至 历史逾期贷款账户还款频率为不定期 占比
                dict_out['loan_second_by12_hdue1_rf_day_count'] = loan_second_by12_hdue1[loan_second_by12_hdue1['repayFrequency']=='日'].shape[0] #近12个月截至 历史逾期贷款账户还款频率为日 计数
                dict_out['loan_second_by12_hdue1_rf_day_ratio'] = loan_second_by12_hdue1[loan_second_by12_hdue1['repayFrequency']=='日'].shape[0]/len(loan_second_by12_hdue1) #近12个月截至 历史逾期贷款账户还款频率为日 占比
                dict_out['loan_second_by12_hdue1_rf_year_count'] = loan_second_by12_hdue1[loan_second_by12_hdue1['repayFrequency']=='年'].shape[0] #近12个月截至 历史逾期贷款账户还款频率为年 计数
                dict_out['loan_second_by12_hdue1_rf_year_ratio'] = loan_second_by12_hdue1[loan_second_by12_hdue1['repayFrequency']=='年'].shape[0]/len(loan_second_by12_hdue1) #近12个月截至 历史逾期贷款账户还款频率为年 占比
                dict_out['loan_second_by12_hdue1_rf_season_count'] = loan_second_by12_hdue1[loan_second_by12_hdue1['repayFrequency']=='季'].shape[0] #近12个月截至 历史逾期贷款账户还款频率为季 计数
                dict_out['loan_second_by12_hdue1_rf_season_ratio'] = loan_second_by12_hdue1[loan_second_by12_hdue1['repayFrequency']=='季'].shape[0]/len(loan_second_by12_hdue1) #近12个月截至 历史逾期贷款账户还款频率为季 占比
                dict_out['loan_second_by12_hdue1_rf_week_count'] = loan_second_by12_hdue1[loan_second_by12_hdue1['repayFrequency']=='周'].shape[0] #近12个月截至 历史逾期贷款账户还款频率为周 计数
                dict_out['loan_second_by12_hdue1_rf_week_ratio'] = loan_second_by12_hdue1[loan_second_by12_hdue1['repayFrequency']=='周'].shape[0]/len(loan_second_by12_hdue1) #近12个月截至 历史逾期贷款账户还款频率为周 占比
                dict_out['loan_second_by12_hdue1_rf_halfyear_count'] = loan_second_by12_hdue1[loan_second_by12_hdue1['repayFrequency']=='半年'].shape[0] #近12个月截至 历史逾期贷款账户还款频率为半年 计数
                dict_out['loan_second_by12_hdue1_rf_halfyear_ratio'] = loan_second_by12_hdue1[loan_second_by12_hdue1['repayFrequency']=='半年'].shape[0]/len(loan_second_by12_hdue1) #近12个月截至 历史逾期贷款账户还款频率为半年 占比

                dict_out['loan_second_by12_hdue1_guaranteeForm_giniimpurity'] = giniimpurity(loan_second_by12_hdue1['guaranteeForm']) #近12个月截至 历史逾期贷款账户担保方式基尼不纯度
                dict_out['loan_second_by12_hdue1_gf_crdit_count'] = loan_second_by12_hdue1[loan_second_by12_hdue1['guaranteeForm']=='信用/免担保'].shape[0] #近12个月截至 历史逾期贷款账户担保方式为其信用/免担保 计数
                dict_out['loan_second_by12_hdue1_gf_crdit_ratio'] = loan_second_by12_hdue1[loan_second_by12_hdue1['guaranteeForm']=='信用/免担保'].shape[0]/len(loan_second_by12_hdue1) #近12个月截至 历史逾期贷款账户担保方式为信用/免担保 占比
                dict_out['loan_second_by12_hdue1_gf_other_count'] = loan_second_by12_hdue1[loan_second_by12_hdue1['guaranteeForm']=='其他'].shape[0] #近12个月截至 历史逾期贷款账户担保方式为其他 计数
                dict_out['loan_second_by12_hdue1_gf_other_ratio'] = loan_second_by12_hdue1[loan_second_by12_hdue1['guaranteeForm']=='其他'].shape[0]/len(loan_second_by12_hdue1) #近12个月截至 历史逾期贷款账户担保方式为其他 占比
                dict_out['loan_second_by12_hdue1_gf_combine_nowarranty_count'] = loan_second_by12_hdue1[loan_second_by12_hdue1['guaranteeForm']=='组合（不含保证）'].shape[0] #近12个月截至 历史逾期贷款账户担保方式为组合（不含保证） 计数
                dict_out['loan_second_by12_hdue1_gf_combine_nowarranty_ratio'] = loan_second_by12_hdue1[loan_second_by12_hdue1['guaranteeForm']=='组合（不含保证）'].shape[0]/len(loan_second_by12_hdue1) #近12个月截至 历史逾期贷款账户担保方式为组合（不含保证） 占比
                dict_out['loan_second_by12_hdue1_gf_combine_warranty_count'] = loan_second_by12_hdue1[loan_second_by12_hdue1['guaranteeForm']=='组合(含保证)'].shape[0] #近12个月截至 历史逾期贷款账户担保方式为组合(含保证) 计数
                dict_out['loan_second_by12_hdue1_gf_combine_warranty_ratio'] = loan_second_by12_hdue1[loan_second_by12_hdue1['guaranteeForm']=='组合(含保证)'].shape[0]/len(loan_second_by12_hdue1) #近12个月截至 历史逾期贷款账户担保方式为组合(含保证) 占比
                dict_out['loan_second_by12_hdue1_gf_mortgage_count'] = loan_second_by12_hdue1[loan_second_by12_hdue1['guaranteeForm']=='抵押'].shape[0] #近12个月截至 历史逾期贷款账户担保方式为抵押 计数
                dict_out['loan_second_by12_hdue1_gf_mortgage_ratio'] = loan_second_by12_hdue1[loan_second_by12_hdue1['guaranteeForm']=='抵押'].shape[0]/len(loan_second_by12_hdue1) #近12个月截至 历史逾期贷款账户担保方式为抵押 占比
                dict_out['loan_second_by12_hdue1_gf_warranty_count'] = loan_second_by12_hdue1[loan_second_by12_hdue1['guaranteeForm']=='保证'].shape[0] #近12个月截至 历史逾期贷款账户担保方式为保证计数
                dict_out['loan_second_by12_hdue1_gf_warranty_ratio'] = loan_second_by12_hdue1[loan_second_by12_hdue1['guaranteeForm']=='保证'].shape[0]/len(loan_second_by12_hdue1) #近12个月截至 历史逾期贷款账户担保方式为保证 占比
                dict_out['loan_second_by12_hdue1_gf_pledge_count'] = loan_second_by12_hdue1[loan_second_by12_hdue1['guaranteeForm']=='质押'].shape[0] #近12个月截至 历史逾期贷款账户担保方式为质押 计数
                dict_out['loan_second_by12_hdue1_gf_pledge_ratio'] = loan_second_by12_hdue1[loan_second_by12_hdue1['guaranteeForm']=='质押'].shape[0]/len(loan_second_by12_hdue1) #近12个月截至 历史逾期贷款账户担保方式为质押 占比
                dict_out['loan_second_by12_hdue1_gf_farm_group_count'] = loan_second_by12_hdue1[loan_second_by12_hdue1['guaranteeForm']=='农户联保'].shape[0] #近12个月截至 历史逾期贷款账户担保方式为农户联保 计数
                dict_out['loan_second_by12_hdue1_gf_farm_group_ratio'] = loan_second_by12_hdue1[loan_second_by12_hdue1['guaranteeForm']=='农户联保'].shape[0]/len(loan_second_by12_hdue1) #近12个月截至 历史逾期贷款账户担保方式为农户联保 占比

                dict_out['loan_second_by12_hdue1_businessType_giniimpurity'] = giniimpurity(loan_second_by12_hdue1['businessType']) #近12个月截至 历史逾期贷款账户业务种类基尼不纯度
                dict_out['loan_second_by12_hdue1_bt_other_person_count'] = loan_second_by12_hdue1[loan_second_by12_hdue1['businessType']=='其他个人消费贷款'].shape[0] #近12个月截至 历史逾期贷款账户业务种类为其他个人消费贷款 计数
                dict_out['loan_second_by12_hdue1_bt_other_person_ratio'] = loan_second_by12_hdue1[loan_second_by12_hdue1['businessType']=='其他个人消费贷款'].shape[0]/len(loan_second_by12_hdue1) #近12个月截至 历史逾期贷款账户业务种类为其他个人消费贷款 占比
                dict_out['loan_second_by12_hdue1_bt_other_loan_count'] = loan_second_by12_hdue1[loan_second_by12_hdue1['businessType']=='其他贷款'].shape[0] #近12个月截至 历史逾期贷款账户业务种类为其他贷款 计数
                dict_out['loan_second_by12_hdue1_bt_other_loan_ratio'] = loan_second_by12_hdue1[loan_second_by12_hdue1['businessType']=='其他贷款'].shape[0]/len(loan_second_by12_hdue1) #近12个月截至 历史逾期贷款账户业务种类为其他贷款 占比
                dict_out['loan_second_by12_hdue1_bt_person_business_count'] = loan_second_by12_hdue1[loan_second_by12_hdue1['businessType']=='个人经营性贷款'].shape[0] #近12个月截至 历史逾期贷款账户业务种类为个人经营性贷款 计数
                dict_out['loan_second_by12_hdue1_bt_person_business_ratio'] = loan_second_by12_hdue1[loan_second_by12_hdue1['businessType']=='个人经营性贷款'].shape[0]/len(loan_second_by12_hdue1) #近12个月截至 历史逾期贷款账户业务种类为个人经营性贷款 占比
                dict_out['loan_second_by12_hdue1_bt_farm_loan_count'] = loan_second_by12_hdue1[loan_second_by12_hdue1['businessType']=='农户贷款'].shape[0] #近12个月截至 历史逾期贷款账户业务种类为农户贷款 计数
                dict_out['loan_second_by12_hdue1_bt_farm_loan_ratio'] = loan_second_by12_hdue1[loan_second_by12_hdue1['businessType']=='农户贷款'].shape[0]/len(loan_second_by12_hdue1) #近12个月截至 历史逾期贷款账户业务种类为农户贷款 占比
                dict_out['loan_second_by12_hdue1_bt_person_car_count'] = loan_second_by12_hdue1[loan_second_by12_hdue1['businessType']=='个人汽车消费贷款'].shape[0] #近12个月截至 历史逾期贷款账户业务种类为个人汽车消费贷款 计数
                dict_out['loan_second_by12_hdue1_bt_person_car_ratio'] = loan_second_by12_hdue1[loan_second_by12_hdue1['businessType']=='个人汽车消费贷款'].shape[0]/len(loan_second_by12_hdue1) #近12个月截至 历史逾期贷款账户业务种类为个人汽车消费贷款 占比
                dict_out['loan_second_by12_hdue1_bt_person_study_count'] = loan_second_by12_hdue1[loan_second_by12_hdue1['businessType']=='个人助学贷款'].shape[0] #近12个月截至 历史逾期贷款账户业务种类为个人助学贷款 计数
                dict_out['loan_second_by12_hdue1_bt_person_study_ratio'] = loan_second_by12_hdue1[loan_second_by12_hdue1['businessType']=='个人助学贷款'].shape[0]/len(loan_second_by12_hdue1) #近12个月截至 历史逾期贷款账户业务种类为个人助学贷款 占比
                dict_out['loan_second_by12_hdue1_bt_house_commercial_count'] = loan_second_by12_hdue1[loan_second_by12_hdue1['businessType']=='个人住房商业贷款'].shape[0] #近12个月截至 历史逾期贷款账户业务种类为个人住房商业贷款 计数
                dict_out['loan_second_by12_hdue1_bt_house_commercial_ratio'] = loan_second_by12_hdue1[loan_second_by12_hdue1['businessType']=='个人住房商业贷款'].shape[0]/len(loan_second_by12_hdue1) #近12个月截至 历史逾期贷款账户业务种类为个人住房商业贷款 占比
                dict_out['loan_second_by12_hdue1_bt_finance_lease_count'] = loan_second_by12_hdue1[loan_second_by12_hdue1['businessType']=='融资租赁业务'].shape[0] #近12个月截至 历史逾期贷款账户业务种类为融资租赁业务 计数
                dict_out['loan_second_by12_hdue1_bt_finance_lease_ratio'] = loan_second_by12_hdue1[loan_second_by12_hdue1['businessType']=='融资租赁业务'].shape[0]/len(loan_second_by12_hdue1) #近12个月截至 历史逾期贷款账户业务种类为融资租赁业务 占比
                dict_out['loan_second_by12_hdue1_bt_house_fund_count'] = loan_second_by12_hdue1[loan_second_by12_hdue1['businessType']=='个人住房公积金贷款'].shape[0] #近12个月截至 历史逾期贷款账户业务种类为个人住房公积金贷款 计数
                dict_out['loan_second_by12_hdue1_bt_house_fund_ratio'] = loan_second_by12_hdue1[loan_second_by12_hdue1['businessType']=='个人住房公积金贷款'].shape[0]/len(loan_second_by12_hdue1) #近12个月截至 历史逾期贷款账户业务种类为个人住房公积金贷款 占比
                dict_out['loan_second_by12_hdue1_bt_person_house_count'] = loan_second_by12_hdue1[loan_second_by12_hdue1['businessType']=='个人商用房（含商住两用）贷款'].shape[0] #近12个月截至 历史逾期贷款账户业务种类为个人商用房（含商住两用）贷款 计数
                dict_out['loan_second_by12_hdue1_bt_person_house_ratio'] = loan_second_by12_hdue1[loan_second_by12_hdue1['businessType']=='个人商用房（含商住两用）贷款'].shape[0]/len(loan_second_by12_hdue1) #近12个月截至 历史逾期贷款账户业务种类为个人商用房（含商住两用）贷款 占比
                dict_out['loan_second_by12_hdue1_bt_stock_pledge_count'] = loan_second_by12_hdue1[loan_second_by12_hdue1['businessType']=='股票质押式回购交易'].shape[0] #近12个月截至 历史逾期贷款账户业务种类为股票质押式回购交易计数
                dict_out['loan_second_by12_hdue1_bt_stock_pledge_ratio'] = loan_second_by12_hdue1[loan_second_by12_hdue1['businessType']=='股票质押式回购交易'].shape[0]/len(loan_second_by12_hdue1) #近12个月截至 历史逾期贷款账户业务种类为股票质押式回购交易 占比

                #近12个月截至 历史逾期率
                for var in numeric_vers:
                    dict_out['loan_second_by12_hdue1R_'+var+'_max'] = dict_out['loan_second_by12_hdue1_'+var+'_max']/dict_out['loan_second_by12_'+var+'_max'] if dict_out['loan_second_by12_'+var+'_max']>0 else np.nan #近12个月截至 历史逾期var最大值比率
                    dict_out['loan_second_by12_hdue1R_'+var+'_min'] = dict_out['loan_second_by12_hdue1_'+var+'_min']/dict_out['loan_second_by12_'+var+'_min'] if dict_out['loan_second_by12_'+var+'_min']>0 else np.nan #近12个月截至 历史逾期var最小值比率
                    dict_out['loan_second_by12_hdue1R_'+var+'_mean'] = dict_out['loan_second_by12_hdue1_'+var+'_mean']/dict_out['loan_second_by12_'+var+'_mean'] if dict_out['loan_second_by12_'+var+'_mean']>0 else np.nan  #近12个月截至 历史逾期var平均值比率
                    dict_out['loan_second_by12_hdue1R_'+var+'_sum'] = dict_out['loan_second_by12_hdue1_'+var+'_sum']/dict_out['loan_second_by12_'+var+'_sum'] if dict_out['loan_second_by12_'+var+'_sum']>0 else np.nan  #近12个月截至 历史逾期var求和比率
                    dict_out['loan_second_by12_hdue1R_'+var+'_range'] = dict_out['loan_second_by12_hdue1_'+var+'_range']/dict_out['loan_second_by12_'+var+'_range'] if dict_out['loan_second_by12_'+var+'_range']>0 else np.nan  #近12个月截至 历史逾期var区间率比率
                for var in loopvars:
                    dict_out['loan_second_by12_hdue1R_'+var] = dict_out['loan_second_by12_hdue1_'+var]/dict_out['loan_second_by12_'+var] if dict_out['loan_second_by12_'+var]>0 else np.nan #近12个月截至 历史逾期var最大值比率
"""

import re

numeric_vers = ['repayedAmount', 'repayTerm_ratio', 'balance_ratio', 'RepayedAmount_ratio', 'repayAmt',
                'repayMons_ratio', 'loanAmount', 'repayTerms', 'balance', 'leftRepayTerms', 'planRepayAmount',
                'RepayedAmount', 'currentOverdueTerms', 'currentOverdueAmount', 'overdue31Amount', 'overdue61Amount',
                'overdue91Amount', 'overdue180Amount', 'startDate_to_report', 'byDate_to_report', 'is_now', 'is_vouch',
                'repayMons', 'classify5_num', 'logo', 'due_class', 'month60_to_report_min', 'month60_to_report_mean',
                'month60_to_report_max', 'month60_State_num_size', 'month60_State_num_sum', 'month60_State_num_max',
                'month60_State_num_mean', 'month60_State_num_meanbig0', 'month60_Amount_num_sum',
                'month60_Amount_num_max', 'month60_Amount_num_mean', 'month60_Amount_num_meanbig0',
                'month60_State_countN', 'month60_State_countNr', 'month60_State_countC', 'month60_State_countCr',
                'month60_State_countD', 'month60_State_countDr', 'month60_State_countG', 'month60_State_countGr',
                'month60_State_countNull', 'month60_State_countNullr', 'month60_State_countUnknow',
                'month60_State_countUnknowr', 'month60_State_count0', 'month60_State_count0r', 'month60_State_count1',
                'month60_State_count1r', 'month60_State_count2', 'month60_State_count2r', 'month60_State_count3',
                'month60_State_count3r', 'month60_State_count4', 'month60_State_count4r', 'month60_State_count5',
                'month60_State_count5r', 'month60_State_count6', 'month60_State_count6r', 'month60_State_count7',
                'month60_State_count7r']
loopvars = ['loanGrantOrg_giniimpurity', 'loanGrantOrg_nunique', 'org_giniimpurity', 'org_commercial_bank_count',
            'org_commercial_bank_ratio', 'org_consumer_finance_count', 'org_consumer_finance_ratio',
            'org_micro_loan_count', 'org_micro_loan_ratio', 'org_other_count', 'org_other_ratio',
            'org_trust_company_count', 'org_trust_company_ratio', 'org_car_finance_count', 'org_car_finance_ratio',
            'org_lease_finance_count', 'org_lease_finance_ratio', 'org_myself_count', 'org_myself_ratio',
            'org_village_banks_count', 'org_village_banks_ratio', 'org_finance_company_count',
            'org_finance_company_ratio', 'org_foreign_banks_count', 'org_foreign_banks_ratio',
            'org_provident_fund_count', 'org_provident_fund_ratio', 'org_securities_firm_count',
            'org_securities_firm_ratio', 'class_giniimpurity', 'class_ncycle_count', 'class_ncycle_ratio',
            'class_cycle_sub_count', 'class_cycle_sub_ratio', 'class_cycle_count', 'class_cycle_ratio',
            'classify5_giniimpurity', 'c5_unknow_count', 'c5_unknow_ratio', 'c5_normal_count', 'c5_normal_ratio',
            'c5_loss_count', 'c5_loss_ratio', 'c5_attention_count', 'c5_attention_ratio', 'c5_suspicious_count',
            'c5_suspicious_ratio', 'c5_secondary_count', 'c5_secondary_ratio', 'c5_noclass_count', 'c5_noclass_ratio',
            'accountStatus_giniimpurity', 'as_settle_count', 'as_settle_ratio', 'as_normal_count', 'as_normal_ratio',
            'as_overdue_count', 'as_overdue_ratio', 'as_bad_debts_count', 'as_bad_debts_ratio', 'as_unknow_count',
            'as_unknow_ratio', 'as_roll_out_count', 'as_roll_out_ratio', 'repayType_giniimpurity', 'rt_unknow_count',
            'rt_unknow_ratio', 'rt_equality_count', 'rt_equality_ratio', 'rt_onschedule_count', 'rt_onschedule_ratio',
            'rt_not_distinguish_count', 'rt_not_distinguish_ratio', 'rt_circulation_count', 'rt_circulation_ratio',
            'rt_once_count', 'rt_once_ratio', 'repayFrequency_giniimpurity', 'rf_month_count', 'rf_month_ratio',
            'rf_once_count', 'rf_once_ratio', 'rf_other_count', 'rf_other_ratio', 'rf_irregular_count',
            'rf_irregular_ratio', 'rf_day_count', 'rf_day_ratio', 'rf_year_count', 'rf_year_ratio', 'rf_season_count',
            'rf_season_ratio', 'rf_week_count', 'rf_week_ratio', 'rf_halfyear_count', 'rf_halfyear_ratio',
            'guaranteeForm_giniimpurity', 'gf_crdit_count', 'gf_crdit_ratio', 'gf_other_count', 'gf_other_ratio',
            'gf_combine_nowarranty_count', 'gf_combine_nowarranty_ratio', 'gf_combine_warranty_count',
            'gf_combine_warranty_ratio', 'gf_mortgage_count', 'gf_mortgage_ratio', 'gf_warranty_count',
            'gf_warranty_ratio', 'gf_pledge_count', 'gf_pledge_ratio', 'gf_farm_group_count', 'gf_farm_group_ratio',
            'businessType_giniimpurity', 'bt_other_person_count', 'bt_other_person_ratio', 'bt_other_loan_count',
            'bt_other_loan_ratio', 'bt_person_business_count', 'bt_person_business_ratio', 'bt_farm_loan_count',
            'bt_farm_loan_ratio', 'bt_person_car_count', 'bt_person_car_ratio', 'bt_person_study_count',
            'bt_person_study_ratio', 'bt_house_commercial_count', 'bt_house_commercial_ratio', 'bt_finance_lease_count',
            'bt_finance_lease_ratio', 'bt_house_fund_count', 'bt_house_fund_ratio', 'bt_person_house_count',
            'bt_person_house_ratio', 'bt_stock_pledge_count', 'bt_stock_pledge_ratio']
import pandas as pd

# 1.先得到列表中的 特征名
rh_feature = pd.read_excel('./人行特征.xlsx')
feature = list(set(rh_feature['feature'].values.tolist()))

# todo//:测试
# 先列举出 特征的多种情况
# feature = ['loan_second_now_gf_other_ratio', 'loan_second_now_repayMons_range',
#            'loan_second_nowR_gf_crdit_count', 'loan_second_nowR_repayedAmount_range',
#            'loan_second_nowR_loanGrantOrg_giniimpurity']
# example 1: loan_second_now_gf_other_ratio --- 直接就可以在 代码中匹配到的

# example 2: loan_second_now_planRepayAmount_mean -- 直接无法匹配 需要自己构造
# dict_out['loan_second_now_'+var+'_mean'] = loan_second_now[var].mean() #现行var平均值

# example 3: loan_second_nowR_gf_crdit_count -- 直接无法匹配 需要自己构造, 已经可以构造 dict_out['loan_second_now_gf_crdit_count] 特征
# dict_out['loan_second_nowR_'+var] = dict_out['loan_second_now_'+var]/dict_out['loan_second_'+var] if dict_out['loan_second_'+var]>0 else np.nan #现行var最大值比率

# example 4: loan_second_nowR_repayedAmount_mean -- 直接无法匹配 需要自己构造  dict_out['loan_second_now_repayedAmount_mean']
# dict_out['loan_second_nowR_'+var+'_mean'] = dict_out['loan_second_now_'+var+'_mean']/dict_out['loan_second_'+var+'_mean'] if dict_out['loan_second_'+var+'_mean']>0 else np.nan  #现行var平均值比率


# 1.1暂时只取前半部分，不带R的
var_front = "loan_second_by12_hdue1"  # 指定前缀
var_frontR = "loan_second_by12_hdue1R"  # 指定前缀
list_feature = []
for i in feature:
    list_re = re.findall(r"^"+var_front+"_\w+", i)
    if len(list_re) == 0:
        pass
    else:
        list_feature.append(list_re[0])
        # print(list_re)


# 1.3 在获取字符串中匹配的 行
for var in list_feature:

    list_print = re.findall(r"dict_out\['"+var+"'].*", str_loan)
    # 在字符串中没有找到对应的 代码
    if len(list_print) == 0:
        min_sum_max_var = var[23:-4]
        mean_var = var[23:-5]
        range_var = var[23:-6]

        # print(min_sum_max_var)

        if 'max' in var[-6:]:

            shuchu = "dict_out['"+var_front+"_"+min_sum_max_var+"_max'] = "+var_front+"['"+min_sum_max_var+"'].max()#var最大值"
            print(shuchu)
        elif 'mean' in var[-6:]:
            shuchu = "dict_out['"+var_front+"_"+mean_var+"_mean'] = "+var_front+"['"+mean_var+"'].mean() #var平均值"
            print(shuchu)
        elif 'min' in var[-6:]:
            shuchu = "dict_out['"+var_front+"_"+min_sum_max_var+"_min'] = "+var_front+"['"+min_sum_max_var+"'].min() #var最小值"
            print(shuchu)
        elif 'sum' in var[-6:]:
            shuchu = "dict_out['"+var_front+"_"+min_sum_max_var+"_sum'] = "+var_front+"['"+min_sum_max_var+"'].sum() #var求和"
            print(shuchu)
        elif 'range' in var[-6:]:
            shuchu = "dict_out['"+var_front+"_"+range_var+"_max'] = "+var_front+"['"+range_var+"'].max()#var最大值"
            print(shuchu)
            shuchu = "dict_out['"+var_front+"_"+range_var+"_min'] = "+var_front+"['"+range_var+"'].min() #var最小值"
            print(shuchu)
            shuchu = "dict_out['"+var_front+"_"+range_var+"_range'] = (dict_out['"+var_front+"_"+range_var+"_max']-dict_out['"+var_front+"_"+range_var+"_min'])/dict_out['"+var_front+"_"+range_var+"_max'] if dict_out['"+var_front+"_"+range_var+"_max']>0 else np.nan #var区间率"
            print(shuchu)

    # 找到对应的 代码输出
    else:
        print(list_print[0])

# 2.1取带R的 特征特殊 再加工
list_feature_R = []
for i in feature:
    list_re = re.findall(r"^"+var_frontR+"_\w+", i)
    if len(list_re) == 0:
        pass
    else:
        list_feature_R.append(list_re[0])

list_feature_R_new = []  # 前置特征 例子：loan_second_now_bt_other_person_ratio

# 短的部分特征 例子：rf_once_count
list_feature_short = []
# 2.2 需要对list_feature_R 内增加一些 特征
for var in list_feature_R:

    # 对特征切片 返回 分开后的 列表
    # print(var)
    var_loopvars = var.split('R_')[1]
    list_feature_short.append(var_loopvars)
    # 如果数据在 loopvars 列表中 那么造一个特征
    if var_loopvars in loopvars:
        var_structure = ''+var_front+'_' + var_loopvars
        # 添加数据到新列表
        list_feature_R_new.append(var_structure)

list_feature_R_2 = list_feature_R_new + list_feature_R # 全部需要跑出来的数据
# print(list_feature_R_2)
# print(list_feature_short)


for var in list_feature_R_2:

    list_print = re.findall(r"dict_out\['"+var+"'].*", str_loan)
    # 在字符串中没有找到对应的 代码
    if len(list_print) == 0:

        min_sum_max_var = var[24:-4]
        mean_var = var[24:-5]
        range_var = var[24:-6]
        # 还需要再加一层判断 判断这个 var 是在 numeric_vers 还是在 loopvars
        # print(min_sum_max_var)
        if 'max' in var[-6:]:
            shuchu = "dict_out['"+var_front+"_"+min_sum_max_var+"_max'] = "+var_front+"['"+min_sum_max_var+"'].max() #var最大值比率"
            print(shuchu)
        elif 'mean' in var[-6:]:
            shuchu = "dict_out['"+var_front+"_"+mean_var+"_mean'] = "+var_front+"['"+mean_var+"'].mean()  #var平均值比率"
            print(shuchu)
        elif 'min' in var[-6:]:
            shuchu = "dict_out['"+var_front+"_"+min_sum_max_var+"_min'] = "+var_front+"['"+min_sum_max_var+"'].min() #var最小值比率"
            print(shuchu)
        elif 'sum' in var[-6:]:
            shuchu = "dict_out['"+var_front+"_"+min_sum_max_var+"_sum'] = "+var_front+"['"+min_sum_max_var+"'].sum() #var求和比率"
            print(shuchu)
        elif 'range' in var[-6:]:
            shuchu = "dict_out['"+var_front+"_"+range_var+"_max'] = "+var_front+"['"+range_var+"'].max()#var最大值"
            print(shuchu)
            shuchu = "dict_out['"+var_front+"_"+range_var+"_min'] = "+var_front+"['"+range_var+"'].min() #var最小值比率"
            print(shuchu)
            shuchu = "dict_out['"+var_front+"_" + range_var + "_range'] = (dict_out['"+var_front+"_" + range_var + "_max']-dict_out['"+var_front+"_" + range_var + "_min'])/dict_out['"+var_front+"_" + range_var + "_max'] if dict_out['"+var_front+"_" + range_var + "_max']>0 else np.nan #var区间率"
            print(shuchu)

    # 找到对应的 代码输出
    else:
        print(list_print[0])

# 2.3最后输出 前置后加工的 特征

for var in list_feature_short:
    list_print = re.findall(r"dict_out\['"+var_front+"_"+var+"'].*", str_loan)
    # 在字符串中没有找到对应的 代码
    if len(list_print) == 0:
        shuchu = "dict_out['"+var_frontR+"_"+var+"'] = dict_out['"+var_front+"_"+var+"']/dict_out['loan_second_by12_"+var+"'] if dict_out['loan_second_by12_"+var+"']>0 else np.nan #var最大值比率"
        print(shuchu)
    # 找到对应的 代码输出
    else:
        print(list_print[0])
        shuchu = "dict_out['"+var_frontR+"_"+var+"'] = dict_out['"+var_front+"_"+var+"']/dict_out['loan_second_by12_"+var+"'] if dict_out['loan_second_by12_"+var+"']>0 else np.nan #var最大值比率"
        print(shuchu)

