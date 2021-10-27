import pandas as pd
import re
rh_feature = pd.read_excel('./人行特征.xlsx')


str_loan = '''
dict_out['loan_second_nowR_'+var+'_range']
'''

feature = list(set(rh_feature['feature'].values.tolist()))
list_feature = []
for i in feature:
    list_re = re.findall(r"^loan_second_nowR_\w+", i)
    if len(list_re) == 0:
        pass
    else:
        list_feature.append(list_re[0])
        print(list_re)

print(list_feature)