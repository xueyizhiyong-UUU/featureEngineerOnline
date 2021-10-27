import json
from pandas import json_normalize
import pandas as pd
import numpy as np
a = pd.DataFrame([{'aaa':np.nan}])
if a['aaa'].values[0]:
    print(1)
