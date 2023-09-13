import pandas as pd
import numpy as np
from plugins.dataframe.cctv import CCTV


cctv = CCTV(path='data/sample/cctv/SMG_26_20221107_C_001.csv')
df = cctv.df
df['CCTV_USE'].replace([np.nan, '고정식', '회전식'], '_', inplace=True)
df['CCTV_USE'] = df['CCTV_USE'].apply(lambda x: f'{x.replace(" ", "")}용' if x != '_' else x)
print(df['CCTV_USE'].unique())

df = df[['ATNRG_NM', 'SAFETY_ADDR', 'CCTV_USE', 'LA', 'LO', 'PLCST_NM', 'PLCST_DEPT_NM', 'PLCST_DEPT_CD']]
df.columns = [
    'A','B', 'C', 'D', 'E', 'F', 'G', 'H'
]
