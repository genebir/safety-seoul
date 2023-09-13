from airflow.hooks.base import BaseHook
from plugins.dataframe.cctv import CCTV
import numpy as np
import logging
import pandas as pd


class CCTVHook(BaseHook):

    def __init__(self,
                 path: str,
                 encoding: str = 'utf-8'):
        super().__init__()
        self.cctv = CCTV(path=path,
                         encoding=encoding)
        self.df = self.cctv.df

    def missing_value_repair(self) -> None:
        """
        CCTV_USE 컬럼의 결측치를 '_'로 대체하고, 나머지 값의 띄어쓰기를 제거하고, ~용으로 표기한다.

        :return:
        """

        # step 1
        # 결측치 _로 대체
        self.df['CCTV_USE'].replace([np.nan, '고정식', '회전식'], '_', inplace=True)
        logging.log(level=logging.INFO, msg='Step 1 Complete >>>>>' + self.df['CCTV_USE'].unique())
        # step 2
        # 띄어쓰기 제거
        self.df['CCTV_USE'] = self.df['CCTV_USE'].apply(lambda x: f'{x.replace(" ", "")}용' if x != '_' else x)
        logging.log(level=logging.INFO, msg='Step 2 Complete >>>>>' + self.df['CCTV_USE'].unique())
        logging.log(level=logging.INFO, msg='>>>>>>>>>> Repair Complete <<<<<<<<<<<')

    def mk_integrated(self) -> pd.DataFrame:
        """
        데이터 통합을 위한 형태로 데이터프레임을 재구성한다.

        :return: self.df
        """

        self.df = self.df[['ATNRG_NM',
                           'SAFETY_ADDR',
                           'CCTV_USE',
                           'LA',
                           'LO',
                           'PLCST_NM',
                           'PLCST_DEPT_NM',
                           'PLCST_DEPT_CD']]
        logging.log(level=logging.INFO, msg=self.df)
        self.df.columns = ['ATNRG_NM',
                           'SET_ADDR',
                           'USAGE',
                           'LAT',
                           'LON',
                           'PLCST_NM',
                           'PLCST_DEPT_NM',
                           'PLCST_DEPT_CD']
        logging.log(level=logging.INFO, msg=self.df.columns)
        return self.df