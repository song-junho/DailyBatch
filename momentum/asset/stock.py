from collections import deque
import pandas as pd
from tqdm import tqdm
import pickle
from lib.numeric_pack import *
from lib.stock_pack import *


class Stock:

    def __init__(self, list_date_eom):

        self.dict_df_stock_monthly = {}
        self.list_date_eom = list_date_eom

        with open(r"D:\MyProject\StockPrice\DictDfStock.pickle", 'rb') as fr:
            self.dict_df_stock = pickle.load(fr)

        self.list_cmp_cd = set_all_cmp_cd(list_date_eom[0])

        self.set_chg_period()

    def set_chg_period(self):

        # 변화 period 칼럼 생성 (ex. chg_20)
        list_chg_period = [5, 20, 60, 120, 240]
        for cmp_cd in tqdm(self.list_cmp_cd):

            # call by ref -> self.dict_df_stock 변경됨
            df_stock = self.dict_df_stock[cmp_cd]
            for chg_period in list_chg_period:
                col_nm = "chg_" + str(chg_period)
                df_stock[col_nm] = df_stock["Close"].pct_change(chg_period)

    def run(self, date_eom):

        dict_df_stock_monthly = {}

        for eom in tqdm(date_eom):

            q_ = deque([])
            for cmp_cd in self.dict_df_stock.keys():

                try:
                    df = pd.DataFrame(self.dict_df_stock[cmp_cd].loc[eom]).T
                    df = df[["StockCode", "chg_5", "chg_20", "chg_60", "chg_120", "chg_240"]].rename(
                        columns={"StockCode": "cmp_cd"})
                    q_.append(df)
                except:
                    continue
            if len(q_) == 0:
                continue
            else:
                dict_df_stock_monthly[eom] = pd.concat(q_)

        return dict_df_stock_monthly
