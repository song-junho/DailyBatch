from collections import deque
import pandas as pd
from tqdm import tqdm
import pickle
from lib.numeric_pack import *
from lib.stock_pack import *
from scipy import stats


class Stock:

    def __init__(self, list_date_eom):

        self.dict_df_stock_monthly = {}
        self.list_date_eom = list_date_eom

        with open(r"D:\MyProject\StockPrice\DictDfStock.pickle", 'rb') as fr:
            self.dict_df_stock = pickle.load(fr)

        self.list_cmp_cd = set_all_cmp_cd(list_date_eom[0])

        self.set_chg_period()

    @ staticmethod
    def get_z_score(df):

        start_index = list(df.columns).index('period_0to5')
        for col_nm in df.columns[start_index:start_index+6]:
            df[col_nm] = df[col_nm].astype("float")
            period = col_nm.split("_")[1]
            df["z_score_" + period] = 0
            df = df.fillna(0)

            len_period = int(period.split("to")[1]) - int(period.split("to")[0])

            # 일평균 수익률로 환산 후 z_score
            x = df[col_nm]
            x = (x + 1) ** (1 / len_period) - 1
            z_score = stats.zscore(x)

            # df.loc[~df[col_nm].isna(), "z_score_" + period] = z_score
            df["z_score_" + period] = z_score

        df["z_score_avg"] = df[df.columns[6:]].T.mean()

        return df

    def set_chg_period(self):

        # 변화 period 칼럼 생성 (ex. chg_20)
        list_chg_period = [5, 20, 60, 120, 240]
        for cmp_cd in tqdm(self.list_cmp_cd):

            # call by ref -> self.dict_df_stock 변경됨
            # 1. 누적 기간별 수익률
            df_stock = self.dict_df_stock[cmp_cd]
            for chg_period in list_chg_period:
                col_nm = "chg_" + str(chg_period)
                df_stock[col_nm] = df_stock["Close"].pct_change(chg_period)

            # 2. 구간별 수익률
            df_stock["period_0to5"] = df_stock["Close"].pct_change(list_chg_period[0])
            for i, _ in enumerate(list_chg_period[:-1]):
                col_nm = "period_" + str(list_chg_period[i]) + "to" + str(list_chg_period[i + 1])
                period = list_chg_period[i + 1] - list_chg_period[i]

                df_stock[col_nm] = 0
                df_stock[col_nm][list_chg_period[i]:] = df_stock["Close"][:-list_chg_period[i]].pct_change(period)


    def run(self, date_eom):

        dict_df_stock_monthly = {}

        for eom in tqdm(date_eom):

            q_ = deque([])
            for cmp_cd in self.dict_df_stock.keys():

                try:
                    df = pd.DataFrame(self.dict_df_stock[cmp_cd].loc[eom]).T
                    df = df[["StockCode", "chg_5", "chg_20", "chg_60", "chg_120", "chg_240", 'period_0to5', 'period_5to20', 'period_20to60', 'period_60to120',
       'period_120to240']].rename(
                        columns={"StockCode": "cmp_cd"})
                    q_.append(df)
                except:
                    continue
            if len(q_) == 0:
                continue
            else:
                dict_df_stock_monthly[eom] = pd.concat(q_)

        # 모멘텀 기간별 z_score 칼럼 생성
        for eom in tqdm(date_eom):

            df = dict_df_stock_monthly[eom]
            df = self.get_z_score(df)
            dict_df_stock_monthly[eom] = df

        return dict_df_stock_monthly
