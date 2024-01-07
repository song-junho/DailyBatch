import pickle
import datetime
import pandas as pd
from tqdm import tqdm
from lib import numeric_pack, stock_pack


class UpdateHotIssue:

    def get_df_stock_highlight(self, df_stock):
        '''
        종목 내 주요 주목일자 필터링
        '''

        df_stock["ma_60"] = df_stock["V_Value"].rolling(window=60, closed="left").mean()
        df_stock["ratio"] = df_stock["V_Value"] / df_stock["ma_60"]
        df_stock["market_ratio"] = df_stock["V_Value"] / df_stock["MarketCap"]

        # df_stock = df_stock[df_stock["market_ratio"] > 0.01] # 회전율 2% 이상
        df_stock = df_stock[df_stock["ratio"] > 1.5]

        return df_stock

    def update_hot_issue_date(self):

        # load data
        with open(r'D:\MyProject\StockPrice\HotIssueDate.pickle', 'rb') as fr:
            df_hot_issue_date = pickle.load(fr)


        # 기존 데이터 백업
        file_name = 'HotIssueDate' + datetime.datetime.today().strftime("%Y%m%d")
        with open(r'D:\MyProject\StockPrice\백업\{}.pickle'.format(file_name), 'wb') as fw:
            pickle.dump(df_hot_issue_date, fw)

        # load data
        with open(r'D:\MyProject\StockPrice\DictDfStock.pickle', 'rb') as fr:
            dict_df_stock = pickle.load(fr)

        max_date = dict_df_stock["005930"].index[-1]

        list_cmp_cd = stock_pack.set_all_cmp_cd()

        df_res = pd.DataFrame()

        for cmp_cd in tqdm(list_cmp_cd):
            df_stock = dict_df_stock[cmp_cd]

            df_tmp = self.get_df_stock_highlight(df_stock)[["StockCode"]]
            df_res = pd.concat([df_res, df_tmp])

        # 전처리
        if len(df_res) > 0:
            df_res = df_res.reset_index()
            df_res["Date"] = df_res["Date"].astype("str")
            df_res = df_res[["Date", "StockCode"]].rename(columns={"StockCode": "cmp_cd", "Date": "issue_date"})

        df_hot_issue_date = pd.concat([df_hot_issue_date, df_res]).drop_duplicates(
            ["cmp_cd", "issue_date"]).reset_index(drop=True)

        # save data
        with open(r'D:\MyProject\StockPrice\HotIssueDate.pickle', 'wb') as fw:
            pickle.dump(df_hot_issue_date, fw)