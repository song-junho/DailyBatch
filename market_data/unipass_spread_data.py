import yfinance as yf
from tqdm import tqdm
import pandas as pd
import pickle
from concurrent.futures import ThreadPoolExecutor, wait
from dateutil.relativedelta import relativedelta
import config
import calendar
from datetime import datetime
import requests
from bs4 import BeautifulSoup
import db


class UnipassSpreadData:

    def __init__(self):

        self.dict_info_raw = config.UNIPASS_INFO["raw"]
        self.df_info_raw = pd.DataFrame(columns=["sector", "sector_sub", "code", "name"])

        self.dict_info_spread = config.UNIPASS_INFO["spread"]
        self.df_info_spread = pd.DataFrame(columns=["sector", "sector_sub", "name_raw", "name_prd", "type_raw", "type_prd"])

        self.df_trade_data = self.load()  # 수출입 품목 원데이터
        self.df_data = pd.DataFrame()


    def set_info(self):

        self.df_info_raw = pd.DataFrame(self.dict_info_raw).T.reset_index()
        self.df_info_raw = self.df_info_raw.rename(columns={"index": "name"})[["sector", "sector_sub", "code", "name"]]

        self.df_info_spread = pd.DataFrame(self.dict_info_spread).T.reset_index()
        self.df_info_spread = self.df_info_spread.rename(columns={"index": "name"})[["sector", "sector_sub", "name_raw", "name_prd", "type_raw", "type_prd"]]

    def get_data(self):

        df_unipass = self.df_info_raw
        df_unipass_spread = self.df_info_spread
        df_trade_data = self.df_trade_data

        stack = []
        for i, rows in df_unipass_spread.iterrows():

            name_raw = rows["name_raw"]
            name_prd = rows["name_prd"]

            type_raw = rows["type_raw"]
            type_prd = rows["type_prd"]

            # 관세청 데이터에 없는 품목
            if len(df_unipass.loc[df_unipass["name"] == name_raw]) == 0:
                continue

            raw_code = df_unipass.loc[df_unipass["name"] == name_raw, "code"].values[0]
            prd_code = df_unipass.loc[df_unipass["name"] == name_prd, "code"].values[0]

            # 원재료
            col_price = type_raw + "_" + "price"
            df_raw_price = df_trade_data[df_trade_data["code"] == raw_code][["date", "name", col_price]].rename(
                columns={col_price: "raw_price"})

            # 제품
            col_price = type_prd + "_" + "price"
            df_prd_price = df_trade_data[df_trade_data["code"] == prd_code][["date", "name", col_price]].rename(
                columns={col_price: "prd_price"})

            df_spread_price = pd.merge(left=df_raw_price, right=df_prd_price, on="date", how="inner")
            df_spread_price = df_spread_price.rename(columns={"name_x": "name_raw", "name_y": "name_prd"})

            stack.append(df_spread_price)

        df_spread_price = pd.concat(stack)
        df_spread_price["name"] = df_spread_price["name_prd"] + "_" + df_spread_price["name_raw"]
        df_spread_price["spread"] = df_spread_price["prd_price"] + df_spread_price["raw_price"]
        df_spread_price = df_spread_price.drop_duplicates(["date", "name"]).reset_index(drop=True)
        df_spread_price = pd.merge(left=df_spread_price, right=df_unipass[["sector", "sector_sub", "name"]],
                                   left_on="name_prd", right_on="name", how="left")
        df_spread_price = df_spread_price.drop(columns="name_y").rename(columns={"name_x": "name"})[
            ["date", "sector", "sector_sub", "name", "spread"]]

        self.df_data = df_spread_price

    def load(self):

        with open(r"D:\MyProject\MyData\MacroData\UnipassData.pickle", 'rb') as fr:
            return pickle.load(fr)

    def save(self):

        with open(r"D:\MyProject\MyData\MacroData\UnipassSpreadData.pickle", 'wb') as fw:
            pickle.dump(self.df_data, fw)

        self.df_info_spread.to_sql(name='unipass_spread_info', con=db.conn, if_exists='replace', index=False, schema='financial_data')

    def run(self):

        print("[STRAT]|" + datetime.today().strftime("%Y-%m-%d %H:%M:%S") + "|" + self.__class__.__name__)
        self.set_info()
        self.get_data()
        self.save()
        print("[END]|" + datetime.today().strftime("%Y-%m-%d %H:%M:%S") + "|" + self.__class__.__name__)
