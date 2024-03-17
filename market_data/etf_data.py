import yfinance as yf
from tqdm import tqdm
import pandas as pd
import pickle
from fredapi import Fred
from PublicDataReader import Ecos
import config
import calendar
from datetime import datetime
import time
import db


class EtfData:

    def __init__(self):

        self.fred = Fred(api_key=config.API_KEY["FRED"])
        self.ecos = Ecos(config.API_KEY["ECOS"])

        self.dict_etf_data = {}
        self.df_etf_info = pd.DataFrame(columns=["category_0", "category_1", "ticker", "name"])

    def set_etf_info(self):

        dict_etf_data_info = config.ETF_INFO

        for category_0 in dict_etf_data_info.keys():
            for category_1 in dict_etf_data_info[category_0].keys():
                for value in dict_etf_data_info[category_0][category_1]:
                    ticker = value["ticker"]
                    name = value["name"]

                    df = pd.DataFrame(
                        {
                            "category_0": [category_0],
                            "category_1": [category_1],
                            "ticker": [ticker],
                            "name": [name]
                        }
                    )
                    self.df_etf_info = pd.concat([self.df_etf_info, df])

    def get_data(self, ticker):

        df = yf.Ticker(ticker).history(period="max")

        # 전처리
        try:
            df = df.reset_index(drop=False)
            df["Date"] = pd.to_datetime(df["Date"].dt.strftime("%Y-%m-%d"))
            df = df[["Date", "Close"]].rename(columns={"Date": "date", "Close": "val"})
            df = df.set_index("date")
        except:
            print(ticker)

        return df

    def collect(self):

        for ticker in self.df_etf_info["ticker"]:

            df = self.get_data(ticker)
            self.dict_etf_data[ticker] = df

    def save(self):

        with open(r"D:\MyProject\MyData\MacroData\EtfData.pickle", 'wb') as fw:
            pickle.dump(self.dict_etf_data, fw)

        self.df_etf_info.to_sql(name='etf_info', con=db.conn, if_exists='replace', index=False, schema='financial_data')

    def run(self):

        print("[STRAT]|" + datetime.today().strftime("%Y-%m-%d %H:%M:%S") + "|" + self.__class__.__name__)
        self.set_etf_info()
        self.collect()
        self.save()
        print("[END]|" + datetime.today().strftime("%Y-%m-%d %H:%M:%S") + "|" + self.__class__.__name__)
