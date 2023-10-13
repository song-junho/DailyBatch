import pandas as pd
import pickle
import FinanceDataReader as fdr
import db


class UpdateKrxStockInfo:

    def __init__(self):

        self.df_krx_stock_info = pd.DataFrame()

    def get_krx_stock_info(self):

        df_krx_info = fdr.StockListing("KRX")
        df_krx_info = df_krx_info[df_krx_info["Market"].isin(["KOSPI", "KOSDAQ", "KOSDAQ GLOBAL"])]
        df_krx_info = df_krx_info[~df_krx_info["Name"].str.contains("스팩")]
        df_krx_info = df_krx_info.sort_values("Code").reset_index(drop=True)
        df_krx_info = df_krx_info.rename(columns={"Code": "Symbol"})
        df_krx_info = df_krx_info[~(df_krx_info["Symbol"].str[-1] != "0")].reset_index(drop=True)

        self.df_krx_stock_info = df_krx_info

    def save(self):

        self.df_krx_stock_info.to_sql('krx_stock_info', db.engine, if_exists='replace', index=False, schema='financial_data')

    def run(self):

        self.get_krx_stock_info()
        self.save()


