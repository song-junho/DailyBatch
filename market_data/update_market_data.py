from pykrx import stock
import pandas as pd
import datetime
from tqdm import tqdm
import pickle
from concurrent.futures import ThreadPoolExecutor, wait
import threading
from lib import numeric_pack, stock_pack
import time
import random

sleep_time = 10


class UpdateMarketData:

    def __init__(self, is_update_all=False):

        self.is_update_all = is_update_all

        # load data
        with open(r'D:\MyProject\StockPrice\DictDfStock.pickle', 'rb') as fr:
            self.dict_df_stock = pickle.load(fr)

        # 기존 데이터 백업
        file_name = 'DictDfStock_' + datetime.datetime.today().strftime("%Y%m%d")
        with open(r'D:\MyProject\StockPrice\백업\{}.pickle'.format(file_name), 'wb') as fw:
            pickle.dump(self.dict_df_stock, fw)

        # 전체 업데이트인 경우 빈값으로 변경
        if self.is_update_all:
            for cmp_cd in self.dict_df_stock.keys():
                self.dict_df_stock[cmp_cd] = pd.DataFrame()
            self.start_date = datetime.datetime(2000, 1, 1)
            self.sleep_range = random.uniform(2, 5)  # sleep 값 범위
            self.thread_count = 15  # 멀티스레드 총 수

        else:
            self.start_date = self.dict_df_stock["005930"].index[-1]
            self.sleep_range = random.uniform(0, 3)  # sleep 값 범위
            self.thread_count = 5  # 멀티스레드 총 수

        self.end_date = datetime.datetime.today()

        # 상장종목 리스트
        self.list_cmp_cd = stock_pack.set_all_cmp_cd(self.start_date, self.end_date)

    def set_all_cmp_cd(self):

        max_date = numeric_pack.get_list_mkt_date(self.start_date, self.end_date)[-1]
        list_cmp_cd = stock.get_market_ticker_list(max_date, market="KOSPI") + stock.get_market_ticker_list(max_date,
                                                                                                            market="KOSDAQ")
        return list_cmp_cd

    def create_update_data(self) -> dict:

        def mutithreading(list_cmp_cd):

            start_date = self.start_date.strftime("%Y-%m-%d")
            end_date = self.end_date.strftime("%Y-%m-%d")

            for cmp_cd in tqdm(list_cmp_cd):

                # 1. 시가총액 데이터
                df_market_cap = pd.DataFrame()

                loop_count = 0
                while(1):
                    time.sleep(self.sleep_range)
                    try:
                        df = stock.get_market_cap_by_date(start_date, end_date, cmp_cd)
                        break
                    except:
                        print("err", cmp_cd)
                        time.sleep(sleep_time)
                        loop_count += 1
                        if loop_count > 10:
                            break
                        else:
                            continue

                if len(df) == 0:
                    continue
                if len(df[df["시가총액"] == 0]) / len(df) > 0.5:
                    continue

                df["티커"] = cmp_cd
                df.index.name = "Date"
                df_market_cap = pd.concat([df_market_cap, df])

                # 2. 가격데이터
                df_price = pd.DataFrame()

                loop_count = 0
                while (1):
                    time.sleep(self.sleep_range)
                    try:
                        df = stock.get_market_ohlcv_by_date(start_date, end_date, cmp_cd, adjusted=True)
                        df = numeric_pack.price_to_adj_price(df)  # 수정주가 변환
                        break
                    except:
                        print("err", cmp_cd)
                        time.sleep(sleep_time)
                        loop_count += 1
                        if loop_count > 10:
                            break
                        else:
                            continue

                df["티커"] = cmp_cd
                df.index.name = "Date"
                df_price = pd.concat([df_price, df])

                df_res = pd.merge(left=df_market_cap[["시가총액"]], right=df_price, left_index=True, right_index=True, how="left")
                df_res["Market"] = ""

                df_res = \
                    df_res.rename(columns={
                        "티커": "StockCode",
                        "시가": "Open",
                        "고가": "High",
                        "저가": "Low",
                        "종가": "Close",
                        "등락률": "Change",
                        "시가총액": "MarketCap",
                        "거래량": "Volume",
                        "Market": "Market",
                        "거래대금": "V_Value",

                    })
                # print(v_date)
                with _lock:
                    dict_res[cmp_cd] = df_res[['StockCode', 'Open', 'High', 'Low', 'Close', 'Change', 'MarketCap',
       'Volume', 'Market', 'V_Value']]

        dict_res = {}
        _lock = threading.Lock()

        list_cmp_cd = sorted(self.list_cmp_cd)
        n = int(len(list_cmp_cd) / self.thread_count)
        nested_list_cmp_cd = [list_cmp_cd[i * n:(i + 1) * n] for i in range((len(list_cmp_cd) + n - 1) // n)]

        threads = []
        with ThreadPoolExecutor(max_workers=self.thread_count) as executor:

            for nest in nested_list_cmp_cd:
                threads.append(executor.submit(mutithreading, nest))
            wait(threads)

        return dict_res

    def update_market_data(self):

        dict_res = self.create_update_data()
        print(dict_res.keys())

        for cmp_cd, data in tqdm(dict_res.items()):

            # 신규 상장 종목 초기화
            if cmp_cd not in self.dict_df_stock.keys():
                self.dict_df_stock[cmp_cd] = pd.DataFrame()

            # 기존 데이터와 결합 후, 중복 제거(keep=업데이트)
            self.dict_df_stock[cmp_cd] = pd.concat([self.dict_df_stock[cmp_cd], data])
            self.dict_df_stock[cmp_cd] = self.dict_df_stock[cmp_cd].reset_index().drop_duplicates("Date", keep="last").set_index("Date")

        # save data
        with open(r'D:\MyProject\StockPrice\DictDfStock.pickle', 'wb') as fw:
            pickle.dump(self.dict_df_stock, fw)
