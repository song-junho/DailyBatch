import pandas as pd
from tqdm import tqdm
import pickle
from collections import deque
import exchange_calendars as ecals
from datetime import datetime
from lib import numeric_pack, stock_pack
from multiprocessing import Pool

XKRX = ecals.get_calendar("XKRX")


class UpdateDaily:

    def __init__(self, is_update_all=False):

        # 종목별 가격 데이터
        with open(r'D:\MyProject\StockPrice\DictDfStock.pickle', 'rb') as fr:
            self.dict_df_stock = pickle.load(fr)

        # 일별 데이터 Queue
        self.dict_daily_deque = {}

        # 1.일별 종목 마켓데이터 , 2.일별 종목 deque, 3.업데이트 시작일자
        self.dict_df_stock_daily, self.dict_daily_deque, self.date_start = self.initialize_data(is_update_all)

        # 전종목 리스트
        self.list_cmp_cd = stock_pack.set_all_cmp_cd()

    def initialize_data(self, is_update_all):

        """
        dict_df_stock_daily, dict_daily_deque 초기화
        1. 전체 업데이트
        2. 최신 업데이트
        :param is_update_all:
        :return: dict_df_stock_daily, dict_daily_deque
        """

        if is_update_all:

            # 전체 일자 업데이트
            dict_df_stock_daily = {}

            # 전체 일자 임시 저장 deque 생성
            dict_daily_deque = {}

            for v_date in tqdm(XKRX.schedule.index):
                if v_date > datetime.today():
                    break
                dict_df_stock_daily[v_date] = pd.DataFrame()
                dict_daily_deque[v_date] = deque([])

            date_start = min(dict_df_stock_daily.keys())

        else:
            # 이전 데이터 load
            with open(r"D:\MyProject\StockPrice\DictDfStockDaily.pickle", 'rb') as fr:
                dict_df_stock_daily = pickle.load(fr)

            # 전체 일자 임시 저장 deque 생성
            dict_daily_deque = {}

            # 최신 업데이트 일자 이후 업데이트
            date_start = max(dict_df_stock_daily.keys())

            # 전체 일자 임시 저장 deque 생성
            for v_date in tqdm(XKRX.schedule.index):

                date_diff = (v_date - datetime.today())
                if date_diff.days > -2:
                    break
                elif v_date >= max(dict_df_stock_daily.keys()):
                    dict_df_stock_daily[v_date] = pd.DataFrame()
                    dict_daily_deque[v_date] = deque([])
                else:
                    continue

        return dict_df_stock_daily, dict_daily_deque, date_start

    def update_dict_stock_daily(self, list_cmp_cd):

        date_start = self.date_start
        dict_daily_deque = {}

        for cmp_cd in tqdm(list_cmp_cd):

            df_stock = self.dict_df_stock[cmp_cd].loc[date_start:]
            for v_date, rows in df_stock.iterrows():

                if v_date not in dict_daily_deque.keys():
                    dict_daily_deque[v_date] = deque([])
                dict_daily_deque[v_date].append(rows.to_frame().T)

        return dict_daily_deque

    def multiprocessing(self):

        process_count = 3

        list_cmp_cd = sorted(self.list_cmp_cd)
        n = int(len(list_cmp_cd) / process_count)
        nested_list_cmp_cd = [list_cmp_cd[i * n:(i + 1) * n] for i in range((len(list_cmp_cd) + n - 1) // n)]

        p = Pool(process_count)

        # 월별 인덱스 생성
        res = p.map_async(self.update_dict_stock_daily, nested_list_cmp_cd)

        # dictionary 병합
        for output in res.get():
            list_date = list(filter(lambda x : x >= self.date_start, sorted(list(output.keys()))))
            for t_date in list_date:
                if t_date not in self.dict_daily_deque.keys():
                    self.dict_daily_deque[t_date] = deque([])
                self.dict_daily_deque[t_date].extend(output[t_date])

        p.close()
        p.join()

        # dict_df_stock_daily 데이터 병합
        for p_date in tqdm(self.dict_daily_deque.keys()):
            if len(self.dict_daily_deque[p_date]) == 0:
                continue
            df = pd.concat(self.dict_daily_deque[p_date]).drop_duplicates(["StockCode"])
            self.dict_df_stock_daily[p_date] = df

    def run(self):

        self.multiprocessing()

        with open(r"D:\MyProject\StockPrice\DictDfStockDaily.pickle", 'wb') as fw:
            pickle.dump(self.dict_df_stock_daily, fw)
