
import pandas as pd
import pickle

from tqdm import tqdm
from collections import deque
from dateutil.relativedelta import relativedelta
import copy
from functools import reduce
import exchange_calendars as ecals
from multiprocessing import Pool
import db
import pyarrow as pa
from datetime import timedelta


import warnings
warnings.filterwarnings('ignore')

# 한국장 영업일
XKRX = ecals.get_calendar("XKRX")

context = pa.default_serialization_context()


class ThemeIndex:

    def __init__(self, start_date, end_date):

        self.list_date_som = pd.date_range(start=start_date, end=end_date, freq="MS")
        self.list_date_eom = pd.date_range(start=start_date, end=(end_date + relativedelta(months=1)), freq="M")

        # 테마 인덱스
        self.dict_theme_index = {}

        # 테마 인덱스 (월별 구간 수익률) , strapping 용도
        self.dict_monthly_theme_index = {}

        # 테마 키워드
        self.df_cmp_keyword = pd.read_excel(r"D:\MyProject\Notion\키워드_사전.xlsx", dtype="str")

        # 테마 리스트
        self.list_theme = self.set_list_theme()

        # 테마-종목 key-value
        self.dict_theme_list_cmp = self.set_dict_theme_list_cmp()

        # 한국장 영업일
        self.list_krx_date = XKRX.schedule.index

        # 가격 Dictionary 생성
        with open(r"D:\MyProject\StockPrice\DictDfStock.pickle", 'rb') as fr:
            self.dict_df_stock = pickle.load(fr)

    def set_list_theme(self):

        df = self.df_cmp_keyword.groupby("keyword").count()
        df = (df[df["cmp_cd"] >= 5])

        return sorted(list(df[df["cmp_cd"] > 4].index))

    def set_dict_theme_list_cmp(self):

        dict_theme_list_cmp = {}

        for theme in self.list_theme:
            dict_theme_list_cmp[theme] = self.df_cmp_keyword[self.df_cmp_keyword["keyword"] == theme]["cmp_cd"].to_list()

        return dict_theme_list_cmp

    def get_df_stock(self, cmp_cd, som, eom):
        '''
        Redis 캐시 자원 활용
         . 특정 기간 범위 종목 데이터 저장 및 반환
        :param cmp_cd:
        :param som:
        :param eom:
        :return:
        '''

        key_nm = cmp_cd + str(som) + str(eom)

        df_stock = db.redis_client.get(key_nm)
        if df_stock is None:
            df_stock = self.dict_df_stock[cmp_cd]
            df_stock = df_stock.loc[som:eom]
            df_stock = df_stock[df_stock["Volume"] > 0]

            db.redis_client.set(key_nm, context.serialize(df_stock).to_buffer().to_pybytes(), timedelta(minutes=3))
        else:
            df_stock = context.deserialize(db.redis_client.get(key_nm))

        return df_stock

    def insert_monthly_theme_index(self, list_cmp_cd, theme, som, eom, dict_monthly_theme_index):

        monthly_index = deque([])

        limit_len = len(list(filter(lambda x: x if (x > som) & (x < eom) else None, XKRX.schedule.index)))

        for cmp_cd in list_cmp_cd:

            df_stock = self.get_df_stock(cmp_cd, som, eom)

            # 당월 데이터가 KRX 영업일*0.8 미만인 종목은 제외
            if (len(df_stock) < limit_len * 0.8) & (eom != self.list_date_eom[-1]):
                continue
            elif len(df_stock) == 0:
                continue
            else:
                df_stock[cmp_cd] = df_stock["Close"] / df_stock["Close"].iloc[0]

            monthly_index.append(df_stock[[cmp_cd]])

        if len(monthly_index) == 0:
            return
        else:
            df = reduce(lambda left, right: pd.merge(left, right, left_index=True, right_index=True, how="left"), monthly_index)
            df["index"] = df.mean(axis='columns')
            dict_monthly_theme_index[theme].append(df[["index"]])

    def thread_theme(self, list_theme):

        # 테마 월별 구간 수익률 적재용 , dict_monthly_theme_index 초기화
        dict_monthly_theme_index = {}
        for theme in list_theme:
            dict_monthly_theme_index[theme] = deque([])

        # 테마 인덱싱 스레드 함수
        for som, eom in tqdm(zip(self.list_date_som, self.list_date_eom), total=len(self.list_date_som)):

            for theme in list_theme:
                self.insert_monthly_theme_index(self.dict_theme_list_cmp[theme], theme, som, eom, dict_monthly_theme_index)

        # 각 프로세스가 종료되면 별도의 heap에서 변형된 데이터도 소멸되기 떄문에 반환한다.
        return dict_monthly_theme_index

    def make_theme_index(self):

        list_theme_t = self.list_theme
        n = int(len(list_theme_t) / 3)
        list_theme_t = [list_theme_t[i * n:(i + 1) * n] for i in range((len(list_theme_t) + n - 1) // n)]
        p = Pool(len(list_theme_t))

        # 월별 인덱스 생성
        res = p.map_async(self.thread_theme, list_theme_t)

        # dictionary 병합
        for x in res.get():
            self.dict_monthly_theme_index.update(x)

        p.close()
        p.join()

    def index_strapping(self):

        for theme_keyword in tqdm(self.dict_monthly_theme_index.keys()):

            monthly_theme = self.dict_monthly_theme_index[theme_keyword]

            df_theme_index = pd.DataFrame()
            for month_index in monthly_theme:

                # 직전 인덱스 값 초기화
                if len(df_theme_index) == 0:
                    latest_index = 1
                else:
                    latest_index = df_theme_index.iloc[-1]["index"]

                df = pd.DataFrame(month_index["index"] * latest_index)
                df_theme_index = pd.concat([df_theme_index, df])

            self.dict_theme_index[theme_keyword] = df_theme_index

    def save(self):

        with open(r'D:\MyProject\StockPrice\DictThemeIndex.pickle', 'wb') as fw:
            pickle.dump(self.dict_theme_index, fw)

    def set_col_z_score(self):

        list_theme_nm = sorted(list(self.dict_theme_index.keys()))
        for theme_nm in tqdm(list_theme_nm):

            if len(self.dict_theme_index[theme_nm]) == 0:
                continue

            list_window = [20, 40, 60, 120, 240]

            for window in list_window:
                raw = self.dict_theme_index[theme_nm]["index"]
                avg = self.dict_theme_index[theme_nm]["index"].rolling(window=window).mean()
                std = self.dict_theme_index[theme_nm]["index"].rolling(window=window).std()
                self.dict_theme_index[theme_nm]["z_score_" + str(window)] = (raw - avg) / std


    def create_theme_index(self):

        # 테마 월별 구간 수익률 적재용 , dict_monthly_theme_index 초기화
        for theme in self.list_theme:
            self.dict_monthly_theme_index[theme] = deque([])

        # 테마 인덱스 생성
        self.make_theme_index()

        # 월별 구간 수익률 strapping
        self.index_strapping()

    def run(self):

        self.create_theme_index()
        self.set_col_z_score()
        self.save()

