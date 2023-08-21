import yfinance as yf
from tqdm import tqdm
import pandas as pd
import pickle
from concurrent.futures import ThreadPoolExecutor, wait
from dateutil.relativedelta import relativedelta
import config
import calendar
from datetime import datetime
import time
import db


class UnipassData:

    def __init__(self, is_update_all=False):

        self.dict_data = {}
        self.dict_info = config.UNIPASS_INFO

        self.df_info = pd.DataFrame(columns=["sector", "sector_sub", "code", "name"])

        if is_update_all:
            self.date_range = pd.date_range("2000-01-01", datetime.today(), freq='M')
        else:
            start_date = datetime.today() - relativedelta(year=1)
            self.date_range = pd.date_range(start_date, datetime.today(), freq='M')

    def set_info(self):

        self.df_info = pd.DataFrame(self.dict_info).T.reset_index()
        self.df_info = self.df_info.rename(columns={"index": "name"})[["sector", "sector_sub", "code", "name"]]

    def get_data(self, ticker, ticker_info):

        df = pd.DataFrame()

        if ticker_info["release"] == "fred":

            # http 에러 간혹 발생
            i = 0
            while 1:
                try:
                    df = self.fred.get_series(ticker)
                    break
                except:
                    if i >10:
                        break
                    else:
                        i += 1
                        continue

            # 전처리
            df = pd.DataFrame(df)
            df.index.name = 'date'
            df = df.rename(columns={0: 'val'})

            # 일자 eom 형식으로 전처리
            if ticker_info["freq"] != "d":
                df = df.reset_index()
                df["date"] = df["date"].apply(lambda x: x.strftime('%Y%m%d'))
                df["date"] = df["date"].apply(
                    lambda x: x[:4] + "-" + x[4:6] + "-" + str(calendar.monthrange(int(x[:4]), int(x[4:6]))[1]))
                df["date"] = pd.to_datetime(df["date"])
                df = df.drop_duplicates("date", keep="last")

                df = df.set_index('date')

        elif ticker_info["release"] == "yahoo":
            df = yf.Ticker(ticker).history(period="max")

            # 전처리
            df = df.reset_index(drop=False)
            df["Date"] = pd.to_datetime(df["Date"].dt.strftime("%Y-%m-%d"))
            df = df[["Date", "Close"]].rename(columns={"Date": "date", "Close": "val"})
            df = df.set_index("date")

        elif ticker_info["release"] == "eos":

            today = datetime.today()
            time.sleep(0.5)

            if ticker_info["freq"] == "d":

                start_date = '20000101'

                end_date = str(today.year) + str(today.month).zfill(2) + str(today.day).zfill(2)

            elif ticker_info["freq"] == "m":

                start_date = '200001'

                end_date = str(today.year) + str(today.month).zfill(2)

            elif ticker_info["freq"] == "q":

                start_date = '2000Q1'

                end_date = str(today.year + 1) + "Q4"

            # 품목별 수출 데이터인 경우
            if ticker_info["stat_cd"] == "901Y039":

                product_key = ticker.split("_")[0]
                type_key = ticker.split("_")[1]

                # 재고/출하 지표 생성
                if type_key == "T00":

                    df_0 = self.dict_macro_data[product_key + "_" + "T41"].reset_index()
                    df_1 = self.dict_macro_data[product_key + "_" + "T42"].reset_index()

                    df_merge = pd.merge(left=df_0, right=df_1, on="date", how="left")
                    df_merge["ratio"] = df_merge["val_y"] / df_merge["val_x"]
                    df = df_merge[["date", "ratio"]].rename(columns={"ratio": "val"})
                    df["date"] = df["date"].apply(lambda x: x.strftime("%Y%m"))

                else:

                    df = self.ecos.get_statistic_search(통계표코드=ticker_info["stat_cd"], 통계항목코드1=product_key,
                                                        통계항목코드2=type_key, 주기="M",
                                                        검색시작일자=start_date, 검색종료일자=end_date)

                    df = df[["시점", "값"]].rename(columns={"시점": "date", "값": "val"})

                    df["val"] = df["val"].astype("float")

            else:

                df = self.ecos.get_statistic_search(통계표코드=ticker_info["stat_cd"], 통계항목코드1=ticker,
                                                    주기=ticker_info["freq"].upper(),
                                                    검색시작일자=start_date, 검색종료일자=end_date)

                df = df[["시점", "값"]].rename(columns={"시점": "date", "값": "val"})

                df["val"] = df["val"].astype("float")

            # 일자 데이터 형변환
            if (ticker_info["freq"] == "d") or (ticker_info["freq"] == "m"):

                df["date"] = df["date"].apply(
                    lambda x: x[:4] + "-" + x[4:6] + "-" + str(calendar.monthrange(int(x[:4]), int(x[4:6]))[1]))

                df["date"] = pd.to_datetime(df["date"])

            elif ticker_info["freq"] == "q":

                pass

            df = df.drop_duplicates("date", keep="last")
            df = df.set_index("date")

        # yoy 변화값 추가
        if ticker_info["freq"] == "m":
            # 월간 데이터의 경우 기본으로  yoy 비교 데이터를 넣는다.
            # 대부분 economic 데이터인데, 원데이터 자체를 분석에 활용하기 부적절하기 떄문
            # 기본 raw data 타입이 'rate' , 'sentiment' 같은 부류인 경우는 yoy가 필요하진 않다.
            df["pct_chg"] = df["val"].pct_change(12)

        if ticker_info["freq"] == "q":
            # 월간 데이터의 경우 기본으로  yoy 비교 데이터를 넣는다.
            # 대부분 economic 데이터인데, 원데이터 자체를 분석에 활용하기 부적절하기 떄문
            # 기본 raw data 타입이 'rate' , 'sentiment' 같은 부류인 경우는 yoy가 필요하진 않다.
            df["pct_chg"] = df["val"].pct_change(4)

        return df

    def collect(self, macro_type):

        thread_count = 10
        list_hs_code = self.df_info["code"].to_list()

        list_hs_code = sorted(list_hs_code)
        n = int(len(list_hs_code) / thread_count)
        nested_list_hs_code = [list_hs_code[i * n:(i + 1) * n] for i in range((len(list_hs_code) + n - 1) // n)]

        threads = []
        with ThreadPoolExecutor(max_workers=thread_count) as executor:
            for nest in nested_list_hs_code:
                threads.append(executor.submit(self.get_data, nest, self.date_range))
            wait(threads)

        df_trade_data = pd.concat([x.result() for x in threads])

    def save(self):

        with open(r"D:\MyProject\MyData\MacroData\UnipassData.pickle", 'wb') as fw:
            pickle.dump(self.dict_data, fw)

        self.df_info.to_sql(name='unipass_info', con=db.conn, if_exists='replace', index=False, schema='financial_data')

    def run(self):

        print("[STRAT]|" + datetime.today().strftime("%Y-%m-%d %H:%M:%S") + "|" + self.__class__.__name__)
        self.set_info()
        self.collect("FICC_INFO")
        self.save()
        print("[END]|" + datetime.today().strftime("%Y-%m-%d %H:%M:%S") + "|" + self.__class__.__name__)
