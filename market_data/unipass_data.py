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
import time
import random


class UnipassData:

    def __init__(self, is_update_all=False):

        self.dict_info = config.UNIPASS_INFO["raw"]
        self.df_info = pd.DataFrame(columns=["sector", "sector_sub", "code", "name", "main_type"])
        self.df_info_class = pd.DataFrame(columns=["연도", "세번", "세번10단위품명", "중분류코드", "중분류명", "소분류코드", "소분류명", "세분류코드", "세분류명"])
        self.is_update_all = is_update_all

        if is_update_all:
            self.date_range = pd.date_range("2000-01-01", datetime.today(), freq='M')
            self.date_range = list(map(lambda x: int(str(x.year) + str(x.month).zfill(2)), self.date_range))
            self.df_data_hs = pd.DataFrame()
            self.df_data_cls = pd.DataFrame()
        else:
            start_date = datetime.today() - relativedelta(years=1)
            self.date_range = pd.date_range(start_date, datetime.today(), freq='M')
            self.date_range = list(map(lambda x: int(str(x.year) + str(x.month).zfill(2)), self.date_range))
            self.df_data_hs = self.load_data_hs()  # 기 데이터 로드

            self.df_data_cls = self.load_data_cls()  # 기 데이터 로드

    def set_info_class(self):

        df_info_class = pd.read_excel(r"D:\MyProject\MyData\관세청조회코드_v1.1.xlsx", sheet_name="성질통합분류코드")
        df_info_class.columns = df_info_class.loc[3]
        df_info_class = df_info_class.loc[4:].reset_index(drop=True)
        self.df_info_class = df_info_class[["연도", "세번", "세번10단위품명", "중분류코드", "중분류명", "소분류코드", "소분류명", "세분류코드", "세분류명"]]

    def set_info(self):

        self.df_info = pd.DataFrame(self.dict_info).T.reset_index()
        self.df_info = self.df_info.rename(columns={"index": "name"})[["sector", "sector_sub", "code", "name", "main_type"]]

    def get_data_by_hs_code(self, list_hs_code):
        """
        HS CODE 로 데이터를 쌓는 함수

        :param list_hs_code:
        :return:
        """

        df_trade_data = pd.DataFrame(
            columns=["code", "statkor", "date", "export_amt", "import_amt", "export_price", "import_price", "maint_type"])

        dict_trade_data = {
            "code": []
            , "statkor": []
            , "date": []
            , "export_amt": []
            , "import_amt": []
            , "export_price": []
            , "import_price": []
            , "main_type": []
        }

        for hs_code in tqdm(list_hs_code):

            name = self.df_info.loc[self.df_info["code"] == hs_code, "name"].values[0]
            main_type = self.df_info.loc[self.df_info["code"] == hs_code, "main_type"].values[0]

            # 만약 해당 품목 데이터 수가 12개 미만 이라면, 전체 일자로 조회해서 쌓는다.
            if self.is_update_all:
                date_range = pd.date_range("2000-01-01", datetime.today(), freq='M')
                date_range = list(map(lambda x: int(str(x.year) + str(x.month).zfill(2)), date_range))
            else:
                if len(self.df_data_hs[self.df_data_hs["name"] == name]) <= 12:
                    date_range = pd.date_range("2000-01-01", datetime.today(), freq='M')
                    date_range = list(map(lambda x: int(str(x.year) + str(x.month).zfill(2)), date_range))
                else:
                    date_range = self.date_range

            for date_index in range(1, len(date_range), 12):

                start_yymm = date_range[-len(date_range) if (-date_index - 11) < -len(date_range) else (-date_index - 11)]
                end_yymm = date_range[-date_index]
                print(name, start_yymm, end_yymm)

                while True:
                    try:
                        req_url = f'https://apis.data.go.kr/1220000/Itemtrade/getItemtradeList?serviceKey={config.API_KEY["UNIPASS"]}&strtYymm={start_yymm}&endYymm={end_yymm}&hsSgn={hs_code}'
                        r = requests.get(req_url)
                        time.sleep(random.uniform(0.5, 2))
                        break
                    except ConnectionResetError:
                        time.sleep(random.uniform(0.5, 2))
                        continue
                    except ConnectionError:
                        time.sleep(random.uniform(0.5, 2))
                        continue
                    except Exception as e:
                        print("ERROR : ", e)

                soup = BeautifulSoup(r.text, "xml")
                items = soup.find_all("item")
                for item in items:

                    statkor = item.find("statKor").get_text()
                    t_date = item.find("year").get_text()
                    expdlr = int(item.find("expDlr").get_text())
                    expwgt = int(item.find("expWgt").get_text())
                    impdlr = int(item.find("impDlr").get_text())
                    impwgt = int(item.find("impWgt").get_text())

                    if t_date == "총계":
                        continue
                    else:
                        year = int(t_date.split('.')[0])
                        month = int(t_date.split('.')[1])
                        day = calendar.monthrange(year, month)[1]

                        t_date = datetime(year, month, day)

                    exp_price = expdlr / expwgt if (expdlr != 0) & (expwgt != 0) else 0
                    imp_price = impdlr / impwgt if (impdlr != 0) & (impwgt != 0) else 0

                    dict_trade_data["code"].append(hs_code)
                    dict_trade_data["statkor"].append(statkor)
                    dict_trade_data["date"].append(t_date)
                    dict_trade_data["export_amt"].append(expdlr)
                    dict_trade_data["import_amt"].append(impdlr)
                    dict_trade_data["export_price"].append(exp_price)
                    dict_trade_data["import_price"].append(imp_price)
                    dict_trade_data["main_type"].append(main_type)

        df_trade_data = pd.concat([df_trade_data, pd.DataFrame(dict_trade_data)])

        return df_trade_data

    def get_data_by_cls_code(self, list_cls_cd):
        """
        CLS CODE 로 데이터를 쌓는 함수
        :return:
        """

        date_range = self.date_range

        q_ = []
        for imexTpcd in [1, 2]:

            for imexTmprUnfcClsfCd in tqdm(list_cls_cd):

                for date_index in (range(1, len(date_range), 12)):

                    start_yymm = date_range[-len(date_range) if (-date_index - 11) < -len(date_range) else (-date_index - 11)]
                    end_yymm = date_range[-date_index]
                    print(imexTmprUnfcClsfCd, start_yymm, end_yymm)

                    while True:
                        count_n = 0
                        try:
                            req_url = f'http://apis.data.go.kr/1220000/newtempertrade/getNewtempertradeList?serviceKey={config.API_KEY["UNIPASS"]}&strtYymm={start_yymm}&endYymm={end_yymm}&imexTpcd={imexTpcd}&imexTmprUnfcClsfCd={imexTmprUnfcClsfCd}'
                            r = requests.get(req_url)
                            time.sleep(random.uniform(0.5, 2))

                            soup = BeautifulSoup(r.text, "xml")
                            items = soup.find_all("item")

                            # item 개수가 0개, 2001년 이후 기간, 재시도 횟수 5회 미만인 경우. 진행
                            if (len(items) == 0) & (date_index//100 > 2001) & (count_n < 5):
                                print("NoItem", req_url)
                                count_n += 1

                                time.sleep(random.uniform(0.5, 2))
                                continue
                            else:
                                break
                        except ConnectionResetError:
                            time.sleep(random.uniform(0.5, 5))
                            continue
                        except ConnectionError:
                            time.sleep(random.uniform(0.5, 5))
                            continue
                        except Exception as e:
                            print("ERROR : ", e)

                    # soup = BeautifulSoup(r.text, "xml")
                    # items = soup.find_all("item")

                    for item in items:

                        statkor = item.find("statCdCntnKor1").get_text()
                        year = item.find("year").get_text()
                        godsCd = item.find("godsCd").get_text()
                        godsKor = item.find("godsKor").get_text()
                        expdlr = int(item.find("dlr").get_text())
                        wgt = int(item.find("wgt").get_text())

                        # 100만 달러 미만 저장 x
                        if int(expdlr) > (1000 * 1000):
                            q_.append(pd.DataFrame(data=[[imexTpcd, statkor, year, godsCd, godsKor, expdlr, wgt]]))

        df_res = pd.concat(q_)
        df_res.columns = ["imexTpcd", "dept", "date", "godsCd", "godsKor", "dlr", "wgt"]
        return df_res

    def collect(self, data_type='HS'):

        if data_type == "HS":

            thread_count = 10
            list_hs_code = self.df_info["code"].to_list()

            list_hs_code = sorted(list_hs_code)
            n = int(len(list_hs_code) / thread_count)
            nested_list_hs_code = [list_hs_code[i * n:(i + 1) * n] for i in range((len(list_hs_code) + n - 1) // n)]

            threads = []
            with ThreadPoolExecutor(max_workers=thread_count) as executor:
                for nest in nested_list_hs_code:
                    threads.append(executor.submit(self.get_data_by_hs_code, nest))
                wait(threads)

            df_data = pd.concat([x.result() for x in threads])

            df_data = df_data.groupby(["code", "date"]).sum().reset_index()
            df_data = df_data.drop(columns=["statkor"])

            df_data = pd.merge(left=self.df_info[["sector", "sector_sub", "name", "code", "main_type"]], right=df_data, on="code",
                                     how="left")
            df_data = df_data.rename(columns={"sector_x": "sector", "sector_sub_x": "secotor_sub"})

            self.df_data_hs = pd.concat([self.df_data_hs, df_data]).drop_duplicates(["sector", "code", "date"])

        elif data_type == "CLS":

            thread_count = 5
            lis_cls_cd = list(self.df_info_class["중분류코드"].unique())

            lis_cls_cd = sorted(lis_cls_cd)
            n = int(len(lis_cls_cd) / thread_count)
            nested_lis_cls_cd = [lis_cls_cd[i * n:(i + 1) * n] for i in range((len(lis_cls_cd) + n - 1) // n)]

            threads = []
            with ThreadPoolExecutor(max_workers=thread_count) as executor:
                for nest in nested_lis_cls_cd:
                    threads.append(executor.submit(self.get_data_by_cls_code, nest))
                wait(threads)

            df_data = pd.concat([x.result() for x in threads])

            self.df_data_cls = pd.concat([self.df_data_cls, df_data]).drop_duplicates(["imexTpcd", "dept", "date", "godsCd"])

    def load_data_hs(self):

        with open(r"D:\MyProject\MyData\MacroData\UnipassDataHs.pickle", 'rb') as fr:
            return pickle.load(fr)

    def load_data_cls(self):

        with open(r"D:\MyProject\MyData\MacroData\UnipassDataCls.pickle", 'rb') as fr:
            return pickle.load(fr)

    def save(self, data_type='HS'):

        if data_type == 'HS':

            with open(r"D:\MyProject\MyData\MacroData\UnipassDataHs.pickle", 'wb') as fw:
                pickle.dump(self.df_data_hs, fw)
            self.df_info.to_sql(name='unipass_info', con=db.conn, if_exists='replace', index=False,
                                schema='financial_data')
        elif data_type == 'CLS':

            with open(r"D:\MyProject\MyData\MacroData\UnipassDataCls.pickle", 'wb') as fw:
                pickle.dump(self.df_data_cls, fw)

    def run(self):

        print("[STRAT]|" + datetime.today().strftime("%Y-%m-%d %H:%M:%S") + "|" + self.__class__.__name__)
        self.set_info()
        self.set_info_class()
        # self.collect(data_type="HS")
        # self.save(data_type='HS')

        self.collect(data_type="CLS")
        self.save(data_type='CLS')
        print("[END]|" + datetime.today().strftime("%Y-%m-%d %H:%M:%S") + "|" + self.__class__.__name__)
