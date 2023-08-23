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


class UnipassData:

    def __init__(self, is_update_all=False):

        self.dict_info = config.UNIPASS_INFO
        self.df_info = pd.DataFrame(columns=["sector", "sector_sub", "code", "name"])

        if is_update_all:
            self.date_range = pd.date_range("2000-01-01", datetime.today(), freq='M')
            self.date_range = list(map(lambda x: int(str(x.year) + str(x.month).zfill(2)), self.date_range))
            self.df_data = pd.DataFrame()
        else:
            start_date = datetime.today() - relativedelta(years=1)
            self.date_range = pd.date_range(start_date, datetime.today(), freq='M')
            self.date_range = list(map(lambda x: int(str(x.year) + str(x.month).zfill(2)), self.date_range))
            self.df_data = self.load()  # 기 데이터 로드


    def set_info(self):

        self.df_info = pd.DataFrame(self.dict_info).T.reset_index()
        self.df_info = self.df_info.rename(columns={"index": "name"})[["sector", "sector_sub", "code", "name"]]

    def get_data(self, list_hs_code, ticker_info):

        df_trade_data = pd.DataFrame(
            columns=["code", "statkor", "date", "export_amt", "import_amt", "export_price", "import_price"])

        dict_trade_data = {
            "code": []
            , "statkor": []
            , "date": []
            , "export_amt": []
            , "import_amt": []
            , "export_price": []
            , "import_price": []
        }

        for hs_code in tqdm(list_hs_code):

            name = self.df_info.loc[self.df_info["code"] == hs_code, "name"].values[0]

            for date_index in range(1, len(self.date_range), 12):

                start_yymm = self.date_range[-date_index - 11]
                end_yymm = self.date_range[-date_index]
                print(name, start_yymm, end_yymm)

                req_url = f'https://apis.data.go.kr/1220000/Itemtrade/getItemtradeList?serviceKey={config.API_KEY["UNIPASS"]}&strtYymm={start_yymm}&endYymm={end_yymm}&hsSgn={hs_code}'
                r = requests.get(req_url)

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

        df_trade_data = pd.concat([df_trade_data, pd.DataFrame(dict_trade_data)])

        return df_trade_data

    def collect(self):

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

        df_data = pd.concat([x.result() for x in threads])

        df_data = df_data.groupby(["code", "date"]).sum().reset_index()
        df_data = df_data.drop(columns=["statkor"])
        df_data = pd.merge(left=df_data, right=self.df_info, on="code", how="left")

        self.df_data = pd.concat([self.df_data, df_data]).drop_duplicates(["sector", "code", "date"])

    def load(self):

        with open(r"D:\MyProject\MyData\MacroData\UnipassData.pickle", 'rb') as fr:
            return pickle.load(fr)

    def save(self):

        with open(r"D:\MyProject\MyData\MacroData\UnipassData.pickle", 'wb') as fw:
            pickle.dump(self.df_data, fw)

        self.df_info.to_sql(name='unipass_info', con=db.conn, if_exists='replace', index=False, schema='financial_data')

    def run(self):

        print("[STRAT]|" + datetime.today().strftime("%Y-%m-%d %H:%M:%S") + "|" + self.__class__.__name__)
        self.set_info()
        self.collect()
        self.save()
        print("[END]|" + datetime.today().strftime("%Y-%m-%d %H:%M:%S") + "|" + self.__class__.__name__)
