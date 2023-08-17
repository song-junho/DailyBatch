import config
import calendar
from datetime import datetime
from tqdm import tqdm
import db
import pandas as pd
import pickle
import exchange_calendars as ecals
from multiprocessing import Pool


class UpdateExpReturn:

    def __init__(self):

        list_item_cd = (211500)

        q = 'SELECT * FROM financial_data.financial_statement_ttm' + ' where item_cd = {}'.format(list_item_cd)
        self.df_fin_ttm = pd.read_sql_query(q, db.conn).sort_values(['cmp_cd', 'yymm', 'fin_typ', 'freq']).drop_duplicates(
            ["term_typ", "cmp_cd", "item_cd", "yymm", "freq"], keep="last").reset_index(drop=True)

        with open(r'D:\MyProject\종목분석_환경\multiple_DB\dict_multiple_cmp_cd.pickle', 'rb') as fr:
            self.dict_multiple_cmp_cd = pickle.load(fr)

        # 한국 개장일자
        XKRX = ecals.get_calendar("XKRX")
        self.list_krx_date = list(XKRX.schedule.index)

        self.list_krx_date = list(filter(lambda x: x > datetime(2007, 1, 1), self.list_krx_date))
        self.list_krx_date = list(filter(lambda x: x < datetime(2023, 8, 1), self.list_krx_date))

    def yymm_to_date(self, yymm):

        year = yymm // 100
        month = yymm % 100

        if month == 12:
            year = year + 1
            month = 3
        else:
            month = month + 3

        t_date = datetime(year, month, 1)
        t_date = list(filter(lambda x: x < t_date, self.list_krx_date))[-1]

        return t_date

    @ staticmethod
    def date_to_yymm(t_date):

        year = t_date.year
        month = t_date.month
        day = t_date.day

        mmdd = str(month).zfill(2) + str(day).zfill(2)

        if (mmdd < '0330'):
            yy = str(year - 1)
            mm = '09'
        elif (mmdd >= '0330') & (mmdd < '0520'):
            yy = str(year - 1)
            mm = '12'
        elif (mmdd >= '0520') & (mmdd < '0820'):
            yy = str(year)
            mm = '03'
        elif (mmdd >= '0820') & (mmdd < '1120'):
            yy = str(year)
            mm = '06'
        elif mmdd >= '1120':
            yy = str(year)
            mm = '09'

        yymm = int(yy + mm)

        return yymm

    def create_df_froe(self):
        """
        예상 ROE 데이터 반환
        :return:
        """

        def rolling_avg(group_df):
            return group_df['val'].rolling(window=4, min_periods=1).mean()

        list_cmp_cd = self.df_fin_ttm["cmp_cd"].unique()
        stack_ = []

        df_roe = self.df_fin_ttm[self.df_fin_ttm["item_cd"] == 211500]
        df_roe = df_roe[df_roe["freq"] == "yoy"]
        df_roe = df_roe[df_roe["yymm"] >= 200603]

        for cmp_cd in list_cmp_cd:
            df = df_roe[df_roe["cmp_cd"] == cmp_cd][["cmp_cd", "yymm", "val"]]

            df["std"] = df["val"].rolling(window=12).std()
            df["std_pct"] = df["std"] / df["val"]

            stack_.append(df)

        df_froe = pd.concat(stack_)
        df_froe["rolling_avg"] = df_froe.groupby('cmp_cd', group_keys=False).apply(rolling_avg)
        df_froe["f_roe"] = (df_froe["val"] / df_froe["rolling_avg"]) * df_froe["val"] - df_froe["std"]

        return df_froe

    def create_dict_exp_return(self, list_cmp_cd, df_froe):

        cache_table = {}
        dict_exp_return = {}

        for cmp_cd in tqdm(list_cmp_cd):

            df_froe_cmp = df_froe[df_froe["cmp_cd"] == cmp_cd]

            if cmp_cd not in self.dict_multiple_cmp_cd.keys():
                continue
            else:
                df_multiple_cmp = self.dict_multiple_cmp_cd[cmp_cd]
                df_multiple_cmp = df_multiple_cmp[df_multiple_cmp["item_cd"] == 900005]

            dict_data = {"date": [], "f_roe": [], "pbr": [], "exp_return": []}
            for t_date in self.list_krx_date:

                yymm = self.date_to_yymm(t_date)
                key_nm = cmp_cd + str(yymm)

                # 상장폐지
                if len(df_froe_cmp.loc[df_froe_cmp["yymm"] == yymm]) == 0:
                    continue

                if key_nm not in cache_table.keys():
                    f_roe = df_froe_cmp.loc[df_froe_cmp["yymm"] == yymm, "f_roe"].values[0]
                else:
                    f_roe = cache_table[key_nm]

                # 상장
                if len(df_multiple_cmp.loc[df_multiple_cmp["date"] == t_date]) == 0:
                    continue

                pbr = df_multiple_cmp.loc[df_multiple_cmp["date"] == t_date, "multiple"].values[0]
                exp_return = f_roe / pbr

                dict_data["date"].append(t_date)
                dict_data["f_roe"].append(f_roe)
                dict_data["pbr"].append(pbr)
                dict_data["exp_return"].append(exp_return)

            dict_exp_return[cmp_cd] = dict_data

        return dict_exp_return

    def save(self, dict_exp_return):

        # save data
        with open(r'D:\MyProject\StockPrice\dict_exp_return.pickle', 'wb') as fw:
            pickle.dump(dict_exp_return, fw)

    def run(self):

        df_froe = self.create_df_froe()

        list_cmp_cd = sorted(df_froe["cmp_cd"].unique())
        dict_exp_return = {}

        n = int(len(list_cmp_cd) / 5)
        nested_list_cmp_cd = [list_cmp_cd[i * n:(i + 1) * n] for i in range((len(list_cmp_cd) + n - 1) // n)]

        list_params = []
        for x in nested_list_cmp_cd:
            list_params.append([x, df_froe])

        p = Pool(len(list_params))

        # 월별 인덱스 생성
        res = p.starmap_async(self.create_dict_exp_return, list_params)

        # dictionary 병합
        for x in res.get():
            dict_exp_return.update(x)

        p.close()
        p.join()

        self.save(dict_exp_return)
