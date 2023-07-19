import pandas as pd
from tqdm import tqdm
import pickle
import copy
from functools import reduce
from abc import *


class ChgFreq(metaclass=ABCMeta):

    def __init__(self):

        self.dict_main_data = {}
        self.dict_chg_freq = {}
        self.asset_type = None
        self.feature_type = None

    @abstractmethod
    def set_type(self):
        self.asset_type = None
        self.feature_type = None

    @abstractmethod
    def set_main_data(self):
        self.dict_main_data = {}

    def preprocessing(self, df, val_nm):

        df = df.resample("1M").last()
        df = df.reset_index(drop=False)
        df["pct_change"] = df[val_nm].pct_change()
        df["Date"] = pd.to_datetime(df["Date"].dt.strftime("%Y-%m-%d"))
        df = df[["Date", val_nm, "pct_change"]].rename(columns={"Date": "date", val_nm: "val"})
        df = df.set_index("date")

        return df

    def save(self):

        with open(r'D:\MyProject\StockPrice\dict_{}_{}_chg_freq.pickle'.format(self.asset_type, self.feature_type),
                  'wb') as fw:
            pickle.dump(self.dict_chg_freq, fw)

    def create_chg_freq(self, std_col):

        list_nm = list(self.dict_main_data.keys())

        # 전처리: 월간 데이터 형식
        for key_nm in tqdm(list_nm):
            df = self.dict_main_data[key_nm]
            if len(df) == 0:
                continue
            self.dict_main_data[key_nm] = self.preprocessing(df, std_col)

        # 주기별 변화율 데이터 생성
        for key_nm in tqdm(list_nm):

            if len(self.dict_main_data[key_nm]) == 0:
                continue

            list_tmp = []
            for freq in [1, 3, 6, 12]:
                df = copy.deepcopy(self.dict_main_data[key_nm])
                if freq == 1:
                    df = df[["pct_change"]].rename(columns={"pct_change": str(freq) + "M"})
                    list_tmp.append(df)
                else:
                    df["pct_change"] = df["val"].pct_change(freq)
                    df = df[["pct_change"]].rename(columns={"pct_change": str(freq) + "M"})
                    list_tmp.append(df)

            df_chg = reduce(lambda left, right: pd.merge(left, right, left_index=True, right_index=True), list_tmp)

            self.dict_chg_freq[key_nm] = pd.merge(left=self.dict_main_data[key_nm][["val"]], right=df_chg,
                                                  left_index=True,
                                                  right_index=True)

    @abstractmethod
    def run(self):
        pass


class StockChgFreq(ChgFreq):
    """
    월간 데이터 , 1M, 3M, 6M, 12M 주기별 변화율 저장
    """

    def set_type(self):
        self.asset_type = 'stock'
        self.feature_type = 'krx'

    def set_main_data(self):
        with open(r'D:\MyProject\StockPrice\DictDfStock.pickle', 'rb') as fr:
            self.dict_main_data = pickle.load(fr)

    def run(self):
        self.set_type()
        self.set_main_data()
        self.create_chg_freq("Close")
        self.save()


class ThemeChgFreq(ChgFreq):
    """
    월간 데이터 , 1M, 3M, 6M, 12M 주기별 변화율 저장
    """

    def set_type(self):
        self.asset_type = 'stock'
        self.feature_type = 'theme'

    def set_main_data(self):
        with open(r'D:\MyProject\StockPrice\DictThemeIndex.pickle', 'rb') as fr:
            self.dict_main_data = pickle.load(fr)

    def run(self):
        self.set_type()
        self.set_main_data()
        self.create_chg_freq("index")
        self.save()
