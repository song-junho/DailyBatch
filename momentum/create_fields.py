from .asset.stock import Stock
from multiprocessing import Pool
import pandas as pd
from tqdm import tqdm
import pickle
from lib.numeric_pack import *

# 일자
start_date = "2006-01-01"
end_date = datetime.today()


def save(asset_type, res_process):

    # process 결과물 병합후 저장
    dict_list = res_process.get()

    # dictionary 병합
    dict_df_stock_monthly = {}
    for x in dict_list:
        dict_df_stock_monthly.update(x)

    if asset_type == 'stock':

        with open(r"D:\MyProject\StockPrice\DictDfStockMonthly.pickle", 'wb') as fw:
            pickle.dump(dict_df_stock_monthly, fw)


def create_stock_field():

    # 영업일(개장일) 기준 월말 리스트 생성
    list_date = pd.date_range(start_date, end_date, freq="M")
    list_mkt_date = get_list_mkt_date(start_date, end_date)
    list_date_eom = get_list_eom_date(list_mkt_date)

    # 멀티프로세스로 구분할 일자 리스트 생성
    list_date_eom = sorted(list_date_eom)
    n = int(len(list_date_eom) / 2)
    nested_date_eom = [list_date_eom[i * n:(i + 1) * n] for i in range((len(list_date_eom) + n - 1) // n)]

    # 자산: 종목 클래스 생성
    stock_price_momentum = Stock(list_date_eom)
    p = Pool(len(nested_date_eom))
    save('stock', p.map_async(stock_price_momentum.run, nested_date_eom))
    p.close()
    p.join()
