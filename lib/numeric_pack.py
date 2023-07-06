import exchange_calendars as ecals
import pandas as pd
from datetime import datetime
import numpy as np

# 한국 개장일자
XKRX = ecals.get_calendar("XKRX")


def get_list_mkt_date(date_start, date_end):

    list_date = pd.date_range(date_start, date_end)
    list_date = sorted(set(list_date) & set(XKRX.schedule.index))

    return list_date

def get_list_eom_date(list_date):

    '''
    list_date 기준 EOM date 반환
    :param list_date:
    :return:
    '''

    list_date_eom = []  # 월말

    year = 0
    month = list_date[0].month
    days = 0

    for p_date in list_date:

        if p_date.month != month:
            list_date_eom.append(datetime(year, month, days))

        year = p_date.year
        month = p_date.month
        days = p_date.day

    return list_date_eom

def change_date_to_mkt_date(df):

    '''
    DataFrame의 'date' 칼럼의 값을 마켓 일자로 변경
    '''

    df["date"] = df["date"].astype("str")

    list_mkt_date = XKRX.schedule.index.astype("str")
    df["date"] = df["date"].apply(lambda x : sorted([mkt_date for mkt_date in list_mkt_date if mkt_date <= x])[-1])
    df["date"] = pd.to_datetime(df["date"])

    return df

def price_to_adj_price(df):
    '''
    수정주가 변환
    '''

    df["price_pct_chg"] = (df["등락률"] / 100) + 1

    # 역순으로 변환
    latest_price = df["종가"].iloc[-1]
    list_pct_chg = df["price_pct_chg"].sort_index(ascending=False).to_list()
    list_pct_chg = np.cumprod(list_pct_chg)

    df["price_pct_chg"] = (df["등락률"] / 100) + 1

    list_adj_price = [int(latest_price / x) for x in list_pct_chg]
    list_adj_price.reverse()
    list_adj_price.append(latest_price)

    df["adj_price"] = list_adj_price[1:]
    df["시가"] = (df["시가"] * (df["adj_price"] / df["종가"])).astype("int64")
    df["고가"] = (df["고가"] * (df["adj_price"] / df["종가"])).astype("int64")
    df["저가"] = (df["저가"] * (df["adj_price"] / df["종가"])).astype("int64")
    df["종가"] = (df["종가"] * (df["adj_price"] / df["종가"])).astype("int64")

    return df