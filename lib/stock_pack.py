from lib import numeric_pack
from pykrx import stock
from datetime import timedelta
import db
from datetime import datetime
import json
import pandas as pd


def set_all_cmp_cd():

    list_cmp_cd = db.redis_client.get("list_cmp_cd")

    if list_cmp_cd is None:
        df_krx = pd.read_sql_query('SELECT * FROM financial_data.krx_stock_info', db.conn)
        list_cmp_cd = df_krx[df_krx["Market"].isin(["KOSPI", "KOSDAQ", "KOSDAQ GLOBAL"])]["Symbol"].to_list()
    else:
        list_cmp_cd = json.loads(list_cmp_cd)

    list_cmp_cd.remove('107640')
    list_cmp_cd.remove('464080')
    list_cmp_cd.remove('160190')
    list_cmp_cd.remove('295310')

    return list_cmp_cd
