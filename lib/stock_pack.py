from lib import numeric_pack
from pykrx import stock
from datetime import timedelta
import db
from datetime import datetime
import json


def set_all_cmp_cd(start_date, end_date=datetime.today()):

    list_cmp_cd = db.redis_client.get("list_cmp_cd")

    if list_cmp_cd is None:

        max_date = numeric_pack.get_list_mkt_date(start_date, end_date)[-1]
        list_cmp_cd = stock.get_market_ticker_list(max_date, market="KOSPI") + stock.get_market_ticker_list(max_date, market="KOSDAQ")
        db.redis_client.set("list_cmp_cd", json.dumps(list_cmp_cd), timedelta(minutes=3))

    else:
        list_cmp_cd = json.loads(list_cmp_cd)

    return list_cmp_cd
