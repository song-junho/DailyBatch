from market_data import UpdateMarketData, UpdateDaily, MacroData, EtfData
from hot_issue import UpdateHotIssue
from hot_theme import UpdateHotTheme
from index_data.theme import ThemeIndex
from exp_return import UpdateExpReturn
from datetime import datetime
import momentum.create_fields
import lib


def main():

    # 업데이트: 종목 마켓데이터(key:cmp_cd)
    is_update_all = False
    update_market_data = UpdateMarketData(is_update_all)
    update_market_data.run()
    del update_market_data

    # 업데이트: 종목 마켓데이터(key:date)
    is_update_all = False
    update_daily = UpdateDaily(is_update_all)
    update_daily.run()
    del update_daily

    # 업데이트: 종목 마켓데이터(기간 변화율)
    c_chg_freq = lib.StockChgFreq()
    c_chg_freq.run()
    del c_chg_freq

    # 업데이트: 매크로 데이터
    MacroData().run()

    # 업데이트: 매크로 데이터
    EtfData().run()

    # 업데이트: 모멘텀 데이터 생성 (Stock)
    momentum.create_fields.create_stock_field()

    # 업데이트: 일별 핫이슈 종목 정보
    update_hot_issue = UpdateHotIssue()
    update_hot_issue.update_hot_issue_date()
    del update_hot_issue

    # 업데이트: 일별 주요 테마 정보
    UpdateHotTheme.update_hot_theme(False)

    # 업데이트: 인덱스(테마)
    c_theme_index = ThemeIndex(datetime(2006, 1, 1), datetime.today())
    c_theme_index.run()
    del c_theme_index

    # 업데이트: 인덱스(테마)(기간 변화율)
    c_chg_freq = lib.ThemeChgFreq()
    c_chg_freq.run()
    del c_chg_freq

    # 업데이트: 종목별 기대수익률
    UpdateExpReturn().run()

if __name__ == "__main__":

    main()
