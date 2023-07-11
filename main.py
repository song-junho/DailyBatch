from market_data import UpdateMarketData, UpdateDaily, MacroData
from hot_issue import UpdateHotIssue
from hot_theme import UpdateHotTheme


def main():

    # 업데이트: 종목 마켓데이터(key:cmp_cd)
    is_update_all = False
    update_market_data = UpdateMarketData(is_update_all)
    update_market_data.update_market_data()
    del update_market_data

    # 업데이트: 종목 마켓데이터(key:date)
    is_update_all = False
    update_daily = UpdateDaily(is_update_all)
    update_daily.run()
    del update_daily

    # 업데이트: 매크로 데이터
    MacroData().run()

    # 업데이트: 일별 핫이슈 종목 정보
    update_hot_issue = UpdateHotIssue()
    update_hot_issue.update_hot_issue_date()
    del update_hot_issue

    # 업데이트: 일별 주요 테마 정보
    UpdateHotTheme.update_hot_theme(False)

if __name__ == "__main__":

    main()
