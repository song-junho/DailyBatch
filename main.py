from market_data import UpdateMarketData
from hot_issue import UpdateHotIssue
from hot_theme import UpdateHotTheme


def main():

    is_update_all = False
    update_market_data = UpdateMarketData(is_update_all)
    update_market_data.update_market_data()
    del update_market_data

    update_hot_issue = UpdateHotIssue()
    update_hot_issue.update_hot_issue_date()
    del update_hot_issue

    UpdateHotTheme.update_hot_theme(False)

if __name__ == "__main__":

    main()