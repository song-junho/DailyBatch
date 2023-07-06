import pickle
import datetime
import pandas as pd
from tqdm import tqdm
import  xlwings as xw
from dateutil.relativedelta import relativedelta


class UpdateHotTheme:

    @staticmethod
    def update_hot_theme(is_update_all=False):

        date_today = datetime.datetime.today() - relativedelta(days=1)

        # load data
        with open(r'D:\MyProject\StockPrice\HotIssueDate.pickle', 'rb') as fr:
            df_hot_issue_date = pickle.load(fr)

        # load data
        with open(r'D:\MyProject\StockPrice\DictDfStock.pickle', 'rb') as fr:
            dict_df_stock = pickle.load(fr)

        # 기존 데이터 백업
        with open(r'D:\MyProject\StockPrice\HotTheme.pickle', 'rb') as fr:
            df_hot_theme = pickle.load(fr)
        file_name = 'HotTheme' + datetime.datetime.today().strftime("%Y%m%d")
        with open(r'D:\MyProject\StockPrice\백업\{}.pickle'.format(file_name), 'wb') as fw:
            pickle.dump(df_hot_theme, fw)
        del df_hot_theme

        # 6. 업종사전 생성
        df_cmp_keyword = pd.read_excel(r"D:\MyProject\Notion\키워드_사전.xlsx", dtype="str")

        if is_update_all is True:
            df_hot_theme = pd.DataFrame()
            date_updated = df_hot_issue_date["issue_date"].min()

        else:
            with open(r'D:\MyProject\StockPrice\HotTheme.pickle', 'rb') as fr:
                df_hot_theme = pickle.load(fr)
            date_updated = df_hot_theme["issue_date"].max()
            if date_today.strftime("%Y-%m-%d") == date_updated:
                date_updated = (date_today - relativedelta(days=1)).strftime("%Y-%m-%d")
            # date_updated = '2023-04-23'

        df_hot_issue_date = df_hot_issue_date[df_hot_issue_date["issue_date"] > date_updated]
        df_merge = pd.merge(left=df_hot_issue_date, right=df_cmp_keyword, on="cmp_cd", how="left")
        df_hot_issue_stock_total = df_merge[~df_merge["cmp_nm"].isna()].reset_index(drop=True)

        list_date_range = sorted(df_hot_issue_stock_total["issue_date"].unique())

        for v_date in tqdm(list_date_range):

            df_hot_issue_stock = df_hot_issue_stock_total[df_hot_issue_stock_total["issue_date"] == v_date]

            df_keyword_count = df_cmp_keyword.groupby("keyword").count().reset_index(drop=False)[["keyword", "cmp_cd"]]
            df_keyword_count = df_keyword_count.rename(columns={"cmp_cd": "total_count"})

            df_hot_keyword_count = df_hot_issue_stock.groupby("keyword").count().reset_index(drop=False)[
                ["keyword", "cmp_cd"]]
            df_hot_keyword_count = df_hot_keyword_count.rename(columns={"cmp_cd": "hot_count"})

            df_hot_ratio = pd.merge(left=df_hot_keyword_count, right=df_keyword_count, on="keyword", how="left")
            df_hot_ratio["ratio"] = df_hot_ratio["hot_count"] / df_hot_ratio["total_count"]
            df_hot_ratio = df_hot_ratio[df_hot_ratio["ratio"] > 0.5].sort_values("hot_count",
                                                                                 ascending=False).reset_index(drop=True)

            df_res = pd.merge(left=df_hot_ratio, right=df_hot_issue_stock, on="keyword")
            df_res["market_cap"] = 0
            df_res["change"] = 0
            df_res["volume"] = 0
            df_res["vol_ratio"] = 0

            for i, rows in df_res.iterrows():

                cmp_cd = rows["cmp_cd"]
                issue_date = rows["issue_date"]

                try:
                    df_stock = dict_df_stock[cmp_cd].loc[issue_date]
                except:
                    print(cmp_cd, issue_date)
                    continue
                change = df_stock["Change"]
                market_cap = df_stock["MarketCap"] // 100000000
                volume = df_stock["V_Value"] // 100000000
                vol_ratio = (volume / market_cap) * 100

                df_res.loc[i, "market_cap"] = market_cap
                df_res.loc[i, "change"] = change
                df_res.loc[i, "volume"] = volume
                df_res.loc[i, "vol_ratio"] = vol_ratio

            df_res = df_res.sort_values(["hot_count", "keyword", "volume"], ascending=False).reset_index(drop=True)
            df_res = df_res[df_res["volume"] > 100].reset_index(drop=True)[
                ["issue_date", "keyword", "hot_count", "total_count", "ratio", "cmp_cd", "cmp_nm", "market_cap",
                 "change", "volume", "vol_ratio"]]
            df_res["ratio"] = round(df_res["ratio"], 2)
            df_res["vol_ratio"] = round(df_res["vol_ratio"], 2)
            df_res["change"] = round(df_res["change"], 2)

            df_hot_theme = pd.concat([df_hot_theme, df_res])

        # 중복 제거
        # 중복시, 최근 업데이트 데이터 생존
        df_hot_theme = df_hot_theme.drop_duplicates(["issue_date", "keyword", "cmp_cd"], keep="last")

        # save data
        with open(r'D:\MyProject\StockPrice\HotTheme.pickle', 'wb') as fw:
            pickle.dump(df_hot_theme, fw)

        # daily_issue 엑셀 데이터 업데이트

        date_latest = df_hot_theme["issue_date"].max()
        # date_latest = '2023-04-24'

        df_excel = df_hot_theme[df_hot_theme["issue_date"] == date_latest]
        df_excel["ratio"] = round(df_excel["ratio"], 2)
        df_excel["vol_ratio"] = round(df_excel["vol_ratio"], 2)
        df_excel["change"] = round(df_excel["change"], 2)

        xw.Book(r"D:\MyProject\Notion\daily_issue\daily_issue.xlsx").set_mock_caller()
        wb = xw.Book.caller()

        sheet = wb.sheets[0]
        latest_row_num = sheet.range("A1").end("down").row
        wb_latest_date = str(sheet.range("A" + str(latest_row_num)).value).split(" ")[0]

        print(df_excel)
        if date_latest == wb_latest_date:
            print("최신일자 업데이트 완료")
            return 0
        elif latest_row_num > 100000:
            latest_row_num = 1
        sheet["A" + str(latest_row_num + 1)].options(index=False, header=False).value = df_excel

        latest_row_num = sheet.range("A1").end("down").row
        sheet.range("A"+str(latest_row_num), "K"+str(latest_row_num)).color = (202, 197, 182)
        print(df_excel)
