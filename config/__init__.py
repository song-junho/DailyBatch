import pathlib
import json


PATH_API_KEY = pathlib.Path(__file__).parents[1] / 'config' / f'api_key.json'
PATH_MACRO_INFO = pathlib.Path(__file__).parents[1] / 'config' / f'macro_info.json'
PATH_ETF_INFO = pathlib.Path(__file__).parents[1] / 'config' / f'etf_info.json'
PATH_UNIPASS_INFO = pathlib.Path(__file__).parents[1] / 'config' / f'unipass_info.json'

PATH_MYSQL_KEY = pathlib.Path(__file__).parents[1] / 'config' / f'mysql_key.json'

with open(PATH_API_KEY, 'r', encoding='utf-8') as fp:
    API_KEY = json.load(fp)

with open(PATH_MACRO_INFO, 'r', encoding='utf-8') as fp:
    MACRO_INFO = json.load(fp)

with open(PATH_ETF_INFO, 'r', encoding='utf-8') as fp:
    ETF_INFO = json.load(fp)

with open(PATH_UNIPASS_INFO, 'r', encoding='utf-8') as fp:
    UNIPASS_INFO = json.load(fp)

with open(PATH_MYSQL_KEY, 'r', encoding='utf-8') as fp:
    MYSQL_KEY = json.load(fp)
