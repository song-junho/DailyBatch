import pathlib
import json


PATH_API_KEY = pathlib.Path(__file__).parents[1] / 'config' / f'api_key.json'
PATH_DATA_INFO = pathlib.Path(__file__).parents[1] / 'config' / f'data_info.json'

with open(PATH_API_KEY, 'r', encoding='utf-8') as fp:
    API_KEY = json.load(fp)

with open(PATH_DATA_INFO, 'r', encoding='utf-8') as fp:
    DATA_INFO = json.load(fp)