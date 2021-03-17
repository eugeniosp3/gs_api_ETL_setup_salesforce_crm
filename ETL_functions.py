import pandas as pd
import requests
from random import randint
import time


def create_ein_formatted_list(df):
    """ Enter a dataframe into this function which has a clearly labeled EIN column of all EINs you want to pass into XX-XXXXXX format"""
    ein_list = []
    orgs_list = df['add_ein'].to_list()
    for r in orgs_list:
        r = str(r)
        stringy1 = r[:2]
        stringy2 = r[2:]
        result = stringy1 + '-' + stringy2
        result = result.replace('.0', '')
        if result == 'na-n':
            continue
        ein_list.append(result)
    return ein_list


def run_api_get_data(ein_list, skey):
    appended_data = []
    counter_var = 0
    try:
        for r in ein_list:
            URL = 'https://apidata.guidestar.org/premier/v1/{ein}?Subscription-Key={skey}'.format(ein=r, skey=skey)
            response = requests.get(URL)
            main_json = response.json()
            pd.json_normalize(main_json, max_level=12)
            t1 = pd.json_normalize(main_json, max_level=10)
            appended_data.append(t1)
            counter_var += 1
            time.sleep(randint(6, 8))
    except:
        pass
    return appended_data