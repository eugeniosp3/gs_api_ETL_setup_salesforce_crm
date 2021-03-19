import pandas as pd
import requests
from random import randint
import time

df = pd.read_csv('runthese.csv')


api_keys = 'must_get_from_vendor'

def create_ein_formatted_list(df):
    """ Enter a dataframe into this function which has a clearly labeled EIN column of all EINs you want to pass into XX-XXXXXX format"""
    orgs_list = df['ein'].to_list()

    return orgs_list


def run_api_get_data(ein_list, api_keys):
    appended_data = []
    counter_var = 0
    try:
        for r in ein_list:
            URL = 'http://www.yoururl.com/{ein}?API-Key={skey}'.format(ein=r, skey=api_keys)
            response = requests.get(URL)
            main_json = response.json()
            pd.json_normalize(main_json, max_level=12)
            t1 = pd.json_normalize(main_json, max_level=8)
            appended_data.append(t1)
            counter_var += 1
            print('Counter at: {x}'.format(x=counter_var), 'EIN Passed: ', str(r), '\n', 'Status Code: ', response.status_code)

            time.sleep(randint(6, 8))
    except:
        pass
    return appended_data

ein_list = create_ein_formatted_list(df)
appended_from_api = run_api_get_data(ein_list, api_keys)

appended_data = pd.concat(appended_from_api)
appended_data.to_csv('RANTHESE.csv')