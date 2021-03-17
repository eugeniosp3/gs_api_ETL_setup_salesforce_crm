import pandas as pd
import requests
from random import randint
import time
import random
from ETL_functions import *
from transformations_1.py import *


df = pd.read_csv('file_name.csv')
df1 = df[:500]
df2 = df[500:1000]
df3 = df[1000:1500]
df4 = df[1500:1901]

df_list = [df, df1, df2, df3, df4]

api_keys = ['8b7e258748b442bf8a359ab7c9892aeb', '31c63ecab17640988053c0f3d8513e46', '44dafde34d8047328c52ac166b11706c',
            'ed9cc9d71d84429985b7a1b21ad0dbcd', 'keyba27d366fbaa4531906abdbb35fd00c6', 'e26ebab56626441492911d78ccdf0ee4',
            '300e1a21004e416981a62782969418c6', '22c653602d4848c0b59aed70eb21e0a0']

def begin_api_process(df_list, api_keys):
    full_list_requested = []
    for adf in df_list:
        from_df_to_list = create_ein_formatted_list(adf)
        to_append = run_api_get_data(a_list, [x for x in random.sample(api_keys, 1)])
        full_list_request.append(to_append)
    write_to_csv = pd.contact(full_list_requested)
    return write_to_csv

main_df = begin_api_process(df_list, api_keys)
working_df = main_df_factory(df)
create_files = create_load_files()
