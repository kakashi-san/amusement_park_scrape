import pandas as pd
from pathlib import Path
from modules.interfaces import IRequestsSourcer, IConfigParamsData
import requests
import datetime
from random import randint
from time import sleep
import os
from os.path import join as opj
from pathlib import Path
from utils import *

start_date = '2023-08-11'
end_date = '2023-12-31'
api_data_path = Path('data/input/cedarpoint/cedar_point.xlsx')
api_data = pd.read_excel(api_data_path)
api_data = api_data.reset_index(drop=True)
output_dir = Path('long_path/to/my_dir')



for idx, api_row in api_data.iterrows():
    api_url = api_row['API endpoint']
    ticker = api_row['Company']
    park_name = api_row['Park Name']

    sleep(randint(1,5) )


    generate_raw_monthly_date(
        api_url=api_url,
        start_date=start_date,
        end_date=end_date,
        ticker=ticker,
        park_name=park_name,
        op_path=output_dir,
        filter_cols=filter_dict[park_name]
    )