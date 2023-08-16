import pandas as pd
from pathlib import Path
from modules.interfaces import IRequestsSourcer, IConfigParamsData
import requests
import datetime

start_date = '2023-08-11'
end_date = '2023-12-31'
api_data_path = Path('data/input/cedarpoint/cedar_point.xlsx')
api_data = pd.read_excel(api_data_path)

class GetRequestsSourcer(IRequestsSourcer):
    def source_page(self):
        return requests.get(
            self.url,
            params=self.params,
            )


def generate_date_range(
    start_date,
    end_date
    ):
    '''
    helper function to generate extensions from iter params.
    '''

    month_list = pd.period_range(
        start=start_date,
        end=end_date,
        freq='M',
        ).to_list()

    return [month.strftime("%m/%Y") for month in month_list]

def get_time_24hr(time_str):
    if not time_str:
        return None
    # Convert to datetime object using the given format
    try:
        time_obj = datetime.datetime.strptime(time_str, '%I %p')
    except:
        time_obj = datetime.datetime.strptime(time_str, '%I:%M %p')

    # Convert to 24-hour format
    time_24hr = time_obj.strftime('%H:%M')

    return time_24hr

def send_request_get(url):
    payload = {'api_key': '0cd548101663bb8674791e24f0831643', 'url': url}

    response = requests.get('http://api.scraperapi.com', params=payload)
    return response

def transform_data(page_src):
    '''
    helper function to transform data.
    '''
    json_data = page_src.json()
    collector = []
    for park_idx, park_data in json_data.items():
            print(park_idx)
            if isinstance(json_data[park_idx], dict):
                raw_park_data = pd.DataFrame.from_dict(json_data[park_idx]['calendar'])
            else:
                continue
            if not raw_park_data.empty:
                raw_park_data = raw_park_data[['day', 'dayTitle', 'hours']]
                raw_park_data['day'] = raw_park_data['day'].apply(int)
                raw_park_data.rename(columns={'dayTitle':'park_type'}, inplace=True)
                raw_park_data.sort_values(by='day', inplace=True)
                raw_park_data.reset_index(inplace=True, drop=True)

                raw_park_data['open_hours'] = raw_park_data['hours'].apply(lambda x: x.split('-')[0])
                raw_park_data['close_hours'] = raw_park_data['hours'].apply(lambda x: x.split('-')[1])
                raw_park_data.drop(columns=['hours'], inplace=True)
                raw_park_data['month'] = month
                collector.append(raw_park_data)
    if collector:
        concat_data = pd.concat(collector)
        concat_data['Date'] = concat_data['day'].astype(str).str.zfill(2) + '/' + concat_data['month']

        concat_data['open_hours'] = concat_data['open_hours'].str.strip().apply(get_time_24hr)
        concat_data['close_hours'] = concat_data['close_hours'].str.strip().apply(get_time_24hr)

        concat_data.drop(columns=['day', 'month'], inplace=True)

        return concat_data
    return pd.DataFrame()

months_range = generate_date_range(start_date, end_date)

collect = []

for _, api_row in api_data.iterrows():
    api_url = api_row['API endpoint']
    ticker = api_row['Company']
    park_name = api_row['Park Name']

    

    for month in months_range:
        get_url = "/".join(
            [api_url, month]
            )

        print('----------------------------')
        print(get_url)
        page_src = send_request_get(
            url=get_url
        )

        
        try:
            raw_data = transform_data(page_src)
        
        except:
            print(raw_data)
            print(page_src)
            breakpoint()
        raw_data['ticker'] = ticker
        raw_data['park_name'] = park_name


        collect.append(raw_data)

pd.concat(collect).to_excel('cedarpoint_data.xlsx')



