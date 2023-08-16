import pandas as pd
from pathlib import Path
from modules.config_parser import YAMLConfigReader
import os
import calendar
import datetime
import requests
from modules.interfaces import IRequestsSourcer, IConfigParamsData
api_data_path = Path('data/input/seaworld/seaworld_api_data.xlsx')
api_data = pd.read_excel(api_data_path)
start_date = '2023-08-11'
end_date = '2023-12-31'

class DateParamsGen:
    @staticmethod
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

        return [month.strftime("%Y-%m-%d") for month in month_list]
    @staticmethod
    def get_last_day_of_month(date: str):
        '''
        helper function to get last day of the month.
        '''
        year, month, day = date.split('-')
        year = int(year)
        month = int(month)
        _, num_days = calendar.monthrange(year, month)
        return datetime.date(year, month, num_days).strftime('%Y-%m-%d')

    @staticmethod
    def get_last_day_of_prev_month(date: str):
        '''
        helper function to get last day of the previous month.
        '''
        year, month, day = date.split('-')
        year = int(year)
        month = int(month) - 1
        _, num_days = calendar.monthrange(year, month)
        return datetime.date(year, month, num_days).strftime('%Y-%m-%d')


class SWParamsSetGen:
    def __init__(
        self,
        start_date,
        end_date
    ):
        self.start_date = start_date
        self.end_date = end_date

        self.dpg = DateParamsGen()
        self._date_range = self.dpg.generate_date_range(
            start_date=self.start_date,
            end_date=self.end_date
        )
        print(self._date_range)
    def generate_params(self):
        params = []
        for date in self._date_range:
            param = {
                'start' : self.dpg.get_last_day_of_prev_month(
                    date=date
                ),
                'end' : self.dpg.get_last_day_of_month(
                    date=date
                )
            }
            params.append(param)
            print(date)
            print(param)
        return params
def extract_open_close_hrs(ip_str):
    if '-' in ip_str:
        return ip_str.split('-') 
    else:
        return None, None

def transform_data(sw_point_data):
    raw_data = pd.DataFrame.from_dict(sw_point_data)
    if not raw_data.empty:

        raw_data.dropna(subset=['type'],inplace=True)
        raw_data['date'] = raw_data['start'].apply(lambda x: str(x).split('T')[0])
        # raw_data.drop(columns=['isParkHours', 'isOpen','startReal', 'endReal','start', 'end', 'signUrl', 'url', 'time', 'customColor'], inplace=True)
        raw_data['open hours']  = raw_data['title'].apply(lambda x: extract_open_close_hrs(x)[0])
        # try:
        raw_data['close hours'] = raw_data['title'].apply(lambda x: extract_open_close_hrs(x)[1])

        raw_data.drop(columns=['title'],inplace=True)
        raw_data.sort_values(by='date', inplace=True)
        return raw_data
    return pd.DataFrame()
# day_week_map = {
#     0: 'Monday',
#     1: 'Tuesday',
#     2: 'Wednesday',
#     3: 'Thursday',
#     4: 'Friday',
#     5: 'Saturday',
#     6: 'Sunday'
# }
def get_time_24hr(time_str):
    if not time_str:
        return None
    # Convert to datetime object using the given format
    time_obj = datetime.datetime.strptime(time_str, '%I:%M %p')

    # Convert to 24-hour format
    time_24hr = time_obj.strftime('%H:%M')

    return time_24hr

# def week_of_month(tgtdate):
#     # tgtdate = tgtdate.to_datetime()

#     days_this_month = calendar.mdays[tgtdate.month]
#     for i in range(1, days_this_month):
#         d = datetime.datetime(tgtdate.year, tgtdate.month, i)
#         if d.day - d.weekday() > 0:
#             startdate = d
#             break
#     # now we canuse the modulo 7 appraoch
#     return (tgtdate - startdate).days //7 + 1

class GetRequestsSourcer(IRequestsSourcer):
    def source_page(self):
        return requests.get(
            self.url,
            params=self.params,
            )

init_df = []
for _, api_row in api_data.iterrows():
    swpsg = SWParamsSetGen(
        start_date=start_date,
        end_date=end_date
    )
    api_url = api_row['API endpoint']
    ticker = api_row['Company']
    park_name = api_row['Park Name']
      

    for params in swpsg.generate_params():
        print(params)
        print(api_url)
        print('----------------------------')
        gps = GetRequestsSourcer(
            url=api_url,
            params=params
            )
        page_src = gps.source_page().json()
        response_url = gps.source_page().url
        raw_data = transform_data(page_src)
        if not raw_data.empty:
            raw_data.drop(columns='type', inplace=True)
            raw_data['ticker'] = ticker
            raw_data['url'] = response_url
            raw_data['park_name'] = park_name
            raw_data['open hours'] = raw_data['open hours'].str.strip().apply(get_time_24hr)
            raw_data['close hours'] = raw_data['close hours'].str.strip().apply(get_time_24hr)
            # raw_data['day_of_week'] = pd.to_datetime(
            #     raw_data['date']
            #     ).dt.day_of_week.map(day_week_map)
            # breakpoint()
            init_df.append(raw_data)
# init_df[]
pd.concat(init_df)[['date','startReal','park_name', 'ticker', 'open hours', 'close hours', 'url']].to_excel('init_sw_df.xlsx')