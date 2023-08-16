import pandas as pd
from pathlib import Path
from modules.config_parser import YAMLConfigReader
import os
import time
import re
import calendar
import datetime
import requests
from modules.interfaces import IRequestsSourcer, IConfigParamsData
from bs4 import BeautifulSoup
from modules.page_sourcer import ChromePageSourcer

api_data_path = Path('data/input/sixflags/sixflags_page_src_data.xlsx')
api_data = pd.read_excel(api_data_path)
start_date = '2023-08-11'
end_date = '2023-12-31'
webdriver_path = 'data/chrome/chromedriver.exe'


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

class GetRequestsSourcer(IRequestsSourcer):
    def source_page(self):
        return requests.get(
            self.url,
            params=self.params,
            )

def parse_month_day(value_str):
    print("*"*100, value_str)
    if not 'Park Hours:' in str(value_str):
        try:
            return int(value_str)
        except:
            return None
    for value_match in re.findall(r'[0-9]*\s\s', value_str):
        return int(value_match)

def parse_park_hours(value_str):
    return re.findall(r'[0-9]+\:[0-9]+\s[a-z]*', str(value_str))

def trasnsform_data(sauce):

    soup = BeautifulSoup(sauce, 'html.parser')
    calendar_months = soup.find_all('div', attrs={'class': 'jet-calendar-caption__name'})
    
    tables = pd.read_html(soup.prettify())
    collect = []
    assert len(calendar_months) == len(tables)
    months_n_tables = tuple(zip(calendar_months, tables))
    for (month,table) in months_n_tables:
        print(table)
        collect_enable = False

        for index, row in table.iterrows():
            for column in table.columns:

                print('row number:', index)
                print('column name:',column)
                print('value: ',row[column])
                print('day_number', parse_month_day(row[column]))
                print('park_hours', parse_park_hours(row[column]))
                day_number = parse_month_day(row[column])
                if day_number == 1:
                    collect_enable = not collect_enable
                if collect_enable:
                    park_hours = parse_park_hours(row[column])
                    collect_dict = {}
                    collect_dict['day_number'] = day_number

                    collect_dict['open hour'] = park_hours[0] if park_hours else None
                    collect_dict['close hour'] = park_hours[1] if park_hours else None
                    collect_dict['month'] = month.text
                    collect.append(collect_dict.copy())
        
    pre_final_data = pd.DataFrame(collect)
    pre_final_data.dropna(subset=['day_number'], inplace=True)
    return pre_final_data

final_data = []

for _, api_row in api_data.iterrows():
    
    api_url = api_row['URL']
    ticker = api_row['Company']
    park_name = api_row['Park Name']
    params = []

    # gps = GetRequestsSourcer(
    #     url=api_url,
    #     params=params
    #     )

    cps = ChromePageSourcer(
        page_url=api_url,
        webdriver_path=webdriver_path,
    )

    sauce = cps.get_page_source()

    # page_src = gps.source_page()
    # response_url = gps.source_page().url
    time.sleep(10)

    raw_data = trasnsform_data(sauce)
    raw_data['ticker'] = ticker
    raw_data['park_name'] = park_name
    # sauce = cps.get_page_source()
    # breakpoint()
    # print(response_url)

    final_data.append(raw_data)

final_data = pd.concat(final_data)
final_data.to_excel('final_data.xlsx')

