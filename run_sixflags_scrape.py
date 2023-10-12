import pandas as pd
from pathlib import Path
from modules.config_parser import YAMLConfigReader
import os
import time
import re
import calendar
import datetime
from random import randint
import requests
from modules.interfaces import IRequestsSourcer, IConfigParamsData
from bs4 import BeautifulSoup
from modules.page_sourcer import ChromePageSourcer, RHPageSourcer

api_data_path = Path('data/input/sixflags/sixflags_page_src_data.xlsx')
api_data = pd.read_excel(api_data_path)
start_date = '2023-08-11'
end_date = '2023-12-31'
webdriver_path = r"C:\Users\pednsid\Downloads\chromedriver-win32\chromedriver-win32\chromedriver.exe"

filter_dict = {
	'Discovery Kingdom' : ['Park Hours:'],
	'Magic Mountain' : ['Park Hours:'],
	'Over Georgia' : ['Park Hours:'], # needs fix.
	'Great America': ['Park Hours:'],
	'America' : ['Park Hours:'],
	'New England' : ['Park Hours:'],
	'St. Louis' : ['Park Hours:'],
	'Great Adventure' : ['Park Hours:', 'Wild Safari'],
	'Great Escape & Splashwater Kingdom' : ['Park Hours:'],
	'Fiesta Texas' : ['Park Hours:'],
	'Over Texas' : ['Park Hours:'],
	'La Ronde, Canada' : ['Fright Fest'],
	'Mexico' : ['Horario Del Parque:'],
	'Hurricane Harbor, Oaxtepec, Mexico' : ['Horario Del Parque:'],
	'Over Georgia - Waterpark (Hurricane Harbor)' : ['Waterpark:'],
	'White Water - Atlanta' : ['Park Hours:'],
	'Great America - Water Park (Hurricane Harbor Chicago)': ['Park Hours:'],
	'America - Water Park' : ['Waterpark:'],
	'New England - Water Park' : ['Waterpark:'],
	'St. Louis - Water Park (Hurricane Harbor)' : ['Hurricane Harbor'], 
	'Hurricane Harbor - New Jersey' : ['Park Hours:'],
	'Great Escape & Splashwater Kingdom - Water Park' : ['Waterpark:'],
	'Fiesta Texas - Water Park (Hurricane Harbor - San Antonio)' : ['Hurricane Harbor San Antonio'],
	'Hurricane Harbor Texas  - Arlington' :['Park Hours:'],
	'Hurricane Harbor - Concord': ['Park Hours:'],
	'Hurricane Harbor - Los Angeles': ['Park Hours:'],
	'Hurricane Harbor Phoenix' : ['Park Hours:'],
	'Magic Water Illinois (Hurricane Harbor Rockford)': ['Park Hours:'],
	'Darien Lake' : ['Park Hours:'],
	'Frontier City' : ['Park Hours:'],
	'White Water Bay (Hurricane Harbor OKC)' : ['Park Hours:'],
	'Hurricane Harbor Splashtown' : ['Park Hours:'],
}

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
    # print("*"*100, value_str)
    # if not 'Park Hours:' in str(value_str):
    #     try:
    #         return int(value_str)
    #     except:
    #         breakpoint() 
    value_str = value_str if isinstance(value_str, str) else str(value_str)
    try:  
       return re.search(r'[0-9]+', value_str).group()  
    except:
        breakpoint()


def parse_park_hours(value_str):
    return re.findall(r'[0-9]+\:[0-9]+\s[a-z]*', str(value_str))

def parse_event_str(value_str):
    return re.findall(
        r"(?:[A-Z][a-z]*\s*)+:*(?:\s*[0-9]+:+[0-9]+\s*[a-z]+\s*-*)+",
        # r'(?:[A-z]+[\s:]+)+[0-9]+\:[0-9]+\s*[a-z]*\s*-\s*[0-9]+\:[0-9]+\s*[a-z]*',
        # r'(?:[A-z]+[\s:]+)+[0-9]+\:[0-9]+[a-z]*\s*-\s*[0-9]+\:[0-9]+\s*[a-z]*',
        # r'(?:[A-z]+[\s:]+)*',
        # r'[A-z]+\s[A-z:]*\s*[0-9]+\:[0-9]+\s[a-z]*\s*-\s*[0-9]+\:[0-9]+\s[a-z]*',
         str(value_str)
         )
def parse_hour_range(value_str):
    return re.search(
        r"(?:\s*[0-9]+:+[0-9]+\s*[a-z]+\s*-*)+",
        # r'[0-9]+\:[0-9]+\s*[a-z]*\s*-\s*[0-9]+\:[0-9]+\s*[a-z]*',
        str(value_str)
        ).group().strip()

def parse_event_name_str(value_str):
    return re.search(
        r"(?:[A-Z][a-z]*\s*)+:*",
        # r'(?:[A-z]+[\s:]+)+',
         str(value_str)
        ).group().strip()

def trasnsform_data(
    sauce,
    park_name
    ):

    soup = BeautifulSoup(sauce, 'html.parser')
    calendar_months = soup.find_all('div', attrs={'class': 'jet-calendar-caption__name'})
    # print("CALENDAR MONTHS:",calendar_months)
    
    tables = pd.read_html(soup.prettify())
    # print("TABLES:", tables)
    collect = []
    assert len(calendar_months) == len(tables)
    months_n_tables = tuple(zip(calendar_months, tables))
    for (month,table) in months_n_tables:
        collect_enable = False
        # print("Collect Enable: ", collect_enable)

        for index, row in table.iterrows():
            for column in table.columns:

                # print('row number:', index)
                # print('column name:',column)
                print('value: ',row[column])
                # print('month: ', month)
                        

                # print('day_number', parse_month_day(row[column]))
                # print('park_hours', parse_park_hours(row[column]))
                # print('event_str', parse_event_str(row[column]))
                day_number = parse_month_day(row[column])
                if day_number is None:
                    breakpoint()
                if int(day_number) == 1:
                    collect_enable = not collect_enable
                if collect_enable:
                #     park_hours = parse_park_hours(row[column])
                    collect_dict = {}
                    # print('day number: ', day_number)
                    collect_dict['day_number'] = day_number
                    collect_dict['month'] = month.text

                    events = parse_event_str(row[column])
                    try:
                        if events:
                            for idx, event in enumerate(events):
                                event_name = parse_event_name_str(event)
                                event_hrs = parse_hour_range(event)
                                # print('event', event)
                                # print('event_name', event_name)
                                # print('event_hrs', event_hrs)
                                print('EVENT NAME:', event_name)
                                print('EVENT DICT:', filter_dict[park_name])
                                print('EVENT HRS:', event_hrs)
                                if not event_name:
                                    event_name = [None]
                                if event_name in filter_dict[park_name]:
                                    print('EVENT NAME IN FILTER:', event_name)
                                    collect_dict[f'event_name_{idx}'] = event_name
                                    collect_dict[f'event_hrs_{idx}'] = event_hrs
                    except:
                        breakpoint()
                    collect.append(collect_dict.copy())
                    
                

        
    pre_final_data = pd.DataFrame(collect)
    # pre_final_data.dropna(subset=['day_number'], inplace=True)
    return pre_final_data

final_data = []

for row_idx, api_row in api_data.iterrows():
    save_file = f'raw_data_{row_idx}.xlsx'
    if os.path.exists(save_file):
        print('Scraped Already')
        continue
    api_url = api_row['URL']
    ticker = api_row['Company']
    park_name = api_row['Park Name']
    params = []
    print("PARK NAME: ", park_name)
    # gps = GetRequestsSourcer(
    #     url=api_url,
    #     params=params
    #     )
    # cps = RHPageSourcer(
    #     page_url=api_url
    # )

    cps = ChromePageSourcer(
        page_url=api_url,
        webdriver_path=webdriver_path,
    )


    sauce = cps.get_page_source()
    # page_src = gps.source_page()
    # response_url = gps.source_page().url
    time.sleep(randint(1,5))

    raw_data = trasnsform_data(
        sauce,
        park_name
        )
    raw_data['ticker'] = ticker
    raw_data['park_name'] = park_name
    raw_data.to_excel(save_file)
    # sauce = cps.get_page_source()
    # breakpoint()
    # print(response_url)

    final_data.append(raw_data)

final_data = pd.concat(final_data)
final_data.to_excel('sixflags_data.xlsx')

