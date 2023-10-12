import requests
import pandas as pd
from time import sleep
from random import randint
import datetime
from os.path import join as opj
from pathlib import Path

def generate_data(test_url):
    x = requests.get(test_url)
    return pd.DataFrame.from_dict(x.json())

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

def get_hr_int(date_str):
    return int(date_str.split(':')[0])

def apply_correction(x):
    if x[0] > x[1]:
        print("adjusted:", f"{get_hr_int(x[1]) + 24}:00")
        return f"{get_hr_int(x[1]) + 24}:00"
    return x[1]

def transform_data(
    page_src,
    month
):
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
        concat_data['close_hours'] = concat_data['close_hours'].apply(lambda x: '24:00' if x == '00:00' else x)
        # breakpoint()q

        concat_data['close_hours'] = concat_data[['open_hours','close_hours']].apply(apply_correction, axis=1)



        concat_data.drop(columns=['day', 'month'], inplace=True)

        return concat_data
    return pd.DataFrame()

def send_request_get(url):
    payload = {'api_key': '0cd548101663bb8674791e24f0831643', 'url': url}

    response = requests.get('http://api.scraperapi.com', params=payload)
    return response

def collate_raws(raw_data):
    if not 'park_type' in raw_data.columns:
        return pd.DataFrame()

    unique_park_types = raw_data['park_type'].unique().tolist()
    collect = []
    print(raw_data.columns)
    print("UNIQUE PARK TYPES:",unique_park_types)
    
    for p_type in unique_park_types:
        a = raw_data[raw_data['park_type'] == p_type]
        a.rename(
            columns=
            {
                'open_hours' : f'{p_type}_open_hours',
                'close_hours' : f'{p_type}_close_hours',
            },
            inplace=True
        )
    #     print(a)
        collect.append(a)
    if collect:
        seed = collect[0]
        print(seed)
        for a_c in collect[1:]:
            print(seed)
            seed = pd.merge(
                seed,
                a_c,
                on='Date',
                how='outer'
            )
        return seed

    return pd.DataFrame()

def generate_raw_monthly_date(
    api_url,
    start_date,
    end_date,
    ticker,
    park_name,
    op_path,
    filter_cols=[]
):
    months_range = generate_date_range(start_date, end_date)
    op_path = op_path / Path(park_name)
    op_path.mkdir(exist_ok=True)

    for month in months_range:
        file_path =  str(month).replace('/', '-')+'_'+'.xlsx'
        p1 = op_path / file_path
        if p1.exists():
            print("File already exists")
            continue


        get_url = "/".join(
            [api_url, month]
            )

        print('----------------------------')
        print(get_url)
        page_src = send_request_get(
            url=get_url
        )
        # try:
        raw_data = transform_data(
            page_src,
            month
        )
        if raw_data.empty:
            print("Empty dataframe")
            pd.DataFrame().to_excel(p1)
            continue

        # except:
        #     breakpoint()
        raw_data['ticker'] = ticker
        raw_data['park_name'] = park_name
        try:
            if filter_cols:
                raw_data = raw_data[raw_data['park_type'].isin(filter_cols)]
        except:
            breakpoint()
        op_data = collate_raws(raw_data)
        open_hr_cols = [col for col in op_data.columns if 'open_hours' in col]
        close_hr_cols = [col for col in op_data.columns if 'close_hours' in col]

        op_data['consolidated_open'] = op_data[open_hr_cols].apply(cons_open, axis=1)
        op_data['consolidated_close'] = op_data[close_hr_cols].apply(cons_close, axis=1)
        
        op_data.to_excel(
                op_path / file_path
            )

        
def cons_open(x):
    mini = '24:00'
    print("x____",x.values)
    for idx,val in enumerate(x):
        
        if val :
            print(val)
            try:
                mini = min(mini, val)
            except:
                continue
    print(mini)
    return mini

    
def cons_close(x):
    maxi = '0:00'
    for idx,val in enumerate(x):
        
        if val:
            print(val)
            try:
                maxi = max(maxi, val)
            except:
                continue
    print(maxi)
    return maxi


filter_dict = {
    "Cedar Point" : ["Cedar Point", ],
    "Kings Island" : ["Kings Island", "WinterFest"],
    "Carowinds" : ["Carowinds", "SCarowinds", "WinterFest"],
    "Worlds of Fun" : ["Worlds of Fun", "Halloween Haunt", ""],
    "California's Great America" : ["California's Great America", "WinterFest"],
    "Knott's Berry Farm" : ["Knott's Summer Nights", "Knott's Scary Farm"],
    "Canada's Wonderland" : ["Canada's Wonderland", "Halloween Haunt", "WinterFest"],
    "Dorney Park & Wildwater Kingdom" : ["Dorney Park"],
    "Kings Dominion" : ["Kings Dominion", "Halloween Haunt", "WinterFest"],
    "Michigan's Adventure" : ["Michigan's Adventure"],
    "Valleyfair" : ["Valleyfair"],
    "Cedar Point Shores" : ["Cedar Point Shores"],
    "Knott's Soak City" : ["Knott's Soak City"],
    "Kings Island - Soak City Water Park" : ["Soak City Water Park"],
    "Carowinds - Waterpark (Carolina Harbor)" : ["Carolina Harbor"],
    "Oceans of Fun" : ["Oceans of Fun"],
    "California's Great America - Water Park (South Bay Shores)" : ["South Bay Shores"],
    "Canada's Wonderland - Splash Works" : ["Splash Works"],
    "Dorney Park & Wildwater Kingdom - Water Park" : ["Wildwater Kingdom"],
    "Kings Dominion - Waterpark (Soak City)" : ["Soak City", ],
    "Michigan's Adventure - WildWater Adventure" : ["WildWater Adventure", ],
    "Valleyfair - Waterpark (Soak City)" : ["Soak City"],
    "Schlitterbahn - Galvestone, Texas" : ["Waterpark Hours"],
    "Schlitterbahn - New Braunfels, Texas" : ["Waterpark Hours"],
}