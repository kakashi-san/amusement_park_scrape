import time
from pathlib import Path

import pandas as pd

from modules.page_sourcer import YAMLConfigReader, SubConfigParser, BaseURLsCreater
from modules.page_sourcer import ChromePageSourcer, BaseURLsCreater, RequestsPageSourcer
from bs4 import BeautifulSoup

config_yaml_path = Path('./seaworld_config copy.yaml')
base_url_config_keys = ('URL_CONFIG', 'root', 'base')
extensions_url_config_keys = ('URL_CONFIG', 'root', 'extensions')
concat_str_config_keys = ('URL_CONFIG', 'root', 'concat_str')
path_web_driver_config_keys = ('DRIVER_OPTIONS', 'webdriver', 'chrome', 'driver_path')
options_webdriver_config_keys = ('DRIVER_OPTIONS', 'webdriver', 'chrome', 'options')
# pre_sep_config_keys = ('URL_CONFIG', 'iterables', 'date_range', 'pre_sep')
# iter_params_config_keys = ('URL_CONFIG', 'iterables')

yaml_reader = YAMLConfigReader(
    yaml_config_path=config_yaml_path
)

yaml_data = yaml_reader.read_config()
scp = SubConfigParser(config_data=yaml_data)

base_urls = scp.parse_sub_section_by_keys(sub_config_keys=base_url_config_keys)
extensions = scp.parse_sub_section_by_keys(sub_config_keys=extensions_url_config_keys)
concat_str = scp.parse_sub_section_by_keys(sub_config_keys=concat_str_config_keys)
webdriver_path = scp.parse_sub_section_by_keys(sub_config_keys=path_web_driver_config_keys)
webdriver_options = scp.parse_sub_section_by_keys(sub_config_keys=options_webdriver_config_keys)
# pre_sep = scp.parse_sub_section_by_keys(sub_config_keys=pre_sep_config_keys)
# iter_params = scp.parse_sub_section_by_keys(sub_config_keys=iter_params_config_keys)


buc = BaseURLsCreater(
    base_urls=base_urls,
    extensions=extensions,
    concat_str=concat_str
)
base_urls = buc.create_base_urls(
    skip_base=False
)
for base_url in base_urls:
    print(base_url)
    cps = ChromePageSourcer(
        page_url=base_url,
        webdriver_path=webdriver_path,
    )

    # cps.get_page_source()
    time.sleep(10)
    sauce = cps.get_page_source()
    soup = BeautifulSoup(sauce)
    calendars = soup.find_all(name='table', attrs={'class': 'jet-calendar-grid'})
    tables = pd.read_html(soup.prettify())
    for tab_idx, tab in enumerate(tables):
        tab.to_csv(f"{tab_idx}_combined_calendar_data.csv")
    # for index, calendar in enumerate(calendars):
    #     tables = pd.read_html(calendar.prettify().encode('utf-8'))


    #     for calendar_date_div in calendar_date_divs:
    #
    #     if calendar_date_div.find_all(
    #             name='div',
    #             attrs={
    #                 'class': 'jet-listing-dynamic-field__inline-wrap'
    #             }
    #     ):
    #         relevant_divs.append(calendar_date_div)
    # print(relevant_divs)
# print(base_urls)

# ssi = SeaWorldSourceIterator(
#     base_urls=base_urls,
#     iter_params=iter_params,
#     concat_str=pre_sep
# )

# print(ssi.create_source_iterator())
