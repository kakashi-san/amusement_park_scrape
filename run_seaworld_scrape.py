import pandas as pd
from pathlib import Path
from modules.config_parser import YAMLConfigReader
from modules.interfaces import IRequestsSourcer,
 IConfigParamsData, IParamsGen


class GetRequestsSourcer(IRequestsSourcer):
    def source_page(self):
        return requests.get(
            self.url,
            params=self.params
            )


class SeaWorldDateParamsGen(IParamsGen):
    def __init__(self, config_params_data):
        self.config_params_data = config_params_data

    def generate_params(self):
        return {
            'start' : self.config_params_data['date_range']['start_date'],
            'end' : self.config_params_data['date_range']['end_date']'
        }

class SeaWorldItemParamsGen(IParamsGen):
    def __init__(self, config_params_data):
        self.config_params_data = config_params_data

    def generate_params(self):
        return {
            'item' : self.config_params_data['item']
        }:

config_params_data = {
    'date_range' : {
        'start_date' : '2023/07/01',
        'end_date' : '2023/12/31'
    }
}

config_path = 'config/seaworld_config.yaml'
parks_list_path = 'data/input/Parks List.xlsm'
link_to_itemid_map = pd.read_excel('data/input/seaworld/link_to_itemid_map.xlsx').to_dict()
config_reader = YAMLConfigReader(config_path)
url = 'https://seaworld.com/orlando/park-info/theme-park-hours/'
itemid = link_to_itemid_map[url]
company_name = 'SeaWorld [SEAS]'

parks_data = pd.read_excel(parks_list_path, sheet_name='Sheet2')
parks_url_list = parks_data[parks_data['Company'] == company_name]['URL'].to_list()

base_url_config_keys = ('URL_CONFIG', 'root', 'base')

config = config_reader.read_config()
scp = SubConfigParser(config_data=config)

base_urls  = scp.parse_sub_section_by_keys(
    sub_config_keys=base_url_config_keys
    )

