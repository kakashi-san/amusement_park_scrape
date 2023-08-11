from interfaces import IRequestsSourcer,
 IConfigParamsData, IParamsGen
import requests
import pandas as pd

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