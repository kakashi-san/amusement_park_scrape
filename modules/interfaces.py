from abc import ABC, abstractmethod
from typing import List
from typing import Dict, Any
from dataclasses import dataclass

class IConfigReader(ABC):
    '''
    abstract class to read config file
    '''
    @property
    @abstractmethod
    def config_path(self):
        '''
        abstract method to store config path
        '''

    @abstractmethod
    def read_config(self):
        '''
        abstract method to read config file.
        '''

class IRequestsSourcer(ABC):
    '''
    abstract class to source pages
    using:
        1. url
        2. params
        3. source_page
    '''
    @property
    def params(self):
        return self._params

    @property
    def url(self):
        return self._url

    def __init__(
        self,
        url,
        params
        ):
        self._url = url
        self._params = params

    @abstractmethod
    def source_page(self) -> str:
        '''
        abstract method to source page
        '''
# class SWIterParamsGen:
#     def __init__(
#         params_path = 
#     ):
@dataclass
class IConfigParamsData(ABC):
    config_params_data: Dict[str, Any]

# class GenerateURL