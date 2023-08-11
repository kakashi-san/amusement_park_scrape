from abc import ABC, abstractmethod
from typing import List
from typing import Dict

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
@dataclass
class IConfigParamsData(ABC):
    config_params_data: Dict[str, Any]

class IParamsGen(ABC):
    '''
    abstract class to generate params
    using:
        1. params_data
        2. config_data
    '''
    @property
    @abstractmethod
    def params(self):
        '''
        abstract method/property to store params data
        '''
        return self._params

    @params.setter
    def params(self, params):
        '''
        abstract method/property to store params data
        '''
        self._params = params

    def __init__(
        self,
        config_params_data: IConfigParamsData
        ):
        self._params = config_params_data.config_params_data

    
    @abstractmethod
    def generate_params(
        self,
        ) -> Dict[str, Any]:
        '''
        abstract method to generate params data
        '''