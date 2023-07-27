"""
this module provides implementation for getting page source from url
"""
from abc import ABC, abstractmethod
from pathlib import Path
import requests
from typing import Dict, Any, List, Union
from ruamel.yaml import YAML
import pandas as pd
from dataclasses import dataclass
import calendar
import datetime 
from selenium.webdriver.chrome.options import Options
from selenium.webdriver import Chrome

class PageSourcer(ABC):
    """
    abstract class to get page source from url
    """
    @property
    def page_url(self):
        """
        getter method for page url
        """
        pass

    @abstractmethod
    def get_page_source(self):
        """
        abstract method to get page source from the url.
        """

class WebDriverPageSourcer(PageSourcer):
    """
    class to handle page source using webdriver
    """
    @property
    def page_url(self):
        return self._page_url

    def __init__(
            self,
            page_url: str,
            webdriver_path: Path,
    ) -> None:
        self._page_url = page_url
        self.webdriver_path = webdriver_path


class RequestsPageSourcer(PageSourcer):
    """
    a class to get page source using requests library
    """

    def __init__(
            self,
            page_url: str,
            **kwargs
    ) -> None:
        super().__init__(
            page_url
        )
        self._kwargs = kwargs

    def get_page_source(self):
        return requests.get(
            self.page_url,
            **self._kwargs,
            timeout=5
            ).content


class ChromePageSourcer(WebDriverPageSourcer):
    """
    class to get page source for url using Chrome Driver
    """

    def get_page_source(self):
        return self.driver.page_source
    
    def __init__(
            self,
            page_url,
            webdriver_path,
            chrome_options=None,
    ) -> None:
        super().__init__(page_url, webdriver_path)

        options = Options()
        if chrome_options:
            for option in chrome_options:
                options.add_experimental_option(*option)

        self.driver = Chrome(
            chrome_options=options,
            executable_path=webdriver_path,
        )

        self.driver.get(
            url=self.page_url
        )


class ConfigReader(ABC):
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

class YAMLConfigReader(ConfigReader):

    @property
    def config_path(self):
        return self._config_path
            
    def read_config(self):
        '''
        helper function to read config from yaml files.
        '''
        with open(self.config_path, 'r') as f:
            data = YAML().load(f)
            return data
        
    def __init__(
        self,
        yaml_config_path,
        ) -> None:
        self._config_path = yaml_config_path



class ISubConfigParser(ABC):
    '''
    abstract class to parse
    config sub-section
    '''

    @abstractmethod
    def parse_sub_section_by_keys(
        self,
        sub_config_keys
        ):
        """
        abstract method to parse 
        """

class SubConfigParser(ISubConfigParser):
    '''
    class to read Scraping 
    '''
    def parse_sub_section_by_keys(
            self,
            sub_config_keys
            ):
        data= self._config_data
        for key in sub_config_keys:
            data = data[key]
        return data
    
    def __init__(
            self,
            config_data,
            ) -> None:
        self._config_data = config_data

@dataclass
class IBaseURLsCreater(ABC):
    # base urls to be created from the config data.
    base_urls : List[str]
    
    # extensions to be created from the config data.
    extensions : Dict[str, List[str]]

    # concatenation string to be used for url creation
    concat_str : str

    @abstractmethod
    def create_base_urls(self):
        '''
        abstract method to create base urls.
        '''


class BaseURLsCreater(IBaseURLsCreater):
    
    def create_base_urls(
        self,
        skip_base=True,
        ):

        urls = self.base_urls

        for _, value in self.extensions.items():
            if value:
                urls = self.generate_urls(
                    urls=urls,
                    extensions=value,
                    concat_str=self.concat_str
                    )

        if skip_base:
            urls = urls[1:]
        
        return urls        

    @staticmethod
    def generate_urls(
        urls: List[str],
        extensions: List[str],
        concat_str: str,
        ):
        urls.extend([
                url + concat_str + extension
                for extension in extensions
                for url in urls
            ])
        
        return urls

@dataclass
class ISourceIterator(ABC):
    '''
    abstract class to iterate over the multiple page sources.
    '''
    base_urls : List[str]
    iter_params: Union[Dict[str, Any], Any]
    concat_str: str

    def create_source_iterator(self):
        '''
        abstract method to create source iterator.
        '''

class SourceIterator(ISourceIterator):
    def create_source_iterator(self):
        '''
        helper function to create source iterator.
        '''
        iter_extensions = self._generate_extensions_from_iter_params()
        if iter_extensions:
            return self.generate_urls(
                urls=self.base_urls,
                extensions=iter_extensions,
                concat_str=self.concat_str
            )
        
        return self.base_urls

    @abstractmethod
    def _generate_extensions_from_iter_params(self):
        '''
        helper function to generate extensions from iter params.
        '''
        pass

    @staticmethod
    def generate_urls(
        urls: List[str],
        extensions: List[str],
        concat_str: str,
        ):
        urls.extend([
                url + concat_str + extension
                for extension in extensions
                for url in urls
            ])
        
        return urls

class CedarPointSourceIterator(SourceIterator):
    def __init__(
        self,
        base_urls: List[str],
        iter_params: Union[Dict[str, Any], Any],
        concat_str: str,
        ) -> None:
        super().__init__(
            base_urls=base_urls,
            iter_params=iter_params,
            concat_str='&'
        )

    def _generate_extensions_from_iter_params(self):
        '''
        helper function to generate extensions from iter params.
        '''
        date_range = self.iter_params['date_range']
        start_date = date_range['start_date']
        end_date = date_range['end_date']

        month_list = pd.period_range(
            start=start_date,
            end=end_date,
            freq='M',
            ).to_list()

        return [month.strftime("%m/%Y") for month in month_list]


class SeaWorldSourceIterator(SourceIterator):
    def __init__(
        self,
        base_urls: List[str],
        iter_params: Union[Dict[str, Any], Any],
        concat_str: str,
        ) -> None:
        super().__init__(
            base_urls=base_urls,
            iter_params=iter_params,
            concat_str=concat_str   
    )

    def _get_item_extensions_from_iter_params(self):
        '''
        helper function to generate extensions from iter params.
        '''
        item_config = self.iter_params['item']
        item_id = item_config['item_id']
        pre_sep = item_config['pre_sep']

        return [
            pre_sep + 'itemId=' + item_id
            ]
            
        
    def _generate_date_extensions_from_iter_params(self):
        '''
        helper function to generate extensions from iter params.
        '''
        date_range = self.iter_params['date_range']
        start_date = date_range['start_date']
        end_date = date_range['end_date']

        month_list = pd.period_range(
            start=start_date,
            end=end_date,
            freq='M',
            ).to_list()

        return [
                '&'+'start=' + self.get_last_day_of_prev_month(month.strftime("%m/%Y"))
                + '&' + 'end=' + self.get_last_day_of_month(month.strftime("%m/%Y"))
            for month in month_list
            ]
    def create_source_iterator(self):

        item_urls = self.generate_urls(
            urls=self.base_urls,
            extensions=self._get_item_extensions_from_iter_params(),
            concat_str=''
        )
        date_urls = self.generate_urls(
            urls=item_urls,
            extensions=self._generate_date_extensions_from_iter_params(),
            concat_str=''
            )

        return date_urls[3:]

    def _generate_extensions_from_iter_params(self):
        '''
        helper function to generate extensions from iter params.
        '''
        return 

    @staticmethod
    def get_first_day_of_month(date: str):
        '''
        helper function to get first day of the month.
        '''
        return date.split('/')[1] + '-' +  date.split('/')[0] + '-' +'01'
    @staticmethod
    def get_last_day_of_month(date: str):
        '''
        helper function to get last day of the month.
        '''
        month, year = date.split('/')
        year = int(year)
        month = int(month)
        _, num_days = calendar.monthrange(year, month)
        return datetime.date(year, month, num_days).strftime('%Y-%m-%d')

    @staticmethod
    def get_last_day_of_prev_month(date: str):
        '''
        helper function to get last day of the previous month.
        '''
        month, year = date.split('/')
        year = int(year)
        month = int(month) - 1
        _, num_days = calendar.monthrange(year, month)
        return datetime.date(year, month, num_days).strftime('%Y-%m-%d')