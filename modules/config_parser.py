from .interfaces import IConfigReader
from ruamel.yaml import YAML

class YAMLConfigReader(IConfigReader):

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