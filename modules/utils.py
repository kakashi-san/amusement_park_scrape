"""
the module contains utility functions used for web scraping
"""
from typing import Dict, Any
from pathlib import Path
from ruamel.yaml import YAML


def read_config_yaml(
    config_yaml_path: Path
) -> Dict[Any, Any]:
    """
    helper function to read config from yaml files.
    """
    with open(config_yaml_path, 'r') as f:
        data = YAML().load(f)
        return data
