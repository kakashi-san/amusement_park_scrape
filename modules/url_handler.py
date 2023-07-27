from typing import Dict, Any, List

from modules.utils import read_config_yaml


def get_url_map_from_config(
    config_data: Dict[str, Any],
    config_key: str='URL_CONFIG',
) -> Dict[str, str]:
    
    url_config = config_data[config_key]

    url_map = url_config['map']

    url_extensions = [
        url_map['category_extension'],
        url_map['sub_category_extension'],
        ]
    
    aux_extensions = url_map['aux_extensions'] if isinstance(
        url_map['aux_extensions'], list
        ) else [
            url_map['aux_extensions']
            ]

    url_extensions.extend(aux_extensions)

    return {
        'base_url' : url_map['base_url'],
        'concat_str' : url_config['utils']['concat_str'],
        'extensions': url_extensions,
    }

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

def generate_urls_from_config(
         config_data: Dict[str, Any],
         config_key: str='URL_CONFIG',
) -> Dict[str, str]:
    
    url_config = config_data[config_key]

    url_map = url_config['map']

    urls = [url_map['base_url']]
    concat_str = url_config['utils']['concat_str']

    if url_map['category_extensions']:

        urls = generate_urls(
            urls=urls,
            extensions=url_map['category_extensions'],
            concat_str=concat_str
        )


    if url_map['sub_category_extensions']:
        urls = generate_urls(
            urls=urls,
            extensions=url_map['sub_category_extensions'],
            concat_str=concat_str
        )


    if url_map['aux_extensions']:
        urls = generate_urls(
            urls=urls,
            extensions=url_map['aux_extensions'],
            
        )

    return urls

def join_url_n_extensions(
        base_url: str,
        extensions: List[str],
        concat_str: str,
):
    return f"{concat_str}".join([
            base_url,
            f"{concat_str}".join(extensions)]
        )

def make_url_from_config(
    url_config:Dict[str, str]
    ) -> str:
    base_url = url_config['base_url']
    extensions = url_config['extensions']
    concat_str = url_config['concat_str']

    url = join_url_n_extensions(
        base_url=base_url,
        extensions=extensions,
        concat_str=concat_str
    )

    return url



def generate_url_from_config(
    config_yaml_path
    ):
    config_data = read_config_yaml(
        config_yaml_path=config_yaml_path
    )

    url_config = get_url_map_from_config(
        config_data=config_data
    )

    url = make_url_from_config(
        url_config=url_config
    )

    return url