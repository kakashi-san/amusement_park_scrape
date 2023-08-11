from pathlib import Path
from modules.page_sourcer import YAMLConfigReader, SubConfigParser, BaseURLsCreater
from modules.page_sourcer import ChromePageSourcer, BaseURLsCreater, SeaWorldSourceIterator



config_yaml_path = Path('./seaworld_config.yaml')
base_url_config_keys = ('URL_CONFIG', 'root', 'base')
extensions_url_config_keys = ('URL_CONFIG', 'root', 'extensions')
concat_str_config_keys = ('URL_CONFIG', 'root', 'concat_str')
pre_sep_config_keys = ('URL_CONFIG', 'iterables', 'date_range', 'pre_sep')
iter_params_config_keys = ('URL_CONFIG', 'iterables')

yaml_reader = YAMLConfigReader(
    yaml_config_path=config_yaml_path
)

yaml_data = yaml_reader.read_config()
scp = SubConfigParser(config_data=yaml_data)

base_urls  = scp.parse_sub_section_by_keys(sub_config_keys=base_url_config_keys)
extensions = scp.parse_sub_section_by_keys(sub_config_keys=extensions_url_config_keys)
concat_str = scp.parse_sub_section_by_keys(sub_config_keys=concat_str_config_keys)
pre_sep = scp.parse_sub_section_by_keys(sub_config_keys=pre_sep_config_keys)
iter_params = scp.parse_sub_section_by_keys(sub_config_keys=iter_params_config_keys)


buc = BaseURLsCreater(
    base_urls=base_urls,
    extensions=extensions,
    concat_str=concat_str
    )
base_urls = buc.create_base_urls()
# print(base_urls)

ssi = SeaWorldSourceIterator(
    base_urls=base_urls,
    iter_params=iter_params,
    concat_str=pre_sep
)

print(ssi.create_source_iterator())