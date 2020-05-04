import yaml


def get_configs(path):
    try:
        with open(str(path)) as file:
            config = yaml.load(file, Loader=yaml.FullLoader)
            return config
    except Exception as err:
        print("Error while loading configs from YAML", err)


def get_host_from_configs(configs):

    hosts = []
    for config in configs.get('workers'):
        data = list(config.values())[0]
        uri = str(data.get('host'))+":"+str(data.get('port'))
        hosts.append(uri)
    return hosts
