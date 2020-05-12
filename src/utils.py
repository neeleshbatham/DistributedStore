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


def get_replication_factor(configs):
    return configs.get('replication_factor')


class RequestObj:
    def __init__(self, data):
        self.data = data


def handle_cli(fl, request_handler):
    command = ''
    while command != 'q':
        command = input(
            '----------------------------------------------\
            \n[SET] <key> <value>\n[GET] <key>\n[DELETE] <key> \n[q] quit. \
            \n----------------------------------------------\
            \n\nEnter Command: ').split()
        print(command)
        if len(command) != 0:
            if command[0] == 'SET':
                if len(command) == 3:
                    data = {'key': command[1], 'value':command[2]}
                    req = RequestObj(data)
                    res = request_handler.handle_set(req)
                    print(res)
                else:
                    print("ERROR: While SET Check for Usage")

            if command[0] == 'GET':
                if len(command) == 2:
                    data = {'key': command[1]}
                    req = RequestObj(data)
                    res = request_handler.handle_get(req)
                    print(res)
                else:
                    print("ERROR: While GET Check for Usage")

            if command[0] == 'DELETE':
                if len(command) == 2:
                    data = {'key': command[1]}
                    req = RequestObj(data)
                    res = request_handler.handle_delete(req)
                    print(res)
                else:
                    print("ERROR: While DELETE Check for Usage")
            elif command[0] == 'q':
                fl.terminate()
