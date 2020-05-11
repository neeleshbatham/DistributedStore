import os
import flask
import json
import random
import requests
import argparse
from multiprocessing import Process
from flask import request, jsonify
from flask import render_template
from src.utils import get_configs, get_host_from_configs


app = flask.Flask(__name__)
app.config["DEBUG"] = False


class RequestHandler:

    def __init__(self, host_list):
        self.key_node_map = {}
        self.host_list = host_list

    def route_host(self):
        """
        Pick up random host from host list.
        :return: host
        """
        host = random.choice(self.host_list)
        return host

    def _handle_data(self, data):
        """
        converts bytes data to json/dict
        :param data:bytes: data
        :return: dict: data
        """
        if isinstance(data, bytes):
            data = data.decode('utf-8')
            data = json.loads(data)
        return data

    def _update_node_key_map(self, keys, host):
        """
        updates KEY - NODE Map after setting the key.
        :param keys: list of keys
        :param host: host
        :return: none
        """
        for key in keys:
            self.key_node_map[key] = host

    def _get_keys(self, data):
        """
        Get keys from data
        :param data: dict: list of key value pairs
        :return: list of keys
        """
        keys = []
        for each in data:
            key = each['key']
            keys.append(key)
        return keys

    def _get_host_by_key(self, key):
        """
        Get host for the key.
        To look in which host the key is stored.
        :param key: key
        :return: host
        """
        print("Key NODE MAP =========>>>", self.key_node_map)
        print("\n")
        value = self.key_node_map.get(key)
        return value

    def handle_get(self, incoming_request):
        data = self._handle_data(incoming_request.data)
        keys = self._get_keys(data)
        host = self._get_host_by_key(keys[0])
        print("Looking into for HOST  {0}=========== {1}".format(host,keys))

        try:
            result = requests.post('http://{0}/node/get'.format(host), json=data)
            response = self._handle_data(result.content)
            return response
        except Exception as e:
            print(e)

    def handle_set(self, incoming_request):
        data = self._handle_data(incoming_request.data)
        host = self.route_host()
        keys = self._get_keys(data)
        print("KEYS AND PORT While setting >>",keys, host)
        print("\n")
        try:
            result = requests.post('http://{0}/node/set'.format(host), json=data)
            response = self._handle_data(result.content)
            if response:
                self._update_node_key_map(keys, host)
            print("Updated KEY_NODE_MAP======>>", self.key_node_map)
            print("\n")

            return response
        except Exception as e:
            print(e)

    def handle_delete(self, incoming_request):
        data = self._handle_data(incoming_request.data)
        keys = self._get_keys(data)
        host = self._get_host_by_key(keys[0])
        try:
            result = requests.post('http://{0}/node/delete'.format(host), json=data)
            return result
        except Exception as e:
            print(e)


@app.route("/")
@app.route("/health")
def health():
    """
    HEALTH API endpoint to check workers health.
    :return:
    """
    data = health_check(host)
    return render_template('health.html', title='Health', data=data)


@app.route("/get", methods=['POST'])
def get():
    """
    GET KEYS
    :return:
    """
    results = request_handler.handle_get(request)
    return jsonify(results)


@app.route("/set", methods=['PUT', 'POST'])
def set():
    """
    SET KEYS
    :return:
    """
    results = request_handler.handle_set(request)
    return jsonify(results)


@app.route('/delete', methods=['DELETE', 'POST'])
def delete():
    """
    DELETE/EXPIRE KEY
    """
    results = request_handler.handle_delete(request)
    return jsonify(results)


def health_check(host_list):
    """
    Health check for the passed worker nodes.
    :param host_list: list: list of workers hosts passed
    :return:
    """
    health_dict = dict()
    for host in host_list:
        try:
            result = requests.get("http://{0}/health".format(host))
            if result.status_code == 200:
                print("STATUS: SUCCESS Worker running at ==>> ", host)
                health_dict[host] = 200
        except Exception:
            print("STATUS: FAILURE Worker NOT running at ==>> ", host)
            health_dict[host] = 500
            continue
    return health_dict


class StartService():
    def __init__(self):
        pass

    @staticmethod
    def run_flask():
        app.run(host='0.0.0.0', port=8000, debug=False, use_reloader=True)


class RequestObj:
    def __init__(self,data):
        self.data = data


def handle_cli(fl):
    fl.start()
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
                    data = [{'key': command[1], 'value':command[2]}]
                    req = RequestObj(data)
                    res = request_handler.handle_set(req)
                    print(res)
                else:
                    print("ERROR: While SET Check for Usage")

            if command[0] == 'GET':
                if len(command) == 2:
                    data = [{'key': command[1]}]
                    req = RequestObj(data)
                    res = request_handler.handle_get(req)
                    print(res)
                else:
                    print("ERROR: While GET Check for Usage")

            if command[0] == 'DELETE':
                if len(command) == 2:
                    data = [{'key': command[1]}]
                    req = RequestObj(data)
                    res = request_handler.handle_delete(req)
                    print(res)
                else:
                    print("ERROR: While DELETE Check for Usage")
            elif command[0] == 'q':
                fl.terminate()


if __name__ == "__main__":

    parser = argparse.ArgumentParser(description='Process some integers.')
    parser.add_argument('host', metavar='host', type=str, nargs='*', help='Ports')
    args = parser.parse_args()
    host = args.host

    if len(host) == 0:  # Then read from config.yml
        print("Reading from config file.")
        configs = get_configs(os.getcwd() + '/config.yml')
        host = get_host_from_configs(configs)

    print("=====>> Incoming Worker Host ===>>", host)
    request_handler = RequestHandler(host)
    health_check(host)

    flask_app = StartService.run_flask
    fl = Process(target=flask_app)
    handle_cli(fl)




