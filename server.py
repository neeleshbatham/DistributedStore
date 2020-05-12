import os
import sys
import flask
import json
import hashlib
import requests
from multiprocessing import Process
from flask import request, jsonify, render_template
from src.api import API
from src.utils import get_configs, get_host_from_configs, handle_cli, get_replication_factor


app = flask.Flask(__name__)
app.config["DEBUG"] = False

default_replication_factor = 1


class HashRing(object):

    def __init__(self, nodes=None):
        self.nodes = nodes

    def _hash_digest(self, key):
        md5 = hashlib.md5
        hash = md5(bytes(key, 'utf-8'))
        return int(hash.hexdigest(), 16)

    def get_node(self, key, total_nodes):
        hash_key = self._hash_digest(key)
        return hash_key % total_nodes


class ApiHandler:
    def __init__(self, api):
        self.api = api

    def method_helper(self, request):
        data = request.data

        if isinstance(data, bytes):
            data = data.decode('utf-8')
            data = json.loads(data)

        if 'set' in request.url:
            result = self.api.set(data)

        if 'get' in request.url:
            result = self.api.get(data)

        if 'delete' in request.url:
            result = self.api.delete(data)

        return result


class MainHandler(object):

    def __init__(self, nodes):
        self.nodes = nodes
        self.hash = HashRing(nodes)

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

    def handle_set(self, request):
        """
        Handle SET requests
        :param request: request object
        :return: response
        """
        data = self._handle_data(request.data)
        server = self.hash.get_node(data.get('key'), len(self.nodes))
        server_name = self.nodes[server]

        print("Saving Key Here: ", server_name)
        result = requests.put('http://{0}/node/set'.format(server_name), json=data)
        if replication_factor > 0:
            self._replicate(replication_factor, server, data)
        response = self._handle_data(result.content)

        return response

    def handle_get(self, request):
        """
        Handle GET requests
        :param request: request object
        :return: response
        """
        data = self._handle_data(request.data)
        server = self.hash.get_node(data.get('key'), len(self.nodes))
        server_name = self.nodes[server]
        try:
            result = requests.put('http://{0}/node/get'.format(server_name), json=data)
            response = self._handle_data(result.content)
            if result.status_code != 200:
                raise
        except Exception as err:
            print(err)
            aux_nodes = self.nodes.copy()
            del aux_nodes[server]
            try:
                for node in aux_nodes:
                    result = requests.post('http://{0}/node/get'.format(node), json=data)
                    if result.status_code == 200:
                        response = self._handle_data(result.content)
                        break
            except Exception as err:
                print("Exception while finding the key in replicas", err)
        return response

    def handle_delete(self, request):
        """
        Handle DELETE requests
        :param request: request object
        :return: response
        """
        data = self._handle_data(request.data)
        server = self.hash.get_node(data.get('key'), len(self.nodes))
        server_name = self.nodes[server]
        result = requests.post('http://{0}/node/delete'.format(server_name), json=data)
        response = self._handle_data(result.content)
        return response

    def _replicate(self, replication_factor, server, data):
        aux_nodes = self.nodes.copy()
        del aux_nodes[server]
        if replication_factor > len(self.nodes):
            nodes = self.nodes
        else:
            nodes = aux_nodes

        count = 0
        for node in nodes:
            if count < replication_factor:
                print("Replicating at: ", node)
                requests.put('http://{0}/node/set'.format(node), json=data)
                count += 1
            else:
                break


"""===================Internal URL====================="""


@app.route('/node/get', methods=['POST'])
def node_get():
    results = api_handler.method_helper(request)
    return jsonify(results)


@app.route('/node/set', methods=['PUT', 'POST'])
def node_set():
    results = api_handler.method_helper(request)
    return jsonify(results)


@app.route('/node/delete', methods=['DELETE', 'POST'])
def node_delete():
    results = api_handler.method_helper(request)
    return jsonify(results)


"""===================External URL========================"""


@app.route('/get', methods=['POST'])
def get():
    results = handler.handle_get(request)
    return jsonify(results)


@app.route('/set', methods=['PUT', 'POST'])
def set():
    results = handler.handle_set(request)
    return jsonify(results)


@app.route('/delete', methods=['DELETE', 'POST'])
def delete():
    results = handler.handle_delete(request)
    return jsonify(results)


@app.route('/node/health', methods=['GET'])
def health_check():
    results = {200: "OK"}
    return jsonify(results)


def health_check_nodes(*args):
    """
    Health check for the passed worker nodes.
    :param host_list: list: list of workers hosts passed
    :return:
    """
    health_dict = dict()
    host_list = args[0]
    for host in host_list:
        try:
            result = requests.get("http://{0}/node/health".format(host))
            if result.status_code == 200:
                print("STATUS: SUCCESS Worker running at ==>> ", host)
                health_dict[host] = 200
        except Exception:
            print("STATUS: FAILURE Worker NOT running at ==>> ", host)
            health_dict[host] = 500
            continue
    return health_dict


class StartService:
    def __init__(self):
        pass

    @staticmethod
    def run_flask(*args):
        app.run(port=args[0], debug=False, use_reloader=True)


if __name__ == "__main__":
    node_list = []  # For Dev Test, Originally it will read from config.
    port = sys.argv[1]
    if len(node_list) == 0:  # Then read from config.yml
        print("Reading from config file.")
        configs = get_configs(os.getcwd() + '/config.yml')
        node_list = get_host_from_configs(configs)
    replication_factor = get_replication_factor(configs)

    handler = MainHandler(node_list)
    api_handler = ApiHandler(API())

    print("====> Replication Factor===>>", replication_factor)
    print("====>> Incoming Worker Host ===>>", node_list)

    flask_app = StartService.run_flask
    fl = Process(target=flask_app, args=(port,))
    fl.start()
    fl1 = Process(target=health_check_nodes, args =(node_list,))
    fl1.start()

    handle_cli(fl, handler)
