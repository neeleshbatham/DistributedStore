import flask
import json
from flask import request, jsonify
from src.api import API

app = flask.Flask(__name__)
app.config["DEBUG"] = False


class ApiHandler:
    def __init__(self, api):
        self.api = api

    def method_helper(self, requests):
        data = requests.data

        if isinstance(data, bytes):
            data = data.decode('utf-8')
            data = json.loads(data)
        if 'set' in requests.url:
            result = self.api.set(data)

        if 'get' in requests.url:
            result = self.api.get(data)

        if 'delete' in requests.url:
            result = self.api.delete(data)
        return result


api_handler = ApiHandler(API())


@app.route('/node/get', methods=['POST'])
def get():
    results = api_handler.method_helper(request)
    return jsonify(results)


@app.route('/node/set', methods=['PUT', 'POST'])
def set():
    results = api_handler.method_helper(request)
    return jsonify(results)


@app.route('/node/delete', methods=['DELETE', 'POST'])
def delete():
    results = api_handler.method_helper(request)
    return jsonify(results)


@app.route('/health', methods=['GET'])
def health_check():
    results = {200: "OK"}
    return jsonify(results)


class Server:
    def __init__(self, port, host=None):
        app.run(port=port, threaded=True)


if __name__ == "__main__":
    import sys
    port = sys.argv[1]
    run = Server(port=port)
