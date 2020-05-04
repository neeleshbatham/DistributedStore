from .db import Database

HTTP_SUCCESS = 200
HTTP_NOT_FOUND = 404
HTTP_BAD_REQUEST = 400
HTTP_SERVER_ERROR = 500


class API(object):

    def __init__(self):
        self.database = Database()

    def fetch(self, data=None):
        result = None
        if data:
            result = self.database.fetch(**{"data":data})
        else:
            result = self.database.fetch()
        return result, HTTP_SUCCESS

    def get(self, data=None):
        keys = self.database.get(**{"data": data})
        statusCode = HTTP_SUCCESS if len(keys) != 0 else HTTP_NOT_FOUND
        return keys, statusCode

    def set(self, data=None):
        keysAdded, keysFailed = self.database.set(**{"data": data})
        result = {
            "keys_added": keysAdded,
            "keys_failed": keysFailed
        }
        statusCode = HTTP_BAD_REQUEST if keysFailed else HTTP_SUCCESS
        return result, statusCode

    def delete(self, data=None):
        keys = self.database.delete(**{"data": data})
        statusCode = HTTP_SUCCESS if len(keys) != 0 else HTTP_NOT_FOUND
        return keys, statusCode
