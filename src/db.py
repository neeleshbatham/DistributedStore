from .singleton import Singleton


class Database(object):
    __metaclass__ = Singleton

    def __init__(self, **kwargs):
        self.store = {}

    def get(self, **kwargs):
        result = []
        print(self.store, "Current Store========>>")
        key = kwargs["data"]['key']
        value = self.store.get(key)
        if value:
            result.append({
                "key": key,
                "value": value if value else None
            })
        return result

    def set(self, **kwargs):
        keys_added = 0
        keys_failed = []
        data = kwargs.get('data')
        print(data, type(data))
        try:
            self.store[data["key"]] = data['value']
            keys_added += 1
        except Exception:
            keys_added.append(data)
        print("Updated Store ======>", self.store)
        return keys_added, keys_failed

    def delete(self, **kwargs):
        keys_deleted = []
        key = kwargs["data"]['key']
        if key in self.store.keys():
            self.store.pop(key)
            keys_deleted.append({
                    "key": key
                })
        return keys_deleted

