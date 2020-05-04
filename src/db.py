from .singleton import Singleton


class Database(object):
    __metaclass__ = Singleton

    def __init__(self, **kwargs):
        self.store = {}

    def get(self, **kwargs):
        print("In GET DB here===========>>>", kwargs)
        result = []
        print(self.store, "STORE========>>")

        for key in kwargs.get("data"):
            value = self.store.get(key["key"])
            if value:
                result.append({
                    "key": key["key"],
                    "value": value if value else None
                })
        return result

    def set(self, **kwargs):
        keys_added = 0
        keys_failed = []
        data = kwargs.get('data')
        print(data, type(data))
        for key_value_pair in data:
            try:
                self.store[key_value_pair["key"]] = key_value_pair['value']
                keys_added += 1
            except Exception:
                keys_added.append(key_value_pair)
        print("SET STORED============>", self.store)
        print(keys_added, keys_failed)
        return keys_added, keys_failed

    def delete(self, **kwargs):
        keys_deleted = []
        data = kwargs.get('data')
        print(data, type(data))

        for key in kwargs.get("data"):
            if key["key"] in self.store.keys():
                self.store.pop(key["key"])
                keys_deleted.append({
                        "key": key["key"]
                    })
        return keys_deleted

