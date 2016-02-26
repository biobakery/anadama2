def default():
    pass

class BaseBackend(object):
    def lookup(self, dep):
        raise NotImplementedError()

    def save(self, dep_keys, dep_vals):
        raise NotImplementedError()


class LevelDBBackend(BaseBackend):
    pass

class SqliteBackend(BaseBackend):
    pass

class PickleBackend(BaseBackend):
    pass
