import os
import sys
import json

import leveldb

from .util import mkdirp

ENV_VAR = "ANADAMA_BACKEND_DIR"

_default_backend = None

def default():
    global _default_backend
    if _default_backend is None:
        _default_backend = LevelDBBackend()
    return _default_backend


def discover_data_directory():
    if ENV_VAR in os.environ:
        return _try_dir(os.environ[ENV_VAR])
    elif "HOME" in os.environ:
        return _try_dir(os.path.join(os.environ['HOME'], ".config",
                                     "anadama", "db"))
    else:
        return _fallback_datadir()


def _try_dir(maybe_datadir):
    if os.path.isdir(maybe_datadir):
        pass
    else:
        try:
            mkdirp(maybe_datadir)
        except Exception as e:
            msg = ("Unable to create anadama "
                   "database directory `{}': "+str(e))
            print >> sys.stderr, msg.format(maybe_datadir)
            fallback = _fallback_datadir()
            print >> sys.stderr, "Using fallback directory: "+fallback
            return fallback
    return maybe_datadir

    
def _fallback_datadir():
    try:
        mkdirp(".anadama/db")
        return os.path.abspath(".anadama/db")
    except:
        mkdirp("/tmp/anadama/db")        
        return "/tmp/anadama/db"

    
class BaseBackend(object):
    def __init__(self, data_directory=None, autocreate=True):
        self.data_directory = data_directory or discover_data_directory()
        if autocreate and not self.exists():
            self.create()

    def lookup(self, dep):
        raise NotImplementedError()

    def lookup_many(self, deps):
        raise NotImplementedError()

    def save(self, dep_keys, dep_vals):
        raise NotImplementedError()

    def create(self):
        raise NotImplementedError()

    def exists(self):
        raise NotImplementedError()

    def keys(self):
        raise NotImplementedError()

    def delete(self, key):
        raise NotImplementedError()

    def delete_many(self, keys):
        raise NotImplementedError()

    def close(self):
        raise NotImplementedError()



class LevelDBBackend(BaseBackend):
    def __init__(self, *args, **kwargs):
        self.db = None
        super(LevelDBBackend, self).__init__(*args, **kwargs)
        if not self.db:
            self.db = leveldb.LevelDB(self.data_directory,
                                      create_if_missing=False)


    def exists(self):
        return all([os.path.exists(os.path.join(self.data_directory, f))
                    for f in ("CURRENT", "LOCK", "LOG")])


    def create(self):
        self.db = leveldb.LevelDB(self.data_directory, create_if_missing=True,
                                  error_if_exists=True)


    def _get(self, key):
        try:
            val = self.db.Get(key)
        except KeyError:
            return None
        return json.loads(val)


    def lookup(self, dep):
        return self._get(dep._key)


    def lookup_many(self, deps):
        return [self.lookup(dep) for dep in deps]


    def save(self, dep_keys, dep_vals):
        if not dep_keys:
            return
        batch = leveldb.WriteBatch()
        for key, val in zip(dep_keys, dep_vals):
            batch.Put(key, json.dumps(val))
        self.db.Write(batch)


    def keys(self):
        return self.db.RangeIter(include_value=False)

    def delete(self, key):
        return self.db.Delete(key)

    def delete_many(self, keys):
        batch = leveldb.WriteBatch()
        for k in keys:
            batch.Delete(k)
        self.db.Write(batch)

    def close(self):
        del self.db
        self.db = None
