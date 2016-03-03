import os
import sys
import json

import leveldb

from .util import mkdirp


def default():
    return LevelDBBackend()

def discover_data_directory():
    if "HOME" in os.environ:
        maybe_datadir = os.path.join(os.environ['HOME'], ".config",
                                     "anadama", "db")
        if not os.path.isdir(maybe_datadir):
            try:
                mkdirp(maybe_datadir)
            except Exception as e:
                msg = ("Unable to create anadama "
                       "database directory `{}': "+str(e))
                print >> sys.stderr, msg.format(maybe_datadir)
                fallback = _fallback_datadir()
                print >> sys.stderr, "Using fallback directory: "+fallback
                return fallback
        else:
            return maybe_datadir
    else:
        return _fallback_datadir()

    
def _fallback_datadir():
    try:
        mkdirp(".anadama/db")
        return os.path.abspath(".anadama/db")
    except:
        return "/tmp/anadama"

    
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
        batch = self.db.WriteBatch()
        for key, val in zip(dep_keys, dep_vals):
            batch.Put(key, json.dumps(val))
        self.db.Write(batch)

