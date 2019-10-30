# -*- coding: utf-8 -*-
import os
import sys
import json

import six
import leveldb

from .util import mkdirp
from .tracked import s3_folder

ENV_VAR = "ANADAMA_BACKEND_DIR"
LOCAL_DB_FOLDER = ".anadama"

_default_backend = None

def default(output_dir=None):
    global _default_backend
    if s3_folder(output_dir):
        output_dir=os.getcwd()
    if output_dir is not None:
        return LevelDBBackend(
            _try_dir(os.path.abspath(os.path.join(output_dir, LOCAL_DB_FOLDER, "db")))
            )
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

def auto(data_dir, *args, **kwargs):
    """Return the right type of backend for the given data_dir"""
    # just one type of backend for now
    return LevelDBBackend(data_dir, *args, **kwargs)


def _try_dir(maybe_datadir):
    if os.path.isdir(maybe_datadir):
        pass
    else:
        try:
            mkdirp(maybe_datadir)
        except Exception as e:
            msg = six.u("Unable to create anadama "
                        "database directory `{}': \n"+str(e))
            sys.stderr.write(msg.format(maybe_datadir))
            fallback = _fallback_datadir()
            sys.stderr.write(six.u("Using fallback directory: "+fallback+"\n"))
            return fallback
    return maybe_datadir

    
def _fallback_datadir():
    try:
        folder=os.path.join(LOCAL_DB_FOLDER,"db")
        mkdirp(folder)
        return os.path.abspath(folder)
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
            val = self.db.Get(key.encode("utf-8"))
        except KeyError:
            return None
        return json.loads(val.decode("utf-8"))


    def lookup(self, dep):
        return self._get(dep.name)


    def lookup_many(self, deps):
        return [self.lookup(dep) for dep in deps]


    def save(self, dep_keys, dep_vals):
        if not dep_keys:
            return
        batch = leveldb.WriteBatch()
        for key, val in zip(dep_keys, dep_vals):
            decoded_val=[]
            for v in val:
                try:
                    decoded_val.append(v.decode("utf-8"))
                except AttributeError:
                    decoded_val.append(v)
            batch.Put(key.encode("utf-8"), json.dumps(decoded_val).encode("utf-8"))
        self.db.Write(batch)


    def keys(self):
        return self.db.RangeIter(include_value=False)

    def delete(self, key):
        return self.db.Delete(key.encode("utf-8"))

    def delete_many(self, keys):
        batch = leveldb.WriteBatch()
        for k in keys:
            batch.Delete(k.encode("utf-8"))
        self.db.Write(batch)

    def close(self):
        del self.db
        self.db = None
