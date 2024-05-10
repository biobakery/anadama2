# -*- coding: utf-8 -*-
from __future__ import print_function
import sys
import json
import optparse
import itertools

import six

from . import backends
from .util import Bag


desc = "Manipulate the AnADAMA dependency database"

def dump_dependencies(backend):
    for key in backend.keys():
        b = Bag()
        b.name = key
        six.print_(key, "\t", json.dumps(backend.lookup(b)))


def forget(backend, key=None):
    keys = []
    if key:
        keys.append(key)
    if sys.stdin.isatty():
        stdin_keys = iter(line.strip() for line in sys.stdin)
        keys = itertools.chain(keys, stdin_keys)
    for key in keys:
        try:
            backend.delete(key)
        except Exception as e:
            sys.stderr.write("Error inserting key {}: {}\n".format(key, e))


def entry_point(argv=None):
    parser = optparse.OptionParser(description=desc)
    parser.add_option("-d", "--dump-dependencies", action="store_true",
                      help="Print all known dependencies to standard out.")
    parser.add_option("-f", "--forget",
                      help="""Remove dependency from storage backend. Also accepts
                      keys line-by-line from standard in""")
    parser.add_option("-b", "--backend-dir", default=None,
                      help="Specify explicitly where to look for backend data")
    opts, _ = parser.parse_args(args=argv)
    if not opts.backend_dir:
        be_dir = backends.discover_data_directory()
    else:
        be_dir = opts.backend_dir
    backend = backends.auto(be_dir)
    if not backend.exists():
        msg = "Error - backend doesn't exist or is unrecognized: "
        sys.stderr.write(msg+be_dir+"\n")
        sys.exit(1)
    if opts.dump_dependencies:
        return dump_dependencies(backend)
    if opts.forget:
        return forget(backend, opts.forget)



if __name__ == '__main__':
    entry_point(sys.argv)
