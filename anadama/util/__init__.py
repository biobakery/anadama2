import re
import os
import json
import zlib
import errno
import inspect
import mimetypes
import subprocess
from functools import wraps
from itertools import izip_longest
from multiprocessing import cpu_count
from collections import namedtuple

from .. import Task

max_cpus = max(1, cpu_count()-1)

class SparseMetadataException(ValueError):
    pass

biopython_to_metaphlan = {
    "fasta": "multifasta",
    "fastq": "multifastq",
    "bam"  : "bam",
}

def first(iterable):
    return next(iter(iterable))

def intatleast1(n):
    return max(1, int(n))

def generator_flatten(gen):
    for item in gen:
        if inspect.isgenerator(item) or type(item) in (list, tuple):
            for value in generator_flatten(item):
                yield value
        else:
            yield item


def addext(name_str, tag_str):
    return name_str + "." + tag_str


def rmext(name_str, all=False):
    """removes file extensions 

    :keyword all: Boolean; removes all extensions if True, else just
      the outside one

    """

    _match = lambda name_str: re.match(r'(.+)(\..*)', name_str)
    path, name_str = os.path.split(name_str)
    match = _match(name_str)
    while match:
        name_str = match.group(1)
        match = _match(name_str)
        if not all:
            break

    return os.path.join(path, name_str)


def addtag(name_str, tag_str):
    path, name_str = os.path.split(name_str)
    match = re.match(r'(.+)(\..*)', name_str)
    if match:
        base, ext = match.groups()
        return os.path.join(path, base + "_" + tag_str + ext)
    else:
        return os.path.join(path, name_str + "_" + tag_str)
        

def guess_seq_filetype(guess_from):
    guess_from = os.path.split(guess_from)[-1]
    if re.search(r'\.f.*q(\.gz|\.bz2)?$', guess_from): #fastq, fnq, fq
        return 'fastq'
    elif re.search(r'\.f.*a(\.gz|\.bz2)?$', guess_from): #fasta, fna, fa
        return 'fasta'
    elif guess_from.endswith('.sff'):
        return 'sff'
    elif guess_from.endswith('.bam'):
        return 'bam'
    elif guess_from.endswith('.sam'):
        return 'sam'


def dict_to_cmd_opts_iter(opts_dict,
                          shortsep=" ",  longsep="=",
                          shortdash="-", longdash="--"):
    """sep separates long options and their values, singlesep separates
    short options and their values e.g. --long=foobar vs -M 2

    """
    longkv = lambda k, v: longdash + k + longsep + v
    shortkv = lambda k, v: shortdash + k + shortsep + v

    for key, val in opts_dict.iteritems():
        kv = longkv if len(key) > 1 else shortkv
        if val is False or None:
            continue
        elif val is True:
            yield longdash+key if len(key) > 1 else shortdash+key
        elif type(val) in (tuple, list):
            for subval in val:
                yield kv(key, subval)
        else:
            yield kv(key, str(val))

        
def dict_to_cmd_opts(*args, **kwds):
    return " ".join(dict_to_cmd_opts_iter(*args, **kwds))


def mkdirp(path):
    try:
        return os.makedirs(path)
    except OSError as e:
        if e.errno == errno.EEXIST and os.path.isdir(path):
            pass
        else:
            raise


def _new_file(*names, **opts):
    basedir = opts.get("basedir")
    for name in names:
        if basedir:
            name = os.path.abspath(
                os.path.join(
                    basedir, os.path.basename(name)
                )
            )
        
        dir = os.path.dirname(name)
        if dir and not os.path.exists(dir):
            mkdirp(dir)

        yield name


def new_file(*names, **opts):
    iterator = _new_file(*names, basedir=opts.get("basedir"))
    if len(names) == 1:
        return iterator.next()
    else:
        return list(iterator)


def is_compressed(fname):
    recognized_compression_types = ("gzip", "bzip2")
    return mimetypes.guess_type(fname)[1] in recognized_compression_types


def filter_compressed(fname_list):
    """Return only files that are known to be compressed and in a
    recognized compression format"""
    return [
        (i,fname) for i,fname in enumerate(fname_list)
        if is_compressed(fname)
    ]


def which_compressed_idxs(fname_mtx):
    for i, raw_tuple in enumerate(fname_mtx):
        for j, fname in enumerate(raw_tuple):
            if is_compressed(fname):
                yield i, j

def take(raw_seq_files, index_list):
    return [
        raw_seq_files[i][j] for i, j in index_list
    ]
        
###
# Serialization things

class SerializationError(TypeError):
    pass


def deserialize_csv(file_handle):
    for i, line in enumerate(file_handle):
        cols = line.split('\t')
        if line.strip() == "":
            continue
        if len(cols) < 2:
            raise AttributeError(
                "Improper formatting in file %s - "
                "only %d column(s) found on line %d" % (
                    file_handle.name, len(cols), i)
            )

        yield ( 
            cols[0], 
            [ col.strip() for col in cols[1:] ] 
            )

        
def deserialize_map_file(file_handle):
    """Returns a list of namedtuples according to the contents of the
    file_handle according to the customs of QIIME
    """
    mangle = lambda field: re.sub(r'\s+', '_', field.strip().replace('#', ''))

    header_line = file_handle.readline()
    if not header_line:
        return
    header = [ mangle(s) for s in header_line.split('\t') ]

    cls = namedtuple('Sample', header, rename=True)

    i = 1
    for row in file_handle:
        i+=1
        if row.startswith('#'):
            continue
        try:
            yield cls._make([ r.strip() for r in row.split('\t') ])
        except TypeError:
            raise SparseMetadataException(
                "Unable to deserialize sample-specific metadata:"+\
                " The file %s has missing values at row %i" %(
                    file_handle.name, i)
            )


def serialize_map_file(namedtuples, output_fname):
    namedtuples_iter = iter(namedtuples)
    with open(output_fname, 'w') as map_file:
        first = namedtuples_iter.next()
        print >> map_file, "#"+"\t".join(first._fields)
        print >> map_file, "\t".join(first)
        for record in namedtuples_iter:
            print >> map_file, "\t".join(record)


def _defaultfunc(obj):
    try:
        return obj._serializable_attrs
    except AttributeError:
        pass

    if hasattr(obj, 'isoformat'):
        return obj.isoformat()
        
    raise SerializationError("Unable to serialize object %s" %(obj))
        

def serialize(obj, to_fp=None):
    if to_fp:
        return json.dump(obj, to_fp, default=_defaultfunc)
    else:
        return json.dumps(obj, default=_defaultfunc)


def deserialize(s=None, from_fp=None):
    if s:
        return json.loads(s)
    elif from_fp:
        return json.load(from_fp)


class SerializableMixin(object):
    """Mixin that defines a few methods to simplify serializing objects
    """

    serializable_attrs = []

    @property
    def _serializable_attrs(self):
        if hasattr(self, "_custom_serialize"):
            return self._custom_serialize()
        else:
            return dict([
                (key, getattr(self, key))
                for key in self.serializable_attrs
            ])


def islambda(func):
    return getattr(func,'func_name') == '<lambda>'


def memoized(func):
    cache = func.cache = {}

    @wraps(func)
    def memoizer(*args, **kwargs):
        if args not in cache:
            cache[args] = func(*args, **kwargs)
        return cache[args]
    return memoizer


_PATH_list = None
@memoized
def find_on_path(bin_str):
    """ Finds an executable living on the shells PATH variable.
    :param bin_str: String; executable to find

    :returns: Absolute path to `bin_str` or False if not found
    :rtype: str
    """

    global _PATH_list
    if _PATH_list is None:
        _PATH_list = os.environ['PATH'].split(':')

    for dir_ in _PATH_list:
        candidate = os.path.join(dir_, bin_str)
        if os.path.exists(candidate) and os.access(candidate, os.X_OK):
            return candidate

    return False


def partition(it, binsize, pad=None):
    iters = [iter(it)]*binsize
    return izip_longest(fillvalue=pad, *iters)    

def _adler32(fname):
    """Compute the adler32 checksum on a file.

    :param fname: File path to the file to checksum
    :type fname: str
    """

    with open(fname, 'r') as f:
        checksum = 1
        while True:
            buf = f.read(1024*1024*8)
            if not buf:
                break
            checksum = zlib.adler32(buf, checksum)
            
    return checksum


class ShellException(OSError):
    pass


def sh(cmd, **kwargs):
    kwargs['stdout'] = kwargs.get('stdout', subprocess.PIPE)
    kwargs['stderr'] = kwargs.get('stderr', subprocess.PIPE)
    proc = subprocess.Popen(cmd, **kwargs)
    ret = proc.communicate()
    if proc.returncode:
        msg = "Command `{}' failed. \nOut: {}\nErr: {}"
        raise ShellException(proc.returncode, msg.format(cmd, ret[0], ret[1]))
    return ret


class HasNoEqual(object):
    def __eq__(self, other):
        return False

def noop(*args, **kwargs):
    return None

def istask(x):
    return isinstance(x, Task)

def isnottask(x):
    return not isinstance(x, Task)

def underscore(s):
    """Remove all whitespace and replace with underscores"""
    return re.sub(r'\s+', '_', s)
