import re
import os
import json
import errno
import inspect
import mimetypes
from collections import namedtuple

class SparseMetadataException(ValueError):
    pass

biopython_to_metaphlan = {
    "fasta": "multifasta",
    "fastq": "multifastq",
    "bam"  : "bam",
}

def generator_flatten(gen):
    for item in gen:
        if inspect.isgenerator(item):
            for value in generator_flatten(item):
                yield value
        else:
            yield item

def addext(name_str, tag_str):
    return name_str + "." + tag_str

def addtag(name_str, tag_str):
    path, name_str = os.path.split(name_str)
    match = re.match(r'([^.]+)(\..*)', name_str)
    if match:
        base, ext = match.groups()
        return os.path.join(path, base + "_" + tag_str + ext)
    else:
        return os.path.join(path, name_str + "_" + tag_str)

def guess_seq_filetype(guess_from):
    guess_from = os.path.split(guess_from)[-1]
    if re.search(r'\.f.*q$', guess_from): #fastq, fnq, fq
        return 'fastq'
    elif re.search(r'\.f.*a$', guess_from): #fasta, fna, fa
        return 'fasta'
    elif guess_from.endswith('.sff'):
        return 'sff'
    elif guess_from.endswith('.bam'):
        return 'bam'
    elif guess_from.endswith('.sam'):
        return 'sam'

def dict_to_cmd_opts_iter(opts_dict, sep="=", singlesep=" "):
    """sep separates long options and their values, singlesep separates
    short options and their values e.g. --long=foobar vs -M 2

    """

    for key, val in opts_dict.iteritems():
        if len(key) > 1:
            key = "--%s"% (key)
        else:
            key = "-%s"% (key)
            
        if val:
            if len(key) == 2:
                yield key+singlesep+val
            else:
                yield key+sep+val
        else:
            yield key


def dict_to_cmd_opts(opts_dict, sep="=", singlesep=" "):
    return " ".join(dict_to_cmd_opts_iter(
        opts_dict, sep=sep, singlesep=singlesep))


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
            name = os.path.join(basedir, os.path.basename(name))
        
        dir = os.path.dirname(name)
        if not os.path.exists(dir):
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
        fname for fname in fname_list
        if is_compressed(fname)
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
        except TypeError as e:
            raise SparseMetadataException(
                "Unable to deserialize sample-specific metadata:"+\
                " The file %s has missing values at row %i" %(
                    file_handle.name, i)
            )


def _defaultfunc(obj):
    if hasattr(obj, '_serializable_attrs'):
        return obj._serializable_attrs
    elif hasattr(obj, 'isoformat'):
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

_PATH_list = None

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
