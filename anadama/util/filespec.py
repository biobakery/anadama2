import re
import os
import glob
from itertools import chain, imap, ifilter
from operator import itemgetter

DEFAULT_DATA_DIR = "./"

third = itemgetter(2)

def parse(pattern_str, data_dir=DEFAULT_DATA_DIR):
    if pattern_str.startswith("glob:"):
        pattern = os.path.join(data_dir,
                               pattern_str.split("glob:", 1)[1])
        files = map(os.path.abspath, glob.glob(pattern))
    elif pattern_str.startswith("re:"):
        matcher = lambda s: re.search(pattern_str.split("re:", 1)[1], s)
        allfiles = chain.from_iterable( imap(third, os.walk(data_dir)) )
        files = ifilter(matcher, allfiles)
    elif ',' in pattern_str:
        files = pattern_str.split(',')
        nonexistent = [ not os.path.exists(f) for f in files ]
        if nonexistent:
            raise OSError("No such file or directory: "+", ".join(nonexistent))
    else:
        if not os.path.exists(pattern_str):
            raise OSError("No such file or directory: "+pattern_str)
        files = [pattern_str]

    if not files:
        raise ValueError("The pattern matched no files")
    return files
