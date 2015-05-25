"""
Decorators to extend workflow functions
"""
from functools import wraps

from util import find_on_path
from provenance import BINARY_PROVENANCE as bin_provenance_registry

class requires(object):
    """Convenience wrapper for tracking binaries used to perform tasks

    """

    def __init__(self, binaries=list(), version_methods=list()):
        self.required_binaries = binaries
        self.version_methods   = version_methods
        if len(binaries) == len(version_methods):
            self._update_binary_provenance()


    def _update_binary_provenance(self):
        """Add to the BINARY_PROVENANCE set the list of required binaries and
        version commands in the form of (binary, command) tuples

        """
        map( bin_provenance_registry.add, 
             zip(self.required_binaries, self.version_methods) )


    def _maybe_halt(self):
        missing = list()
        for binary in self.required_binaries:
            candidate = find_on_path(binary)
            if not candidate:
                missing.append(binary)
            else:
                yield candidate
            
        if missing:
            raise ValueError("Unable to continue, please install the "
                             "following binaries: %s" %(str(missing)))


    def __call__(self, fn):
        @wraps(fn)
        def wrapper(*args, **kwargs):
            binaries_with_paths = list(self._maybe_halt())
            ret = fn(*args, **kwargs)
            if ret:
                if type(ret) is dict:
                    ret = [ret]
                for t in ret:
                    deps = t.get('file_dep', [])
                    t['file_dep'] = list(set(list(deps)+binaries_with_paths))
                    yield t
                        
        wrapper.required_binaries = self.required_binaries
        wrapper.version_methods = self.version_methods
        return wrapper

    
        
