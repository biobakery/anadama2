"""
Decorators to extend workflow functions
"""

from util import find_on_path

class requires(object):
    """Convenience wrapper for tracking binaries used to perform tasks

    """

    def __init__(self, binaries=list()):
        self.required_binaries = binaries

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
                        
        return wrapper

    
        
