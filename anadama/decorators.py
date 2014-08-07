"""
Decorators to extend workflow functions
"""

from util import find_on_path

class requires(object):
    """Convenience wrapper for tracking binaries used to perform tasks

    """

    def __init__(self, binaries=list()):
        missing = list()
        required_binaries = list()
        for binary in binaries:
            candidate = find_on_path(binary)
            if not candidate:
                missing.append(binary)
            else:
                required_binaries.append(candidate)
            
        if missing:
            raise ValueError("Unable to continue, please install the "
                             "following binaries: %s" %(str(missing)))

        self.required_binaries = required_binaries

    def __call__(self, fn):
        def wrapper(*args, **kwargs):
            ret = fn(*args, **kwargs)
            deps = ret.get('file_dep', [])
            ret['file_dep'] = list(set(deps+self.required_binaries))
            return ret

        return wrapper

    
        
