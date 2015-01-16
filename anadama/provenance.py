import subprocess

BINARY_PROVENANCE = set()


def find_versions(module_or_name):
    """Import a module. Then, as a side effect of module import, the
    :py:data:`provenance.BINARY_PROVENANCE` module-level variable will
    be populated with commands. Run each of these commands and return
    a list of each command's output.

    See also :py:obj:`decorators.requires`

    :param module_or_name: The module to import
    :type module_or_name: String or module
    :return: List of string 2-tuples. Each tuple is composed of the
             name of the dependency and the output of the dependency
             version-determining command.

    """

    if type(module_or_name) is str:
        module_or_name = __import__(module_or_name)

    return [ 
        (binary, _find_version_call(version_command))
        for binary, version_command in BINARY_PROVENANCE
    ]
                

def _find_version_call(command):
    try:
        ret = subprocess.check_output(command, 
                                      shell=True, 
                                      stderr=subprocess.STDOUT)
    except Exception as e:
        ret = str(e)

    if not ret:
        ret = "<No output>"

    return ret.strip()

    
