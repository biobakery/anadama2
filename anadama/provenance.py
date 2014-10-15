import subprocess

BINARY_PROVENANCE = set()

def find_versions(module_or_name):

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

    
