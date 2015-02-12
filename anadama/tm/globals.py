import os, json, sys

config = {}

def init(directory):
    ''' method contains global variables '''
    global config
    # load configuration parameters (if they exist)
    path = os.path.join(directory, "configuration_parameters.txt")
    if os.path.exists(path):
        print >> sys.stdout, "loading configuration parameters from " + path
        with open(path) as config_file:
            config = json.load(config_file)
            print >> sys.stdout, "configuration loaded."

