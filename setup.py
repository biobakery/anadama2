# -*- coding: utf-8 -*-
import os
import sys

from setuptools import setup, find_packages
import distutils

# try to import urllib.request.urlretrieve for python3
try:
    from urllib.request import urlretrieve
except ImportError:
    from urllib import urlretrieve

VERSION="0.3.0"

requires = [
    'networkx==1.11',
    'leveldb==0.193',
    'six',
    'cloudpickle==0.2.1',
    'pweave==0.25',
    'markdown'
]

if os.name == 'posix' and sys.version_info[0] < 3:
    requires.append("subprocess32")

COUNTER_URL="http://bitbucket.org/biobakery/anadama2/downloads/counter.txt"

def download(url, download_file):
    """ Download a file from a url """

    try:
        print("Downloading "+url)
        file, headers = urlretrieve(url,download_file)
        # print final return to start new line of stdout
        print("\n")
    except EnvironmentError:
        print("WARNING: Unable to download "+url)

counter_file=os.path.basename(COUNTER_URL)
if not os.path.isfile(counter_file):
    print("Downloading counter file to track anadama2 downloads"+
    " since the global PyPI download stats are currently turned off.")
    download(COUNTER_URL,counter_file)

class SphinxBuild(distutils.cmd.Command):
    """ This is a custom command to build the API Docs"""
    description = "build the API docs"
    # add "=" to the option name to make it an argument instead of a flag
    user_options = [
        ('source=', None, 'the path to the sphinx source'),
        ('dest=', None, 'the path to the build destination')]

    def initialize_options(self):
        # set the source and dest to the defaults
        setup_location =  os.path.dirname(os.path.realpath(__file__))
        self.source = os.path.join(setup_location,"doc","source")
        self.dest = os.path.join(setup_location,"html")

    def finalize_options(self):
        # check that the directories provided exist
        if not os.path.isdir(self.source):
            sys.exit("The source directory does not exist,"+
                " please select another location: " + self.source)
        if not os.path.isdir(self.dest):
            sys.exit("The dest directory does not exist,"+
                " please select another location: " + self.dest)

    def run(self):
        # check for sphinx install
        import subprocess
        try:
            subprocess.check_output(["sphinx-build","--help"])
        except (EnvironmentError, subprocess.CalledProcessError):
            sys.exit("ERROR: Please install Sphinx ('pip install Sphinx')")

        # run the sphinx build command
        try:
            subprocess.check_call(["sphinx-build",self.source,self.dest])
        except (EnvironmentError, subprocess.CalledProcessError):
            sys.exit("ERROR: Unable to run sphinx build")

setup(
    name='anadama2',
    version=VERSION,
    description=('AnADAMA - '
                 'Another '
                 'Automated '
                 'Data '
                 'Analysis '
                 'Management '
                 'Application'),
    packages=find_packages(exclude=['ez_setup', 'tests', 'tests.*']),
    zip_safe=False,
    install_requires=requires,
    classifiers=[
        "Development Status :: 2 - Pre-Alpha"
    ],
    test_suite="tests.test_suite",
    cmdclass={ 'sphinx_build' : SphinxBuild }
)
