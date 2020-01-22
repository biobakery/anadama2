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

VERSION="0.6.7"
AUTHOR = "AnADAMA2 Development Team"
AUTHOR_EMAIL = "anadama-users@googlegroups.com"

# read requirements from file
requires_file=os.path.join(os.path.dirname(os.path.abspath(__file__)),"requirements.txt")
with open(requires_file) as file_handle:
    requires=[line.strip() for line in file_handle.readlines()]

if os.name != 'posix' or sys.version_info[0] > 2:
    try:
        requires.remove("subprocess32")
    except ValueError:
        pass

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
    author=AUTHOR,
    author_email=AUTHOR_EMAIL,
    version=VERSION,
    license="MIT",
    description="AnADAMA2: Another Automated Data Analysis Management Application 2",
    long_description="AnADAMA2 is the next generation of AnADAMA. AnADAMA is "+\
        "a tool to create reproducible workflows and execute them efficiently."+\
        " Tasks can be run locally or in a grid computing environment to increase"+\
        " efficiency. Essential information from all tasks is recorded, using the "+\
        "default logger and command line reporters, to ensure reproducibility. "+\
        "A auto-doc feature allows for workflows to generate documentation automatically"+\
        " to further ensure reproducibility by capturing the latest essential workflow "+\
        "information. AnADAMA2 was architected to be modular allowing users to "+\
        "customize the application by subclassing the base grid meta-schedulers, "+\
        "reporters, and tracked objects (ie files, executables, etc).",
    url="http://huttenhower.sph.harvard.edu/anadama2",
    keywords=['microbial','microbiome','bioinformatics','workflow','grid','anadama','anadama2'],
    platforms=['Linux','MacOS'],
    classifiers=[
        "Programming Language :: Python",
        "Development Status :: 5 - Production/Stable",
        "Environment :: Console",
        "Operating System :: MacOS",
        "Operating System :: Unix",
        "Programming Language :: Python :: 2.7",
        "Programming Language :: Python :: 3.4",
        "Topic :: Scientific/Engineering :: Bio-Informatics"
        ],
    packages=find_packages(exclude=['ez_setup', 'tests', 'tests.*']),
    zip_safe=False,
    entry_points={
        'console_scripts': [
            'anadama2_aws_batch_task = anadama2.grid.aws_batch_task:main',
        ]
    },
    install_requires=requires,
    test_suite="tests.test_suite",
    cmdclass={ 'sphinx_build' : SphinxBuild }
)
