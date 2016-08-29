import os
import sys

from setuptools import setup, find_packages
import distutils

requires = [
    'networkx==1.11',
    'leveldb==0.193',
    'six'
]

if os.name == 'posix' and sys.version_info[0] < 3:
    requires.append("subprocess32")

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
    version='0.1.2',
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
