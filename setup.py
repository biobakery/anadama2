import os
import sys

from setuptools import setup, find_packages

requires = [
        'networkx==1.9',
        'leveldb==0.193'
]

if os.name == 'posix' and sys.version_info[0] < 3:
    requires.append("subprocess32")

setup(
    name='anadama',
    version='0.1.1',
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
    test_suite="tests.test_suite"
)
