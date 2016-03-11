from setuptools import setup, find_packages

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
    install_requires=[
        'networkx==1.9',
        'leveldb==0.193'
    ],
    classifiers=[
        "Development Status :: 2 - Pre-Alpha"
    ],
    test_suite="tests.test_suite"
)
