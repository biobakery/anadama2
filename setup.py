from setuptools import setup, find_packages

setup(
    name='anadama',
    version='0.0.1',
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
        'doit==0.25.0',
        'networkx==1.9',
        'PyYAML',
        'requests'
    ],
    classifiers=[
        "Development Status :: 2 - Pre-Alpha"
    ],
    entry_points= {
        'console_scripts': [
            'anadama = anadama.cli:main',
        ],
    }
)
