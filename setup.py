##############################################################################
#
# Copyright (c) 2008 Zope Foundation and Contributors.
# All Rights Reserved.
#
# This software is subject to the provisions of the Zope Public License,
# Version 2.1 (ZPL).  A copy of the ZPL should accompany this distribution.
# THIS SOFTWARE IS PROVIDED "AS IS" AND ANY AND ALL EXPRESS OR IMPLIED
# WARRANTIES ARE DISCLAIMED, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED
# WARRANTIES OF TITLE, MERCHANTABILITY, AGAINST INFRINGEMENT, AND FITNESS
# FOR A PARTICULAR PURPOSE.
#
##############################################################################
"""A backend for ZODB that stores pickles in a relational database."""


# The choices for the Trove Development Status line:
# Development Status :: 5 - Production/Stable
# Development Status :: 4 - Beta
# Development Status :: 3 - Alpha

classifiers = """\
Intended Audience :: Developers
License :: OSI Approved :: Zope Public License
Programming Language :: Python
Programming Language :: Python :: 2.7
Programming Language :: Python :: 3.4
Programming Language :: Python :: 3.5
Programming Language :: Python :: 3.6
Programming Language :: Python :: Implementation :: CPython
Programming Language :: Python :: Implementation :: PyPy
Topic :: Database
Topic :: Software Development :: Libraries :: Python Modules
Operating System :: Microsoft :: Windows
Operating System :: Unix
"""

import os
from setuptools import setup
from setuptools import find_packages

doclines = __doc__.split("\n")

def read_file(*path):
    base_dir = os.path.dirname(__file__)
    file_path = (base_dir, ) + tuple(path)
    with open(os.path.join(*file_path)) as f:
        result = f.read()
    return result

VERSION = read_file('version.txt').strip()

tests_require = [
    'mock',
    # random2 is a forward port of python 2's random to
    # python 3. Our test_cache_stats (inherited from ZEO)
    # needs that. Without it, the tests can work on Py2 but
    # not Py3.
    'random2',
    'zope.testing',
    'ZODB [test]',
    'zc.zlibstorage'
]

setup(
    name="RelStorage",
    version=VERSION,
    author="Zope Foundation and Contributors",
    author_email="shane@willowrise.com",
    maintainer="Shane Hathaway",
    maintainer_email="shane@willowrise.com",
    url="http://relstorage.readthedocs.io/",
    keywords="ZODB SQL RDBMS MySQL PostgreSQL Oracle",
    packages=find_packages(),
    include_package_data=True,
    license="ZPL 2.1",
    platforms=["any"],
    description=doclines[0],
    classifiers=filter(None, classifiers.split("\n")),
    long_description=read_file("README.rst"),
    zip_safe=False,  # otherwise ZConfig can't see component.xml
    setup_requires=[
        'cffi',
    ],
    install_requires=[
        'cffi',
        'perfmetrics',
        'zope.interface',
        'zc.lockfile',
        # ZODB and ZEO are handled as environment-markers in extras.
    ],
    # If a new-enough CFFI is installed, build the module as part of installation.
    # Otherwise, we'll build it at import time. The wheels we distribute should have
    # cffi installed so we distribute the built binaries.
    cffi_modules = ['relstorage/cache/_cache_ring_build.py:ffi'],
    tests_require=tests_require,
    extras_require={
        # We previously used MySQL-python (C impl) on CPython 2.7,
        # because it had the longest history. However, it is no longer
        # maintained, so we switch to its maintained fork mysqlclient;
        # this is consistent with Python 3. The best option for PyPy
        # is PyMySQL because MySQL-python doesn't support it (and
        # binary drivers like that tend to be slow). Although both
        # PyMySQL and mysqlclient support Python 3, use mysqlclient on
        # Python 3 because it's a binary driver and *probably* faster
        # for CPython; it requires some minor code changes to support,
        # so be sure to test this configuration.
        'mysql:platform_python_implementation=="CPython" and python_version == "2.7"': [
            'mysqlclient>=1.3.7',
        ],
        'mysql:platform_python_implementation=="CPython" and python_version >= "3.3"': [
            'mysqlclient>=1.3.7',
        ],
        'mysql:platform_python_implementation=="PyPy"': [
            'PyMySQL>=0.6.6',
        ],
        'postgresql: platform_python_implementation == "CPython"': [
            # 2.4.1+ is required for proper bytea handling
            'psycopg2>=2.6.1',
        ],
        'postgresql: platform_python_implementation == "PyPy"': [
            'psycopg2cffi>=2.7.4',
        ],
        'oracle': [
            'cx_Oracle>=5.0.0'
        ],
        ":python_full_version >= '2.7.9'": [
            'ZODB >= 4.4.2',
            'ZEO >= 4.2.0',
        ],
        ":python_full_version == '3.6.0rc1'": [
            # For some reason ZEO isn't getting installed
            # on 3.6rc1/pip 9.0.1/tox 2.5.1. Looks like the
            # version selection <, >= environment markers aren't working.
            # So we give a full version spec, which seems to work.
            'ZODB >= 4.4.2',
            'ZEO >= 4.2.0',
        ],
        ":python_full_version < '2.7.9'": [
            # We must pin old versions prior to 2.7.9 because ZEO
            # 5 only runs on versions with good SSL support.
            'ZODB >= 4.4.2, <5.0',
            'ZEO >= 4.2.0, <5.0'
        ],
        'test': tests_require,
    },
    entry_points={
        'console_scripts': [
            'zodbconvert = relstorage.zodbconvert:main',
            'zodbpack = relstorage.zodbpack:main',
        ],
        'zodburi.resolvers': [
            ('postgres = '
             'relstorage.zodburi_resolver:postgresql_resolver [postgresql]'),
            'mysql = relstorage.zodburi_resolver:mysql_resolver [mysql]',
            'oracle = relstorage.zodburi_resolver:oracle_resolver [oracle]'
        ]},
    test_suite='relstorage.tests.alltests.make_suite',
)
