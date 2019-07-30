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

import os
from setuptools import setup
from setuptools import find_packages


def read_file(*path):
    base_dir = os.path.dirname(__file__)
    file_path = (base_dir, ) + tuple(path)
    with open(os.path.join(*file_path)) as f:
        result = f.read()
    return result

VERSION = read_file('version.txt').strip()

memcache_require = [
    'pylibmc; platform_python_implementation=="CPython" and sys_platform != "win32"',
    'python-memcached; platform_python_implementation=="PyPy" or sys_platform == "win32"',
]

tests_require = [
    'mock ; python_version == "2.7"',
    # random2 is a forward port of python 2's random to
    # python 3. Our test_cache_stats (inherited from ZEO)
    # needs that. Without it, the tests can work on Py2 but
    # not Py3.
    'random2',
    'zope.testing',
    'ZODB [test]',
    # We have the aforementioned test_cache_stats from ZEO,
    # which obviously needs a ZEO dependency. But on Python 2.7,
    # ZEO depends on trollius, which is deprecated and can't be installed on
    # PyPy 2.7 on Windows, so don't install it.
    'ZEO >= 5.2; python_version > "2.7"',
    'zc.zlibstorage',
    'zope.testrunner',
    'nti.testing',
    'gevent >= 1.5a1; sys_platform != "win32"',
    'pyperf',
    'psutil',
] + memcache_require


setup(
    name="RelStorage",
    version=VERSION,
    author="Zope Foundation and Contributors",
    author_email="shane@willowrise.com",
    maintainer="Shane Hathaway",
    maintainer_email="shane@willowrise.com",
    url="http://relstorage.readthedocs.io/",
    project_urls={
        'Bug Tracker': 'https://github.com/zodb/relstorage/issues',
        'Source Code': 'https://github.com/zodb/relstorage/',
        'Documentation': 'http://relstorage.readthedocs.io',
    },
    keywords="ZODB SQL RDBMS MySQL PostgreSQL Oracle",
    packages=find_packages('src'),
    package_dir={'': 'src'},
    include_package_data=True,
    license="ZPL 2.1",
    platforms=["any"],
    description="A backend for ZODB that stores pickles in a relational database.",
    classifiers=[
        "Intended Audience :: Developers",
        "License :: OSI Approved :: Zope Public License",
        "Programming Language :: Python",
        "Programming Language :: Python :: 2.7",
        "Programming Language :: Python :: 3.5",
        "Programming Language :: Python :: 3.6",
        "Programming Language :: Python :: 3.7",
        "Programming Language :: Python :: Implementation :: CPython",
        "Programming Language :: Python :: Implementation :: PyPy",
        "Topic :: Database",
        "Topic :: Software Development :: Libraries :: Python Modules",
        "Operating System :: Microsoft :: Windows",
        "Operating System :: Unix",
    ],
    long_description=read_file("README.rst"),
    # We cannot be used from an archive. ZConfig can't see
    # our component.xml, and we rely on being able to use __file__ to
    # list and locate auxiliary files (e.g., schema.py finds SQL files)
    zip_safe=False,
    setup_requires=[
        'cffi',
    ],
    install_requires=[
        'cffi',
        'perfmetrics',
        'zope.interface',
        'zope.dottedname',
        'zc.lockfile',
        # These are the versions we're testing against. ZODB 5.2.2 is
        # when checkSecure() went away and IMVCCAfterCompletionStorage
        # was added, and 5.1.2 is when Connection.new_oid was added
        # (https://github.com/zopefoundation/ZODB/issues/139)
        'ZODB >= 5.5',
        # We directly use this, and its a transient dep of ZODB.
        # version 2.0 is where things became text, and 2.1 partly
        # relaxed those requirements.
        'transaction >= 2.4.0',
    ],
    # If a new-enough CFFI is installed, build the module as part of installation.
    # Otherwise, we'll build it at import time. The wheels we distribute should have
    # cffi installed so we distribute the built binaries.
    cffi_modules=[
        'src/relstorage/cache/_cache_ring_build.py:ffi',
    ],
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
        # so be sure to test this configuration. mysqlclient doesn't
        # compile on windows for Python 2.7 and only has wheels for
        # 3.6 and 3.7, though, so only install the pure-python
        # version.

        # pylint:disable=line-too-long
        'mysql:platform_python_implementation=="CPython" and (sys_platform != "win32" or python_version > "3.5")': [
            'mysqlclient >= 1.4',
        ],
        'mysql:platform_python_implementation=="PyPy" or (sys_platform == "win32" and python_version < "3.6")': [
            'PyMySQL>=0.6.6',
        ],
        # Notes on psycopg2: In 2.8, they stopped distributing full
        # binary wheels under that name. For that, you have to use
        # psycopg2-binary (which in 2.8.3 appears to be compiled with
        # the libpq from postgres 11). But that's not recommended for
        # production usage because it can have conflicts with other libraries,
        # and the authors specifically request that other modules not depend on
        # psycopg2-binary.
        # See http://initd.org/psycopg/docs/install.html#binary-packages
        'postgresql: platform_python_implementation == "CPython"': [
            # 2.4.1+ is required for proper bytea handling;
            # 2.6+ is needed for 64-bit lobject support.
            # 2.7+ is needed for Python 3.7 support and PostgreSQL 10+.
            # 2.7.6+ is needed for PostgreSQL 11
            'psycopg2 >= 2.8.3',
        ],
        'postgresql: platform_python_implementation == "PyPy"': [
            # 2.8.0+ is needed for Python 3.7
            'psycopg2cffi >= 2.8.1',
        ],
        'oracle': [
            'cx_Oracle>=5.0.0'
        ],
        'memcache': memcache_require,
        'test': tests_require,
        'docs': [
            'sphinxcontrib-programoutput',
            'repoze.sphinx.autointerface',
            'sphinx_rtd_theme',
        ],
        'all_tested_drivers': [
            # Install all the supported drivers for the platform.
            # Spread them out across the versions to not load any one
            # up too heavy for better parallelism.

            # First, mysql
            # pymysql on 3.6 on all platforms. We get coverage data from Travis,
            # and we get 2.7 from PyPy and Windows.
            'PyMySQL >= 0.6.6; python_version == "3.6" or platform_python_implementation == "PyPy" or (sys_platform == "win32" and python_version < "3.6")',
            # mysqlclient (binary) on all CPythons. It's the default,
            # except on old Windows. We get coverage from Travis.
            'mysqlclient >= 1.4;platform_python_implementation=="CPython" and (sys_platform != "win32" or python_version > "3.5")',
            # mysql-connector-python on Python 3.7 for coverage on Travis and ensuring it works
            # on Windows, and PyPy for testing there, since it's one of two pure-python versions.
            'mysql-connector-python >= 8.0.16; python_version == "3.7" or platform_python_implementation == "PyPy"',

            # postgresql
            # pure-python
            # pg8000 on Python 2.7 or PyPy. We get coverage from Travis, and we also
            # get Windows.
            'pg8000 >= 1.11.0; python_version == "2.7" or platform_python_implementation == "PyPy"',
            # CFFI, runs on all implementations.
            # We get coverage from 3.5 on Travis and verification it works on Windows from Travis.
            # We get 2.7 testing from PyPy on Travis.
            'psycopg2cffi >= 2.7.4; python_version == "3.5" or platform_python_implementation == "PyPy"',
            # Psycopg2 on all CPython, it's the default
            'psycopg2 >= 2.6.1; platform_python_implementation == "CPython"',
        ],
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
        ]
    },
)
