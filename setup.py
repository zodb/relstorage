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

VERSION = "1.7.0a1.dev0"

# The choices for the Trove Development Status line:
# Development Status :: 5 - Production/Stable
# Development Status :: 4 - Beta
# Development Status :: 3 - Alpha

classifiers = """\
Intended Audience :: Developers
License :: OSI Approved :: Zope Public License
Programming Language :: Python
Programming Language :: Python :: Implementation :: CPython
Programming Language :: Python :: Implementation :: PyPy
Topic :: Database
Topic :: Software Development :: Libraries :: Python Modules
Operating System :: Microsoft :: Windows
Operating System :: Unix
"""

import os
from setuptools import setup

doclines = __doc__.split("\n")


def read_file(*path):
    base_dir = os.path.dirname(__file__)
    file_path = (base_dir, ) + tuple(path)
    with open(os.path.join(*file_path)) as f:
        result = f.read()
    return result

tests_require = ['mock', 'zope.testing', 'ZODB [test]']

setup(
    name="RelStorage",
    version=VERSION,
    author="Zope Foundation and Contributors",
    maintainer="Shane Hathaway",
    maintainer_email="shane@hathawaymix.org",
    url="http://pypi.python.org/pypi/RelStorage",
    packages=[
        'relstorage',
        'relstorage.adapters',
        'relstorage.adapters.tests',
        'relstorage.tests',
        'relstorage.tests.blob',
    ],
    package_data={
        'relstorage': ['component.xml'],
    },
    license="ZPL 2.1",
    platforms=["any"],
    description=doclines[0],
    classifiers=filter(None, classifiers.split("\n")),
    long_description=(
        read_file("README.txt") + "\n\n" +
        "Change History\n" +
        "==============\n\n" +
        read_file("CHANGES.txt")),
    zip_safe=False,  # otherwise ZConfig can't see component.xml
    install_requires=[
        'perfmetrics',
        'ZODB >= 4.3.0, <5.0',
        # ZEO is needed for blob layout
        'ZEO >= 4.2.0b1, <5.0',
        'zope.interface',
        'zc.lockfile',
    ],
    tests_require = tests_require,
    extras_require={
        'mysql:platform_python_implementation=="CPython"': [
            'MySQL-python>=1.2.2',
        ],
        'mysql:platform_python_implementation=="PyPy"' : [
            'PyMySQL>=0.6.6',
        ],
        'postgresql: platform_python_implementation == "CPython"': [
            'psycopg2>=2.0',
        ],
        'postgresql: platform_python_implementation == "PyPy"': [
            'psycopg2cffi>=2.7.0',
        ],
        'oracle': [
            'cx_Oracle>=4.3.1'
        ],
        'test': tests_require,
    },
    entry_points = {
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
