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
import sys
import os
import platform

from setuptools import setup
from setuptools import find_packages
from setuptools import Extension

# Based on code from
# http://cython.readthedocs.io/en/latest/src/reference/compilation.html#distributing-cython-modules
def _dummy_cythonize(extensions, **_kwargs):
    for extension in extensions:
        sources = []
        for sfile in extension.sources:
            path, ext = os.path.splitext(sfile)
            if ext in ('.pyx', '.py'):
                ext = '.cpp' if extension.language == 'c++' else '.c'
                sfile = path + ext
            sources.append(sfile)
        extension.sources[:] = sources
    return extensions

try:
    from Cython.Build import cythonize
except ImportError:
    # The generated source files had better already exist.
    cythonize = _dummy_cythonize


def read_file(*path):
    base_dir = os.path.dirname(__file__)
    file_path = (base_dir, ) + tuple(path)
    with open(os.path.join(*file_path), 'rt', encoding='utf-8') as f:
        result = f.read()
    return result

VERSION = read_file('version.txt').strip()
PYPY = hasattr(sys, 'pypy_version_info')
WINDOWS = sys.platform.startswith("win")
PY3 = sys.version_info[0] == 3

memcache_require = [
    # Couldn't get this building on 3.12b3;
    # It's deprecated though.
    'pylibmc; platform_python_implementation=="CPython" and sys_platform != "win32" and python_version < "3.12"',
    'python-memcached; platform_python_implementation=="PyPy" or sys_platform == "win32"',
]

tests_require = [
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
    'gevent >= 23.7.0',
    'pyperf',
    # Versions of PyPy2 prior to 7.4 (maybe?) are incompatible with
    # psutil >= 5.6.4.
    # https://github.com/giampaolo/psutil/issues/1659
    'psutil; platform_python_implementation=="CPython" or python_version!="2.7"',
] + memcache_require

# Extra compiler arguments passed to C++ extensions
cpp_compile_args = []

# Extra linker arguments passed to C++ extensions
cpp_link_args = []

plat_platform = platform.platform()
plat_machine = platform.machine()
plat_compiler = platform.python_compiler()

if not os.environ.get('RELSTORAGE_SETUP_NO_FLAGS'):
    # Provide a temporary, undocumented, unsupported, escape hatch in case we get
    # any flags wrong. If you need this, please let the maintainers know!
    if sys.platform == 'darwin' or 'clang' in plat_compiler:
        # The clang compiler doesn't use --std=c++11 by default
        cpp_compile_args.append("--std=gnu++11")
    elif WINDOWS and "MSC" in plat_compiler:
        # Older versions of MSVC (Python 2.7) don't handle C++ exceptions
        # correctly by default. While newer versions do handle exceptions
        # by default, they don't do it fully correctly ("By default....the
        # compiler generates code that only partially supports C++
        # exceptions."). So we need an argument on all versions.

        #"/EH" == exception handling.
        #    "s" == standard C++,
        #    "c" == extern C functions don't throw
        # OR
        #   "a" == standard C++, and Windows SEH; anything may throw, compiler optimizations
        #          around try blocks are less aggressive. Because this catches SEH,
        #          which Windows uses internally, the MS docs say this can be a security issue.
        #          DO NOT USE.
        # /EHsc is suggested, and /EHa isn't supposed to be linked to other things not built
        # with it. Leaving off the "c" should just result in slower, safer code.
        # Other options:
        #    "r" == Always generate standard confirming checks for noexcept blocks, terminating
        #           if violated. IMPORTANT: We rely on this.
        # See
        # https://docs.microsoft.com/en-us/cpp/build/reference/eh-exception-handling-model
        #    ?view=msvc-170
        handler = "/EHsr"
        cpp_compile_args.append(handler)
        # To disable most optimizations:
        #cpp_compile_args.append('/Od')

        # To enable assertions:
        #cpp_compile_args.append('/UNDEBUG')

        # To enable more compile-time warnings (/Wall produces a mountain of output).
        #cpp_compile_args.append('/W4')

        # To link with the debug C runtime...except we can't because we need
        # the Python debug lib too, and they're not around by default
        # cpp_compile_args.append('/MDd')

        # Support fiber-safe thread-local storage: "the compiler mustn't
        # cache the address of the TLS array, or optimize it as a common
        # subexpression across a function call." This would probably solve
        # some of the issues we had with MSVC caching the thread local
        # variables on the stack, leading to having to split some
        # functions up. Revisit those.
        cpp_compile_args.append("/GT")

extra_compile_args = cpp_compile_args
extra_link_args = cpp_link_args

setup(
    name="RelStorage",
    version=VERSION,
    author="Shane Hathaway with Zope Foundation and Contributors",
    author_email="shane@willowrise.com",
    maintainer="Jason Madden",
    maintainer_email="jason@nextthought.com",
    url="https://relstorage.readthedocs.io/",
    project_urls={
        'Bug Tracker': 'https://github.com/zodb/relstorage/issues',
        'Source Code': 'https://github.com/zodb/relstorage/',
        'Documentation': 'https://relstorage.readthedocs.io',
    },
    keywords="ZODB SQL RDBMS MySQL PostgreSQL Oracle",
    packages=find_packages('src'),
    package_dir={'': 'src'},
    include_package_data=True,
    license="ZPL 2.1",
    platforms=["any"],
    description="A backend for ZODB that stores pickles in a relational database.",
    # 3.8: importlib.metadata
    python_requires=">=3.9",
    classifiers=[
        "Intended Audience :: Developers",
        "License :: OSI Approved :: Zope Public License",
        "Programming Language :: Python",
        "Programming Language :: Python :: 3 :: Only",
        "Programming Language :: Python :: 3.9",
        "Programming Language :: Python :: 3.10",
        "Programming Language :: Python :: 3.11",
        "Programming Language :: Python :: 3.12",
        "Programming Language :: Python :: 3.13",
        "Programming Language :: Python :: Implementation :: CPython",
        "Programming Language :: Python :: Implementation :: PyPy",
        "Topic :: Database",
        "Topic :: Software Development :: Libraries :: Python Modules",
        "Operating System :: Microsoft :: Windows",
        "Operating System :: Unix",
        "Development Status :: 4 - Beta",
    ],
    long_description=read_file("README.rst"),
    # We cannot be used from an archive. ZConfig can't see
    # our component.xml, and we rely on being able to use __file__ to
    # list and locate auxiliary files (e.g., schema.py finds SQL files)
    zip_safe=False,
    setup_requires=[
    ],
    install_requires=[
        # PyPA standard version and requirement handling.
        'packaging',
        'perfmetrics >= 3.0.0',
        'zope.interface',
        'zope.dottedname',
        'zc.lockfile',
        'BTrees >= 4.7.2', # unsigned btrees in 4.7+; correct errors in 4.7.2
        # These are the versions we're testing against. ZODB 5.2.2 is
        # when checkSecure() went away and IMVCCAfterCompletionStorage
        # was added, and 5.1.2 is when Connection.new_oid was added
        # (https://github.com/zopefoundation/ZODB/issues/139).
        # 5.6 introduced TransactionMetaData.extension_bytes
        # (https://github.com/zodb/relstorage/issues/424)
        'ZODB >= 5.6.0',
        # We directly use this, and its a transient dep of ZODB.
        # version 2.0 is where things became text, and 2.1 partly
        # relaxed those requirements.
        'transaction >= 2.4.0',
    ],
    ext_modules=cythonize(
        # For Python 2 on Windows, we used to compile with the /EHsc flag:
        # https://docs.microsoft.com/en-us/cpp/build/reference/eh-exception-handling-model
        # /EHsc: `s`: enable stack unwinding, catch C++ exceptions in catch(...)
        #        `c`: extern C functions never throw C++ exceptions.
        # XXX: Why this flag or Python 2/Windows? Only?!
        # /EHa (catch SEH and standard) seems better.
        [
            Extension(
                name="relstorage.cache.cache",
                language="c++",
                sources=[
                    'src/relstorage/cache/cache.pyx',
                    'src/relstorage/cache/c_cache.cpp',
                ],
                include_dirs=['include', 'src/relstorage'],
                extra_compile_args=extra_compile_args,
                extra_link_args=extra_link_args,
            ),
            Extension(
                name="relstorage._inthashmap",
                language="c++",
                sources=[
                    'src/relstorage/_inthashmap.pyx',
                ],
                include_dirs=['include'],
                extra_compile_args=extra_compile_args,
                extra_link_args=extra_link_args,
            ),
            Extension(
                name="relstorage.cache._objectindex",
                language="c++",
                sources=[
                    'src/relstorage/cache/_objectindex.pyx',
                ],
                include_dirs=['include', 'src/relstorage'],
                extra_compile_args=extra_compile_args,
                extra_link_args=extra_link_args,
            ),
        ],
        annotate=True,
        compiler_directives={
            'language_level': '3str',
            'always_allow_keywords': False,
            'infer_types': True,
            'nonecheck': False,
        },
    ),
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
        # compile on windows for Python 2.7 (it doesn't support the
        # old Visual C) and is hard to compile for anything newer; it sometimes
        # has wheels but not always, so don't suggest it.
        #
        # Note that mysqlclient 2.0.0 has a crashing bug:
        # https://github.com/PyMySQL/mysqlclient-python/issues/435


        # pylint:disable=line-too-long
        'mysql:platform_python_implementation=="CPython" and (sys_platform != "win32")': [
            'mysqlclient >= 2.0.0',
        ],
        'mysql:platform_python_implementation=="PyPy" or (sys_platform == "win32")': [
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
        'postgresql: platform_python_implementation == "CPython" and python_version != "3.13"' : [
            # 2.4.1+ is required for proper bytea handling;
            # 2.6+ is needed for 64-bit lobject support;
            # 2.7+ is needed for Python 3.7 support and PostgreSQL 10+;
            # 2.7.6+ is needed for PostgreSQL 11;
            # 2.8 is needed for conn.info
            # 2.9.10 will be needed for Python 3.13, but it's not out yet.
            'psycopg2 >= 2.8.3',
        ],
        'postgresql: platform_python_implementation == "CPython" and python_version == "3.13"': [
            # psycopg2 2.9.10 is needed, but not available yet.
            # See also 'all tested drivers'
            'pg8000',
        ],
        'postgresql: platform_python_implementation == "PyPy"': [
            # 2.8.0+ is needed for Python 3.7
            'psycopg2cffi >= 2.8.1',
        ],
        'oracle': [
            # 5.2 added support for LOBs larger than 4GB.
            # 6.0 lets lobs be used across DB fetches, uses
            # temp lob caching.
            # 6.2 lets lobs be bound directly to a cursor, and
            # 7.0 lets lobs be set directly from bytes, without an intermediate
            # temporary lob.
            'cx_Oracle>=6.0'
        ],
        'sqlite': [],
        'sqlite3': [],
        'memcache': memcache_require,
        'test': tests_require,
        'docs': [
            'sphinx',
            'sphinxcontrib-programoutput',
            'repoze.sphinx.autointerface',
            'sphinx_rtd_theme',
            'ZEO',
            'furo',
        ],
        'all_tested_drivers': [
            # Install all the supported drivers for the platform.
            # Spread them out across the versions to not load any one
            # up too heavy for better parallelism.

            # First, mysql
            # pymysql on 3.9 on all platforms.
            'PyMySQL >= 0.6.6; python_version == "3.9"',
            # mysqlclient (binary) on all CPythons. It's the default.
            'mysqlclient >= 2.0.0',
            # mysql-connector-python; one of two pure-python versions
            # This requirement is repeated in the driver class.
            'mysql-connector-python >= 8.0.32; python_version == "3.10"',

            # postgresql
            # pure-python
            # pg8000
            # This requirement is repeated in the driver class.
            'pg8000 >= 1.29.0; python_version == "3.11" or python_version == "3.13"',
            # CFFI, runs on all implementations.
            'psycopg2cffi >= 2.7.4; python_version == "3.11" or platform_python_implementation == "PyPy"',
            # Psycopg2 on all CPython, it's the default
            'psycopg2 >= 2.8.3; platform_python_implementation == "CPython" and python_version != "3.13"',
        ],
    },
    entry_points={
        'console_scripts': [
            'zodbconvert = relstorage.zodbconvert:main',
            'zodbpack = relstorage.zodbpack:main',
        ],
        'zodburi.resolvers': [
            'postgres = relstorage.zodburi_resolver:postgresql_resolver',
            'mysql = relstorage.zodburi_resolver:mysql_resolver',
            'oracle = relstorage.zodburi_resolver:oracle_resolver',
            'sqlite = relstorage.zodburi_resolver:sqlite_resolver',
        ],
        'gevent.plugins.monkey.did_patch_builtins': [
            'psycopg2 = relstorage.adapters.postgresql.drivers.psycopg2:_gevent_did_patch',
        ],
    },
)
