##############################################################################
#
# Copyright (c) 2008 Zope Corporation and Contributors.
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
"""A backend for ZODB that stores pickles in a relational database.

This is designed to be a drop-in replacement for the standard ZODB
combination of FileStorage and ZEO.  Multiple ZODB clients can
share the same database without any additional configuration.
Supports undo, historical database views, packing, and lossless
migration between FileStorage and RelStorage instances.

The supported relational databases are PostgreSQL 8.1 and above
(using the psycopg2 Python module), MySQL 5.0 and above (using the
MySQLdb 1.2.2 Python module), and Oracle 10g (using cx_Oracle 4.3).

A small patch to ZODB is required.  See the patch files distributed
with RelStorage.
"""

VERSION = "1.0-beta1"

classifiers = """\
Development Status :: 4 - Beta
Intended Audience :: Developers
License :: OSI Approved :: Zope Public License
Programming Language :: Python
Topic :: Database
Topic :: Software Development :: Libraries :: Python Modules
Operating System :: Microsoft :: Windows
Operating System :: Unix
"""


from distutils.core import setup

doclines = __doc__.split("\n")

setup(
    name="RelStorage",
    version=VERSION,
    maintainer="Shane Hathaway",
    maintainer_email="shane@hathawaymix.org",
    url="http://wiki.zope.org/ZODB/RelStorage",
    packages=['relstorage', 'relstorage.adapters', 'relstorage.tests'],
    package_data={
        'relstorage': ['component.xml'],
    },
    license="ZPL 2.1",
    platforms=["any"],
    description=doclines[0],
    classifiers=filter(None, classifiers.split("\n")),
    long_description = "\n".join(doclines[2:]),
    )
