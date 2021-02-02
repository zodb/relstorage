==============
 Installation
==============

RelStorage 3.0 is supported on Python 2.7.9+, Python 3.6, 3.7,
3.8, and 3.9, as well as PyPy2 and PyPy 3 7.1 or later.

.. seealso:: :doc:`supported-databases`

You can install RelStorage using pip::

    pip install RelStorage

If you use a recent version of pip to install RelStorage on a
supported platform (macOS, Windows or "manylinux"), you can get a
pre-built binary wheel. If you install from source or on a different
platform, you will need to have a functioning C/C++ compiler and the
ability to compile `Cython extensions
<https://cython.readthedocs.io/>`_.

RelStorage requires ZODB 5. To use ZODB and ZEO 4 (which
supports Python 2.7.8 and earlier), install RelStorage 2.1. If you
need to use even older versions of ZODB/ZEO, install RelStorage 1.6.
Likewise, if you need Python 2.6 support, install RelStorage 1.6 (note
that 1.6 *does not* run on Python 3 or PyPy).


Database Adapter
================

You also need the Python database adapter that corresponds with your
database.

.. tip::
   The easiest way to get the recommended and tested database adapter for
   your platform and database is to install the corresponding RelStorage
   extra; RelStorage has extras for all three databases that install
   the recommended driver on all platforms::

    pip install "RelStorage[mysql]"
    pip install "RelStorage[postgresql]"
    pip install "RelStorage[oracle]"
    pip install "RelStorage[sqlite3]"

   Installing those packages may require you to have database client
   software and development libraries already installed. Some packages
   may provide binary wheels on PyPI for some platforms. In the case
   of psycopg2, that binary package (which is not recommended for
   production use) can be installed with the name ``psycopg2-binary``.
   Note that the ``postgresql`` extra in RelStorage does **not**
   install the binary but attempts to install from source.


.. toctree::
   :maxdepth: 2

   postgresql/install
   mysql/install
   oracle/install
   sqlite3/install



Memcache Integration
====================

.. note:: Memcache support is deprecated and will be removed in a
          future release.

If you want to use Memcache servers as an external shared cache for
RelStorage clients, you'll need to install either `pylibmc
<https://pypi.python.org/pypi/pylibmc>`_ (C based, requires Memcache
development libraries and CPython) or `python-memcached
<https://pypi.python.org/pypi/python-memcached>`_ (pure-Python, works
on CPython and PyPy, compatible with gevent).


Once RelStorage is installed, it's time to :doc:`configure the database <configure-database>`
you'll be using.
