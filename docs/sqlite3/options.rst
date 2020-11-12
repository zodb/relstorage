========================
 SQLite Adapter Options
========================

The name of ZConfig section is ``<sqlite3>``.

This adapter uses the built-in sqlite3 module provided by the Python
standard library. It is available on Python 2.7 (including PyPy) and
Python 3.6 and above (including PyPy3), as long as the underlying
version of SQLite is at least 3.11. The best performance can be
obtained by ensuring the underlying SQLite is at least 3.24.

A SQLite database can be used by multiple processes concurrently, but
because it uses shared memory, those processes *must* all be on the
same machine. The database files also should reside locally.

Using a persistent cache file is not supported with this driver and
will be automatically disabled (``cache-local-dir`` has no effect). In some cases, it may be advantageous
to also disable RelStorage's in-memory pickle cache
altogether (``cache-local-mb 0``) and allow the operating system's
filesystem cache to serve that purpose.

This adapter supports blobs, but you still must configure a
``blob-cache-dir``, or use a ``shared-blob-dir``.

.. note::

   SQLite is limited to 8-byte *signed* integers for OIDs and TIDs
   (other database use unsigned integers). If you expect to go through
   more than nine quintillion objects, or use the database past the
   year 5908, SQLite might not be the right choice.

For more, see :doc:`faq`.

There is one required setting:

data-dir
    The path to a directory to hold the data.

    Choosing a dedicated directory is strongly recommended. A network
    filesystem is generally not recommended.

    Several files will be created in this directory automatically by
    RelStorage. Some are persistent while others are transient. Do not
    remove them or data corruption may result.

    For information on backing this directory up, see
    :ref:`the FAQ about backing up SQLite <backing-up-sqlite>`.

Optional settings include:

driver
    This can be set to "gevent sqlite3" to activate a mode that yields
    to the gevent loop periodically. Normally, sqlite3 gives up the
    GIL and allows other threads to run while executing SQLite
    queries, but for a gevent-based system, that's not particularly
    helpful. This driver will call ``gevent.sleep()`` approximately every
    ``gevent_yield_interval`` virtual machine instractions executed by
    any given connection. (It's approximate because the instruction
    count is tracked per-prepared statement. RelStorage uses a few
    different prepared statements during normal operations.)

    The gevent sqlite driver supports RelStorage's critical commit section.

gevent_yield_interval
    Only used if the driver is ``gevent sqlite``. The default is
    currently 100, but that's arbitrary and subject to change. Choosing a specific value that
    works well for your application is recommended. Note that this
    will interact with the ``cache-local-mb`` value as that's shared
    among Connections and could reduce the number of SQLite queries
    executed.


RelStorage configures SQLite to work well and be safe. For advanced
tuning, nearly the entire set of `SQLite PRAGMAs
<https://www.sqlite.org/pragma.html>`_ are available. Put them in the
``pragmas`` section of the configuration. For example, this
configuration is meant to make SQLite run as fast as possible while
ignoring safety considerations::

        <sqlite3>
            data-dir /dev/shm
            <pragmas>
                synchronous off
                checkpoint_fullfsync off
                defer_foreign_keys on
                fullfsync off
                ignore_check_constraints on
                foreign_keys off
            </pragmas>
        </sqlite3>

Particularly useful pragmas to consider adjusting include
``cache_size`` and ``mmap_size``.

Setting ``wal_autocheckpoint`` to a larger value than the default may
improve write speed at the expense of read speed and substantially
increased disk usage. (A setting of 0 is not recommended.)

Setting ``max_page_count`` can be used to enforce a crude quota
system.

The ``journal_mode`` cannot be changed.
