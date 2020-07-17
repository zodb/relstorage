========================================
 Installing Database Drivers for SQLite
========================================

Support for SQLite is provided by the standard library :mod:`sqlite3`
module; no extra library is required. The underlying SQLite database
must be at least version 3.8.3.

gevent
======

SQLite3 can be used with gevent by explicitly choosing a gevent-aware
driver. This uses a progress handler to switch periodically, and also
moves certain blocking operations into gevent's thread pool.

For additional details and warnings, see the "driver" section in
:doc:`options`.
