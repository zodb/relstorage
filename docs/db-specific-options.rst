===================================
 Database-Specific Adapter Options
===================================

Each adapter supports one common option:

driver
    The name of the driver to use for this database. Defaults to
    "auto", meaning to choose the best driver from among the
    possibilities. Most of the time this option should be omitted and
    RelStorage will choose the best option.

    This is handy to set when an environment might have multiple
    drivers installed, some of which might be non-optimal. For
    example, on PyPy, PyMySQL is generally faster than MySQLdb, but
    both can be installed (in the form of mysqlclient for the latter).

    This is also convenient when using zodbshootout to compare driver
    speeds.

    If you specify a driver that is not installed, an error will be raised.

    Each adapter will document the available driver names.

    .. versionadded:: 2.0b2


.. toctree::

   postgresql/options
   mysql/options
   oracle/options
   sqlite3/options
