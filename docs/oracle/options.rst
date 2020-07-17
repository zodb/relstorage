.. _oracle-adapter-options:

========================
 Oracle Adapter Options
========================

The Oracle adapter has been tested against Oracle 12, but likely works
with Oracle 10 as well. The only supported driver is `cx_Oracle
<https://cx-oracle.readthedocs.io/en/latest/user_guide/installation.html>`_.

The Oracle adapter accepts:

driver
    Other than "auto" the only supported value is "cx_Oracle".

    .. caution::
        (This is a historical note. Only version 6.0 and above of
        cx_Oracle is supported in RelStorage 3.)

        If you use cx_Oracle 5.2.1 or 5.3 (in general, any version >=
        5.2 but < 6.0) you must be sure that it is compiled against a
        version of the Oracle client that is compatible with the
        Oracle database to which you will be connecting.

        Specifically, if you will be connecting to Oracle database 11
        or earlier, you must *not* compile against client version 12.
        (Compiling against an older client and connecting to a newer
        database is fine.) If you use a client that is too new,
        RelStorage will fail to commit with the error ``DatabaseError:
        ORA-03115: unsupported network datatype or representation``.

        For more details, see :issue:`172`.

user
    The Oracle account name

password
    The Oracle account password

dsn
    The Oracle data source name.  The Oracle client library will
    normally expect to find the DSN in ``/etc/oratab``.
