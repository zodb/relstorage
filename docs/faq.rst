======
 FAQs
======

Q: How can I help improve RelStorage?

    A: The best way to help is to test and to provide database-specific
    expertise.  Ask questions about RelStorage on the zodb-dev mailing list.

Q: Can I perform SQL queries on the data in the database?

    A: No. Like FileStorage and DirectoryStorage, RelStorage stores
    the data as pickles, making it hard for anything but ZODB to
    interpret the data. An earlier project called Ape attempted to
    store data in a truly relational way, but it turned out that Ape
    worked too much against ZODB principles and therefore could not be
    made reliable enough for production use. RelStorage, on the other
    hand, is much closer to an ordinary ZODB storage, and is therefore
    more appropriate for production use.

Q: How does RelStorage performance compare with FileStorage?

    A: According to benchmarks, RelStorage with PostgreSQL is often faster than
    FileStorage, especially under high concurrency. See
    :doc:`performance` for more.

Q: Why should I choose RelStorage?

    A: Because RelStorage is a fairly small layer that builds on world-class
    databases.  These databases have proven reliability and scalability, along
    with numerous support options.

Q: Can RelStorage replace ZRS (Zope Replication Services)?

    A: Yes, RelStorage inherits the replication capabilities of PostgreSQL,
    MySQL, and Oracle.

Q: How do I set up an environment to run the RelStorage tests?

    A: See :doc:`developing`.

Q: Why do I get ``DatabaseError: ORA-03115: unsupported network
datatype or representation`` when using Oracle?

    See the "driver" section of :ref:`oracle-adapter-options` for more
    information.


SQLite
======

Q: Why does RelStorage support a SQLite backend? Doesn't that defeat
the point?

   A: The SQLite backend fills a gap between FileStorage and an
   external RDBMS server.

   FileStorage is fast, requires few resources, and has no external
   dependencies. This makes it well suited to small applications,
   embedded applications, or applications where resources are
   constrained or ease of deployment is important (for example, in
   containers).

   However, a FileStorage can only be opened by one process at a time.
   Within that process, as soon as a thread begins committing, other
   threads are locked out of committing.

   An external RDBMS server (e.g., PostgreSQL) is fast, flexible and
   provides lots of options for managing backups and replications and
   performance. It can be used concurrently by many clients on many
   machines, any number of which can be committing in parallel. But
   that flexibility comes with a cost: it must be setup and managed.
   Sometimes running additional processes complicates deployment
   scenarios.

   A SQLite database combines the low resource usage and deployment
   simplicity of FileStorage with the ability for many processes to
   open the database concurrently. Plus, it's faster than ZEO. The
   tradeoff: all processes using the database must be on a single
   machine on order to share memory.

Q: Why is adding or updating new objects so slow?

   A: If the database has grown substantially, it's possible that
   sub-optimal query plans are being used. Try to connect to the
   database using the ``sqlite3`` command line tool and issue the
   `ANALYZE <https://www.sqlite.org/lang_analyze.html>`_ command.

   RelStorage issues `PRAGMA OPTIMIZE
   <https://www.sqlite.org/pragma.html#pragma_optimize>`_ when
   connections are closed, but that doesn't always work (the database
   may be locked by another connection``.

   RelStorage issues the ``ANALYZE`` command automatically when the
   database is packed.

Q: What maintenance does SQLite require?

   A: Not much. If the database changes substantially, you might need
   to ``ANALYZE`` it for best performance. If lots of objects are
   added and removed, you may want to `VACUUM
   <https://www.sqlite.org/lang_vacuum.html>`_ it.

Q: Why is the SQLite database constrained to processes on a single
machine?

   A: RelStorage uses SQLite's `write-ahead log (wal)
   <https://www.sqlite.org/wal.html>`_ to implement concurrency
   and MVCC. This requires all processes using the database to be able
   to share memory.

Q: What level of concurrency does SQLite support? Does it allow
parallel commits?

   A: When RelStorage is used with SQLite, concurrent reads are always
   supported with no limit.

   However, SQLite only allows one connection to write to a database
   at a time. It does not support the object-level locks as used by
   the other databases in RelStorage 3+. Most of the work RelStorage
   does during commit goes into a temporary database, though, and the
   database is locked as close to the end of the commit process as
   possible.

Q: Should I disable RelStorage's cache when used with SQLite?

   A: Probably (with ``cache-local-mb 0``). Let the operating system
   cache the SQLite data file instead.
