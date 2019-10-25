======
 FAQs
======

Q: How can I help improve RelStorage?

    A: The best way to help is to test and to provide
    database-specific expertise. Ask questions about RelStorage on the
    zodb-dev mailing list.

Q: Which relational database should I use?

   A: If your organization already has a standard database server, use
   that one. It'll work fine, and the advantages of a known quantity
   and in-house expertise can't be overstated.

   Otherwise, if you're making a choice, the RelStorage
   maintainers would suggest PostgreSQL 11+, PostgreSQL 9.6+, MySQL
   8.0+, MySQL 5.7.19+ or Oracle, in that order. (Consider SQLite for
   smaller deployments or testing.)

   Oracle is not used in production by the RelStorage maintainers and
   tends to lag behind in feature development; it also does not fully
   support parallel commit. If possible, choose PostgreSQL. Oracle
   support may be deprecated and eventually removed.

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

    A: Because RelStorage is a fairly small layer that builds on
    world-class databases. These databases have proven reliability and
    scalability, along with numerous support options.

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
   scenarios or is undesirable (for example, in containers).

   A SQLite database combines the low resource usage and deployment
   simplicity of FileStorage with the ability for many processes to
   read from and write to the database concurrently. Plus, it's
   typically faster than ZEO. The tradeoff: all processes using the
   database must be on a single machine on order to share memory.

Q: Why is adding or updating new objects so slow?

   A: If the database has grown substantially, it's possible that
   sub-optimal query plans are being used. Try to connect to the
   database using the ``sqlite3`` command line tool and issue the
   `ANALYZE <https://www.sqlite.org/lang_analyze.html>`_ command to
   gather statistics for the database query optimizer.

   When RelStorage creates a new SQLite database, it pre-populates
   statistics `to produce fast query plans
   <https://www.sqlite.org/optoverview.html#manctrl>`_. Over time
   those might become incorrect.

   To combat this, RelStorage issues `PRAGMA OPTIMIZE
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

   A: Possibly (with ``cache-local-mb 0``). Let the operating system
   cache the SQLite data file instead. Depending on the operating
   system, the resources of the machine, and what other processes are
   running, this may result in an overall better use of memory while
   still serving responses very quickly.

Q: What's performance like?

   Generally, overall it's quite good compared to ZEO and not bad
   compared to FileStorage. However, this is highly system-dependent.
   Results of benchmarks have been much better on Linux systems than
   on macOS. Some things, such as allocating new object IDs, are
   slower.

   Here are some zodbshootout benchmark results comparing FileStorage,
   a ZEO server using FileStorage running on localhost, and
   RelStorage+SQLite. The standard benchmarks were run using 1, 5, and
   20 concurrent readers or writers using threads of the same process,
   or for ZEO and SQLite, separate processes. Transactions consisted
   of 1, 10, or 100 objects.

   The computer was a 2011 MacBook Pro that was otherwise mostly idle
   running Gentoo Linux and tuned by pyperf for consistency.

   In the graphs, the bars indicate time, so shorter bars are better,
   and the centered vertical black lines are confidence intervals
   (shorter ones of those indicate more predictable performance).

   First, adding objects. SQLite wins every test (or at least
   essentially ties FileStorage) except with 20 concurrent threads
   adding 100 objects. This could be due to the ZEO networking code
   releasing the GIL longer than SQLite, or due to the fact that
   RelStorage currently has an extra temporary buffer copy involved.

   .. image:: faq_sql_add_objects.png
      :target: _images/faq_sql_add_objects.png


   Updating existing objects is very similar.

   .. figure:: faq_sql_update_objects.png
      :target: _images/faq_sql_update_objects.png


   Writing and then reading the same set of objects is more of a mixed
   bag, especially where threads are combined and transaction sizes
   are small. FileStorage usually wins, possibly thanks to its
   append-only file format. SQLite holds its own against ZEO, except
   when transaction sizes are small. This is probably an artifact of
   the dual connections (one read, one write) RelStorage requires to
   implement MVCC.

   .. figure:: faq_sql_write_read_objects.png
      :target: _images/faq_sql_write_read_objects.png


   Finally, reading objects. SQLite outperforms ZEO (possibly thanks
   to not needing a network round-trip), but FileStorage usually wins.
   The best RelStorage can do is tie. For direct reading of objects,
   it's hard to beat FileStorage's in-memory index of exact disk
   offsets. SQLite has to traverse a BTree structure within the
   database file.


   .. figure:: faq_sql_read_cold_objects.png
      :target: _images/faq_sql_read_cold_objects.png
