=======================
 Setting Up PostgreSQL
=======================

.. highlight:: shell

.. important::

   RelStorage can only be installed into a single schema within a
   database. This is usually the default "public" schema. It may be
   possible to use other schemas, but this is not supported or tested.

If you installed PostgreSQL from a binary package, you probably have a
user account named ``postgres``. Since PostgreSQL respects the name of
the logged-in user by default, switch to the ``postgres`` account to
create the RelStorage user and database. Even ``root`` does not have
the PostgreSQL privileges that the ``postgres`` account has. For
example::

    $ sudo su - postgres
    $ createuser --pwprompt zodbuser
    $ createdb -O zodbuser zodb

Alternately, you can use the ``psql`` PostgreSQL client and issue SQL
statements to create users and databases. For example::

    $ psql -U postgres -c "CREATE USER zodbuser WITH PASSWORD 'relstoragetest';"
    $ psql -U postgres -c "CREATE DATABASE zodb OWNER zodbuser;"

New PostgreSQL accounts often require modifications to ``pg_hba.conf``,
which contains host-based access control rules. The location of
``pg_hba.conf`` varies, but ``/etc/postgresql/8.4/main/pg_hba.conf`` is
common. PostgreSQL processes the rules in order, so add new rules
before the default rules rather than after. Here is a sample rule that
allows only local connections by ``zodbuser`` to the ``zodb``
database::

    local  zodb  zodbuser  md5

PostgreSQL re-reads ``pg_hba.conf`` when you ask it to reload its
configuration file::

    /etc/init.d/postgresql reload

Configuration
=============

The default PostgreSQL server configuration will work fine for most
users. However, some configuration changes may yield increased performance.

Defaults and Background
-----------------------

This section is current for PostgreSQL 13 and earlier versions.

``max_connections`` (100) gives the number of worker processes that could
possibly be active at a time. Each worker consumes (at most)
``work_mem`` (4MB) + ``temp_buffers`` (8MB) = 12MB (plus a tiny bit of
overhead).

``shared_buffers`` is the amount of memory that PostgreSQL will
allocate to keeping database data in memory. It is perhaps the single
most important tunable, larger values are better. If data is not in
this, then a worker will have to go to the operating system with an
I/O request (or two). The default is a measly 128MB.

``max_wal_size`` determines how often the data must be taken from the
write-ahead log and placed into the main tables. Reasons to keep this
small are (a) low amount of disk space; (b) reduced crash recovery
time; (c) if you're doing replication in the WAL-based way, keeping
online replicas more up-to-date.

``random_page_cost`` (4.0) is relative to ``seq_page_cost`` (1.0) and
tells how relatively expensive it is to do random I/O versus large
blocks of sequential I/O. This in turn influences whether the planner
will use an index or not. For solid-state drives, the
``random_page_cost`` should generally be lowered.


General
-------

Many PostgreSQL configuration defaults are conservative on modern
machines. Without knowing the resources available to any particular
installation, some general tips are listed below.

.. important:: Be sure you understand the consequences before changing
               any settings. Some of those listed here may be risky,
               depending on your level of risk tolerance.

* Increase ``temp_buffers``. This prevents having to use disk tables for
  temporary storage. RelStorage does a lot with temp tables. In my
  benchmarks, I use 32MB.

* ``work_mem`` improves sorting and hashing, that sort of thing.
  RelStorage doesn't do much of that *except* when you do a native GC,
  and then it can make a big difference. Because this is a max that's
  not allocated unless needed, it should be safe to increase it. In my
  benchmarks, I leave this alone.

* Increase ``shared_buffers`` as much as you are able. When I
  benchmark, on my 16GB laptop, I use 2GB. The rule of thumb for
  dedicated servers is 25% of available RAM.

* If deploying on SSDs, then the cost of random page access can probably
  be lowered some more. I know they're old SSDs, but the cost is
  relative to sequential access, not absolute. This is probably not
  important though, unless you're experiencing issues accessing blobs
  (the only thing doing sequential scans).

* If you are not doing replication, setting ``wal_level = minimal``
  will improve write speed and reduce disk usage. Similarly, setting
  ``wal_compression = on`` will reduce disk IO for writes (at a tiny
  CPU cost). I benchmark with both those settings.

* If you're not doing replication and can stand some longer recovery
  times, increasing ``max_wal_size`` (I use 10GB) has benefit for
  heavy writes. Even if you are doing replication, increasing
  ``checkpoint_timeout`` (I use 30 minutes, up from 5),
  ``checkpoint_completion_target`` (I use 0.9, up from 0.5) and either
  increasing or disabling ``checkpoint_flush_after`` (I disable, the
  default is a skimpy 256KB) also help. This especially helps on
  spinning rust, and for very "bursty" workloads.

* If our IO bandwidth is constrained, and you can't increase
  ``shared_buffers`` enough to compensate, disabling the background
  writer can help too. ``bgwriter_lru_maxpages = 0`` and
  ``bgwriter_flush_after = 0``. I set these when I benchmark using
  spinning rust.

* Setting ``synchronous_commit = off`` makes for faster turnaround
  time on ``COMMIT`` calls. This is safe in the sense that it can
  never corrupt the database in the event of a crash, but it might
  leave the application *thinking* something was saved when it really
  wasn't. Since the whole site will go down in the event of a database
  crash anyway, you might consider setting this to off if you're
  struggling with database performance. I benchmark with it off.


Large Sites
-----------

* For very large sites processing many large or concurrent
  transactions, or deploying many RelStorage instances to a single
  database server, it may be necessary to increase the value of
  ``max_locks_per_transaction`` beginning with RelStorage 3.5.0a4. The
  default value (64) allows about 6,400 objects to be locked because
  it is multiplied by the value of ``max_connections`` (which defaults
  to 100). Large sites may have already increased this second value.

* For systems with very high write levels, setting
  ``wal_writer_flush_after = 10MB`` (or something higher than the
  default of 1MB) and ``wal_writer_delay = 10s`` will improve write
  speed without any appreciable safety loss (because your write volume
  is so high already). I run write benchmarks this way.

* Likewise for high writes, I increase ``autovacuum_max_workers`` from
  the default of 3 to 8 so they can keep up. Similarly, consider
  lowering ``autovacuum_vacuum_scale_factor`` from its default of 20%
  to 10% or even 1%. You might also raise
  ``autovacuum_vacuum_cost_limit`` from its default of 200 to 1000
  or 2000.

Packing
-------

* For packing large databases, a larger value of the PostgreSQL
  configuration paramater ``work_mem`` is likely to yield improved
  performance. The default is 4MB; try 16MB if packing performance is
  unacceptable.

* For packing large databases, setting the ``pack_object``,
  ``object_ref`` and ``object_refs_added`` tables to `UNLOGGED
  <https://www.postgresql.org/docs/12/sql-createtable.html#SQL-CREATETABLE-UNLOGGED>`_
  can provide a performance boost (if replication doesn't matter and
  you don't care about the contents of these tables). This can be done
  after the schema is created with ``ALTER TABLE table SET UNLOGGED``.
