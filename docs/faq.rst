======
 FAQs
======

.. toctree::

   sqlite3/faq

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

Q: Why do I get ``DatabaseError: ORA-03115: unsupported network datatype or representation`` when using Oracle?
    A: See the "driver" section of :ref:`oracle-adapter-options` for more
    information.

Q: Is there anything I need to know about transactions?
    A: Yes, see :ref:`to-know-about-transactions`

Q: Is it important to ``close()`` the ``DB`` object?
    A: Yes! Both FileStorage and RelStorage perform maintenance tasks
    when the database is closed. For more information, see :ref:`to-know-about-close`.
