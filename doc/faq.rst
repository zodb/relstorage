======
 FAQs
======

Q: How can I help improve RelStorage?

    A: The best way to help is to test and to provide database-specific
    expertise.  Ask questions about RelStorage on the zodb-dev mailing list.

Q: Can I perform SQL queries on the data in the database?

    A: No.  Like FileStorage and DirectoryStorage, RelStorage stores the data
    as pickles, making it hard for anything but ZODB to interpret the data.  An
    earlier project called Ape attempted to store data in a truly relational
    way, but it turned out that Ape worked too much against ZODB principles and
    therefore could not be made reliable enough for production use.  RelStorage,
    on the other hand, is much closer to an ordinary ZODB storage, and is
    therefore more appropriate for production use.

Q: How does RelStorage performance compare with FileStorage?

    A: According to benchmarks, RelStorage with PostgreSQL is often faster than
    FileStorage, especially under high concurrency.

Q: Why should I choose RelStorage?

    A: Because RelStorage is a fairly small layer that builds on world-class
    databases.  These databases have proven reliability and scalability, along
    with numerous support options.

Q: Can RelStorage replace ZRS (Zope Replication Services)?

    A: Yes, RelStorage inherits the replication capabilities of PostgreSQL,
    MySQL, and Oracle.

Q: How do I set up an environment to run the RelStorage tests?

    A: See README.txt in the relstorage/tests directory.
