===================================
 Tips and Other Useful Information
===================================

While RelStorage is essentially a drop-in replacement for FileStorage
or ZEO, there are a few differences. This document will highlight some
of those. Much of this information is applicable for all storages, though.

.. _to-know-about-transactions:

The Importance of Managing Transaction Lifetimes
================================================

ZODB, and many other Python libraries, use :mod:`transaction` to
represent application-level transactions.

When using RelStorage, an application-level transaction is also backed
by a RDBMS transaction. In order to present a consistent view of the
data in the database, RDBMS transactions typically need to reserve
certain resources. In some database implementations, if these resources
are reserved for long enough, problems can develop. For example, under
PostgreSQL, problems with the auto-vacuum processes might emerge,
manifesting as reduced performance and increased disk usage.

For this reason, it is important to be aware of, and manage, the
lifetime of application-level transactions.

A secondary reason to manage application-level transaction lifetimes
is that after transaction boundaries (starting a new transaction) is the
only time a ZODB Connection can see new object states, including new
objects. (Technically there are some ways around this, but they're not
recommended and can lead to consistency errors.)

Use Explicit Transaction Managers
---------------------------------

The best way to ensure transaction lifetimes are managed correctly is
to be *explicit* about transaction lifetimes. That means making a call
to ``begin()``, followed by a call to ``commit()`` or ``abort()``
after the application has done its work (in a timely fashion).

Unfortunately, for historical reasons, the :mod:`transaction` library
does not require this by default. In that mode, any use of a ZODB
``Connection`` or persistent object may *automatically* start a new
transaction. Version 3 of the :mod:`transaction` library provides a
new ``explicit`` attribute; when set to `True`, the library will
enforce proper lifetime management. This has the added benefit of
being much more efficient.

Applications that use the global transaction manager can set this
attribute (in each thread) *before* opening a database connection:

.. code-block:: python

    import transaction
    transaction.manager.explicit = True
    transaction.begin()
    try:
        ...
    except:
        transaction.abort()
        raise
    else:
        transaction.commit()

For new applications, and carefully written existing applications,
this is usually an easy change.

It's also possible to use a non-global transaction manager just with
the ZODB. This manager can be put into explicit mode:

.. code-block:: python

    import transaction
    txm = transaction.TransactionManager(explicit=True)
    db = ZODB.config.databaseFromFile('path/to/config')
    conn = db.open(txm)
    txm.begin()
    try:
        ...
    except:
        transaction.abort()
        raise
    else:
        transaction.commit()

.. caution:: This won't work with other libraries that expect to use
             the global transaction manager.

Using a transaction runner that's aware of explicit transactions, such
as :class:`nti.transactions.loop.TransactionLoop` is another option.

.. _to-know-about-close:

The Importance of Closing The Database
======================================

Once you've opened a ZODB database, it's important to always call
``close`` on that database when you are finished with it.

.. important:: This includes at program exit time if a script just
               "falls off the end."

FileStorage writes its index out when the database is closed, and
RelStorage writes its persistent cache files when the database is
closed. If the database is never closed, this work doesn't happen.

The way in which this is done is application specific. Some frameworks
or application servers (such as gunicorn) have hooks or events that
are called or fired which make good times to close the database.

A simple way to do this can be with :mod:`atexit` handlers. To
decouple the handler from the database, the application might want to
maintain a registry of open databases. Here's an example using
``zope.component``:

.. code-block:: python

   import atexit
   from zope import component
   from ZODB.interfaces import IDatabase

   def close_database():
      db = component.getGlobalSiteManager().queryUtility(IDatabase)
      if db is None:
          return # already closed
      db.close()
      component.getGlobalSiteManager().unregisterUtility(db, IDatabase)

   atexit.register(close_database)
   db = ZODB.config.databaseFromFile('path/to/config')
   component.getGlobalSiteManager().registerUtility(db, IDatabase)
   try:
       serve_forever()
   except:
        close_database()
        raise

It might also be necessary to register a signal handler to perform the
same operation in the event of unclean shutdowns. See :issue:`183` for
more discussion.

.. _to-know-about-pack:

Packing A Database May Increase Disk Usage
==========================================

After packing a RelStorage, whether history-free or history-preserving,
whether through the ZODB APIs or the command line tool
:doc:`zodbpack`, you may find that the database disk usage has
actually increased, sometimes by a substantial fraction of the main
database size.

RelStorage deliberately stores information in the database about the
object references it discovered during packing. The next time a pack
is run, this information is used for objects that haven't changed,
making it unnecessary for RelStorage to discover it again. In turn,
this makes subsequent packs substantially faster when there are many
objects that haven't changed (which is typically the case for many
applications.)

A future version of RelStorage might provide the option to remove this
data when a pack finishes.

You can also use the ``multi-zodb-gc`` script provided by the
``zc.zodbdgc`` project to pack a RelStorage. It does not store this
persistent data, but it may be substantially slower than the native
packing capabilities, especially on large databases.

Use ``readCurrent(ob)`` Judiciously
===================================

At least on PostgreSQL, this involves disk I/O. See
:doc:`postgresql/index` for more.
