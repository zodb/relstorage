==========================================
 Migrating to a new version of RelStorage
==========================================

Sometimes RelStorage needs a schema modification along with a software
upgrade.  Hopefully, this will not often be necessary.

Migration to RelStorage version 1.5 requires a schema upgrade.
See `migrate-to-1.5.txt`_.

.. _`migrate-to-1.5.txt`: https://github.com/zodb/relstorage/blob/master/notes/migrate-to-1.5.txt

Migration to RelStorage version 1.4.2 requires a schema upgrade if
you are using a history-free database (meaning keep-history is false).
See `migrate-to-1.4.txt`_.

.. _`migrate-to-1.4.txt`: https://github.com/zodb/relstorage/blob/master/notes/migrate-to-1.4.txt

See the `notes subdirectory`_ if you are upgrading from an older version.

.. _`notes subdirectory`: https://github.com/zodb/relstorage/tree/master/notes
