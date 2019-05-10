.. _migrate-to-1.1.2:


==========================================================
 Migrating from RelStorage version 1.1.1 to version 1.1.2
==========================================================

.. highlight:: sql

Before following these directions, first upgrade to the schema of
RelStorage version 1.1.1 by following the directions in :ref:`migrate-to-1.1.1`.

Only Oracle needs a schema update for this release::

    DROP TABLE temp_pack_visit;
    CREATE GLOBAL TEMPORARY TABLE temp_pack_visit (
        zoid        NUMBER(20) NOT NULL PRIMARY KEY,
        keep_tid    NUMBER(20)
    );
