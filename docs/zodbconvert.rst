=================================================
 Copying Data Between ZODB Storages: zodbconvert
=================================================

RelStorage comes with a script named ``zodbconvert`` that converts
databases between formats. Use it to convert a FileStorage instance to
RelStorage and back, or to convert between different kinds of
RelStorage instances, or to convert other kinds of storages that
support the storage iterator protocol.

When converting between two history-preserving databases (note that
FileStorage uses a history-preserving format), ``zodbconvert``
preserves all objects and transactions, meaning you can still use the
ZODB undo feature after the conversion, and you can convert back using
the same process in reverse. When converting from a history-free
database to either a history-free database or a history-preserving
database, ``zodbconvert`` retains all data, but the converted
transactions will not be undoable. When converting from a
history-preserving storage to a history-free storage, ``zodbconvert``
drops all historical information during the conversion.

How to use ``zodbconvert``
==========================

Create a ZConfig style configuration file that specifies two storages,
one named "source", the other "destination". The configuration file
format is very much like zope.conf. Then run ``zodbconvert``, providing
the name of the configuration file as a parameter.

The utility does not modify the source storage. Before copying the
data, the utility verifies the destination storage is completely empty.
If the destination storage is not empty, the utility aborts without
making any changes to the destination, unless the ``--incremental``
option is used (in which case the destination *must* be a previously
copied version of the source).

.. highlight:: guess

Here is a sample ``zodbconvert`` configuration file::

  <filestorage source>
    path /zope/var/Data.fs
  </filestorage>

  <relstorage destination>
    <mysql>
      db zodb
    </mysql>
  </relstorage>

This configuration file specifies that the utility should copy all of
the transactions from Data.fs to a MySQL database called "zodb". If you
want to reverse the conversion, exchange the names "source" and
"destination". All storage types and storage options available in
zope.conf are also available in this configuration file.

Incremental Copies
------------------

The option ``--incremental`` asks zodbconvert to perform a partial
copy of the data. It requires that the destination storage contains a
*complete* copy of all data from the source up through the last
transaction recorded in the destination. (For the first invocation of
the program, the destination storage should be empty.)

For this to work correctly, the source database must provide a
consistent view of the database through its iterators. RelStorage does
this, as does a FileStorage; it is not know if ZEO or other storages
do this.

This can be used to update a previous copy or update a backup. It can
also be used to perform an online migration while the source is in use
and changing, without requiring much downtime. Each invocation of
``zodbconvert --incremental`` will come closer and closer to "catching
up" to the most recent transaction in the source database (and run for
shorter periods). When the copy is "close enough," stop writing to the
source database, perform one final ``zodbconvert --incremental`` (thus
matching the exact state of the source database), and finally
re-configure your software to use the destination database instead of
the source database and restart writing to the "new" database.

.. tip::

   When the destination is a history-free RelStorage, using
   ``--incremental`` is several times slower than performing a regular
   copy. For this reason, and for the reason outlined below, using
   ``--incremental`` with history-free RelStorage destinations is
   discouraged.

.. caution::

   If a copy into a history-free RelStorage is interrupted and does
   not complete, *it is not safe* to restart the copy with
   ``--incremental``; doing so could result in data loss. This is
   because copying into a history-free RelStorage copies in order of
   object ID, while copying with ``--incremental`` copies in order of
   transaction ID.

   To safely use incremental with a history-free RelStorage
   destination, first ensure that a complete copy of the source has
   been performed at least once. After that, the destination can be
   updated with ``--incremental``.

   ``zodbconvert`` will not warn you about the potential corruption
   that can occur if a non-incremental copy was halted part way and
   then a ``--incremental`` copy was begun.

   To safely copy into a history-free database,  perform a
   complete copy *without* ``--incremental`` and then perform further
   updates with ``--incremental``. If the source database is so large
   that performing a single complete copy is impractical, you must
   begin the very first copy with ``--incremental``.

Tips
====

When using a storage wrapper, such as `zc.zlibstorage
<https://pypi.org/project/zc.zlibstorage/>`_ to provide
transformations of the stored data, it is not necessary to keep the
wrapper when both the source and destination storage would use the
same wrapper.

In fact it is faster, to *remove the wrapper* on both the source and
destination storage when performing the copy. This avoids the
redundant decompressing and recompressing of the data as it is read
from one storage and copied to the next.

The exception, of course, is if you want the destination to feature a
transformation (or untransformation) that the source does not have.

For example, if you initially have a configuration like this::

  <zlibstorage>
    <filestorage>
      path /zope/var/Data.fs
    </filestorage>
  </zlibstorage>

The fastest way to create a compressed RelStorage copy will be to omit
the storage wrapper::

  <filestorage source>
    path /zope/var/Data.fs
  </filestorage>

  <relstorage destination>
    <mysql>
      db zodb
    </mysql>
  </relstorage>


To use the resulting RelStorage, you'll need to re-apply the wrapper::

  <zlibstorage>
    <relstorage>
      <mysql>
        db zodb
      </mysql>
    </relstorage>
  </zlibstorage>


In contrast, this configuration will produce an *uncompressed* RelStorage::

  <zlibstorage>
    <filestorage source>
      path /zope/var/Data.fs
    </filestorage>
  </zlibstorage>

  <relstorage destination>
    <mysql>
      db zodb
    </mysql>
  </relstorage>


Options for ``zodbconvert``
===========================

  .. program-output:: zodbconvert --help
