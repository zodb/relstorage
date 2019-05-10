=============
 zodbconvert
=============

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

Options for ``zodbconvert``
===========================

  .. program-output:: zodbconvert --help
