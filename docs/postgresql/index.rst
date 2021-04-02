============
 PostgreSQL
============

.. toctree::

   install
   setup
   options
   pgbouncer

.. tip::

   Using ZODB's ``readCurrent(ob)`` method will result in taking
   shared locks (``SELECT FOR SHARE``) in PostgreSQL for the row
   holding the data for *ob*.

   This operation performs disk I/O, and consequently has an
   associated cost. We recommend using this method judiciously.

   For more information, see `this article on PostgreSQL
   implementation details
   <https://buttondown.email/nelhage/archive/22ab771c-25b4-4cd9-b316-31a86f737acc>`_.
