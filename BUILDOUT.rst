========================================
Building for running tests with buildout
========================================

Run the buildout as usual, but to include database support specify a
db option, with one or more database names sperated by commas.  For
example, to build for postgres, mysql and oracle::

  bin/buildout db=postgresql,mysql,oracle

Or just postgres:

  bin/buildout db=postgresql

Before running tests, you'll need to run database setup scripts in the
``.travis`` subdirectory.

Then run tests with ``bin/test``.
