=====================================
 Migrating to RelStorage version 3.0
=====================================

A small migration is necessary for all schemas. It is accomplished
automatically the first time RelStorage 3.0 is used. See `the release
announcement
<https://dev.nextthought.com/blog/2019/11/relstorage-30.html>`_ for
details.

Oracle
======

The Oracle users will need to have ``CREATE VIEW`` privileges granted
to them before a history-free RelStorage can be opened.
