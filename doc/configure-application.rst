==============================
 Configuring Your Application
==============================

Once RelStorage is installed together with the appropriate database
adapter, and you have created a user and database to use with
RelStorage, it's time to configure the application to use RelStorage.

.. note:: This is just a quick-start guide. There is a :doc:`full list
          of supported options <relstorage-options>` in a separate
          document, as well as a :doc:`list of database-specific
          options <db-specific-options>`.

.. highlight:: guess

Configuring Plone
=================

To install RelStorage in Plone, see the instructions in the following
article:

    http://shane.willowrise.com/archives/how-to-install-plone-with-relstorage-and-mysql/

Plone uses the ``plone.recipe.zope2instance`` Buildout recipe to
generate zope.conf, so the easiest way to configure RelStorage in a
Plone site is to set the ``rel-storage`` parameter in ``buildout.cfg``.
The ``rel-storage`` parameter contains options separated by newlines,
with these values:

    * ``type``: any database type supported (``postgresql``, ``mysql``,
      or ``oracle``)
    * RelStorage options like ``cache-servers`` and ``poll-interval``
    * Adapter-specific options

An example::

    rel-storage =
        type mysql
        db plone
        user plone
        passwd PASSWORD

Configuring Zope 2
==================

To integrate RelStorage in Zope 2, specify a RelStorage backend in
``etc/zope.conf``. Remove the main mount point and add one of the
following blocks. For PostgreSQL::

    %import relstorage
    <zodb_db main>
      mount-point /
      <relstorage>
        <postgresql>
          # The dsn is optional, as are each of the parameters in the dsn.
          dsn dbname='zodb' user='username' host='localhost' password='pass'
        </postgresql>
      </relstorage>
    </zodb_db>

For MySQL::

    %import relstorage
    <zodb_db main>
      mount-point /
      <relstorage>
        <mysql>
          # Most of the options provided by MySQLdb are available.
          # See component.xml.
          db zodb
        </mysql>
      </relstorage>
    </zodb_db>

For Oracle (10g XE in this example)::

    %import relstorage
    <zodb_db main>
      mount-point /
      <relstorage>
        <oracle>
          user username
          password pass
          dsn XE
        </oracle>
     </relstorage>
    </zodb_db>

To add ZODB blob support, provide a blob-dir option that specifies
where to store the blobs.  For example::

    %import relstorage
    <zodb_db main>
      mount-point /
      <relstorage>
        blob-dir ./blobs
        <postgresql>
          dsn dbname='zodb' user='username' host='localhost' password='pass'
        </postgresql>
      </relstorage>
    </zodb_db>

Configuring ``repoze.zodbconn``
===============================

To use RelStorage with ``repoze.zodbconn``, a package that makes ZODB
available to WSGI applications, create a configuration file with
contents similar to the following::

    %import relstorage
    <zodb main>
      <relstorage>
        <mysql>
          db zodb
        </mysql>
      </relstorage>
      cache-size 100000
    </zodb>

``repoze.zodbconn`` expects a ZODB URI.  Use a URI of the form
``zconfig://path/to/configuration#main``.
