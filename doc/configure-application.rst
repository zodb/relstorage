==============================
 Configuring Your Application
==============================

Once RelStorage is installed together with the appropriate database
adapter, and you have created a user and database to use with
RelStorage, it's time to configure the application to use RelStorage.
This means telling RelStorage about the database to use, how to
connect to it, and specifying any additional options.

.. note:: This is just a quick-start guide. There is a :doc:`full list
          of supported options <relstorage-options>` in a separate
          document, as well as a :doc:`list of database-specific
          options <db-specific-options>`.

.. highlight:: guess

Configuring Plone
=================

Plone uses the `plone.recipe.zope2instance`_ Buildout recipe to
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

You'll also need to make sure that the correct version of RelStorage
and its database drivers are installed (typically by adding them to
the ``[eggs]`` section in the ``buildout.cfg``).

.. note:: For a detailed walk through of installing historic versions
         of RelStorage in historic versions of Plone 3, see `this blog
         post
         <http://shane.willowrise.com/archives/how-to-install-plone-with-relstorage-and-mysql/>`_.
         It's important to note that this information is not directly
         applicable to newer versions (Plone 4 does not use fake eggs,
         and the version of ZODB used by Plone 4, 3.9.5 and above,
         does not need patched). The comments section may contain
         further hints for newer versions.

.. _plone.recipe.zope2instance: https://pypi.python.org/pypi/plone.recipe.zope2instance

Configuring using ZConfig
=========================

The most common way to configure RelStorage will involve writing a
configuration file using the `ZConfig
<https://github.com/zopefoundation/ZConfig/blob/master/doc/zconfig.pdf>`_
syntax. You will write a ``<relstorage>`` element containing
the general RelStorage options, and containing one database-specific
element (``<postgresql>``, ``<mysql>`` or ``<oracle>``). (Where in the
file the ``<relstorage>`` element goes is specific to the framework
or application you're using and will be covered next.)

In all cases, you'll need to add ``%import relstorage`` to the
top-level of the file to let ZConfig know about the RelStorage
specific elements.


Examples for PostgreSQL::

      <relstorage>
        <postgresql>
          # The dsn is optional, as are each of the parameters in the dsn.
          dsn dbname='zodb' user='username' host='localhost' password='pass'
        </postgresql>
      </relstorage>

MySQL::

      <relstorage>
        <mysql>
          # Most of the options provided by MySQLdb are available.
          # See component.xml.
          db zodb
        </mysql>
      </relstorage>

And Oracle (10g XE in this example)::

      <relstorage>
        <oracle>
          user username
          password pass
          dsn XE
        </oracle>
     </relstorage>

To add ZODB blob support, provide a ``blob-dir`` option that specifies
where to store the blobs.  For example::

      <relstorage>
        blob-dir ./blobs
        <postgresql>
          dsn dbname='zodb' user='username' host='localhost' password='pass'
        </postgresql>
      </relstorage>


Configuring Zope 2
------------------

To integrate RelStorage in Zope 2, specify a RelStorage backend in
``etc/zope.conf``. Remove the main mount point replace it with the
``<relstorage>`` element. For example (using PostgreSQL)::

    %import relstorage
    <zodb_db main>
      mount-point /
      <relstorage>
        <postgresql>
          dsn dbname='zodb' user='username' host='localhost' password='pass'
        </postgresql>
      </relstorage>
    </zodb_db>


Configuring ``repoze.zodbconn``
-------------------------------

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
``zconfig://path/to/configuration#main`` where
``path/to/configuration`` is the complete path to the configuration
file, and ``main`` is the name given to the ``<zodb>`` element.

Manually Opening a Database
---------------------------

You can also manually open a ZODB database in Python code. Once you
have a configuration file as outlined above, you can use the
``ZODB.config.databaseFromURL`` API to get a ZODB database::

   path = "path/to/configuration"
   import ZODB.config
   db = ZODB.config.databaseFromURL(path)
   conn = db.open()
   ...
