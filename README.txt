
To make Zope store in RelStorage, patch ZODB/Connection.py using the
provided patch.  The patch is for Zope 2.10.5.  Add the following to
zope.conf, modifying the adapter and params lines to fit your needs.


For PostgreSQL, use this in etc/zope.conf:

%import relstorage
<zodb_db main>
  mount-point /
  <relstorage>
    <postgresql>
    </postgresql>
  </relstorage>
</zodb_db>


For Oracle, use this in etc/zope.conf:

%import relstorage
<zodb_db main>
  mount-point /
  <relstorage>
    <oracle>
      user johndoe
      password opensesame
      dsn XE
    </oracle>
  </relstorage>
</zodb_db>

