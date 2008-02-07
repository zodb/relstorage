
To make Zope store in RelStorage, first patch ZODB/Connection.py using the
provided patch.  The patch is for Zope 2.10.5.  Then modify etc/zope.conf.


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


For MySQL, use this in etc/zope.conf:

%import relstorage
<zodb_db main>
  <relstorage>
    <mysql>
      db zodb
    </mysql>
  </relstorage>
  mount-point /
</zodb_db>
