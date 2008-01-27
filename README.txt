
To make Zope store in RelStorage, patch ZODB/Connection.py using the
provided patch, then add the following to zope.conf, modifying the
adapter and params lines to fit your needs.


%import relstorage
<zodb_db main>
  mount-point /
  <relstorage>
    adapter postgresql
    params host=localhost dbname=zodb user=zope password=...
  </relstorage>
</zodb_db>

