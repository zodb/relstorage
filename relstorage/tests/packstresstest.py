
import logging

from ZODB.DB import DB
from relstorage.relstorage import RelStorage
import transaction
from persistent.mapping import PersistentMapping
import random

logging.basicConfig()
logging.getLogger().setLevel(logging.DEBUG)

if 1:
    from relstorage.adapters.mysql import MySQLAdapter
    a = MySQLAdapter(db='packtest')
else:
    from relstorage.adapters.postgresql import PostgreSQLAdapter
    a = PostgreSQLAdapter(dsn="dbname='packtest'")

s = RelStorage(a)
d = DB(s)
c = d.open()

print 'size:'
print d.getSize()

if 1:
    print 'initializing...'
    container = PersistentMapping()
    c.root()['container'] = container
    container_size = 10000
    for i in range(container_size):
        container[i] = PersistentMapping()
    transaction.commit()

    print 'generating transactions...'
    for trans in range(10000):
        print trans
        sources = (random.randint(0, container_size - 1) for j in range(100))
        for source in sources:
            obj = container[source]
            obj[trans] = container[random.randint(0, container_size - 1)]
        transaction.commit()

    print 'size:'
    print d.getSize()

print 'packing...'
d.pack()

print 'size:'
print d.getSize()

d.close()
