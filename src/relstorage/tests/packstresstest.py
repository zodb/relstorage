from __future__ import print_function

import logging
import os
import random

import transaction
from persistent.mapping import PersistentMapping
from ZODB.DB import DB

from relstorage.options import Options
from relstorage.storage import RelStorage

logging.basicConfig()
logging.getLogger().setLevel(logging.DEBUG)

use = 'oracle'
keep_history = True

if use == 'mysql':
    from relstorage.adapters.mysql import MySQLAdapter
    a = MySQLAdapter(db='packtest',
                     user='relstoragetest',
                     passwd='relstoragetest',
                     options=Options(keep_history=keep_history),)
elif use == 'postgresql':
    from relstorage.adapters.postgresql import PostgreSQLAdapter
    a = PostgreSQLAdapter(dsn="dbname='packtest' "
                          'user=relstoragetest '
                          'password=relstoragetest',
                          options=Options(keep_history=keep_history),)
elif use == 'oracle':
    from relstorage.adapters.oracle import OracleAdapter
    dsn = os.environ.get('ORACLE_TEST_DSN', 'XE')
    a = OracleAdapter(user='packtest',
                      password='relstoragetest',
                      dsn=dsn,
                      options=Options(keep_history=keep_history),)
else:
    raise AssertionError("which database?")

s = RelStorage(a)
d = DB(s)
c = d.open()

print('size:')
print(d.getSize())


print('initializing...')
container = PersistentMapping()
c.root()['container'] = container
container_size = 10000
for i in range(container_size):
    container[i] = PersistentMapping()
transaction.commit()

print('generating transactions...')
for trans in range(100):
    print(trans)
    sources = (random.randint(0, container_size - 1) for j in range(100))
    for source in sources:
        obj = container[source]
        obj[trans] = container[random.randint(0, container_size - 1)]
    transaction.commit()

print('size:')
print(d.getSize())

print('packing...')
d.pack()

print('size:')
print(d.getSize())

d.close()
