import logging
import sys
format = '%(asctime)s [%(name)s] %(levelname)s %(message)s'
logging.basicConfig(stream=sys.stdout, level=logging.DEBUG, format=format)
import transaction
from relstorage.storage import RelStorage
from relstorage.options import Options
from relstorage.adapters.mysql import MySQLAdapter
from ZODB.DB import DB
options = Options()
adapter = MySQLAdapter(db='shane', options=options)
storage = RelStorage(adapter, options=options)
db = DB(storage)
conn = db.open()
root = conn.root()
root['x'] = root.get('x', 0) + 1
transaction.commit()
conn.close()
db.pack()
