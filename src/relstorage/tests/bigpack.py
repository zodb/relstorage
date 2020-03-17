import logging
import sys
import time

import transaction
from BTrees.OOBTree import OOBTree  # pylint:disable=no-name-in-module,import-error
from ZODB.DB import DB

from relstorage.adapters.postgresql import PostgreSQLAdapter
from relstorage.storage import RelStorage

log = logging.getLogger('bigpack')


def fill_db(db):
    conn = db.open()
    root = conn.root()
    root['tree'] = tree = OOBTree()
    for j in range(1, 101):
        log.info("Filling %d/100", j)
        for i in range(0, 100000, j):
            tree[i] = []
        transaction.commit()


def main():
    logging.basicConfig(
        stream=sys.stderr,
        level=logging.DEBUG,
        format='%(asctime)s [%(name)s] %(levelname)s %(message)s')
    log.info("Opening")
    adapter = PostgreSQLAdapter()
    storage = RelStorage(adapter)
    db = DB(storage)
    log.info("Filling")
    fill_db(db)
    log.info("Packing")
    start = time.time()
    db.pack()
    end = time.time()
    log.info("Packed in %0.3f seconds", end - start)


if __name__ == '__main__':
    main()
