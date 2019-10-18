##############################################################################
#
# Copyright (c) 2016 Zope Foundation and Contributors.
# All Rights Reserved.
#
# This software is subject to the provisions of the Zope Public License,
# Version 2.1 (ZPL).  A copy of the ZPL should accompany this distribution.
# THIS SOFTWARE IS PROVIDED "AS IS" AND ANY AND ALL EXPRESS OR IMPLIED
# WARRANTIES ARE DISCLAIMED, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED
# WARRANTIES OF TITLE, MERCHANTABILITY, AGAINST INFRINGEMENT, AND FITNESS
# FOR A PARTICULAR PURPOSE.
#
##############################################################################
from __future__ import absolute_import
from __future__ import print_function

import os

import doctest
import re
import unittest
import shutil

import ZODB.tests.util
import zope.testing.renormalizing
import zope.testing.setupstack

import random2

from relstorage.cache.storage_cache import StorageCache

from relstorage.cache.tests import MockOptions, MockAdapter
from relstorage.storage.tpc.temporary_storage import TemporaryStorage

def _build_history():
    random = random2.Random(42)
    history = []
    serial = 2
    for i in range(1000):
        serial += 1
        oid = random.randint(i + 1000, i + 6000)
        history.append((b's', oid, serial,
                        b'x' * random.randint(200, 2000)))
        for _ in range(10):
            oid = random.randint(i + 1000, i + 6000)
            history.append((b'l', oid, serial,
                            b'x' * random.randint(200, 2000)))
    return history

def _send_queue(self, tid_int, temp_objects):
    self.cache.set_all_for_tid(tid_int, temp_objects)
    temp_objects.reset()

now = 0

def cache_run(name, size):
    history = _build_history()
    serial = 1
    #random.seed(42)
    global now
    now = 1278864701.5
    options = MockOptions()
    options.cache_local_mb = size * (1<<20)
    options.cache_local_dir = '.'
    options.cache_local_compression = 'zlib'
    # We can interleave between instances
    adapter = MockAdapter()
    poller = adapter.poller
    cache = StorageCache(adapter, options, name)
    new_cache = cache.new_instance()
    assert hasattr(cache.cache, 'tracer'), cache.cache
    assert hasattr(new_cache.cache, 'tracer'), new_cache.cache
    assert new_cache.cache.tracer is cache.cache.tracer
    assert new_cache.adapter is cache.adapter
    assert new_cache.adapter.poller is cache.adapter.poller

    all_oids = {row[1] for row in history}

    try:
        # Establish polling
        cache.poll(None, None, None)
        # poll again, having determined all of these oids have been created.
        poller.poll_tid += 1
        poller.poll_changes = [
            (oid, poller.poll_tid) for oid in sorted(all_oids)
        ]
        cache.poll(None, None, None)
        # Poll again for no changes
        poller.poll_changes = []
        new_cache.poll(None, None, None)

        new_cache_ts = TemporaryStorage()
        cache_ts = TemporaryStorage()
        data_tot = 0
        for action, oid, serial, data in history:
            data_tot += len(data)
            now += 1
            if action == b's':
                cache_ts.store_temp(oid, data)
                #print("Storing", oid, serial)
                _send_queue(cache, serial, cache_ts)
                cache.adapter.mover.data[oid] = (data, serial)
                poller.poll_tid = serial
                poller.poll_changes = [(oid, serial)]
                cache.poll(None, None, None)
                poller.poll_changes = []
                new_cache.poll(None, None, None)
                assert cache.highest_visible_tid == serial
            else:
                v = new_cache.load(None, oid)
                if v[0] is None:
                    #print("Store after miss", oid, 1)
                    new_cache_ts.store_temp(oid, data)
                    _send_queue(new_cache, 1, new_cache_ts)
                    cache.adapter.mover.data[oid] = (data, 1)
    finally:
        cache.close()

def setUp(test):
    import time
    import ZEO.scripts.cache_stats
    import ZEO.scripts.cache_simul
    # setupstack doesn't ignore problems when files can't be
    # found
    # XXX: Restore this.
    zope.testing.setupstack.rmtree = lambda p: shutil.rmtree(p, True)

    zope.testing.setupstack.setUpDirectory(test)
    def ctime(t):
        return time.asctime(time.gmtime(t-3600*4))

    assert ZEO.scripts.cache_stats.ctime is time.ctime
    ZEO.scripts.cache_stats.ctime = ctime
    assert ZEO.scripts.cache_simul.ctime is time.ctime
    ZEO.scripts.cache_simul.ctime = ctime

    os.environ["ZEO_CACHE_TRACE"] = 'single'
    test.globs['timetime'] = time.time
    time.time = lambda: now

    #cache_run('cache', 2)

def tearDown(test):
    import time
    import ZEO.scripts.cache_stats
    import ZEO.scripts.cache_simul

    ZEO.scripts.cache_stats.ctime = time.ctime
    ZEO.scripts.cache_simul.ctime = time.ctime
    os.environ.pop('ZEO_CACHE_TRACE')

    time.time = test.globs['timetime']
    zope.testing.setupstack.tearDown(test)

def test_suite():
    suite = unittest.TestSuite()
    try:
        __import__('ZEO')
    except ImportError:
        class NoTest(unittest.TestCase):
            @unittest.skip("ZEO not installed")
            def test_cache_trace_analysis(self):
                "Does nothing"
        suite.addTest(unittest.makeSuite(NoTest))
    else:
        suite.addTest(
            doctest.DocFileSuite(
                'cache_trace_analysis.rst',
                setUp=setUp,
                tearDown=tearDown,
                checker=ZODB.tests.util.checker + \
                    zope.testing.renormalizing.RENormalizing([
                        (re.compile(r'31\.3%'), '31.2%'),
                    ]),
                )
            )

    return suite

if __name__ == '__main__':
    unittest.main(defaultTest='test_suite')
