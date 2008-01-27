##############################################################################
#
# Copyright (c) 2008 Zope Corporation and Contributors.
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
"""Compare the speed of RelStorage with FileStorage + ZEO.

Splits into many processes to avoid contention over the global
interpreter lock.
"""

import cPickle
import logging
import os
import shutil
import signal
import sys
import tempfile
import time
import traceback

import transaction
from BTrees.IOBTree import IOBTree
from ZODB.DB import DB
from ZODB.Connection import Connection

from relstorage.relstorage import RelStorage
from relstorage.adapters.postgresql import PostgreSQLAdapter
from relstorage.adapters.oracle import OracleAdapter
from relstorage.tests.testoracle import getOracleParams

debug = False
txn_count = 10
object_counts = [10000]  # [1, 100, 10000]
concurrency_levels = range(1, 16, 2)
contenders = [
    ('ZEO + FileStorage', 'zeofs_test'),
    ('PostgreSQLAdapter', 'postgres_test'),
    ('OracleAdapter', 'oracle_test'),
    ]
repetitions = 3
max_attempts = 20


class ChildProcessError(Exception):
    """A child process failed"""


def run_in_child(wait, func, *args, **kw):
    pid = os.fork()
    if pid == 0:
        # child
        try:
            try:
                logging.basicConfig()
                if debug:
                    logging.getLogger().setLevel(logging.DEBUG)

                func(*args, **kw)
            except:
                traceback.print_exc()
                os._exit(1)
        finally:
            os._exit(0)
    elif wait:
        pid_again, code = os.waitpid(pid, 0)
        if code:
            raise ChildProcessError(
                "process running %r failed with exit code %d" % (func, code))
    return pid


class SpeedTest:

    def __init__(self, concurrency, objects_per_txn):
        self.concurrency = concurrency
        self.data_to_store = dict((n, 1) for n in range(objects_per_txn))

    def populate(self, make_storage):
        # initialize the database
        storage = make_storage()
        db = DB(storage)
        conn = db.open()
        root = conn.root()
        root['speedtest'] = t = IOBTree()
        for i in range(self.concurrency):
            t[i] = IOBTree()
        transaction.commit()
        conn.close()
        db.close()
        if debug:
            print >> sys.stderr, 'Populated storage.'

    def write_test(self, storage, n):
        db = DB(storage)
        start = time.time()
        for i in range(txn_count):
            conn = db.open()
            root = conn.root()
            myobj = root['speedtest'][n]
            myobj[i] = IOBTree(self.data_to_store)
            transaction.commit()
            conn.close()
        end = time.time()
        db.close()
        return end - start

    def read_test(self, storage, n):
        db = DB(storage)
        start = time.time()
        for i in range(txn_count):
            conn = db.open()
            root = conn.root()
            got = len(list(root['speedtest'][n][i]))
            expect = len(self.data_to_store)
            if got != expect:
                raise AssertionError
            conn.close()
        end = time.time()
        db.close()
        return end - start

    def run_tests(self, make_storage):
        """Run a write and read test.

        Returns the mean time per write transaction and
        the mean time per read transaction.
        """
        run_in_child(True, self.populate, make_storage)
        r = range(self.concurrency)

        def write(n):
            return self.write_test(make_storage(), n)
        def read(n):
            return self.read_test(make_storage(), n)

        write_times = distribute(write, r)
        read_times = distribute(read, r)
        count = float(self.concurrency * txn_count)
        return (sum(write_times) / count, sum(read_times) / count)

    def run_zeo_server(self, store_fn, sock_fn):
        from ZODB.FileStorage import FileStorage
        from ZEO.StorageServer import StorageServer

        fs = FileStorage(store_fn, create=True)
        ss = StorageServer(sock_fn, {'1': fs})

        import ThreadedAsync.LoopCallback
        ThreadedAsync.LoopCallback.loop()

    def start_zeo_server(self, store_fn, sock_fn):
        pid = run_in_child(False, self.run_zeo_server, store_fn, sock_fn)
        # parent
        if debug:
            sys.stderr.write('Waiting for ZEO server to start...')
        while not os.path.exists(sock_fn):
            if debug:
                sys.stderr.write('.')
                sys.stderr.flush()
            time.sleep(0.1)
        if debug:
            sys.stderr.write(' started.\n')
            sys.stderr.flush()
        return pid

    def zeofs_test(self):
        dir = tempfile.mkdtemp()
        try:
            store_fn = os.path.join(dir, 'storage')
            sock_fn = os.path.join(dir, 'sock')
            zeo_pid = self.start_zeo_server(store_fn, sock_fn)
            try:
                def make_storage():
                    from ZEO.ClientStorage import ClientStorage
                    return ClientStorage(sock_fn)
                return self.run_tests(make_storage)
            finally:
                os.kill(zeo_pid, signal.SIGTERM)
        finally:
            shutil.rmtree(dir)

    def postgres_test(self):
        adapter = PostgreSQLAdapter()
        adapter.zap()
        def make_storage():
            return RelStorage(adapter)
        return self.run_tests(make_storage)

    def oracle_test(self):
        user, password, dsn = getOracleParams()
        adapter = OracleAdapter(user, password, dsn)
        adapter.zap()
        def make_storage():
            return RelStorage(adapter)
        return self.run_tests(make_storage)


def distribute(func, param_iter):
    """Call a function in separate processes concurrently.

    param_iter is an iterator that provides the parameter for each
    function call.  The parameter is passed as the single argument.
    The results of calling the function are appended to a list, which
    is returned once all functions have returned.  If any function
    raises an error, the error is re-raised in the caller.
    """
    dir = tempfile.mkdtemp()
    try:
        waiting = set()  # set of child process IDs
        for param in param_iter:
            pid = os.fork()
            if pid == 0:
                # child
                try:
                    logging.basicConfig()
                    if debug:
                        logging.getLogger().setLevel(logging.DEBUG)

                    fn = os.path.join(dir, str(os.getpid()))
                    try:
                        res = 1, func(param)
                    except:
                        traceback.print_exc()
                        res = 0, sys.exc_info()[:2]
                    f = open(fn, 'wb')
                    try:
                        cPickle.dump(res, f)
                    finally:
                        f.close()
                finally:
                    os._exit(0)
            else:
                # parent
                waiting.add(pid)
        results = []
        try:
            while waiting:
                for pid in list(waiting):
                    pid_again, code = os.waitpid(pid, os.WNOHANG)
                    if not pid_again:
                        continue
                    waiting.remove(pid)
                    if code:
                        raise ChildProcessError(
                            "A process failed with exit code %d" % code)
                    else:
                        fn = os.path.join(dir, str(pid))
                        f = open(fn, 'rb')
                        try:
                            ok, value = cPickle.load(f)
                            if ok:
                                results.append(value)
                            else:
                                raise ChildProcessError(
                                    "a child process raised an error: "
                                    "%s: %s" % tuple(value))
                        finally:
                            f.close()
                time.sleep(0.1)
            return results
        finally:
            # kill the remaining processes
            for pid in waiting:
                try:
                    os.kill(pid, signal.SIGTERM)
                except OSError:
                    pass
    finally:
        shutil.rmtree(dir)


def main():

    # results: {(objects_per_txn, concurrency, contender, direction): [time]}}
    results = {}
    for objects_per_txn in object_counts:
        for concurrency in concurrency_levels:
            for contender_name, method_name in contenders:
                for direction in (0, 1):
                    key = (objects_per_txn, concurrency,
                            contender_name, direction)
                    results[key] = []

    try:
        for objects_per_txn in object_counts:
            for concurrency in concurrency_levels:
                test = SpeedTest(concurrency, objects_per_txn)
                for contender_name, method_name in contenders:
                    print >> sys.stderr, (
                        'Testing %s with objects_per_txn=%d and concurrency=%d'
                        % (contender_name, objects_per_txn, concurrency))
                    method = getattr(test, method_name)
                    key = (objects_per_txn, concurrency, contender_name)

                    for rep in range(repetitions):
                        for attempt in range(max_attempts):
                            msg = '  Running %d/%d...' % (rep + 1, repetitions)
                            if attempt > 0:
                                msg += ' (attempt %d)' % (attempt + 1)
                            print >> sys.stderr, msg,
                            try:
                                w, r = method()
                            except ChildProcessError:
                                if attempt >= max_attempts - 1:
                                    raise
                            else:
                                break
                        msg = 'write %5.3fs, read %5.3fs' % (w, r)
                        print >> sys.stderr, msg
                        results[key + (0,)].append(w)
                        results[key + (1,)].append(r)

    # The finally clause causes test results to print even if the tests
    # stop early.
    finally:
        # show the results in CSV format
        print >> sys.stderr, (
            'Average time per transaction in seconds.  Best of 3.')

        for objects_per_txn in object_counts:
            print '** Results with objects_per_txn=%d **' % objects_per_txn

            line = ['"Concurrency"']
            for contender_name, func in contenders:
                for direction in (0, 1):
                    dir_text = ['write', 'read'][direction]
                    line.append('%s - %s' % (contender_name, dir_text))
            print ', '.join(line)

            for concurrency in concurrency_levels:
                line = [str(concurrency)]

                for contender_name, method_name in contenders:
                    for direction in (0, 1):
                        key = (objects_per_txn, concurrency,
                            contender_name, direction)
                        lst = results[key]
                        if lst:
                            t = min(lst)
                            line.append('%5.3f' % t)
                        else:
                            line.append('?')

                print ', '.join(line)
            print


if __name__ == '__main__':
    main()
