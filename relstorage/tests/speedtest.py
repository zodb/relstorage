##############################################################################
#
# Copyright (c) 2008 Zope Foundation and Contributors.
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
import optparse
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

from relstorage.options import Options
from relstorage.storage import RelStorage

debug = False
txn_count = 10
all_contenders = [
    ('ZEO + FileStorage', 'zeofs_test'),
    ('PostgreSQLAdapter', 'postgres_test'),
    ('MySQLAdapter', 'mysql_test'),
    ('OracleAdapter', 'oracle_test'),
    ]
repetitions = 3
max_attempts = 20
keep_history = True


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


class ZEOServerRunner(object):

    def __init__(self, addr=None, external=False):
        self.dir = tempfile.mkdtemp()
        self.store_fn = os.path.join(self.dir, 'storage')
        if addr is None:
            addr = os.path.join(self.dir, 'sock')
        self.sock_fn = addr
        self.pid = None
        self.external = external

    def run(self):
        from ZODB.FileStorage import FileStorage
        from ZEO.StorageServer import StorageServer

        fs = FileStorage(self.store_fn, create=True)
        ss = StorageServer(self.sock_fn, {'1': fs})

        try:
            import ThreadedAsync.LoopCallback
            ThreadedAsync.LoopCallback.loop()
        except ImportError:
            import asyncore
            asyncore.loop()

    def start(self):
        if self.external:
            return
        self.pid = run_in_child(False, self.run)
        # parent
        if isinstance(self.sock_fn, tuple):
            time.sleep(1)
            return
        sys.stderr.write('Waiting for ZEO server to start...')
        while not os.path.exists(self.sock_fn):
            sys.stderr.write('.')
            sys.stderr.flush()
            time.sleep(0.1)
        sys.stderr.write(' started.\n')
        sys.stderr.flush()

    def stop(self):
        if self.external:
            return
        os.kill(self.pid, signal.SIGTERM)
        shutil.rmtree(self.dir)


class SpeedTest:

    def __init__(self, concurrency, objects_per_txn, zeo_runner):
        self.concurrency = concurrency
        self.data_to_store = dict((n, 1) for n in range(objects_per_txn))
        self.zeo_runner = zeo_runner

    def populate(self, make_storage):
        # initialize the database
        storage = make_storage()
        db = DB(storage)
        conn = db.open()
        root = conn.root()

        # clear the database
        root['speedtest'] = None
        transaction.commit()
        db.pack()

        # put a tree in the database
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
        time.sleep(.1)
        db.close()
        return end - start

    def read_test(self, storage, n):
        db = DB(storage, cache_size=len(self.data_to_store)+400)
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
        cold = end-start
        conn = db.open()
        conn.cacheMinimize()
        conn.close()
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
        hot = end-start
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
        steamin = end-start
        db.close()
        return cold, hot, steamin

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
        cold_times = [t[0] for t in read_times]
        hot_times = [t[1] for t in read_times]
        steamin_times = [t[2] for t in read_times]
        count = float(self.concurrency * txn_count)
        return (sum(write_times) / count,
                sum(cold_times) / count,
                sum(hot_times) / count,
                sum(steamin_times) / count,
                )

    def zeofs_test(self, _):
        def make_storage():
            from ZEO.ClientStorage import ClientStorage
            return ClientStorage(self.zeo_runner.sock_fn)
        return self.run_tests(make_storage)

    def postgres_test(self, opts):
        from relstorage.adapters.postgresql import PostgreSQLAdapter

        db = opts.db or 'relstoragetest'
        keep_history = bool(options.keep_history)
        if keep_history:
            db += '_hf'
        dsn = 'dbname=%s user=relstoragetest password=relstoragetest' % db
        options = Options(keep_history=keep_history)
        adapter = PostgreSQLAdapter(dsn=dsn)
        adapter.schema.prepare()
        adapter.schema.zap_all()
        def make_storage():
            return RelStorage(adapter)
        return self.run_tests(make_storage)

    def oracle_test(self, opts):
        from relstorage.adapters.oracle import OracleAdapter
        dsn = os.environ.get('ORACLE_TEST_DSN', 'XE')
        db = opts.db or 'relstoragetest'
        keep_history = bool(options.keep_history)
        if keep_history:
            db += '_hf'
        options = Options(keep_history=keep_history)
        adapter = OracleAdapter(
            user=db,
            password='relstoragetest',
            dsn=dsn,
            options=options,
            )
        adapter.schema.prepare()
        adapter.schema.zap_all()
        def make_storage():
            return RelStorage(adapter)
        return self.run_tests(make_storage)

    def mysql_test(self, opts):
        from relstorage.adapters.mysql import MySQLAdapter
        db = opts.db or 'relstoragetest'
        keep_history = not opts.no_keep_history
        if not keep_history:
            db += '_hf'
        options = Options(keep_history=keep_history)
        kw = {}
        if opts.mysql_port:
            kw['port'] = opts.mysql_port
        if opts.mysql_host:
            kw['host'] = opts.mysql_host
        adapter = MySQLAdapter(
            options=options,
            db=db,
            user=opts.mysql_user,
            passwd=opts.mysql_passwd,
            **kw)
        adapter.schema.prepare()
        adapter.schema.zap_all()
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


parser = optparse.OptionParser()
parser.add_option(
    "-Z", "--external-zeo", dest='external_zeo', action='store_true',
    help="Use an external ZEO server. Don't start one internally.",
    )
parser.add_option(
    "-z", "--zeo-address", dest='zeo_address',
    help="The address to use for the ZEO server."
    " The default is a unix-domain socket",
    )
parser.add_option(
    "-r", "--run", dest="run", action="append", type="choice",
    choices=['zeofs', 'postgres', 'mysql', 'oracle'],
    help=("Specify which tests to run. Default is just ZEO. "
          "Must be zeofs, postgres, mysql, or oracle. "
          "Can be used multiple times. "),
    )
parser.add_option(
    "-n", "--object-counts", dest="counts", default="1,100,10000",
    help="Object counts to use, separated by commas",
    )
parser.add_option(
    "-c", "--concurrency", dest="concurrency", default="1,2,4,8,16",
    help="Concurrency levels to use, separated by commas",
    )
parser.add_option(
    "-K", "--no-keep-history", dest="no_keep_history", action='store_true',
    help="Don't keep history",
    )
parser.add_option(
    "-d", "--db", dest="db", default="relstoragetest",
    help="Relational db name",
    )
parser.add_option(
    "--mysql-user", dest="mysql_user", default="relstoragetest",
    help="MySQL user, defaults to relstoragetest",
    )
parser.add_option(
    "--mysql-password", dest="mysql_passwd", default="relstoragetest",
    help="MySQL password, defaults to relstoragetest",
    )
parser.add_option(
    "--mysql-host", dest="mysql_host",
    help="MySQL host name",
    )
parser.add_option(
    "--mysql-port", dest="mysql_port", type='int',
    help="MySQL port",
    )




def main(args=None):
    if args is None:
        args = sys.argv[1:]

    options, args = parser.parse_args(args)

    assert not args

    object_counts = [int(x.strip())
                     for x in options.counts.split(',')]
    concurrency_levels = [int(x.strip())
                          for x in options.concurrency.split(',')]
    contenders = [(l, n) for (l, n) in all_contenders
                  if n[:-5] in (options.run or ['zeofs'])]

    addr = options.zeo_address
    if addr and ':' in addr:
        h, p = addr.split(':')
        addr = h, int(p)

    zeo_runner = ZEOServerRunner(addr, options.external_zeo)
    zeo_runner.start()

    # results: {(objects_per_txn, concurrency, contender, direction): [time]}}
    results = {}
    for objects_per_txn in object_counts:
        for concurrency in concurrency_levels:
            for contender_name, method_name in contenders:
                for direction in (0, 1, 2, 3):
                    key = (objects_per_txn, concurrency,
                            contender_name, direction)
                    results[key] = []

    try:
        for objects_per_txn in object_counts:
            for concurrency in concurrency_levels:
                test = SpeedTest(concurrency, objects_per_txn, zeo_runner)
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
                                w, c, h, s = method(options)
                            except ChildProcessError:
                                if attempt >= max_attempts - 1:
                                    raise
                            else:
                                break
                        msg = (
                            'write %6.4fs, cold %6.4fs'
                            ', hot %6.4fs, steamin %6.4fs'
                            % (w, c, h, s))
                        print >> sys.stderr, msg
                        results[key + (0,)].append(w)
                        results[key + (1,)].append(c)
                        results[key + (2,)].append(h)
                        results[key + (3,)].append(s)

    # The finally clause causes test results to print even if the tests
    # stop early.
    finally:
        zeo_runner.stop()

        # show the results in CSV format
        print >> sys.stderr, (
            'Average time per transaction in seconds.  Best of 3.')

        for objects_per_txn in object_counts:
            print '** Results with objects_per_txn=%d **' % objects_per_txn

            line = ['"Concurrency"']
            for contender_name, func in contenders:
                for direction in (0, 1, 2, 3):
                    dir_text = ['write', 'cold', 'hot', 'steamin'][direction]
                    line.append('%s - %s' % (contender_name, dir_text))
            print ', '.join(line)

            for concurrency in concurrency_levels:
                line = [str(concurrency)]

                for contender_name, method_name in contenders:
                    for direction in (0, 1, 2, 3):
                        key = (objects_per_txn, concurrency,
                            contender_name, direction)
                        lst = results[key]
                        if lst:
                            t = min(lst)
                            line.append('%6.4f' % t)
                        else:
                            line.append('?')

                print ', '.join(line)
            print


if __name__ == '__main__':
    main()
