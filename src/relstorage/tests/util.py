import os
import platform
import unittest

# ZODB >= 3.9.  The blob directory can be a private cache.
shared_blob_dir_choices = (False, True)

# We define GitHub actions to be similar to travis
RUNNING_ON_GITHUB_ACTIONS = os.environ.get('GITHUB_ACTIONS')
RUNNING_ON_TRAVIS = os.environ.get('TRAVIS') or RUNNING_ON_GITHUB_ACTIONS
RUNNING_ON_APPVEYOR = os.environ.get('APPVEYOR')
RUNNING_ON_CI = RUNNING_ON_TRAVIS or RUNNING_ON_APPVEYOR

def _do_not_skip(reason): # pylint:disable=unused-argument
    def dec(f):
        return f
    return dec

if RUNNING_ON_CI:
    skipOnCI = unittest.skip
else:
    skipOnCI = _do_not_skip

if RUNNING_ON_APPVEYOR:
    skipOnAppveyor = unittest.skip
else:
    skipOnAppveyor = _do_not_skip
PYPY = platform.python_implementation() == 'PyPy'

CACHE_SERVERS = None
CACHE_MODULE_NAME = None

if RUNNING_ON_TRAVIS and not PYPY:
    # We expect to have access to a local memcache server
    # on travis. Use it if we can import drivers.
    #
    # This definitely leaks sockets on PyPy, and I don't think it's our fault.
    # pylint:disable=unused-import
    try:
        import pylibmc
        CACHE_SERVERS = ["localhost:11211"]
        CACHE_MODULE_NAME = 'relstorage.pylibmc_wrapper'
    except ImportError:
        try:
            import memcache
            CACHE_SERVERS = ["localhost:11211"]
            CACHE_MODULE_NAME = 'memcache'
        except ImportError:
            pass

USE_SMALL_BLOBS = ((RUNNING_ON_CI # slow here
                    or platform.system() == 'Darwin' # interactive testing
                    or os.environ.get("RS_SMALL_BLOB")) # define
                   and not os.environ.get('RS_LARGE_BLOB'))

# mysqlclient (aka MySQLdb) and possibly other things that
# use libmysqlclient.so will try to connect over the
# default Unix socket that was established when that
# library was compiled if no host is given. But that
# server may not be running, or may not be the one we want
# to use for testing, so explicitly ask it to use TCP
# socket by giving an IP address (using 'localhost' will
# still try to use the socket.) (The TCP port can be bound
# by non-root, but the default Unix socket often requires
# root permissions to open.)
STANDARD_DATABASE_SERVER_HOST = '127.0.0.1'
DEFAULT_DATABASE_SERVER_HOST = os.environ.get('RS_DB_HOST',
                                              STANDARD_DATABASE_SERVER_HOST)


TEST_UNAVAILABLE_DRIVERS = not bool(os.environ.get('RS_SKIP_UNAVAILABLE_DRIVERS'))
if RUNNING_ON_CI:
    TEST_UNAVAILABLE_DRIVERS = False


class MinimalTestLayer(object):

    __bases__ = ()
    __module__ = ''

    def __init__(self, name, module='', base=None):
        self.__name__ = name
        if module:
            self.__module__ = module
        if base:
            self.__bases__ = (base,)

    def setUp(self):
        pass

    def tearDown(self):
        pass

    def testSetUp(self):
        pass

    def testTearDown(self):
        pass


GeventLayer = MinimalTestLayer('gevent', __name__)

class _Availability(object):
    """
    Has a boolean value telling whether the driver or database is available,
    and a string explaining why it is/is not.

    Note that this includes checking whether we can connect to the database.
    """



    def __init__(self, factory, drivers, max_priority, use_adapter, db_name,
                 raised_exceptions=(TypeError, AttributeError)):
        from relstorage.adapters.interfaces import DriverNotAvailableError
        self.raised_exceptions = raised_exceptions
        self.driver_name = factory.driver_name
        self.escaped_driver_name = self.driver_name.replace(' ', '').replace('/', '_')

        try:
            self.driver = drivers.select_driver(self.driver_name)
        except DriverNotAvailableError:
            self.driver = None
        else:
            if self.driver.gevent_cooperative() and 'gevent' not in self.driver_name:
                # Pure-python implementations that can do gevent but
                # don't have to still need to have it in their name so we can get them
                # by layer.
                self.escaped_driver_name = 'gevent_' + self.escaped_driver_name

        self._available = self.driver is not None and self.driver.priority <= max_priority

        if not self._available:
            if self.driver is None:
                msg = 'Driver %s is not installed' % (self.driver_name,)
            else:
                msg = 'Driver %s has test priority %d >= max %d' % (
                    self.driver_name, self.driver.priority, max_priority
                )
        else:
            msg = 'Driver %s is installed' % (self.driver_name,)
        self._msg = msg

        if self.driver is not None:
            type(self.driver).STRICT = True

        if self._available:
            # See if we can connect.
            self.__check_db_access(use_adapter, db_name)


    def __str__(self):
        return self._msg

    def __bool__(self):
        return self._available

    __nonzero__ = __bool__

    def __check_db_access_cb(self, _conn, _cursor):
        "Does nothing"

    __check_db_access_cb.transaction_read_only = True

    def __check_db_access(self, use_adapter, db_name):
        # We need to get an adapter to get a connmanager to try to connect.
        from relstorage.options import Options
        options = Options(driver=self.driver_name)

        adapter_maker = use_adapter()
        adapter_maker.driver_name = self.driver_name
        adapter = adapter_maker.make_adapter(options, db_name)
        try:
            adapter.connmanager.open_and_call(self.__check_db_access_cb)
        except self.raised_exceptions:
            # We're called from test_suite(), and zope.testrunner
            # ignores errors at that time, so we need to print it ourself.
            import traceback; traceback.print_exc()
            raise
        except Exception as e:  # pylint:disable=broad-except
            self._available = False
            self._msg = "%s: Failed to connect: %r %s" % (self._msg, type(e), e)


class AbstractTestSuiteBuilder(object):

    __name__ = None # PostgreSQL, MySQL, Oracle
    # Drivers with a priority over this amount won't be part of the
    # test run even if installed.
    MAX_PRIORITY = int(os.environ.get('RS_MAX_TEST_PRIORITY', '100'))

    # Ask the drivers to be in their strictest possible mode.
    STRICT_DRIVER = True

    # The kind of exceptions that are raised, not ignored,
    # when trying to check access to the database. They indicate
    # programming problems.
    RAISED_EXCEPTIONS = (TypeError, AttributeError)

    def __init__(self, driver_options, use_adapter, extra_test_classes=()):
        """
        :param driver_options: The ``IDBDriverOptions``
        :param use_adapter: A mixin class implementing the abstract methods
            defined by ``StorageCreatingMixin``.
        """
        self.database_layer = MinimalTestLayer(self.__name__)
        self.gevent_layer = MinimalTestLayer('gevent_' + self.__name__,
                                             '',
                                             GeventLayer)
        self.gevent_layer.__bases__ += (self.database_layer,)
        self.drivers = driver_options
        self.extra_test_classes = extra_test_classes
        self.base_dbname = os.environ.get('RELSTORAGETEST_DBNAME', 'relstoragetest')
        self.db_names = {
            'data': self.base_dbname,
            '1': self.base_dbname,
            '2': self.base_dbname + '2',
            'dest': self.base_dbname + '2',
        }

        self.use_adapter = use_adapter
        use_adapter.base_dbname = self.base_dbname
        self.large_blob_size = self._compute_large_blob_size(USE_SMALL_BLOBS)

    def _compute_large_blob_size(self, use_small_blobs):
        raise NotImplementedError

    def test_suite(self):
        from .reltestbase import AbstractIDBDriverTest
        from .reltestbase import AbstractIDBOptionsTest

        suite = unittest.TestSuite()
        suite.addTest(unittest.makeSuite(type(
            self.__name__ + 'DBOptionsTest',
            (AbstractIDBOptionsTest,),
            {'db_options': self.drivers}
        )))
        for factory in self.drivers.known_driver_factories():
            available = _Availability(
                factory, self.drivers, self.MAX_PRIORITY,
                self.use_adapter,
                self.db_names['data'],
                self.RAISED_EXCEPTIONS
            )
            # Checking the driver is just a unit test, it doesn't connect or
            # need a layer
            suite.addTest(unittest.makeSuite(
                self.__skipping_if_not_available(
                    type(
                        self.__name__ + 'DBDriverTest_' + available.escaped_driver_name,
                        (AbstractIDBDriverTest,),
                        {'driver': available.driver}
                    ),
                    available.driver is not None)))

            # On CI, we don't even add tests for unavailable drivers to the
            # list of tests; this makes the output much shorter and easier to read,
            # but it does make zope-testrunner's discovery options less useful.
            if not available and not TEST_UNAVAILABLE_DRIVERS:
                continue

            # We put the various drivers into a zope.testrunner layer
            # for ease of selection by name, e.g.,
            # zope-testrunner --layer PG8000Driver
            driver_suite = unittest.TestSuite()
            layer_name = available.escaped_driver_name

            base = self.database_layer
            if available.driver and available.driver.gevent_cooperative():
                base = self.gevent_layer

            driver_suite.layer = MinimalTestLayer(layer_name,
                                                  base.__name__ + '.drivers',
                                                  base)
            self._add_driver_to_suite(driver_suite, layer_name, available)
            suite.addTest(driver_suite)
        return suite

    def _default_make_check_class(self, bases, name, klass_dict=None):
        base_klass_dict = self._make_base_klass_dict()
        if klass_dict:
            base_klass_dict.update(klass_dict)

        klass = type(
            name,
            (self.use_adapter,) + bases,
            base_klass_dict
        )

        return klass

    _default_make_test_class = _default_make_check_class

    def _make_base_klass_dict(self):
        return {}

    def __make_test_class(self, base, extra_bases, maker_base_name, maker_default, klass_dict=None):
        name = self.__name__ + base.__name__
        maker = getattr(
            self,
            maker_base_name + base.__name__,
            maker_default
        )
        __traceback_info__ = maker, base
        klass = maker((base,) + extra_bases, name, klass_dict)
        klass.__module__ = self.__module__
        klass.__name__ = name
        return klass

    def _make_check_classes(self):
        # The classes that inherit from ZODB tests and use 'check' instead of 'test_'

        # This class  is sadly not super() cooperative, so we must
        # try to explicitly put it last in the MRO.
        from ZODB.tests.util import TestCase as ZODBTestCase
        from .hftestbase import HistoryFreeFromFileStorage
        from .hftestbase import HistoryFreeToFileStorage
        from .hftestbase import HistoryFreeRelStorageTests

        from .hptestbase import HistoryPreservingFromFileStorage
        from .hptestbase import HistoryPreservingToFileStorage
        from .hptestbase import HistoryPreservingRelStorageTests

        classes = []

        for _, bases in (
                ('HF', (HistoryFreeFromFileStorage,
                        HistoryFreeToFileStorage,
                        HistoryFreeRelStorageTests)),
                ('HP', (HistoryPreservingFromFileStorage,
                        HistoryPreservingToFileStorage,
                        HistoryPreservingRelStorageTests))
        ):
            for base in bases:
                classes.append(self.__make_test_class(
                    base,
                    (ZODBTestCase,),
                    '_make_check_class_',
                    self._default_make_check_class,
                ))

        return classes

    def _make_test_classes(self):
        # Classes that inherit from RelStorageTestBase and not the ZODB
        # tests. These use the standard convention of 'test_' methods instead
        # of 'check' and don't have ridiculous class hierarchies.
        from .packundo import HistoryFreeTestPack
        from .packundo import HistoryPreservingTestPack

        classes = []


        for base in (
                HistoryFreeTestPack,
                HistoryPreservingTestPack,
        ):
            classes.append(self.__make_test_class(
                base,
                (),
                '_make_test_class_',
                self._default_make_test_class,
            ))

        return classes

    def _make_zodbconvert_classes(self):
        from .reltestbase import AbstractRSDestHPZodbConvertTests
        from .reltestbase import AbstractRSDestHFZodbConvertTests
        from .reltestbase import AbstractRSSrcZodbConvertTests

        classes = []
        for base in (AbstractRSSrcZodbConvertTests,
                     AbstractRSDestHPZodbConvertTests,
                     AbstractRSDestHFZodbConvertTests):
            klass = type(
                self.__name__ + base.__name__[8:],
                (self.use_adapter, base),
                {}
            )
            klass.__module__ = self.__module__
            classes.append(klass)
        return classes

    def __skipping_if_not_available(self, klass, availability):
        klass.__module__ = self.__module__
        klass = unittest.skipUnless(
            availability,
            str(availability))(klass)
        return klass

    def _new_class_for_driver(self, base, driver_available):
        klass = type(
            base.__name__ + '_' + driver_available.escaped_driver_name,
            (base,),
            {'driver_name': driver_available.driver_name}
        )

        return self.__skipping_if_not_available(klass, driver_available)

    def _add_driver_to_suite(self, suite, layer_prefix, driver_available):
        for klass in self._make_check_classes():
            klass = self._new_class_for_driver(klass, driver_available)
            suite.addTest(unittest.makeSuite(klass, "check"))

        for klass in self._make_test_classes():
            klass = self._new_class_for_driver(klass, driver_available)
            suite.addTest(unittest.makeSuite(klass))

        for klass in self._make_zodbconvert_classes():
            suite.addTest(unittest.makeSuite(
                self._new_class_for_driver(klass,
                                           driver_available)))

        for klass in self.extra_test_classes:
            suite.addTest(unittest.makeSuite(
                self._new_class_for_driver(klass,
                                           driver_available)))

        from relstorage.tests.blob.testblob import storage_reusable_suite
        from relstorage.options import Options
        from relstorage.storage import RelStorage

        for shared_blob_dir in shared_blob_dir_choices:
            for keep_history in (False, True):
                # TODO: Make any of the tests that are needing this
                # subclass StorageCreatingMixin so we unify where
                # that's handled.
                history_layer = MinimalTestLayer(
                    '%s%s' % (
                        'Shared' if shared_blob_dir else 'Unshared',
                        'HistoryPreserving' if keep_history else 'HistoryFree',
                    ),
                    suite.layer.__module__ + '.' + suite.layer.__name__ + '.blobs',
                    suite.layer
                )
                layer_name = history_layer.__name__
                def create_storage(name, blob_dir,
                                   shared_blob_dir=shared_blob_dir,
                                   keep_history=keep_history, **kw):
                    if not driver_available:
                        raise unittest.SkipTest(str(driver_available))
                    assert 'driver' not in kw
                    kw['driver'] = driver_available.driver_name
                    db = self.db_names[name]
                    if not keep_history:
                        db += '_hf'

                    options = Options(
                        keep_history=keep_history,
                        shared_blob_dir=shared_blob_dir,
                        blob_dir=os.path.abspath(blob_dir),
                        **kw)

                    adapter_maker = self.use_adapter()
                    adapter_maker.driver_name = driver_available.driver_name
                    adapter = adapter_maker.make_adapter(options, db)
                    __traceback_info__ = adapter, options
                    storage = RelStorage(adapter, name=name, options=options)
                    storage.zap_all()
                    return storage

                prefix = '%s_%s' % (
                    layer_prefix,
                    layer_name
                )

                # If the blob directory is a cache, don't test packing,
                # since packing can not remove blobs from all caches.
                test_packing = shared_blob_dir

                blob_suite = storage_reusable_suite(
                    prefix, create_storage,
                    keep_history=keep_history,
                    test_blob_storage_recovery=True,
                    test_packing=test_packing,
                    test_undo=keep_history,
                    test_blob_cache=(not shared_blob_dir),
                    # PostgreSQL blob chunks are max 2GB in size
                    large_blob_size=(not shared_blob_dir) and (self.large_blob_size) + 100,
                    storage_is_available=driver_available
                )

                blob_suite.layer.__bases__ = blob_suite.layer.__bases__ + (history_layer,)
                blob_suite.layer.__module__ = "%s.%s" % (
                    history_layer.__module__,
                    history_layer.__name__)
                suite.addTest(blob_suite)

        return suite
