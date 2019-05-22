import os
import platform
import unittest

from relstorage._compat import ABC

# ZODB >= 3.9.  The blob directory can be a private cache.
shared_blob_dir_choices = (False, True)
support_blob_cache = True

RUNNING_ON_TRAVIS = os.environ.get('TRAVIS')
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


CACHE_SERVERS = None
CACHE_MODULE_NAME = None

if RUNNING_ON_TRAVIS:
    # We expect to have access to a local memcache server
    # on travis. Use it if we can import drivers.
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
DEFAULT_DATABASE_SERVER_HOST = os.environ.get('RS_DB_HOST',
                                              '127.0.0.1')


TEST_UNAVAILABLE_DRIVERS = not bool(os.environ.get('RS_SKIP_UNAVAILABLE_DRIVERS'))
if RUNNING_ON_CI:
    TEST_UNAVAILABLE_DRIVERS = False


class MinimalTestLayer(object):

    __bases__ = ()
    __module__ = ''

    def __init__(self, name):
        self.__name__ = name

    def setUp(self):
        pass

    def tearDown(self):
        pass

    def testSetUp(self):
        pass

    def testTearDown(self):
        pass


class AbstractTestSuiteBuilder(ABC):

    __name__ = None # PostgreSQL, MySQL, Oracle

    def __init__(self, driver_options, use_adapter):
        """
        :param driver_options: The ``IDBDriverOptions``
        :param use_adapter: A mixin class implementing the abstract methods
            defined by ``StorageCreatingMixin``.
        """

        self.drivers = driver_options
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
        from relstorage.adapters.interfaces import DriverNotAvailableError
        from .reltestbase import AbstractIDBDriverTest
        from .reltestbase import AbstractIDBOptionsTest
        suite = unittest.TestSuite()
        suite.addTest(unittest.makeSuite(type(
            self.__name__ + 'DBOptionsTest',
            (AbstractIDBOptionsTest,),
            {'db_options': self.drivers}
        )))
        for factory in self.drivers.known_driver_factories():
            driver_name = factory.driver_name
            try:
                driver = self.drivers.select_driver(driver_name)
            except DriverNotAvailableError:
                driver = None

            available = driver is not None

            # On CI, we don't even add tests for unavailable drivers to the
            # list of tests; this makes the output much shorter and easier to read,
            # but it does make zope-testrunner's discovery options less useful.
            if available or TEST_UNAVAILABLE_DRIVERS:
                # Checking the driver is just a unit test, it doesn't connect or
                # need a layer
                suite.addTest(unittest.makeSuite(
                    self.__skipping_if_not_available(
                        type(
                            self.__name__ + 'DBDriverTest_' + driver_name,
                            (AbstractIDBDriverTest,),
                            {'driver': driver}
                        ),
                        driver_name,
                        available)))

                # We put the various drivers into a zope.testrunner layer
                # for ease of selection by name, e.g.,
                # zope-testrunner --layer PG8000Driver
                driver_suite = unittest.TestSuite()
                layer_name = '%s%s' % (
                    self.__name__,
                    self.__escape_driver_name(driver_name),
                )
                driver_suite.layer = MinimalTestLayer(layer_name)
                driver_suite.layer.__module__ = self.__module__
                self._add_driver_to_suite(driver_name, driver_suite, layer_name, available)
                suite.addTest(driver_suite)
        return suite

    def __escape_driver_name(self, driver_name):
        return driver_name.replace(' ', '').replace('/', '_')

    def _default_make_check_class(self, base, name):
        klass = type(
            name,
            (self.use_adapter, base, ),
            {}
        )

        return klass

    def _make_check_classes(self):
        # The classes that inherit from ZODB tests and use 'check' instead of 'test_'
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
                name = self.__name__ + base.__name__
                maker = getattr(self, '_make_check_class_' + base.__name__,
                                self._default_make_check_class)
                klass = maker(base, name)
                klass.__module__ = self.__module__
                klass.__name__ = name
                classes.append(klass)
        return classes

    def _make_zodbconvert_classes(self):
        from .reltestbase import AbstractRSDestZodbConvertTests
        from .reltestbase import AbstractRSSrcZodbConvertTests

        classes = []
        for base in (AbstractRSSrcZodbConvertTests, AbstractRSDestZodbConvertTests):
            klass = type(
                self.__name__ + base.__name__[8:],
                (self.use_adapter, base),
                {}
            )
            klass.__module__ = self.__module__
            classes.append(klass)
        return classes

    def __skipping_if_not_available(self, klass, driver_name, is_available):
        klass.__module__ = self.__module__
        klass = unittest.skipUnless(
            is_available,
            "Driver %s is not installed" % (driver_name,))(klass)
        return klass

    def _new_class_for_driver(self, driver_name, base, is_available):
        klass = type(
            base.__name__ + '_' + self.__escape_driver_name(driver_name),
            (base,),
            {'driver_name': driver_name}
        )
        return self.__skipping_if_not_available(klass, driver_name, is_available)

    def _add_driver_to_suite(self, driver_name, suite, layer_prefix, is_available):
        for klass in self._make_check_classes():
            klass = self._new_class_for_driver(driver_name, klass, is_available)
            suite.addTest(unittest.makeSuite(klass, "check"))

        for klass in self._make_zodbconvert_classes():
            suite.addTest(unittest.makeSuite(
                self._new_class_for_driver(driver_name,
                                           klass,
                                           is_available)))

        from relstorage.tests.blob.testblob import storage_reusable_suite
        from relstorage.options import Options
        from relstorage.storage import RelStorage

        for shared_blob_dir in shared_blob_dir_choices:
            for keep_history in (False, True):
                def create_storage(name, blob_dir,
                                   shared_blob_dir=shared_blob_dir,
                                   keep_history=keep_history, **kw):
                    if not is_available:
                        raise unittest.SkipTest("Driver %s is not installed" % (driver_name,))
                    assert driver_name not in kw
                    kw['driver'] = driver_name
                    db = self.db_names[name]
                    if not keep_history:
                        db += '_hf'

                    options = Options(
                        keep_history=keep_history,
                        shared_blob_dir=shared_blob_dir,
                        blob_dir=os.path.abspath(blob_dir),
                        **kw)

                    adapter_maker = self.use_adapter()
                    adapter = adapter_maker.make_adapter(options, db)
                    storage = RelStorage(adapter, name=name, options=options)
                    storage.zap_all(slow=True)
                    return storage

                prefix = '%s_%s%s' % (
                    layer_prefix,
                    'Shared' if shared_blob_dir else 'Unshared',
                    'WithHistory' if keep_history else 'NoHistory',
                )

                # If the blob directory is a cache, don't test packing,
                # since packing can not remove blobs from all caches.
                test_packing = shared_blob_dir

                if keep_history:
                    pack_test_name = 'blob_packing.txt'
                else:
                    pack_test_name = 'blob_packing_history_free.txt'

                suite.addTest(storage_reusable_suite(
                    prefix, create_storage,
                    test_blob_storage_recovery=True,
                    test_packing=test_packing,
                    test_undo=keep_history,
                    pack_test_name=pack_test_name,
                    test_blob_cache=(not shared_blob_dir),
                    # PostgreSQL blob chunks are max 2GB in size
                    large_blob_size=(not shared_blob_dir) and (self.large_blob_size) + 100,
                    storage_is_available=is_available
                ))

        return suite
