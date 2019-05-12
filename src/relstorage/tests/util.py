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


class AbstractTestSuiteBuilder(ABC):

    __name__ = None # PostgreSQL, MySQL, Oracle

    def __init__(self, driver_options, use_adapter, cfg_mixin):
        """
        :param driver_options: The ``IDBDriverOptions``
        :param cfg_mixin: Optional; for zodbconvert tests.
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
        self.cfg_mixin = cfg_mixin
        self.large_blob_size = self._compute_large_blob_size(USE_SMALL_BLOBS)

    def _compute_large_blob_size(self, use_small_blobs):
        raise NotImplementedError

    def test_suite(self):
        from relstorage.adapters.interfaces import DriverNotAvailableError

        suite = unittest.TestSuite()
        for driver_name in self.drivers.known_driver_names():
            try:
                self.drivers.select_driver(driver_name)
            except DriverNotAvailableError:
                available = False
            else:
                available = True
            self._add_driver_to_suite(driver_name, suite, available)
        return suite

    def _default_make_check_class(self, base, name):
        klass = type(
            name,
            (self.use_adapter, base, ), # XXX: TODO: ZConfigTests need to go somewhere
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
        if self.cfg_mixin is None:
            return []

        from .reltestbase import AbstractRSDestZodbConvertTests
        from .reltestbase import AbstractRSSrcZodbConvertTests

        classes = []
        for base in (AbstractRSSrcZodbConvertTests, AbstractRSDestZodbConvertTests):
            klass = type(
                self.__name__ + base.__name__[8:],
                (self.use_adapter, self.cfg_mixin, base),
                {}
            )
            klass.__module__ = self.__module__
            classes.append(klass)
        return classes

    def _new_class_for_driver(self, driver_name, base, is_available):
        klass = type(
            base.__name__ + '_' + driver_name,
            (base,),
            {'driver_name': driver_name}
        )
        klass.__module__ = self.__module__
        klass = unittest.skipUnless(
            is_available,
            "Driver %s is not installed" % (driver_name,))(klass)
        return klass

    def _add_driver_to_suite(self, driver_name, suite, is_available):
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

                prefix = '%s%s%s_%s' % (
                    self.__name__,
                    'Shared' if shared_blob_dir else 'Unshared',
                    'WithHistory' if keep_history else 'NoHistory',
                    driver_name,
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
