import os
import time
import unittest

def wait_until(label=None, func=None, timeout=30, onfail=None):
    """Copied from ZEO.tests.forker, because it does not exist in ZODB 3.8"""
    if label is None:
        if func is not None:
            label = func.__name__
    elif not isinstance(label, basestring) and func is None:
        func = label
        label = func.__name__

    if func is None:
        def wait_decorator(f):
            wait_until(label, f, timeout, onfail)

        return wait_decorator

    giveup = time.time() + timeout
    while not func():
        if time.time() > giveup:
            if onfail is None:
                raise AssertionError("Timed out waiting for: ", label)
            else:
                return onfail()
        time.sleep(0.01)


try:
    from ZEO.ClientStorage import BlobCacheLayout
except ImportError:
    # ZODB 3.8.  The blob directory must be shared.
    shared_blob_dir_choices = (True,)
    support_blob_cache = False
else:
    # ZODB >= 3.9.  The blob directory can be a private cache.
    shared_blob_dir_choices = (False, True)
    support_blob_cache = True

RUNNING_ON_TRAVIS = os.environ.get('TRAVIS')
RUNNING_ON_APPVEYOR = os.environ.get('APPVEYOR')
RUNNING_ON_CI = RUNNING_ON_TRAVIS or RUNNING_ON_APPVEYOR

if RUNNING_ON_CI:
    skipOnCI = unittest.skip
else:
    def skipOnCI(reason):
        def dec(f):
            return f
        return dec


CACHE_SERVERS = None
CACHE_MODULE_NAME = None

if RUNNING_ON_TRAVIS:
    # We expect to have access to a local memcache server
    # on travis. Use it if we can import drivers.
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
