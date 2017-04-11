import os
import platform
import unittest
import pkg_resources

# ZODB >= 3.9.  The blob directory can be a private cache.
shared_blob_dir_choices = (False, True)
support_blob_cache = True

RUNNING_ON_TRAVIS = os.environ.get('TRAVIS')
RUNNING_ON_APPVEYOR = os.environ.get('APPVEYOR')
RUNNING_ON_CI = RUNNING_ON_TRAVIS or RUNNING_ON_APPVEYOR

# pylint:disable=no-member
zodb_dist = pkg_resources.get_distribution('ZODB')
zodb_dist_version = pkg_resources.parse_version(zodb_dist.version)
RUNNING_ON_ZODB4 = zodb_dist.version[0] == '4'

# Unfortunately there's no way to detect this short of running the
# code and getting the runtime warning. So we do version detection
# instead.
SUPPORTS_BLOB_PERMS = zodb_dist_version < pkg_resources.parse_version('5.2.2')

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

if RUNNING_ON_ZODB4:
    skipOnZODB4 = unittest.skip
else:
    skipOnZODB4 = _do_not_skip


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
