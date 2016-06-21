import os
import time
import unittest
from relstorage._compat import string_types

from ZEO.tests.forker import wait_until


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
