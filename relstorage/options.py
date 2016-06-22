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

from __future__ import absolute_import
from relstorage._compat import iteritems

# Previously conditionally true, only true on ZODB >= 3.10, but we
# don't support older versions anymore.
default_strict_tpc = True


class Options(object):
    """Options for configuring and tuning RelStorage.

    These parameters can be provided as keyword options in the :class:`.RelStorage`
    constructor.  For example:

        storage = RelStorage(adapter, pack_gc=True, pack_prepack_only=True)

    Alternatively, the RelStorage constructor accepts an options
    parameter, which should be an Options instance.
    """
    def __init__(self, **kwoptions):
        self.name = None
        self.read_only = False
        self.blob_dir = None
        self.shared_blob_dir = True
        self.blob_cache_size = None
        self.blob_cache_size_check = 10
        self.blob_chunk_size = 1 << 20
        self.keep_history = True
        self.replica_conf = None
        self.ro_replica_conf = None
        self.replica_timeout = 600.0
        self.revert_when_stale = False
        self.poll_interval = 0
        self.pack_gc = True
        self.pack_prepack_only = False
        self.pack_skip_prepack = False
        self.pack_batch_timeout = 1.0
        self.pack_commit_busy_delay = 5.0
        self.cache_servers = ()  # ['127.0.0.1:11211']
        self.cache_module_name = 'relstorage.pylibmc_wrapper'
        self.cache_prefix = ''
        self.cache_local_mb = 10
        self.cache_local_object_max = 16384
        self.cache_local_compression = 'zlib'
        self.cache_delta_size_limit = 10000
        self.commit_lock_timeout = 30
        self.commit_lock_id = 0
        self.create_schema = True
        self.strict_tpc = default_strict_tpc

        # If share_local_cache is off, each storage instance has a private
        # cache rather than a shared cache.  This option exists mainly for
        # simulating disconnected caches in tests.
        self.share_local_cache = True

        for key, value in iteritems(kwoptions):
            if key in self.__dict__:
                setattr(self, key, value)
            else:
                raise TypeError("Unknown parameter: %s" % key)

    def __repr__(self):
        return 'relstorage.options.Options(**' + repr(self.__dict__) + ')'
