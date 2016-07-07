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

import warnings

class Options(object):
    """Options for configuring and tuning RelStorage.

    These parameters can be provided as keyword options in the :class:`.RelStorage`
    constructor.  For example:

        storage = RelStorage(adapter, pack_gc=True, pack_prepack_only=True)

    Alternatively, the RelStorage constructor accepts an options
    parameter, which should be an Options instance.

    .. versionchanged:: 2.0b2
       The `poll_interval` option is now ignored and raises a warning. It is always
       effectively 0.
    """

    name = None
    read_only = False
    blob_dir = None
    shared_blob_dir = True
    blob_cache_size = None
    blob_cache_size_check = 10
    blob_chunk_size = 1 << 20
    keep_history = True
    replica_conf = None
    ro_replica_conf = None
    replica_timeout = 600.0
    revert_when_stale = False
    pack_gc = True
    pack_prepack_only = False
    pack_skip_prepack = False
    pack_batch_timeout = 1.0
    pack_commit_busy_delay = 5.0
    cache_servers = ()  # ['127.0.0.1:11211']
    cache_module_name = 'relstorage.pylibmc_wrapper'
    cache_prefix = ''
    cache_local_mb = 10
    cache_local_object_max = 16384
    cache_local_compression = 'zlib'
    cache_local_dir = None
    cache_local_dir_count = 20
    cache_delta_size_limit = 10000
    commit_lock_timeout = 30
    commit_lock_id = 0
    create_schema = True
    driver = 'auto'

    # If share_local_cache is off, each storage instance has a private
    # cache rather than a shared cache.  This option exists mainly for
    # simulating disconnected caches in tests.
    share_local_cache = True

    def __init__(self, **kwoptions):
        poll_interval = kwoptions.pop('poll_interval', self)
        if poll_interval is not self:
            # Would like to use a DeprecationWarning, but they're ignored
            # by default.
            warnings.warn("poll_interval is ignored", stacklevel=3)

        for key, value in kwoptions.items():
            if not hasattr(self, key):
                raise TypeError("Unknown parameter: %s" % key)
            setattr(self, key, value)

    @classmethod
    def copy_valid_options(cls, other_options):
        """
        Produce a new options featuring only the valid settings from
        *other_options*.
        """
        option_dict = {}
        for key in cls.valid_option_names():
            value = getattr(other_options, key, None)
            if value is not None:
                option_dict[key] = value
        return cls(**option_dict)

    @classmethod
    def valid_option_names(cls):
        # Still include poll_interval so we can warn
        return ['poll_interval'] + [x for x in vars(cls)
                                    if not callable(getattr(cls, x)) and not x.startswith('_')]

    def __repr__(self):
        return 'relstorage.options.Options(**' + repr(self.__dict__) + ')'
