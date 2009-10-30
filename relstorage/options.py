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

class Options(object):
    """Options for configuring and tuning RelStorage.

    These parameters can be provided as keyword options in the RelStorage
    constructor.  For example:

        storage = RelStorage(adapter, pack_gc=True, pack_dry_run=True)

    Alternatively, the RelStorage constructor accepts an options
    parameter, which should be an Options instance.
    """
    def __init__(self, **kwoptions):
        self.name = None
        self.read_only = False
        self.blob_dir = None
        self.keep_history = True
        self.replica_conf = None
        self.replica_timeout = 600.0
        self.poll_interval = 0
        self.pack_gc = True
        self.pack_dry_run = False
        self.pack_batch_timeout = 5.0
        self.pack_duty_cycle = 0.5
        self.pack_max_delay = 20.0
        self.cache_servers = ()  # ['127.0.0.1:11211']
        self.cache_module_name = 'memcache'
        self.cache_prefix = ''
        self.cache_local_mb = 10
        self.cache_delta_size_limit = 10000
        self.commit_lock_timeout = 30
        self.commit_lock_id = 0

        for key, value in kwoptions.iteritems():
            if key in self.__dict__:
                setattr(self, key, value)
            else:
                raise TypeError("Unknown parameter: %s" % key)
