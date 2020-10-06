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

from relstorage._compat import PYPY


class Options(object):
    """Options for configuring and tuning RelStorage.

    These parameters can be provided as keyword options in the :class:`.RelStorage`
    constructor.  For example:

        storage = RelStorage(adapter, pack_gc=True, pack_prepack_only=True)

    Alternatively, the RelStorage constructor accepts an options
    parameter, which should be an Options instance.

    .. versionchanged:: 3.0
       No longer accepts the ``poll_interval`` option.

    .. versionchanged:: 2.0b2
       The ``poll_interval`` option is now ignored and raises a warning. It is always
       effectively 0.

    .. versionchanged:: 2.0b7
       The ``cache_delta_size_limit`` grew to 20000 on CPython.

    .. versionchanged:: 3.0a7
       Add the ``blob_cache_size_check_external`` option, defaulting to False.

       Change the default for ``shared_blob_dir`` to False.

    .. versionchanged:: 3.3.3
       Since packing no longer holds the commit lock, ``pack_batch_timeout`` is the interval
       between committing deletions. Changed from 1.0s to 15s.
       Note that this is not universally applied.
    """

    #: The adapter factory configuration.
    adapter = None

    #: The name
    name = None
    #: Read only?
    read_only = False
    #: Preserve history?
    keep_history = True


    ###
    # Blob configuration
    ###

    #: directory to hold blobs
    blob_dir = None
    #: Is the blob directory shared?
    shared_blob_dir = False
    #: How much local blob data to keep
    blob_cache_size = None
    #: Percentage of ``blob_cache_size``; when any individual
    #: storage instance has written this many bytes of data into
    #: the blob cache, launch a cleanup process.
    blob_cache_size_check = 10
    #: Run the cache size check in an external process?
    blob_cache_size_check_external = False
    #: The size to break blobs into for storage.
    #: Only applies to some databases.
    blob_chunk_size = 1 << 20


    #: File containing replica info
    replica_conf = None
    #: File containing read-only replica info
    ro_replica_conf = None
    #: ?
    replica_timeout = 600.0
    #: Specifies what to do when a database connection is stale.
    revert_when_stale = False
    #: Perform a GC when packing
    pack_gc = True
    #: Only prepack
    pack_prepack_only = False
    #: Skip prepack
    pack_skip_prepack = False
    #: Amount of time between commits/log messages.
    pack_batch_timeout = 15.0

    #: List of memcache servers
    cache_servers = ()  # ['127.0.0.1:11211']
    #: Module to wrap a memcache connection with.
    cache_module_name = 'relstorage.pylibmc_wrapper'
    #: Database-specific prefix key
    cache_prefix = ''
    #: How much memory to use for the pickle cache
    cache_local_mb = 10
    #: The largest pickle to hold in the pickle cache
    cache_local_object_max = 16384
    #: How to compress local pickles
    cache_local_compression = 'none'
    #: Directory holding persistent cache files
    cache_local_dir = None
    #: Switch checkpoints after this many writes
    cache_delta_size_limit = 100000 if not PYPY else 50000

    #: How long to wait for a commit lock, in seconds.
    commit_lock_timeout = 30
    #: Lock ID for Oracle
    commit_lock_id = 0

    #: Automatically create the schema if needed
    create_schema = True
    #: Which database driver to use
    driver = 'auto'

    # Deprecated things
    #: How many persistent cache files to keep
    cache_local_dir_count = None
    #: How many persistent cache files to read
    cache_local_dir_read_count = None
    #: How big a cache file can be
    cache_local_dir_write_max_size = None
    #: Compress the cache files?
    cache_local_dir_compress = None
    #: How long to pause if we can't get the lock.
    #:
    #: ..versionchanged:: 3.0a5
    #:    Deprecated and unused.
    pack_commit_busy_delay = 5.0

    # We'll emit a warning for each option seen in here.
    _deprecated_options = (
        'cache_local_dir_count',
        'cache_local_dir_read_count',
        'cache_local_dir_write_max_size',
        'cache_local_dir_compress',
        'pack_commit_busy_delay',
    )


    def __init__(self, **kwoptions):
        for key, value in kwoptions.items():
            if not hasattr(self, key):
                raise TypeError("Unknown parameter: %s (Known: %s)" % (
                    key,
                    self.valid_option_names()
                ))
            if key in self._deprecated_options:
                import warnings
                warnings.warn(
                    "The option %s is deprecated and ignored." % (key,),
                    FutureWarning
                )

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
        return sorted(
            x
            for x in vars(cls)
            if not callable(getattr(cls, x)) and not x.startswith('_')
        )

    def __repr__(self):
        opts = []
        for k, v in sorted(self.__dict__.items()):
            opt = '%s=%r' % (k, v)
            opts.append(opt)
        opts = ', '.join(opts)
        return 'relstorage.options.Options(%s)' % (opts,)

    def __eq__(self, other):
        if not isinstance(other, Options):
            return NotImplemented
        return all(getattr(self, key) == getattr(other, key)
                   for key in self.valid_option_names())

    def copy(self, **kw):
        """
        Produce a copy of these options, with keyword arguments overriding.
        """
        options = dict(self.__dict__)
        options.update(kw)
        return self.__class__(**options)

    new_instance = copy
