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
"""ZConfig directive implementations for binding RelStorage to Zope"""
from __future__ import absolute_import

from relstorage.options import Options
from relstorage.storage import RelStorage

logger = __import__('logging').getLogger(__name__)

class BaseConfig(object):

    def __init__(self, config):
        self.config = config
        self.name = config.getSectionName()

class RelStorageFactory(BaseConfig):
    """Open a storage configured via ZConfig"""
    def open(self):
        config = self.config
        # Hoist the driver setting to the section we really want it.
        config.driver = config.adapter.config.driver
        # But don't remove it or otherwise mutate the config object;
        # that would prevent us from being correctly opened again.
        #config.adapter.config.driver = None
        options = Options.copy_valid_options(config)
        options.adapter = config.adapter
        # The adapter factories may modify the global options (or raise an exception)
        # if something at the top-level is specifically not allowed based on
        # their configuration.
        adapter = config.adapter.create(options)
        return RelStorage(adapter, name=config.name, options=options)


class PostgreSQLAdapterFactory(BaseConfig):
    def create(self, options):
        from .adapters.postgresql import PostgreSQLAdapter
        return PostgreSQLAdapter(
            dsn=self.config.dsn,
            options=options)


class OracleAdapterFactory(BaseConfig):
    def create(self, options):
        from .adapters.oracle import OracleAdapter
        config = self.config
        return OracleAdapter(
            user=config.user,
            password=config.password,
            dsn=config.dsn,
            options=options)


class MySQLAdapterFactory(BaseConfig):
    def create(self, options):
        from .adapters.mysql import MySQLAdapter
        params = {}
        for key in self.config.getSectionAttributes():
            if key == 'driver':
                continue
            value = getattr(self.config, key)
            if value is not None:
                params[key] = value
        return MySQLAdapter(options=options, **params)


class SQLitePragmas(BaseConfig):

    @property
    def pragmas(self):
        pragmas = self.config.pragmas
        # The keys could be repeated so the values are lists
        pragmas = {k: v for k, (v,) in pragmas.items()}
        # Ignore busy_timeout: That's the same thing as commit-lock-timeout
        pragmas.pop('busy_timeout', None)
        for attr in self.config.getSectionAttributes():
            if attr == 'pragmas':
                continue
            val = getattr(self.config, attr)
            if val is not None:
                pragmas[attr] = val
        return pragmas

class Sqlite3AdapterFactory(BaseConfig):
    def create(self, options):
        from .adapters.sqlite.adapter import Sqlite3Adapter
        if options.cache_local_dir:
            # A persistent cache makes absolutely no sense.
            # Disable.
            logger.info("Ignoring cache-local-dir setting.")
            del options.cache_local_dir
        if self.config.pragmas:
            pragmas = self.config.pragmas.pragmas
        else:
            pragmas = {}
        return Sqlite3Adapter(
            self.config.data_dir,
            pragmas=pragmas,
            options=options)
