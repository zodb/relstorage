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

from ZODB.config import BaseConfig

from relstorage.options import Options
from relstorage.storage import RelStorage
from relstorage.adapters.replica import ReplicaSelector


class RelStorageFactory(BaseConfig):
    """Open a storage configured via ZConfig"""
    def open(self):
        config = self.config
        options = Options()
        for key in options.__dict__.keys():
            value = getattr(config, key, None)
            if value is not None:
                setattr(options, key, value)
        adapter = config.adapter.create(options)
        return RelStorage(adapter, name=config.name, options=options)


class PostgreSQLAdapterFactory(BaseConfig):
    def create(self, options):
        from adapters.postgresql import PostgreSQLAdapter
        return PostgreSQLAdapter(
            dsn=self.config.dsn,
            options=options,
            )


class OracleAdapterFactory(BaseConfig):
    def create(self, options):
        from adapters.oracle import OracleAdapter
        config = self.config
        return OracleAdapter(
            user=config.user,
            password=config.password,
            dsn=config.dsn,
            options=options,
            )


class MySQLAdapterFactory(BaseConfig):
    def create(self, options):
        from adapters.mysql import MySQLAdapter
        params = {}
        for key in self.config.getSectionAttributes():
            value = getattr(self.config, key)
            if value is not None:
                params[key] = value
        return MySQLAdapter(options=options, **params)

