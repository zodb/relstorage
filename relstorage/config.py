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

from relstorage import RelStorage, Options


class RelStorageFactory(BaseConfig):
    """Open a storage configured via ZConfig"""
    def open(self):
        config = self.config
        adapter = config.adapter.open()
        options = Options()
        for key in options.__dict__.keys():
            value = getattr(config, key, None)
            if value is not None:
                setattr(options, key, value)
        return RelStorage(adapter, name=config.name, create=config.create,
            read_only=config.read_only, options=options)


class PostgreSQLAdapterFactory(BaseConfig):
    def open(self):
        from adapters.postgresql import PostgreSQLAdapter
        return PostgreSQLAdapter(self.config.dsn)


class OracleAdapterFactory(BaseConfig):
    def open(self):
        from adapters.oracle import OracleAdapter
        config = self.config
        return OracleAdapter(config.user, config.password, config.dsn)


class MySQLAdapterFactory(BaseConfig):
    def open(self):
        from adapters.mysql import MySQLAdapter
        options = {}
        for key in self.config.getSectionAttributes():
            value = getattr(self.config, key)
            if value is not None:
                options[key] = value
        return MySQLAdapter(**options)

