# -*- coding: utf-8 -*-
from __future__ import absolute_import
from __future__ import print_function

import ZConfig.datatypes
from ZODB.DemoStorage import DemoStorage

from relstorage.options import Options
from relstorage.storage import RelStorage

try:
    from urllib import parse as urlparse
    from urllib.parse import parse_qsl
except ImportError:
    # Py2
    import urlparse
    from urlparse import parse_qsl


datatypes = ZConfig.datatypes.stock_datatypes.copy()
datatypes['string-list'] = lambda s: tuple(s.split(','))

def asBoolean(s):
    if s == '0':
        return False
    if s == '1':
        return True
    return ZConfig.datatypes.stock_datatypes['boolean'](s)

datatypes['boolean'] = asBoolean

class Resolver(object):
    _bool_args = ()
    _int_args = ()
    _string_args = ()
    _bytesize_args = ()
    _float_args = ()
    _tuple_args = ()

    def interpret_kwargs(self, kw):
        unused = kw.copy()
        new = {}

        converters = [
            (datatypes['boolean'], self._bool_args),
            (datatypes['integer'], self._int_args),
            (datatypes['string'], self._string_args),
            (datatypes['byte-size'], self._bytesize_args),
            (datatypes['float'], self._float_args),
            (datatypes['string-list'], self._tuple_args),
        ]
        for convert, arg_names in converters:
            for arg_name in arg_names:
                value = unused.pop(arg_name, None)
                if value is not None:
                    value = convert(value)
                    new[arg_name] = value

        return new, unused


# Not a real resolver, but we use interpret_kwargs
class PostgreSQLAdapterHelper(Resolver):
    _int_args = ('connect_timeout', )
    _string_args = ('ssl_mode', )

    def __call__(self, parsed_uri, kw):
        dsn_args = [
            ('dbname', parsed_uri.path[1:]),
            ('user', parsed_uri.username),
            ('password', parsed_uri.password),
            ('host', parsed_uri.hostname),
            ('port', parsed_uri.port)
        ]

        kw, unused = self.interpret_kwargs(kw)
        dsn_args.extend(kw.items())

        dsn = ' '.join("%s='%s'" % arg for arg in dsn_args)

        def factory(options):
            from relstorage.adapters.postgresql import PostgreSQLAdapter
            return PostgreSQLAdapter(dsn=dsn, options=options)
        return factory, unused


class MySQLAdapterHelper(Resolver):
    _int_args = ('connect_timeout', 'client_flag', 'load_infile', 'compress',
                 'named_pipe')
    _string_args = ('unix_socket', 'init_command', 'read_default_file',
                    'read_default_group')

    def __call__(self, parsed_uri, kw):
        settings = {
            'db': parsed_uri.path[1:],
            'user': parsed_uri.username,
            'passwd': parsed_uri.password,
            'host': parsed_uri.hostname,
            'port': parsed_uri.port
        }

        kw, unused = self.interpret_kwargs(kw)
        settings.update(kw)

        def factory(options):
            from relstorage.adapters.mysql import MySQLAdapter
            return MySQLAdapter(options=options, **settings)
        return factory, unused


class OracleAdapterHelper(Resolver):
    _int_args = ('twophase', )
    _string_args = ('user', 'password', 'dsn')

    def __call__(self, parsed_uri, kw):
        kw, unused = self.interpret_kwargs(kw)

        def factory(options):
            from relstorage.adapters.oracle import OracleAdapter
            return OracleAdapter(options=options, **kw)
        return factory, unused

class SqliteAdapterHelper(Resolver):
    _string_args = ('path',)

    def __call__(self, parsed_uri, kw):
        kw, unused = self.interpret_kwargs(kw)

        def factory(options):
            from relstorage.adapters.sqlite.adapter import Sqlite3Adapter
            return Sqlite3Adapter(options=options, **kw)
        return factory, unused

# The relstorage support is inspired by django-zodb.

class RelStorageURIResolver(Resolver):
    _bool_args = (
        'read_only',
        'keep_history',
        'pack_gc',
        'pack_dry_run',
        'shared_blob_dir',
        'demostorage',
    )
    _int_args = (
        'cache_local_mb',
        'commit_lock_timeout',
        'commit_lock_id',
    )
    _string_args = (
        'name', 'blob_dir', 'replica_conf',
        'cache_module_name', 'cache_prefix',
        'cache_delta_size_limit', 'cache_local_compression',
        'driver',
    )
    _bytesize_args = (
        'blob_cache_size', 'blob_cache_size_check',
        'blob_cache_chunk_size',
        'cache_local_object_max',
    )
    _float_args = ('replica_timeout', 'pack_batch_timeout',
                   'pack_duty_cycle', 'pack_max_delay')
    _tuple_args = ('cache_servers',)

    def __init__(self, adapter_helper):
        self.adapter_helper = adapter_helper

    def __call__(self, uri):
        uri = uri.replace('postgres://', 'http://', 1)
        uri = uri.replace('mysql://', 'http://', 1)
        uri = uri.replace('oracle://', 'http://', 1)
        uri = uri.replace('sqlite://', 'http://', 1)
        parsed_uri = urlparse.urlsplit(uri)
        kw = dict(parse_qsl(parsed_uri.query))

        adapter_factory, kw = self.adapter_helper(parsed_uri, kw)
        kw, unused = self.interpret_kwargs(kw)

        demostorage = kw.pop('demostorage', False)
        options = Options(**kw)

        def factory():
            adapter = adapter_factory(options)
            storage = RelStorage(adapter=adapter, options=options)
            return storage if not demostorage else DemoStorage(base=storage)
        return factory, unused

postgresql_resolver = RelStorageURIResolver(PostgreSQLAdapterHelper())
mysql_resolver = RelStorageURIResolver(MySQLAdapterHelper())
oracle_resolver = RelStorageURIResolver(OracleAdapterHelper())
sqlite_resolver = RelStorageURIResolver(SqliteAdapterHelper())
