import cgi
import urlparse
from ZODB.DemoStorage import DemoStorage

from relstorage.options import Options
from relstorage.storage import RelStorage


#################################################
# Utility to interpret inputs, talen from zodburi


TRUETYPES = ('1', 'on', 'true', 't', 'yes')
FALSETYPES = ('', '0', 'off', 'false', 'f', 'no')


class SuffixMultiplier(object):
    # d is a dictionary of suffixes to integer multipliers.  If no suffixes
    # match, default is the multiplier.  Matches are case insensitive.  Return
    # values are in the fundamental unit.
    def __init__(self, d, default=1):
        self._d = d
        self._default = default
        # all keys must be the same size
        self._keysz = None
        for k in d.keys():
            if self._keysz is None:
                self._keysz = len(k)
            else:
                if self._keysz != len(k):
                    raise ValueError('suffix length missmatch')

    def __call__(self, v):
        v = v.lower()
        for s, m in self._d.items():
            if v[-self._keysz:] == s:
                return int(v[:-self._keysz]) * m
        return int(v) * self._default

convert_bytesize = SuffixMultiplier({'kb': 1024,
                                     'mb': 1024*1024,
                                     'gb': 1024*1024*1024,
                                    })


def convert_int(value):
    # boolean values are also treated as integers
    value = value.lower()
    if value in FALSETYPES:
        return 0
    if value in TRUETYPES:
        return 1
    return int(value)

def convert_tuple(value):
    return tuple(value.split(','))


class Resolver(object):
    _int_args = ()
    _string_args = ()
    _bytesize_args = ()
    _float_args = ()
    _tuple_args = ()

    def interpret_kwargs(self, kw):
        unused = kw.copy()
        new = {}
        convert_string = lambda s: s
        converters = [
            (convert_int, self._int_args),
            (convert_string, self._string_args),
            (convert_bytesize, self._bytesize_args),
            (float, self._float_args),
            (convert_tuple, self._tuple_args),
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

        dsn = ' '.join("%s='%s'"%arg for arg in dsn_args)

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


# The relstorage support is inspired by django-zodb.

class RelStorageURIResolver(Resolver):
    _int_args = ('poll_interval', 'cache_local_mb', 'commit_lock_timeout',
                 'commit_lock_id', 'read_only', 'shared_blob_dir',
                 'keep_history', 'pack_gc', 'pack_dry_run', 'strict_tpc',
                 'create', 'demostorage',)
    _string_args = ('name', 'blob_dir', 'replica_conf',
                    'cache_module_name', 'cache_prefix',
                    'cache_delta_size_limit', 'cache_local_compression')
    _bytesize_args = ('blob_cache_size', 'blob_cache_size_check',
                      'blob_cache_chunk_size', 'cache_local_object_max')
    _float_args = ('replica_timeout', 'pack_batch_timeout',
                   'pack_duty_cycle', 'pack_max_delay')
    _tuple_args = ('cache_servers',)

    def __init__(self, adapter_helper):
        self.adapter_helper = adapter_helper

    def __call__(self, uri):
        uri = uri.replace('postgres://', 'http://', 1)
        uri = uri.replace('mysql://', 'http://', 1)
        uri = uri.replace('oracle://', 'http://', 1)
        parsed_uri = urlparse.urlsplit(uri)
        kw = dict(cgi.parse_qsl(parsed_uri.query))

        adapter_factory, kw = self.adapter_helper(parsed_uri, kw)
        kw, unused = self.interpret_kwargs(kw)

        demostorage = kw.pop('demostorage', False)
        options = Options(**kw)

        def factory():
            adapter = adapter_factory(options)
            storage = RelStorage(adapter=adapter, options=options)
            if demostorage:
                storage = DemoStorage(base=storage)
            return storage
        return factory, unused

postgresql_resolver = RelStorageURIResolver(PostgreSQLAdapterHelper())
mysql_resolver = RelStorageURIResolver(MySQLAdapterHelper())
oracle_resolver = RelStorageURIResolver(OracleAdapterHelper())
