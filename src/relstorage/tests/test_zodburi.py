from __future__ import absolute_import
from __future__ import print_function

import unittest

from relstorage.zodburi_resolver import RelStorageURIResolver

from . import mock

class AbstractURIResolverTestBase(unittest.TestCase):

    adapter_name = None
    prefix = ''

    def _get_helper(self):
        raise NotImplementedError()

    def _makeOne(self):
        helper = self._get_helper()
        return RelStorageURIResolver(helper())

    def setUp(self):
        # relstorage.options.Options is little more than a dict.
        # We make it comparable to simplify the tests.
        from relstorage.options import Options
        Options.__eq__ = lambda s, o: vars(s) == vars(o)

        self.patcher1 = mock.patch('relstorage.zodburi_resolver.RelStorage')
        self.patcher2 = mock.patch(self.adapter_name)
        self.RelStorage = self.patcher1.start()
        try:
            self.DBAdapter = self.patcher2.start()
        except ImportError as e:
            raise unittest.SkipTest(str(e))

    def tearDown(self):
        self.patcher1.stop()
        self.patcher2.stop()

    def test_bool_args(self):
        resolver = self._makeOne()
        f = resolver.interpret_kwargs
        kwargs = f({'read_only':'1'})
        self.assertEqual(kwargs[0], {'read_only':1})
        kwargs = f({'read_only':'true'})
        self.assertEqual(kwargs[0], {'read_only':1})
        kwargs = f({'read_only':'on'})
        self.assertEqual(kwargs[0], {'read_only':1})
        kwargs = f({'read_only':'off'})
        self.assertEqual(kwargs[0], {'read_only':0})
        kwargs = f({'read_only':'no'})
        self.assertEqual(kwargs[0], {'read_only':0})
        kwargs = f({'read_only':'false'})
        self.assertEqual(kwargs[0], {'read_only':0})

    def _format_db(self, dbname='somedb', user='someuser', password='somepass',
                   host='somehost', port='5432', **kwargs):
        raise NotImplementedError()

    def test_call(self):
        from relstorage.options import Options
        resolver = self._makeOne()
        factory, _dbkw = resolver(
            self.prefix + '://someuser:somepass@somehost:5432/somedb'
            '?read_only=1&cache_servers=123,456')
        factory()

        expected_options = Options(read_only=1, cache_servers=('123', '456'))
        self.DBAdapter.assert_called_once_with(
            options=expected_options, **self._format_db())
        self.RelStorage.assert_called_once_with(
            adapter=self.DBAdapter(), options=expected_options)

    def test_call_adapter_options(self):
        from relstorage.options import Options
        resolver = self._makeOne()
        factory, _dbkw = resolver(
            self.prefix + '://someuser:somepass@somehost:5432/somedb'
            '?read_only=1&connect_timeout=10')
        factory()

        expected_options = Options(read_only=1)
        self.DBAdapter.assert_called_once_with(
            options=expected_options,
            **self._format_db(connect_timeout=10))
        self.RelStorage.assert_called_once_with(
            adapter=self.DBAdapter(), options=expected_options)

    def test_invoke_factory_driver_auto(self, driver_name='auto'):
        from relstorage.options import Options
        resolver = self._makeOne()
        factory, _dbkw = resolver(
            self.prefix + '://someuser:somepass@somehost:5432/somedb'
            '?driver=' + driver_name
        )
        factory()
        expected_options = Options(driver=driver_name)
        self.DBAdapter.assert_called_once_with(
            options=expected_options,
            **self._format_db())

    def test_invoke_factory_demostorage(self):
        from ZODB.DemoStorage import DemoStorage
        resolver = self._makeOne()
        factory, _dbkw = resolver(
            self.prefix + '://someuser:somepass@somehost:5432/somedb'
            '?read_only=1&demostorage=true')
        self.assertTrue(isinstance(factory(), DemoStorage))

    def test_dbargs(self):
        resolver = self._makeOne()
        _factory, dbkw = resolver(
            self.prefix + '://someuser:somepass@somehost:5432/somedb'
            '?read_only=1&connection_pool_size=1&connection_cache_size=1'
            '&database_name=dbname')
        self.assertEqual(dbkw, {'connection_pool_size': '1',
                                'connection_cache_size': '1',
                                'database_name': 'dbname'})


class TestPostgreSQLURIResolver(AbstractURIResolverTestBase):
    adapter_name = 'relstorage.adapters.postgresql.PostgreSQLAdapter'
    prefix = 'postgres'

    def _get_helper(self):
        from relstorage.zodburi_resolver import PostgreSQLAdapterHelper
        return PostgreSQLAdapterHelper


    def _format_db(self, dbname='somedb', user='someuser', password='somepass',
                   host='somehost', port='5432', **kwargs):
        dsn = ("dbname='%s' user='%s' password='%s' host='%s' port='%s'"
               % (dbname, user, password, host, port))
        if 'connect_timeout' in kwargs:
            dsn += " connect_timeout='%s'" % kwargs['connect_timeout']
        else:
            assert not kwargs

        return {'dsn': dsn}


class TestMySQLURIResolver(AbstractURIResolverTestBase):
    adapter_name = 'relstorage.adapters.mysql.MySQLAdapter'
    prefix = 'postgres'

    def _get_helper(self):
        from relstorage.zodburi_resolver import MySQLAdapterHelper
        return MySQLAdapterHelper

    def _format_db(self, dbname='somedb', user='someuser', password='somepass',
                   host='somehost', port='5432', **kwargs):
        args = dict(locals())
        args.update(kwargs)
        args['db'] = args['dbname']; del args['dbname']
        args['passwd'] = args['password']; del args['password']
        args['port'] = int(args['port'])

        del args['kwargs']
        del args['self']
        return args

class TestOracleURIResolver(AbstractURIResolverTestBase):
    adapter_name = 'relstorage.adapters.oracle.OracleAdapter'
    prefix = 'oracle'

    def _get_helper(self):
        from relstorage.zodburi_resolver import OracleAdapterHelper
        return OracleAdapterHelper

    # The default URI the test uses puts user and password and host into
    # the URI proper, but oracle expects them in the query params.
    def _format_db(self, dbname='somedb', user='someuser', password='somepass',
                   host='somehost', port='5432', **kwargs):
        return {}

class TestSQLiteURIResolver(AbstractURIResolverTestBase):
    adapter_name = 'relstorage.adapters.sqlite.adapter.Sqlite3Adapter'
    prefix = 'sqlite'

    def _get_helper(self):
        from relstorage.zodburi_resolver import SqliteAdapterHelper
        return SqliteAdapterHelper

    def _format_db(self, dbname='somedb', user='someuser', password='somepass',
                   host='somehost', port='5432', **kwargs):
        return {}


del AbstractURIResolverTestBase # So it doesn't get discovered as a test

class TestEntryPoints(unittest.TestCase):

    def _check_entry_point(self, name, cls, helper_cls):
        from pkg_resources import load_entry_point

        target = load_entry_point('relstorage', 'zodburi.resolvers', name)
        self.assertIsInstance(target, cls)
        self.assertIsInstance(target.adapter_helper, helper_cls)

    def test_postgres(self):
        from relstorage.zodburi_resolver import PostgreSQLAdapterHelper
        self._check_entry_point('postgres', RelStorageURIResolver, PostgreSQLAdapterHelper)

    def test_mysql(self):
        from relstorage.zodburi_resolver import MySQLAdapterHelper
        self._check_entry_point('mysql', RelStorageURIResolver, MySQLAdapterHelper)

    def test_oracle(self):
        from relstorage.zodburi_resolver import OracleAdapterHelper
        self._check_entry_point('oracle', RelStorageURIResolver, OracleAdapterHelper)

    def test_sqlite(self):
        from relstorage.zodburi_resolver import SqliteAdapterHelper
        self._check_entry_point('sqlite', RelStorageURIResolver, SqliteAdapterHelper)
