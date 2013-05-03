import mock
import unittest
from pkg_resources import DistributionNotFound


class TestPostgreSQLURIResolver(unittest.TestCase):

    def _makeOne(self):
        from relstorage.zodburi_resolver import RelStorageURIResolver, PostgreSQLAdapterHelper
        return RelStorageURIResolver(PostgreSQLAdapterHelper())

    def setUp(self):
        # relstorage.options.Options is little more than a dict.
        # We make it comparable to simplify the tests.
        from relstorage.options import Options
        Options.__eq__ = lambda s, o: vars(s) == vars(o)

        self.patcher1 = mock.patch('relstorage.zodburi_resolver.RelStorage')
        self.patcher2 = mock.patch('relstorage.adapters.postgresql.PostgreSQLAdapter')
        self.RelStorage = self.patcher1.start()
        self.PostgreSQLAdapter = self.patcher2.start()

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

    def test_call(self):
        from relstorage.options import Options
        resolver = self._makeOne()
        factory, dbkw = resolver(
            'postgres://someuser:somepass@somehost:5432/somedb'
            '?read_only=1')
        factory()

        expected_options = Options(read_only=1)
        self.PostgreSQLAdapter.assert_called_once_with(
            dsn="dbname='somedb' user='someuser' password='somepass' "
                "host='somehost' port='5432'", options=expected_options)
        self.RelStorage.assert_called_once_with(
            adapter=self.PostgreSQLAdapter(), options=expected_options)

    def test_call_adapter_options(self):
        from relstorage.options import Options
        resolver = self._makeOne()
        factory, dbkw = resolver(
            'postgres://someuser:somepass@somehost:5432/somedb'
            '?read_only=1&connect_timeout=10')
        factory()

        expected_options = Options(read_only=1)
        self.PostgreSQLAdapter.assert_called_once_with(
            dsn="dbname='somedb' user='someuser' password='somepass' "
                "host='somehost' port='5432' connect_timeout='10'",
            options=expected_options)
        self.RelStorage.assert_called_once_with(
            adapter=self.PostgreSQLAdapter(), options=expected_options)

    def test_invoke_factory_demostorage(self):
        from ZODB.DemoStorage import DemoStorage
        resolver = self._makeOne()
        factory, dbkw = resolver(
            'postgres://someuser:somepass@somehost:5432/somedb'
            '?read_only=1&demostorage=true')
        self.assertTrue(isinstance(factory(), DemoStorage))

    def test_dbargs(self):
        resolver = self._makeOne()
        factory, dbkw = resolver(
            'postgres://someuser:somepass@somehost:5432/somedb'
            '?read_only=1&connection_pool_size=1&connection_cache_size=1'
            '&database_name=dbname')
        self.assertEqual(dbkw, {'connection_pool_size': '1',
                                'connection_cache_size': '1',
                                'database_name': 'dbname'})


class TestEntryPoints(unittest.TestCase):
    def test_it(self):
        from pkg_resources import load_entry_point
        from relstorage.zodburi_resolver import (
            RelStorageURIResolver,
            PostgreSQLAdapterHelper,
            MySQLAdapterHelper,
            OracleAdapterHelper
            )

        expected = [
            ('postgres', RelStorageURIResolver, PostgreSQLAdapterHelper),
            ('mysql', RelStorageURIResolver, MySQLAdapterHelper),
            ('oracle', RelStorageURIResolver, OracleAdapterHelper)
        ]

        for name, cls, helper_cls in expected:
            try:
                target = load_entry_point('relstorage', 'zodburi.resolvers', name)
                self.assertTrue(isinstance(target, cls))
                self.assertTrue(isinstance(target.adapter_helper, helper_cls))
            except DistributionNotFound as e:
                import warnings
                warnings.warn('%s not found, skipping the zodburi test for %s'%
                              (e[0], name))
