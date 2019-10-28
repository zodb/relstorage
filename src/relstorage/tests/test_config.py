# -*- coding: utf-8 -*-
##############################################################################
#
# Copyright (c) 2019 Zope Foundation and Contributors.
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

"""
Tests for config.py
"""
from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import unittest

from relstorage import config
from relstorage import options

from . import mock

class MockZConfig(object):

    def __init__(self, section_name):
        self.__section_name = section_name

    def getSectionName(self):
        return self.__section_name

    def getSectionAttributes(self):
        return (k for k in self.__dict__ if not k.startswith('_'))

class MockStorage(MockZConfig):
    adapter = None
    name = 'name'

class MockAdapter(object):

    def __init__(self):
        self.config = MockZConfig('adapter')
        self.config.driver = None
        self.options = None

    def create(self, opts):
        self.options = opts

    def __eq__(self, other):
        return isinstance(other, MockAdapter)

class TestConfig(unittest.TestCase):

    DRIVER_NAME = 'a_driver'

    def _make_expected_options(self, **kwargs):
        o = options.Options(name='name',
                            driver=self.DRIVER_NAME)
        for k, v in kwargs.items():
            setattr(o, k, v)
        return o

    @mock.patch('relstorage.config.RelStorage', autospec=True)
    def test_driver_stays_set(self, mock_storage):
        # There was a bug where opening a configuration object more than
        # once would lose the driver settings.
        root_zconfig = MockStorage('relstorage')
        root_zconfig.adapter = MockAdapter()
        root_zconfig.adapter.config.driver = self.DRIVER_NAME

        factory = config.RelStorageFactory(root_zconfig)

        factory.open()

        mock_storage.assert_called_once_with(
            None,
            name='name',
            options=self._make_expected_options(adapter=MockAdapter()))

        # The driver was copied to the right place to make an Options instance
        self.assertEqual(root_zconfig.driver, self.DRIVER_NAME)
        # And it was left where it was
        self.assertEqual(root_zconfig.adapter.config.driver, self.DRIVER_NAME)

        # It was in the options
        self.assertEqual(root_zconfig.adapter.options.driver, self.DRIVER_NAME)

    @mock.patch('relstorage.adapters.mysql.MySQLAdapter')
    @mock.patch('relstorage.config.RelStorage', autospec=True)
    def test_mysql_doesnt_pass_driver_as_connect_kwarg(self, _, mock_mysql):

        root_zconfig = MockStorage('relstorage')
        adapter_zconfig = MockZConfig('adapter')
        adapter_zconfig.driver = self.DRIVER_NAME
        adapter_zconfig.a_conn_setting = 42 # pylint:disable=attribute-defined-outside-init
        root_zconfig.adapter = config.MySQLAdapterFactory(adapter_zconfig)


        factory = config.RelStorageFactory(root_zconfig)

        factory.open()

        mock_mysql.assert_called_once_with(
            a_conn_setting=42,
            options=self._make_expected_options(adapter=root_zconfig.adapter))


class TestOptions(unittest.TestCase):

    def test_deprecated_warnings(self):
        import warnings
        for name in options.Options._deprecated_options:
            with warnings.catch_warnings(record=True) as caught:
                warnings.simplefilter('default')
                class C(object):
                    pass
                setattr(C, name, 42)
                options.Options.copy_valid_options(C)

            self.assertEqual(1, len(caught))
            self.assertIn(name, str(caught[0].message))
