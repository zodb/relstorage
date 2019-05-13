##############################################################################
#
# Copyright (c) 2017 Zope Foundation and Contributors.
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

import unittest

from relstorage.adapters._util import noop_when_history_free

from ..mover import metricmethod_sampled


class TestNoOp(unittest.TestCase):


    def test_history_free(self):

        class Thing(object):

            keep_history = False

            @noop_when_history_free
            @metricmethod_sampled
            def thing(self, arg1, arg2=False):
                "Docs"
                assert arg1
                assert arg2


        thing = Thing()
        # Before calling it has a wrapper
        thing_thing = thing.thing
        self.assertTrue(hasattr(thing_thing, '__wrapped__'))
        self.assertEqual(Thing.thing.__doc__, "Docs")
        self.assertEqual(Thing.thing.__doc__, thing_thing.__doc__)
        self.assertEqual(Thing.thing.__name__, 'thing')
        self.assertEqual(Thing.thing.__name__, thing_thing.__name__)

        # call with false args to be sure doesn't run
        thing.thing(0) # one argument
        thing.thing(0, arg2=0) # kwarg

        # It still has a wrapper, but it's not the original
        self.assertTrue(hasattr(thing.thing, '__wrapped__'))
        self.assertIsNot(thing.thing, thing_thing)
        self.assertEqual(Thing.thing.__doc__, thing.thing.__doc__)
        self.assertEqual(Thing.thing.__name__, 'thing')
        self.assertEqual(Thing.thing.__name__, thing.thing.__name__)

    def test_keep_history(self):

        class Thing(object):

            keep_history = True
            ran = False

            @noop_when_history_free
            def thing(self, arg1, arg2=False):
                "Docs"
                assert arg1
                assert arg2
                self.ran = True
                return arg1


        thing = Thing()
        # Before calling it has a wrapper
        thing_thing = thing.thing
        self.assertTrue(hasattr(thing_thing, '__wrapped__'))
        self.assertEqual(Thing.thing.__doc__, "Docs")
        self.assertEqual(Thing.thing.__doc__, thing_thing.__doc__)
        self.assertEqual(Thing.thing.__name__, 'thing')
        self.assertEqual(Thing.thing.__name__, thing_thing.__name__)

        # call with false args to be sure does run
        self.assertRaises(AssertionError, thing.thing, 0) # one argument
        thing.thing(1, arg2=1)
        self.assertRaises(AssertionError, thing.thing, 1, arg2=0)

        # It has access to self
        self.assertTrue(thing.ran)

        # It returns a value...
        self.assertIs(self, thing.thing(self, 1))
        # ...even the first time its called
        self.assertIs(self, Thing().thing(self, 1))

        # It still has a wrapper, but it's not the original
        self.assertTrue(hasattr(thing.thing, '__wrapped__'))
        self.assertIsNot(thing.thing, thing_thing)
        self.assertEqual(Thing.thing.__doc__, thing.thing.__doc__)
        self.assertEqual(Thing.thing.__name__, 'thing')
        self.assertEqual(Thing.thing.__name__, thing.thing.__name__)

def test_suite():
    suite = unittest.TestSuite()
    suite.addTest(unittest.makeSuite(TestNoOp))
    return suite

if __name__ == '__main__':
    unittest.main(defaultTest='test_suite')
