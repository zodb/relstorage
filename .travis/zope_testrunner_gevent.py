#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
Script to run zope.testrunner in a gevent monkey-patched environment.

Using ``python -m gevent.monkey zope-testrunner ...`` is insufficient.

This is because up through 1.5a2 there is a serious bug in the way the
monkey-patcher patches the spawned process. The net effect is that the gevent
threadpool isn't functional.

"""
import gevent.monkey
gevent.monkey.patch_all()
# pylint:disable=wrong-import-position, wrong-import-order
import sys

from zope.testrunner import run

sys.argv[:] = [
    'zope-testrunner',
    '--test-path', 'src',
    '-v',
    '--color',
    '--keepbytecode',
] + sys.argv[1:]
print(sys.argv)
run()
