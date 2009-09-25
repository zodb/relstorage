##############################################################################
#
# Copyright (c) 2009 Zope Foundation and Contributors.
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
"""Utilities needed by RelStorage"""

try:
    from ZODB.blob import is_blob_record
    # ZODB 3.9
except ImportError:
    try:
        from ZODB.blob import Blob
    except ImportError:
        # ZODB < 3.8
        def is_blob_record(record):
            False
    else:
        # ZODB 3.8
        import cPickle
        import cStringIO

        def find_global_Blob(module, class_):
            if module == 'ZODB.blob' and class_ == 'Blob':
                return Blob

        def is_blob_record(record):
            """Check whether a database record is a blob record.

            This is primarily intended to be used when copying data from one
            storage to another.

            """
            if record and ('ZODB.blob' in record):
                unpickler = cPickle.Unpickler(cStringIO.StringIO(record))
                unpickler.find_global = find_global_Blob

                try:
                    return unpickler.load() is Blob
                except (MemoryError, KeyboardInterrupt, SystemExit):
                    raise
                except Exception:
                    pass

            return False
