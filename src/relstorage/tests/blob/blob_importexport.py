##############################################################################
#
# Copyright (c) 2005,2019 Zope Foundation and Contributors.
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
Import/export support for blob data
"""
from __future__ import absolute_import
from __future__ import print_function

from ZODB.DB import DB
import ZODB.blob
import transaction
from persistent.mapping import PersistentMapping

from . import TestBlobMixin

class TestBlobImportExportMixin(TestBlobMixin):

    def setUp(self):
        super(TestBlobImportExportMixin, self).setUp()
        self.database1 = self.database
        self.storage2 = self.create_storage('2')
        self.database2 = DB(self.storage2)

    def tearDown(self):
        self.database2.close()
        self.storage2.close()
        super(TestBlobImportExportMixin, self).tearDown()

    def testExport(self):
        # Create our root object for database1:

        connection1 = self.database1.open()
        root1 = connection1.root()

        # Put a couple blob objects in our database1 and on the filesystem:
        transaction.begin()
        self.data1 = b'x'*100000
        blob1 = ZODB.blob.Blob()
        with blob1.open('w') as f:
            f.write(self.data1)
        self.data2 = b'y'*100000
        blob2 = ZODB.blob.Blob()
        with blob2.open('w') as f:
            f.write(self.data2)
        d = PersistentMapping({'blob1':blob1, 'blob2':blob2})
        root1['blobdata'] = d
        transaction.commit()
        connection1.close()

        # Export our blobs from a database1 connection:
        conn = self.database1.open()
        root = conn.root()
        oid = root['blobdata']._p_oid
        exportfile = 'export'
        conn.exportFile(oid, exportfile).close()
        conn.close()
        return exportfile

    def testImport(self):
        # Import our exported data into database2:
        exportfile = self.testExport()
        connection2 = self.database2.open()
        root2 = connection2.root()
        transaction.begin()
        data = root2._p_jar.importFile(exportfile)
        root2['blobdata'] = data
        transaction.commit()

        # Make sure our data exists:
        self.assertEqual({'blob1', 'blob2'},
                         set(root2['blobdata'].keys()))

        with root2['blobdata']['blob1'].open() as f:
            b2d = f.read()
        self.assertEqual(b2d, self.data1)
        with root2['blobdata']['blob2'].open() as f:
            b2d = f.read()
        self.assertEqual(b2d, self.data2)
        transaction.get().abort()
