# -*- coding: utf-8 -*-

import os
import unittest

import sqlalchemy as sa

from repository import Repository
import db

class Attachment:
    def __init__(self, objid, objtype, filename):
        self.objid = objid
        self.objtype = objtype
        self.filename = filename

    @staticmethod
    def create(objid, objtype, filename):
        assert isinstance(objid, unicode)
        assert objtype
        assert isinstance(filename, unicode)

        q = u"""
INSERT INTO attachments VALUES ('{0}','{1}','{2}')
""".format(objid, objtype, filename)

        db.conn.execute(q)

        return Attachment(objid, objtype, filename)

    @staticmethod
    def find(objid, objtype, filename=None):
        assert isinstance(objid, unicode)
        assert objtype
        assert isinstance(filename, unicode) or filename is None

        if filename:
            filename_cond = u"AND filename = '%s'" % filename
        else:
            filename_cond = u''
        q = u"""
SELECT filename
  FROM attachments
 WHERE objid = '{0}'
   AND objtype = '{1}'
   {2}
 ORDER BY filename
""".format(objid, objtype, filename_cond)

        rs = db.conn.execute(q)
        a = []
        for r in rs:
            a.append(Attachment(objid, objtype, r[0]))
        return a

    def update(self):
        pass

    def destroy(self):
        q = u"""
DELETE FROM attachments
 WHERE objid = '{0}'
   AND objtype = '{1}'
   AND filename = '{2}'
""".format(self.objid, self.objtype, self.filename)

        db.conn.execute(q)

        return True


class TestAttachment(unittest.TestCase):
    def setUp(self):
        db.creds = {}
        db.creds['host'] = os.environ.get('PGHOST', 'localhost')
        db.creds['port'] = os.environ.get('PGPORT', 5432)
        db.creds['dbname'] = os.environ.get('PGDATABASE', 'datacatalog')
        db.creds['username'] = os.environ.get('PGUSER', 'postgres')
        db.creds['password'] = os.environ.get('PGPASSWORD', 'postgres')

        self.repo = Repository()
        self.repo.destroy()
        self.repo.create()

    def tearDown(self):
        db.conn.close()

    def test_create_001(self):
        a = Attachment.create(u'aaa', 'table', u'filename.xls')
        self.assertEqual('aaa', a.objid)
        self.assertEqual('table', a.objtype)
        self.assertEqual('filename.xls', a.filename)

        # unicode string
        a = Attachment.create(u'タグ', 'tag', u'ファイル名.xls')
        self.assertEqual(u'タグ', a.objid)
        self.assertEqual('tag', a.objtype)
        self.assertEqual(u'ファイル名.xls', a.filename)

    def test_create_002(self):
        a = Attachment.create(u'aaa', 'table', u'filename.xls')
        self.assertEqual(u'aaa', a.objid)
        self.assertEqual('table', a.objtype)
        self.assertEqual(u'filename.xls', a.filename)

        with self.assertRaises(sa.exc.IntegrityError) as cm:
            a = Attachment.create(u'aaa', 'table', u'filename.xls')
        self.assertTrue(str(cm.exception).startswith('(psycopg2.IntegrityError) duplicate key value violates unique constraint'))

    def test_find_001(self):
        a = Attachment.create(u'aaa', 'table', u'filename.xls')
        aa = Attachment.find(u'aaa', 'table')
        self.assertEquals(1, len(aa))
        self.assertEqual('aaa', aa[0].objid)
        self.assertEqual('table', aa[0].objtype)
        self.assertEqual('filename.xls', aa[0].filename)

        aa = Attachment.find(u'aaa', 'table', u'filename.xls')
        self.assertEquals(1, len(aa))
        self.assertEqual('aaa', aa[0].objid)
        self.assertEqual('table', aa[0].objtype)
        self.assertEqual('filename.xls', aa[0].filename)

    def test_find_002(self):
        Attachment.create(u'aaa', 'table', u'filename.xls')
        Attachment.create(u'aaa', 'table', u'filename.ppt')

        a = Attachment.find(u'aaa', 'table')
        self.assertEquals(2, len(a))
        self.assertEqual('filename.ppt', a[0].filename)
        self.assertEqual('filename.xls', a[1].filename)

        a = Attachment.find(u'aaa', 'table', u'filename.ppt')
        self.assertEquals(1, len(a))
        self.assertEqual('filename.ppt', a[0].filename)

    def test_destroy_001(self):
        Attachment.create(u'aaa', 'table', u'filename.xls')

        a = Attachment.find(u'aaa', 'table')
        self.assertEquals(1, len(a))
        self.assertEquals('filename.xls', a[0].filename)

        self.assertTrue(a[0].destroy())

        a = Attachment.find(u'aaa', 'table')
        self.assertEquals(0, len(a))

    def test_destroy_002(self):
        Attachment.create(u'aaa', 'table', u'filename.xls')
        Attachment.create(u'aaa', 'table', u'filename.ppt')

        a = Attachment.find(u'aaa', 'table')
        self.assertEquals(2, len(a))
        self.assertEquals('filename.ppt', a[0].filename)
        self.assertEquals('filename.xls', a[1].filename)

        self.assertTrue(a[0].destroy())

        a = Attachment.find(u'aaa', 'table')
        self.assertEquals(1, len(a))
        self.assertEquals('filename.xls', a[0].filename)

if __name__ == '__main__':
    unittest.main()
