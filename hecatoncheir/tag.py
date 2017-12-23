# -*- coding: utf-8 -*-

import os
import unittest

import sqlalchemy as sa

from repository import Repository
import db


class Tag2:
    def __init__(self, label, description=None, comment=None, num_of_tables=0):
        assert isinstance(label, unicode)
        assert isinstance(description, unicode) or description is None
        assert isinstance(comment, unicode) or comment is None

        self.label = label
        self.description = description
        self.comment = comment
        self.num_of_tables = num_of_tables

    """
    CREATE TABLE tags2 (
      label TEXT PRIMARY KEY,
      description TEXT NOT NULL,
      comment TEXT NOT NULL
    );
    """

    @staticmethod
    def create(label, description=None, comment=None):
        assert isinstance(label, unicode)
        assert isinstance(description, unicode) or description is None
        assert isinstance(comment, unicode) or comment is None

        if not description:
            description = u''
        if not comment:
            comment = u''

        q = (u"INSERT INTO tags2 VALUES ('{0}','{1}','{2}')"
             .format(label, description, comment))
        db.conn.execute(q)

        return Tag2(label, description, comment)

    @staticmethod
    def find(label):
        assert isinstance(label, unicode)
        q = (u"SELECT description, comment FROM tags2 WHERE label = '{0}'"
             .format(label))
        rs = db.conn.execute(q)
        r = rs.fetchone()
        if not r:
            return None
        description = r[0]
        comment = r[1]

        # Getting a number of tables with the tag label
        q = u"SELECT count(*) FROM tags WHERE tag_label = '{0}'".format(label)
        rs = db.conn.execute(q)
        r = rs.fetchone()

        return Tag2(label, description, comment, r[0])

    @staticmethod
    def findall():
        tags = []
        rs = db.conn.execute(u"SELECT label FROM tags2 ORDER BY label")
        for r in rs.fetchall():
            tags.append(Tag2.find(r[0]))
        return tags

    def update(self):
        assert isinstance(self.label, unicode)
        assert (isinstance(self.description, unicode) or
                self.description is None)
        assert isinstance(self.comment, unicode) or self.comment is None

        q = u"""
UPDATE tags2
   SET description = '{1}',
       comment = '{2}'
 WHERE label = '{0}'
""".format(self.label, self.description, self.comment)

        db.conn.execute(q)

        return True

    def destroy(self):
        assert isinstance(self.label, unicode)

        q = u"DELETE FROM tags2 WHERE label = '{0}'".format(self.label)

        db.conn.execute(q)

        return True


class TestTag2(unittest.TestCase):
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
        t = Tag2.create(u'l', u'd', u'c')
        self.assertEqual('l', t.label)
        self.assertEqual('d', t.description)
        self.assertEqual('c', t.comment)

        # unicode characters
        t = Tag2.create(u'タグ', u'説明', u'コメント')
        self.assertEqual(u'タグ', t.label)
        self.assertEqual(u'説明', t.description)
        self.assertEqual(u'コメント', t.comment)

    def test_create_002(self):
        t = Tag2.create(u'l', u'd', u'c')

        with self.assertRaises(sa.exc.IntegrityError) as cm:
            t = Tag2.create(u'l', u'd', u'c')
        err_msg = ('(psycopg2.IntegrityError) duplicate key value violates '
                   'unique constraint "tags2_pkey"')
        self.assertTrue(str(cm.exception).startswith(err_msg))

    def test_find_001(self):
        Tag2.create(u'l', u'd', u'c')

        t = Tag2.find(u'l')
        self.assertEqual('l', t.label)
        self.assertEqual('d', t.description)
        self.assertEqual('c', t.comment)

        # unicode characters
        Tag2.create(u'タグ', u'説明', u'コメント')
        t = Tag2.find(u'タグ')
        self.assertEqual(u'タグ', t.label)
        self.assertEqual(u'説明', t.description)
        self.assertEqual(u'コメント', t.comment)

    def test_find_002(self):
        Tag2.create(u'l', u'd', u'c')

        t = Tag2.find(u'l2')
        self.assertIsNone(t)

    def test_find_003(self):
        Tag2.create(u'l', u'd', u'c')

        # Get a number of tables of the tag 'l'
        t = Tag2.find(u'l')
        self.assertEquals(0, t.num_of_tables)

        # Create a new table record with a tag 'l'
        from table import Table2
        t = Table2.create('d', 's', 't', {'timestamp':
                                          '2016-04-27T10:06:41.653836'})
        t.data['tags'] = ['l']
        t.update()

        # Get a number of tables of the tag 'l' again
        t = Tag2.find(u'l')
        self.assertEquals(1, t.num_of_tables)

    def test_findall_001(self):
        Tag2.create(u'l', u'd', u'c')

        t = Tag2.findall()
        self.assertEqual(1, len(t))
        self.assertEqual('l', t[0].label)
        self.assertEqual('d', t[0].description)
        self.assertEqual('c', t[0].comment)

        Tag2.create(u'タグ', u'説明', u'コメント')

        t = Tag2.findall()
        self.assertEqual(2, len(t))
        self.assertEqual('l', t[0].label)
        self.assertEqual('d', t[0].description)
        self.assertEqual('c', t[0].comment)
        self.assertEqual(u'タグ', t[1].label)
        self.assertEqual(u'説明', t[1].description)
        self.assertEqual(u'コメント', t[1].comment)

    def test_update_001(self):
        Tag2.create(u'l', u'd', u'c')
        t = Tag2.find(u'l')

        # unicode characters
        t.description = u'タグ'
        t.comment = u'コメント'

        t.update()

        t2 = Tag2.find(u'l')
        self.assertEqual('l', t2.label)
        self.assertEqual(u'タグ', t2.description)
        self.assertEqual(u'コメント', t2.comment)

    def test_destroy_001(self):
        Tag2.create(u'l', u'd', u'c')
        Tag2.create(u'タグ', u'説明', u'コメント')

        t = Tag2.find(u'l')
        self.assertIsNotNone(t)
        self.assertIsNotNone(Tag2.find(u'タグ'))

        t.destroy()

        self.assertIsNone(Tag2.find(u'l'))
        self.assertIsNotNone(Tag2.find(u'タグ'))

        t = Tag2.find(u'タグ')
        t.destroy()
        self.assertIsNone(Tag2.find(u'タグ'))

if __name__ == '__main__':
    unittest.main()
