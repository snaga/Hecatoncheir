import unittest

import sqlalchemy as sa

import db

class Tag2:
    def __init__(self, label, description=None, comment=None, num_of_tables=0):
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
        if not description:
            description = ''
        if not comment:
            comment = ''

        q = "INSERT INTO tags2 VALUES ('{0}','{1}','{2}')".format(label, description, comment)
        db.conn.execute(q)

        return Tag2(label, description, comment)

    @staticmethod
    def find(label):
        q = "SELECT description, comment FROM tags2 WHERE label = '{0}'".format(label)
        rs = db.conn.execute(q)
        r = rs.fetchone()
        if not r:
            return None
        return Tag2(label, r[0], r[1])

    @staticmethod
    def findall():
        tags = []
        rs = db.conn.execute("SELECT label,description,comment FROM tags2 ORDER BY label")
        for r in rs.fetchall():
            tags.append(Tag2(r[0], r[1], r[2]))
        return tags

    def update(self):
        q = """
UPDATE tags2
   SET description = '{1}',
       comment = '{2}'
 WHERE label = '{0}'
""".format(self.label, self.description, self.comment)

        db.conn.execute(q)

        return True

    def destroy(self):
        q = "DELETE FROM tags2 WHERE label = '{0}'".format(self.label)

        db.conn.execute(q)

        return True


class TestTag2(unittest.TestCase):
    def setUp(self):
        db.creds = {}
        db.creds['username'] = 'postgres'
        db.creds['password'] = 'postgres'
        db.creds['dbname'] = 'datacatalog'
        db.connect()
        db.conn.execute('truncate tags2 cascade')

    def tearDown(self):
        db.conn.close()

    def test_create_001(self):
        t = Tag2.create('l', 'd', 'c')
        self.assertEqual('l', t.label)
        self.assertEqual('d', t.description)
        self.assertEqual('c', t.comment)

        t = Tag2.create('l2', 'd2', 'c2')
        self.assertEqual('l2', t.label)
        self.assertEqual('d2', t.description)
        self.assertEqual('c2', t.comment)

    def test_create_002(self):
        t = Tag2.create('l', 'd', 'c')

        with self.assertRaises(sa.exc.IntegrityError) as cm:
            t = Tag2.create('l', 'd', 'c')
        self.assertTrue(str(cm.exception).startswith('(psycopg2.IntegrityError) duplicate key value violates unique constraint "tags2_pkey"'))

    def test_find_001(self):
        Tag2.create('l', 'd', 'c')

        t = Tag2.find('l')
        self.assertEqual('l', t.label)
        self.assertEqual('d', t.description)
        self.assertEqual('c', t.comment)

    def test_find_002(self):
        Tag2.create('l', 'd', 'c')

        t = Tag2.find('l2')
        self.assertIsNone(t)
        
    def test_findall_001(self):
        Tag2.create('l', 'd', 'c')

        t = Tag2.findall()
        self.assertEqual(1, len(t))
        self.assertEqual('l', t[0].label)
        self.assertEqual('d', t[0].description)
        self.assertEqual('c', t[0].comment)

        Tag2.create('l2', 'd2', 'c2')

        t = Tag2.findall()
        self.assertEqual(2, len(t))
        self.assertEqual('l', t[0].label)
        self.assertEqual('d', t[0].description)
        self.assertEqual('c', t[0].comment)
        self.assertEqual('l2', t[1].label)
        self.assertEqual('d2', t[1].description)
        self.assertEqual('c2', t[1].comment)

    def test_update_001(self):
        t = Tag2.create('l', 'd', 'c')

        t.description = 'd2'
        t.comment = 'c2'

        t.update()

        t2 = Tag2.find('l')
        self.assertEqual('l', t2.label)
        self.assertEqual('d2', t2.description)
        self.assertEqual('c2', t2.comment)

    def test_destroy_001(self):
        Tag2.create('l', 'd', 'c')
        Tag2.create('l2', 'd2', 'c2')

        t = Tag2.find('l')
        self.assertIsNotNone(t)
        self.assertIsNotNone(Tag2.find('l2'))

        t.destroy()

        self.assertIsNone(Tag2.find('l'))
        self.assertIsNotNone(Tag2.find('l2'))


if __name__ == '__main__':
    unittest.main()
