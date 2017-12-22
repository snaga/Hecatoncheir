from datetime import datetime
import json
import os
import unittest

import sqlalchemy as sa

from repository import Repository
from utils import jsonize
import db


class Schema2:
    def __init__(self, database_name, schema_name, description=None, comment=None, num_of_tables=0):
        assert isinstance(description, unicode) or description is None
        assert isinstance(comment, unicode) or comment is None

        self.database_name = database_name
        self.schema_name = schema_name
        self.description = description
        self.comment = comment
        self.num_of_tables = num_of_tables

    """
    CREATE TABLE schemas2 (
      database_name TEXT,
      schema_name TEXT,
      description TEXT NOT NULL,
      comment TEXT NOT NULL,
      PRIMARY KEY (database_name, schema_name)
    );
    """

    @staticmethod
    def create(database_name, schema_name, description=None, comment=None):
        assert isinstance(description, unicode) or description is None
        assert isinstance(comment, unicode) or comment is None

        if not description:
            description = u''
        if not comment:
            comment = u''

        q = u"""
INSERT INTO schemas2 (database_name, schema_name, description, comment)
VALUES ('{0}', '{1}', '{2}', '{3}')
""".format(database_name, schema_name, description, comment)
        rs = db.conn.execute(q)

        return Schema2(database_name, schema_name, description, comment)


    @staticmethod
    def find(database_name, schema_name):
        q = u"""
SELECT database_name,
       schema_name,
       COUNT(DISTINCT table_name)
  FROM repo
 WHERE database_name = '{0}'
   AND schema_name = '{1}'
GROUP BY
       database_name,
       schema_name
""".format(database_name, schema_name)

        rs = db.conn.execute(q)
        r = rs.fetchone()
        if r:
            num_of_tables = r[2]
        else:
            num_of_tables = 0

        # Get those schema description and comment.
        description = None
        comment = None

        q = u"""
SELECT description,
       comment
  FROM schemas2
 WHERE database_name = '{0}'
   AND schema_name = '{1}'
""".format(database_name, schema_name)
        rs = db.conn.execute(q)
        r = rs.fetchone()
        if r:
            description = r[0]
            comment = r[1]

        return Schema2(database_name, schema_name, description, comment, num_of_tables)

    @staticmethod
    def findall():
        q = u"""
SELECT DISTINCT
       database_name,
       schema_name
  FROM repo
 ORDER BY
       database_name,
       schema_name
"""
        a = []
        rs = db.conn.execute(q)
        for r in rs:
            a.append(Schema2.find(r[0], r[1]))
        return a

    def update(self):
        assert isinstance(self.description, unicode) or self.description is None
        assert isinstance(self.comment, unicode) or self.comment is None

        q = u"""
UPDATE schemas2
   SET description = '{2}',
       comment = '{3}'
 WHERE database_name = '{0}'
   AND schema_name = '{1}'
""".format(self.database_name, self.schema_name, self.description, self.comment)
        rs = db.conn.execute(q)

        return True

    def destroy(self):
        q = u"""
DELETE FROM schemas2
 WHERE database_name = '{0}'
   AND schema_name = '{1}'
""".format(self.database_name, self.schema_name)
        db.conn.execute(q)

        return True


class TestSchema2(unittest.TestCase):
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

        from table import Table2
        Table2.create('d', 's', 't', {'timestamp': '2016-04-27T10:06:41.653836'})
        Table2.create('d', 's2', 't2', {'timestamp': '2016-04-27T10:06:41.653836'})
        Table2.create('d', 's3', 't3', {'timestamp': '2016-04-27T10:06:41.653836'})

    def tearDown(self):
        db.conn.close()

    def test_find_001(self):
        s = Schema2.find('d', 's')
        self.assertTrue(isinstance(s, Schema2))
        self.assertEquals('d', s.database_name)
        self.assertEquals('s', s.schema_name)
        self.assertIsNone(s.description)
        self.assertIsNone(s.comment)
        self.assertEquals(1, s.num_of_tables)

    def test_find_002(self):
        s = Schema2.find('d', 's4')
        self.assertIsNone(s)

    def test_findall_001(self):
        a = Schema2.findall()
        self.assertEquals(3, len(a))
        self.assertEquals('d', a[0].database_name)
        self.assertEquals('s', a[0].schema_name)
        self.assertEquals('d', a[1].database_name)
        self.assertEquals('s2', a[1].schema_name)
        self.assertEquals('d', a[2].database_name)
        self.assertEquals('s3', a[2].schema_name)

    def test_update_001(self):
        Schema2.create('d', 's')
        s = Schema2.find('d', 's')
        self.assertTrue(isinstance(s, Schema2))
        self.assertEquals('d', s.database_name)
        self.assertEquals('s', s.schema_name)
        self.assertEquals('', s.description)
        self.assertEquals('', s.comment)
        self.assertEquals(1, s.num_of_tables)

        s.description = u'desc'
        s.comment = u'com'
        s.update()

        s = Schema2.find('d', 's')
        self.assertTrue(isinstance(s, Schema2))
        self.assertEquals('desc', s.description)
        self.assertEquals('com', s.comment)

    def test_destroy_001(self):
        Schema2.create('d', 's', u'desc', u'com')

        s = Schema2.find('d', 's')
        self.assertTrue(isinstance(s, Schema2))
        self.assertEquals(u'desc', s.description)
        self.assertEquals(u'com', s.comment)

        s.destroy()

        s = Schema2.find('d', 's')
        self.assertTrue(isinstance(s, Schema2))
        self.assertIsNone(s.description)
        self.assertIsNone(s.comment)

if __name__ == '__main__':
    unittest.main()
