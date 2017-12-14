from datetime import datetime
import json
import unittest

import sqlalchemy as sa

from utils import jsonize
import db


class Table2:
    def __init__(self, database_name, schema_name, table_name, data):
        self.database_name = database_name
        self.schema_name = schema_name
        self.table_name = table_name
        self.data = data

    @staticmethod
    def create(database_name, schema_name, table_name, data):
        q = """
INSERT INTO repo VALUES ('{0}','{1}','{2}','{3}','{4}')
""".format(database_name, schema_name, table_name,
           datetime.now().strftime("%Y-%m-%dT%H:%M:%S.%f"),
           jsonize(data).replace("'", "''"))

        db.conn.execute(q)

        return Table2(database_name, schema_name, table_name, data)

    @staticmethod
    def find(database_name=None, schema_name=None, table_name=None, tag=None):
        q = """
SELECT database_name,
       schema_name,
       table_name,
       data
  FROM repo
"""

        conds = ['1=1']
        if database_name:
            conds.append("database_name = '%s'" % database_name)
        if schema_name:
            conds.append("schema_name = '%s'" % schema_name)
        if table_name:
            conds.append("table_name = '%s'" % table_name)

        q = q + ' WHERE ' + ' AND '.join(conds)
        q = q + ' ORDER BY database_name,schema_name,table_name,created_at DESC'

        tables = []
        rs = db.conn.execute(q)
        found = []
        for r in rs:
            # pick only the latest table data.
            if (r[0], r[1], r[2]) in found:
                continue
            data = json.loads(r[3])
            if tag:
                if not data.get('tags') or tag not in data.get('tags'):
                    continue
            tables.append(Table2(r[0], r[1], r[2], data))
            found.append((r[0], r[1], r[2]))
        return tables

    def _update_tags(self):
        # Update the table-tag mappings
        tagid = '%s.%s.%s' % (self.database_name, self.schema_name, self.table_name)
        q = "DELETE FROM tags WHERE tag_id = '%s'" % tagid
        db.conn.execute(q)

        if not self.data.get('tags'):
            return

        for tag in self.data.get('tags'):
            q = "INSERT INTO tags (tag_id, tag_label) VALUES ('%s', '%s')" % (tagid, tag)
            db.conn.execute(q)

    def update(self):
        # Insert the latest record of the table
        q = """
INSERT INTO repo VALUES ('{0}','{1}','{2}','{3}','{4}')
""".format(self.database_name, self.schema_name, self.table_name,
           datetime.now().strftime("%Y-%m-%dT%H:%M:%S.%f"),
           jsonize(self.data).replace("'", "''"))
        db.conn.execute(q)

        self._update_tags()

        return True

    def destroy(self):
        q = """
DELETE FROM repo
 WHERE database_name = '{0}'
   AND schema_name = '{1}'
   AND table_name = '{2}'
""".format(self.database_name, self.schema_name, self.table_name)

        db.conn.execute(q)
        return True


class TestTable2(unittest.TestCase):
    def setUp(self):
        db.creds = {}
        db.creds['username'] = 'postgres'
        db.creds['password'] = 'postgres'
        db.creds['dbname'] = 'datacatalog'
        db.connect()
        db.conn.execute('truncate repo cascade')

    def tearDown(self):
        db.conn.close()

    def test_create_001(self):
        t = Table2.create('d', 's', 't', {'timestamp': '2016-04-27T10:06:41.653836'})
        self.assertEquals('d', t.database_name)
        self.assertEquals('s', t.schema_name)
        self.assertEquals('t', t.table_name)
        self.assertEquals({'timestamp': '2016-04-27T10:06:41.653836'}, t.data)

        t = Table2.create('d', 's', 't', {'timestamp': '2016-05-27T10:06:41.653836'})
        self.assertEquals('d', t.database_name)
        self.assertEquals('s', t.schema_name)
        self.assertEquals('t', t.table_name)
        self.assertEquals({'timestamp': '2016-05-27T10:06:41.653836'}, t.data)

        t = Table2.create('d2', 's', 't', {'timestamp': '2016-05-27T10:06:41.653836'})
        self.assertEquals('d2', t.database_name)
        self.assertEquals('s', t.schema_name)
        self.assertEquals('t', t.table_name)
        self.assertEquals({'timestamp': '2016-05-27T10:06:41.653836'}, t.data)

    def test_find_001(self):
        t = Table2.create('d', 's', 't', {'timestamp': '2016-04-27T10:06:41.653836'})
        self.assertIsNotNone(t)
        t = Table2.create('d', 's', 't', {'timestamp': '2016-05-27T10:06:41.653836'})
        self.assertIsNotNone(t)
        t = Table2.create('d2', 's', 't', {'timestamp': '2016-05-27T10:06:41.653836'})
        self.assertIsNotNone(t)

        t = Table2.find('d', 's', 't')
        self.assertEquals(1, len(t))
        self.assertEquals('d', t[0].database_name)
        self.assertEquals('s', t[0].schema_name)
        self.assertEquals('t', t[0].table_name)
        self.assertEquals({'timestamp': '2016-05-27T10:06:41.653836'}, t[0].data)

        t = Table2.find('d2', 's', 't')
        self.assertEquals(1, len(t))
        self.assertEquals('d2', t[0].database_name)
        self.assertEquals('s', t[0].schema_name)
        self.assertEquals('t', t[0].table_name)
        self.assertEquals({'timestamp': '2016-05-27T10:06:41.653836'}, t[0].data)

        t = Table2.find(schema_name='s')
        self.assertEquals(2, len(t))
        self.assertEquals('d', t[0].database_name)
        self.assertEquals('s', t[0].schema_name)
        self.assertEquals('t', t[0].table_name)
        self.assertEquals({'timestamp': '2016-05-27T10:06:41.653836'}, t[0].data)
        self.assertEquals('d2', t[1].database_name)
        self.assertEquals('s', t[1].schema_name)
        self.assertEquals('t', t[1].table_name)
        self.assertEquals({'timestamp': '2016-05-27T10:06:41.653836'}, t[1].data)

    def test_find_002(self):
        # find by tag
        t = Table2.create('d', 's', 't', {'timestamp': '2016-04-27T10:06:41.653836',
                                          'tags': ['tag1', 'tag2']})
        self.assertIsNotNone(t)
        t = Table2.create('d', 's', 't2', {'timestamp': '2016-05-27T10:06:41.653836',
                                           'tags': ['tag1', 'tag3']})
        self.assertIsNotNone(t)
        t = Table2.create('d', 's', 't3', {'timestamp': '2016-05-27T10:06:41.653836'})
        self.assertIsNotNone(t)

        t = Table2.find(tag='tag1')
        self.assertEquals(2, len(t))
        self.assertEquals('t', t[0].table_name)
        self.assertEquals('t2', t[1].table_name)

        t = Table2.find(tag='tag2')
        self.assertEquals(1, len(t))
        self.assertEquals('t', t[0].table_name)

        t = Table2.find(tag='tag3')
        self.assertEquals(1, len(t))
        self.assertEquals('t2', t[0].table_name)

        t = Table2.find(tag='tag4')
        self.assertEquals(0, len(t))

    def test_update_001(self):
        t = Table2.create('d', 's', 't', {'timestamp': '2016-04-27T10:06:41.653836'})
        t = Table2.find('d', 's', 't')
        self.assertEquals({'timestamp': '2016-04-27T10:06:41.653836'}, t[0].data)

        now = datetime.now().strftime("%Y-%m-%dT%H:%M:%S")
        t[0].data['timestamp'] = now
        t[0].update()

        t = Table2.find('d', 's', 't')
        self.assertEquals({'timestamp': now}, t[0].data)

    def print_table(self, table):
        rs = db.conn.execute('SELECT * FROM %s' % table)
        for r in rs:
            print('[%s] ' % table + ','.join(r).replace('\n', '\\n'))

    def test_update_002(self):
        t = Table2.create('d', 's', 't', {'timestamp': '2016-04-27T10:06:41.653836'})
        t = Table2.find('d', 's', 't')
        self.assertIsNone(t[0].data.get('tags'))

        t[0].data['tags'] = ['t1']
        t[0].data['timestamp'] = '2016-04-27T10:06:41.653837'
        t[0].update()

        t = Table2.find('d', 's', 't')
        self.assertEquals(['t1'], t[0].data.get('tags'))

        t[0].data['tags'] = ['t1', 't2']
        t[0].data['timestamp'] = '2016-04-27T10:06:41.653838'
        t[0].update()

        t = Table2.find('d', 's', 't')
        self.assertEquals(['t1', 't2'], t[0].data.get('tags'))

    def test_destroy_001(self):
        t = Table2.create('d', 's', 't', {'timestamp': '2016-04-27T10:06:41.653836'})
        self.assertIsNotNone(t)
        t = Table2.create('d', 's', 't', {'timestamp': '2016-05-27T10:06:41.653836'})
        self.assertIsNotNone(t)
        t = Table2.create('d2', 's', 't', {'timestamp': '2016-05-27T10:06:41.653836'})
        self.assertIsNotNone(t)

        # find and destory
        t = Table2.find('d', 's', 't')
        self.assertEquals(1, len(t))

        t[0].destroy()

        # already destroyed
        t = Table2.find('d', 's', 't')
        self.assertEquals(0, len(t))

        # still alive
        t = Table2.find('d2', 's', 't')
        self.assertEquals(1, len(t))

if __name__ == '__main__':
    unittest.main()
