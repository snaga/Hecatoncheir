# -*- coding: utf-8 -*-

from datetime import datetime
import json
import os
import unittest

import sqlalchemy as sa

from repository import Repository
import db


def get_datamap_items(database_name, schema_name, table_name,
                      column_name=None):
    """Get one or more datamap entries from the repository

    FIXME:
    This function is kept for the backward compatibility for now,
    and should be removed by the further refactoring.

    Args:
      database_name (str):
      schema_name_name (str):
      table_name (str):
      column_name (str):
    
    Returns:
      list: a list which consists of one or more datamap entries.
    """
    assert database_name and schema_name and table_name

    items = DatamappingItem.find(database_name, schema_name,
                                 table_name, column_name)
    datamap = []
    for item in items:
        datamap.append(json.loads(item[0]))
    return datamap


class DatamappingItem():
    def __init__(self, record_id,
                 database_name, schema_name, table_name, column_name,
                 source_database_name, source_schema_name, source_table_name,
                 source_column_name, data):
        self.record_id = record_id
        self.database_name = database_name
        self.schema_name = schema_name
        self.table_name = table_name
        self.column_name = column_name
        self.source_database_name = source_database_name
        self.source_schema_name = source_schema_name
        self.source_table_name = source_table_name
        self.source_column_name = source_column_name
        self.data = data

    @staticmethod
    def create(record_id,
               database_name=None, schema_name=None, table_name=None, column_name=None,
               source_database_name=None, source_schema_name=None, source_table_name=None,
               source_column_name=None, data=None):
        assert database_name and schema_name and table_name
        assert isinstance(data, dict)

        query = """
INSERT INTO datamapping (
  lineno,
  record_id,
  database_name,schema_name,table_name,column_name,
  source_database_name,source_schema_name,source_table_name,source_column_name,
  created_at,
  data)
 VALUES (
  {0},
  '{1}',
  '{2}', '{3}', '{4}', {5},
  {6}, {7}, {8}, {9},
  {10}, '{11}')
""".format(0,
           record_id,
           database_name, schema_name, table_name,
           db.fmt_nullable(column_name),
           db.fmt_nullable(source_database_name),
           db.fmt_nullable(source_schema_name),
           db.fmt_nullable(source_table_name),
           db.fmt_nullable(source_column_name),
           db.fmt_datetime(datetime.now().isoformat()),
           json.dumps(data))

        db.engine.execute(query)

        return DatamappingItem(record_id,
                               database_name, schema_name, table_name, column_name,
                               source_database_name, source_schema_name, source_table_name, source_column_name,
                               data)

    @staticmethod
    def find(record_id=None,
             database_name=None, schema_name=None, table_name=None, column_name=None):
        cond = ["1=1"]
        if record_id:
            cond.append("record_id = '%s'" % record_id)
        if database_name:
            cond.append("database_name = '%s'" % database_name)
        if schema_name:
            cond.append("schema_name = '%s'" % schema_name)
        if table_name:
            cond.append("table_name = '%s'" % table_name)
        if column_name:
            cond.append("column_name = '%s'" % column_name)

        query = """
SELECT
  record_id,
  database_name,
  schema_name,
  table_name,
  column_name,
  source_database_name,
  source_schema_name,
  source_table_name,
  source_column_name,
  created_at,
  data
FROM
  datamapping
WHERE
  ({0})
ORDER BY
  record_id
""".format(' AND '.join(cond))

        rs = db.engine.execute(query)
        items = []
        for r in rs:
            items.append(DatamappingItem(r[0],
                                         r[1], r[2], r[3], r[4], # target
                                         r[5], r[6], r[7], r[8], # source
                                         json.loads(r[10])))

        return items

    def update(self):
        cond = []
        cond.append("record_id = '%s'" % self.record_id)
        cond.append("database_name = '%s'" % self.database_name)
        cond.append("schema_name = '%s'" % self.schema_name)
        cond.append("table_name = '%s'" % self.table_name)
        if self.column_name:
            cond.append("column_name = '%s'" % self.column_name)
        else:
            cond.append("column_name is null")

        query = """
UPDATE datamapping
SET
  source_database_name = {0},
  source_schema_name = {1},
  source_table_name = {2},
  source_column_name = {3},
  created_at = {4},
  data = '{5}'
WHERE
  ({6})
""".format(db.fmt_nullable(self.source_database_name),
           db.fmt_nullable(self.source_schema_name),
           db.fmt_nullable(self.source_table_name),
           db.fmt_nullable(self.source_column_name),
           db.fmt_datetime(datetime.now().isoformat()),
           json.dumps(self.data),
           ' AND '.join(cond))

        db.engine.execute(query)

        return True

    def destroy(self):
        cond = []
        cond.append("record_id = '%s'" % self.record_id)
        cond.append("database_name = '%s'" % self.database_name)
        cond.append("schema_name = '%s'" % self.schema_name)
        cond.append("table_name = '%s'" % self.table_name)
        if self.column_name:
            cond.append("column_name = '%s'" % self.column_name)
        else:
            cond.append("column_name is null")

        query = "DELETE FROM datamapping WHERE ({0})".format(' AND '.join(cond))

        db.engine.execute(query)

        return True


class TestDatamappingItem(unittest.TestCase):
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

        self.maxDiff = None

    def tearDown(self):
        pass

    def test_create_001(self):
        d = DatamappingItem.create('REC001',
                                   'dqwbtest', 'test_schema', 'test_table', 'test_column',
                                   'dqwbtest', 'test_source_schema', 'test_source_table', 'test_source_column',
                                   {})

        self.assertEquals('REC001', d.record_id)
        self.assertEquals('dqwbtest', d.database_name)
        self.assertEquals('test_schema', d.schema_name)
        self.assertEquals('test_table', d.table_name)
        self.assertEquals('test_column', d.column_name)
        self.assertEquals('dqwbtest', d.source_database_name)
        self.assertEquals('test_source_schema', d.source_schema_name)
        self.assertEquals('test_source_table', d.source_table_name)
        self.assertEquals('test_source_column', d.source_column_name)
        self.assertEquals({}, d.data)

    def test_create_002(self):
        d = DatamappingItem.create('REC001',
                                   'dqwbtest', 'test_schema', 'test_table', None,
                                   'dqwbtest', 'test_source_schema', 'test_source_table', None,
                                   {})

        self.assertEquals('REC001', d.record_id)
        self.assertEquals('dqwbtest', d.database_name)
        self.assertEquals('test_schema', d.schema_name)
        self.assertEquals('test_table', d.table_name)
        self.assertIsNone(d.column_name)
        self.assertEquals('dqwbtest', d.source_database_name)
        self.assertEquals('test_source_schema', d.source_schema_name)
        self.assertEquals('test_source_table', d.source_table_name)
        self.assertIsNone(d.source_column_name)
        self.assertEquals({}, d.data)

    def test_find_001(self):
        d = DatamappingItem.create('REC001',
                                   'dqwbtest', 'test_schema', 'test_table', 'test_column',
                                   'dqwbtest', 'test_source_schema', 'test_source_table', 'test_source_column',
                                   {})
        d = DatamappingItem.create('REC002',
                                   'dqwbtest', 'test_schema', 'test_table', 'test_column2',
                                   'dqwbtest', 'test_source_schema', 'test_source_table', 'test_source_column',
                                   {})

        dd = DatamappingItem.find(record_id='REC001')
        self.assertEquals(1, len(dd))
        self.assertEquals('REC001', dd[0].record_id)
        self.assertEquals('dqwbtest', dd[0].database_name)
        self.assertEquals('test_schema', dd[0].schema_name)
        self.assertEquals('test_table', dd[0].table_name)
        self.assertEquals('test_column', dd[0].column_name)
        self.assertEquals('dqwbtest', dd[0].source_database_name)
        self.assertEquals('test_source_schema', dd[0].source_schema_name)
        self.assertEquals('test_source_table', dd[0].source_table_name)
        self.assertEquals('test_source_column', dd[0].source_column_name)
        self.assertEquals({}, dd[0].data)

        dd = DatamappingItem.find(record_id='REC002')
        self.assertEquals(1, len(dd))
        self.assertEquals('REC002', dd[0].record_id)
        self.assertEquals('test_column2', dd[0].column_name)

        dd = DatamappingItem.find(database_name='dqwbtest')
        self.assertEquals(2, len(dd))
        self.assertEquals('REC001', dd[0].record_id)
        self.assertEquals('REC002', dd[1].record_id)

        dd = DatamappingItem.find(schema_name='test_schema')
        self.assertEquals(2, len(dd))
        self.assertEquals('REC001', dd[0].record_id)
        self.assertEquals('REC002', dd[1].record_id)

        dd = DatamappingItem.find(table_name='test_table')
        self.assertEquals(2, len(dd))
        self.assertEquals('REC001', dd[0].record_id)
        self.assertEquals('REC002', dd[1].record_id)

        dd = DatamappingItem.find(column_name='test_column')
        self.assertEquals(1, len(dd))
        self.assertEquals('REC001', dd[0].record_id)

        dd = DatamappingItem.find(database_name='nosuchdb')
        self.assertEquals(0, len(dd))

    def test_find_002(self):
        d = DatamappingItem.create('REC001',
                                   'dqwbtest', 'test_schema', 'test_table', None,
                                   'dqwbtest', 'test_source_schema', 'test_source_table', None,
                                   {})

        dd = DatamappingItem.find(record_id='REC001')
        d = dd[0]
        self.assertEquals('REC001', d.record_id)
        self.assertEquals('dqwbtest', d.database_name)
        self.assertEquals('test_schema', d.schema_name)
        self.assertEquals('test_table', d.table_name)
        self.assertIsNone(d.column_name)
        self.assertEquals('dqwbtest', d.source_database_name)
        self.assertEquals('test_source_schema', d.source_schema_name)
        self.assertEquals('test_source_table', d.source_table_name)
        self.assertIsNone(d.source_column_name)
        self.assertEquals({}, d.data)

    def test_update_001(self):
        DatamappingItem.create('REC001',
                               'dqwbtest', 'test_schema', 'test_table', 'test_column',
                               'dqwbtest', 'test_source_schema', 'test_source_table', 'test_source_column',
                               {})
        d = DatamappingItem.find(record_id='REC001')[0]

        self.assertEquals('REC001', d.record_id)
        self.assertEquals('dqwbtest', d.database_name)
        self.assertEquals('test_schema', d.schema_name)
        self.assertEquals('test_table', d.table_name)
        self.assertEquals('test_column', d.column_name)
        self.assertEquals('dqwbtest', d.source_database_name)
        self.assertEquals('test_source_schema', d.source_schema_name)
        self.assertEquals('test_source_table', d.source_table_name)
        self.assertEquals('test_source_column', d.source_column_name)
        self.assertEquals({}, d.data)

        d.source_database_name = 'dqwbtest2'
        d.source_schema_name = 'test_source_schema2'
        d.source_table_name = 'test_source_table2'
        d.source_column_name = 'test_source_column2'
        d.data = {'foo': 'bar'}

        self.assertTrue(d.update())

        self.assertEquals('REC001', d.record_id)
        self.assertEquals('dqwbtest', d.database_name)
        self.assertEquals('test_schema', d.schema_name)
        self.assertEquals('test_table', d.table_name)
        self.assertEquals('test_column', d.column_name)
        self.assertEquals('dqwbtest2', d.source_database_name)
        self.assertEquals('test_source_schema2', d.source_schema_name)
        self.assertEquals('test_source_table2', d.source_table_name)
        self.assertEquals('test_source_column2', d.source_column_name)
        self.assertEquals({'foo': 'bar'}, d.data)

        d.source_column_name = None
        self.assertTrue(d.update())
        self.assertIsNone(d.source_column_name)

        d.source_column_name = 'foo'
        self.assertTrue(d.update())
        self.assertEquals('foo', d.source_column_name)

    def test_destroy_001(self):
        DatamappingItem.create('REC001',
                               'dqwbtest', 'test_schema', 'test_table', 'test_column',
                               'dqwbtest', 'test_source_schema', 'test_source_table', 'test_source_column',
                               {})
        DatamappingItem.create('REC002',
                               'dqwbtest', 'test_schema', 'test_table', None,
                               'dqwbtest', 'test_source_schema', 'test_source_table', 'test_source_column',
                               {})
        self.assertEquals(2, len(DatamappingItem.find()))

        d = DatamappingItem.find(record_id='REC002')[0]
        self.assertTrue(d.destroy())

        self.assertEquals(1, len(DatamappingItem.find()))

        d = DatamappingItem.find()[0]
        self.assertEquals('REC001', d.record_id)
        self.assertTrue(d.destroy())

        self.assertEquals(0, len(DatamappingItem.find()))


if __name__ == '__main__':
    unittest.main()
