#!/usr/bin/env python2.7
# -*- coding: utf-8 -*-

import os
import sys
import time
import unittest
sys.path.append('..')

from hecatoncheir import DbProfilerRepository
from hecatoncheir.exception import InternalError
from hecatoncheir.metadata import Tag, TagDesc, SchemaDesc

class TestDbProfilerRepository(unittest.TestCase):
    repo = None

    def setUp(self):
        fname = "foo.db"
        self.repo = DbProfilerRepository.DbProfilerRepository(filename=fname)
#        fname = "datacatalog"
#        self.repo = DbProfilerRepository.DbProfilerRepository(filename=fname, host='localhost', user='snaga')
        self.repo.init()
        self.repo.open()
        self.maxDiff = None

    def tearDown(self):
        self.repo.close()
        self.repo.destroy()

    def testDbProfilerRepository_001(self):
        repo = DbProfilerRepository.DbProfilerRepository()
        self.assertEqual("repo.db", repo.filename)

        repo = DbProfilerRepository.DbProfilerRepository(filename="myrepo.db")
        self.assertEqual("myrepo.db", repo.filename)

        repo = DbProfilerRepository.DbProfilerRepository(None)
        self.assertEqual("repo.db", repo.filename)

    def testInit_001(self):
        fname = "foo.db"
        if os.path.exists(fname) is True:
            os.unlink(fname)
        self.assertFalse(os.path.exists(fname))

        repo = DbProfilerRepository.DbProfilerRepository(fname)
        self.assertTrue(repo.init())
        if not repo.use_pgsql:
            self.assertTrue(os.path.exists(fname))
        # already exists
        self.assertTrue(repo.init())
        if not repo.use_pgsql:
            self.assertTrue(os.path.exists(fname))

        fname = "/foo.db"
        repo = DbProfilerRepository.DbProfilerRepository(fname)
        if repo.use_pgsql:
            self.assertTrue(repo.init())
        else:
            self.assertFalse(repo.init())
        if not repo.use_pgsql:
            self.assertFalse(os.path.exists(fname))

    def testDestroy_001(self):
        fname = "foo.db"
        if os.path.exists(fname) is True:
            os.unlink(fname)
        self.assertFalse(os.path.exists(fname))

        repo = DbProfilerRepository.DbProfilerRepository(fname)
        self.assertTrue(repo.init())
        if not repo.use_pgsql:
            self.assertTrue(os.path.exists(fname))

        self.assertTrue(repo.destroy())
        if not repo.use_pgsql:
            self.assertFalse(os.path.exists(fname))

        # permission denied
        repo = DbProfilerRepository.DbProfilerRepository("/etc/passwd")
        self.assertFalse(repo.destroy())
        if not repo.use_pgsql:
            self.assertTrue(os.path.exists("/etc/passwd"))

    def testAppend_table_001(self):
        t = {}
        t['database_name'] = u'test_database'
        t['schema_name'] = u'test_schema'
        t['table_name'] = u'test_table'
        t['timestamp'] = '2016-04-27T10:06:41.653836'

        self.assertTrue(self.repo.append_table(t))
        self.assertTrue(self.repo.append_table(t))

        # fail with query error
        t = {}
        t['database_name'] = u'test\'_database'
        t['schema_name'] = u'test_schema'
        t['table_name'] = u'test_table'
        t['timestamp'] = '2016-04-27T10:06:41.653836'

        with self.assertRaises(InternalError) as cm:
            self.repo.append_table(t)
        self.assertTrue(cm.exception.value.startswith('append_table() failed: '))

    def testAppend_table_002(self):
        # different timestamp must go to the different records.
        t = {}
        t['database_name'] = u'test_database'
        t['schema_name'] = u'test_schema'
        t['table_name'] = u'test_table'
        t['timestamp'] = '2016-04-27T10:06:41.653836'
        self.assertTrue(self.repo.append_table(t))

        t = {}
        t['database_name'] = u'test_database'
        t['schema_name'] = u'test_schema'
        t['table_name'] = u'test_table'
        t['timestamp'] = '2016-04-28T10:06:41.653836'
        self.assertTrue(self.repo.append_table(t))

        d = self.repo.get()
        self.assertEqual(2, len(d))

        self.assertEqual('test_database', d[0]['database_name'])
        self.assertEqual('test_schema', d[0]['schema_name'])
        self.assertEqual('test_table', d[0]['table_name'])
        self.assertEqual('2016-04-27T10:06:41.653836', d[0]['timestamp'])

        self.assertEqual('test_database', d[1]['database_name'])
        self.assertEqual('test_schema', d[1]['schema_name'])
        self.assertEqual('test_table', d[1]['table_name'])
        self.assertEqual('2016-04-28T10:06:41.653836', d[1]['timestamp'])

    def testAppend_table_003(self):
        # append table records with tags
        t = {}
        t['database_name'] = u'test_database'
        t['schema_name'] = u'test_schema'
        t['table_name'] = u'test_table'
        t['timestamp'] = '2016-04-27T10:06:41.653836'
        t['tags'] = [u'tag1', u'tag2']

        self.assertTrue(self.repo.append_table(t))

        t = {}
        t['database_name'] = u'test_database'
        t['schema_name'] = u'test_schema'
        t['table_name'] = u'test_table2'
        t['timestamp'] = '2016-04-27T10:06:41.653836'
        t['tags'] = [u'tag2', u'tag3']

        self.assertTrue(self.repo.append_table(t))

        t = self.repo.get_table('test_database', 'test_schema', 'test_table')
        self.assertEqual(['tag1', 'tag2'], t['tags'])

        t = self.repo.get_table('test_database', 'test_schema', 'test_table2')
        self.assertEqual(['tag2', 'tag3'], t['tags'])

    def testGet_table_001(self):
        tab = self.repo.get_table('test_database', 'test_schema', 'test_table')
        self.assertIsNone(tab)

        t = {}
        t['database_name'] = u'test_database'
        t['schema_name'] = u'test_schema'
        t['table_name'] = u'test_table'
        t['timestamp'] = '2016-04-27T10:06:41.653836'
        self.assertTrue(self.repo.append_table(t))

        t = {}
        t['database_name'] = u'test_database'
        t['schema_name'] = u'test_schema'
        t['table_name'] = u'test_table'
        t['timestamp'] = '2016-04-27T10:06:40.653836'
        self.assertTrue(self.repo.append_table(t))

        tab = self.repo.get_table('test_database', 'test_schema', 'test_table')
        self.assertEqual('test_database', tab['database_name'])
        self.assertEqual('test_schema', tab['schema_name'])
        self.assertEqual('test_table', tab['table_name'])
        self.assertEqual('2016-04-27T10:06:41.653836', tab['timestamp'])

        # fail with query error
        with self.assertRaises(InternalError) as cm:
            self.repo.get_table('test\'_database', 'test_schema', 'test_table')
        self.assertTrue(cm.exception.value.startswith("Could not get table data: "))

    def test_remove_table_001(self):
        t = {}
        t['database_name'] = u'test_database'
        t['schema_name'] = u'test_schema'
        t['table_name'] = u'test_table'
        t['timestamp'] = '2016-04-27T10:06:41.653836'

        # remove single entry at once.
        self.assertIsNone(self.repo.get_table('test_database', 'test_schema', 'test_table'))
        self.assertTrue(self.repo.append_table(t))
        self.assertIsNotNone(self.repo.get_table('test_database', 'test_schema', 'test_table'))

        self.assertTrue(self.repo.remove_table('test_database', 'test_schema', 'test_table'))
        self.assertIsNone(self.repo.get_table('test_database', 'test_schema', 'test_table'))

        # remove two entries at once.
        self.assertTrue(self.repo.append_table(t))
        self.assertTrue(self.repo.append_table(t))
        self.assertIsNotNone(self.repo.get_table('test_database', 'test_schema', 'test_table'))
        self.assertTrue(self.repo.remove_table('test_database', 'test_schema', 'test_table'))
        self.assertIsNone(self.repo.get_table('test_database', 'test_schema', 'test_table'))

        # fail with query error
        with self.assertRaises(InternalError) as cm:
            self.repo.remove_table('test\'_database', 'test_schema', 'test_table')
        self.assertTrue(cm.exception.value.startswith("Could not remove table data: "))

    def testSet_001(self):
        t = {}
        t['database_name'] = u'test_database'
        t['schema_name'] = u'test_schema'
        t['table_name'] = u'test_table'
        t['timestamp'] = '2016-04-27T10:06:41.653836'
        d = []
        d.append(t)

        self.repo.set(d)

        data_all = self.repo.get()
        self.assertIsNotNone(data_all)
        self.assertEqual(1, len(data_all))
        self.assertEqual('test_database', data_all[0]['database_name'])
        self.assertEqual('test_schema', data_all[0]['schema_name'])
        self.assertEqual('test_table', data_all[0]['table_name'])
        self.assertEqual('2016-04-27T10:06:41.653836', data_all[0]['timestamp'])

    def testGet_001(self):
        t = {}
        t['database_name'] = u'test_database'
        t['schema_name'] = u'test_schema'
        t['table_name'] = u'test_table'
        t['timestamp'] = '2016-04-27T10:06:41.653836'
        d = []
        d.append(t)

        self.repo.set(d)
        d2 = self.repo.get()

        self.assertIsNotNone(d2)
        self.assertEqual(1, len(d2))
        self.assertEqual('test_database', d2[0]['database_name'])
        self.assertEqual('test_schema', d2[0]['schema_name'])
        self.assertEqual('test_table', d2[0]['table_name'])
        self.assertEqual('2016-04-27T10:06:41.653836', d2[0]['timestamp'])

    def testGet_table_002(self):
        t = {}
        t['database_name'] = u'test_database'
        t['schema_name'] = u'test_schema'
        t['table_name'] = u'test_table'
        t['timestamp'] = '2016-04-27T10:06:41.653836'

        self.assertTrue(self.repo.append_table(t))

        t2 = self.repo.get_table('nosuchdatabase', 'test_schema', 'test_table')
        self.assertIsNone(t2)

        t2 = self.repo.get_table('test_database', 'nosuchschema', 'test_table')
        self.assertIsNone(t2)

        t2 = self.repo.get_table('test_database', 'test_schema', 'nosuchtable')
        self.assertIsNone(t2)

    def testGet_table_003(self):
        t = {}
        t['database_name'] = u'test_database'
        t['schema_name'] = u'test_schema'
        t['table_name'] = u'test_table'
        t['timestamp'] = '2016-04-27T10:06:41.653836'
        self.assertTrue(self.repo.append_table(t))

        t = {}
        t['database_name'] = u'test_database'
        t['schema_name'] = u'test_schema'
        t['table_name'] = u'test_table'
        t['timestamp'] = '2016-04-28T10:06:41.653836'
        self.assertTrue(self.repo.append_table(t))

        t = {}
        t['database_name'] = u'test_database'
        t['schema_name'] = u'test_schema'
        t['table_name'] = u'test_table'
        t['timestamp'] = '2016-04-26T10:06:41.653836'
        self.assertTrue(self.repo.append_table(t))

        t2 = self.repo.get_table('test_database', 'test_schema', 'test_table')

        self.assertIsNotNone(t2)
        self.assertEqual('test_database', t2['database_name'])
        self.assertEqual('test_schema', t2['schema_name'])
        self.assertEqual('test_table', t2['table_name'])
        self.assertEqual('2016-04-28T10:06:41.653836', t2['timestamp']) # latest entry

    def testGet_table_list_001(self):
        t = {}
        t['database_name'] = u'test_database'
        t['schema_name'] = u'test_schema'
        t['table_name'] = u'test_table'
        t['timestamp'] = '2016-04-27T10:06:41.653836'

        self.assertTrue(self.repo.append_table(t))

        tlist = self.repo.get_table_list()
        self.assertEqual(1, len(tlist))
        self.assertEqual('test_database', tlist[0][0])
        self.assertEqual('test_schema', tlist[0][1])
        self.assertEqual('test_table', tlist[0][2])

    def testGet_table_list_002(self):
        t = {}
        t['database_name'] = u'test_database'
        t['schema_name'] = u'test_schema'
        t['table_name'] = u'test_table'
        t['timestamp'] = '2016-04-27T10:06:41.653836'

        self.assertTrue(self.repo.append_table(t))

        t = {}
        t['database_name'] = u'test_database'
        t['schema_name'] = u'test_schema'
        t['table_name'] = u'test_table'
        t['timestamp'] = '2016-04-28T10:06:41.653836'

        self.assertTrue(self.repo.append_table(t))
        self.assertEqual(2, len(self.repo.get()))

        tlist = self.repo.get_table_list()
        self.assertEqual(1, len(tlist))
        self.assertEqual('test_database', tlist[0][0])
        self.assertEqual('test_schema', tlist[0][1])
        self.assertEqual('test_table', tlist[0][2])

    def testGet_table_list_003(self):
        t = {}
        t['database_name'] = u'test_database'
        t['schema_name'] = u'test_schema'
        t['table_name'] = u'test_table'
        t['timestamp'] = '2016-04-27T10:06:41.653836'

        self.assertTrue(self.repo.append_table(t))

        t = {}
        t['database_name'] = u'test_database'
        t['schema_name'] = u'test_schema'
        t['table_name'] = u'test_table2'
        t['timestamp'] = '2016-04-27T10:06:41.653836'

        self.assertTrue(self.repo.append_table(t))
        self.assertEqual(2, len(self.repo.get()))

        tlist = self.repo.get_table_list()
        self.assertEqual(2, len(tlist))
        self.assertEqual('test_database', tlist[0][0])
        self.assertEqual('test_schema', tlist[0][1])
        self.assertEqual('test_table', tlist[0][2])
        self.assertEqual('test_database', tlist[1][0])
        self.assertEqual('test_schema', tlist[1][1])
        self.assertEqual('test_table2', tlist[1][2])

    def testGet_table_list_004(self):
        t = {}
        t['database_name'] = u'test_database'
        t['schema_name'] = u'test_schema'
        t['table_name'] = u'test_table'
        t['timestamp'] = '2016-04-27T10:06:41.653836'
        self.assertTrue(self.repo.append_table(t))

        t = {}
        t['database_name'] = u'test_database'
        t['schema_name'] = u'test_schema'
        t['table_name'] = u'test_table2'
        t['timestamp'] = '2016-04-27T10:06:41.653836'
        self.assertTrue(self.repo.append_table(t))

        t = {}
        t['database_name'] = u'test_database'
        t['schema_name'] = u'test_schema3'
        t['table_name'] = u'test_table'
        t['timestamp'] = '2016-04-27T10:06:41.653836'
        self.assertTrue(self.repo.append_table(t))

        t = {}
        t['database_name'] = u'test_database4'
        t['schema_name'] = u'test_schema'
        t['table_name'] = u'test_table'
        t['timestamp'] = '2016-04-27T10:06:41.653836'
        self.assertTrue(self.repo.append_table(t))

        # by database name
        tlist = self.repo.get_table_list(database_name='test_database4')
        self.assertEqual(1, len(tlist))
        self.assertEqual(['test_database4', 'test_schema', 'test_table'], tlist[0])

        tlist = self.repo.get_table_list(database_name='test_database')
        self.assertEqual(3, len(tlist))

        # by schema name
        tlist = self.repo.get_table_list(schema_name='test_schema3')
        self.assertEqual(1, len(tlist))
        self.assertEqual(['test_database', 'test_schema3', 'test_table'], tlist[0])

        tlist = self.repo.get_table_list(schema_name='test_schema')
        self.assertEqual(3, len(tlist))

        # by table name
        tlist = self.repo.get_table_list(table_name='test_table2')
        self.assertEqual(1, len(tlist))
        self.assertEqual(['test_database', 'test_schema', 'test_table2'], tlist[0])

        tlist = self.repo.get_table_list(table_name='test_table')
        self.assertEqual(3, len(tlist))

        # by table name
        tlist = self.repo.get_table_list(database_name='test_database',
                                         schema_name='test_schema',
                                         table_name='test_table')
        self.assertEqual(1, len(tlist))
        self.assertEqual(['test_database', 'test_schema', 'test_table'], tlist[0])


    def testGet_table_list_005(self):
        t = {}
        t['database_name'] = u'test_database'
        t['schema_name'] = u'test_schema'
        t['table_name'] = u'test_table'
        t['timestamp'] = '2016-04-27T10:06:41.653836'
        t['tags'] = [u'tag1', u'tag2']

        self.assertTrue(self.repo.append_table(t))

        t = {}
        t['database_name'] = u'test_database'
        t['schema_name'] = u'test_schema'
        t['table_name'] = u'test_table2'
        t['timestamp'] = '2016-04-27T10:06:41.653836'
        t['tags'] = [u'tag1', u'tag3']
        self.assertTrue(self.repo.append_table(t))

        # by tag
        tlist = self.repo.get_table_list(tag='tag1')
        self.assertEqual(2, len(tlist))
        self.assertEqual([u'test_database', u'test_schema', u'test_table'],
                         tlist[0])
        self.assertEqual([u'test_database', u'test_schema', u'test_table2'],
                         tlist[1])

        tlist = self.repo.get_table_list(tag='tag2')
        self.assertEqual(1, len(tlist))
        self.assertEqual([u'test_database', u'test_schema', u'test_table'],
                         tlist[0])

        tlist = self.repo.get_table_list(tag='tag3')
        self.assertEqual(1, len(tlist))
        self.assertEqual([u'test_database', u'test_schema', u'test_table2'],
                         tlist[0])


    def testGet_table_history_001(self):
        t = {}
        t['database_name'] = u'test_database'
        t['schema_name'] = u'test_schema'
        t['table_name'] = u'test_table'
        t['timestamp'] = '2016-04-27T10:06:41.653836'
        self.assertTrue(self.repo.append_table(t))

        t = {}
        t['database_name'] = u'test_database'
        t['schema_name'] = u'test_schema'
        t['table_name'] = u'test_table'
        t['timestamp'] = '2016-04-28T10:06:41.653836'
        self.assertTrue(self.repo.append_table(t))

        t = {}
        t['database_name'] = u'test_database'
        t['schema_name'] = u'test_schema'
        t['table_name'] = u'test_table'
        t['timestamp'] = '2016-04-26T10:06:41.653836'
        self.assertTrue(self.repo.append_table(t))

        thist = self.repo.get_table_history('test_database', 'test_schema', 'test_table')
        self.assertEqual(3, len(thist))
        self.assertEqual('2016-04-28T10:06:41.653836', thist[0]['timestamp']) # latest
        self.assertEqual('2016-04-27T10:06:41.653836', thist[1]['timestamp'])
        self.assertEqual('2016-04-26T10:06:41.653836', thist[2]['timestamp']) # oldest

        # fail
        with self.assertRaises(InternalError) as cm:
            self.repo.get_table_history('test\'_database', 'test_schema', 'test_table')
        self.assertTrue(cm.exception.value.startswith("Could not get table data with its history: "))

    def test_put_datamap_item_001(self):
        dm = {}
        dm['lineno'] = 1
        dm['database_name'] = 'dqwbtest'
        dm['schema_name'] = 'test_schema'
        dm['table_name'] = 'test_table'
        dm['column_name'] = 'test_column'
        dm['record_id'] = 'REC001'

        dm['source_database_name'] = 'dqwbtest'
        dm['source_schema_name'] = 'test_source_schema'
        dm['source_table_name'] = 'test_source_table'
        dm['source_column_name'] = 'test_source_column'
        res = self.repo.put_datamap_item(dm)

        self.assertTrue(res)

        d = 'dqwbtest'
        s = 'test_schema'
        t = 'test_table'
        c = 'test_column'
        dm2 = self.repo.get_datamap_items(d,s,t,c)
        self.assertEqual(1, len(dm2))

        self.assertEqual([{u'lineno': 1, u'record_id': u'REC001', u'source_schema_name': u'test_source_schema', u'source_database_name': u'dqwbtest', u'database_name': u'dqwbtest', u'table_name': u'test_table', u'schema_name': u'test_schema', u'source_column_name': u'test_source_column', u'source_table_name': u'test_source_table', u'column_name': u'test_column'}], dm2)

    def test_get_datamap_001(self):
        dm = {}
        dm['lineno'] = 1
        dm['database_name'] = 'dqwbtest'
        dm['schema_name'] = 'test_schema'
        dm['table_name'] = 'test_table'
        dm['column_name'] = 'test_column'
        dm['record_id'] = 'REC001'

        dm['source_database_name'] = 'dqwbtest'
        dm['source_schema_name'] = 'test_source_schema'
        dm['source_table_name'] = 'test_source_table'
        dm['source_column_name'] = 'test_source_column'
        res = self.repo.put_datamap_item(dm)

        dm['lineno'] = 2
        dm['record_id'] = 'REC002'
        self.repo.put_datamap_item(dm)

        d = 'dqwbtest'
        s = 'test_schema'
        t = 'test_table'
        d = self.repo.get_datamap_items(d,s,t)
        self.assertEquals(2, len(d))
        self.assertEquals(d[0]['record_id'], 'REC001')
        self.assertEquals(d[1]['record_id'], 'REC002')

    def test_get_schemas_001(self):
        t = {}
        t['database_name'] = u'test_database'
        t['schema_name'] = u'test_schema'
        t['table_name'] = u'test_table'
        t['timestamp'] = '2016-04-27T10:06:41.653836'
        self.assertTrue(self.repo.append_table(t))

        t['database_name'] = u'test_database'
        t['schema_name'] = u'test_schema'
        t['table_name'] = u'test_table2'
        t['timestamp'] = '2016-04-27T10:06:41.653836'
        self.assertTrue(self.repo.append_table(t))

        t['database_name'] = u'test_database'
        t['schema_name'] = u'test_schema2'
        t['table_name'] = u'test_table'
        t['timestamp'] = '2016-04-27T10:06:41.653836'
        self.assertTrue(self.repo.append_table(t))

        self.assertEqual([[u'test_database', u'test_schema', 2],
                          [u'test_database', u'test_schema2', 1]],
                         self.repo.get_schemas())

        # fail
        repo = DbProfilerRepository.DbProfilerRepository("/dev/null")
        with self.assertRaises(InternalError) as cm:
            repo.get_schemas()
        self.assertTrue(cm.exception.value.startswith("Could not get schema names: "))

    def test_get_schemas_002(self):
        t = {}
        t['database_name'] = u'test_database'
        t['schema_name'] = u'test_schema'
        t['table_name'] = u'test_table'
        t['timestamp'] = '2016-04-27T10:06:41.653836'
        self.assertTrue(self.repo.append_table(t))

        t['database_name'] = u'test_database2'
        t['schema_name'] = u'test_schema2'
        t['table_name'] = u'test_table2'
        t['timestamp'] = '2016-04-27T10:06:41.653836'
        self.assertTrue(self.repo.append_table(t))

        self.assertEqual([[u'test_database', u'test_schema', 1]],
                         self.repo.get_schemas('test_database'))

        self.assertEqual([[u'test_database2', u'test_schema2', 1]],
                         self.repo.get_schemas('test_database2'))

    def test_get_table_count_001(self):
        t = {}
        t['database_name'] = u'test_database'
        t['schema_name'] = u'test_schema'
        t['table_name'] = u'test_table'
        t['timestamp'] = '2016-04-27T10:06:41.653836'
        t['tags'] = [u'tag1',u'tag2']
        self.assertTrue(self.repo.append_table(t))

        self.assertEqual(1, self.repo.get_table_count_by_tag(u'tag1'))
        self.assertEqual(1, self.repo.get_table_count_by_tag(u'tag2'))
        self.assertEqual(0, self.repo.get_table_count_by_tag(u'tag3'))

        t['database_name'] = u'test_database'
        t['schema_name'] = u'test_schema'
        t['table_name'] = u'test_table2'
        t['timestamp'] = '2016-04-27T10:06:41.653836'
        t['tags'] = [u'tag1']
        self.assertTrue(self.repo.append_table(t))

        self.assertEqual(2, self.repo.get_table_count_by_tag(u'tag1'))
        self.assertEqual(1, self.repo.get_table_count_by_tag(u'tag2'))
        self.assertEqual(0, self.repo.get_table_count_by_tag(u'tag3'))

    def test_has_table_record_001(self):
        t = {}
        t['database_name'] = u'test_database'
        t['schema_name'] = u'test_schema'
        t['table_name'] = u'test_table'
        t['timestamp'] = '2016-04-27T10:06:41.653836'
        t['tags'] = [u'tag1',u'tag2']
        self.assertFalse(self.repo.has_table_record(t))

        self.assertTrue(self.repo.append_table(t))

        self.assertTrue(self.repo.has_table_record(t))


    def test_put_tag_001(self):
        self.assertTrue(self.repo.put_tag(Tag(u'tag1', u'd.s.t')))
        self.assertTrue(self.repo.put_tag(Tag(u'tag2', u'd.s.t')))
        self.assertTrue(self.repo.put_tag(Tag(u'tag2', u'd.s.t')))

        # fail
        with self.assertRaises(InternalError) as cm:
            self.repo.put_tag(Tag(u'tag3', u'd.s.t2\''))
        self.assertTrue(cm.exception.value.startswith("Could not register tag: "))

    def test_get_tag_labels_001(self):
        self.assertEqual([], self.repo.get_tag_labels())

        self.assertTrue(self.repo.put_tag(Tag(u'tag1', u'd.s.t')))
        self.assertEqual([u'tag1'], self.repo.get_tag_labels())

        self.assertTrue(self.repo.put_tag(Tag(u'tag2', u'd.s.t')))
        self.assertEqual([u'tag1', u'tag2'], self.repo.get_tag_labels())

    def test_get_tags_001(self):
        self.assertTrue(self.repo.put_tag(Tag(u'tag1', u'd.s.t')))
        self.assertTrue(self.repo.put_tag(Tag(u'tag2', u'd.s.t')))
        self.assertTrue(self.repo.put_tag(Tag(u'tag2', u'd.s.t')))

        self.assertEqual(['tag1', 'tag2'], [x.label for x in self.repo.get_tags(target=u'd.s.t')])

        self.assertTrue(self.repo.put_tag(Tag(u'tag3', u'd.s.t2')))

        self.assertEqual(['tag1', 'tag2'], [x.label for x in self.repo.get_tags(target=u'd.s.t')])
        self.assertEqual(['tag3'], [x.label for x in self.repo.get_tags(target=u'd.s.t2')])

        # fail
        with self.assertRaises(InternalError) as cm:
            self.repo.get_tags(label=u'd.s.t2\'')
        self.assertTrue(cm.exception.value.startswith("Could not get tags: "))

    def test_set_tag_description_001(self):
        self.assertTrue(self.repo.set_tag_description(TagDesc(u'tag1')))
        self.assertTrue(self.repo.set_tag_description(TagDesc(u'tag1', u'short desc')))
        self.assertTrue(self.repo.set_tag_description(TagDesc(u'tag1', u'short desc', u'comment')))

    def test_get_tag_description_001(self):
        self.assertTrue(self.repo.set_tag_description(TagDesc(u'tag1')))
        self.assertEqual({'comment': None, 'label': u'tag1', 'desc': None},
                         self.repo.get_tag_description(u'tag1').__dict__)

        self.assertTrue(self.repo.set_tag_description(TagDesc(u'tag1', u'short desc')))
        self.assertEqual({'comment': None, 'label': u'tag1', 'desc': u'short desc'},
                         self.repo.get_tag_description(u'tag1').__dict__)

        self.assertTrue(self.repo.set_tag_description(TagDesc(u'tag1', u'short desc2', u'comment2')))
        self.assertEqual({'comment': u'comment2', 'label': u'tag1', 'desc': u'short desc2'},
                         self.repo.get_tag_description(u'tag1').__dict__)

        self.assertEqual({'comment': None, 'label': u'nosuchtag1', 'desc': None},
                         self.repo.get_tag_description(u'nosuchtag1').__dict__)

    def test_set_schema_description_001(self):
        self.assertTrue(self.repo.set_schema_description(SchemaDesc(u'schema1', u'desc', u'comment')))
        self.assertTrue(self.repo.set_schema_description(SchemaDesc(u'schema1', u'', u'')))
        self.assertTrue(self.repo.set_schema_description(SchemaDesc(u'スキーマ2', u'短い説明', u'コメント')))

    def test_get_schema_description_001(self):
        self.repo.set_schema_description(SchemaDesc(u'schema1', u'desc', u'comment'))
        self.assertEqual({'comment': u'comment', 'name': u'schema1', 'desc': u'desc'},
                         self.repo.get_schema_description(u'schema1').__dict__)
        self.repo.set_schema_description(SchemaDesc(u'schema1', u'', u''))
        self.assertEqual({'comment': u'', 'name': u'schema1', 'desc': u''},
                         self.repo.get_schema_description(u'schema1').__dict__)

        self.repo.set_schema_description(SchemaDesc(u'スキーマ2', u'短い説明', u'コメント'))
        self.assertEqual({'comment': u'コメント', 'name': u'スキーマ2', 'desc': u'短い説明'},
                         self.repo.get_schema_description(u'スキーマ2').__dict__)

        self.assertEqual({'comment': u'', 'name': u'schema1', 'desc': u''},
                         self.repo.get_schema_description(u'schema1').__dict__)

        self.assertEqual(None, self.repo.get_schema_description(u'nosuchschema1'))

    def test_add_file_001(self):
        # tag1: 1st item
        self.assertTrue(self.repo.add_file('tag', 'tag1', 'file1.xls'))
        self.assertEqual([u'file1.xls'], self.repo.get_files('tag', 'tag1'))

        # tag1: 2nd item
        self.assertTrue(self.repo.add_file('tag', 'tag1', 'file2.ppt'))
        self.assertEqual([u'file1.xls', u'file2.ppt'], self.repo.get_files('tag', 'tag1'))

        # tag1: dup
        self.assertTrue(self.repo.add_file('tag', 'tag1', 'file2.ppt'))
        self.assertEqual([u'file1.xls', u'file2.ppt'], self.repo.get_files('tag', 'tag1'))

        # invalid objtype
        with self.assertRaises(InternalError) as cm:
            self.repo.add_file('foo', 'tag1', 'file2.ppt')
        self.assertEqual(u"invalid object type: foo", cm.exception.value)

        # tag2:
        self.assertTrue(self.repo.add_file('tag', 'tag2', 'file1.ppt'))
        self.assertEqual([u'file1.ppt'], self.repo.get_files('tag', 'tag2'))

        # SCHEMA1:
        self.assertTrue(self.repo.add_file('schema', 'SCHEMA1', 'file1.ppt'))
        self.assertEqual([u'file1.ppt'], self.repo.get_files('schema', 'SCHEMA1'))

        # TABLE1:
        self.assertTrue(self.repo.add_file('table', 'TABLE1', 'tab1.ppt'))
        self.assertEqual([u'tab1.ppt'], self.repo.get_files('table', 'TABLE1'))

        # check
        self.assertEqual([u'file1.xls', u'file2.ppt'], self.repo.get_files('tag', 'tag1'))
        self.assertEqual([u'file1.ppt'], self.repo.get_files('tag', 'tag2'))
        self.assertEqual([u'file1.ppt'], self.repo.get_files('schema', 'SCHEMA1'))

    def test_delete_file_001(self):
        self.assertTrue(self.repo.add_file('tag', 'tag1', 'file1.xls'))
        self.assertTrue(self.repo.add_file('tag', 'tag1', 'file2.xls'))
        self.assertTrue(self.repo.add_file('tag', 'tag2', 'file1.ppt'))
        self.assertEqual(['file1.xls','file2.xls'], self.repo.get_files('tag', 'tag1'))

        # delete tag1
        self.assertTrue(self.repo.delete_files('tag', 'tag1'))
        self.assertEqual([], self.repo.get_files('tag', 'tag1'))
        self.assertEqual([u'file1.ppt'], self.repo.get_files('tag', 'tag2'))

        # delete tag2
        self.assertTrue(self.repo.delete_files('tag', 'tag2'))
        self.assertEqual([], self.repo.get_files('tag', 'tag1'))
        self.assertEqual([], self.repo.get_files('tag', 'tag2'))

        # test for schema
        self.assertTrue(self.repo.add_file('schema', 'SCHEMA1', 'schema1.xls'))
        self.assertEqual([u'schema1.xls'], self.repo.get_files('schema', 'SCHEMA1'))
        self.assertTrue(self.repo.delete_files('schema', 'SCHEMA1'))
        self.assertEqual([], self.repo.get_files('schema', 'SCHEMA1'))

        # test for table
        self.assertTrue(self.repo.add_file('table', 'TABLE1', 'table1.xls'))
        self.assertEqual([u'table1.xls'], self.repo.get_files('table', 'TABLE1'))
        self.assertTrue(self.repo.delete_files('table', 'TABLE1'))
        self.assertEqual([], self.repo.get_files('table', 'TABLE1'))

    def test_get_tags_002(self):
        self.assertTrue(self.repo.put_tag(Tag(u'tag1', u'd.s.t')))
        self.assertTrue(self.repo.put_tag(Tag(u'tag1', u'd.s.t2')))

        self.assertEqual(['d.s.t', 'd.s.t2'], [x.target for x in self.repo.get_tags(label=u'tag1')])

        self.assertTrue(self.repo.put_tag(Tag(u'tag2', u'd.s.t3')))
        self.assertEqual(['d.s.t', 'd.s.t2'], [x.target for x in self.repo.get_tags(label=u'tag1')])
        self.assertEqual(['d.s.t3'], [x.target for x in self.repo.get_tags(label=u'tag2')])

        # fail
        with self.assertRaises(InternalError) as cm:
            self.repo.get_tags(target=u'tag2\'')
        self.assertTrue(cm.exception.value.startswith("Could not get tags: "))

    def test_delete_tags_001(self):
        self.assertTrue(self.repo.put_tag(Tag(u'tag1', u'd.s.t')))
        self.assertTrue(self.repo.put_tag(Tag(u'tag1', u'd.s.t2')))

        self.assertEqual(['d.s.t', 'd.s.t2'], [x.target for x in self.repo.get_tags(label=u'tag1')])

        self.assertTrue(self.repo.delete_tags(target=u'd.s.t2'))

        self.assertEqual(['d.s.t'], [x.target for x in self.repo.get_tags(label=u'tag1')])

        # fail
        with self.assertRaises(InternalError) as cm:
            self.repo.delete_tags(target=u'tag2\'')
        self.assertTrue(cm.exception.value.startswith("Could not delete tags: "))

    def test_put_textelement_001(self):
        self.assertTrue(self.repo.put_textelement('id1', 'elem1'))
        self.assertTrue(self.repo.put_textelement('id1', None))
        self.assertTrue(self.repo.put_textelement('id2', u'エレメント2'))

    def test_get_textelements_001(self):
        self.repo.put_textelement('id1', 'elem1')
        self.assertEqual(['elem1'], self.repo.get_textelements('id1'))

        self.repo.put_textelement('id1', None)
        self.assertEqual(['elem1', ''], self.repo.get_textelements('id1'))

        self.repo.put_textelement('id2', u'エレメント2')
        self.assertEqual([u'エレメント2'], self.repo.get_textelements('id2'))
        self.assertEqual(['elem1', ''], self.repo.get_textelements('id1'))

    def test_get_datamap_source_tables_001(self):
        d = {}
        d['lineno'] = 1
        d['database_name'] = 'db_name'
        d['schema_name'] = 'sch_name'
        d['table_name'] = 'tab_name'
        d['column_name'] = 'col_name'
        d['record_id'] = 'rec_id'
        d['source_database_name'] = 'src_db_name'
        d['source_schema_name'] = 'src_sch_name'
        d['source_table_name'] = 'src_tab_name'
        d['source_column_name'] = 'src_col_name'
        self.repo.put_datamap_item(d)

        d['lineno'] = 2
        d['record_id'] = 'rec_id2'
        d['source_table_name'] = 'src_tab_name2'
        self.repo.put_datamap_item(d)

        t = self.repo.get_datamap_source_tables('db_name', 'sch_name', 'tab_name')
        self.assertEquals([u'src_tab_name', u'src_tab_name2'], t)

    def testPut_fk_001(self):
        # append table records with tags
        t1 = {}
        t1['database_name'] = u'test_database'
        t1['schema_name'] = u'test_schema'
        t1['table_name'] = u'test_table'
        t1['timestamp'] = '2016-04-27T10:06:41.653836'
        t1['columns'] = [{'column_name': 'c1'},
                         {'column_name': 'c2'}]

        self.assertTrue(self.repo.append_table(t1))

        t2 = {}
        t2['database_name'] = u'test_database'
        t2['schema_name'] = u'test_schema'
        t2['table_name'] = u'test_table2'
        t2['timestamp'] = '2016-04-27T10:06:41.653836'
        t2['columns'] = [{'column_name': 'c1'},
                         {'column_name': 'c2'}]

        self.assertTrue(self.repo.append_table(t2))

        # test_schema.test_table.c1 -> test_schema.test_table2.c1
        self.assertTrue(self.repo.put_table_fk('test_database', 'test_schema', 'test_table', 'c1',
                                               'test_database', 'test_schema', 'test_table2', 'c1'))

        tab1 = self.repo.get_table('test_database', 'test_schema', 'test_table')
        tab2 = self.repo.get_table('test_database', 'test_schema', 'test_table2')
#        print(tab1)
#        print(tab2)

        self.assertEqual(['test_schema.test_table2.c1'], tab1['columns'][0]['fk'])
        self.assertEqual(['test_schema.test_table.c1'], tab2['columns'][0]['fk_ref'])

        # test_schema.test_table.c2 -> test_schema.test_table2.c1
        self.assertTrue(self.repo.put_table_fk('test_database', 'test_schema', 'test_table', 'c2',
                                               'test_database', 'test_schema', 'test_table2', 'c1', guess=True))

        tab1 = self.repo.get_table('test_database', 'test_schema', 'test_table')
        tab2 = self.repo.get_table('test_database', 'test_schema', 'test_table2')
#        print(tab1)
#        print(tab2)

        self.assertEqual(['test_schema.test_table2.c1'], tab1['columns'][0]['fk']) # test_table.c1
        self.assertEqual(['?test_schema.test_table2.c1'], tab1['columns'][1]['fk']) # test_table.c2
        self.assertEqual(['test_schema.test_table.c1', '?test_schema.test_table.c2'], tab2['columns'][0]['fk_ref'])

        # put again to the existing entry.
        self.assertTrue(self.repo.put_table_fk('test_database', 'test_schema', 'test_table', 'c1',
                                               'test_database', 'test_schema', 'test_table2', 'c1'))

    def testRemove_fk_001(self):
        # append table records with tags
        t1 = {}
        t1['database_name'] = u'test_database'
        t1['schema_name'] = u'test_schema'
        t1['table_name'] = u'test_table'
        t1['timestamp'] = '2016-04-27T10:06:41.653836'
        t1['columns'] = [{'column_name': 'c1',
                          'fk': ['test_schema.test_table2.c1']}]

        self.assertTrue(self.repo.append_table(t1))

        t2 = {}
        t2['database_name'] = u'test_database'
        t2['schema_name'] = u'test_schema'
        t2['table_name'] = u'test_table2'
        t2['timestamp'] = '2016-04-27T10:06:41.653836'
        t2['columns'] = [{'column_name': 'c1',
                          'fk_ref': ['test_schema.test_table.c1']}]

        self.assertTrue(self.repo.append_table(t2))

        tab1 = self.repo.get_table('test_database', 'test_schema', 'test_table')
        tab2 = self.repo.get_table('test_database', 'test_schema', 'test_table2')
        self.assertEqual(['test_schema.test_table2.c1'], tab1['columns'][0]['fk'])
        self.assertEqual(['test_schema.test_table.c1'], tab2['columns'][0]['fk_ref'])

        self.assertTrue(self.repo.remove_table_fk('test_database', 'test_schema', 'test_table', 'c1',
                                               'test_database', 'test_schema', 'test_table2', 'c1'))

        tab1 = self.repo.get_table('test_database', 'test_schema', 'test_table')
        tab2 = self.repo.get_table('test_database', 'test_schema', 'test_table2')
        self.assertEqual([], tab1['columns'][0]['fk'])
        self.assertEqual([], tab2['columns'][0]['fk_ref'])

        # remove again from the empty fk entries.
        self.assertFalse(self.repo.remove_table_fk('test_database', 'test_schema', 'test_table', 'c1',
                                                   'test_database', 'test_schema', 'test_table2', 'c1'))

    def testClear_fk_001(self):
        # append table records with tags
        t1 = {}
        t1['database_name'] = u'test_database'
        t1['schema_name'] = u'test_schema'
        t1['table_name'] = u'test_table'
        t1['timestamp'] = '2016-04-27T10:06:41.653836'
        t1['columns'] = [{'column_name': 'c1',
                          'fk': ['test_schema.test_table2.c1']}]

        self.assertTrue(self.repo.append_table(t1))

        t2 = {}
        t2['database_name'] = u'test_database'
        t2['schema_name'] = u'test_schema'
        t2['table_name'] = u'test_table2'
        t2['timestamp'] = '2016-04-27T10:06:41.653836'
        t2['columns'] = [{'column_name': 'c1',
                          'fk_ref': ['test_schema.test_table.c1']}]

        self.assertTrue(self.repo.append_table(t2))

        tab1 = self.repo.get_table('test_database', 'test_schema', 'test_table')
        tab2 = self.repo.get_table('test_database', 'test_schema', 'test_table2')
        self.assertEqual(['test_schema.test_table2.c1'], tab1['columns'][0]['fk'])
        self.assertEqual(['test_schema.test_table.c1'], tab2['columns'][0]['fk_ref'])

        # clear table t1.
        self.assertTrue(self.repo.clear_table_fk('test_database', 'test_schema', 'test_table', 'c1'))

        tab1 = self.repo.get_table('test_database', 'test_schema', 'test_table')
        tab2 = self.repo.get_table('test_database', 'test_schema', 'test_table2')
        self.assertEqual([], tab1['columns'][0]['fk'])
        self.assertEqual(['test_schema.test_table.c1'], tab2['columns'][0]['fk_ref'])

        # clear table t2.
        self.assertTrue(self.repo.clear_table_fk('test_database', 'test_schema', 'test_table2', 'c1'))

        tab1 = self.repo.get_table('test_database', 'test_schema', 'test_table')
        tab2 = self.repo.get_table('test_database', 'test_schema', 'test_table2')
        self.assertEqual([], tab1['columns'][0]['fk'])
        self.assertEqual([], tab2['columns'][0]['fk_ref'])

        # clear again. fail.
        self.assertFalse(self.repo.clear_table_fk('test_database', 'test_schema', 'test_table', 'c1'))
        self.assertFalse(self.repo.clear_table_fk('test_database', 'test_schema', 'test_table2', 'c1'))

    def testGet_bg_term_001(self):
        self.assertIsNone(self.repo.get_bg_term(u'term'))

        self.assertTrue(self.repo.put_bg_term(u'term', 'sd', 'ld', 'owner', ['cat1','cat2'], ['synA', 'synB'], ['term2', 'term3'], ['TAB1', 'TAB2']))

        a = {'id': '0',
             'term': u'term',
             'description_short': 'sd',
             'description_long': 'ld',
             'owned_by': 'owner',
             'categories': ['cat1','cat2'],
             'synonyms': ['synA', 'synB'],
             'related_terms': ['term2', 'term3'],
             'assigned_assets': ['TAB1', 'TAB2']}
        b = self.repo.get_bg_term(u'term')

        # excluding `created_at' since it's a timestamp value.
        self.assertTrue('created_at' in b)
        created_at = b['created_at']
        del b['created_at']

        self.assertEqual(a, b)

        # updating the record
        time.sleep(1)
        self.assertTrue(self.repo.put_bg_term(u'term', 'sd2', 'ld', 'owner', ['cat1','cat2'], ['synA', 'synB'], ['term2', 'term3'], ['TAB1', 'TAB2']))
        b = self.repo.get_bg_term(u'term')
        self.assertFalse(created_at == b['created_at']) # created_at field must be changed.

        self.assertTrue('created_at' in b)
        del b['created_at']

        a['description_short'] = u'sd2'
        self.assertEqual(a, b)

    def testGet_bg_terms_all_001(self):
        self.assertEqual([], self.repo.get_bg_terms_all())

        self.repo.put_bg_term(u'term3', '', '', '', [], [], [], [])
        self.repo.put_bg_term(u'term1', '', '', '', [], [], [], [])
        self.repo.put_bg_term(u'term2', '', '', '', [], [], [], [])

        self.assertEqual([u'term1', u'term2', u'term3'], self.repo.get_bg_terms_all())

        self.repo.put_bg_term(u'term3', '', '', '', [], [], [], [])
        self.repo.put_bg_term(u'term11', '', '', '', [], [], [], [])

        self.assertEqual([u'term11', u'term1', u'term2', u'term3'], self.repo.get_bg_terms_all())

    """
    Unit tests for accessing validation rules.
    """
    def test_get_validation_rules_001(self):
        self.assertEqual([], self.repo.get_validation_rules())

    def test_get_validation_rules_002(self):
        self.assertTrue(self.repo.create_validation_rule('database_name1','schema_name1','table_name1','column_name','description1','rule','param','param2'))
        self.assertTrue(self.repo.create_validation_rule('database_name2','schema_name2','table_name2','column_name','description2','rule','param','param2'))
        self.assertTrue(self.repo.create_validation_rule('database_name3','schema_name3','table_name3','column_name','description3','rule','param','param2'))

        a = [(1, u'database_name1', u'schema_name1',u'table_name1',u'column_name',u'description1',u'rule',u'param',u'param2')]
        self.assertEqual(a, self.repo.get_validation_rules(database_name='database_name1'))

        a = [(2, u'database_name2', u'schema_name2',u'table_name2',u'column_name',u'description2',u'rule',u'param',u'param2')]
        self.assertEqual(a, self.repo.get_validation_rules(schema_name='schema_name2'))

        a = [(3, u'database_name3', u'schema_name3',u'table_name3',u'column_name',u'description3',u'rule',u'param',u'param2')]
        self.assertEqual(a, self.repo.get_validation_rules(table_name='table_name3'))

        self.assertEqual([], self.repo.get_validation_rules(database_name='database_name1',schema_name='schema_name2',table_name='table_name3'))

    def test_create_validation_rule_001(self):
        self.assertEqual(1, self.repo.create_validation_rule('database_name','schema_name','table_name','column_name','description1','rule','param','param2'))

        self.assertEqual(2, self.repo.create_validation_rule('database_name','schema_name','table_name','column_name','description2','rule'))

        self.assertEqual(3, self.repo.create_validation_rule('database_name','schema_name','table_name','column_name','description3','rule', 'param'))

        # same rule as #1
        self.assertEqual(None, self.repo.create_validation_rule('database_name','schema_name','table_name','column_name','description4','rule','param','param2'))

        a = [(1,
              u'database_name',
              u'schema_name',
              u'table_name',
              u'column_name',
              u'description1',
              u'rule',
              u'param',
              u'param2'),
             (2,
              u'database_name',
              u'schema_name',
              u'table_name',
              u'column_name',
              u'description2',
              u'rule',
              u'',
              u''),
             (3,
              u'database_name',
              u'schema_name',
              u'table_name',
              u'column_name',
              u'description3',
              u'rule',
              u'param',
              u'')]

        self.assertEqual(a, self.repo.get_validation_rules())

    def test_get_validation_rule_001(self):
        # 1
        self.assertEqual(1, self.repo.create_validation_rule('database_name','schema_name','table_name','column_name','description1','rule','param','param2'))

        a = (1,
             u'database_name',
              u'schema_name',
              u'table_name',
              u'column_name',
              u'description1',
              u'rule',
              u'param',
              u'param2')
        self.assertEqual(a, self.repo.get_validation_rule(1))

        # 2
        self.assertEqual(2, self.repo.create_validation_rule('database_name','schema_name','table_name','column_name','description2','rule'))
        a = (2,
             u'database_name',
              u'schema_name',
              u'table_name',
              u'column_name',
              u'description2',
              u'rule',
              u'',
              u'')
        self.assertEqual(a, self.repo.get_validation_rule(2))

        # 3: not found
        self.assertEqual(None, self.repo.get_validation_rule(3))

    def test_update_validation_rule_001(self):
        # 1
        self.assertEqual(1, self.repo.create_validation_rule('database_name','schema_name','table_name','column_name','description1','rule','param','param2'))

        a = (1,
             u'database_name',
              u'schema_name',
              u'table_name',
              u'column_name',
              u'description1',
              u'rule',
              u'param',
              u'param2')
        self.assertEqual(a, self.repo.get_validation_rule(1))

        # update 1
        self.assertTrue(self.repo.update_validation_rule(1, 'database_name','schema_name','table_name','column_name','description2','rule'))
        a = (1,
             u'database_name',
              u'schema_name',
              u'table_name',
              u'column_name',
              u'description2',
              u'rule',
              u'',
              u'')
        self.assertEqual(a, self.repo.get_validation_rule(1))

    def test_delete_validation_rule_001(self):
        # 1
        self.assertEqual(1, self.repo.create_validation_rule('database_name','schema_name','table_name','column_name','description1','rule','param','param2'))

        a = (1,
             u'database_name',
              u'schema_name',
              u'table_name',
              u'column_name',
              u'description1',
              u'rule',
              u'param',
              u'param2')
        self.assertEqual(a, self.repo.get_validation_rule(1))

        # delete 1
        self.assertTrue(self.repo.delete_validation_rule(1))

        # deleted
        self.assertEqual(None, self.repo.get_validation_rule(1))

if __name__ == '__main__':
    unittest.main()
