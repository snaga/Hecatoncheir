#!/usr/bin/env python2.7
# -*- coding: utf-8 -*-

import os
import sys
import time
import unittest
sys.path.append('..')

import sqlalchemy as sa

from hecatoncheir import DbProfilerRepository
from hecatoncheir.exception import InternalError
from hecatoncheir import db
from hecatoncheir.table import Table2
from hecatoncheir.validation import ValidationRule

class TestDbProfilerRepository(unittest.TestCase):
    repo = None

    def setUp(self):
        fname = "foo.db"
        self.repo = DbProfilerRepository.DbProfilerRepository(filename=fname)
#        fname = "datacatalog"
#        self.repo = DbProfilerRepository.DbProfilerRepository(filename=fname, host='localhost', user='snaga')
        self.repo.destroy()
        self.repo.init()
        self.repo.open()
        self.maxDiff = None

    def tearDown(self):
        self.repo.close()

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

        # connecting a repository database should fail. (permission denied)
        fname = "/foo.db"
        with self.assertRaises(sa.exc.OperationalError) as cm:
            DbProfilerRepository.DbProfilerRepository(fname)
        self.assertEquals('(sqlite3.OperationalError) unable to open database file', str(cm.exception))

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

    def testGet_001(self):
        t = {}
        t['database_name'] = u'test_database'
        t['schema_name'] = u'test_schema'
        t['table_name'] = u'test_table'
        t['timestamp'] = '2016-04-27T10:06:41.653836'

        self.assertIsNotNone(Table2.create(t['database_name'], t['schema_name'], t['table_name'], t))

        d2 = self.repo.get()

        self.assertIsNotNone(d2)
        self.assertEqual(1, len(d2))
        self.assertEqual('test_database', d2[0]['database_name'])
        self.assertEqual('test_schema', d2[0]['schema_name'])
        self.assertEqual('test_table', d2[0]['table_name'])
        self.assertEqual('2016-04-27T10:06:41.653836', d2[0]['timestamp'])

    def testGet_table_history_001(self):
        t = {}
        t['database_name'] = u'test_database'
        t['schema_name'] = u'test_schema'
        t['table_name'] = u'test_table'
        t['timestamp'] = '2016-04-27T10:06:41.653836'
        self.assertIsNotNone(Table2.create(t['database_name'], t['schema_name'], t['table_name'], t))

        t = {}
        t['database_name'] = u'test_database'
        t['schema_name'] = u'test_schema'
        t['table_name'] = u'test_table'
        t['timestamp'] = '2016-04-28T10:06:41.653836'
        self.assertIsNotNone(Table2.create(t['database_name'], t['schema_name'], t['table_name'], t))

        t = {}
        t['database_name'] = u'test_database'
        t['schema_name'] = u'test_schema'
        t['table_name'] = u'test_table'
        t['timestamp'] = '2016-04-26T10:06:41.653836'
        self.assertIsNotNone(Table2.create(t['database_name'], t['schema_name'], t['table_name'], t))

        thist = self.repo.get_table_history('test_database', 'test_schema', 'test_table')
        self.assertEqual(3, len(thist))
        self.assertEqual('2016-04-28T10:06:41.653836', thist[0]['timestamp']) # latest
        self.assertEqual('2016-04-27T10:06:41.653836', thist[1]['timestamp'])
        self.assertEqual('2016-04-26T10:06:41.653836', thist[2]['timestamp']) # oldest

        # fail
        with self.assertRaises(InternalError) as cm:
            self.repo.get_table_history('test\'_database', 'test_schema', 'test_table')
        self.assertTrue(cm.exception.value.startswith("Could not get table data with its history: "))

    def testPut_fk_001(self):
        # append table records with tags
        t1 = {}
        t1['database_name'] = u'test_database'
        t1['schema_name'] = u'test_schema'
        t1['table_name'] = u'test_table'
        t1['timestamp'] = '2016-04-27T10:06:41.653836'
        t1['columns'] = [{'column_name': 'c1'},
                         {'column_name': 'c2'}]

        self.assertIsNotNone(Table2.create(t1['database_name'], t1['schema_name'], t1['table_name'], t1))

        t2 = {}
        t2['database_name'] = u'test_database'
        t2['schema_name'] = u'test_schema'
        t2['table_name'] = u'test_table2'
        t2['timestamp'] = '2016-04-27T10:06:41.653836'
        t2['columns'] = [{'column_name': 'c1'},
                         {'column_name': 'c2'}]

        self.assertIsNotNone(Table2.create(t2['database_name'], t2['schema_name'], t2['table_name'], t2))

        # test_schema.test_table.c1 -> test_schema.test_table2.c1
        self.assertTrue(self.repo.put_table_fk('test_database', 'test_schema', 'test_table', 'c1',
                                               'test_database', 'test_schema', 'test_table2', 'c1'))

        tab1 = Table2.find('test_database', 'test_schema', 'test_table')[0].data
        tab2 = Table2.find('test_database', 'test_schema', 'test_table2')[0].data
#        print(tab1)
#        print(tab2)

        self.assertEqual(['test_schema.test_table2.c1'], tab1['columns'][0]['fk'])
        self.assertEqual(['test_schema.test_table.c1'], tab2['columns'][0]['fk_ref'])

        # test_schema.test_table.c2 -> test_schema.test_table2.c1
        self.assertTrue(self.repo.put_table_fk('test_database', 'test_schema', 'test_table', 'c2',
                                               'test_database', 'test_schema', 'test_table2', 'c1', guess=True))

        tab1 = Table2.find('test_database', 'test_schema', 'test_table')[0].data
        tab2 = Table2.find('test_database', 'test_schema', 'test_table2')[0].data
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

        self.assertIsNotNone(Table2.create(t1['database_name'], t1['schema_name'], t1['table_name'], t1))

        t2 = {}
        t2['database_name'] = u'test_database'
        t2['schema_name'] = u'test_schema'
        t2['table_name'] = u'test_table2'
        t2['timestamp'] = '2016-04-27T10:06:41.653836'
        t2['columns'] = [{'column_name': 'c1',
                          'fk_ref': ['test_schema.test_table.c1']}]

        self.assertIsNotNone(Table2.create(t2['database_name'], t2['schema_name'], t2['table_name'], t2))

        tab1 = Table2.find('test_database', 'test_schema', 'test_table')[0].data
        tab2 = Table2.find('test_database', 'test_schema', 'test_table2')[0].data
        self.assertEqual(['test_schema.test_table2.c1'], tab1['columns'][0]['fk'])
        self.assertEqual(['test_schema.test_table.c1'], tab2['columns'][0]['fk_ref'])

        self.assertTrue(self.repo.remove_table_fk('test_database', 'test_schema', 'test_table', 'c1',
                                               'test_database', 'test_schema', 'test_table2', 'c1'))

        tab1 = Table2.find('test_database', 'test_schema', 'test_table')[0].data
        tab2 = Table2.find('test_database', 'test_schema', 'test_table2')[0].data
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

        self.assertIsNotNone(Table2.create(t1['database_name'], t1['schema_name'], t1['table_name'], t1))

        t2 = {}
        t2['database_name'] = u'test_database'
        t2['schema_name'] = u'test_schema'
        t2['table_name'] = u'test_table2'
        t2['timestamp'] = '2016-04-27T10:06:41.653836'
        t2['columns'] = [{'column_name': 'c1',
                          'fk_ref': ['test_schema.test_table.c1']}]

        self.assertIsNotNone(Table2.create(t2['database_name'], t2['schema_name'], t2['table_name'], t2))

        tab1 = Table2.find('test_database', 'test_schema', 'test_table')[0].data
        tab2 = Table2.find('test_database', 'test_schema', 'test_table2')[0].data
        self.assertEqual(['test_schema.test_table2.c1'], tab1['columns'][0]['fk'])
        self.assertEqual(['test_schema.test_table.c1'], tab2['columns'][0]['fk_ref'])

        # clear table t1.
        self.assertTrue(self.repo.clear_table_fk('test_database', 'test_schema', 'test_table', 'c1'))

        tab1 = Table2.find('test_database', 'test_schema', 'test_table')[0].data
        tab2 = Table2.find('test_database', 'test_schema', 'test_table2')[0].data
        self.assertEqual([], tab1['columns'][0]['fk'])
        self.assertEqual(['test_schema.test_table.c1'], tab2['columns'][0]['fk_ref'])

        # clear table t2.
        self.assertTrue(self.repo.clear_table_fk('test_database', 'test_schema', 'test_table2', 'c1'))

        tab1 = Table2.find('test_database', 'test_schema', 'test_table')[0].data
        tab2 = Table2.find('test_database', 'test_schema', 'test_table2')[0].data
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

if __name__ == '__main__':
    unittest.main()
