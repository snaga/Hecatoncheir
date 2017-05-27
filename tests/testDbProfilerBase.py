#!/usr/bin/env python2.7
# -*- coding: utf-8 -*-

import json
import os
import sys
import unittest
sys.path.append('..')

from hecatoncheir import logger as log
from hecatoncheir.exception import DriverError, QueryError
from hecatoncheir.metadata import TableColumnMeta, TableMeta
from hecatoncheir.pgsql import PgProfiler

"""
The DbProfilerBase class is an abstract class, so we test it through
the PgProfiler class which inherited from DbProfilerBase.
"""
class TestDbProfilerBase(unittest.TestCase):
    def setUp(self):
        self.host = '127.0.0.1'
        self.port = 5432
        self.dbname = u'dqwbtest'
        self.user = 'dqwbuser'
        self.passwd = 'dqwbuser'
        pass

    def test_DbProfilerBase_001(self):
        p = PgProfiler.PgProfiler(self.host, self.port, self.dbname, self.user, self.passwd)
        self.assertIsNotNone(p)

    def test_connect_001(self):
        p = PgProfiler.PgProfiler(self.host, self.port, self.dbname, self.user, self.passwd)
        self.assertTrue(p.connect())

        p = PgProfiler.PgProfiler(self.host, self.port, self.dbname, 'nosuchuser', self.passwd)
        with self.assertRaises(DriverError) as cm:
            self.assertTrue(p.connect())
        self.assertEqual(u'Could not connect to the server: FATAL:  role "nosuchuser" does not exist',
                         cm.exception.value)

    def test_run_column_profiling_001(self):
        p = PgProfiler.PgProfiler(self.host, self.port, self.dbname, self.user, self.passwd)
        self.assertTrue(p.connect())

        tablemeta = TableMeta(self.dbname, u'public', u'customer')
        tablemeta.column_names = ['c_custkey','c_name','c_address','c_nationkey','c_phone','c_acctbal','c_mktsegment','c_comment']
        columnmeta = {}
        for col in tablemeta.column_names:
            columnmeta[col] = TableColumnMeta(unicode(col))

        self.assertTrue(p.run_column_profiling(tablemeta, columnmeta))
        self.assertEqual(0, columnmeta['c_custkey'].nulls)
        self.assertEqual('3373', columnmeta['c_custkey'].min)
        self.assertEqual('147004', columnmeta['c_custkey'].max)

        self.assertEqual(10, len(columnmeta['c_comment'].most_freq_values))
        self.assertEqual(10, len(columnmeta['c_comment'].least_freq_values))
        self.assertEqual(28, columnmeta['c_comment'].cardinality)

    def test_run_001(self):
        p = PgProfiler.PgProfiler(self.host, self.port, self.dbname, self.user, self.passwd)
        self.assertTrue(p.connect())

        d = p.run(u'public', u'customer')
        self.assertEqual(u'dqwbtest', d['database_name'])
        self.assertEqual(u'public', d['schema_name'])
        self.assertEqual(u'customer', d['table_name'])

        self.assertEqual(8, len(d['columns']))

        self.assertEqual(u'c_custkey', d['columns'][0]['column_name'])
        self.assertEqual([], d['columns'][0]['validation'])

        self.assertEqual(u'c_name', d['columns'][1]['column_name'])
        self.assertEqual([], d['columns'][1]['validation'])

        self.assertEqual(u'c_address', d['columns'][2]['column_name'])
        self.assertEqual([], d['columns'][2]['validation'])

        self.assertEqual(u'c_nationkey', d['columns'][3]['column_name'])
        self.assertEqual([], d['columns'][3]['validation'])

        self.assertEqual(u'c_phone', d['columns'][4]['column_name'])
        self.assertEqual([], d['columns'][4]['validation'])

        self.assertEqual(u'c_acctbal', d['columns'][5]['column_name'])
        self.assertEqual([], d['columns'][5]['validation'])

        self.assertEqual(u'c_mktsegment', d['columns'][6]['column_name'])
        self.assertEqual([], d['columns'][6]['validation'])

        self.assertEqual(u'c_comment', d['columns'][7]['column_name'])
        self.assertEqual([], d['columns'][7]['validation'])

    def test_run_002(self):
        p = PgProfiler.PgProfiler(self.host, self.port, self.dbname, self.user, self.passwd)
        self.assertTrue(p.connect())

        r = [(0, u'dqwbtest', u'public', u'customer', u'c_custkey', u'eval desc', u'eval', u'{c_custkey} > 10000', u''),
             (1, u'dqwbtest', u'public', u'customer', u'c_custkey', u'regexp desc', u'regexp', u'^[^7]\\d+$', u''),
             (2, u'dqwbtest', u'public', u'customer', u'c_custkey', u'columnstat desc', u'columnstat', '{nulls} == 1', '')]

        d = p.run(u'public', u'customer', validation_rules=r)

        self.assertEqual(u'dqwbtest', d['database_name'])
        self.assertEqual(u'public', d['schema_name'])
        self.assertEqual(u'customer', d['table_name'])

        self.assertEqual(8, len(d['columns']))

        # validation results
        self.assertEqual(3, len(d['columns'][0]['validation']))
        self.assertEqual({'column_names': [u'c_custkey'],
                           'description': u'eval desc',
                           'invalid_count': 1,
                           'label': 0,
                           'rule': [u'c_custkey', u'{c_custkey} > 10000'],
                           'statistics': [28, 1]},
                         d['columns'][0]['validation'][0])  # c_custkey
        self.assertEqual({'column_names': [u'c_custkey'],
                           'description': u'regexp desc',
                           'invalid_count': 1,
                           'label': 1,
                           'rule': [u'c_custkey', u'^[^7]\\d+$'],
                           'statistics': [28, 1]},
                         d['columns'][0]['validation'][1])  # c_custkey
        self.assertEqual({'column_names': [u'c_custkey'],
                           'description': u'columnstat desc',
                           'invalid_count': 1,
                           'label': 2,
                           'rule': [u'c_custkey', u'{nulls} == 1'],
                           'statistics': [1, 1]},
                         d['columns'][0]['validation'][2])  # c_custkey

        self.assertEqual([], d['columns'][1]['validation'])  # c_name
        self.assertEqual([], d['columns'][2]['validation'])  # c_address
        self.assertEqual([], d['columns'][3]['validation'])  # c_nationkey
        self.assertEqual([], d['columns'][4]['validation'])  # c_phone
        self.assertEqual([], d['columns'][5]['validation'])  # c_acctbal
        self.assertEqual([], d['columns'][6]['validation'])  # c_mktsegment
        self.assertEqual([], d['columns'][7]['validation'])  # c_comment

if __name__ == '__main__':
    unittest.main()
