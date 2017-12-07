#!/usr/bin/env python2.7
# -*- coding: utf-8 -*-

import json
import os
import sys
import unittest
sys.path.append('..')

from hecatoncheir import logger as log
from hecatoncheir.exception import DriverError, ProfilingError
from hecatoncheir.pgsql import PgProfiler

class TestPgProfiler(unittest.TestCase):
    def setUp(self):
        self.host = os.environ.get('PGHOST', '127.0.0.1')
        self.port = os.environ.get('PGPORT', 5432)
        self.dbname = os.environ.get('PGDATABASE', 'dqwbtest')
        self.user = os.environ.get('PGUSER', 'dqwbuser')
        self.passwd = os.environ.get('PGPASSWORD', 'dqwbuser')

    def test_PgProfiler_001(self):
        p = PgProfiler.PgProfiler(self.host, self.port, self.dbname, self.user, self.passwd)
        self.assertIsNotNone(p)

    def test_get_schema_names_001(self):
        p = PgProfiler.PgProfiler(self.host, self.port, self.dbname, self.user, self.passwd)
        s = p.get_schema_names()
        self.assertEqual([u'public'], s)

        # detecting error on lazy connection
        p = PgProfiler.PgProfiler(self.host, self.port, self.dbname, 'foo', 'foo')
        with self.assertRaises(DriverError) as cm:
            s = p.get_schema_names()
        self.assertEqual('Could not connect to the server: FATAL:  role "foo" does not exist',
                         cm.exception.value)

    def test_get_table_names_001(self):
        p = PgProfiler.PgProfiler(self.host, self.port, self.dbname, self.user, self.passwd)
        t = p.get_table_names(u'public')

        self.assertEqual([u'SUPPLIER',
                          u'customer',
                          u'lineitem',
                          u'nation',
                          u'nation2',
                          u'orders',
                          u'part',
                          u'partsupp',
                          u'region'],
                         t)

    def test_get_table_names_002(self):
        p = PgProfiler.PgProfiler(self.host, self.port, self.dbname, self.user, self.passwd)
        t = p.get_table_names(u'PUBLIC')

        self.assertEqual([], t)

    def test_get_column_names_001(self):
        p = PgProfiler.PgProfiler(self.host, self.port, self.dbname, self.user, self.passwd)
        c = p.get_column_names(u'public', u'customer')

        self.assertEqual([u'c_custkey',
                          u'c_name',
                          u'c_address',
                          u'c_nationkey',
                          u'c_phone',
                          u'c_acctbal',
                          u'c_mktsegment',
                          u'c_comment'],
                         c)

        # case-sensitive?
        c = p.get_column_names(u'public', u'CUSTOMER')
        self.assertEqual([], c)
        c = p.get_column_names(u'PUBLIC', u'customer')
        self.assertEqual([], c)

    def test_get_column_names_002(self):
        # case-sensitive
        p = PgProfiler.PgProfiler(self.host, self.port, self.dbname, self.user, self.passwd)
        c = p.get_column_names(u'public', u'SUPPLIER')

        self.assertEqual([u's_suppkey',
                          u'S_NAME',
                          u's_address',
                          u's_nationkey',
                          u's_phone',
                          u's_acctbal',
                          u's_comment'],
                         c)

    def test_get_sample_rows_001(self):
        # case-sensitive
        p = PgProfiler.PgProfiler(self.host, self.port, self.dbname, self.user, self.passwd)
        c = p.get_sample_rows(u'public', u'SUPPLIER')

        self.assertEqual(11, len(c))
        self.assertEqual([u's_suppkey',
                          u'S_NAME',
                          u's_address',
                          u's_nationkey',
                          u's_phone',
                          u's_acctbal',
                          u's_comment'],
                         c[0])
        self.assertEqual(['2',
                          'Supplier#000000002',
                          '89eJ5ksX3ImxJQBvxObC,',
                          '5',
                          '15-679-861-2259',
                          '4032.68',
                          'furiously stealthy frays thrash alongside of the slyly express deposits. blithely regular req'],
                         c[1])
        self.assertEqual(['11',
                          'Supplier#000000011',
                          'JfwTs,LZrV, M,9C',
                          '18',
                          None,
                          '3393.08',
                          'quickly bold asymptotes mold carefully unusual pearls. requests boost at the blith'],
                         c[10])

    def test_get_column_datatypes_001(self):
        p = PgProfiler.PgProfiler(self.host, self.port, self.dbname, self.user, self.passwd)
        c = p.get_column_datatypes(u'public', u'customer')

        self.assertEqual(['integer', None], c['c_custkey'])
        self.assertEqual(['character varying', 25], c['c_name'])
        self.assertEqual(['character varying', 40], c['c_address'])
        self.assertEqual(['integer', None], c['c_nationkey'])
        self.assertEqual(['character', 15], c['c_phone'])
        self.assertEqual(['real', None], c['c_acctbal'])
        self.assertEqual(['character', 10], c['c_mktsegment'])
        self.assertEqual(['character varying', 117], c['c_comment'])

        # case-sensitive?
        c = p.get_column_datatypes(u'public', u'CUSTOMER')
        self.assertEqual({}, c)
        c = p.get_column_datatypes(u'PUBLIC', u'customer')
        self.assertEqual({}, c)

    def test_get_column_datatypes_002(self):
        # case-sensitive
        p = PgProfiler.PgProfiler(self.host, self.port, self.dbname, self.user, self.passwd)
        c = p.get_column_datatypes(u'public', u'SUPPLIER')

        self.assertEqual({u'S_NAME': [u'character', 25],
                          u's_acctbal': [u'real', None],
                          u's_address': [u'character varying', 40],
                          u's_comment': [u'character varying', 101],
                          u's_nationkey': [u'integer', None],
                          u's_phone': [u'character', 15],
                          u's_suppkey': [u'integer', None]}, c)

    def test_get_row_count_001(self):
        p = PgProfiler.PgProfiler(self.host, self.port, self.dbname, self.user, self.passwd)
        c = p.get_row_count(u'public', u'customer')
        self.assertEqual(28, c)

        # case-sensitive?
        with self.assertRaises(ProfilingError) as cm:
            c = p.get_row_count(u'public', u'CUSTOMER')
        self.assertEqual('Could not get row count/num of nulls/min/max values.', cm.exception.value)
        with self.assertRaises(ProfilingError) as cm:
            c = p.get_row_count(u'PUBLIC', u'customer')
        self.assertEqual('Could not get row count/num of nulls/min/max values.', cm.exception.value)

    def test_get_row_count_002(self):
        p = PgProfiler.PgProfiler(self.host, self.port, self.dbname, self.user, self.passwd)
        c = p.get_row_count(u'public', u'customer', use_statistics=True)

        # case-sensitive?
        with self.assertRaises(ProfilingError) as cm:
            c = p.get_row_count(u'public', u'CUSTOMER')
        self.assertEqual('Could not get row count/num of nulls/min/max values.', cm.exception.value)
        with self.assertRaises(ProfilingError) as cm:
            c = p.get_row_count(u'PUBLIC', u'customer')
        self.assertEqual('Could not get row count/num of nulls/min/max values.', cm.exception.value)

    def test_get_column_nulls_001(self):
        p = PgProfiler.PgProfiler(self.host, self.port, self.dbname, self.user, self.passwd)
        c = p.get_column_nulls(u'public', u'SUPPLIER')

        self.assertEqual(0, c['s_suppkey'])
        self.assertEqual(0, c['S_NAME'])
        self.assertEqual(0, c['s_address'])
        self.assertEqual(0, c['s_nationkey'])
        self.assertEqual(1, c['s_phone'])
        self.assertEqual(0, c['s_acctbal'])
        self.assertEqual(0, c['s_comment'])

        # case-sensitive?
        with self.assertRaises(ProfilingError) as cm:
            c = p.get_column_nulls(u'public', u'CUSTOMER')
        self.assertEqual('Could not get row count/num of nulls/min/max values.', cm.exception.value)
        with self.assertRaises(ProfilingError) as cm:
            c = p.get_column_nulls(u'PUBLIC', u'customer')
        self.assertEqual('Could not get row count/num of nulls/min/max values.', cm.exception.value)

    def test_get_column_nulls_002(self):
        p = PgProfiler.PgProfiler(self.host, self.port, self.dbname, self.user, self.passwd)
        c = p.get_column_nulls(u'public', u'SUPPLIER', use_statistics=True)

        self.assertEqual(0, c['s_suppkey'])
        self.assertEqual(0, c['S_NAME'])
        self.assertEqual(0, c['s_address'])
        self.assertEqual(0, c['s_nationkey'])
        self.assertEqual(1, c['s_phone'])
        self.assertEqual(0, c['s_acctbal'])
        self.assertEqual(0, c['s_comment'])

        # case-sensitive?
        with self.assertRaises(ProfilingError) as cm:
            c = p.get_column_nulls(u'public', u'CUSTOMER')
        self.assertEqual('Could not get row count/num of nulls/min/max values.', cm.exception.value)
        with self.assertRaises(ProfilingError) as cm:
            c = p.get_column_nulls(u'PUBLIC', u'customer')
        self.assertEqual('Could not get row count/num of nulls/min/max values.', cm.exception.value)

    def test_has_minmax_001(self):
        self.assertFalse(PgProfiler.PgProfiler.has_minmax(['byteA', 1]))
        self.assertFalse(PgProfiler.PgProfiler.has_minmax(['oid', 1]))
        self.assertFalse(PgProfiler.PgProfiler.has_minmax(['boolean', 1]))
        self.assertFalse(PgProfiler.PgProfiler.has_minmax(['xid', 1]))
        self.assertFalse(PgProfiler.PgProfiler.has_minmax(['ARRAY', 1]))
        self.assertFalse(PgProfiler.PgProfiler.has_minmax(['json', 1]))
        self.assertFalse(PgProfiler.PgProfiler.has_minmax(['jsonb', 1]))

        self.assertTrue(PgProfiler.PgProfiler.has_minmax(['int', 1]))

    def testget_column_min_max_001(self):
        p = PgProfiler.PgProfiler(self.host, self.port, self.dbname, self.user, self.passwd)
        c = p.get_column_min_max(u'public', u'customer')

        self.assertEqual([3373, 147004], c['c_custkey'])
        self.assertEqual([u'Customer#000003373', u'Customer#000147004'], c['c_name'])
        self.assertEqual([u',5lO7OHiDTFNK6t1HuLmIyQalgXDNgVH tytO9h', u'zwkHSjRJYn9yMpk3gWuXkULpteHpSoXXCWXiFOT'], c['c_address'])
        self.assertEqual([0, 24], c['c_nationkey'])
        self.assertEqual([u'10-356-493-3518', u'34-132-612-5205'], c['c_phone'])
        self.assertEqual([-686.4, 9705.43], c['c_acctbal'])
        self.assertEqual([u'AUTOMOBILE', u'MACHINERY '], c['c_mktsegment'])
        self.assertEqual([u'accounts across the even instructions haggle ironic deposits. slyly re', u'unusual, even packages are among the ironic pains. regular, final accou'], c['c_comment'])

        # case-sensitive?
        with self.assertRaises(ProfilingError) as cm:
            c = p.get_column_min_max(u'public', u'CUSTOMER')
        self.assertEqual('Could not get row count/num of nulls/min/max values.', cm.exception.value)

        with self.assertRaises(ProfilingError) as cm:
            c = p.get_column_min_max(u'PUBLIC', u'customer')
        self.assertEqual('Could not get row count/num of nulls/min/max values.', cm.exception.value)

    def test_get_column_most_freq_values_001(self):
        p = PgProfiler.PgProfiler(self.host, self.port, self.dbname, self.user, self.passwd)
        p.profile_most_freq_values_enabled = 20
        c = p.get_column_most_freq_values(u'public', u'customer')

        self.assertEqual([[3373, 1L],
[16252, 1L],
[21061, 1L],
[28547, 1L],
[32113, 1L],
[36901, 1L],
[39136, 1L],
[44485, 1L],
[55624, 1L],
[56614, 1L],
[61001, 1L],
[64340, 1L],
[66958, 1L],
[78002, 1L],
[81763, 1L],
[84487, 1L],
[86116, 1L],
[88910, 1L],
[104480, 1L],
[107779, 1L]], c['c_custkey'])
#        self.assertEqual([], c['c_name'])
#        self.assertEqual([], c['c_address'])
#        self.assertEqual([], c['c_nationkey'])
#        self.assertEqual([], c['c_phone'])
#        self.assertEqual([], c['c_acctbal'])
        self.assertEqual( [[u'AUTOMOBILE', 7],  [u'BUILDING  ', 7],  [u'MACHINERY ', 6],  [u'FURNITURE ', 5],  [u'HOUSEHOLD ', 3]], c['c_mktsegment'])
#        self.assertEqual([], c['c_comment'])

        # case-sensitive?
        c = p.get_column_most_freq_values(u'public', u'CUSTOMER')
        self.assertEqual({}, c)
        c = p.get_column_most_freq_values(u'PUBLIC', u'customer')
        self.assertEqual({}, c)

    def test_get_column_least_freq_values_001(self):
        p = PgProfiler.PgProfiler(self.host, self.port, self.dbname, self.user, self.passwd)
        p.profile_most_freq_values_enabled = 20
        c = p.get_column_least_freq_values(u'public', u'customer')

        self.assertEqual([[3373, 1L],
[16252, 1L],
[21061, 1L],
[28547, 1L],
[32113, 1L],
[36901, 1L],
[39136, 1L],
[44485, 1L],
[55624, 1L],
[56614, 1L],
[61001, 1L],
[64340, 1L],
[66958, 1L],
[78002, 1L],
[81763, 1L],
[84487, 1L],
[86116, 1L],
[88910, 1L],
[104480, 1L],
[107779, 1L]], c['c_custkey'])
#        self.assertEqual([], c['c_name'])
#        self.assertEqual([], c['c_address'])
#        self.assertEqual([], c['c_nationkey'])
#        self.assertEqual([], c['c_phone'])
        self.assertEqual( [[-686.4, 1L],
  [-659.55, 1L],
  [-546.88, 1L],
  [-275.34, 1L],
  [-65.46, 1L],
  [358.38, 1L],
  [818.33, 1L],
  [1661.37, 1L],
  [2095.42, 1L],
  [3205.60, 1L],
  [4128.41, 1L],
  [4543.02, 1L],
  [4652.75, 1L],
  [4809.84, 1L],
  [5009.55, 1L],
  [5555.41, 1L],
  [6264.23, 1L],
  [6295.5, 1L],
  [6629.21, 1L],
  [6958.60, 1L]], c['c_acctbal'])
#        self.assertEqual([], c['c_mktsegment'])
#        self.assertEqual([], c['c_comment'])

        # case-sensitive?
        c = p.get_column_least_freq_values(u'public', u'CUSTOMER')
        self.assertEqual({}, c)
        c = p.get_column_least_freq_values(u'PUBLIC', u'customer')
        self.assertEqual({}, c)

    def test_get_column_cardinalities_001(self):
        p = PgProfiler.PgProfiler(self.host, self.port, self.dbname, self.user, self.passwd)
        c = p.get_column_cardinalities(u'public', u'customer')

        self.assertEqual(28, c['c_custkey'])
        self.assertEqual(28, c['c_name'])
        self.assertEqual(28, c['c_address'])
        self.assertEqual(21, c['c_nationkey'])
        self.assertEqual(28, c['c_phone'])
        self.assertEqual(28, c['c_acctbal'])
        self.assertEqual(5, c['c_mktsegment'])
        self.assertEqual(28, c['c_comment'])

        # case-sensitive?
        c = p.get_column_cardinalities(u'public', u'CUSTOMER')
        self.assertEqual({}, c)
        c = p.get_column_cardinalities(u'PUBLIC', u'customer')
        self.assertEqual({}, c)

    def test_run_record_validation_001(self):
        p = PgProfiler.PgProfiler(self.host, self.port, self.dbname, self.user, self.passwd)
        r = [(1, 'dqwbtest','public','customer','c_custkey','','regexp','^\d+$', ''),
             (2, 'dqwbtest','public','customer','c_custkey','','eval','{c_custkey} > 0 and {c_custkey} < 1000000', ''),
             (3, 'dqwbtest','public','customer','c_acctbal','','eval','{c_acctbal} > 0', ''),
             (4, 'dqwbtest','public','customer','c_custkey,c_nationkey','','eval','{c_custkey} > {c_nationkey}', ''),
             (5, 'dqwbtest','public','customer','c_custkey,c_nationkey','','eval','{c_custkey} < {c_nationkey}', ''),
             (6, 'dqwbtest','public','customer','c_custkey','','eval','{c_custkey} > 0 and', ''),
             (7, 'dqwbtest','public','customer','c_custkey','','eval','{c_custkey2} > 0', '')]

        c = p.run_record_validation(u'public', u'customer', r)
#        print(json.dumps(c, indent=2))

        self.maxDiff = None
        # record validation (regrep/eval) only
        self.assertEqual(5, c['c_acctbal'][0]['invalid_count'])

        self.assertEqual(0, c['c_custkey'][0]['invalid_count'])
        self.assertEqual(0, c['c_custkey'][1]['invalid_count'])
        self.assertEqual(0, c['c_custkey'][2]['invalid_count'])
        self.assertEqual(28, c['c_custkey'][3]['invalid_count'])
        self.assertEqual(28, c['c_custkey'][4]['invalid_count'])
        self.assertEqual(28, c['c_custkey'][5]['invalid_count'])

        self.assertEqual(0, c['c_nationkey'][0]['invalid_count'])
        self.assertEqual(28, c['c_nationkey'][1]['invalid_count'])

        # case-sensitive?
        # here, table names do not match to any rule
        c = p.run_record_validation(u'public', u'CUSTOMER', r)
        self.assertEqual({}, c)
        c = p.run_record_validation(u'PUBLIC', u'customer', r)
        self.assertEqual({}, c)

if __name__ == '__main__':
    unittest.main()
