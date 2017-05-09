#!/usr/bin/env python2.7
# -*- coding: utf-8 -*-

import os
import sys
import unittest
sys.path.append('..')

from hecatoncheir import DbProfilerException
from hecatoncheir.mssql import MSSQLProfiler

class TestMSSQLProfiler(unittest.TestCase):
    def setUp(self):
        self.host = '127.0.0.1'
        self.port = 1433
        self.dbname = 'dqwbtest'
        self.user = 'dqwbuser'
        self.passwd = 'dqwbuser'
        pass

    def test_MSSQLProfiler_001(self):
        p = MSSQLProfiler.MSSQLProfiler(self.host, self.port, self.dbname, self.user, self.passwd)
        self.assertIsNotNone(p)

    def test_get_schema_names_001(self):
        p = MSSQLProfiler.MSSQLProfiler(self.host, self.port, self.dbname, self.user, self.passwd)
        s = p.get_schema_names()
        self.assertEqual([u'dbo', u'guest'], s)

        # detecting error on lazy connection
        p = MSSQLProfiler.MSSQLProfiler(self.host, self.port, self.dbname, 'foo', 'foo')
        with self.assertRaises(DbProfilerException.DriverError) as cm:
            s = p.get_schema_names()
        self.assertTrue(cm.exception.value.startswith('Could not connect to the server: '))

    def test_get_table_names_001(self):
        p = MSSQLProfiler.MSSQLProfiler(self.host, self.port, self.dbname, self.user, self.passwd)
        t = p.get_table_names(u'dbo')

        self.assertEqual([u'customer',
                          u'lineitem',
                          u'nation',
                          u'nation2',
                          u'orders',
                          u'part',
                          u'partsupp',
                          u'region',
                          u'supplier'],
                         t)

    def test_get_column_names_001(self):
        p = MSSQLProfiler.MSSQLProfiler(self.host, self.port, self.dbname, self.user, self.passwd)
        c = p.get_column_names(u'dbo', u'customer')

        self.assertEqual([u'c_custkey',
                          u'c_name',
                          u'c_address',
                          u'c_nationkey',
                          u'c_phone',
                          u'c_acctbal',
                          u'c_mktsegment',
                          u'c_comment'],
                         c)

    def test_get_sample_rows_001(self):
        # case-sensitive
        p = MSSQLProfiler.MSSQLProfiler(self.host, self.port, self.dbname, self.user, self.passwd)
        c = p.get_sample_rows(u'dbo', u'SUPPLIER')

        self.assertEqual(11, len(c))
        self.assertEqual([u's_suppkey',
                          u's_name',
                          u's_address',
                          u's_nationkey',
                          u's_phone',
                          u's_acctbal',
                          u's_comment'],
                         c[0])
        self.assertEqual([2,
                          'Supplier#000000002       ',
                          '89eJ5ksX3ImxJQBvxObC,',
                          5,
                          '15-679-861-2259',
                          4032.679931640625,
                          'furiously stealthy frays thrash alongside of the slyly express deposits. blithely regular req'],
                         c[1])
        self.assertEqual([11,
                          'Supplier#000000011       ',
                          'JfwTs,LZrV, M,9C',
                          18,
                          '28-613-996-1505',
                          3393.080078125,
                          'quickly bold asymptotes mold carefully unusual pearls. requests boost at the blith'],
                         c[10])

    def test_getcolumn_datatypes_001(self):
        p = MSSQLProfiler.MSSQLProfiler(self.host, self.port, self.dbname, self.user, self.passwd)
        c = p.get_column_datatypes(u'dbo', u'customer')

        self.assertEqual(['int', None], c['c_custkey'])
        self.assertEqual(['varchar', 25], c['c_name'])
        self.assertEqual(['varchar', 40], c['c_address'])
        self.assertEqual(['int', None], c['c_nationkey'])
        self.assertEqual(['char', 15], c['c_phone'])
        self.assertEqual(['real', None], c['c_acctbal'])
        self.assertEqual(['char', 10], c['c_mktsegment'])
        self.assertEqual(['varchar', 117], c['c_comment'])

    def test_get_row_count_001(self):
        p = MSSQLProfiler.MSSQLProfiler(self.host, self.port, self.dbname, self.user, self.passwd)
        c = p.get_row_count(u'dbo', u'customer')
        self.assertEqual(28, c)

    def test_get_column_nulls_001(self):
        p = MSSQLProfiler.MSSQLProfiler(self.host, self.port, self.dbname, self.user, self.passwd)
        c = p.get_column_nulls(u'dbo', u'customer')

        self.assertEqual(0, c['c_custkey'])
        self.assertEqual(0, c['c_name'])
        self.assertEqual(0, c['c_address'])
        self.assertEqual(0, c['c_nationkey'])
        self.assertEqual(0, c['c_phone'])
        self.assertEqual(0, c['c_acctbal'])
        self.assertEqual(0, c['c_mktsegment'])
        self.assertEqual(0, c['c_comment'])

    def test_has_minmax_001(self):
        self.assertFalse(MSSQLProfiler.MSSQLProfiler.has_minmax(['BINary', 1]))
        self.assertFalse(MSSQLProfiler.MSSQLProfiler.has_minmax(['varbinary', 1]))

        self.assertTrue(MSSQLProfiler.MSSQLProfiler.has_minmax(['int', 1]))

    def test_get_column_min_max_001(self):
        p = MSSQLProfiler.MSSQLProfiler(self.host, self.port, self.dbname, self.user, self.passwd)
        c = p.get_column_min_max(u'dbo', u'customer')

        self.assertEqual([3373, 147004], c['c_custkey'])
        self.assertEqual([u'Customer#000003373', u'Customer#000147004'], c['c_name'])
        self.assertEqual([u',5lO7OHiDTFNK6t1HuLmIyQalgXDNgVH tytO9h', u'zwkHSjRJYn9yMpk3gWuXkULpteHpSoXXCWXiFOT'], c['c_address'])
        self.assertEqual([0, 24], c['c_nationkey'])
        self.assertEqual([u'10-356-493-3518', u'34-132-612-5205'], c['c_phone'])
        self.assertEqual([-686.4000244140625, 9705.4296875], c['c_acctbal'])
        self.assertEqual([u'AUTOMOBILE', u'MACHINERY '], c['c_mktsegment'])
        self.assertEqual([u'accounts across the even instructions haggle ironic deposits. slyly re', u'unusual, even packages are among the ironic pains. regular, final accou'], c['c_comment'])

    def test_get_column_most_freq_values_001(self):
        p = MSSQLProfiler.MSSQLProfiler(self.host, self.port, self.dbname, self.user, self.passwd)
        p.profile_most_freq_values_enabled = 20
        c = p.get_column_most_freq_values(u'dbo', u'customer')

        self.assertEqual([[3373, 1],
[16252, 1],
[21061, 1],
[28547, 1],
[32113, 1],
[36901, 1],
[39136, 1],
[44485, 1],
[55624, 1],
[56614, 1],
[61001, 1],
[64340, 1],
[66958, 1],
[78002, 1],
[81763, 1],
[84487, 1],
[86116, 1],
[88910, 1],
[104480, 1],
[107779, 1]], c['c_custkey'])
#        self.assertEqual([], c['c_name'])
#        self.assertEqual([], c['c_address'])
#        self.assertEqual([], c['c_nationkey'])
#        self.assertEqual([], c['c_phone'])
#        self.assertEqual([], c['c_acctbal'])
        self.assertEqual( [[u'AUTOMOBILE', 7],  [u'BUILDING  ', 7],  [u'MACHINERY ', 6],  [u'FURNITURE ', 5],  [u'HOUSEHOLD ', 3]], c['c_mktsegment'])
#        self.assertEqual([], c['c_comment'])

    def test_get_column_least_freq_values_001(self):
        p = MSSQLProfiler.MSSQLProfiler(self.host, self.port, self.dbname, self.user, self.passwd)
        p.profile_most_freq_values_enabled = 20
        c = p.get_column_least_freq_values(u'dbo', u'customer')

        self.assertEqual([[3373, 1],
[16252, 1],
[21061, 1],
[28547, 1],
[32113, 1],
[36901, 1],
[39136, 1],
[44485, 1],
[55624, 1],
[56614, 1],
[61001, 1],
[64340, 1],
[66958, 1],
[78002, 1],
[81763, 1],
[84487, 1],
[86116, 1],
[88910, 1],
[104480, 1],
[107779, 1]], c['c_custkey'])
#        self.assertEqual([], c['c_name'])
#        self.assertEqual([], c['c_address'])
#        self.assertEqual([], c['c_nationkey'])
#        self.assertEqual([], c['c_phone'])
        self.assertEqual( [[-686.4000244140625, 1],
  [-659.5499877929688, 1],
  [-546.8800048828125, 1],
  [-275.3399963378906, 1],
  [-65.45999908447266, 1],
  [358.3800048828125, 1],
  [818.3300170898438, 1],
  [1661.3699951171875, 1],
  [2095.419921875, 1],
  [3205.60009765625, 1],
  [4128.41015625, 1],
  [4543.02001953125, 1],
  [4652.75, 1],
  [4809.83984375, 1],
  [5009.5498046875, 1],
  [5555.41015625, 1],
  [6264.22998046875, 1],
  [6295.5, 1],
  [6629.2099609375, 1],
  [6958.60009765625, 1]], c['c_acctbal'])
#        self.assertEqual([], c['c_mktsegment'])
#        self.assertEqual([], c['c_comment'])

    def test_get_column_cardinalities_001(self):
        p = MSSQLProfiler.MSSQLProfiler(self.host, self.port, self.dbname, self.user, self.passwd)
        c = p.get_column_cardinalities(u'dbo', u'customer')

        self.assertEqual(28, c['c_custkey'])
        self.assertEqual(28, c['c_name'])
        self.assertEqual(28, c['c_address'])
        self.assertEqual(21, c['c_nationkey'])
        self.assertEqual(28, c['c_phone'])
        self.assertEqual(28, c['c_acctbal'])
        self.assertEqual(5, c['c_mktsegment'])
        self.assertEqual(28, c['c_comment'])

    def test_run_record_validation_001(self):
        p = MSSQLProfiler.MSSQLProfiler(self.host, self.port, self.dbname, self.user, self.passwd)
        r = [(1, 'dqwbtest','dbo','customer','c_custkey','','regexp','^\d+$', ''),
             (2, 'dqwbtest','dbo','customer','c_custkey','','eval','{c_custkey} > 0 and {c_custkey} < 1000000', ''),
             (3, 'dqwbtest','dbo','customer','c_acctbal','','eval','{c_acctbal} > 0', ''),
             (4, 'dqwbtest','dbo','customer','c_custkey,c_nationkey','','eval','{c_custkey} > {c_nationkey}', ''),
             (5, 'dqwbtest','dbo','customer','c_custkey,c_nationkey','','eval','{c_custkey} < {c_nationkey}', ''),
             (6, 'dqwbtest','dbo','customer','c_custkey','','eval','{c_custkey} > 0 and', ''),
             (7, 'dqwbtest','dbo','customer','c_custkey','','eval','{c_custkey2} > 0', '')]

        c = p.run_record_validation(u'dbo', u'customer', r)

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
        c = p.run_record_validation(u'dbo', u'CUSTOMER', r)
        self.assertEqual({}, c)
        c = p.run_record_validation(u'DBO', u'customer', r)
        self.assertEqual({}, c)

if __name__ == '__main__':
    unittest.main()
