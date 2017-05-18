#!/usr/bin/env python2.7
# -*- coding: utf-8 -*-

import os
import sys
import unittest
from decimal import *
sys.path.append('..')

from hecatoncheir.exception import DriverError, InternalError
from hecatoncheir.mysql import MyProfiler

"""
sudo grep temporary.password /var/log/mysqld.log
mysql -u root -p
set global validate_password_policy=LOW;
set password for root@localhost=password('mysql123');
select database();

CREATE DATABASE dqwbtest CHARACTER SET utf8mb4;

mysql -u root -p dqwbtest < data/mysql_ddl.sql
mysql -u root -p dqwbtest < data/mysql_data.sql
"""

class TestMyProfiler(unittest.TestCase):
    def setUp(self):
        self.host = '127.0.0.1'
        self.port = 3306
        self.dbname = 'dqwbtest'
        self.user = 'root'
        self.passwd = 'mysql123'

    def test_MyProfiler_001(self):
        p = MyProfiler.MyProfiler(self.host, self.port, self.dbname, self.user, self.passwd)
        self.assertIsNotNone(p)

    def test_get_schema_names_001(self):
        p = MyProfiler.MyProfiler(self.host, self.port, self.dbname, self.user, self.passwd)
        s = p.get_schema_names()
        self.assertEqual([self.dbname], s)

        # detecting error on lazy connection
        p = MyProfiler.MyProfiler(self.host, self.port, self.dbname, 'foo', 'foo')
        with self.assertRaises(DriverError) as cm:
            s = p.get_schema_names()
        self.assertEqual("Could not connect to the server: Access denied for user 'foo'@'localhost' (using password: YES)", cm.exception.value)

    def test_get_table_names_001(self):
        p = MyProfiler.MyProfiler(self.host, self.port, self.dbname, self.user, self.passwd)
        t = p.get_table_names(self.dbname)

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
        p = MyProfiler.MyProfiler(self.host, self.port, self.dbname, self.user, self.passwd)
        c = p.get_column_names(self.dbname, u'customer')

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
        p = MyProfiler.MyProfiler(self.host, self.port, self.dbname, self.user, self.passwd)
        c = p.get_sample_rows(self.dbname, u'supplier')

        self.assertEqual(11, len(c))
        self.assertEqual([u's_suppkey',
                          u's_name',
                          u's_address',
                          u's_nationkey',
                          u's_phone',
                          u's_acctbal',
                          u's_comment'],
                         c[0])
        self.assertEqual([2L,
                          'Supplier#000000002',
                          '89eJ5ksX3ImxJQBvxObC,',
                          5L,
                          '15-679-861-2259',
                          4032.68,
                          'furiously stealthy frays thrash alongside of the slyly express deposits. blithely regular req'],
                         c[1])
        self.assertEqual([11L,
                          'Supplier#000000011',
                          'JfwTs,LZrV, M,9C',
                          18L,
                          '28-613-996-1505',
                          3393.08,
                          'quickly bold asymptotes mold carefully unusual pearls. requests boost at the blith'],
                         c[10])

        with self.assertRaises(InternalError) as cm:
            p.get_sample_rows(u'public', u'SUPPLIER')
        self.assertEqual("Could not get column names of the table: public.SUPPLIER", cm.exception.value)

    def test_get_column_datatypes_001(self):
        p = MyProfiler.MyProfiler(self.host, self.port, self.dbname, self.user, self.passwd)
        c = p.get_column_datatypes(self.dbname, u'customer')

        self.assertEqual(['int', None], c['c_custkey'])
        self.assertEqual(['varchar', 25], c['c_name'])
        self.assertEqual(['varchar', 40], c['c_address'])
        self.assertEqual(['int', None], c['c_nationkey'])
        self.assertEqual(['char', 15], c['c_phone'])
        self.assertEqual(['decimal', None], c['c_acctbal'])
        self.assertEqual(['char', 10], c['c_mktsegment'])
        self.assertEqual(['varchar', 117], c['c_comment'])

    def test_get_row_count_001(self):
        p = MyProfiler.MyProfiler(self.host, self.port, self.dbname, self.user, self.passwd)
        c = p.get_row_count(self.dbname, u'customer')
        self.assertEqual(28, c)

    def test_get_column_nulls_001(self):
        p = MyProfiler.MyProfiler(self.host, self.port, self.dbname, self.user, self.passwd)
        c = p.get_column_nulls(self.dbname, u'customer')

        self.assertEqual(0, c['c_custkey'])
        self.assertEqual(0, c['c_name'])
        self.assertEqual(0, c['c_address'])
        self.assertEqual(0, c['c_nationkey'])
        self.assertEqual(0, c['c_phone'])
        self.assertEqual(0, c['c_acctbal'])
        self.assertEqual(0, c['c_mktsegment'])
        self.assertEqual(0, c['c_comment'])

    def test_has_minmax_001(self):
        self.assertFalse(MyProfiler.MyProfiler.has_minmax(['tinyblob', 1]))
        self.assertFalse(MyProfiler.MyProfiler.has_minmax(['blob', 1]))
        self.assertFalse(MyProfiler.MyProfiler.has_minmax(['mediumblob', 1]))
        self.assertFalse(MyProfiler.MyProfiler.has_minmax(['longblob', 1]))

        self.assertTrue(MyProfiler.MyProfiler.has_minmax(['int', 1]))

    def test_get_column_min_max_001(self):
        p = MyProfiler.MyProfiler(self.host, self.port, self.dbname, self.user, self.passwd)
        c = p.get_column_min_max(self.dbname, u'customer')

        self.assertEqual([3373, 147004], c['c_custkey'])
        self.assertEqual([u'Customer#000003373', u'Customer#000147004'], c['c_name'])
        self.assertEqual([u',5lO7OHiDTFNK6t1HuLmIyQalgXDNgVH tytO9h', u'zwkHSjRJYn9yMpk3gWuXkULpteHpSoXXCWXiFOT'], c['c_address'])
        self.assertEqual([0, 24], c['c_nationkey'])
        self.assertEqual([u'10-356-493-3518', u'34-132-612-5205'], c['c_phone'])
        self.assertEqual([-686.40, 9705.43], c['c_acctbal'])
        self.assertEqual([u'AUTOMOBILE', u'MACHINERY'], c['c_mktsegment'])
        self.assertEqual([u'accounts across the even instructions haggle ironic deposits. slyly re', u'unusual, even packages are among the ironic pains. regular, final accou'], c['c_comment'])

    def test_get_column_most_freq_values_001(self):
        p = MyProfiler.MyProfiler(self.host, self.port, self.dbname, self.user, self.passwd)
        p.profile_most_freq_values_enabled = 20
        c = p.get_column_most_freq_values(self.dbname, u'customer')

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
        self.assertEqual( [[u'AUTOMOBILE', 7],  [u'BUILDING', 7],  [u'MACHINERY', 6],  [u'FURNITURE', 5],  [u'HOUSEHOLD', 3]], c['c_mktsegment'])
#        self.assertEqual([], c['c_comment'])

    def test_get_column_least_freq_values_001(self):
        p = MyProfiler.MyProfiler(self.host, self.port, self.dbname, self.user, self.passwd)
        p.profile_most_freq_values_enabled = 20
        c = p.get_column_least_freq_values(self.dbname, u'customer')

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
#        print(c['c_acctbal'])
        self.assertEqual([[-686.40, 1L],
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
                          [6295.50, 1L],
                          [6629.21, 1L],
                          [6958.60, 1L]], c['c_acctbal'])
#        self.assertEqual([], c['c_mktsegment'])
#        self.assertEqual([], c['c_comment'])

    def test_get_column_cardinalities_001(self):
        p = MyProfiler.MyProfiler(self.host, self.port, self.dbname, self.user, self.passwd)
        c = p.get_column_cardinalities(self.dbname, u'customer')

        self.assertEqual(28, c['c_custkey'])
        self.assertEqual(28, c['c_name'])
        self.assertEqual(28, c['c_address'])
        self.assertEqual(21, c['c_nationkey'])
        self.assertEqual(28, c['c_phone'])
        self.assertEqual(28, c['c_acctbal'])
        self.assertEqual(5, c['c_mktsegment'])
        self.assertEqual(28, c['c_comment'])

    def test_run_record_validation_001(self):
        p = MyProfiler.MyProfiler(self.host, self.port, self.dbname, self.user, self.passwd)
        r = [(1, 'dqwbtest','dqwbtest','customer','c_custkey','','regexp','^\d+$', ''),
             (2, 'dqwbtest','dqwbtest','customer','c_custkey','','eval','{c_custkey} > 0 and {c_custkey} < 1000000', ''),
             (3, 'dqwbtest','dqwbtest','customer','c_acctbal','','eval','{c_acctbal} > 0', ''),
             (4, 'dqwbtest','dqwbtest','customer','c_custkey,c_nationkey','','eval','{c_custkey} > {c_nationkey}', ''),
             (5, 'dqwbtest','dqwbtest','customer','c_custkey,c_nationkey','','eval','{c_custkey} < {c_nationkey}', ''),
             (6, 'dqwbtest','dqwbtest','customer','c_custkey','','eval','{c_custkey} > 0 and', ''),
             (7, 'dqwbtest','dqwbtest','customer','c_custkey','','eval','{c_custkey2} > 0', '')]

        c = p.run_record_validation(self.dbname, u'customer', r)
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
        self.assertEqual({}, p.run_record_validation(self.dbname, u'CUSTOMER', r))
        self.assertEqual({}, p.run_record_validation(self.dbname.upper(), u'customer', r))

if __name__ == '__main__':
    unittest.main()
