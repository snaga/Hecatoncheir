#!/usr/bin/env python2.7
# -*- coding: utf-8 -*-

import json
import os
import sys
import unittest
sys.path.append('..')

from hecatoncheir.exception import DriverError
from hecatoncheir.oracle import OraProfiler

class TestOracleProfiler(unittest.TestCase):
    def setUp(self):
        self.host = '127.0.0.1'
        self.port = 1521
        self.dbname = 'orcl'
        self.user = 'scott'
        self.passwd = 'tiger'
        pass

    def test_OraProfiler_001(self):
        p = OraProfiler.OraProfiler(self.host, self.port, self.dbname, self.user, self.passwd)
        self.assertIsNotNone(p)

    def test_get_schema_names_001(self):
        p = OraProfiler.OraProfiler(self.host, self.port, self.dbname, self.user, self.passwd)
        s = p.get_schema_names()
        self.assertEqual([u'SCOTT'], s)

        # detecting error on lazy connection
        p = OraProfiler.OraProfiler(self.host, self.port, self.dbname, 'foo', 'foo')
        with self.assertRaises(DriverError) as cm:
            s = p.get_schema_names()
        self.assertEqual('Could not connect to the server: ORA-01017: invalid username/password; logon denied', cm.exception.value)

    def test_get_table_names_001(self):
        p = OraProfiler.OraProfiler(self.host, self.port, self.dbname, self.user, self.passwd)
        t = p.get_table_names(u'SCOTT')

        self.assertEqual([u'CUSTOMER',
                          u'DEPT',
                          u'LINEITEM',
                          u'NATION',
                          u'NATION2',
                          u'ORDERS',
                          u'PART',
                          u'PARTSUPP',
                          u'REGION',
                          u'SUPPLIER'],
                         t)

    def test_get_column_names_001(self):
        p = OraProfiler.OraProfiler(self.host, self.port, self.dbname, self.user, self.passwd)
        c = p.get_column_names(u'SCOTT', u'CUSTOMER')

        self.assertEqual([u'C_CUSTKEY',
                          u'C_NAME',
                          u'C_ADDRESS',
                          u'C_NATIONKEY',
                          u'C_PHONE',
                          u'C_ACCTBAL',
                          u'C_MKTSEGMENT',
                          u'C_COMMENT'],
                         c)

        c = p.get_column_names(u'SCOTT', u'CUSTOMER2')

        self.assertEqual([u'C_CUSTKEY',
                          u'C_NAME',
                          u'C_ADDRESS',
                          u'C_NATIONKEY',
                          u'C_PHONE',
                          u'C_ACCTBAL',
                          u'C_MKTSEGMENT',
                          u'C_COMMENT'],
                         c)

    def test_get_sample_rows_001(self):
        # case-sensitive
        p = OraProfiler.OraProfiler(self.host, self.port, self.dbname, self.user, self.passwd)
        c = p.get_sample_rows(u'SCOTT', u'SUPPLIER')

        self.assertEqual(11, len(c))
        self.assertEqual([u'S_SUPPKEY',
                          u'S_NAME',
                          u'S_ADDRESS',
                          u'S_NATIONKEY',
                          u'S_PHONE',
                          u'S_ACCTBAL',
                          u'S_COMMENT'],
                         c[0])
        self.assertEqual([2,
                          'Supplier#000000002       ',
                          '89eJ5ksX3ImxJQBvxObC,',
                          5,
                          '15-679-861-2259',
                          4032.67993,
                          'furiously stealthy frays thrash alongside of the slyly express deposits. blithely regular req'],
                         c[1])
        self.assertEqual([11,
                          'Supplier#000000011       ',
                          'JfwTs,LZrV, M,9C',
                          18,
                          '28-613-996-1505',
                          3393.08008,
                          'quickly bold asymptotes mold carefully unusual pearls. requests boost at the blith'],
                         c[10])

    def test_get_column_datatypes_001(self):
        p = OraProfiler.OraProfiler(self.host, self.port, self.dbname, self.user, self.passwd)
        c = p.get_column_datatypes(u'SCOTT', u'CUSTOMER')

        self.assertEqual(['NUMBER', 22], c['C_CUSTKEY'])
        self.assertEqual(['VARCHAR2', 25], c['C_NAME'])
        self.assertEqual(['VARCHAR2', 40], c['C_ADDRESS'])
        self.assertEqual(['NUMBER', 22], c['C_NATIONKEY'])
        self.assertEqual(['CHAR', 15], c['C_PHONE'])
        self.assertEqual(['FLOAT', 22], c['C_ACCTBAL'])
        self.assertEqual(['CHAR', 10], c['C_MKTSEGMENT'])
        self.assertEqual(['VARCHAR2', 117], c['C_COMMENT'])

        c = p.get_column_datatypes(u'SCOTT', u'CUSTOMER2')

        self.assertEqual(['NUMBER', 22], c['C_CUSTKEY'])
        self.assertEqual(['VARCHAR2', 25], c['C_NAME'])
        self.assertEqual(['VARCHAR2', 40], c['C_ADDRESS'])
        self.assertEqual(['NUMBER', 22], c['C_NATIONKEY'])
        self.assertEqual(['CHAR', 15], c['C_PHONE'])
        self.assertEqual(['FLOAT', 22], c['C_ACCTBAL'])
        self.assertEqual(['CHAR', 10], c['C_MKTSEGMENT'])
        self.assertEqual(['VARCHAR2', 117], c['C_COMMENT'])

    def test_get_row_count_001(self):
        p = OraProfiler.OraProfiler(self.host, self.port, self.dbname, self.user, self.passwd)
        c = p.get_row_count(u'SCOTT', u'CUSTOMER')
        self.assertEqual(28, c)

    def test_get_column_nulls_001(self):
        p = OraProfiler.OraProfiler(self.host, self.port, self.dbname, self.user, self.passwd)
        c = p.get_column_nulls(u'SCOTT', u'CUSTOMER')

        self.assertEqual(0, c['C_CUSTKEY'])
        self.assertEqual(0, c['C_NAME'])
        self.assertEqual(0, c['C_ADDRESS'])
        self.assertEqual(0, c['C_NATIONKEY'])
        self.assertEqual(0, c['C_PHONE'])
        self.assertEqual(0, c['C_ACCTBAL'])
        self.assertEqual(0, c['C_MKTSEGMENT'])
        self.assertEqual(0, c['C_COMMENT'])

    def test_has_minmax_001(self):
        self.assertFalse(OraProfiler.OraProfiler.has_minmax(['blob', None]))
        self.assertFalse(OraProfiler.OraProfiler.has_minmax(['CLOB', None]))
        self.assertFalse(OraProfiler.OraProfiler.has_minmax(['NCLOB', None]))
        self.assertFalse(OraProfiler.OraProfiler.has_minmax(['BFILE', None]))
        self.assertFalse(OraProfiler.OraProfiler.has_minmax(['RAW', None]))
        self.assertFalse(OraProfiler.OraProfiler.has_minmax(['LONG RAW', None]))

        self.assertTrue(OraProfiler.OraProfiler.has_minmax(['NUMBER', 22]))
        self.assertTrue(OraProfiler.OraProfiler.has_minmax(['VARCHAR2', 22]))

    def test_get_column_min_max_001(self):
        p = OraProfiler.OraProfiler(self.host, self.port, self.dbname, self.user, self.passwd)
        c = p.get_column_min_max(u'SCOTT', u'CUSTOMER')

        self.assertEqual([3373, 147004], c['C_CUSTKEY'])
        self.assertEqual([u'Customer#000003373', u'Customer#000147004'], c['C_NAME'])
        self.assertEqual([u',5lO7OHiDTFNK6t1HuLmIyQalgXDNgVH tytO9h',
                          u'zwkHSjRJYn9yMpk3gWuXkULpteHpSoXXCWXiFOT'], c['C_ADDRESS'])
        self.assertEqual([0, 24], c['C_NATIONKEY'])
        self.assertEqual([u'10-356-493-3518', u'34-132-612-5205'], c['C_PHONE'])
        self.assertEqual([-686.400024, 9705.42969], c['C_ACCTBAL'])
        self.assertEqual([u'AUTOMOBILE', u'MACHINERY '], c['C_MKTSEGMENT'])
        self.assertEqual([u'accounts across the even instructions haggle ironic deposits. slyly re',
                          u'unusual, even packages are among the ironic pains. regular, final accou'],
                         c['C_COMMENT'])

    def test_get_column_most_freq_values_001(self):
        p = OraProfiler.OraProfiler(self.host, self.port, self.dbname, self.user, self.passwd)
        p.profile_most_freq_values_enabled = 20
        c = p.get_column_most_freq_values(u'SCOTT', u'CUSTOMER')

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
                          [107779, 1]], c['C_CUSTKEY'])
#        self.assertEqual([], c['C_NAME'])
#        self.assertEqual([], c['C_ADDRESS'])
#        self.assertEqual([], c['C_NATIONKEY'])
#        self.assertEqual([], c['C_PHONE'])
#        self.assertEqual([], c['C_ACCTBAL'])
        self.assertEqual([[u'AUTOMOBILE', 7],
                          [u'BUILDING  ', 7],
                          [u'MACHINERY ', 6],
                          [u'FURNITURE ', 5],
                          [u'HOUSEHOLD ', 3]], c['C_MKTSEGMENT'])
#        self.assertEqual([], c['C_COMMENT'])

    def test_get_column_least_freq_values_001(self):
        p = OraProfiler.OraProfiler(self.host, self.port, self.dbname, self.user, self.passwd)
        p.profile_most_freq_values_enabled = 20
        c = p.get_column_least_freq_values(u'SCOTT', u'CUSTOMER')

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
                          [107779, 1]], c['C_CUSTKEY'])
#        self.assertEqual([], c['C_NAME'])
#        self.assertEqual([], c['C_ADDRESS'])
#        self.assertEqual([], c['C_NATIONKEY'])
#        self.assertEqual([], c['C_PHONE'])
        self.assertEqual([[-686.400024, 1],
                          [-659.549988, 1],
                          [-546.880005, 1],
                          [-275.339996, 1],
                          [-65.4599991, 1],
                          [358.380005, 1],
                          [818.330017, 1],
                          [1661.3700000000001, 1],
                          [2095.41992, 1],
                          [3205.6001, 1],
                          [4128.41016, 1],
                          [4543.02002, 1],
                          [4652.75, 1],
                          [4809.83984, 1],
                          [5009.549800000001, 1],
                          [5555.41016, 1],
                          [6264.22998, 1],
                          [6295.5, 1],
                          [6629.20996, 1],
                          [6958.600100000001, 1]], c['C_ACCTBAL'])
#        self.assertEqual([], c['C_MKTSEGMENT'])
#        self.assertEqual([], c['C_COMMENT'])

    def test_get_column_cardinalities_001(self):
        p = OraProfiler.OraProfiler(self.host, self.port, self.dbname, self.user, self.passwd)
        c = p.get_column_cardinalities(u'SCOTT', u'CUSTOMER')

        self.assertEqual(28, c['C_CUSTKEY'])
        self.assertEqual(28, c['C_NAME'])
        self.assertEqual(28, c['C_ADDRESS'])
        self.assertEqual(21, c['C_NATIONKEY'])
        self.assertEqual(28, c['C_PHONE'])
        self.assertEqual(28, c['C_ACCTBAL'])
        self.assertEqual(5, c['C_MKTSEGMENT'])
        self.assertEqual(28, c['C_COMMENT'])

    def test_run_record_valition_001(self):
        p = OraProfiler.OraProfiler(self.host, self.port, self.dbname, self.user, self.passwd)
        r = [(1, 'ORCL','SCOTT','CUSTOMER','C_CUSTKEY','','regexp','^\d+$', ''),
             (2, 'ORCL','SCOTT','CUSTOMER','C_CUSTKEY','','eval','{C_CUSTKEY} > 0 and {C_CUSTKEY} < 1000000', ''),
             (3, 'ORCL','SCOTT','CUSTOMER','C_ACCTBAL','','eval','{C_ACCTBAL} > 0', ''),
             (4, 'ORCL','SCOTT','CUSTOMER','C_CUSTKEY,C_NATIONKEY','','eval','{C_CUSTKEY} > {C_NATIONKEY}', ''),
             (5, 'ORCL','SCOTT','CUSTOMER','C_CUSTKEY,C_NATIONKEY','','eval','{C_CUSTKEY} < {C_NATIONKEY}', ''),
             (6, 'ORCL','SCOTT','CUSTOMER','C_CUSTKEY','','eval','{C_CUSTKEY} > 0 and', ''),
             (7, 'ORCL','SCOTT','CUSTOMER','C_CUSTKEY','','eval','{C_CUSTKEY2} > 0', '')]

        c = p.run_record_validation(u'SCOTT', u'CUSTOMER', r)

        self.maxDiff = None
        # record validation (regrep/eval) only
        self.assertEqual(5, c['C_ACCTBAL'][0]['invalid_count'])

        self.assertEqual(0, c['C_CUSTKEY'][0]['invalid_count'])
        self.assertEqual(0, c['C_CUSTKEY'][1]['invalid_count'])
        self.assertEqual(0, c['C_CUSTKEY'][2]['invalid_count'])
        self.assertEqual(28, c['C_CUSTKEY'][3]['invalid_count'])
        self.assertEqual(28, c['C_CUSTKEY'][4]['invalid_count'])
        self.assertEqual(28, c['C_CUSTKEY'][5]['invalid_count'])

        self.assertEqual(0, c['C_NATIONKEY'][0]['invalid_count'])
        self.assertEqual(28, c['C_NATIONKEY'][1]['invalid_count'])

        # case-sensitive?
        self.assertEqual({}, p.run_record_validation('SCOTT', 'customer', r))
        self.assertEqual({}, p.run_record_validation('scott', u'CUSTOMER', r))

if __name__ == '__main__':
    unittest.main()
