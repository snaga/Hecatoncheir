#!/usr/bin/env python2.7
# -*- coding: utf-8 -*-

import json
import os
import re
import sys
import unittest
sys.path.append('..')

from hecatoncheir import DbProfilerException
from hecatoncheir import DbProfilerValidator
from hecatoncheir.pgsql import PgDriver

class TestDbProfilerValidator(unittest.TestCase):
    # --------------------------------
    # DbProfilerValidator
    # --------------------------------
    def test_DbProfilerValidator_001(self):
        v = DbProfilerValidator.DbProfilerValidator("public", "t1")
        self.assertEqual("public", v.schema_name)
        self.assertEqual("t1", v.table_name)
        self.assertEqual(0, len(v.get_validation_results()))
        """
        v.add_rule_regexp("c1:regexp:\\d+", "c1", '^\d+$')
        v.add_rule_notnull("c1:notnull", "c1")

        print v.rule_list

        n = ['c1', 'c2']
        r = ['123', 'abc']
        v.validateRecord(n, r)
        r = ['abc', 'abc']
        v.validateRecord(n, r)
        r = ['abc123', 'abc']
        v.validateRecord(n, r)
        r = [None, 'abc']
        v.validateRecord(n, r)
        print v.stats
        """

    # -----------------------------
    # on-the-fly validation
    # -----------------------------
    def test_add_rule_regexp_001(self):
        v = DbProfilerValidator.DbProfilerValidator("public", "t1")

        v.add_rule_regexp("c1:regexp:\\d+", "c1", '^\d+$')

        self.assertEqual(1, len(v.record_validators))
        self.assertTrue('c1:regexp:\\d+' in v.record_validators)
        self.assertEqual(['c1', '^\d+$'], v.record_validators['c1:regexp:\\d+'].rule)

    def test_add_rule_eval_001(self):
        v = DbProfilerValidator.DbProfilerValidator("public", "t1")

        v.add_rule_eval("c1:eval1", "c1", '{c1} > 100')

        self.assertEqual(1, len(v.record_validators))
        self.assertTrue('c1:eval1' in v.record_validators)
        self.assertEqual(['c1', '{c1} > 100'], v.record_validators['c1:eval1'].rule)

    def test_add_rule_eval_002(self):
        v = DbProfilerValidator.DbProfilerValidator("public", "t1")

        # eval using multi column
        v.add_rule_eval("c1c2:eval1", "c1,c2", '{c1} > {c2}')

        self.assertEqual(1, len(v.record_validators))
        self.assertTrue('c1c2:eval1' in v.record_validators)
        self.assertEqual(['c1,c2', '{c1} > {c2}'], v.record_validators['c1c2:eval1'].rule)

    # -----------------------------
    # post-scan validation
    # -----------------------------
    def test_add_rule_sql_001(self):
        v = DbProfilerValidator.DbProfilerValidator("public", "t1")

        self.assertEqual(0, len(v.sql_validators))
        v.add_rule_sql("c1:count", "c1", 'SELECT count(distinct c1) as count FROM t1', '{count} < 100')
        self.assertEqual(1, len(v.sql_validators))

        self.assertTrue('c1:count' in v.sql_validators)
        self.assertEqual('c1', v.sql_validators['c1:count'].rule[0])
        self.assertEqual('{count} < 100', v.sql_validators['c1:count'].rule[2])

        v.add_rule_sql("c2:count", "c2", 'SELECT count(distinct c2) as count FROM t1', '{count} < 10')
        self.assertEqual(2, len(v.sql_validators))

        self.assertTrue('c2:count' in v.sql_validators)
        self.assertEqual('c2', v.sql_validators['c2:count'].rule[0])
        self.assertEqual('{count} < 10', v.sql_validators['c2:count'].rule[2])

    def test_add_rule_001(self):
        # success
        a = [(1, u'dqwbtest', u'public', u'customer', u'c_custkey', u'\u3059\u3079\u3066\u6570\u5b57\u306e\u307f\u3067\u69cb\u6210\u3055\u308c\u3066\u3044\u308b', u'regexp', u'^\\d+$', u''),
             (2, u'dqwbtest', u'public', u'customer', u'c_acctbal', u'c_acctbal\u306e\u5024\u304c0\u3088\u308a\u5927\u304d\u3044', u'eval', u'{c_acctbal} > 0', u''),
             (3, u'dqwbtest', u'public', u'customer', u'c_custkey', u'\u30ab\u30fc\u30c7\u30a3\u30ca\u30ea\u30c6\u30a3\u304c28', u'columnstat', u'{cardinality} == 28', u''),
             (4, u'dqwbtest', u'public', u'customer', u'c_custkey', u'\u30ec\u30b3\u30fc\u30c9\u6570\u304c0', u'sql', u'select count(*) from\ncustomer', u'{count} == 0')]

        v = DbProfilerValidator.DbProfilerValidator("public", "customer")
        for r in a:
            self.assertTrue(v.add_rule(r[0],r[1],r[2],r[3],r[4],r[5],r[6],r[7],r[8]))

        # fail. different table name
        v = DbProfilerValidator.DbProfilerValidator("public", "supplier")
        for r in a:
            self.assertFalse(v.add_rule(r[0],r[1],r[2],r[3],r[4],r[5],r[6],r[7],r[8]))

        r = (1, u'dqwbtest', u'public', u'customer', u'c_custkey', u'\u3059\u3079\u3066\u6570\u5b57\u306e\u307f\u3067\u69cb\u6210\u3055\u308c\u3066\u3044\u308b', u'regex', u'^\\d+$', u'')
        v = DbProfilerValidator.DbProfilerValidator("public", "customer")

        # exception. unsupported rule
        with self.assertRaises(DbProfilerException.InternalError) as cm:
            v.add_rule(r[0],r[1],r[2],r[3],r[4],r[5],r[6],r[7],r[8])
        self.assertEqual(u"サポートされていない検証ルールです: regex", cm.exception.value)

    def test_has_validation_rules_001(self):
        v = DbProfilerValidator.DbProfilerValidator("public", "t1")

        self.assertFalse(v.has_validation_rules())
        v.add_rule_columnstat("c1:columnstat", "c1", '{min} < 100')
        self.assertTrue(v.has_validation_rules())

    def test_validate_record_001(self):
        v = DbProfilerValidator.DbProfilerValidator("public", "t1")

        # regexp
        v.add_rule_regexp("c1:regexp:\\d+", "c1", '^\d+$')

        n = ['c1']
        r = ['123']
        self.assertTrue(v.validate_record(n, r))

        r = ['abc']
        self.assertFalse(v.validate_record(n, r))

        self.assertEqual(1, len(v.get_validation_results()['c1']))
        self.assertEqual({'label': 'c1:regexp:\\d+',
                          'invalid_count': 1,
                          'column_names': ['c1'],
                          'description': '',
                          'rule': ['c1', '^\\d+$'],
                          'statistics': [2, 1]},
                         v.get_validation_results()['c1'][0])

        n = ['c0', 'c1']
        r = ['abc', '123']
        self.assertTrue(v.validate_record(n, r))

        r = ['abc', 'abc']
        self.assertFalse(v.validate_record(n, r))

        self.assertEqual(1, len(v.get_validation_results()['c1']))
        self.assertEqual({'label': 'c1:regexp:\\d+',
                          'invalid_count': 2,
                          'column_names': ['c1'],
                          'description': '',
                          'rule': ['c1', '^\\d+$'],
                          'statistics': [4, 2]},
                         v.get_validation_results()['c1'][0])

    def test_validate_record_002(self):
        v = DbProfilerValidator.DbProfilerValidator("public", "t1")

        # eval using multi column
        v.add_rule_eval("c1c2:evalmulti:{c1} > {c2}", "c1,c2", '{c1} > {c2}')

        n = ['c1', 'c2']
        r = ['1', '0']
        self.assertTrue(v.validate_record(n, r))

        r = ['1', '1']
        self.assertFalse(v.validate_record(n, r))


    def test_validate_record_003(self):
        v = DbProfilerValidator.DbProfilerValidator("public", "t1")

        # eval
        v.add_rule_eval("c1:eval1", 'c1', "{c1} > 100")

        self.assertTrue(v.validate_record(['c1'], ['123']))
        self.assertFalse(v.validate_record(['c1'], ['100']))

        # FIXME: Must be false
        self.assertTrue(v.validate_record(['c1'], ['abc']))

        self.assertEqual(1, len(v.get_validation_results()['c1']))
        self.assertEqual({'label': 'c1:eval1',
                          'invalid_count': 1,
                          'column_names': ['c1'],
                          'description': '',
                          'rule': ['c1', '{c1} > 100'],
                          'statistics': [3, 1]},
                         v.get_validation_results()['c1'][0])

        self.assertTrue(v.validate_record(['c0', 'c1'], ['abc', '123']))
        self.assertFalse(v.validate_record(['c0', 'c1'], ['abc', '100']))

        # FIXME: Must be false
        self.assertTrue(v.validate_record(['c0', 'c1'], ['abc', 'abc']))

        self.assertEqual(1, len(v.get_validation_results()['c1']))
        self.assertEqual({'label': 'c1:eval1',
                          'invalid_count': 2,
                          'column_names': ['c1'],
                          'description': '',
                          'rule': ['c1', '{c1} > 100'],
                          'statistics': [6, 2]},
                         v.get_validation_results()['c1'][0])

    def test_table_data_get_column_data_001(self):
        v = DbProfilerValidator.DbProfilerValidator("public", "t1")

        json_str = """
  {
    "columns": [
      {
        "cardinality": 199999,
        "column_name": "c1",
        "data_type": [
          "NUMBER",
          "22"
        ],
        "max": "200000",
        "min": "1",
        "nulls": 0,
        "validation": {}
      },
      {
        "cardinality": 200000,
        "column_name": "c2",
        "data_type": [
          "NUMBER",
          "22"
        ],
        "max": "200000",
        "min": "1",
        "nulls": 3,
        "validation": {}
      }
    ],
    "database_name": "orcl",
    "row_count": 200000,
    "schema_name": "public",
    "table_name": "t1",
    "timestamp": "2016-05-01T16:15:18.028309"
  }
"""
        table_data = json.loads(json_str)

        self.assertIsNone(v.table_data_get_column_data(None, None))
        self.assertIsNone(v.table_data_get_column_data(table_data, None))

        c1 = {u'validation': {}, u'data_type': [u'NUMBER', u'22'], u'min': u'1', u'max': u'200000', u'nulls': 0, u'cardinality': 199999, u'column_name': u'c1'}

        self.assertEqual(c1, v.table_data_get_column_data(table_data, 'c1'))
        self.assertIsNone(v.table_data_get_column_data(table_data, 'nosuch'))

    def test_validate_table_001(self):
        v = DbProfilerValidator.DbProfilerValidator("public", "t1")

        json_str = """
  {
    "columns": [
      {
        "cardinality": 199999,
        "column_name": "c1",
        "data_type": [
          "NUMBER",
          "22"
        ],
        "max": "200000",
        "min": "1",
        "nulls": 0,
        "validation": {}
      },
      {
        "cardinality": 200000,
        "column_name": "c2",
        "data_type": [
          "NUMBER",
          "22"
        ],
        "max": "200000",
        "min": "1",
        "nulls": 3,
        "validation": {}
      }
    ],
    "database_name": "orcl",
    "row_count": 200000,
    "schema_name": "public",
    "table_name": "t1",
    "timestamp": "2016-05-01T16:15:18.028309"
  }
"""
        table_data = json.loads(json_str)

        # column c1
        v.add_rule_columnstat("c1:notnull", "c1", '{nulls} == 0') # ok
        v.add_rule_columnstat("c1:unique", "c1", '{cardinality} == {rows}') # fail
        v.add_rule_columnstat("c1:minmax", "c1", '{min} > 1') # fail
        v.add_rule_columnstat("c1:cardinality", "c1", '{cardinality} == 199999') # ok

        # column c2
        v.add_rule_columnstat("c2:notnull", "c2", '{nulls} == 0') # fail
        v.add_rule_columnstat("c2:unique", "c2", '{cardinality} == {rows}') # ok
        v.add_rule_columnstat("c2:minmax", "c2", '{min} >= 1') # ok
        v.add_rule_columnstat("c2:cardinality", "c2", '{cardinality} == 199999') # fail

        # dealing with exception
        v.add_rule_columnstat("c1:notnull2", "c1", '{nulls} === 0') # fail
        v.add_rule_columnstat("c1:notnull3", "c1", '{nulls2} == 0') # fail

        self.assertEqual((10,6), v.validate_table(table_data))
        v.update_table_data(table_data)

        self.assertEqual(6, len(table_data['columns'][0]['validation']))
        self.assertEqual(2, table_data['columns'][0]['validation'][0]['invalid_count']) # c1:unique
        self.assertEqual(2, table_data['columns'][0]['validation'][1]['invalid_count']) # c1:minmax
        self.assertEqual(0, table_data['columns'][0]['validation'][2]['invalid_count']) # c1:notnull
        self.assertEqual(0, table_data['columns'][0]['validation'][3]['invalid_count']) # c1:cardinality
        self.assertEqual(2, table_data['columns'][0]['validation'][4]['invalid_count']) # c1:notnull3
        self.assertEqual(2, table_data['columns'][0]['validation'][5]['invalid_count']) # c1:notnull2

        self.assertEqual(4, len(table_data['columns'][1]['validation']))
        self.assertEqual(0, table_data['columns'][1]['validation'][0]['invalid_count']) # c2:minmax
        self.assertEqual(0, table_data['columns'][1]['validation'][1]['invalid_count']) # c2:unique
        self.assertEqual(2, table_data['columns'][1]['validation'][2]['invalid_count']) # c2:cardinality
        self.assertEqual(2, table_data['columns'][1]['validation'][3]['invalid_count']) # c2:notnull

    def test_validate_sql_001(self):
        pg = PgDriver.PgDriver('host=/tmp dbname=%s' % "dqwbtest", "dqwbuser", "dqwbuser")
        pg.connect()

        v = DbProfilerValidator.DbProfilerValidator("public", "t1")

        v.add_rule_sql("c_custkey:count", "c_custkey", u'select count(distinct c_custkey) from customer', '{count} < 100')
        self.assertEqual((1,0), v.validate_sql(pg))

        v.add_rule_sql("l_orderkey:count", "l_orderkey", u'select count(l_orderkey) from lineitem', '{count} < 100')
        self.assertEqual((2,1), v.validate_sql(pg))

    def test_validate_sql_002(self):
        pg = PgDriver.PgDriver('host=/tmp dbname=%s' % "dqwbtest", "dqwbuser", "dqwbuser")
        pg.connect()

        v = DbProfilerValidator.DbProfilerValidator("public", "t1")

        # invalid syntax
        v.add_rule_sql("002-1", "c_custkey", u'select count(c_custkey) from customer', '{count} === 100')
        # wrong parameter name
        v.add_rule_sql("002-2", "c_custkey", u'select count(c_custkey) from customer', '{count2} == 100')
        # SQL error.
        v.add_rule_sql("002-3", "c_custkey", u'select count(c_custkey) from customer2', '{count} == 100')

        self.assertEqual((3,3), v.validate_sql(pg))

    def test_validate_sql_003(self):
        v = DbProfilerValidator.DbProfilerValidator("public", "t1")

        v.add_rule_sql("c_custkey:count", "c_custkey", 'select count(distinct c_custkey) from customer', '{count} < 100')

        with self.assertRaises(DbProfilerException.DriverError) as cm:
            v.validate_sql(None)
        self.assertEqual(u"Database driver not found.", cm.exception.value)

if __name__ == '__main__':
    unittest.main()
