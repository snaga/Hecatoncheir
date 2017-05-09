#!/usr/bin/env python2.7
# -*- coding: utf-8 -*-

import sys
import unittest
sys.path.append('..')

from hecatoncheir import DbProfilerException
from hecatoncheir.pgsql import PgDriver
from hecatoncheir.validator import SQLValidator

class TestSQLValidator(unittest.TestCase):
    conn = None

    def setUp(self):
        self.d = PgDriver.PgDriver('host=/tmp dbname=%s' % "dqwbtest", "dqwbuser", "dqwbuser")
        self.d.connect()

    def test_validate_eval_001(self):
        kv = {}
        kv['COL1'] = 101
        kv['COL2'] = 201
        self.assertTrue(SQLValidator.validate_eval(kv, '{COL1} > 100'))
        self.assertFalse(SQLValidator.validate_eval(kv, '{COL1} > 101'))
        self.assertTrue(SQLValidator.validate_eval(kv, '200 > {COL1} and {COL1} > 100'))
        self.assertTrue(SQLValidator.validate_eval(kv, '{COL1} < {COL2}'))
        self.assertFalse(SQLValidator.validate_eval(kv, '{COL1} >= {COL2}'))

        # wrong column name
        with self.assertRaises(DbProfilerException.ValidationError) as cm:
            SQLValidator.validate_eval(kv, '{COL3} > 100')
        self.assertEqual(u"パラメータエラー: `{COL3} > 100' {'COL2': 201, 'COL1': 101}", cm.exception.value)

        # invalid syntax
        with self.assertRaises(DbProfilerException.ValidationError) as cm:
            SQLValidator.validate_eval(kv, '{COL1} > 100 and')
        self.assertEqual(u"文法エラー: `101 > 100 and'", cm.exception.value)

    def test_validate_001(self):
        v = SQLValidator.SQLValidator('001-1', rule=['c_custkey', u'select count(distinct c_custkey) from customer', '{count} > 27'])
        self.assertTrue(v.validate(self.d))
        v = SQLValidator.SQLValidator('001-1', rule=['c_custkey', u'select count(distinct c_custkey) from customer', '{count} > 28'])
        self.assertFalse(v.validate(self.d))

        with self.assertRaises(DbProfilerException.DriverError) as cm:
            v.validate(None)
        self.assertEqual(u'Database driver not found.', cm.exception.value)

    def test_validate_002(self):
        v = SQLValidator.SQLValidator('002-1', rule=['c_custkey', u'select count(distinct c_custkey1) from customer', '{count} > 27'])
        with self.assertRaises(DbProfilerException.ValidationError) as cm:
            v.validate(self.d)
        self.assertEqual(u"SQLエラー: `select count(distinct c_custkey1) from customer'", cm.exception.value)

    def test_validate_003(self):
        v = SQLValidator.SQLValidator('003-1', rule=['c_custkey', u'select count(distinct c_custkey) from customer', '{countt} > 27'])
        with self.assertRaises(DbProfilerException.ValidationError) as cm:
            v.validate(self.d)
        self.assertEqual(u"パラメータエラー: `{countt} > 27' {'count': 28L}", cm.exception.value)

        v = SQLValidator.SQLValidator('003-1', rule=['c_custkey', u'select count(distinct c_custkey) from customer', '{countt} === 27'])
        with self.assertRaises(DbProfilerException.ValidationError) as cm:
            v.validate(self.d)
        self.assertEqual(u"パラメータエラー: `{countt} === 27' {'count': 28L}", cm.exception.value)

if __name__ == '__main__':
    unittest.main()
