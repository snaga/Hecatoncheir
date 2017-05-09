#!/usr/bin/env python2.7
# -*- coding: utf-8 -*-

import os
import sys
import unittest
sys.path.append('..')

from hecatoncheir import CSVUtils

class TestCSVUtils(unittest.TestCase):
    def setUp(self):
        pass

    def test_csv2list_001(self):
        self.assertEqual(['1','2','3'], CSVUtils.csv2list(u'1,2,3'))
        self.assertEqual(['1\n','2\r3','4\n\r5'], CSVUtils.csv2list(u'"1\n","2\r3","4\n\r5"'))
        self.assertEqual(['1,','2,3',',4'], CSVUtils.csv2list(u'"1,","2,3",",4"'))
        self.assertEqual(['1"2',''], CSVUtils.csv2list(u'"1""2",'))

    def test_csvquote_001(self):
        self.assertEqual('foo', CSVUtils.csvquote('foo'))
        self.assertEqual('"foo,"', CSVUtils.csvquote('foo,'))
        self.assertEqual('"fo""o"', CSVUtils.csvquote('fo"o'))
        self.assertEqual('"fo\no"', CSVUtils.csvquote('fo\no'))

    def test_list2csv_001(self):
        self.assertEqual('1,2,3', CSVUtils.list2csv(['1','2','3']))
        self.assertEqual('"1\n","2\r3","4\n\r5"', CSVUtils.list2csv(['1\n','2\r3','4\n\r5']))
        self.assertEqual('"1,","2,3",",4"', CSVUtils.list2csv(['1,','2,3',',4']))
        self.assertEqual('"1""2",', CSVUtils.list2csv(['1"2',None]))

    def test_csvreader_001(self):
        csv = CSVUtils.CSVReader('validation_test.txt')

        with self.assertRaises(IOError) as cm:
            csv = CSVUtils.CSVReader('validation_test_nosuch.txt')
        self.assertEqual("[Errno 2] No such file or directory: 'validation_test_nosuch.txt'", str(cm.exception))

    def test_check_header_001(self):
        # DATABASE_NAME,SCHEMA_NAME,TABLE_NAME,COLUMN_NAME,RULE,PARAM,PARAM2
        csv = CSVUtils.CSVReader('validation_test.txt')

        # csv has the exact same columns
        self.assertTrue(csv.check_header(['database_name','schema_name','table_name','column_name','description', 'rule','param','param2']))
        self.assertEqual(['DATABASE_NAME','SCHEMA_NAME','TABLE_NAME','COLUMN_NAME','DESCRIPTION', 'RULE','PARAM', 'PARAM2'], csv.header)

        # csv has a required column
        csv = CSVUtils.CSVReader('validation_test.txt')
        self.assertTrue(csv.check_header(['database_name']))
        self.assertEqual(['DATABASE_NAME','SCHEMA_NAME','TABLE_NAME','COLUMN_NAME','DESCRIPTION', 'RULE','PARAM', 'PARAM2'], csv.header)

    def test_check_header_002(self):
        # csv does not have a required column
        csv = CSVUtils.CSVReader('validation_test.txt')
        self.assertFalse(csv.check_header(['nosuch']))

        # csv does not have a required column
        csv = CSVUtils.CSVReader('validation_test.txt')
        self.assertFalse(csv.check_header(['database_name','schema_name','table_name','column_name','rule','param','param2', 'extra']))

    def test_check_readline_001(self):
        csv = CSVUtils.CSVReader('validation_test.txt')
        self.assertTrue(csv.check_header(['database_name','schema_name','table_name','column_name','rule','param','param2']))

        v = csv.readline().next()
        self.assertEqual([u'', u'public', u't1', u'c1', u'desc regexp', u'regexp', u'^\\d+$', ''], v)

    def test_check_readline_002(self):
        csv = CSVUtils.CSVReader('validation_test.txt')
        self.assertTrue(csv.check_header(['database_name','schema_name','table_name','column_name','rule','param','param2']))

        for r in csv.readline():
            print(r)
        self.assertTrue(True)

    def test_check_readline_as_dict_001(self):
        csv = CSVUtils.CSVReader('validation_test.txt')
        self.assertTrue(csv.check_header(['database_name','schema_name','table_name','column_name','rule','param','param2']))

        v = csv.readline_as_dict().next()
        self.assertEqual({'DATABASE_NAME':u'', 'PARAM': u'^\\d+$', 'PARAM2': u'', 'TABLE_NAME': u't1', 'RULE': u'regexp', 'SCHEMA_NAME': u'public', 'COLUMN_NAME': u'c1', 'DESCRIPTION': 'desc regexp'}, v)

    def test_check_readline_as_dict_002(self):
        csv = CSVUtils.CSVReader('validation_test.txt')
        self.assertTrue(csv.check_header(['database_name','schema_name','table_name','column_name','rule','param','param2']))

        for r in csv.readline_as_dict():
            print(r)
        self.assertTrue(True)

if __name__ == '__main__':
    unittest.main()
