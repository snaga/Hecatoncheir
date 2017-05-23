#!/usr/bin/env python2.7
# -*- coding: utf-8 -*-

import json
import os
import sys
import unittest
sys.path.append('..')

from hecatoncheir import DbProfilerExp, DbProfilerRepository
from hecatoncheir.DbProfilerExp import export_html

class TestDbProfilerExp(unittest.TestCase):
    repo = None

    def setUp(self):
        self.repo = DbProfilerRepository.DbProfilerRepository()
        self.repo.init()
        self.repo.open()

    def tearDown(self):
        self.repo.close()
        self.repo.destroy()

    def test_parse_table_name_001(self):
        self.assertEqual((None, None, u'a'), DbProfilerExp.parse_table_name(u'a'))
        self.assertEqual((None, u'a', u'b'), DbProfilerExp.parse_table_name(u'a.b'))
        self.assertEqual((u'a', u'b', u'c'), DbProfilerExp.parse_table_name(u'a.b.c'))

    def testExport_html_001(self):
        t = {}
        t['database_name'] = u'test_database'
        t['schema_name'] = u'test_schema'
        t['table_name'] = u't1'
        t['timestamp'] = '2016-04-27T10:06:41.653836'
        t['row_count'] = 0
        t['columns'] = []
        t['tags'] = [u'tag1', u'tag2']

        self.assertTrue(self.repo.append_table(t))

        table_list = self.repo.get_table_list()

        self.assertTrue(export_html(self.repo, tables=table_list, tags=None, schemas=None,
                                    template_path='../hecatoncheir/templates/en',
                                    output_title=self.repo.filename, output_path='./out/export_html_001'))

    def testExport_html_002(self):
        t = {}
        t['database_name'] = u'test_database'
        t['schema_name'] = u'test_schema'
        t['table_name'] = u't1'
        t['timestamp'] = '2016-04-27T10:06:41.653836'
        t['row_count'] = 0
        t['columns'] = []
        t['tags'] = [u'tag1', u'tag2', u'tag3', u'tag4', u'tag5', u'tag6', u'tag7']

        self.assertTrue(self.repo.append_table(t))

        table_list = self.repo.get_table_list()

        self.assertTrue(export_html(self.repo, tables=table_list, tags=None, schemas=None,
                                    template_path='../hecatoncheir/templates/en',
                                    output_title=self.repo.filename, output_path='./out/export_html_002'))

    def testExport_html_003(self):
        t = {}
        t['database_name'] = u'test_database'
        t['schema_name'] = u'test_schema1'
        t['table_name'] = u't1'
        t['timestamp'] = '2016-04-27T10:06:41.653836'
        t['row_count'] = 0
        t['columns'] = []

        self.assertTrue(self.repo.append_table(t))

        t['schema_name'] = u'test_schema2'
        self.assertTrue(self.repo.append_table(t))
        t['schema_name'] = u'test_schema3'
        self.assertTrue(self.repo.append_table(t))
        t['schema_name'] = u'test_schema4'
        self.assertTrue(self.repo.append_table(t))
        t['schema_name'] = u'test_schema5'
        self.assertTrue(self.repo.append_table(t))
        t['schema_name'] = u'test_schema6'
        self.assertTrue(self.repo.append_table(t))

        t['schema_name'] = u'test_schema7'
        self.assertTrue(self.repo.append_table(t))

        table_list = self.repo.get_table_list()

        self.assertTrue(export_html(self.repo, tables=table_list, tags=None, schemas=None,
                                    template_path='../hecatoncheir/templates/en',
                                    output_title=self.repo.filename, output_path='./out/export_html_003'))

    def testExport_html_004(self):
        t = {}
        t['database_name'] = u'test_database'
        t['schema_name'] = u'test_schema'
        t['table_name'] = u't1'
        t['timestamp'] = '2016-04-27T10:06:41.653836'
        t['row_count'] = 0
        t['columns'] = []
        t['tags'] = [u'tag1', u'tag2', u'tag3', u'tag4', u'tag5', u'tag6', u'tag7']

        self.assertTrue(self.repo.append_table(t))

        table_list = self.repo.get_table_list()

        # test for tag ordering on the global index page.
        self.assertTrue(export_html(self.repo, tables=table_list, tags=['tag7','tag6','tag5','tag4','tag3','tag2'], schemas=None,
                                    template_path='../hecatoncheir/templates/en',
                                    output_title=self.repo.filename, output_path='./out/export_html_004'))

        html = ""
        for l in open("./out/export_html_004/index.html"):
            html = html + l

        self.assertTrue(html.find("tag1") < 0)
        self.assertTrue(html.find("tag3") < html.find("tag2"))
        self.assertTrue(html.find("tag4") < html.find("tag3"))
        self.assertTrue(html.find("tag5") < html.find("tag4"))
        self.assertTrue(html.find("tag6") < html.find("tag5"))
        self.assertTrue(html.find("tag7") < html.find("tag6"))

        html = ""
        for l in open("./out/export_html_004/index-tags.html"):
            html = html + l

        self.assertTrue(html.find("tag2") < html.find("tag1"))
        self.assertTrue(html.find("tag3") < html.find("tag2"))
        self.assertTrue(html.find("tag4") < html.find("tag3"))
        self.assertTrue(html.find("tag5") < html.find("tag4"))
        self.assertTrue(html.find("tag6") < html.find("tag5"))
        self.assertTrue(html.find("tag7") < html.find("tag6"))

    def testExport_html_005(self):
        t = {}
        t['database_name'] = u'test_database'
        t['schema_name'] = u'test_schema'
        t['table_name'] = u't1'
        t['timestamp'] = '2016-04-27T10:06:41.653836'
        t['row_count'] = 0
        t['columns'] = [
            {"cardinality": 199999,
             "column_name": "c1",
             "data_type": ["NUMBER", "22"],
             "max": "200001",
             "min": "1",
             "nulls": 0,
             "validation": {},
             "fk_ref": [ "public.t1.c2", "?public.t2.c3" ]},
            {"cardinality": 200000,
             "column_name": "c2",
             "data_type": ["NUMBER", "23"],
             "max": "200000",
             "min": "1",
             "nulls": 3,
             "validation": {},
             "fk": [ "public.t1.c1" ]}]

        self.assertTrue(self.repo.append_table(t))

        table_list = self.repo.get_table_list()

        self.assertTrue(export_html(self.repo, tables=table_list, tags=None, schemas=None,
                                    template_path='../hecatoncheir/templates/en',
                                    output_title=self.repo.filename, output_path='./out/export_html_005'))

if __name__ == '__main__':
    unittest.main()
