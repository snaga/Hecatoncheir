#!/usr/bin/env python2.7
# -*- coding: utf-8 -*-

import json
import os
import re
import sys
import unittest
sys.path.append('..')

from hecatoncheir import DbProfilerFormatter
from hecatoncheir.exception import DbProfilerException, InternalError

class TestDbProfilerFormatter(unittest.TestCase):
    def setUp(self):
        self.maxDiff = None

    def test_format_non_null_ratio_001(self):
        self.assertEqual('75.00 %', DbProfilerFormatter.format_non_null_ratio(100,25))
        self.assertEqual('100.00 %', DbProfilerFormatter.format_non_null_ratio(100,0))

        # N/A
        self.assertEqual('N/A', DbProfilerFormatter.format_non_null_ratio(100,None))
        self.assertEqual('N/A', DbProfilerFormatter.format_non_null_ratio(None,20))

    def test_format_cardinality_001(self):
        self.assertEqual('25.00 %', DbProfilerFormatter.format_cardinality(100,25,0))
        self.assertEqual('50.00 %', DbProfilerFormatter.format_cardinality(100,25,50))
        self.assertEqual('N/A', DbProfilerFormatter.format_cardinality(100,0,100))

        # N/A
        self.assertEqual('N/A', DbProfilerFormatter.format_cardinality(None,25,50))
        self.assertEqual('N/A', DbProfilerFormatter.format_cardinality(100,None,50))
        self.assertEqual('N/A', DbProfilerFormatter.format_cardinality(100,25,None))

    def test_format_value_freq_ratio_001(self):
        self.assertEqual('0.00 %', DbProfilerFormatter.format_value_freq_ratio(100,0,0))
        self.assertEqual('50.00 %', DbProfilerFormatter.format_value_freq_ratio(100,0,50))
        self.assertEqual('100.00 %', DbProfilerFormatter.format_value_freq_ratio(100,50,50))

        # empty table
        self.assertEqual('0.00 %', DbProfilerFormatter.format_value_freq_ratio(0,0,0))

        # N/A
        self.assertEqual('N/A', DbProfilerFormatter.format_value_freq_ratio(None,0,0))
        self.assertEqual('N/A', DbProfilerFormatter.format_value_freq_ratio(0,None,0))
        self.assertEqual('N/A', DbProfilerFormatter.format_value_freq_ratio(0,0,None))

    def test_filter_markdown2html_001(self):
        md = ''
        a = ''
        self.assertEqual(a, DbProfilerFormatter.filter_markdown2html(md))

        md = None
        a = ''
        self.assertEqual(a, DbProfilerFormatter.filter_markdown2html(md))

        md = "# title 1"
        a = '<h1>title 1</h1>'
        self.assertEqual(a, DbProfilerFormatter.filter_markdown2html(md))

    def test_filter_term2popover_001(self):
        a = ('aaa <a tabindex="0" data-toggle="popover" data-trigger="focus" data-html="true" title="abc" data-content="xyz" class="glossary-term">abc</a> bbb', 140)
        b = DbProfilerFormatter.filter_term2popover('aaa abc bbb', 4, 'abc', 'xyz')
        self.assertEqual(a, b)

        a = ('aaa <a tabindex="0" data-toggle="popover" data-trigger="focus" data-html="true" title="abc" data-content="xyz" class="glossary-term">jkl</a> bbb', 140)
        b = DbProfilerFormatter.filter_term2popover('aaa abc bbb', 4, 'jkl', 'xyz', 'abc')
        self.assertEqual(a, b)

    def test_create_popover_content_001(self):
        t = {}
        t['term'] = 'aa'
        t['description_short'] = 'desc'
        a = u"desc<br/><div align=right><a href='glossary.html#aa' target='_glossary'>Details...</a></div>"
        self.assertEqual(a, DbProfilerFormatter.create_popover_content(t))

        t['synonyms'] = ['a','b']
        a = u"desc<br/><br/>Synonym: a, b<div align=right><a href='glossary.html#aa' target='_glossary'>Details...</a></div>"
        self.assertEqual(a, DbProfilerFormatter.create_popover_content(t))

        t['related_terms'] = ['c','d']
        a = u"desc<br/><br/>Synonym: a, b<br/>Related Terms: c, d<div align=right><a href='glossary.html#aa' target='_glossary'>Details...</a></div>"
        self.assertEqual(a, DbProfilerFormatter.create_popover_content(t))

        t['assigned_assets'] = ['e', 'f']
        a = u"desc<br/><br/>Synonym: a, b<br/>Related Terms: c, d<br/>Assigned Assets: e, f<div align=right><a href='glossary.html#aa' target='_glossary'>Details...</a></div>"
        self.assertEqual(a, DbProfilerFormatter.create_popover_content(t))

    def test_filter_glossaryterms_001(self):
        terms = [{'term': 'PV', 'description_short': 'Page Views'}]

        html = 'foo'
        a = 'foo'
        self.assertEqual(a, DbProfilerFormatter.filter_glossaryterms(html, terms))

        html = 'foo PV bar'
        a = u'foo <a tabindex="0" data-toggle="popover" data-trigger="focus" data-html="true" title="PV" data-content="Page Views<br/><div align=right><a href=\'glossary.html#PV\' target=\'_glossary\'>Details...</a></div>" class="glossary-term">PV</a> bar'
        self.assertEqual(a, DbProfilerFormatter.filter_glossaryterms(html, terms))

    def test_format_number_001(self):
        self.assertEqual('1', DbProfilerFormatter.format_number('001'))
        self.assertEqual('100', DbProfilerFormatter.format_number('100'))
        self.assertEqual('1,000', DbProfilerFormatter.format_number('1000'))
        self.assertEqual('1,000,000', DbProfilerFormatter.format_number('1000000'))

        self.assertEqual('N/A', DbProfilerFormatter.format_number(''))
        self.assertEqual('N/A', DbProfilerFormatter.format_number(None))

        with self.assertRaises(InternalError) as cm:
            DbProfilerFormatter.format_number('a')
        self.assertEqual("Could not convert `a' to long.", cm.exception.value)

    def test_format_minmax_001(self):
        self.assertEqual('[ 0, 1 ]', DbProfilerFormatter.format_minmax('0', '1'))
        self.assertEqual('[ 01234567890123456789, 12345678901234567890 ]',
                         DbProfilerFormatter.format_minmax('012345678901234567890123456789',
                                                           '123456789012345678901234567890'))

        self.assertEqual('N/A', DbProfilerFormatter.format_minmax(None, None))

    def test_is_column_unique_001(self):
        self.assertEqual(False, DbProfilerFormatter.is_column_unique(None))

        self.assertEqual(True, DbProfilerFormatter.is_column_unique([{'freq': 1}]))
        self.assertEqual(False, DbProfilerFormatter.is_column_unique([{'freq': 2}]))
        self.assertEqual(True, DbProfilerFormatter.is_column_unique([{'freq': 1}, {'freq': 1}]))
        self.assertEqual(False, DbProfilerFormatter.is_column_unique([{'freq': 2}, {'freq': 1}]))

    def test_format_fks_001(self):
        self.assertEqual([['db', 'public.customer', 'c_custkey', False]],
                         DbProfilerFormatter.format_fks('db',
                                                       ['public.customer.c_custkey']))

        self.assertEqual([['db', 'public.customer', 'c_custkey', True]],
                         DbProfilerFormatter.format_fks('db',
                                                       ['?public.customer.c_custkey']))
        self.assertEqual([],
                         DbProfilerFormatter.format_fks('db',
                                                       None))

    def test_format_freq_values_001(self):
        self.assertEqual([], DbProfilerFormatter.format_freq_values(None, 10, 0))
        self.assertEqual([], DbProfilerFormatter.format_freq_values([], 10, 0))

        self.assertEqual([{'i': 0,
                          'freq': '10',
                          'ratio': '50.00 %',
                          'value': 'aaa'},
                          {'i': 1,
                          'freq': '9',
                          'ratio': '45.00 %',
                          'value': 'bbb'}],
                         DbProfilerFormatter.format_freq_values([{'value': 'aaa', 'freq': 10}, {'value': 'bbb', 'freq': 9}], 20, 0))

    def test_format_validation_item_001(self):
        self.assertEqual(None, DbProfilerFormatter.format_validation_item(None))

        v = {u'statistics': [1, 0],
             u'description': u'\u975eNULL\u3067\u3042\u308b',
             u'rule': [u'c_custkey', u'{nulls} == 0'],
             u'label': 3,
             u'column_names': [u'c_custkey'],
             u'invalid_count': 0}

        self.assertEqual({'desc': u'\u975eNULL\u3067\u3042\u308b',
                          'label': 3,
                          'invalid': 0,
                          'rule': u'{nulls} == 0',
                          'column_name': u'c_custkey'},
                         DbProfilerFormatter.format_validation_item(v))

    def test_format_validation_items_001(self):
        self.assertEqual(([], 0),
                         DbProfilerFormatter.format_validation_items(None))

        v = [{u'statistics': [1, 0],
              u'description': u'\u975eNULL\u3067\u3042\u308b',
              u'rule': [u'c_custkey', u'{nulls} == 0'],
              u'label': 3,
              u'column_names': [u'c_custkey'],
              u'invalid_count': 0},
             {u'statistics': [1, 0],
              u'description': u'\u975eNULL\u3067\u3042\u308b',
              u'rule': [u'c_custkey', u'{nulls} == 0'],
              u'label': 3,
              u'column_names': [u'c_custkey'],
              u'invalid_count': 0},
             {u'statistics': [1, 1],
              u'description': u'\u975eNULL\u3067\u3042\u308b',
              u'rule': [u'c_custkey', u'{nulls} == 1'],
              u'label': 4,
              u'column_names': [u'c_custkey'],
              u'invalid_count': 1}]

        self.assertEqual(([{'desc': u'\u975eNULL\u3067\u3042\u308b',
                            'label': 3,
                            'invalid': 0,
                            'rule': u'{nulls} == 0',
                            'column_name': u'c_custkey'},
                           {'column_name': u'c_custkey',
                            'desc': u'\u975eNULL\u3067\u3042\u308b',
                            'invalid': 1,
                            'label': 4,
                            'rule': u'{nulls} == 1'}], 1),
                         DbProfilerFormatter.format_validation_items(v))

    def test_filter_plain2html_001(self):
        self.assertEqual('', DbProfilerFormatter.filter_plain2html(None))
        self.assertEqual('a<br/>&nbsp;b<br/>&nbsp;&nbsp;c', DbProfilerFormatter.filter_plain2html('a\n b\n  c'))

    def test_format_table_datamapping_001(self):
        self.assertEqual([], DbProfilerFormatter.format_table_datamapping(None))

        dm = []
        self.assertEqual([], DbProfilerFormatter.format_table_datamapping(dm))

        # data mapping for tables
        dm = [{'source_table_name': 'SRCTBL1, SRCTBL2'},
              {'column_name': 'c2',
               'transformation_role': 'direct',
               'source_table_name': 'SRCTBL1, SRCTBL3'}]
        self.assertEqual(['SRCTBL1<br/>&nbsp;SRCTBL2'],
                         DbProfilerFormatter.format_table_datamapping(dm))

    def test_format_column_datamapping_001(self):
        self.assertEqual([], DbProfilerFormatter.format_column_datamapping(None, 'c1'))

        dm = []
        self.assertEqual([], DbProfilerFormatter.format_column_datamapping(dm, 'c1'))

        # data mapping for tables
        dm = [{'source_table_name': 'SRCTBL1, SRCTBL2'},
              {'column_name': 'c1',
               'transformation_role': 'direct',
               'source_table_name': 'SRCTBL2, SRCTBL3'},
              {'column_name': 'c2',
               'transformation_role': 'direct',
               'source_table_name': 'SRCTBL1, SRCTBL3'}]
        self.assertEqual([{'column_name': 'c1',
                           'source_table_name': 'SRCTBL2<br/>&nbsp;SRCTBL3',
                           'transformation_role': 'direct'}],
                         DbProfilerFormatter.format_column_datamapping(dm, 'c1'))
        self.assertEqual([{'column_name': 'c2',
                           'source_table_name': 'SRCTBL1<br/>&nbsp;SRCTBL3',
                           'transformation_role': 'direct'}],
                         DbProfilerFormatter.format_column_datamapping(dm, 'c2'))

    def test_to_table_html_001(self):
        data = """
  {
    "columns": [
      {
        "cardinality": 199999,
        "column_name": "c1",
        "data_type": [
          "NUMBER",
          "22"
        ],
        "max": "200001",
        "min": "1",
        "nulls": 0,
        "validation": {},
        "fk_ref": [ "public.t2.c2" ],
        "comment": "this is column comment on c1 with Term2."
      },
      {
        "cardinality": 200000,
        "column_name": "c2",
        "data_type": [
          "NUMBER",
          "23"
        ],
        "max": "200000",
        "min": "1",
        "nulls": 3,
        "validation": {},
        "fk": [ "?public.t3.c1" ]
      }
    ],
    "database_name": "orcl",
    "row_count": 200000,
    "schema_name": "public",
    "table_name": "t1",
    "timestamp": "2016-05-01T16:15:18.028309",
    "comment": "this is table comment. glossary Term1."
  }
"""

        terms = [{'term': u'Term1', 'description_short': u'short description of Term1'},
                 {'term': u'Term2', 'description_short': u'short description of Term2 for c1'}]

        html = DbProfilerFormatter.to_table_html(json.loads(data), glossary_terms=terms,
                                                 files=['aaa.txt'])
        self.assertIsNotNone(html)
#        print(html)

        # column name, data type, min/max values and cardinality
        self.assertIsNotNone(re.search('<tr>.*c1.*NUMBER \(22\).*\[ 1, 200001 \].*100.00 %', html))
        self.assertIsNotNone(re.search('<tr>.*c2.*NUMBER \(23\).*\[ 1, 200000 \].*100.00 %', html))

        # column comment with tooltip
        self.assertIsNotNone(re.search('#c1_comment.* data-toggle="tooltip" data-html="true" data-placement="top" title=" this is column comment on c1 with Term2. ">', html))
        # glossary markup in column comment.
        self.assertIsNotNone(re.search(u'<a tabindex="0" data-toggle="popover" data-trigger="focus" data-html="true" title="Term2" data-content="short description of Term2 for c1<br/><div align=right><a href=\'glossary.html#Term2\' target=\'_glossary\'>Details...</a></div>" class="glossary-term">Term2</a>', html))

        # table comment with attached file
        self.assertIsNotNone(re.search('this is table comment.', html))
        self.assertIsNotNone(re.search(u'Attached files:', html))
        self.assertIsNotNone(re.search(u'<a href="attachments/aaa.txt">aaa.txt</a></li>', html))
        # glossary markup in table comment.
        self.assertIsNotNone(re.search(u'<a tabindex="0" data-toggle="popover" data-trigger="focus" data-html="true" title="Term1" data-content="short description of Term1<br/><div align=right><a href=\'glossary.html#Term1\' target=\'_glossary\'>Details...</a></div>" class="glossary-term">Term1</a>', html))

        # foreign keys
        self.assertIsNotNone(re.search(u'orcl\.public\.t2\.html#c2.*Being refered from c2 on public.t2', html))
        self.assertIsNotNone(re.search(u'orcl\.public\.t3\.html#c1.*Refering c1 on public.t3 \(guess\)', html))


    def test_to_table_html_002(self):
        data = """
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
        with self.assertRaises(DbProfilerException) as cm:
            DbProfilerFormatter.to_table_html(json.loads(data), template_file='foo.html')
        self.assertEqual("Template file `foo.html' not found.", cm.exception.value)

    def test_to_index_html_001(self):
        data = """
[
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
    "timestamp": "2016-05-01T16:15:18.028309",
    "comment": "this is table comment. foo PV bar."
  }
]
"""

        terms = [{'term': 'PV', 'description_short': 'Page Views'}]

        html = DbProfilerFormatter.to_index_html(json.loads(data), glossary_terms=terms, reponame='testrepo')
        self.assertIsNotNone(html)

#        print(html)
        t = """
                <td>public.t1</td>
                <td>orcl</td>
                <td>public</td>
                <td><a href="orcl.public.t1.html" target="_blank">t1</a></td>
                <td></td>
                <td class="number">200,000</td>
                <td class="number">2</td>
"""
        self.assertTrue(html.find(t) > 0)

        # table comment with tooltip
        self.assertIsNotNone(re.search('#t1_comment.* data-toggle="tooltip" data-html="true" data-placement="top" title=" this is table comment. foo PV bar. ">', html))
        # glossary markup in table comment.
        a = u'foo <a tabindex="0" data-toggle="popover" data-trigger="focus" data-html="true" title="PV" data-content="Page Views<br/><div align=right><a href=\'glossary.html#PV\' target=\'_glossary\'>Details...</a></div>" class="glossary-term">PV</a> bar'
        self.assertIsNotNone(re.search(a, html))

    def test_to_index_html_002(self):
        data = """
[
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
]
"""

        with self.assertRaises(DbProfilerException) as cm:
            DbProfilerFormatter.to_index_html(json.loads(data), reponame='testrepo', template_file='foo.html')
        self.assertEqual("Template file `foo.html' not found.", cm.exception.value)

    def test_to_index_html_003(self):
        data = """
[
  {
    "columns": [
    ],
    "database_name": "orcl",
    "row_count": 200000,
    "schema_name": "public",
    "table_name": "t1",
    "timestamp": "2016-05-01T16:15:18.028309"
  }
]
"""
        # max_panels is not specified, so 'schema7' could be appeared.
        html = DbProfilerFormatter.to_index_html(json.loads(data),
                                                 schemas=[['d','schema%s'%x,0,'Schema for %s'%x] for x in range(1,8)],
                                                 reponame='testrepo')
#        print(html)
        self.assertTrue(html.find('schema6') > 0) # found
        self.assertTrue(html.find('schema7') > 0) # found
        # short description of schema
        self.assertTrue(html.find('Schema for 7') > 0) # found
        self.assertTrue(html.find('index-schemas.html') < 0) # not found

        # max_panels=6, so 'schema7' must not be appeared.
        html = DbProfilerFormatter.to_index_html(json.loads(data),
                                                 schemas=[['d','schema%s'%x,0,None] for x in range(1,8)],
                                                 reponame='testrepo',
                                                 max_panels=6)
#        print(html)
        self.assertTrue(html.find('schema6') > 0) # found
        self.assertTrue(html.find('schema7') < 0) # not found
        self.assertTrue(html.find('index-schemas.html') > 0) # found

    def test_to_index_html_004(self):
        data = """
[
  {
    "columns": [
    ],
    "database_name": "orcl",
    "row_count": 200000,
    "schema_name": "public",
    "table_name": "t1",
    "timestamp": "2016-05-01T16:15:18.028309"
  }
]
"""
        # max_panels is not specified, so 'tag7' could be appeared.
        html = DbProfilerFormatter.to_index_html(json.loads(data),
                                                 tags=[['tag%s'%x,0,'Tag for %s'%x] for x in range(1,8)],
                                                 reponame='testrepo')
#        print(html)
        self.assertTrue(html.find('tag6') > 0) # found
        self.assertTrue(html.find('tag7') > 0) # found
        # short description of tag
        self.assertTrue(html.find('Tag for 7') > 0) # found
        self.assertTrue(html.find('index-tags.html') < 0) # not found

        # max_panels=6, so 'tag7' must not be appeared.
        html = DbProfilerFormatter.to_index_html(json.loads(data),
                                                 tags=[['tag%s'%x,0,None] for x in range(1,8)],
                                                 reponame='testrepo',
                                                 max_panels=6)
#        print(html)
        self.assertTrue(html.find('tag6') > 0) # found
        self.assertTrue(html.find('tag7') < 0) # not found
        self.assertTrue(html.find('index-tags.html') > 0) # found

    def test_to_index_html_005(self):
        data = """
[
  {
    "columns": [
    ],
    "database_name": "orcl",
    "row_count": 200000,
    "schema_name": "public",
    "table_name": "t1",
    "timestamp": "2016-05-01T16:15:18.028309"
  }
]
"""
        # index page for the tag 'tag0'
        html = DbProfilerFormatter.to_index_html(json.loads(data),
                                                 tags=[['tag0',0,'MyTag']],
                                                 files=['aaa.txt'],
                                                 reponame='testrepo')
#        print(html)
        # tag comment with attached file.
        self.assertTrue(re.search(u'Attached files:', html))
        self.assertTrue(re.search(u'<li><a href="attachments/aaa.txt">aaa.txt</a></li>', html))

        # index page for the schema 'orcl'
        html = DbProfilerFormatter.to_index_html(json.loads(data),
                                                 schemas=[['orcl','public',0,'MySchema']],
                                                 files=['bbb.txt'],
                                                 reponame='testrepo')
#        print(html)
        # tag comment with attached file.
        self.assertTrue(re.search(u'Attached files:', html))
        self.assertTrue(re.search(u'<li><a href="attachments/bbb.txt">bbb.txt</a></li>', html))

    def test_to_glossary_html_001(self):
        data = None
        self.assertIsNotNone(DbProfilerFormatter.to_glossary_html(data))

        data = [{'term': u'Term1',
                 'description_short': u'short description of Term1',
                 'description_long': u'long description of Term1',
                 'categories': ['term1cat', 'term1cat2'],
                 'assigned_terms': ['Term2', 'Term4'],
                 'assigned_assets': ['table1', 'table2'],
                 'assigned_assets2': {'table1': ['dqwbtest.public.table1']},
                 'owned_by': 'snaga'},
                {'term': u'Term2', 'description_short': u'short description of Term2 for c1'}]
        html = DbProfilerFormatter.to_glossary_html(data)

#        print(html)

        # term, short desc, long desc, categories, related terms, related assets, owner
        a = u'<td class="wrap"><a name="Term1"></a>Term1</td>'
        self.assertIsNotNone(re.search(a, html))
        a = u'<td class="wrap">short description of '
        self.assertIsNotNone(re.search(a, html))
        a = u'<td class="wrap"><p>long description of '
        self.assertIsNotNone(re.search(a, html))
        a = u'<td class="wrap">term1cat, term1cat2</td>'
        self.assertIsNotNone(re.search(a, html))
        a = u'<td class="wrap">short description of <a tabindex="0" data-toggle="popover" data-trigger="focus" data-html="true" title="Term1" data-content="short description of Term1<br/><br/>Assigned Assets: table1, table2<div align=right><a href=\'glossary.html#Term1\' target=\'_glossary\'>Details...</a></div>" class="glossary-term">Term1</a></td>'
        self.assertIsNotNone(re.search(a, html))
        a = u'<td class="wrap"><a href="dqwbtest.public.table1.html" target=_blank >table1</a>, table2</td>'
        self.assertIsNotNone(re.search(a, html))
        a = u'<td class="wrap">snaga</td>'
        self.assertIsNotNone(re.search(a, html))

        a = u'<td class="wrap"><a name="Term2"></a>Term2</td>'
        self.assertIsNotNone(re.search(a, html))

if __name__ == '__main__':
    unittest.main()
