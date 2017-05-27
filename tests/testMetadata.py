#!/usr/bin/env python2.7
# -*- coding: utf-8 -*-

from datetime import datetime
import os
import sys
import unittest
sys.path.append('..')

from hecatoncheir.DbProfilerBase import migrate_table_meta
from hecatoncheir.metadata import TableColumnMeta, TableMeta, Tag, TagDesc, SchemaDesc

class TestTableColumnMeta(unittest.TestCase):
    def setUp(self):
        self.maxDiff = None
        pass

    def tearDown(self):
        pass

    def test_migrate_table_meta_001(self):
        t1 = {}
        t1['database_name'] = 'test_database'
        t1['schema_name'] = 'test_schema'
        t1['table_name'] = 'test_table'
        t1['table_name_nls'] = 'tab_nls'
        t1['columns'] = [{'column_name': 'c',
                          'column_name_nls': 'col_nls',
                          'comment': 'col comment',
                          'fk': ['SCHEMA.TABLE1.COL1'],
                          'fk_ref': ['SCHEMA.TABLE1.COL_REF1']},
                         {'column_name': 'c2',
                          'column_name_nls': 'col_nls2',
                          'comment': 'col comment2',
                          'fk': ['SCHEMA.TABLE1.COL2'],
                          'fk_ref': ['SCHEMA.TABLE1.COL_REF2']}]
        t1['comment'] = 'table comment'
        t1['tags'] = ['TAG1', 'TAG2']
        t1['owner'] = 'owner'

        t2 = {}
        t2['database_name'] = 'test_database'
        t2['schema_name'] = 'test_schema'
        t2['table_name'] = 'test_table'
        t2['columns'] = [{'column_name': 'c'}, {'column_name': 'c2'}]

#        print(t2)
        migrate_table_meta(t1, t2)
#        print(t2)
        self.assertEqual({'comment': 'table comment',
                          'tags': ['TAG1', 'TAG2'],
                          'database_name': 'test_database',
                          'table_name_nls': 'tab_nls',
                          'table_name': 'test_table',
                          'schema_name': 'test_schema',
                          'owner': 'owner',
                          'columns': [{'comment': 'col comment',
                                       'column_name_nls': 'col_nls',
                                       'fk_ref': ['SCHEMA.TABLE1.COL_REF1'],
                                       'fk': ['SCHEMA.TABLE1.COL1'],
                                       'column_name': 'c'},
                                      {'comment': 'col comment2',
                                       'column_name_nls': 'col_nls2',
                                       'fk_ref': ['SCHEMA.TABLE1.COL_REF2'],
                                       'fk': ['SCHEMA.TABLE1.COL2'],
                                       'column_name': 'c2'}]},
                         t2)

    def test_tablecolumnmeta_001(self):
        m = TableColumnMeta(u'c')
        self.assertEqual('c', m.name)

        # unicode only, not str.
        with self.assertRaises(AssertionError) as cm:
            c = TableColumnMeta('c')

        # not empty string
        with self.assertRaises(ValueError) as cm:
            c = TableColumnMeta('')
        self.assertEqual("Invalid column name: ''", cm.exception[0])

        # not None
        with self.assertRaises(ValueError) as cm:
            c = TableColumnMeta(None)
        self.assertEqual("Invalid column name: 'None'", cm.exception[0])

    def test_makedic_001(self):
        m = TableColumnMeta(u'c')
        self.assertEqual({'most_freq_vals': [], 'data_type': [], 'min': None, 'max': None, 'nulls': None, 'validation': [], 'least_freq_vals': [], 'cardinality': None, 'column_name': 'c', 'column_name_nls': None, 'comment': None}, m.makedic())

        m.datatype = ['v', 1]
        m.nulls = 2L
        m.min = u'3'
        m.max = u'4'
        m.most_freq_values = [['a', 5], ['b', 6], ['c', 7]]
        m.least_freq_values = [['d', 8], ['e', 9], ['f', 10]]
        m.cardinality = 11
        m.validation = [{'L1': 12, 'L2': 13}]
        m.comment = u'm'
        self.assertEqual({'column_name': 'c',
                          'column_name_nls': None,
                          'data_type': ['v', 1],
                          'min': u'3',
                          'max': u'4',
                          'nulls': 2L,
                          'most_freq_vals': [{'freq': 5, 'value': 'a'},
                                             {'freq': 6, 'value': 'b'},
                                             {'freq': 7, 'value': 'c'}],
                          'least_freq_vals': [{'freq': 8, 'value': 'd'},
                                              {'freq': 9, 'value': 'e'},
                                              {'freq': 10, 'value': 'f'}],
                          'cardinality': 11,
                          'validation': [{'L1': 12, 'L2': 13}],
                          'comment': 'm'}, m.makedic())

    def test_repr_001(self):
        m = TableColumnMeta(u'c')
        m.name_nls = u'n'
        m.datatype = ['v', 1]
        m.nulls = 2L
        m.min = u'3'
        m.max = u'4'
        m.most_freq_values = [['a', 5], ['b', 6], ['c', 7]]
        m.least_freq_values = [['d', 8], ['e', 9], ['f', 10]]
        m.cardinality = 11
        m.validation = [{'L1': 12, 'L2': 13}]
        m.comment = u'm'
        self.assertEqual(u'{"comment": "m", "nulls": 2, "data_type": ["v", 1], "min": "3", "max": "4", "most_freq_vals": [{"freq": 5, "value": "a"}, {"freq": 6, "value": "b"}, {"freq": 7, "value": "c"}], "least_freq_vals": [{"freq": 8, "value": "d"}, {"freq": 9, "value": "e"}, {"freq": 10, "value": "f"}], "column_name_nls": "n", "cardinality": 11, "validation": [{"L2": 13, "L1": 12}], "column_name": "c"}', unicode(m))

    def test_from_json_001(self):
        j = '{"most_freq_vals": [{"freq": 5, "value": "a"}, {"freq": 6, "value": "b"}, {"freq": 7, "value": "c"}], "data_type": ["v", 1], "min": "3", "max": "4", "nulls": 2, "validation": [{"L2": 13, "L1": 12}], "least_freq_vals": [{"freq": 8, "value": "d"}, {"freq": 9, "value": "e"}, {"freq": 10, "value": "f"}], "cardinality": 11, "column_name": "c", "column_name_nls": "n", "comment": "m"}'

        m = TableColumnMeta(u'c')
        m.from_json(j)

        self.assertEqual({'column_name': 'c',
                          'column_name_nls': 'n',
                          'data_type': ['v', 1],
                          'min': u'3',
                          'max': u'4',
                          'nulls': 2L,
                          'most_freq_vals': [{'freq': 5, 'value': 'a'},
                                             {'freq': 6, 'value': 'b'},
                                             {'freq': 7, 'value': 'c'}],
                          'least_freq_vals': [{'freq': 8, 'value': 'd'},
                                              {'freq': 9, 'value': 'e'},
                                              {'freq': 10, 'value': 'f'}],
                          'cardinality': 11,
                          'validation': [{'L1': 12, 'L2': 13}],
                          'comment': 'm'}, m.makedic())


class TestTableMeta(unittest.TestCase):
    def setUp(self):
        self.maxDiff = None
        pass

    def tearDown(self):
        pass

    def test_tablemeta_001(self):
        m = TableMeta(u'd', u's', u't')
        self.assertEqual('d', m.database_name)
        self.assertEqual('s', m.schema_name)
        self.assertEqual('t', m.table_name)

        # unicode only, not str.
        with self.assertRaises(AssertionError) as cm:
            c = TableMeta('d', u's', u't')

        # not empty string
        with self.assertRaises(ValueError) as cm:
            c = TableMeta('', u's', u't')
        self.assertEqual("Invalid database name: ''", cm.exception[0])

        # not None
        with self.assertRaises(ValueError) as cm:
            c = TableMeta(u'd', None, u't')
        self.assertEqual("Invalid schema name: 'None'", cm.exception[0])

    def test_get_column_meta_001(self):
        tablemeta = TableMeta(u'db', u'public', u'customer')
        tablemeta.column_names = ['c_custkey','c_name','c_address','c_nationkey','c_phone','c_acctbal','c_mktsegment','c_comment']
        columnmeta = {}
        for col in tablemeta.column_names:
            tablemeta.columns.append(TableColumnMeta(unicode(col)))

        self.assertEqual(u'c_custkey', tablemeta.get_column_meta('c_custkey').name)
        self.assertEqual(u'c_comment', tablemeta.get_column_meta('c_comment').name)

        with self.assertRaises(ValueError) as cm:
            tablemeta.get_column_meta('nosuch')
        self.assertEqual('Column "nosuch" not found on table "public.customer".',
                         cm.exception[0])

    def test_makedic_001(self):
        m = TableMeta(u'd', u's', u't')
        self.assertEqual('d', m.database_name)
        self.assertEqual('s', m.schema_name)
        self.assertEqual('t', m.table_name)

        self.assertEqual({'timestamp': None, 'database_name': 'd', 'table_name_nls': None, 'row_count': None, 'table_name': 't', 'schema_name': 's', 'columns': [], 'comment': None, 'sample_rows': None}, m.makedic())

        m.table_name_nls = u'テーブル'
        m.timestamp = datetime(2016, 11, 5, 18, 49, 47, 795589)
        m.row_count = 1L
        m.comment = u'M'

        c = TableColumnMeta(u'c')
        c.name_nls = u'n'
        c.datatype = ['v', 1]
        c.nulls = 2L
        c.min = u'3'
        c.max = u'4'
        c.most_freq_values = [['a', 5], ['b', 6], ['c', 7]]
        c.least_freq_values = [['d', 8], ['e', 9], ['f', 10]]
        c.cardinality = 11
        c.validation = [{'L1': 12, 'L2': 13}]
        c.comment = u'm'

        m.columns.append(c)

        self.assertEqual({'timestamp': datetime(2016, 11, 5, 18, 49, 47, 795589),
                          'database_name': u'd',
                          'table_name_nls': u'\u30c6\u30fc\u30d6\u30eb',
                          'row_count': 1L,
                          'table_name': u't',
                          'schema_name': u's',
                          'comment': u'M',
                          'sample_rows': None,
                          'columns': [{'column_name': u'c',
                                       'column_name_nls': u'n',
                                       'data_type': ['v', 1],
                                       'min': u'3',
                                       'max': u'4',
                                       'nulls': 2L,
                                       'most_freq_vals': [{'freq': 5, 'value': 'a'}, {'freq': 6, 'value': 'b'}, {'freq': 7, 'value': 'c'}],
                                       'least_freq_vals': [{'freq': 8, 'value': 'd'}, {'freq': 9, 'value': 'e'}, {'freq': 10, 'value': 'f'}],
                                       'cardinality': 11,
                                       'validation': [{'L2': 13, 'L1': 12}],
                                       'comment': u'm'}]}, m.makedic())
#        print(m.to_json())

    def test_from_json_001(self):
        j = '''
{
  "timestamp": "2016-11-05T18:49:47.795589",
  "database_name": "d",
  "table_name_nls": "\u30c6\u30fc\u30d6\u30eb",
  "row_count": 1,
  "table_name": "t",
  "schema_name": "s",
  "comment": "M",
  "columns": [
    {
      "most_freq_vals": [
        {
          "freq": 5,
          "value": "a"
        },
        {
          "freq": 6,
          "value": "b"
        },
        {
          "freq": 7,
          "value": "c"
        }
      ],
      "data_type": [
        "v",
        1
      ],
      "min": "3",
      "max": "4",
      "nulls": 2,
      "validation": [{
        "L2": 13,
        "L1": 12
      }],
      "least_freq_vals": [
        {
          "freq": 8,
          "value": "d"
        },
        {
          "freq": 9,
          "value": "e"
        },
        {
          "freq": 10,
          "value": "f"
        }
      ],
      "cardinality": 11,
      "column_name": "c",
      "column_name_nls": "n",
      "comment": "m"
    }
  ]
}
'''

        m = TableMeta(u'd', u's', u't')
        m.from_json(j)

        self.assertEqual({'timestamp': datetime(2016, 11, 5, 18, 49, 47, 795589),
                          'database_name': u'd',
                          'table_name_nls': u'\u30c6\u30fc\u30d6\u30eb',
                          'row_count': 1L,
                          'table_name': 't',
                          'schema_name': 's',
                          'comment': 'M',
                          'sample_rows': None,
                          'columns': [{'column_name': 'c',
                                       'column_name_nls': 'n',
                                       'data_type': ['v', 1],
                                       'min': u'3',
                                       'max': u'4',
                                       'nulls': 2L,
                                       'most_freq_vals': [{'freq': 5, 'value': 'a'}, {'freq': 6, 'value': 'b'}, {'freq': 7, 'value': 'c'}],
                                       'least_freq_vals': [{'freq': 8, 'value': 'd'}, {'freq': 9, 'value': 'e'}, {'freq': 10, 'value': 'f'}],
                                       'cardinality': 11,
                                       'validation': [{'L2': 13, 'L1': 12}],
                                       'comment': 'm'}]}, m.makedic())

class TestTag(unittest.TestCase):
    def setUp(self):
        self.maxDiff = None
        pass

    def test_tag_001(self):
        t = Tag(u'l', u't')
        self.assertEqual(u'l', t.label)
        self.assertEqual(u't', t.target)

        with self.assertRaises(AssertionError) as cm:
            t = Tag('l', u't')

        with self.assertRaises(ValueError) as cm:
            t = Tag(u'', u't')
        self.assertEqual("Invalid tag label: ''", cm.exception[0])

        with self.assertRaises(ValueError) as cm:
            t = Tag(u'l', None)
        self.assertEqual("Invalid tag target: 'None'", cm.exception[0])


class TestTagDesc(unittest.TestCase):
    def setUp(self):
        self.maxDiff = None
        pass

    def test_tagdesc_001(self):
        t = TagDesc(u'l', u'd', u'c')
        self.assertEqual(u'l', t.label)
        self.assertEqual(u'd', t.desc)
        self.assertEqual(u'c', t.comment)

        with self.assertRaises(AssertionError) as cm:
            t = TagDesc('l', u'd', u'c')

        with self.assertRaises(ValueError) as cm:
            t = TagDesc(u'', u'd', u'c')
        self.assertEqual("Invalid tag label: ''", cm.exception[0])


class TestSchemaDesc(unittest.TestCase):
    def setUp(self):
        self.maxDiff = None
        pass

    def test_schemadesc_001(self):
        t = SchemaDesc(u's', u'd', u'c')
        self.assertEqual(u's', t.name)
        self.assertEqual(u'd', t.desc)
        self.assertEqual(u'c', t.comment)

        with self.assertRaises(AssertionError) as cm:
            t = SchemaDesc('s', u'd', u'c')

        with self.assertRaises(ValueError) as cm:
            t = SchemaDesc(u'', u'd', u'c')
        self.assertEqual("Invalid schema name: ''", cm.exception[0])

if __name__ == '__main__':
    unittest.main()
