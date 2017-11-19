#!/usr/bin/env python2.7
# -*- coding: utf-8 -*-

import json
import os
import sys
import unittest
sys.path.append('../..')

import BigQueryDriver
from hecatoncheir import DbProfilerBase, DbProfilerValidator, logger as log
from hecatoncheir.QueryResult import QueryResult
from hecatoncheir.exception import InternalError
from hecatoncheir.logger import str2unicode as _s2u
from hecatoncheir.msgutil import gettext as _


class BigQueryProfiler(DbProfilerBase.DbProfilerBase):
    dbdriver = None
    dbconn = None
    column_cache = None

    def __init__(self, credential, debug=False):
        os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = credential

        with open(credential) as f:
            l = f.read()
        creds = json.loads(l)
        project = creds['project_id']

        DbProfilerBase.DbProfilerBase.__init__(self, None, None, project,
                                               None, None, debug)

        self.dbdriver = BigQueryDriver.BigQueryDriver(project)
        self.connect()

    def get_schema_names(self):
        ignores = []

        datasets = []
        for n in self.dbdriver.conn.list_datasets():
            datasets.append(n.dataset_id)
        return datasets

    def get_table_names(self, schema_name):
        tables = []
        ds = self.dbdriver.conn.dataset(schema_name)
        for t in self.dbdriver.conn.list_dataset_tables(ds):
            tables.append(t.table_id)
        return tables

    def _get_table(self, schema_name, table_name):
        ds = self.dbdriver.conn.dataset(schema_name)
        tab_ref = self.dbdriver.driver.table.TableReference(ds, table_name)
        return self.dbdriver.conn.get_table(tab_ref)

    def get_column_names(self, schema_name, table_name):
        tab = self._get_table(schema_name, table_name)
        fields = []
        for f in tab.schema:
            fields.append(f.name)
        return fields

    def get_sample_rows(self, schema_name, table_name, rows_limit=10):
        tab = self._get_table(schema_name, table_name)
        rows = []
        rows.append(self.get_column_names(schema_name, table_name))
        for r in self.dbdriver.conn.list_rows(tab, max_results=rows_limit):
            rows.append(list(r))
        return rows

    def get_column_datatypes(self, schema_name, table_name):
        tab = self._get_table(schema_name, table_name)
        types = {}
        for f in tab.schema:
            types[f.name] = [f.field_type, 0]
        return types

    def get_row_count(self, schema_name, table_name):
        tab = self._get_table(schema_name, table_name)
        return tab.num_rows

    def _init_column_cache(self, schema_name, table_name):
        if not self.column_cache:
            self.column_cache = {}
        if schema_name not in self.column_cache:
            self.column_cache[schema_name] = {}
        if table_name not in self.column_cache[schema_name]:
            self.column_cache[schema_name][table_name] = {}

    def _get_column(self, schema_name, table_name):
        column_names = self.get_column_names(schema_name, table_name)
        columns = []
        for col in column_names:
            columns.append('COUNT(CASE WHEN %s IS NULL THEN 1 ELSE NULL END) %s_nulls' % (col, col))
            columns.append('MIN(%s) %s_min' % (col, col))
            columns.append('MAX(%s) %s_max' % (col, col))
        q = u'SELECT {0} FROM {1}.{2}'.format(','.join(columns), schema_name, table_name)
        #print(q)
        r = self.dbdriver.q2rs(q).resultset[0]
        for c in column_names:
            self.column_cache[schema_name][table_name][c] = (r.pop(0), r.pop(0), r.pop(0))
            #print('column %s cache %s' % (c, self.column_cache[schema_name][table_name][c]))

    def get_column_nulls(self, schema_name, table_name):
        column_names = self.get_column_names(schema_name, table_name)
        self._init_column_cache(schema_name, table_name)

        if not self.column_cache[schema_name][table_name]:
            self._get_column(schema_name, table_name)

        nulls = {}
        for c in column_names:
            nulls[c] = self.column_cache[schema_name][table_name][c][0]
        return nulls

    def get_column_min_max(self, schema_name, table_name):
        column_names = self.get_column_names(schema_name, table_name)
        self._init_column_cache(schema_name, table_name)

        if not self.column_cache[schema_name][table_name]:
            self._get_column(schema_name, table_name)

        minmax = {}
        for c in column_names:
            minmax[c] = [self.column_cache[schema_name][table_name][c][1],
                         self.column_cache[schema_name][table_name][c][2]]
        return minmax

    def _get_column_freq_values(self, schema_name, table_name, column_name, ascending):
        limit = 10

        q = u"""
WITH TEMP AS (
SELECT
  {2},
  COUNT(*) AS COUNT
FROM
  {0}.{1}
WHERE
  {2} IS NOT NULL
GROUP BY
  {2}
)
SELECT * FROM TEMP
ORDER BY
  2 {3}, 1
LIMIT {4}
""".format(schema_name, table_name, column_name,
           'ASC' if ascending else 'DESC',
           limit)

        rs = self.dbdriver.q2rs(q)
        return rs.resultset

    def get_column_most_freq_values(self, schema_name, table_name):
        freqs = {}
        column_names = self.get_column_names(schema_name, table_name)
        for col in column_names:
            freqs[col] = self._get_column_freq_values(schema_name, table_name, col, False)
        return freqs

    def get_column_least_freq_values(self, schema_name, table_name):
        freqs = {}
        column_names = self.get_column_names(schema_name, table_name)
        for col in column_names:
            freqs[col] = self._get_column_freq_values(schema_name, table_name, col, True)
        return freqs

    def get_column_cardinalities(self, schema_name, table_name):
        column_names = self.get_column_names(schema_name, table_name)
        column_cardinalities = {}
        for col in column_names:
            q = u"""
WITH TEMP AS (
SELECT
  DISTINCT {2}
FROM
  {0}.{1}
WHERE
  {2} IS NOT NULL
)
SELECT
  COUNT(*)
FROM
  TEMP
""".format(schema_name, table_name, col)

            rs = self.dbdriver.q2rs(q)
            column_cardinalities[col] = rs.resultset[0][0]
        return column_cardinalities

    def run_record_validation(self, schema_name, table_name, validation_rules):
        timeout = 600
        log.trace('run_record_validation: start. %s.%s' %
                  (schema_name, table_name))

        validator = DbProfilerValidator.DbProfilerValidator(
            schema_name, table_name, validation_rules=validation_rules)
        if not validator.record_validators:
            log.info(_("Skipping record validation since no validation rule."))
            return {}

        column_names = self.get_column_names(schema_name, table_name)
        if not column_names:
            raise InternalError(
                'No column found on the table `%s\'.')
        query = u'SELECT %s FROM %s.%s' % (','.join(column_names),
                                           schema_name, table_name)

        failed = 0
        count = 0
        try:
            if not self.dbdriver.conn:
                self.dbdriver.connect()

            query_job = self.dbdriver.conn.query(query)
            assert query_job
            res_iter = query_job.result(timeout=timeout)
            assert query_job.state == 'DONE'

            fnames = []
            for f in query_job.query_results().schema:
                fnames.append(f.name)

            for row in res_iter:
                if not validator.validate_record(fnames, list(row)):
                    failed += 1
                count += 1
        except Exception as ex:
            raise ex

        log.trace("run_record_validation: end. "
                  "row count %d invalid record %d" % (count, failed))
        return validator.get_validation_results()


class TestBigQueryProfiler(unittest.TestCase):
    def setUp(self):
        self.dbname = os.environ.get('BQ_PROJECT', '')
        self.maxDiff = None

    def test_BigQueryProfiler_001(self):
        p = BigQueryProfiler('bigquery.json')
        self.assertIsNotNone(p)

    def test_get_schema_names_001(self):
        p = BigQueryProfiler('bigquery.json')
        self.assertIsNotNone(p)
        self.assertEquals(['snagatest'], p.get_schema_names())

    def test_get_table_names_001(self):
        p = BigQueryProfiler('bigquery.json')
        self.assertIsNotNone(p)
        self.assertEquals([u'lineitem', u'orders', u'region'],
                          p.get_table_names('snagatest'))

    def test_get_column_names_001(self):
        p = BigQueryProfiler('bigquery.json')
        self.assertIsNotNone(p)
        self.assertEquals([u'o_orderkey',
                           u'o_custkey',
                           u'o_orderstatus',
                           u'o_totalprice',
                           u'o_orderDATE',
                           u'o_orderpriority',
                           u'o_clerk',
                           u'o_shippriority',
                           u'o_comment'],
                          p.get_column_names('snagatest', 'orders'))

    def test_get_sample_rows_001(self):
        p = BigQueryProfiler('bigquery.json')
        self.assertIsNotNone(p)
        rows = p.get_sample_rows('snagatest', 'orders')
        self.assertEquals(11, len(rows))
        self.assertEquals([u'o_orderkey',
                           u'o_custkey',
                           u'o_orderstatus',
                           u'o_totalprice',
                           u'o_orderDATE',
                           u'o_orderpriority',
                           u'o_clerk',
                           u'o_shippriority',
                           u'o_comment'],
                          rows[0])

    def test_get_column_datatypes_001(self):
        p = BigQueryProfiler('bigquery.json')
        self.assertIsNotNone(p)
        self.assertEquals([u'INTEGER',
                           u'INTEGER',
                           u'STRING',
                           u'FLOAT',
                           u'STRING',
                           u'STRING',
                           u'STRING',
                           u'INTEGER',
                           u'STRING'],
                          p.get_column_datatypes('snagatest', 'orders'))

    def test_get_row_count_001(self):
        p = BigQueryProfiler('bigquery.json')
        self.assertIsNotNone(p)
        self.assertEquals(1500000, p.get_row_count('snagatest', 'orders'))

    def test_get_column_nulls_001(self):
        p = BigQueryProfiler('bigquery.json')
        self.assertIsNotNone(p)
        c = p.get_column_nulls('snagatest', 'orders')

        self.assertEquals(0, c['o_shippriority'])
        self.assertEquals(0, c['o_orderDATE'])
        self.assertEquals(0, c['o_custkey'])
        self.assertEquals(0, c['o_orderstatus'])
        self.assertEquals(0, c['o_comment'])
        self.assertEquals(0, c['o_orderpriority'])
        self.assertEquals(0, c['o_totalprice'])
        self.assertEquals(0, c['o_orderkey'])
        self.assertEquals(0, c['o_clerk'])

    def test_get_column_min_max_001(self):
        p = BigQueryProfiler('bigquery.json')
        self.assertIsNotNone(p)
        c = p.get_column_min_max('snagatest', 'orders')

        self.assertEquals([0, 0], c['o_shippriority'])
        self.assertEquals([u'1992-01-01', u'1998-08-02'], c['o_orderDATE'])
        self.assertEquals([1, 149999], c['o_custkey'])
        self.assertEquals([u'F', u'P'], c['o_orderstatus'])
        self.assertEquals([u'Tiresias about the blithely express accounts haggle furiously ', u'waters x-ray with the furiously pending packages. regular theodolites nag abou'], c['o_comment'])
        self.assertEquals([u'1-URGENT', u'5-LOW'], c['o_orderpriority'])
        self.assertEquals([857.71, 555285.16], c['o_totalprice'])
        self.assertEquals([1, 6000000], c['o_orderkey'])
        self.assertEquals([u'Clerk#000000001', u'Clerk#000001000'], c['o_clerk'])

    def test_get_column_most_freq_values_001(self):
        p = BigQueryProfiler('bigquery.json')
        self.assertIsNotNone(p)
        c = p.get_column_most_freq_values('snagatest', 'orders')
        self.assertEqual([[u'1995-01-13', 702],
                          [u'1998-01-21', 698],
                          [u'1996-05-31', 696],
                          [u'1993-03-19', 695],
                          [u'1994-09-12', 694],
                          [u'1992-10-24', 692],
                          [u'1998-03-21', 692],
                          [u'1998-02-16', 691],
                          [u'1993-01-22', 690],
                          [u'1994-03-15', 690]],
                         c['o_orderDATE'])

    def test_get_column_least_freq_values_001(self):
        p = BigQueryProfiler('bigquery.json')
        self.assertIsNotNone(p)
        c = p.get_column_least_freq_values('snagatest', 'orders')
        self.assertEqual([[u'1996-01-11', 534],
                          [u'1992-08-03', 538],
                          [u'1997-01-03', 539],
                          [u'1996-10-03', 540],
                          [u'1996-12-06', 547],
                          [u'1994-11-02', 550],
                          [u'1992-09-27', 554],
                          [u'1993-06-21', 554],
                          [u'1995-11-14', 557],
                          [u'1993-05-20', 558]],
                         c['o_orderDATE'])

    def test_get_column_cardinalities_001(self):
        p = BigQueryProfiler('bigquery.json')
        self.assertIsNotNone(p)
        self.assertEquals({u'o_shippriority': 1,
                           u'o_orderDATE': 2406,
                           u'o_custkey': 99996,
                           u'o_orderstatus': 3,
                           u'o_comment': 1367237,
                           u'o_orderpriority': 5,
                           u'o_totalprice': 1464556,
                           u'o_orderkey': 1500000,
                           u'o_clerk': 1000},
                          p.get_column_cardinalities('snagatest', 'orders'))

    def test_run_record_validator_001(self):
        p = BigQueryProfiler('bigquery.json')
        self.assertIsNotNone(p)
        project_id = p.dbdriver.project
        r = [(1, project_id,'snagatest','region','r_name','','regexp','^A', ''),
             (2, project_id,'snagatest','region','r_regionkey','','eval','{r_regionkey} > 0', '')]
        c = p.run_record_validation(u'snagatest', u'region', r)
        self.assertEquals([5,2], c['r_name'][0]['statistics'])
        self.assertEquals([5,1], c['r_regionkey'][0]['statistics'])

if __name__ == '__main__':
    unittest.main()
