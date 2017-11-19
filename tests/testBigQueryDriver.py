#!/usr/bin/env python2.7
# -*- coding: utf-8 -*-

import os
import sys
import unittest
sys.path.append('..')

from hecatoncheir.QueryResult import QueryResult
from hecatoncheir.exception import DriverError, InternalError, QueryError, QueryTimeout
from hecatoncheir.bigquery import BigQueryDriver

class TestBigQueryDriver(unittest.TestCase):
    dbname = None
    dbuser = None
    dbpass = None

    def setUp(self):
        self.dbname = os.environ.get('BQ_PROJECT', '')
        self.dbuser = "dqwbuser"
        self.dbpass = "dqwbuser"

    def test_BigQueryDriver_001(self):
        bq = BigQueryDriver.BigQueryDriver('a', 'b', 'c')
        self.assertTrue(bq is not None)
        self.assertEqual('a', bq.project)
        self.assertEqual('b', bq.dbuser)
        self.assertEqual('c', bq.dbpass)

    def test_connect_001(self):
        # connection success
        bq = BigQueryDriver.BigQueryDriver(self.dbname, self.dbuser, self.dbpass)
        try:
            bq.connect()
        except DriverError as e:
            self.fail()
        self.assertIsNotNone(bq.conn)

    def test_connect_002(self):
        # connection failure
        # FIXME:
        bq = BigQueryDriver.BigQueryDriver(self.dbname, "nosuchuser", '')
        with self.assertRaises(DriverError) as cm:
            bq.connect()
        self.assertEqual('', cm.exception.value)

    def test_query_to_resultset_001(self):
        bq = BigQueryDriver.BigQueryDriver(self.dbname, self.dbuser, self.dbpass)
        try:
            bq.connect()
        except DriverError as e:
            self.fail()
        self.assertIsNotNone(bq.conn)

        # ok
        rs = bq.query_to_resultset(u'select 1 as c')
        self.assertEqual('c', rs.column_names[0])
        self.assertEqual(1, rs.resultset[0][0])

        # exception
        with self.assertRaises(QueryError) as cm:
             bq.query_to_resultset(u'select 1 as c from bar')
        self.assertEqual('Could not execute a query: 400 Table name "bar" cannot be resolved: dataset name is missing.', cm.exception.value)

        # query timeout (no timeout)
        rs = bq.query_to_resultset(u'select l_orderkey from snagatest.lineitem order by l_orderkey limit 1')
        self.assertEqual("QueryResult:{'column': (u'l_orderkey',), 'query': u'select l_orderkey from snagatest.lineitem order by l_orderkey limit 1', 'result': [[1]]}",
                         str(rs))

        # query timeout
        # FIXME:
        with self.assertRaises(QueryTimeout) as cm:
             bq.query_to_resultset(u'select * from snagatest.lineitem order by l_shipdate desc limit 1', timeout=1)
        self.assertEqual('Query timeout: select * from snagatest.lineitem order by l_shipdate desc limit 1', cm.exception.value)

        # ok
        rs = bq.query_to_resultset(u'select * from snagatest.region order by r_regionkey')
        self.assertEqual(5, len(rs.resultset))

        # exception
        with self.assertRaises(InternalError) as cm:
            bq.query_to_resultset(u'select * from snagatest.region order by r_regionkey', max_rows=4)
        self.assertEqual('Exceeded the record limit (4) for QueryResult.', cm.exception.value)

        # q2rs ok
        rs = bq.q2rs(u'select 1 as c')
        self.assertEqual('c', rs.column_names[0])
        self.assertEqual(1, rs.resultset[0][0])

    def test_disconnect_001(self):
        bq = BigQueryDriver.BigQueryDriver(self.dbname, self.dbuser, self.dbpass)
        conn = bq.connect()
        self.assertIsNotNone(conn)
        self.assertTrue(bq.disconnect())
        self.assertIsNone(bq.conn)
        self.assertFalse(bq.disconnect())

if __name__ == '__main__':
    unittest.main()
