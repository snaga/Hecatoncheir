#!/usr/bin/env python2.7
# -*- coding: utf-8 -*-

import sys
import unittest
sys.path.append('..')

from hecatoncheir.DbProfilerException import DriverError, InternalError, QueryError
from hecatoncheir.QueryResult import QueryResult
from hecatoncheir.pgsql import PgDriver

class TestPgDriver(unittest.TestCase):
    dbname = None
    dbuser = None
    dbpass = None

    def setUp(self):
        self.dbname = "dqwbtest"
        self.dbuser = "dqwbuser"
        self.dbpass = "dqwbuser"

    def test_PgDriver_001(self):
        pg = PgDriver.PgDriver('a', 'b', 'c')
        self.assertTrue(pg is not None)
        self.assertEqual('a', pg.connstr)
        self.assertEqual('b', pg.dbuser)
        self.assertEqual('c', pg.dbpass)

    def test_connect_001(self):
        # connection success
        pg = PgDriver.PgDriver('host=/tmp dbname=%s' % self.dbname, self.dbuser, self.dbpass)
        try:
            pg.connect()
        except DriverError as e:
            self.fail()
        self.assertIsNotNone(pg.conn)

    def test_connect_002(self):
        # connection failure
        pg = PgDriver.PgDriver('host=/tmp dbname=%s' % self.dbname, "nosuchuser", '')
        with self.assertRaises(DriverError) as cm:
            pg.connect()
        self.assertEqual('Could not connect to the server: FATAL:  role "nosuchuser" does not exist', cm.exception.value)

    def test_query_to_resultset_001(self):
        pg = PgDriver.PgDriver('host=/tmp dbname=%s' % self.dbname, self.dbuser, self.dbpass)
        try:
            pg.connect()
        except DriverError as e:
            self.fail()
        self.assertIsNotNone(pg.conn)

        # ok
        rs = pg.query_to_resultset(u'select 1 as c')
        self.assertEqual('c', rs.column_names[0])
        self.assertEqual(1, rs.resultset[0][0])

        # exception
        with self.assertRaises(QueryError) as cm:
             pg.query_to_resultset(u'select 1 as c from bar')
        self.assertEqual('Could not execute a query: relation "bar" does not exist', cm.exception.value)

        # ok
        rs = pg.query_to_resultset(u'select generate_series(1,1000)')
        self.assertEqual(1000, len(rs.resultset))

        # exception
        with self.assertRaises(InternalError) as cm:
             pg.query_to_resultset(u'select generate_series(1,1000)', max_rows=100)
        self.assertEqual('Exceeded the record limit (100) for QueryResult.', cm.exception.value)

    def test_disconnect_001(self):
        pg = PgDriver.PgDriver('host=/tmp dbname=%s' % self.dbname, self.dbuser, self.dbpass)
        conn = pg.connect()
        self.assertIsNotNone(conn)
        self.assertTrue(pg.disconnect())
        self.assertIsNone(pg.conn)
        self.assertFalse(pg.disconnect())

if __name__ == '__main__':
    unittest.main()
