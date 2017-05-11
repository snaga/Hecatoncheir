#!/usr/bin/env python2.7
# -*- coding: utf-8 -*-

import sys
import unittest
sys.path.append('..')

from hecatoncheir.DbProfilerException import DriverError, InternalError, QueryError
from hecatoncheir.oracle import OraDriver

class TestOraDriver(unittest.TestCase):
    dbuser = "scott"
    dbpass = "tiger"

    def setUp(self):
        pass

    def test_OraDriver_001(self):
        ora = OraDriver.OraDriver('a','b','c','d','e')
        self.assertTrue(ora is not None)
        self.assertEqual('a', ora.host)
        self.assertEqual('b', ora.port)
        self.assertEqual('c', ora.dbname)
        self.assertEqual('d', ora.dbuser)
        self.assertEqual('e', ora.dbpass)

    def test_connect_001(self):
        ora = OraDriver.OraDriver(None, None, 'orcl', self.dbuser, self.dbpass)
        self.assertTrue(ora.connect())
        self.assertIsNotNone(ora.conn)

    def test_connect_002(self):
        # connection failure
        ora = OraDriver.OraDriver(None, None, 'orcl', self.dbuser, '')

        with self.assertRaises(DriverError) as cm:
            ora.connect()
        self.assertEqual('Could not connect to the server: ORA-01005: null password given; logon denied', cm.exception.value)

    def test_query_to_resultset_001(self):
        ora = OraDriver.OraDriver(None, None, 'orcl', self.dbuser, self.dbpass)
        try:
            ora.connect()
        except DbProfilerException, e:
            self.fail()
        self.assertIsNotNone(ora.conn)

        # ok
        rs = ora.query_to_resultset(u'select 1 as c from dual')
        self.assertEqual('C', rs.column_names[0])
        self.assertEqual(1, rs.resultset[0][0])

        # exception
        with self.assertRaises(QueryError) as cm:
             ora.query_to_resultset(u'select 1 as c from bar')
        self.assertEqual('Could not execute a query: ORA-00942: table or view does not exist', cm.exception.value)

        # ok
        rs = ora.query_to_resultset(u'select * from lineitem')
        self.assertEqual(110, len(rs.resultset))

        # exception
        with self.assertRaises(InternalError) as cm:
             ora.query_to_resultset(u'select * from lineitem', max_rows=100)
        self.assertEqual('Exceeded the record limit (100) for QueryResult.', cm.exception.value)

    def test_disconnect_001(self):
        ora = OraDriver.OraDriver(None, None, 'orcl', self.dbuser, self.dbpass)
        conn = ora.connect()
        self.assertIsNotNone(conn)
        self.assertTrue(ora.disconnect())
        self.assertIsNone(ora.conn)
        self.assertFalse(ora.disconnect())

if __name__ == '__main__':
    unittest.main()
