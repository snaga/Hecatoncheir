#!/usr/bin/env python2.7
# -*- coding: utf-8 -*-

import sys
import unittest
sys.path.append('..')

from hecatoncheir.DbProfilerException import DriverError, InternalError, QueryError, QueryTimeout
from hecatoncheir.mssql import MSSQLDriver

class TestMSSQLDriver(unittest.TestCase):
    dbuser = "dqwbuser"
    dbpass = "dqwbuser"
    dbname = "dqwbtest"

    def setUp(self):
        pass

    def test_MSSQLDriver_001(self):
        db = MSSQLDriver.MSSQLDriver('a', 'b', 'c', 'd')
        self.assertTrue(db is not None)
        self.assertEqual('a', db.host)
        self.assertEqual('b', db.dbname)
        self.assertEqual('c', db.dbuser)
        self.assertEqual('d', db.dbpass)

    def test_connect_001(self):
        # connection success
        db = MSSQLDriver.MSSQLDriver('127.0.0.1', self.dbname, self.dbuser, self.dbpass)
        db.connect()
        self.assertIsNotNone(db.conn)

    def test_connect_002(self):
        # connection failure
        db = MSSQLDriver.MSSQLDriver('127.0.0.1', self.dbname, self.dbuser, 'foo')
        with self.assertRaises(DriverError) as cm:
            db.connect()
        self.assertTrue(cm.exception.value.startswith('Could not connect to the server: '))

    def test_query_to_resultset_001(self):
        driver = MSSQLDriver.MSSQLDriver('127.0.0.1', self.dbname, self.dbuser, self.dbpass)
        try:
            driver.connect()
        except DriverError as e:
            self.fail()
        self.assertIsNotNone(driver.conn)

        # ok
        rs = driver.query_to_resultset(u'select 1 as c')
        self.assertEqual('c', rs.column_names[0])
        self.assertEqual(1, rs.resultset[0][0])

        # exception
        with self.assertRaises(QueryError) as cm:
             driver.query_to_resultset(u'select 1 as c from bar')
        self.assertEqual("Could not execute a query: Invalid object name 'bar'.", cm.exception.value)

        # query timeout (no timeout)
        rs = driver.query_to_resultset(u"WAITFOR DELAY '00:00:02'")
        self.assertEqual("QueryResult:{'column': [], 'query': u\"WAITFOR DELAY '00:00:02'\", 'result': []}",
                         str(rs))

        # query timeout
        # FIXME: Need to be fixed to handle QueryTimeout instead of NotImplementedError
        with self.assertRaises(NotImplementedError) as cm:
            driver.query_to_resultset(u"WAITFOR DELAY '00:00:10'", timeout=1)

        # ok
        rs = driver.query_to_resultset(u'select * from lineitem')
        self.assertEqual(110, len(rs.resultset))

        # exception
        with self.assertRaises(InternalError) as cm:
             driver.query_to_resultset(u'select * from lineitem', max_rows=100)
        self.assertEqual('Exceeded the record limit (100) for QueryResult.', cm.exception.value)

        # q2rs ok
        rs = driver.q2rs(u'select 1 as c')
        self.assertEqual('c', rs.column_names[0])
        self.assertEqual(1, rs.resultset[0][0])

    def test_disconnect_001(self):
        db = MSSQLDriver.MSSQLDriver('127.0.0.1', self.dbname, self.dbuser, self.dbpass)
        conn = db.connect()
        self.assertIsNotNone(conn)
        self.assertTrue(db.disconnect())
        self.assertIsNone(db.conn)
        self.assertFalse(db.disconnect())

if __name__ == '__main__':
    unittest.main()
