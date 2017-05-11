#!/usr/bin/env python2.7
# -*- coding: utf-8 -*-

import sys
import unittest
sys.path.append('..')

from hecatoncheir.DbProfilerException import DriverError, InternalError, QueryError, QueryTimeout
from hecatoncheir.mysql import MyDriver

class TestMyDriver(unittest.TestCase):
    dbuser = "root"
    dbpass = "mysql123"
    dbname = "dqwbtest"

    def setUp(self):
        pass

    def test_MyDriver_001(self):
        my = MyDriver.MyDriver('a','b','c','d','e')
        self.assertTrue(my is not None)
        self.assertEqual('a', my.host)
        self.assertEqual('b', my.port)
        self.assertEqual('c', my.dbname)
        self.assertEqual('d', my.dbuser)
        self.assertEqual('e', my.dbpass)

    def test_connect_001(self):
        # connection success
        my = MyDriver.MyDriver('127.0.0.1', 3306, self.dbname, self.dbuser, self.dbpass)
        self.assertTrue(my.connect())
        self.assertIsNotNone(my.conn)

    def test_connect_002(self):
        # connection failure
        my = MyDriver.MyDriver('127.0.0.1', 3306, self.dbname, self.dbuser, '')
        with self.assertRaises(DriverError) as cm:
            my.connect()
        self.assertEqual("Could not connect to the server: Access denied for user 'root'@'localhost' (using password: NO)", cm.exception.value)

    def test_query_to_resultset_001(self):
        my = MyDriver.MyDriver('127.0.0.1', 3306, self.dbname, self.dbuser, self.dbpass)
        try:
            my.connect()
        except DbProfilerException, e:
            self.fail()
        self.assertIsNotNone(my.conn)

        # ok
        rs = my.query_to_resultset(u'select 1 as c from dual')
        self.assertEqual('c', rs.column_names[0])
        self.assertEqual(1, rs.resultset[0][0])

        # exception
        with self.assertRaises(QueryError) as cm:
             my.query_to_resultset(u'select 1 as c from bar')
        self.assertEqual("Could not execute a query: Table 'dqwbtest.bar' doesn't exist", cm.exception.value)

        # query timeout (no timeout)
        rs = my.query_to_resultset(u'select sleep(2)')
        self.assertEqual("QueryResult:{'column': ('sleep(2)',), 'query': u'select sleep(2)', 'result': [[0L]]}",
                         str(rs))

        # query timeout
        with self.assertRaises(QueryTimeout) as cm:
            my.query_to_resultset(u'select sleep(2)', timeout=1)
        self.assertEqual('Query timeout: select sleep(2)', cm.exception.value)

        # ok
        rs = my.query_to_resultset(u'select * from lineitem')
        self.assertEqual(110, len(rs.resultset))

        # exception
        with self.assertRaises(InternalError) as cm:
             my.query_to_resultset(u'select * from lineitem', max_rows=100)
        self.assertEqual("Exceeded the record limit (100) for QueryResult.", cm.exception.value)

    def test_disconnect_001(self):
        my = MyDriver.MyDriver('127.0.0.1', 3306, self.dbname, self.dbuser, self.dbpass)
        conn = my.connect()
        self.assertIsNotNone(conn)
        self.assertTrue(my.disconnect())
        self.assertIsNone(my.conn)
        self.assertFalse(my.disconnect())

if __name__ == '__main__':
    unittest.main()
