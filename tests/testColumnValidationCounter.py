#!/usr/bin/env python2.7
# -*- coding: utf-8 -*-

import sys
import unittest
sys.path.append('..')

from hecatoncheir import ColumnValidationCounter
from hecatoncheir.exception import InternalError

class TestColumnValidationCounter(unittest.TestCase):
    def setUp(self):
        pass

    def test_ColumnValidationCounter_001(self):
        c = ColumnValidationCounter.ColumnValidationCounter()

    def test_add_001(self):
        c = ColumnValidationCounter.ColumnValidationCounter()

        # single column
        c.add('foo', 'bar')
        self.assertEqual(0, c._column_counter['foo']['bar'])

        # multi column
        c.add('foo,foo2', 'bar2')
        self.assertEqual(0, c._column_counter['foo']['bar2'])
        self.assertEqual(0, c._column_counter['foo2']['bar2'])

    def test_add_001(self):
        c = ColumnValidationCounter.ColumnValidationCounter()

        # single column
        c.add('foo', 'bar')
        self.assertEqual(0, c._column_counter['foo']['bar'])
        c.incr('foo', 'bar')
        self.assertEqual(1, c._column_counter['foo']['bar'])

        # multi column
        c.add('foo, foo2', 'bar2')
        self.assertEqual(0, c._column_counter['foo']['bar2'])
        self.assertEqual(0, c._column_counter['foo2']['bar2'])
        c.incr('foo, foo2', 'bar2')
        self.assertEqual(1, c._column_counter['foo']['bar2'])
        self.assertEqual(1, c._column_counter['foo2']['bar2'])

        # key error
        with self.assertRaises(InternalError) as cm:
            c.incr('foo3', 'bar')
        self.assertEqual('ColumnValidationCounter#incr() key error: foo3',
                         cm.exception.value)

        with self.assertRaises(InternalError) as cm:
            c.incr('foo', 'bar3')
        self.assertEqual('ColumnValidationCounter#incr() key error: foo, bar3',
                         cm.exception.value)

    def test_get_001(self):
        c = ColumnValidationCounter.ColumnValidationCounter()

        # single column
        c.add('foo', 'bar')
        c.incr('foo', 'bar')
        self.assertEqual(1, c.get('foo','bar'))
        c.add('foo', 'bar2')
        self.assertEqual(0, c.get('foo','bar2'))

        self.assertEqual({'bar2': 0, 'bar': 1}, c.get('foo'))

    def test_get_002(self):
        c = ColumnValidationCounter.ColumnValidationCounter()

        # single column
        c.add('foo', 'bar')
        self.assertEqual(0, c.get('foo','bar'))
        c.incr('foo', 'bar')
        self.assertEqual(1, c.get('foo','bar'))

        # multi column
        c.add('foo, foo2', 'bar2')
        self.assertEqual(0, c.get('foo','bar2'))
        self.assertEqual(0, c.get('foo2','bar2'))
        c.incr('foo, foo2', 'bar2')
        self.assertEqual(1, c.get('foo','bar2'))
        self.assertEqual(1, c.get('foo2','bar2'))

        # key error
        with self.assertRaises(InternalError) as cm:
            c.get('foo3', 'bar')
        self.assertEqual('ColumnValidationCounter#get() key error: foo3',
                         cm.exception.value)

        with self.assertRaises(InternalError) as cm:
            c.get('foo', 'bar3')
        self.assertEqual('ColumnValidationCounter#get() key error: foo, bar3',
                         cm.exception.value)

if __name__ == '__main__':
    unittest.main()
