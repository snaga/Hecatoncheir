#!/usr/bin/env python2.7
# -*- coding: utf-8 -*-

import sys
import unittest
sys.path.append('..')

from hecatoncheir.validator import RegexpValidator

class TestRegexpValidator(unittest.TestCase):
    def setUp(self):
        pass

    def test_validate_001(self):
        v = RegexpValidator.RegexpValidator('foo', rule=['COL1', '^aaa'])
        cols = ['COL1','COL2','COL3']

        self.assertEqual('foo', v.label)
        self.assertTrue(v.validate(cols, ['aaa','bbb','ccc']))
        self.assertFalse(v.validate(cols, [' aa','bbb','ccc']))

        # Round 2
        v = RegexpValidator.RegexpValidator('bar', rule=['COL2', '^aaa'])

        self.assertEqual('bar', v.label)
        self.assertFalse(v.validate(cols, ['aaa','bbb','ccc']))
        self.assertTrue(v.validate(cols, ['aaa','aaa','ccc']))

    def test_validate_002(self):
        v = RegexpValidator.RegexpValidator('foo', rule=['c_custkey', '^\\d+$'])
        cols = ['c_custkey','c_custname','c_custaddress']

        self.assertEqual('foo', v.label)
        self.assertTrue(v.validate(cols, ['123','bbb','ccc']))
        self.assertFalse(v.validate(cols, ['12a','bbb','ccc']))

if __name__ == '__main__':
    unittest.main()
