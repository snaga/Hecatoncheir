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

#    def test_add_rule_regexp_001(self):
#        v = DbProfilerValidator.DbProfilerColumnValidator("S1", "T1", "C1")
#        v.add_rule_regexp('C1:regexp', '^$')
#        self.assertEqual([['C1:regexp', '^$']], v.rule_regexp_pattern)
#        v.add_rule_regexp('C1:regexp2', '^\.$')
#        self.assertEqual([['C1:regexp', '^$'], ['C1:regexp2', '^\.$']], v.rule_regexp_pattern)

#    def test_eval_rule_regexp_001(self):
#        v = DbProfilerValidator.DbProfilerColumnValidator("S1", "T1", "C1")
#        v.add_rule_regexp('C1:regexp1', '^$')
#        self.assertEqual({'C1:regexp1': 0}, v.eval_rule_regexp(''))
#        self.assertEqual({'C1:regexp1': 1}, v.eval_rule_regexp(' '))
#
#        v.add_rule_regexp('C1:regexp2', '^\.$')
#        self.assertEqual({'C1:regexp1': 1, 'C1:regexp2': 1}, v.eval_rule_regexp(' '))

if __name__ == '__main__':
    unittest.main()
