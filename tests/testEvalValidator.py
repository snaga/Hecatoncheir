#!/usr/bin/env python2.7
# -*- coding: utf-8 -*-

import sys
import unittest
sys.path.append('..')

from hecatoncheir.validator import EvalValidator

class TestEvalValidator(unittest.TestCase):
    def setUp(self):
        pass

    def test_validate_001(self):
        cols = ['COL1','COL2','COL3']

        # Round 1
        v = EvalValidator.EvalValidator('foo',rule=['COL1', u'{COL1} > 100'])

        self.assertEqual('foo', v.label)
        self.assertFalse(v.validate(cols, [100,101,101]))
        self.assertTrue(v.validate(cols, [101,101,101]))

        # Round 2
        v = EvalValidator.EvalValidator('bar', rule=['COL2', u'{COL2} > 100'])

        self.assertEqual('bar', v.label)
        self.assertTrue(v.validate(cols, [100,101,101]))
        self.assertFalse(v.validate(cols, [101,100,101]))

        # Round 3
        v = EvalValidator.EvalValidator('baz', rule=['COL1,COL2', u'{COL1} > {COL2}'])

        self.assertEqual('baz', v.label)
        self.assertFalse(v.validate(cols, [100,100,101]))
        self.assertFalse(v.validate(cols, [100,101,101]))
        self.assertTrue(v.validate(cols, [101,100,101]))

        v = EvalValidator.EvalValidator('c1:eval1', rule=['c1', "{c1} > 100"])
        cols = ['c1']

        self.assertEqual('c1:eval1', v.label)
        # FIXME: Must be false
        self.assertTrue(v.validate(cols, ['abc']))

#    def test_add_rule_eval_001(self):
#        v = DbProfilerValidator.DbProfilerColumnValidator("S1", "T1", "C1")
#        v.add_rule_eval('C1:eval', '{COL1} > 100')
#        self.assertEqual([['C1:eval', '{COL1} > 100']], v.rule_eval_format)
#        v.add_rule_eval('C1:eval2', '{COL1} > 100 and {COL1} < 200')
#        self.assertEqual([['C1:eval', '{COL1} > 100'], ['C1:eval2', '{COL1} > 100 and {COL1} < 200']], v.rule_eval_format)
#
#    def test_eval_rule_eval_001(self):
#        v = DbProfilerValidator.DbProfilerColumnValidator("S1", "T1", "C1")
#        v.add_rule_eval('C1:eval', '{COL1} > 100')
#        kv = {}
#        kv['COL1'] = 101
#        self.assertEqual({'C1:eval': 0}, v.eval_rule_eval(kv))
#        kv['COL1'] = 100
#        self.assertEqual({'C1:eval': 1}, v.eval_rule_eval(kv))
#
#        v.add_rule_eval('C1:eval2', '{COL1} > 100 and {COL1} < 200')
#        self.assertEqual({'C1:eval': 1, 'C1:eval2': 1}, v.eval_rule_eval(kv))

if __name__ == '__main__':
    unittest.main()
