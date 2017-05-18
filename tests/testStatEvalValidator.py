#!/usr/bin/env python2.7
# -*- coding: utf-8 -*-

import sys
import unittest
sys.path.append('..')

from hecatoncheir.exception import ValidationError
from hecatoncheir.validator import StatEvalValidator

class TestStatEvalValidator(unittest.TestCase):
    def setUp(self):
        pass

    def test_validate_001(self):
        v = StatEvalValidator.StatEvalValidator('foo', rule=[u'COL1', '{min} > 100'])
        s = {'columns': [{
                    'column_name': 'COL1',
                    'nulls': 0,
                    'min': 100,
                    'max': 1000,
                    'cardinality': 10}],
             'row_count': 10}
        self.assertFalse(v.validate(s))

        v = StatEvalValidator.StatEvalValidator('foo', rule=[u'COL1', '{min} > 100'])
        s = {'columns': [{
                    'column_name': 'COL1',
                    'nulls': 0,
                    'min': 101,
                    'max': 1000,
                    'cardinality': 10}],
             'row_count': 10}
        self.assertTrue(v.validate(s))

    def test_validate_002(self):
        v = StatEvalValidator.StatEvalValidator('foo', rule=[u'COL1', '{max} > 1000'])
        s = {'columns': [{
                    'column_name': 'COL1',
                    'nulls': 0,
                    'min': 100,
                    'max': 1000,
                    'cardinality': 10}],
             'row_count': 10}
        self.assertFalse(v.validate(s))

        v = StatEvalValidator.StatEvalValidator('foo', rule=[u'COL1', '{max} > 1000'])
        s = {'columns': [{
                    'column_name': 'COL1',
                    'nulls': 0,
                    'min': 100,
                    'max': 1001,
                    'cardinality': 10}],
             'row_count': 10}
        self.assertTrue(v.validate(s))

    def test_validate_003(self):
        v = StatEvalValidator.StatEvalValidator('foo', rule=[u'COL1', '{cardinality} > 10'])
        s = {'columns': [{
                    'column_name': 'COL1',
                    'nulls': 0,
                    'min': 100,
                    'max': 1001,
                    'cardinality': 10}],
             'row_count': 10}
        self.assertFalse(v.validate(s))

        v = StatEvalValidator.StatEvalValidator('foo', rule=[u'COL1', '{cardinality} > 10'])
        s = {'columns': [{
                    'column_name': 'COL1',
                    'nulls': 0,
                    'min': 100,
                    'max': 1001,
                    'cardinality': 11}],
             'row_count': 11}
        self.assertTrue(v.validate(s))

    def test_validate_004(self):
        v = StatEvalValidator.StatEvalValidator('foo', rule=[u'COL2', '{cardinality} > 10'])
        s = {'columns': [{
                    'column_name': 'COL1',
                    'nulls': 0,
                    'min': 100,
                    'max': 1001,
                    'cardinality': 10}],
             'row_count': 10}
        with self.assertRaises(ValidationError) as cm:
            v.validate(s)
        self.assertEqual("Column `COL2' not found. Check your validation rule again.", cm.exception.value)


if __name__ == '__main__':
    unittest.main()
