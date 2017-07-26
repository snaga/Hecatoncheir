#!/usr/bin/env python2.7
# -*- coding: utf-8 -*-

import sys
import unittest
sys.path.append('..')

from hecatoncheir import DbProfilerRepository
from hecatoncheir import DbProfilerVerify

class TestDbProfilerVerify(unittest.TestCase):
    def setUp(self):
        pass

    def test_verify_column_001(self):
        col = {'validation':
                   [{'label': 'rule1',
                     'invalid_count': 1},
                    {'label': 'rule2',
                     'invalid_count': 1},
                    {'label': 'rule3',
                     'invalid_count': 0}]
               }
        r = DbProfilerVerify.verify_column(col)
        self.assertEqual((1,2), r)

    def test_verify_table_001(self):
        tab = {'columns':
                   [{'validation':
                         [{'label': 'rule1',
                           'invalid_count': 1},
                          {'label': 'rule2',
                           'invalid_count': 1},
                          {'label': 'rule3',
                           'invalid_count': 0}]
                     }]
               }
        r = DbProfilerVerify.verify_table(tab)
        self.assertEqual((1,2), r)

    def test_verify_001(self):
        repo = DbProfilerRepository.DbProfilerRepository('repo.db')
        repo.init()
        repo.open()

        v = DbProfilerVerify.DbProfilerVerify('repo.db', verbose=True)
        self.assertEqual(False, v.verify())
        self.assertEqual(False, v.verify(None))
        self.assertEqual(False, v.verify([['d', 's', 't'],['dd', 'ss', 'tt']]))

if __name__ == '__main__':
    unittest.main()
