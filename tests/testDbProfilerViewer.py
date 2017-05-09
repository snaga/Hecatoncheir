#!/usr/bin/env python2.7
# -*- coding: utf-8 -*-

import json
import os
import sys
import time
import unittest
sys.path.append('..')

import requests

class TestDbProfilerViewer(unittest.TestCase):
    def setUp(self):
        self.maxDiff = None
        self.headers = {'content-type': 'application/json'}
        for i in range(1,10):
            ep = "http://localhost:8080" + '/api/validation'
            requests.delete(ep + '/%d' % i,
                            headers=self.headers)

    def tearDown(self):
        pass

    def test_api_validation_create_001(self):
        ep = "http://localhost:8080" + '/api/validation'

        payload = {'database_name': 'db',
                   'schema_name': 'schema',
                   'table_name': 'table',
                   'column_name': 'col',
                   'description': 'desc',
                   'rule': 'rule',
                   'param': None,
                   'param2': None}
        r = requests.post(ep,
                          data=json.dumps(payload),
                          headers=self.headers)
        self.assertEqual(201,r.status_code)
        self.assertEqual({'id': 1, 'status': 'success'}, r.json())

        r = requests.post(ep,
                          data=json.dumps(payload),
                          headers=self.headers)
        self.assertEqual(400,r.status_code)
        self.assertEqual({u'message': u'the same rule already exists.', u'status': u'error'}, r.json())

    def test_api_validation_create_002(self):
        ep = "http://localhost:8080" + '/api/validation'

        # empty data
        r = requests.post(ep,
                          headers=self.headers)
        self.assertEqual(400,r.status_code)
        self.assertEqual({u'message': u'the request data is empty.', u'status': u'error'}, r.json())

        # incorrect data
        r = requests.post(ep,
                          data='data',
                          headers=self.headers)
        self.assertEqual(400,r.status_code)
        self.assertEqual({u'message': u'incorrect data format.', u'status': u'error'}, r.json())

    def test_api_validation_get_001(self):
        ep = "http://localhost:8080" + '/api/validation'

        payload = {'database_name': 'db',
                   'schema_name': 'schema',
                   'table_name': 'table',
                   'column_name': 'col',
                   'description': 'desc',
                   'rule': 'rule',
                   'param': None,
                   'param2': None}
        r = requests.post(ep,
                          data=json.dumps(payload),
                          headers=self.headers)
        self.assertEqual(201,r.status_code)
        self.assertEqual({'id': 1, 'status': 'success'}, r.json())

        r = requests.get(ep + "/1",
                         headers=self.headers)
        self.assertEqual(201,r.status_code)
        self.assertEqual({'id': 1,
                          'status': 'success',
                          'database_name': 'db',
                          'schema_name': 'schema',
                          'table_name': 'table',
                          'column_name': 'col',
                          'description': 'desc',
                          'rule': 'rule',
                          'param': '',
                          'param2': ''}, r.json())

        r = requests.get(ep + "/2",
                         headers=self.headers)
        self.assertEqual(400, r.status_code)
        self.assertEqual({'message': 'rule id 2 not found.', 'status': 'error'}, r.json())

    def test_api_validation_get_002(self):
        ep = "http://localhost:8080" + '/api/validation'

        payload = {'database_name': 'db',
                   'schema_name': 'schema',
                   'table_name': 'table',
                   'column_name': 'col',
                   'description': 'desc',
                   'rule': 'rule',
                   'param': None,
                   'param2': None}
        r = requests.post(ep,
                          data=json.dumps(payload),
                          headers=self.headers)
        self.assertEqual(201,r.status_code)
        self.assertEqual({'id': 1, 'status': 'success'}, r.json())

        payload = {'database_name': 'db2',
                   'schema_name': 'schema2',
                   'table_name': 'table2',
                   'column_name': 'col2',
                   'description': 'desc2',
                   'rule': 'rule2',
                   'param': 'param2',
                   'param2': 'param22'}
        r = requests.post(ep,
                          data=json.dumps(payload),
                          headers=self.headers)
        self.assertEqual(201,r.status_code)
        self.assertEqual({'id': 2, 'status': 'success'}, r.json())

        r = requests.get(ep,
                         headers=self.headers)
        self.assertEqual(201,r.status_code)
        self.assertEqual({'status': 'success',
                          'data': [{'id': 1,
                                    'database_name': 'db',
                                    'schema_name': 'schema',
                                    'table_name': 'table',
                                    'column_name': 'col',
                                    'description': 'desc',
                                    'rule': 'rule',
                                    'param': '',
                                    'param2': ''},
                                   {'id': 2,
                                    'database_name': 'db2',
                                    'schema_name': 'schema2',
                                    'table_name': 'table2',
                                    'column_name': 'col2',
                                    'description': 'desc2',
                                    'rule': 'rule2',
                                    'param': 'param2',
                                    'param2': 'param22'}]}, r.json())

    def test_api_validation_put_001(self):
        ep = "http://localhost:8080" + '/api/validation'

        payload = {'database_name': 'db',
                   'schema_name': 'schema',
                   'table_name': 'table',
                   'column_name': 'col',
                   'description': 'desc',
                   'rule': 'rule',
                   'param': None,
                   'param2': None}
        r = requests.post(ep,
                          data=json.dumps(payload),
                          headers=self.headers)
        self.assertEqual(201,r.status_code)
        self.assertEqual({'id': 1, 'status': 'success'}, r.json())

        r = requests.get(ep + "/1",
                         headers=self.headers)
        self.assertEqual(201,r.status_code)
        self.assertEqual({'id': 1,
                          'status': 'success',
                          'database_name': 'db',
                          'schema_name': 'schema',
                          'table_name': 'table',
                          'column_name': 'col',
                          'description': 'desc',
                          'rule': 'rule',
                          'param': '',
                          'param2': ''}, r.json())

        # update
        payload = {'database_name': 'db2',
                   'schema_name': 'schema2',
                   'table_name': 'table2',
                   'column_name': 'col2',
                   'description': 'desc2',
                   'rule': 'rule2',
                   'param': 'param2',
                   'param2': 'param22'}
        r = requests.put(ep + "/1",
                         data=json.dumps(payload),
                         headers=self.headers)
        self.assertEqual(201,r.status_code)
        self.assertEqual({'id': 1, 'status': 'success'}, r.json())

        r = requests.get(ep + "/1",
                         headers=self.headers)
        self.assertEqual(201,r.status_code)
        self.assertEqual({'id': 1,
                          'status': 'success',
                          'database_name': 'db2',
                          'schema_name': 'schema2',
                          'table_name': 'table2',
                          'column_name': 'col2',
                          'description': 'desc2',
                          'rule': 'rule2',
                          'param': 'param2',
                          'param2': 'param22'}, r.json())

    def test_api_validation_delete_001(self):
        ep = "http://localhost:8080" + '/api/validation'

        payload = {'database_name': 'db',
                   'schema_name': 'schema',
                   'table_name': 'table',
                   'column_name': 'col',
                   'description': 'desc',
                   'rule': 'rule',
                   'param': None,
                   'param2': None}
        r = requests.post(ep,
                          data=json.dumps(payload),
                          headers=self.headers)
        self.assertEqual(201,r.status_code)
        self.assertEqual({'id': 1, 'status': 'success'}, r.json())

        # first delete
        r = requests.delete(ep + '/1',
                            headers=self.headers)
        self.assertEqual(201,r.status_code)
        self.assertEqual({'id': 1, 'status': 'success'}, r.json())

        # second delete
        r = requests.delete(ep + '/1',
                            headers=self.headers)
        self.assertEqual(400,r.status_code)

if __name__ == '__main__':
    unittest.main()
