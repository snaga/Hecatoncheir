#!/usr/bin/env python2.7
# -*- coding: utf-8 -*-

import json
import os
import sys
import time
import unittest
sys.path.append('..')

from flask import Flask

from hecatoncheir import webapi
from hecatoncheir import DbProfilerRepository

class WebAPITestCase(unittest.TestCase):
    def setUpEmptyOne(self, obj):
        res = self.cl.put(obj, data=json.dumps({}))
        self.assertEqual(200, res.status_code)
        self.assertFound(obj)

    def assertFound(self, uri):
        res = self.cl.get(uri)
        self.assertEqual(200, res.status_code)

    def assertNotFound(self, uri):
        res = self.cl.get(uri)
        self.assertEqual(404, res.status_code)

    def assertGetWasSuccess(self, uri):
        res = self.cl.get(uri)
        self.assertEqual(200, res.status_code)
        return json.loads(res.data)

class TestTableItem(WebAPITestCase):
    def setUp(self):
        os.environ["DBPROF_REPOFILE"] = './repo.db'
        try:
            os.unlink(os.environ["DBPROF_REPOFILE"])
        except Exception as ex:
            pass

        repo = DbProfilerRepository.DbProfilerRepository(os.environ["DBPROF_REPOFILE"])
        repo.init()

        self.app = Flask(__name__)
        apihelper = webapi.ApiHelper(self.app)
        self.cl = self.app.test_client()

    def test_put_001(self):
        self.assertNotFound('/api/v1/table/dqwbtest.public.customer')

        # create one
        d = {}
        res = self.cl.put('/api/v1/table/dqwbtest.public.customer',
                          data=json.dumps(d))
        self.assertEqual(200, res.status_code)

        self.assertFound('/api/v1/table/dqwbtest.public.customer')

        # already exists.
        res = self.cl.put('/api/v1/table/dqwbtest.public.customer',
                          data=json.dumps(d))
        self.assertEqual(500, res.status_code)

    def test_get_001(self):
        self.setUpEmptyOne('/api/v1/table/dqwbtest.public.customer')

        res = self.cl.get('/api/v1/table/dqwbtest.public.customer')
        self.assertEqual(200, res.status_code)
        a = {"timestamp": None,
             "database_name": "dqwbtest",
             "table_name": "customer",
             "schema_name": "public"}
        b = json.loads(res.data)
        b['timestamp'] = None
        self.assertEqual(a, b)

    def test_post_001(self):
        self.setUpEmptyOne('/api/v1/table/dqwbtest.public.customer')

        # update
        d = {"table_name_nls": "bar",
             "comment": "hogefuga",
             "owner": "who",
             "tags": "tag1,     tag 2"}
        res = self.cl.post('/api/v1/table/dqwbtest.public.customer',
                           data=json.dumps(d))
        self.assertEqual(200, res.status_code)

        a = {"timestamp": None,
             "database_name": "dqwbtest",
             "table_name": "customer",
             "schema_name": "public",
             "table_name_nls": "bar",
             "comment": "hogefuga",
             "owner": "who",
             "tags": ["tag1", "tag 2"]}
        b = self.assertGetWasSuccess('/api/v1/table/dqwbtest.public.customer')
        b['timestamp'] = None
        self.assertEqual(a, b)

        # 2nd update
        d = {"table_name_nls": "bar",
             "comment": "hogehoge",
             "owner": "who",
             "tags": "tag1,     tag 2"}
        res = self.cl.post('/api/v1/table/dqwbtest.public.customer',
                           data=json.dumps(d))
        self.assertEqual(200, res.status_code)
        print(res.data)

        a = {"timestamp": None,
             "database_name": "dqwbtest",
             "table_name": "customer",
             "schema_name": "public",
             "table_name_nls": "bar",
             "comment": "hogehoge",
             "owner": "who",
             "tags": ["tag1", "tag 2"]}
        b = self.assertGetWasSuccess('/api/v1/table/dqwbtest.public.customer')
        b['timestamp'] = None
        self.assertEqual(a, b)

    def test_delete_001(self):
        self.setUpEmptyOne('/api/v1/table/dqwbtest.public.customer')
        self.assertFound('/api/v1/table/dqwbtest.public.customer')

        # delete
        res = self.cl.delete('/api/v1/table/dqwbtest.public.customer')
        self.assertEqual(200, res.status_code)

        self.assertNotFound('/api/v1/table/dqwbtest.public.customer')

        # 2nd delete
        res = self.cl.delete('/api/v1/table/dqwbtest.public.customer')
        self.assertEqual(404, res.status_code)

class TestColumnItem(WebAPITestCase):
    def setUp(self):
        os.environ["DBPROF_REPOFILE"] = './repo.db'
        try:
            os.unlink(os.environ["DBPROF_REPOFILE"])
        except Exception as ex:
            pass

        repo = DbProfilerRepository.DbProfilerRepository(os.environ["DBPROF_REPOFILE"])
        repo.init()

        self.app = Flask(__name__)
        apihelper = webapi.ApiHelper(self.app)
        self.cl = self.app.test_client()

    def test_put_001(self):
        self.setUpEmptyOne('/api/v1/table/dqwbtest.public.customer')
        self.assertNotFound('/api/v1/column/dqwbtest.public.customer.c_custkey')

        # create one
        d = {"column_name_nls": "Column Name"}
        res = self.cl.put('/api/v1/column/dqwbtest.public.customer.c_custkey',
                          data=json.dumps(d))
        self.assertEqual(200, res.status_code)

        self.assertFound('/api/v1/column/dqwbtest.public.customer.c_custkey')

        # already exists.
        d = {"column_name_nls": "Column Name"}
        res = self.cl.put('/api/v1/column/dqwbtest.public.customer.c_custkey',
                          data=json.dumps(d))
        self.assertEqual(500, res.status_code)

        a = {u'timestamp': None,
             u'database_name': u'dqwbtest',
             u'schema_name': u'public',
             u'table_name': u'customer',
             u'columns': [{u'column_name': u'c_custkey',
                           u'column_name_nls': u'Column Name'}]}
        b = self.assertGetWasSuccess('/api/v1/table/dqwbtest.public.customer')
        b['timestamp'] = None
        self.assertEqual(a, b)

    def test_get_001(self):
        self.setUpEmptyOne('/api/v1/table/dqwbtest.public.customer')
        self.setUpEmptyOne('/api/v1/column/dqwbtest.public.customer.c_custkey')

        # get
        res = self.cl.get('/api/v1/column/dqwbtest.public.customer.c_custkey')
        self.assertEqual(200, res.status_code)
        a = {"column_name": "c_custkey"}
        b = json.loads(res.data)
        self.assertEqual(a, b)

    def test_post_001(self):
        self.setUpEmptyOne('/api/v1/table/dqwbtest.public.customer')
        self.setUpEmptyOne('/api/v1/column/dqwbtest.public.customer.c_custkey')

        # update
        d = {"comment": "foo"}
        res = self.cl.post('/api/v1/column/dqwbtest.public.customer.c_custkey',
                           data=json.dumps(d))
        self.assertEqual(200, res.status_code)

        a = {u'comment': u'foo', u'column_name': u'c_custkey'}
        b = self.assertGetWasSuccess('/api/v1/column/dqwbtest.public.customer.c_custkey')
        self.assertEqual(a, b)

        # update again
        d = {"comment": "bar"}
        res = self.cl.post('/api/v1/column/dqwbtest.public.customer.c_custkey',
                           data=json.dumps(d))
        self.assertEqual(200, res.status_code)

        a = {u'comment': u'bar', u'column_name': u'c_custkey'}
        b = self.assertGetWasSuccess('/api/v1/column/dqwbtest.public.customer.c_custkey')
        self.assertEqual(a, b)

    def test_delete_001(self):
        self.setUpEmptyOne('/api/v1/table/dqwbtest.public.customer')
        self.setUpEmptyOne('/api/v1/column/dqwbtest.public.customer.c_custkey')

        self.assertFound('/api/v1/table/dqwbtest.public.customer')
        self.assertFound('/api/v1/column/dqwbtest.public.customer.c_custkey')

        # delete
        res = self.cl.delete('/api/v1/column/dqwbtest.public.customer.c_custkey')
        self.assertEqual(200, res.status_code)

        self.assertNotFound('/api/v1/column/dqwbtest.public.customer.c_custkey')

        # 2nd delete
        res = self.cl.delete('/api/v1/column/dqwbtest.public.customer.c_custkey')
        self.assertEqual(404, res.status_code)


class TestTagDescriptionItem(WebAPITestCase):
    def setUp(self):
        os.environ["DBPROF_REPOFILE"] = './repo.db'
        try:
            os.unlink(os.environ["DBPROF_REPOFILE"])
        except Exception as ex:
            pass

        repo = DbProfilerRepository.DbProfilerRepository(os.environ["DBPROF_REPOFILE"])
        repo.init()

        self.app = Flask(__name__)
        apihelper = webapi.ApiHelper(self.app)
        self.cl = self.app.test_client()

    def setUpEmptyTag(self, uri):
        d = {"desc": "", "comment": ""}
        res = self.cl.put(uri,
                          data=json.dumps(d))
        self.assertEqual(200, res.status_code)

    def test_put_001(self):
        self.assertNotFound('/api/v1/tag/mytag')

        # create one
        d = {"desc": "descdesc", "comment": "commentcomment"}
        res = self.cl.put('/api/v1/tag/mytag',
                          data=json.dumps(d))
        self.assertEqual(200, res.status_code)

        self.assertFound('/api/v1/tag/mytag')

        # already exists.
        d = {"desc": "descdesc", "comment": "commentcomment"}
        res = self.cl.put('/api/v1/tag/mytag',
                          data=json.dumps(d))
        self.assertEqual(500, res.status_code)

        a = {u'comment': u'commentcomment',
             u'desc': u'descdesc',
             u'label': u'mytag'}
        b = self.assertGetWasSuccess('/api/v1/tag/mytag')
        self.assertEqual(a, b)

    def test_get_001(self):
        self.setUpEmptyTag('/api/v1/tag/mytag')

        # get
        res = self.cl.get('/api/v1/tag/mytag')
        self.assertEqual(200, res.status_code)

        a = {u'comment': u'',
             u'desc': u'',
             u'label': u'mytag'}
        b = json.loads(res.data)

        self.assertEqual(a, b)

    def test_post_001(self):
        self.setUpEmptyTag('/api/v1/tag/mytag')

        # update
        d = {"comment": "comment1",
             "desc": "desc1"}
        res = self.cl.post('/api/v1/tag/mytag',
                           data=json.dumps(d))
        self.assertEqual(200, res.status_code)

        a = {u'label': u'mytag',
             "comment": "comment1",
             "desc": "desc1"}
        b = self.assertGetWasSuccess('/api/v1/tag/mytag')
        self.assertEqual(a, b)

        # update again
        d = {"comment": "comment2",
             "desc": "desc2"}
        res = self.cl.post('/api/v1/tag/mytag',
                           data=json.dumps(d))
        self.assertEqual(200, res.status_code)

        a = {u'label': u'mytag',
             "comment": "comment2",
             "desc": "desc2"}
        b = self.assertGetWasSuccess('/api/v1/tag/mytag')
        self.assertEqual(a, b)

    def test_delete_001(self):
        self.setUpEmptyTag('/api/v1/tag/mytag')

        self.assertFound('/api/v1/tag/mytag')

        # delete
        res = self.cl.delete('/api/v1/tag/mytag')
        self.assertEqual(200, res.status_code)

        self.assertNotFound('/api/v1/tag/mytag')

        # 2nd delete
        res = self.cl.delete('/api/v1/tag/mytag')
        self.assertEqual(404, res.status_code)

class TestValidationItem(WebAPITestCase):
    def setUp(self):
        self.maxDiff = None

        os.environ["DBPROF_REPOFILE"] = './repo.db'
        try:
            os.unlink(os.environ["DBPROF_REPOFILE"])
        except Exception as ex:
            pass

        repo = DbProfilerRepository.DbProfilerRepository(os.environ["DBPROF_REPOFILE"])
        repo.init()

        self.app = Flask(__name__)
        apihelper = webapi.ApiHelper(self.app)
        self.cl = self.app.test_client()

    def test_put_001(self):
        self.assertNotFound('/api/v1/validation/1')
        self.assertNotFound('/api/v1/validation/2')

        # create first one
        d = {"id": "0",
             "database_name": "DBNAME",
             "schema_name": "SCHEMA",
             "table_name": "TABLE",
             "column_name": "COLUMN",
             "description": "DESCRIPTION",
             "rule": "columnstat",
             "param": "{max} == 10",
             "param2": ""}
        res = self.cl.put('/api/v1/validation/0',
                          data=json.dumps(d))
        self.assertEqual(200, res.status_code)

        self.assertFound('/api/v1/validation/1')
        self.assertNotFound('/api/v1/validation/2')

        # create second one
        d = {"id": "0",
             "database_name": "DBNAME",
             "schema_name": "SCHEMA",
             "table_name": "TABLE",
             "column_name": "COLUMN",
             "description": "DESCRIPTION",
             "rule": "columnstat",
             "param": "{max} == 100",
             "param2": ""}
        res = self.cl.put('/api/v1/validation/0',
                          data=json.dumps(d))
        self.assertEqual(200, res.status_code)

        self.assertFound('/api/v1/validation/1')
        self.assertFound('/api/v1/validation/2')

    def test_get_001(self):
        self.assertNotFound('/api/v1/validation/1')

        # create first one
        d = {"id": "0",
             "database_name": "DBNAME",
             "schema_name": "SCHEMA",
             "table_name": "TABLE",
             "column_name": "COLUMN",
             "description": "DESCRIPTION",
             "rule": "columnstat",
             "param": "{max} == 10",
             "param2": ""}
        res = self.cl.put('/api/v1/validation/0',
                          data=json.dumps(d))
        self.assertEqual(200, res.status_code)

        self.assertFound('/api/v1/validation/1')

        res = self.cl.get('/api/v1/validation/1')

        a = {"id": "1",
             "database_name": "DBNAME",
             "schema_name": "SCHEMA",
             "table_name": "TABLE",
             "column_name": "COLUMN",
             "description": "DESCRIPTION",
             "rule": "columnstat",
             "param": "{max} == 10",
             "param2": ""}
        b = json.loads(res.data)

        self.assertEqual(a, b)

    def test_post_001(self):
        self.assertNotFound('/api/v1/validation/1')

        # create first one
        d = {"id": "0",
             "database_name": "DBNAME",
             "schema_name": "SCHEMA",
             "table_name": "TABLE",
             "column_name": "COLUMN",
             "description": "DESCRIPTION",
             "rule": "columnstat",
             "param": "{max} == 10",
             "param2": ""}
        res = self.cl.put('/api/v1/validation/0',
                          data=json.dumps(d))
        self.assertEqual(200, res.status_code)

        self.assertFound('/api/v1/validation/1')

        res = self.cl.get('/api/v1/validation/1')
        b = json.loads(res.data)
        self.assertEqual("{max} == 10", b['param'])

        # update first one
        d = {"id": "1",
             "database_name": "DBNAME",
             "schema_name": "SCHEMA",
             "table_name": "TABLE",
             "column_name": "COLUMN",
             "description": "DESCRIPTION",
             "rule": "columnstat",
             "param": "{max} == 100",
             "param2": ""}
        res = self.cl.post('/api/v1/validation/1',
                          data=json.dumps(d))
        self.assertEqual(200, res.status_code)

        # updated?
        res = self.cl.get('/api/v1/validation/1')
        b = json.loads(res.data)
        self.assertEqual("{max} == 100", b['param'])

    def test_delete_001(self):
        self.assertNotFound('/api/v1/validation/1')

        # create first one
        d = {"id": "0",
             "database_name": "DBNAME",
             "schema_name": "SCHEMA",
             "table_name": "TABLE",
             "column_name": "COLUMN",
             "description": "DESCRIPTION",
             "rule": "columnstat",
             "param": "{max} == 10",
             "param2": ""}
        res = self.cl.put('/api/v1/validation/0',
                          data=json.dumps(d))
        self.assertEqual(200, res.status_code)

        self.assertFound('/api/v1/validation/1')

        res = self.cl.delete('/api/v1/validation/1')

        self.assertNotFound('/api/v1/validation/1')

if __name__ == '__main__':
    unittest.main()
