#!/usr/bin/env python2.7
# -*- coding: utf-8 -*-

import json
import os
import unittest
from datetime import datetime

import sqlalchemy as sa

import db
import logger as log
from exception import DbProfilerException, InternalError
from logger import str2unicode as _s2u
from msgutil import gettext as _, jsonize
from repository import Repository
from table import Table2
from validation import ValidationRule


class DbProfilerRepository():
    filename = None
    engine = None
    use_pgsql = None
    host = None
    port = None
    user = None
    password = None

    def __init__(self, filename=None, host=None, port=None,
                 user=None, password=None):
        self.filename = filename if filename else 'repo.db'
        self.host = host
        self.port = port if port else 5432
        self.user = user if user else 'postgres'
        self.password = password if password else 'postgres'
        self.use_pgsql = True if self.host else False

        if self.host:
            db.creds = {}
            db.creds['host'] = self.host
            db.creds['port'] = self.port
            db.creds['username'] = self.user
            db.creds['password'] = self.password
            db.creds['dbname'] = self.filename
            db.connect()
        else:
            db.creds = {}
            db.creds['dbname'] = self.filename
            db.creds['use_sqlite'] = True
            db.connect()

    def init(self):
        repo = Repository()
        try:
            repo.create()
        except sa.exc.OperationalError as ex:
            err_msg = '(sqlite3.OperationalError) table repo already exists'
            if str(ex).startswith(err_msg):
                log.info(_("The repository has already been initialized."))
                return True
            log.error(_("Could not initialize the repository."))
            return False
        log.info(_("The repository has been initialized."))
        return True

    def destroy(self):
        repo = Repository()
        repo.destroy()
        if not self.use_pgsql:
            os.unlink(self.filename)
        log.info(_("The repository has been destroyed."))
        return True

    def get(self):
        jsondata = u""
        try:
            data_all = []

            for r in db.engine.execute("SELECT * FROM repo"):
                data_all.append(json.loads(unicode(r[4])))

            log.info(_("Retrieved all data from the repository `%s'.") %
                     self.filename)
        except Exception as e:
            log.error(_("Could not retreive from the repository `%s'") %
                      self.filename, detail=unicode(e))
            return None
        return data_all

    def fmt_datetime(self, ts):
        if self.use_pgsql:
            return "'%s'" % ts
        return "datetime('%s')" % ts

    def get_table_history(self, database_name, schema_name, table_name):
        """
        Get a table record history from the repository by object name,
        newest one coming first.

        Args:
            database_name(str): database name
            schema_name(str):   schema name
            table_name(str):    table name

        Returns:
            a list of dictionaries of table records. [{table record}, ...]
        """
        assert database_name and schema_name and table_name

        table_history = []

        query = """
SELECT data
  FROM repo
 WHERE database_name = '{0}'
   AND schema_name = '{1}'
   AND table_name = '{2}'
""".format(database_name, schema_name, table_name)

        log.trace("get_table_history: query = %s" % query)

        try:
            for r in db.engine.execute(query):
                data = json.loads(unicode(r[0]))

                # let's sort by the timestamp field in the table data,
                # not the created_at field of the repo table.
                done = False
                for i, t in enumerate(table_history):
                    if data['timestamp'] > t['timestamp']:
                        table_history.insert(i+1, data)
                        done = True
                        continue
                if not done:
                    table_history.insert(0, data)
        except Exception as ex:
            raise InternalError(
                "Could not get table data with its history: " + str(ex),
                query=query, source=ex)

        # newest first.
        return [x for x in reversed(table_history)]

    def put_table_fk(self, database_name1, schema_name1, table_name1,
                     column_name1,
                     database_name2, schema_name2, table_name2, column_name2,
                     guess=False):
        """
        Returns:
            True when successfully added from fk and/or fk_ref.
            Otherwise, Flase.
        """
        if database_name1 != database_name2:
            return False

        ret = False

        t = Table2.find(database_name1, schema_name1, table_name1)
        assert len(t) == 1
        t1 = t[0]
        tab1 = t1.data

        fk = '%s.%s.%s' % (schema_name2, table_name2, column_name2)
        if guess:
            fk = '?' + fk
        for col in tab1['columns']:
            if col['column_name'] == column_name1:
                if 'fk' not in col:
                    col['fk'] = []
                assert isinstance(col['fk'], list)
                if fk in col['fk']:
                    col['fk'].remove(fk)
                if fk not in col['fk']:
                    col['fk'].append(fk)
                    ret = True

        t1.update()

        t = Table2.find(database_name2, schema_name2, table_name2)
        assert len(t) == 1
        t2 = t[0]
        tab2 = t2.data

        fk_ref = '%s.%s.%s' % (schema_name1, table_name1, column_name1)
        if guess:
            fk_ref = '?' + fk_ref
        for col in tab2['columns']:
            if col['column_name'] == column_name2:
                if 'fk_ref' not in col:
                    col['fk_ref'] = []
                assert isinstance(col['fk_ref'], list)
                if fk_ref in col['fk_ref']:
                    col['fk_ref'].remove(fk_ref)
                if fk_ref not in col['fk_ref']:
                    col['fk_ref'].append(fk_ref)
                    ret = True

        t2.update()

        return ret

    def remove_table_fk(self, database_name1, schema_name1, table_name1,
                        column_name1,
                        database_name2, schema_name2, table_name2,
                        column_name2):
        """
        Returns:
            True when successfully removed from fk and/or fk_ref.
            Otherwise, Flase.
        """
        if database_name1 != database_name2:
            return False

        ret = False

        t = Table2.find(database_name1, schema_name1, table_name1)
        assert len(t) == 1
        t1 = t[0]
        tab1 = t1.data

        fk = '%s.%s.%s' % (schema_name2, table_name2, column_name2)
        for col in tab1['columns']:
            if col['column_name'] == column_name1:
                if 'fk' not in col or col['fk'] is None:
                    col['fk'] = []
                assert isinstance(col['fk'], list)
                if fk in col['fk']:
                    col['fk'].remove(fk)
                    ret = True

        t1.update()

        t = Table2.find(database_name2, schema_name2, table_name2)
        if len(t) == 0:
            return False
        t2 = t[0]
        tab2 = t2.data

        fk_ref = '%s.%s.%s' % (schema_name1, table_name1, column_name1)
        for col in tab2['columns']:
            if 'fk_ref' not in col or col['fk_ref'] is None:
                col['fk_ref'] = []
            assert isinstance(col['fk_ref'], list)
            if fk_ref in col['fk_ref']:
                col['fk_ref'].remove(fk_ref)
                ret = True

        t2.update()

        return ret

    def clear_table_fk(self, database_name, schema_name, table_name,
                       column_name):
        """
        Returns:
            True when successfully cleared fk and/or fk_ref.
            Otherwise, Flase.
        """
        ret = False

        t = Table2.find(database_name, schema_name, table_name)
        assert len(t) == 1
        t1 = t[0]
        tab = t[0].data

        for col in tab['columns']:
            if col['column_name'] == column_name:
                if 'fk' in col and len(col['fk']) > 0:
                    assert isinstance(col['fk'], list)
                    col['fk'] = []
                    ret = True
                if 'fk_ref' in col and len(col['fk_ref']) > 0:
                    assert isinstance(col['fk_ref'], list)
                    col['fk_ref'] = []
                    ret = True
        t1.update()

        return ret

    def open(self):
        assert db.engine
        return

    def close(self):
        pass


class TestDbProfilerRepository(unittest.TestCase):
    repo = None

    def setUp(self):
        host = os.environ.get('PGHOST', 'localhost')
        dbname = os.environ.get('PGDATABASE', 'datacatalog')
        user = os.environ.get('PGUSER', 'postgres')
        password = os.environ.get('PGPASSWORD', 'postgres')

        self.repo = DbProfilerRepository(filename=dbname,
                                         host=host,
                                         user=user,
                                         password=password)
        self.repo.init()
        self.repo.open()
        self.maxDiff = None

    def tearDown(self):
        self.repo.close()
        self.repo.destroy()

    def test_get_schemas_001(self):
        pass

if __name__ == '__main__':
    unittest.main()
