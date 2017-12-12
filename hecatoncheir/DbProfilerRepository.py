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


class DbProfilerRepository():
    filename = None
    engine = None
    use_pgsql = None
    host = None
    port = None
    user = None
    password = None

    def __init__(self, filename=None, host=None, port=None, user=None, password=None):
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
            if str(ex).startswith('(sqlite3.OperationalError) table repo already exists'):
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

    def set(self, data):
        try:
            db.engine.execute("DELETE FROM repo")
        except Exception as e:
            log.error(_("Could not initialize the repository."),
                      detail=unicode(e))
            return False

        for d in data:
            self.append_table(d)

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

    def append_table(self, tab):
        """
        Update a table record if the same record (with same timestamp)
        already exist.
        Otherwise, append the table record to the repository.

        Args:
            tab: a dictionary of table record.

        Returns:
            True on success, otherwise False.
        """
        assert (tab['database_name'] and tab['schema_name'] and
                tab['table_name'] and tab['timestamp'])

        try:        
            t = Table2.find(tab['database_name'], tab['schema_name'],
                            tab['table_name'])
            if t:
                assert len(t) == 1
                t[0].data = tab
                t[0].update()
            else:
                Table2.create(tab['database_name'], tab['schema_name'],
                              tab['table_name'], tab)
        except sa.exc.OperationalError as ex:
            raise InternalError('Could not append table data: ' + str(ex))

        log.trace("append_table: end")
        return True

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

    def get_datamap_items(self, database_name, schema_name, table_name,
                          column_name=None):
        """Get one or more datamap entries from the repository

        Args:
          database_name (str):
          schema_name_name (str):
          table_name (str):
          column_name (str):

        Returns:
          list: a list which consists of one or more datamap entries.
        """
        assert database_name and schema_name and table_name

        query = u"""
SELECT data FROM datamapping
WHERE database_name = '%s' AND schema_name = '%s' AND table_name = '%s'
""" % (database_name, schema_name, table_name)
        if column_name:
            query = query + u" AND column_name = '%s'" % column_name
        query = query + u"ORDER BY lineno"

        datamap = []
        try:
            for r in db.engine.execute(query):
                datamap.append(json.loads(r[0]))
        except Exception as ex:
            raise InternalError("Could not get data:" + str(ex), query=query)

        return datamap

    def put_datamap_item(self, datamap):
        """Put a datamap entry into the repository

        Args:
          datamap (dict): a dictionary holding a datamap entry.

        Returns:
          bool: True on success, otherwise False.
        """
        assert (datamap.get('database_name') and datamap.get('schema_name') and
                datamap.get('table_name') and datamap.get('record_id'))

        for k in datamap:
            if isinstance(datamap[k], unicode):
                datamap[k] = datamap[k].replace("'", "''")

        query = u"""
DELETE FROM datamapping
 WHERE database_name = '%s'
   AND schema_name = '%s'
   AND table_name = '%s'
   AND column_name = '%s'
   AND record_id = '%s'
""" % (datamap['database_name'], datamap['schema_name'],
            datamap['table_name'], datamap.get('column_name', ''),
            datamap.get('record_id', ''))

        try:
            db.engine.execute(query)
            log.trace(u'Successfully removed the previous datamap entry: %s' %
                      datamap)
        except Exception as ex:
            raise InternalError("Could not register data mapping: " + str(ex),
                                query=query)

        query = u"""
INSERT INTO datamapping (
  lineno,
  database_name,schema_name,table_name,column_name,
  record_id,
  source_database_name,source_schema_name,source_table_name,source_column_name,
  created_at,
  data)
 VALUES (
  %d,
  '%s', '%s', '%s', '%s',
  '%s',
  '%s', '%s', '%s', '%s',
  %s, '%s')
""" % (datamap['lineno'], datamap['database_name'], datamap['schema_name'],
            datamap['table_name'], datamap.get('column_name', ''),
            datamap.get('record_id', ''),
            datamap.get('source_database_name', ''),
            datamap.get('source_schema_name', ''),
            datamap.get('source_table_name', ''),
            datamap.get('source_column_name', ''),
            self.fmt_datetime(datetime.now().isoformat()),
            json.dumps(datamap))

        try:
            db.engine.execute(query)
            log.trace(u'Successfully stored the datamap entry: %s' % datamap)
        except Exception as ex:
            raise InternalError("Could not register data mapping: " + str(ex),
                                query=query)

        return True

    def get_datamap_source_tables(self, database_name, schema_name,
                                  table_name):
        """Get source talbe names from the data mapping info.

        Args:
          database_name (str):
          schema_name (str):
          tablename (str):

        Returns:
          list: a list of source table names.
        """
        assert database_name and schema_name and table_name

        tables = []
        for d in self.get_datamap_items(database_name, schema_name,
                                        table_name):
            if d['source_table_name'] not in tables:
                tables.append(d['source_table_name'])

        return tables

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
        if len(t) == 0:
            return False
        tab1 = t[0].data

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

        self.append_table(tab1)

        t = Table2.find(database_name2, schema_name2, table_name2)
        if len(t) == 0:
            return False
        tab2 = t[0].data

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

        self.append_table(tab2)

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
        if len(t) == 0:
            return False
        tab1 = t[0].data

        fk = '%s.%s.%s' % (schema_name2, table_name2, column_name2)
        for col in tab1['columns']:
            if col['column_name'] == column_name1:
                if 'fk' not in col or col['fk'] is None:
                    col['fk'] = []
                assert isinstance(col['fk'], list)
                if fk in col['fk']:
                    col['fk'].remove(fk)
                    ret = True

        self.append_table(tab1)

        t = Table2.find(database_name2, schema_name2, table_name2)
        if len(t) == 0:
            return False
        tab2 = t[0].data

        fk_ref = '%s.%s.%s' % (schema_name1, table_name1, column_name1)
        for col in tab2['columns']:
            if 'fk_ref' not in col or col['fk_ref'] is None:
                col['fk_ref'] = []
            assert isinstance(col['fk_ref'], list)
            if fk_ref in col['fk_ref']:
                col['fk_ref'].remove(fk_ref)
                ret = True

        self.append_table(tab2)

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
        if len(t) == 0:
            return False
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
        self.append_table(tab)

        return ret

    def get_bg_term(self, term):
        """
        Returns:
            list: the term and its attributes if the term exists.
                  Otherwise, None.
        """
        data = None

        query = u"""
SELECT
  id,
  term,
  description_short,
  description_long,
  created_at,
  owned_by,
  categories,
  synonyms,
  related_terms,
  assigned_assets
FROM
  business_glossary
WHERE
  term = '%s'
AND
  is_latest = 1
""" % term
        try:
            rows = db.engine.execute(query)
            r = rows.fetchone()
            if r:
                data = {}
                for i, c in enumerate(r):
                    colname = rows.keys()[i]
                    if colname in ['categories', 'synonyms',
                                   'related_terms',
                                   'assigned_assets']:
                        data[colname] = json.loads(c)
                    else:
                        data[colname] = c
        except Exception as ex:
            raise InternalError("Could not get a business term: " + str(ex),
                                query=query, source=ex)
        return data

    def put_bg_term(self, term, desc_short, desc_long, owner, categories,
                    synonyms, related_terms, assigned_assets):
        """
        Returns:
            True when the term get registered successfully. Otherwise, False.
        """
        assert isinstance(categories, list) or categories is None
        assert isinstance(synonyms, list) or synonyms is None
        assert isinstance(related_terms, list) or related_terms is None
        assert isinstance(assigned_assets, list) or assigned_assets is None

        log.trace("put_bg_term: start")

        query1 = (u"UPDATE business_glossary SET is_latest = 0 "
                  u"WHERE term = '%s' AND is_latest = 1" % term)

        query2 = u"""
INSERT INTO business_glossary (
  id,
  term,
  description_short,
  description_long,
  created_at,
  owned_by,
  categories,
  synonyms,
  related_terms,
  assigned_assets,
  is_latest
) VALUES (
  {0},
  '{1}',
  '{2}',
  '{3}',
  {9},
  '{4}',
  '{5}',
  '{6}',
  '{7}',
  '{8}',
  1
)
""".format(0, term, desc_short, desc_long, owner,
           json.dumps(categories),
           json.dumps(synonyms),
           json.dumps(related_terms),
           json.dumps(assigned_assets),
           'now()' if self.use_pgsql else 'datetime()')

        log.trace(query1)
        log.trace(query2)

        query = None
        try:
            query = query1
            db.engine.execute(query1)

            query = query2
            db.engine.execute(query2)
        except Exception as ex:
            raise InternalError("Could not register a business term: " + str(ex),
                                query=query, source=ex)

        log.trace("put_bg_term: end")
        return True

    def get_bg_terms_all(self):
        """
        Returns:
            list: a list of terms in the business glossary.
        """
        query = (u"SELECT term FROM business_glossary WHERE is_latest = 1 "
                 u"ORDER BY length(term) desc,term")
        try:
            data = []
            for r in db.engine.execute(query):
                data.append(r[0])
        except Exception as ex:
            raise InternalError("Could not get a list of business terms: " + str(ex),
                                query=query, source=ex)
        return data

    def get_validation_rules(self, database_name=None, schema_name=None,
                             table_name=None, column_name=None,
                             description=None, rule=None,
                             param=None, param2=None):
        """
        Returns:
            list: a list of tuples containing  validation rules.
        """
        query = u"SELECT id FROM validation_rule"
        cond = []
        if database_name:
            cond.append("database_name = '%s'" % database_name)
        if schema_name:
            cond.append("schema_name = '%s'" % schema_name)
        if table_name:
            cond.append("table_name = '%s'" % table_name)
        if column_name:
            cond.append("column_name = '%s'" % column_name)
        if rule:
            cond.append("rule = '%s'" % rule)
        if param is not None:
            cond.append("param = '%s'" % param.replace("'", "''"))
        if param2 is not None:
            cond.append("param2 = '%s'" % param2.replace("'", "''"))
        if len(cond) > 0:
            query = query + " WHERE (%s)" % ' AND '.join(cond)

        ids = []
        try:
            for r in db.engine.execute(query):
                ids.append(r[0])
        except Exception as ex:
            raise InternalError("Could not get validation rules: " + str(ex),
                                query=query, source=ex)

        rules = []
        for i in ids:
            rules.append(self.get_validation_rule(i))
        return rules

    def create_validation_rule(self, database_name, schema_name, table_name,
                               column_name, description, rule,
                               param='', param2=''):
        """
        Args:
            database_name(str):
            schema_name(str):
            table_name(str):
            column_name(str):
            description(str):
            rule(str):
            param(str):
            param2(str):

        Returns:
            integer when the rule successfully gets created. None when already
            exists.
        """

        r = self.get_validation_rules(database_name, schema_name, table_name,
                                      column_name, description, rule,
                                      param, param2)
        assert len(r) <= 1
        if r:
            log.warning((_("The same validation rule already exists: ") +
                         u"{0},{1},{2},{3},{4},{5},{6},{7}"
                         .format(database_name, schema_name, table_name,
                                 column_name, description, rule,
                                 param, param2)))
            return None

        query = u"""
INSERT INTO validation_rule (id,database_name,schema_name,table_name,
                             column_name,description,rule,param,param2)
  VALUES ((SELECT coalesce(max(id),0)+1 FROM validation_rule),
          '{0}','{1}','{2}','{3}','{4}','{5}','{6}','{7}');
""".format(database_name, schema_name, table_name, column_name, description,
           rule,
           '' if param is None else "%s" % param.replace("'", "''"),
           '' if param2 is None else "%s" % param2.replace("'", "''"))

        log.trace("create_validation_rule: %s" % query.replace('\n', ''))
        id = None
        try:
            db.engine.execute(query)
            rows = db.engine.execute("SELECT max(id) FROM validation_rule")
            id = rows.fetchone()[0]
        except Exception as ex:
            raise InternalError("Could not register validation rule: " + str(ex),
                                query=query, source=ex)
        return id

    def get_validation_rule(self, id):
        """
        Args:
            id(integer):

        Returns:
            tuple: (id,database_name,schema_name,table_name,column_name,
                    description,rule,param,param2) or None
        """
        query = (u"SELECT id,database_name,schema_name,table_name,column_name,"
                 u"description,rule,param,param2 FROM validation_rule "
                 u"WHERE id = %d" % id)

        log.trace("get_validation_rule: %s" % query.replace('\n', ''))
        tup = None
        try:
            rows = db.engine.execute(query)
            r = rows.fetchone()
            if r:
                tup = tuple(r)
        except Exception as ex:
            raise InternalError("Could not get validation rule: " + str(ex),
                                query=query, source=ex)
        return tup

    def update_validation_rule(self, id, database_name, schema_name,
                               table_name, column_name, description,
                               rule, param=None, param2=None):
        """
        Args:
            id(integer):
            database_name(str):
            schema_name(str):
            table_name(str):
            column_name(str):
            description(str):
            rule(str):
            param(str):
            param2(str):

        Returns:
            True when the rule successfully gets updated, otherwise False.
        """

        query = u"""
UPDATE validation_rule
   SET database_name = '{0}',
       schema_name = '{1}',
       table_name = '{2}',
       column_name = '{3}',
       description = '{4}',
       rule = '{5}',
       param = '{6}',
       param2 = '{7}'
 WHERE id = {8}
""".format(database_name, schema_name, table_name, column_name, description,
           rule,
           '' if param is None else "%s" % param,
           '' if param2 is None else "%s" % param2, id)

        log.trace("update_validation_rule: %s" % query.replace('\n', ''))
        rowcount = 0
        try:
            rows = db.engine.execute(query)
            rowcount = rows.rowcount
        except Exception as ex:
            raise InternalError("Could not update validation rule: " + str(ex),
                                query=query, source=ex)
        if rowcount == 0:
            return False
        return True

    def delete_validation_rule(self, id):
        """
        Args:
            id(integer):

        Returns:
            True when the rule successfully gets deleted, otherwise False.
        """

        query = u"DELETE FROM validation_rule WHERE id = %s" % id

        log.trace("delete_validation_rule: %s" % query.replace('\n', ''))
        rowcount = 0
        try:
            rows = db.engine.execute(query)
            rowcount = rows.rowcount
        except Exception as ex:
            raise InternalError("Could not delete validation rule: " + str(ex),
                                query=query, source=ex)
        if rowcount == 0:
            return False
        return True

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
