# -*- coding: utf-8 -*-

import os
import unittest

import sqlalchemy as sa

from repository import Repository
import db


def sql_escape(s):
    if not s:
        return ''
    return s.replace("'", "''")


def get_validation_rules(database_name=None, schema_name=None,
                         table_name=None):
    """
    Get validation rules in the list composed by the old format (tuples).

    FIXME:
    This function is kept for the backward compatibility for now,
    and should be removed by the further refactoring.

    Returns:
      list: a list of tuples containing validation rules.
    """
    rules = []
    for r in ValidationRule.find(database_name=database_name,
                                 schema_name=schema_name,
                                 table_name=table_name):
        rules.append((r.id,
                      r.database_name,
                      r.schema_name,
                      r.table_name,
                      r.column_name,
                      r.description,
                      r.rule,
                      r.param,
                      r.param2))
    return rules


class ValidationRule:
    def __init__(self, id_, database_name, schema_name, table_name,
                 column_name, description, rule,
                 param=None, param2=None):
        self.id = id_
        self.database_name = database_name
        self.schema_name = schema_name
        self.table_name = table_name
        self.column_name = column_name
        self.description = description
        self.rule = rule
        self.param = param
        self.param2 = param2

    @staticmethod
    def create(database_name, schema_name, table_name,
               column_name, description, rule,
               param=None, param2=None):
        rs = db.engine.execute("SELECT max(id) FROM validation_rule")
        r = rs.fetchone()
        id_ = r[0]+1 if r[0] else 1

        query = u"""
INSERT INTO validation_rule (id,database_name,schema_name,table_name,
                             column_name,description,rule,param,param2)
  VALUES ('{0}','{1}','{2}','{3}','{4}','{5}','{6}','{7}', '{8}');
""".format(id_, database_name, schema_name, table_name, column_name,
           description, rule, sql_escape(param), sql_escape(param2))

        db.engine.execute(query)

        return ValidationRule(id_, database_name, schema_name, table_name,
                              column_name, description, rule,
                              param, param2)

    @staticmethod
    def find(id_=None, database_name=None, schema_name=None, table_name=None):
        cond = ['1=1']
        if id_:
            cond.append('id = %s' % id_)
        if database_name:
            cond.append("database_name = '%s'" % database_name)
        if schema_name:
            cond.append("schema_name = '%s'" % schema_name)
        if table_name:
            cond.append("table_name = '%s'" % table_name)

        query = u"""
SELECT
  id,
  database_name,
  schema_name,
  table_name,
  column_name,
  description,
  rule,
  param,
  param2
FROM
  validation_rule
"""

        rs = db.engine.execute(query + "WHERE " +
                               " AND ".join(cond) + " ORDER BY id")
        rules = []
        for r in rs:
            v = ValidationRule(r[0], r[1], r[2], r[3], r[4],
                               r[5], r[6], r[7], r[8])
            rules.append(v)
        return rules

    def update(self):
        query = u"""
UPDATE
  validation_rule
SET
  database_name = '{1}',
  schema_name = '{2}',
  table_name = '{3}',
  column_name = '{4}',
  description = '{5}',
  rule = '{6}',
  param = '{7}',
  param2 = '{8}'
WHERE
  id = '{0}'
""".format(self.id, self.database_name, self.schema_name,
           self.table_name, self.column_name,
           self.description, self.rule,
           sql_escape(self.param), sql_escape(self.param2))

        db.engine.execute(query)

        return True

    def destroy(self):
        query = u"DELETE FROM validation_rule WHERE id = '%s'" % self.id

        db.engine.execute(query)

        return True


class TestValidationRule(unittest.TestCase):
    def setUp(self):
        db.creds = {}
        db.creds['host'] = os.environ.get('PGHOST', 'localhost')
        db.creds['port'] = os.environ.get('PGPORT', 5432)
        db.creds['dbname'] = os.environ.get('PGDATABASE', 'datacatalog')
        db.creds['username'] = os.environ.get('PGUSER', 'postgres')
        db.creds['password'] = os.environ.get('PGPASSWORD', 'postgres')

        self.repo = Repository()
        self.repo.destroy()
        self.repo.create()

        self.maxDiff = None

    def tearDown(self):
        db.conn.close()

    def test_create_001(self):
        v = ValidationRule.create('d', 's', 't', 'c', 'desc', 'r', 'p', 'p2')
        self.assertEquals(1, v.id)
        self.assertEquals('d', v.database_name)
        self.assertEquals('s', v.schema_name)
        self.assertEquals('t', v.table_name)
        self.assertEquals('c', v.column_name)
        self.assertEquals('desc', v.description)
        self.assertEquals('r', v.rule)
        self.assertEquals('p', v.param)
        self.assertEquals('p2', v.param2)

        v = ValidationRule.create('d', 's', 't', 'c', 'desc', 'r', 'p')
        self.assertEquals(2, v.id)
        self.assertEquals('p', v.param)
        self.assertIsNone(v.param2)

        v = ValidationRule.create('d', 's', 't', 'c', 'desc', 'r')
        self.assertEquals(3, v.id)
        self.assertIsNone(v.param)
        self.assertIsNone(v.param2)

    def test_create_002(self):
        # unicode characters
        v = ValidationRule.create('d', 's', u'テーブル名', u'カラム名',
                                  u'説明', 'r', u'パラメータ', u'パラメータ2')
        self.assertTrue(v.update())

    def test_create_003(self):
        # dup error
        v = ValidationRule.create('d', 's', 't', 'c', 'desc', 'r', 'p', 'p2')
        with self.assertRaises(sa.exc.IntegrityError) as cm:
            ValidationRule.create('d', 's', 't', 'c', 'desc', 'r', 'p', 'p2')
        m = str(cm.exception)
        self.assertTrue(m.startswith('(psycopg2.IntegrityError) '))

    def test_find_001(self):
        ValidationRule.create('d', 's', 't', 'c', 'desc',
                              'r', 'p', 'p2')
        ValidationRule.create('d', 's', 't', 'c', 'desc2',
                              'r', 'p2', 'p2')
        r = ValidationRule.find(id_=1)

        self.assertEquals(1, len(r))
        self.assertEquals(1, r[0].id)
        self.assertEquals('d', r[0].database_name)
        self.assertEquals('s', r[0].schema_name)
        self.assertEquals('t', r[0].table_name)
        self.assertEquals('c', r[0].column_name)
        self.assertEquals('desc', r[0].description)
        self.assertEquals('r', r[0].rule)
        self.assertEquals('p', r[0].param)
        self.assertEquals('p2', r[0].param2)

        r = ValidationRule.find(id_=2)

        self.assertEquals(1, len(r))
        self.assertEquals('desc2', r[0].description)
        self.assertEquals('p2', r[0].param)

        r = ValidationRule.find(id_=3)

        self.assertEquals(0, len(r))

    def test_find_002(self):
        v1 = ValidationRule.create('d', 's', 't', 'c', 'desc',
                                   'r', 'p', 'p2')
        v2 = ValidationRule.create('d', 's2', 't', 'c', 'desc2',
                                   'r', 'p2', 'p2')

        r = ValidationRule.find(database_name='d')

        self.assertEquals(2, len(r))
        self.assertEquals(v1.id, r[0].id)
        self.assertEquals(v2.id, r[1].id)

        r = ValidationRule.find(schema_name='s2')

        self.assertEquals(1, len(r))
        self.assertEquals(v2.id, r[0].id)

        r = ValidationRule.find(table_name='t')

        self.assertEquals(2, len(r))
        self.assertEquals(v1.id, r[0].id)
        self.assertEquals(v2.id, r[1].id)

        r = ValidationRule.find(table_name='nosuch')

        self.assertEquals(0, len(r))

    def test_update_001(self):
        v = ValidationRule.create('d', 's', 't', 'c', 'desc',
                                  'r', 'p', 'p2')
        v2 = ValidationRule.create('d', 's', 't', 'c', 'desc2',
                                   'r', 'p2', 'p2')

        self.assertEquals(1, v.id)
        self.assertEquals('d', v.database_name)
        self.assertEquals('s', v.schema_name)
        self.assertEquals('t', v.table_name)
        self.assertEquals('c', v.column_name)
        self.assertEquals('desc', v.description)
        self.assertEquals('r', v.rule)
        self.assertEquals('p', v.param)
        self.assertEquals('p2', v.param2)

        v.database_name = v.database_name + '_'
        v.schema_name = v.schema_name + '_'
        v.table_name = v.table_name + '_'
        v.column_name = v.column_name + '_'
        v.description = v.description + '_'
        v.rule = v.rule + '_'
        v.param = v.param + '_'
        v.param2 = v.param2 + '_'

        self.assertTrue(v.update())

        # all fields got changed
        v = ValidationRule.find(id_=1)
        self.assertEquals(1, len(v))
        self.assertEquals('d_', v[0].database_name)
        self.assertEquals('s_', v[0].schema_name)
        self.assertEquals('t_', v[0].table_name)
        self.assertEquals('c_', v[0].column_name)
        self.assertEquals('desc_', v[0].description)
        self.assertEquals('r_', v[0].rule)
        self.assertEquals('p_', v[0].param)
        self.assertEquals('p2_', v[0].param2)

        # not changed
        v = ValidationRule.find(id_=2)
        self.assertEquals(1, len(v))
        self.assertEquals('desc2', v[0].description)

    def test_destroy_001(self):
        ValidationRule.create('d', 's', 't', 'c', 'desc', 'r', 'p', 'p2')
        ValidationRule.create('d', 's', 't', 'c', 'desc2', 'r', 'p2', 'p2')

        vv = ValidationRule.find()
        self.assertEquals(2, len(vv))

        v = ValidationRule.find(id_=2)
        self.assertEquals(1, len(v))
        self.assertEquals('desc2', v[0].description)
        self.assertTrue(v[0].destroy())

        v = ValidationRule.find(id_=2)
        self.assertEquals(0, len(v))

        v = ValidationRule.find(id_=1)
        self.assertEquals(1, len(v))
        self.assertEquals('desc', v[0].description)

    def test_get_validation_rules_001(self):
        self.assertEqual([], get_validation_rules())

    def test_get_validation_rules_002(self):
        self.assertIsNotNone(ValidationRule.create('db1', 's1', 't1', 'c',
                                                   'desc1', 'r', 'p', 'p2'))
        self.assertIsNotNone(ValidationRule.create('db2', 's2', 't2', 'c',
                                                   'desc2', 'r', 'p', 'p2'))
        self.assertIsNotNone(ValidationRule.create('db3', 's3', 't3', 'c',
                                                   'desc3', 'r', 'p', 'p2'))

        a = [(1, u'db1', u's1', u't1', u'c', u'desc1',
              u'r', u'p', u'p2')]
        self.assertEqual(a, get_validation_rules(database_name='db1'))

        a = [(2, u'db2', u's2', u't2', u'c', u'desc2',
              u'r', u'p', u'p2')]
        self.assertEqual(a, get_validation_rules(schema_name='s2'))

        a = [(3, u'db3', u's3', u't3', u'c', u'desc3',
              u'r', u'p', u'p2')]
        self.assertEqual(a, get_validation_rules(table_name='t3'))

        self.assertEqual([], get_validation_rules(database_name='db1',
                                                  schema_name='s2',
                                                  table_name='t3'))

if __name__ == '__main__':
    unittest.main()
