#!/usr/bin/env python2.7
# -*- coding: utf-8 -*-

import copy

from hecatoncheir import DbProfilerBase
from hecatoncheir.DbProfilerException import InternalError
from hecatoncheir import DbProfilerValidator
import PgDriver
from hecatoncheir.QueryResult import QueryResult
from hecatoncheir import logger as log
from hecatoncheir.logger import str2unicode as _s2u
from hecatoncheir.msgutil import gettext as _


class PgProfiler(DbProfilerBase.DbProfilerBase):
    dbdriver = None
    dbconn = None
    column_cache = None

    def __init__(self, host, port, dbname, dbuser, dbpass, debug=False):
        DbProfilerBase.DbProfilerBase.__init__(self, host, port, dbname,
                                               dbuser, dbpass, debug)
        connstr = "host=%s port=%d dbname=%s" % (host, port, dbname)
        self.dbdriver = PgDriver.PgDriver(connstr, dbuser, dbpass)
        self.column_cache = {}
#        print(self.dbconn)

    def get_schema_names(self):
        ignores = [u'information_schema',
                   u'pg_catalog',
                   u'pg_temp_1',
                   u'pg_toast',
                   u'pg_toast_temp_1']

        q = u'SELECT nspname FROM pg_namespace ORDER BY nspname'
        return self._query_schema_names(q, ignores)

    def get_table_names(self, schema_name):
        q = u'''
SELECT table_name
  FROM information_schema.tables
 WHERE table_schema = '%s'
 ORDER BY table_name
''' % schema_name

        return self._query_table_names(q)

    def get_column_names(self, schema_name, table_name):
        q = u'''
SELECT column_name
  FROM information_schema.columns
 WHERE table_schema = '%s'
   AND table_name = '%s'
 ORDER BY ordinal_position
''' % (schema_name, table_name)

        return self._query_column_names(q)

    def get_sample_rows(self, schema_name, table_name, rows_limit=10):
        column_name = self.get_column_names(schema_name, table_name)

        select_list = '"' + '","'.join(column_name) + '"'
        q = u'SELECT {0} FROM "{1}"."{2}" LIMIT {3}'.format(
            select_list, schema_name, table_name, rows_limit)
        return self._query_sample_rows(q)

    def get_column_datatypes(self, schema_name, table_name):
        q = u'''
SELECT column_name,
       data_type,
       character_maximum_length
  FROM information_schema.columns
 WHERE table_schema = '%s'
   AND table_name = '%s'
 ORDER BY ordinal_position
''' % (schema_name, table_name)

        return self._query_column_datetypes(q)

    def get_row_count(self, schema_name, table_name):
        if (schema_name, table_name) not in self.column_cache:
            self.__get_column_profile_phase1(schema_name, table_name)
        return self.column_cache[(schema_name, table_name)][0]

    def get_column_nulls(self, schema_name, table_name):
        if (schema_name, table_name) not in self.column_cache:
            self.__get_column_profile_phase1(schema_name, table_name)
        return self.column_cache[(schema_name, table_name)][2]

    @staticmethod
    def has_minmax(data_type):
        assert isinstance(data_type, list)
        log.trace("has_minmax: " + unicode(data_type))
        if data_type[0].upper() in ['BYTEA', 'OID', 'BOOLEAN', 'XID', 'ARRAY',
                                    'JSON', 'JSONB']:
            return False
        return True

    def get_column_min_max(self, schema_name, table_name):
        if (schema_name, table_name) not in self.column_cache:
            self.__get_column_profile_phase1(schema_name, table_name)
        return self.column_cache[(schema_name, table_name)][1]

    def __get_column_profile_phase1(self, schema_name, table_name):
        column_names = self.get_column_names(schema_name, table_name)
        select_list = []
        # num of rows
        select_list.append('COUNT(*)')

        for n, c in enumerate(column_names):
            log.trace("__get_column_profile_phase1: %s" % c)
            # nulls
            select_list.append(
                'COUNT(CASE WHEN "%s" IS NULL THEN 1 ELSE NULL END)' % c)
            # min,max
            if PgProfiler.has_minmax(self.get_column_datatypes(schema_name,
                                                               table_name)[c]):
                select_list.append(u'MIN("%s")' % c)
                select_list.append(u'MAX("%s")' % c)
            else:
                select_list.append('NULL')
                select_list.append('NULL')
        q = u'SELECT %s FROM "%s"."%s"' % (','.join(select_list),
                                           schema_name, table_name)
        log.trace(q)

        (num_rows, _minmax, _nulls) = self._query_column_profile(column_names,
                                                                 q)

        # cache the results
        self.column_cache[(schema_name, table_name)] = (num_rows,
                                                        _minmax, _nulls)
        return True

    def get_column_most_freq_values(self, schema_name, table_name):
        return self.__get_column_freq_values(schema_name, table_name, False)

    def get_column_least_freq_values(self, schema_name, table_name):
        return self.__get_column_freq_values(schema_name, table_name, True)

    def __get_column_freq_values(self, schema_name, table_name, ascending):
        column_names = self.get_column_names(schema_name, table_name)
        data_types = self.get_column_datatypes(schema_name, table_name)

        value_freqs = {}
        for col in column_names:
            value_freqs[col] = []

            if not PgProfiler.has_minmax(data_types[col]):
                continue

            ascdesc = "ASC" if ascending else "DESC"
            q = u'''
WITH TEMP AS (
SELECT
  "{2}",
  COUNT(*) AS COUNT
FROM
  "{0}"."{1}"
WHERE
  "{2}" IS NOT NULL
GROUP BY
  "{2}"
)
SELECT * FROM TEMP
ORDER BY
  2 {3}, 1
LIMIT {4}
'''.format(schema_name, table_name, col, ascdesc,
                self.profile_most_freq_values_enabled)

            self._query_value_freqs(q, col, value_freqs)
        return value_freqs

    def get_column_cardinalities(self, schema_name, table_name):
        column_names = self.get_column_names(schema_name, table_name)
        column_cardinalities = {}
        for col in column_names:
            q = u'''
WITH TEMP AS (
SELECT
  DISTINCT "{2}"
FROM
  "{0}"."{1}"
WHERE
  "{2}" IS NOT NULL
)
SELECT COUNT(*) FROM TEMP
'''.format(schema_name, table_name, col)

            self._query_column_cardinality(q, col, column_cardinalities)
        return column_cardinalities

    def run_record_validation(self, schema_name, table_name,
                              validation_rules=None, fetch_size=500000):
        log.trace('run_record_validation: start. %s.%s' %
                  (schema_name, table_name))

        v = DbProfilerValidator.DbProfilerValidator(
            schema_name, table_name, validation_rules=validation_rules)
        if not v.record_validators:
            log.info(_("Skipping record validation since no validation rule."))
            return {}

        column_names = self.get_column_names(schema_name, table_name)
        if not column_names:
            raise InternalError(
                'No column found on the table `%s\'.')
        q = u'SELECT "%s" FROM "%s"."%s"' % ('","'.join(column_names),
                                             schema_name, table_name)

        (count, failed) = self._query_record_validation(q, v,
                                                        fetch_size=fetch_size)

        log.trace("run_record_validation: end. "
                  "row count %d invalid record %d" % (count, failed))
        return v.get_validation_results()
