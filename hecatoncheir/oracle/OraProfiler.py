#!/usr/bin/env python2.7
# -*- coding: utf-8 -*-

from hecatoncheir import DbProfilerBase
from hecatoncheir.DbProfilerException import InternalError
from hecatoncheir import DbProfilerValidator
import OraDriver
from hecatoncheir.QueryResult import QueryResult
from hecatoncheir import logger as log
from hecatoncheir.logger import str2unicode as _s2u
from hecatoncheir.msgutil import gettext as _


class OraProfiler(DbProfilerBase.DbProfilerBase):
    dbdriver = None
    dbconn = None
    column_cache = None

    def __init__(self, host, port, dbname, dbuser, dbpass, debug=False):
        DbProfilerBase.DbProfilerBase.__init__(self, host, port, dbname,
                                               dbuser, dbpass, debug)
        # TNS name
        self.dbdriver = OraDriver.OraDriver(host, port, dbname, dbuser, dbpass)
        self.column_cache = {}

    def get_schema_names(self):
        ignores = [u'ANONYMOUS', u'APEX_030200', u'APEX_PUBLIC_USER',
                   u'APPQOSSYS', u'BI', u'CTXSYS', u'DBSNMP', u'DIP',
                   u'EXFSYS', u'FLOWS_FILES', u'HR', u'IX', u'MDDATA',
                   u'MDSYS', u'MGMT_VIEW', u'OE', u'OLAPSYS', u'ORACLE_OCM',
                   u'ORDDATA', u'ORDPLUGINS', u'ORDSYS', u'OUTLN',
                   u'OWBSYS', u'OWBSYS_AUDIT', u'PM', u'SH',
                   u'SI_INFORMTN_SCHEMA', u'SPATIAL_CSW_ADMIN_USR',
                   u'SPATIAL_WFS_ADMIN_USR', u'SYS', u'SYSMAN', u'SYSTEM',
                   u'WMSYS', u'XDB', u'XS$NULL']

        q = u"SELECT USERNAME FROM ALL_USERS ORDER BY USERNAME"
        return self._query_schema_names(q, ignores)

    def get_table_names(self, schema_name):
        q = u'''
SELECT TABLE_NAME
  FROM ALL_TABLES
 WHERE OWNER = '%s'
 ORDER BY TABLE_NAME
''' % schema_name

        return self._query_table_names(q)

    def get_column_names(self, schema_name, table_name):
        q = u'''
SELECT COLUMN_NAME
  FROM ALL_TAB_COLUMNS
 WHERE OWNER = '%s'
   AND TABLE_NAME = '%s'
 ORDER BY COLUMN_ID
''' % (schema_name, table_name)

        column_names = self._query_column_names(q)

        if len(column_names) > 0:
            return column_names

        # ALL_TAB_COLUMNSから取れなかった場合、SYNONYMの可能性があるので、
        # ALL_SYNONYMSを参照して再度取得を試みる。
        q = u'''
SELECT COLUMN_NAME
  FROM ALL_TAB_COLUMNS TC,
       ALL_SYNONYMS S
 WHERE TC.OWNER = S.OWNER
   AND TC.TABLE_NAME = S.TABLE_NAME
   AND S.OWNER = '%s'
   AND S.SYNONYM_NAME = '%s'
 ORDER BY COLUMN_ID
''' % (schema_name, table_name)

        return self._query_column_names(q)

    def get_sample_rows(self, schema_name, table_name, rows_limit=10):
        column_name = self.get_column_names(schema_name, table_name)

        select_list = '"' + '","'.join(column_name) + '"'
        q = (u'SELECT {0} FROM "{1}"."{2}" WHERE '
             'ROWNUM <= {3}'.format(select_list, schema_name, table_name,
                                    rows_limit))
        return self._query_sample_rows(q)

    def get_column_datatypes(self, schema_name, table_name):
        q = u'''
SELECT COLUMN_NAME,
       DATA_TYPE,
       DATA_LENGTH
  FROM ALL_TAB_COLUMNS
 WHERE OWNER = '%s'
   AND TABLE_NAME = '%s'
 ORDER BY COLUMN_ID
''' % (schema_name, table_name)

        data_types = self._query_column_datetypes(q)

        if len(data_types) > 0:
            return data_types

        q = u'''
SELECT COLUMN_NAME,
       DATA_TYPE,
       DATA_LENGTH
  FROM ALL_TAB_COLUMNS TC,
       ALL_SYNONYMS S
 WHERE TC.OWNER = S.OWNER
   AND TC.TABLE_NAME = S.TABLE_NAME
   AND S.OWNER = '%s'
   AND S.SYNONYM_NAME = '%s'
 ORDER BY COLUMN_ID
''' % (schema_name, table_name)

        return self._query_column_datetypes(q)

    @property
    def parallel_hint(self):
        if self.parallel_degree > 1:
            return '/*+ PARALLEL(%d) */' % self.parallel_degree
        return ''

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
        if data_type[0].upper() in ['BLOB', 'CLOB', 'NCLOB', 'BFILE', 'RAW',
                                    'LONG RAW']:
            return False
        return True

    def get_column_min_max(self, schema_name, table_name):
        if (schema_name, table_name) not in self.column_cache:
            self.__get_column_profile_phase1(schema_name, table_name)
        return self.column_cache[(schema_name, table_name)][1]

    def __get_column_profile_phase1(self, schema_name, table_name):
        column_names = self.get_column_names(schema_name, table_name)
        if column_names is None:
            return None
        data_types = self.get_column_datatypes(schema_name, table_name)

        select_list = []
        # num of rows
        select_list.append('COUNT(*)')

        for n, c in enumerate(column_names):
            log.trace("__get_column_profile_phase1: %s" % c)
            # nulls
            tmp = 'COUNT(CASE WHEN "%s" IS NULL THEN 1 ELSE NULL END)' % c
            select_list.append(tmp)
            # min,max
            if OraProfiler.has_minmax(data_types[c]):
                select_list.append(u'MIN("%s")' % c)
                select_list.append(u'MAX("%s")' % c)
            else:
                select_list.append('NULL')
                select_list.append('NULL')
        q = u'SELECT %s %s FROM "%s"."%s"' % (self.parallel_hint,
                                              ','.join(select_list),
                                              schema_name, table_name)
        log.trace(q)

        (num_rows, _minmax, _nulls) = self._query_column_profile(column_names,
                                                                 q)

        # cache the results
        self.column_cache[(schema_name, table_name)] = (num_rows, _minmax,
                                                        _nulls)
        return True

    def get_column_most_freq_values(self, schema_name, table_name):
        return self.__get_column_freq_values(schema_name, table_name, False)

    def get_column_least_freq_values(self, schema_name, table_name):
        return self.__get_column_freq_values(schema_name, table_name, True)

    def __get_column_freq_values(self, schema_name, table_name, ascending):
        column_names = self.get_column_names(schema_name, table_name)
        if column_names is None:
            return None
        data_types = self.get_column_datatypes(schema_name, table_name)

        value_freqs = {}
        for col in column_names:
            value_freqs[col] = []

            if not OraProfiler.has_minmax(data_types[col]):
                continue

            ascdesc = "ASC" if ascending else "DESC"
            q = u'''
WITH TEMP AS (
SELECT {5}
  "{2}",
  COUNT(*) AS COUNT
FROM
  "{0}"."{1}"
WHERE
  "{2}" IS NOT NULL
GROUP BY
  "{2}"
ORDER BY
  2 {3}, 1
)
SELECT * FROM TEMP
WHERE ROWNUM <= {4}
'''.format(schema_name, table_name, col, ascdesc,
                self.profile_most_freq_values_enabled, self.parallel_hint)

            self._query_value_freqs(q, col, value_freqs)
        return value_freqs

    def get_column_cardinalities(self, schema_name, table_name):
        column_names = self.get_column_names(schema_name, table_name)
        if column_names is None:
            return None

        column_cardinalities = {}
        for col in column_names:
            q = u'''
WITH TEMP AS (
SELECT {3}
  DISTINCT "{2}"
FROM
  "{0}"."{1}"
WHERE
  "{2}" IS NOT NULL
)
SELECT COUNT(*) FROM TEMP
'''.format(schema_name, table_name, col, self.parallel_hint)

            self._query_column_cardinality(q, col, column_cardinalities)
        return column_cardinalities

    def run_record_validation(self, schema_name, table_name,
                              validation_rules=None, fetch_size=500000):
        log.trace('run_record_validation: start. %s.%s' % (schema_name,
                                                           table_name))

        v = DbProfilerValidator.DbProfilerValidator(
            schema_name, table_name, validation_rules=validation_rules)
        if not v.record_validators:
            log.info(_("Skipping record validation since no validation rule."))
            return {}

        column_names = self.get_column_names(schema_name, table_name)
        if not column_names:
            msg = 'No column found on the table `%s\'.' % table_name
            raise InternalError(msg)
        q = u'SELECT %s "%s" FROM "%s"."%s"' % (self.parallel_hint,
                                                '","'.join(column_names),
                                                schema_name, table_name)

        (count, failed) = self._query_record_validation(q, v,
                                                        fetch_size=fetch_size)

        log.trace(("run_record_validation: end. "
                   "row count %d invalid record %d" %
                  (count, failed)))
        return v.get_validation_results()
