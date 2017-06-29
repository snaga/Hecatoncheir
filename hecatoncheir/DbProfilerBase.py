#!/usr/bin/env python2.7
# -*- coding: utf-8 -*-

import copy
import json
import sys
from abc import ABCMeta, abstractmethod
from datetime import datetime

import dateutil.parser

import DbProfilerValidator
import logger as log
from exception import (DbProfilerException, InternalError, QueryError,
                       QueryTimeout)
from logger import str2unicode as _s2u, to_unicode as _2u
from metadata import TableColumnMeta, TableMeta
from msgutil import gettext as _


def migrate_table_meta(olddata, newdata):
    if olddata is None:
        return

    # table meta data to be migrated
    newdata['table_name_nls'] = olddata.get('table_name_nls')
    newdata['comment'] = olddata.get('comment')
    newdata['tags'] = olddata.get('tags')
    newdata['owner'] = olddata.get('owner')

    # column meta data to be migrated
    for c in newdata['columns']:
        for oldc in olddata.get('columns', []):
            if oldc['column_name'] == c['column_name']:
                c['column_name_nls'] = oldc.get('column_name_nls')
                c['comment'] = oldc.get('comment')
                c['fk'] = oldc.get('fk')
                c['fk_ref'] = oldc.get('fk_ref')

    return newdata


class DbProfilerBase(object):
    __metaclass__ = ABCMeta

    host = None
    port = None
    dbname = None
    dbuser = None
    dbpass = None

    profile_row_count_enabled = True
    profile_min_max_enabled = True
    profile_nulls_enabled = True
    profile_most_freq_values_enabled = 10
    profile_column_cardinality_enabled = True
    profile_sample_rows = True
    column_profiling_threshold = 100000000
    skip_table_profiling = False
    skip_column_profiling = False

    parallel_degree = 0
    timeout = None

    def __init__(self, host, port, dbname, dbuser, dbpass, debug=False):
        self.host = host
        self.port = port
        self.dbname = dbname
        self.dbuser = dbuser
        self.dbpass = dbpass
        log.debug_enabled = debug

    def connect(self):
        if self.dbconn is None:
            log.info(_("Connecting the database."))
            try:
                self.dbdriver.connect()
            except DbProfilerException as e:
                log.error(_("Could not connect to the database."),
                          detail=e.source)
                log.error(_("Abort."))
                sys.exit(1)

            self.dbconn = self.dbdriver.conn
            log.info(_("Connected to the database."))
        return True

    @abstractmethod
    def get_schema_names(self):
        raise NotImplementedError

    def _query_schema_names(self, query, ignores=[]):
        """Common code shared by PostgreSQL/MySQL/Oracle/MSSQL profilers
        to get schema names.

        Args:
          query(str): a query string to be executed on each database.
          ignores(list): a list of schema names to be ignored.

        Returns:
          list: a list of the schema names.
        """
        rs = self.dbdriver.q2rs(query, timeout=self.timeout)
        schema_names = []
        for r in rs.resultset:
            if r[0] not in ignores:
                schema_names.append(_s2u(r[0]))
            log.trace("_query_schema_names: " + unicode(r))
        return schema_names

    @abstractmethod
    def get_table_names(self, schema_name):
        raise NotImplementedError

    def _query_table_names(self, query):
        """Common code shared by PostgreSQL/MySQL/Oracle/MSSQL profilers
        to get table names in the schema.

        Args:
          query(str): a query string to be executed on each database.

        Returns:
          list: a list of the table names.
        """
        rs = self.dbdriver.q2rs(query, timeout=self.timeout)
        table_names = []
        for r in rs.resultset:
            table_names.append(_s2u(r[0]))
            log.trace("_query_table_names: " + unicode(r))
        return table_names

    @abstractmethod
    def get_column_names(self, schema_name, table_name):
        raise NotImplementedError

    def _query_column_names(self, query):
        """Common code shared by PostgreSQL/MySQL/Oracle/MSSQL profilers
        to get column names of the table.

        Args:
          query(str): a query string to be executed on each database.

        Returns:
          list: a list of the column names.
        """
        rs = self.dbdriver.q2rs(query, timeout=self.timeout)
        column_names = []
        for r in rs.resultset:
            column_names.append(_s2u(r[0]))
            log.trace("_query_column_names: " + unicode(r))
        return column_names

    @abstractmethod
    def get_sample_rows(self, schema_name, table_name, rows_limit=10):
        raise NotImplementedError

    def _query_sample_rows(self, query):
        """Common code shared by PostgreSQL/MySQL/Oracle/MSSQL profilers
       to collect sample rows from the table.

        Args:
          query(str): a query string to be executed on each database.

        Returns:
          list: a list of the column names and rows:
                [[column names], [row1], [row2], ...]
        """
        rs = self.dbdriver.q2rs(query, timeout=self.timeout)
        sample_rows = []
        sample_rows.append(list(rs.column_names))
        for r in rs.resultset:
            sample_rows.append(list(r))
        return sample_rows

    @abstractmethod
    def get_column_datatypes(self, schema_name, table_name):
        """Get column data types of the table.

        Args:
            schema_name (str): Schema name
            table_name (str): Table name

        Returns:
            dic: {column_name, [type, len]}
        """
        raise NotImplementedError

    def _query_column_datetypes(self, query):
        """Common code shared by PostgreSQL/MySQL/Oracle/MSSQL profilers
        to get data types of the columns.

        Args:
          query(str): a query string to be executed on each database.

        Returns:
          dict: {column_name, [type, len]}
        """
        data_types = {}
        rs = self.dbdriver.q2rs(query, timeout=self.timeout)
        for r in rs.resultset:
            data_types[r[0]] = [r[1], r[2]]
            log.trace("_query_column_datetypes: " + unicode(r))
        return data_types

    @abstractmethod
    def get_row_count(self, schema_name, table_name):
        raise NotImplementedError

    @abstractmethod
    def get_column_nulls(self, schema_name, table_name):
        """Get number of null values in each column

        Args:
            schema_name (str): Schema name
            table_name (str): Table name

        Returns:
            dic: {column_name, count}
        """
        raise NotImplementedError

    @abstractmethod
    def get_column_min_max(self, schema_name, table_name):
        """Get min/max values of each column

        Args:
            schema_name (str): Schema name
            table_name (str): Table name

        Returns:
            dic: {column_name, [min, max]}
        """
        raise NotImplementedError

    def _query_column_profile(self, column_names, query):
        """Common code shared by PostgreSQL/MySQL/Oracle/MSSQL profilers
        to collect column profiles of the table.

        Args:
          column_names(list): column names.
          query(str): a query string to be executed on each database.

        Returns:
          tuple: (num_rows, minmax, nulls)
                 minmax and nulls are dictionaries having column names as
                 the keys.
        """
        _minmax = {}
        _nulls = {}
        num_rows = None
        try:
            rs = self.dbdriver.q2rs(query, timeout=self.timeout)
            assert len(rs.resultset) == 1

            a = copy.copy(list(rs.resultset[0]))
            num_rows = a.pop(0)
            log.trace("_query_column_profile: rows %d" % num_rows)
            i = 0
            while len(a) > 0:
                nulls = a.pop(0)
                colmin = a.pop(0)
                colmax = a.pop(0)
                log.trace(("_query_column_profile: col %s %d %s %s" %
                          (column_names[i], nulls, colmin, colmax)))
                _minmax[column_names[i]] = [colmin, colmax]
                _nulls[column_names[i]] = nulls
                i += 1
        except QueryError as e:
            log.error(_("Could not get row count/num of "
                        "nulls/min/max values."),
                      detail=e.value, query=query)
            raise e

        log.trace("_query_column_profile: %s" % str(_minmax))
        return (num_rows, _minmax, _nulls)

    @abstractmethod
    def get_column_most_freq_values(self, schema_name, table_name):
        """Get most frequent values of the columns in the table.

        Args:
            schema_name (str): Schema name
            table_name (str): Table name

        Returns:
            dic: {column_name: [[value1,count1],[value2,count2],...]}
        """
        raise NotImplementedError

    @abstractmethod
    def get_column_least_freq_values(self, schema_name, table_name):
        """Get least frequent values of the columns in the table.

        Args:
            schema_name (str): Schema name
            table_name (str): Table name

        Returns:
            dic: {column_name: [[value1,count1],[value2,count2],...]}
        """
        raise NotImplementedError

    def _query_value_freqs(self, query, column_name, freqs):
        """Common code shared by PostgreSQL/MySQL/Oracle/MSSQL profilers
        to get the frequencies of the column.
        This function updates a dictionary, and does not return any value.

        Args:
          query(str): a query string to be executed on each database.
          column_name(str): column name.
          freqs(dict): a dictionary which holds the frequencies of the columns.
                       This function updates this dictionary as output.
        """
        rs = self.dbdriver.q2rs(query, timeout=self.timeout)
        for r in rs.resultset:
            log.trace(("_query_value_freqs: col %s val %s freq %d" %
                       (column_name, _s2u(r[0]), _s2u(r[1]))))
            freqs[column_name].append([_s2u(r[0]), _s2u(r[1])])

    @abstractmethod
    def get_column_cardinalities(self, schema_name, table_name):
        """Get cardinality of each column

        Args:
            schema_name (str): Schema name
            table_name (str): Table name

        Returns:
            dic: {column_name, cardinality}
        """
        raise NotImplementedError

    def _query_column_cardinality(self, query, column_name, cardinalities):
        """Common code shared by PostgreSQL/MySQL/Oracle/MSSQL profilers
        to get cardinality of the column.
        This function updates a dictionary, and does not return any value.

        Args:
          query(str): a query string to be executed on each database.
          column_name(str): column name.
          cardinality(dict): a dictionary which holds the column cardinality.
                             This function updates this dictionary as output.
        """
        rs = self.dbdriver.q2rs(query, timeout=self.timeout)
        for r in rs.resultset:
            cardinalities[column_name] = int(r[0])
            log.trace(("_query_column_cardinality: col %s cardinality %d" %
                       (column_name, cardinalities[column_name])))

    @abstractmethod
    def run_record_validation(self, schema_name, table_name, validation_rules,
                              fetch_size):
        """Get validation results of each column

        Args:
            schema_name (str): Schema name
            table_name (str): Table name

        Returns:
            dic: {column_name, {'rule_name': invalid_count}}
        """
        raise NotImplementedError

    def _query_record_validation(self, query, validator, fetch_size):
        """Common code shared by PostgreSQL/MySQL/Oracle/MSSQL profilers
        to run the record validation.
        This function updates the validator members, so no need to return
        values.

        Args:
          query(str): a query string to be executed on each database.
          validator(DbProfilerValidator):
                      a validator object with record validation rules.
          fetch_size(int): fetch size for the cursor operation.

        Returns:
          tuple: a pair of int values: total count and failed count of
                 the records.
        """
        # Use a server-side cursor to avoid running the client memory out.
        if not self.dbconn:
            self.connect()
        cur = self.dbconn.cursor()
        cur.execute(query)
        assert cur.description
        fnames = [x[0] for x in cur.description]
        log.trace("_query_record_validation: desc = %s" % unicode(fnames))
        count = 0
        failed = 0
        while True:
            rs = cur.fetchmany(fetch_size)
            if not rs:
                break
            for r in rs:
                if not validator.validate_record(fnames, r):
                    failed += 1
                count += 1
        cur.close()
        return (count, failed)

    def run_column_profiling(self, tablemeta):
        if self.profile_nulls_enabled is True:
            log.info(_("Number of nulls: start"))
            nulls = self.get_column_nulls(tablemeta.schema_name,
                                          tablemeta.table_name)
            for col in tablemeta.column_names:
                cm = tablemeta.get_column_meta(col)
                cm.nulls = long(nulls[col])
            log.info(_("Number of nulls: end"))

        if self.profile_min_max_enabled is True:
            log.info(_("Min/Max values: start"))
            minmax = self.get_column_min_max(tablemeta.schema_name,
                                             tablemeta.table_name)
            for col in tablemeta.column_names:
                if isinstance(minmax[col][0], str):
                    minmax[col][0] = minmax[col][0].decode('utf-8')
                    minmax[col][1] = minmax[col][1].decode('utf-8')
                cm = tablemeta.get_column_meta(col)
                cm.min = u'%s' % minmax[col][0]
                cm.max = u'%s' % minmax[col][1]
            log.info(_("Min/Max values: end"))

        if self.profile_most_freq_values_enabled > 0:
            log.info(_("Most/Least freq values(1/2): start"))
            most_freqs = self.get_column_most_freq_values(
                tablemeta.schema_name,
                tablemeta.table_name)
            log.info(_("Most/Least freq values(2/2): start"))
            least_freqs = self.get_column_least_freq_values(
                tablemeta.schema_name,
                tablemeta.table_name)
            for col in tablemeta.column_names:
                cm = tablemeta.get_column_meta(col)
                cm.most_freq_values = most_freqs[col]
                cm.least_freq_values = least_freqs[col]
            log.info(_("Most/Least freq values: end"))

        if self.profile_column_cardinality_enabled is True:
            log.info(_("Column cardinality: start"))
            column_cardinality = self.get_column_cardinalities(
                tablemeta.schema_name,
                tablemeta.table_name)
            for col in tablemeta.column_names:
                cm = tablemeta.get_column_meta(col)
                cm.cardinality = column_cardinality[col]
            log.info(_("Column cardinality: end"))

        return True

    def _run_record_validation(self, tablemeta, validation_rules,
                               skip_record_validation):
        log.info(_("Record validation: start"))
        if skip_record_validation:
            log.info(_("Record validation: skipping"))
            return
        if not validation_rules:
            log.info(_("Record validation: no validation rule"))
            return

        validation = self.run_record_validation(tablemeta.schema_name,
                                                tablemeta.table_name,
                                                validation_rules)
        assert isinstance(validation, dict)

        for col in tablemeta.column_names:
            if validation and col in validation:
                cm = tablemeta.get_column_meta(col)
                cm.validation = validation[col]
        log.info(_("Record validation: end"))
        return True

    def run_postscan_validation(self, table_data, validation_rules):
        if not validation_rules:
            return table_data

        v = DbProfilerValidator.DbProfilerValidator(table_data['schema_name'],
                                                    table_data['table_name'],
                                                    self, validation_rules)

        log.info(_("Column statistics validation: start"))
        validated1, failed1 = v.validate_table(table_data)
        log.info(_("Column statistics validation: end (%d)") % validated1)
        log.info(_("SQL validation: start"))
        validated2, failed2 = v.validate_sql(self.dbdriver)
        log.info(_("SQL validation: end (%d)") % validated2)

        v.update_table_data(table_data)
        return table_data

    def _profile_data_types(self, tm):
        log.info(_("Data types: start"))
        data_types = self.get_column_datatypes(tm.schema_name, tm.table_name)
        if len(data_types) == 0:
            log.error(_("Could not get data types."))
            raise InternalError(_("Could not get column data types at all."))
        for col in tm.columns:
            col.datatype = data_types[col.name]
        log.info(_("Data types: end"))

    def _profile_row_count(self, tm):
        if not self.profile_row_count_enabled:
            return

        log.info(_("Row count: start"))
        try:
            rows = self.get_row_count(tm.schema_name, tm.table_name)
        except QueryTimeout as e:
            log.info(_("Row count: Timeout caught. Using the database "
                       "statistics."))
            rows = self.get_row_count(tm.schema_name, tm.table_name,
                                      use_statistics=True)
            # Once timeout occured, column profiling should be skipped
            # in order to avoid heavy loads.
            self.skip_column_profiling = True

        tm.row_count = rows
        log.info(_("Row count: end (%s)") %
                 "{:,d}".format(tm.row_count))

    def _profile_sample_rows(self, tm):
        if not self.profile_sample_rows:
            log.info(_("Sample rows: skipping"))
            return

        log.info(_("Sample rows: start"))
        tm.sample_rows = self.get_sample_rows(tm.schema_name,
                                              tm.table_name)
        log.info(_("Sample rows: end"))

    def _build_tablemeta(self, schema_name, table_name):
        # table meta
        tablemeta = TableMeta(self.dbname, schema_name, table_name)
        tablemeta.timestamp = datetime.now()
        tablemeta.column_names = self.get_column_names(schema_name, table_name)
        # column meta
        columnmeta = {}
        for col in tablemeta.column_names:
            columnmeta[col] = TableColumnMeta(unicode(col))
            # Add all column meta data to the table meta.
            tablemeta.columns.append(columnmeta[col])
        return tablemeta

    def run(self, schema_name=None, table_name=None,
            skip_record_validation=False, validation_rules=None,
            timeout=None):
        assert schema_name and table_name

        # set query timeout
        self.timeout = timeout

        # column profiling requires table profiling
        if self.skip_table_profiling:
            self.skip_column_profiling = True

        log.info(_("Profiling %s.%s: start") % (schema_name, table_name))
        if isinstance(validation_rules, list):
            log.info(_("%d validation rule(s)") % len(validation_rules))

        # build table and column meta data.
        tablemeta = self._build_tablemeta(schema_name, table_name)

        # data types
        self._profile_data_types(tablemeta)

        # sample rows
        self._profile_sample_rows(tablemeta)

        # continue to profile table?
        if self.skip_table_profiling:
            log.info(_("Skipping table and column profiling."))
            log.info(_("Profiling %s.%s: end") % (schema_name, table_name))
            return tablemeta.makedic()

        # number of rows
        self._profile_row_count(tablemeta)

        # continue to profile columns?
        if self.skip_column_profiling:
            log.info(_("Skipping column profiling."))
            log.info(_("Profiling %s.%s: end") % (schema_name, table_name))
            return tablemeta.makedic()

        # exceeded the threshold.
        if tablemeta.row_count > self.column_profiling_threshold:
            log.info((_("Skipping column profiling because "
                        "the table has more than %s rows") %
                      ("{:,d}".format(self.column_profiling_threshold))))
            log.info(_("Profiling %s.%s: end") % (schema_name, table_name))
            return tablemeta.makedic()

        # column profiling and validation
        self.run_column_profiling(tablemeta)
        self._run_record_validation(tablemeta, validation_rules,
                                    skip_record_validation)
        table_data = tablemeta.makedic()
        table_data = self.run_postscan_validation(table_data,
                                                  validation_rules)
        log.info(_("Profiling %s.%s: end") % (schema_name, table_name))

        return table_data
