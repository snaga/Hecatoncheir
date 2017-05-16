#!/usr/bin/env python2.7
# -*- coding: utf-8 -*-

from abc import ABCMeta, abstractmethod
import copy
from datetime import datetime
import json
import sys

import dateutil.parser

from DbProfilerFormatter import DbProfilerJSONEncoder
from DbProfilerException import DbProfilerException, InternalError, QueryError
import DbProfilerValidator
import logger as log
from logger import str2unicode as _s2u
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


class TableColumnMeta:
    name = None
    name_nls = None
    datatype = []
    nulls = None
    min = None
    max = None
    most_freq_values = None
    least_freq_values = None
    cardinality = None
    validation = {}
    comment = None

    def __init__(self, schema_name, table_name, column_name=None):
        self.schema_name = schema_name
        self.table_name = table_name
        self.name = column_name
        self.most_freq_values = []
        self.least_freq_values = []

    def makedic(self):
        dic = {}
        dic['column_name'] = self.name
        dic['column_name_nls'] = self.name_nls
        dic['data_type'] = self.datatype
        dic['nulls'] = self.nulls
        dic['min'] = self.min
        dic['max'] = self.max
        dic['most_freq_vals'] = []
        for v in self.most_freq_values:
            dic['most_freq_vals'].append({"value": v[0], "freq": v[1]})
        dic['least_freq_vals'] = []
        for v in self.least_freq_values:
            dic['least_freq_vals'].append({"value": v[0], "freq": v[1]})
        dic['cardinality'] = self.cardinality
        dic['validation'] = self.validation
        dic['comment'] = self.comment
        return dic

    def from_json(self, j):
        dic = json.loads(j)
        self.name = dic['column_name']
        self.name_nls = dic['column_name_nls']
        self.datatype = dic['data_type']
        self.nulls = dic['nulls']
        self.min = dic['min']
        self.max = dic['max']
        for v in dic['most_freq_vals']:
            self.most_freq_values.append([v['value'], v['freq']])
        for v in dic['least_freq_vals']:
            self.least_freq_values.append([v['value'], v['freq']])
        self.cardinality = dic['cardinality']
        self.validation = dic['validation']
        self.comment = dic['comment']

    def to_json(self):
        return json.dumps(self.makedic(), cls=DbProfilerJSONEncoder)

    def __repr__(self):
        return self.to_json()


class TableMeta:
    table_name_nls = None
    timestamp = None
    row_count = None
    column_names = None
    columns = None
    comment = None
    sample_rows = None

    def __init__(self, database_name, schema_name, table_name):
        self.database_name = database_name
        self.schema_name = schema_name
        self.table_name = table_name
        self.columns = []

    def makedic(self):
        dic = {}
        dic['database_name'] = self.database_name
        dic['schema_name'] = self.schema_name
        dic['table_name'] = self.table_name
        dic['table_name_nls'] = self.table_name_nls
        dic['timestamp'] = self.timestamp
        dic['row_count'] = self.row_count
        dic['columns'] = []
        for c in self.columns:
            dic['columns'].append(c.makedic())
        dic['comment'] = self.comment
        dic['sample_rows'] = self.sample_rows
        log.debug('dic: %s' % dic)
        return dic

    def from_json(self, j):
        dic = json.loads(j)
        self.table_name_nls = dic['table_name_nls']
        self.timestamp = dateutil.parser.parse(dic['timestamp'])
        self.row_count = dic['row_count']
        for c in dic['columns']:
            cm = TableColumnMeta(self.schema_name, self.table_name)
            cm.from_json(json.dumps(c))
            self.columns.append(cm)
        self.comment = dic['comment']
        self.sample_rows = json.loads(dic.get('sample_rows', 'null'))

    def to_json(self):
        return json.dumps(self.makedic(), cls=DbProfilerJSONEncoder, indent=2)

    def __repr__(self):
        return self.to_json()


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

    def run_column_profiling(self, schema_name, table_name, tablemeta,
                             columnmeta):
        if self.profile_nulls_enabled is True:
            log.info(_("Number of nulls: start"))
            nulls = self.get_column_nulls(schema_name, table_name)
            for col in tablemeta.column_names:
                columnmeta[col].nulls = nulls[col]
            log.info(_("Number of nulls: end"))

        if self.profile_min_max_enabled is True:
            log.info(_("Min/Max values: start"))
            minmax = self.get_column_min_max(schema_name, table_name)
            for col in tablemeta.column_names:
                columnmeta[col].min = minmax[col][0]
                columnmeta[col].max = minmax[col][1]
            log.info(_("Min/Max values: end"))

        if self.profile_most_freq_values_enabled > 0:
            log.info(_("Most/Least freq values(1/2): start"))
            most_freqs = self.get_column_most_freq_values(schema_name,
                                                          table_name)
            log.info(_("Most/Least freq values(2/2): start"))
            least_freqs = self.get_column_least_freq_values(schema_name,
                                                            table_name)
            for col in tablemeta.column_names:
                columnmeta[col].most_freq_values = most_freqs[col]
                columnmeta[col].least_freq_values = least_freqs[col]
            log.info(_("Most/Least freq values: end"))

        if self.profile_column_cardinality_enabled is True:
            log.info(_("Column cardinality: start"))
            column_cardinality = self.get_column_cardinalities(schema_name,
                                                               table_name)
            for col in tablemeta.column_names:
                columnmeta[col].cardinality = column_cardinality[col]
            log.info(_("Column cardinality: end"))

    def _run_record_validation(self, schema_name, table_name, tablemeta,
                               columnmeta, validation_rules,
                               skip_record_validation):
        log.info(_("Record validation: start"))
        if skip_record_validation:
            log.info(_("Record validation: skipping"))
            return
        if not validation_rules:
            log.info(_("Record validation: no validation rule"))
            return

        validation = self.run_record_validation(schema_name, table_name,
                                                validation_rules)
        assert isinstance(validation, dict)

        for col in tablemeta.column_names:
            if validation and col in validation:
                columnmeta[col].validation = validation[col]
        log.info(_("Record validation: end"))

    def run_postscan_validation(self, schema_name, table_name, tablemeta,
                                columnmeta, table_data, validation_rules):
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

        # table meta
        tablemeta = TableMeta(self.dbname, schema_name, table_name)
        tablemeta.timestamp = datetime.now()
        tablemeta.column_names = self.get_column_names(schema_name, table_name)
        # column meta
        columnmeta = {}
        for col in tablemeta.column_names:
            columnmeta[col] = TableColumnMeta(schema_name, table_name, col)

        log.info(_("Data types: start"))
        data_types = self.get_column_datatypes(schema_name, table_name)
        if len(data_types) == 0:
            log.error(_("Could not get data types."))
            raise InternalError(_("Could not get column data types at all."))
        for col in tablemeta.column_names:
            columnmeta[col].datatype = data_types[col]
        log.info(_("Data types: end"))

        if self.skip_table_profiling:
            log.info(_("Skipping table profiling."))
        elif self.profile_row_count_enabled is True:
            log.info(_("Row count: start"))
            tablemeta.row_count = self.get_row_count(schema_name, table_name)
            log.info(_("Row count: end (%s)") %
                     "{:,d}".format(tablemeta.row_count))

        if self.profile_sample_rows:
            log.info(_("Sample rows: start"))
            tablemeta.sample_rows = self.get_sample_rows(schema_name,
                                                         table_name)
            log.info(_("Sample rows: end"))
        else:
            log.info(_("Sample rows: skipping"))

        if self.skip_column_profiling:
            log.info(_("Skipping column profiling."))
        elif tablemeta.row_count > self.column_profiling_threshold:
            log.info((_("Skipping column profiling because "
                        "the table has more than %s rows") %
                      ("{:,d}".format(self.column_profiling_threshold))))
        else:
            self.run_column_profiling(schema_name, table_name, tablemeta,
                                      columnmeta)
            self._run_record_validation(schema_name, table_name, tablemeta,
                                        columnmeta, validation_rules,
                                        skip_record_validation)

        # Add all column meta data to the table meta.
        for col in tablemeta.column_names:
            tablemeta.columns.append(columnmeta[col])

        table_data = tablemeta.makedic()
        if (self.skip_column_profiling or
                tablemeta.row_count > self.column_profiling_threshold):
            log.info(_("Skipping column statistics validation "
                       "and SQL validation."))
        else:
            table_data = self.run_postscan_validation(schema_name, table_name,
                                                      tablemeta, columnmeta,
                                                      table_data,
                                                      validation_rules)
        log.info(_("Profiling %s.%s: end") % (schema_name, table_name))

        return table_data
