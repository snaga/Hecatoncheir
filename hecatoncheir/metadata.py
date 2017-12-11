#!/usr/bin/env python2.7
# -*- coding: utf-8 -*-

import copy
import json
import sys
from datetime import datetime

import dateutil.parser

from hecatoncheir import logger as log
from hecatoncheir.msgutil import DbProfilerJSONEncoder
from hecatoncheir.msgutil import gettext as _


class TableColumnMeta:
    def __init__(self, name):
        if not name:
            raise ValueError("Invalid column name: '%s'" % name)
        self.name = name
        self.name_nls = None
        self.data_type = []
        self.nulls = None
        self.min = None
        self.max = None
        self.most_freq_vals = []
        self.least_freq_vals = []
        self.cardinality = None
        self.validation = []
        self.comment = None
        self.__assert()

        # FIXME:
        #   Backward compatibility stuff.
        #   Needs to be removed in the future.
        self.most_freq_values = []
        self.least_freq_values = []
        self.datatype = []

    def __assert(self):
        assert isinstance(self.name, unicode)
        assert isinstance(self.name_nls, unicode) or self.name_nls is None
        assert isinstance(self.data_type, list) or self.data_type is None
        assert isinstance(self.nulls, long) or self.nulls is None
        assert isinstance(self.min, unicode) or self.min is None
        assert isinstance(self.max, unicode) or self.max is None
        assert (isinstance(self.most_freq_vals, list) or
                self.most_freq_vals is None)
        assert (isinstance(self.least_freq_vals, list) or
                self.least_freq_vals is None)
        assert isinstance(self.cardinality, int) or self.cardinality is None
        assert isinstance(self.validation, list) or self.validation is None
        assert isinstance(self.comment, unicode) or self.comment is None

    def makedic(self):
        self.__assert()
        d = copy.deepcopy(self.__dict__)

        # FIXME:
        #   Backward compatibility stuff.
        #   Needs to be removed in the future.
        d['column_name'] = d['name']
        d['column_name_nls'] = d['name_nls']
        d['data_type'] = self.datatype
        del d['name']
        del d['name_nls']
        del d['datatype']

        d['most_freq_vals'] = []
        for v in self.most_freq_values:
            d['most_freq_vals'].append({'value': v[0], 'freq': v[1]})
        del d['most_freq_values']

        d['least_freq_vals'] = []
        for v in self.least_freq_values:
            d['least_freq_vals'].append({'value': v[0], 'freq': v[1]})
        del d['least_freq_values']

        return d

    def to_json(self):
        return json.dumps(self.makedic(), cls=DbProfilerJSONEncoder)

    def from_json(self, j):
        dic = json.loads(j)
        self.name = dic['column_name']
        self.name_nls = dic['column_name_nls']
        self.datatype = dic['data_type']
        self.nulls = long(dic['nulls'])
        self.min = dic['min']
        self.max = dic['max']
        for v in dic['most_freq_vals']:
            self.most_freq_values.append([v['value'], v['freq']])
        for v in dic['least_freq_vals']:
            self.least_freq_values.append([v['value'], v['freq']])
        self.cardinality = dic['cardinality']
        self.validation = dic['validation']
        self.comment = dic['comment']

    def __repr__(self):
        return self.to_json()


class TableMeta:
    def __init__(self, database_name, schema_name, table_name):
        if not database_name:
            raise ValueError("Invalid database name: '%s'" % database_name)
        if not schema_name:
            raise ValueError("Invalid schema name: '%s'" % schema_name)
        if not table_name:
            raise ValueError("Invalid table name: '%s'" % table_name)

        self.database_name = database_name
        self.schema_name = schema_name
        self.table_name = table_name
        self.table_name_nls = None
        self.timestamp = None
        self.row_count = None
        self.column_names = None
        self.columns = []
        self.comment = None
        self.sample_rows = None
        self.__assert()

    def __assert(self):
        assert isinstance(self.database_name, unicode)
        assert isinstance(self.schema_name, unicode)
        assert isinstance(self.table_name, unicode)
        assert (isinstance(self.table_name_nls, unicode) or
                self.table_name_nls is None)
        assert isinstance(self.timestamp, datetime) or self.timestamp is None
        assert isinstance(self.row_count, long) or self.row_count is None
        assert isinstance(self.column_names, list) or self.column_names is None
        assert isinstance(self.columns, list) or self.columns is None
        assert isinstance(self.comment, unicode) or self.comment is None
        assert isinstance(self.sample_rows, list) or self.sample_rows is None

    def get_column_meta(self, column_name):
        for c in self.columns:
            if c.name == column_name:
                return c
        raise ValueError(_(u'Column "%s" not found on table "%s.%s".') %
                         (column_name, self.schema_name, self.table_name))

    def makedic(self):
        self.__assert()
        d = copy.deepcopy(self.__dict__)

        # FIXME:
        #   Backward compatibility stuff.
        #   Needs to be removed in the future.
        d['columns'] = []
        for c in self.columns:
            d['columns'].append(c.makedic())
        del d['column_names']

        return d

    def from_json(self, j):
        dic = json.loads(j)
        self.table_name_nls = dic['table_name_nls']
        self.timestamp = dateutil.parser.parse(dic['timestamp'])
        self.row_count = long(dic['row_count'])
        for c in dic['columns']:
            cm = TableColumnMeta(c['column_name'])
            cm.from_json(json.dumps(c))
            self.columns.append(cm)
        self.comment = dic['comment']
        self.sample_rows = json.loads(dic.get('sample_rows', 'null'))

    def to_json(self):
        return json.dumps(self.makedic(), cls=DbProfilerJSONEncoder, indent=2)

    def __repr__(self):
        return self.to_json()
