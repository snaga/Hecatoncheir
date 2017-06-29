#!/usr/bin/env python2.7
# -*- coding: utf-8 -*-

import copy
import hashlib
import json
import os
import re
import sys

import CSVUtils
import logger as log
from CSVUtils import list2csv
from exception import DriverError, InternalError, ValidationError
from logger import to_unicode
from msgutil import gettext as _
from validator.RegexpValidator import RegexpValidator
from validator.EvalValidator import EvalValidator
from validator.StatEvalValidator import StatEvalValidator
from validator.SQLValidator import SQLValidator


class ValidationException(Exception):
    def __init__(self, value):
        self.value = value

    def __str__(self):
        return str(self.value)


# ------------------------------------------
# DbProfilerValidator
# ------------------------------------------
class DbProfilerValidator():
    schema_name = None
    table_name = None
    table_data = None

    def __init__(self, schema_name, table_name, caller=None,
                 validation_rules=None):
        self.schema_name = schema_name
        self.table_name = table_name
        self.caller = caller

        self.record_validators = {}
        self.statistics_validators = {}
        self.sql_validators = {}

        self.descriptions = {}

        assert validation_rules is None or isinstance(validation_rules, list)
        num_rules = 0
        if validation_rules:
            for r in validation_rules:
                log.trace("DbProfilerValidator: " + str(r))
                assert len(r) == 9
                if self.add_rule(r[0], r[1], r[2], r[3], r[4], r[5], r[6],
                                 r[7], r[8]):
                    num_rules += 1
        log.debug(
            u"DbProfilerValidator: initialized with %d validation rules" %
            num_rules)

    # -----------------------------
    # on-the-fly validation
    # -----------------------------
    def add_rule_regexp(self, label, column_name, regexpr):
        # new record validator
        v = RegexpValidator(label, rule=[column_name, regexpr])
        self.record_validators[label] = v

    def add_rule_eval(self, label, column_name, format):
        # new record validator
        v = EvalValidator(label, rule=[column_name, format])
        self.record_validators[label] = v

    # -----------------------------
    # validation with column statistics
    # -----------------------------
    def add_rule_columnstat(self, label, column_name, format):
        # new statistics validator
        v = StatEvalValidator(label, rule=[column_name, format])
        self.statistics_validators[label] = v

    # -----------------------------
    # SQL validation
    # -----------------------------
    def add_rule_sql(self, label, column_name, query, expr):
        v = SQLValidator(label, rule=[column_name, query, expr])
        self.sql_validators[label] = v

    # -----------------------------
    # Add a validation rule
    # -----------------------------
    def add_rule(self, id_, database_name, schema_name, table_name,
                 column_name, description, rule, param, param2=None):
        assert isinstance(id_, int)

        if self.schema_name != schema_name or self.table_name != table_name:
            return False

        label = id_
        log.debug("add_rule: label = %s" % label)

        assert param
        if rule == 'regexp':
            self.add_rule_regexp(label, column_name, param)
        elif rule == 'eval':
            self.add_rule_eval(label, column_name, param)
        elif rule == 'columnstat':
            self.add_rule_columnstat(label, column_name, param)
        elif rule == 'sql':
            assert param2
            self.add_rule_sql(label, column_name, param, param2)
        else:
            raise InternalError(_("Unsupported validation rule: %s") % rule)
        self.descriptions[label] = description

        return True

    def has_validation_rules(self):
        if (len(self.record_validators) + len(self.statistics_validators)) > 0:
            return True
        return False

    def validate_record(self, column_names, column_values):
        validated_count = 0
        failed_count = 0

        assert len(column_names) == len(column_values)

        # new record validator
        for label in self.record_validators:
            validator = self.record_validators[label]
            validated_count += 1
            try:
                if validator.validate(column_names, column_values) is False:
                    log.trace("VALIDATION FAILED: %s %s %s %s" %
                              (validator.label, unicode(validator.rule),
                               validator.column_names, unicode(column_values)))
                    failed_count += 1
                else:
                    log.trace("VALIDATION OK: %s %s %s %s" %
                              (validator.label, unicode(validator.rule),
                               validator.column_names, unicode(column_values)))
            except ValidationError as e:
                log.error(u'%s' % e.value)
                log.trace("VALIDATION FAILED: %s %s %s %s" %
                          (validator.label, unicode(validator.rule),
                           validator.column_names, unicode(column_values)))
                failed_count += 1
                continue

        if failed_count > 0:
            return False
        return True

    def has_invalid_results(self, res):
        for r in res:
            if res[r] > 0:
                return True
        return False

    def table_data_get_column_data(self, table_data, column_name):
        if table_data is None or column_name is None:
            return None

        for column_data in table_data['columns']:
            if column_data['column_name'] == column_name:
                return column_data
        return None

    def get_validator_by_label(self, label):
        if self.record_validators.get(label) is not None:
            return self.record_validators[label]
        elif self.statistics_validators.get(label) is not None:
            return self.statistics_validators[label]
        elif self.sql_validators.get(label) is not None:
            return self.sql_validators[label]
        return None

    def _val_to_res(self, validator, desc):
        val = validator.__dict__.copy()
        val['invalid_count'] = validator.statistics[1]
        val['description'] = desc
        return val

    def validator_to_results(self, validator, desc, results):
        for col in validator.column_names:
            # assign this result to every column
            if col not in results:
                results[col] = []
            results[col].append(self._val_to_res(validator, desc))
        return results

    def get_validation_results(self):
        """Get validation results for each column

        Returns:
            dict: a dictionary containing validation results for each column
                  dict[column_name] = [{'invalid_count': int,
                                        'label': str,
                                        'rule': list[str,...],
                                        'column_names': list[str,...],
                                        'description': str},
                                       {'invalid_count': int,
                                        'label': str,
                                        'rule': list[str,...],
                                        'column_names': list[str,...],
                                        'description': str}
                                       ...]
        """
        dd = {}
        for label in self.record_validators:
            validator = self.record_validators[label]
            desc = self.descriptions.get(label, '')
            self.validator_to_results(validator, desc, dd)
        for label in self.statistics_validators:
            validator = self.statistics_validators[label]
            desc = self.descriptions.get(label, '')
            self.validator_to_results(validator, desc, dd)
        for label in self.sql_validators:
            validator = self.sql_validators[label]
            desc = self.descriptions.get(label, '')
            self.validator_to_results(validator, desc, dd)
        return dd

    # post scan (offline) validation
    def validate_table(self, table_data):
        validated_count = 0
        failed_count = 0

        # Run statistics validators.
        for label in self.statistics_validators:
            validator = self.statistics_validators[label]
            log.info(_("Validating column statistics: [%s] %s") %
                     (label, '; '.join(validator.rule)))
            validated_count += 1
            try:
                res = validator.validate(table_data)
            except ValidationError as e:
                log.error(u'%s' % e.value)
                res = False

            if res is False:
                log.trace("VALIDATION FAILED: %s %s %s" %
                          (validator.label, unicode(validator.rule),
                           validator.column_names))
                failed_count += 1
            else:
                log.trace("VALIDATION OK: %s %s %s" %
                          (validator.label, unicode(validator.rule),
                           validator.column_names))

        return (validated_count, failed_count)

    def equals(self, a, b):
        return (a['column_names'] == b['column_names'] and
                a['description'] == b['description'] and
                a['rule'] == b['rule'] and
                a['label'] == b['label'])

    def to_str(self, a):
        return (u'label:%s, column:%s, desc:%s, rule:%s, invalid:%s, '
                u'stats:%s') % (a['label'], a['column_names'],
                                a['description'], a['rule'],
                                a['invalid_count'], a.get('statistics'))

    def update_table_data(self, table_data):
        # Update table data with failed count and column validation results.
        log.trace('update_table_data: start table_data=%s' % table_data)

        res = self.get_validation_results()
        log.trace("update_table_data: res = %s" % res)

        # walk through every column on the table
        for tabcol in table_data['columns']:
            colname = tabcol['column_name']
            log.trace("update_table_data: %s" % colname)

            # Update validation results (statistics and invalid_count)
            # on each column.
            if colname not in res:
                continue   # nothing to update for this column.
            for newone in res[colname]:
                log.trace("newone: %s" % self.to_str(newone))
                copied = False
                for oldone in tabcol['validation']:
                    log.trace("oldone: %s" % self.to_str(oldone))
                    if (self.equals(newone, oldone)):
                        if 'statistics' not in oldone:
                            oldone['statistics'] = [0, 0]
                        if 'statistics' not in newone:
                            newone['statistics'] = [0, 0]
                        oldone['statistics'][0] += newone['statistics'][0]
                        oldone['statistics'][1] += newone['statistics'][1]
                        oldone['invalid_count'] += newone['invalid_count']
                        copied = True
                        log.trace("updated: %s" % self.to_str(oldone))
                if not copied:
                    tabcol['validation'].append(copy.deepcopy(newone))
                    log.trace("appended: %s" % self.to_str(newone))
        log.trace('update_table_data: end table_data=%s' % table_data)

    def validate_sql(self, dbdriver):
        if dbdriver is None:
            raise DriverError(u'Database driver not found.')

        validated_count = 0
        failed_count = 0
        for label in self.sql_validators:
            validator = self.sql_validators[label]
            log.info(_("Validating with SQL: %s") % '; '.join(validator.rule))
            validated_count += 1

            try:
                res = validator.validate(dbdriver)
            except ValidationError as e:
                log.error(_("SQL validation error: %s") %
                          '; '.join(validator.rule),
                          detail=e.source.value if e.source else None)
                failed_count += 1
                continue

            if res is False:
                failed_count += 1

        return (validated_count, failed_count)
