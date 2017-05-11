#!/usr/bin/env python2.7
# -*- coding: utf-8 -*-

import re

from hecatoncheir.DbProfilerException import ValidationError
import RecordValidator
from hecatoncheir.msgutil import gettext as _


class EvalValidator(RecordValidator.RecordValidator):
    """RecordValidator for the Eval rule

    v = EvalValidator('foo',rule=['COL1', u'{COL1} > 100'])
    assert v.validate(['COL1','COL2','COL3'], [101,101,101]) == True

    v = EvalValidator('foo',rule=['COL1,COL2', u'{COL1} > {COL2}'])
    assert v.validate(['COL1','COL2','COL3'], [101,101,101]) == False
    """
    _idx = None

    def __init__(self, label, rule):
        RecordValidator.RecordValidator.__init__(self, label, rule)

    def validate(self, column_names, record):
        """Validate one record

        Args:
            columns (list): a list of column names associated with each field.
            record (list): a record, consisting of list of fields.

        Returns:
            True on evaluation succeeded, otherwise False.
        """
        # self.rule: [ column name, format ]
        assert len(self.rule) == 2
        assert len(column_names) == len(record)

        self.statistics[0] += 1

        kv = {}
        for k, v in zip(column_names, record):
            # if the value is not a number, it needs to be quoted with "'".
            try:
                float(v)
                kv[k] = v
            except ValueError as e:
                kv[k] = "'" + v + "'"
        try:
            s = self.rule[1].format(**kv)
        except KeyError as e:
            self.statistics[1] += 1
            raise ValidationError(
                _("Parameter error: ") + "`%s'" % kv,
                self.rule)

        try:
            if eval(s) is False:
                self.statistics[1] += 1
                return False
        except SyntaxError:
            self.statistics[1] += 1
            raise ValidationError(
                _("Syntax error: ") + "`%s'" % s,
                self.rule)

        return True
