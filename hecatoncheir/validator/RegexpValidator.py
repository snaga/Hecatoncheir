#!/usr/bin/env python2.7
# -*- coding: utf-8 -*-

import re

import RecordValidator


class RegexpValidator(RecordValidator.RecordValidator):
    """RecordValidator for Regular Expression

    v = RegexpValidator('foo', rule=['COL1', '^aaa'])
    assert v.validate(['COL1','COL2','COL3'], ['aaa','bbb','ccc']) == True
    """
    def __init__(self, label, rule):
        RecordValidator.RecordValidator.__init__(self, label, rule)

    def validate(self, column_names, record):
        """Validate one record

        Args:
            columns (list): a list of column names associated with each field.
            record (list): a record, consisting of record values.

        Returns:
            True on regexp matches, otherwise False.
        """
        # self.rule: [ column_name, regexp ]
        assert len(self.rule) == 2
        assert len(column_names) == len(record)

        idx = column_names.index(self.rule[0])
        assert idx is not None

        # if the value is an int or a float, it needs to be converted
        # into string before evaluating.
        if re.search(self.rule[1], unicode(record[idx])) is not None:
            self.statistics[0] += 1
            return True
        self.statistics[0] += 1
        self.statistics[1] += 1  # fail count
        return False
