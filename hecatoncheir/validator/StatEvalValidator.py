#!/usr/bin/env python2.7
# -*- coding: utf-8 -*-

from hecatoncheir import DbProfilerException
import StatisticsValidator
from hecatoncheir.msgutil import gettext as _


class StatEvalValidator(StatisticsValidator.StatisticsValidator):
    """Validator for the min/max value

    v = StatEvalValidator('foo', [u'COL1', '{min} > 100'])
    s = {'row_count': 100,
        'columns': [{
            'column_name': 'COL1',
            'min': 101,
            'max': 1000,
            'cardinality': 10}]}
    assert v.validate(s) == True
    """
    def __init__(self, label, rule):
        StatisticsValidator.StatisticsValidator.__init__(self, label, rule)

    def validate(self, stats):
        """Validate a min/max rule based the column statistics

        Args:
            stats (dict): a table statistics. see Data_Structure.txt
                          for more info.

        Returns:
            True if the expression is true, otherwise False.
        """
        # rule: [ column_name, expression ]
        assert len(self.rule) == 2
        assert 'columns' in stats

        c = None
        for col in stats['columns']:
            if col['column_name'] == self.rule[0]:
                c = col
                break

        if c is None:
            raise DbProfilerException.ValidationError(
                _("Column `%s' not found. Check your validation rule again.") %
                self.rule[0],
                self.rule)
        assert 'row_count' in stats
        assert ('nulls' in c and 'min' in c and 'max' in c and
                'cardinality' in c)
        kv = {'rows': stats['row_count'], 'nulls': c['nulls'], 'min': c['min'],
              'max': c['max'], 'cardinality': c['cardinality']}

        self.statistics[0] += 1

        try:
            s = self.rule[1].format(**kv)
        except KeyError as e:
            self.statistics[1] += 1
            raise DbProfilerException.ValidationError(
                _("Parameter error: ") + "`%s'" % kv,
                self.rule)

        try:
            if eval(s) is False:
                self.statistics[1] += 1
                return False
        except SyntaxError:
            self.statistics[1] += 1
            raise DbProfilerException.ValidationError(
                _("Syntax error: ") + "`%s'" % s,
                self.rule)

        return True
