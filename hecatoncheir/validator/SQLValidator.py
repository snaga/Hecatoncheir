#!/usr/bin/env python2.7
# -*- coding: utf-8 -*-

from hecatoncheir import logger as log
from hecatoncheir.exception import (DriverError, QueryError, ValidationError)
from hecatoncheir.msgutil import gettext as _


# -------------------------------------------
# eval validator, used by sql validator
# -------------------------------------------
def validate_eval(kv, p):
    """Validate a format with column values

    Args:
      kv (dict): column name as key, and value
      p (str): format to be evaluated

    Returns:
      bool: True on success, otherwise False
    """
    try:
        s = p.format(**kv)
    except KeyError as e:
        raise ValidationError(
            _("Parameter error: ") + "`%s' %s" % (p, kv),
            rule=p, params=kv)

    try:
        return eval(s)
    except SyntaxError as e:
        raise ValidationError(
            _("Syntax error: ") + "`%s'" % s,
            rule=p, params=kv)


class SQLValidator():
    """Base class of SQLValidator
    """
    label = None
    rule = None
    queryresult = None
    statistics = None

    def __init__(self, label, rule):
        """Constructor

        Args:
            label (str): label
            rule (list): [column_name(s), query, expr]
        """
        assert isinstance(rule, list)

        self.label = label
        self.rule = rule
        self.column_names = rule[0].replace(' ', '').split(',')
        self.query = rule[1]
        self.statistics = [0, 0]

    def validate(self, dbdriver):
        if dbdriver is None:
            raise DriverError(
                _("Database driver not found."))

        self.statistics[0] += 1
        try:
            res = dbdriver.q2rs(self.query)
        except QueryError as e:
            raise ValidationError(
                _("SQL error: ") + "`%s'" % self.query, self.label,
                source=e)

        assert res
        assert len(res.column_names) == len(res.resultset[0])
        assert len(res.resultset) == 1

        kv = {}
        for k, v in zip(res.column_names, res.resultset[0]):
            kv[k] = v

        res = validate_eval(kv, self.rule[2])
        if not res:
            self.statistics[1] += 1
        return res

    def __repr__(self):
        return u'SQLValidator:' + repr({'label': self.label, 'rule': self.rule,
                                        'queryresult': self.queryresult})
