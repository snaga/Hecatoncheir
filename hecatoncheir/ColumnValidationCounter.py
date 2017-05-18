#!/usr/bin/env python2.7
# -*- coding: utf-8 -*-

import logger as log
from exception import InternalError


class ColumnValidationCounter():
    def __init__(self):
        self._column_counter = {}   # dict[column_name][label] = invalid_count

    def add(self, column_name, label):
        cols = column_name.replace(' ', '').split(",")
        if len(cols) >= 2:
            for col in cols:
                self.add(col, label)
            return

        if column_name not in self._column_counter:
            self._column_counter[column_name] = {}
        if label not in self._column_counter[column_name]:
            self._column_counter[column_name][label] = 0
        log.trace("ColumnValidationCounter#add(): %s %s done." %
                  (column_name, label))

    def incr(self, column_name, label):
        cols = column_name.replace(' ', '').split(",")
        if len(cols) >= 2:
            for col in cols:
                self.incr(col, label)
            return

        if column_name not in self._column_counter:
            m = ("ColumnValidationCounter#incr() key error: %s" %
                 column_name)
            raise InternalError(m)
        if label not in self._column_counter[column_name]:
            m = ("ColumnValidationCounter#incr() key error: %s, %s" %
                 (column_name, label))
            raise InternalError(m)
        self._column_counter[column_name][label] += 1

        m = ("ColumnValidationCounter#incr(): %s,%s done." %
             (column_name, label))
        log.trace(m)
        return self._column_counter[column_name][label]

    def get(self, column_name, label=None):
        """Get the counter data by column name (and label)

        Args:
            column_name (str): column name
            label (str): label string of validation rule

        Returns:
            int: when both column name & label are specified.
            dict: when only column name specified: {'label': count, ...}
        """
        if column_name not in self._column_counter and label is None:
            return None

        if column_name not in self._column_counter:
            m = "ColumnValidationCounter#get() key error: %s" % column_name
            raise InternalError(m)

        if label is None:
            return self._column_counter[column_name]

        if label not in self._column_counter[column_name]:
            m = ("ColumnValidationCounter#get() key error: %s, %s" %
                 (column_name, label))
            raise InternalError(m)

        return self._column_counter[column_name][label]
