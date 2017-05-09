#!/usr/bin/env python2.7
# -*- coding: utf-8 -*-

from copy import deepcopy


class QueryResult():
    query = None
    column_names = None
    resultset = None

    def __init__(self, query):
        self.query = query
        self.column_names = []
        self.resultset = []

    def __repr__(self):
        return u'QueryResult:' + repr({'query': self.query,
                                       'column': self.column_names,
                                       'result': self.resultset})
