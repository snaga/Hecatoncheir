#!/usr/bin/env python2.7
# -*- coding: utf-8 -*-


class DriverError(Exception):
    source = None

    def __init__(self, value, source=None):
        self.value = value
        self.source = source

    def __str__(self):
        return self.value


class QueryError(Exception):
    source = None
    query = None

    def __init__(self, value, query, source=None):
        self.value = value
        self.query = query
        self.source = source

    def __str__(self):
        return self.value


class QueryTimeout(Exception):
    source = None
    query = None

    def __init__(self, value, query, source=None):
        self.value = value
        self.query = query
        self.source = source

    def __str__(self):
        return self.value


class ValidationError(Exception):
    source = None

    def __init__(self, value, rule, params=None, source=None):
        self.value = value
        self.rule = rule
        self.params = params
        self.source = source

    def __str__(self):
        return self.value


class ProfilingError(Exception):
    source = None
    query = None

    def __init__(self, value, target=None, query=None, source=None):
        assert target in ['table', 'column']

        self.value = value
        self.target = target
        self.query = query
        self.source = source

    def __str__(self):
        return self.value


class InternalError(Exception):
    source = None
    query = None

    def __init__(self, value, query=None, source=None):
        self.value = value
        self.query = query
        self.source = source

    def __str__(self):
        return self.value


class DbProfilerException(Exception):
    source = None

    def __init__(self, value, source=None):
        self.value = value
        self.source = source

    def __str__(self):
        return self.value
