#!/usr/bin/env python2.7
# -*- coding: utf-8 -*-

from abc import ABCMeta, abstractmethod


class DbDriverBase:
    __metaclass__ = ABCMeta

    @abstractmethod
    def __init__(self, connstr, dbuser, dbpass):
        raise NotImplementedError

    @abstractmethod
    def connect(self):
        raise NotImplementedError

    @abstractmethod
    def query_to_resultset(self, label, query, max_rows=10000, timeout=None):
        raise NotImplementedError

    @abstractmethod
    def q2rs(self, label, query, max_rows=10000, timeout=None):
        raise NotImplementedError

    @abstractmethod
    def disconnect(self):
        raise NotImplementedError
