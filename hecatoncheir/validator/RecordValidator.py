#!/usr/bin/env python2.7
# -*- coding: utf-8 -*-

from abc import ABCMeta, abstractmethod


class RecordValidator():
    """Base class of RecordValidator

    Args:
        label (str): validator label
        rule (list): [column_name(s), param, param, ...]
    """
    label = None
    column_names = None
    rule = None
    statistics = None

    def __init__(self, label, rule):
        self.label = label
        self.rule = rule
        self.column_names = rule[0].replace(' ', '').split(',')
        self.statistics = [0, 0]

    @abstractmethod
    def validate(self, column_names, record):
        raise NotImplementedError
