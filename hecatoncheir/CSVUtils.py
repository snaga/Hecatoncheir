#!/usr/bin/env python2.7
# -*- coding: utf-8 -*-

import csv

from logger import to_unicode


def csv2list(s):
    assert isinstance(s, unicode)
    for l in csv.reader([s.encode('utf-8')]):
        v = [x.decode('utf-8') for x in l]
    return v


def csvquote(s):
    do_quote = False
    if s.find('"') >= 0:
        s = s.replace(u'"', u'""')
        do_quote = True
    if s.find(',') >= 0 or s.find('\n') >= 0 or s.find('\r') >= 0:
        do_quote = True
    if do_quote:
        s = '"' + s + '"'
    return s


def list2csv(listitem):
    csv = None
    for i in listitem:
        if i is None:
            i = u''
        if isinstance(i, unicode) is False:
            i = unicode(i)
        i = csvquote(i)
        if csv is None:
            csv = i
        else:
            csv = csv + "," + i
    assert isinstance(csv, unicode)
    return csv


class CSVReader():
    def __init__(self, csvfile):
        """Constructor

        Args:
            csvfile (str): csv file name
        """
        self.csvfile = csvfile
        self.header = None

        self.reader = csv.reader(open(self.csvfile, 'rb'),
                                 delimiter=',', quotechar='"')

    def check_header(self, required_columns):
        """Checking CSV header

        Args:
            requied_columns (list): a list of required column names:
                                    ['c1','c2','c3',...]
        Returns:
            True if the csv file has the exact same header, otherwise False.
        """
        r = self.reader.next()
        matched = len([(x, y) for x in required_columns
                       for y in r if x.upper() == y.upper()])
        if matched != len(required_columns):
            return False
        self.header = [x.upper() for x in r]
        return True

    def readline(self):
        """Read a csv line and returns a list of values.

        Returns:
            list: a list of values ['val1','val2','val3',...]
        """
        for r in self.reader:
            r2 = [to_unicode(x) for x in r]
            yield r2
        return

    def readline_as_dict(self, use_lower=False):
        """Read a csv line and returns a dictionary.

        Returns:
            dict: a dictionary of 'column-value' pairs
                  {'c1':'val1', 'c2':'val2', 'c3':'val3',...}
        """
        for r in self.reader:
            r2 = [to_unicode(x) for x in r]
            r3 = {}
            for i, k in enumerate(self.header):
                v = None if len(r2) <= i else r2[i]
                if use_lower:
                    r3[k.lower()] = v
                else:
                    r3[k.upper()] = v
            yield r3
        return
