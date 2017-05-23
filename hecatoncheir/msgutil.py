#!/usr/bin/env python2.7
# -*- coding: utf-8 -*-

import datetime
import decimal
import gettext
import os
import json

import hecatoncheir

lang = os.environ.get('LANGUAGE')
if not lang:
    lang = os.environ.get('LC_ALL')
if not lang:
    lang = os.environ.get('LC_MESSAGES')
if not lang:
    lang = os.environ.get('LANG')

gettext = gettext.translation('hecatoncheir',
                              hecatoncheir.__path__[0] + '/locale',
                              [lang], fallback=True).ugettext


class DbProfilerJSONEncoder(json.JSONEncoder):
    def default(self, o):
        try:
            iterable = iter(o)
        except TypeError:
            pass
        else:
            return list(iterable)

        if isinstance(o, datetime.datetime):
            return o.isoformat()
        elif isinstance(o, datetime.date):
            return o.isoformat()
        elif isinstance(o, decimal.Decimal):
            return (str(o) for o in [o])
        return super(DbProfilerJSONEncoder, self).default(o)
