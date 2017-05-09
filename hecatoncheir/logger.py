#!/usr/bin/env python2.7
# -*- coding: utf-8 -*-

from datetime import datetime, date
from decimal import Decimal
import codecs
import locale
import os
import json
import sys


def to_unicode(s):
    if isinstance(s, unicode):
        return s
    if not isinstance(s, str):
        s = str(s)

    encodings = ('utf_8', 'euc_jp', 'euc_jis_2004', 'euc_jisx0213',
                 'shift_jis', 'shift_jis_2004', 'shift_jisx0213',
                 'iso2022jp', 'iso2022_jp_1', 'iso2022_jp_2', 'iso2022_jp_3',
                 'iso2022_jp_ext', 'latin_1', 'ascii')
    for enc in encodings:
        try:
            s2 = s.decode(enc)
            if isinstance(s2, unicode):
                return s2
        except:
            pass
    raise TypeError('Could not determine encoding of the string.')


def str2unicode(s):
    if isinstance(s, str):
        to_unicode(s)
    return s

trace_enabled = None
debug_enabled = False
setup_done = False


def setup():
    global setup_done
    if setup_done:
        return

    if os.environ.get('PYTHONIOENCODING') is None:
        msg = (u"Please specify the environment variable "
               u"PYTHONIOENCODING first.")
        print >> sys.stderr, u"[%s] WARNING: %s" % (ts(), to_unicode(msg))

    global trace_enabled
    if trace_enabled is None:
        if os.environ.get('__DBPROF_TRACE') == '1':
            trace_enabled = True

    setup_done = True


def ts():
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S")


def trace(msg):
    if not trace_enabled:
        return
    setup()
    print >> sys.stderr, u"[%s] TRACE: %s" % (ts(), to_unicode(msg))


def debug(msg):
    if not debug_enabled:
        return
    setup()
    print >> sys.stderr, u"[%s] DEBUG: %s" % (ts(), to_unicode(msg))


def info(msg):
    setup()
    print >> sys.stderr, u"[%s] INFO: %s" % (ts(), to_unicode(msg))


def info_cont(msg):
    setup()
    print >> sys.stderr, u"[%s] INFO: %s" % (ts(), to_unicode(msg)),


def info_end(msg):
    setup()
    print >> sys.stderr, msg


def warning(msg):
    setup()
    print >> sys.stderr, u"[%s] WARNING: %s" % (ts(), to_unicode(msg))


def error(msg, detail=None, query=None):
    setup()
    print >> sys.stderr, u"[%s] ERROR: %s" % (ts(), to_unicode(msg))
    if detail is not None:
        if isinstance(detail, str) or isinstance(detail, unicode):
            detail = detail.rstrip()
        print >> sys.stderr, u"[%s] DETAIL: %s" % (ts(), to_unicode(detail))
    if query is not None:
        print >> sys.stderr, u"[%s] QUERY: %s" % (ts(), to_unicode(query))


def dump_datetime_default(o):
    if isinstance(o, datetime):
        return o.isoformat()
    if isinstance(o, date):
        return o.isoformat()
    if isinstance(o, Decimal):
        return float(o)
    raise TypeError(repr(o) + " is not JSON serializable")


def dump(msg, obj):
    setup()
    tmp = "----------------------- %s -------------------------" % msg
    print >> sys.stderr, u"[%s] DUMP: %s" % (ts(), tmp)
    for line in json.dumps(obj, indent=2,
                           default=dump_datetime_default).split('\n'):
        print >> sys.stderr, u"[%s] DUMP: %s" % (ts(), to_unicode(line))
    tmp = "----------------------- /%s ------------------------" % msg
    print >> sys.stderr, u"[%s] DUMP: %s" % (ts(), tmp)


if __name__ == "__main__":
    info(u"INFOだふー")
    info(u"INFOだふー")
    info(u"INFOだふー")
    trace(u"TRACEだふー")
    trace_enabled = True
    trace(u"TRACEだふー2")
    debug(u"DEBUGだふー1")
    debug_enabled = True
    debug(u"DEBUGだふー2")
    debug(u"DEBUGだふー2 SJIS".encode('sjis'))
