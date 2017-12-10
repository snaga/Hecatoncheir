#!/usr/bin/env python2.7
# -*- coding: utf-8 -*-

import getopt
import os
import re
import sys

import DbProfilerRepository
import logger as log
from msgutil import gettext as _


def verify_column(col):
    assert 'validation' in col
    valid = 0
    invalid = 0
    for v in col['validation']:
        if v['invalid_count'] > 0:
            invalid += 1
        else:
            valid += 1
    return (valid, invalid)


def verify_table(tab):
    valid = 0
    invalid = 0
    for c in tab['columns']:
        v, i = verify_column(c)
        valid += v
        invalid += i
    return (valid, invalid)


class DbProfilerVerify():
    repofile = None

    def __init__(self, repofile, debug=False, verbose=False):
        self.repofile = repofile
        self.verbose = verbose

        log.debug_enabled = debug

    def verify_msg(self, t, v, i):
        if i > 0:
            return _("%s.%s: %d invalid (%d/%d)") % (t[1], t[2], i, i, v+i)
        if v > 0:
            return _("%s.%s: All valid (%d/%d)") % (t[1], t[2], v, v+i)
        return _("%s.%s: No validation") % (t[1], t[2])

    def verify(self, table_list=None):
        repo = DbProfilerRepository.DbProfilerRepository(self.repofile)
        repo.open()

        log.info(_("Verifying the validation results."))

        if not table_list:
            table_list = [(x.database_name, x.schema_name, x.table_name)
                          for x in Table2.find()]
        valid = 0
        invalid = 0
        for t in table_list:
            table = repo.get_table(t[0], t[1], t[2])
            if not table:
                log.error(_("%s.%s not found.") % (t[1], t[2]))
                continue
            v, i = verify_table(table)
            if self.verbose:
                log.info(self.verify_msg(t, v, i))
            valid += v
            invalid += i

        if invalid == 0:
            log.info(_("No invalid results: %d/%d") % (invalid, valid+invalid))
        else:
            log.info(_("Invalid results: %d/%d") % (invalid, valid+invalid))

        repo.close()
        return (True if invalid > 0 else False)
