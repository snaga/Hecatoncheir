#!/usr/bin/env python2.7
# -*- coding: utf-8 -*-

import os
import sys
import unittest
sys.path.append('..')

from hecatoncheir import logger as log

class TestLogger(unittest.TestCase):
    def setUp(self):
        global log

    def test_logger_001(self):
        self.assertIsNotNone(log)

    def test_trace_001(self):
        log.trace(u"trace")

        log.trace_enabled = True
        log.trace(u"trace")

        log.trace(u"日本語unicode")
        log.trace("日本語str")
        log.trace({"foo": "bar"})
        log.trace({"日本語foo": "日本語bar"})

    def test_debug_001(self):
        log.debug_enabled = True
        log.debug(u"debug デバッグ")
        log.debug("debug デバッグ")

    def test_info_001(self):
        log.info(u"info インフォ")
        log.info("info インフォ")

    def test_warning_001(self):
        log.warning(u"warn 警告")
        log.warning("warn 警告")

    def test_error_001(self):
        log.error(u"error エラー")
        log.error("error エラー")

if __name__ == '__main__':
    unittest.main()
