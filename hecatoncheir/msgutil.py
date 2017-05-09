#!/usr/bin/env python2.7
# -*- coding: utf-8 -*-

import gettext
import os

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
