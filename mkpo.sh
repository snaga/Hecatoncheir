#!/bin/sh

set -e

LANG=$1
if [ -z $LANG ]; then
  echo "Usage: $0 <LANG>"
  exit 1
fi

( find . -maxdepth 1 -type f -name 'dm-*'; \
  find hecatoncheir -type f -name '*.py' ) | \
  xargs pygettext.py -d hecatoncheir -o ${LANG}_new.pot

if [ -f po/${LANG}.po ]; then
    OLDPO=po/${LANG}.po
fi
msgcat --sort-output --no-location ${OLDPO} ${LANG}_new.pot > ${LANG}2.pot
rm ${LANG}_new.pot
mv ${LANG}2.pot ${LANG}.pot
