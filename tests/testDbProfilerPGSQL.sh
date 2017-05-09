#!/bin/sh

set -e

DBNAME=dqwbtest
DBUSER=dqwbuser
DBPASS=dqwbuser

_topdir=..

CMD="python ${_topdir}/dm-import-csv testDbProfilerPGSQL.db validation_test_pgsql.txt"
echo $CMD
$CMD

# with validation
CMD="python ${_topdir}/dm-run-profiler --dbtype pgsql --host 127.0.0.1 --port 5432 --dbname ${DBNAME} --user ${DBUSER} --pass ${DBPASS} -o testDbProfilerPGSQL.db --enable-validation"
echo $CMD
$CMD

# without validation
CMD="python ${_topdir}/dm-run-profiler --dbtype pgsql --host 127.0.0.1 --port 5432 --dbname ${DBNAME} --user ${DBUSER} --pass ${DBPASS} -o testDbProfilerPGSQL.db public.orders public.SUPPLIER"
echo $CMD
$CMD

CMD="python ${_topdir}/dm-import-csv testDbProfilerPGSQL.db test_glossary.txt"
echo $CMD
$CMD

CMD="python ${_topdir}/dm-import-csv testDbProfilerPGSQL.db test_pgsql_table_nls.txt"
echo $CMD
$CMD

CMD="python ${_topdir}/dm-attach-file testDbProfilerPGSQL.db tag:タグ1 add test_pgsql_table_nls.txt"
echo $CMD
$CMD

CMD="python ${_topdir}/dm-attach-file testDbProfilerPGSQL.db schema:dqwbtest.public add test_pgsql_table_nls.txt"
echo $CMD
$CMD

CMD="python ${_topdir}/dm-attach-file testDbProfilerPGSQL.db table:dqwbtest.public.SUPPLIER add test_pgsql_table_nls.txt"
echo $CMD
$CMD

CMD="python ${_topdir}/dm-import-csv --encoding utf-8 testDbProfilerPGSQL.db test_pgsql_column_nls.txt"
echo $CMD
$CMD

CMD="python ${_topdir}/dm-import-csv --encoding utf-8 testDbProfilerPGSQL.db test_tag_comment.csv"
echo $CMD
$CMD

CMD="python ${_topdir}/dm-import-csv --encoding utf-8 testDbProfilerPGSQL.db test_schema_comment.csv"
echo $CMD
$CMD

CMD="python ${_topdir}/dm-import-datamapping --encoding utf-8 testDbProfilerPGSQL.db test_pgsql_datamapping.csv"
echo $CMD
$CMD

rm -rf /vagrant/testDbProfilerPGSQL

CMD="python ${_topdir}/dm-export-repo --template ../hecatoncheir/templates/ja testDbProfilerPGSQL.db /vagrant/testDbProfilerPGSQL/ja"
echo $CMD
$CMD

CMD="python ${_topdir}/dm-export-repo --template ../hecatoncheir/templates/en testDbProfilerPGSQL.db /vagrant/testDbProfilerPGSQL/en"
echo $CMD
$CMD

CMD="python ${_topdir}/dm-export-repo testDbProfilerPGSQL.db /vagrant/testDbProfilerPGSQL"
echo $CMD
$CMD

CMD="python ${_topdir}/dm-verify-results testDbProfilerPGSQL.db"
echo $CMD
$CMD
