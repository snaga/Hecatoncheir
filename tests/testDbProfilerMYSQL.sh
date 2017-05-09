#!/bin/sh

CMD="python ../dm-import-csv testDbProfilerMYSQL.db validation_test_mysql.txt"
echo $CMD
$CMD

CMD="python ../dm-run-profiler --dbtype mysql --host 127.0.0.1 --port 3306 --dbname dqwbtest --user root --pass mysql123 -o testDbProfilerMYSQL.db --enable-validation"
echo $CMD
$CMD

CMD="python ../dm-import-csv testDbProfilerMYSQL.db test_mysql_table_nls.txt"
echo $CMD
$CMD

CMD="python ../dm-import-csv --encoding utf-8 testDbProfilerMYSQL.db test_mysql_column_nls.txt"
echo $CMD
$CMD

CMD="python ../dm-import-datamapping --encoding utf-8 testDbProfilerMYSQL.db test_mysql_datamapping.csv"
echo $CMD
$CMD

rm -rf /vagrant/testDbProfilerMYSQL

CMD="python ../dm-export-repo testDbProfilerMYSQL.db /vagrant/testDbProfilerMYSQL"
echo $CMD
$CMD

CMD="python ../dm-verify-results testDbProfilerMYSQL.db"
echo $CMD
$CMD
