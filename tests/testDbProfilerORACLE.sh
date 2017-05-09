#!/bin/sh

export NLS_LANG=JAPANESE_JAPAN.UTF8

CMD="python ../dm-import-csv testDbProfilerORACLE.db validation_test_oracle.txt"
echo $CMD
$CMD

CMD="python ../dm-run-profiler --dbtype oracle --host 127.0.0.1 --port 1521 --dbname ORCL --user scott --pass tiger -o testDbProfilerORACLE.db --enable-validation"
#CMD="python ../dm-run-profiler --dbtype oracle --host 127.0.0.1 --port 1521 --dbname ORCL --user scott --pass tiger -o testDbProfilerORACLE.db --enable-validation --disable-sample-rows"
echo $CMD
$CMD

CMD="python ../dm-import-csv testDbProfilerORACLE.db test_oracle_table_nls.txt"
echo $CMD
$CMD

CMD="python ../dm-import-csv --encoding utf-8 testDbProfilerORACLE.db test_oracle_column_nls.txt"
echo $CMD
$CMD

CMD="python ../dm-import-datamapping --encoding utf-8 testDbProfilerORACLE.db test_oracle_datamapping.csv"
echo $CMD
$CMD

rm -rf /vagrant/testDbProfilerORACLE

CMD="python ../dm-export-repo testDbProfilerORACLE.db /vagrant/testDbProfilerORACLE"
echo $CMD
$CMD

CMD="python ../dm-verify-results testDbProfilerORACLE.db"
echo $CMD
$CMD
