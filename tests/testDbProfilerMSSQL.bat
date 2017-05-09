rem ..\DbProfilerMerge.py testDbProfilerMSSQL.db validation_test_mssql.txt

python ..\dm-run-profiler --dbtype mssql --host 127.0.0.1 --port 1433 --dbname dqwbtest --user dqwbuser --pass  dqwbuser -o testDbProfilerMSSQL.db --enable-validation

python ..\dm-export-repo testDbProfilerMSSQL.db testDbProfilerMSSQL

@pause
