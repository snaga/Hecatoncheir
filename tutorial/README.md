
# Tutorial with Oracle Database

## Prepare the test data

```
$ sqlplus scott/tiger@ORCL
```

```
SQL> @oracle_data.sql
```

## Run the profiler for the first time

```
$ dm-run-profiler --dbtype oracle --tnsname ORCL --user SCOTT --pass tiger -s SCOTT
```

## Import supplimental metadata for tables and columns

```
$ dm-import-csv repo.db table_nls.csv
```

```
$ dm-import-csv repo.db column_nls.csv
```

## Import data mapping

```
$ dm-import-datamapping repo.db datamapping.csv
```

## Generate a data catalog

```
$ dm-export-repo repo.db ./html
```

## Import validation rules

```
dm-import-csv repo.db validation.txt
```

## Run the profiler with data validation

```
$ dm-run-profiler --dbtype oracle --tnsname ORCL --user SCOTT --pass tiger -s SCOTT --enable-validation
```

## Launch the server

```
$ dm-run-server repo.db 8080
```
