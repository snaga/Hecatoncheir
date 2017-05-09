#!/bin/sh

PATH=.:$PATH
export PATH

dm-run-profiler
dm-run-profiler --help
dm-run-profiler --dbtype

# Oracle
dm-run-profiler --dbtype oracle
dm-run-profiler --dbtype oracle --host
dm-run-profiler --dbtype oracle --host 127.0.0.1
dm-run-profiler --dbtype oracle --host 127.0.0.1 --port
dm-run-profiler --dbtype oracle --host 127.0.0.1 --port 1521
dm-run-profiler --dbtype oracle --host 127.0.0.1 --port 1521 --dbname
dm-run-profiler --dbtype oracle --host 127.0.0.1 --port 1521 --dbname orcl
dm-run-profiler --dbtype oracle --host 127.0.0.1 --port 1521 --dbname orcl --user
dm-run-profiler --dbtype oracle --host 127.0.0.1 --port 1521 --dbname orcl --user scott
dm-run-profiler --dbtype oracle --host 127.0.0.1 --port 1521 --dbname orcl --user scott --pass
dm-run-profiler --dbtype oracle --host 127.0.0.1 --port 1521 --dbname orcl --user scott --pass tiger
dm-run-profiler --dbtype oracle --host 127.0.0.1 --port 1521 --dbname orcl --user scott --pass tiger -s
dm-run-profiler --dbtype oracle --host 127.0.0.1 --port 1521 --dbname orcl --user scott --pass tiger -s scott

dm-run-profiler --dbtype oracle2 --host 127.0.0.1 --port 1521 --dbname orcl --user scott --pass tiger -s scott
dm-run-profiler --dbtype oracle --host 127.0.0.11 --port 1521 --dbname orcl --user scott --pass tiger -s scott
dm-run-profiler --dbtype oracle --host 127.0.0.1 --port 15211 --dbname orcl --user scott --pass tiger -s scott
dm-run-profiler --dbtype oracle --host 127.0.0.1 --port 1521 --dbname orcl2 --user scott --pass tiger -s scott
dm-run-profiler --dbtype oracle --host 127.0.0.1 --port 1521 --dbname orcl --user scott2 --pass tiger -s scott
dm-run-profiler --dbtype oracle --host 127.0.0.1 --port 1521 --dbname orcl --user scott --pass tiger2 -s scott
dm-run-profiler --dbtype oracle --host 127.0.0.1 --port 1521 --dbname orcl --user scott --pass tiger -s scott2

# Postgres
dm-run-profiler --dbtype pgsql
dm-run-profiler --dbtype pgsql --host
dm-run-profiler --dbtype pgsql --host 127.0.0.1
dm-run-profiler --dbtype pgsql --host 127.0.0.1 --port
dm-run-profiler --dbtype pgsql --host 127.0.0.1 --port 5432
dm-run-profiler --dbtype pgsql --host 127.0.0.1 --port 5432 --dbname
dm-run-profiler --dbtype pgsql --host 127.0.0.1 --port 5432 --dbname postgres
dm-run-profiler --dbtype pgsql --host 127.0.0.1 --port 5432 --dbname postgres --user
dm-run-profiler --dbtype pgsql --host 127.0.0.1 --port 5432 --dbname postgres --user postgres
dm-run-profiler --dbtype pgsql --host 127.0.0.1 --port 5432 --dbname postgres --user postgres --pass
dm-run-profiler --dbtype pgsql --host 127.0.0.1 --port 5432 --dbname postgres --user postgres --pass postgres
dm-run-profiler --dbtype pgsql --host 127.0.0.1 --port 5432 --dbname postgres --user postgres --pass postgres -s
dm-run-profiler --dbtype pgsql --host 127.0.0.1 --port 5432 --dbname postgres --user postgres --pass postgres -s public

dm-run-profiler --dbtype pgsql2 --host 127.0.0.1 --port 5432 --dbname postgres --user postgres --pass postgres -s public
dm-run-profiler --dbtype pgsql --host 127.0.0.11 --port 5432 --dbname postgres --user postgres --pass postgres -s public
dm-run-profiler --dbtype pgsql --host 127.0.0.1 --port 15432 --dbname postgres --user postgres --pass postgres -s public
dm-run-profiler --dbtype pgsql --host 127.0.0.1 --port 5432 --dbname postgres2 --user postgres --pass postgres -s public
dm-run-profiler --dbtype pgsql --host 127.0.0.1 --port 5432 --dbname postgres --user postgres2 --pass postgres -s public
dm-run-profiler --dbtype pgsql --host 127.0.0.1 --port 5432 --dbname postgres --user postgres --pass postgres2 -s public
dm-run-profiler --dbtype pgsql --host 127.0.0.1 --port 5432 --dbname postgres --user postgres --pass postgres -s public2

# MySQL
dm-run-profiler --dbtype mysql
dm-run-profiler --dbtype mysql --host
dm-run-profiler --dbtype mysql --host 127.0.0.1
dm-run-profiler --dbtype mysql --host 127.0.0.1 --port
dm-run-profiler --dbtype mysql --host 127.0.0.1 --port 3306
dm-run-profiler --dbtype mysql --host 127.0.0.1 --port 3306 --dbname
dm-run-profiler --dbtype mysql --host 127.0.0.1 --port 3306 --dbname testdb
dm-run-profiler --dbtype mysql --host 127.0.0.1 --port 3306 --dbname testdb --user
dm-run-profiler --dbtype mysql --host 127.0.0.1 --port 3306 --dbname testdb --user root
dm-run-profiler --dbtype mysql --host 127.0.0.1 --port 3306 --dbname testdb --user root --pass
dm-run-profiler --dbtype mysql --host 127.0.0.1 --port 3306 --dbname testdb --user root --pass mysql
dm-run-profiler --dbtype mysql --host 127.0.0.1 --port 3306 --dbname testdb --user root --pass mysql -s
dm-run-profiler --dbtype mysql --host 127.0.0.1 --port 3306 --dbname testdb --user root --pass mysql -s testdb

dm-run-profiler --dbtype mysql2 --host 127.0.0.1 --port 3306 --dbname testdb --user root --pass mysql -s testdb
dm-run-profiler --dbtype mysql --host 127.0.0.11 --port 3306 --dbname testdb --user root --pass mysql -s testdb
dm-run-profiler --dbtype mysql --host 127.0.0.1 --port 13306 --dbname testdb --user root --pass mysql -s testdb
dm-run-profiler --dbtype mysql --host 127.0.0.1 --port 3306 --dbname testdb2 --user root --pass mysql -s testdb
dm-run-profiler --dbtype mysql --host 127.0.0.1 --port 3306 --dbname testdb --user root2 --pass mysql -s testdb
dm-run-profiler --dbtype mysql --host 127.0.0.1 --port 3306 --dbname testdb --user root --pass mysql2 -s testdb
dm-run-profiler --dbtype mysql --host 127.0.0.1 --port 3306 --dbname testdb --user root --pass mysql -s testdb2
