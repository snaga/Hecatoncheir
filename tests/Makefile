all: validation_test validation_record datamapping

clean:
	rm -f validation_test_oracle.txt validation_test_mysql.txt validation_test_mssql.txt
	rm -f validation_record_oracle.txt validation_record_mysql.txt validation_record_mssql.txt
	rm -f test_oracle_datamapping.csv test_mysql_datamapping.csv test_mssql_datamapping.csv
	rm -rf attachments
	rm -f *.db

validation_test: validation_test_oracle.txt validation_test_mysql.txt validation_test_mssql.txt
validation_record: validation_record_oracle.txt validation_record_mysql.txt validation_record_mssql.txt
datamapping: test_oracle_datamapping.csv test_mysql_datamapping.csv test_mssql_datamapping.csv

# -------------------------------------------
# validation rules for columnstat and sql
# -------------------------------------------
validation_test_oracle.txt: validation_test_pgsql.txt
	cat validation_test_pgsql.txt \
	 | perl -e 's/^dqwbtest/ORCL/g;' -p \
	 | perl -e 's/customer/CUSTOMER/g;s/c_custkey/C_CUSTKEY/g;s/c_acctbal/C_ACCTBAL/g;s/c_nationkey/C_NATIONKEY/g;' -p \
	 | perl -e 's/n_nationkey/N_NATIONKEY/g;s/nation/NATION/g;s/n_regionkey/N_REGIONKEY/g;' -p \
	 | perl -e 's/public/SCOTT/g;s/count/COUNT/g;s/count\(\*\)/COUNT\(\*\) COUNT/ig;' -p > validation_test_oracle.txt

validation_test_mysql.txt: validation_test_pgsql.txt
	cat validation_test_pgsql.txt \
	 | perl -e 's/count\(\*\)/count\(\*\) count/ig;s/public/dqwbtest/g;' -p > validation_test_mysql.txt

validation_test_mssql.txt: validation_test_pgsql.txt
	cat validation_test_pgsql.txt \
	 | perl -e 's/public/dbo/g;' -p > validation_test_mssql.txt

# -------------------------------------------
# validation rules for regexp and eval
# -------------------------------------------
validation_record_oracle.txt: validation_record_pgsql.txt
	cat validation_record_pgsql.txt \
	 | perl -e 's/^dqwbtest/ORCL/g;' -p \
	 | perl -e 's/customer/CUSTOMER/g;s/c_custkey/C_CUSTKEY/g;s/c_acctbal/C_ACCTBAL/g;s/c_nationkey/C_NATIONKEY/g;' -p \
	 | perl -e 's/n_nationkey/N_NATIONKEY/g;s/nation/NATION/g;s/n_regionkey/N_REGIONKEY/g;' -p \
	 | perl -e 's/public/SCOTT/g;s/count/COUNT/g;s/count\(\*\)/COUNT\(\*\) COUNT/ig;' -p > validation_record_oracle.txt

validation_record_mysql.txt: validation_record_pgsql.txt
	cat validation_record_pgsql.txt \
	 | perl -e 's/count\(\*\)/count\(\*\) count/ig;s/public/dqwbtest/g;' -p > validation_record_mysql.txt

validation_record_mssql.txt: validation_record_pgsql.txt
	cat validation_record_pgsql.txt \
	 | perl -e 's/public/dbo/g;' -p > validation_record_mssql.txt

# -------------------------------------------
# data mapping info
# -------------------------------------------
test_oracle_datamapping.csv: test_pgsql_datamapping.csv
	cat test_pgsql_datamapping.csv \
	 | perl -e 's/dqwbtest/ORCL/g;' -p \
	 | perl -e 's/customer/CUSTOMER/g;s/c_custkey/C_CUSTKEY/g;s/c_acctbal/C_ACCTBAL/g;s/c_nationkey/C_NATIONKEY/g;' -p \
	 | perl -e 's/n_nationkey/N_NATIONKEY/g;s/nation/NATION/g;s/n_regionkey/N_REGIONKEY/g;' -p \
	 | perl -e 's/public/SCOTT/g;s/count/COUNT/g;s/count\(\*\)/COUNT\(\*\) COUNT/ig;' -p > test_oracle_datamapping.csv

test_mysql_datamapping.csv: test_pgsql_datamapping.csv
	cat test_pgsql_datamapping.csv \
	 | perl -e 's/count\(\*\)/count\(\*\) count/ig;s/public/dqwbtest/g;' -p > test_mysql_datamapping.csv

test_mssql_datamapping.csv: test_pgsql_datamapping.csv
	cat test_pgsql_datamapping.csv \
	 | perl -e 's/public/dbo/g;' -p > test_mssql_datamapping.csv
