.. _ref-validation-rule:

===============================
Data Validation Rules reference
===============================

Column validation with its statistics
=====================================

You can verify your data with using several column statistics (c.f. min, max, number of nulls) collected by data profiling.

``rows``, ``nulls``, ``min``, ``max`` , ``cardinality`` are available for writing the validation rules.

::

  DATABASE_NAME,SCHEMA_NAME,TABLE_NAME,COLUMN_NAME,DESCRIPTION,RULE,PARAM,PARAM2
  ORCL,SCOTT,CUSTOMER,C_CUSTKEY,Not null,columnstat,{nulls} == 0
  ORCL,SCOTT,CUSTOMER,C_CUSTKEY,Unique,columnstat,{rows} == {cardinality}
  ORCL,SCOTT,CUSTOMER,C_CUSTKEY,Larger than zero,columnstat,{min} > 0
  ORCL,SCOTT,CUSTOMER,C_CUSTKEY,Min is zero,columnstat,{min} == 0
  ORCL,SCOTT,CUSTOMER,C_CUSTKEY,"Max is larger than 100,000",columnstat,{max} > 100000
  ORCL,SCOTT,CUSTOMER,C_CUSTKEY,"Max is larger than 1,000,000",columnstat,{max} > 1000000
  ORCL,SCOTT,CUSTOMER,C_CUSTKEY,Cardinality is 28,columnstat,{cardinality} == 28
  ORCL,SCOTT,CUSTOMER,C_CUSTKEY,Cardinality is larger than 28,columnstat,{cardinality} > 28


Record validation with table scan
=================================

You can verify your data with using table scan when the column statistics are not enough for you, specifically for data range or data patterns.

You can write validation rules with column names as variables. Also, you can use any combinations of two or more column names in single rule.

::

  DATABASE_NAME,SCHEMA_NAME,TABLE_NAME,COLUMN_NAME,DESCRIPTION,RULE,PARAM,PARAM2
  ORCL,SCOTT,CUSTOMER,C_CUSTKEY,Number,regexp,^\d+$
  ORCL,SCOTT,CUSTOMER,C_CUSTKEY,"Between 0 and 1,000,000",eval,{C_CUSTKEY} > 0 and {C_CUSTKEY} < 1000000
  ORCL,SCOTT,CUSTOMER,C_ACCTBAL,Larger than zero,eval,{C_ACCTBAL} > 0
  ORCL,SCOTT,CUSTOMER,"C_CUSTKEY,C_NATIONKEY",Custkey is larger than Nationkey,eval,{C_CUSTKEY} > {C_NATIONKEY}
  ORCL,SCOTT,CUSTOMER,"C_CUSTKEY,C_NATIONKEY",eval,Custkey is smaller than Nation key,{C_CUSTKEY} < {C_NATIONKEY}


Validation with SQL queries
===========================

You can verify your data with executing SQL and examining those results.

You can write validation rules with output column names in SQL as variables.

::

  DATABASE_NAME,SCHEMA_NAME,TABLE_NAME,COLUMN_NAME,DESCRIPTION,RULE,PARAM,PARAM2
  ORCL,SCOTT,CUSTOMER,C_CUSTKEY,1 or more records,sql,select count(*) count from customer,{COUNT} > 0
  ORCL,SCOTT,CUSTOMER,C_CUSTKEY,zero record,sql,select count(*) count from customer,{COUNT} == 0
