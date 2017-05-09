.. _ref-validation-rule:

================================
データ検証ルール設定リファレンス
================================

カラム統計情報によるカラム検証
==============================

データプロファイリングで収集したカラムの統計情報（最大値、最小値、NULL値数などを）を用いて検証を行います。

``rows``, ``nulls``, ``min``, ``max`` , ``cardinality`` の各変数を使ってルールを記述することができます。

::

  DATABASE_NAME,SCHEMA_NAME,TABLE_NAME,COLUMN_NAME,DESCRIPTION,RULE,PARAM,PARAM2
  orcl,SCOTT,CUSTOMER,C_CUSTKEY,Not null,columnstat,{nulls} == 0
  orcl,SCOTT,CUSTOMER,C_CUSTKEY,Unique,columnstat,{rows} == {cardinality}
  orcl,SCOTT,CUSTOMER,C_CUSTKEY,Larger than zero,columnstat,{min} > 0
  orcl,SCOTT,CUSTOMER,C_CUSTKEY,Min is zero,columnstat,{min} == 0
  orcl,SCOTT,CUSTOMER,C_CUSTKEY,"Max is larger than 100,000",columnstat,{max} > 100000
  orcl,SCOTT,CUSTOMER,C_CUSTKEY,"Max is larger than 1,000,000",columnstat,{max} > 1000000
  orcl,SCOTT,CUSTOMER,C_CUSTKEY,Cardinality is 28,columnstat,{cardinality} == 28
  orcl,SCOTT,CUSTOMER,C_CUSTKEY,Cardinality is larger than 28,columnstat,{cardinality} > 28


テーブルスキャンによるレコード検証
==================================

カラムの統計情報だけでは検証できない値の範囲やデータのパターンなどについて、テーブルの全レコードをスキャンして検証します。

各カラムの名前を変数としてルールを記述することができます。また、複数のカラムの組み合わせでルールを記述することもできます。

::

  DATABASE_NAME,SCHEMA_NAME,TABLE_NAME,COLUMN_NAME,DESCRIPTION,RULE,PARAM,PARAM2
  orcl,SCOTT,CUSTOMER,C_CUSTKEY,Number,regexp,^\d+$
  orcl,SCOTT,CUSTOMER,C_CUSTKEY,"Between 0 and 1,000,000",eval,{C_CUSTKEY} > 0 and {C_CUSTKEY} < 1000000
  orcl,SCOTT,CUSTOMER,C_ACCTBAL,Larger than zero,eval,{C_ACCTBAL} > 0
  orcl,SCOTT,CUSTOMER,"C_CUSTKEY,C_NATIONKEY",Custkey is larger than Nationkey,eval,{C_CUSTKEY} > {C_NATIONKEY}
  orcl,SCOTT,CUSTOMER,"C_CUSTKEY,C_NATIONKEY",eval,Custkey is smaller than Nation key,{C_CUSTKEY} < {C_NATIONKEY}


SQLクエリによる検証
===================

指定したSQLを実行して、その結果を用いて検証を行うことができます。

SQLの出力する列を変数としてルールを記述することができます。

::

  DATABASE_NAME,SCHEMA_NAME,TABLE_NAME,COLUMN_NAME,DESCRIPTION,RULE,PARAM,PARAM2
  orcl,SCOTT,CUSTOMER,C_CUSTKEY,1 or more records,sql,select count(*) count from customer,{COUNT} > 0
  orcl,SCOTT,CUSTOMER,C_CUSTKEY,zero record,sql,select count(*) count from customer,{COUNT} == 0
