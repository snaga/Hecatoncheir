.. _ref-csv-format:

=========================
CSV file format reference
=========================

Table Metadata CSV
==================

A CSV file to import Table Metadata must have following fields in its header line (the 1st line).

+-----------------+--------------------------+-----------+
| Field Name      | Value                    | Required? |
+=================+==========================+===========+
| DATABASE_NAME   | Database Name            | Requried  |
+-----------------+--------------------------+-----------+
| SCHEMA_NAME     | Schema Name              | Required  |
+-----------------+--------------------------+-----------+
| TABLE_NAME      | Table Name               | Required  |
+-----------------+--------------------------+-----------+
| TABLE_NAME_NLS  | Table Name (in NLS)      |           |
+-----------------+--------------------------+-----------+
| TABLE_COMMENT   | Table Comment            |           |
+-----------------+--------------------------+-----------+
| TAGS            | Tags (Comma separated)   |           |
+-----------------+--------------------------+-----------+
| TABLE_OWNER     | Owner                    |           |
+-----------------+--------------------------+-----------+

Table comment can be written in the Markdown format.

::

  DATABASE_NAME,SCHEMA_NAME,TABLE_NAME,TABLE_NAME_NLS,TABLE_COMMENT,TAGS,TABLE_OWNER
  ORCL,SCOTT,CUSTOMER,Customer Master,,,
  ORCL,SCOTT,LINEITEM,Order Detail,,"TAG1,TAG2",
  ORCL,SCOTT,NATION,Country Master,,,
  ORCL,SCOTT,ORDERS,Order Transaction,,TAG1,
  ORCL,SCOTT,PART,Item Master,,,
  ORCL,SCOTT,PARTSUPP,Item-Supplier Bridge Table,,TAG2,
  ORCL,SCOTT,REGION,Region Master,,,
  ORCL,SCOTT,SUPPLIER,Supplier Master,,TAG2,


Column Metadata CSV
===================

A CSV file to import Column Metadata must have following fields in its header line (the 1st line).

+-----------------+------------------------+-----------+
| Field Name      | Value                  | Required? |
+=================+========================+===========+
| DATABASE_NAME   | Database Name          | Required  |
+-----------------+------------------------+-----------+
| SCHEMA_NAME     | Schema Name            | Required  |
+-----------------+------------------------+-----------+
| TABLE_NAME      | Table Name             | Required  |
+-----------------+------------------------+-----------+
| COLUMN_NAME     | Column Name            | Required  |
+-----------------+------------------------+-----------+
| COLUMN_NAME_NLS | Column Name (in NLS)   |           |
+-----------------+------------------------+-----------+
| COLUMN_COMMENT  | Column Comment         |           |
+-----------------+------------------------+-----------+
| FK              | Foreign Key References |           |
+-----------------+------------------------+-----------+

Column comment can be written in the Markdown format.

::

  DATABASE_NAME,SCHEMA_NAME,TABLE_NAME,COLUMN_NAME,COLUMN_NAME_NLS,COLUMN_COMMENT,FK
  ORCL,SCOTT,CUSTOMER,C_CUSTKEY,Customer ID,,
  ORCL,SCOTT,CUSTOMER,C_NAME,Customer Name,,
  ORCL,SCOTT,CUSTOMER,C_ADDRESS,Customer Address,,
  ORCL,SCOTT,CUSTOMER,C_NATIONKEY,Country Code,,NATION.N_NATIONKEY
  ORCL,SCOTT,CUSTOMER,C_PHONE,Phone Number,,
  ORCL,SCOTT,CUSTOMER,C_ACCTBAL,Account Balance,,
  ORCL,SCOTT,CUSTOMER,C_MKTSEGMENT,Market Segment,,
  ORCL,SCOTT,CUSTOMER,C_COMMENT,Comment on this Customer,,


Schema Comment CSV
==================

A CSV file to import Schema Comment must have following fields in its header line (the 1st line).

+-----------------+------------------------+-----------+
| Field Name      | Value                  | Required? |
+=================+========================+===========+
| DATABASE_NAME   | Database Name          | Required  |
+-----------------+------------------------+-----------+
| SCHEMA_NAME     | Schema Name            | Required  |
+-----------------+------------------------+-----------+
| SCHEMA_COMMENT  | Schema Comment         | Required  |
+-----------------+------------------------+-----------+

Schema comment can be written in the Markdown format.

::

  DATABASE_NAME,SCHEMA_NAME,SCHEMA_COMMENT
  ORCL,SCOTT,This is SCOTT schema.


Tag Comment CSV
===============

A CSV file to import Tag Comment must have following fields in its header line (the 1st line).

+--------------+------------------------+-----------+
| Field Name   | Value                  | Required? |
+==============+========================+===========+
| TAG_NAME     | Tag Name               | Required  |
+--------------+------------------------+-----------+
| TAG_COMMENT  | Tag Comment            | Required  |
+--------------+------------------------+-----------+

Tag comment can be written in the Markdown format.

::

  TAG_NAME,TAG_COMMENT
  TAG1,This a comment on TAG1.


Data Mapping CSV
================

If you want to know more about the concept and the details of Data Mapping, please refer:

* `Qamar Shahbaz; Data Mapping for Data Warehouse Design, 1st Edition; Elsevier, 2015 <https://www.elsevier.com/books/data-mapping-for-data-warehouse-design/shahbaz/978-0-12-805185-6>`_

A CSV file to import Data Mapping must have following fields in its header line (the 1st line).

+-------------------------+------------------------------+-----------+
| Field Name              | Value                        | Required? |
+=========================+==============================+===========+
| CHANGE_DATE             | Changed Date                 | Required  |
+-------------------------+------------------------------+-----------+
| DATABASE_NAME           | Target Database Name         | Required  |
+-------------------------+------------------------------+-----------+
| SCHEMA_NAME             | Target Schema Name           | Required  |
+-------------------------+------------------------------+-----------+
| TABLE_NAME              | Target Table Name            | Required  |
+-------------------------+------------------------------+-----------+
| COLUMN_NAME             | Target Column Name           |           |
+-------------------------+------------------------------+-----------+
| DATA_TYPE               | Data Type                    |           |
+-------------------------+------------------------------+-----------+
| PK                      | Is the column a primary key? |           |
+-------------------------+------------------------------+-----------+
| NULLABLE                | Is the column nullable?      |           |
+-------------------------+------------------------------+-----------+
| SOURCE_DATABASE_NAME    | Source Database Name         |           |
+-------------------------+------------------------------+-----------+
| SOURCE_SCHEMA_NAME      | Source Schema Name           |           |
+-------------------------+------------------------------+-----------+
| SOURCE_TABLE_NAME       | Source Table Name            |           |
+-------------------------+------------------------------+-----------+
| SOURCE_COLUMN_NAME      | Source Column Name           |           |
+-------------------------+------------------------------+-----------+
| TRANSFORMATION_CATEGORY | Transformation Category      |           |
+-------------------------+------------------------------+-----------+
| TRANSFORMATION_ROLE     | Transformation Rule          |           |
+-------------------------+------------------------------+-----------+
| UPDATED_BY              | Updated by                   |           |
+-------------------------+------------------------------+-----------+

CR/LF and white space in the ``TRANSFORMATION_ROLE`` field will be converted to ``<br/>`` and ``&nbsp;`` in HTML.

::

  CHANGE_DATE,DATABASE_NAME,SCHEMA_NAME,TABLE_NAME,COLUMN_NAME,DATA_TYPE,PK,NULLABLE,SOURCE_DATABASE_NAME,RECORD_ID,SOURCE_SCHEMA_NAME,SOURCE_TABLE_NAME,SOURCE_COLUMN_NAME,TRANSFORMATION_CATEGORY,TRANSFORMATION_ROLE,UPDATED_BY
  2016-12-03,ORCL,SCOTT,ORDERS,O_ORDERDATE,DATE,,,ORCL,AAA01(Application),DWH_AAA,APPLICATION,APP_DT,,TO_CHAR('YYYYMMDD'),snaga
  2016-12-03,ORCL,SCOTT,ORDERS,O_ORDERDATE,DATE,,,ORCL,BBB01(Reservation),DWH_BBB,RESERVE,REGIST_DATE,Direct,,snaga
  2016-12-03,ORCL,SCOTT,ORDERS,O_ORDERDATE,DATE,,,ORCL,BBB02(Cancel),DWH_BBB,RESERVE,CANCEL_DATE,Direct,,snaga


Business Glossary CSV
=====================

A CSV file to import Business Glossary must have following fields in its header line (the 1st line).

+-------------------------+----------------------------------+-----------+
| Field Name              | Value                            | Required? |
+=========================+==================================+===========+
| TERM                    | Business term                    | Required  |
+-------------------------+----------------------------------+-----------+
| DESCRIPTION_SHORT       | Short description                | Required  |
+-------------------------+----------------------------------+-----------+
| DESCRIPTION_LONG        | Long description                 | Required  |
+-------------------------+----------------------------------+-----------+
| OWNER                   | Owner                            | Required  |
+-------------------------+----------------------------------+-----------+
| CATEGORIES              | Category (Camma separated)       |           |
+-------------------------+----------------------------------+-----------+
| SYNONYMS                | Synonyms (Camma separated)       |           |
+-------------------------+----------------------------------+-----------+
| RELATED_TERMS           | Related terms (Camma separated)  |           |
+-------------------------+----------------------------------+-----------+
| RELATED_ASSETS          | Related tables (Camma separated) |           |
+-------------------------+----------------------------------+-----------+

::

  TERM,DESCRIPTION_SHORT,DESCRIPTION_LONG,OWNER,CATEGORIES,SYNONYMS,RELATED_TERMS,RELATED_ASSETS
  Customer,Customer (Short description),"# Definition of the Customer (Detailed description)
  
  * Customer
  * Supplier
  * Account
  
  This field can be written in Markdown.",snaga,Category1,"Customer","Account,Supplier","SCOTT.CUSTOMER,SCOTT.COMPANY, SUPPLIER"


Data Validation Rule CSV
========================

A CSV file to import Validation Rules must have following fields in its header line (the 1st line).

+-----------------+----------------------------+-----------+
| Field Name      | Value                      | Required? |
+=================+============================+===========+
| DATABASE_NAME   | Database Name              | Required  |
+-----------------+----------------------------+-----------+
| SCHEMA_NAME     | Schema Name                | Required  |
+-----------------+----------------------------+-----------+
| TABLE_NAME      | Table Name                 | Required  |
+-----------------+----------------------------+-----------+
| COLUMN_NAME     | Column Name                | Required  |
+-----------------+----------------------------+-----------+
| DESCRIPTION     | Description                | Required  |
+-----------------+----------------------------+-----------+
| RULE            | Rule Name                  | Required  |
+-----------------+----------------------------+-----------+
| PARAM           | 1st parameter              |           |
+-----------------+----------------------------+-----------+
| PARAM2          | 2nd parameter              |           |
+-----------------+----------------------------+-----------+

See ":ref:`ref-validation-rule`" for more information about Data Validation Rule CSV.
