===================
About Hecatoncheir
===================

Overview
========

Hecatoncheir is software that helps data stewards to ensure data quality management and data governance with using database metadata, statistics and data profiles.

Key features
============

Hecatoncheir allows you to:

* Collect metadata from database dictionaries/catalogs
    * Collect metadata from database dictionaries/catalogs automatically, and format them in human-readable form.
    * Import supplimental metadata and gather them in the single repository to see.
* Profile database tables and columns
    * Collect table/column statistics automatically, and format them in human-readable form.
* Validate data with several business rules
    * Verify data with pre-defined business rules to meet your expectation in terms of its quality and integrity.
* Catalog data sets by tagging and importing additional metadata
    * Define data sets by grouping tables as you need.
    * Add some description and related materials/files to the data sets to share better understanding.
* Build a business glossary
    * Register the definitions of several business terms in table/column/data set descriptions, and browse them in the convenient way.

Requirements
============

Target databases
----------------

The latest version supports following databases:

* Oracle Database / Oracle Exadata
* SQL Server
* PostgreSQL
* MySQL
* Amazon Redshift

Following databases are coming in the future releases:

* DB2
* DB2 PureScale (Neteeza)
* Apache Hive
* Apache Spark
* Vertica
* Google BigQuery

Operating Systems
-----------------

Hecatoncheir can work on following operating systems.

* Red Hat Enterprise Linux 6 (x86_64)
* Red Hat Enterprise Linux 7 (x86_64)
* Windows 7 (64bit / 32bit)

Other requirements
------------------

Following software/modules are necessary to work:

* Python 2.7
* Client libraries/drivers for the target databases
* Python bindings for those modules

For example, you need to have one or more database drivers for the
databases you want to connect to:

* cx-Oracle: Oracle Database / Oracle Exadata
* MySQL-python: MySQL
* psycopg2: PostgreSQL, Amazon Redshift
* pymssql: SQL Server

