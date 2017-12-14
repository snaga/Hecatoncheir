==============
How-tos
==============

Collecting metadata and profiling data
======================================

Itmes collected by data profiling
---------------------------------

You can collect metadata and data profiles (table statistics and column statistics) of specific tables in the database.

Items to be collected by data profiling are following:

* Metadata from the database dictionary
    * Schema Names
    * Table Names
    * Column Names
    * Column Data Types
* Table Statistics
    * Number of Records
    * Number of Columns
* Column Statistics
    * Min/Max Values
    * Non-Null Ratio
    * Cardinality (Uniqueness)
    * Data Distribution (The Most/Least Frequent Values)


Collecting metadata and profiling tables and columns
----------------------------------------------------

Collecting metadata and profiling data can be performed by ``dm-run-profiler`` command.

With specifying several options for the database type, connection
parameters (host name, port number, databse name, user name, password,
etc.) and target table name(s), ``dm-run-profiler`` command
automatically collects metadata and profiles tables/columns.

Here is an example to perform profiling ``SCOTT.CUSTOMER`` table.

::

  dm-run-profiler --dbtype oracle --tnsname ORCL --user scott --pass tiger SCOTT.CUSTOMER

When profiling against Oracle databases, you can specify database with a host name or a TNS name. In this example, we use a TNS name to connect.

You can specify one or more table names in command-line arguments.

::

  dm-run-profiler --dbtype oracle --tnsname ORCL --user scott --pass tiger SCOTT.CUSTOMER SCOTT.ORDERS

If you specify only schema name, the profiler will find all tables in the schema and scan them all.

Following example shows how to scan all tables in ``SCOTT`` schema in ``ORCL`` database on Oracle.

::

  dm-run-profiler --dbtype oracle --tnsname ORCL --user scott --pass tiger SCOTT


When no schema/table name are supplied, a list of schema names will be shown. You need to specify one or more schemas in the list.

See ":ref:`ref-command`" for more information about ``dm-run-profiler`` command.


.. _importing-supplimental-metadata:

Importing supplimental metadata using CSV files
===============================================

Supplimental Metadata Items
---------------------------

Supplimental metadata, which cannot be obtained in the database dictionary, can be imported from CSV files, and combining them with the metadata from database dictionary in the repository allows you to see both in one place.

Following items can be imported from CSV files.

* Tables related
    * NLS (National Language Support) name
    * Comment
    * Tags
    * Owner (Data Steward)
* Columns related
    * NLS (National Language Support) name
    * Comment
    * Foreign key references
* Data mapping related
    * Database name of the data source
    * Schema name of the data source
    * Table name of the data source
    * Column name of the data source
    * Transformation type
    * Transformation rule
    * Maintainer

By organizing those as CSV files and importing them into the repository, you can enrich the metadata which is obtained from the database dictionaries.

See ":ref:`ref-csv-format` for more information about each CSV format.


Importing supplimental metadata
-------------------------------

To import Table Metadata CSV files and Column Metadata CSV files, use ``dm-import-csv`` command. To import Data Mapping CSV files, use ``dm-import-datamapping`` command.

``dm-import-csv`` command can detect CSV format (table related or column related) by analyzing field names in the first line (the header line) in the CSV file, and can import it into the repository with transforming properly.

In following example, it imports a Table Metadata CSV file in Shift-JIS encoding.

::

  dm-import-csv repo.db pgsql_tables.csv

By default, it assumes that file encoding of CSV file is Shift-JIS. However, the file encoding can be specified with an option.

In following example, it imports a Column Metadata CSV file in UTF-8 encoding.

::

  dm-import-csv --encoding utf-8 repo.db pgsql_columns.csv

``dm-import-datamapping`` can import Data mapping CSV files.

::

  dm-import-datamapping repo.db pgsql_datamapping.csv

See ":ref:`ref-command`" for more information about ``dm-import-csv`` command and ``dm-import-datamapping`` command.


Configuring data validation and the examination
===============================================

Configuring data validation
---------------------------

At first, a validation rule file needs to be created to examine data validation.

The validation rule file needs to be CSV format and should have table names and column names with validation rules and parameters to be examined.

See ":ref:`ref-validation-rule`" for more information about writing validation rules.

The validation rule file can be imported to the repository with using ``dm-import-csv`` command.

::

  dm-import-csv repo.db validation_oracle.txt

See ":ref:`ref-command`" for more information about ``dm-import-csv`` command.


Examining the validation
------------------------

Specifying ``--enable-validation`` option for ``dm-run-profiler`` command enables data validation while data profiling.

In following example, ``dm-run-profiler`` command examines data validation while profiling ``SCOTT.CUSTOMER`` table.

::

  dm-run-profiler --dbtype oracle --tnsname ORCL --user scott --pass tiger --enable-validation SCOTT.CUSTOMER

Results of the data validation will be collected by ``dm-run-profiler`` command and be stored in the repository with the metadata and the data profiles.


Verifying the results
---------------------

``dm-verify-results`` command verifies  the results of the data validation.

By running ``dm-verify-results`` command with the repository, it scans the latest data validation results whether it has invalid results against the validation rules or not.

::

  dm-verify-results repo.db

Once invalid result is detedted, the notice message will be shown and the command will exit with the exit code ``1``.

To implement data validation with your own shell scripts, you can detect invalid result(s) automatically with using ``dm-verify-results`` command.


Building a business glossary
============================

Defining business terms
-----------------------

Business terms appered in table names, column names and several comments can be organized as a business glossary with its definitions, synonyms, related terms and related IT assets (tables), and those can be refereed from the data catalog in the convenient way.

And you can search terms in the glossary.

Registering business terms
--------------------------

To register business terms, you need to create a CSV file containing those terms and definitions.

See ":ref:`ref-csv-format`" for more information about the CSV format.

``dm-import-csv`` command can import those terms in the CSV file into the repository.

::

  dm-import-csv repo.db business_glossary.csv

See ":ref:`ref-command`" for more information about ``dm-import-csv`` command.


Defining data sets and building a data catalog
==============================================

Defining data sets
------------------

A data set is defined as a group of tables in the same schema or a group of tables which have the same tag. (Tagging tables can be done by importing the supplimental metadata from CSV files. See ":ref:`importing-supplimental-metadata`" for more info.)


Adding comments to data sets
----------------------------

Some descriptive comment can be added to the data set, a group of tables. (This comment will be shown on the data catalog as a description of the data set for the users.)

If the data set you want to add a commet is a schema, you need to create a CSV file containing schema comment(s).

If the data set you want to add a commet is a tag, you need to create a CSV file containing tag comment(s).

See ":ref:`ref-csv-format`" for more information about "Schema Comment CSV" and "Tag Comment CSV" formats.

Those CSV files can be imported to the repository with ``dm-import-csv`` command.

::

  dm-import-csv repo.db schema_comments.csv
  dm-import-csv repo.db tag_comments.csv

See ":ref:`ref-command`" for more information about ``dm-import-csv`` command.


Attaching files to data sets
----------------------------

Comments for data sets can be written in very flexible and rich style because it accepts the Markdown format. However, in some cases, you may want to add figures, tables or other forms of representation, or at least, just a longer document to the comment.

In such cases, external files (PowerPoint or Excel files, for example) can be attached to the comment of the data set.

To attach files to the comment, ``dm-attach-file`` command can be used.

Run ``dm-attach-file`` command with specifying the repository, the data set type and a file name which you want to attach.

In following example, a file ``Tag1.ppt`` is going to be attached to the data set ``Tag1`` which is a tag.

::

  dm-attach-file repo.db tag:Tag1 add Tag1.ppt

In the second example, a file ``schema_desing.xlsx`` is going to be attached to the data set ``testdb-public`` which is a schema.

::

  dm-attach-file repo.db schema:testdb.public add schema_design.xlsx

See ":ref:`ref-command`" for more information about ``dm-attach-file`` command.


Exporting a data catalog
------------------------

To view several metadata and statistics gathered in the repository, it needs to be exported to HTML files as a data catalog.

By running ``dm-export-repo`` command with specifying the repository and the output directory, a data catalog is generated as a collection of HTML files from the repository data.

::

  dm-export-repo repo.db ./html

``dm-export-repo`` command accepts the non-default (customized) templates on generating HTML files. By using customized templates, you can modify design and layout of the data catalog.

::

  dm-export-repo --template /path/to/mytemplates repo.db html

See ":ref:`ref-command`" for more information about ``dm-export-repo`` command.

