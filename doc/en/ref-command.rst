.. _ref-command:

==================
Command references
==================

dm-attach-file
==============

``dm-attach-file`` command attaches or removes files to/from the data sets (tags/schemas) and tables. Also, it shows a list of the files attached to the data sets/tables.

::

  Usage: dm-attach-file [repo file] [target] [command]
  
  Targets:
      tag:[tag label]
      schema:[schema name]
      table:[table name]
  
  Commands:
      list
      add [file]
      rm [file]
  
  Options:
      --help                     Print this help.

``repo file`` should be a file name of the repository.

``target`` specifies the target data set or table with ``tag``, ``schema`` or ``table`` with its name.

``command`` specifies the action for the data set or table with ``list``, ``add`` or ``rm`` with a file name.


dm-dump-xls
===========

``dm-dump-xls`` command dumps Excel sheets in the CSV format.

It takes a file name of the Excel book and a name of the sheet or an index of the sheet (1,2,3...) to be converted, and dumps the sheet to STDOUT in the CSV format.

::

  Usage: dm-dump-xls <filename.xls> [<sheet name> | <sheet index>]
  
  Options:
      -e STRING                  Output encoding (default: utf-8)
  
      --help                     Print this help.

``-e`` specifies the encoding of the output CSV data.


dm-export-repo
==============

``dm-export-repo`` exports metadata, statistics and other supplimental data stored in the repository as a data catalog in specific format (c.f. HTML).

The repository data can be exported in the following forms.

* HTML files
* CSV files
* JSON files


::

  Usage: dm-export-repo [options...] [repo file] [output directory]
  
  Options:
      --format <STRING>               Output format. (html, json or csv)
      --help                          Print this help.
  
  Options for HTML format:
      --tags <TAG>[,<TAG>]            Tag names to be shown on the top page.
      --schemas <SCHEMA>[,<SCHEMA>]   Schema names to be shown on the top page.
      --template <STRING>             Directory name for template files.
  
  Options for CSV format:
      --encoding <STRING>             Character encoding for output files.

``repo file`` should be a file name of the repository.

``output directory`` should be a directory name for the output.

``--format`` specifies the output format. By default, ``html``.

``--tags`` specifies the tag names which must be appeared on the home page of the data catalog. (by default, tag names will be sorted by the name)

``--schemas`` specifies the schema names which must be appeared on the home page of the data catalog. (by default, schema names will be sorted by the name)

``--templates`` specifies a directory name which contains the template files for generating html files for data catalog.

``--encoding`` specifies character encoding for the output csv files. By default, ``utf-8``.


dm-import-csv
=============

``dm-import-csv`` command imports supplimental metadata of the tables/columns and other additional information from csv files.

It can import following CSV files.

* Table Metadata CSV
* Column Metadata CSV
* Schema Comment CSV
* Tag Comment CSV
* Business Glossary CSV
* Data Validation Rule CSV

::

  Usage: dm-import-csv [repo file] [csv file]
  
  Options:
      -E, --encoding=STRING      Encoding of the CSV file (default: sjis)
      --help                     Print this help.

``repo file`` should be a file name of the repository.

``-E, --encoding`` specifies the input encoding of the CSV files. By default, it is ``sjis``.

See ":ref:`ref-csv-format`" for more information about each CSV format.


dm-import-datamapping
=====================

``dm-import-datamapping`` command imports data mapping information to the repository from the CSV file.

::

  Usage: dm-import-datamapping [repo file] [csv file]
  
  Options:
      -E, --encoding=STRING      Encoding of the CSV file (default: sjis)
      --help                     Print this help.

``repo file`` should be a file name of the repository.

``-E, --encoding`` specifies the input encoding of the CSV files. By default, it is ``sjis``.

See ":ref:`ref-csv-format`" for more information about the CSV format.


dm-run-profiler
===============

``dm-run-profiler`` command connects to the database, collects metadata and data profiles, and store those results in the repository. And validates the data with pre-defined validation rules.

::

  Usage: dm-run-profiler [option...] [schema.table] ...
  
  Options:
      --dbtype=TYPE              Database type
      --host=STRING              Host name
      --port=INTEGER             Port number
      --dbname=STRING            Database name
      --tnsname=STRING           TNS name (Oracle only)
      --user=STRING              User name
      --pass=STRING              User password
      -s=STRING                  Schema name
      -t=STRING                  Table name
      -P=INTEGER                 Parallel degree of table scan
      -o=FILENAME                Output file
      --batch=FILENAME           Batch execution
  
      --enable-validation        Enable record/column/SQL validations
  
      --enable-sample-rows       Enable collecting sample rows. (default)
      --disable-sample-rows      Disable collecting sample rows.
  
      --skip-table-profiling     Skip table (and column) profiling
      --skip-column-profiling    Skip column profiling
      --column-profiling-threshold=INTEGER
                                 Threshold number of rows to skip profiling columns
  
      --timeout=NUMBER           Query timeout in seconds (default:no timeout)

      --help                     Print this help.


``--dbtype`` specifies the database type. It should be ``oracle``, ``mssql``, ``pgsql`` or ``mysql``.

``--host`` specifies a host name to connect to the database.

``--port`` specifies a port number to connect to the database.

``--dbname`` specifies the database name to connect.

``--tnsname`` specifies a TNS name when connecting with TNS name. (Oracle only)

``--user`` specifies an user name to connect to the database.

``--pass`` specifies the password to connect to the database.

``-s`` specifies the target schema name.

``-t`` specifies the target table name.

``-P`` specifies the degree of parallel scan.

``-o`` specifies a file name of the repository.

``--batch`` specifies a file name containing multiple table names for batch processing.

``--enable-validation`` enables the data validation.

``--enable-sample-rows`` enables collecting sample records (up to 10 records). (default)

``--disable-sample-rows`` disables collecting sample records.

``--skip-table-profiling`` disables profiling tables and columns.

``--skip-column-profiling`` disables profiling columns.

``--column-profiling-threshold`` specifies max number of table records to perform column profiling.

``--timeout`` specifies query timeout in seconds. If query execution exeeds this parameter, the query will be cancelled and profiling the table will fail.

dm-run-server
=============

``dm-run-server`` command launches a web server to accept accessing to the repository data through the network.

It allows you to

* View the repository data without exporting as HTML files.
* View changes in the repository immediately.
* Edit several data in the repository (cf. comments, tags, etc) directly using the browser.

::

  Usage: dm-run-server [repo file] [port]
  
  Options:
      --help                     Print this help.

``repo file`` should be a file name of the repository.

``port`` should be a port number to connect to the server. (by default, it is 8080.)

dm-verify-results
=================

``dm-verify-results`` verifies the data condition by scanning validation results in the repository.

It exits with the exit code ``1`` if there are invalid results, otherwise ``0``.

::

  Usage: dm-verify-results [repo file]
  
  Options:
      --help                     Print this help.


``repo file`` should be a file name of the repository.
