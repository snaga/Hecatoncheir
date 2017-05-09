===========
Quick start
===========

Getting the code and installation
=================================

Get the source code from the Github repository with running ``git clone``.

::

  [snaga@localhost tmp]$ git clone https://github.com/snaga/Hecatoncheir.git
  Cloning into 'Hecatoncheir'...
  remote: Counting objects: 3482, done.
  remote: Compressing objects: 100% (237/237), done.
  remote: Total 3482 (delta 176), reused 19 (delta 19), pack-reused 3226
  Receiving objects: 100% (3482/3482), 864.87 KiB | 1.09 MiB/s, done.
  Resolving deltas: 100% (2534/2534), done.
  [snaga@localhost tmp]$ cd Hecatoncheir/
  [snaga@localhost Hecatoncheir]$ ls
  LICENSE        build.bat       dm-import-csv          env.sh
  QuickStart.md  build.sh        dm-import-datamapping  requirements.txt
  README.md      demo            dm-run-profiler        setup.py
  README.oracle  dm-attach-file  dm-run-server          src
  bin            dm-export-repo  dm-verify-results
  [snaga@localhost Hecatoncheir]$

Then, install them with running ``pip install .`` .

::

  [snaga@localhost Hecatoncheir]$ sudo /usr/local/bin/pip install .
  Processing /disk/disk1/snaga/Hecatoncheir/Hecatoncheir
  Requirement already satisfied: jinja2==2.8 in /usr/local/lib/python2.7/site-packages (from hecatoncheir==0.8)
  Requirement already satisfied: MarkupSafe in /usr/local/lib/python2.7/site-packages (from jinja2==2.8->hecatoncheir==0.8)
  Installing collected packages: hecatoncheir
    Running setup.py install for hecatoncheir ... done
  Successfully installed hecatoncheir-0.8
  [snaga@localhost Hecatoncheir]$

If the following commands get installed properly, the installation is succeeded.

::

  [snaga@localhost tmp]$ ls /usr/local/bin/dm-*
  /usr/local/bin/dm-attach-file  /usr/local/bin/dm-import-datamapping
  /usr/local/bin/dm-dump-xls     /usr/local/bin/dm-run-profiler
  /usr/local/bin/dm-export-repo  /usr/local/bin/dm-run-server
  /usr/local/bin/dm-import-csv   /usr/local/bin/dm-verify-results
  [snaga@localhost tmp]$


Collecting metadata and profiling data
======================================

Let's try collecting metadata and profiling your tables. Here, for example, we are going to pick ``CUSTOMER`` table on ``SCOTT`` schema on our Oracle database.

To collect metadata and profile data, use the ``dm-run-profiler`` command.

Run ``dm-run-profiler`` command with specifying database type, tns name, user name, password and the target table name (``SCOTT.CUSTOMER``).

::

  [snaga@localhost tmp]$ dm-run-profiler --dbtype oracle --tnsname orcl --user scott --pass tiger SCOTT.CUSTOMER
  [2017-05-09 12:38:07] INFO: TNS info: scott@orcl
  [2017-05-09 12:38:07] INFO: Connecting the database.
  [2017-05-09 12:38:07] INFO: Connected to the database.
  [2017-05-09 12:38:07] INFO: The repository has been initialized.
  [2017-05-09 12:38:07] INFO: The repository file `repo.db' has been opened.
  [2017-05-09 12:38:07] INFO: ----------------------------------------------
  [2017-05-09 12:38:07] INFO: Parallel degree for table scan: 0
  [2017-05-09 12:38:07] INFO: Skipping table profiling: False
  [2017-05-09 12:38:07] INFO: Row count profiling: True
  [2017-05-09 12:38:07] INFO: Skippig column profiling: False
  [2017-05-09 12:38:07] INFO: Maximum row count to enable column profiling: 100,000,000 rows
  [2017-05-09 12:38:07] INFO: Min/Max values: True
  [2017-05-09 12:38:07] INFO: Number of null values: True
  [2017-05-09 12:38:07] INFO: Top-N most/least freq values: 10 values
  [2017-05-09 12:38:07] INFO: Column cardinality: True
  [2017-05-09 12:38:07] INFO: Data validation: False
  [2017-05-09 12:38:07] INFO: Obtaining sample records: True
  [2017-05-09 12:38:07] INFO: ----------------------------------------------
  [2017-05-09 12:38:07] INFO: Profiling on 1 tables.
  [2017-05-09 12:38:07] INFO: Profiling SCOTT.CUSTOMER: start
  [2017-05-09 12:38:07] INFO: Data types: start
  [2017-05-09 12:38:07] INFO: Data types: end
  [2017-05-09 12:38:07] INFO: Row count: start
  [2017-05-09 12:38:07] INFO: Row count: end (28)
  [2017-05-09 12:38:07] INFO: Sample rows: start
  [2017-05-09 12:38:07] INFO: Sample rows: end
  [2017-05-09 12:38:07] INFO: Number of nulls: start
  [2017-05-09 12:38:07] INFO: Number of nulls: end
  [2017-05-09 12:38:07] INFO: Min/Max values: start
  [2017-05-09 12:38:07] INFO: Min/Max values: end
  [2017-05-09 12:38:07] INFO: Most/Least freq values(1/2): start
  [2017-05-09 12:38:07] INFO: Most/Least freq values(2/2): start
  [2017-05-09 12:38:07] INFO: Most/Least freq values: end
  [2017-05-09 12:38:07] INFO: Column cardinality: start
  [2017-05-09 12:38:07] INFO: Column cardinality: end
  [2017-05-09 12:38:07] INFO: Record validation: start
  [2017-05-09 12:38:07] INFO: Record validation: no validation rule
  [2017-05-09 12:38:07] INFO: Profiling SCOTT.CUSTOMER: end
  [2017-05-09 12:38:07] INFO: Profiling errors have occured on 1/0 tables.
  [2017-05-09 12:38:07] INFO: Completed profiling 1 tables.
  [snaga@localhost tmp]$

Once collecting metadata and profiling data get completed, these will be stored in the repository file. (by default, ``repo.db``)

::

  [snaga@localhost tmp]$ ls -l repo.db
  -rw-r--r-- 1 snaga snaga 35840 May  9 12:38 repo.db
  [snaga@localhost tmp]$


Exporting to the HTML files
===========================

To export collected metadata and data profile to the HTML files, use ``dm-export-repo`` command.

By running ``dm-export-repo`` command with specifying the repository file and the output directory, it exports HTML files to the ouput directory. By default, it exports as HTML files.

::

  [snaga@localhost tmp]$ dm-export-repo repo.db html
  [2017-05-09 12:39:10] INFO: Created the output directory `html'.
  [2017-05-09 12:39:10] INFO: The repository file `repo.db' has been opened.
  [2017-05-09 12:39:10] INFO: Generated html/orcl.SCOTT.CUSTOMER.html.
  [2017-05-09 12:39:10] INFO: Generated html/orcl.SCOTT.html.
  [2017-05-09 12:39:10] INFO: Generated html/validation-valid.html.
  [2017-05-09 12:39:10] INFO: Generated html/validation-invalid.html.
  [2017-05-09 12:39:10] INFO: Generated html/index.html.
  [2017-05-09 12:39:10] INFO: Generated html/index-tags.html.
  [2017-05-09 12:39:10] INFO: Generated html/index-schemas.html.
  [2017-05-09 12:39:10] INFO: Generated html/glossary.html.
  [2017-05-09 12:39:10] INFO: Copied the static directory to `html'.
  [snaga@localhost tmp]$ ls -l html
  total 140
  -rw-rw-r-- 1 snaga snaga  5111 May  9 12:39 glossary.html
  -rw-rw-r-- 1 snaga snaga  6037 May  9 12:39 index-schemas.html
  -rw-rw-r-- 1 snaga snaga  5612 May  9 12:39 index-tags.html
  -rw-rw-r-- 1 snaga snaga  6037 May  9 12:39 index.html
  -rw-rw-r-- 1 snaga snaga 79360 May  9 12:39 orcl.SCOTT.CUSTOMER.html
  -rw-rw-r-- 1 snaga snaga  6040 May  9 12:39 orcl.SCOTT.html
  drwxr-xr-x 4 snaga snaga  4096 May  6 17:39 static
  -rw-rw-r-- 1 snaga snaga  4466 May  9 12:39 validation-invalid.html
  -rw-rw-r-- 1 snaga snaga  4704 May  9 12:39 validation-valid.html
  [snaga@localhost tmp]$

By opening those HTML files with your web browser, you can see those metadata coming from the data dictionaries and data profiles coming from your actual tables.
