================
クイックスタート
================

ソースコードの入手とインストール
================================

Githubのソースコードレポジトリを ``git clone`` してソースコードを取得します。

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

次に ``pip install .`` でインストールを行います。

::

  [snaga@localhost Hecatoncheir]$ sudo /usr/local/bin/pip install .
  Processing /disk/disk1/snaga/Hecatoncheir/Hecatoncheir
  Requirement already satisfied: jinja2==2.8 in /usr/local/lib/python2.7/site-packages (from hecatoncheir==0.8)
  Requirement already satisfied: MarkupSafe in /usr/local/lib/python2.7/site-packages (from jinja2==2.8->hecatoncheir==0.8)
  Installing collected packages: hecatoncheir
    Running setup.py install for hecatoncheir ... done
  Successfully installed hecatoncheir-0.8
  [snaga@localhost Hecatoncheir]$

以下のコマンドがインストールされたら完了です。

::

  [snaga@localhost tmp]$ ls /usr/local/bin/dm-*
  /usr/local/bin/dm-attach-file  /usr/local/bin/dm-import-datamapping
  /usr/local/bin/dm-dump-xls     /usr/local/bin/dm-run-profiler
  /usr/local/bin/dm-export-repo  /usr/local/bin/dm-run-server
  /usr/local/bin/dm-import-csv   /usr/local/bin/dm-verify-results
  [snaga@localhost tmp]$


メタデータ取得とデータのプロファイリング
========================================

次に、実際のテーブルに対してメタデータ取得とデータプロファイリングを実行します。ここではOracleの ``SCOTT`` スキーマ、 ``CUSTOMER`` テーブルを対象とします。

メタデータの収集とデータプロファイリングをするには ``dm-run-profiler`` コマンドを使います。

データベースの種類、TNS名、ユーザ名、パスワード、プロファイリング対象のテーブル名（ ``SCOTT.CUSTOMER`` ）を指定して、 ``dm-run-profiler`` コマンドを実行します。

::

  [snaga@localhost tmp]$ dm-run-profiler --dbtype oracle --tnsname orcl --user scott --pass tiger SCOTT.CUSTOMER
  [2017-04-30 15:57:09] INFO: 接続先(TNS): scott@orcl
  [2017-04-30 15:57:09] INFO: データベースに接続しています。
  [2017-04-30 15:57:09] INFO: データベースに接続しました。
  [2017-04-30 15:57:09] INFO: レポジトリを初期化しました。
  [2017-04-30 15:57:09] INFO: レポジトリファイル repo.db をオープンしました。
  [2017-04-30 15:57:09] INFO: ----------------------------------------------
  [2017-04-30 15:57:09] INFO: テーブルスキャン並列度: 0
  [2017-04-30 15:57:09] INFO: テーブルプロファイリングスキップ: False
  [2017-04-30 15:57:09] INFO: 行数プロファイリング: True
  [2017-04-30 15:57:09] INFO: カラムプロファイリングスキップ: False
  [2017-04-30 15:57:09] INFO: カラムプロファイリング閾値: 100,000,000行
  [2017-04-30 15:57:09] INFO: 最小値/最大値プロファイリング: True
  [2017-04-30 15:57:09] INFO: NULL値数プロファイリング: True
  [2017-04-30 15:57:09] INFO: 最頻値プロファイリング: 10 件
  [2017-04-30 15:57:09] INFO: カーディナリティプロファイリング: True
  [2017-04-30 15:57:09] INFO: データ検証有効化: False
  [2017-04-30 15:57:09] INFO: サンプルレコード取得: True
  [2017-04-30 15:57:09] INFO: ----------------------------------------------
  [2017-04-30 15:57:09] INFO: 1 テーブルのプロファイリングを開始します。
  [2017-04-30 15:57:09] INFO: テーブル SCOTT.CUSTOMER のプロファイリングを開始します。
  [2017-04-30 15:57:09] INFO: データ型の取得: 開始
  [2017-04-30 15:57:09] INFO: データ型の取得: 完了
  [2017-04-30 15:57:09] INFO: 行数の取得: 開始
  [2017-04-30 15:57:09] INFO: 行数の取得: 完了 (28)
  [2017-04-30 15:57:09] INFO: サンプル行の取得: 開始
  [2017-04-30 15:57:09] INFO: サンプル行の取得：完了
  [2017-04-30 15:57:09] INFO: NULL値数の取得: 開始
  [2017-04-30 15:57:09] INFO: NULL値数の取得: 完了
  [2017-04-30 15:57:09] INFO: 最小値/最大値の取得: 開始
  [2017-04-30 15:57:09] INFO: 最小値/最大値の取得: 完了
  [2017-04-30 15:57:09] INFO: 最頻値の取得(1/2): 開始
  [2017-04-30 15:57:09] INFO: 最頻値の取得(2/2): 開始
  [2017-04-30 15:57:09] INFO: 最頻値の取得: 完了
  [2017-04-30 15:57:09] INFO: カーディナリティの取得: 開始
  [2017-04-30 15:57:09] INFO: カーディナリティの取得: 完了
  [2017-04-30 15:57:09] INFO: レコード検証: 開始
  [2017-04-30 15:57:09] INFO: レコード検証: データ検証ルールがありません
  [2017-04-30 15:57:09] INFO: テーブル SCOTT.CUSTOMER のプロファイリングが完了しました。
  [2017-04-30 15:57:09] INFO: 1 テーブル中 0 テーブルのプロファイリングに失敗しました
  [2017-04-30 15:57:09] INFO: 1 テーブルのプロファイリングを完了しました
  [snaga@localhost tmp]$

メタデータ取得とプロファイリングが完了すると、取得したデータはレポジトリ（デフォルトでは ``repo.db``  というファイル名）に保存されます。

::

  [snaga@localhost tmp]$ ls -l repo.db
  -rw-r--r-- 1 snaga snaga 35840  4月 30 15:57 2017 repo.db
  [snaga@localhost tmp]$


HTMLファイルへのエクスポート
============================

取得したメタデータとデータプロファイルをHTMLファイルに出力するには、 ``dm-export-repo`` コマンドを使います。

レポジトリファイルと出力用ディレクトリを指定して ``dm-export-repo`` コマンドを実行すると、指定したディレクトリにファイルを出力します。デフォルトではHTMLファイルフォーマットで出力します。

::

  [snaga@localhost tmp]$ dm-export-repo repo.db html
  [2017-04-30 15:58:14] INFO: 出力用ディレクトリ html を作成しました。
  [2017-04-30 15:58:14] INFO: レポジトリファイル repo.db をオープンしました。
  [2017-04-30 15:58:14] INFO: html/orcl.SCOTT.CUSTOMER.html を出力しました。
  [2017-04-30 15:58:14] INFO: html/orcl.SCOTT.html を出力しました。
  [2017-04-30 15:58:14] INFO: html/validation-valid.html を出力しました。
  [2017-04-30 15:58:14] INFO: html/validation-invalid.html を出力しました。
  [2017-04-30 15:58:14] INFO: html/index.html を出力しました。
  [2017-04-30 15:58:14] INFO: html/index-tags.html を出力しました。
  [2017-04-30 15:58:14] INFO: html/index-schemas.html を出力しました。
  [2017-04-30 15:58:14] INFO: html/glossary.html を出力しました。
  [2017-04-30 15:58:15] INFO: static ディレクトリを html にコピーしました。
  [snaga@localhost tmp]$ ls -l html
  total 140
  -rw-rw-r-- 1 snaga snaga  5111  4月 30 15:58 2017 glossary.html
  -rw-rw-r-- 1 snaga snaga  6043  4月 30 15:58 2017 index-schemas.html
  -rw-rw-r-- 1 snaga snaga  5620  4月 30 15:58 2017 index-tags.html
  -rw-rw-r-- 1 snaga snaga  6043  4月 30 15:58 2017 index.html
  -rw-rw-r-- 1 snaga snaga 79935  4月 30 15:58 2017 orcl.SCOTT.CUSTOMER.html
  -rw-rw-r-- 1 snaga snaga  6046  4月 30 15:58 2017 orcl.SCOTT.html
  drwxr-xr-x 4 snaga snaga  4096  4月 29 16:41 2017 static
  -rw-rw-r-- 1 snaga snaga  4466  4月 30 15:58 2017 validation-invalid.html
  -rw-rw-r-- 1 snaga snaga  4704  4月 30 15:58 2017 validation-valid.html
  [snaga@localhost tmp]$

このHTMLファイルをブラウザで表示することによって、データディクショナリから収集したメタデータとデータプロファイリングの結果を確認することができます。
