.. _ref-command:

====================
コマンドリファレンス
====================

dm-attach-fileコマンド
======================

``dm-attach-file`` コマンドは、データセット（タグ／スキーマ）およびテーブルのコメントに添付するファイルを登録、一覧表示、削除します。

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

``repo file`` はレポジトリファイル名です。

``target`` は、``tag``, ``schema``, ``table`` のいずれかのoターゲットを名前と共に指定します。

``command`` は、``list``, ``add``, ``rm`` のいずれかの操作をファイル名とともに指定します。


dm-dump-xlsコマンド
===================

``dm-dump-xls`` コマンドは、指定したExcelファイルのシートをCSV形式で出力します。

Excelのファイル名、およびシート名、またはシート番号（1,2,3…）を指定すると、シートの内容をCSV形式で標準出力に出力します。

::

  Usage: dm-dump-xls <filename.xls> [<sheet name> | <sheet index>]
  
  Options:
      -e STRING                  Output encoding (default: utf-8)
  
      --help                     Print this help.

``-e`` オプションは出力するCSVデータのエンコーディングを指定します。


dm-export-repoコマンド
======================

``dm-export-repo`` コマンドは、レポジトリに保存されたメタデータ、統計情報、その他の補足的なデータを、をデータカタログとして指定したファイル形式（HTMLなど）でエクスポートします。

以下の形式でレポジトリのデータをエクスポートすることができます。

* HTMLファイル
* CSVファイル
* JSONファイル


::

  Usage: dm-export-repo [options...] [repo file] [output directory]
  
  Options:
      --format <STRING>               Output format. (html, json or csv)
      --help                          Print this help.
  
  Options for HTML format:
      --tags <TAG>[,<TAG>]            Tag names to be shown on the top page.
      --schemas <SCHEMA>[,<SCHEMA>]   Schema names to be shown on the top page.
      --template <STRING>             Directory name for template files.

``repo file`` はレポジトリファイル名です。

``output directory`` は出力先のディレクトリ名です。

``--format`` は出力するフォーマットを指定します。デフォルトは ``html`` です。

``--tags`` はデータカタログとして出力する際、トップページに優先的に表示するタグ名を指定します（指定していないものはタグ名順にソートして表示）。

``--schemas`` はデータカタログとして出力する際、トップページに優先的に表示するスキーマ名を指定します（指定していないものはスキーマ名順にソートして表示）。

``--templates`` はデータカタログとしてHTMLを生成する際に使用するテンプレートファイルのあるディレクトリです。


dm-import-csvコマンド
=====================

``dm-import-csv`` コマンドは、テーブルやカラムに関する付加的な情報をCSVファイルからレポジトリにインポートします。

以下のCSVファイルをインポートすることができます。

* テーブルメタデータCSV
* カラムメタデータCSV
* スキーマコメントCSV
* タグコメントCSV
* ビジネス用語辞書CSV
* データ検証ルールCSV

::

  Usage: dm-import-csv [repo file] [csv file]
  
  Options:
      -E, --encoding=STRING      Encoding of the CSV file (default: sjis)
      --help                     Print this help.

``repo file`` はレポジトリファイル名です。

``-E, --encoding`` は入力となるCSVファイルのエンコーディングです。デフォルトは ``sjis`` です。

各CSVファイルのフォーマットについては「:ref:`ref-csv-format`」を参照してください。


dm-import-datamappingコマンド
=============================

``dm-import-datamapping`` コマンドは、データマッピングの情報をCSVファイルからレポジトリにインポートします。

::

  Usage: dm-import-datamapping [repo file] [csv file]
  
  Options:
      -E, --encoding=STRING      Encoding of the CSV file (default: sjis)
      --help                     Print this help.

``repo file`` はレポジトリファイル名です。

``-E, --encoding`` は入力となるCSVファイルのエンコーディングです。デフォルトは ``sjis`` です。

CSVファイルのフォーマットについては「:ref:`ref-csv-format`」を参照してください。


dm-run-profilerコマンド
=======================

``dm-run-profiler`` コマンドは、データベースに接続してメタデータおよびデータプロファイルの取得を行い、その結果をレポジトリに保存します。また、あらかじめ定義したルールに従ってデータ検証を行います。

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


``--dbtype`` はデータベース種別の指定です。 ``oracle``, ``mssql``, ``pgsql``, ``mysql`` のいずれかを指定できます。

``--host`` はデータベースに接続するホスト名です。

``--port`` はデータベースに接続するポート番号です。

``--dbname`` は接続するデータベースです。

``--tnsname`` はTNS接続を使ってデータベースに接続する際のTNS名です（Oracleのみ）。

``--user`` はデータベースに接続するユーザ名です。

``--pass`` はデータベースに接続するパスワードです。

``-s`` は対象とするスキーマ名です。

``-t`` は対象とするテーブル名です。

``-P`` は内部でテーブルスキャンを実行する際の並列度です。

``-o`` は出力するレポジトリのファイル名です。

``--batch`` は一括して処理する複数のスキーマ名およびテーブル名を記述したファイルです。

``--enable-validation`` はデータ検証の機能を有効にします。

``--enable-sample-rows`` はサンプルレコード（10行）の取得を有効にします。（デフォルト）

``--disable-sample-rows`` はサンプルレコードの取得を無効にします。

``--skip-table-profiling`` はテーブルおよびカラムのプロファイリングを無効にします。

``--skip-column-profiling`` はカラムのプロファイリングを無効にします。

``--column-profiling-threshold`` はカラムのプロファイリングを行うレコード数の上限を指定します。

``--timeout`` はクエリのタイムアウトを秒数で指定します。クエリの実行時間がこれを超えると中断され、テーブルのプロファイリングは失敗として扱われます。


dm-run-serverコマンド
=====================

``dm-run-server`` コマンドは、指定したレポジトリのデータにネットワーク経由でアクセスするためのWebサーバを起動します。

``dm-run-server`` コマンドを使うことによって、以下を実現することができます。

* レポジトリのデータを都度HTMLにエクスポートしなくても閲覧可能
* レポジトリのデータの変更をリアルタイムに閲覧可能
* コメントやタグなど、一部のデータをWebブラウザから変更可能

::

  Usage: dm-run-server [repo file] [port]
  
  Options:
      --help                     Print this help.

``repo file`` はレポジトリファイル名です。

``port`` はWebサーバに接続するためのポート番号です。（デフォルト8080）

dm-verify-resultsコマンド
=========================

``dm-verify-results`` コマンドは、レポジトリ内に保存されたデータ検証結果を参照して、違反しているレコードがないかどうかを確認します。

違反しているレコードがある場合は終了コード ``1`` を、ない場合には終了コード ``0`` を返します。

::

  Usage: dm-verify-results [repo file]
  
  Options:
      --help                     Print this help.


``repo file`` はレポジトリファイル名です。
