#!/bin/sh

export __DBPROF_TRACE=0
export LANGUAGE=en

function oracle()
{
    python2.7 testOraProfiler.py
    python2.7 testOracleDriver.py
}

function mysql()
{
    python2.7 testMyProfiler.py
    python2.7 testMySQLDriver.py
}

function pgsql()
{
    python2.7 testPgDriver.py
    python2.7 testPgProfiler.py
}

function mssql()
{
    python2.7 testMSSQLDriver.py
    python2.7 testMSSQLProfiler.py
}

python2.7 testCSVUtils.py
python2.7 testColumnValidationCounter.py
python2.7 testDbProfilerBase.py
python2.7 testDbProfilerExp.py
python2.7 testDbProfilerFormatter.py
python2.7 testDbProfilerRepository.py
python2.7 testDbProfilerValidator.py
python2.7 testDbProfilerVerify.py
python2.7 testDbProfilerViewer.py
python2.7 testEvalValidator.py
python2.7 testLogger.py
python2.7 testMetadata.py
python2.7 testRegexpValidator.py
python2.7 testSQLValidator.py
python2.7 testStatEvalValidator.py

if [ "$1" == "base" ]; then
    cat /dev/null
elif [ "$1" == "mysql" ]; then
    mysql
elif [ "$1" == "mssql" ]; then
    mssql
elif [ "$1" == "oracle" ]; then
    oracle
elif [ "$1" == "pgsql" ]; then
    pgsql
else
    mysql
    mssql
    oracle
    pgsql
fi
