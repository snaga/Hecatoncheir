#!/usr/bin/env python2.7
# -*- coding: utf-8 -*-

from copy import deepcopy
import re
import threading

from hecatoncheir import DbDriverBase, logger as log
from hecatoncheir.QueryResult import QueryResult
from hecatoncheir.exception import (DriverError, InternalError, QueryError,
                                    QueryTimeout)
from hecatoncheir.logger import to_unicode


class MSSQLDriver(DbDriverBase.DbDriverBase):
    host = None
    port = None
    dbname = None
    dbuser = None
    dbpass = None
    conn = None
    driver = None

    def __init__(self, host, dbname, dbuser, dbpass):
        self.host = host
        self.dbname = dbname
        self.dbuser = dbuser
        self.dbpass = dbpass

        name = "pymssql"
        try:
            self.driver = __import__(name, fromlist=[''])
        except Exception as e:
            raise DriverError(
                u"Could not load the driver module: %s" % name, source=e)

    def connect(self):
        try:
            self.conn = self.driver.connect(self.host, self.dbuser,
                                            self.dbpass, self.dbname)
        except Exception as e:
            # assuming OperationalError
            msg = to_unicode(e[0][1]).replace('\n', ' ')
            msg = re.sub(r'DB-Lib.*', '', msg)
            raise DriverError(
                u"Could not connect to the server: %s" % msg, source=e)

        return True

    def __get_spid(self):
        cur = self.conn.cursor()
        cur.execute("SELECT @@SPID")
        r = cur.fetchone()
        print(r)
        cur.close()
        return r[0]

    def cancel_callback(self):
        log.info("cancel_callback start")
        try:
            conn = self.driver.connect(self.host, self.dbuser, self.dbpass,
                                       self.dbname)
            cur = conn.cursor()
            log.info("KILL %d" % self.__spid)
            cur.execute("KILL %d" % self.__spid)
            cur.close()
            conn.close()
        except Exception as e:
            print(e)
            log.info("cancel_callback failed")
            raise e
        log.info("cancel_callback end")

    def query_to_resultset(self, query, max_rows=10000, timeout=None):
        """Build a QueryResult object from the query

        Args:
            query (str): a query string to be executed.
            max_rows (int): max rows which can be kept in a QueryResult object.

        Returns:
            QueryResult: an object holding query, column names and result set.
        """
        assert query
        assert isinstance(query, unicode)
        log.trace('query_to_resultset: start query=%s' % query)

        # FIXME: Query timeout is not supported on SQL Server.
        #
        # KILL <spid> does not work as expected so far.
        # See below for more information:
        # http://stackoverflow.com/questions/43529410/run-sql-kill-from-python
        if timeout:
            raise NotImplementedError(
                'Query timeout is not implemented on SQL Server')

        res = QueryResult(query)
        monitor = None
        try:
            if self.conn is None:
                self.connect()

            if timeout and int(timeout) > 0:
                self.__spid = self.__get_spid()
                monitor = threading.Timer(timeout, self.cancel_callback)
                monitor.start()

            cur = self.conn.cursor()
            cur.execute(res.query)

            desc = []
            if cur.description:
                for d in cur.description:
                    desc.append(d[0])
                res.column_names = deepcopy(tuple(desc))

                for i, r in enumerate(cur.fetchall()):
                    # let's consider the memory size.
                    if i > max_rows:
                        raise InternalError(
                           (u'Exceeded the record limit (%d) '
                            u'for QueryResult.' % max_rows),
                           query=query)
                    res.resultset.append(deepcopy(r))
            cur.close()
        except InternalError as e:
            raise e
        except DriverError as e:
            raise e
        except Exception as e:
            print(e)
            # assuming ProgrammingError
            msg = to_unicode(e[1]).replace('\n', ' ')
            msg = re.sub(r'DB-Lib.*', '', msg)
            raise QueryError(
                "Could not execute a query: %s" % msg,
                query=query, source=e)
        finally:
            if monitor:
                monitor.cancel()
            if self.conn:
                self.conn.rollback()
        log.trace('query_to_resultset: end')
        return res

    def disconnect(self):
        if self.conn is None:
            return False

        try:
            self.conn.close()
        except Exception as e:
            raise DriverError(
                u"Could not disconnect from the server: %s" % to_unicode(e),
                source=e)
        self.conn = None
        return True
