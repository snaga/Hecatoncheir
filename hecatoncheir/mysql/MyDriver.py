#!/usr/bin/env python2.7
# -*- coding: utf-8 -*-

from copy import deepcopy
from decimal import Decimal
import threading

from hecatoncheir import DbDriverBase, logger as log
from hecatoncheir.QueryResult import QueryResult
from hecatoncheir.exception import (DriverError, InternalError, QueryError,
                                    QueryTimeout)


class MyDriver(DbDriverBase.DbDriverBase):
    host = None
    port = None
    dbname = None
    dbuser = None
    dbpass = None
    conn = None
    driver = None

    def __init__(self, host, port, dbname, dbuser, dbpass):
        self.host = host
        self.port = port
        self.dbname = dbname
        self.dbuser = dbuser
        self.dbpass = dbpass

        name = "MySQLdb"
        try:
            self.driver = __import__(name, fromlist=[''])
        except Exception as e:
            raise DriverError(
                "Could not load the driver module: %s" % name, source=e)

    def connect(self):
        try:
            self.conn = self.driver.connect(host=self.host, db=self.dbname,
                                            user=self.dbuser,
                                            passwd=self.dbpass)
        except Exception as e:
            raise DriverError(
                u"Could not connect to the server: %s" %
                e.args[1].split('\n')[0], source=e)
        return True

    def cancel_callback(self):
        log.trace("cancel_callback start")
        try:
            conn = self.driver.connect(host=self.host, db=self.dbname,
                                       user=self.dbuser,
                                       passwd=self.dbpass)
            cur = conn.cursor()
            log.trace("KILL %d" % self.conn_id)
            cur.execute("KILL %d" % self.conn_id)
            cur.close()
            conn.close()
        except Exception as e:
            log.trace("cancel_callback failed")
            raise e
        log.trace("cancel_callback end")

    def __get_connection_id(self):
        cur = self.conn.cursor()
        cur.execute("SELECT CONNECTION_ID()")
        r = cur.fetchone()
        cur.close()
        return r[0]

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

        res = QueryResult(query)
        monitor = None
        try:
            if self.conn is None:
                self.connect()

            if timeout and int(timeout) > 0:
                self.conn_id = self.__get_connection_id()
                monitor = threading.Timer(timeout, self.cancel_callback)
                monitor.start()

            cur = self.conn.cursor()
            cur.execute(res.query)

            desc = []
            for d in cur.description:
                desc.append(d[0])
            res.column_names = deepcopy(tuple(desc))

            for i, r in enumerate(cur.fetchall()):
                # let's consider the memory size.
                if i > max_rows:
                    raise InternalError(
                        u'Exceeded the record limit (%d) for QueryResult.' %
                        max_rows, query=query)
                res.resultset.append(deepcopy([float(x) if
                                               isinstance(x, Decimal) else
                                               x for x in r]))
            cur.close()
        except InternalError as e:
            raise e
        except DriverError as e:
            raise e
        except Exception as e:
            raise QueryError(
                "Could not execute a query: %s" %
                e.args[1].split('\n')[0], query=query, source=e)
        finally:
            if monitor:
                monitor.cancel()
            if self.conn:
                try:
                    self.conn.rollback()
                except Exception as e:
                    if (e.args[0] == 2006 and
                            e.args[1] == 'MySQL server has gone away'):
                        self.connect()
                        raise QueryTimeout(
                            "Query timeout: %s" % query,
                            query=query, source=e)
                    else:
                        raise InternalError("unexepected error.", source=e)
        log.trace('query_to_resultset: end')
        return res

    def disconnect(self):
        if self.conn is None:
            return False

        try:
            self.conn.close()
        except Exception as e:
            raise DriverError(
                u"Could not disconnect from the server: %s" %
                e.args[1].split('\n')[0], source=e)

        self.conn = None
        return True
