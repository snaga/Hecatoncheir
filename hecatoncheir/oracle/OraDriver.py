#!/usr/bin/env python2.7
# -*- coding: utf-8 -*-

from copy import deepcopy
import threading

from hecatoncheir import DbDriverBase, logger as log
from hecatoncheir.QueryResult import QueryResult
from hecatoncheir.exception import (DriverError, InternalError, QueryError,
                                    QueryTimeout)


class OraDriver(DbDriverBase.DbDriverBase):
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

        name = "cx_Oracle"
        try:
            self.driver = __import__(name, fromlist=[''])
        except Exception as e:
            msg = u"Could not load the driver module: %s" % name
            raise DriverError(msg, source=e)

    def connect(self):
        try:
            if self.host is not None and self.port is not None:
                # use host name and port number
                dsn_tns = self.driver.makedsn(self.host, self.port,
                                              self.dbname)
            else:
                # use tns name
                dsn_tns = self.dbname
            log.trace("dsn_tns: %s" % dsn_tns)
            self.conn = self.driver.connect(self.dbuser, self.dbpass, dsn_tns)
        except Exception as e:
            msg = (u"Could not connect to the server: %s" %
                   unicode(e).split('\n')[0])
            raise DriverError(msg, source=e)

        return True

    def cancel_callback(self):
        log.trace("cancel_callback start")
        try:
            self.conn.cancel()
        except Exception as e:
            log.trace("cancel_callback failed")
            raise e
        log.trace("cancel_callback end")

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
                        msg = (u'Exceeded the record limit (%d) '
                               'for QueryResult.' % max_rows)
                        raise InternalError(msg, query=query)
                    res.resultset.append(deepcopy(r))
            cur.close()
        except InternalError as e:
            raise e
        except DriverError as e:
            raise e
        except Exception as e:
            if str(e.args[0]).startswith('ORA-01013: '):
                raise QueryTimeout(
                    "Query timeout: %s" % query,
                    query=query, source=e)
            msg = "Could not execute a query: %s" % str(e.args[0]).decode('utf-8').split('\n')[0]
            raise QueryError(msg, query=query, source=e)
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
            msg = (u"Could not disconnect from the server: %s" %
                   unicode(e).split('\n')[0])
            raise DriverError(msg, source=e)
        self.conn = None
        return True
