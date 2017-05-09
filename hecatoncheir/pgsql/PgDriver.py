#!/usr/bin/env python2.7
# -*- coding: utf-8 -*-

from copy import deepcopy

from hecatoncheir import DbDriverBase
from hecatoncheir import DbProfilerException
from hecatoncheir.QueryResult import QueryResult
from hecatoncheir import logger as log


class PgDriver(DbDriverBase.DbDriverBase):
    connstr = None
    dbuser = None
    dbpass = None
    conn = None
    driver = None

    def __init__(self, connstr, dbuser, dbpass):
        self.connstr = connstr
        self.dbuser = dbuser
        self.dbpass = dbpass

        name = "psycopg2"
        try:
            self.driver = __import__(name, fromlist=[''])
        except Exception as e:
            raise DbProfilerException.DriverError(
                u"Could not load the driver module: %s" % name, source=e)

    def connect(self):
        s = self.connstr + " user=" + self.dbuser + " password=" + self.dbpass
        try:
            self.conn = self.driver.connect(s)
        except Exception as e:
            raise DbProfilerException.DriverError(
                u"Could not connect to the server: %s" %
                e.args[0].split('\n')[0], source=e)

        return True

    def query_to_resultset(self, query, max_rows=10000):
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
        try:
            if self.conn is None:
                self.connect()

            cur = self.conn.cursor()
            cur.execute(res.query)

            desc = []
            for d in cur.description:
                desc.append(d[0])
            res.column_names = deepcopy(tuple(desc))

            for i, r in enumerate(cur.fetchall()):
                # let's consider the memory size.
                if i > max_rows:
                    raise DbProfilerException.InternalError(
                        u'Exceeded the record limit (%d) for QueryResult.' %
                        max_rows, query=query)
                res.resultset.append(deepcopy(r))
            cur.close()
        except DbProfilerException.InternalError as e:
            raise e
        except DbProfilerException.DriverError as e:
            raise e
        except Exception as e:
            msg = unicode(e).split('\n')[0]
            raise DbProfilerException.QueryError(
                "Could not execute a query: %s" % msg,
                query=query, source=e)
        finally:
            if self.conn:
                self.conn.rollback()

        log.trace('query_to_resultset: end')
        return res

    def q2rs(self, query, max_rows=10000):
        return self.query_to_resultset(query, max_rows)

    def disconnect(self):
        if self.conn is None:
            return False

        try:
            self.conn.close()
        except Exception as e:
            raise DbProfilerException.DriverError(
                u"Could not disconnect from the server: %s" %
                e.args[0].split('\n')[0], source=e)
        self.conn = None
        return True
