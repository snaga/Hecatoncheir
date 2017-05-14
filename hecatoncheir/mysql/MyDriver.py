#!/usr/bin/env python2.7
# -*- coding: utf-8 -*-

from copy import deepcopy
from decimal import Decimal

from hecatoncheir import DbDriverBase
from hecatoncheir.DbProfilerException import (DriverError, InternalError,
                                              QueryError)
from hecatoncheir.QueryResult import QueryResult
from hecatoncheir import logger as log


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
            raise DriverError(
                u"Could not disconnect from the server: %s" %
                e.args[1].split('\n')[0], source=e)

        self.conn = None
        return True
