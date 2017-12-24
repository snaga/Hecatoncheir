#!/usr/bin/env python2.7
# -*- coding: utf-8 -*-

from copy import deepcopy
import sys

from hecatoncheir import DbDriverBase, logger as log
from hecatoncheir.QueryResult import QueryResult
from hecatoncheir.exception import (DriverError, InternalError, QueryError,
                                    QueryTimeout)


class BigQueryDriver(DbDriverBase.DbDriverBase):
    project = None
    conn = None
    driver = None

    def __init__(self, project):
        self.project = project

        name = "google.cloud.bigquery"
        try:
            self.driver = __import__(name, fromlist=['google.cloud'])
        except Exception as e:
            raise DriverError(
                u"Could not load the driver module: %s" % name, source=e)

    def connect(self):
        try:
            self.conn = self.driver.Client(project=self.project)
        except Exception as ex:
            raise DriverError(
                u"Could not connect to the server: %s" %
                ex.args[0].split('\n')[0], source=ex)

        return True

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
        try:
            if not self.conn:
                self.connect()

            query_job = self.conn.query(res.query)
            assert query_job
            res_iter = query_job.result(timeout=timeout)
            assert query_job.state == 'DONE'

            desc = []
            for f in query_job.query_results().schema:
                desc.append(f.name)
            res.column_names = deepcopy(tuple(desc))

            for row in res_iter:
                # let's consider the memory size.
                if len(res.resultset) > max_rows:
                    raise InternalError(
                        u'Exceeded the record limit (%d) for QueryResult.' %
                        max_rows, query=query)
                res.resultset.append(deepcopy(list(row)))
        except InternalError as ex:
            raise ex
        except DriverError as ex:
            raise ex
        except Exception as ex:
            msg = unicode(ex)
            if msg == ('Operation did not complete within '
                       'the designated timeout.'):
                raise QueryTimeout(
                    "Query timeout: %s" % query,
                    query=query, source=ex)
            raise QueryError(
                "Could not execute a query: %s" % msg,
                query=query, source=ex)

        log.trace('query_to_resultset: end')
        return res

    def disconnect(self):
        if self.conn is None:
            return False

        self.conn = None
        return True
