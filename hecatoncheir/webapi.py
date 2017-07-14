#!/usr/bin/env python2.7
# -*- coding: utf-8 -*-

import json
import os
from datetime import datetime

from flask import request
from flask_restful import reqparse, abort, Api, Resource

from hecatoncheir import DbProfilerRepository
from hecatoncheir.CSVUtils import csv2list
from hecatoncheir.metadata import TagDesc


def open_repo():
    filename = os.environ["DBPROF_REPOFILE"]
    repo = DbProfilerRepository.DbProfilerRepository(filename)
    repo.open()
    return repo


class ApiHelper():
    api = None

    def __init__(self, app):
        self.api = Api(app)

        resources = [
            [TableItem, '/api/v1/table/<db>.<schema>.<table>'],
            [ColumnItem, '/api/v1/column/<db>.<schema>.<table>.<column>'],
            [TagDescriptionItem, '/api/v1/tag/<tag>'],
            [ValidationItem, '/api/v1/validation/<id_>']]

        for r in resources:
            self.api.add_resource(r[0], r[1])


class TableItem(Resource):
    """
    Table Item API

    Create:
      PUT /api/v1/table/<db>.<schema>.<table>
    Read:
      GET /api/v1/table/<db>.<schema>.<table>
    Update:
      POST /api/v1/table/<db>.<schema>.<table>
    Delete:
      DELETE /api/v1/table/<db>.<schema>.<table>
    """
    def put(self, db, schema, table):
        repo = open_repo()
        tab = repo.get_table(db, schema, table)
        if tab:
            repo.close()
            abort(500)

        data = json.loads(request.data)
        data['database_name'] = db
        data['schema_name'] = schema
        data['table_name'] = table
        data['timestamp'] = datetime.now().strftime("%Y-%m-%dT%H:%M:%S.%f")

        ret = repo.append_table(data)
        if not ret:
            repo.close()
            abort(500)
        tab = repo.get_table(db, schema, table)
        repo.close()
        if not tab:
            repo.close()
            abort(500)
        return tab

    def get(self, db, schema, table):
        repo = open_repo()
        tab = repo.get_table(db, schema, table)
        repo.close()
        if not tab:
            abort(404)
        return tab

    def post(self, db, schema, table):
        repo = open_repo()
        tab = repo.get_table(db, schema, table)
        if not tab:
            repo.close()
            abort(404)

        data = json.loads(request.data)
#        data = request.form.copy()
        data['database_name'] = db
        data['schema_name'] = schema
        data['table_name'] = table

        # append or update table info.
        for k in data:
            tab[k] = data[k]
        tab['tags'] = csv2list(tab.get('tags', u''))

        ret = repo.append_table(tab)
        repo.close()
        if not ret:
            abort(500)
        return tab

    def delete(self, db, schema, table):
        repo = open_repo()
        tab = repo.get_table(db, schema, table)
        if not tab:
            repo.close()
            abort(404)
        ret = repo.remove_table(db, schema, table)
        return ret


class ColumnItem(Resource):
    """
    Column Item API

    Create:
      PUT /api/v1/column/<db>.<schema>.<table>.<column>
    Read:
      GET /api/v1/column/<db>.<schema>.<table>.<column>
    Update:
      POST /api/v1/column/<db>.<schema>.<table>.<column>
    Delete:
      DELETE /api/v1/column/<db>.<schema>.<table>.<column>
    """
    def put(self, db, schema, table, column):
        repo = open_repo()
        tab = repo.get_table(db, schema, table)

        for col in tab.get('columns', []):
            if col['column_name'] == column:
                repo.close()
                abort(500)

        col = {}
        col['column_name'] = column
        data = json.loads(request.data)
        for k in data:
            col[k] = data[k]
        if 'columns' not in tab:
            tab['columns'] = []
        tab['columns'].append(col)

        ret = repo.append_table(tab)
        repo.close()
        if not ret:
            abort(500)
        return col

    def get(self, db, schema, table, column):
        repo = open_repo()
        tab = repo.get_table(db, schema, table)

        found = None
        for col in tab.get('columns', []):
            if col['column_name'] == column:
                found = col
                break
        if not found:
            repo.close()
            abort(404)

        repo.close()
        return found

    def post(self, db, schema, table, column):
        repo = open_repo()
        tab = repo.get_table(db, schema, table)

        for col in tab.get('columns', []):
            if col['column_name'] == column:
                found = col
                break
        if not found:
            repo.close()
            abort(404)

        col = found
        data = json.loads(request.data)
#        data = request.form.copy()
        for k in data:
            col[k] = data[k]
        ret = repo.append_table(tab)
        repo.close()
        if not ret:
            abort(500)
        return col

    def delete(self, db, schema, table, column):
        repo = open_repo()
        tab = repo.get_table(db, schema, table)

        found = False
        for i, col in enumerate(tab.get('columns', [])):
            if col['column_name'] == column:
                del tab['columns'][i]
                found = True
                break
        if not found:
            repo.close()
            abort(404)

        ret = repo.append_table(tab)
        repo.close()
        if not ret:
            abort(500)
        return ret


class TagDescriptionItem(Resource):
    """
    Tag Description Item API

    Create:
      PUT /api/v1/tag/<tag>
    Read:
      GET /api/v1/tag/<tag>
    Update:
      POST /api/v1/tag/<tag>
    Delete:
      DELETE /api/v1/tag/<tag>
    """
    def put(self, tag):
        data = json.loads(request.data)
        repo = open_repo()
        tdesc = repo.get_tag_description(tag)
        if tdesc:
            abort(500)
        repo.set_tag_description(TagDesc(tag,
                                         data.get('desc'),
                                         data.get('comment')))
        repo.close()
        return True

    def get(self, tag):
        repo = open_repo()
        tagdesc = repo.get_tag_description(tag)
        repo.close()
        if not tagdesc:
            abort(404)
        return tagdesc.__dict__

    def post(self, tag):
        data = json.loads(request.data)
        repo = open_repo()
        tdesc = repo.get_tag_description(tag)
        if not tdesc:
            abort(404)
        tdesc.desc = data.get('desc')
        tdesc.comment = data.get('comment')
        repo.set_tag_description(tdesc)
        repo.close()
        return True

    def delete(self, tag):
        repo = open_repo()
        tdesc = repo.get_tag_description(tag)
        if not tdesc:
            abort(404)
        repo.delete_tag_description(tdesc)
        repo.close()
        return True


class ValidationItem(Resource):
    """
    Validation Item API

    Create:
      PUT /api/v1/validation/0
    Read:
      GET /api/v1/validation/<id>
    Update:
      POST /api/v1/validation/<id>
    Delete:
      DELETE /api/v1/validation/<id>
    """
    def put(self, id_):
        data = json.loads(request.data)
        repo = open_repo()
        try:
            id_ = int(id_)
        except Exception as ex:
            abort(500)

        if id_ != 0:
            abort(500)

        try:
            res = repo.create_validation_rule(
                data['database_name'],
                data['schema_name'],
                data['table_name'],
                data['column_name'],
                data['description'],
                data['rule'],
                data.get('param'),
                data.get('param2'))

            if not res:
                abort(500)
        except Exception as ex:
            abort(500)

        repo.close()
        return res

    def get(self, id_):
        repo = open_repo()
        r = None
        try:
            id_ = int(id_)
            r = repo.get_validation_rule(id_)
        except Exception as ex:
            abort(500)
        repo.close()

        if not r:
            abort(404)

        resp = {'id': unicode(r[0]),
                'database_name': r[1],
                'schema_name': r[2],
                'table_name': r[3],
                'column_name': r[4],
                'description': r[5],
                'rule': r[6],
                'param': r[7],
                'param2': r[8]}
        return resp

    def post(self, id_):
        data = json.loads(request.data)
        repo = open_repo()
        try:
            id_ = int(id_)
            res = repo.update_validation_rule(
                id_,
                data['database_name'],
                data['schema_name'],
                data['table_name'],
                data['column_name'],
                data['description'],
                data['rule'],
                data.get('param'),
                data.get('param2'))
            if not res:
                abort(500)
        except Exception as ex:
            abort(500)
        repo.close()
        return True

    def delete(self, id_):
        repo = open_repo()
        try:
            id_ = int(id_)
            if repo.delete_validation_rule(id_) is False:
                abort(500)
        except Exception as ex:
            abort(500)
        repo.close()
        return True
