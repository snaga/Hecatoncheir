#!/usr/bin/env python2.7
# -*- coding: utf-8 -*-

import copy
import getopt
import json
import os
import re
import shutil
import sys

import CSVUtils
import DbProfilerFormatter
import DbProfilerRepository
import DbProfilerVerify
import logger as log
from CSVUtils import list2csv
from msgutil import gettext as _
from schema import Schema2
from tag import Tag2


def export_file(filename, body):
    try:
        f = open(filename, "w")
        f.write(body.encode('utf-8'))
        f.close()
        log.info(_("Generated %s.") % filename)
    except IOError as e:
        log.error(_("Could not generate %s: %s") % (filename, unicode(e)))
        return False
    return True


def parse_table_name(s):
    assert isinstance(s, unicode)
    ss = s.split('.')
    table = None
    if len(ss):
        table = ss.pop()
    schema = None
    if len(ss):
        schema = ss.pop()
    db = None
    if len(ss):
        db = ss.pop()
    return (db, schema, table)


# top_schemas: list of schema name string: [u's5']
# all_schemas: list of Schema2 objects: [Schema2, Schema2, Schema2, ...]
# schema_index: list of lists (dbname, schemaname, num_of_tables, desc)
#        [['db', 's5', 1, 'desc'], ['db', 's1', 1, 'desc'], ['db', 's2', 1, 'desc'], ...]
def get_schema_ordered_list(top_schemas, all_schemas):
    assert isinstance(top_schemas, list) or top_schemas is None
    assert isinstance(all_schemas, list)

    schema_index = []
    for schema in all_schemas:
        schema_index.append([schema.database_name, schema.schema_name,
                             schema.num_of_tables, schema.description])
    if not top_schemas:
        return schema_index
    # move top-level shcmeas to beginning of the list.
    for top in reversed(top_schemas):
        assert isinstance(top, unicode)
        for i, schema in enumerate(schema_index):
            if schema[1] == top:
                tmp = schema_index.pop(i)
                schema_index.insert(0, tmp)
    return schema_index


# top_tags: list of label string: [u's5', u's3']
# all_tags: list of Tag2 objects: [Tag2, Tag2, Tag2, ...]
# tag_index: list of the lists (schema, num_of_tables, desc):
#     [['s5', 1, 'desc'], ['s3', 1, 'desc'], ['s1', 1, 'desc'], ...]
def get_tag_ordered_list(top_tags, all_tags):
    assert isinstance(top_tags, list) or top_tags is None
    assert isinstance(all_tags, list)

    tag_index = []
    for tag in all_tags:
        tag_index.append([tag.label, tag.num_of_tables, tag.description])

    if not top_tags:
        return tag_index
    # move top-level tags to beginning of the list.
    for top in reversed(top_tags):
        assert isinstance(top, unicode)
        for i, tag in enumerate(tag_index):
            if tag[0] == top:
                tmp = tag_index.pop(i)
                tag_index.insert(0, tmp)
    return tag_index


def export_html(repo, tables=[], tags=[], schemas=[], template_path=None,
                output_title='title', output_path='./html'):
    """
    Args:
        repo (obj): a DbProfilerRepository object.
        tables (list): a list of table names:
                       [<schema>.<table>,<schema2>.<table2>,...]
        tags (list): a list of tag names to export: [<tag1>,<tag2>,...]
        schemas (list): a list of schema names to export:
                        [<schema1>,<schema2>,...]
        template_path (str): a path to the template directory.
        output_title (str): a title of the index page.
        output_path (str): a path to the output directory to export data.

    Returns:
        True if succeed.
    """
    if not template_path:
        template_path = DbProfilerFormatter.get_default_template_path()
    template_index = template_path + "/templ_index.html"
    template_table = template_path + "/templ_table.html"
    template_glossary = template_path + "/templ_glossary.html"
    template_static = template_path + "/static"

    terms = []
    for term in repo.get_bg_terms_all():
        t = repo.get_bg_term(term)
        asset_names = {}
        for a in t['assigned_assets']:
            n = parse_table_name(a)
            if not n[0] and not n[1] and not n[2]:
                continue
            table_list = repo.get_table_list(n[0], n[1], n[2])
            asset_names[a] = ['.'.join(x) for x in table_list]
        t['assigned_assets2'] = asset_names
        terms.append(t)

    tables_all = []
    tables_by_schema = {}
    tables_by_tag = {}
    tables_valid = []
    tables_invalid = []
    for tab in tables:
        database_name = tab[0]
        schema_name = tab[1]
        table_name = tab[2]
        data = repo.get_table(database_name, schema_name, table_name)
        dmentries = repo.get_datamap_items(database_name, schema_name,
                                           table_name)
        files = (repo.get_files('table', '.'.join(tab[0:3])) if
                 repo.get_files('table', '.'.join(tab[0:3])) else [])

        filename = output_path + ("/%s.%s.%s.html" %
                                  (database_name, schema_name, table_name))

        validation_rules = repo.get_validation_rules(database_name,
                                                     schema_name, table_name)
        export_file(filename, DbProfilerFormatter.to_table_html(
                data, validation_rules=validation_rules,
                datamapping=dmentries,
                files=['%s/%s' % ('.'.join(tab[0:3]), x) for x in files],
                glossary_terms=terms,
                template_file=template_table))

        # append table data to the global index list
        tables_all.append(data)

        # append table data to the schema dict
        # tables_by_schema (dict): {'db.schema': [table_data1,
        #                                         table_data2,...]}
        k = database_name + '.' + schema_name
        if k not in tables_by_schema:
            tables_by_schema[k] = []
        tables_by_schema[k].append(data)

        # append table data to the tag dict
        # tables_by_tag (dict): {'tag': [table_data1,
        #                                table_data2,...]}
        if data.get('tags') is not None:
            for t in data['tags']:
                if t == '':
                    continue
                if t not in tables_by_tag:
                    tables_by_tag[t] = []
                tables_by_tag[t].append(data)

        # append table data to the valid/invalid dict
        # tables_by_validation (dict): {'invalid': [table_data1,
        #                                           table_data2,...],
        #                               'valid': [table_data3,
        #                                         table_data4,...] }
        (valid, invalid) = DbProfilerVerify.verify_table(data)
        if invalid > 0:
            tables_invalid.append(data)
        elif valid > 0:
            tables_valid.append(data)

    # create index page for each schema from the schema dict
    for schema in tables_by_schema:
        d, s = schema.split('.')
        filename = output_path + "/%s.html" % schema
        files = (repo.get_files('schema', schema) if
                 repo.get_files('schema', schema) else [])
        ss = Schema2.find(d, s)
        desc = ss.description
        export_file(filename, DbProfilerFormatter.to_index_html(
                tables_by_schema[schema],
                comment=desc.comment if desc else None,
                files=['%s/%s' % (schema, x) for x in files],
                schemas=[[d, s, len(tables_by_schema[schema]),
                          desc.desc if desc else None]],
                reponame=schema, glossary_terms=terms,
                template_file=template_index))

    # create index page for each tag from the tag dict
    for tag in tables_by_tag:
        filename = output_path + "/tag-%s.html" % tag
        files = (repo.get_files('tag', tag) if
                 repo.get_files('tag', tag) else [])
        tmp = Tag2.find(tag)
        assert tmp
        export_file(filename, DbProfilerFormatter.to_index_html(
                tables_by_tag[tag],
                comment=tmp.comment,
                files=['tag-%s/%s' % (tag, x) for x in files],
                tags=[[tmp.label, tmp.num_of_tables, tmp.description]],
                reponame=tag, glossary_terms=terms,
                template_file=template_index))

    # create index page for validation results (valid/invalid)
    # from the valid/invalid dict
    filename = output_path + "/validation-valid.html"
    export_file(filename, DbProfilerFormatter.to_index_html(
            tables_valid, show_validation='valid', reponame='valid',
            glossary_terms=terms,
            template_file=template_index))
    filename = output_path + "/validation-invalid.html"
    export_file(filename, DbProfilerFormatter.to_index_html(
            tables_invalid, show_validation='invalid', reponame='invalid',
            glossary_terms=terms,
            template_file=template_index))

    # create global index page
    filename = output_path + "/index.html"

    schemas2 = get_schema_ordered_list(schemas, Schema2.findall())

    tags2 = get_tag_ordered_list(tags, Tag2.findall())

    export_file(filename, DbProfilerFormatter.to_index_html(
            tables_all, schemas=schemas2, tags=tags2,
            show_validation='both', reponame=output_title,
            glossary_terms=terms, template_file=template_index,
            max_panels=6))

    # create index page for tags
    filename = output_path + "/index-tags.html"

    export_file(filename, DbProfilerFormatter.to_index_html(
            tables_all, tags=tags2, reponame=output_title,
            glossary_terms=terms, template_file=template_index,
            max_panels=99))

    # create index page for schemas
    filename = output_path + "/index-schemas.html"

    export_file(filename, DbProfilerFormatter.to_index_html(
            tables_all, schemas=schemas2, reponame=output_title,
            glossary_terms=terms, template_file=template_index,
            max_panels=99))

    # create the busines glossary page
    filename = output_path + "/glossary.html"
    export_file(filename, DbProfilerFormatter.to_glossary_html(
            glossary_terms=terms, template_file=template_glossary))

    # copy static files
    try:
        shutil.copytree(template_static, output_path + '/static')
        log.info(_("Copied the static directory to `%s'.") % output_path)
    except OSError as e:
        if e.errno == 17:
            log.warning(_("Could not copy the static directory "
                          "because `%s/static' already exists.") % output_path)
        else:
            raise e

    return True


def export_json(repo, tables=[], output_path='./json'):
    json_data = []
    try:
        f = open(output_path + "/EXPORT.JSON", "a")
        for tab in tables:
            database_name = tab[0]
            schema_name = tab[1]
            table_name = tab[2]
            data = repo.get_table(database_name, schema_name, table_name)
            json_data.append(data)
        f.write(json.dumps(json_data, indent=2).encode('utf-8'))
        f.close()
        log.info(_("Generated JSON file."))
    except IOError, e:
        log.error(_("Could not generate JSON file."))
        sys.exit(1)

    return True


def export_csv(repo, tables=[], output_path='./csv', encoding=None):
    if not encoding:
        encoding = 'utf-8'

    try:
        f = open(output_path + "/TABLE_COLUMN_META.CSV", "w")
        f2 = open(output_path + "/TABLE_COLUMN_VALIDATION.CSV", "w")
        f_tab = open(output_path + "/TABLE_META.CSV", "w")

        header = [u'TIMESTAMP', u'DATABASE_NAME', u'SCHEMA_NAME',
                  u'TABLE_NAME', u'COLUMN_NAME', u'DATA_TYPE', u'DATA_LEN',
                  u'MIN', u'MAX', u'NULLS', u'NON_NULLS', u'CARDINARLITY']
        f.write(list2csv(header).encode(encoding) + "\n")

        header2 = [u'TIMESTAMP', u'DATABASE_NAME', u'SCHEMA_NAME',
                   u'TABLE_NAME', u'COLUMN_NAME', u'VALIDATION_RULE',
                   u'INVALID_RECORDS']
        f2.write(list2csv(header2).encode(encoding) + "\n")

        head_tab = [u'TIMESTAMP', u'DATABASE_NAME', u'SCHEMA_NAME',
                    u'TABLE_NAME', u'ROW_COUNT']
        f_tab.write(list2csv(head_tab).encode(encoding) + "\n")

        for tab in tables:
            database_name = tab[0]
            schema_name = tab[1]
            table_name = tab[2]
            hist = repo.get_table_history(database_name, schema_name,
                                          table_name)
            for data in hist:
                # table metadata
                tmp = data['timestamp'].replace('T', ' ')
                line = list2csv([re.sub(r'\.\d+$', '', tmp),
                                 database_name, schema_name, table_name,
                                 data.get('row_count', u'')])
                log.trace(line)
                f_tab.write(line.encode(encoding) + "\n")

                for c in data['columns']:
                    log.trace("export_csv: " + unicode(c))

                    # column metadata
                    tmp = data['timestamp'].replace('T', ' ')
                    if (data.get('row_count') is not None and
                            c.get('nulls') is not None):
                        non_null_values = data['row_count'] - c['nulls']
                    else:
                        non_null_values = None
                    line = list2csv([re.sub(r'\.\d+$', '', tmp),
                                     database_name, schema_name,
                                     table_name,
                                     c['column_name'], c['data_type'][0],
                                     c['data_type'][1],
                                     c['min'], c['max'], c['nulls'],
                                     non_null_values,
                                     c['cardinality']])
                    log.trace(line)
                    f.write(line.encode(encoding) + "\n")

                    # validation result
                    for k in c.get('validation', []):
                        tmp = data['timestamp'].replace('T', ' ')
                        # timestamp,db,schema,table,column,desc,result
                        line = list2csv([re.sub(r'\.\d+$', '', tmp),
                                         database_name, schema_name,
                                         table_name, c['column_name'],
                                         k['description'], k['invalid_count']])
                        f2.write(line.encode(encoding) + "\n")
        f.close()
        f2.close()
        f_tab.close()
        log.info(_("Generated CSV file."))
    except IOError, e:
        log.error(_("Could not generate CSV file."), detail=unicode(e))
        sys.exit(1)

    return True
