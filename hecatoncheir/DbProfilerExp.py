#!/usr/bin/env python2.7
# -*- coding: utf-8 -*-

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
from msgutil import gettext as _


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
        export_file(filename, DbProfilerFormatter.to_index_html(
                tables_by_schema[schema],
                comment=repo.get_schema_comment(schema),
                files=['%s/%s' % (schema, x) for x in files],
                schemas=[[d, s, len(tables_by_schema[schema])]],
                reponame=schema, glossary_terms=terms,
                template_file=template_index))

    # create index page for each tag from the tag dict
    for tag in tables_by_tag:
        filename = output_path + "/tag-%s.html" % tag
        files = (repo.get_files('tag', tag) if
                 repo.get_files('tag', tag) else [])
        export_file(filename, DbProfilerFormatter.to_index_html(
                tables_by_tag[tag],
                comment=repo.get_tag_comment(tag),
                files=['tag-%s/%s' % (tag, x) for x in files],
                tags=[[tag, len(tables_by_tag[tag])]],
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

    schemas2 = []
    if schemas is None:
        schemas2 = repo.get_schemas()
    else:
        for s in repo.get_schemas():
            if s[1] in schemas:
                schemas2.append(s)

    if tags is None:
        tags = []
    tags2 = []
    tags3 = []
    for t in sorted(repo.get_tag_label_with_count()):
        if t[0] in tags:
            tags2.append(t)
        else:
            tags3.append(t)
    # sort by tags
    tags2 = [y for x in tags for y in tags2 if x == y[0]]
    tags2.extend(tags3)
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


def export_csv(repo, tables=[], output_path='./csv'):
    try:
        f = open(output_path + "/TABLE_COLUMN_META.CSV", "w")
        f2 = open(output_path + "/TABLE_COLUMN_VALIDATION.CSV", "w")
        f_tab = open(output_path + "/TABLE_META.CSV", "w")

        header = [u'TIMESTAMP', u'DATABASE_NAME', u'SCHEMA_NAME',
                  u'TABLE_NAME', u'COLUMN_NAME', u'DATA_TYPE', u'DATA_LEN',
                  u'MIN', u'MAX', u'NULLS', u'NON_NULLS', u'CARDINARLITY']
        f.write(str(list2csv(header)) + "\n")

        header2 = [u'TIMESTAMP', u'DATABASE_NAME', u'SCHEMA_NAME',
                   u'TABLE_NAME', u'COLUMN_NAME', u'VALIDATION_RULE',
                   u'INVALID_RECORDS']
        f2.write(str(list2csv(header2)) + "\n")

        head_tab = [u'TIMESTAMP', u'DATABASE_NAME', u'SCHEMA_NAME',
                    u'TABLE_NAME', u'ROW_COUNT']
        f_tab.write(str(list2csv(head_tab)) + "\n")

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
                                 data['row_count']])
                log.trace(line)
                f_tab.write(line.encode('utf-8') + "\n")

                for c in data['columns']:
                    log.trace("export_csv: " + unicode(c))

                    # column metadata
                    tmp = data['timestamp'].replace('T', ' ')
                    line = list2csv([re.sub(r'\.\d+$', '', tmp),
                                     database_name, schema_name,
                                     table_name,
                                     c['column_name'], c['data_type'][0],
                                     c['data_type'][1],
                                     c['min'], c['max'], c['nulls'],
                                     data['row_count'] - c['nulls'],
                                     c['cardinality']])
                    log.trace(line)
                    f.write(line.encode('utf-8') + "\n")

                    # validation result
                    if 'validation' in c:
                        for k in c['validation']:
                            tmp = data['timestamp'].replace('T', ' ')
                            line = list2csv([re.sub(r'\.\d+$', '', tmp),
                                             database_name, schema_name,
                                             table_name, c['column_name'],
                                             k, c['validation'][k]])
                            f2.write(str(line) + "\n")
        f.close()
        f2.close()
        f_tab.close()
        log.info(_("Generated CSV file."))
    except IOError, e:
        log.error(_("Could not generate CSV file."), detail=unicode(e))
        sys.exit(1)

    return True
