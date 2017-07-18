#!/usr/bin/env python2.7
# -*- coding: utf-8 -*-

import copy
import json
import os
import re

import markdown
from jinja2 import Template, Environment, FileSystemLoader

import CSVUtils
import DbProfilerVerify
import logger as log
from exception import DbProfilerException, InternalError
from msgutil import DbProfilerJSONEncoder
from msgutil import gettext as _


def coalesce2(d, s, r):
    if s in d and d[s] is not None:
        return d[s]
    return r


def jsonize(data):
    return json.dumps(data, cls=DbProfilerJSONEncoder, sort_keys=True,
                      indent=2)


def format_non_null_ratio(rows, nulls):
    """Format percentage of non-null value of the column

    Args:
        rows (int): number of rows
        nulls (int): number of null-values

    Returns:
        str: non-null ratio with the format '%.2f'.
    """
    if rows is None or nulls is None:
        return 'N/A'

    if rows == 0:   # avoid devide by zero error.
        return "%.2f %%" % 0
    return "%.2f %%" % (float(rows - nulls) / rows * 100.0)


def format_cardinality(rows, cardinality, nulls):
    """Format cardinality of the column

    Args:
        rows (int): number of rows
        cardinality (int): number of distinct values
        nulls (int): number of null-values

    Returns:
        str: cardinality with the format '%.2f'.
    """
    if rows is None or cardinality is None or nulls is None:
        return "N/A"
    if rows == nulls:
        return 'N/A'

    return "%.02f" % (float(cardinality) / float(rows - nulls) * 100) + " %"


def format_data_type(d1, d2):
    if d2:
        return "%s (%s)" % (d1, d2)
    else:
        return "%s" % (d1)


def format_value_freq_ratio(rows, nulls, freq):
    """Format a ratio of the value in the table

    Calculate ratio of the value in the table, excluding nulls.

    Args:
        rows (int): number of rows.
        nulls (int): number of nulls.
        freq (int): frequency of the value.

    Returns:
        str: ratio of the value in the table with '%.2f' format
    """
    if rows is None or nulls is None or freq is None:
        return 'N/A'

    if rows - nulls > 0:
        return "%.2f %%" % (float(freq)/(rows-nulls)*100)
    return "%.2f %%" % .0


def get_validation_rule(column_name, rule, invalid):
    rule_s = CSVUtils.csv2list(rule)

    vali = {}
    vali['column_name'] = column_name
    vali['rule'] = rule_s[3]
    vali['condition'] = rule_s[4]
    if len(rule_s) >= 6 and rule_s[5]:
        vali['condition'] = vali['condition'] + "; " + rule_s[5]
    vali['invalid'] = invalid
    return vali


def filter_markdown2html(text):
    if text is None:
        return ''
    md = markdown.Markdown()
    return md.convert(text)


def filter_term2popover(html, pos, term, content, orig=None):
    log.trace("filter_term2popover: %s" % term)
    title = orig if orig else term
    popover = (u'<a tabindex="0" data-toggle="popover" data-trigger="focus" '
               u'data-html="true" title="{1}" data-content="{2}" '
               u'class="glossary-term">{0}</a>'.format(
                term, title, content))
    html_new = html[0:pos] + popover + html[pos+len(term):]
    pos += len(popover)
    return (html_new, pos)


def create_popover_content(term):
    assert isinstance(term, dict)
    synonyms = ', '.join(term.get('synonyms', []))
    if synonyms:
        synonyms = u'<br/>%s: ' % _("Synonym") + synonyms
    related = ', '.join(term.get('related_terms', []))
    if related:
        related = u'<br/>%s: ' % _("Related Terms") + related
    assets = ', '.join(term.get('assigned_assets', []))
    if assets:
        assets = u'<br/>%s: ' % _("Assigned Assets") + assets
    content = (u"{1}<br/>{2}{3}{4}<div align=right><a href='glossary.html#{0}'"
               u" target='_glossary'>{5}</a></div>".format
               (term['term'], term['description_short'], synonyms, related,
                assets, _("Details...")))
    return content


def filter_glossaryterms(html, glossary_terms):
    if glossary_terms is None:
        return html

    pos = 0
    while pos < len(html):
        for t in glossary_terms:
            term = t['term']
            desc = t['description_short']
            if html[pos:].startswith(term):
                content = create_popover_content(t)
                (html, pos) = filter_term2popover(html, pos, term, content)
            for synonym in t.get('synonyms', []):
                if not synonym:
                    continue
                if html[pos:].startswith(synonym):
                    content = create_popover_content(t)
                    (html, pos) = filter_term2popover(html, pos, synonym,
                                                      content, orig=term)
        pos += 1
    return html


def format_comment_tooltip(msg, maxlen=20):
    if not msg:
        return None
    tmp = re.sub('<[^>]+>', ' ', filter_markdown2html(msg))
    msg_short = tmp[0:maxlen] + '...' if len(tmp) > maxlen else tmp
    return msg_short


def get_default_template_path():
    return os.path.abspath(os.path.dirname(__file__) + "/templates/en")


def format_number(value):
    if not value:
        return 'N/A'
    try:
        n = long(value)
    except ValueError as ex:
        raise InternalError(_("Could not convert `%s' to long.") % value)
    return "{:,d}".format(n)


def format_minmax(minval, maxval):
    assert (minval is None and maxval is None) or (minval is not None and maxval is not None)

    if minval and maxval:
        return '[ %s, %s ]' % (unicode(minval)[0:20],
                               unicode(maxval)[0:20])
    return 'N/A'


def to_table_html(profile_data, validation_rules=None, datamapping=None,
                  files=None,
                  glossary_terms=None, template_file=None, editable=False):
    if not template_file:
        template_file = get_default_template_path() + "/templ_table.html"

    p = profile_data

    tab = {}
    tab['database_name'] = p['database_name']
    tab['schema_name'] = p['schema_name']
    tab['table_name'] = p['table_name']
    tab['timestamp'] = format_timestamp(p['timestamp'])
    tab['row_count'] = format_number(p.get('row_count'))
    tab['num_columns'] = len(p['columns'])
    tab['tags'] = ([x for x in p.get('tags') if len(x) > 0] if
                   p.get('tags') else [])
    tab['owner'] = p.get('owner')
    tab['sample_rows'] = p.get('sample_rows')

    # Have any comment to show?
    tab['comment'] = filter_markdown2html(p.get('comment'))
    tab['comment'] = filter_glossaryterms(tab['comment'], glossary_terms)

    tab['columns'] = []
    for c in p['columns']:
        col = {}
        col['column_name'] = c['column_name']
        col['column_name_nls'] = coalesce2(c, 'column_name_nls', "")
        col['column_name_nls'] = filter_glossaryterms(col['column_name_nls'],
                                                      glossary_terms)
        col['data_type'] = format_data_type(c['data_type'][0],
                                            c['data_type'][1])
        if 'fk' in c and c['fk']:
            col['fk'] = []
            for fk in c['fk']:
                guess = True if fk[0] == '?' else False
                tmp = fk[1:].split('.') if guess else fk.split('.')
                # db, table, column, guess
                col['fk'].append([tab['database_name'],
                                  tmp[0] + '.' + tmp[1],
                                  tmp[2], guess])

        if 'fk_ref' in c and c['fk_ref']:
            col['fk_ref'] = []
            for fk in c['fk_ref']:
                guess = True if fk[0] == '?' else False
                tmp = fk[1:].split('.') if guess else fk.split('.')
                # db, table, column, guess
                col['fk_ref'].append([tab['database_name'],
                                      tmp[0] + '.' + tmp[1],
                                      tmp[2], guess])

        col['minmax'] = format_minmax(c.get('min'), c.get('max'))

        # non-null ratio
        col['nulls'] = format_number(c.get('nulls'))
        col['non_null_ratio'] = format_non_null_ratio(p.get('row_count'),
                                                      c.get('nulls'))

        # cardinality
        col['cardinality'] = format_cardinality(p.get('row_count'),
                                                c['cardinality'],
                                                c.get('nulls'))

        # null/dist attributes
        col['uniq'] = False
        if ('most_freq_vals' in c and len(c['most_freq_vals']) > 0 and
                c['most_freq_vals'][0]["freq"] == 1):
            col['uniq'] = True
        col['notnull'] = True if c.get('nulls') == 0 else False

        # most freq values
        profile_most_freq_values_enabled = (len(c['most_freq_vals']) if
                                            'most_freq_vals' in c else 0)
        profile_least_freq_values_enabled = (len(c['least_freq_vals']) if
                                             'least_freq_vals' in c else 0)

        if 'most_freq_vals' in c:
            most_freq_vals = []
            for i, val in enumerate(c['most_freq_vals']):
                mfv = {}
                mfv['i'] = i
                mfv['value'] = val['value']
                mfv['freq'] = format_number(val['freq'])
                mfv['ratio'] = format_value_freq_ratio(p['row_count'], c['nulls'],
                                                       val['freq'])
                most_freq_vals.append(mfv)

            col['profile_most_freq_values_enabled'] = (
                profile_most_freq_values_enabled)
            col['most_freq_vals'] = most_freq_vals

        # least freq values
        if 'least_freq_vals' in c:
            least_freq_vals = []
            for i, val in enumerate(c['least_freq_vals']):
                lfv = {}
                lfv['i'] = i
                lfv['value'] = val['value']
                lfv['freq'] = format_number(val['freq'])
                lfv['ratio'] = format_value_freq_ratio(p['row_count'], c['nulls'],
                                                       val['freq'])
                least_freq_vals.append(lfv)

            col['profile_least_freq_values_enabled'] = (
                profile_least_freq_values_enabled)
            col['least_freq_vals'] = least_freq_vals

        # validation results
        if 'validation' in c:
            data_validation = []
            col_num_invalid = 0
            for v in c['validation']:
                tmp = {}
                tmp['column_name'] = ', '.join(v['column_names'])
                tmp['rule'] = '; '.join(v['rule'][1:])
                tmp['label'] = v['label']
                tmp['invalid'] = v['invalid_count']
                if v['description']:
                    tmp['desc'] = v['description']
                else:
                    tmp['desc'] = tmp['rule']
                data_validation.append(tmp)

                if v['invalid_count'] > 0:
                    col_num_invalid += 1
            if data_validation:
                col['validations'] = data_validation
                col['invalid'] = col_num_invalid
            else:
                assert 'validations' not in col

        # comment
        col['comment'] = filter_markdown2html(c.get('comment'))
        col['comment'] = filter_glossaryterms(col['comment'], glossary_terms)
        col['comment_raw'] = c.get('comment', '')
        col['comment_tooltip'] = format_comment_tooltip(c.get('comment'), 140)

        # data mapping
        tab['datamapping'] = []
        prev = None
        if datamapping:
            col['datamapping'] = []
            for dm in datamapping:
                if dm['column_name'] == col['column_name']:
                    dm['transformation_role'] = (dm['transformation_role']
                                                 .replace('\n', '<br/>'))
                    dm['transformation_role'] = (dm['transformation_role']
                                                 .replace(' ', '&nbsp;'))
                    col['datamapping'].append(dm)
                    prev = dm
                elif dm['column_name'] is None or dm['column_name'] == '':
                    dm['source_table_name'] = (dm['source_table_name']
                                               .replace(',', '\n'))
                    dm['source_table_name'] = (dm['source_table_name']
                                               .replace('\n', '<br/>'))
                    dm['source_table_name'] = (dm['source_table_name']
                                               .replace(' ', '&nbsp;'))
                    tab['datamapping'].append(dm['source_table_name'])
            if len(col['datamapping']) == 0:
                del col['datamapping']

        # append column data
        tab['columns'].append(col)

    templ = get_template_from_file(template_file)
    html = templ.render(title='%s.%s' % (tab['schema_name'],
                                         tab['table_name']),
                        table=tab,
                        files=([(os.path.basename(p), p) for p in files] if
                               files else None),
                        validation_rules=validation_rules,
                        editable=editable)

    return html


def format_timestamp(ts):
    t = re.sub('\.\d+$', '', ts)
    t = re.sub(':\d+$', '', t)
    t = re.sub('T', ' ', t)
    return t


def get_template_from_file(filename, encoding='utf-8'):
    html_templ = u""
    try:
        for l in open(filename):
            html_templ = html_templ + l.decode(encoding)
    except Exception as e:
        raise DbProfilerException("Template file `%s' not found." % (filename))
    fsl = FileSystemLoader(os.path.join(os.path.dirname(filename), ''))
    return Environment(loader=fsl).from_string(html_templ)


def to_index_html(data, reponame, schemas=None, tags=None,
                  show_validation='none', comment=None,
                  files=None, glossary_terms=None,
                  template_file=None, editable=False, max_panels=99):
    if not template_file:
        template_file = get_default_template_path() + "/templ_index.html"

    if comment:
        comment = filter_markdown2html(comment)
        comment = filter_glossaryterms(comment, glossary_terms)

    tables = []
    valid_tables = 0
    invalid_tables = 0
    for t in data:
        tab = {}
        tab['database_name'] = t['database_name']
        tab['schema_name'] = t['schema_name']
        tab['table_name'] = t['table_name']
        tab['table_name_nls'] = coalesce2(t, 'table_name_nls', '')
        tab['table_name_nls'] = filter_glossaryterms(tab['table_name_nls'],
                                                     glossary_terms)
        tab['row_count'] = format_number(t.get('row_count'))
        tab['timestamp'] = format_timestamp(t['timestamp'])
        tab['num_columns'] = len(t['columns'])

        # Have one or more validation results?
        data_validation = []
        data_val_labels = []
        # check every column to look for validation results
        for c in t['columns']:
            assert 'validation' in c
            if c['validation'] is None:
                continue
            # look every rule on the single column
            for v in c['validation']:
                tmp = {}
                tmp['column_name'] = ', '.join(v['column_names'])
                tmp['rule'] = '; '.join(v['rule'][1:])
                tmp['label'] = v['label']
                tmp['invalid'] = v['invalid_count']
                if v['description']:
                    tmp['desc'] = v['description']
                else:
                    tmp['desc'] = tmp['rule']
                if tmp['label'] not in data_val_labels:
                    data_validation.append(tmp)
                    data_val_labels.append(tmp['label'])

            v, i = DbProfilerVerify.verify_column(c)
            log.debug("to_index_html: %s %d %d" % (c['column_name'], v, i))

        if data_validation:
            v, i = DbProfilerVerify.verify_table(t)
            tab['validation'] = data_validation
            tab['invalid'] = i
            if i > 0:
                invalid_tables += 1
            else:
                valid_tables += 1
        else:
            assert 'validation' not in tab

        # Have any comment to show?
        tab['comment'] = filter_markdown2html(t.get('comment'))
        tab['comment'] = filter_glossaryterms(tab['comment'], glossary_terms)
        tab['comment_tooltip'] = format_comment_tooltip(t.get('comment'), 140)

        tables.append(tab)

    templ_schemas = []
    if schemas:
        for s in schemas:
            assert len(s) >= 4
            ts = {}
            ts['dbname'] = s[0]
            ts['label'] = s[1]
            ts['tables'] = s[2]
            ts['desc'] = s[3]
            templ_schemas.append(ts)

    templ_tags = []
    if tags:
        for s in tags:
            assert len(s) >= 3
            ts = {}
            ts['label'] = s[0]
            ts['tables'] = s[1]
            ts['desc'] = s[2]
            templ_tags.append(ts)

    templ_validation = []
    if show_validation == 'both' and valid_tables == 0 and invalid_tables == 0:
        show_validation = 'none'
    if show_validation == 'both':
        templ_validation.append({'label': 'Valid', 'tables': valid_tables})
        templ_validation.append({'label': 'Invalid', 'tables': invalid_tables})
    elif show_validation == 'valid':
        templ_validation.append({'label': 'Valid', 'tables': valid_tables})
        templ_validation.append({})
    elif show_validation == 'invalid':
        templ_validation.append({})
        templ_validation.append({'label': 'Invalid', 'tables': invalid_tables})

    templ = get_template_from_file(template_file)
    html = templ.render(title='Data Catalog [%s]' % reponame,
                        comment=comment,
                        files=([(os.path.basename(p), p) for p in files] if
                               files else None),
                        tables=tables, tags=templ_tags[:max_panels],
                        schemas=templ_schemas[:max_panels],
                        tags_index=(len(templ_tags) > max_panels),
                        schemas_index=(len(templ_schemas) > max_panels),
                        validation=templ_validation,
                        editable=editable)

    return html


def to_glossary_html(glossary_terms=None, template_file=None, editable=False):
    if not template_file:
        template_file = get_default_template_path() + "/templ_glossary.html"

    terms = copy.deepcopy(glossary_terms) if glossary_terms else []

    for t in terms:
        t['description_short'] = filter_glossaryterms(
            t.get('description_short'), glossary_terms)

        t['description_long'] = filter_markdown2html(t.get('description_long'))
        t['description_long'] = filter_glossaryterms(
            t.get('description_long'), glossary_terms)

        t['categories'] = ', '.join(t.get('categories', []))
        t['synonyms'] = ', '.join(t.get('synonyms', []))
        t['related_terms'] = ', '.join(t.get('related_terms', []))
        t['related_terms'] = filter_glossaryterms(t['related_terms'],
                                                  glossary_terms)
        aa2 = t.get('assigned_assets2')
        s = []
        for aa in t.get('assigned_assets', []):
            if aa2.get(aa) and len(aa2[aa]) >= 1:
                n = aa2[aa][0]
                m = ''
                if len(aa2[aa]) > 1:
                    m = (u'data-toggle="tooltip" data-placement="top" '
                         u'title="該当するテーブル名が2つ以上あります"')
                s.append(u'<a href="{0}.html" target=_blank {2}>{1}</a>'
                         .format(n, aa, m))
            else:
                s.append(aa)
        t['related_assets'] = ', '.join(s)

    templ = get_template_from_file(template_file)
    html = templ.render(title='Business Glossary',
                        terms=terms, editable=editable)

    return html
