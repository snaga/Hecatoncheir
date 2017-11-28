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
    if not html:
        return ''
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
    if value is None:
        return 'N/A'
    try:
        n = long(value)
    except ValueError as ex:
        raise InternalError(_("Could not convert `%s' to long.") % value)
    return "{:,d}".format(n)


def format_minmax(minval, maxval):
    assert ((minval is None and maxval is None) or
            (minval is not None and maxval is not None))

    if minval and maxval:
        return '[ %s, %s ]' % (unicode(minval)[0:20],
                               unicode(maxval)[0:20])
    return 'N/A'


def is_column_unique(most_freq_vals):
    if not most_freq_vals:
        return False
    if most_freq_vals and most_freq_vals[0]['freq'] == 1:
        return True
    return False


def format_fks(dbname, fks):
    assert dbname
    assert isinstance(fks, list) or fks is None

    if fks is None:
        return []

    fk_list = []
    for fk in fks:
        guess = True if fk[0] == '?' else False
        tmp = fk[1:].split('.') if guess else fk.split('.')
        assert len(tmp) >= 3
        # db, table, column, guess
        fk_list.append([dbname,
                        tmp[0] + '.' + tmp[1],
                        tmp[2], guess])
    return fk_list


def format_freq_values(freq_vals, row_count, nulls):
    if not freq_vals:
        return []

    freq_vals_out = []
    for i, val in enumerate(freq_vals):
        fv = {}
        fv['i'] = i
        fv['value'] = val['value']
        fv['freq'] = format_number(val['freq'])
        # row_count and nulls can be None here.
        fv['ratio'] = format_value_freq_ratio(row_count, nulls,
                                              val['freq'])
        freq_vals_out.append(fv)
    return freq_vals_out


def format_validation_item(val):
    if not val:
        return None
    assert isinstance(val, dict)

    tmp = {}
    tmp['column_name'] = ', '.join(val['column_names'])
    tmp['rule'] = '; '.join(val['rule'][1:])
    tmp['label'] = val['label']
    tmp['invalid'] = val['invalid_count']
    if val['description']:
        tmp['desc'] = val['description']
    else:
        tmp['desc'] = tmp['rule']
    return tmp


def format_validation_items(vals):
    if not vals:
        return [], 0

    results = []
    num_invalid = 0
    appended = []
    for v in vals:
        if v['label'] in appended:
            continue
        results.append(format_validation_item(v))
        appended.append(v['label'])
        if v['invalid_count'] > 0:
            num_invalid += 1
    return (results, num_invalid)


def filter_plain2html(s):
    if not s:
        return ''
    return s.replace('\n', '<br/>').replace(' ', '&nbsp;')


def format_table_datamapping(datamapping):
    if not datamapping:
        return []
    assert isinstance(datamapping, list)
    mapping = []
    for dm in datamapping:
        assert 'source_table_name' in dm
        if dm.get('column_name'):
            continue
        # data mapping for tables
        dm['source_table_name'] = (dm['source_table_name']
                                   .replace(',', '\n'))
        dm['source_table_name'] = filter_plain2html(dm['source_table_name'])
        mapping.append(dm['source_table_name'])
    return mapping


def format_column_datamapping(datamapping, column_name):
    if not datamapping:
        return []
    assert isinstance(datamapping, list)
    assert column_name
    mapping = []
    for dm in datamapping:
        if not dm.get('column_name'):
            continue
        if dm['column_name'] == column_name:
            # data mapping for columns
            dm['source_table_name'] = (dm['source_table_name']
                                       .replace(',', '\n'))
            dm['source_table_name'] = (
                filter_plain2html(dm['source_table_name']))
            dm['transformation_role'] = (
                filter_plain2html(dm.get('transformation_role')))
            mapping.append(dm)
    return mapping


def format_column_metadata(colmeta, tabmeta, row_count, glossary_terms,
                           datamapping):
    col = {}
    col['column_name'] = colmeta['column_name']
    col['column_name_nls'] = colmeta.get('column_name_nls', '')
    col['column_name_nls'] = filter_glossaryterms(col['column_name_nls'],
                                                  glossary_terms)
    col['data_type'] = format_data_type(colmeta['data_type'][0],
                                        colmeta['data_type'][1])

    # foreign keys
    col['fk'] = format_fks(tabmeta['database_name'], colmeta.get('fk'))
    col['fk_ref'] = format_fks(tabmeta['database_name'], colmeta.get('fk_ref'))

    col['minmax'] = format_minmax(colmeta.get('min'), colmeta.get('max'))

    # non-null ratio
    col['nulls'] = format_number(colmeta.get('nulls'))
    col['non_null_ratio'] = format_non_null_ratio(row_count,
                                                  colmeta.get('nulls'))

    # cardinality
    col['cardinality'] = format_cardinality(row_count,
                                            colmeta['cardinality'],
                                            colmeta.get('nulls'))

    # null/dist attributes
    col['uniq'] = is_column_unique(colmeta.get('most_freq_vals'))
    col['notnull'] = True if colmeta.get('nulls') == 0 else False

    # most freq values
    col['most_freq_vals'] = format_freq_values(colmeta.get('most_freq_vals'),
                                               row_count,
                                               colmeta.get('nulls'))

    # least freq values
    col['least_freq_vals'] = format_freq_values(colmeta.get('least_freq_vals'),
                                                row_count,
                                                colmeta.get('nulls'))

    # validation results
    if colmeta.get('validation'):
        data_validation, col_num_invalid = (
            format_validation_items(colmeta['validation']))
        if data_validation:
            col['validations'] = data_validation
            col['invalid'] = col_num_invalid

    # comment
    col['comment'] = filter_markdown2html(colmeta.get('comment'))
    col['comment'] = filter_glossaryterms(col['comment'], glossary_terms)
    col['comment_raw'] = colmeta.get('comment', '')
    col['comment_tooltip'] = (
        format_comment_tooltip(colmeta.get('comment'), 140))

    # data mapping
    col['datamapping'] = (
        format_column_datamapping(datamapping, col['column_name']))

    return col


def format_table_metadata(tabmeta, glossary_terms, datamapping=None):
    tab = {}
    tab['database_name'] = tabmeta['database_name']
    tab['schema_name'] = tabmeta['schema_name']
    tab['table_name'] = tabmeta['table_name']
    tab['table_name_nls'] = filter_glossaryterms(tabmeta.get('table_name_nls'),
                                                 glossary_terms)
    tab['timestamp'] = format_timestamp(tabmeta['timestamp'])
    tab['row_count'] = format_number(tabmeta.get('row_count'))
    tab['num_columns'] = len(tabmeta['columns'])

    tab['tags'] = ([x for x in tabmeta.get('tags') if len(x) > 0] if
                   tabmeta.get('tags') else [])
    tab['owner'] = tabmeta.get('owner')
    tab['sample_rows'] = tabmeta.get('sample_rows')

    # Have any comment to show?
    tab['comment'] = filter_markdown2html(tabmeta.get('comment'))
    tab['comment'] = filter_glossaryterms(tab['comment'], glossary_terms)
    tab['comment_tooltip'] = (
        format_comment_tooltip(tabmeta.get('comment'), 140))

    tab['datamapping'] = format_table_datamapping(datamapping)

    return tab


def to_table_html(tabdata, validation_rules=None, datamapping=None,
                  files=None,
                  glossary_terms=None, template_file=None, editable=False):
    if not template_file:
        template_file = get_default_template_path() + "/templ_table.html"

    tab = format_table_metadata(tabdata, glossary_terms, datamapping)

    tab['columns'] = []
    for c in tabdata['columns']:
        col = format_column_metadata(c, tab, tabdata.get('row_count'),
                                     glossary_terms, datamapping)
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
        tab = format_table_metadata(t, glossary_terms)

        # check every column to look for validation results
        data_validation = []
        for c in t['columns']:
            if not c.get('validation'):
                continue
            # look every rule on the single column
            results, invalid = format_validation_items(c['validation'])
            data_validation.extend(results)

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
