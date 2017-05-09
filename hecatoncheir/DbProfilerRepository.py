#!/usr/bin/env python2.7
# -*- coding: utf-8 -*-

from datetime import datetime
import json
import os
import sqlite3

import DbProfilerFormatter
from DbProfilerException import DbProfilerException, InternalError
import logger as log
from logger import str2unicode as _s2u
from msgutil import gettext as _


def are_same_tables(d1, d2):
    if d1['database_name'] != d2['database_name']:
        return False
    if d1['schema_name'] != d2['schema_name']:
        return False
    if d1['table_name'] != d2['table_name']:
        return False
    return True


class DbProfilerRepository():
    filename = None
    _conn = None

    def __init__(self, filename='repo.db'):
        if filename is None:
            self.filename = 'repo.db'
            return
        self.filename = filename

    def init(self):
        try:
            if os.path.exists(self.filename):
                log.info(_("The repository already exists."))
                return True
            self.__init_sqlite3(self.filename)
        except Exception as e:
            log.error(_("Could not create the repository."), detail=unicode(e))
            return False
        log.info(_("The repository has been initialized."))
        return True

    def __init_sqlite3(self, filename):
        conn = sqlite3.connect(filename)
        cursor = conn.cursor()

        query = """
create table repo (
  database_name text not null,
  schema_name text not null,
  table_name text not null,
  created_at text not null,
  data text not null
);
"""
        cursor.execute(query)

        query = """
create index database_schema_table_idx
  ON repo(database_name, schema_name, table_name);
"""
        cursor.execute(query)

        query = """
create index database_schema_table_created_idx
  ON repo(database_name, schema_name, table_name, created_at);
"""
        cursor.execute(query)

        query = """
create table datamapping (
  lineno integer not null,
  database_name text not null,
  schema_name text not null,
  table_name text not null,
  column_name text,
  record_id text,
  source_database_name text not null,
  source_schema_name text not null,
  source_table_name text not null,
  source_column_name text,
  created_at text not null,
  data text not null
);
"""
        cursor.execute(query)

        query = """
create index dm_dst_idx
  ON datamapping(database_name, schema_name, table_name);
"""
        cursor.execute(query)

        query = """
create index dm_dstc_idx
  ON datamapping(database_name, schema_name, table_name, column_name);
"""
        cursor.execute(query)

        query = """
create table tags (
  tag_id text not null,
  tag_label text not null
);
"""
        cursor.execute(query)

        query = """
create index tags_id_idx ON tags(tag_id);
"""
        cursor.execute(query)

        query = """
create index tags_label_idx ON tags(tag_label);
"""
        cursor.execute(query)

        query = """
create table business_glossary (
  id text not null,
  term text not null,
  description_short text not null,
  description_long text not null,
  created_at text not null,
  owned_by text not null,
  is_latest integer not null,
  categories text,
  synonyms text,
  related_terms text,
  assigned_assets text
);
"""
        cursor.execute(query)

        query = """
create table validation_rule (
  id integer primary key,
  database_name text not null,
  schema_name text not null,
  table_name text not null,
  column_name text not null,
  description text not null,
  rule text not null,
  param text,
  param2 text
);
"""
        cursor.execute(query)

        query = """
create unique index validation_rule_idx on
  validation_rule(database_name,schema_name,table_name,column_name,rule,param,param2);
"""
        cursor.execute(query)

        query = """
create table textelement (
  id_ text not null,
  text_ text not null
);
"""
        cursor.execute(query)

        conn.close()

    def destroy(self):
        try:
            if os.path.exists(self.filename):
                os.unlink(self.filename)
        except Exception as e:
            log.error(_("Could not destroy the repository."),
                      detail=unicode(e))
            return False
        log.info(_("The repository has been destroyed."))
        return True

    def set(self, data):
        try:
            cursor = self._conn.cursor()
            cursor.execute("DELETE FROM repo")
            self._conn.commit()
        except Exception as e:
            log.error(_("Could not initialize the repository."),
                      detail=unicode(e))
            return False

        for d in data:
            self.append_table(d)

        return True

    def get(self):
        jsondata = u""
        try:
            data_all = []

            cursor = self._conn.cursor()
            for r in cursor.execute("SELECT * FROM repo"):
                data_all.append(json.loads(unicode(r[4])))

            log.info(_("Retrieved all data from the repository `%s'.") %
                     self.filename)
        except Exception as e:
            log.error(_("Could not retreive from the repository `%s'") %
                      self.filename, detail=unicode(e))
            return None
        return data_all

    def get_schemas(self):
        """Get a list of database name and number of tables in each schema.

        Returns:
            list: a list of lists: [[dbname,schemaname,num of tables], ...]
        """
        log.trace("get_schemas: start")

        query = """
SELECT database_name,
       schema_name,
       COUNT(DISTINCT table_name)
  FROM repo
 GROUP BY
       database_name,
       schema_name
 ORDER BY
       database_name,
       schema_name
"""

        schemas = []
        try:
            cursor = self._conn.cursor()
            log.debug("get_schemas: query = %s" % query)
            cursor.execute(query)
            for r in cursor.fetchall():
                r2 = [_s2u(x) for x in r]
                schemas.append(r2)
        except Exception as e:
            log.trace("get_schemas: " + unicode(e))
            raise InternalError(_("Could not get schema names: "),
                                query=query, source=e)

        log.trace("get_schemas: end")
        return schemas

    def get_tags(self):
        """Get a list of tag names and number of tags associated with tables.

        Returns:
            list: a list of lists: [[tag,num of tables], ...]
        """
        log.trace("get_tags: start")

        query = """
SELECT tag_label,
       COUNT(*)
  FROM tags
 WHERE tag_label <> ''
 GROUP BY
       tag_label
 ORDER BY
       COUNT(*) DESC
"""

        tags = []
        try:
            cursor = self._conn.cursor()
            log.debug("get_tags: query = %s" % query)
            for r in cursor.execute(query):
                tags.append([r[0], r[1]])
        except Exception as e:
            log.trace("get_tags: " + unicode(e))
            raise InternalError(_("Could not get tag info: "),
                                query=query, source=e)

        log.trace("get_tags: end")
        return tags

    def has_table_record(self, tab):
        """
        Check if the table record exist in the repository.

        Args:
            tab: a dictionary of table record.

        Returns:
            True if the table record exists, otherwise False.
        """
        assert (tab['database_name'] and tab['schema_name'] and
                tab['table_name'])

        log.trace("has_table_record: start %s.%s.%s" %
                  (tab['database_name'], tab['schema_name'],
                   tab['table_name']))

        query = """
SELECT COUNT(*)
  FROM repo
 WHERE database_name = '{database_name}'
   AND schema_name = '{schema_name}'
   AND table_name = '{table_name}'
   AND created_at = datetime('{timestamp}')
""".format(**tab)

        try:
            cursor = self._conn.cursor()
            log.debug("has_table_record: query = %s" % query)
            cursor.execute(query)
            r = cursor.fetchone()
            log.debug("has_table_record: r = %s" % unicode(r))
            if r[0] > 0:
                return True
        except Exception as e:
            log.trace("has_table_record: " + unicode(e))
            raise InternalError(_("Could not get table info: "),
                                query=query, source=e)

        log.trace("has_table_record: end")
        return False

    def append_table(self, tab):
        """
        Update a table record if the same record (with same timestamp)
        already exist.
        Otherwise, append the table record to the repository.

        Args:
            tab: a dictionary of table record.

        Returns:
            True on success, otherwise False.
        """
        assert (tab['database_name'] and tab['schema_name'] and
                tab['table_name'] and tab['timestamp'])

        query = None

        log.trace("append_table: start %s.%s.%s" %
                  (tab['database_name'], tab['schema_name'],
                   tab['table_name']))

        try:
            if self.has_table_record(tab):
                query = """
UPDATE repo
   SET data = '%s'
 WHERE database_name = '{database_name}'
   AND schema_name = '{schema_name}'
   AND table_name = '{table_name}'
   AND created_at = datetime('{timestamp}')
""".format(**tab) % DbProfilerFormatter.jsonize(tab).replace("'", "''")
            else:
                query = """
INSERT INTO repo VALUES ('{database_name}','{schema_name}','{table_name}',
                         datetime('{timestamp}'), '%s')
""".format(**tab) % DbProfilerFormatter.jsonize(tab).replace("'", "''")
                log.trace("append_table: INSERT")

            log.debug("append_table: query = %s" % query)

            assert self._conn
            cursor = self._conn.cursor()

            assert cursor
            cursor.execute(query)
            self._conn.commit()
        except Exception as e:
            raise InternalError(_("Could not register table data: "),
                                query=query, source=e)

        # Remove all tag id/label pairs to replace with new ones.
        tagid = "%s.%s.%s" % (tab['database_name'], tab['schema_name'],
                              tab['table_name'])
        self.delete_tag_id(tagid)
        if tab.get('tags'):
            for label in tab['tags']:
                self.put_tag(tagid, label)

        log.trace("append_table: end")
        return True

    def get_table(self, database_name, schema_name, table_name):
        """
        Get a table record from the repository by object name.

        Args:
            database_name(str): database name
            schema_name(str):   schema name
            table_name(str):    table name

        Returns:
            a dictionary of table record. {table_record}
        """
        assert database_name and schema_name and table_name

        table = None
        log.trace('get_table: start %s.%s.%s' %
                  (database_name, schema_name, table_name))

        query = """
SELECT data
  FROM repo
 WHERE database_name = '{0}'
   AND schema_name = '{1}'
   AND table_name = '{2}'
 ORDER BY created_at DESC
 LIMIT 1
""".format(database_name, schema_name, table_name)

        log.debug("get_table: query = %s" % query)

        try:
            cursor = self._conn.cursor()
            cursor.execute(query)
            r = cursor.fetchone()
            if r:
                table = json.loads(unicode(r[0]))
        except Exception as e:
            raise InternalError(_("Could not get table data: "),
                                query=query, source=e)

        log.trace('get_table: end')
        return table

    def get_table_history(self, database_name, schema_name, table_name):
        """
        Get a table record history from the repository by object name,
        newest one coming first.

        Args:
            database_name(str): database name
            schema_name(str):   schema name
            table_name(str):    table name

        Returns:
            a list of dictionaries of table records. [{table record}, ...]
        """
        assert database_name and schema_name and table_name

        table_history = []

        query = """
SELECT data
  FROM repo
 WHERE database_name = '{0}'
   AND schema_name = '{1}'
   AND table_name = '{2}'
 ORDER BY created_at DESC
""".format(database_name, schema_name, table_name)

        log.trace("get_table_history: query = %s" % query)

        try:
            cursor = self._conn.cursor()
            for r in cursor.execute(query):
                table_history.append(json.loads(unicode(r[0])))
        except Exception as e:
            raise InternalError(
                _("Could not get table data with its history: "),
                query=query, source=e)

        return table_history

    def get_tag_labels(self, tag_id):
        labels = []
        try:
            cursor = self._conn.cursor()
            query = u"SELECT tag_label FROM tags WHERE tag_id = '%s'" % tag_id
            for r in cursor.execute(query):
                labels.append(r[0])
        except Exception as e:
            raise InternalError(_("Could not get tag labels: "),
                                query=query, source=e)
        return labels

    def get_tag_ids(self, tag_label):
        ids = []
        try:
            cursor = self._conn.cursor()
            query = (u"SELECT tag_id FROM tags WHERE tag_label = '%s'" %
                     tag_label)
            for r in cursor.execute(query):
                ids.append(r[0])
        except Exception as e:
            raise InternalError(_("Could not get tag ids: "),
                                query=query, source=e)
        return ids

    def delete_tag_id(self, tag_id):
        log.trace('delete_tag_id: start %s' % tag_id)
        try:
            cursor = self._conn.cursor()
            query = u"DELETE FROM tags WHERE tag_id = '%s'" % tag_id
            cursor.execute(query)
            self._conn.commit()
        except Exception as e:
            raise InternalError(_("Could not delete tag id: "),
                                query=query, source=e)
        log.trace('delete_tag_id: end')
        return True

    def put_tag(self, tag_id, tag_label):
        log.trace('put_tag: start %s %s' % (tag_id, tag_label))
        if not tag_label:
            return False
        try:
            cursor = self._conn.cursor()
            query = (u"DELETE FROM tags WHERE tag_id = '%s' "
                     u"AND tag_label = '%s'" %
                     (tag_id, tag_label))
            cursor.execute(query)
            query = (u"INSERT INTO tags VALUES ('%s', '%s')" %
                     (tag_id, tag_label))
            cursor.execute(query)
            self._conn.commit()
        except Exception as e:
            raise InternalError(_("Could not register tag: "),
                                query=query, source=e)
        log.trace('put_tag: end')
        return True

    def set_tag_comment(self, tag_label, tag_comment):
        self.delete_textelement(u'tag_comment:%s' % tag_label)
        return self.put_textelement(u'tag_comment:%s' % tag_label, tag_comment)

    def get_tag_comment(self, tag_label):
        elem = self.get_textelements(u'tag_comment:%s' % tag_label)
        return elem[0] if elem else None

    def set_schema_comment(self, schema_name, schema_comment):
        self.delete_textelement(u'schema_comment:%s' % schema_name)
        return self.put_textelement(u'schema_comment:%s' %
                                    schema_name, schema_comment)

    def get_schema_comment(self, schema_name):
        elem = self.get_textelements(u'schema_comment:%s' % schema_name)
        return elem[0] if elem else None

    def add_file(self, objtype, objid, filename):
        """Assign a file name to the object.

        Args:
            objtype(str): object type ['tag','schema','table']
            objid(str): object identifier
            filename(str): a file name or a file path.

        Returns:
            bool: True if succeeded or the file already exists.
        """
        if objtype not in ['tag', 'schema', 'table']:
            raise InternalError(_('invalid object type: %s') % objtype)

        if filename in self.get_files(objtype, objid):
            return True
        id_ = u'%s:%s' % (objtype, objid)
        return self.put_textelement(id_, filename)

    def get_files(self, objtype, objid):
        """Get file names assigned to the object.

        Args:
            objtype(str): object type ['tag','schema','table']
            objid(str): object identifier

        Returns:
            list: a list of file names.
        """
        if objtype not in ['tag', 'schema', 'table']:
            raise InternalError(_('invalid object type: %s') % objtype)

        id_ = u'%s:%s' % (objtype, objid)
        return self.get_textelements(id_)

    def delete_files(self, objtype, objid):
        """Remove file names associated to the object.

        Args:
            objtype(str): object type ['tag','schema','table']
            objid(str): object identifier

        Returns:
            bool: True if succeeded.
        """
        if objtype not in ['tag', 'schema', 'table']:
            raise InternalError(_('invalid object type: %s') % objtype)

        id_ = u'%s:%s' % (objtype, objid)
        return self.delete_textelement(id_)

    def put_textelement(self, id_, text):
        log.trace('put_textelement: start')
        try:
            cursor = self._conn.cursor()
            query = (u"INSERT INTO textelement VALUES ('%s', '%s')" %
                     (id_, text if text else ''))
            cursor.execute(query)
            self._conn.commit()
        except Exception as e:
            raise InternalError(_("Could not register text element: "),
                                query=query, source=e)
        log.trace('put_textelement: end')
        return True

    def get_textelements(self, id_):
        log.trace('get_textelements: start')
        texts = []
        try:
            cursor = self._conn.cursor()
            query = u"SELECT text_ FROM textelement WHERE id_= '%s'" % id_
            for r in cursor.execute(query):
                texts.append(r[0])
        except Exception as e:
            raise InternalError(_("Could not get text element: "),
                                query=query, source=e)
        log.trace('get_textelements: end')
        return texts

    def delete_textelement(self, id_):
        log.trace('delete_textelement: start')
        try:
            cursor = self._conn.cursor()
            query = u"DELETE FROM textelement WHERE id_= '%s'" % id_
            cursor.execute(query)
            self._conn.commit()
        except Exception as e:
            raise InternalError(_("Could not delete text element: "),
                                query=query, source=e)
        log.trace('delete_textelement: end')
        return True

    def get_table_list(self, database_name=None, schema_name=None,
                       table_name=None):
        table_list = []

        cond = []
        if database_name:
            cond.append("database_name = '%s'" % database_name)
        if schema_name:
            cond.append("schema_name = '%s'" % schema_name)
        if table_name:
            cond.append("table_name = '%s'" % table_name)
        where = "WHERE (%s)" % " AND ".join(cond) if cond else ''

        query = """
SELECT DISTINCT database_name, schema_name, table_name
  FROM repo
{0}
 ORDER BY database_name, schema_name, table_name
""".format(where)

        log.trace("get_table_list: query = %s" % query)

        try:
            cursor = self._conn.cursor()
            for r in cursor.execute(query):
                table_list.append([r[0], r[1], r[2]])
        except Exception as e:
            log.error(_("Could not get data."), detail=unicode(e))
            return None

        return table_list

    def merge(self, data1, data2):
        if data1 is None:
            return data2

        for d1 in data1:
            for d2 in data2:
                if are_same_tables(d1, d2):
                    data1.remove(d1)
                    log.trace(u"add: %s.%s %s -> %s" %
                              (d1['schema_name'], d1['table_name'],
                               d1['timestamp'], d2['timestamp']))
                log.trace(u"add: %s.%s" %
                          (d2['schema_name'], d2['table_name']))
        data1.extend(data2)
        return data1

    def get_datamap_items(self, database_name, schema_name, table_name,
                          column_name=None):
        """Get one or more datamap entries from the repository

        Args:
          database_name (str):
          schema_name_name (str):
          table_name (str):
          column_name (str):

        Returns:
          list: a list which consists of one or more datamap entries.
        """
        assert database_name and schema_name and table_name

        query = u"""
SELECT data FROM datamapping
WHERE database_name = '%s' AND schema_name = '%s' AND table_name = '%s'
""" % (database_name, schema_name, table_name)
        if column_name:
            query = query + u" AND column_name = '%s'" % column_name
        query = query + u"ORDER BY lineno"

        datamap = []
        try:
            cursor = self._conn.cursor()
            for r in cursor.execute(query):
                datamap.append(json.loads(r[0]))
        except Exception as e:
            raise InternalError(_("Could not get data."), query=query)

        return datamap

    def put_datamap_item(self, datamap):
        """Put a datamap entry into the repository

        Args:
          datamap (dict): a dictionary holding a datamap entry.

        Returns:
          bool: True on success, otherwise False.
        """
        assert (datamap.get('database_name') and datamap.get('schema_name') and
                datamap.get('table_name') and datamap.get('record_id'))

        for k in datamap:
            if isinstance(datamap[k], unicode):
                datamap[k] = datamap[k].replace("'", "''")

        query = u"""
DELETE FROM datamapping
 WHERE database_name = '%s'
   AND schema_name = '%s'
   AND table_name = '%s'
   AND column_name = '%s'
   AND record_id = '%s'
""" % (datamap['database_name'], datamap['schema_name'],
            datamap['table_name'], datamap.get('column_name', ''),
            datamap.get('record_id', ''))

        try:
            cursor = self._conn.cursor()
            cursor.execute(query)
            self._conn.commit()
            log.trace(u'Successfully removed the previous datamap entry: %s' %
                      datamap)
        except Exception as e:
            raise InternalError(_("Could not register data mapping."),
                                query=query)

        query = u"""
INSERT INTO datamapping (
  lineno,
  database_name,schema_name,table_name,column_name,
  record_id,
  source_database_name,source_schema_name,source_table_name,source_column_name,
  created_at,
  data)
 VALUES (
  %d,
  '%s', '%s', '%s', '%s',
  '%s',
  '%s', '%s', '%s', '%s',
  datetime('%s'), '%s')
""" % (datamap['lineno'], datamap['database_name'], datamap['schema_name'],
            datamap['table_name'], datamap.get('column_name', ''),
            datamap.get('record_id', ''),
            datamap.get('source_database_name', ''),
            datamap.get('source_schema_name', ''),
            datamap.get('source_table_name', ''),
            datamap.get('source_column_name', ''),
            datetime.now().isoformat(),
            json.dumps(datamap))

        try:
            cursor = self._conn.cursor()
            cursor.execute(query)
            self._conn.commit()
            log.trace(u'Successfully stored the datamap entry: %s' % datamap)
        except Exception as e:
            raise InternalError(_("Could not register data mapping."),
                                query=query)

        return True

    def get_datamap_source_tables(self, database_name, schema_name,
                                  table_name):
        """Get source talbe names from the data mapping info.

        Args:
          database_name (str):
          schema_name (str):
          tablename (str):

        Returns:
          list: a list of source table names.
        """
        assert database_name and schema_name and table_name

        tables = []
        for d in self.get_datamap_items(database_name, schema_name,
                                        table_name):
            if d['source_table_name'] not in tables:
                tables.append(d['source_table_name'])

        return tables

    def put_table_fk(self, database_name1, schema_name1, table_name1,
                     column_name1,
                     database_name2, schema_name2, table_name2, column_name2,
                     guess=False):
        """
        Returns:
            True when successfully added from fk and/or fk_ref.
            Otherwise, Flase.
        """
        if database_name1 != database_name2:
            return False

        ret = False

        tab1 = self.get_table(database_name1, schema_name1, table_name1)
        if tab1 is None:
            return False

        fk = '%s.%s.%s' % (schema_name2, table_name2, column_name2)
        if guess:
            fk = '?' + fk
        for col in tab1['columns']:
            if col['column_name'] == column_name1:
                if 'fk' not in col:
                    col['fk'] = []
                assert isinstance(col['fk'], list)
                if fk in col['fk']:
                    col['fk'].remove(fk)
                if fk not in col['fk']:
                    col['fk'].append(fk)
                    ret = True

        self.append_table(tab1)

        tab2 = self.get_table(database_name2, schema_name2, table_name2)
        if tab2 is None:
            return False

        fk_ref = '%s.%s.%s' % (schema_name1, table_name1, column_name1)
        if guess:
            fk_ref = '?' + fk_ref
        for col in tab2['columns']:
            if col['column_name'] == column_name2:
                if 'fk_ref' not in col:
                    col['fk_ref'] = []
                assert isinstance(col['fk_ref'], list)
                if fk_ref in col['fk_ref']:
                    col['fk_ref'].remove(fk_ref)
                if fk_ref not in col['fk_ref']:
                    col['fk_ref'].append(fk_ref)
                    ret = True

        self.append_table(tab2)

        return ret

    def remove_table_fk(self, database_name1, schema_name1, table_name1,
                        column_name1,
                        database_name2, schema_name2, table_name2,
                        column_name2):
        """
        Returns:
            True when successfully removed from fk and/or fk_ref.
            Otherwise, Flase.
        """
        if database_name1 != database_name2:
            return False

        ret = False

        tab1 = self.get_table(database_name1, schema_name1, table_name1)
        if tab1 is None:
            return False

        fk = '%s.%s.%s' % (schema_name2, table_name2, column_name2)
        for col in tab1['columns']:
            if col['column_name'] == column_name1:
                if 'fk' not in col or col['fk'] is None:
                    col['fk'] = []
                assert isinstance(col['fk'], list)
                if fk in col['fk']:
                    col['fk'].remove(fk)
                    ret = True

        self.append_table(tab1)

        tab2 = self.get_table(database_name2, schema_name2, table_name2)
        if tab2 is None:
            return False

        fk_ref = '%s.%s.%s' % (schema_name1, table_name1, column_name1)
        for col in tab2['columns']:
            if 'fk_ref' not in col or col['fk_ref'] is None:
                col['fk_ref'] = []
            assert isinstance(col['fk_ref'], list)
            if fk_ref in col['fk_ref']:
                col['fk_ref'].remove(fk_ref)
                ret = True

        self.append_table(tab2)

        return ret

    def clear_table_fk(self, database_name, schema_name, table_name,
                       column_name):
        """
        Returns:
            True when successfully cleared fk and/or fk_ref.
            Otherwise, Flase.
        """
        ret = False

        tab = self.get_table(database_name, schema_name, table_name)
        if tab is None:
            return False

        for col in tab['columns']:
            if col['column_name'] == column_name:
                if 'fk' in col and len(col['fk']) > 0:
                    assert isinstance(col['fk'], list)
                    col['fk'] = []
                    ret = True
                if 'fk_ref' in col and len(col['fk_ref']) > 0:
                    assert isinstance(col['fk_ref'], list)
                    col['fk_ref'] = []
                    ret = True
        self.append_table(tab)

        return ret

    def get_bg_term(self, term):
        """
        Returns:
            list: the term and its attributes if the term exists.
                  Otherwise, None.
        """
        data = None

        query = u"""
SELECT
  id,
  term,
  description_short,
  description_long,
  created_at,
  owned_by,
  categories,
  synonyms,
  related_terms,
  assigned_assets
FROM
  business_glossary
WHERE
  term = '%s'
AND
  is_latest = 1
""" % term
        try:
            cursor = self._conn.cursor()
            cursor.execute(query)
            r = cursor.fetchone()
            if r:
                data = {}
                for i, c in enumerate(r):
                    if cursor.description[i][0] in ['categories', 'synonyms',
                                                    'related_terms',
                                                    'assigned_assets']:
                        data[cursor.description[i][0]] = json.loads(c)
                    else:
                        data[cursor.description[i][0]] = c
        except Exception as e:
            raise InternalError(_("Could not get a business term: "),
                                query=query, source=e)
        return data

    def put_bg_term(self, term, desc_short, desc_long, owner, categories,
                    synonyms, related_terms, assigned_assets):
        """
        Returns:
            True when the term get registered successfully. Otherwise, False.
        """
        assert isinstance(categories, list) or categories is None
        assert isinstance(synonyms, list) or synonyms is None
        assert isinstance(related_terms, list) or related_terms is None
        assert isinstance(assigned_assets, list) or assigned_assets is None

        log.trace("put_bg_term: start")

        query1 = (u"UPDATE business_glossary SET is_latest = 0 "
                  u"WHERE term = '%s' AND is_latest = 1" % term)

        query2 = u"""
INSERT INTO business_glossary (
  id,
  term,
  description_short,
  description_long,
  created_at,
  owned_by,
  categories,
  synonyms,
  related_terms,
  assigned_assets,
  is_latest
) VALUES (
  {0},
  '{1}',
  '{2}',
  '{3}',
  datetime(),
  '{4}',
  '{5}',
  '{6}',
  '{7}',
  '{8}',
  1
)
""".format(0, term, desc_short, desc_long, owner,
           json.dumps(categories),
           json.dumps(synonyms),
           json.dumps(related_terms),
           json.dumps(assigned_assets))

        log.trace(query1)
        log.trace(query2)

        query = None
        try:
            cursor = self._conn.cursor()
            query = query1
            cursor.execute(query1)

            cursor = self._conn.cursor()
            query = query2
            cursor.execute(query2)

            self._conn.commit()
        except Exception as e:
            raise InternalError(_("Could not register a business term: "),
                                query=query, source=e)

        log.trace("put_bg_term: end")
        return True

    def get_bg_terms_all(self):
        """
        Returns:
            list: a list of terms in the business glossary.
        """
        query = (u"SELECT term FROM business_glossary WHERE is_latest = 1 "
                 u"ORDER BY length(term) desc,term")
        try:
            cursor = self._conn.cursor()
            cursor.execute(query)
            data = []
            for r in cursor.fetchall():
                data.append(r[0])
        except Exception as e:
            raise InternalError(_("Could not get a list of business terms: "),
                                query=query, source=e)
        return data

    def get_validation_rules(self, database_name=None, schema_name=None,
                             table_name=None, column_name=None,
                             description=None, rule=None,
                             param=None, param2=None):
        """
        Returns:
            list: a list of tuples containing  validation rules.
        """
        query = u"SELECT id FROM validation_rule"
        cond = []
        if database_name:
            cond.append("database_name = '%s'" % database_name)
        if schema_name:
            cond.append("schema_name = '%s'" % schema_name)
        if table_name:
            cond.append("table_name = '%s'" % table_name)
        if column_name:
            cond.append("column_name = '%s'" % column_name)
        if rule:
            cond.append("rule = '%s'" % rule)
        if param is not None:
            cond.append("param = '%s'" % param.replace("'", "''"))
        if param2 is not None:
            cond.append("param2 = '%s'" % param2.replace("'", "''"))
        if len(cond) > 0:
            query = query + " WHERE (%s)" % ' AND '.join(cond)

        ids = []
        try:
            cursor = self._conn.cursor()
            cursor.execute(query)
            for r in cursor.fetchall():
                ids.append(r[0])
        except Exception as e:
            raise InternalError(_("Could not get validation rules: "),
                                query=query, source=e)

        rules = []
        for i in ids:
            rules.append(self.get_validation_rule(i))
        return rules

    def create_validation_rule(self, database_name, schema_name, table_name,
                               column_name, description, rule,
                               param='', param2=''):
        """
        Args:
            database_name(str):
            schema_name(str):
            table_name(str):
            column_name(str):
            description(str):
            rule(str):
            param(str):
            param2(str):

        Returns:
            integer when the rule successfully gets created. None when already
            exists.
        """

        r = self.get_validation_rules(database_name, schema_name, table_name,
                                      column_name, description, rule,
                                      param, param2)
        assert len(r) <= 1
        if r:
            log.warning((_("The same validation rule already exists: ") +
                         u"{0},{1},{2},{3},{4},{5},{6},{7}"
                         .format(database_name, schema_name, table_name,
                                 column_name, description, rule,
                                 param, param2)))
            return None

        query = u"""
INSERT INTO validation_rule (id,database_name,schema_name,table_name,
                             column_name,description,rule,param,param2)
  VALUES ((SELECT coalesce(max(id),0)+1 FROM validation_rule),
          '{0}','{1}','{2}','{3}','{4}','{5}','{6}','{7}');
""".format(database_name, schema_name, table_name, column_name, description,
           rule,
           '' if param is None else "%s" % param.replace("'", "''"),
           '' if param2 is None else "%s" % param2.replace("'", "''"))

        log.trace("create_validation_rule: %s" % query.replace('\n', ''))
        id = None
        try:
            cursor = self._conn.cursor()
            cursor.execute(query)
            cursor.execute("SELECT max(id) FROM validation_rule")
            id = cursor.fetchone()[0]
            self._conn.commit()
        except Exception as e:
            raise InternalError(_("Could not register validation rule: "),
                                query=query, source=e)
        return id

    def get_validation_rule(self, id):
        """
        Args:
            id(integer):

        Returns:
            tuple: (id,database_name,schema_name,table_name,column_name,
                    description,rule,param,param2) or None
        """
        query = (u"SELECT id,database_name,schema_name,table_name,column_name,"
                 u"description,rule,param,param2 FROM validation_rule "
                 u"WHERE id = %d" % id)

        log.trace("get_validation_rule: %s" % query.replace('\n', ''))
        tup = None
        try:
            cursor = self._conn.cursor()
            cursor.execute(query)
            r = cursor.fetchone()
            if r:
                tup = tuple(r)
        except Exception as e:
            raise InternalError(_("Could not get validation rule: "),
                                query=query, source=e)
        return tup

    def update_validation_rule(self, id, database_name, schema_name,
                               table_name, column_name, description,
                               rule, param=None, param2=None):
        """
        Args:
            id(integer):
            database_name(str):
            schema_name(str):
            table_name(str):
            column_name(str):
            description(str):
            rule(str):
            param(str):
            param2(str):

        Returns:
            True when the rule successfully gets updated, otherwise False.
        """

        query = u"""
UPDATE validation_rule
   SET database_name = '{0}',
       schema_name = '{1}',
       table_name = '{2}',
       column_name = '{3}',
       description = '{4}',
       rule = '{5}',
       param = '{6}',
       param2 = '{7}'
 WHERE id = {8}
""".format(database_name, schema_name, table_name, column_name, description,
           rule,
           '' if param is None else "%s" % param,
           '' if param2 is None else "%s" % param2, id)

        log.trace("update_validation_rule: %s" % query.replace('\n', ''))
        rowcount = 0
        try:
            cursor = self._conn.cursor()
            cursor.execute(query)
            rowcount = cursor.rowcount
            self._conn.commit()
        except Exception as e:
            raise InternalError(_("Could not update validation rule: "),
                                query=query, source=e)
        if rowcount == 0:
            return False
        return True

    def delete_validation_rule(self, id):
        """
        Args:
            id(integer):

        Returns:
            True when the rule successfully gets deleted, otherwise False.
        """

        query = u"DELETE FROM validation_rule WHERE id = %s" % id

        log.trace("delete_validation_rule: %s" % query.replace('\n', ''))
        rowcount = 0
        try:
            cursor = self._conn.cursor()
            cursor.execute(query)
            rowcount = cursor.rowcount
            self._conn.commit()
        except Exception as e:
            raise InternalError(_("Could not delete validation rule: "),
                                query=query, source=e)
        if rowcount == 0:
            return False
        return True

    def open(self):
        if self._conn:
            log.info(_("The repository file `%s' has already been opened.") %
                     self.filename)
            return

        repo_found = False
        try:
            repo_found = os.path.exists(self.filename)
        except Exception as e:
            raise DbProfilerException(
                _("Could not access to the repository file `%s'.") %
                self.filename)

        if repo_found is False:
            raise InternalError(_("The repository file `%s' not found.") %
                                self.filename)

        try:
            self._conn = sqlite3.connect(self.filename)
        except Exception as e:
            raise DbProfilerException(
                _("Could not read the repository file `%s'.") % self.filename)

        assert self._conn
        log.info(_("The repository file `%s' has been opened.") %
                 self.filename)
        return

    def close(self):
        if self._conn:
            self._conn.close()
            self._conn = None
