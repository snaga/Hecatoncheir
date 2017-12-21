# -*- coding: utf-8 -*-

from datetime import datetime
import json
import os
import unittest

import sqlalchemy as sa

from repository import Repository
import db


def get_bg_term(term):
    """
    Get a glossary term in the dictionary format.

    FIXME:
    This function is kept for the backward compatibility for now,
    and should be removed by the further refactoring.

    Returns:
      dict: a dictionary of key-value pairs.
    """
    data = {}

    t = GlossaryTerm.find(term)

    data['term'] = t[0].term
    data['description_short'] = t[0].desc_short
    data['description_long'] = t[0].desc_long
    data['owned_by'] = t[0].owner
    data['categories'] = t[0].categories
    data['synonyms'] = t[0].synonyms
    data['related_terms'] = t[0].related_terms
    data['assigned_assets'] = t[0].assigned_assets

    return data


class GlossaryTerm:
    def __init__(self, term, desc_short, desc_long, owner,
               categories, synonyms, related_terms, assigned_assets):
        self.term = term
        self.desc_short = desc_short
        self.desc_long = desc_long
        self.owner = owner
        self.categories = categories
        self.synonyms = synonyms
        self.related_terms = related_terms
        self.assigned_assets = assigned_assets

    @staticmethod
    def create(term, desc_short, desc_long, owner,
               categories, synonyms, related_terms, assigned_assets):
        query = u"""
INSERT INTO business_glossary (
  id,
  term,
  description_short,
  description_long,
  owned_by,
  categories,
  synonyms,
  related_terms,
  assigned_assets,
  created_at,
  updated_at
) VALUES (
  {0},
  '{1}',
  '{2}',
  '{3}',
  '{4}',
  '{5}',
  '{6}',
  '{7}',
  '{8}',
  {9},
  {10}
)
""".format(0,
           term, desc_short, desc_long, owner,
           json.dumps(categories),
           json.dumps(synonyms),
           json.dumps(related_terms),
           json.dumps(assigned_assets),
           db.fmt_datetime(datetime.now().isoformat()),
           db.fmt_datetime(datetime.now().isoformat()))

        db.engine.execute(query)

        return GlossaryTerm(term, desc_short, desc_long, owner,
                            categories, synonyms, related_terms,
                            assigned_assets)

    @staticmethod
    def find(term=None):
        cond = ['1=1']
        if term:
            cond.append("term = '%s'" % term)

        query = u"""
SELECT
  term,
  description_short,
  description_long,
  owned_by,
  categories,
  synonyms,
  related_terms,
  assigned_assets
FROM
  business_glossary
WHERE
  {0}
ORDER BY
  term
""".format(' AND '.join(cond))

        rs = db.engine.execute(query)
        terms = []
        for r in rs:
            t = GlossaryTerm(r[0], r[1], r[2], r[3],
                             json.loads(r[4]), json.loads(r[5]), json.loads(r[6]),
                             json.loads(r[7]))
            terms.append(t)
        return terms

    def update(self):
        query = u"""
UPDATE
  business_glossary
SET
  description_short = {1},
  description_long = {2},
  owned_by = {3},
  categories = '{4}',
  synonyms = '{5}',
  related_terms = '{6}',
  assigned_assets = '{7}',
  updated_at = {8}
WHERE
  term = '{0}'
""".format(self.term,
           db.fmt_nullable(self.desc_short),
           db.fmt_nullable(self.desc_long),
           db.fmt_nullable(self.owner),
           json.dumps(self.categories),
           json.dumps(self.synonyms),
           json.dumps(self.related_terms),
           json.dumps(self.assigned_assets),
           db.fmt_datetime(datetime.now().isoformat()))

        db.engine.execute(query)

        return True

    def destroy(self):
        query = u"DELETE FROM business_glossary WHERE term = '%s'" % self.term

        db.engine.execute(query)

        return True


class TestBusinessGlossaryTerm(unittest.TestCase):
    def setUp(self):
        db.creds = {}
        db.creds['host'] = os.environ.get('PGHOST', 'localhost')
        db.creds['port'] = os.environ.get('PGPORT', 5432)
        db.creds['dbname'] = os.environ.get('PGDATABASE', 'datacatalog')
        db.creds['username'] = os.environ.get('PGUSER', 'postgres')
        db.creds['password'] = os.environ.get('PGPASSWORD', 'postgres')

        self.repo = Repository()
        self.repo.destroy()
        self.repo.create()

        self.maxDiff = None

    def tearDown(self):
        db.conn.close()

    def test_create_001(self):
        t = GlossaryTerm.create(u'term', 'sd2', 'ld', 'owner',
                                ['cat1','cat2'],
                                ['synA', 'synB'],
                                ['term2', 'term3'],
                                ['TAB1', 'TAB2'])

        self.assertTrue(isinstance(t, GlossaryTerm))
        self.assertEquals(u'term', t.term)
        self.assertEquals('sd2', t.desc_short)
        self.assertEquals('ld', t.desc_long)
        self.assertEquals('owner', t.owner)
        self.assertEquals(['cat1','cat2'], t.categories)
        self.assertEquals(['synA', 'synB'], t.synonyms)
        self.assertEquals(['term2', 'term3'], t.related_terms)
        self.assertEquals(['TAB1', 'TAB2'], t.assigned_assets)

        # unicode string
        t = GlossaryTerm.create(u'用語', u'短い説明', u'長い説明', u'所有者',
                                [u'カテゴリ1', 'uカテゴリ2'],
                                [u'同義語A', u'同義語B'],
                                [u'関連語2', u'関連語3'],
                                [u'テーブル1', u'テーブル2'])

        self.assertTrue(isinstance(t, GlossaryTerm))
        self.assertEquals(u'用語', t.term)
        self.assertEquals(u'短い説明', t.desc_short)
        self.assertEquals([u'テーブル1', u'テーブル2'], t.assigned_assets)

    def test_find_001(self):
        GlossaryTerm.create(u'term', 'sd2', 'ld', 'owner',
                            ['cat1','cat2'],
                            ['synA', 'synB'],
                            ['term2', 'term3'],
                            ['TAB1', 'TAB2'])
        # uncode string
        GlossaryTerm.create(u'用語', u'短い説明', u'長い説明', u'所有者',
                            [u'カテゴリ1', 'uカテゴリ2'],
                            [u'同義語A', u'同義語B'],
                            [u'関連語2', u'関連語3'],
                            [u'テーブル1', u'テーブル2'])

        d = GlossaryTerm.find('term')
        self.assertEquals(1, len(d))

        self.assertTrue(isinstance(d[0], GlossaryTerm))
        self.assertEquals(u'term', d[0].term)
        self.assertEquals('sd2', d[0].desc_short)
        self.assertEquals('ld', d[0].desc_long)
        self.assertEquals('owner', d[0].owner)
        self.assertEquals(['cat1','cat2'], d[0].categories)
        self.assertEquals(['synA', 'synB'], d[0].synonyms)
        self.assertEquals(['term2', 'term3'], d[0].related_terms)
        self.assertEquals(['TAB1', 'TAB2'], d[0].assigned_assets)

        d = GlossaryTerm.find(u'用語')
        self.assertEquals(1, len(d))
        self.assertEquals([u'テーブル1', u'テーブル2'], d[0].assigned_assets)

        # Get all terms
        d = GlossaryTerm.find()
        self.assertEquals(2, len(d))

        self.assertEquals(u'term', d[0].term)
        self.assertEquals(u'用語', d[1].term)


    def test_update_001(self):
        t = GlossaryTerm.create(u'term', 'sd2', 'ld', 'owner',
                                ['cat1','cat2'],
                                ['synA', 'synB'],
                                ['term2', 'term3'],
                                ['TAB1', 'TAB2'])

        t.desc_short = u'短い説明'
        t.desc_long = u'長い説明'
        t.owner = u'所有者'
        t.categories = [u'カテゴリ1', u'カテゴリ2']
        t.synonyms = [u'同義語A', u'同義語B']
        t.related_terms = [u'関連語2', u'関連語3']
        t.assigned_assets = [u'テーブル1', u'テーブル2']

        self.assertTrue(t.update())

        d = GlossaryTerm.find('term')
        self.assertEquals(1, len(d))

        self.assertEquals('term', d[0].term)
        self.assertEquals(u'短い説明', d[0].desc_short)
        self.assertEquals(u'長い説明', d[0].desc_long)
        self.assertEquals(u'所有者', d[0].owner)
        self.assertEquals([u'カテゴリ1', u'カテゴリ2'], d[0].categories)
        self.assertEquals([u'同義語A', u'同義語B'], d[0].synonyms)
        self.assertEquals([u'関連語2', u'関連語3'], d[0].related_terms)
        self.assertEquals([u'テーブル1', u'テーブル2'], d[0].assigned_assets)

    def test_destroy_001(self):
        t = GlossaryTerm.create(u'term', 'sd2', 'ld', 'owner',
                                ['cat1','cat2'],
                                ['synA', 'synB'],
                                ['term2', 'term3'],
                                ['TAB1', 'TAB2'])

        t = GlossaryTerm.create(u'term2', 'sd2', 'ld', 'owner',
                                ['cat1','cat2'],
                                ['synA', 'synB'],
                                ['term2', 'term3'],
                                ['TAB1', 'TAB2'])

        d = GlossaryTerm.find()
        self.assertEquals(2, len(d))

        d = GlossaryTerm.find('term2')
        self.assertEquals(1, len(d))
        self.assertTrue(d[0].destroy())

        d = GlossaryTerm.find()
        self.assertEquals(1, len(d))

        d = GlossaryTerm.find('term')
        self.assertEquals(1, len(d))
        self.assertTrue(d[0].destroy())

        d = GlossaryTerm.find()
        self.assertEquals(0, len(d))

if __name__ == '__main__':
    unittest.main()
