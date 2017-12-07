import unittest

import sqlalchemy as sa

conn = None
creds = None


def connect():
    global creds
    global conn
    assert isinstance(creds, dict)

    host = creds.get('host', 'localhost')
    port = creds.get('port', 5432)
    dbname = creds.get('dbname', 'postgres')
    user = creds.get('username', 'postgres')
    password = creds.get('password', '')
    use_sqlite = creds.get('use_sqlite')

    if use_sqlite:
        connstr = 'sqlite:///' + dbname + '.db'
    else:
        connstr = 'postgresql://{3}:{4}@{0}:{1}/{2}'.format(host, port, dbname, user, password)
    print(connstr)

    engine = sa.create_engine(connstr)
    conn = engine.connect()
    return conn


def version():
    global conn
    rs = conn.execute("SELECT version()")
    for r in rs:
        return r[0]


class TestTag2(unittest.TestCase):
    def setUp(self):
        global creds
        creds = {}
        creds['username'] = 'postgres'
        creds['password'] = 'postgres'
        creds['dbname'] = 'datacatalog'

    def test_connect_001(self):
        self.assertIsNotNone(connect())

    def test_connect_002(self):
        creds['use_sqlite'] = True
        self.assertIsNotNone(connect())

    def test_version_001(self):
        connect()
        self.assertTrue(version().startswith('PostgreSQL '))


if __name__ == '__main__':
    unittest.main()
