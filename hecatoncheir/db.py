import os
import unittest

import sqlalchemy as sa

conn = None
creds = None
engine = None


def parse_connection_string(connstr):
    creds = {}
    s = None
    if connstr.startswith('postgresql://'):
        creds['use_sqlite'] = False
        s = connstr[len('postgresql://'):]
    elif connstr.startswith('sqlite:///'):
        creds['use_sqlite'] = True
        creds['dbname'] = connstr[len('sqlite:///'):]
        return creds
    else:
        creds['use_sqlite'] = True
        creds['dbname'] = connstr
        return creds

    creds['dbname'] = None
    if s:
        t = s.split('/', 2)
        if len(t) == 2:
            creds['dbname'] = t[1]
        s = t[0]
    if not creds['dbname']:
        raise ValueError("Unrecognized connection string format: %s" % connstr)

    host_port = None
    user_pass = None
    if s:
        t = s.split('@', 2)
        if len(t) == 2:
            user_pass = t[0].split(':')
            host_port = t[1].split(':')
        elif len(t) == 1:
            user_pass = ['', '']
            host_port = t[0].split(':')

        if len(host_port) == 1:
            host_port.append('5432')
        if len(user_pass) == 1:
            user_pass.append('')

    if not host_port:
        raise ValueError("Unrecognized connection string format: %s" % connstr)

    if host_port[0]:
        creds['host'] = host_port[0]
    else:
        raise ValueError("Unrecognized connection string format: %s" % connstr)
    creds['port'] = host_port[1]

    if user_pass[0]:
        creds['username'] = user_pass[0]
    else:
        creds['username'] = os.environ.get('USER')
    creds['password'] = user_pass[1]

    return creds


def connect():
    global creds
    global conn
    global engine
    assert isinstance(creds, dict)

    host = creds.get('host', 'localhost')
    port = creds.get('port', 5432)
    dbname = creds.get('dbname', 'postgres')
    user = creds.get('username', 'postgres')
    password = creds.get('password', '')
    use_sqlite = creds.get('use_sqlite')

    if use_sqlite:
        connstr = 'sqlite:///' + dbname
    else:
        connstr = 'postgresql://{3}:{4}@{0}:{1}/{2}'.format(host, port, dbname,
                                                            user, password)

    engine = sa.create_engine(connstr)
    conn = engine.connect()
    return conn


def version():
    global conn
    rs = conn.execute("SELECT version()")
    for r in rs:
        return r[0]


def fmt_datetime(ts):
    if str(engine).startswith('Engine(postgresql://'):
        return "'%s'" % ts
    return "datetime('%s')" % ts


def fmt_nullable(v):
    return "'%s'" % v if v is not None else 'null'


def quote_string(s):
    return s.replace("'", "''")


def dump_table(table):
    """
    Utility function for testing purpose.
    """
    rs = conn.execute('SELECT * FROM %s' % table)
    found = False
    for r in rs:
        print('[%s] ' % table + ','.join(r).replace('\n', '\\n'))
        found = True
    if not found:
        print('[%s] No record.' % table)


class TestDb(unittest.TestCase):
    def setUp(self):
        self.default_user = os.environ['USER']

    def test_parse_connection_string_001(self):
        self.assertEquals({'use_sqlite': True,
                           'dbname': 'foo.db'},
                          parse_connection_string('sqlite:///foo.db'))

    def test_parse_connection_string_002(self):
        self.assertEquals({'use_sqlite': True,
                           'dbname': 'bar.db'},
                          parse_connection_string('bar.db'))

    def test_parse_connection_string_011(self):
        self.assertEquals({'use_sqlite': False,
                           'host': 'localhost',
                           'port': '5432',
                           'dbname': 'testdb',
                           'username': self.default_user,
                           'password': ''},
                          parse_connection_string('postgresql://localhost/testdb'))

    def test_parse_connection_string_012(self):
        self.assertEquals({'use_sqlite': False,
                           'host': 'localhost',
                           'port': '1234',
                           'dbname': 'testdb',
                           'username': self.default_user,
                           'password': ''},
                          parse_connection_string('postgresql://localhost:1234/testdb'))

    def test_parse_connection_string_013(self):
        self.assertEquals({'use_sqlite': False,
                           'host': 'localhost',
                           'port': '5432',
                           'dbname': 'testdb',
                           'username': 'foo',
                           'password': ''},
                          parse_connection_string('postgresql://foo@localhost/testdb'))

    def test_parse_connection_string_014(self):
        self.assertEquals({'use_sqlite': False,
                           'host': 'localhost',
                           'port': '1234',
                           'dbname': 'testdb',
                           'username': 'foo',
                           'password': ''},
                          parse_connection_string('postgresql://foo@localhost:1234/testdb'))

    def test_parse_connection_string_015(self):
        self.assertEquals({'use_sqlite': False,
                           'host': 'localhost',
                           'port': '5432',
                           'dbname': 'testdb',
                           'username': 'foo',
                           'password': 'bar'},
                          parse_connection_string('postgresql://foo:bar@localhost/testdb'))

    def test_parse_connection_string_016(self):
        self.assertEquals({'use_sqlite': False,
                           'host': 'localhost',
                           'port': '1234',
                           'dbname': 'testdb',
                           'username': 'foo',
                           'password': 'bar'},
                          parse_connection_string('postgresql://foo:bar@localhost:1234/testdb'))

    def test_parse_connection_string_021(self):
        with self.assertRaises(ValueError) as cm:
            parse_connection_string('postgresql://localhost')
        self.assertEquals('Unrecognized connection string format: postgresql://localhost',
                          str(cm.exception))

        with self.assertRaises(ValueError) as cm:
            parse_connection_string('postgresql://localhost/')
        self.assertEquals('Unrecognized connection string format: postgresql://localhost/',
                          str(cm.exception))

        with self.assertRaises(ValueError) as cm:
            parse_connection_string('postgresql:///testdb')
        self.assertEquals('Unrecognized connection string format: postgresql:///testdb',
                          str(cm.exception))

    def test_connect_001(self):
        global creds
        creds = parse_connection_string('postgresql://postgres@127.0.0.1/postgres')

        self.assertIsNotNone(connect())

    def test_connect_002(self):
        global creds
        creds = parse_connection_string('postgresql://postgres@127.0.0.1/postgres')

        creds['use_sqlite'] = True
        self.assertIsNotNone(connect())

    def test_version_001(self):
        global creds
        creds = parse_connection_string('postgresql://postgres@127.0.0.1/postgres')
        connect()

        self.assertTrue(version().startswith('PostgreSQL '))


if __name__ == '__main__':
    unittest.main()
