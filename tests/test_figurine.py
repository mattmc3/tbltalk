#!/usr/bin/env python3

import sqlite3
from unittest import TestCase, main as unittest_main
from unittest import mock
from datetime import datetime
from collections import namedtuple
from contextlib import contextmanager
from figurine import (DbTable, SqlStatement, DotDict,
                      DbEngine, OpenConnectionDbEngine)
# dotdict is useful for testing, but not something we want to rely on from the
# actual thing we are testing. Catch-22, so make our own that never changes so
# we can have fancy-pants tests.
from .utils import DotDict as expando, TestDatabase

TEST_DATE = datetime(2006, 1, 2, 15, 4, 5, 123456)


class DbTableTest(TestCase):
    testdb_maker = TestDatabase()

    def setUp(self):
        self.con = self.__class__.testdb_maker.connect()
        self.dbengine = OpenConnectionDbEngine(sqlite3, self.con)

    def tearDown(self):
        if self.con:
            self.con.close()
            self.con = None

    @contextmanager
    def get_dbengine(self):
        ''' Some tests require new dbengines for each individual test. '''
        with self.__class__.testdb_maker.connect() as con:
            dbengine = OpenConnectionDbEngine(sqlite3, con)
            yield dbengine

    def create_sqlitedb(self, dbname):
        with sqlite3.connect(dbname) as con:
            cur = con.cursor()
            cur.execute("""
                CREATE TABLE people (
                    id INTEGER PRIMARY KEY,
                    name TEXT,
                    age INT
                )""")
            cur.execute('CREATE UNIQUE INDEX un_people ON people (name)')
            cur.execute("""
                CREATE TABLE pets (
                    id INTEGER PRIMARY KEY,
                    name TEXT,
                    species TEXT,
                    breed TEXT,
                    owner_id INT
                )""")
            sqlinsert_person = ("INSERT INTO people (name, age) "
                                "VALUES (?, ?)")
            sqlinsert_pets = ("INSERT INTO pets "
                              "(name, species, breed, owner_id) "
                              "VALUES (?, ?, ?, ?)")
            cur.executemany(sqlinsert_person,
                            [('Mr. Dearly', 32),
                             ('Mrs. Dearly', 27)])
            cur.executemany(sqlinsert_pets,
                            [('Pongo', 'dog', 'dalmatian', 1),
                             ('Missis', 'dog', 'dalmatian', 2),
                             ('Perdita', 'dog', 'dalmatian', 2),
                             ('Prince', 'dog', 'dalmatian', None)])

    def test_initial_db_setup(self):
        testdb = TestDatabase()
        with self.__class__.testdb_maker.connect() as con:
            cur = con.cursor()
            cur.execute("SELECT COUNT(*) FROM movies")
            num_movies = cur.fetchone()[0]
            self.assertEqual(num_movies, 9)
            cur.execute("SELECT COUNT(*) FROM characters")
            num_characters = cur.fetchone()[0]
            self.assertEqual(num_characters, 42)

    def test_query(self):
        TestData = namedtuple('TestData', 'sql params result')
        tests = (
            TestData(("SELECT COUNT(*) AS c "
                      "FROM characters "
                      "WHERE character_type = ?"),
                     ('Droid',),
                     [{'c': 5}]),
            TestData(("SELECT name as the_droids_youre_looking_for "
                      "FROM characters "
                      "WHERE character_type = ? "
                      "AND first_appeared_movie_id = ? "),
                     ('Droid', 1),
                     [{'the_droids_youre_looking_for': 'R2-D2'},
                      {'the_droids_youre_looking_for': 'C-3PO'}]),
        )
        for td in tests:
            o = DbTable(self.dbengine)
            result = list(o.query(td.sql, td.params))
            self.assertEqual(result, td.result)

    def test_scalar(self):
        TestData = namedtuple('TestData', 'sql result params')
        tests = (
            TestData("SELECT COUNT(*) FROM movies", 9, ()),
            TestData("SELECT COUNT(*) FROM characters", 42, ()),
            TestData("SELECT 99", 99, ()),
            TestData("SELECT NULL", None, ()),
            TestData("SELECT -1 FROM movies WHERE 1 = 0", None, ()),
            TestData(("SELECT COUNT(*) "
                      "FROM characters "
                      "WHERE first_appeared_movie_id IS NULL"), 1, ()),
            TestData(("SELECT COUNT(*) "
                      "FROM characters "
                      "WHERE character_type = ?"), 5, ('Droid',)),
        )
        for td in tests:
            o = DbTable(self.dbengine)
            self.assertEqual(o.scalar(td.sql, td.params), td.result)

    def test_has_pk(self):
        TestData = namedtuple('TestData', 'obj pk_field has_pk')
        tests = (
            TestData(expando({'id': 1, 'b': 2}), 'id', True),
            TestData(expando({'a': 1, 'the_id': 2}), 'the_id', True),
            TestData(expando({'a': 1, 'b': 2}), 'pk', False),
        )
        for td in tests:
            o = DbTable(self.dbengine, pk_field=td.pk_field)
            self.assertEqual(o.has_pk(td.obj), td.has_pk)

    def test_get_pk(self):
        TestData = namedtuple('TestData', 'obj pk_field pk_value')
        tests = (
            TestData(expando({'a': 1, 'b': 2}), 'a', 1),
            TestData(expando({'a': 1, 'b': 2}), 'b', 2),
            TestData(expando({'a': 1, 'b': 2}), 'c', None),
        )
        for td in tests:
            o = DbTable(self.dbengine, pk_field=td.pk_field)
            self.assertEqual(o.get_pk(td.obj), td.pk_value)

    # def test_default_value(self):
    #     TestData = namedtuple('TestData', 'column_default utcnow result')
    #     tests = (
    #         TestData(None, TEST_DATE, None),
    #         TestData('CURRENT_TIME', TEST_DATE, '15:04:05'),
    #         TestData('CURRENT_DATE', TEST_DATE, '2006-01-02'),
    #         TestData('CURRENT_TIMESTAMP', TEST_DATE, '2006-01-02 15:04:05'),
    #     )

    #     with mock.patch('figurine.datetime') as dt_mock:
    #         dt_mock.utcnow.return_value = TEST_DATE
    #         dt_mock.side_effect = lambda *args, **kw: datetime(*args, **kw)
    #         for td in tests:
    #             o = DbTable(self.dbengine)
    #             column = expando({'COLUMN_DEFAULT': td.column_default})
    #             self.assertEqual(o.default_value(column), td.result)

    # def default_value(self, column):
    #     ''' Gets a default value for the column '''
    #     result = None
    #     deflt = column.COLUMN_DEFAULT
    #     if not deflt:
    #         result = None
    #     elif deflt.upper() == "CURRENT_TIME":
    #         result = datetime.utcnow().strftime("%H:%M:%S")
    #     elif deflt.upper() == "CURRENT_DATE":
    #         result = datetime.utcnow().strftime("%Y-%m-%d")
    #     elif deflt.upper() == "CURRENT_TIMESTAMP":
    #         result = datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")
    #     return result

    def test_create_delete_sql(self):
        TestData = namedtuple('TestData', 'table_name kwargs sql')
        tests = (
            TestData('tbl', {}, "DELETE FROM tbl"),
            TestData('tbl',
                     {'where': 'id = ?'},
                     "DELETE FROM tbl WHERE id = ?"),
            TestData('tbl',
                     {'where': 'name = ? or age < 18'},
                     "DELETE FROM tbl WHERE name = ? or age < 18"),
        )
        for td in tests:
            o = DbTable(self.dbengine, table_name=td.table_name)
            sql = o.create_delete_sql(**td.kwargs)
            self.assertEqual(sql, td.sql)

    def test_delete(self):
        with self.get_dbengine() as dbengine:
            o = DbTable(dbengine, table_name="movies")
            countquery = "SELECT COUNT(*) FROM movies"
            self.assertEqual(o.scalar(countquery), 9)
            o.delete_by_id(9)
            self.assertEqual(o.scalar(countquery), 8)
            o.delete(where="episode = ?", params=('I',))
            self.assertEqual(o.scalar(countquery), 7)
            o.delete(where="name like ?", params=('%Star%Wars%',))
            self.assertEqual(o.scalar(countquery), 2)

    def test_create_insert_statement(self):
        TestData = namedtuple('TestData',
                              'dbtbl_kwargs obj stmt')
        tests = (
            TestData({'table_name': 'tbl'},
                     {'id': 1, 'a': 2, 'b': 3},
                     SqlStatement(("INSERT INTO tbl (a, b)\n"
                                   "VALUES (?, ?)"), (2, 3))),
            TestData({'table_name': 'tbl', 'pk_field': "tbl_key",
                      'pk_autonumber': False},
                     {'tbl_key': 1, 'a': 2, 'b': 3},
                     SqlStatement(("INSERT INTO tbl (tbl_key, a, b)\n"
                                   "VALUES (?, ?, ?)"), (1, 2, 3))),
        )
        for td in tests:
            o = DbTable(self.dbengine, **td.dbtbl_kwargs)
            stmt = o.create_insert_statement(td.obj)
            self.assertEqual(stmt, td.stmt)

    def test_failed_insert(self):
        countquery = "SELECT COUNT(*) FROM characters"
        o = DbTable(self.dbengine, table_name="characters")
        self.assertEqual(o.scalar(countquery), 42)
        with self.assertRaises(Exception):
            o.insert({'name': 'General Grievous', 'fake_field': 'error'})
        # verify no change
        self.assertEqual(o.scalar(countquery), 42)

    def test_insert(self):
        countquery = "SELECT COUNT(*) FROM characters"
        o = DbTable(self.dbengine, table_name="characters")
        self.assertEqual(o.scalar(countquery), 42)
        id = o.insert({
            'name': 'Wedge Antilles',
            'sex': 'M',
            'character_type': 'Human',
            'allegiance': '',
            'first_appeared_movie_id': 1,
            'has_force': 0,
        })
        self.assertEqual(id, 43)
        self.assertEqual(o.scalar(countquery), 43)

    def test_create_update_statement(self):
        TestData = namedtuple('TestData',
                              'dbtbl_kwargs obj key stmt')
        tests = (
            TestData({'table_name': 'tbl'},
                     {'id': 1, 'a': 'A', 'b': 3}, 1,
                     SqlStatement(("UPDATE tbl\n"
                                   "SET a = ?\n"
                                   ",b = ?\n"
                                   "WHERE id = ?"),
                                  ('A', 3, 1)),),
        )
        for td in tests:
            o = DbTable(self.dbengine, **td.dbtbl_kwargs)
            stmt = o.create_update_statement(td.obj, td.key)
            self.assertEqual(stmt, td.stmt)

    def test_update(self):
        o = DbTable(self.dbengine, table_name="characters")
        rebels = o.find_by(allegiance='The Rebel Alliance',
                           first_appeared_movie_id=1)
        smugglers = o.find_by(allegiance='Smuggler',
                              first_appeared_movie_id=1)
        self.assertEqual(len(rebels), 6)
        self.assertEqual(len(smugglers), 2)
        for s in smugglers:
            s.allegiance = 'The Rebel Alliance'
            o.update(s, s.id)

        rebels = o.find_by(allegiance='The Rebel Alliance',
                           first_appeared_movie_id=1)
        smugglers = o.find_by(allegiance='Smuggler',
                              first_appeared_movie_id=1)
        self.assertEqual(len(rebels), 8)
        self.assertEqual(len(smugglers), 0)

    def test_create_select_sql(self):
        TestData = namedtuple('TestData', 'table_name kwargs select')
        tests = (
            TestData('pets', {}, "SELECT * FROM pets"),
            TestData('pets', {'distinct': True},
                     "SELECT DISTINCT * FROM pets"),
            TestData('tbl', {'columns': 'col1, col2'},
                     "SELECT col1, col2 FROM tbl"),
            TestData('tbl', {'columns': ('col1', 'col2')},
                     "SELECT col1, col2 FROM tbl"),
            TestData('tbl', {'columns': ('COUNT(*) as c',)},
                     "SELECT COUNT(*) as c FROM tbl"),
            TestData('tbl',
                     {'where': 'name = ? and age > ?'},
                     "SELECT * FROM tbl WHERE name = ? and age > ?"),
            TestData('tbl',
                     {'columns': ('a', 'b'),
                      'distinct': True,
                      'where': 'name = ? and age > ?',
                      'orderby': ('name DESC', 'dob'),
                      'limit': 10},
                     ("SELECT DISTINCT a, b "
                      "FROM tbl "
                      "WHERE name = ? and age > ? "
                      "ORDER BY name DESC, dob "
                      "LIMIT 10")),
        )
        for td in tests:
            o = DbTable(self.dbengine, table_name=td.table_name)
            sql = o.create_select_sql(**td.kwargs)
            self.assertEqual(sql, td.select)

    def test_get_by_id(self):
        TestData = namedtuple('TestData', 'dbtbl_kwargs id result')
        tests = (
            TestData({'table_name': 'movies'}, 1,
                     DotDict([('id', 1),
                              ('name', 'Star Wars (A New Hope)'),
                              ('episode', 'IV'),
                              ('director', 'George Lucas'),
                              ('released_year', 1977),
                              ('chronology', 5)])),
        )
        for td in tests:
            o = DbTable(self.dbengine, **td.dbtbl_kwargs)
            result = o.get_by_id(td.id)
            self.assertEqual(td.result, result)

    def test_dynamicquery_single(self):
        o = DbTable(self.dbengine, table_name="characters")
        thrawn = o.single(id=42)
        self.assertEqual(thrawn.name, 'Grand Admiral Thrawn')
        who = o.single(id=43)
        self.assertEqual(who, None)

    def test_dynamicquery_find_by(self):
        o = DbTable(self.dbengine, table_name="characters")
        smugglers = o.find_by_allegiance(allegiance='Smuggler',
                                         first_appeared_movie_id=1,
                                         orderby="name")
        self.assertEqual(len(smugglers), 2)
        self.assertEqual(smugglers[0].name, 'Chewbacca')
        self.assertEqual(smugglers[1].name, 'Han Solo')

    def test_min(self):
        TestData = namedtuple('TestData',
                              'dbtbl_kwargs min_kwargs result')
        tests = (
            TestData({'table_name': 'movies'},
                     {'columns': 'released_year'}, 1977),
            TestData({'table_name': 'characters'},
                     {'columns': 'name'}, 'Admiral Ackbar'),
            TestData({'table_name': 'characters'},
                     {'columns': 'name',
                      'where': 'character_type = ?',
                      'params': ('Droid',)}, 'BB-8'),
        )
        for td in tests:
            o = DbTable(self.dbengine, **td.dbtbl_kwargs)
            result = o.min(**td.min_kwargs)
            self.assertEqual(result, td.result)

    def test_max(self):
        TestData = namedtuple('TestData',
                              'dbtbl_kwargs max_kwargs result')
        tests = (
            TestData({'table_name': 'movies'},
                     {'columns': 'released_year'}, 2017),
            TestData({'table_name': 'characters'},
                     {'columns': 'name'}, 'Yoda'),
            TestData({'table_name': 'characters'},
                     {'columns': 'name',
                      'where': 'character_type = ?',
                      'params': ('Droid',)}, 'R2-D2'),
        )
        for td in tests:
            o = DbTable(self.dbengine, **td.dbtbl_kwargs)
            result = o.max(**td.max_kwargs)
            self.assertEqual(result, td.result)

    def test_all_basic(self):
        o = DbTable(self.dbengine, table_name="movies")
        movies = list(o.all())
        self.assertEqual(9, len(movies))

if __name__ == '__main__':
    unittest_main()
