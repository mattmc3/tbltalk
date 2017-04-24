#!/usr/bin/env python3

import sqlite3
from unittest import TestCase, main as unittest_main
from collections import namedtuple
from tbltalk import DbConnection, SqlResultException, DbRow, dbrow_factory
from .utils import get_db_backend, popdb


class TestDbConnection(TestCase):
    def setUp(self):
        pass

    def tearDown(self):
        pass

    def get_con(self):
        return sqlite3.connect(':memory:')

    def get_dbcon(self):
        be = get_db_backend('sqlite')
        con = sqlite3.connect(':memory:')
        cur = con.cursor()
        cur.executescript(be.popsql)
        con.commit()
        return DbConnection(con)

    def test_initial_db_setup(self):
        with self.get_dbcon() as dbcon:
            num_movies = dbcon.scalar("SELECT COUNT(*) FROM movies")
            self.assertEqual(num_movies, 9)
            num_characters = dbcon.scalar("SELECT COUNT(*) FROM characters")
            self.assertEqual(num_characters, 42)

    def test_scalar(self):
        with self.get_dbcon() as dbcon:
            george = 'George Lucas'
            count = dbcon.scalar("SELECT COUNT(*) FROM movies WHERE director = ?",
                                 params=(george,))
            self.assertEqual(count, 5)

    def test_scalar_with_no_results(self):
        with self.get_dbcon() as dbcon:
            value = dbcon.scalar("SELECT 1 WHERE 1 = 0")
            self.assertIsNone(value)

    def test_scalar_null(self):
        with self.get_dbcon() as dbcon:
            value = dbcon.scalar("SELECT NULL")
            self.assertIsNone(value)

    def test_execute(self):
        with self.get_dbcon() as dbcon:
            george = 'George Lucas'
            count = dbcon.scalar("SELECT COUNT(*) FROM movies WHERE director = ?",
                                 params=(george,))
            self.assertEqual(count, 5)
            dbcon.execute("UPDATE movies SET director = ? WHERE director = ?",
                          params=('George Walton Lucas Jr.', george))
            count = dbcon.scalar("SELECT COUNT(*) FROM movies WHERE director = ?",
                                 params=(george,))
            self.assertEqual(count, 0)

    def test_executemany(self):
        with self.get_dbcon() as dbcon:
            count = dbcon.scalar("SELECT COUNT(*) FROM characters")
            self.assertEqual(count, 42)
            insertsql = ("INSERT INTO characters "
                         "(name, sex, character_type, allegiance, "
                         "first_appeared_movie_id, has_force, "
                         "died_in_movie_id) "
                         "VALUES (?, ?, ?, ?, ?, ?, ?)")
            newchars = [
                ('Porkins', 'M', 'Human', 'The Rebel Alliance', 1, False, 1),
                ('Wedge Antilles', 'M', 'Human', 'The Rebel Alliance', 1, False, None),
            ]

            dbcon.executemany(insertsql, newchars)
            count = dbcon.scalar("SELECT COUNT(*) FROM characters")
            self.assertEqual(count, 44)

    def test_iter(self):
        with self.get_dbcon() as dbcon:
            sql = "SELECT * FROM characters WHERE sex = ? ORDER BY id"
            params = ('F',)
            character_iter = dbcon.iter(sql, params)
            self.assertEqual(next(character_iter)[1], "Leia Organa")
            others = 7
            while others > 0:
                next(character_iter)
                others -= 1
            with self.assertRaises(StopIteration):
                next(character_iter)

    def test_one(self):
        with self.get_dbcon() as dbcon:
            sql = "SELECT * FROM characters WHERE sex = ? ORDER BY id"
            params = ('F',)
            character = dbcon.one(sql, params)
            self.assertEqual(character[1], "Leia Organa")

    def test_one_with_no_results(self):
        with self.get_dbcon() as dbcon:
            sql = "SELECT * FROM characters WHERE name = ?"
            params = ('Bob',)
            character = dbcon.one(sql, params)
            self.assertIsNone(character)

    def test_executescript(self):
        be = get_db_backend('sqlite')
        with sqlite3.connect(':memory:') as con:
            dbcon = DbConnection(con)
            dbcon.executescript(be.popsql)
            num_movies = dbcon.scalar("SELECT COUNT(*) FROM movies")
            self.assertEqual(num_movies, 9)

    def test_all(self):
        with self.get_dbcon() as dbcon:
            sql = "SELECT id, episode FROM movies WHERE episode IS NOT NULL order by id"
            expected = [
                (1, 'IV'),
                (2, 'V'),
                (3, 'VI'),
                (4, 'I'),
                (5, 'II'),
                (6, 'III'),
                (7, 'VII'),
                (9, 'VIII'),  # Rogue One snuck in there at #8
            ]
            actual = dbcon.all(sql)
            self.assertEqual(expected, actual)

    def test_exactly_one(self):
        with self.get_dbcon() as dbcon:
            sql = ("SELECT name "
                   "FROM characters "
                   "WHERE has_force = ? "
                   "AND died_in_movie_id IS NULL "
                   "AND name LIKE ?")
            params = (True, "%Skywalker")
            the_last_jedi = dbcon.exactly_one(sql, params)
            self.assertEqual(the_last_jedi, ('Luke Skywalker',))

    def test_exactly_one_fails_with_multiple_results(self):
        with self.get_dbcon() as dbcon:
            sql = ("SELECT name "
                   "FROM characters "
                   "WHERE has_force = ? "
                   "AND died_in_movie_id IS NULL ")
            params = (True,)
            with self.assertRaises(SqlResultException):
                dont_forget_leia_and_rey = dbcon.exactly_one(sql, params)

    def test_exactly_one_fails_with_no_results(self):
        with self.get_dbcon() as dbcon:
            sql = ("SELECT * "
                   "FROM characters "
                   "WHERE id = ? "
                   "AND character_type = ?")
            params = (999, "Droid")
            with self.assertRaises(SqlResultException):
                these_arent_the_droids_youre_looking_for = \
                    dbcon.exactly_one(sql, params)

    def test_row_factory(self):
        with self.get_dbcon() as dbcon:
            dbcon.row_factory = dbrow_factory
            sql = "SELECT id, episode FROM movies WHERE id <= 3"
            expected = [
                DbRow((('id', 1), ('episode', 'IV'))),
                DbRow((('id', 2), ('episode', 'V'))),
                DbRow((('id', 3), ('episode', 'VI'))),
            ]
            original_trio = dbcon.all(sql)
            self.assertEqual(expected, original_trio)


if __name__ == '__main__':
    unittest_main()
