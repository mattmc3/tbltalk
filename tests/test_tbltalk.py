# #!/usr/bin/env python3

# import sqlite3
# from unittest import TestCase, main as unittest_main
# from unittest import mock
# from datetime import datetime
# from collections import namedtuple
# from contextlib import contextmanager
# from collections import namedtuple
# from tbltalk import (DbTable, SqlStatement, DotDict,
#                      DbEngine, sqlparam)
# # dotdict is useful for testing, but not something we want to rely on from the
# # actual thing we are testing. Catch-22, so make our own that never changes so
# # we can have fancy-pants tests.
# from .utils import (DotDict as expando, get_db_backend,
#                     get_args_and_kwargs, popdb)

# TestDbBackend = namedtuple('TestDbBackend', 'backend_name args kwargs')
# TEST_DATE = datetime(2006, 1, 2, 15, 4, 5, 123456)
# TEST_DB_BACKENDS = {
#     'sqlite_memory': TestDbBackend('sqlite', *get_args_and_kwargs(':memory:')),
#     'sqlite_file': TestDbBackend('sqlite', *get_args_and_kwargs('test.db')),
#     'pg': TestDbBackend('postgres', *get_args_and_kwargs("dbname=test user=test")),
#     'mariadb': TestDbBackend('mariadb', *get_args_and_kwargs(host='localhost', user='root', password='', db='test', charset='utf8mb4')),
#     'mssql': TestDbBackend('mssql', *get_args_and_kwargs("localhost", "test", "", "test", autocommit=True)),
#     'mssql_odbc': TestDbBackend('mssql_odbc', *get_args_and_kwargs("DRIVER=FreeTDS;SERVER=localhost;PORT=1433;DATABASE=test;UID=test;PWD=;")),
# }

# DB_BACKEND = 'sqlite_memory'
# USE_SAME_CURSOR = True


# class DbTableTest(TestCase):
#     def setUp(self):
#         self.con = None
#         self.dbengine = self.get_dbengine()

#     def tearDown(self):
#         if self.con:
#             self.con.commit()
#             self.con.close()
#         self.con = None

#     def get_dbengine(self):
#         test_backend = TEST_DB_BACKENDS[DB_BACKEND]
#         be = get_db_backend(test_backend.backend_name)
#         dbengine = DbEngine(be.dbapi, be.dialect, *test_backend.args, **test_backend.kwargs)
#         con = dbengine.connect()
#         cur = con.cursor()
#         popdb(be.popsql, cur)
#         con.commit()
#         if USE_SAME_CURSOR:
#             self.con = con
#             dbengine.set_shared_connection(con)
#         else:
#             con.close()
#         return dbengine

#     def p(self, name=None, idx=0):
#         return sqlparam(self.dbengine.dbapi.paramstyle, name, idx)

#     def test_initial_db_setup(self):
#         dbengine = self.get_dbengine()
#         with dbengine.cursor() as cur:
#             cur.execute("SELECT COUNT(*) FROM movies")
#             num_movies = cur.fetchone()[0]
#             self.assertEqual(num_movies, 9)
#             cur.execute("SELECT COUNT(*) FROM characters")
#             num_characters = cur.fetchone()[0]
#             self.assertEqual(num_characters, 42)

#     def test_query(self):
#         TestData = namedtuple('TestData', 'sql params result')
#         tests = (
#             TestData(("SELECT COUNT(*) AS c "
#                       "FROM characters "
#                       f"WHERE character_type = {self.p()}"),
#                      ('Droid',),
#                      [{'c': 5}]),
#             TestData(("SELECT name as the_droids_youre_looking_for "
#                       "FROM characters "
#                       f"WHERE character_type = {self.p()} "
#                       f"AND first_appeared_movie_id = {self.p()} "),
#                      ('Droid', 1),
#                      [{'the_droids_youre_looking_for': 'R2-D2'},
#                       {'the_droids_youre_looking_for': 'C-3PO'}]),
#         )
#         for td in tests:
#             o = DbTable(self.dbengine)
#             result = list(o.query(td.sql, td.params))
#             self.assertEqual(result, td.result)

#     def test_scalar(self):
#         TestData = namedtuple('TestData', 'sql result params')
#         tests = (
#             TestData("SELECT COUNT(*) FROM movies", 9, ()),
#             TestData("SELECT COUNT(*) FROM characters", 42, ()),
#             TestData("SELECT 99", 99, ()),
#             TestData("SELECT NULL", None, ()),
#             TestData("SELECT -1 FROM movies WHERE 1 = 0", None, ()),
#             TestData(("SELECT COUNT(*) "
#                       "FROM characters "
#                       "WHERE first_appeared_movie_id IS NULL"), 1, ()),
#             TestData(("SELECT COUNT(*) "
#                       "FROM characters "
#                       f"WHERE character_type = {self.p()}"), 5, ('Droid',)),
#         )
#         for td in tests:
#             o = DbTable(self.dbengine)
#             self.assertEqual(o.scalar(td.sql, td.params), td.result)

#     def test_has_pk(self):
#         TestData = namedtuple('TestData', 'obj pk_field has_pk')
#         tests = (
#             TestData(expando({'id': 1, 'b': 2}), 'id', True),
#             TestData(expando({'a': 1, 'the_id': 2}), 'the_id', True),
#             TestData(expando({'a': 1, 'b': 2}), 'pk', False),
#         )
#         for td in tests:
#             o = DbTable(self.dbengine, pk_field=td.pk_field)
#             self.assertEqual(o.has_pk(td.obj), td.has_pk)

#     def test_get_pk(self):
#         TestData = namedtuple('TestData', 'obj pk_field pk_value')
#         tests = (
#             TestData(expando({'a': 1, 'b': 2}), 'a', 1),
#             TestData(expando({'a': 1, 'b': 2}), 'b', 2),
#             TestData(expando({'a': 1, 'b': 2}), 'c', None),
#         )
#         for td in tests:
#             o = DbTable(self.dbengine, pk_field=td.pk_field)
#             self.assertEqual(o.get_pk(td.obj), td.pk_value)

#     def test_create_delete_sql(self):
#         TestData = namedtuple('TestData', 'table_name kwargs sql')
#         tests = (
#             TestData('tbl', {}, "DELETE FROM tbl"),
#             TestData('tbl',
#                      {'where': f'id = {self.p()}'},
#                      f"DELETE FROM tbl WHERE id = {self.p()}"),
#             TestData('tbl',
#                      {'where': f'name = {self.p()} or age < 18'},
#                      f"DELETE FROM tbl WHERE name = {self.p()} or age < 18"),
#         )
#         for td in tests:
#             o = DbTable(self.dbengine, table_name=td.table_name)
#             sql = o.create_delete_sql(**td.kwargs)
#             self.assertEqual(sql, td.sql)

#     def test_delete(self):
#         dbengine = self.get_dbengine()
#         # remove all characters because FKs...
#         ch = DbTable(dbengine, table_name="characters")
#         self.assertEqual(ch.count(), 42)
#         ch.delete(where="1=1")
#         self.assertEqual(ch.count(), 0)

#         o = DbTable(dbengine, table_name="movies")
#         countquery = "SELECT COUNT(*) FROM movies"
#         self.assertEqual(o.scalar(countquery), 9)
#         o.delete_by_id(9)
#         self.assertEqual(o.scalar(countquery), 8)

#         where = f"episode = {self.p()}"
#         o.delete(where=where, params=('I',))
#         self.assertEqual(o.scalar(countquery), 7)

#         where = f"name like {self.p()}"
#         o.delete(where=where, params=('%Star%Wars%',))
#         self.assertEqual(o.scalar(countquery), 2)

#     def test_create_insert_statement(self):
#         TestData = namedtuple('TestData',
#                               'dbtbl_kwargs obj cols vals params')
#         tests = (
#             TestData({'table_name': 'tbl'},
#                      {'id': 1, 'a': 2, 'b': 3},
#                      'a, b',
#                      f'{self.p()}, {self.p()}',
#                      (2, 3)),
#             TestData({'table_name': 'tbl', 'pk_field': "tbl_key",
#                       'pk_autonumber': False},
#                      {'tbl_key': 1, 'a': 2, 'b': 3},
#                      'tbl_key, a, b',
#                      f'{self.p()}, {self.p()}, {self.p()}',
#                      (1, 2, 3)),
#         )
#         for td in tests:
#             o = DbTable(self.dbengine, **td.dbtbl_kwargs)
#             insert_sql = self.dbengine.dialect.insert_sql.format(
#                 table=o.table_name, columns=td.cols,
#                 values=td.vals, pk_field=o.pk_field
#             )
#             stmt = o.create_insert_statement(td.obj)
#             self.assertEqual(stmt, SqlStatement(insert_sql, td.params))

#     def test_failed_insert(self):
#         countquery = "SELECT COUNT(*) c FROM characters"
#         o = DbTable(self.dbengine, table_name="characters")
#         self.assertEqual(o.scalar(countquery), 42)
#         with self.assertRaises(Exception):
#             o.insert({'name': 'General Grievous', 'fake_field': 'error'})
#         # verify no change
#         self.assertEqual(o.scalar(countquery), 42)

#     def test_insert(self):
#         countquery = "SELECT COUNT(*) c FROM characters"
#         o = DbTable(self.dbengine, table_name="characters")
#         self.assertEqual(o.scalar(countquery), 42)
#         id = o.insert({
#             'name': 'Wedge Antilles',
#             'sex': 'M',
#             'character_type': 'Human',
#             'allegiance': '',
#             'first_appeared_movie_id': 1,
#             'has_force': False,
#         })
#         self.assertEqual(id, 43)
#         self.assertEqual(o.scalar(countquery), 43)

#     def test_create_update_statement(self):
#         TestData = namedtuple('TestData',
#                               'dbtbl_kwargs obj key stmt')
#         tests = (
#             TestData({'table_name': 'tbl'},
#                      {'id': 1, 'a': 'A', 'b': 3}, 1,
#                      SqlStatement(("UPDATE tbl "
#                                    f"SET a = {self.p()}, b = {self.p()} "
#                                    f"WHERE id = {self.p()}"),
#                                   ('A', 3, 1)),),
#         )
#         for td in tests:
#             o = DbTable(self.dbengine, **td.dbtbl_kwargs)
#             stmt = o.create_update_statement(td.obj, td.key)
#             self.assertEqual(stmt, td.stmt)

#     def test_update(self):
#         o = DbTable(self.dbengine, table_name="characters")
#         rebels = o.find_by(allegiance='The Rebel Alliance',
#                            first_appeared_movie_id=1)
#         smugglers = o.find_by(allegiance='Smuggler',
#                               first_appeared_movie_id=1)
#         self.assertEqual(len(rebels), 6)
#         self.assertEqual(len(smugglers), 2)
#         for s in smugglers:
#             s.allegiance = 'The Rebel Alliance'
#             o.update(s, s.id)

#         rebels = o.find_by(allegiance='The Rebel Alliance',
#                            first_appeared_movie_id=1)
#         smugglers = o.find_by(allegiance='Smuggler',
#                               first_appeared_movie_id=1)
#         self.assertEqual(len(rebels), 8)
#         self.assertEqual(len(smugglers), 0)

#     def test_create_select_sql(self):
#         TestData = namedtuple('TestData', 'table_name kwargs select')
#         tests = (
#             TestData('pets', {}, "SELECT * FROM pets"),
#             TestData('pets', {'distinct': True},
#                      "SELECT DISTINCT * FROM pets"),
#             TestData('tbl', {'columns': 'col1, col2'},
#                      "SELECT col1, col2 FROM tbl"),
#             TestData('tbl', {'columns': ('col1', 'col2')},
#                      "SELECT col1, col2 FROM tbl"),
#             TestData('tbl', {'columns': ('COUNT(*) as c',)},
#                      "SELECT COUNT(*) as c FROM tbl"),
#             TestData('tbl',
#                      {'where': f'name = {self.p()} and age > {self.p()}'},
#                      f"SELECT * FROM tbl WHERE name = {self.p()} and age > {self.p()}"),
#             TestData('tbl',
#                      {'columns': ('a', 'b'),
#                       'distinct': True,
#                       'where': f'name = {self.p()} and age > {self.p()}',
#                       'orderby': ('name DESC', 'dob'),
#                       'limit': 10},
#                      ("SELECT DISTINCT{top} a, b "
#                       "FROM tbl "
#                       f"WHERE name = {self.p()} and age > {self.p()} "
#                       "ORDER BY name DESC, dob{limit}")),
#         )
#         for td in tests:
#             o = DbTable(self.dbengine, table_name=td.table_name)
#             sql = o.create_select_sql(**td.kwargs)
#             expected = td.select
#             if 'limit' in td.kwargs:
#                 kw = self.dbengine.dialect.keywords.limit
#                 top, limit = "", ""
#                 if kw.upper() == "TOP":
#                     top = " TOP {}".format(td.kwargs['limit'])
#                 else:
#                     limit = " LIMIT {}".format(td.kwargs['limit'])
#                 expected = expected.format(top=top, limit=limit)
#             self.assertEqual(sql, expected)

#     def test_get_by_id(self):
#         TestData = namedtuple('TestData', 'dbtbl_kwargs id result')
#         tests = (
#             TestData({'table_name': 'movies'}, 1,
#                      DotDict([('id', 1),
#                               ('name', 'Star Wars (A New Hope)'),
#                               ('episode', 'IV'),
#                               ('director', 'George Lucas'),
#                               ('release_year', 1977),
#                               ('chronology', 5)])),
#         )
#         for td in tests:
#             o = DbTable(self.dbengine, **td.dbtbl_kwargs)
#             result = o.get_by_id(td.id)
#             self.assertEqual(td.result, result)

#     def test_dynamicquery_single(self):
#         o = DbTable(self.dbengine, table_name="characters")
#         thrawn = o.single(id=42)
#         self.assertEqual(thrawn.name, 'Grand Admiral Thrawn')
#         who = o.single(id=43)
#         self.assertEqual(who, None)

#     def test_dynamicquery_find_by(self):
#         o = DbTable(self.dbengine, table_name="characters")
#         smugglers = o.find_by_allegiance(allegiance='Smuggler',
#                                          first_appeared_movie_id=1,
#                                          orderby="name")
#         self.assertEqual(len(smugglers), 2)
#         self.assertEqual(smugglers[0].name, 'Chewbacca')
#         self.assertEqual(smugglers[1].name, 'Han Solo')

#     def test_min(self):
#         TestData = namedtuple('TestData',
#                               'dbtbl_kwargs min_kwargs result')
#         tests = (
#             TestData({'table_name': 'movies'},
#                      {'column': 'release_year'}, 1977),
#             TestData({'table_name': 'characters'},
#                      {'column': 'name'}, 'Admiral Ackbar'),
#             TestData({'table_name': 'characters'},
#                      {'column': 'name',
#                       'where': f'character_type = {self.p()}',
#                       'params': ('Droid',)}, 'BB-8'),
#         )
#         for td in tests:
#             o = DbTable(self.dbengine, **td.dbtbl_kwargs)
#             result = o.min(**td.min_kwargs)
#             self.assertEqual(result, td.result)

#     def test_max(self):
#         TestData = namedtuple('TestData',
#                               'dbtbl_kwargs max_kwargs result')
#         tests = (
#             TestData({'table_name': 'movies'},
#                      {'column': 'release_year'}, 2017),
#             TestData({'table_name': 'characters'},
#                      {'column': 'name'}, 'Yoda'),
#             TestData({'table_name': 'characters'},
#                      {'column': 'name',
#                       'where': f'character_type = {self.p()}',
#                       'params': ('Droid',)}, 'R2-D2'),
#         )
#         for td in tests:
#             o = DbTable(self.dbengine, **td.dbtbl_kwargs)
#             result = o.max(**td.max_kwargs)
#             self.assertEqual(result, td.result)

#     def test_all_basic(self):
#         o = DbTable(self.dbengine, table_name="movies")
#         movies = list(o.all())
#         self.assertEqual(9, len(movies))

#     def test_paged(self):
#         o = DbTable(self.dbengine, table_name="movies")
#         TestData = namedtuple('TestData',
#                               ['dbtbl_kwargs', 'paged_kwargs', 'start_page',
#                                'end_page', 'total_records', 'total_pages',
#                                'result'])
#         tests = (
#             TestData(
#                 {'table_name': 'movies'},
#                 {
#                     'columns': ['id', 'name', 'episode'],
#                     'where': f"director = {self.p()}",
#                     'params': ('George Lucas',),
#                     'orderby': 'release_year',
#                     'page_size': 2,
#                 },
#                 1,
#                 4,
#                 5,
#                 3,
#                 [
#                     [
#                         DotDict((('id', 1),
#                                 ('name', 'Star Wars (A New Hope)'),
#                                 ('episode', 'IV'))),
#                         DotDict((('id', 3),
#                                 ('name', 'Return of the Jedi'),
#                                 ('episode', 'VI'))),
#                     ],
#                     [
#                         DotDict((('id', 4),
#                                 ('name', 'Star Wars: Episode I - The Phantom Menace'),
#                                 ('episode', 'I'))),
#                         DotDict((('id', 5),
#                                 ('name', 'Star Wars: Episode II - Attack of the Clones'),
#                                 ('episode', 'II'))),
#                     ],
#                     [
#                         DotDict((('id', 6),
#                                 ('name', 'Star Wars: Episode III - Revenge of the Sith'),
#                                 ('episode', 'III'))),
#                     ],
#                     []
#                 ]
#             ),
#         )
#         for td in tests:
#             o = DbTable(self.dbengine, **td.dbtbl_kwargs)
#             paged_kwargs = td.paged_kwargs
#             for idx, current_page in enumerate(range(td.start_page, td.end_page + 1)):
#                 paged_kwargs["current_page"] = current_page
#                 result = o.paged(**paged_kwargs)
#                 self.assertEqual(result.current_page, paged_kwargs["current_page"])
#                 self.assertEqual(result.total_records, td.total_records)
#                 self.assertEqual(len(result.records), len(td.result[idx]))
#                 self.assertEqual(result.records, td.result[idx])


# if __name__ == '__main__':
#     unittest_main()

#     # def test_default_value(self):
#     #     TestData = namedtuple('TestData', 'column_default utcnow result')
#     #     tests = (
#     #         TestData(None, TEST_DATE, None),
#     #         TestData('CURRENT_TIME', TEST_DATE, '15:04:05'),
#     #         TestData('CURRENT_DATE', TEST_DATE, '2006-01-02'),
#     #         TestData('CURRENT_TIMESTAMP', TEST_DATE, '2006-01-02 15:04:05'),
#     #     )

#     #     with mock.patch('tbltalk.datetime') as dt_mock:
#     #         dt_mock.utcnow.return_value = TEST_DATE
#     #         dt_mock.side_effect = lambda *args, **kw: datetime(*args, **kw)
#     #         for td in tests:
#     #             o = DbTable(self.dbengine)
#     #             column = expando({'COLUMN_DEFAULT': td.column_default})
#     #             self.assertEqual(o.default_value(column), td.result)

#     # def default_value(self, column):
#     #     ''' Gets a default value for the column '''
#     #     result = None
#     #     deflt = column.COLUMN_DEFAULT
#     #     if not deflt:
#     #         result = None
#     #     elif deflt.upper() == "CURRENT_TIME":
#     #         result = datetime.utcnow().strftime("%H:%M:%S")
#     #     elif deflt.upper() == "CURRENT_DATE":
#     #         result = datetime.utcnow().strftime("%Y-%m-%d")
#     #     elif deflt.upper() == "CURRENT_TIMESTAMP":
#     #         result = datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")
#     #     return result
