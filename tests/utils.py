from collections import OrderedDict
from contextlib import contextmanager
from collections import namedtuple
from tbltalk import DbEngine
from tbltalk.db import (sqlite3_dialect, postgres_dialect, mssql_dialect,
                        mariadb_dialect)
import os
import sqlite3

DbBackend = namedtuple('DbBackend', 'dbapi dialect popsql')


class DotDict(dict):
    '''Quick and dirty implementation of a dot-able dict'''
    def __init__(self, *args, **kwargs):
        super(DotDict, self).__init__(*args, **kwargs)

    def __delattr__(self, name):
        try:
            del self[name]
        except KeyError as err:
            raise AttributeError()

    def __getattr__(self, k):
        try:
            return self[k]
        except KeyError as err:
            raise AttributeError()

    __setattr__ = dict.__setitem__


class TestDbScripts():
    def __init__(self):
        # read sql into variable
        self.starwars_sqlite3 = self.readsql("starwars_sqlite3")
        self.starwars_postgres = self.readsql("starwars_postgres")
        self.starwars_mariadb = self.readsql("starwars_mariadb")
        self.starwars_mssql = self.readsql("starwars_mssql")

    def readsql(self, name):
        dir_path = os.path.dirname(os.path.realpath(__file__))
        sqlfile_path = os.path.join(dir_path, name + ".sql")
        with open(sqlfile_path, 'r') as f:
            return f.read()

scripts = TestDbScripts()


def get_db_backend(dbbackend_name):
    if dbbackend_name == 'sqlite':
        import sqlite3
        return DbBackend(sqlite3, sqlite3_dialect, scripts.starwars_sqlite3)
    elif dbbackend_name in ['pg', 'postgres']:
        import psycopg2
        return DbBackend(psycopg2, postgres_dialect, scripts.starwars_postgres)
    elif dbbackend_name in ['mssql', 'sqlserver']:
        import pymssql
        return DbBackend(pymssql, mssql_dialect, scripts.starwars_mssql)
    elif dbbackend_name in ['mssql_odbc']:
        import pyodbc
        return DbBackend(pyodbc, mssql_dialect, scripts.starwars_mssql)
    elif dbbackend_name in ['mariadb', 'mysql']:
        import pymysql
        return DbBackend(pymysql, mariadb_dialect, scripts.starwars_mariadb)
    else:
        raise ValueError(f"unrecognized backend: {dbbackend_name}")


def get_args_and_kwargs(*args, **kwargs):
    return (args, kwargs)


def popdb(popsql, cur):
    exs = getattr(cur, "executescript", None)
    if callable(exs):
        cur.executescript(popsql)
    else:
        cur.execute(popsql)

# class TestDatabase():
#     def sqlite3(self, opencon, *args, **kwargs):
#         return self._make_engine(opencon, sqlite3, sqlite3_dialect,
#                                  scripts.starwars_sqlite3, *args, **kwargs)

#     def pg(self, openconnection, *args, **kwargs):
#         import psycopg2
#         return self._make_engine(openconnection, psycopg2, postgres_dialect,
#                                  scripts.starwars_postgres, *args, **kwargs)

#     # def mariadb(self, openconnection, *args, **kwargs):
#     #     import pymysql
#     #     # return self._make_engine(openconnection, pymysql, mariadb_dialect,
#     #     #                          scripts.starwars_mariadb, *args, **kwargs)

#     #     dialect = mariadb_dialect
#     #     popsql = scripts.starwars_mariadb
#     #     dbapi = pymysql

#     #     con = dbapi.connect(*args, **kwargs)

#     #     cur = con.cursor()
#     #     exs = getattr(cur, "executescript", None)
#     #     if callable(exs):
#     #         cur.executescript(popsql)
#     #     else:
#     #         cur.execute(popsql)
#     #     con.commit()
#     #     if openconnection:
#     #         return OpenConnectionDbEngine(dbapi, dialect, con)
#     #     else:
#     #         con.close()
#     #         return DbEngine(dbapi, dialect, *args, **kwargs)

#     # def mssql(self, openconnection, *args, **kwargs):
#     #     import pymssql
#     #     return self._make_engine(openconnection, pymssql, mssql_dialect,
#     #                              scripts.starwars_mssql, *args, **kwargs)

#     # def mssql_odbc(self, openconnection, *args, **kwargs):
#     #     import pyodbc
#     #     return self._make_engine(openconnection, pyodbc, mssql_dialect,
#     #                              scripts.starwars_mssql, *args, **kwargs)

#     def _make_engine(self, opencon, dbapi, dialect, popsql, *args, **kwargs):
#         dbengine = DbEngine(dbapi, dialect, *args, **kwargs)
#         con = dbapi.connect(*args, *kwargs)
#         cur = con.cursor()
#         exs = getattr(cur, "executescript", None)
#         if callable(exs):
#             cur.executescript(popsql)
#         else:
#             cur.execute(popsql)
#         con.commit()
#         if opencon:
#             dbengine.set_shared_cursor(cur)
#         else:
#             con.close()
#         return dbengine

#     def popdb(con, sql):
#         cur = con.cursor()
#         cur.executescript(sql)
#         con.commit()
