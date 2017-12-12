from collections import OrderedDict
from contextlib import contextmanager
from collections import namedtuple, Mapping
from tbltalk.db import (sqlite3_dialect, postgres_dialect, mssql_dialect,
                        mariadb_dialect)
import os
import sqlite3

DbBackend = namedtuple('DbBackend', 'dbapi dialect popsql')


class DotDict(dict):
    '''
    A dot-able dict, which allows value assignment and retrieval via
    object properties as well as dict key lookups.
    '''
    def __init__(self, *args, **kwargs):
        if not kwargs or not kwargs.get("deep", False):
            super(DotDict, self).__init__(*args, **kwargs)
        else:
            d = dict(*args, **kwargs)
            for key, val in d.items():
                if isinstance(val, Mapping):
                    val = DotDict(val)
                self[key] = val

    def __delattr__(self, name):
        try:
            del self[name]
        except KeyError as ex:
            msg = "No attribute called: {}".format(name)
            raise AttributeError(msg)

    def __getattr__(self, k):
        try:
            return self[k]
        except KeyError as ex:
            msg = "No attribute called: {}".format(k)
            raise AttributeError(msg)

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
        msg = "unrecognized backend: {}".format(dbbackend_name)
        raise ValueError(msg)


def get_args_and_kwargs(*args, **kwargs):
    return (args, kwargs)


def popdb(popsql, cur):
    exs = getattr(cur, "executescript", None)
    if callable(exs):
        cur.executescript(popsql)
    else:
        cur.execute(popsql)
