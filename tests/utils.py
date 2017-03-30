from collections import OrderedDict
from contextlib import contextmanager
import os
import sqlite3


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


class TestDatabase():
    def __init__(self, *args, **kwargs):
        # read sql into variable
        dir_path = os.path.dirname(os.path.realpath(__file__))
        sqlfile_path = os.path.join(dir_path, "starwars.sql")
        with open(sqlfile_path, 'r') as f:
            self.starwars_sql = f.read()

    @contextmanager
    def starwars_db(self):
        # pop db
        with sqlite3.connect(':memory:') as con:
            cur = con.cursor()
            cur.executescript(self.starwars_sql)
            con.commit()
            yield con

    def connect(self):
        # pop db
        con = sqlite3.connect(':memory:')
        cur = con.cursor()
        cur.executescript(self.starwars_sql)
        con.commit()
        return con
