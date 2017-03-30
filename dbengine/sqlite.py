import sqlite3
from contextlib import contextmanager


class SqliteOpenConnectionEngine:
    def __init__(self, con):
        self.con = con

    def __enter__(self):
        pass

    def __exit__(self, *args):
        pass

    def connect(self):
        return self.con

    @property
    def limit_keyword(self):
        ''' LIMIT or TOP '''
        return "LIMIT"

    def get_paramvar(self, index=0, name=""):
        return '?'


class SqliteEngine:
    def __init__(self, *args, **kwargs):
        self.args = args
        self.kwargs = kwargs

    def connect(self):
        return sqlite3.connect(*self.args, **self.kwargs)

    @property
    def limit_keyword(self):
        ''' LIMIT or TOP '''
        return "LIMIT"

    def get_paramvar(self, index=0, name=""):
        return '?'
