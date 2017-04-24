#!/usr/bin/env python3
'''
A single-file, python micro-ORM that embraces DB-API, the power of SQL, the
simplicity of one-object-per-table, and the Zen of Python.
'''


class SqlResultException(Exception):
    pass


class DbConnection:
    '''
    A wrapper for any DB-API v2.0+ database connection implementation.

    Args:
        impl: Any DB-API v2.0 database connection
    '''
    def __init__(self, impl):
        self.con = impl
        self.row_factory = None
        if hasattr(impl, "row_factory"):
            self.row_factory = impl.row_factory

    def __getattr__(self, key):
        return getattr(self.con, key)

    def __enter__(self, *args):
        return self

    def __exit__(self, *args):
        self.con.__exit__(*args)

    def _process_row(self, cur, row):
        if self.row_factory:
            return self.row_factory(cur, row)
        else:
            return row

    def execute(self, sql, params=()):
        '''Executes a SQL statement'''
        cur = self.cursor()
        cur.execute(sql, params)
        return cur

    def executemany(self, sql, params=()):
        '''Executes a SQL statement'''
        cur = self.cursor()
        cur.executemany(sql, params)

    def executescript(self, sql):
        cur = self.cursor()
        exs = getattr(cur, "executescript", None)
        if callable(exs):
            cur.executescript(sql)
        else:
            cur.execute(sql)

    def iter(self, sql, params=()):
        '''Yields each row from a SQL query'''
        cur = self.execute(sql, params)
        for row in cur:
            yield self._process_row(cur, row)

    def scalar(self, sql, params=()):
        '''Returns a single value'''
        cur = self.execute(sql, params)
        result = cur.fetchone()
        return result[0] if result else None

    def one(self, sql, params=()):
        '''Returns one row'''
        cur = self.execute(sql, params)
        if cur.rowcount == 0:
            return None
        return self._process_row(cur, cur.fetchone())

    def exactly_one(self, sql, params=()):
        '''Returns a single row. If not exactly one row, an error is raised'''
        iresults = self.iter(sql, params)
        result = None
        try:
            result = next(iresults)
        except StopIteration:
            raise SqlResultException("No records found when expecting one")

        try:
            next(iresults)
            raise SqlResultException("Multiple records found when expecting one")
        except StopIteration:
            return result

    def all(self, sql, params=()):
        '''Returns all the results of a query'''
        return list(self.iter(sql, params))
