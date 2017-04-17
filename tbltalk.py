#!/usr/bin/env python3
"""
A micro ORM (sorta) for Python inspired by Massive. Because sometimes you just
need a simple, single file database wrapper.
"""
import re
import inspect
from datetime import datetime
from collections import OrderedDict, namedtuple
SqlStatement = namedtuple('SqlStatement', 'sql params')
PagedResult = namedtuple('PagedResult',
                         ['total_records', 'total_pages', 'page_size',
                          'current_page', 'records'])


class DbEngine:
    ''' A database engine impleminting DB-API v2.0+ '''
    def __init__(self, dbapi, *args, **kwargs):
        self.dbapi = dbapi
        self.args = args
        self.kwargs = kwargs

    def connect(self):
        return self.dbapi.connect(*self.args, **self.kwargs)


class OpenConnectionDbEngine:
    '''
    A way to manage the connection externally, thank-you-very-much.
    Super-useful for SQLite in-memory dbs.
    '''
    def __init__(self, dbapi, conn):
        self.dbapi = dbapi
        self.conn = conn

    def connect(self):
        return self.conn


class DotDict(OrderedDict):
    '''
    Quick and dirty implementation of a dot-able dict, which allows access and
    assignment via object properties rather than dict indexing.
    '''
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

    __setattr__ = OrderedDict.__setitem__


def first(iterable, default=None, raiseOnEmpty=False):
    for element in iterable:
        return element
    if raiseOnEmpty:
        raise ValueError("Empty iterable")
    else:
        return default


def dotdict_row_factory(cur, row):
    '''DB API helper to turn db rows from a cursor into DotDict objects'''
    result = DotDict()
    for idx, col in enumerate(cur.description):
        result[col[0]] = row[idx]
    return result


def to_dotdict(obj):
    if isinstance(obj, DotDict):
        return obj
    elif isinstance(obj, dict):
        return DotDict(obj)
    else:
        result = DotDict()
        for name in dir(obj):
            value = getattr(obj, name)
            if not name.startswith('__') and not inspect.ismethod(value):
                result[name] = value
        return result


class DbTable:
    ''' A class that wraps your database table '''

    def __init__(self, dbengine, table_name=None, pk_field="id",
                 pk_autonumber=True, limit_keyword="LIMIT"):
        self._dbengine = dbengine
        self._schema = None
        self.table_name = table_name or self.__class__.__name__
        self.pk_field = pk_field
        self.pk_autonumber = pk_autonumber
        self.limit_keyword = limit_keyword
        pat = "min|max|count|avg|sum|single|one|first|last|(find|get)(_by)?"
        self._re_dynamic_methods = re.compile(pat)

    def connect(self):
        return self._dbengine.connect()

    def query(self, sql, *params):
        ''' Yields each row from a SQL query '''
        with self.connect() as con:
            # con.row_factory has side effects for always open connections
            cur = con.cursor()
            for row in cur.execute(sql, *params):
                yield dotdict_row_factory(cur, row)

    def scalar(self, sql, *params):
        ''' Returns a single value '''
        with self.connect() as con:
            cur = con.cursor()
            cur.execute(sql, *params)
            result = cur.fetchone()
            if result is None or len(result) == 0:
                return None
            else:
                return result[0]

    def execute(self, sql, *params):
        ''' Executes a SQL statement '''
        with self.connect() as con:
            cur = con.cursor()
            cur.execute(sql, *params)

    def executemany(self, sql, *params):
        ''' Executes a SQL statement multiple times for each set of params'''
        with self.connect() as con:
            cur = con.cursor()
            cur.executemany(sql, *params)

    def executescript(self, sql, *params):
        ''' Executes a SQL statement '''
        with self.connect() as con:
            cur = con.cursor()
            cur.executescript(sql, *params)

    def get_by_id(self, id):
        '''
        Gets a record matching the id provided
        '''
        where = f"{self.pk_field} = {self.make_sqlparam(self.pk_field)}"
        return self.single(where=where, params=(id,))

    def has_pk(self, obj):
        '''
        Determines if the object provided has a property with a name matching
        whatever self.pk_field is set to.
        '''
        return self.pk_field in to_dotdict(obj)

    def get_pk(self, obj):
        '''
        If the object provided has a property with a name matching
        whatever self.pk_field is set to, that value is returned.
        '''
        return to_dotdict(obj).get(self.pk_field, None)

    def create_delete_sql(self, where=None):
        '''
        Makes a SQL delete statement to remove records from the DB according to
        either the provided WHERE.
        '''
        sql = f"DELETE FROM {self.table_name}"
        if where:
            sql += f" WHERE {where}"
        return sql

    def delete_by_id(self, id=None):
        '''
        Deletes a record matching the id provided
        '''
        where = f"{self.pk_field} = {self.make_sqlparam(self.pk_field)}"
        self.delete(where=where, params=(id,))

    def delete(self, where=None, params=()):
        '''
        Executes a SQL delete statement to remove records from the DB
        according to provided WHERE condition.
        '''
        sql = self.create_delete_sql(where=where)
        self.execute(sql, params)

    def create_insert_statement(self, obj):
        '''
        Makes an INSERT SqlStatement to add a record to the DB.
        '''
        obj = to_dotdict(obj)
        filtered_cols = [(k, v) for k, v in obj.items()
                         if k.lower() != self.pk_field.lower() or
                         not self.pk_autonumber]
        cols, params = zip(*filtered_cols)
        if len(cols) == 0:
            raise ValueError("Cannot use an object with no properties to "
                             "create an INSERT statement")
        sqlcol = ", ".join(cols)
        paramvars = [self.make_sqlparam(c, i) for i, c in enumerate(cols)]
        sqlvars = ", ".join(paramvars)
        sql = (f"INSERT INTO {self.table_name} ({sqlcol})\n"
               f"VALUES ({sqlvars})")
        return SqlStatement(sql, params)

    def insert(self, obj):
        '''
        Executes a SQL insert statement to add a record to the DB.
        '''
        with self.connect() as con:
            cur = con.cursor()
            (sql, params) = self.create_insert_statement(obj)
            cur.execute(sql, params)
            cur.execute("select last_insert_rowid()")
            id = cur.fetchone()[0]
        return id

    def create_update_statement(self, obj, id=None):
        '''
        Makes a SQL update statement to modify a record in the DB.
        '''
        obj = to_dotdict(obj)
        if id is None:
            id = obj[self.pk_field]
        filtered_cols = [(k, v) for k, v in obj.items()
                         if k.lower() != self.pk_field.lower()]
        cols, param_vals = zip(*filtered_cols)
        if len(cols) == 0:
            raise ValueError("Cannot use an object with no properties to "
                             "create an UPDATE statement")

        # make SET clause
        setitems = [(c, self.make_sqlparam(c, i))
                    for i, c in enumerate(cols)]
        setclause = "\n,".join([f"{col} = {p}" for col, p in setitems])

        # make where
        param_vals = list(param_vals)
        param_vals.append(id)
        pk_param = self.make_sqlparam(self.pk_field, len(cols))
        where = f"{self.pk_field} = {pk_param}"

        sql = (f"UPDATE {self.table_name}\n"
               f"SET {setclause}\n"
               f"WHERE {where}")
        return SqlStatement(sql, tuple(param_vals))

    def update(self, obj, id=None):
        '''
        Executes a SQL update statement to modify a record in the DB.
        '''
        stmt = self.create_update_statement(obj, id)
        self.execute(stmt.sql, stmt.params)

    def create_upsert_statement(self, obj):
        if self.has_pk(obj):
            return self.create_update_statement(obj)
        else:
            return self.create_insert_statement(obj)

    def create_select_sql(self, columns="*", distinct=False, where=None,
                          groupby=None, having=None, orderby=None, limit=None):
        '''
        Returns all records matching the provided WHERE clause and arguments,
        ordered as specified, and limited if specified.
        '''
        # helper
        def check_cols(x):
            result = x or ""
            if (not isinstance(x, str) and hasattr(x, '__iter__')):
                result = ", ".join(x)
            if ';' in result or "'" in result:
                raise ValueError(f"Possible SQL Injection detected in "
                                 f"SQL input: {x}")
            return result

        def get_toplimit(l):
            topsql, limitsql = "", ""
            lkw = self.limit_keyword.upper()
            if l is not None:
                if lkw == "LIMIT":
                    limitsql = f" LIMIT {l}"
                elif lkw == "TOP":
                    topsql = f" TOP {l}"
                else:
                    raise ValueError(f"Unhandled limit keyword: {lkw}")
            return (topsql, limitsql)

        distinct = " DISTINCT" if distinct else ""
        columns = check_cols(columns)
        orderby = check_cols(orderby)
        groupby = check_cols(groupby)
        top, limit = get_toplimit(limit)

        sql = f"SELECT{distinct}{top} {columns} FROM {self.table_name}"
        if where:
            sql += f" WHERE {where}"
        if groupby:
            sql += f" GROUP BY {groupby}"
        if having:
            sql += f" HAVING {having}"
        if orderby:
            sql += f" ORDER BY {orderby}"
        sql += limit

        return sql

    def all(self, columns="*", distinct=False, where=None, groupby=None,
            having=None, orderby=None, limit=None, params=()):
        '''
        Returns all records matching the provided WHERE clause and arguments,
        ordered as specified, and limited if specified.
        '''
        sql = self.create_select_sql(columns=columns, distinct=distinct,
                                     where=where, groupby=groupby,
                                     having=having, orderby=orderby,
                                     limit=limit)
        return list(self.query(sql, params))

    def paged(self, columns="*", distinct=False, where=None, groupby=None,
              having=None, orderby=None, page_size=20, current_page=1,
              params=()):
        total_records = self.count(distinct=distinct, where=where,
                                   groupby=groupby, having=having,
                                   orderby=orderby, params=params)
        result = DotDict()
        result.total_records = total_records
        result.page_size = page_size
        result.current_page = current_page
        limit = f"{(current_page - 1) * page_size}, {page_size}"
        result.records = self.all(columns=columns, distinct=distinct,
                                  where=where, groupby=groupby, having=having,
                                  orderby=orderby, limit=limit, params=params)
        return result

    def save(self, *args):
        statements = []
        for obj in args:
            statements.append(self.create_upsert_statement(obj))
        with self.connect() as con:
            cur = con.cursor()
            for stmt in statements:
                cur.execute(stmt.sql, stmt.params)

    def __getattr__(self, name):
        '''
        If an unrecognized method is called, assume we wanted dynamicquery
        '''
        if not self._re_dynamic_methods.match(name):
            raise AttributeError(f"object has no attribute '{name}'")

        def runit(*args, **kwargs):
            try:
                return self.dynamicquery(name, *args, **kwargs)
            except Exception as err:
                raise RuntimeError(f"Method not found: {name}. dynamicquery "
                                   f"called instead and it errored") from err
        return runit

    def dynamicquery(self, method_name, *args, **kwargs):
        if len(args) > 0:
            raise ValueError(f"Error in dynamicquery call to {method_name}: "
                             f"Named arguments are required for this type of "
                             f"query - the column name, orderby, columns, etc")
        select_parts = {
            'columns': "*",
            'distinct': False,
            'where': None,
            'groupby': None,
            'having': None,
            'orderby': self.pk_field,
            'limit': None
        }

        # call it what you like, the DB cares but you don't have to.
        # note: don't be fooled - keyword smoothing happens in
        #       create_select_sql, not here.
        aliases = {"column": "columns", "select": "columns", "top": "limit"}

        # key := method_name, val := columns replacement
        agg_methods = {
            'count': "COUNT(*)",
            'sum': "SUM({{column}})",
            'max': "MAX({{column}})",
            'min': "MIN({{column}})",
            'avg': "AVG({{column}})",
        }

        # key := prefix, val := orderby direction
        one_result_methods = {
            'single': '',
            'one': '',
            'fetchone': '',
            'first': '',
            'last': ' DESC'
        }

        counter = 0
        constraints = []
        params = []
        lc_kwargs = {k.lower(): v for k, v in kwargs.items()}

        # arguments are considered to be either known select clause parts,
        # or else where filters
        for key, value in lc_kwargs.items():
            if key == "params":
                params += value
            elif key in select_parts:
                select_parts[key] = value
            elif key in aliases:
                select_parts[aliases[key]] = value
            else:
                # other keywords are considered WHERE equality conditions
                p = self.make_sqlparam(key, counter)
                constraints.append(f"{key} = {p}")
                params.append(value)
                counter += 1

        if select_parts['where'] and len(constraints) > 0:
            raise ValueError("Cannot mix passing in a where clause and "
                             "constraint parameters. Unsure how to make a "
                             "WHERE from that.")

        if len(constraints) > 0:
            select_parts['where'] = " AND ".join(constraints)

        if method_name in agg_methods:
            ag, col = agg_methods[method_name], select_parts['columns']
            select_parts['columns'] = ag.replace("{{column}}", col)
            sqlselect = self.create_select_sql(**select_parts)
            return self.scalar(sqlselect, tuple(params))
        else:
            found = [(k, v) for k, v in one_result_methods.items()
                     if method_name.startswith(k)]
            is_one = len(found) > 0
            if is_one:
                select_parts['orderby'] += found[0][1]
                select_parts['limit'] = 1
                sqlselect = self.create_select_sql(**select_parts)
                return first(self.query(sqlselect, tuple(params)))
            else:
                sqlselect = self.create_select_sql(**select_parts)
                return list(self.query(sqlselect, tuple(params)))

    def make_sqlparam(self, name, index=0):
        """Returns a sql parameter for the specified paramstyle"""
        # we need to be extra careful with params to prevent injection.
        if not isinstance(index, int):
            raise ValueError(f"non-integer index value: {index}")
        if ';' in name or "'" in name:
            raise ValueError("Unsupported name value: {name}")

        # https://www.python.org/dev/peps/pep-0249/#paramstyle
        paramstyle = self._dbengine.dbapi.paramstyle
        if paramstyle == 'qmark':
            return '?'
        elif paramstyle == 'numeric':
            return f':{index}'
        elif paramstyle == 'named':
            return f':{name}'
        elif paramstyle == 'format':
            return f'%s'
        elif paramstyle == 'pyformat':
            return f'%({name})s'
        else:
            raise ValueError(f"Unsupported paramstyle: {paramstyle}")
