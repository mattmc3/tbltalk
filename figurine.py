#!/usr/bin/env python3
"""
A micro ORM (sorta) for Python inspired by Massive. Because sometimes you just
need a simple, single file database wrapper.
"""
from datetime import datetime
from collections import OrderedDict, namedtuple


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

    __setattr__ = dict.__setitem__


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


SqlStatement = namedtuple('SqlStatement', 'sql params')


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

    def connect(self):
        return self._dbengine.connect()

    def query(self, sql, *params):
        ''' Yields each row from a SQL query '''
        with self.connect() as con:
            con.row_factory = dotdict_row_factory
            cur = con.cursor()
            for row in cur.execute(sql, *params):
                yield row

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

    def get_by_id(self, id):
        '''
        Gets a record matching the id provided
        '''
        where = f"{self.pk_field} = {self._get_paramvar(self.pk_field)}"
        return self.single(where=where, params=(id,))

    def has_pk(self, obj):
        '''
        Determines if the object provided has a property with a name matching
        whatever self.pk_field is set to.
        '''
        return hasattr(obj, self.pk_field)

    def get_pk(self, obj):
        '''
        If the object provided has a property with a name matching
        whatever self.pk_field is set to, that value is returned.
        '''
        return obj.get(self.pk_field, None)

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
        where = f"{self.pk_field} = {self._get_paramvar(self.pk_field)}"
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
        filtered_cols = [(k, v) for k, v in obj.items()
                         if k.lower() != self.pk_field.lower() or
                         not self.pk_autonumber]
        cols, params = zip(*filtered_cols)
        if len(cols) == 0:
            raise ValueError("Cannot use an object with no properties to "
                             "create an INSERT statement")
        sqlcol = ", ".join(cols)
        paramvars = [self._get_paramvar(c, i) for i, c in enumerate(cols)]
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

    def create_update_statement(self, obj, id):
        '''
        Makes a SQL update statement to modify a record in the DB.
        '''
        filtered_cols = [(k, v) for k, v in obj.items()
                         if k.lower() != self.pk_field.lower()]
        cols, param_vals = zip(*filtered_cols)
        if len(cols) == 0:
            raise ValueError("Cannot use an object with no properties to "
                             "create an UPDATE statement")

        # make SET clause
        setitems = [(c, self._get_paramvar(c, i))
                    for i, c in enumerate(cols)]
        setclause = "\n,".join([f"{col} = {p}" for col, p in setitems])

        # make where
        param_vals = list(param_vals)
        param_vals.append(id)
        pk_param = self._get_paramvar(self.pk_field, len(cols))
        where = f"{self.pk_field} = {pk_param}"

        sql = (f"UPDATE {self.table_name}\n"
               f"SET {setclause}\n"
               f"WHERE {where}")
        return SqlStatement(sql, tuple(param_vals))

    def update(self, obj, id):
        '''
        Executes a SQL update statement to modify a record in the DB.
        '''
        stmt = self.create_update_statement(obj, id)
        self.execute(stmt.sql, stmt.params)

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
                if not isinstance(l, int):
                    raise ValueError(f"Limit is not an int: {l}")
                elif lkw == "TOP":
                    topsql = f" TOP {l}"
                elif lkw == "LIMIT":
                    limitsql = f" LIMIT {l}"
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

    def all(self, columns="*", distinct=False, where=None,
            orderby=None, limit=None, params=()):
        '''
        Returns all records matching the provided WHERE clause and arguments,
        ordered as specified, and limited if specified.
        '''
        sql = self.create_select_sql(limit=limit, columns=columns,
                                     distinct=distinct, orderby=orderby,
                                     where=where)
        return self.query(sql, params)

    def paged(self, columns="*", where=None, orderby=None, pagesize=20,
              currentpage=1, params=()):
        ''' TODO: Implement me! '''
        pass

    def save(self, *args):
        ''' TODO: Implement me! '''
        pass

    def build_statements(self):
        ''' TODO: Implement me! '''
        pass

    def __getattr__(self, name):
        '''
        If an unrecognized method is called, assume we wanted dynamicquery
        '''
        return lambda *args, **kwargs: self.dynamicquery(name, *args, **kwargs)

    def dynamicquery(self, method_name, *args, **kwargs):
        if len(args) > 0:
            raise ValueError(f"Error in dynamic call to {method_name}:"
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
                p = self._get_paramvar(key, counter)
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

    def _get_paramvar(self, name, index=0):
        # we need to be super careful with params
        if not isinstance(index, int):
            raise ValueError("non-integer index value")
        if ';' in name or "'" in name:
            raise ValueError("bad name value")

        # https://www.python.org/dev/peps/pep-0249/#paramstyle
        ps = self._dbengine.dbapi.paramstyle
        if ps == 'qmark':
            return '?'
        elif ps == 'numeric':
            return f':{index}'
        elif ps == 'named':
            return f':{name}'
        elif ps == 'format':
            return f'%s'
        elif ps == 'pyformat':
            return f'%({name})s'
        else:
            raise ValueError(f"Unsupported paramstyle: {ps}")
