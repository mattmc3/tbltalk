#!/usr/bin/env python3
'''
A single-file, python micro-ORM that embraces DB-API, the power of SQL, the
simplicity of one-object-per-table, and the Zen of Python.
'''
import re
import inspect
from contextlib import contextmanager
from datetime import datetime
from collections import OrderedDict, namedtuple, Mapping
SqlStatement = namedtuple('SqlStatement', 'sql params')
PagedResult = namedtuple('PagedResult',
                         ['total_records', 'total_pages', 'page_size',
                          'current_page', 'records'])


class DbEngine:
    ''' A database engine impleminting DB-API v2.0+ '''
    def __init__(self, dbapi, dialect, *args, **kwargs):
        self.dbapi = dbapi
        self.args = args
        self.kwargs = kwargs
        self.dialect = to_dotdict(dialect)
        self._con = None
        self._cur = None

    def connect(self):
        return self.dbapi.connect(*self.args, **self.kwargs)

    @contextmanager
    def cursor(self):
        if self._con:
            con = self._con
            cur = self._cur
            closeit = False
        else:
            con = self.connect()
            cur = con.cursor()
            closeit = True

        try:
            yield cur
            con.commit()
        except Exception:
            con.rollback()
            raise
        finally:
            if closeit:
                con.close()

    def set_shared_connection(self, con):
        self._con = con
        self._cur = con.cursor()

    @contextmanager
    def use_shared_connection(self, con):
        if self._con and self._con != con:
            raise RuntimeError(("Trying to use a shared cursor when one is "
                                "already in use."))
        self._con = con
        self._cur = self._con.cursor()
        yield self._cur
        self._con = None
        self._cur = None


class DotDict(OrderedDict):
    '''
    Quick and dirty implementation of a dot-able dict, which allows access and
    assignment via object properties rather than dict indexing.
    '''
    def __init__(self, *args, **kwargs):
        od = OrderedDict(*args, **kwargs)
        for key, val in od.items():
            if isinstance(val, Mapping):
                value = DotDict(val)
            self[key] = val

    def __delattr__(self, name):
        try:
            del self[name]
        except KeyError as ex:
            raise AttributeError(f"No attribute called: {name}") from ex

    def __getattr__(self, k):
        try:
            return = self[k]
        except KeyError as ex:
            raise AttributeError(f"No attribute called: {k}") from ex

    __setattr__ = OrderedDict.__setitem__


def first(iterable, default=None, raiseOnEmpty=False):
    for element in iterable:
        return element
    if raiseOnEmpty:
        raise ValueError("Empty iterable")
    else:
        return default


def sqlparam(paramstyle, name=None, index=0):
    ''' Returns a sql parameter for the specified paramstyle '''
    # we need to be extra careful with params to prevent injection.
    if not isinstance(index, int):
        raise ValueError(f"non-integer index value: {index}")
    if name and (';' in name or "'" in name):
        raise ValueError("Unsupported name value: {name}")

    # https://www.python.org/dev/peps/pep-0249/#paramstyle
    if paramstyle == 'qmark':
        return '?'
    elif paramstyle == 'numeric':
        return f':{index}'
    elif paramstyle == 'named':
        if name is None:
            raise ValueError("name is None for paramstyle == 'named'")
        return f':{name}'
    elif paramstyle == 'format':
        return f'%s'
    elif paramstyle == 'pyformat':
        # if name is None:
        #    return f'%s'
        # else:
        #    return f'%({name})s'
        return f'%s'
    else:
        raise ValueError(f"Unsupported paramstyle: {paramstyle}")


def dotdict_row_factory(cur, row):
    '''DB API helper to turn db rows from a cursor into DotDict objects'''
    result = DotDict()
    for idx, col in enumerate(cur.description):
        result[col[0]] = row[idx]
    return result


def to_dotdict(obj):
    ''' Converts an object to a DotDict '''
    if isinstance(obj, DotDict):
        return obj
    elif isinstance(obj, Mapping):
        return DotDict(obj)
    else:
        result = DotDict()
        for name in dir(obj):
            value = getattr(obj, name)
            if not name.startswith('__') and not inspect.ismethod(value):
                result[name] = value
        return result


def safeformat(str, **kwargs):
    ''' string format with safe replacements '''
    class SafeDict(dict):
        def __missing__(self, key):
            return '{' + key + '}'
    replacements = SafeDict(**kwargs)
    return str.format_map(replacements)


class DbTable:
    ''' A class that wraps your database table '''

    def __init__(self, dbengine, table_name=None, pk_field="id",
                 pk_autonumber=True):
        self._dbengine = dbengine
        self._schema = None
        self.table_name = table_name or self.__class__.__name__
        self.pk_field = pk_field
        self.pk_autonumber = pk_autonumber
        pat = "single|one|first|last|(find|get)(_by)?"
        self._re_dynamic_methods = re.compile(pat)
        self._ext_cur = None

    @property
    def dialect(self):
        return self._dbengine.dialect

    @contextmanager
    def cursor(self):
        with self._dbengine.cursor() as cur:
            yield cur

    @contextmanager
    def shared_connection(self, con):
        with self._dbengine.shared_connection(con) as con:
            yield con

    def set_shared_connection(self, con):
        self._dbengine.set_shared_connection(con)

    def query(self, sql, params=()):
        ''' Yields each row from a SQL query '''
        with self.cursor() as cur:
            # con.row_factory has side effects for always open connections
            cur.execute(sql, params)
            for row in cur:
                yield dotdict_row_factory(cur, row)

    def scalar(self, sql, params=()):
        ''' Returns a single value '''
        with self.cursor() as cur:
            cur.execute(sql, params)
            result = cur.fetchone()
            if result is None or len(result) == 0:
                return None
            else:
                return result[0]

    def execute(self, sql, params=()):
        ''' Executes a SQL statement '''
        with self.cursor() as cur:
            cur.execute(sql, params)

    def executemany(self, sql, params=()):
        ''' Executes a SQL statement multiple times for each set of params'''
        with self.cursor() as cur:
            cur.executemany(sql, params)

    def executescript(self, sql, params=()):
        ''' Executes a SQL statement '''
        with self.cursor() as cur:
            cur.executescript(sql, params)

    def get_by_id(self, id):
        '''
        Gets a record matching the id provided
        '''
        where = f"{self.pk_field} = {self.sqlparam(self.pk_field)}"
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
        sqlfmt = self.dialect.delete_sql
        sql = sqlfmt.format(table=self.table_name)
        if where:
            sql += f" {self.dialect.keywords.where} {where}"
        return sql

    def delete_by_id(self, id=None):
        '''
        Deletes a record matching the id provided
        '''
        where = f"{self.pk_field} = {self.sqlparam(self.pk_field)}"
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
        columns = ", ".join(cols)
        paramvars = [self.sqlparam(c, i) for i, c in enumerate(cols)]
        values = ", ".join(paramvars)
        sqlfmt = self.dialect.insert_sql
        sql = sqlfmt.format(table=self.table_name, columns=columns,
                            values=values, pk_field=self.pk_field)
        return SqlStatement(sql, params)

    def insert(self, obj):
        '''
        Executes a SQL insert statement to add a record to the DB.
        '''
        (sql, params) = self.create_insert_statement(obj)
        with self.cursor() as cur:
            cur.execute(sql, params)

            result = None
            if self.dialect.get_last_inserted_id is None:
                try:
                    result = cur.fetchone()
                except Exception:
                    pass
            else:
                cur.execute(self.dialect.get_last_inserted_id)
                result = cur.fetchone()

            id = result[0] if result else 0
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
        setitems = [(c, self.sqlparam(c, i))
                    for i, c in enumerate(cols)]
        setclause = ", ".join([f"{col} = {p}" for col, p in setitems])

        # make where
        param_vals = list(param_vals)
        param_vals.append(id)
        pk_param = self.sqlparam(self.pk_field, len(cols))
        where = f" WHERE {self.pk_field} = {pk_param}"

        sqlfmt = self.dialect.update_sql
        sql = sqlfmt.format(table=self.table_name, set_columns=setclause)
        sql += where
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
        return self._create_select_sql_impl(
            self.dialect.select_sql, columns=columns, distinct=distinct,
            table=self.table_name, where=where, groupby=groupby,
            having=having, orderby=orderby, limit=limit)

    def _create_select_sql_impl(self, select_sqlfmt, columns="*",
                                distinct=False, table=None, where=None,
                                groupby=None, having=None, orderby=None,
                                limit=None, **kwargs):
        def check_cols(cols):
            result = cols or ""
            if (not isinstance(cols, str) and hasattr(cols, '__iter__')):
                result = ", ".join(cols)
            if ';' in result or "'" in result:
                raise ValueError(f"Possible SQL Injection detected in "
                                 f"SQL input: {cols}")
            return result

        def sqlpart(keyword, clause):
            if clause is None or clause == "":
                return ""
            else:
                return " {} {}".format(keyword, clause)

        columns = check_cols(columns)
        orderby = check_cols(orderby)
        groupby = check_cols(groupby)

        distinct = " " + self.dialect.keywords.distinct if distinct else ""
        where = sqlpart(self.dialect.keywords.where, where)
        groupby = sqlpart(self.dialect.keywords.groupby, groupby)
        having = sqlpart(self.dialect.keywords.having, having)
        orderby = sqlpart(self.dialect.keywords.orderby, orderby)
        limit = sqlpart(self.dialect.keywords.limit, limit)

        sql = safeformat(select_sqlfmt, distinct=distinct, columns=columns,
                         table=table, where=where, groupby=groupby,
                         having=having, orderby=orderby, limit=limit)
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

    def one(self, columns="*", distinct=False, where=None, groupby=None,
            having=None, orderby=None, limit=None, params=()):
        '''
        Returns a single record.
        '''
        sql = self.create_select_sql(columns=columns, distinct=distinct,
                                     where=where, groupby=groupby,
                                     having=having, orderby=orderby,
                                     limit=limit)
        return first(self.query(sql, params))

    def count(self, column="*", distinct_count=False, where=None, params=()):
        ''' Returns the count of records matching the where condition. '''
        if distinct_count:
            column = "{} {}".format(self.dialect.keywords.distinct, column)
        return self._agg(self.dialect.keywords.count, column, where, params)

    def avg(self, column, where=None, params=()):
        '''
        Returns the average column value for records matching the where
        condition.
        '''
        return self._agg(self.dialect.keywords.avg, column, where, params)

    def min(self, column, where=None, params=()):
        '''
        Returns the minimum column value for records matching the where
        condition.
        '''
        return self._agg(self.dialect.keywords.min, column, where, params)

    def max(self, column, where=None, params=()):
        '''
        Returns the maximum column value for records matching the where
        condition.
        '''
        return self._agg(self.dialect.keywords.max, column, where, params)

    def _agg(self, agg_fn, column, where, params):
        columns = "{}({}) aggfield1".format(agg_fn, column)
        sql = self.create_select_sql(columns=columns, where=where)
        return self.scalar(sql, params)

    def paged(self, columns="*", distinct=False, where=None, groupby=None,
              having=None, orderby=None, page_size=20, current_page=1,
              params=()):

        # get the total count
        countsqlfmt = self.dialect.paging[0]
        subquery = self.create_select_sql(columns="1 one", distinct=distinct,
                                          where=where, groupby=groupby,
                                          having=having)
        countsql = countsqlfmt.format(subquery=subquery)
        total_records = self.scalar(countsql, params)

        # get the paged result
        page_start = (current_page - 1) * page_size
        pagingsqlfmt = self.dialect.paging[1]
        pagingsql = self._create_select_sql_impl(
            pagingsqlfmt, columns=columns, distinct=distinct,
            table=self.table_name, where=where, groupby=groupby,
            having=having, orderby=orderby)
        pagingsql = pagingsql.format(page_size=page_size,
                                     page_start=page_start,
                                     current_page=current_page)

        result = DotDict()
        result.total_records = total_records
        result.page_size = page_size
        result.current_page = current_page
        result.records = list(self.query(pagingsql, params=params))
        return result

    def save(self, *args):
        statements = []
        for obj in args:
            statements.append(self.create_upsert_statement(obj))
        with self.cursor() as cur:
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
        method_name = method_name.lower()
        select_parts = {
            'columns': "*",
            'distinct': False,
            'where': None,
            'groupby': None,
            'having': None,
            'orderby': None,
            'limit': None
        }

        # call it what you like, the DB cares but you don't have to.
        # note: don't be fooled - keyword smoothing happens in
        #       create_select_sql, not here.
        aliases = {"column": "columns", "select": "columns", "top": "limit"}

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
                p = self.sqlparam(key, counter)
                constraints.append(f"{key} = {p}")
                params.append(value)
                counter += 1

        if select_parts['where'] and len(constraints) > 0:
            raise ValueError("Cannot mix passing in a where clause and "
                             "constraint parameters. Unsure how to make a "
                             "WHERE from that.")

        if len(constraints) > 0:
            select_parts['where'] = " AND ".join(constraints)

        if not select_parts['orderby']:
            select_parts['orderby'] = [self.pk_field]

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

    def sqlparam(self, name, index=0):
        '''
        Returns a sql parameter specific to the needs of the current dbengine
        '''
        return sqlparam(self._dbengine.dbapi.paramstyle, name, index)
