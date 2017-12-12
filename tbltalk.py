#!/usr/bin/env python
'''
A single-file, python micro-ORM that embraces DB-API, the power of SQL, the
simplicity of one-object-per-table, and the Zen of Python.
'''

import re
import inspect
from collections import OrderedDict, namedtuple, Mapping
from copy import deepcopy

VERSION = (1, 0, 0)
__version__ = ".".join([str(part) for part in VERSION])
__license__ = "MIT"
__author__ = "Matt McElheny <mattmc3@gmail.com>"

SqlStatement = namedtuple('SqlStatement', 'sql params')
PagedResult = namedtuple('PagedResult',
                         ['total_records', 'total_pages', 'page_size',
                          'current_page', 'records'])


class SqlResultError(Exception):
    '''
    Custom tbltalk exception raised when there's an issue with a SQL result
    '''
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
            raise SqlResultError("No records found when expecting one")

        try:
            next(iresults)
            msg = "Multiple records found when expecting one"
            raise SqlResultError(msg)
        except StopIteration:
            return result

    def all(self, sql, params=()):
        '''Returns all the results of a query'''
        return list(self.iter(sql, params))


class DbRow(OrderedDict):
    '''
    Represents the results of a single database row. Behaves like an
    OrderedDict with column values accessible as by keys in addition to
    object properties.
    '''
    def __init__(self, *args, **kwargs):
        super(DbRow, self).__init__(*args, **kwargs)

    def __delattr__(self, name):
        try:
            del self[name]
        except KeyError as ex:
            msg = "No attribute called: {}".format(name)
            raise AttributeError(msg) from ex

    def __getattr__(self, k):
        try:
            return self[k]
        except KeyError as ex:
            msg = "No attribute called: {}".format(k)
            raise AttributeError(msg) from ex

    __setattr__ = OrderedDict.__setitem__

    @staticmethod
    def from_object(obj):
        ''' Converts an object to a DbRow '''
        if isinstance(obj, DbRow):
            return obj
        elif isinstance(obj, Mapping):
            return DbRow(obj)
        else:
            result = DbRow()
            for name in dir(obj):
                value = getattr(obj, name)
                if not name.startswith('__') and not inspect.ismethod(value):
                    result[name] = value
            return result


def dbrow_factory(cur, row):
    '''Helper to turn rows from a cursor into DbRow objects'''
    result = DbRow()
    for idx, col in enumerate(cur.description):
        result[col[0]] = row[idx]
    return result


class DbTable:
    ''' A class that wraps your database table '''

    def __init__(self, connection, dialect, table_name=None, pk_field="id",
                 table_schema=None, is_pk_autonumber=True):
        # wrap connection
        if not isinstance(connection, DbConnection):
            connection = DbConnection(connection)
        self.connection = connection
        self.table_schema = table_schema
        self.table_name = table_name
        self.pk_field = pk_field
        self.is_pk_autonumber = is_pk_autonumber

    @property
    def table():
        if self.table_schema:
            return "{}.{}".format(self.table_schema, self.table_name)
        else:
            return self.table_name

    def find_by_id(self, id):
        '''
        Finds a record matching the primary key id provided
        '''
        where = "{} = {}".format(self.pk_field, self.sqlparam(self.pk_field))
        return self.connection.exactly_one(where=where, params=(id,))

    def has_pk(self, obj):
        '''
        Determines if the object provided has a property with a name matching
        whatever self.pk_field is set to.
        '''
        return self.pk_field in DbRow.from_object(obj)

    def get_pk(self, obj):
        '''
        If the object provided has a property with a name matching
        whatever self.pk_field is set to, that value is returned.
        '''
        return DbRow.from_object(obj).get(self.pk_field, None)

    def delete_by_id(self, id=None):
        '''
        Deletes a record matching the id provided
        '''
        where = "{} = {}".format(self.pk_field, self.sqlparam(self.pk_field))
        self.delete(where=where, params=(id,))

    def delete(self, where=None, params=()):
        '''
        Executes a SQL delete statement to remove records from the DB
        according to provided WHERE condition.
        '''
        sql = self.dialect.delete_sql(self.table, where=where)
        self.connection.execute(sql, params)

    def insert(self, obj):
        '''
        Executes a SQL insert statement to add a record to the DB.
        '''
        include_pk = not self.is_pk_autonumber
        cols, paramvars, paramvalues = __parse_obj(obj, include_pk)

        # determine how we'll do the insert to get the ID back
        ins_style = self.dialect.supported_insert_style()
        fetch = False
        insert_query = self.dialect.insert_sql(
            self.table, self.pk_field, cols, paramvars)
        second_query = None
        if ins_style == "insert_sql_return_newid":
            insert_query = self.dialect.insert_sql_return_newid(
                self.table, self.pk_field, cols, paramvars)
            fetch = True
        elif ins_style == "get_last_inserted_id":
            second_query = self.dialect.get_last_inserted_id()
            fetch = True

        # run the insert
        newid = None
        cur = self.connection.cursor()
        cur.execute(insert_query, params=tuple(paramvalues))
        if second_query:
            cur.execute(second_query)
        if fetch:
            result = cur.fetchone()
            newid = result[0] if result else None
        self.connection.commit()
        return newid

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
        sql = sqlfmt.format(table=self.table, set_columns=setclause)
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
            table=self.table, where=where, groupby=groupby,
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
            table=self.table, where=where, groupby=groupby,
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
        self.connection
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

    def __parse_obj(obj, include_pk):
        row = DbRow.from_object(obj)
        cols = []
        paramvars = []
        paramvalues = []
        idx = 0
        for col, val in obj.items():
            if k.lower() != self.pk_field.lower() or include_pk:
                cols.append(col)
                paramvars.append(self.dialect.sqlparam(name=col, index=idx))
                paramvalues.append(val)
                idx += 1
        return cols, paramvars, paramvalues


class DbEngine():
    def __init__(self, dialect, dbapi, *args, **kwargs):
        self.dialect = dialect
        self.dbapi = dbapi
        self.args = args
        self.kwargs = kwargs

    def openconnection():
        with DbConnection(self.dbapi.connect(self.args, self.kwargs)) as conn:
            yield conn


class SqlDialect():
    def __init__(self, lookup):
        self.lookup = lookup

    def column_escape(self, column_name):
        return self.lookup["column_escape"](column_name)

    def sqlparam(self, name=None, index=0):
        ''' Returns a sql parameter in the paramstyle for this dialect '''
        # we need to be extra careful with params to prevent injection.
        if not isinstance(index, int):
            raise ValueError("non-integer index value: {}".format(index))

        # https://www.python.org/dev/peps/pep-0249/#paramstyle
        if paramstyle == 'qmark':
            return '?'
        elif paramstyle == 'numeric':
            return ':{}'.format(index)
        elif paramstyle == 'named':
            if name is None:
                raise ValueError("name is None for paramstyle == 'named'")
            if any(c in name for c in [';', "'"]):
                raise ValueError("Unsupported name value: {}".format(name))
            return ':{}'.format(name)
        elif paramstyle in ['format', 'pyformat']:
            # pyformat could also be '%({name})s', but that's creates a lot
            # of problems as shown in the named block above. Let's take a
            # shortcut.
            return '%s'
        else:
            raise ValueError("Unsupported paramstyle: {}".format(paramstyle))

    def delete_sql(self, table, where=None):
        '''
        Makes a SQL delete statement to remove records from the table according
        to the provided WHERE.
        '''
        sqlfmt = self.lookup["delete_sql"]
        sql = sqlfmt.format(table=table)
        if where:
            sql += " {} {}".format(self.lookup.keywords.where, where)
        return sql

    def select_sql(self, columns="*", distinct=False, table=None, where=None,
                   groupby=None, having=None, orderby=None, limit=None):
        return self.__select_sql(
            self.lookup["select_sql"], columns=columns, distinct=distinct,
            table=self.table, where=where, groupby=groupby,
            having=having, orderby=orderby, limit=limit)

    def __select_sql(self, select_sqlfmt, columns="*", distinct=False,
                     table=None, where=None, groupby=None, having=None,
                     orderby=None, limit=None, **kwargs):
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

        keywords = self.lookup["keywords"]
        distinct = " " + keywords["distinct"] if distinct else ""
        where = sqlpart(keywords["where"], where)
        groupby = sqlpart(keywords["groupby"], groupby)
        having = sqlpart(keywords["having"], having)
        orderby = sqlpart(keywords["orderby"], orderby)
        limit = sqlpart(keywords["limit"], limit)

        sql = safeformat(select_sqlfmt, distinct=distinct, columns=columns,
                         table=table, where=where, groupby=groupby,
                         having=having, orderby=orderby, limit=limit,
                         **kwargs)
        return sql

    def supported_insert_style():
        if self.lookup["insert_sql_return_newid"]:
            return "insert_sql_return_newid"
        elif self.lookup["get_last_inserted_id"]:
            return "get_last_inserted_id"
        else:
            return "insert_sql"

    def insert_sql(self, table, columns, values, escape=True):
        '''
        Makes INSERT sql
        '''
        sqlfmt = self.lookup["insert_sql"]
        return self.__insert_sql_impl(sqlfmt, table, None, columns, values,
                                      escape=escape)

    def insert_sql_return_newid(self, table, pk_field, columns, values,
                                escape=True):
        '''
        Makes INSERT sql that returns the new record ID if possible.
        '''
        sqlfmt = self.lookup["insert_sql_return_newid"]
        if sqlfmt is None:
            msg = "This method not available for this dialect"
            raise NotImplementedError(msg)
        else:
            return self.__insert_sql_impl(sqlfmt, table, pk_field, columns,
                                          values, escape=escape)

    def get_last_inserted_id(self):
        sqlfmt = self.lookup["get_last_inserted_id"]
        if sqlfmt is None:
            msg = "This method not available for this dialect"
            raise NotImplementedError(msg)
        else:
            return sqlfmt

    def __insert_sql_impl(self, sqlfmt, table, pk_field, columns, values,
                          escape=True):
        if len(columns) == 0:
            raise ValueError("No columns supplied")
        if len(columns) != len(values):
            raise ValueError("The number of values supplied does not "
                             "match the number of columns")
        if escape:
            col_sql = ", ".join([self.column_escape(c) for c in columns])
        else:
            col_sql = ", ".join(columns)
        values_sql = ", ".join(values)
        return safeformat(sqlfmt, table=table, pk_field=pk_field,
                          columns=col_sql, values=values_sql)

    @staticmethod
    def __escape_column_name(column_name, begin_esc='"', end_esc='"',
                             replacement='""'):
        if column_name == "*":
            return column_name
        return '{}{}{}'.format(begin_esc,
                               column_name.repace(end_esc, replacement),
                               end_esc)

    @staticmethod
    def __unshoutcase(lookup):
        def lower(s):
            if isinstance(s, str):
                return s.lower()
            else:
                return s
        return map_items(lookup, lower)

    @staticmethod
    def sql92(shoutcase=True, odbc=False, **kwargs):
        def column_escape(column_name):
            return SqlDialect.__escape_column_name(column_name)

        if odbc:
            paramstyle = "qmark"
        else:
            paramstyle = "pyformat"

        lookup = {
            "dialect": "sql",
            "paramstyle": paramstyle,
            "column_escape": column_escape,
            "insert_sql": "INSERT INTO {table} ({columns}) VALUES ({values})",
            "insert_sql_return_newid": None,
            "get_last_inserted_id": None,
            "select_sql": ("SELECT{distinct} {columns} FROM {table}"
                           "{where}{groupby}{having}{orderby}{limit}"),
            "update_sql": "UPDATE {table} SET {set_columns}",
            "delete_sql": "DELETE FROM {table}",
            "info_schema_columns": {
                "query": ("SELECT * "
                          "FROM INFORMATION_SCHEMA.COLUMNS "
                          "WHERE TABLE_NAME = {table}"),
                "query_with_table_schema":
                    ("SELECT * "
                     "FROM INFORMATION_SCHEMA.COLUMNS "
                     "WHERE TABLE_NAME = {table} AND TABLE_SCHEMA = {schema}"),
                "result_mapper": None,
            },
            "paging":
                ("SELECT COUNT(*) FROM ({subquery}) x",
                 ("SELECT {columns} FROM {table}{where}"
                  "{groupby}{having}{orderby} "
                  "LIMIT {page_size} OFFSET {page_start}")),
            "keywords": {
                "select": "SELECT",
                "distinct": "DISTINCT",
                "from": "FROM",
                "where": "WHERE",
                "groupby": "GROUP BY",
                "having": "HAVING",
                "orderby": "ORDER BY",
                "limit": "LIMIT",
                "count": "COUNT",
                "min": "MIN",
                "max": "MAX",
                "avg": "AVG",
            },
        }
        if not shoutcase:
            lookup = SqlDialect.__unshoutcase(lookup)
        return SqlDialect(lookup)

    @staticmethod
    def sqlserver(shoutcase=True, odbc=False, **kwargs):
        def column_escape(column_name):
            return SqlDialect.__escape_column_name(column_name, '[', ']', ']]')

        lookup = deepcopy(SqlDialect.sql92(odbc=odbc))
        lookup.update({
            "dialect": "sqlserver",
            "column_escape": column_escape,
            "select_sql":
                ("SELECT{distinct}{limit} {columns} FROM {table}"
                    "{where}{groupby}{having}{orderby}"),
            "insert_sql_return_newid":
                ("INSERT INTO {table} ({columns}) "
                 "OUTPUT INSERTED.[{pk_field}] VALUES ({values})"),
            "paging":
                ("SELECT COUNT(*) c FROM ({subquery}) x",
                    ("SELECT {columns} FROM {table}{where}"
                     "{groupby}{having}{orderby} "
                     "OFFSET {page_start} ROWS "
                     "FETCH NEXT {page_size} ROWS ONLY")),
        })
        lookup["keywords"]["limit"] = "TOP"
        lookup.update(kwargs)
        if not shoutcase:
            lookup = SqlDialect.__unshoutcase(lookup)
        return SqlDialect(lookup)

    @staticmethod
    def mariadb(shoutcase=True, odbc=False, **kwargs):
        lookup = deepcopy(SqlDialect.sql92(odbc=odbc))
        lookup.update({
            "dialect": "mariadb",
            "paging":
                ("SELECT COUNT(*) FROM ({subquery}) x",
                 ("SELECT {columns} FROM {table}{where}"
                  "{groupby}{having}{orderby} "
                  "LIMIT {page_start}, {page_size}")),
            "get_last_inserted_id": "SELECT LAST_INSERT_ID()",
        })
        lookup.update(kwargs)
        if not shoutcase:
            lookup = SqlDialect.__unshoutcase(lookup)
        return SqlDialect(lookup)

    @staticmethod
    def mysql(shoutcase=True, odbc=False, **kwargs):
        lookup = SqlDialect.mariadb(shoutcase=shoutcase, odbc=odbc)
        lookup["dialect"] = "mysql"
        return SqlDialect(lookup)

    @staticmethod
    def postgres(shoutcase=True, odbc=False, **kwargs):
        lookup = deepcopy(SqlDialect.sql92(odbc=odbc))
        lookup.update({
            "dialect": "postgres",
            "insert_sql_return_newid":
                ("INSERT INTO {table} ({columns}) VALUES ({values}) "
                 "RETURNING {pk_field} AS newid"),
        })
        lookup.update(kwargs)
        if not shoutcase:
            lookup = SqlDialect.__unshoutcase(lookup)
        return SqlDialect(lookup)

    @staticmethod
    def sqlite3(shoutcase=True, odbc=False, **kwargs):
        def map_sqlite_infoschema_column_row(row):
            row.COLUMN_NAME = row.name
            row.DATA_TYPE = row.data_type
            row.IS_NULLABLE = "NO" if str(row.notnull) == "0" else "YES"
            row.COLUMN_DEFAULT = row.dflt_value
            return row

        lookup = deepcopy(SqlDialect.sql92(odbc=odbc))
        lookup.update({
            "dialect": "sqlite3",
            "info_schema_columns": {
                "query": "PRAGMA table_info({table})",
                "query_with_table_schema": "PRAGMA table_info({table})",
                "result_mapper": map_sqlite_infoschema_column_row
            },
            "paging":
                ("SELECT COUNT(*) FROM ({subquery}) x",
                 ("SELECT {columns} FROM {table}{where}"
                  "{groupby}{having}{orderby} "
                  "LIMIT {page_start}, {page_size}")),
            "get_last_inserted_id": "SELECT last_insert_rowid()",
        })
        lookup.update(kwargs)
        if not shoutcase:
            lookup = SqlDialect.__unshoutcase(lookup)
        return SqlDialect(lookup)


def map_items(self, o, fn_map):
    if isinstance(o, dict):
        return {k: recursive_map(v, fn_map) for k, v in o.items()}
    elif isinstance(o, list):
        return [recursive_map(v, fn_map) for v in o]
    elif isinstance(o, tuple):
        return tuple(recursive_map(v, fn_map) for v in o)
    else:
        return fn_map(o)


def safeformat(str, **kwargs):
    ''' string format with safe replacements '''
    class SafeDict(dict):
        def __missing__(self, key):
            return '{' + key + '}'
    replacements = SafeDict(**kwargs)
    return str.format_map(replacements)
