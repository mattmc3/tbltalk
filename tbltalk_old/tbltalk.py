# #!/usr/bin/env python
# '''
# A simple, python micro-ORM that embraces DB-API, the power of SQL, the
# simplicity of one-object-per-table, and the Zen of Python.
# '''
# import re
# import inspect
# from collections import OrderedDict, namedtuple, Mapping
# from .dbconnection import DbConnection
# from .dbrow import DbRow
# SqlStatement = namedtuple('SqlStatement', 'sql params')
# PagedResult = namedtuple('PagedResult',
#                          ['total_records', 'total_pages', 'page_size',
#                           'current_page', 'records'])


# def sqlparam(paramstyle, name=None, index=0):
#     ''' Returns a sql parameter for the specified paramstyle '''
#     # we need to be extra careful with params to prevent injection.
#     if not isinstance(index, int):
#         raise ValueError(f"non-integer index value: {index}")
#     if name and (';' in name or "'" in name):
#         raise ValueError("Unsupported name value: {name}")

#     # https://www.python.org/dev/peps/pep-0249/#paramstyle
#     if paramstyle == 'qmark':
#         return '?'
#     elif paramstyle == 'numeric':
#         return f':{index}'
#     elif paramstyle == 'named':
#         if name is None:
#             raise ValueError("name is None for paramstyle == 'named'")
#         return f':{name}'
#     elif paramstyle == 'format':
#         return f'%s'
#     elif paramstyle == 'pyformat':
#         # if name is None:
#         #    return f'%s'
#         # else:
#         #    return f'%({name})s'
#         return f'%s'
#     else:
#         raise ValueError(f"Unsupported paramstyle: {paramstyle}")


# def safeformat(str, **kwargs):
#     ''' string format with safe replacements '''
#     class SafeDict(dict):
#         def __missing__(self, key):
#             return '{' + key + '}'
#     replacements = SafeDict(**kwargs)
#     return str.format_map(replacements)


# class DbTable:
#     ''' A class that wraps your database table '''

#     def __init__(self, connection, table_name=None, pk_field="id",
#                  table_schema=None, is_pk_autonumber=True):
#         if not isinstance(connection, DbConnection):
#             connection = DbConnection(connection)
#         self.connection = connection
#         self.table_schema = table_schema
#         self.table_name = table_name
#         self.pk_field = pk_field
#         self.is_pk_autonumber = is_pk_autonumber

#     @property
#     def table():
#         if self.table_schema:
#             return f"{self.table_schema}.{self.table_name}"
#         else:
#             return self.table_name

#     def get_by_id(self, id):
#         '''
#         Gets a record matching the id provided
#         '''
#         where = f"{self.pk_field} = {self.sqlparam(self.pk_field)}"
#         return self.connection.exactly_one(where=where, params=(id,))

#     def has_pk(self, obj):
#         '''
#         Determines if the object provided has a property with a name matching
#         whatever self.pk_field is set to.
#         '''
#         return self.pk_field in DbRow.from_object(obj)

#     def get_pk(self, obj):
#         '''
#         If the object provided has a property with a name matching
#         whatever self.pk_field is set to, that value is returned.
#         '''
#         return DbRow.from_object(obj).get(self.pk_field, None)

#     def create_delete_sql(self, where=None):
#         '''
#         Makes a SQL delete statement to remove records from the DB according to
#         the provided WHERE.
#         '''
#         sqlfmt = self.dialect.delete_sql
#         sql = sqlfmt.format(table=self.table)
#         if where:
#             sql += f" {self.dialect.keywords.where} {where}"
#         return sql

#     def delete_by_id(self, id=None):
#         '''
#         Deletes a record matching the id provided
#         '''
#         where = f"{self.pk_field} = {self.sqlparam(self.pk_field)}"
#         self.delete(where=where, params=(id,))

#     def delete(self, where=None, params=()):
#         '''
#         Executes a SQL delete statement to remove records from the DB
#         according to provided WHERE condition.
#         '''
#         sql = self.create_delete_sql(where=where)
#         self.execute(sql, params)

#     def create_insert_statement(self, obj):
#         '''
#         Makes an INSERT SqlStatement to add a record to the DB.
#         '''
#         obj = to_dotdict(obj)
#         filtered_cols = [(k, v) for k, v in obj.items()
#                          if k.lower() != self.pk_field.lower() or
#                          not self.is_pk_autonumber]
#         cols, params = zip(*filtered_cols)
#         if len(cols) == 0:
#             raise ValueError("Cannot use an object with no properties to "
#                              "create an INSERT statement")
#         columns = ", ".join(cols)
#         paramvars = [self.sqlparam(c, i) for i, c in enumerate(cols)]
#         values = ", ".join(paramvars)
#         sqlfmt = self.dialect.insert_sql
#         sql = sqlfmt.format(table=self.table, columns=columns,
#                             values=values, pk_field=self.pk_field)
#         return SqlStatement(sql, params)

#     def insert(self, obj):
#         '''
#         Executes a SQL insert statement to add a record to the DB.
#         '''
#         (sql, params) = self.create_insert_statement(obj)
#         with self.cursor() as cur:
#             cur.execute(sql, params)

#             result = None
#             if self.dialect.get_last_inserted_id is None:
#                 try:
#                     result = cur.fetchone()
#                 except Exception:
#                     pass
#             else:
#                 cur.execute(self.dialect.get_last_inserted_id)
#                 result = cur.fetchone()

#             id = result[0] if result else 0
#         return id

#     def create_update_statement(self, obj, id=None):
#         '''
#         Makes a SQL update statement to modify a record in the DB.
#         '''
#         obj = to_dotdict(obj)
#         if id is None:
#             id = obj[self.pk_field]
#         filtered_cols = [(k, v) for k, v in obj.items()
#                          if k.lower() != self.pk_field.lower()]
#         cols, param_vals = zip(*filtered_cols)
#         if len(cols) == 0:
#             raise ValueError("Cannot use an object with no properties to "
#                              "create an UPDATE statement")

#         # make SET clause
#         setitems = [(c, self.sqlparam(c, i))
#                     for i, c in enumerate(cols)]
#         setclause = ", ".join([f"{col} = {p}" for col, p in setitems])

#         # make where
#         param_vals = list(param_vals)
#         param_vals.append(id)
#         pk_param = self.sqlparam(self.pk_field, len(cols))
#         where = f" WHERE {self.pk_field} = {pk_param}"

#         sqlfmt = self.dialect.update_sql
#         sql = sqlfmt.format(table=self.table, set_columns=setclause)
#         sql += where
#         return SqlStatement(sql, tuple(param_vals))

#     def update(self, obj, id=None):
#         '''
#         Executes a SQL update statement to modify a record in the DB.
#         '''
#         stmt = self.create_update_statement(obj, id)
#         self.execute(stmt.sql, stmt.params)

#     def create_upsert_statement(self, obj):
#         if self.has_pk(obj):
#             return self.create_update_statement(obj)
#         else:
#             return self.create_insert_statement(obj)

#     def create_select_sql(self, columns="*", distinct=False, where=None,
#                           groupby=None, having=None, orderby=None, limit=None):
#         '''
#         Returns all records matching the provided WHERE clause and arguments,
#         ordered as specified, and limited if specified.
#         '''
#         return self._create_select_sql_impl(
#             self.dialect.select_sql, columns=columns, distinct=distinct,
#             table=self.table, where=where, groupby=groupby,
#             having=having, orderby=orderby, limit=limit)

#     def _create_select_sql_impl(self, select_sqlfmt, columns="*",
#                                 distinct=False, table=None, where=None,
#                                 groupby=None, having=None, orderby=None,
#                                 limit=None, **kwargs):
#         def check_cols(cols):
#             result = cols or ""
#             if (not isinstance(cols, str) and hasattr(cols, '__iter__')):
#                 result = ", ".join(cols)
#             if ';' in result or "'" in result:
#                 raise ValueError(f"Possible SQL Injection detected in "
#                                  f"SQL input: {cols}")
#             return result

#         def sqlpart(keyword, clause):
#             if clause is None or clause == "":
#                 return ""
#             else:
#                 return " {} {}".format(keyword, clause)

#         columns = check_cols(columns)
#         orderby = check_cols(orderby)
#         groupby = check_cols(groupby)

#         distinct = " " + self.dialect.keywords.distinct if distinct else ""
#         where = sqlpart(self.dialect.keywords.where, where)
#         groupby = sqlpart(self.dialect.keywords.groupby, groupby)
#         having = sqlpart(self.dialect.keywords.having, having)
#         orderby = sqlpart(self.dialect.keywords.orderby, orderby)
#         limit = sqlpart(self.dialect.keywords.limit, limit)

#         sql = safeformat(select_sqlfmt, distinct=distinct, columns=columns,
#                          table=table, where=where, groupby=groupby,
#                          having=having, orderby=orderby, limit=limit)
#         return sql

#     def all(self, columns="*", distinct=False, where=None, groupby=None,
#             having=None, orderby=None, limit=None, params=()):
#         '''
#         Returns all records matching the provided WHERE clause and arguments,
#         ordered as specified, and limited if specified.
#         '''
#         sql = self.create_select_sql(columns=columns, distinct=distinct,
#                                      where=where, groupby=groupby,
#                                      having=having, orderby=orderby,
#                                      limit=limit)
#         return list(self.query(sql, params))

#     def one(self, columns="*", distinct=False, where=None, groupby=None,
#             having=None, orderby=None, limit=None, params=()):
#         '''
#         Returns a single record.
#         '''
#         sql = self.create_select_sql(columns=columns, distinct=distinct,
#                                      where=where, groupby=groupby,
#                                      having=having, orderby=orderby,
#                                      limit=limit)
#         return first(self.query(sql, params))

#     def count(self, column="*", distinct_count=False, where=None, params=()):
#         ''' Returns the count of records matching the where condition. '''
#         if distinct_count:
#             column = "{} {}".format(self.dialect.keywords.distinct, column)
#         return self._agg(self.dialect.keywords.count, column, where, params)

#     def avg(self, column, where=None, params=()):
#         '''
#         Returns the average column value for records matching the where
#         condition.
#         '''
#         return self._agg(self.dialect.keywords.avg, column, where, params)

#     def min(self, column, where=None, params=()):
#         '''
#         Returns the minimum column value for records matching the where
#         condition.
#         '''
#         return self._agg(self.dialect.keywords.min, column, where, params)

#     def max(self, column, where=None, params=()):
#         '''
#         Returns the maximum column value for records matching the where
#         condition.
#         '''
#         return self._agg(self.dialect.keywords.max, column, where, params)

#     def _agg(self, agg_fn, column, where, params):
#         columns = "{}({}) aggfield1".format(agg_fn, column)
#         sql = self.create_select_sql(columns=columns, where=where)
#         return self.scalar(sql, params)

#     def paged(self, columns="*", distinct=False, where=None, groupby=None,
#               having=None, orderby=None, page_size=20, current_page=1,
#               params=()):

#         # get the total count
#         countsqlfmt = self.dialect.paging[0]
#         subquery = self.create_select_sql(columns="1 one", distinct=distinct,
#                                           where=where, groupby=groupby,
#                                           having=having)
#         countsql = countsqlfmt.format(subquery=subquery)
#         total_records = self.scalar(countsql, params)

#         # get the paged result
#         page_start = (current_page - 1) * page_size
#         pagingsqlfmt = self.dialect.paging[1]
#         pagingsql = self._create_select_sql_impl(
#             pagingsqlfmt, columns=columns, distinct=distinct,
#             table=self.table, where=where, groupby=groupby,
#             having=having, orderby=orderby)
#         pagingsql = pagingsql.format(page_size=page_size,
#                                      page_start=page_start,
#                                      current_page=current_page)

#         result = DotDict()
#         result.total_records = total_records
#         result.page_size = page_size
#         result.current_page = current_page
#         result.records = list(self.query(pagingsql, params=params))
#         return result

#     def save(self, *args):
#         statements = []
#         for obj in args:
#             statements.append(self.create_upsert_statement(obj))
#         self.connection
#         for stmt in statements:
#         with self.cursor() as cur:
#             cur = con.cursor()
#             for stmt in statements:
#                 cur.execute(stmt.sql, stmt.params)

#     def __getattr__(self, name):
#         '''
#         If an unrecognized method is called, assume we wanted dynamicquery
#         '''
#         if not self._re_dynamic_methods.match(name):
#             raise AttributeError(f"object has no attribute '{name}'")

#         def runit(*args, **kwargs):
#             try:
#                 return self.dynamicquery(name, *args, **kwargs)
#             except Exception as err:
#                 raise RuntimeError(f"Method not found: {name}. dynamicquery "
#                                    f"called instead and it errored") from err
#         return runit

#     def dynamicquery(self, method_name, *args, **kwargs):
#         if len(args) > 0:
#             raise ValueError(f"Error in dynamicquery call to {method_name}: "
#                              f"Named arguments are required for this type of "
#                              f"query - the column name, orderby, columns, etc")
#         method_name = method_name.lower()
#         select_parts = {
#             'columns': "*",
#             'distinct': False,
#             'where': None,
#             'groupby': None,
#             'having': None,
#             'orderby': None,
#             'limit': None
#         }

#         # call it what you like, the DB cares but you don't have to.
#         # note: don't be fooled - keyword smoothing happens in
#         #       create_select_sql, not here.
#         aliases = {"column": "columns", "select": "columns", "top": "limit"}

#         # key := prefix, val := orderby direction
#         one_result_methods = {
#             'single': '',
#             'one': '',
#             'fetchone': '',
#             'first': '',
#             'last': ' DESC'
#         }

#         counter = 0
#         constraints = []
#         params = []
#         lc_kwargs = {k.lower(): v for k, v in kwargs.items()}

#         # arguments are considered to be either known select clause parts,
#         # or else where filters
#         for key, value in lc_kwargs.items():
#             if key == "params":
#                 params += value
#             elif key in select_parts:
#                 select_parts[key] = value
#             elif key in aliases:
#                 select_parts[aliases[key]] = value
#             else:
#                 # other keywords are considered WHERE equality conditions
#                 p = self.sqlparam(key, counter)
#                 constraints.append(f"{key} = {p}")
#                 params.append(value)
#                 counter += 1

#         if select_parts['where'] and len(constraints) > 0:
#             raise ValueError("Cannot mix passing in a where clause and "
#                              "constraint parameters. Unsure how to make a "
#                              "WHERE from that.")

#         if len(constraints) > 0:
#             select_parts['where'] = " AND ".join(constraints)

#         if not select_parts['orderby']:
#             select_parts['orderby'] = [self.pk_field]

#         found = [(k, v) for k, v in one_result_methods.items()
#                  if method_name.startswith(k)]
#         is_one = len(found) > 0
#         if is_one:
#             select_parts['orderby'] += found[0][1]
#             select_parts['limit'] = 1
#             sqlselect = self.create_select_sql(**select_parts)
#             return first(self.query(sqlselect, tuple(params)))
#         else:
#             sqlselect = self.create_select_sql(**select_parts)
#             return list(self.query(sqlselect, tuple(params)))

#     def sqlparam(self, name, index=0):
#         '''
#         Returns a sql parameter specific to the needs of the current dbengine
#         '''
#         return sqlparam(self._dbengine.dbapi.paramstyle, name, index)
