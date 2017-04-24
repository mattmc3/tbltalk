sql92_dialect = {
    "dialect": "sql",
    "insert_sql": "INSERT INTO {table} ({columns}) VALUES ({values})",
    "select_sql": ("SELECT{distinct} {columns} FROM {table}"
                   "{where}{groupby}{having}{orderby}{limit}"),
    "update_sql": "UPDATE {table} SET {set_columns}",
    "delete_sql": "DELETE FROM {table}",
    "column_schema":
        ("SELECT * "
         "FROM INFORMATION_SCHEMA.COLUMNS "
         "WHERE TABLE_NAME = {table}"),
    "column_schema_2part":
        ("SELECT * "
         "FROM INFORMATION_SCHEMA.COLUMNS "
         "WHERE TABLE_NAME = {table} AND TABLE_SCHEMA = {schema}"),
    "get_last_inserted_id": None,
    "paging":
        ("SELECT COUNT(*) FROM ({subquery}) x",
         ("SELECT {columns} FROM {table}{where}{groupby}{having}{orderby} "
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
    }
}

mssql_dialect = {**sql92_dialect, **{
    "dialect": "mssql",
    "select_sql": ("SELECT{distinct}{limit} {columns} FROM {table}"
                   "{where}{groupby}{having}{orderby}"),
    "insert_sql": ("INSERT INTO {table} ({columns}) "
                   "OUTPUT INSERTED.[{pk_field}] VALUES ({values})"),
    "paging":
        ("SELECT COUNT(*) c FROM ({subquery}) x",
         ("SELECT {columns} FROM {table}{where}{groupby}{having}{orderby} "
          "OFFSET {page_start} ROWS FETCH NEXT {page_size} ROWS ONLY")),
    "keywords": {**sql92_dialect["keywords"], **{
        "limit": "TOP",
    }},
}}

mariadb_dialect = {**sql92_dialect, **{
    "dialect": "mariadb",
    "paging":
        ("SELECT COUNT(*) FROM ({subquery}) x",
         ("SELECT {columns} FROM {table}{where}{groupby}{having}{orderby} "
          "LIMIT {page_start}, {page_size}")),
    "get_last_inserted_id": "SELECT LAST_INSERT_ID()",
}}

mysql_dialect = {**mariadb_dialect}

oracle_dialect = {**sql92_dialect, **{
    "dialect": "oracle",
    # TODO
}}

postgres_dialect = {**sql92_dialect, **{
    "dialect": "postgres",
    "insert_sql": ("INSERT INTO {table} ({columns}) VALUES ({values}) "
                   "RETURNING {pk_field} AS newid"),
}}

sqlite3_dialect = {**sql92_dialect, **{
    "dialect": "sqlite3",
    "column_schema": "PRAGMA table_info({table})",
    "column_schema_2part": "PRAGMA table_info({table})",
    "paging":
        ("SELECT COUNT(*) FROM ({subquery}) x",
         ("SELECT {columns} FROM {table}{where}{groupby}{having}{orderby} "
          "LIMIT {page_start}, {page_size}")),
    "get_last_inserted_id": "SELECT last_insert_rowid()",
}}

dialects = {
    'sqlite3.Connection': sqlite3_dialect,
    'psycopg2.extensions.connection': postgres_dialect,
    'pymysql.connections.Connection': mariadb_dialect,
}


def unshoutcase(dialect):
    '''
    Makes a dialect use lower-case SQL
    '''
    result = {}
    for key, value in dialect.items():
        if isinstance(value, str):
            result[key] = value.lower()
        elif isinstance(value, dict):
            result[key] = unshoutcase(value)
        else:
            result[key] = value
    return result
