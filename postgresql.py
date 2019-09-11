import json

import pyodbc


def get_postgres_connection(conf):
	connection_str = "DRIVER={{PostgreSQL Unicode}};UID={user};Host={host};Database={database};Pooling=True;Min Pool Size=0;Max Pool Size=100;".format(
		**json.load(conf))
	connection = pyodbc.connect(connection_str)
	connection.setdecoding(pyodbc.SQL_WCHAR, encoding="utf-8")
	connection.setencoding(encoding="utf-8")
	connection.maxwrite = 2 << 32
	return connection
