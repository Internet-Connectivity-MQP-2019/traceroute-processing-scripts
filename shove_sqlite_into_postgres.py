#!/usr/bin/python3
import argparse
import json
import pyodbc
import socket
import struct
import sys
import sqlite3


def process_results(resultset):
	"""Process results into appropriate format. This really just means converting ints back into IPs."""
	new_results = []
	for row in resultset:
		new_row = []
		for item in row:
			if type(item) is int:
				new_row.append(socket.inet_ntoa(struct.pack("!I", item)))
			else:
				new_row.append(item)
		new_results.append(tuple(new_row))
	return new_results


def insert_db(cursor, table, vals_str, vals):
	"""Insert a mass of values into the db"""
	args_str = ",".join((vals_str % row) for row in vals)
	cursor.execute("INSERT INTO {} VALUES {};".format(table, args_str))
	cursor.commit()


def get_from_sqlite(dbname, table):
	select_query = "SELECT * FROM {};".format(table)
	sqlite_connection = sqlite3.connect(dbname)
	sqlite_cursor = sqlite_connection.cursor()
	sqlite_cursor.execute(select_query)
	results = sqlite_cursor.fetchall()
	sqlite_connection.close()
	return results


parser = argparse.ArgumentParser(description="Run queries on groups of SQLite databases")
parser.add_argument("config", type=str, help="DB config file")
parser.add_argument("src_table", type=str, help="Table to select data from")
parser.add_argument("dst_table", type=str, help="Table to insert data into")
parser.add_argument("databases", type=str, help="SQLite databases to shove into db", nargs="+")
args = parser.parse_args()

with open(args.config, "r") as db_config:
	connection_str = "DRIVER={{PostgreSQL Unicode}};UID={user};Host={host};Database={database};Pooling=True;Min Pool Size=0;Max Pool Size=100;".format(
		**json.load(db_config))
	connection = pyodbc.connect(connection_str)
connection.setdecoding(pyodbc.SQL_WCHAR, encoding="utf-8")
connection.setencoding(encoding="utf-8")
connection.maxwrite = 2 << 32
cursor = connection.cursor()
print("Connected to database, fetching data from first sqlite db")

results = get_from_sqlite(args.databases[0], args.src_table)
if len(results) == 0:
	print("Failed to extract rows from database {}, aborting!".format(args.databases[0]), file=sys.stderr)
	exit(1)
print("Retrieved {} results; beginning insert".format(len(results)))

# Assemble a values string that fits the format of results returned by the query, then do a one-off shove into the db
processed_results = process_results(results)
vals_str = "(" + ",".join("'%s'" if type(val) is str else "%d" for val in processed_results[0]) + ")"
del results
insert_db(cursor, args.dst_table, vals_str, processed_results)
del args.databases[0]

# Process remaining dbs
count = len(processed_results)
for db in args.databases:
	print("Retrieving from sqlite db " + db)
	results = get_from_sqlite(db, args.src_table)
	if len(results) == 0:
		print("Failed to extract rows from database; skipping...")
		continue
	count = count + len(results)
	print("Retrieved {} results; beginning preprocessing...".format(len(results)))
	processed_results = process_results(results)
	print("Processed results, inserting into database...")
	del results  # Free up memory before next step -- deleting results should make python garbage collect it
	insert_db(cursor, args.dst_table, vals_str, processed_results)
	print("Inserted {} rows from {} into database!".format(len(processed_results), db))

print("Complete! Transferred {} total rows from {} databases".format(count, len(args.databases) + 1))
