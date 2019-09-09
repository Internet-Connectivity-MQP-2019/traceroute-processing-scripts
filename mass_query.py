#!/usr/bin/python3
import argparse
import csv
import sqlite3
from io import StringIO
from itertools import chain
from multiprocessing.pool import ThreadPool


def process_database(results_db_name, query):
	"""Returns a function to run a query across multiple SQLite databases"""
	def func(db_name):
		# Connect to database, execute query and load into memory directly rather than lock the results db for an INSERT
		# INTO table VALUES {query}; statement
		db = sqlite3.connect(db_name)
		db_cursor = db.cursor()
		db_cursor.execute(query)
		results = db_cursor.fetchall()
		db.close()
		if len(results) == 0:
			db.close()
			return
		if results_db_name == ":memory:":
			return results

		# Connect to re
		results_db_t = sqlite3.connect(results_db_name, 3600)
		results_cursor_t = results_db_t.cursor()
		dynamic_insert(results_cursor_t, results)
		del results
		results_db.close()
	return func


def dynamic_insert(cursor, values):
	values_str = ",".join("?" for arg in values[0])
	cursor.executemany("INSERT INTO mq_result VALUES ({});".format(values_str), values)


parser = argparse.ArgumentParser(description="Run queries on groups of SQLite databases")
parser.add_argument("-t", "--threads", type=int, default=4, help="Number of threads to run queries on")
parser.add_argument("query", type=str, help="SELECT query to execute")
parser.add_argument("-o", "--output", type=str, default=":memory:",
					help="Name of output to save results to. If none is provided, output will be printed as a CSV.")
parser.add_argument("databases", type=str, help="SQLite databases to run queries against", nargs="+")
args = parser.parse_args()

# Create in-memory working DB, populate with data from the first database in the list since we need to create the table
results_db = sqlite3.connect(args.output)
results_cursor = results_db.cursor()
results_cursor.execute("ATTACH DATABASE '{}' AS db;".format(args.databases[0]))
results_cursor.execute("CREATE TABLE mq_result AS {};".format(args.query))
results_cursor.execute("DETACH DATABASE 'db';")
del args.databases[0]

# Use thread pool to process remaining databases
pool = ThreadPool(args.threads)
results = pool.map(process_database(args.output, args.query), args.databases)
pool.close()

# No more processing to do since the data is already saved in the results database; go home.
if args.output != ":memory:":
	results_db.close()
	exit(0)

# Accumulate the results from the thread pool (which should be a list of lists) into one list alongside the data from
# the first database, then print it all out as a CSV.
results_cursor.execute("SELECT * FROM mq_result;")
results.insert(0, results_cursor.fetchall())
results_db.close()

results_flat = list(chain(*results))
tmp_csv_text = StringIO()
csv_writer = csv.writer(tmp_csv_text)
for result in results_flat:
	csv_writer.writerow(result)
print(tmp_csv_text.getvalue())
