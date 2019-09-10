#!/usr/bin/python3
import argparse
import csv
import os
import sqlite3
from io import StringIO
from itertools import chain
from multiprocessing.pool import ThreadPool


def process_database(query, verbose):
	"""Returns a function to run a query across multiple SQLite databases"""
	def func(db_name):
		# Connect to database, execute query and load into memory directly rather than lock the results db for an INSERT
		# INTO table VALUES {query}; statement
		if verbose:
			print("Executing query on {}".format(db_name))
		db = sqlite3.connect(db_name)
		db_cursor = db.cursor()
		try:
			db_cursor.execute(query)
			subquery_results = db_cursor.fetchall()
		except sqlite3.OperationalError:
			print("Failed to get data from {}!".format(db_name))
			return
		finally:
			db.close()

		if verbose:
			print("Got {} results from query".format(len(subquery_results)))
		if len(subquery_results) == 0:
			db.close()
			return
		return subquery_results
	return func


parser = argparse.ArgumentParser(description="Run queries on groups of SQLite databases")
parser.add_argument("-t", "--threads", type=int, default=4, help="Number of threads to run queries on")
parser.add_argument("query", type=str, help="SELECT query to execute")
parser.add_argument("-o", "--output", type=str, default=":memory:",
					help="Name of output to save results to. If none is provided, output will be printed as a CSV.")
parser.add_argument("databases", type=str, help="SQLite databases to run queries against", nargs="+")
parser.add_argument("-v", "--verbose", action="store_true", default=False, help="Verbose output")
args = parser.parse_args()

# Create in-memory working DB, populate with data from the first database in the list since we need to create the table
if args.verbose:
	print("Connecting to database, executing query on first input database")
results_db = sqlite3.connect(args.output)
results_cursor = results_db.cursor()
results_cursor.execute("ATTACH DATABASE '{}' AS db;".format(args.databases[0]))
results_cursor.execute("CREATE TABLE mq_result AS {};".format(args.query))
results_cursor.execute("DETACH DATABASE 'db';")
del args.databases[0]

# Use thread pool to process remaining databases
if args.verbose:
	print("Spinning up thread pool with {} threads".format(args.threads))
pool = ThreadPool(args.threads)
results = pool.map(process_database(args.query, args.verbose), args.databases)
# pool.join()
pool.close()
if args.verbose:
	print("Thread pool closed and joined; accumulating results")

# Save to database
results_flat = list(chain(*results))
if args.output != ":memory:" and len(args.databases) > 0:
	values_str = ",".join("?" for arg in results[0])
	results_cursor.executemany("INSERT INTO mq_result VALUES ({});".format(values_str), results)
	results_db.close()
	exit(0)

# Accumulate the results from the thread pool (which should be a list of lists) into one list alongside the data from
# the first database, then print it all out as a CSV.
results_cursor.execute("SELECT * FROM mq_result;")
results.insert(0, results_cursor.fetchall())
results_db.close()

tmp_csv_text = StringIO()
csv_writer = csv.writer(tmp_csv_text)
for result in results_flat:
	csv_writer.writerow(result)
print(tmp_csv_text.getvalue())
