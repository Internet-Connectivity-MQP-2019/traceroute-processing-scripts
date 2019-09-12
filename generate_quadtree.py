import argparse
import sqlite3

import pandas as pd

from SpatialQuadtree import SpatialQuadtree
from postgresql import get_postgres_connection

parser = argparse.ArgumentParser(description="Generate a quadtree of latencies for a source")
parser.add_argument("source", type=str, help="Source IP to track ping times from")
parser.add_argument("dbconfig", type=str, help="Database config")
parser.add_argument("output", type=str, help="SQLite database to output to")
args = parser.parse_args()

# Fetch data from DB into a Pandas dataframe
print("Loading data from database...")
with open(args.dbconfig, "r") as dbconfig:
	connection = get_postgres_connection(dbconfig)
cursor = connection.cursor()
cursor.execute("SELECT dst_lat, dst_lng, rtt_avg FROM hops_aggregate WHERE src=?", args.source)
results = cursor.fetchall()
connection.close()
print("Loaded {} data points; beginning quadtree generation...".format(len(results)))

# Process data from into a quadtree for graphing
quadtree = SpatialQuadtree(bbox=(-180, -180, 180, 180), max_depth=10, max_items=100, auto_subdivide=False)
for coord in results:
	quadtree.insert(coord[2], (coord[0], coord[1]))
del results
quadtree.force_subdivide()
print("(Mostly) balanced quadtree assembled, now collecting bottom level boxes")
bboxes = pd.DataFrame(quadtree.get_bboxes(), columns=["min_lat", "min_lng", "max_lat", "max_lng", "avg", "stdev", "med", "cnt"])
print("Got {} boxes, dumping to {}".format(len(bboxes), args.output))
connection = sqlite3.connect(args.output)
bboxes.to_sql("boxes", connection, index=False)
connection.close()
