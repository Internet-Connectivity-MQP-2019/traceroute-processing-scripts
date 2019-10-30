import argparse

from SpatialQuadtree import SpatialQuadtree
from postgresql import get_postgres_connection

parser = argparse.ArgumentParser(description="Generate a quadtree of connectivities")
parser.add_argument("dbconfig", type=str, help="Database config")
args = parser.parse_args()

# Fetch data from DB into a Pandas dataframe
print("Loading data from database...")
with open(args.dbconfig, "r") as dbconfig:
	connection = get_postgres_connection(dbconfig)
cursor = connection.cursor()

SCALE = 32
for min_lat_i in range(SCALE):
	for min_lng_i in range(SCALE):
		min_lat = (min_lat_i * 180 / SCALE) - 90
		min_lng = (min_lng_i * 360 / SCALE) - 180
		max_lat = min_lat + 180 / SCALE
		max_lng = min_lng + 360 / SCALE
		print("Processing box {},{} (from {},{} to {},{})".format(min_lat_i, min_lng_i, min_lat, min_lng, max_lat, max_lng))

		cursor.execute("SELECT src_loc[0], src_loc[1], rtt_avg / distance FROM hops_aggregate "
					   "WHERE distance != 0 AND (rtt_avg / distance) < 0.1 AND (rtt_avg / distance) > 0.01 AND indirect = FALSE "
		               " AND BOX(POINT(%s, %s), POINT(%s, %s)) @> src_loc"
					   " " % (min_lat, min_lng, max_lat, max_lng))
						# " AND lat < 71.35 AND lat > 25.28 AND lng > -126.66 AND lng < -66.27")
		print("\tQuery complete, fetching rows")
		results = cursor.fetchall()
		print("\tLoaded {} data points; beginning quadtree generation...".format(len(results)))

		# Process data from into a quadtree for graphing
		quadtree = SpatialQuadtree(bbox=(min_lat, min_lng, max_lat, max_lng), max_depth=24, max_items=2500, auto_subdivide=False)
		# quadtree = SpatialQuadtree(bbox=(25.28, -126.66, 71.35, -66.26), max_depth=10, max_items=100, auto_subdivide=False)
		for coord in results:
			quadtree.insert(coord[2], (coord[0], coord[1]))
		del results
		quadtree.force_subdivide()

		print("\t(Mostly) balanced quadtree assembled, now collecting bottom level boxes")
		boxes = quadtree.get_bboxes()
		print("\tGot {} boxes, dumping to Postgres".format(len(boxes)))
		args_str = ",".join(("(BOX(POINT(%f, %f), POINT(%f, %f)), %f, %f, %f, %d)" % tuple(bbox)) for bbox in boxes)
		args_str = args_str.replace("nan", "'NaN'::float")
		cursor.execute("INSERT INTO quads VALUES " + args_str)
		del quadtree
		del boxes
		del args_str
		cursor.commit()
connection.close()
