import argparse

import cartopy.crs as ccrs
import matplotlib.cm
import matplotlib.pyplot as plt
import pandas as pd
from matplotlib.collections import PatchCollection
from matplotlib.patches import Rectangle

import postgresql
from SpatialQuadtree import SpatialQuadtree

parser = argparse.ArgumentParser(description="Plot quadtree rects on a map")
parser.add_argument("dbconfig", type=str, help="Postgres database configuration")
parser.add_argument("-v", "--verbose", action="store_true", help="Verbose output")
parser.add_argument("--max-lng", type=float, default=180, help="Maximum longitude")
parser.add_argument("--max-lat", type=float, default=90, help="Maximum latitude")
parser.add_argument("--min-lng", type=float, default=-180, help="Minimum longitude")
parser.add_argument("--min-lat", type=float, default=-90, help="Minimum latitude")
parser.add_argument("-d", "--quadtree-max_depth", type=float, default=10, help="Quadtree maximum depth")
parser.add_argument("-i", "--quadtree-max-items", type=float, default=100, help="Quadtree maximum items per box")
parser.add_argument("-p", "--dpi", type=int, default=300, help="Chart DPI")
args = parser.parse_args()
vprint = print if args.verbose else lambda *x, **y: None

with open(args.dbconfig, "r") as dbconfig:
	connection = postgresql.get_postgres_connection(dbconfig)
cursor = connection.cursor()

vprint("Grabbing connectivity data")
cursor.execute("SELECT lng, lat, connectivity FROM hops_ms_per_km WHERE connectivity < 0.03 AND connectivity > 0.003"
						 " AND lng < ? AND lng > ? AND lat < ? AND lat > ?",
						 (args.max_lng, args.min_lng, args.max_lat, args.min_lat))
results = cursor.fetchall()
connection.close()

vprint("Loaded {} data points; beginning quadtree generation".format(len(results)))
vprint("Quadtree will have at most {} levels and will split on more than {} items per box".format(
	args.quadtree_max_depth, args.quadtree_max_items))
quadtree = SpatialQuadtree(bbox=(args.min_lng, args.min_lat, args.max_lng, args.max_lat),
						   max_items=args.quadtree_max_items, max_depth=args.quadtree_max_depth, auto_subdivide=False)
for coord in results:
	quadtree.insert(coord[2], (coord[0], coord[1]))
quadtree.force_subdivide()
vprint("(Mostly) balanced quadtree assembled, now collecting leaf boxes")
quads = pd.DataFrame(quadtree.get_bboxes(), columns=["min_lng", "min_lat", "max_lng", "max_lat", "avg", "stdev", "med", "cnt"])
vprint("Got {} boxes ({:.1f}% reduction), calculating heights+widths and plotting".format(len(quads), 100 * (1 - len(quads) / len(results))))
del results, quadtree

quads["avg"] = pow(quads["avg"], 8)
quads["height"] = quads["max_lat"] - quads["min_lat"]
quads["width"] = quads["max_lng"] - quads["min_lng"]
patches = quads.apply(lambda row: Rectangle((row["min_lng"], row["min_lat"]), row["width"], row["height"]), axis=1)
patch_collection = PatchCollection(patches, cmap=matplotlib.cm.inferno, alpha=0.6)
patch_collection.set_array(quads["avg"])

matplotlib.rcParams["figure.dpi"] = args.dpi
ax = plt.axes(projection=ccrs.PlateCarree())
ax.set_extent((args.min_lng, args.max_lng, args.min_lat, args.max_lat))
ax.coastlines()
# ax.stock_img()
ax.add_collection(patch_collection)

plt.show()
