#!/usr/bin/env python3
import argparse

import cartopy.crs as ccrs
import matplotlib.cm
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
from matplotlib.collections import PatchCollection
from matplotlib.patches import Rectangle
from scipy.interpolate import griddata

import postgresql
from SpatialQuadtree import SpatialQuadtree

parser = argparse.ArgumentParser(description="Plot quadtree-analyzed data on a map.")
parser.add_argument("dbconfig", type=str, help="Postgres database configuration.")
parser.add_argument("-m", "--mode", type=str, default="contour", choices=["contour", "scatter", "boxes"],
					help="Mode to run in; contour mode will plot a contour map, scatter shows a scatterplot, boxes"
						 " will show raw quadtree boxes.")
parser.add_argument("--stock-img", action="store_true", help="Display a stock map image in the background.")
parser.add_argument("-v", "--verbose", action="store_true", help="Verbose output.")
parser.add_argument("--max-lng", type=float, default=180, help="Maximum longitude.")
parser.add_argument("--max-lat", type=float, default=90, help="Maximum latitude.")
parser.add_argument("--min-lng", type=float, default=-180, help="Minimum longitude.")
parser.add_argument("--min-lat", type=float, default=-90, help="Minimum latitude.")
parser.add_argument("-p", "--dpi", type=int, default=300, help="Chart DPI")
parser.add_argument("-e", "--exponent", type=float, default=0.25, help="Exponent used for data processing, improves graph"
																	" contrast at cost of accuracy.")
parser.add_argument("--min-connectivity", type=float, default=0.003, help="Minimum ms/km required for display on"
																		  " map. Useful for removing outliers.")
parser.add_argument("--max-connectivity", type=float, default=0.03, help="Maximum ms/km required for display on"
																		 " map. Useful for removing outliers.")
args = parser.parse_args()
vprint = print if args.verbose else lambda *x, **y: None

with open(args.dbconfig, "r") as dbconfig:
	connection = postgresql.get_postgres_connection(dbconfig)
cursor = connection.cursor()

vprint("Grabbing connectivity data")
cursor.execute("SELECT quad, center(quad), connect_avg FROM quads"
						 " WHERE box(point(?, ?), point(?, ?)) @> quad",
						 (args.min_lat, args.min_lng, args.max_lat, args.max_lng))
results = cursor.fetchall()
connection.close()

vprint("Loaded {} data points".format(len(results)))
# Convert from string quads to numbers
data = []
for result in results:
	quad = list(map(lambda x: float(x), result[0].replace("(", "").replace(")", "").split(',')))
	quad.extend(list(map(lambda x: float(x), result[1].replace("(", "").replace(")", "").split(','))))
	quad.extend(result[2:])
	data.append(quad)
quads = pd.DataFrame(data, columns=["max_lat", "max_lng", "min_lat", "min_lng", "center_y", "center_x", "avg"])

# Common plot setup
matplotlib.rcParams["figure.dpi"] = args.dpi
ax = plt.axes(projection=ccrs.PlateCarree())
ax.set_extent((args.min_lng, args.max_lng, args.min_lat, args.max_lat))
ax.coastlines()
if args.stock_img:
	ax.stock_img()

# Data processing to prepare for the different types of plots
quads["avg"] = pow(quads["avg"], args.exponent)
if args.mode == "boxes":
	vprint("Calculating heights+widths for box plot")
	quads["height"] = quads["max_lat"] - quads["min_lat"]
	quads["width"] = quads["max_lng"] - quads["min_lng"]
	patches = quads.apply(lambda row: Rectangle((row["min_lng"], row["min_lat"]), row["width"], row["height"]), axis=1)
	patch_collection = PatchCollection(patches, cmap=matplotlib.cm.inferno, alpha=0.6)
	patch_collection.set_array(quads["avg"])
	ax.add_collection(patch_collection)
else:
	# Scatter and contour modes both need center x/y coordinate and a filter such that no average is 0. 0 average is an
	# an uncommon edge case that occurs only in certain cases where a bound for a quadtree split occur on one of the
	# existing bounds for the current box.
	quads = quads[quads["avg"] != 0]

if args.mode == "contour":
	# Contour mode requires interpolated data
	vprint("Interpolating data for contour plot")
	X_RES = 180*5
	Y_RES = 90*5
	grid_x = np.linspace(-X_RES, X_RES, 2*X_RES + 1)
	grid_y = np.linspace(-Y_RES, Y_RES, 2*Y_RES + 1)
	z = griddata((quads["center_x"].values, quads["center_y"].values), quads["avg"].values, (grid_x[None, :], grid_y[:, None]), method='linear')
	plt.contourf(grid_x, grid_y, z, 50, transform=ccrs.PlateCarree(), cmap=matplotlib.cm.inferno)
elif args.mode == "scatter":
	plt.scatter(quads["center_x"].values, quads["center_y"].values, c=quads["avg"].values, transform=ccrs.PlateCarree(),
				marker=".", alpha=0.6, cmap=matplotlib.cm.inferno)

plt.show()
print("Done!")
