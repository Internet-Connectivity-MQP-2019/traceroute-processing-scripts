import argparse
import cartopy.crs as ccrs
import matplotlib
import matplotlib.pyplot as plt
import matplotlib.cm
import pandas as pd
import sqlite3

from matplotlib.collections import PatchCollection
from matplotlib.patches import Rectangle

parser = argparse.ArgumentParser(description="Plot quadtree rects on a map")
parser.add_argument("input", type=str, help="Database to retrieve quads from")
args = parser.parse_args()

connection = sqlite3.connect(args.input)
quads = pd.read_sql("SELECT * FROM boxes", connection)

matplotlib.rcParams["figure.dpi"] = 900
ax = plt.axes(projection=ccrs.PlateCarree())
ax.coastlines()
ax.stock_img()

quads["height"] = quads["max_lat"] - quads["min_lat"]
quads["width"] = quads["max_lng"] - quads["min_lng"]
patches = quads.apply(lambda row: Rectangle((row["min_lng"], row["min_lat"]), row["width"], row["height"]), axis=1)
patch_collection = PatchCollection(patches, cmap=matplotlib.cm.afmhot, alpha=0.6)
patch_collection.set_array(quads["avg"])
ax.add_collection(patch_collection)

plt.show()
