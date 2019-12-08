import argparse
import cartopy.crs as ccrs
import matplotlib
import matplotlib.pyplot as plt
import pandas as pd
import numpy as np
from scipy.interpolate import griddata

from postgresql import get_postgres_connection

parser = argparse.ArgumentParser(description="Plot IP point data on a map")
parser.add_argument("source", type=str, help="IP to plot data points from")
parser.add_argument("dbconfig", type=str, help="Database connection config")
args = parser.parse_args()

with open(args.dbconfig, "r") as dbconfig:
	connection = get_postgres_connection(dbconfig)

matplotlib.rcParams["figure.dpi"] = 600
rtts = pd.read_sql_query("SELECT lat, lng, connectivity FROM hops_ms_per_km WHERE connectivity < 0.05 AND connectivity > 0.01"
# rtts = pd.read_sql_query(# "SELECT dst_lat AS lat, dst_lng AS lng, rtt_avg AS rtt FROM hops_aggregate_stdev_filtered WHERE src = '{}' "
# 						 "AND rtt_avg > 30 AND rtt_avg < 200 "
						 " AND lat < 71.35 AND lat > 25.28 AND lng > -126.66 AND lng < -66.27",
						 # "AND dst_lat < 71.35 AND dst_lat > 25.28 AND dst_lng > -100.66 AND dst_lng < -66.27 "
						 # "AND dst_lat < 71.35 AND dst_lat > 18.91 AND dst_lng > -171.79 AND dst_lng < -66.96 "
						 # " ORDER BY rtt_avg ASC "
						 # "LIMIT 100000000".format(args.source),
						 connection)
						 # "GROUP BY dst_lat, dst_lng ORDER BY rtt LIMIT 10000", connection)

ax = plt.axes(projection=ccrs.PlateCarree())
# ax.stock_img()
ax.coastlines()

# rtts["connectivity"] = pow(rtts["connectivity"], 2)
X_RES = 180*5
Y_RES = 90*5
grid_x = np.linspace(-180, 180, 2*X_RES)
grid_y = np.linspace(-90, 180, 2*Y_RES)

plt.scatter(rtts["lng"].values, rtts["lat"].values, c=rtts["connectivity"], transform=ccrs.PlateCarree(), marker=".", alpha=0.1)
# z = griddata((rtts["lng"].values, rtts["lat"].values), rtts["rtt"].values, (grid_x[None, :], grid_y[:, None]), method='linear')
# plt.contourf(grid_x, grid_y, z, 100, transform=ccrs.PlateCarree())
plt.show()
