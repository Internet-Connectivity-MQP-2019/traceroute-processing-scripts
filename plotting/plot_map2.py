import sys

import pandas as pd
import numpy as np
import matplotlib.cm
import matplotlib.pyplot as plt
import cartopy.crs as ccrs
from scipy.interpolate import griddata

df: pd.DataFrame = pd.read_pickle(sys.argv[1])
print("Loaded {} data points".format(len(df)))
#
# df = df[(-127 < df["src_lng"]) & (df["src_lng"] < -65) & (25 < df["src_lat"]) & (df["src_lat"] < 51)]
# print("Filtered to {} data points".format(len(df)))
# df.to_pickle("data/hops_aggregate_us_direct_processed_for_real.pkl")

df = df.groupby(["dst_lng", "dst_lat"], as_index=False).median()
print("Grouped to {} data points".format(len(df)))

matplotlib.rcParams["figure.dpi"] = 900
MAP_RES = 5
xx = np.linspace(-127, -65, (127-65)*MAP_RES)
yy = np.linspace(25, 51, (51-25)*MAP_RES)
x, y = np.meshgrid(xx, yy)
z = griddata((df["dst_lng"], df["dst_lat"]), df["frac_c_efficiency"], (xx[None, :], yy[:, None]), method="nearest")
ax = plt.axes(projection=ccrs.PlateCarree())
ax.set_extent((-127, -65, 25, 51))
ax.coastlines()
ax.set(ylabel="Latitude", xlabel="Longitude", title="Speed-of-Light Efficiency Heatmap (US)")
# plt.scatter(df["dst_lng"].values, df["dst_lat"].values, c=np.power(df["frac_c_efficiency"].values, 1), transform=ccrs.PlateCarree(), cmap=matplotlib.cm.inferno)
plt.contourf(x, y, z, 100, transform=ccrs.PlateCarree())
plt.show()
print("Done!")
