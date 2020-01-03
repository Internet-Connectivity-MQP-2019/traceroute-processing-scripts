import sys

import matplotlib
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
import sphviewer as sph
from matplotlib.colors import DivergingNorm
from scipy.interpolate import griddata


def plot_sph(x: pd.Series, y: pd.Series, w: pd.Series, nb=32, xsize=1000, ysize=1000):
	x_min = np.min(x)
	x_max = np.max(x)
	y_min = np.min(y)
	y_max = np.max(y)
	x0 = np.average((x_min, x_max))
	y0 = np.average((y_min, y_max))

	pos = np.zeros([len(df), 3])
	pos[:, 0] = x
	pos[:, 1] = y
	w = w.to_numpy() * 100

	particles = sph.Particles(pos, mass=w, nb=nb)
	scene = sph.Scene(particles)
	scene.update_camera(r="infinity", x=x0, y=y0, z=0, xsize=xsize, ysize=ysize)
	render = sph.Render(scene)
	render.set_logscale()
	img = render.get_image()
	extent = render.get_extent()
	for i, j in zip(range(4), [x0, x0, y0, y0]):
		extent[i] += j

	return img, extent


# Data loading + grouping
df: pd.DataFrame = pd.read_pickle(sys.argv[1])
print("Loaded {} data points".format(len(df)))
df = df.groupby(["dst_lng", "dst_lat"], as_index=False).mean()
print("Grouped to {} data points".format(len(df.index)))
df = df[(-127 < df["dst_lng"]) & (df["dst_lng"] < -65) & (25 < df["dst_lat"]) & (df["dst_lat"] < 51)]
print("Filtered to {} data points".format(len(df)))

# Set up matplotlib
# matplotlib.rcParams["figure.dpi"] = 900
matplotlib.rcParams["axes.titlesize"] = 10
fig, axes = plt.subplots(2, 2, dpi=900)
axes = np.reshape(axes, (1, 4)).tolist()[0]  # Wat?
for axis in axes:
	axis.axis("off")
ax1 = axes[0]
ax2 = axes[1]
ax3 = axes[2]
ax4 = axes[3]


#########
# PLOTS #
#########

# SCATTER PLOT
scatter_plot = ax1.scatter(df["dst_lng"].values, df["dst_lat"].values, c=df["frac_c_efficiency"].values,
            cmap="inferno", norm=DivergingNorm(df["frac_c_efficiency"].mean()), s=3)
fig.colorbar(scatter_plot, ax=ax1, orientation="horizontal", pad=0).minorticks_on()
ax1.set_title("Scatter plot")


# LINEAR INTERPOLATION HEATMAP
MAP_RES = 50
xx = np.linspace(df["dst_lng"].min(), df["dst_lng"].max(), (df["dst_lng"].max() - df["dst_lng"].min())*MAP_RES)
yy = np.linspace(df["dst_lat"].min(), df["dst_lat"].max(), (df["dst_lat"].max() - df["dst_lat"].min())*MAP_RES)
x, y = np.meshgrid(xx, yy)
z = griddata((df["dst_lng"], df["dst_lat"]), df["frac_c_efficiency"], (x, y),
             method="linear",
             fill_value=df["frac_c_efficiency"].mean())
z = np.flip(z, 0)
linear_heatmap = ax2.imshow(z, cmap=matplotlib.cm.bwr, norm=DivergingNorm(df["frac_c_efficiency"].mean()))
fig.colorbar(linear_heatmap, ax=ax2, orientation="horizontal").minorticks_on()
ax2.set_title("Linear interpolation heatmap")


# SPH HEATMAP -- 16
img16, extent16 = plot_sph(x=df["dst_lng"], y=df["dst_lat"], w=df["frac_c_efficiency"], nb=16)
ax3.imshow(img16, extent=extent16, origin="lower", aspect="auto", cmap="bwr")
ax3.set_title("SPH Heatmap, nb=16")

img32, extent32 = plot_sph(x=df["dst_lng"], y=df["dst_lat"], w=df["frac_c_efficiency"], nb=48)
ax4.imshow(img32, extent=extent32, origin="lower", aspect="auto", cmap="bwr")
ax4.set_title("SPH Heatmap, nb=48")


plt.show()
print("Done!")
