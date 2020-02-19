import numpy as np
import pandas as pd
import seaborn as sns
from matplotlib.axes import Axes
from matplotlib import pyplot as plt


def plot_kde(data, xlabel, ylabel, title, clip):
	sns.set_style("darkgrid")
	plot: Axes = sns.kdeplot(data=data,
	                         shade=True,
	                         kernel="gau",
	                         clip=clip,
	                         legend=False,
	                         color=sns.xkcd_rgb["dark red"])
	plot.set_title(title, fontdict={"size": 14})
	plot.set_xlabel(xlabel)
	plot.set_ylabel(ylabel)


with open("data/siteping.json", "r") as file:
	df: pd.DataFrame = pd.read_json(file)
print("Loaded {} data points".format(len(df)))

df.drop(["_id", "favicon", "ip", "city", "alt_city", "alt_retro", "backToBackId", "connectionInfo", "alt_latitude",
         "alt_longitude", "isMobile", "state", "country", "alt_state"], inplace=True, axis=1)

df = df.groupby(["latitude", "longitude"], as_index=False).agg([np.std, np.median, np.mean, "count"])
df.columns = list(map("".join, df.columns.values))
df["cv"] = df["rttstd"] / df["rttmean"]

df = df["rttmean"]
quantiles = df.quantile([0.025, 0.975]).to_numpy()
plot_kde(data=df,
         xlabel="Mean RTT per location (ms)",
         ylabel="Density",
         title="Distribution of Site Ping RTT Mean by Location",
         clip=quantiles)
plt.show()
