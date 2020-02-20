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


df: pd.DataFrame = pd.read_csv("final_dns_dataset.csv", usecols=[5,9,10,11,12,13])
df.rename({"authoritative_state": "state", "authoritative_latitude": "lat", "authoritative_longitude": "lng"}, inplace=True, axis=1)
df = df.groupby(["lat", "lng"], as_index=False).agg({"state": "first", "rtt": ["median", "mean", "std"], "rtt_normalized": ["median", "mean", "std"]})

df = df["rtt_normalized"]["median"]
quantiles = df.quantile([0.025, 0.975]).to_numpy()
plot_kde(data=df,
         xlabel="Normalized RTT median per location (ms)",
         ylabel="Density",
         title="Distribution of DNS Normalized RTT Median by Location",
         clip=quantiles)
plt.show()
