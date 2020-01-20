import matplotlib
import matplotlib.pyplot as plt
import pandas as pd
import seaborn as sns
from matplotlib.axes import Axes

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


matplotlib.rcParams["figure.dpi"] = 900

df: pd.DataFrame = pd.read_pickle("data/hops_aggregate_direct.pkl")
df = df["frac_c_efficiency"]

quantiles = df.quantile([0.025, 0.975]).to_numpy()
plot_kde(data=df,
         xlabel="Speed-of-light efficiency",
         ylabel="Density",
         title="Distribution of Speed-of-Light Efficiency Between IP Pairs",
         clip=quantiles)
plt.show()
