import sys

import pandas as pd
import plotly.graph_objs as go
from sklearn.cluster import OPTICS
import pickle

df: pd.DataFrame = pd.read_pickle(sys.argv[1])
print("Loaded {} data points".format(len(df)))
df = df[(-127 < df["dst_lng"]) & (df["dst_lng"] < -65) & (25 < df["dst_lat"]) & (df["dst_lat"] < 51)]
print("Filtered to {} data points".format(len(df)))

# c1 = go.Scatter3d(x=df["dst_lng"][:10000], y=df["dst_lat"][:10000], z=df["frac_c_efficiency"][:10000], mode="markers", marker = {
# 	"size": 12,
# 	"opacity": 0.7
# })

vals = df[["dst_lng", "dst_lat"]].to_numpy()
clust = OPTICS(min_samples=1000, xi=0.05, min_cluster_size=0.02, n_jobs=4)
clust.fit(vals)
with open("clusters.pkl", "wb") as out:
	pickle.dump(clust, out)

print(clust.reachability_[clust.ordering_])
print(clust.labels_[clust.ordering_])

# layout = go.Layout(margin={
# 	"l": 0, "r": 0, "b": 0, "t": 0
# })
# fig = go.Figure(data=None, layout=layout)
# fig.show()
