import pandas as pd

datasets = ["caida", "dns", "siteping"]
dfs = {}
for dataset in datasets:
	df: pd.DataFrame = pd.read_pickle("state_adjacency_{}.pkl".format(dataset))
	df.drop(["state1", "h"], inplace=True, axis=1, errors="ignore")
	df.rename({"state2": "state"}, inplace=True, axis=1)
	g = df.groupby("state")
	cols = {col: ["median", "mean"] for col in df.columns if col != "state"}
	cols["state"] = "count"
	df = g.agg(cols)
	dfs[dataset] = df

df = pd.concat(dfs, axis=1, sort=True)
cr = df.corr()

