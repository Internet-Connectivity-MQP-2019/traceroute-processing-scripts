import pandas as pd

datasets = ["caida", "siteping", "dns2", "dns2_norm"]
dfs = {}
for dataset in datasets:
	df: pd.DataFrame = pd.read_pickle("state_adjacency_{}.pkl".format(dataset))
	df.drop(["state1", "h", "p"], inplace=True, axis=1, errors="ignore")
	df.rename({"state2": "state"}, inplace=True, axis=1)
	g = df.groupby("state")
	# Allows processing of ratio too, just remove ratio from list of dropped columns
	if len(df.columns) > 1:
		cols = {col: ["median", "mean"] for col in df.columns if col != "state"}
		cols["state"] = "count"
		df = g.agg(cols)
	else:
		df = pd.DataFrame({("state", "count"): g.state.count()})
	dfs[dataset] = df


# Put everything together
df = pd.concat(dfs, axis=1, sort=True)
cr = df.corr()
