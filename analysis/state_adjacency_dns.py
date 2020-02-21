import networkx as nx
import pandas as pd

df: pd.DataFrame = pd.read_csv("dns_rtt_normalized_p_values_2_1.0.csv", index_col=0)
# df: pd.DataFrame = pd.read_csv("rtt_3_0.2_kruskals_p.csv", index_col=0)
graph = nx.convert_matrix.from_pandas_adjacency(df)
df = nx.convert_matrix.to_pandas_edgelist(graph)
df = df[df["source"] != df["target"]]
df.rename({"source": "state1", "target": "state2", "weight": "p"}, inplace=True, axis=1)
df = df[df.p < 0.05]

df_medians: pd.DataFrame = pd.read_pickle("dns_rtt_normalized_2_1.0_pairs_pickle.pkl")
df_medians.index = df_medians.index.droplevel([2, 3])
df_medians = df_medians.groupby(level=0).median()

df = df.join(df_medians, "state1").join(df_medians, "state2", rsuffix="2")
comp = df["median"] < df["median2"]
df.loc[comp, ["state1", "state2"]] = df.loc[comp, ["state2", "state1"]].values
df.loc[comp, ["median", "median2"]] = df.loc[comp, ["median2", "median"]].values
df["ratio"] = df["median2"] / df["median"]
df.drop(["median", "median2"], inplace=True, axis=1)

df.to_pickle("state_adjacency_dns2_norm.pkl")

