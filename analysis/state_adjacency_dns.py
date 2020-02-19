import networkx as nx
import pandas as pd

df: pd.DataFrame = pd.read_csv("rtt_3_0.2_kruskals_p.csv", index_col=0)
graph = nx.convert_matrix.from_pandas_adjacency(df)
df = nx.convert_matrix.to_pandas_edgelist(graph)
df = df[df["source"] != df["target"]]
df.rename({"source": "state1", "target": "state2", "weight": "p"}, inplace=True, axis=1)
df.sort_values(["p"], ascending=False, inplace=True)
df.to_pickle("state_adjacency_dns.pkl")
