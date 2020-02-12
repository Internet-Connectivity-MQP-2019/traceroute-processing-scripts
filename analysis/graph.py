import matplotlib.pyplot as plt
import networkx as nx
import pandas as pd

df: pd.DataFrame = pd.read_pickle("data/state_adjacency.pkl")
graph = nx.from_pandas_edgelist(df, "state1", "state2", ["ratio", "p"])
plt.show()
