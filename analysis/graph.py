import matplotlib.pyplot as plt
import networkx as nx
from networkx.algorithms.community import *

import pandas as pd

df: pd.DataFrame = pd.read_pickle("data/state_adjacency.pkl")
df = df[df.p >= 0.05]

graph = nx.from_pandas_edgelist(df, "state1", "state2", ["ratio", "p"])
pos = nx.spring_layout(graph, center=(0.5, 0.5), scale=0.5, k=1/len(graph)**0.1, seed=1)

# Base network
nx.draw_networkx_edges(graph, pos=pos, width=0.2)

# Bridges
nx.draw_networkx_edges(graph, edgelist=list(nx.bridges(graph)), pos=pos, width=3, alpha=0.5, edge_color="r")

# Nodes with colors

groups = list(nx.find_cliques(graph))
colors = [[state in com for com in groups].index(True) for state in graph]
nx.draw_networkx_nodes(graph, pos=pos, node_color=colors, cmap="tab20")

# Labels
nx.draw_networkx_labels(graph, pos, font_size=10)

plt.show()
