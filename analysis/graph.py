import matplotlib.pyplot as plt
import networkx as nx
import pandas as pd
from networkx.algorithms.community import asyn_lpa_communities
from networkx.algorithms.dag import topological_sort

valid_comps = True

df: pd.DataFrame = pd.read_pickle("data/state_adjacency_dns.pkl")
df = df[df.p < 0.05 if valid_comps else df.p >= 0.05]
df.sort_values(["ratio"], inplace=True, ascending=True)

graph = nx.from_pandas_edgelist(df, "state1", "state2", ["ratio", "p", "h"], create_using=(nx.DiGraph if valid_comps else nx.Graph))
pos = nx.spring_layout(graph, center=(0.5, 0.5), scale=0.5, k=1/len(graph)**0.1, seed=1)
# pos = nx.kamada_kawai_layout(graph, center=(0.5, 0.5), scale=0.5)

# Base edges
nx.draw_networkx_edges(graph, pos=pos, width=0.2)

# Bridges
if type(graph) is not nx.DiGraph and nx.has_bridges(graph):
	nx.draw_networkx_edges(graph, edgelist=list(nx.bridges(graph)), pos=pos, width=3, alpha=0.5, edge_color="r")

# Nodes with colors
groups = list(asyn_lpa_communities(graph, weight="h"))
colors = [[state in com for com in groups].index(True) for state in graph]
nx.draw_networkx_nodes(graph, pos=pos, node_color=colors, cmap="tab20")

# Labels
nx.draw_networkx_labels(graph, pos, font_size=10)

if type(graph) is nx.DiGraph:
	sorts = list(reversed(list(topological_sort(graph))))

plt.show()
