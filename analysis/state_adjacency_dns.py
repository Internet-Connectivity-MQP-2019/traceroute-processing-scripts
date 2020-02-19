import itertools

import numpy as np
import pandas as pd
from scipy.stats import kruskal

df: pd.DataFrame = pd.read_csv("data/final_dns_dataset.csv")
df.drop(["Unnamed: 0", "Unnamed: 0.1", "Unnamed: 0.1.1", "recursive_ip", "recursive_state", "authoritative_ip", "recursive_latitude",
         "recursive_longitude", "distance", "rtt"], axis=1, inplace=True)
df.rename({
    "authoritative_state": "state",
    "authoritative_latitude": "lat",
    "authoritative_longitude": "long",
    "rtt_normalized": "rtt"
}, inplace=True, axis=1)

df = df.groupby(["lat", "long"], as_index=False).agg({"rtt": "median", "state": "first"})

# Find all values for each state
states_value = {}
for state in df["state"].unique():
	states_value[state] = list(sorted(df[df["state"] == state]["rtt"]))

# Calculate p values between each state
state_permutations = list(itertools.combinations(states_value.keys(), 2))
sp = pd.DataFrame(columns=["state1", "state2", "h", "p", "ratio"])  # State pairs
for state1, state2 in state_permutations:
	# Calculate ratio of scores
	state1_median = np.median(states_value[state1])
	state2_median = np.median(states_value[state2])
	if state2_median < state1_median:
		state1_median, state2_median = state2_median, state1_median
		state1, state2 = state2, state1
	ratio = state1_median / state2_median

	results = kruskal(states_value[state1], states_value[state2])
	sp = sp.append(pd.Series([state1, state2, results[0], results[1], ratio], index=sp.columns), ignore_index=True)

sp.sort_values(["p"], axis=0, inplace=True)
sp.to_pickle("data/state_adjacency_dns.pkl")
