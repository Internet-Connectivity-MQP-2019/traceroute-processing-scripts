import itertools

import matplotlib.pyplot as plt
import networkx as nx
import numpy as np
import pandas as pd
from scipy import stats

# From https://gist.github.com/rogerallen/1583593
us_state_abbrev = {
	'Alabama': 'AL',
	'Alaska': 'AK',
	'Arizona': 'AZ',
	'Arkansas': 'AR',
	'California': 'CA',
	'Colorado': 'CO',
	'Connecticut': 'CT',
	'Delaware': 'DE',
	'District of Columbia': 'DC',
	'Florida': 'FL',
	'Georgia': 'GA',
	'Hawaii': 'HI',
	'Idaho': 'ID',
	'Illinois': 'IL',
	'Indiana': 'IN',
	'Iowa': 'IA',
	'Kansas': 'KS',
	'Kentucky': 'KY',
	'Louisiana': 'LA',
	'Maine': 'ME',
	'Maryland': 'MD',
	'Massachusetts': 'MA',
	'Michigan': 'MI',
	'Minnesota': 'MN',
	'Mississippi': 'MS',
	'Missouri': 'MO',
	'Montana': 'MT',
	'Nebraska': 'NE',
	'Nevada': 'NV',
	'New Hampshire': 'NH',
	'New Jersey': 'NJ',
	'New Mexico': 'NM',
	'New York': 'NY',
	'North Carolina': 'NC',
	'North Dakota': 'ND',
	'Northern Mariana Islands':'MP',
	'Ohio': 'OH',
	'Oklahoma': 'OK',
	'Oregon': 'OR',
	'Palau': 'PW',
	'Pennsylvania': 'PA',
	'Puerto Rico': 'PR',
	'Rhode Island': 'RI',
	'South Carolina': 'SC',
	'South Dakota': 'SD',
	'Tennessee': 'TN',
	'Texas': 'TX',
	'Utah': 'UT',
	'Vermont': 'VT',
	'Virgin Islands': 'VI',
	'Virginia': 'VA',
	'Washington': 'WA',
	'West Virginia': 'WV',
	'Wisconsin': 'WI',
	'Wyoming': 'WY',
}

# Find all values for each state
states_value = {}
df: pd.DataFrame = pd.read_pickle("data/hops_aggregate_us_state.pkl")
df = df[(df.frac_c_efficiency >= 0) & (df.frac_c_efficiency < 1)]
for state in df["state"].unique():
	abbr = us_state_abbrev[state]
	states_value[abbr] = list(sorted(df[df["state"] == state]["frac_c_efficiency"]))

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

	results = stats.kruskal(states_value[state1], states_value[state2])
	sp = sp.append(pd.Series([state1, state2, results[0], results[1], ratio], index=sp.columns), ignore_index=True)

sp.sort_values(["p"], axis=0, inplace=True)
sp.to_pickle("data/state_adjacency.pkl")
