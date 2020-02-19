import itertools

import pandas as pd
import numpy as np
from scipy.stats import stats
from shapely.geometry import Point, asShape
import shapefile as shp

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

def get_state(states, point):
	for name, shape in states:
		if shape.contains(point):
			return name

with open("data/siteping.json", "r") as file:
	df: pd.DataFrame = pd.read_json(file)
print("Loaded {} data points".format(len(df)))

df.drop(["_id", "favicon", "ip", "city", "alt_city", "alt_retro", "backToBackId", "connectionInfo", "alt_latitude",
         "alt_longitude", "isMobile"], inplace=True, axis=1)

df = df[np.abs(stats.zscore(df["rtt"])) <= 2.0]
print("Filtered to {} data points".format(len(df)))

df = df.groupby(["latitude", "longitude"], as_index=False).min()
print("Grouped to {} data points".format(len(df)))

# Convert lat/lng into Points, because the Point constructor doesn't like Pandas
tmp = []
for i in range(len(df)):
	tmp.append(Point(df.iloc[i]["longitude"], df.iloc[i]["latitude"]))
df["point"] = tmp

states_shapes = shp.Reader("data/shapes/states.shp")
states = [(state_record[0], asShape(state_shape)) for state_shape, state_record in zip(states_shapes.shapes(),
                                                                                       states_shapes.records())]

tmp.clear()
for point in df["point"]:
	tmp.append(get_state(states, point))

df["state"] = tmp
df.drop(["point"], axis=1, inplace=True)
df.dropna(axis=0, inplace=True)

df.sort_values(["rtt"], inplace=True)
df.to_pickle("data/siteping.pkl")
df.to_csv("data/siteping_processed.csv", header=True, index=False)

states_value = {}
for state in df["state"].unique():
	abbr = us_state_abbrev[state]
	states_value[abbr] = list(df[df["state"] == state]["rtt"])

state_pairs = list(itertools.combinations(states_value.keys(), 2))
df = pd.DataFrame(columns=["state1", "state2", "h", "p", "ratio"])
for state1, state2 in state_pairs:
	# Calculate ratio of scores
	state1_median = np.median(states_value[state1])
	state2_median = np.median(states_value[state2])
	if state2_median < state1_median:
		state1_median, state2_median = state2_median, state1_median
		state1, state2 = state2, state1
	ratio = state1_median / state2_median
	try:
		results = stats.kruskal(states_value[state1], states_value[state2])
	except ValueError:
		results = [0, 1]
	df = df.append(pd.Series([state1, state2, results[0], results[1], ratio], index=df.columns), ignore_index=True)

df.sort_values("p", inplace=True)
df.to_pickle("data/state_adjacency_siteping.pkl")
