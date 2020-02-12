import itertools

import pandas as pd
import numpy as np
from scipy.stats import stats
from shapely.geometry import Point, asShape
import shapefile as shp

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
	states_value[state] = list(df[df["state"] == state]["rtt"])
result = stats.kruskal(*list(states_value.values()))
print("Kruskal-Wallis test got h={}, p={}".format(result[0], result[1]))

state_pairs = list(itertools.combinations(states_value.keys(), 2))
df = pd.DataFrame(columns=["state1", "state2", "h", "p"])
for state1, state2 in state_pairs:
	try:
		results = stats.kruskal(states_value[state1], states_value[state2])
		df = df.append(pd.Series([state1, state2, results[0], results[1]], index=df.columns), ignore_index=True)
	except ValueError:
		pass

df.sort_values("p", inplace=True)
