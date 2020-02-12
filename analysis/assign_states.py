import pandas as pd
import shapely as shp
from shapely.geometry import Point, asShape


def get_state(states, point):
	for name, shape in states:
		if shape.contains(point):
			return name

df: pd.DataFrame = pd.read_csv("data/hops_aggregate_ext.csv")
df.drop(df.columns.difference(["lat", "lng", "frac_c_efficiency"]), 1, inplace=True)
print("Loaded {} data points".format(len(df)))
df = df.groupby(["lng", "lat"], as_index=False).mean()
print("Grouped to {} data points".format(len(df)))

# Convert lat/lng into Points, because the Point constructor doesn't like Pandas
tmp = []
for coord in df[["lat", "lng"]].to_numpy():
	tmp.append(Point(coord[1], coord[0]))
df["point"] = tmp

states_shapes = shp.Reader("data/shapes/states.shp")
states = [(state_record[0], asShape(state_shape)) for state_shape, state_record in zip(states_shapes.shapes(), states_shapes.records())]

tmp.clear()
for i, point in enumerate(df["point"]):
	tmp.append(get_state(states, point))
	if i % 250 == 0:
		print("{}/{}".format(i, len(df)))
df["state"] = tmp
df.drop("point")

df.to_pickle("data/hops_aggregate_us_state.pkl.gz")