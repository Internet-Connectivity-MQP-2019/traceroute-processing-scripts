#!/usr/bin/env python3
import argparse

import matplotlib
import pandas as pd
import matplotlib.pyplot as plt

import postgresql

parser = argparse.ArgumentParser(description="Plot a histogram to highlight data distributions")
parser.add_argument("dbconfig", type=str, help="PostgreSQL database config.")
parser.add_argument("--dpi", "-p", type=int, default=600, help="Chart DPI")
parser.add_argument("--output", "-o", type=str, help="Output CSV to dump to, if desired")
args = parser.parse_args()

with open(args.dbconfig, "r") as dbconfig:
	connection = postgresql.get_postgres_connection(dbconfig)

df = pd.read_sql_query("SELECT measurements AS data FROM hops_aggregate WHERE NOT indirect AND measurements < 30 LIMIT 10000000", connection)
print("Retrieved {} rows".format(len(df)))

# Main histogram chart
matplotlib.rcParams["figure.dpi"] = args.dpi
fig, ax = plt.subplots()
ax.hist(df["data"], bins=15)
labels = ax.get_xticklabels()
ax.set(ylabel="Measurement count", xlabel="RTT values",
	   title="Average RTT distribution")

# Display 5th and 95th quantiles
quantiles = df.quantile(q=[0.05, 0.95]).to_numpy()
print("Quantiles at {} and {}".format(quantiles[0], quantiles[1]))
ax.axvline(quantiles[0], ls="--", color="r")
ax.axvline(quantiles[1], ls="--", color="r")

plt.show()

if args.output is not None:
	df.to_csv(args.output)
