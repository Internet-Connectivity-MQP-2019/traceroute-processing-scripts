#!/usr/bin/env python3
import argparse

import matplotlib
import pandas as pd
import matplotlib.pyplot as plt

import postgresql

parser = argparse.ArgumentParser(description="Plot a histogram to highlight data distributions")
parser.add_argument("dbconfig", type=str, help="PostgreSQL database config.")
args = parser.parse_args()

with open(args.dbconfig, "r") as dbconfig:
	connection = postgresql.get_postgres_connection(dbconfig)

df = pd.read_sql_query("SELECT CASE rtt_stdev WHEN 0 THEN 0 ELSE rtt_avg / rtt_stdev END AS data FROM hops_aggregate WHERE rtt_stdev < 125 AND rtt_avg < 1000", connection)
print("Retrieved {} rows".format(len(df)))

# Main histogram chart
matplotlib.rcParams["figure.dpi"] = 600
fig, ax = plt.subplots()
ax.hist(df["data"], bins=1500)
labels = ax.get_xticklabels()
ax.set(xlim=[0, 300], ylabel="Measurement count", xlabel="Coefficient of variation",
	   title="CAIDA Coefficient of Variation Distribution")

# Display 5th and 95th quantiles
quantiles = df.quantile(q=[0.05, 0.95]).to_numpy()
print("Quantiles at {} and {}".format(quantiles[0], quantiles[1]))
ax.axvline(quantiles[0], ls="--", color="r")
ax.axvline(quantiles[1], ls="--", color="r")

plt.show()
