import argparse

import numpy as np
import pandas as pd
from scipy.stats import stats

parser = argparse.ArgumentParser(description="Process an aggregated hops CSV into a dataframe, remove outliers, and dump to pkl")
parser.add_argument("csv", type=str, help="CSV file to read")
parser.add_argument("pkl", type=str, help="Pickle file to output to")
parser.add_argument("--verbose", "-v", action="store_true", help="Verbose output")
parser.add_argument("--zscore", "-z", type=float, default=2.0, help="Absolute value Z score to filter by")
parser.add_argument("--indirect", "-i", action="store_true", help="Operate on direct measurements or indirect measurements")
args = parser.parse_args()
vprint = print if args.verbose else lambda *x, **y: None

vprint("Parsing {}; depending on file size this may take some time...".format(args.csv))
df = pd.read_csv(args.csv, header=[0], dtype=np.float32)
vprint("Loaded {} data points, beginning filtering".format(len(df)))

vprint("Filtering to only {}direct values and discarding 0-distance+ <0ms RTTs".format("in" if args.indirect else ""))
df = df[(df["indirect"] == (1 if args.indirect else 0)) & (df["distance"] > 0) & (df["rtt_avg"] > 0)]

vprint("Now at {} rows".format(len(df)))

vprint("Filtering by RTT; RTT max/min/avg/stdev is {:.3f}/{:.3f}/{:.3f}/{:.3f}"
      .format(df["rtt_avg"].max(), df["rtt_avg"].min(), df["rtt_avg"].mean(), df["rtt_avg"].std()))
df = df[np.abs(stats.zscore(df["rtt_avg"])) <= args.zscore]
vprint("Filtered by RTT; RTT max/min/avg/stdev is now {:.3f}/{:.3f}/{:.3f}/{:.3f}"
      .format(df["rtt_avg"].max(), df["rtt_avg"].min(), df["rtt_avg"].mean(), df["rtt_avg"].std()))

vprint("Now at {} rows".format(len(df)))

vprint("Filtering by distance; distance max/min/avg/stdev is {:.3f}/{:.3f}/{:.3f}/{:.3f}"
      .format(df["distance"].max(), df["distance"].min(), df["distance"].mean(), df["distance"].std()))
df = df[np.abs(stats.zscore(df["distance"])) <= args.zscore]
vprint("Filtered by distance; distance max/min/avg/stdev is now {:.3f}/{:.3f}/{:.3f}/{:.3f}"
      .format(df["distance"].max(), df["distance"].min(), df["distance"].mean(), df["distance"].std()))

vprint("Now at {} rows".format(len(df)))

vprint("Calculating RTT/km filtering")
df["connectivity"] = df["rtt_avg"] / df["distance"]

vprint("Filtering by connectivity; connectivity max/min/avg/stdev is {:.3f}/{:.3f}/{:.3f}/{:.3f}"
      .format(df["connectivity"].max(), df["connectivity"].min(), df["connectivity"].mean(), df["connectivity"].std()))
df = df[np.abs(stats.zscore(df["connectivity"])) <= args.zscore]
vprint("Filtered by connectivity; connectivity max/min/avg/stdev is now {:.3f}/{:.3f}/{:.3f}/{:.3f}"
      .format(df["connectivity"].max(), df["connectivity"].min(), df["connectivity"].mean(), df["connectivity"].std()))

vprint("Dataframe reduced to {} rows; sorting...".format(len(df)))
df.sort_values(by=["connectivity", "measurements", "rtt_avg", "distance"], inplace=True, kind="mergesort")

vprint("Dumping dataframe to {}".format(args.pkl))
df.to_pickle(args.pkl)
