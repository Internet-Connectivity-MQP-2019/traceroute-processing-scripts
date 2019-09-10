#!/usr/bin/python3
import json
import socket
import sqlite3
import struct
import sys

import pandas as pd

"""Usage: ./traceroute_hopper_pandas input output_database"""

if len(sys.argv) < 3:
	print("Not enough arguments", file=sys.stderr)
	exit(1)

hops = []
base_src = ""
save_db = sqlite3.connect(sys.argv[2], 3600)
with open(sys.argv[1], "r") as file:
	for i, line in enumerate(file):
		traceroute = json.loads(line)

		# Source hostname changed; have to resolve it (then skip traceroute processing because it's not a traceroute)
		if "hostname" in traceroute.keys():
			base_src = str(socket.gethostbyname(traceroute["hostname"]))
			continue

		if i % 25000 == 0:
			print("Processed {} traceroutes".format(i))
			hops_pd = pd.DataFrame(hops, columns=["src", "dst", "rtt"])
			hops_pd.to_sql("hops", save_db, index=False, if_exists="append")
			hops.clear()
			del hops_pd

		if "hops" not in traceroute.keys():
			# Some traceroutes fail but the rest of the file should still be okay
			continue

		# Regular traceroute entry
		for j, hop in enumerate(traceroute["hops"]):
			src = base_src if j == 0 else traceroute["hops"][j - 1]["addr"]
			dst = hop["addr"]
			rtt = hop["rtt"] if j == 0 else hop["rtt"] - traceroute["hops"][j - 1]["rtt"]

			# Add data to temporary array, converting ips to numbers
			hops.append([struct.unpack("!L", socket.inet_aton(src))[0], struct.unpack("!L", socket.inet_aton(dst))[0],
						 rtt])

# Create pandas dataframe and use it to save the data
print("Saving {} hops to sqlite".format(len(hops)))
pd.DataFrame(hops, columns=["src", "dst", "rtt"]).to_sql("hops", save_db, index=False, if_exists="append")
