#!/usr/bin/python3
import json
import os
import random
import shutil
import socket
import sys
import struct
import pyodbc

import geoip2.database

"""Usage: ./traceroute_hopper_db input output_database"""

if len(sys.argv) < 3:
	print("Not enough arguments", file=sys.stderr)
	exit(1)

locations_cache = {}
base_src = ""
with open(sys.argv[2], "r") as db_config:
	connection_str = "DRIVER={{PostgreSQL Unicode}};UID={user};Host={host};Database={database};Pooling=True;Min Pool Size=0;Max Pool Size=100;".format(
		**json.load(db_config))
	connection = pyodbc.connect(connection_str)
connection.setdecoding(pyodbc.SQL_WCHAR, encoding="utf-8")
connection.setencoding(encoding="utf-8")
connection.maxwrite = 2 << 32

cursor = connection.cursor()
hops = []
with open(sys.argv[1], "r") as file:
	for i, line in enumerate(file):
		traceroute = json.loads(line)

		# Source hostname changed; have to resolve it (then skip traceroute processing because it's not a traceroute)
		if "hostname" in traceroute.keys():
			base_src = str(socket.gethostbyname(traceroute["hostname"]))
			continue

		if i % 20000 == 0 and len(hops) != 0:
			print("Processed {} traceroutes".format(i))
			args_str = ",".join(("('%s', '%s', %d)" % tuple(hop)) for hop in hops)
			cursor.execute("INSERT INTO hops VALUES " + args_str)
			args_str = ""
			hops.clear()

		if "hops" not in traceroute.keys():
			# Some traceroutes fail but the rest of the file should still be okay
			continue

		# Regular traceroute entry
		for j, hop in enumerate(traceroute["hops"]):
			src = base_src if j == 0 else traceroute["hops"][j - 1]["addr"]
			dst = hop["addr"]
			rtt = hop["rtt"] if j == 0 else hop["rtt"] - traceroute["hops"][j - 1]["rtt"]

			# Insert hop data into database
			hops.append([src, dst, rtt])

			# Consider the RTT for the source to the destination too, not just the hop
			if j != 0:
				hops.append([base_src, dst, hop["rtt"]])

print("Committing to database...")
if len(hops) != 0:
	args_str = ",".join(("('%s', '%s', %d)" % tuple(hop)) for hop in hops)
	cursor.execute("INSERT INTO hops VALUES " + args_str)
cursor.commit()
print("Committed!")
connection.close()
