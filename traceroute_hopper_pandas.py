#!/usr/bin/python3
import json
import os
import random
import shutil
import socket
import sqlite3
import sys
import struct

import geoip2.database
import numpy as np
import pandas as pd

"""Usage: ./traceroute_hopper_pandas input geoip_database output_database"""

if len(sys.argv) < 4:
	print("Not enough arguments", file=sys.stderr)
	exit(1)

# Create copy of GeoIP database in /tmp, open it with a geoip reader, and delete it immediately --
# this is done because the GeoIP database gets LOCKED when it's read (seriously, wtf?!) so we need
# to create copies of it for each instance of this script. Since Linux lets you delete files still
# in use (they only get truly deleted when the last thing that has them open closes the file
# descriptor), deleting the file is a handy insta-cleanup
database_filename = "/tmp/geoip-temp" + str(abs(random.randint(0, 9999999))) + ".mmdb"
shutil.copyfile(sys.argv[2], database_filename)
locations_db = geoip2.database.Reader(database_filename)
os.remove(database_filename)

locations_cache = {}
hops = []
base_src = ""
with open(sys.argv[1], "r") as file:
	for i, line in enumerate(file):
		traceroute = json.loads(line)

		# Source hostname changed; have to resolve it (then skip traceroute processing because it's not a traceroute)
		if "hostname" in traceroute.keys():
			base_src = str(socket.gethostbyname(traceroute["hostname"]))
			continue

		# if i % 5000 == 0:
		# 	print("Processed {} traceroutes".format(i))

		if "hops" not in traceroute.keys():
			# Some traceroutes fail but the rest of the file should still be okay
			continue

		# Regular traceroute entry
		for j, hop in enumerate(traceroute["hops"]):
			src = base_src if j == 0 else traceroute["hops"][j - 1]["addr"]
			dst = hop["addr"]
			rtt = hop["rtt"] if j == 0 else hop["rtt"] - traceroute["hops"][j - 1]["rtt"]

			# Get IP locations, first by trying the cache, then by trying the locations db
			coords = {src: {"lat": np.nan, "lng": np.nan, "post": np.nan}, dst: {"lat": np.nan, "lng": np.nan, "post": np.nan}}
			for ip in coords.keys():
				if ip not in locations_cache.keys():
					try:
						response = locations_db.city(ip)
						locations_cache[ip] = {
							"lat": response.location.latitude,
							"lng": response.location.longitude,
							"post": response.postal.code
						}
					except (geoip2.errors.AddressNotFoundError, ValueError):
						locations_cache[ip] = {"lat": np.nan, "lng": np.nan, "post": np.nan}
				pass
			coords[ip] = locations_cache[ip]

			# Add data to temporary array, converting ips to numbers
			hops.append([struct.unpack("!L", socket.inet_aton(src))[0], struct.unpack("!L", socket.inet_aton(dst))[0],
						 rtt, coords[src]["lat"], coords[src]["lng"], coords[src]["post"], coords[dst]["lat"],
						 coords[dst]["lng"], coords[dst]["post"]])
locations_db.close()

# Create pandas dataframe and use it to save the data
print("Saving {} hops to sqlite".format(len(hops)))
hops_pd = pd.DataFrame(hops, columns=["src", "dst", "rtt", "src_lat", "src_lng", "src_post", "dst_lat", "dst_lng", "dst_post"])
save_db = sqlite3.connect(sys.argv[3], 3600)
hops_pd.to_sql("hops", save_db, index=False)
