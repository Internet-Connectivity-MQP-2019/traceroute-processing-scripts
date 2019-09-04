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
base_src = ""
with open(sys.argv[3], "r") as db_config:
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

		if i % 10000 == 0 and len(hops) != 0:
			print("Processed {} traceroutes".format(i))
			args_str = ",".join(("('%s', '%s', %d, %s, %s, %s, %s)" % tuple(hop)) for hop in hops)
			cursor.execute("INSERT INTO hops VALUES " + args_str)
			hops.clear()

		if "hops" not in traceroute.keys():
			# Some traceroutes fail but the rest of the file should still be okay
			continue

		# Regular traceroute entry
		for j, hop in enumerate(traceroute["hops"]):
			src = base_src if j == 0 else traceroute["hops"][j - 1]["addr"]
			dst = hop["addr"]
			rtt = hop["rtt"] if j == 0 else hop["rtt"] - traceroute["hops"][j - 1]["rtt"]

			# Get IP locations, first by trying the cache, then by trying the locations db
			coords = {src: {"lat": "NULL", "lng": "NULL"}, dst: {"lat": "NULL", "lng": "NULL"}}
			for ip in coords.keys():
				if ip not in locations_cache.keys():
					try:
						response = locations_db.city(ip)
						locations_cache[ip] = {
							"lat": response.location.latitude if response.location.latitude is not None else "NULL",
							"lng": response.location.longitude if response.location.longitude is not None else "NULL"
						}
					except (geoip2.errors.AddressNotFoundError, ValueError):
						locations_cache[ip] = {"lat": "NULL", "lng": "NULL"}
				coords[ip] = locations_cache[ip]

			# Insert hop data into database
			hops.append([src, dst, rtt, coords[src]["lat"], coords[src]["lng"], coords[dst]["lat"],
										 coords[dst]["lng"]])

			# Consider the RTT for the source to the destination too, not just the hop
			if j != 0:
				hops.append([base_src, dst, hop["rtt"], locations_cache[base_src]["lat"],
								locations_cache[base_src]["lng"], coords[dst]["lat"], coords[dst]["lng"]])

print("Committing to database...")
if len(hops) != 0:
	args_str = ",".join(("('%s', '%s', %d, %s, %s, %s, %s)" % tuple(hop)) for hop in hops)
	cursor.execute("INSERT INTO hops VALUES " + args_str)
cursor.commit()
print("Committed!")
connection.close()
locations_db.close()
