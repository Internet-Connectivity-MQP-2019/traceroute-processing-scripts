#!/usr/bin/python3
import argparse
import json
import socket

from postgresql import get_postgres_connection

parser = argparse.ArgumentParser(description="Process a scamper-generated multi json and send results to PostgreSQL")
parser.add_argument("dbconfig", type=str, help="DB Config file")
parser.add_argument("input", type=str, help="Multi JSON file to process")
parser.add_argument("--ping", "-p", action="store_true", help="No hop mode, process traceroutes as strict pings only")
args = parser.parse_args()

base_src = ""
with open(args.dbconfig, "r") as db_config:
	connection = get_postgres_connection(db_config)
cursor = connection.cursor()
hops = []
with open(args.input, "r") as file:
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
		if args.ping:
			# Ping mode -- just do one entry
			hops.append((base_src, traceroute["dst"], traceroute["hops"][len(traceroute["hops"]) - 1]["rtt"]))
		else:
			# Perform hop processing
			for j, hop in enumerate(traceroute["hops"]):
				src = base_src if j == 0 else traceroute["hops"][j - 1]["addr"]
				rtt = hop["rtt"] if j == 0 else hop["rtt"] - traceroute["hops"][j - 1]["rtt"]

				# Insert hop data into database
				hops.append((src, hop["addr"], rtt))


print("Committing to database...")
if len(hops) != 0:
	args_str = ",".join(("('%s', '%s', %d)" % hop) for hop in hops)
	cursor.execute("INSERT INTO hops VALUES " + args_str)
cursor.commit()
print("Committed!")
connection.close()
