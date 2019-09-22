#!/usr/bin/env python3
import argparse
import json
import socket
import sys

from postgresql import get_postgres_connection

parser = argparse.ArgumentParser(description="Process a scamper-generated multi json and send results to PostgreSQL")
parser.add_argument("dbconfig", type=str, help="DB Config file")
parser.add_argument("input", type=str, help="Multi JSON file to process")
parser.add_argument("--ping", "-p", action="store_true", help="No hop mode, process traceroutes as strict pings only")
parser.add_argument("--direct", "-d", action="store_true", help="Hop mode with direct routes only")
parser.add_argument("--verbose", "-v", action="store_true", help="Verbose output")
args = parser.parse_args()

if args.ping and args.direct:
	print("Ping mode and direct mode are exclusive, you can use only one at once!", file=sys.stderr)
	exit(1)

vprint = print if args.verbose else lambda *x, **y: None

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
			try:
				base_src = str(socket.gethostbyname(traceroute["hostname"]))
				continue
			except Exception:
				print("Failed to get IP for {}, aborting!".format(traceroute["hostname"]), file=sys.stderr)
				exit(1)

		if i % 20000 == 0 and len(hops) != 0:
			vprint("Processed {} traceroutes".format(i))
			args_str = ",".join(("('%s', '%s', %d, %d)" % tuple(hop)) for hop in hops)
			cursor.execute("INSERT INTO hops VALUES " + args_str)
			args_str = ""
			hops.clear()

		if "hops" not in traceroute.keys():
			# Some traceroutes fail but the rest of the file should still be okay
			continue

		# Regular traceroute entry
		time = traceroute["start"]["sec"]
		if args.ping:
			# Ping mode -- just do one entry, source to final destination
			hops.append((base_src, traceroute["dst"], traceroute["hops"][len(traceroute["hops"]) - 1]["rtt"], time))
		else:
			for j, hop in enumerate(traceroute["hops"]):
				if not args.direct:
					src = base_src if j == 0 else traceroute["hops"][j - 1]["addr"]
					rtt = hop["rtt"] if j == 0 else hop["rtt"] - traceroute["hops"][j - 1]["rtt"]
					hops.append((src, hop["addr"], rtt, time))  # Last hop to current hop

				if j != 0:
					hops.append((base_src, hop["addr"], hop["rtt"], time))  # Base source to this hop


vprint("Committing to database...")
if len(hops) != 0:
	args_str = ",".join(("('%s', '%s', %d, %d)" % hop) for hop in hops)
	cursor.execute("INSERT INTO hops VALUES " + args_str)
cursor.commit()
vprint("Committed!")
connection.close()
