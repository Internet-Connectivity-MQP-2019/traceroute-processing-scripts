#!/usr/bin/env python3
import argparse
import json
import socket
import sys
from statistics import mean

from postgresql import get_postgres_connection

parser = argparse.ArgumentParser(description="Process a scamper-generated multi json and send results to PostgreSQL")
parser.add_argument("dbconfig", type=str, help="DB Config file")
parser.add_argument("input", type=str, help="Multi JSON file to process")
parser.add_argument("--ping", "-p", action="store_true", help="No hop mode, process traceroutes as strict pings only")
parser.add_argument("--direct", "-d", action="store_true", help="Hop mode with direct routes only")
parser.add_argument("--verbose", "-v", action="store_true", help="Verbose output")
parser.add_argument("--atlas", "-a", action="store_true", help="Atlas traceroute parsing mode")
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
		if not args.atlas and "hostname" in traceroute.keys():
			try:
				base_src = str(socket.gethostbyname(traceroute["hostname"]))
				continue
			except Exception:
				print("Failed to get IP for {}, aborting!".format(traceroute["hostname"]), file=sys.stderr)
				exit(1)
		elif args.atlas:
			base_src = traceroute["from"]
			if len(base_src) == 0:
				continue

		# Dump saved buffer of hops into DB, but don't commit.
		if i % 10000 == 0 and len(hops) != 0:
			args_str = ",".join(("('%s', '%s', %f, %d, '%s')" % hop) for hop in hops)
			cursor.execute("INSERT INTO hops VALUES " + args_str)
			vprint("Processed {} traceroutes".format(i))
			args_str = ""
			hops.clear()

		if not args.atlas and "hops" not in traceroute.keys():
			# Some CAIDA traceroutes fail but the rest of the file should still be okay
			continue

		if args.atlas:
			time = traceroute["timestamp"]
		else:
			time = traceroute["start"]["sec"]

		if args.ping:
			# Ping mode -- just do one entry, source to final destination
			if args.atlas:
				# Atlas traceroutes several entries per hop, but we just average those together.
				hops_count = len(traceroute["result"])
				result = mean([result["rtt"] for result in traceroute["result"][hops_count - 1]])
				hops.append((base_src, traceroute["dst_addr"], result, time, 0))
			else:
				hops.append((base_src, traceroute["dst"], traceroute["hops"][len(traceroute["hops"]) - 1]["rtt"], time, 0))
		else:
			# Either direct or full hopping node
			for j, hop in enumerate(traceroute["hops"] if not args.atlas else traceroute["result"]):

				# Verification and preprocessing for Atlas data
				if args.atlas:
					# Atlas is *weird*. Some results don't have RTTs!
					if "result" not in hop.keys():
						continue
					hop["result"] = list(filter(lambda r: "rtt" in r.keys(), hop["result"]))
					if len(hop["result"]) == 0:
						continue
					last_hop = traceroute["result"][j - 1]
					curr_mean = mean([result["rtt"] for result in hop["result"]])  # Also used later

				# Add a hop for the direct source->hop entry
				if args.atlas:
					hops.append((base_src, hop["result"][0]["from"], curr_mean, time, 'f'))
				else:
					hops.append((base_src, hop["addr"], hop["rtt"], time, 'f'))  # Base source to this hop

				# Full hopping mode includes calculations for RTT between individual hops
				if not args.direct and j != 0:
					if args.atlas:
						if len(last_hop["result"]) == 0:
							continue
						src = base_src if j == 0 else last_hop["result"][0]["from"]
						rtt = curr_mean if j == 0 else curr_mean - mean([result["rtt"] for result in last_hop["result"]])
						dst = hop["result"][0]["from"]  # Assume there will always be at least one entry
					else:
						last_hop = traceroute["hops"][j - 1]
						src = base_src if j == 0 else last_hop["addr"]
						rtt = hop["rtt"] if j == 0 else hop["rtt"] - last_hop["rtt"]
						dst = hop["addr"]
					hops.append((src, dst, rtt, time, 't'))  # Last hop to current hop

vprint("Committing to database...")
if len(hops) != 0:
	args_str = ",".join(("('%s', '%s', %f, %d, '%s')" % hop) for hop in hops)
	cursor.execute("INSERT INTO hops VALUES " + args_str)
cursor.commit()
vprint("Committed!")
connection.close()
