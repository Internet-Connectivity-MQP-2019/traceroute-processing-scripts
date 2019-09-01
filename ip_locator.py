#!/usr/bin/python3
import geoip2.database
import json
import sys

"""Usage: ./ip_locator input_file database [output]"""

if len(sys.argv) < 3:
	print("Not enough arguments, needs at least an input file and a database", file=sys.stderr)
	exit(1)

reader = geoip2.database.Reader(sys.argv[2])
locations = []

with open(sys.argv[1], 'r') as ips:
	ips_count = 0
	bad_count = 0
	for ip in ips:
		ips_count = ips_count + 1
		ip = ip.strip("\n")
		try:
			response = reader.city(ip)
			if response.country.name == "United States":
				locations.append({
					"latitude": response.location.latitude,
					"longitude": response.location.longitude
				})
		except (geoip2.errors.AddressNotFoundError, ValueError):
			bad_count = bad_count + 1
			print("Bad IP {}".format(ip), file=sys.stderr)

out = json.dumps(locations, indent=4)
if len(sys.argv) == 4:
	with open(sys.argv[3], "w") as file:
		file.write(out)
	print("Finished processing, retrieved locations for {0}/{1} ({2:.1f}%) of IPs"
			.format(ips_count - bad_count, ips_count, (ips_count - bad_count) * 100/ ips_count))
else:
	print(out)
