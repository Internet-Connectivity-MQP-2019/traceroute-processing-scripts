#!/usr/bin/python3
import argparse
import sys

from postgresql import get_postgres_connection
import geoip2.database


parser = argparse.ArgumentParser(description= "Update IP address locations in PostgreSQL. Assumes that you have the IPs"
											  "already entered in the database.")
parser.add_argument("config", type=str, help="DB config file")
parser.add_argument("geoip_db", type=str, help="GeoIP2 database")
args = parser.parse_args()

with open(args.config, "r") as db_config:
	connection = get_postgres_connection(db_config)
cursor = connection.cursor()
geoip_reader = geoip2.database.Reader(args.geoip_db)

# Fetch list of IPs that need processing -- these are IPs with null lat/lngs, the IPs that
# don't have corresponding locations have lat/lng set to NaN
cursor.execute("SELECT ip FROM locations WHERE coord IS NULL")
ips = [result[0] for result in cursor.fetchall()]
if len(ips) == 0:
	print("Didn't retrieve any IPs that need updating, quitting!")
	sys.exit(0)
print("Retrieved {} IPs, beginning geolocation".format(len(ips)))

locations = []
bad_count = 0
for i, ip in enumerate(ips):
	if i % 5000 == 0 and i != 0:
		print("Located {0} ({1:.1f}%) IPs...".format(i, i * 100 / len(ips)))
	try:
		response = geoip_reader.city(ip)
		if response.location.latitude is None or response.location.longitude is None:
			raise ValueError
		else:
			locations.append((response.location.latitude, response.location.longitude, ip))
	except (geoip2.errors.AddressNotFoundError, ValueError):
		bad_count = bad_count + 1
		locations.append((float('nan'), float('nan'), ip))
print("Located {} total IPs ({} were unlocatable), proceeding to database update".format(len(ips) - bad_count, bad_count))
del ips  # Free up some memory

for i, loc in enumerate(locations):
	if i % 5000 == 0 and i != 0:
		print("Updated {0} ({1:.1f}%) IPs...".format(i, i * 100 / len(locations)))
	cursor.execute("UPDATE locations SET coord=POINT(?, ?) WHERE ip = ?", loc)
cursor.commit()
print("Complete! Updated locations for {} IPs".format(len(locations)))
