import argparse
import csv

from postgresql import get_postgres_connection

parser = argparse.ArgumentParser(description="Insert IPs + times from one of Sam's DNS csv files")
parser.add_argument("config", type=str, help="DB config file")
parser.add_argument("csv", type=str, help="Input CSV file")
args = parser.parse_args()

with open(args.config, "r") as db_config:
	connection = get_postgres_connection(db_config)
cursor = connection.cursor()

hops = []
with open(args.csv, "r") as file:
	reader = csv.reader(file)
	for line in reader:
		hops.append((line[1], line[3], float(line[4])))

args_str = ",".join(("('%s', '%s', %f)" % hop) for hop in hops)
cursor.execute("INSERT INTO hops VALUES " + args_str)
cursor.commit()
connection.close()
