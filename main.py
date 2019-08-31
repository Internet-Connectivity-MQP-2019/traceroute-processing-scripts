import json
import socket

data = []
with open("data/traceroutes.json", "r") as file:
	for line in file:
		data.append(json.loads(line))

print("Loaded data, got {} entries".format(len(data)))

# Format: src: {dst, rtt}
ip_pairs = {}
base_src = ""
for i, entry in enumerate(data):
	# Start entry
	if "hostname" in entry.keys():
		base_src = str(socket.gethostbyname(entry["hostname"]))
		continue

	if i % 5000 == 0:
		print("Processed {} traceroutes".format(i))

	# Regular traceroute entry
	for j, hop in enumerate(entry["hops"]):
		# The source for this entry is either the base source if this is the first hop,
		src = base_src if j == 0 else entry["hops"][j - 1]["addr"]

		# Difference in RTTs should be the RTT between hops
		if src not in ip_pairs.keys():
			ip_pairs[src] = {hop["addr"]: []}
		if hop["addr"] not in ip_pairs[src].keys():
			ip_pairs[src][hop["addr"]] = []
		ip_pairs[src][hop["addr"]].append(hop["rtt"] if j == 0 else hop["rtt"] - entry["hops"][j - 1]["rtt"])

with open("data/processed-traceroutes.json", "w") as file:
	file.write(json.dumps(ip_pairs, indent=4))
print("Finished!")
