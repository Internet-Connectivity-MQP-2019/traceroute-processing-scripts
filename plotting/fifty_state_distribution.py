import bootstrapped.bootstrap as bs
import bootstrapped.stats_functions as bs_stats
import numpy as np
import pandas as pd
import seaborn as sns
from matplotlib import pyplot as plt
from matplotlib.axes import Axes
# From https://gist.github.com/rogerallen/1583593
from scipy.stats import stats

us_state_abbrev = {
	'Alabama': 'AL',
	'Alaska': 'AK',
	'Arizona': 'AZ',
	'Arkansas': 'AR',
	'California': 'CA',
	'Colorado': 'CO',
	'Connecticut': 'CT',
	'Delaware': 'DE',
	'District of Columbia': 'DC',
	'Florida': 'FL',
	'Georgia': 'GA',
	'Hawaii': 'HI',
	'Idaho': 'ID',
	'Illinois': 'IL',
	'Indiana': 'IN',
	'Iowa': 'IA',
	'Kansas': 'KS',
	'Kentucky': 'KY',
	'Louisiana': 'LA',
	'Maine': 'ME',
	'Maryland': 'MD',
	'Massachusetts': 'MA',
	'Michigan': 'MI',
	'Minnesota': 'MN',
	'Mississippi': 'MS',
	'Missouri': 'MO',
	'Montana': 'MT',
	'Nebraska': 'NE',
	'Nevada': 'NV',
	'New Hampshire': 'NH',
	'New Jersey': 'NJ',
	'New Mexico': 'NM',
	'New York': 'NY',
	'North Carolina': 'NC',
	'North Dakota': 'ND',
	'Ohio': 'OH',
	'Oklahoma': 'OK',
	'Oregon': 'OR',
	'Pennsylvania': 'PA',
	'Rhode Island': 'RI',
	'South Carolina': 'SC',
	'South Dakota': 'SD',
	'Tennessee': 'TN',
	'Texas': 'TX',
	'Utah': 'UT',
	'Vermont': 'VT',
	'Virginia': 'VA',
	'Washington': 'WA',
	'West Virginia': 'WV',
	'Wisconsin': 'WI',
	'Wyoming': 'WY',
}


def plot_kde(data, xlabel, title):
	sns.set_style("darkgrid")
	for sr in data.values():
		sr = sr[(np.abs(stats.zscore(sr) <= 2.0)) & (sr > 0)]
		if sr.max() > 1.0:
			sr = sr / sr.max()
		sr = 1 - sr

		plot: Axes = sns.kdeplot(data=sr,
								 shade=True,
								 kernel="gau",
								 clip=[0.0, 1.0],
								 legend=True)

	plot.set_title(title, fontdict={"size": 14})
	plot.set_xlabel(xlabel)
	plot.set_ylabel("Density")


def plot_confidence_interval(data: pd.DataFrame, xlabel, ylabel, title):
	sns.set_style("darkgrid")
	plot: Axes = sns.pointplot(data=data, capsize=0.2, linestyles=":", scale=0.5, )
	plot.set_ylabel(ylabel)
	plot.set_xlabel(xlabel)
	plot.set_title(title)
	plot.set_xticklabels(plot.get_xticklabels(), rotation=90, fontdict={"size": 8})
	plt.show()


df_caida: pd.DataFrame = pd.read_pickle("hops_aggregate_us_state.pkl")\
	.drop(["lng", "lat"], axis=1)
df_caida["state"] = df_caida["state"].map(us_state_abbrev)
df_caida = df_caida[(df_caida["frac_c_efficiency"] <= 1.0) & (df_caida["frac_c_efficiency"] > 0.0)]

df_siteping: pd.DataFrame = pd.read_pickle("siteping.pkl")\
	.drop(["latitude", "longitude", "country"], axis=1)
df_siteping["state"] = df_siteping["state"].map(us_state_abbrev)
df_siteping = df_siteping[df_siteping["rtt"] > 0]

df_dns: pd.DataFrame = pd.read_pickle("dns_rtt_2_1.0_pairs_pickle.pkl")
df_dns = df_dns.droplevel([1, 2, 3]).reset_index().rename({"recursive_state": "state", "median": "rtt"}, axis=1)

# For each data source, calculate bootstrap confidence intervals
df_caida_bs = pd.DataFrame({
    state: bs.bootstrap(df_caida[df_caida["state"] == state]["frac_c_efficiency"].to_numpy(), stat_func=bs_stats.mean).__dict__
    for state in us_state_abbrev.values()
}).sort_values("value", axis=1)
plot_confidence_interval(df_caida_bs, xlabel="State", ylabel="RTT",
                         title="CAIDA + Atlas State Confidence Intervals")

df_dns_bs = pd.DataFrame({
    state: bs.bootstrap(df_dns[df_dns["state"] == state]["rtt"].to_numpy(), stat_func=bs_stats.mean).__dict__
    for state in us_state_abbrev.values()
}).sort_values("value", axis=1, ascending=False)
plot_confidence_interval(df_dns_bs, xlabel="State", ylabel="Speed-of-light Efficiency",
                         title="DNS State Confidence Interval")

df_siteping_bs = pd.DataFrame({
    state: bs.bootstrap(df_siteping[df_siteping["state"] == state]["rtt"].to_numpy(), stat_func=bs_stats.mean).__dict__
    for state in us_state_abbrev.values()
}).sort_values("value", axis=1, ascending=False)
plot_confidence_interval(df_siteping_bs, xlabel="State", ylabel="RTT",
                         title="Site Ping State Confidence Intervals")


# Assemble state values list for use with KDE plotting
state_vals = {}
for state in us_state_abbrev.values():
	state_vals[state] = {
		"caida": 1 - df_caida[df_caida["state"] == state]["frac_c_efficiency"].rename("CAIDA"),
		"siteping": df_siteping[df_siteping["state"] == state]["rtt"].rename("Site Ping"),
		"dns": df_dns[df_dns["state"] == state]["rtt"].rename("DNS")
	}

	plot_kde(state_vals[state], "Quality value", "{} State Distribution".format(state))
	plt.savefig("../charts/state_dists/{}_dist.png".format(state))
	plt.clf()


