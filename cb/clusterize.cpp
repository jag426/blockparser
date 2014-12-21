
// Cluster all addresses based on ownership as proved by multi-input transactions

#include <util.h>
#include <common.h>
#include <errlog.h>
#include <option.h>
#include <rmd160.h>
#include <callback.h>

#include <string.h>

#include <string>
#include <unordered_map>
#include <unordered_set>
#include <vector>

struct Clusterize : public Callback
{
	double time_start;
	double time_last;

	uint64_t nBlocks = 0;
	uint64_t clusterId = 0;
	std::unordered_map<std::string, uint64_t> addr_cluster;
	std::unordered_map<uint64_t, std::unordered_set<std::string>> cluster_addrset;

	std::unordered_set<std::string> tx_inputs;

	virtual void start(const Block *s, const Block *e) {
		info("+--------+-----------+----------+---------+----------+");
		info("| blocks | addresses | clusters |  delta  |   total  |");
		info("+--------+-----------+----------+---------+----------+");
		info("|      0 |         0 |        0 |    -.-- |     0.00 |");
		time_start = time_last = usecs();
	}

	virtual void startInputs(const uint8_t *p) {
		tx_inputs.clear();
	}

	virtual void edge(
			uint64_t      value,
			const uint8_t *upTXHash,
			uint64_t      outputIndex,
			const uint8_t *outputScript,
			uint64_t      outputScriptSize,
			const uint8_t *downTXHash,
			uint64_t      inputIndex,
			const uint8_t *inputScript,
			uint64_t      inputScriptSize
	)
	{
		const std::string output_addr((const char *)outputScript, (size_t) outputScriptSize);
		tx_inputs.insert(output_addr);
	}

	virtual void endInputs(const uint8_t *p)
	{
		std::unordered_set<uint64_t> clusters;
		uint64_t new_cluster = -1;
		std::unordered_set<std::string> new_addrset;

		if (tx_inputs.size() < 2) {
			return;
		}

		for (const auto& tx_input : tx_inputs) {
			new_cluster = addr_cluster[tx_input];
			break;
		}
		
		for (const auto& addr : tx_inputs) {
			clusters.insert(addr_cluster[addr]);
			addr_cluster.erase(addr);
			addr_cluster.insert(std::pair<std::string, uint64_t>(addr, new_cluster));
		}

		for (const auto& cluster : clusters) {
			const auto& addrset = cluster_addrset[cluster];
			new_addrset.insert(addrset.begin(), addrset.end());
			cluster_addrset.erase(cluster);
		}

		cluster_addrset.insert(std::pair<uint64_t, std::unordered_set<std::string>>(new_cluster, new_addrset));
	}

	virtual void endOutput(
			const uint8_t *p,
			uint64_t      value,
			const uint8_t *txHash,
			uint64_t      outputIndex,
			const uint8_t *outputScript,
			uint64_t      outputScriptSize
	)
	{
		const std::string output_addr((const char *)outputScript, (size_t) outputScriptSize);
		if (addr_cluster.count(output_addr) == 0) {
			std::unordered_set<std::string> new_addrset({output_addr});
			addr_cluster.insert(std::pair<std::string, uint64_t>(output_addr, clusterId));
			cluster_addrset.insert(std::pair<uint64_t, std::unordered_set<std::string>>(clusterId, new_addrset));
			clusterId++;
		}
	}

	virtual void endBlock(const Block *b)
	{
		if (++nBlocks % 1000 == 0) {
			double time_new = usecs();
			info("| %6u | %9u | %8u | %7.2f | %8.2f |",
					nBlocks,
					addr_cluster.size(),
					cluster_addrset.size(),
					(time_new - time_last ) / (1000 * 1000),
					(time_new - time_start) / (1000 * 1000));
			time_last = time_new;
		}
	}

	////////// boring stuff below here /////////////
	optparse::OptionParser parser;
	Clusterize()
	{
		parser
			.usage("")
			.version("")
			.description("build clusters of addresses provably "
			             "owned by the same party.")
			.epilog("")
		;
	}
	virtual const char *name() const { return "clusterize"; }
	virtual const optparse::OptionParser *optionParser() const { return &parser; }
	virtual bool needTXHash() const { return true; }
};

static Clusterize clusterize;

