# Hedera Analytics

Analytics tools for Hedera Hashgraph — staking profiler, data refinery, and Prolog facts.

## Components

### Hedera Staking Profiler
Live Hedera staking/restaking destination profiler for the Orkid Settlement Engine.

Aggregates public, free-tier data sources to rank the highest-performing Hedera destinations:

1. Native Hedera node staking (via Hedera Mirror Node `/api/v1/network/nodes` and `/api/v1/network/stake`)
2. Liquid staking via Stader HBARX (token and staking contract state from the Hedera Mirror Node)
3. DeFi LP restaking via SaucerSwap V1/V2 pools (via the SaucerSwap REST API)

Outputs a ranked `hedera_yield_matrix.json` consumed by backend APIs.

### Prolog Facts
Prolog knowledge base for Hedera ledger facts and social research.

- `hedera_facts.pl` — Hedera ledger facts (account associations, consensus timestamps)
- `refinery_hedera_facts.pl` — Data refinery output for Hedera-specific facts

## Usage

### Staking Profiler
```bash
cd analytics
python hedera_staking_profiler.py
```

Environment variables:
- `HEDERA_NETWORK` — mainnet | testnet (default: mainnet)
- `SAUCERSWAP_API_KEY` — Optional production key
- `DUNE_API_KEY` — Optional Dune Analytics key
- `COINGECKO_API_KEY` — Optional free CoinGecko key
- `HBAR_PRICE_USD` — Optional manual override for HBAR price

### Prolog Facts
```bash
cd prolog
swipl -s hedera_facts.pl
```

## License

MIT

---

## Part of Orkid Labs

**hedera-analytics** — Hedera Hashgraph analytics toolkit — staking profiler, data refinery, and Prolog fact extraction.

- Install: `pip install hedera-analytics`
- Package: https://pypi.org/project/hedera-analytics
- API docs: https://docs.orkidlabs.xyz/packages/hedera-analytics
- Docs: https://docs.orkidlabs.xyz/packages/hedera-analytics

Related Orkid projects: [hedera-utils](https://github.com/orkid-labs/hedera-utils), [settlement-types](https://github.com/orkid-labs/settlement-types)

Orkid Labs builds gasless, MEV-protected swap routing and agent-native DeFi infrastructure — [orkidlabs.xyz](https://orkidlabs.xyz) · [@orkid-labs on npm](https://www.npmjs.com/org/orkid-labs) · [github.com/orkid-labs](https://github.com/orkid-labs)
