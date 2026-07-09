#!/usr/bin/env python3
"""
hedera_staking_profiler.py

Live Hedera staking/restaking destination profiler for the Orkid Settlement Engine.

This script aggregates public, free-tier data sources to rank the highest-performing
Hedera destinations that are compatible with a native `CryptoTransfer` execution flow:

  1. Native Hedera node staking (via Hedera Mirror Node `/api/v1/network/nodes` and
     `/api/v1/network/stake`).
  2. Liquid staking via Stader HBARX (token and staking contract state from the
     Hedera Mirror Node).
  3. DeFi LP restaking via SaucerSwap V1/V2 pools (via the SaucerSwap REST API).

Outputs a ranked `hedera_yield_matrix.json` consumed by the Axum/Tower backend
at `POST /api/v1/settle` to inject the optimal target address into the Hiero Hook
calldata (HIP-1195).

Environment variables:
  HEDERA_NETWORK           mainnet | testnet  (default: mainnet)
  SAUCERSWAP_API_KEY       Optional production key. Falls back to the public demo key.
  DUNE_API_KEY             Optional Dune Analytics key. When provided, the profiler
                           pulls on-chain SaucerSwap V1 pool data directly from Dune
                           instead of the SaucerSwap REST API.
  DUNE_QUERY_TIMEOUT       Max seconds to wait for a Dune query execution (default 120).
  COINGECKO_API_KEY        Optional free CoinGecko key (pro not required).
  HBAR_PRICE_USD           Optional manual override for HBAR price.
  STADER_FEE_FRACTION      Protocol fee estimate (default 0.10).
  OUTPUT_PATH              Override for the generated JSON file.
"""

from __future__ import annotations

import json
import os
import sys
import time
from dataclasses import asdict, dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple
from urllib.parse import urljoin

import requests

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

HBAR_DECIMALS = 8
TINYBAR = 10 ** HBAR_DECIMALS

HEDERA_NETWORKS = {
    "mainnet": {
        "mirror_node": "https://mainnet-public.mirrornode.hedera.com",
        "json_rpc": "https://mainnet.hashio.io/api",
    },
    "testnet": {
        "mirror_node": "https://testnet.mirrornode.hedera.com",
        "json_rpc": "https://testnet.hashio.io/api",
    },
}

STADER_MAINNET = {
    "hbarx_token_id": "0.0.834116",
    "staking_contract_id": "0.0.1027588",
    "rewards_contract_id": "0.0.834120",
    "undelegation_contract_id": "0.0.1027587",
    "treasury_account_id": "0.0.1412503",
}

SAUCERSWAP_MAINNET = {
    "v1_factory": "0.0.1062784",
    "v2_factory": "0.0.3946833",
    "whbar_new_token_id": "0.0.1456986",
    "whbar_new_contract_id": "0.0.1456985",
    "api_base": "https://api.saucerswap.finance",
    "demo_api_key": "875e1017-87b8-4b12-8301-6aa1f1aa073b",
}

TARGET_TOKEN_IDS = {
    "hbarx": STADER_MAINNET["hbarx_token_id"],
    "whbar_new": SAUCERSWAP_MAINNET["whbar_new_token_id"],
}

FALLBACKS = {
    "hbar_price_usd": 0.05,
    "native_apy": 0.015,
    "stader_hbarx_apy": 0.035,
    "stader_hbarx_exchange_rate": 1.08,
    "max_slippage_bps": 50,
}

# DuneSQL query used when DUNE_API_KEY is provided. It pulls the latest
# SaucerSwap V1 pools that contain HBARX or WHBAR, together with 7-day
# swap volume and the most recent reserve sync.
DUNE_SAUCEERSWAP_HBARX_QUERY = """
WITH target_tokens AS (
  SELECT
    0x00000000000000000000000000000000000cba44 AS token,
    'HBARX' AS symbol
  UNION ALL
  SELECT
    0x0000000000000000000000000000000000163b5a,
    'WHBAR'
),
pool_tokens AS (
  SELECT DISTINCT
    t0.contract_address,
    t0.output_0 AS token0,
    t1.output_0 AS token1
  FROM saucer_swap_hedera.pool_call_token0 t0
  JOIN saucer_swap_hedera.pool_call_token1 t1
    ON t0.contract_address = t1.contract_address
  WHERE (
    t0.output_0 IN (SELECT token FROM target_tokens)
    OR t1.output_0 IN (SELECT token FROM target_tokens)
  )
),
swap_volume AS (
  SELECT
    contract_address,
    COUNT(*) AS swap_count_7d,
    SUM(CAST(amount0In AS DOUBLE) + CAST(amount0Out AS DOUBLE)) AS volume0_7d,
    SUM(CAST(amount1In AS DOUBLE) + CAST(amount1Out AS DOUBLE)) AS volume1_7d
  FROM saucer_swap_hedera.pool_evt_swap
  WHERE evt_block_date >= CURRENT_DATE - INTERVAL '7' DAY
  GROUP BY contract_address
),
latest_sync AS (
  SELECT
    contract_address,
    reserve0,
    reserve1,
    evt_block_time,
    ROW_NUMBER() OVER (PARTITION BY contract_address ORDER BY evt_block_time DESC) AS rn
  FROM saucer_swap_hedera.pool_evt_sync
  WHERE evt_block_date >= CURRENT_DATE - INTERVAL '30' DAY
)
SELECT
  pt.contract_address,
  pt.token0,
  pt.token1,
  COALESCE(sv.swap_count_7d, 0) AS swap_count_7d,
  COALESCE(sv.volume0_7d, 0) AS volume0_7d,
  COALESCE(sv.volume1_7d, 0) AS volume1_7d,
  ls.reserve0,
  ls.reserve1,
  ls.evt_block_time AS last_sync_time
FROM pool_tokens pt
LEFT JOIN swap_volume sv ON pt.contract_address = sv.contract_address
LEFT JOIN latest_sync ls ON pt.contract_address = ls.contract_address AND ls.rn = 1
ORDER BY (COALESCE(sv.volume0_7d, 0) + COALESCE(sv.volume1_7d, 0)) DESC
LIMIT 50
"""


# ---------------------------------------------------------------------------
# Data models
# ---------------------------------------------------------------------------

@dataclass
class HederaNode:
    node_id: int
    node_account_id: str
    description: str
    stake_total: float
    stake_rewarded: float
    stake_not_rewarded: float
    min_stake: float
    max_stake: float
    cap_utilization: float
    reward_rate_start: int
    staking_period_start: Optional[str]


@dataclass
class NativeStakingProfile:
    network_apy: float
    reward_rate_per_hbar_per_day: float
    stake_total_hbar: float
    max_stake_rewarded_hbar: float
    reward_balance_threshold_hbar: float
    unreserved_reward_balance_hbar: float
    nodes: List[Dict[str, Any]] = field(default_factory=list)


@dataclass
class StaderProfile:
    token_id: str
    staking_contract_id: str
    total_supply: float
    treasury_hbar: float
    contract_hbar: float
    exchange_rate: float
    implied_apy: float
    fee_fraction: float
    exit_liquidity_score: float


@dataclass
class SaucerSwapPool:
    pool_id: int
    version: int
    contract_id: str
    token_a_id: str
    token_a_symbol: str
    token_b_id: str
    token_b_symbol: str
    tvl_usd: float
    volume_24h_usd: float
    fee_bps: int
    estimated_fee_apy: float
    estimated_total_apy: float
    liquidity_depth_score: float
    contains_target: bool


@dataclass
class RankedTarget:
    rank: int
    strategy: str
    category: str
    target_address: str
    target_type: str
    net_apy: float
    risk_score: float
    slippage_bps: int
    max_allocation_hbar: float
    execution_flow: str
    requires_swap: bool
    memo_payload: str
    metadata: Dict[str, Any]


# ---------------------------------------------------------------------------
# HTTP client
# ---------------------------------------------------------------------------

class HederaHTTPClient:
    def __init__(self, total_timeout: int = 30, max_retries: int = 3):
        self.total_timeout = total_timeout
        self.max_retries = max_retries
        self.session = requests.Session()
        self.session.headers.update({
            "Accept": "application/json",
            "User-Agent": "orkid-hedera-profiler/1.0",
        })

    def get(self, url: str, params: Optional[Dict[str, Any]] = None, headers: Optional[Dict[str, str]] = None) -> Optional[Any]:
        params = params or {}
        headers = headers or {}
        last_exception: Optional[Exception] = None

        for attempt in range(self.max_retries):
            try:
                response = self.session.get(url, params=params, headers=headers, timeout=self.total_timeout)
                if response.status_code == 429:
                    last_exception = requests.exceptions.RequestException(f"rate limited (429) on attempt {attempt + 1}")
                    sleep_s = 2 ** attempt + 0.5
                    time.sleep(sleep_s)
                    continue
                response.raise_for_status()
                return response.json()
            except requests.exceptions.Timeout as exc:
                last_exception = exc
                time.sleep(2 ** attempt)
            except requests.exceptions.RequestException as exc:
                last_exception = exc
                time.sleep(2 ** attempt)

        print(f"[WARN] HTTP GET failed: {url} -> {last_exception}", file=sys.stderr)
        return None

    def paginate(self, url: str, params: Optional[Dict[str, Any]] = None, headers: Optional[Dict[str, str]] = None) -> List[Dict[str, Any]]:
        results: List[Dict[str, Any]] = []
        params = dict(params or {})
        headers = headers or {}
        next_url: Optional[str] = url

        while next_url and len(results) < 10_000:
            data = self.get(next_url, params=params, headers=headers)
            if not data:
                break

            if isinstance(data, list):
                results.extend(data)
                break

            items = (
                data.get("nodes")
                or data.get("contracts")
                or data.get("tokens")
                or data.get("transactions")
                or data.get("balances")
                or data.get("result", [])
                or data.get("data", [])
                or []
            )
            results.extend(items)

            link = data.get("links", {}).get("next") if isinstance(data, dict) else None
            if not link:
                break
            next_url = urljoin(url, link)
            params = {}

        return results


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def tinybar_to_hbar(tinybar: int) -> float:
    return float(tinybar) / TINYBAR


def safe_get(data: Dict[str, Any], *keys: str, default: Any = None) -> Any:
    for key in keys:
        if not isinstance(data, dict):
            return default
        data = data.get(key, default)
    return data


def compute_native_apy(stake_info: Dict[str, Any]) -> Tuple[float, float]:
    """Apply HIP-782 smoothing to compute the effective daily reward per HBAR and annual APY."""
    unreserved_balance = int(stake_info.get("unreserved_staking_reward_balance", 0) or 0)
    reward_balance_threshold = int(stake_info.get("reward_balance_threshold", 1) or 1)
    max_reward_rate_per_hbar = int(stake_info.get("max_staking_reward_rate_per_hbar", 0) or 0)
    max_stake_rewarded = int(stake_info.get("max_stake_rewarded", 1) or 1)
    stake_total = int(stake_info.get("stake_total", 1) or 1)

    if reward_balance_threshold <= 0 or max_stake_rewarded <= 0 or stake_total <= 0:
        return 0.0, FALLBACKS["native_apy"]

    balance_ratio = min(unreserved_balance, reward_balance_threshold) / reward_balance_threshold
    balance_ratio = max(0.0, min(1.0, balance_ratio))

    stake_ratio = min(1.0, max_stake_rewarded / stake_total)
    effective_rate = max_reward_rate_per_hbar * balance_ratio * (2.0 - balance_ratio) * stake_ratio

    daily_return = effective_rate / TINYBAR
    annual_apy = daily_return * 365.0
    return effective_rate, annual_apy


def estimate_pool_apy(tvl_usd: float, volume_24h_usd: float, fee_bps: int) -> float:
    if tvl_usd <= 0 or volume_24h_usd <= 0 or fee_bps <= 0:
        return 0.0
    daily_fee = volume_24h_usd * (fee_bps / 10_000)
    return (daily_fee / tvl_usd) * 365.0


def liquidity_depth_score(tvl_usd: float, volume_24h_usd: float) -> float:
    if tvl_usd <= 0:
        return 0.0
    turnover = volume_24h_usd / tvl_usd
    tvl_score = min(1.0, tvl_usd / 1_000_000.0)
    turnover_score = min(1.0, turnover / 0.10)
    return round(0.6 * tvl_score + 0.4 * turnover_score, 4)


def risk_score_for_category(category: str) -> float:
    mapping = {
        "native_staking": 1.0,
        "liquid_staking": 2.0,
        "dex_lp_v1": 3.0,
        "dex_lp_v2": 3.5,
    }
    return mapping.get(category, 4.0)


def slippage_bps_for_category(category: str) -> int:
    mapping = {
        "native_staking": 0,
        "liquid_staking": 10,
        "dex_lp_v1": 30,
        "dex_lp_v2": 25,
    }
    return mapping.get(category, FALLBACKS["max_slippage_bps"])


# ---------------------------------------------------------------------------
# Dune client (optional SaucerSwap on-chain source)
# ---------------------------------------------------------------------------

class DuneClient:
    """Minimal Dune Analytics API client that creates a temporary query and
    polls for execution results. Requires a Dune API key (free tier works)."""

    def __init__(self, api_key: str, timeout: int = 120):
        self.api_key = api_key
        self.timeout = timeout
        self.session = requests.Session()
        self.session.headers.update({
            "X-DUNE-API-KEY": api_key,
            "Accept": "application/json",
            "Content-Type": "application/json",
        })

    def _request(self, method: str, path: str, **kwargs) -> Optional[Any]:
        url = f"https://api.dune.com/api/v1{path}"
        resp = None
        try:
            resp = self.session.request(method, url, timeout=30, **kwargs)
            resp.raise_for_status()
            return resp.json()
        except requests.exceptions.RequestException as exc:
            detail = ""
            try:
                detail = resp.text[:500] if resp is not None else ""
            except Exception:
                pass
            print(f"[WARN] Dune API {method} {path} failed: {exc} {detail}", file=sys.stderr)
            return None

    def create_query(self, name: str, query_sql: str, description: str = "") -> Optional[int]:
        payload = {
            "name": name,
            "query_sql": query_sql,
        }
        if description:
            payload["description"] = description
        data = self._request("POST", "/query", json=payload)
        if data and "query_id" in data:
            return int(data["query_id"])
        return None

    def execute_query(self, query_id: int) -> Optional[str]:
        data = self._request("POST", f"/query/{query_id}/execute")
        if data and "execution_id" in data:
            return str(data["execution_id"])
        return None

    def get_execution_status(self, execution_id: str) -> Optional[str]:
        data = self._request("GET", f"/execution/{execution_id}/status")
        if data:
            return str(data.get("state", "")).upper()
        return None

    def get_execution_results(self, execution_id: str) -> Optional[List[Dict[str, Any]]]:
        data = self._request("GET", f"/execution/{execution_id}/results")
        if not data:
            return None
        result = data.get("result", {})
        rows = result.get("rows")
        if rows is None:
            metadata = data.get("result_metadata", {})
            rows = data.get("rows", [])
            if not rows and "data" in data:
                rows = data["data"].get("rows", [])
        return rows

    def run_query(self, name: str, query_sql: str, description: str = "") -> Optional[List[Dict[str, Any]]]:
        """Create, execute and poll a Dune query. Returns rows or None."""
        query_id = self.create_query(name, query_sql, description)
        if not query_id:
            return None

        execution_id = self.execute_query(query_id)
        if not execution_id:
            return None

        deadline = time.time() + self.timeout
        while time.time() < deadline:
            status = self.get_execution_status(execution_id)
            if status == "QUERY_STATE_COMPLETED":
                return self.get_execution_results(execution_id)
            if status in {"QUERY_STATE_FAILED", "QUERY_STATE_CANCELLED", "QUERY_STATE_EXECUTING_FAILED"}:
                print(f"[WARN] Dune execution {execution_id} ended with state {status}", file=sys.stderr)
                return None
            time.sleep(3.0)

        print(f"[WARN] Dune execution {execution_id} timed out after {self.timeout}s", file=sys.stderr)
        return None


# ---------------------------------------------------------------------------
# Data fetchers
# ---------------------------------------------------------------------------

class HederaProfiler:
    def __init__(self, network: str = "mainnet", http: Optional[HederaHTTPClient] = None):
        self.network = network
        self.cfg = HEDERA_NETWORKS.get(network, HEDERA_NETWORKS["mainnet"])
        self.http = http or HederaHTTPClient()

    # ------------------------------------------------------------------
    # Native staking
    # ------------------------------------------------------------------
    def fetch_network_stake(self) -> Optional[NativeStakingProfile]:
        url = f"{self.cfg['mirror_node']}/api/v1/network/stake"
        data = self.http.get(url)
        if not data:
            return None

        try:
            effective_rate, annual_apy = compute_native_apy(data)
            return NativeStakingProfile(
                network_apy=annual_apy,
                reward_rate_per_hbar_per_day=effective_rate,
                stake_total_hbar=tinybar_to_hbar(int(data.get("stake_total", 0))),
                max_stake_rewarded_hbar=tinybar_to_hbar(int(data.get("max_stake_rewarded", 0))),
                reward_balance_threshold_hbar=tinybar_to_hbar(int(data.get("reward_balance_threshold", 0))),
                unreserved_reward_balance_hbar=tinybar_to_hbar(int(data.get("unreserved_staking_reward_balance", 0))),
            )
        except Exception as exc:
            print(f"[WARN] Failed to parse network stake: {exc}", file=sys.stderr)
            return None

    def fetch_nodes(self) -> List[HederaNode]:
        url = f"{self.cfg['mirror_node']}/api/v1/network/nodes"
        nodes = self.http.paginate(url, params={"limit": 100})
        result: List[HederaNode] = []
        for node in nodes:
            try:
                stake_total = tinybar_to_hbar(int(node.get("stake", 0)))
                stake_rewarded = tinybar_to_hbar(int(node.get("stake_rewarded", 0)))
                stake_not_rewarded = tinybar_to_hbar(int(node.get("stake_not_rewarded", 0)))
                max_stake = tinybar_to_hbar(int(node.get("max_stake", 1)))
                min_stake = tinybar_to_hbar(int(node.get("min_stake", 0)))
                cap_util = stake_total / max_stake if max_stake > 0 else 0.0

                staking_period = safe_get(node, "staking_period", "from")
                result.append(
                    HederaNode(
                        node_id=int(node.get("node_id", -1)),
                        node_account_id=str(node.get("node_account_id", "")),
                        description=str(node.get("description", "")),
                        stake_total=stake_total,
                        stake_rewarded=stake_rewarded,
                        stake_not_rewarded=stake_not_rewarded,
                        min_stake=min_stake,
                        max_stake=max_stake,
                        cap_utilization=min(1.0, cap_util),
                        reward_rate_start=int(node.get("reward_rate_start", 0) or 0),
                        staking_period_start=str(staking_period) if staking_period else None,
                    )
                )
            except Exception as exc:
                print(f"[WARN] Skipping malformed node: {exc}", file=sys.stderr)
                continue
        return result

    # ------------------------------------------------------------------
    # Stader HBARX
    # ------------------------------------------------------------------
    def fetch_stader_profile(self, native_apy: float) -> Optional[StaderProfile]:
        token_id = STADER_MAINNET["hbarx_token_id"]
        staking_contract = STADER_MAINNET["staking_contract_id"]
        treasury = STADER_MAINNET["treasury_account_id"]
        fee_fraction = float(os.environ.get("STADER_FEE_FRACTION", "0.10"))

        try:
            token_data = self.http.get(f"{self.cfg['mirror_node']}/api/v1/tokens/{token_id}")
            supply_raw = int(token_data.get("total_supply", 0)) if token_data else 0
            total_supply = supply_raw / TINYBAR if supply_raw else 0.0

            staking_account = self.http.get(f"{self.cfg['mirror_node']}/api/v1/accounts/{staking_contract}")
            contract_balance = tinybar_to_hbar(
                int(staking_account.get("balance", {}).get("balance", 0)) if staking_account else 0
            )

            treasury_account = self.http.get(f"{self.cfg['mirror_node']}/api/v1/accounts/{treasury}")
            treasury_hbar = tinybar_to_hbar(
                int(treasury_account.get("balance", {}).get("balance", 0)) if treasury_account else 0
            )

            backing_hbar = max(contract_balance, treasury_hbar, 0.0)

            if total_supply > 0 and backing_hbar > 0:
                exchange_rate = backing_hbar / total_supply
            else:
                exchange_rate = FALLBACKS["stader_hbarx_exchange_rate"]

            base_apy = native_apy if native_apy > 0 else FALLBACKS["native_apy"]
            implied_apy = base_apy * (1.0 - fee_fraction)

            exit_score = min(1.0, (backing_hbar / 1_000_000.0) * 0.5 + (total_supply / 10_000_000.0) * 0.5)

            return StaderProfile(
                token_id=token_id,
                staking_contract_id=staking_contract,
                total_supply=total_supply,
                treasury_hbar=treasury_hbar,
                contract_hbar=contract_balance,
                exchange_rate=exchange_rate,
                implied_apy=implied_apy,
                fee_fraction=fee_fraction,
                exit_liquidity_score=exit_score,
            )
        except Exception as exc:
            print(f"[WARN] Stader profile failed: {exc}", file=sys.stderr)
            return None

    # ------------------------------------------------------------------
    # SaucerSwap pools
    # ------------------------------------------------------------------
    def _saucerswap_headers(self) -> Dict[str, str]:
        key = os.environ.get("SAUCERSWAP_API_KEY") or SAUCERSWAP_MAINNET["demo_api_key"]
        return {"x-api-key": key}

    def _parse_pool_v1(self, raw: Dict[str, Any]) -> Optional[SaucerSwapPool]:
        try:
            token_a = raw.get("tokenA", {})
            token_b = raw.get("tokenB", {})
            token_a_id = str(token_a.get("id", ""))
            token_b_id = str(token_b.get("id", ""))
            token_a_symbol = str(token_a.get("symbol", ""))
            token_b_symbol = str(token_b.get("symbol", ""))

            contains_target = token_a_id in TARGET_TOKEN_IDS.values() or token_b_id in TARGET_TOKEN_IDS.values()

            reserve_a_raw = int(raw.get("tokenReserveA", 0))
            reserve_b_raw = int(raw.get("tokenReserveB", 0))
            decimals_a = int(token_a.get("decimals", HBAR_DECIMALS))
            decimals_b = int(token_b.get("decimals", HBAR_DECIMALS))
            price_a_usd = float(token_a.get("priceUsd", 0) or 0)
            price_b_usd = float(token_b.get("priceUsd", 0) or 0)

            tvl_usd = (reserve_a_raw / (10 ** decimals_a)) * price_a_usd + (reserve_b_raw / (10 ** decimals_b)) * price_b_usd
            volume_24h_usd = float(raw.get("volume24hUsd", 0) or 0)
            fee_bps = 30

            fee_apy = estimate_pool_apy(tvl_usd, volume_24h_usd, fee_bps)

            return SaucerSwapPool(
                pool_id=int(raw.get("id", -1)),
                version=1,
                contract_id=str(raw.get("contractId", "")),
                token_a_id=token_a_id,
                token_a_symbol=token_a_symbol,
                token_b_id=token_b_id,
                token_b_symbol=token_b_symbol,
                tvl_usd=tvl_usd,
                volume_24h_usd=volume_24h_usd,
                fee_bps=fee_bps,
                estimated_fee_apy=fee_apy,
                estimated_total_apy=fee_apy,
                liquidity_depth_score=liquidity_depth_score(tvl_usd, volume_24h_usd),
                contains_target=contains_target,
            )
        except Exception as exc:
            print(f"[WARN] Skipping malformed V1 pool: {exc}", file=sys.stderr)
            return None

    def _parse_pool_v2(self, raw: Dict[str, Any]) -> Optional[SaucerSwapPool]:
        try:
            token_a = raw.get("tokenA", {})
            token_b = raw.get("tokenB", {})
            token_a_id = str(token_a.get("id", ""))
            token_b_id = str(token_b.get("id", ""))
            token_a_symbol = str(token_a.get("symbol", ""))
            token_b_symbol = str(token_b.get("symbol", ""))

            contains_target = token_a_id in TARGET_TOKEN_IDS.values() or token_b_id in TARGET_TOKEN_IDS.values()

            decimals_a = int(token_a.get("decimals", HBAR_DECIMALS))
            decimals_b = int(token_b.get("decimals", HBAR_DECIMALS))
            price_a_usd = float(token_a.get("priceUsd", 0) or 0)
            price_b_usd = float(token_b.get("priceUsd", 0) or 0)

            amount_a_raw = abs(int(raw.get("amountA", 0)))
            amount_b_raw = abs(int(raw.get("amountB", 0)))
            tvl_usd = (amount_a_raw / (10 ** decimals_a)) * price_a_usd + (amount_b_raw / (10 ** decimals_b)) * price_b_usd
            volume_24h_usd = float(raw.get("volume24hUsd", 0) or 0)
            fee_bps = int(raw.get("fee", 0)) or 3000

            fee_apy = estimate_pool_apy(tvl_usd, volume_24h_usd, fee_bps)

            return SaucerSwapPool(
                pool_id=int(raw.get("id", -1)),
                version=2,
                contract_id=str(raw.get("contractId", "")),
                token_a_id=token_a_id,
                token_a_symbol=token_a_symbol,
                token_b_id=token_b_id,
                token_b_symbol=token_b_symbol,
                tvl_usd=tvl_usd,
                volume_24h_usd=volume_24h_usd,
                fee_bps=fee_bps,
                estimated_fee_apy=fee_apy,
                estimated_total_apy=fee_apy,
                liquidity_depth_score=liquidity_depth_score(tvl_usd, volume_24h_usd),
                contains_target=contains_target,
            )
        except Exception as exc:
            print(f"[WARN] Skipping malformed V2 pool: {exc}", file=sys.stderr)
            return None

    def _evm_to_hedera_contract_id(self, evm_address: str) -> Optional[str]:
        """Resolve a Hedera contract ID from an EVM address via the Mirror Node."""
        try:
            data = self.http.get(f"{self.cfg['mirror_node']}/api/v1/contracts/{evm_address}")
            if data and isinstance(data, dict):
                return str(data.get("contract_id", "")) or None
        except Exception as exc:
            print(f"[WARN] Contract ID lookup failed for {evm_address}: {exc}", file=sys.stderr)
        return None

    def _parse_dune_pool_row(self, row: Dict[str, Any], pool_id: int = -1) -> Optional[SaucerSwapPool]:
        """Convert a Dune result row into a SaucerSwapPool object."""
        try:
            evm_address = str(row.get("contract_address", ""))
            contract_id = self._evm_to_hedera_contract_id(evm_address) or evm_address

            token0_evm = str(row.get("token0", ""))
            token1_evm = str(row.get("token1", ""))

            token0_id = self._evm_token_to_hedera_id(token0_evm)
            token1_id = self._evm_token_to_hedera_id(token1_evm)

            token_a_symbol = "HBARX" if token0_evm.lower() == "0x00000000000000000000000000000000000cba44" else "WHBAR"
            token_b_symbol = "HBARX" if token1_evm.lower() == "0x00000000000000000000000000000000000cba44" else "WHBAR"
            if token0_evm.lower() not in {
                "0x00000000000000000000000000000000000cba44",
                "0x0000000000000000000000000000000000163b5a",
            }:
                token_a_symbol = token0_evm[-8:]
            if token1_evm.lower() not in {
                "0x00000000000000000000000000000000000cba44",
                "0x0000000000000000000000000000000000163b5a",
            }:
                token_b_symbol = token1_evm[-8:]

            reserve0 = float(row.get("reserve0", 0)) / TINYBAR
            reserve1 = float(row.get("reserve1", 0)) / TINYBAR

            # Volume is 7-day from Dune; convert to 24h for APY estimation.
            volume0_7d = float(row.get("volume0_7d", 0)) / TINYBAR
            volume1_7d = float(row.get("volume1_7d", 0)) / TINYBAR
            volume_24h_hbar = (volume0_7d + volume1_7d) / 7.0

            # TVL in HBAR terms. HBARX is valued at the Stader exchange rate
            # (fallback if live rate unavailable). WHBAR is 1:1 HBAR.
            hbarx_rate = FALLBACKS["stader_hbarx_exchange_rate"]
            tvl_hbar = reserve0 * hbarx_rate + reserve1

            # USD-normalize with the HBAR price we fetched earlier.
            hbar_price = self._last_hbar_price if hasattr(self, "_last_hbar_price") else FALLBACKS["hbar_price_usd"]
            tvl_usd = tvl_hbar * hbar_price
            volume_24h_usd = volume_24h_hbar * hbar_price

            fee_bps = 30
            fee_apy = estimate_pool_apy(tvl_usd, volume_24h_usd, fee_bps)

            return SaucerSwapPool(
                pool_id=pool_id,
                version=1,
                contract_id=contract_id,
                token_a_id=token0_id,
                token_a_symbol=token_a_symbol,
                token_b_id=token1_id,
                token_b_symbol=token_b_symbol,
                tvl_usd=tvl_usd,
                volume_24h_usd=volume_24h_usd,
                fee_bps=fee_bps,
                estimated_fee_apy=fee_apy,
                estimated_total_apy=fee_apy,
                liquidity_depth_score=liquidity_depth_score(tvl_usd, volume_24h_usd),
                contains_target=True,
            )
        except Exception as exc:
            print(f"[WARN] Skipping malformed Dune pool row: {exc}", file=sys.stderr)
            return None

    @staticmethod
    def _evm_token_to_hedera_id(evm_address: str) -> str:
        """Convert a Hedera EVM token address back to shard.realm.num form."""
        if not evm_address or not evm_address.startswith("0x"):
            return ""
        try:
            # The last 8 hex digits encode the token ID on Hedera mainnet.
            token_num = int(evm_address[-8:], 16)
            return f"0.0.{token_num}"
        except ValueError:
            return evm_address

    def fetch_saucerswap_pools_from_dune(self) -> List[SaucerSwapPool]:
        """Fetch SaucerSwap on-chain data via Dune Analytics when DUNE_API_KEY is set."""
        api_key = os.environ.get("DUNE_API_KEY")
        if not api_key:
            return []

        timeout = int(os.environ.get("DUNE_QUERY_TIMEOUT", "120"))
        client = DuneClient(api_key, timeout=timeout)
        rows = client.run_query(
            "orkid_hedera_saucerswap_hbarx_whbar",
            DUNE_SAUCEERSWAP_HBARX_QUERY,
            "SaucerSwap V1 pools containing HBARX or WHBAR with 7-day volume and reserves.",
        )
        if not rows:
            return []

        pools: List[SaucerSwapPool] = []
        for idx, row in enumerate(rows, start=1):
            parsed = self._parse_dune_pool_row(row, pool_id=idx)
            if parsed:
                pools.append(parsed)
        return pools

    def fetch_saucerswap_pools(self) -> List[SaucerSwapPool]:
        # Prefer Dune when an API key is provided; it bypasses the SaucerSwap demo key
        # rate limits and gives raw on-chain reserves/volume.
        dune_pools = self.fetch_saucerswap_pools_from_dune()
        if dune_pools:
            return dune_pools

        pools: List[SaucerSwapPool] = []
        headers = self._saucerswap_headers()
        base = SAUCERSWAP_MAINNET["api_base"]

        try:
            v1_data = self.http.get(f"{base}/pools/full", params={"limit": 200}, headers=headers)
            if isinstance(v1_data, list):
                for raw in v1_data:
                    parsed = self._parse_pool_v1(raw)
                    if parsed:
                        pools.append(parsed)
        except Exception as exc:
            print(f"[WARN] SaucerSwap V1 fetch failed: {exc}", file=sys.stderr)

        # Small delay between V1 and V2 to respect the demo key rate limit.
        time.sleep(1.0)

        try:
            v2_data = self.http.get(f"{base}/v2/pools/full", params={"limit": 200}, headers=headers)
            if isinstance(v2_data, list):
                for raw in v2_data:
                    parsed = self._parse_pool_v2(raw)
                    if parsed:
                        pools.append(parsed)
        except Exception as exc:
            print(f"[WARN] SaucerSwap V2 fetch failed: {exc}", file=sys.stderr)

        return pools

    # ------------------------------------------------------------------
    # Cross-chain price data
    # ------------------------------------------------------------------
    def fetch_hbar_price(self) -> float:
        override = os.environ.get("HBAR_PRICE_USD")
        if override:
            self._last_hbar_price = float(override)
            return self._last_hbar_price

        try:
            url = "https://api.coingecko.com/api/v3/simple/price"
            params = {"ids": "hedera-hashgraph", "vs_currencies": "usd"}
            key = os.environ.get("COINGECKO_API_KEY")
            headers = {}
            if key:
                headers["x-cg-demo-api-key"] = key
            data = self.http.get(url, params=params, headers=headers)
            if data and isinstance(data, dict):
                self._last_hbar_price = float(data.get("hedera-hashgraph", {}).get("usd", 0) or FALLBACKS["hbar_price_usd"])
                return self._last_hbar_price
        except Exception as exc:
            print(f"[WARN] CoinGecko price fetch failed: {exc}", file=sys.stderr)
        self._last_hbar_price = FALLBACKS["hbar_price_usd"]
        return self._last_hbar_price


# ---------------------------------------------------------------------------
# Ranking / matrix construction
# ---------------------------------------------------------------------------

def build_ranked_targets(
    native_profile: Optional[NativeStakingProfile],
    nodes: List[HederaNode],
    stader: Optional[StaderProfile],
    pools: List[SaucerSwapPool],
    hbar_price: float,
) -> List[RankedTarget]:
    targets: List[RankedTarget] = []
    native_apy = native_profile.network_apy if native_profile else FALLBACKS["native_apy"]

    sorted_nodes = sorted(nodes, key=lambda n: (n.cap_utilization, -n.stake_total))
    for node in sorted_nodes:
        if node.cap_utilization >= 1.0 and node.stake_total > 0:
            continue

        dilution_factor = max(0.5, 1.0 - node.cap_utilization)
        net_apy = native_apy * dilution_factor

        targets.append(
            RankedTarget(
                rank=0,
                strategy=f"native_node_{node.node_id}",
                category="native_staking",
                target_address=node.node_account_id,
                target_type="account",
                net_apy=net_apy,
                risk_score=risk_score_for_category("native_staking"),
                slippage_bps=slippage_bps_for_category("native_staking"),
                max_allocation_hbar=node.max_stake - node.stake_total if node.max_stake > node.stake_total else 0.0,
                execution_flow="CryptoTransfer",
                requires_swap=False,
                memo_payload=f"CAST:native:{node.node_account_id}",
                metadata={
                    "node_id": node.node_id,
                    "description": node.description,
                    "cap_utilization": node.cap_utilization,
                    "stake_total_hbar": node.stake_total,
                    "stake_rewarded_hbar": node.stake_rewarded,
                    "max_stake_hbar": node.max_stake,
                    "dilution_factor": dilution_factor,
                },
            )
        )

    if stader:
        targets.append(
            RankedTarget(
                rank=0,
                strategy="stader_hbarx",
                category="liquid_staking",
                target_address=stader.token_id,
                target_type="token",
                net_apy=stader.implied_apy,
                risk_score=risk_score_for_category("liquid_staking"),
                slippage_bps=slippage_bps_for_category("liquid_staking"),
                max_allocation_hbar=max(0.0, stader.total_supply * stader.exchange_rate),
                execution_flow="CryptoTransfer",
                requires_swap=False,
                memo_payload=f"CAST:liquid:{stader.token_id}",
                metadata={
                    "staking_contract_id": stader.staking_contract_id,
                    "exchange_rate": stader.exchange_rate,
                    "total_supply_hbarx": stader.total_supply,
                    "backing_hbar": max(stader.contract_hbar, stader.treasury_hbar),
                    "fee_fraction": stader.fee_fraction,
                    "exit_liquidity_score": stader.exit_liquidity_score,
                },
            )
        )
    else:
        targets.append(
            RankedTarget(
                rank=0,
                strategy="stader_hbarx",
                category="liquid_staking",
                target_address=STADER_MAINNET["hbarx_token_id"],
                target_type="token",
                net_apy=FALLBACKS["stader_hbarx_apy"],
                risk_score=risk_score_for_category("liquid_staking"),
                slippage_bps=slippage_bps_for_category("liquid_staking"),
                max_allocation_hbar=1_000_000.0,
                execution_flow="CryptoTransfer",
                requires_swap=False,
                memo_payload=f"CAST:liquid:{STADER_MAINNET['hbarx_token_id']}",
                metadata={
                    "note": "fallback profile because live Stader data was unavailable",
                    "exchange_rate": FALLBACKS["stader_hbarx_exchange_rate"],
                },
            )
        )

    for pool in pools:
        if not pool.contains_target:
            continue
        category = "dex_lp_v2" if pool.version == 2 else "dex_lp_v1"

        if pool.liquidity_depth_score < 0.05:
            continue

        targets.append(
            RankedTarget(
                rank=0,
                strategy=f"saucerswap_v{pool.version}_pool_{pool.pool_id}",
                category=category,
                target_address=pool.contract_id,
                target_type="contract",
                net_apy=pool.estimated_total_apy,
                risk_score=risk_score_for_category(category),
                slippage_bps=slippage_bps_for_category(category),
                max_allocation_hbar=(pool.tvl_usd / max(hbar_price, 0.001)) * 0.1,
                execution_flow="CryptoTransfer -> SaucerSwap deposit",
                requires_swap=True,
                memo_payload=f"CAST:lp:{pool.contract_id}:{pool.token_a_id}:{pool.token_b_id}",
                metadata={
                    "pool_id": pool.pool_id,
                    "version": pool.version,
                    "token_a": pool.token_a_symbol,
                    "token_b": pool.token_b_symbol,
                    "tvl_usd": pool.tvl_usd,
                    "volume_24h_usd": pool.volume_24h_usd,
                    "fee_bps": pool.fee_bps,
                    "fee_apy": pool.estimated_fee_apy,
                    "liquidity_depth_score": pool.liquidity_depth_score,
                },
            )
        )

    def score(t: RankedTarget) -> float:
        return t.net_apy - (t.risk_score - 1.0) * 0.005

    targets.sort(key=score, reverse=True)
    for idx, target in enumerate(targets, start=1):
        target.rank = idx

    return targets


def build_matrix(profiler: HederaProfiler) -> Dict[str, Any]:
    hbar_price = profiler.fetch_hbar_price()

    native_profile = profiler.fetch_network_stake()
    nodes = profiler.fetch_nodes()

    if native_profile:
        native_profile.nodes = [asdict(n) for n in nodes]

    stader = profiler.fetch_stader_profile(native_profile.network_apy if native_profile else 0.0)
    pools = profiler.fetch_saucerswap_pools()
    ranked = build_ranked_targets(native_profile, nodes, stader, pools, hbar_price)

    price_context = {
        "hbar_price_usd": hbar_price,
        "price_source": "CoinGecko free tier" if not os.environ.get("HBAR_PRICE_USD") else "manual override",
        "price_timestamp": datetime.now(timezone.utc).isoformat(),
    }

    return {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "network": profiler.network,
        "version": "1.0.0",
        "price_context": price_context,
        "native_staking": asdict(native_profile) if native_profile else None,
        "liquid_staking": asdict(stader) if stader else None,
        "defi_yield": {
            "saucerswap_pools": [asdict(p) for p in pools if p.contains_target],
            "all_pool_count": len(pools),
        },
        "ranked_targets": [asdict(t) for t in ranked],
        "execution_guardrails": {
            "max_slippage_bps": 50,
            "default_gas_margin_hbar": 1.0,
            "quote_expiry_seconds": 30,
            "min_liquidity_depth_score": 0.05,
            "preferred_categories": ["native_staking", "liquid_staking", "dex_lp_v2", "dex_lp_v1"],
        },
    }


# ---------------------------------------------------------------------------
# Main entry point
# ---------------------------------------------------------------------------

def main() -> None:
    network = os.environ.get("HEDERA_NETWORK", "mainnet").lower()
    if network not in HEDERA_NETWORKS:
        print(f"[ERROR] Unknown network {network}. Choose from {list(HEDERA_NETWORKS.keys())}", file=sys.stderr)
        sys.exit(1)

    profiler = HederaProfiler(network=network)
    matrix = build_matrix(profiler)

    output_path = Path(
        os.environ.get("OUTPUT_PATH")
        or "/Users/jacobcavazos/Projects/past_performance/data/hedera_yield_matrix.json"
    )
    output_path.parent.mkdir(parents=True, exist_ok=True)

    with output_path.open("w", encoding="utf-8") as f:
        json.dump(matrix, f, indent=2, default=str)

    print(f"[OK] Wrote Hedera yield matrix to {output_path}")
    if matrix["ranked_targets"]:
        top = matrix["ranked_targets"][0]
        print(f"[INFO] Top target: {top['strategy']} @ {top['net_apy']:.4%} APY")


if __name__ == "__main__":
    main()
