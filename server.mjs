import { createServer } from 'http';
import { readFile, writeFile } from 'fs/promises';
import { existsSync } from 'fs';
import { fileURLToPath } from 'url';
import { dirname, join } from 'path';
import https from 'https';

import {
  JSONRpcProvider,
  getContract,
  MotoSwapFactoryAbi,
  MotoswapPoolAbi,
  OP_20_ABI,
  MOTOSWAP_STAKING_ABI,
  NativeSwapAbi,
  MOTOCHEF_ABI,
} from 'opnet';
import { networks } from '@btc-vision/bitcoin';
import { Address } from '@btc-vision/transaction';

// ── Constants ────────────────────────────────────────────────────────────────
const PORT = 3002;
const REFRESH_INTERVAL_MS = 60_000;
const HISTORY_PERSIST_FILE = join(dirname(fileURLToPath(import.meta.url)), 'tvl-history.json');
const MAX_HISTORY_POINTS = 1440;
const __dirname = dirname(fileURLToPath(import.meta.url));

const FACTORY = '0x893f92bb75fadf5333bd588af45217f33cdd1120a1b740165184c012ea1c883d';
const ROUTER  = '0x80f8375d061d638a0b45a4eb4decbfd39e9abba913f464787194ce3c02d2ea5a';
const STAKING = '0x2e955b42e6ff0934ccb3d4f1ba4d0e219ba22831dfbcabe3ff5e185bdf942a5e';
const NATIVE_SWAP = '0xb056ba05448cf4a5468b3e1190b0928443981a93c3aff568467f101e94302422';
const MOTOCHEF_FACTORY = '0x6be3f70cad127633b09819de120d86f6b7501a093b9c7aef8dbd98256ff9c9ae';

const DECIMALS = 18;

// ── Token Discovery ──────────────────────────────────────────────────────────
// The system dynamically discovers ALL tokens from two sources:
//   1. Known core protocol tokens (hardcoded)
//   2. Tokens deployed via the OP20 Deployer factory
// Then filters to only include tokens that have BOTH:
//   a. A NativeSwap pool (so we get real BTC pricing)
//   b. At least one MotoSwap (OP20Swap) LP pool
const KNOWN_TOKENS = {
  MOTO: '0x0a6732489a31e6de07917a28ff7df311fc5f98f6e1664943ac1c3fe7893bdab5',
  PILL: '0xfb7df2f08d8042d4df0506c0d4cee3cfa5f2d7b02ef01ec76dd699551393a438',
  ODYS: '0xc573930e4c67f47246589ce6fa2dbd1b91b58c8fdd7ace336ce79e65120f79eb',
};

const OP20_DEPLOYER = '0x1d2d60f610018e30c043f5a2af2ce57931759358f83ed144cb32717a9ad22345';

const OP20FactoryAbi = [
  { name: 'getDeploymentsCount', type: 'function', inputs: [], outputs: [{ name: 'count', type: 'UINT32' }] },
  { name: 'getDeploymentByIndex', type: 'function', inputs: [{ name: 'index', type: 'UINT32' }], outputs: [{ name: 'deployer', type: 'ADDRESS' }, { name: 'token', type: 'ADDRESS' }, { name: 'block', type: 'UINT64' }] },
  { name: 'address', type: 'function', inputs: [], outputs: [{ name: 'address', type: 'ADDRESS' }] },
  { name: 'deployer', type: 'function', inputs: [], outputs: [{ name: 'deployer', type: 'ADDRESS' }] },
];

// MotoChef Factory ABI — extends OP20 Factory with MotoChef-specific methods
const MotoChefFactoryAbi = [
  { name: 'getDeploymentsCount', type: 'function', inputs: [], outputs: [{ name: 'count', type: 'UINT32' }] },
  { name: 'getDeploymentByIndex', type: 'function', inputs: [{ name: 'index', type: 'UINT32' }], outputs: [
    { name: 'deployer', type: 'ADDRESS' },
    { name: 'token', type: 'ADDRESS' },
    { name: 'motoChef', type: 'ADDRESS' },
    { name: 'block', type: 'UINT64' },
  ]},
  { name: 'getTokenMotoChef', type: 'function', inputs: [{ name: 'tokenAddress', type: 'ADDRESS' }], outputs: [{ name: 'motoChefAddress', type: 'ADDRESS' }] },
  { name: 'address', type: 'function', inputs: [], outputs: [{ name: 'address', type: 'ADDRESS' }] },
];

// Tokens explicitly excluded (even if they pass both checks)
const EXCLUDED_TOKENS = new Set([]);

const network = networks.regtest;
const provider = new JSONRpcProvider('https://regtest.opnet.org', network);

// ── Helpers ──────────────────────────────────────────────────────────────────
function formatBigintNum(raw, decimals = DECIMALS) {
  if (raw == null) return 0;
  const s = raw.toString();
  if (s.length <= decimals) return Number('0.' + s.padStart(decimals, '0'));
  return Number(s.slice(0, s.length - decimals) + '.' + s.slice(s.length - decimals));
}

function addrToHex(addr) {
  if (!addr) return '';
  if (typeof addr === 'string') return addr;
  if (typeof addr.toHex === 'function') return addr.toHex();
  if (typeof addr.toString === 'function') return addr.toString();
  return String(addr);
}

// ── AMM Helpers ──────────────────────────────────────────────────────────────
function getAmountOut(amountIn, reserveIn, reserveOut) {
  if (amountIn <= 0 || reserveIn <= 0 || reserveOut <= 0) return 0;
  const amountInWithFee = amountIn * 997;
  const numerator = amountInWithFee * reserveOut;
  const denominator = reserveIn * 1000 + amountInWithFee;
  return numerator / denominator;
}

function getAmountIn(amountOut, reserveIn, reserveOut) {
  if (amountOut <= 0 || reserveIn <= 0 || reserveOut <= 0 || amountOut >= reserveOut) return Infinity;
  const numerator = reserveIn * amountOut * 1000;
  const denominator = (reserveOut - amountOut) * 997;
  return numerator / denominator + 1;
}

function computePriceImpact(amountIn, reserveIn, reserveOut) {
  if (reserveIn <= 0 || reserveOut <= 0 || amountIn <= 0) return 0;
  const spotPrice = reserveOut / reserveIn;
  const amountOut = getAmountOut(amountIn, reserveIn, reserveOut);
  const execPrice = amountOut / amountIn;
  return Math.abs(1 - execPrice / spotPrice) * 100;
}

function findMaxTradeForImpact(targetImpactPct, reserveIn, reserveOut) {
  if (reserveIn <= 0 || reserveOut <= 0) return 0;
  let lo = 0, hi = reserveIn * 0.5;
  for (let i = 0; i < 60; i++) {
    const mid = (lo + hi) / 2;
    const impact = computePriceImpact(mid, reserveIn, reserveOut);
    if (impact < targetImpactPct) lo = mid; else hi = mid;
  }
  return (lo + hi) / 2;
}

// ── BTC Price Fetcher ────────────────────────────────────────────────────────
let btcUsdPrice = 0;
let btcPriceSource = 'none';
let btcPriceUpdated = null;

function httpsGet(url) {
  return new Promise((resolve, reject) => {
    https.get(url, { headers: { 'User-Agent': 'MotoSwap-TVL/1.0' } }, (res) => {
      let data = '';
      res.on('data', chunk => data += chunk);
      res.on('end', () => {
        try { resolve(JSON.parse(data)); }
        catch (e) { reject(new Error('JSON parse error: ' + e.message)); }
      });
    }).on('error', reject);
  });
}

async function fetchBtcPrice() {
  try {
    const data = await httpsGet('https://api.coingecko.com/api/v3/simple/price?ids=bitcoin&vs_currencies=usd');
    if (data.bitcoin?.usd) {
      btcUsdPrice = data.bitcoin.usd;
      btcPriceSource = 'coingecko';
      btcPriceUpdated = new Date().toISOString();
      return;
    }
  } catch (e) { console.log('[Price] CoinGecko failed:', e.message); }
  try {
    const data = await httpsGet('https://api.coinbase.com/v2/prices/BTC-USD/spot');
    if (data.data?.amount) {
      btcUsdPrice = parseFloat(data.data.amount);
      btcPriceSource = 'coinbase';
      btcPriceUpdated = new Date().toISOString();
      return;
    }
  } catch (e) { console.log('[Price] Coinbase failed:', e.message); }
  if (btcUsdPrice === 0) {
    btcUsdPrice = 95000;
    btcPriceSource = 'fallback';
    btcPriceUpdated = new Date().toISOString();
  }
}

// ── TVL History ──────────────────────────────────────────────────────────────
let tvlHistory = [];

async function loadHistory() {
  try {
    if (existsSync(HISTORY_PERSIST_FILE)) {
      const raw = await readFile(HISTORY_PERSIST_FILE, 'utf8');
      tvlHistory = JSON.parse(raw);
      console.log(`[History] Loaded ${tvlHistory.length} data points`);
    }
  } catch (e) { tvlHistory = []; }
}

async function saveHistory() {
  try { await writeFile(HISTORY_PERSIST_FILE, JSON.stringify(tvlHistory)); } catch (e) {}
}

function addHistoryPoint(data) {
  tvlHistory.push({
    t: Date.now(),
    block: data.chain?.blockNumber || 0,
    btcPrice: btcUsdPrice,
    totalTvlUsd: data.tvl?.total?.usd || 0,
    totalTvlBtc: data.tvl?.total?.btc || 0,
    poolsTvlUsd: data.tvl?.pools?.usd || 0,
    stakingTvlUsd: data.tvl?.staking?.usd || 0,
    farmingTvlUsd: data.tvl?.farming?.usd || 0,
    pools: (data.pools || []).map(p => ({ label: p.pairLabel, tvlUsd: p.tvlUsd || 0 })),
  });
  if (tvlHistory.length > MAX_HISTORY_POINTS) tvlHistory = tvlHistory.slice(-MAX_HISTORY_POINTS);
}

// ── Cached data ──────────────────────────────────────────────────────────────
let cachedData = null;
let fetching = false;

// ── Main Data Fetching ───────────────────────────────────────────────────────
async function fetchAllData() {
  if (fetching) return cachedData;
  fetching = true;

  await fetchBtcPrice();

  const result = {
    chain: { blockNumber: null, gas: null, errors: [] },
    btcPrice: { usd: btcUsdPrice, source: btcPriceSource, updated: btcPriceUpdated },
    prices: {},
    tokens: {},
    nativeSwap: {},
    pools: [],
    staking: {
      address: STAKING,
      totalStaked: 0, rewardTokens: [], errors: [],
      tvlUsd: 0, tvlBtc: 0,
    },
    farming: {
      motoChefs: [],
      totalTvlUsd: 0, totalTvlBtc: 0,
      totalBtcStaked: 0, totalBtcStakedUsd: 0,
      errors: [],
    },
    tvl: { total: { usd: 0, btc: 0 }, pools: { usd: 0, btc: 0 }, staking: { usd: 0, btc: 0 }, farming: { usd: 0, btc: 0 } },
    protocol: {
      name: 'MotoSwap', network: 'OPNet Regtest',
      factoryAddress: FACTORY, routerAddress: ROUTER,
      stakingAddress: STAKING, nativeSwapAddress: NATIVE_SWAP,
    },
    qualifiedTokens: [],
    excludedTokens: [...EXCLUDED_TOKENS],
    lastUpdated: new Date().toISOString(),
    errors: [],
  };

  // 1. Chain data
  try { result.chain.blockNumber = Number(await provider.getBlockNumber()); }
  catch (e) { result.chain.errors.push('getBlockNumber: ' + e.message); }
  try {
    const gas = await provider.gasParameters();
    result.chain.gas = JSON.parse(JSON.stringify(gas, (_, v) => typeof v === 'bigint' ? v.toString() : v));
  } catch (e) { result.chain.errors.push('gasParameters: ' + e.message); }

  // ══════════════════════════════════════════════════════════════════════════
  // 2. TOKEN-FIRST DISCOVERY
  //    a) Enumerate ALL tokens from OP20 Deployer + known tokens
  //    b) For every unique token pair, call factory.getPool(A, B)
  //    c) Any non-zero result = a MotoSwap pool exists
  // ══════════════════════════════════════════════════════════════════════════

  let factory;
  try { factory = getContract(FACTORY, MotoSwapFactoryAbi, provider, network); }
  catch (e) { result.errors.push('Factory init: ' + e.message); }

  // addr(lower) -> symbol
  const tokenSymbolByAddr = {};
  // sym -> addr
  const candidateTokens = {};
  // All discovered pool addresses (before enrichment)
  const rawPools = []; // { poolAddress, token0Addr, token1Addr }

  // 2a. Enumerate ALL tokens from OP20 Deployer
  const allTokenAddrs = new Set(); // lowercase addresses
  const addrToOriginal = {};       // lowercase -> original hex

  try {
    const deployer = getContract(OP20_DEPLOYER, OP20FactoryAbi, provider, network);
    const countRes = await deployer.getDeploymentsCount();
    const totalDeployments = Number(countRes.properties.count);
    console.log(`[Discovery] OP20 Deployer has ${totalDeployments} deployments`);

    for (let i = 0; i < totalDeployments; i++) {
      try {
        const dep = await deployer.getDeploymentByIndex(i);
        const tokenAddr = addrToHex(dep.properties.token);
        if (!tokenAddr || tokenAddr.endsWith('0000000000000000000000000000000000000000000000000000000000000000')) continue;
        const lower = tokenAddr.toLowerCase();
        if (!allTokenAddrs.has(lower)) {
          allTokenAddrs.add(lower);
          addrToOriginal[lower] = tokenAddr;
        }
      } catch (e) {
        if (i < 3) console.log(`[Discovery] OP20 deployment #${i} error: ${e.message?.substring(0, 80)}`);
      }
    }
    console.log(`[Discovery] Found ${allTokenAddrs.size} unique token addresses from OP20 Deployer`);
  } catch (e) {
    console.log(`[Discovery] OP20 Deployer error: ${e.message?.substring(0, 100)}`);
  }

  // Add known tokens
  for (const [sym, addr] of Object.entries(KNOWN_TOKENS)) {
    const lower = addr.toLowerCase();
    if (!allTokenAddrs.has(lower)) {
      allTokenAddrs.add(lower);
      addrToOriginal[lower] = addr;
    }
  }
  console.log(`[Discovery] Total unique token addresses (deployer + known): ${allTokenAddrs.size}`);

  // 2b. Get symbol for each token (needed for display & dedup)
  const addrSymbolMap = {}; // lowercase addr -> symbol
  for (const lower of allTokenAddrs) {
    const addr = addrToOriginal[lower];
    if (EXCLUDED_TOKENS.has(lower)) continue;
    try {
      const c = getContract(addr, OP_20_ABI, provider, network);
      let symbol = addr.substring(0, 10);
      try { const r = await c.symbol(); symbol = r.properties.symbol || symbol; } catch (_) {}

      // Deduplicate symbols
      let key = symbol;
      let suffix = 2;
      while (candidateTokens[key]) { key = `${symbol}_${suffix++}`; }
      candidateTokens[key] = addr;
      tokenSymbolByAddr[lower] = key;
      addrSymbolMap[lower] = key;
      console.log(`[Discovery] Token: ${key} at ${addr.substring(0, 30)}...`);
    } catch (e) {
      const key = addr.substring(0, 12);
      candidateTokens[key] = addr;
      tokenSymbolByAddr[lower] = key;
      addrSymbolMap[lower] = key;
    }
  }

  // 2c. For every unique pair of tokens, check factory.getPool()
  if (factory) {
    const tokenList = [...allTokenAddrs].map(l => addrToOriginal[l]);
    console.log(`[Discovery] Checking ${tokenList.length * (tokenList.length - 1) / 2} possible pairs for MotoSwap pools...`);

    for (let i = 0; i < tokenList.length; i++) {
      for (let j = i + 1; j < tokenList.length; j++) {
        try {
          const r = await factory.getPool(
            Address.fromString(tokenList[i]),
            Address.fromString(tokenList[j])
          );
          const poolAddr = addrToHex(r.properties.pool);
          if (!poolAddr || poolAddr.endsWith('0000000000000000000000000000000000000000000000000000000000000000')) continue;

          // Pool exists! Get token0/token1 ordering from the actual pool
          const poolContract = getContract(poolAddr, MotoswapPoolAbi, provider, network);
          let token0Addr = tokenList[i], token1Addr = tokenList[j];
          try { const t0 = await poolContract.token0(); token0Addr = addrToHex(t0.properties.token0); } catch (_) {}
          try { const t1 = await poolContract.token1(); token1Addr = addrToHex(t1.properties.token1); } catch (_) {}

          rawPools.push({ poolAddress: poolAddr, token0Addr, token1Addr });
          const sym0 = addrSymbolMap[tokenList[i].toLowerCase()] || '??';
          const sym1 = addrSymbolMap[tokenList[j].toLowerCase()] || '??';
          console.log(`[Discovery] Pool found: ${sym0}/${sym1} at ${poolAddr.substring(0, 30)}...`);
        } catch (e) {
          // No pool for this pair — normal
        }
      }
    }
    console.log(`[Discovery] Found ${rawPools.length} MotoSwap pools`);
  }

  console.log(`[Discovery] Total unique tokens: ${Object.keys(candidateTokens).length}`);

  // 4. NativeSwap pricing for every discovered token
  const nsContract = getContract(NATIVE_SWAP, NativeSwapAbi, provider, network);

  for (const [sym, addr] of Object.entries(candidateTokens)) {
    try {
      const r = await nsContract.getReserve(Address.fromString(addr));
      const p = r.properties;
      const liquidity = BigInt(p.liquidity?.toString() || '0');
      const virtualBTCReserve = BigInt(p.virtualBTCReserve?.toString() || '0');
      const virtualTokenReserve = BigInt(p.virtualTokenReserve?.toString() || '0');

      let priceBtc = 0;
      if (virtualTokenReserve > 0n) {
        const priceSatsScaled = (virtualBTCReserve * (10n ** 18n)) / virtualTokenReserve;
        priceBtc = Number(priceSatsScaled) / 1e8;
      }

      if (liquidity === 0n && virtualBTCReserve === 0n) {
        console.log(`[Pricing] ${sym}: No NativeSwap pool`);
        // Still include token but without USD pricing
        result.prices[sym] = { btc: 0, usd: 0, source: 'none' };
        continue;
      }

      result.nativeSwap[sym] = {
        address: addr,
        liquidity: formatBigintNum(liquidity, DECIMALS),
        liquidityRaw: liquidity.toString(),
        reservedLiquidity: formatBigintNum(BigInt(p.reservedLiquidity?.toString() || '0'), DECIMALS),
        virtualBTCReserve: Number(virtualBTCReserve) / 1e8,
        virtualBTCReserveSats: Number(virtualBTCReserve),
        virtualTokenReserve: formatBigintNum(virtualTokenReserve, DECIMALS),
        virtualTokenReserveRaw: virtualTokenReserve.toString(),
        priceBtc,
        priceUsd: priceBtc * btcUsdPrice,
      };
      result.prices[sym] = { btc: priceBtc, usd: priceBtc * btcUsdPrice, source: 'nativeswap' };
      console.log(`[Pricing] ${sym}: ${(Number(virtualBTCReserve) / 1e8).toFixed(4)} BTC reserve, price=${priceBtc.toFixed(10)} BTC`);
    } catch (e) {
      console.log(`[Pricing] ${sym}: NativeSwap failed — ${e.message?.substring(0, 80)}`);
      result.prices[sym] = { btc: 0, usd: 0, source: 'none' };
    }
  }

  // 5. Token metadata
  for (const [sym, addr] of Object.entries(candidateTokens)) {
    try {
      const c = getContract(addr, OP_20_ABI, provider, network);
      let name = sym, symbol = sym, decimals = DECIMALS;
      let totalSupplyRaw = 0n, maxSupplyRaw = 0n;
      try { const r = await c.name(); name = r.properties.name; } catch (_) {}
      try { const r = await c.symbol(); symbol = r.properties.symbol; } catch (_) {}
      try { const r = await c.decimals(); decimals = Number(r.properties.decimals); } catch (_) {}
      try { const r = await c.totalSupply(); totalSupplyRaw = r.properties.totalSupply; } catch (_) {}
      try { const r = await c.maximumSupply(); maxSupplyRaw = r.properties.maximumSupply; } catch (_) {}

      const totalSupply = formatBigintNum(totalSupplyRaw, decimals);
      const maxSupply = formatBigintNum(maxSupplyRaw, decimals);
      const price = result.prices[sym] || { btc: 0, usd: 0 };

      result.tokens[sym] = {
        address: addr, name, symbol, decimals,
        totalSupply, maxSupply,
        pctMinted: maxSupply > 0 ? (totalSupply / maxSupply) * 100 : 0,
        totalSupplyRaw: totalSupplyRaw.toString(),
        maxSupplyRaw: maxSupplyRaw.toString(),
        priceBtc: price.btc,
        priceUsd: price.usd,
        marketCapUsd: totalSupply * price.usd,
        marketCapBtc: totalSupply * price.btc,
        fdvUsd: maxSupply * price.usd,
        fdvBtc: maxSupply * price.btc,
        nativeSwapLiquidity: result.nativeSwap[sym]?.liquidity || 0,
        nativeSwapBtcReserve: result.nativeSwap[sym]?.virtualBTCReserve || 0,
        hasNativeSwap: !!result.nativeSwap[sym],
      };
    } catch (e) {
      result.tokens[sym] = { address: addr, error: e.message };
    }
  }

  // 6. Enrich pool data with reserves, TVL, pricing
  for (const raw of rawPools) {
    try {
      const poolContract = getContract(raw.poolAddress, MotoswapPoolAbi, provider, network);
      let reserve0 = 0n, reserve1 = 0n, lpSupply = 0n;
      let blockTimestampLast = '0', kLast = '0';

      try { const res = await poolContract.getReserves(); reserve0 = res.properties.reserve0; reserve1 = res.properties.reserve1; blockTimestampLast = res.properties.blockTimestampLast?.toString() || '0'; } catch (_) {}
      try { const lp = await poolContract.totalSupply(); lpSupply = lp.properties.totalSupply; } catch (_) {}
      try { const k = await poolContract.kLast(); kLast = k.properties.kLast?.toString() || '0'; } catch (_) {}

      const t0sym = tokenSymbolByAddr[raw.token0Addr.toLowerCase()] || raw.token0Addr.slice(0, 10);
      const t1sym = tokenSymbolByAddr[raw.token1Addr.toLowerCase()] || raw.token1Addr.slice(0, 10);

      const reserve0Num = formatBigintNum(reserve0, DECIMALS);
      const reserve1Num = formatBigintNum(reserve1, DECIMALS);
      const lpSupplyNum = formatBigintNum(lpSupply, DECIMALS);

      const t0price = result.prices[t0sym] || { btc: 0, usd: 0 };
      const t1price = result.prices[t1sym] || { btc: 0, usd: 0 };
      const token0ValueUsd = reserve0Num * t0price.usd;
      const token1ValueUsd = reserve1Num * t1price.usd;
      const tvlUsd = token0ValueUsd + token1ValueUsd;
      const tvlBtc = (reserve0Num * t0price.btc) + (reserve1Num * t1price.btc);

      let priceRatio = 0, priceRatioLabel = '';
      if (reserve0Num > 0) {
        priceRatio = reserve1Num / reserve0Num;
        priceRatioLabel = `1 ${t0sym} = ${priceRatio.toFixed(4)} ${t1sym}`;
      }

      let lpValueUsd = 0, lpValueBtc = 0;
      if (lpSupplyNum > 0) { lpValueUsd = tvlUsd / lpSupplyNum; lpValueBtc = tvlBtc / lpSupplyNum; }

      result.pools.push({
        pairLabel: `${t0sym}/${t1sym}`,
        poolAddress: raw.poolAddress,
        token0Address: raw.token0Addr, token1Address: raw.token1Addr,
        token0Symbol: t0sym, token1Symbol: t1sym,
        reserve0Num, reserve1Num,
        reserve0Raw: reserve0.toString(), reserve1Raw: reserve1.toString(),
        lpSupplyNum, lpSupplyRaw: lpSupply.toString(),
        blockTimestampLast, kLast,
        tvlUsd, tvlBtc,
        token0ValueUsd, token1ValueUsd,
        token0PriceUsd: t0price.usd, token1PriceUsd: t1price.usd,
        token0PriceBtc: t0price.btc, token1PriceBtc: t1price.btc,
        priceRatio, priceRatioLabel,
        lpValueUsd, lpValueBtc,
      });

      console.log(`[Pools] ${t0sym}/${t1sym}: TVL=${tvlUsd.toFixed(2)} USD at ${raw.poolAddress.substring(0, 20)}...`);
    } catch (e) {
      console.log(`[Pools] Error enriching pool ${raw.poolAddress.substring(0, 20)}: ${e.message?.substring(0, 80)}`);
    }
  }

  // Track which tokens are in pools
  const tokensWithLP = new Set();
  for (const pool of result.pools) {
    tokensWithLP.add(pool.token0Symbol);
    tokensWithLP.add(pool.token1Symbol);
  }

  result.qualifiedTokens = Object.keys(candidateTokens);
  result.tokensWithLP = [...tokensWithLP];
  result.discoveredTokenCount = Object.keys(candidateTokens).length;
  console.log(`[Discovery] All tokens: ${result.qualifiedTokens.join(', ')}`);
  console.log(`[Discovery] Tokens in LP: ${[...tokensWithLP].join(', ') || 'none'}`);

  // 5. TVL aggregation
  let totalPoolsTvlUsd = 0, totalPoolsTvlBtc = 0;
  for (const pool of result.pools) {
    totalPoolsTvlUsd += pool.tvlUsd;
    totalPoolsTvlBtc += pool.tvlBtc;
  }
  result.tvl.pools = { usd: totalPoolsTvlUsd, btc: totalPoolsTvlBtc };

  // Pool dominance
  for (const pool of result.pools) {
    pool.dominancePct = (totalPoolsTvlUsd + (result.staking.tvlUsd || 0)) > 0
      ? (pool.tvlUsd / (totalPoolsTvlUsd + (result.staking.tvlUsd || 0))) * 100 : 0;
  }

  // 6. Token distribution + LP flag
  for (const [sym, tokenData] of Object.entries(result.tokens)) {
    if (tokenData.error) continue;
    tokenData.hasLP = tokensWithLP.has(sym);
    let inPools = 0;
    for (const pool of result.pools) {
      if (pool.token0Symbol === sym) inPools += pool.reserve0Num;
      if (pool.token1Symbol === sym) inPools += pool.reserve1Num;
    }
    const circulating = tokenData.totalSupply;
    tokenData.distribution = {
      inPools,
      inPoolsPct: circulating > 0 ? (inPools / circulating) * 100 : 0,
    };
  }

  // 7. Staking
  try {
    const s = getContract(STAKING, MOTOSWAP_STAKING_ABI, provider, network);
    try {
      const ts = await s.totalSupply();
      result.staking.totalStaked = formatBigintNum(ts.properties.totalSupply, DECIMALS);
      result.staking.totalStakedRaw = ts.properties.totalSupply.toString();
    } catch (e) { result.staking.errors.push('totalSupply: ' + e.message); }
    try {
      const rt = await s.enabledRewardTokens();
      const addrs = rt.properties.enabledRewardTokens || [];
      result.staking.rewardTokens = addrs.map(a => {
        const hex = addrToHex(a);
        const sym = tokenSymbolByAddr[hex.toLowerCase()] || hex.slice(0, 10);
        return { address: hex, symbol: sym };
      });
    } catch (e) { result.staking.errors.push('enabledRewardTokens: ' + e.message); }
  } catch (e) { result.staking.errors.push('staking init: ' + e.message); }

  // 8. Token balances in staking (only for qualified tokens)
  let stakingTvlUsd = 0, stakingTvlBtc = 0;
  result.staking.balances = {};
  for (const sym of result.qualifiedTokens) {
    const addr = candidateTokens[sym];
    try {
      const c = getContract(addr, OP_20_ABI, provider, network);
      const bal = await c.balanceOf(Address.fromString(STAKING));
      const balNum = formatBigintNum(bal.properties.balance, DECIMALS);
      const price = result.prices[sym];
      const balUsd = balNum * (price?.usd || 0);
      const balBtc = balNum * (price?.btc || 0);
      result.staking.balances[sym] = { amount: balNum, raw: bal.properties.balance.toString(), usd: balUsd, btc: balBtc };
      stakingTvlUsd += balUsd;
      stakingTvlBtc += balBtc;

      // Update token distribution
      if (result.tokens[sym]?.distribution) {
        result.tokens[sym].distribution.inStaking = balNum;
        result.tokens[sym].distribution.inStakingPct = result.tokens[sym].totalSupply > 0
          ? (balNum / result.tokens[sym].totalSupply) * 100 : 0;
        result.tokens[sym].distribution.free = Math.max(0, result.tokens[sym].totalSupply - result.tokens[sym].distribution.inPools - balNum);
        result.tokens[sym].distribution.freePct = result.tokens[sym].totalSupply > 0
          ? (result.tokens[sym].distribution.free / result.tokens[sym].totalSupply) * 100 : 0;
      }
    } catch (e) { result.staking.errors.push(`${sym} balanceOf: ` + e.message); }
  }
  result.staking.tvlUsd = stakingTvlUsd;
  result.staking.tvlBtc = stakingTvlBtc;
  result.tvl.staking = { usd: stakingTvlUsd, btc: stakingTvlBtc };

  // 9. MotoChef Farming
  // Build lookup: poolAddress -> pool data (for cross-referencing LP tokens)
  const poolByAddr = {};
  for (const pool of result.pools) {
    poolByAddr[pool.poolAddress.toLowerCase()] = pool;
  }

  let farmingTvlUsd = 0, farmingTvlBtc = 0;
  console.log(`[Farming] Querying MotoChef Factory for deployed farms...`);
  try {
    const mcFactory = getContract(MOTOCHEF_FACTORY, MotoChefFactoryAbi, provider, network);
    console.log(`[Farming] MotoChef Factory contract initialized`);

    // Discover all MotoChef instances from factory deployments
    const seenMotoChefs = new Set();
    let factoryCount = 0;
    try {
      const countRes = await mcFactory.getDeploymentsCount();
      factoryCount = Number(countRes.properties.count);
      console.log(`[Farming] MotoChef Factory has ${factoryCount} deployments`);
    } catch (e) { console.log(`[Farming] Factory getDeploymentsCount: ${e.message?.substring(0, 100)}`); }

    // Scan factory deployments for unique MotoChef addresses
    for (let i = 0; i < factoryCount; i++) {
      try {
        const dep = await mcFactory.getDeploymentByIndex(i);
        // Log first deployment for debugging
        if (i === 0) {
          const mc = addrToHex(dep.properties.motoChef);
          console.log(`[Farming] Sample deployment: token=${addrToHex(dep.properties.token).substring(0, 20)}..., motoChef=${mc.endsWith('00000000') ? 'none' : mc.substring(0, 20)}...`);
        }
        const mcAddr = dep.properties.motoChef ? addrToHex(dep.properties.motoChef) : null;
        const tokenAddr = addrToHex(dep.properties.token);
        if (mcAddr && !mcAddr.endsWith('0000000000000000000000000000000000000000000000000000000000000000')) {
          if (!seenMotoChefs.has(mcAddr.toLowerCase())) {
            seenMotoChefs.add(mcAddr.toLowerCase());
            console.log(`[Farming] Discovered MotoChef at ${mcAddr.substring(0, 30)}... for token ${tokenAddr.substring(0, 20)}...`);
          }
        }
        // Scan all deployments for any non-zero MotoChef, but stop early if all same token
        if (i >= 5 && seenMotoChefs.size === 0) {
          // Quick check: if all tokens so far are the same, skip ahead to last few
          break;
        }
        if (seenMotoChefs.size > 0 && i >= 20) break;
      } catch (e) {
        if (i < 3) console.log(`[Farming] Deployment #${i} error: ${e.message?.substring(0, 80)}`);
      }
    }

    console.log(`[Farming] Found ${seenMotoChefs.size} unique MotoChef contracts from ${factoryCount} deployments`);

    // Also check core tokens via getTokenMotoChef
    for (const sym of result.qualifiedTokens) {
      const tokenAddr = candidateTokens[sym];
      try {
        const mcResult = await mcFactory.getTokenMotoChef(Address.fromString(tokenAddr));
        const mcAddr = addrToHex(mcResult.properties.motoChefAddress);
        if (!mcAddr || mcAddr === '0x' || mcAddr.endsWith('0000000000000000000000000000000000000000000000000000000000000000')) {
          continue;
        }

        console.log(`[Farming] ${sym}: MotoChef found at ${mcAddr.substring(0, 30)}...`);
        const mc = getContract(mcAddr, MOTOCHEF_ABI, provider, network);

        const chefData = {
          address: mcAddr,
          rewardToken: sym,
          rewardTokenAddress: tokenAddr,
          pools: [],
          totalAllocPoint: 0,
          motoPerBlock: 0,
          motoPerBlockRaw: '0',
          bonusEndBlock: 0,
          bonusMultiplier: 0,
          totalBtcStaked: 0,
          totalBtcStakedSats: 0,
          tvlUsd: 0,
          tvlBtc: 0,
          errors: [],
        };

        // Global MotoChef params
        try { const r = await mc.totalAllocPoint(); chefData.totalAllocPoint = Number(r.properties.totalAllocPoint); } catch (e) { chefData.errors.push('totalAllocPoint: ' + e.message?.substring(0, 80)); }
        try { const r = await mc.getMotoPerBlock(); chefData.motoPerBlock = formatBigintNum(r.properties.motoPerBlock, DECIMALS); chefData.motoPerBlockRaw = r.properties.motoPerBlock.toString(); } catch (e) { chefData.errors.push('getMotoPerBlock: ' + e.message?.substring(0, 80)); }
        try { const r = await mc.getBonusEndBlock(); chefData.bonusEndBlock = Number(r.properties.bonusEndBlock); } catch (e) { chefData.errors.push('getBonusEndBlock: ' + e.message?.substring(0, 80)); }
        try { const r = await mc.getBonusMultiplier(); chefData.bonusMultiplier = Number(r.properties.bonusMultiplier); } catch (e) { chefData.errors.push('getBonusMultiplier: ' + e.message?.substring(0, 80)); }
        try {
          const r = await mc.totalBTCStaked();
          const btcRaw = BigInt(r.properties.totalBTCStaked?.toString() || '0');
          chefData.totalBtcStakedSats = Number(btcRaw);
          chefData.totalBtcStaked = Number(btcRaw) / 1e8;
        } catch (e) { chefData.errors.push('totalBTCStaked: ' + e.message?.substring(0, 80)); }

        // Enumerate farming pools
        let poolsLength = 0;
        try { const r = await mc.getPoolsLength(); poolsLength = Number(r.properties.poolsLength); } catch (e) { chefData.errors.push('getPoolsLength: ' + e.message?.substring(0, 80)); }

        console.log(`[Farming] ${sym} MotoChef: ${poolsLength} farm pools, ${chefData.motoPerBlock} ${sym}/block, bonus ${chefData.bonusMultiplier}x until block ${chefData.bonusEndBlock}`);

        for (let pid = 0; pid < poolsLength; pid++) {
          try {
            const poolInfo = await mc.getPoolInfo(pid);
            const lpTokenResult = await mc.getLpToken(pid);
            const lpTokenAddr = addrToHex(lpTokenResult.properties.lpToken);
            const allocPoint = Number(poolInfo.properties.allocPoint);
            const lastRewardBlock = Number(poolInfo.properties.lastRewardBlock);

            // Check how many LP tokens the MotoChef holds (= staked by farmers)
            let lpStaked = 0, lpStakedRaw = 0n;
            try {
              const lpContract = getContract(lpTokenAddr, OP_20_ABI, provider, network);
              const bal = await lpContract.balanceOf(Address.fromString(mcAddr));
              lpStakedRaw = bal.properties.balance;
              lpStaked = formatBigintNum(lpStakedRaw, DECIMALS);
            } catch (_) {}

            // Cross-reference with known pool to calculate TVL
            const knownPool = poolByAddr[lpTokenAddr.toLowerCase()];
            let farmPoolTvlUsd = 0, farmPoolTvlBtc = 0;
            let pairLabel = lpTokenAddr.substring(0, 16) + '...';
            let poolType = 'unknown';

            if (knownPool && knownPool.lpSupplyNum > 0) {
              // LP token farm — TVL = (lpStaked / totalLpSupply) * poolTvl
              const share = lpStaked / knownPool.lpSupplyNum;
              farmPoolTvlUsd = share * knownPool.tvlUsd;
              farmPoolTvlBtc = share * knownPool.tvlBtc;
              pairLabel = knownPool.pairLabel;
              poolType = 'lp';
            } else {
              // Could be a single-token pool (MOTO or BTC)
              // Check if lpToken matches a known token
              const matchedSym = Object.entries(result.tokens).find(([_, t]) => t.address?.toLowerCase() === lpTokenAddr.toLowerCase());
              if (matchedSym) {
                const [tSym, tData] = matchedSym;
                farmPoolTvlUsd = lpStaked * (tData.priceUsd || 0);
                farmPoolTvlBtc = lpStaked * (tData.priceBtc || 0);
                pairLabel = tSym;
                poolType = 'single';
              }
            }

            const allocPct = chefData.totalAllocPoint > 0 ? (allocPoint / chefData.totalAllocPoint) * 100 : 0;

            chefData.pools.push({
              poolId: pid,
              lpTokenAddress: lpTokenAddr,
              pairLabel,
              poolType,
              allocPoint,
              allocPct,
              lastRewardBlock,
              lpStaked,
              lpStakedRaw: lpStakedRaw.toString(),
              tvlUsd: farmPoolTvlUsd,
              tvlBtc: farmPoolTvlBtc,
            });

            chefData.tvlUsd += farmPoolTvlUsd;
            chefData.tvlBtc += farmPoolTvlBtc;

            console.log(`[Farming] ${sym} pool #${pid}: ${pairLabel} (${poolType}) — ${lpStaked.toFixed(4)} LP staked, TVL=${farmPoolTvlUsd.toFixed(2)} USD, alloc=${allocPct.toFixed(1)}%`);
          } catch (e) {
            chefData.errors.push(`pool#${pid}: ` + e.message?.substring(0, 100));
          }
        }

        // BTC staking TVL
        if (chefData.totalBtcStaked > 0) {
          const btcTvlUsd = chefData.totalBtcStaked * btcUsdPrice;
          chefData.tvlUsd += btcTvlUsd;
          chefData.tvlBtc += chefData.totalBtcStaked;
          chefData.totalBtcStakedUsd = btcTvlUsd;
          console.log(`[Farming] ${sym} MotoChef: ${chefData.totalBtcStaked.toFixed(8)} BTC staked (${btcTvlUsd.toFixed(2)} USD)`);
        }

        farmingTvlUsd += chefData.tvlUsd;
        farmingTvlBtc += chefData.tvlBtc;
        result.farming.motoChefs.push(chefData);

        console.log(`[Farming] ${sym} MotoChef total TVL: ${chefData.tvlUsd.toFixed(2)} USD`);
      } catch (e) {
        // No MotoChef for this token — that's fine
        if (!e.message?.includes('does not exist') && !e.message?.includes('not found')) {
          console.log(`[Farming] ${sym}: MotoChef query failed — ${e.message?.substring(0, 100)}`);
        }
      }
    }

    // Also process MotoChefs discovered from factory scan that weren't found by getTokenMotoChef
    const processedMcAddrs = new Set(result.farming.motoChefs.map(c => c.address.toLowerCase()));
    for (const mcAddrLower of seenMotoChefs) {
      if (processedMcAddrs.has(mcAddrLower)) continue;
      const mcAddr = '0x' + mcAddrLower.replace(/^0x/, '');
      console.log(`[Farming] Processing discovered MotoChef at ${mcAddr.substring(0, 30)}...`);
      try {
        const mc = getContract(mcAddr, MOTOCHEF_ABI, provider, network);

        // Try to identify the reward token
        let rewardSym = 'UNKNOWN';
        // MotoChef doesn't have a direct "rewardToken" getter, but we can check owner or just label it
        const chefData = {
          address: mcAddr,
          rewardToken: rewardSym,
          rewardTokenAddress: '',
          pools: [],
          totalAllocPoint: 0,
          motoPerBlock: 0,
          motoPerBlockRaw: '0',
          bonusEndBlock: 0,
          bonusMultiplier: 0,
          totalBtcStaked: 0,
          totalBtcStakedSats: 0,
          tvlUsd: 0,
          tvlBtc: 0,
          errors: [],
        };

        try { const r = await mc.totalAllocPoint(); chefData.totalAllocPoint = Number(r.properties.totalAllocPoint); } catch (_) {}
        try { const r = await mc.getMotoPerBlock(); chefData.motoPerBlock = formatBigintNum(r.properties.motoPerBlock, DECIMALS); chefData.motoPerBlockRaw = r.properties.motoPerBlock.toString(); } catch (_) {}
        try { const r = await mc.getBonusEndBlock(); chefData.bonusEndBlock = Number(r.properties.bonusEndBlock); } catch (_) {}
        try { const r = await mc.getBonusMultiplier(); chefData.bonusMultiplier = Number(r.properties.bonusMultiplier); } catch (_) {}
        try {
          const r = await mc.totalBTCStaked();
          const btcRaw = BigInt(r.properties.totalBTCStaked?.toString() || '0');
          chefData.totalBtcStakedSats = Number(btcRaw);
          chefData.totalBtcStaked = Number(btcRaw) / 1e8;
        } catch (_) {}

        let poolsLength = 0;
        try { const r = await mc.getPoolsLength(); poolsLength = Number(r.properties.poolsLength); } catch (_) {}

        console.log(`[Farming] Discovered MotoChef: ${poolsLength} pools, ${chefData.motoPerBlock}/block, bonus ${chefData.bonusMultiplier}x`);

        for (let pid = 0; pid < poolsLength; pid++) {
          try {
            const poolInfo = await mc.getPoolInfo(pid);
            const lpTokenResult = await mc.getLpToken(pid);
            const lpTokenAddr = addrToHex(lpTokenResult.properties.lpToken);
            const allocPoint = Number(poolInfo.properties.allocPoint);

            let lpStaked = 0, lpStakedRaw = 0n;
            try {
              const lpContract = getContract(lpTokenAddr, OP_20_ABI, provider, network);
              const bal = await lpContract.balanceOf(Address.fromString(mcAddr));
              lpStakedRaw = bal.properties.balance;
              lpStaked = formatBigintNum(lpStakedRaw, DECIMALS);
            } catch (_) {}

            const knownPool = poolByAddr[lpTokenAddr.toLowerCase()];
            let farmPoolTvlUsd = 0, farmPoolTvlBtc = 0;
            let pairLabel = lpTokenAddr.substring(0, 16) + '...';
            let poolType = 'unknown';

            if (knownPool && knownPool.lpSupplyNum > 0) {
              const share = lpStaked / knownPool.lpSupplyNum;
              farmPoolTvlUsd = share * knownPool.tvlUsd;
              farmPoolTvlBtc = share * knownPool.tvlBtc;
              pairLabel = knownPool.pairLabel;
              poolType = 'lp';
            } else {
              const matchedSym = Object.entries(result.tokens).find(([_, t]) => t.address?.toLowerCase() === lpTokenAddr.toLowerCase());
              if (matchedSym) {
                const [tSym, tData] = matchedSym;
                farmPoolTvlUsd = lpStaked * (tData.priceUsd || 0);
                farmPoolTvlBtc = lpStaked * (tData.priceBtc || 0);
                pairLabel = tSym;
                poolType = 'single';
              }
            }

            const allocPct = chefData.totalAllocPoint > 0 ? (allocPoint / chefData.totalAllocPoint) * 100 : 0;

            chefData.pools.push({
              poolId: pid, lpTokenAddress: lpTokenAddr, pairLabel, poolType,
              allocPoint, allocPct, lastRewardBlock: Number(poolInfo.properties.lastRewardBlock || 0),
              lpStaked, lpStakedRaw: lpStakedRaw.toString(),
              tvlUsd: farmPoolTvlUsd, tvlBtc: farmPoolTvlBtc,
            });
            chefData.tvlUsd += farmPoolTvlUsd;
            chefData.tvlBtc += farmPoolTvlBtc;
            console.log(`[Farming] Discovered pool #${pid}: ${pairLabel} (${poolType}) — ${lpStaked.toFixed(4)} LP, TVL=${farmPoolTvlUsd.toFixed(2)} USD`);
          } catch (e) { chefData.errors.push(`pool#${pid}: ` + e.message?.substring(0, 80)); }
        }

        if (chefData.totalBtcStaked > 0) {
          const btcTvlUsd = chefData.totalBtcStaked * btcUsdPrice;
          chefData.tvlUsd += btcTvlUsd;
          chefData.tvlBtc += chefData.totalBtcStaked;
          chefData.totalBtcStakedUsd = btcTvlUsd;
        }

        farmingTvlUsd += chefData.tvlUsd;
        farmingTvlBtc += chefData.tvlBtc;
        result.farming.motoChefs.push(chefData);
        console.log(`[Farming] Discovered MotoChef TVL: ${chefData.tvlUsd.toFixed(2)} USD`);
      } catch (e) {
        console.log(`[Farming] Discovered MotoChef error: ${e.message?.substring(0, 100)}`);
      }
    }
  } catch (e) {
    result.farming.errors.push('MotoChef Factory: ' + e.message?.substring(0, 100));
    console.log(`[Farming] MotoChef Factory error: ${e.message?.substring(0, 100)}`);
  }

  result.farming.totalTvlUsd = farmingTvlUsd;
  result.farming.totalTvlBtc = farmingTvlBtc;
  result.tvl.farming = { usd: farmingTvlUsd, btc: farmingTvlBtc };

  // Update token distributions with farming data
  for (const chef of result.farming.motoChefs) {
    for (const farmPool of chef.pools) {
      if (farmPool.poolType === 'lp' && farmPool.lpStaked > 0) {
        const knownPool = poolByAddr[farmPool.lpTokenAddress.toLowerCase()];
        if (knownPool && knownPool.lpSupplyNum > 0) {
          const share = farmPool.lpStaked / knownPool.lpSupplyNum;
          // Attribute proportional reserves to farming
          for (const [sym, tokenData] of Object.entries(result.tokens)) {
            if (tokenData.error || !tokenData.distribution) continue;
            let tokenInPool = 0;
            if (knownPool.token0Symbol === sym) tokenInPool = knownPool.reserve0Num;
            if (knownPool.token1Symbol === sym) tokenInPool = knownPool.reserve1Num;
            if (tokenInPool > 0) {
              const inFarming = tokenInPool * share;
              tokenData.distribution.inFarming = (tokenData.distribution.inFarming || 0) + inFarming;
              tokenData.distribution.inFarmingPct = tokenData.totalSupply > 0
                ? (tokenData.distribution.inFarming / tokenData.totalSupply) * 100 : 0;
            }
          }
        }
      }
    }
  }

  // 10. Total TVL
  result.tvl.total = {
    usd: totalPoolsTvlUsd + stakingTvlUsd + farmingTvlUsd,
    btc: totalPoolsTvlBtc + stakingTvlBtc + farmingTvlBtc,
  };

  // Recalc pool dominance with total
  for (const pool of result.pools) {
    pool.dominancePct = result.tvl.total.usd > 0 ? (pool.tvlUsd / result.tvl.total.usd) * 100 : 0;
  }

  // ══════════════════════════════════════════════════════════════════════════
  // 11. ARBITRAGE ANALYSIS
  // ══════════════════════════════════════════════════════════════════════════
  result.arbitrage = { directArb: [], triangularArb: [], priceImpact: [], liquidityDepth: [] };
  try {
    const validPools = result.pools.filter(p => !p.error && p.reserve0Num > 0 && p.reserve1Num > 0);
    console.log(`[Arbitrage] Analyzing ${validPools.length} pools for arbitrage opportunities...`);

    // ── A. Direct Arbitrage per pool ──
    for (const pool of validPools) {
      const t0 = pool.token0Symbol;
      const t1 = pool.token1Symbol;
      const r0 = pool.reserve0Num;
      const r1 = pool.reserve1Num;
      const p0btc = result.prices[t0]?.btc || 0;
      const p1btc = result.prices[t1]?.btc || 0;
      const p0usd = result.prices[t0]?.usd || 0;
      const p1usd = result.prices[t1]?.usd || 0;

      // Pool implied price: how many token1 per token0
      const poolPrice = r0 > 0 ? r1 / r0 : 0;
      // Oracle price: token0 in BTC / token1 in BTC = how many token1 per token0 at oracle
      const oraclePrice = p1btc > 0 ? p0btc / p1btc : 0;

      let deviationPct = 0;
      if (oraclePrice > 0) {
        deviationPct = Math.abs(poolPrice - oraclePrice) / oraclePrice * 100;
      }

      // Direction: if pool price > oracle price, pool overvalues token1 relative to token0
      // i.e., you get more token1 per token0 than oracle says → sell token0, buy token1
      let direction = 'none';
      let action = '';
      let sellToken = '', buyToken = '';
      let reserveIn = 0, reserveOut = 0;
      let sellPriceUsd = 0, buyPriceUsd = 0;

      if (poolPrice > oraclePrice && oraclePrice > 0) {
        direction = 'sell_token0';
        action = `Sell ${t0} for ${t1}`;
        sellToken = t0; buyToken = t1;
        reserveIn = r0; reserveOut = r1;
        sellPriceUsd = p0usd; buyPriceUsd = p1usd;
      } else if (poolPrice < oraclePrice && oraclePrice > 0) {
        direction = 'sell_token1';
        action = `Sell ${t1} for ${t0}`;
        sellToken = t1; buyToken = t0;
        reserveIn = r1; reserveOut = r0;
        sellPriceUsd = p1usd; buyPriceUsd = p0usd;
      }

      // Optimal trade size via quadratic formula for constant-product AMM
      // Target price Pt, gamma = 0.997 (fee factor)
      // optimal_in = (sqrt(gamma * Pt * Rin * Rout) - Rin * gamma) / gamma  (simplified)
      let optimalInput = 0;
      let expectedOutput = 0;
      let profitUsd = 0;
      const gamma = 0.997;

      if (direction !== 'none' && reserveIn > 0 && reserveOut > 0) {
        const Pt = oraclePrice > 0 ? (direction === 'sell_token0' ? oraclePrice : 1 / oraclePrice) : 0;
        if (Pt > 0) {
          const sqrtVal = Math.sqrt(gamma * Pt * reserveIn * reserveOut);
          optimalInput = Math.max(0, (sqrtVal - reserveIn * gamma) / gamma);
          // Cap at 30% of reserve to avoid unrealistic trades
          optimalInput = Math.min(optimalInput, reserveIn * 0.3);
          if (optimalInput > 0) {
            expectedOutput = getAmountOut(optimalInput, reserveIn, reserveOut);
            const inputValueUsd = optimalInput * sellPriceUsd;
            const outputValueUsd = expectedOutput * buyPriceUsd;
            profitUsd = outputValueUsd - inputValueUsd;
          }
        }
      }

      const isViable = deviationPct > 0.6 && profitUsd > 0;

      const arbEntry = {
        pool: pool.pairLabel,
        poolAddress: pool.poolAddress,
        token0: t0, token1: t1,
        poolPrice, oraclePrice,
        deviationPct,
        direction, action,
        sellToken, buyToken,
        optimalInput, expectedOutput,
        optimalInputUsd: optimalInput * sellPriceUsd,
        expectedOutputUsd: expectedOutput * buyPriceUsd,
        profitUsd,
        profitPct: (optimalInput * sellPriceUsd) > 0 ? (profitUsd / (optimalInput * sellPriceUsd)) * 100 : 0,
        isViable,
      };
      result.arbitrage.directArb.push(arbEntry);
      if (deviationPct > 1) {
        console.log(`[Arbitrage] ${pool.pairLabel}: ${deviationPct.toFixed(2)}% deviation → ${action || 'none'}, profit=${profitUsd.toFixed(2)} USD`);
      }
    }

    // ── B. Triangular Arbitrage ──
    // Find all possible 3-pool cycles
    const poolMap = {};
    for (const pool of validPools) {
      const key01 = `${pool.token0Symbol}-${pool.token1Symbol}`;
      const key10 = `${pool.token1Symbol}-${pool.token0Symbol}`;
      poolMap[key01] = pool;
      poolMap[key10] = pool;
    }

    const tokenSet = new Set();
    for (const pool of validPools) {
      tokenSet.add(pool.token0Symbol);
      tokenSet.add(pool.token1Symbol);
    }
    const tokens = [...tokenSet];

    for (let i = 0; i < tokens.length; i++) {
      for (let j = i + 1; j < tokens.length; j++) {
        for (let k = j + 1; k < tokens.length; k++) {
          const [A, B, C] = [tokens[i], tokens[j], tokens[k]];
          // Check if all 3 pairs have pools
          const poolAB = poolMap[`${A}-${B}`] || poolMap[`${B}-${A}`];
          const poolBC = poolMap[`${B}-${C}`] || poolMap[`${C}-${B}`];
          const poolCA = poolMap[`${C}-${A}`] || poolMap[`${A}-${C}`];
          if (!poolAB || !poolBC || !poolCA) continue;

          // Helper: swap through a pool
          function swapThrough(pool, fromToken, amountIn) {
            if (pool.token0Symbol === fromToken) {
              return { out: getAmountOut(amountIn, pool.reserve0Num, pool.reserve1Num), outToken: pool.token1Symbol };
            } else {
              return { out: getAmountOut(amountIn, pool.reserve1Num, pool.reserve0Num), outToken: pool.token0Symbol };
            }
          }

          // Clockwise: A → B → C → A
          function evalCycle(startToken, path, pools) {
            // Ternary search for optimal input
            const priceA = result.prices[startToken]?.usd || 0;
            let lo = 0.001, hi = 0;
            // Max input: 20% of the first pool's relevant reserve
            const firstPool = pools[0];
            if (firstPool.token0Symbol === startToken) hi = firstPool.reserve0Num * 0.2;
            else hi = firstPool.reserve1Num * 0.2;
            if (hi <= 0) return null;

            function profitForInput(input) {
              let amount = input;
              let token = startToken;
              for (let s = 0; s < pools.length; s++) {
                const r = swapThrough(pools[s], token, amount);
                amount = r.out;
                token = r.outToken;
              }
              return amount - input; // profit in startToken units
            }

            // Ternary search (unimodal profit function)
            for (let iter = 0; iter < 80; iter++) {
              const m1 = lo + (hi - lo) / 3;
              const m2 = hi - (hi - lo) / 3;
              if (profitForInput(m1) < profitForInput(m2)) lo = m1;
              else hi = m2;
            }
            const optInput = (lo + hi) / 2;
            const profit = profitForInput(optInput);

            // Trace the full path for display
            let amount = optInput;
            let token = startToken;
            const steps = [{ token: startToken, amount: optInput }];
            for (let s = 0; s < pools.length; s++) {
              const r = swapThrough(pools[s], token, amount);
              amount = r.out;
              token = r.outToken;
              steps.push({ token: r.outToken, amount: r.out, pool: pools[s].pairLabel });
            }

            return {
              optimalInput: optInput,
              optimalInputUsd: optInput * priceA,
              finalOutput: optInput + profit,
              profitTokens: profit,
              profitUsd: profit * priceA,
              profitPct: optInput > 0 ? (profit / optInput) * 100 : 0,
              steps,
              isViable: profit > 0 && (profit * priceA) > 0.01,
            };
          }

          // Clockwise: A → B → C → A
          const cwResult = evalCycle(A, [A, B, C, A], [poolAB, poolBC, poolCA]);
          if (cwResult) {
            result.arbitrage.triangularArb.push({
              label: `${A} > ${B} > ${C} > ${A}`,
              direction: 'clockwise',
              tokens: [A, B, C],
              ...cwResult,
            });
            if (cwResult.isViable) {
              console.log(`[Arbitrage] Tri-arb CW ${A}>${B}>${C}>${A}: profit=${cwResult.profitUsd.toFixed(2)} USD`);
            }
          }

          // Counter-clockwise: A → C → B → A
          const ccwResult = evalCycle(A, [A, C, B, A], [poolCA, poolBC, poolAB]);
          if (ccwResult) {
            result.arbitrage.triangularArb.push({
              label: `${A} > ${C} > ${B} > ${A}`,
              direction: 'counter-clockwise',
              tokens: [A, C, B],
              ...ccwResult,
            });
            if (ccwResult.isViable) {
              console.log(`[Arbitrage] Tri-arb CCW ${A}>${C}>${B}>${A}: profit=${ccwResult.profitUsd.toFixed(2)} USD`);
            }
          }
        }
      }
    }

    // ── C. Price Impact / Slippage Table ──
    const tradeSizesUsd = [100, 1000, 10000, 100000];
    for (const pool of validPools) {
      const t0 = pool.token0Symbol;
      const t1 = pool.token1Symbol;
      const p0usd = result.prices[t0]?.usd || 0;
      const p1usd = result.prices[t1]?.usd || 0;

      const impactEntry = {
        pool: pool.pairLabel,
        poolAddress: pool.poolAddress,
        token0: t0, token1: t1,
        sells: [],
      };

      // Sell token0 for token1
      for (const sizeUsd of tradeSizesUsd) {
        const amountIn = p0usd > 0 ? sizeUsd / p0usd : 0;
        const amountOut = amountIn > 0 ? getAmountOut(amountIn, pool.reserve0Num, pool.reserve1Num) : 0;
        const outputUsd = amountOut * p1usd;
        const spotPrice = pool.reserve0Num > 0 ? pool.reserve1Num / pool.reserve0Num : 0;
        const execPrice = amountIn > 0 ? amountOut / amountIn : 0;
        const impact = spotPrice > 0 ? Math.abs(1 - execPrice / spotPrice) * 100 : 0;
        impactEntry.sells.push({
          direction: `${t0} > ${t1}`,
          sizeUsd,
          amountIn, amountOut,
          inputUsd: sizeUsd,
          outputUsd,
          execPrice,
          spotPrice,
          impactPct: impact,
          slippageUsd: sizeUsd - outputUsd,
        });
      }

      // Sell token1 for token0
      for (const sizeUsd of tradeSizesUsd) {
        const amountIn = p1usd > 0 ? sizeUsd / p1usd : 0;
        const amountOut = amountIn > 0 ? getAmountOut(amountIn, pool.reserve1Num, pool.reserve0Num) : 0;
        const outputUsd = amountOut * p0usd;
        const spotPrice = pool.reserve1Num > 0 ? pool.reserve0Num / pool.reserve1Num : 0;
        const execPrice = amountIn > 0 ? amountOut / amountIn : 0;
        const impact = spotPrice > 0 ? Math.abs(1 - execPrice / spotPrice) * 100 : 0;
        impactEntry.sells.push({
          direction: `${t1} > ${t0}`,
          sizeUsd,
          amountIn, amountOut,
          inputUsd: sizeUsd,
          outputUsd,
          execPrice,
          spotPrice,
          impactPct: impact,
          slippageUsd: sizeUsd - outputUsd,
        });
      }

      result.arbitrage.priceImpact.push(impactEntry);
    }

    // ── D. Liquidity Depth ──
    const impactThresholds = [2, 5, 10];
    for (const pool of validPools) {
      const p0usd = result.prices[pool.token0Symbol]?.usd || 0;
      const p1usd = result.prices[pool.token1Symbol]?.usd || 0;

      const depthEntry = {
        pool: pool.pairLabel,
        poolAddress: pool.poolAddress,
        token0: pool.token0Symbol, token1: pool.token1Symbol,
        depths: [],
      };

      for (const threshold of impactThresholds) {
        // Sell token0
        const maxT0 = findMaxTradeForImpact(threshold, pool.reserve0Num, pool.reserve1Num);
        // Sell token1
        const maxT1 = findMaxTradeForImpact(threshold, pool.reserve1Num, pool.reserve0Num);
        depthEntry.depths.push({
          impactPct: threshold,
          maxToken0: maxT0,
          maxToken0Usd: maxT0 * p0usd,
          maxToken1: maxT1,
          maxToken1Usd: maxT1 * p1usd,
        });
      }

      result.arbitrage.liquidityDepth.push(depthEntry);
    }

    // Summary stats
    const viableArbs = result.arbitrage.directArb.filter(a => a.isViable);
    const maxDeviation = Math.max(...result.arbitrage.directArb.map(a => a.deviationPct), 0);
    const totalArbProfit = viableArbs.reduce((sum, a) => sum + a.profitUsd, 0);
    const avgDeviation = result.arbitrage.directArb.length > 0
      ? result.arbitrage.directArb.reduce((sum, a) => sum + a.deviationPct, 0) / result.arbitrage.directArb.length : 0;
    const bestTriArb = result.arbitrage.triangularArb.reduce((best, t) => t.profitUsd > (best?.profitUsd || 0) ? t : best, null);

    result.arbitrage.summary = {
      totalProfitAvailable: totalArbProfit,
      maxDeviation,
      avgDeviation,
      viableCount: viableArbs.length,
      totalPools: validPools.length,
      bestTriArb: bestTriArb ? { label: bestTriArb.label, profitUsd: bestTriArb.profitUsd } : null,
    };

    console.log(`[Arbitrage] Summary: ${viableArbs.length} viable direct arbs, total profit=$${totalArbProfit.toFixed(2)}, max deviation=${maxDeviation.toFixed(2)}%`);
  } catch (e) {
    console.log(`[Arbitrage] Error during analysis: ${e.message?.substring(0, 200)}`);
    result.arbitrage.error = e.message;
  }

  addHistoryPoint(result);
  cachedData = result;
  fetching = false;
  return result;
}

// ── Startup ──────────────────────────────────────────────────────────────────
console.log('[MotoSwap TVL] Starting initial data fetch...');
await loadHistory();
fetchAllData()
  .then(() => console.log('[MotoSwap TVL] Initial data loaded.'))
  .catch(e => console.error('[MotoSwap TVL] Initial fetch error:', e.message));

setInterval(() => {
  console.log('[MotoSwap TVL] Refreshing data...');
  fetchAllData()
    .then(() => { console.log('[MotoSwap TVL] Data refreshed at', new Date().toISOString()); saveHistory(); })
    .catch(e => console.error('[MotoSwap TVL] Refresh error:', e.message));
}, REFRESH_INTERVAL_MS);

// ── HTTP Server ──────────────────────────────────────────────────────────────
const server = createServer(async (req, res) => {
  res.setHeader('Access-Control-Allow-Origin', '*');
  res.setHeader('Access-Control-Allow-Methods', 'GET');
  const url = new URL(req.url, `http://localhost:${PORT}`);

  if (req.method === 'GET' && url.pathname === '/api/data') {
    res.writeHead(200, { 'Content-Type': 'application/json' });
    if (cachedData) { res.end(JSON.stringify(cachedData)); }
    else {
      try { const data = await fetchAllData(); res.end(JSON.stringify(data)); }
      catch (e) { res.end(JSON.stringify({ error: e.message })); }
    }
    return;
  }

  if (req.method === 'GET' && url.pathname === '/api/history') {
    res.writeHead(200, { 'Content-Type': 'application/json' });
    const limit = parseInt(url.searchParams.get('limit')) || tvlHistory.length;
    res.end(JSON.stringify(tvlHistory.slice(-limit)));
    return;
  }

  if (req.method === 'GET' && (url.pathname === '/' || url.pathname === '/motoscope.html' || url.pathname === '/motoscope')) {
    try {
      const html = await readFile(join(__dirname, 'motoscope.html'), 'utf8');
      res.writeHead(200, { 'Content-Type': 'text/html; charset=utf-8' });
      res.end(html);
    } catch (e) {
      res.writeHead(500, { 'Content-Type': 'text/plain' });
      res.end('Failed to read index.html: ' + e.message);
    }
    return;
  }

  res.writeHead(404, { 'Content-Type': 'text/plain' });
  res.end('Not Found');
});

server.listen(PORT, () => {
  console.log(`[MotoSwap TVL] Dashboard: http://localhost:${PORT}`);
  console.log(`[MotoSwap TVL] API:       http://localhost:${PORT}/api/data`);
  console.log(`[MotoSwap TVL] History:   http://localhost:${PORT}/api/history`);
});
