import { createServer } from 'http';
import { readFile, writeFile } from 'fs/promises';
import { existsSync } from 'fs';
import { fileURLToPath } from 'url';
import { dirname, join } from 'path';

import {
  JSONRpcProvider,
  getContract,
  MOTOSWAP_ROUTER_ABI,
  OP_20_ABI,
} from 'opnet';
import { Wallet, Address, TransactionFactory, OPNetLimitedProvider } from '@btc-vision/transaction';
import { networks } from '@btc-vision/bitcoin';

// ── Constants ────────────────────────────────────────────────────────────────
const PORT = 3004;
const MOTOSCOPE = 'http://localhost:3002';
const __dirname = dirname(fileURLToPath(import.meta.url));
const WALLET_FILE = join(__dirname, 'bot-wallet.json');
const TRADES_FILE = join(__dirname, 'bot-trades.json');
const SCAN_INTERVAL_MS = 30_000;

const ROUTER = '0x80f8375d061d638a0b45a4eb4decbfd39e9abba913f464787194ce3c02d2ea5a';
const TOKENS = {
  MOTO: '0x0a6732489a31e6de07917a28ff7df311fc5f98f6e1664943ac1c3fe7893bdab5',
  PILL: '0xfb7df2f08d8042d4df0506c0d4cee3cfa5f2d7b02ef01ec76dd699551393a438',
  ODYS: '0xc573930e4c67f47246589ce6fa2dbd1b91b58c8fdd7ace336ce79e65120f79eb',
};
const DECIMALS = 18;
const network = networks.regtest;
const provider = new JSONRpcProvider('https://regtest.opnet.org', network);
const txProvider = new OPNetLimitedProvider('https://regtest.opnet.org');

let wallet = null;

// ── Bot State ────────────────────────────────────────────────────────────────
const state = {
  wallet: { p2tr: '', p2wpkh: '' },
  balances: { BTC: 0, MOTO: 0, PILL: 0, ODYS: 0 },
  rawBalances: { MOTO: 0n, PILL: 0n, ODYS: 0n }, // exact on-chain BigInt values
  balancesUsd: { BTC: 0, MOTO: 0, PILL: 0, ODYS: 0 },
  opportunities: { directArb: [], triangularArb: [], summary: {}, priceImpact: [], liquidityDepth: [] },
  prices: {},
  pools: [],
  btcPrice: 95000,
  trades: [],
  pnl: { totalProfit: 0, totalTrades: 0, wins: 0, losses: 0, bestTrade: null, worstTrade: null },
  config: { slippagePct: 2, maxGasSats: 50000, autoMode: false },
  status: 'idle',
  executingTrade: null,
  lastScan: null,
  error: null,
  connected: false,
  repeatProgress: null,
};

// ── Helpers ──────────────────────────────────────────────────────────────────
function fmt(raw, dec = DECIMALS) {
  if (raw == null) return 0;
  const s = raw.toString();
  if (s.length <= dec) return Number('0.' + s.padStart(dec, '0'));
  return Number(s.slice(0, s.length - dec) + '.' + s.slice(s.length - dec));
}

function toBig(num, dec = DECIMALS) {
  if (num <= 0) return 0n;
  const s = num.toFixed(dec);
  const [w, f] = s.split('.');
  return BigInt(w + (f || '').padEnd(dec, '0'));
}

function decodeRevert(r) {
  if (!r) return 'unknown';
  if (typeof r === 'string') return r;
  if (r instanceof Uint8Array) {
    try { return new TextDecoder().decode(r); } catch { return String(r); }
  }
  return String(r);
}

function readBody(req) {
  return new Promise((resolve) => {
    const chunks = [];
    req.on('data', c => chunks.push(c));
    req.on('end', () => {
      try { resolve(JSON.parse(Buffer.concat(chunks).toString())); }
      catch { resolve({}); }
    });
    req.on('error', () => resolve({}));
  });
}

// ── Send a swap TX bypassing simulation (for same-block approve+swap) ────────
async function sendSwapDirect(routerAddr, calldata, gasEstimate = 10000n) {
  const challenge = await provider.getChallenge();
  const gasParams = await provider.gasParameters();
  const gasSatFee = gasEstimate > 0n ? gasEstimate : 10000n;
  const feeRate = gasParams?.bitcoin?.conservative || 10;

  const utxos = await txProvider.fetchUTXOMultiAddr({
    addresses: [wallet.p2tr],
    minAmount: 330n,
    requestedAmount: gasSatFee + 100_000n,
  });
  if (!utxos?.length) throw new Error('No UTXOs for swap TX');

  const routerContract = getContract(routerAddr, MOTOSWAP_ROUTER_ABI, provider, network, wallet.address);
  const contractAddr = await routerContract.contractAddress;
  const p2op = routerContract.p2op;

  const factory = new TransactionFactory();
  const tx = await factory.signInteraction({
    calldata,
    contract: contractAddr.toHex(),
    to: p2op,
    from: wallet.p2tr,
    utxos,
    signer: wallet.keypair,
    mldsaSigner: wallet.mldsaKeypair,
    network,
    feeRate,
    priorityFee: 0n,
    gasSatFee,
    challenge,
  });

  // Broadcast funding TX then interaction TX
  if (tx.fundingTransaction) {
    const r1 = await provider.sendRawTransaction(tx.fundingTransaction, false);
    if (r1?.error) throw new Error('Funding TX failed: ' + r1.error);
  }
  const r2 = await provider.sendRawTransaction(tx.interactionTransaction, false);
  if (r2?.error) throw new Error('Interaction TX failed: ' + r2.error);

  return { transactionId: r2?.result || r2 };
}

// ── Wallet Management ────────────────────────────────────────────────────────
async function initWallet() {
  if (existsSync(WALLET_FILE)) {
    const d = JSON.parse(await readFile(WALLET_FILE, 'utf8'));
    wallet = Wallet.fromWif(d.wif, d.quantumPrivateKey, network);
    console.log('[Bot] Loaded wallet from bot-wallet.json');
  } else {
    console.log('[Bot] Generating new wallet (ML-DSA key generation may take a moment)...');
    wallet = Wallet.generate(network);
    await writeFile(WALLET_FILE, JSON.stringify({
      wif: wallet.toWIF(),
      quantumPrivateKey: wallet.quantumPrivateKeyHex,
      p2tr: wallet.p2tr,
      p2wpkh: wallet.p2wpkh,
      created: new Date().toISOString(),
    }, null, 2));
    console.log('[Bot] New wallet saved to bot-wallet.json');
  }
  state.wallet.p2tr = wallet.p2tr;
  state.wallet.p2wpkh = wallet.p2wpkh;
  state.wallet.ecPubKeyHex = wallet.publicKey.toString('hex');
  state.wallet.mldsaPubKeyHex = wallet.quantumPublicKeyHex;
  console.log('[Bot] ────────────────────────────────────────');
  console.log('[Bot] P2TR (BTC gas):', wallet.p2tr);
  console.log(`[Bot] ML-DSA Public Key (${state.wallet.mldsaPubKeyHex.length / 2} bytes): 0x${state.wallet.mldsaPubKeyHex.substring(0, 40)}...`);
  console.log('[Bot] ────────────────────────────────────────');
  console.log('[Bot] For OP20 token deposits: copy the ML-DSA public key from the dashboard');
  console.log('[Bot] Paste it (with 0x prefix) as the recipient in OP_WALLET');
}

// ── Register Keys on OPNet + Split UTXOs ─────────────────────────────────────
async function registerKeysAndSplit() {
  // Check if keys already registered
  try {
    const raw = await provider.getPublicKeysInfoRaw([wallet.p2tr]);
    const info = raw?.[wallet.p2tr] || raw;
    if (info && !info.error && info.mldsaPublicKey) {
      console.log('[Bot] Keys already registered on-chain (full ML-DSA key revealed)');
      state.wallet.keysRegistered = true;
      return true;
    }
  } catch (_) {}

  // Step 1: Register EC + ML-DSA keys via OPNet contract interaction
  // The 5th param to getContract (sender) is CRITICAL - without it, simulation fails
  console.log('[Bot] Registering EC + ML-DSA keys via contract interaction...');
  try {
    const tokenContract = getContract(TOKENS.MOTO, OP_20_ABI, provider, network, wallet.address);
    const sim = await tokenContract.increaseAllowance(
      Address.fromString(ROUTER),
      1n
    );

    if (sim.revert) {
      console.log('[Bot] Allowance simulation reverted:', decodeRevert(sim.revert));
    } else {
      console.log('[Bot] Simulation passed. Sending contract interaction TX...');
      const tx = await sim.sendTransaction({
        signer: wallet.keypair,
        mldsaSigner: wallet.mldsaKeypair,
        refundTo: wallet.p2tr,
        maximumAllowedSatToSpend: 100_000n,
        network,
        revealMLDSAPublicKey: true,
      });
      const txId = tx?.transactionId || tx?.txId || tx?.txid || JSON.stringify(tx).substring(0, 100);
      console.log('[Bot] Contract interaction TX sent:', txId);
      console.log('[Bot] EC + ML-DSA keys registered on OPNet consensus layer');
      state.wallet.keysRegistered = true;
    }
  } catch (e) {
    console.error('[Bot] Key registration failed:', e.message?.substring(0, 200));
  }

  // Step 2: Split UTXOs for gas management
  try {
    const utxos = await txProvider.fetchUTXOMultiAddr({
      addresses: [wallet.p2wpkh, wallet.p2tr],
      minAmount: 330n,
      requestedAmount: 1000000000000000n,
    });

    if (utxos && utxos.length < 8) {
      console.log(`[Bot] Only ${utxos.length} UTXOs, splitting into 10 for parallel TX chains...`);
      const splitResult = await txProvider.splitUTXOs(wallet, network, 10, 50000n);
      console.log('[Bot] UTXO split TX:', JSON.stringify(splitResult).substring(0, 200));
    } else {
      console.log(`[Bot] ${utxos?.length || 0} UTXOs available, split not needed.`);
    }
  } catch (e) {
    console.error('[Bot] UTXO split failed:', e.message);
  }

  // Verify key registration
  await new Promise(r => setTimeout(r, 2000));
  try {
    const raw = await provider.getPublicKeysInfoRaw([wallet.p2tr]);
    const info = raw?.[wallet.p2tr] || raw;
    const keys = Object.keys(info || {});
    console.log('[Bot] Post-registration key info:', keys.join(', '));
    if (info?.mldsaPublicKey) {
      console.log('[Bot] Full ML-DSA key revealed on-chain');
      state.wallet.keysRegistered = true;
    } else if (info?.mldsaHashedPublicKey) {
      console.log('[Bot] ML-DSA hash registered, waiting for full key reveal in next block...');
    }
  } catch (_) {}

  return state.wallet.keysRegistered || false;
}

// ── One-time Max Approvals for Router ────────────────────────────────────────
const APPROVALS_FILE = join(__dirname, 'bot-approvals.json');
const MAX_ALLOWANCE = (2n ** 128n) - 1n; // huge allowance so we never need to re-approve

async function setupApprovals() {
  // Check if already approved
  let approved = {};
  try {
    approved = JSON.parse(await readFile(APPROVALS_FILE, 'utf8'));
  } catch (_) {}

  const tokenNames = Object.keys(TOKENS);
  const needsApproval = tokenNames.filter(t => !approved[t]);
  if (needsApproval.length === 0) {
    console.log('[Bot] All tokens pre-approved for Router');
    return;
  }

  console.log(`[Bot] Setting up max Router approvals for: ${needsApproval.join(', ')}`);
  for (const name of needsApproval) {
    try {
      const tc = getContract(TOKENS[name], OP_20_ABI, provider, network, wallet.address);
      const sim = await tc.increaseAllowance(Address.fromString(ROUTER), MAX_ALLOWANCE);
      if (sim.revert) {
        console.log(`[Bot] ${name} approve sim reverted:`, decodeRevert(sim.revert));
        continue;
      }
      const tx = await sim.sendTransaction({
        signer: wallet.keypair,
        mldsaSigner: wallet.mldsaKeypair,
        refundTo: wallet.p2tr,
        maximumAllowedSatToSpend: 100_000n,
        network,
      });
      console.log(`[Bot] ${name} approved -> Router (tx: ${tx.transactionId?.substring(0, 16)}...)`);
      approved[name] = tx.transactionId;
    } catch (e) {
      console.error(`[Bot] ${name} approval failed:`, e.message?.substring(0, 120));
    }
  }
  await writeFile(APPROVALS_FILE, JSON.stringify(approved, null, 2));
  console.log('[Bot] Approvals saved. Trades will only need swap TXs now.');
}

// ── Balance Refresh ──────────────────────────────────────────────────────────
async function refreshBalances() {
  // BTC balance
  try {
    const bal = await provider.getBalance(wallet.p2tr, false);
    const confirmed = bal?.confirmed ?? bal;
    state.balances.BTC = Number(confirmed || 0) / 1e8;
  } catch (e) {
    try {
      const utxos = await provider.getUTXOs(wallet.p2tr);
      let total = 0;
      if (utxos && Array.isArray(utxos)) {
        for (const u of utxos) total += Number(u.value || u.amount || 0);
      }
      state.balances.BTC = total / 1e8;
    } catch (_) {
      console.log('[Bot] BTC balance unavailable');
    }
  }

  // Token balances
  for (const [sym, addr] of Object.entries(TOKENS)) {
    try {
      const c = getContract(addr, OP_20_ABI, provider, network, wallet.address);
      const r = await c.balanceOf(wallet.address);
      if (!r.revert) {
        state.rawBalances[sym] = r.properties.balance;
        state.balances[sym] = fmt(r.properties.balance);
      }
    } catch (e) {
      console.log(`[Bot] ${sym} balance error:`, e.message?.substring(0, 60));
    }
  }

  // USD values
  for (const sym of ['MOTO', 'PILL', 'ODYS']) {
    state.balancesUsd[sym] = state.balances[sym] * (state.prices?.[sym]?.usd || 0);
  }
  state.balancesUsd.BTC = state.balances.BTC * state.btcPrice;

  console.log('[Bot] Balances:', Object.entries(state.balances).map(([k, v]) => `${k}=${typeof v === 'number' ? v.toFixed(4) : v}`).join(', '));
}

// ── MotoScope Scanner ────────────────────────────────────────────────────────
async function scan() {
  try {
    if (state.status !== 'executing') state.status = 'scanning';
    const resp = await fetch(`${MOTOSCOPE}/api/data`);
    if (!resp.ok) throw new Error(`HTTP ${resp.status}`);
    const data = await resp.json();

    if (data.arbitrage) {
      state.opportunities = {
        directArb: data.arbitrage.directArb || [],
        triangularArb: data.arbitrage.triangularArb || [],
        summary: data.arbitrage.summary || {},
        priceImpact: data.arbitrage.priceImpact || [],
        liquidityDepth: data.arbitrage.liquidityDepth || [],
      };
    }
    state.prices = data.prices || {};
    state.pools = data.pools || [];
    state.btcPrice = data.btcPrice?.usd || 95000;
    state.lastScan = Date.now();
    state.connected = true;
    if (state.status !== 'executing') state.status = 'idle';
    state.error = null;

    const viableCount = (state.opportunities.directArb || []).filter(a => a.isViable).length;
    const triViable = (state.opportunities.triangularArb || []).filter(a => a.isViable).length;
    console.log(`[Bot] Scan complete: ${viableCount} direct + ${triViable} tri-arb opportunities`);
  } catch (e) {
    state.connected = false;
    if (state.status !== 'executing') state.status = 'error';
    state.error = 'MotoScope: ' + e.message;
    console.error('[Bot] Scan failed:', e.message);
  }
}

// ── Trade Execution Helpers ──────────────────────────────────────────────────
function addStep(trade, label) {
  trade.steps.push({ label, status: 'pending', ts: Date.now() });
}

function updateStep(trade, status) {
  if (trade.steps.length) trade.steps[trade.steps.length - 1].status = status;
}

function completeStep(trade, tx, detail) {
  if (!trade.steps.length) return;
  const s = trade.steps[trade.steps.length - 1];
  s.status = 'done';
  s.completedAt = Date.now();
  if (tx) s.txId = tx.transactionId || tx.txId || tx.txid || '';
  if (detail) s.detail = detail;
}

function failTrade(trade, e) {
  trade.status = 'failed';
  trade.error = e.message;
  trade.completedAt = Date.now();
  if (trade.steps.length) {
    const last = trade.steps[trade.steps.length - 1];
    last.status = 'failed';
    last.error = e.message;
  }
  console.error(`[Bot] Trade ${trade.id} failed:`, e.message);
}

// Compute the correct profit for a single trade.
// Full-cycle arb (same token in & out): profit = (output - input) * price
// Conversion trades (different tokens): profit = value_received - value_spent
function computeTradeProfit(trade) {
  if (trade.status !== 'completed') return 0;

  const startToken = trade.startToken || trade.sellToken;
  const endToken = trade.endToken || trade.buyToken || 'MOTO';

  // Full cycle arb: same token in and out (e.g. MOTO → ODYS → PILL → MOTO)
  if (startToken === endToken) {
    let delta = 0;
    const pre = trade.preBalances?.[startToken] || 0;
    const post = trade.postBalances?.[startToken] || 0;
    delta = post - pre;

    // If balance didn't update yet (TX not confirmed), use quotedOutput - inputAmount
    if (Math.abs(delta) < 0.0001 && trade.quotedOutput && trade.inputAmount) {
      delta = trade.quotedOutput - trade.inputAmount;
    }
    // Still zero? Try actualOut - inputAmount
    if (Math.abs(delta) < 0.0001 && trade.actualOut > 0.0001 && trade.inputAmount) {
      delta = trade.actualOut - trade.inputAmount;
    }

    const price = state.prices?.[startToken]?.usd || 0;
    return delta * price;
  }

  // Conversion trade (e.g. ODYS → MOTO): intentional swap, NOT arbitrage loss.
  // These are token conversions the user chose to make, not failed arb trades.
  return 0;
}

function updatePnl(trade) {
  // Recompute profit using correct logic
  trade.actualProfit = computeTradeProfit(trade);
  const profit = trade.actualProfit;

  state.pnl.totalTrades++;
  state.pnl.totalProfit += profit;
  if (profit > 0.001) state.pnl.wins++;
  else if (profit < -0.001) state.pnl.losses++;

  if (!state.pnl.bestTrade || profit > (state.pnl.bestTrade.profit || -Infinity)) {
    state.pnl.bestTrade = { id: trade.id, profit, pool: trade.pool || trade.label };
  }
  if (!state.pnl.worstTrade || profit < (state.pnl.worstTrade.profit || Infinity)) {
    state.pnl.worstTrade = { id: trade.id, profit, pool: trade.pool || trade.label };
  }
}

// Prune old trades: drop everything before the first profitable trade.
// Those are just failed attempts / conversions from before the bot was working.
function pruneOldTrades() {
  // Trades are stored newest-first. Reverse to find first winner chronologically.
  const chronological = [...state.trades].reverse();
  let firstWinIdx = -1;
  for (let i = 0; i < chronological.length; i++) {
    const t = chronological[i];
    if (t.status === 'completed') {
      const profit = computeTradeProfit(t);
      if (profit > 0.01) { firstWinIdx = i; break; }
    }
  }
  if (firstWinIdx > 0) {
    const kept = chronological.slice(firstWinIdx);
    const pruned = state.trades.length - kept.length;
    state.trades = kept.reverse(); // back to newest-first
    console.log(`[Bot] Pruned ${pruned} pre-win trades, keeping ${state.trades.length}`);
  }
}

// Rebuild PnL from scratch using all stored trades (fixes bad persisted data)
function recalculatePnl() {
  state.pnl = { totalProfit: 0, totalTrades: 0, wins: 0, losses: 0, bestTrade: null, worstTrade: null };
  for (const trade of state.trades) {
    if (trade.status !== 'completed') continue;
    // Recompute and overwrite the stored profit
    trade.actualProfit = computeTradeProfit(trade);
    const profit = trade.actualProfit;
    state.pnl.totalTrades++;
    state.pnl.totalProfit += profit;
    if (profit > 0.001) state.pnl.wins++;
    else if (profit < -0.001) state.pnl.losses++;
    if (!state.pnl.bestTrade || profit > (state.pnl.bestTrade.profit || -Infinity)) {
      state.pnl.bestTrade = { id: trade.id, profit, pool: trade.pool || trade.label };
    }
    if (!state.pnl.worstTrade || profit < (state.pnl.worstTrade.profit || Infinity)) {
      state.pnl.worstTrade = { id: trade.id, profit, pool: trade.pool || trade.label };
    }
  }
  console.log(`[Bot] PnL recalculated: ${state.pnl.totalTrades} trades, $${state.pnl.totalProfit.toFixed(2)} total`);
}

async function saveTrades() {
  try {
    await writeFile(TRADES_FILE, JSON.stringify({ trades: state.trades.slice(0, 200), pnl: state.pnl }, null, 2));
  } catch (_) {}
}

async function loadTrades() {
  try {
    if (existsSync(TRADES_FILE)) {
      const d = JSON.parse(await readFile(TRADES_FILE, 'utf8'));
      state.trades = d.trades || [];
      console.log(`[Bot] Loaded ${state.trades.length} historical trades`);
      // ALWAYS recalculate PnL from trades (never trust persisted pnl)
      recalculatePnl();
      saveTrades(); // persist corrected data
    }
  } catch (_) {}
}

// ── Execute Direct Arbitrage ─────────────────────────────────────────────────
async function executeDirect(poolAddress) {
  if (state.status === 'executing') throw new Error('Already executing a trade');
  state.status = 'executing';

  const trade = {
    id: 'D' + Date.now(),
    type: 'direct',
    poolAddress,
    startedAt: Date.now(),
    steps: [],
    status: 'running',
  };
  state.executingTrade = trade;
  state.trades.unshift(trade);

  try {
    // Find opportunity
    const opp = state.opportunities.directArb.find(a => a.poolAddress === poolAddress);
    if (!opp) throw new Error('Opportunity not found for pool: ' + poolAddress);
    if (!opp.isViable) throw new Error('Opportunity is not viable (deviation < 0.6% or no profit)');

    trade.pool = opp.pool;
    trade.sellToken = opp.sellToken;
    trade.buyToken = opp.buyToken;
    trade.deviationPct = opp.deviationPct;
    trade.direction = opp.action;

    // Use actual wallet balance (capped at optimalInput if we have more)
    const currentBalance = state.balances[opp.sellToken] || 0;
    if (currentBalance <= 0) {
      throw new Error(`No ${opp.sellToken} balance. Fund the bot wallet first.`);
    }
    if (state.balances.BTC <= 0) {
      throw new Error('No BTC for gas fees. Fund the bot wallet first.');
    }

    // Use whatever we have — cap at optimal if we have more, else use full balance
    const actualInput = Math.min(currentBalance, opp.optimalInput || Infinity);
    const inputRatio = opp.optimalInput > 0 ? actualInput / opp.optimalInput : 1;

    trade.inputAmount = actualInput;
    trade.inputAmountUsd = (opp.optimalInputUsd || 0) * inputRatio;
    trade.expectedOutput = (opp.expectedOutput || 0) * inputRatio;
    trade.expectedOutputUsd = (opp.expectedOutputUsd || 0) * inputRatio;
    trade.expectedProfit = (opp.profitUsd || 0) * inputRatio;
    console.log(`[Bot] Using ${actualInput.toFixed(4)} ${opp.sellToken} (wallet balance${actualInput < opp.optimalInput ? ', less than optimal ' + opp.optimalInput.toFixed(0) : ''})`);

    trade.preBalances = { ...state.balances };

    const sellAddr = TOKENS[opp.sellToken];
    const buyAddr = TOKENS[opp.buyToken];
    if (!sellAddr || !buyAddr) throw new Error('Unknown token in pair');

    // Use exact raw on-chain balance (toBig float conversion causes rounding errors)
    const rawBal = state.rawBalances[opp.sellToken] || 0n;
    const amountIn = (rawBal > 0n && actualInput >= (state.balances[opp.sellToken] || 0))
      ? rawBal : toBig(actualInput);
    if (amountIn <= 0n) throw new Error('Calculated zero input amount');

    const slipFactor = state.config.slippagePct / 100;
    const minOut = toBig(trade.expectedOutput * (1 - slipFactor));
    const deadline = BigInt(Math.floor(Date.now() / 1000) + 3600);

    // ── Step 1: Get on-chain quote (approvals done at startup) ──
    addStep(trade, 'Getting on-chain quote');
    console.log(`[Bot] Quoting ${actualInput.toFixed(4)} ${opp.sellToken} -> ${opp.buyToken}...`);

    const routerContract = getContract(ROUTER, MOTOSWAP_ROUTER_ABI, provider, network, wallet.address);
    const quoteSim = await routerContract.getAmountsOut(
      amountIn,
      [Address.fromString(sellAddr), Address.fromString(buyAddr)]
    );

    let quotedOut = minOut;
    if (!quoteSim.revert && quoteSim.properties?.amountsOut?.length > 1) {
      quotedOut = quoteSim.properties.amountsOut[quoteSim.properties.amountsOut.length - 1];
      trade.quotedOutput = fmt(quotedOut);
      console.log(`[Bot] Quoted output: ${trade.quotedOutput} ${opp.buyToken}`);
    }
    completeStep(trade, null, trade.quotedOutput ? `${trade.quotedOutput.toFixed(4)} ${opp.buyToken}` : 'using estimate');

    // Adjust min output with slippage from quoted amount
    const adjustedMinOut = quotedOut > 0n
      ? quotedOut * BigInt(100 - state.config.slippagePct) / 100n
      : minOut;

    // ── Step 2: Execute swap ──
    addStep(trade, `Swapping ${opp.sellToken} for ${opp.buyToken}`);
    console.log(`[Bot] Executing swap...`);

    let swapTx;
    try {
      // Try simulation-based swap (works when allowance is confirmed)
      const swapSim = await routerContract.swapExactTokensForTokensSupportingFeeOnTransferTokens(
        amountIn,
        adjustedMinOut,
        [Address.fromString(sellAddr), Address.fromString(buyAddr)],
        wallet.address,
        deadline
      );
      if (swapSim.revert) throw new Error(decodeRevert(swapSim.revert));
      updateStep(trade, 'simulated');
      swapTx = await swapSim.sendTransaction({
        signer: wallet.keypair,
        mldsaSigner: wallet.mldsaKeypair,
        refundTo: wallet.p2tr,
        maximumAllowedSatToSpend: 100_000n,
        network,
      });
    } catch (simErr) {
      const msg = simErr.message || '';
      if (msg.includes('allowance') || msg.includes('Insufficient')) {
        // Allowance not yet confirmed — approve + swap in same block
        console.log('[Bot] Allowance pending, sending approve + swap in same block...');
        addStep(trade, `Approving ${opp.sellToken} (same-block)`);

        const tokenContract = getContract(sellAddr, OP_20_ABI, provider, network, wallet.address);
        const approveSim = await tokenContract.increaseAllowance(Address.fromString(ROUTER), amountIn);
        if (approveSim.revert) throw new Error('Approve reverted: ' + decodeRevert(approveSim.revert));
        await approveSim.sendTransaction({
          signer: wallet.keypair,
          mldsaSigner: wallet.mldsaKeypair,
          refundTo: wallet.p2tr,
          maximumAllowedSatToSpend: 100_000n,
          network,
        });
        completeStep(trade, null, 'approved');
        console.log('[Bot] Approve broadcast, now sending swap direct...');

        // Build swap calldata locally, bypass simulation
        const calldata = routerContract.encodeCalldata(
          'swapExactTokensForTokensSupportingFeeOnTransferTokens',
          [amountIn, adjustedMinOut, [Address.fromString(sellAddr), Address.fromString(buyAddr)], wallet.address, deadline]
        );
        swapTx = await sendSwapDirect(ROUTER, calldata);
      } else {
        throw simErr;
      }
    }
    completeStep(trade, swapTx);
    console.log('[Bot] Swap broadcast');

    // ── Step 4: Confirm balances ──
    addStep(trade, 'Confirming final balances');
    await refreshBalances();
    trade.postBalances = { ...state.balances };

    const sellDelta = (trade.postBalances[opp.sellToken] || 0) - (trade.preBalances[opp.sellToken] || 0);
    const buyDelta = (trade.postBalances[opp.buyToken] || 0) - (trade.preBalances[opp.buyToken] || 0);

    trade.actualIn = Math.abs(sellDelta);
    trade.actualOut = buyDelta;
    trade.netTokenDelta = opp.buyToken === 'MOTO' ? buyDelta : (trade.postBalances.MOTO || 0) - (trade.preBalances.MOTO || 0);

    // If balance didn't change yet (TX not confirmed), use quoted output estimate
    if (Math.abs(buyDelta) < 0.0001 && trade.quotedOutput) {
      trade.actualOut = trade.quotedOutput;
      console.log('[Bot] Balance not yet updated, using quoted estimate');
    }

    // computeTradeProfit handles the logic: cycle = real profit, conversion = $0
    trade.actualProfit = computeTradeProfit(trade);

    completeStep(trade, null, `+${trade.actualOut.toFixed(4)} ${opp.buyToken}`);

    trade.status = 'completed';
    trade.completedAt = Date.now();
    updatePnl(trade);

    console.log(`[Bot] Trade complete: ${opp.action}`);
    console.log(`[Bot]   In:  ${Math.abs(sellDelta).toFixed(4)} ${opp.sellToken}`);
    console.log(`[Bot]   Out: ${buyDelta.toFixed(4)} ${opp.buyToken}`);
    console.log(`[Bot]   P&L: $${trade.actualProfit.toFixed(2)}`);

  } catch (e) {
    failTrade(trade, e);
  }

  state.executingTrade = null;
  state.status = 'idle';
  saveTrades();
  return trade;
}

// ── Build multi-hop path to MOTO from whatever token we hold ─────────────
// Cycle direction: MOTO > ODYS > PILL > MOTO
// If we have MOTO → full cycle [MOTO, ODYS, PILL, MOTO] (3 hops, profit in MOTO)
// If we have ODYS → [ODYS, PILL, MOTO] (2 hops, convert to MOTO)
// If we have PILL → [PILL, MOTO] (1 hop, convert to MOTO)
// Always ends in MOTO.
function buildMotoPath(cycleSteps, balances) {
  const cycleTokens = cycleSteps.slice(0, -1).map(s => s.token); // e.g. ['MOTO','ODYS','PILL']
  const MIN_BAL = 0.01; // ignore dust balances

  // If we have MOTO, do the full cycle
  if ((balances['MOTO'] || 0) >= MIN_BAL) {
    return { startToken: 'MOTO', heldBalance: balances['MOTO'], steps: cycleSteps };
  }

  // Find which cycle token we hold, then build path from there → MOTO
  for (let i = 1; i < cycleTokens.length; i++) {
    const token = cycleTokens[i];
    if ((balances[token] || 0) >= MIN_BAL) {
      // Slice from index i to end (which is MOTO)
      const subPath = cycleSteps.slice(i); // e.g. if i=1: [{ODYS,..},{PILL,..},{MOTO,..}]
      return { startToken: token, heldBalance: balances[token], steps: subPath };
    }
  }
  return null; // no cycle tokens held
}

// ── Execute Triangular Arbitrage (single multi-hop swap, always ends in MOTO) ──
async function executeTri(label) {
  if (state.status === 'executing') throw new Error('Already executing a trade');
  state.status = 'executing';

  const trade = {
    id: 'R' + Date.now(),
    type: 'triangular',
    label,
    startedAt: Date.now(),
    steps: [],
    legs: [],
    status: 'running',
  };
  state.executingTrade = trade;
  state.trades.unshift(trade);

  try {
    const opp = state.opportunities.triangularArb.find(t => t.label === label);
    if (!opp) throw new Error('Tri-arb opportunity not found: ' + label);
    if (!opp.isViable) throw new Error('Tri-arb is not viable');
    if (state.balances.BTC <= 0) throw new Error('No BTC for gas fees');

    const origPath = opp.steps;
    if (!origPath || origPath.length < 4) throw new Error('Invalid tri-arb path (need 4+ steps)');

    // Build route: always ends in MOTO, starts from whatever we hold
    const route = buildMotoPath(origPath, state.balances);
    if (!route) {
      const held = Object.entries(state.balances).filter(([k,v]) => v > 0 && k !== 'BTC').map(([k]) => k);
      throw new Error(`No cycle token balance. You hold: ${held.join(', ') || 'nothing'}.`);
    }

    const { startToken, heldBalance, steps: routeSteps } = route;
    const endToken = routeSteps[routeSteps.length - 1].token; // always MOTO
    const walletBalance = heldBalance;
    const startAmount = Math.min(walletBalance, routeSteps[0].amount || Infinity);

    // Build multi-hop address path for the router
    const hopTokenNames = routeSteps.map(s => s.token);
    const routePath = hopTokenNames.map(t => Address.fromString(TOKENS[t]));
    const routeLabel = hopTokenNames.join(' > ');
    const numHops = routeSteps.length - 1;

    trade.inputAmount = startAmount;
    trade.startToken = startToken;
    trade.endToken = endToken;
    trade.expectedProfit = opp.profitUsd;
    trade.tokens = hopTokenNames;
    trade.direction = routeLabel;
    trade.preBalances = { ...state.balances };

    console.log(`[Bot] Tri-arb: ${routeLabel} (${numHops}-hop multi-hop, single TX)`);
    console.log(`[Bot] Input: ${startAmount.toFixed(4)} ${startToken} -> output: MOTO`);

    // Use exact raw on-chain balance (toBig float conversion causes rounding errors)
    const rawBal = state.rawBalances[startToken] || 0n;
    const amountIn = rawBal > 0n ? rawBal : toBig(startAmount);
    if (amountIn <= 0n) throw new Error('Zero input amount');
    console.log(`[Bot] Raw amount: ${amountIn.toString()}`);

    const slipFactor = state.config.slippagePct / 100;
    const deadline = BigInt(Math.floor(Date.now() / 1000) + 3600);

    // For full cycle (start=MOTO, end=MOTO): minOut = input * (1 - slippage)
    // For partial (start=ODYS, end=MOTO): use the last step's expected amount scaled
    let minOutEstimate;
    if (startToken === endToken) {
      minOutEstimate = toBig(startAmount * (1 - slipFactor));
    } else {
      // Scale end amount proportionally to input ratio
      const inputRatio = routeSteps[0].amount > 0 ? startAmount / routeSteps[0].amount : 1;
      const expectedEnd = (routeSteps[routeSteps.length - 1].amount || 0) * inputRatio;
      minOutEstimate = toBig(expectedEnd * (1 - slipFactor));
    }

    // ── Step 1: Get on-chain quote via router.getAmountsOut ──
    addStep(trade, `Quoting ${routeLabel}`);
    const rc = getContract(ROUTER, MOTOSWAP_ROUTER_ABI, provider, network, wallet.address);

    let quotedFinalOut = 0n;
    try {
      const quoteSim = await rc.getAmountsOut(amountIn, routePath);
      if (!quoteSim.revert && quoteSim.properties?.amountsOut?.length > 0) {
        const outs = quoteSim.properties.amountsOut;
        quotedFinalOut = outs[outs.length - 1];
        const quotedHuman = fmt(quotedFinalOut);
        trade.quotedOutput = quotedHuman;
        console.log(`[Bot] Quoted output: ${quotedHuman} ${endToken} (input: ${startAmount.toFixed(4)} ${startToken})`);
        completeStep(trade, null, `${quotedHuman.toFixed(4)} ${endToken}`);
      } else {
        console.log('[Bot] Quote reverted, using estimate');
        completeStep(trade, null, 'using estimate');
      }
    } catch (qErr) {
      console.log('[Bot] Quote failed:', qErr.message?.substring(0, 80));
      completeStep(trade, null, 'using estimate');
    }

    const adjustedMinOut = quotedFinalOut > 0n
      ? quotedFinalOut * BigInt(100 - state.config.slippagePct) / 100n
      : minOutEstimate;

    // ── Step 2: Execute single multi-hop swap ──
    addStep(trade, `Swapping ${routeLabel}`);
    console.log(`[Bot] Executing ${numHops}-hop swap: ${routeLabel}...`);

    let swapTx;
    try {
      const swapSim = await rc.swapExactTokensForTokensSupportingFeeOnTransferTokens(
        amountIn, adjustedMinOut, routePath, wallet.address, deadline
      );
      if (swapSim.revert) throw new Error(decodeRevert(swapSim.revert));
      updateStep(trade, 'simulated');
      swapTx = await swapSim.sendTransaction({
        signer: wallet.keypair, mldsaSigner: wallet.mldsaKeypair,
        refundTo: wallet.p2tr, maximumAllowedSatToSpend: 100_000n, network,
      });
    } catch (simErr) {
      const msg = simErr.message || '';
      if (msg.includes('allowance') || msg.includes('Insufficient')) {
        console.log('[Bot] Allowance pending, approve + swap same block...');
        addStep(trade, `Approving ${startToken} (same-block)`);
        const tc = getContract(TOKENS[startToken], OP_20_ABI, provider, network, wallet.address);
        const aSim = await tc.increaseAllowance(Address.fromString(ROUTER), amountIn);
        if (aSim.revert) throw new Error('Approve reverted: ' + decodeRevert(aSim.revert));
        await aSim.sendTransaction({
          signer: wallet.keypair, mldsaSigner: wallet.mldsaKeypair,
          refundTo: wallet.p2tr, maximumAllowedSatToSpend: 100_000n, network,
        });
        completeStep(trade, null, 'approved');

        const calldata = rc.encodeCalldata(
          'swapExactTokensForTokensSupportingFeeOnTransferTokens',
          [amountIn, adjustedMinOut, routePath, wallet.address, deadline]
        );
        swapTx = await sendSwapDirect(ROUTER, calldata);
      } else {
        throw simErr;
      }
    }
    completeStep(trade, swapTx);
    console.log('[Bot] Multi-hop swap broadcast');

    // ── Step 3: Confirm balances ──
    addStep(trade, 'Confirming final balances');
    await refreshBalances();
    trade.postBalances = { ...state.balances };

    // Track MOTO gain (always the output)
    const motoDelta = (trade.postBalances.MOTO || 0) - (trade.preBalances.MOTO || 0);
    trade.actualOut = motoDelta > 0 ? motoDelta : (trade.quotedOutput || trade.postBalances.MOTO || 0);
    trade.netTokenDelta = motoDelta > 0 ? motoDelta : (startToken === 'MOTO' ? 0 : (trade.quotedOutput || 0));

    // computeTradeProfit handles the logic: cycle = real profit, conversion = $0
    trade.actualProfit = computeTradeProfit(trade);

    completeStep(trade, null, `+${trade.actualOut.toFixed(4)} MOTO`);

    trade.status = 'completed';
    trade.completedAt = Date.now();
    updatePnl(trade);

    console.log(`[Bot] Tri-arb complete: ${routeLabel}`);
    console.log(`[Bot]   In:  ${startAmount.toFixed(4)} ${startToken}`);
    console.log(`[Bot]   Out: ${trade.actualOut.toFixed(4)} MOTO`);
    console.log(`[Bot]   P&L: $${trade.actualProfit.toFixed(2)} (${startToken === endToken ? 'arb' : 'conversion'})`);

  } catch (e) {
    failTrade(trade, e);
  }

  state.executingTrade = null;
  state.status = 'idle';
  saveTrades();
  return trade;
}

// ── Smart Swap: Auto-detect best route to MOTO ──────────────────────────────
async function findBestRoutes() {
  const routes = [];
  const rc = getContract(ROUTER, MOTOSWAP_ROUTER_ABI, provider, network, wallet.address);

  // Check which tokens we hold
  const held = {};
  for (const sym of ['MOTO', 'PILL', 'ODYS']) {
    const bal = state.balances[sym] || 0;
    const raw = state.rawBalances[sym] || 0n;
    if (bal >= 0.01 && raw > 0n) {
      held[sym] = { balance: bal, raw };
    }
  }

  if (Object.keys(held).length === 0) return routes;

  // For each held token, query all possible paths to MOTO
  for (const [sym, info] of Object.entries(held)) {
    const paths = [];

    if (sym === 'MOTO') {
      paths.push(['MOTO', 'ODYS', 'PILL', 'MOTO']); // full cycle
      paths.push(['MOTO', 'PILL', 'ODYS', 'MOTO']); // reverse cycle
    } else if (sym === 'ODYS') {
      paths.push(['ODYS', 'MOTO']);           // direct 1-hop
      paths.push(['ODYS', 'PILL', 'MOTO']);   // 2-hop via PILL
    } else if (sym === 'PILL') {
      paths.push(['PILL', 'MOTO']);           // direct 1-hop
      paths.push(['PILL', 'ODYS', 'MOTO']);   // 2-hop via ODYS
    }

    for (const pathTokens of paths) {
      try {
        const addrPath = pathTokens.map(t => Address.fromString(TOKENS[t]));
        const quote = await rc.getAmountsOut(info.raw, addrPath);
        if (!quote.revert && quote.properties?.amountsOut?.length > 0) {
          const outs = quote.properties.amountsOut;
          const outRaw = outs[outs.length - 1];
          const outHuman = fmt(outRaw);

          let motoGain, motoGainPct;
          if (sym === 'MOTO') {
            motoGain = outHuman - info.balance;
            motoGainPct = info.balance > 0 ? (motoGain / info.balance) * 100 : 0;
          } else {
            motoGain = outHuman;
            motoGainPct = 100; // converting all to MOTO
          }

          routes.push({
            startToken: sym,
            endToken: 'MOTO',
            path: pathTokens,
            label: pathTokens.join(' > '),
            hops: pathTokens.length - 1,
            inputRaw: info.raw.toString(),
            inputHuman: info.balance,
            outputRaw: outRaw.toString(),
            outputHuman: outHuman,
            motoGain,
            motoGainPct,
            motoOutput: outHuman,
            inputUsd: info.balance * (state.prices[sym]?.usd || 0),
            outputUsd: outHuman * (state.prices.MOTO?.usd || 0),
          });
        }
      } catch (e) {
        console.log(`[Bot] Route ${pathTokens.join('>')} quote failed:`, e.message?.substring(0, 80));
      }
    }
  }

  // Sort: non-MOTO tokens first (conversion priority), then by MOTO output
  routes.sort((a, b) => {
    // Prioritize converting non-MOTO tokens to MOTO
    if (a.startToken !== 'MOTO' && b.startToken === 'MOTO') return -1;
    if (a.startToken === 'MOTO' && b.startToken !== 'MOTO') return 1;
    // Among same category, sort by MOTO output
    return b.motoOutput - a.motoOutput;
  });

  return routes;
}

async function executeSmartSwap(routeLabel) {
  if (state.status === 'executing') throw new Error('Already executing a trade');
  state.status = 'executing';

  // Refresh balances first to get accurate rawBalances
  await refreshBalances();

  const routes = await findBestRoutes();
  const route = routeLabel ? routes.find(r => r.label === routeLabel) : routes[0];
  if (!route) throw new Error('No viable routes found. Ensure you have MOTO, PILL, or ODYS tokens.');

  const trade = {
    id: 'S' + Date.now(),
    type: 'smart-swap',
    label: route.label,
    startedAt: Date.now(),
    steps: [],
    status: 'running',
    startToken: route.startToken,
    sellToken: route.startToken,
    buyToken: 'MOTO',
    endToken: 'MOTO',
    direction: route.label,
    inputAmount: route.inputHuman,
    expectedOutput: route.outputHuman,
    expectedProfit: route.startToken === 'MOTO'
      ? route.motoGain * (state.prices.MOTO?.usd || 0)
      : route.outputUsd,
  };
  state.executingTrade = trade;
  state.trades.unshift(trade);

  try {
    trade.preBalances = { ...state.balances };

    const amountIn = BigInt(route.inputRaw);
    const minOut = BigInt(route.outputRaw) * BigInt(100 - state.config.slippagePct) / 100n;
    const deadline = BigInt(Math.floor(Date.now() / 1000) + 3600);
    const addrPath = route.path.map(t => Address.fromString(TOKENS[t]));

    const rc = getContract(ROUTER, MOTOSWAP_ROUTER_ABI, provider, network, wallet.address);

    // Step 1: Quote (already have it)
    addStep(trade, `Quoting ${route.label}`);
    trade.quotedOutput = route.outputHuman;
    console.log(`[Bot] Smart swap: ${route.label} | In: ${route.inputHuman.toFixed(4)} ${route.startToken} | Expected: ${route.outputHuman.toFixed(4)} MOTO`);
    completeStep(trade, null, `${route.outputHuman.toFixed(4)} MOTO expected`);

    // Step 2: Execute swap
    addStep(trade, `Swapping ${route.label}`);
    let swapTx;
    try {
      const swapSim = await rc.swapExactTokensForTokensSupportingFeeOnTransferTokens(
        amountIn, minOut, addrPath, wallet.address, deadline
      );
      if (swapSim.revert) throw new Error(decodeRevert(swapSim.revert));
      updateStep(trade, 'simulated');
      swapTx = await swapSim.sendTransaction({
        signer: wallet.keypair, mldsaSigner: wallet.mldsaKeypair,
        refundTo: wallet.p2tr, maximumAllowedSatToSpend: 100_000n, network,
      });
    } catch (simErr) {
      const msg = simErr.message || '';
      if (msg.includes('allowance') || msg.includes('Insufficient')) {
        console.log(`[Bot] Allowance pending for ${route.startToken}, approve + swap same block...`);
        addStep(trade, `Approving ${route.startToken}`);
        const tc = getContract(TOKENS[route.startToken], OP_20_ABI, provider, network, wallet.address);
        const aSim = await tc.increaseAllowance(Address.fromString(ROUTER), amountIn);
        if (aSim.revert) throw new Error('Approve reverted: ' + decodeRevert(aSim.revert));
        await aSim.sendTransaction({
          signer: wallet.keypair, mldsaSigner: wallet.mldsaKeypair,
          refundTo: wallet.p2tr, maximumAllowedSatToSpend: 100_000n, network,
        });
        completeStep(trade, null, 'approved');

        const calldata = rc.encodeCalldata(
          'swapExactTokensForTokensSupportingFeeOnTransferTokens',
          [amountIn, minOut, addrPath, wallet.address, deadline]
        );
        swapTx = await sendSwapDirect(ROUTER, calldata);
      } else {
        throw simErr;
      }
    }
    completeStep(trade, swapTx);
    console.log('[Bot] Smart swap TX broadcast');

    // Step 3: Wait briefly then confirm balances
    addStep(trade, 'Confirming balances');
    // Small delay to let regtest process the TX
    await new Promise(r => setTimeout(r, 3000));
    await refreshBalances();
    trade.postBalances = { ...state.balances };

    const motoDelta = (trade.postBalances.MOTO || 0) - (trade.preBalances.MOTO || 0);
    trade.actualOut = motoDelta > 0 ? motoDelta : route.outputHuman;
    trade.netTokenDelta = motoDelta > 0 ? motoDelta : (route.motoGain || route.outputHuman);

    // computeTradeProfit handles the logic: cycle = real profit, conversion = $0
    trade.actualProfit = computeTradeProfit(trade);

    completeStep(trade, null, `+${trade.actualOut.toFixed(4)} MOTO`);

    trade.status = 'completed';
    trade.completedAt = Date.now();
    updatePnl(trade);

    console.log(`[Bot] Smart swap complete: ${route.label}`);
    console.log(`[Bot]   In:  ${route.inputHuman.toFixed(4)} ${route.startToken}`);
    console.log(`[Bot]   MOTO delta: ${motoDelta.toFixed(4)}`);
    console.log(`[Bot]   P&L: $${trade.actualProfit.toFixed(2)} (${route.startToken === 'MOTO' ? 'arb' : 'conversion'})`);

  } catch (e) {
    failTrade(trade, e);
  }

  state.executingTrade = null;
  state.status = 'idle';
  saveTrades();
  return trade;
}

// ── Repeat Swap: Execute N swaps sequentially ────────────────────────────────
async function executeRepeatSwap(count, routeLabel, delayMs = 5000) {
  state.repeatProgress = { current: 0, total: count, trades: [], running: true };
  const results = [];

  for (let i = 0; i < count; i++) {
    state.repeatProgress.current = i + 1;
    console.log(`[Bot] Repeat swap ${i + 1}/${count}...`);

    try {
      const trade = await executeSmartSwap(routeLabel || null);
      results.push(trade);
      state.repeatProgress.trades.push(trade);

      // Stop early on failure or zero balance
      if (trade.status === 'failed') {
        console.log(`[Bot] Repeat swap ${i + 1} failed, stopping early.`);
        break;
      }

      const hasBal = Object.entries(state.balances)
        .some(([k, v]) => k !== 'BTC' && v >= 0.01);
      if (!hasBal) {
        console.log('[Bot] No token balance remaining, stopping repeat.');
        break;
      }

      // Wait between trades (except after last)
      if (i < count - 1) {
        await new Promise(r => setTimeout(r, delayMs));
      }
    } catch (e) {
      console.error(`[Bot] Repeat swap ${i + 1} error:`, e.message);
      results.push({ status: 'failed', error: e.message });
      state.repeatProgress.trades.push({ status: 'failed', error: e.message });
      break;
    }
  }

  state.repeatProgress.running = false;
  const successful = results.filter(t => t.status === 'completed').length;
  const failed = results.filter(t => t.status === 'failed').length;
  let totalMotoGained = 0;
  results.forEach(t => { totalMotoGained += (t.netTokenDelta || 0); });

  console.log(`[Bot] Repeat complete: ${successful}/${count} successful, ${totalMotoGained.toFixed(4)} MOTO total`);
  return { totalTrades: results.length, successful, failed, totalMotoGained, trades: results };
}

// ── HTTP Server ──────────────────────────────────────────────────────────────
const server = createServer(async (req, res) => {
  res.setHeader('Access-Control-Allow-Origin', '*');
  res.setHeader('Access-Control-Allow-Methods', 'GET, POST, OPTIONS');
  res.setHeader('Access-Control-Allow-Headers', 'Content-Type');

  if (req.method === 'OPTIONS') {
    res.writeHead(204);
    res.end();
    return;
  }

  const url = new URL(req.url, `http://localhost:${PORT}`);

  // ── Serve UI ──
  if (req.method === 'GET' && (url.pathname === '/' || url.pathname === '/index.html')) {
    try {
      const html = await readFile(join(__dirname, 'index.html'), 'utf8');
      res.writeHead(200, { 'Content-Type': 'text/html; charset=utf-8' });
      res.end(html);
    } catch (e) {
      res.writeHead(500, { 'Content-Type': 'text/plain' });
      res.end('Failed to read index.html: ' + e.message);
    }
    return;
  }

  // ── GET /api/status ──
  if (req.method === 'GET' && url.pathname === '/api/status') {
    res.writeHead(200, { 'Content-Type': 'application/json' });
    res.end(JSON.stringify(state));
    return;
  }

  // ── GET /api/trades ──
  if (req.method === 'GET' && url.pathname === '/api/trades') {
    res.writeHead(200, { 'Content-Type': 'application/json' });
    res.end(JSON.stringify(state.trades));
    return;
  }

  // ── GET /api/balances ──
  if (req.method === 'GET' && url.pathname === '/api/balances') {
    try {
      await refreshBalances();
      res.writeHead(200, { 'Content-Type': 'application/json' });
      res.end(JSON.stringify({ balances: state.balances, balancesUsd: state.balancesUsd }));
    } catch (e) {
      res.writeHead(500, { 'Content-Type': 'application/json' });
      res.end(JSON.stringify({ error: e.message }));
    }
    return;
  }

  // ── POST /api/execute ──
  if (req.method === 'POST' && url.pathname === '/api/execute') {
    const body = await readBody(req);
    if (!body.poolAddress) {
      res.writeHead(400, { 'Content-Type': 'application/json' });
      res.end(JSON.stringify({ error: 'Missing poolAddress' }));
      return;
    }
    try {
      const trade = await executeDirect(body.poolAddress);
      res.writeHead(200, { 'Content-Type': 'application/json' });
      res.end(JSON.stringify(trade));
    } catch (e) {
      res.writeHead(500, { 'Content-Type': 'application/json' });
      res.end(JSON.stringify({ error: e.message }));
    }
    return;
  }

  // ── POST /api/execute-tri ──
  if (req.method === 'POST' && url.pathname === '/api/execute-tri') {
    const body = await readBody(req);
    if (!body.label) {
      res.writeHead(400, { 'Content-Type': 'application/json' });
      res.end(JSON.stringify({ error: 'Missing label' }));
      return;
    }
    try {
      const trade = await executeTri(body.label);
      res.writeHead(200, { 'Content-Type': 'application/json' });
      res.end(JSON.stringify(trade));
    } catch (e) {
      res.writeHead(500, { 'Content-Type': 'application/json' });
      res.end(JSON.stringify({ error: e.message }));
    }
    return;
  }

  // ── POST /api/config ──
  if (req.method === 'POST' && url.pathname === '/api/config') {
    const body = await readBody(req);
    if (body.slippagePct != null) {
      state.config.slippagePct = Math.max(0.1, Math.min(50, Number(body.slippagePct)));
    }
    if (body.autoMode != null) {
      state.config.autoMode = Boolean(body.autoMode);
    }
    if (body.maxGasSats != null) {
      state.config.maxGasSats = Math.max(1000, Number(body.maxGasSats));
    }
    res.writeHead(200, { 'Content-Type': 'application/json' });
    res.end(JSON.stringify(state.config));
    return;
  }

  // ── POST /api/scan ──
  if (req.method === 'POST' && url.pathname === '/api/scan') {
    scan().then(() => refreshBalances());
    res.writeHead(200, { 'Content-Type': 'application/json' });
    res.end(JSON.stringify({ ok: true }));
    return;
  }

  // ── POST /api/reveal ──
  if (req.method === 'POST' && url.pathname === '/api/reveal') {
    try {
      const ok = await registerKeysAndSplit();
      await setupApprovals();
      if (ok) {
        await refreshBalances();
        res.writeHead(200, { 'Content-Type': 'application/json' });
        res.end(JSON.stringify({ ok: true, message: 'UTXOs split for gas management' }));
      } else {
        res.writeHead(400, { 'Content-Type': 'application/json' });
        res.end(JSON.stringify({ ok: false, error: 'No UTXOs available. Fund the wallet first.' }));
      }
    } catch (e) {
      res.writeHead(500, { 'Content-Type': 'application/json' });
      res.end(JSON.stringify({ ok: false, error: e.message }));
    }
    return;
  }

  // ── GET /api/routes ──
  if (req.method === 'GET' && url.pathname === '/api/routes') {
    try {
      const routes = await findBestRoutes();
      res.writeHead(200, { 'Content-Type': 'application/json' });
      res.end(JSON.stringify({ routes, balances: state.balances, rawBalances: Object.fromEntries(Object.entries(state.rawBalances).map(([k,v])=>[k,v.toString()])) }));
    } catch (e) {
      res.writeHead(500, { 'Content-Type': 'application/json' });
      res.end(JSON.stringify({ error: e.message, routes: [] }));
    }
    return;
  }

  // ── POST /api/smart-swap ──
  if (req.method === 'POST' && url.pathname === '/api/smart-swap') {
    const body = await readBody(req);
    try {
      const trade = await executeSmartSwap(body.route);
      res.writeHead(200, { 'Content-Type': 'application/json' });
      res.end(JSON.stringify(trade));
    } catch (e) {
      res.writeHead(500, { 'Content-Type': 'application/json' });
      res.end(JSON.stringify({ error: e.message }));
    }
    return;
  }

  // ── POST /api/repeat-swap ──
  if (req.method === 'POST' && url.pathname === '/api/repeat-swap') {
    const body = await readBody(req);
    const count = Math.max(1, Math.min(20, Number(body.count) || 1));
    const delayMs = Math.max(2000, Math.min(30000, Number(body.delayMs) || 5000));
    const routeLabel = body.route || null;

    if (state.repeatProgress?.running) {
      res.writeHead(409, { 'Content-Type': 'application/json' });
      res.end(JSON.stringify({ error: 'Repeat swap already running' }));
      return;
    }

    // Start repeat in background, respond immediately
    res.writeHead(200, { 'Content-Type': 'application/json' });
    res.end(JSON.stringify({ ok: true, count, delayMs }));
    executeRepeatSwap(count, routeLabel, delayMs).catch(e => {
      console.error('[Bot] Repeat swap error:', e.message);
      if (state.repeatProgress) state.repeatProgress.running = false;
    });
    return;
  }

  res.writeHead(404, { 'Content-Type': 'text/plain' });
  res.end('Not Found');
});

// ── Startup ──────────────────────────────────────────────────────────────────
console.log('[Bot] MotoArb - MotoSwap Arbitrage Bot');
console.log('[Bot] ════════════════════════════════════');

await initWallet();
await loadTrades();

// Reveal public key on-chain + split UTXOs (needed for OP20 deposits)
await refreshBalances();
if (state.balances.BTC > 0) {
  await registerKeysAndSplit();
  await setupApprovals();
  await refreshBalances();
} else {
  console.log('[Bot] No BTC balance yet. Fund P2TR address to reveal public key.');
}

// Initial scan (also fetches prices)
console.log('[Bot] Performing initial scan...');
await scan();

// Now that prices are loaded, prune old failed trades and recalculate PnL
pruneOldTrades();
recalculatePnl();
saveTrades();

// Periodic scan
setInterval(async () => {
  if (state.status !== 'executing') {
    await scan();
    await refreshBalances();
  }
}, SCAN_INTERVAL_MS);

server.listen(PORT, () => {
  console.log('[Bot] ════════════════════════════════════');
  console.log(`[Bot] Dashboard: http://localhost:${PORT}`);
  console.log(`[Bot] API:       http://localhost:${PORT}/api/status`);
  console.log(`[Bot] MotoScope: ${MOTOSCOPE}`);
  console.log('[Bot] ════════════════════════════════════');
});
