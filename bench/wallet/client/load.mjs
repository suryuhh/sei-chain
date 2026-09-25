// Sends presigned transactions open loop at RATE per second, each through viem 2.56.9's sendRawTransaction and
// then its default waitForTransactionReceipt, whatever the earlier waits are doing.
//   node load.mjs URL CHAINID FILE RATE
// FILE holds "rawhex hash" per line. After the last wait it re-reads every CHECK_EVERY-th receipt through a plain
// eth_getTransactionReceipt and compares it with what the wait returned. Prints one LOAD line.
import { readFileSync } from "node:fs";
import { createPublicClient, http, defineChain } from "viem";

const [url, chainArg, file, rateArg] = process.argv.slice(2);
const chainId = Number(chainArg);
const rate = Number(rateArg);
const CHECK_EVERY = 10;
const chain = defineChain({ id: chainId, name: "local", nativeCurrency: { name: "S", symbol: "S", decimals: 18 }, rpcUrls: { default: { http: [url] } } });
const client = createPublicClient({ chain, transport: http(url) });
const lines = readFileSync(file, "utf8").trim().split("\n").map((l) => l.split(" "));
const n = lines.length;

const res = new Array(n);
let inflight = 0, maxInflight = 0, inflightSum = 0, inflightSamples = 0, errors = 0;
const lags = [];
const sampler = setInterval(() => { inflightSum += inflight; inflightSamples++; }, 20);

async function one(i, lag) {
  const [rawHex, hash] = lines[i];
  inflight++; if (inflight > maxInflight) maxInflight = inflight;
  const t0 = performance.now();
  const w0 = Date.now();
  try {
    const h = await client.sendRawTransaction({ serializedTransaction: "0x" + rawHex });
    const r = await client.waitForTransactionReceipt({ hash: h });
    res[i] = { ms: performance.now() - t0, wall: Date.now() - w0, ok: r.status === "success", same: r.transactionHash === hash, r };
  } catch (e) {
    errors++;
    res[i] = { ms: performance.now() - t0, wall: Date.now() - w0, ok: false, same: false, err: String(e).slice(0, 200) };
  }
  inflight--;
}

const start = performance.now();
const pending = [];
let next = 0;
await new Promise((resolve) => {
  const tick = () => {
    const now = performance.now();
    while (next < n && start + (next * 1000) / rate <= now) {
      const lag = now - (start + (next * 1000) / rate);
      lags.push(lag);
      pending.push(one(next, lag));
      next++;
    }
    if (next >= n) return resolve();
    setTimeout(tick, 2);
  };
  tick();
});
const sendSpan = (performance.now() - start) / 1000;
await Promise.all(pending);
const span = (performance.now() - start) / 1000;
clearInterval(sampler);

let checkN = 0, checkOk = 0;
const hx = (v) => (v === null || v === undefined ? null : "0x" + BigInt(v).toString(16));
for (let i = 0; i < n; i += CHECK_EVERY) {
  if (!res[i] || !res[i].r) continue;
  checkN++;
  const body = JSON.stringify({ jsonrpc: "2.0", id: i, method: "eth_getTransactionReceipt", params: [lines[i][1]] });
  const p = await (await fetch(url, { method: "POST", headers: { "content-type": "application/json" }, body })).json();
  const a = res[i].r, b = p.result;
  if (b && a.transactionHash === b.transactionHash && a.blockHash === b.blockHash && hx(a.blockNumber) === b.blockNumber &&
      hx(a.gasUsed) === b.gasUsed && hx(a.cumulativeGasUsed) === b.cumulativeGasUsed && hx(a.transactionIndex) === b.transactionIndex &&
      b.status === "0x1") checkOk++;
}
const ms = res.filter((x) => x).map((x) => x.ms).sort((a, b) => a - b);
const q = (p) => (ms.length ? ms[Math.min(ms.length - 1, Math.floor(p * (ms.length - 1)))] : -1);
const walls = res.filter((x) => x).map((x) => x.wall).sort((a, b) => a - b);
const wallP50 = walls.length ? walls[Math.floor(0.5 * (walls.length - 1))] : -1;
lags.sort((a, b) => a - b);
const lagP99 = lags.length ? lags[Math.floor(0.99 * (lags.length - 1))] : -1;
const done = res.filter((x) => x).length;
const statusOk = res.filter((x) => x && x.ok).length;
const hashOk = res.filter((x) => x && x.same).length;
const firstErr = (res.find((x) => x && x.err) || {}).err || "";
console.log(`LOAD n=${n} done=${done} status_ok=${statusOk} hash_ok=${hashOk} errors=${errors} check_n=${checkN} check_ok=${checkOk} ` +
  `wait_p50_ms=${q(0.5).toFixed(2)} wait_p90_ms=${q(0.9).toFixed(2)} wait_p99_ms=${q(0.99).toFixed(2)} wait_mean_ms=${(ms.reduce((a, b) => a + b, 0) / Math.max(1, ms.length)).toFixed(2)} ` +
  `wall_p50_ms=${wallP50} max_inflight=${maxInflight} mean_inflight=${(inflightSum / Math.max(1, inflightSamples)).toFixed(1)} lag_p99_ms=${lagP99.toFixed(1)} send_span_s=${sendSpan.toFixed(3)} span_s=${span.toFixed(3)}`);
if (firstErr) console.log(`LOAD_FIRST_ERROR ${firstErr.replace(/\s+/g, " ")}`);
process.exit(0);
