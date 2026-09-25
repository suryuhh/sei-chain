// Times send -> receipt in hand through each library's own default wait, then re-reads every receipt through a
// plain eth_getTransactionReceipt once all waits are done and compares it with what the wait returned.
//   node wait.mjs URL LIB N KEYHEX CHAINID   (PAUSE_MS: idle time before each send; JITTER_MS: up to this much more)
// ethers 6.17.0: JsonRpcProvider.broadcastTransaction(raw).wait(); viem 2.56.9: sendRawTransaction + waitForTransactionReceipt.
import { ethers } from "ethers";
import { createPublicClient, http, defineChain } from "viem";

const [url, lib, nArg, keyHex, chainArg] = process.argv.slice(2);
const n = Number(nArg);
const chainId = Number(chainArg);
const pause = Number(process.env.PAUSE_MS || 0);
const wallet = new ethers.Wallet(keyHex);
const to = "0x1000000000000000000000000000000000000001";
const provider = new ethers.JsonRpcProvider(url, chainId, { staticNetwork: true });
const chain = defineChain({ id: chainId, name: "local", nativeCurrency: { name: "S", symbol: "S", decimals: 18 }, rpcUrls: { default: { http: [url] } } });
const client = createPublicClient({ chain, transport: http(url) });

const hx = (v) => (v === null || v === undefined ? null : "0x" + BigInt(v).toString(16));
let nonce = Number(await provider.send("eth_getTransactionCount", [wallet.address, "latest"]));
const got = [];
for (let i = 0; i < n; i++) {
  const jitter = Math.random() * Number(process.env.JITTER_MS || 400);  // a send lands at a random phase of the block, never locked to it
  if (pause + jitter > 0) await new Promise((r) => setTimeout(r, pause + jitter));
  const raw = await wallet.signTransaction({ type: 0, chainId, nonce, to, value: 1n, gasLimit: 21000n, gasPrice: BigInt(process.env.GAS_PRICE_WEI || "2000000000") });
  nonce++;
  const hash = ethers.keccak256(raw);
  const w0 = Date.now();
  const t0 = performance.now();
  let rc;
  if (lib === "ethers") {
    const resp = await provider.broadcastTransaction(raw);
    const r = await resp.wait();
    rc = { hash: r.hash, blockHash: r.blockHash, blockNumber: hx(r.blockNumber), status: hx(r.status), gasUsed: hx(r.gasUsed), transactionIndex: hx(r.index), cumulativeGasUsed: hx(r.cumulativeGasUsed) };
  } else {
    const h = await client.sendRawTransaction({ serializedTransaction: raw });
    const r = await client.waitForTransactionReceipt({ hash: h });
    rc = { hash: r.transactionHash, blockHash: r.blockHash, blockNumber: hx(r.blockNumber), status: r.status === "success" ? "0x1" : "0x0", gasUsed: hx(r.gasUsed), transactionIndex: hx(r.transactionIndex), cumulativeGasUsed: hx(r.cumulativeGasUsed) };
  }
  const ms = performance.now() - t0;
  const wall = Date.now() - w0;
  got.push({ i, sent: hash, rc });
  console.log(`WAIT i=${i} wait_ms=${ms.toFixed(2)} wall_ms=${wall} status=${rc.status} hash=${rc.hash} sent=${hash} block=${rc.blockNumber}`);
}
for (const g of got) {
  const r = await provider.send("eth_getTransactionReceipt", [g.sent]);
  const plain = r && { hash: r.transactionHash, blockHash: r.blockHash, blockNumber: hx(r.blockNumber), status: hx(r.status), gasUsed: hx(r.gasUsed), transactionIndex: hx(r.transactionIndex), cumulativeGasUsed: hx(r.cumulativeGasUsed) };
  const same = plain !== null && g.rc.hash === g.sent && JSON.stringify(plain) === JSON.stringify(g.rc);
  console.log(`CHECK i=${g.i} ${same ? "ok" : "mismatch"} waited=${JSON.stringify(g.rc)} plain=${JSON.stringify(plain)}`);
}
console.log(`CLIENT_DONE n=${n}`);
process.exit(0);
