// Presigns one 21,000-gas legacy transfer from each key FROM..TO-1 (nonce 0), one line "rawhex hash" per key,
// in key order.
//   node sign.mjs CHAINID GAS_PRICE_WEI FROM TO FILE
import { writeFileSync } from "node:fs";
import { ethers } from "ethers";
const [chainArg, priceArg, fromArg, toArg, file] = process.argv.slice(2);
const chainId = Number(chainArg);
const to = "0x1000000000000000000000000000000000000001";
const out = [];
for (let i = Number(fromArg); i < Number(toArg); i++) {
  const w = new ethers.Wallet(ethers.id("evawait-" + i));
  const raw = await w.signTransaction({ type: 0, chainId, nonce: 0, to, value: 1n, gasLimit: 21000n, gasPrice: BigInt(priceArg) });
  out.push(raw.slice(2) + " " + ethers.keccak256(raw));
}
writeFileSync(file, out.join("\n") + "\n");
console.log(`SIGNED n=${out.length}`);
