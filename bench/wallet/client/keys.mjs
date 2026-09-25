// The benchmark's EVM keys. Key i is keccak256("evawait-" + i); prints "i address" per key.
//   node keys.mjs N
import { ethers } from "ethers";
const n = Number(process.argv[2]);
for (let i = 0; i < n; i++) console.log(`${i} ${new ethers.Wallet(ethers.id("evawait-" + i)).address}`);
