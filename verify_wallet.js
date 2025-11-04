import { ethers } from 'ethers';
import dotenv from 'dotenv';

dotenv.config();

const privateKey = process.env.HL_PRIVATE_KEY;
const wallet = new ethers.Wallet(privateKey);

console.log('Private key:', privateKey);
console.log('Derived wallet address:', wallet.address);
console.log('Expected wallet address:', process.env.HL_WALLET);
console.log('Match:', wallet.address.toLowerCase() === process.env.HL_WALLET.toLowerCase());
