// index.js
const { ethers } = require("ethers");
const mongoose = require("mongoose");
require("dotenv").config();

// MongoDB Schemas
const TokenSchema = new mongoose.Schema({
  tokenId: { type: String, required: true, unique: true },
  currentOwner: { type: String, required: true },
  lastTransferBlock: { type: Number, required: true },
});

const TransferSchema = new mongoose.Schema({
  tokenId: { type: String, required: true },
  from: { type: String, required: true },
  to: { type: String, required: true },
  blockNumber: { type: Number, required: true },
  transactionHash: { type: String, required: true },
  timestamp: { type: Date, default: Date.now },
});

const Token = mongoose.model("Token", TokenSchema);
const Transfer = mongoose.model("Transfer", TransferSchema);

const contractABI = [
  "event Transfer(address indexed from, address indexed to, uint256 indexed tokenId)",
  "function totalSupply() view returns (uint256)",
];

async function startIndexing() {
  try {
    await mongoose.connect(process.env.MONGODB_URI);
    console.log("Connected to MongoDB");

    const provider = new ethers.JsonRpcProvider(process.env.RPC_URL);
    const contract = new ethers.Contract(
      process.env.CONTRACT_ADDRESS,
      contractABI,
      provider
    );

    const lastTransfer = await Transfer.findOne().sort({ blockNumber: -1 });
    const startBlock = lastTransfer ? lastTransfer.blockNumber + 1 : 0;

    await indexHistoricalEvents(contract, startBlock);

    contract.on("Transfer", async (from, to, tokenId, event) => {
      try {
        await processTransferEvent(from, to, tokenId, event);
      } catch (error) {
        console.error("Error processing transfer event:", error);
      }
    });
  } catch (error) {
    console.error("Error in startIndexing:", error);
    process.exit(1);
  }
}

async function indexHistoricalEvents(contract, fromBlock) {
  try {
    const filter = contract.filters.Transfer();
    const events = await contract.queryFilter(filter, fromBlock);

    for (const event of events) {
      await processTransferEvent(
        event.args[0],
        event.args[1],
        event.args[2],
        event
      );
    }
    console.log(`Indexed ${events.length} historical events`);
  } catch (error) {
    console.error("Error indexing historical events:", error);
  }
}

async function processTransferEvent(from, to, tokenId, event) {
  const transfer = new Transfer({
    tokenId: tokenId.toString(),
    from: from.toLowerCase(),
    to: to.toLowerCase(),
    blockNumber: event.blockNumber,
    transactionHash: event.transactionHash,
  });

  await transfer.save();

  await Token.findOneAndUpdate(
    { tokenId: tokenId.toString() },
    {
      tokenId: tokenId.toString(),
      currentOwner: to.toLowerCase(),
      lastTransferBlock: event.blockNumber,
    },
    { upsert: true }
  );

  console.log(`Indexed transfer of token ${tokenId} from ${from} to ${to}`);
}

async function getTokensByOwner(ownerAddress) {
  return Token.find({
    currentOwner: ownerAddress.toLowerCase(),
  }).select("tokenId");
}

startIndexing().catch(console.error);
