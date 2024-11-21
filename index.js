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

const FailedEventSchema = new mongoose.Schema({
  tokenId: { type: String, required: true },
  from: { type: String, required: true },
  to: { type: String, required: true },
  blockNumber: { type: Number, required: true },
  transactionHash: { type: String, required: true },
  error: { type: String },
  retryCount: { type: Number, default: 0 },
  timestamp: { type: Date, default: Date.now },
});

const Token = mongoose.model("Token", TokenSchema);
const Transfer = mongoose.model("Transfer", TransferSchema);
const FailedEvent = mongoose.model("FailedEvent", FailedEventSchema);

const contractABI = [
  "event Transfer(address indexed from, address indexed to, uint256 indexed tokenId)",
  "function totalSupply() view returns (uint256)",
  "function ownerOf(uint256 tokenId) view returns (address)",
];

async function createIndexes() {
  await Token.collection.createIndex({ currentOwner: 1 });
  await Transfer.collection.createIndex({ blockNumber: -1 });
  await Transfer.collection.createIndex({ tokenId: 1 });
  await FailedEvent.collection.createIndex({ timestamp: 1 });
}

async function startIndexing() {
  let provider;
  let contract;

  while (true) {
    try {
      await mongoose.connect(process.env.MONGODB_URI, {
        serverSelectionTimeoutMS: 5000,
        retryWrites: true,
        maxPoolSize: 10,
      });

      console.log("Connected to MongoDB");
      await createIndexes();

      // Simplified provider initialization
      provider = new ethers.JsonRpcProvider(process.env.RPC_URL);

      contract = new ethers.Contract(
        process.env.CONTRACT_ADDRESS,
        contractABI,
        provider
      );

      const lastTransfer = await Transfer.findOne().sort({ blockNumber: -1 });
      const startBlock = lastTransfer ? lastTransfer.blockNumber + 1 : 0;

      const latestBlock = await provider.getBlockNumber();
      const CHUNK_SIZE = 2000;

      for (let from = startBlock; from <= latestBlock; from += CHUNK_SIZE) {
        const to = Math.min(from + CHUNK_SIZE - 1, latestBlock);
        await indexHistoricalEvents(contract, from, to);
      }

      setupEventListener(contract);
      startPeriodicTasks(contract);

      break;
    } catch (error) {
      console.error("Critical error in startIndexing:", error);
      await cleanup();
      await new Promise((resolve) => setTimeout(resolve, 10000));
    }
  }
}

function setupEventListener(contract) {
  contract.on("Transfer", async (from, to, tokenId, event) => {
    try {
      await processTransferEvent(from, to, tokenId, event);
    } catch (error) {
      console.error("Error processing transfer event:", error);
      await storeFailedEvent(from, to, tokenId, event, error);
    }
  });

  contract.provider.on("error", async (error) => {
    console.error("Provider error:", error);
    await cleanup();
    setTimeout(() => startIndexing(), 5000);
  });
}

function startPeriodicTasks(contract) {
  setInterval(async () => {
    await validateAndRepairData(contract);
  }, 3600000);

  setInterval(async () => {
    await retryFailedEvents(contract);
  }, 300000);
}

async function indexHistoricalEvents(contract, fromBlock, toBlock) {
  const retries = 3;

  for (let attempt = 1; attempt <= retries; attempt++) {
    try {
      const filter = contract.filters.Transfer();
      const events = await contract.queryFilter(filter, fromBlock, toBlock);

      for (const event of events) {
        await processTransferEvent(
          event.args[0],
          event.args[1],
          event.args[2],
          event
        );
      }

      console.log(`Indexed blocks ${fromBlock} to ${toBlock}`);
      return;
    } catch (error) {
      console.error(
        `Error in attempt ${attempt} for blocks ${fromBlock}-${toBlock}:`,
        error
      );
      if (attempt === retries) throw error;
      await new Promise((resolve) => setTimeout(resolve, 2000 * attempt));
    }
  }
}

async function processTransferEvent(from, to, tokenId, event) {
  const session = await mongoose.startSession();
  session.startTransaction();

  try {
    const transfer = new Transfer({
      tokenId: tokenId.toString(),
      from: from.toLowerCase(),
      to: to.toLowerCase(),
      blockNumber: event.blockNumber,
      transactionHash: event.transactionHash,
    });

    await transfer.save({ session });

    await Token.findOneAndUpdate(
      { tokenId: tokenId.toString() },
      {
        tokenId: tokenId.toString(),
        currentOwner: to.toLowerCase(),
        lastTransferBlock: event.blockNumber,
      },
      { upsert: true, session }
    );

    await session.commitTransaction();
    console.log(`Indexed transfer of token ${tokenId} from ${from} to ${to}`);
  } catch (error) {
    await session.abortTransaction();
    throw error;
  } finally {
    session.endSession();
  }
}

async function storeFailedEvent(from, to, tokenId, event, error) {
  try {
    const failedEvent = new FailedEvent({
      tokenId: tokenId.toString(),
      from: from.toLowerCase(),
      to: to.toLowerCase(),
      blockNumber: event.blockNumber,
      transactionHash: event.transactionHash,
      error: error.message,
    });
    await failedEvent.save();
  } catch (storeError) {
    console.error("Error storing failed event:", storeError);
  }
}

async function retryFailedEvents(contract) {
  const failedEvents = await FailedEvent.find({
    retryCount: { $lt: 3 },
    timestamp: { $lt: new Date(Date.now() - 300000) },
  });

  for (const event of failedEvents) {
    try {
      await processTransferEvent(event.from, event.to, event.tokenId, {
        blockNumber: event.blockNumber,
        transactionHash: event.transactionHash,
      });
      await FailedEvent.findByIdAndDelete(event._id);
    } catch (error) {
      await FailedEvent.findByIdAndUpdate(event._id, {
        $inc: { retryCount: 1 },
        error: error.message,
      });
    }
  }
}

async function validateAndRepairData(contract) {
  try {
    const totalSupply = await contract.totalSupply();

    for (let i = 0; i < totalSupply; i++) {
      const tokenId = i.toString();
      const onchainOwner = await contract.ownerOf(tokenId);

      const dbToken = await Token.findOne({ tokenId });
      if (
        !dbToken ||
        dbToken.currentOwner.toLowerCase() !== onchainOwner.toLowerCase()
      ) {
        console.log(`Repairing data for token ${tokenId}`);
        await Token.findOneAndUpdate(
          { tokenId },
          {
            currentOwner: onchainOwner.toLowerCase(),
            lastTransferBlock: await contract.provider.getBlockNumber(),
          },
          { upsert: true }
        );
      }
    }
  } catch (error) {
    console.error("Error in validation:", error);
  }
}

async function getTokensByOwner(ownerAddress) {
  return Token.find({
    currentOwner: ownerAddress.toLowerCase(),
  }).select("tokenId");
}

async function cleanup() {
  try {
    if (mongoose.connection.readyState !== 0) {
      await mongoose.connection.close();
    }
  } catch (error) {
    console.error("Error in cleanup:", error);
  }
}

process.on("SIGINT", async () => {
  console.log("Shutting down gracefully...");
  await cleanup();
  process.exit(0);
});

process.on("unhandledRejection", (error) => {
  console.error("Unhandled promise rejection:", error);
});

startIndexing().catch(console.error);
