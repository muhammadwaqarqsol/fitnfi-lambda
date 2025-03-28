require("dotenv").config();
const {Web3} = require("web3");
const axios = require("axios");
const { Pool } = require("pg");

// Load environment variables
const RPC_URL = process.env.RPC_URL;
const SECRET_KEY = process.env.SECRET_KEY;
const DATABASE_URL = process.env.DATABASE_URL;
const CONTRACT_ADDRESS = process.env.CONTRACT_ADDRESS;
const API_URL = process.env.API_URL;
// Check required variables
if (!RPC_URL || !SECRET_KEY || !DATABASE_URL || !CONTRACT_ADDRESS) {
  console.error("❌ Missing environment variables");
  process.exit(1);
}

// Initialize Web3
let web3, contract;
try {
  web3 = new Web3(RPC_URL);
  const ERC1155_ABI = [
    {
      constant: true,
      inputs: [
        { name: "account", type: "address" },
        { name: "id", type: "uint256" },
      ],
      name: "balanceOf",
      outputs: [{ name: "", type: "uint256" }],
      stateMutability: "view",
      type: "function",
    },
  ];
  contract = new web3.eth.Contract(ERC1155_ABI, CONTRACT_ADDRESS);
  console.log("✅ Web3 initialized successfully");
} catch (error) {
  console.error("❌ Web3 initialization failed:", error);
}
async function getUserBalance(userAddress, orgId) {
  try {
    const balance = await contract.methods.balanceOf(userAddress, orgId).call();
    console.log(`✅ Balance for ${userAddress}, org ${orgId}:`, balance);
    return balance;
  } catch (error) {
    console.error("❌ Error fetching balance:", error);
  }
}

// Initialize PostgreSQL connection
const pool = new Pool({
  connectionString: DATABASE_URL,
});

pool.on("error", (err) => {
  console.error("❌ Unexpected database error:", err);
  process.exit(1);
});

async function testDatabase() {
  try {
    const client = await pool.connect();
    const result = await client.query("SELECT NOW()");
    console.log("✅ Database connected:", result.rows[0]);
    client.release();
  } catch (error) {
    console.error("❌ Database connection failed:", error);
  }
}


async function testApiCall() {
  const url = `${API_URL}`;
  const body = {
    user_id: "123",
    amount: "100",
    new_balance: "500",
    org_id: "1",
  };

  const headers = {
    Authorization: `Bearer ${SECRET_KEY}`,
    Accept: "application/json",
    "Content-Type": "application/json",
  };

  try {
    const response = await axios.post(url, body, { headers });
    console.log("✅ API call successful:", response.data);
  } catch (error) {
    console.error("❌ API call failed:", error.response?.data || error.message);
  }
}

// Run tests
(async () => {
  await testDatabase();
  await getUserBalance("0xEdb8373211332CC6F141CEBB7B8587C7CFb68243", 1);
  await testApiCall();
  pool.end();
})();
