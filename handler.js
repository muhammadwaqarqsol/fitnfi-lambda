const { Web3 } = require("web3");
const axios = require("axios");
const { Pool } = require("pg");

// Environment variables
const RPC_URL = process.env.RPC_URL;
const SECRET_KEY = process.env.SECRET_KEY;
const DATABASE_URL = process.env.DATABASE_URL;
const CONTRACT_ADDRESS = process.env.CONTRACT_ADDRESS;
const API_URL = process.env.API_URL;

// Validate environment variables
if (!RPC_URL || !SECRET_KEY || !DATABASE_URL || !CONTRACT_ADDRESS) {
  const missing = [
    !RPC_URL ? "RPC_URL" : null,
    !SECRET_KEY ? "SECRET_KEY" : null,
    !DATABASE_URL ? "DATABASE_URL" : null,
    !CONTRACT_ADDRESS ? "CONTRACT_ADDRESS" : null,
  ]
    .filter(Boolean)
    .join(", ");
  console.error(`Missing required environment variables: ${missing}`);
}

// Initialize Web3 and database connection outside of handler function
let web3;
let contract;
pool = new Pool({
    connectionString: DATABASE_URL,
});

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



} catch (error) {
  console.error("Failed to initialize web3 or database:", error);
}

async function getUserBalance(userAddress, orgId) {
  try {
    return await contract.methods.balanceOf(userAddress, orgId).call();
  } catch (error) {
    console.error("Error fetching balance from blockchain:", error);
    throw error;
  }
}

async function sendWalletTopupApi(userId, amount, newBalance, orgId, token) {
  const url = `${API_URL}`;
  if (!url) {
    throw new Error("API_URL is not defined");
  }

  const body = {
    user_id: userId.toString(),
    amount: amount.toString(),
    new_balance: newBalance.toString(),
    org_id: orgId.toString(),
  };

  const headers = {
    Authorization: `Bearer ${token}`,
    Accept: "application/json",
    "Content-Type": "application/json",
  };

  try {
    const response = await axios.post(url, body, { headers });
    console.log("API call successful:", response.data);
    return response.data;
  } catch (error) {
    console.error(
      "Error calling the API:",
      error.response?.data || error.message
    );
    throw error; // Rethrow to handle in the calling function
  }
}

module.exports.sqsProcessor = async (event) => {
  // Validate event input
  if (!event || !event.Records || !Array.isArray(event.Records)) {
    console.error("Invalid event structure:", event);
    return { statusCode: 400, body: "Invalid event structure" };
  }

  console.log(`Processing ${event.Records.length} SQS messages`);

  // Track failures for logging
  const successfulRecords = [];
  const failedRecords = [];

  for (const record of event.Records) {
    let client = null;

    try {
      // Parse message body with error handling
      let messageBody;
      try {
        messageBody = JSON.parse(record.body);
      } catch (parseError) {
        console.error("Failed to parse SQS message body:", parseError);
        failedRecords.push({
          messageId: record.messageId,
          error: "Invalid JSON",
        });
        continue;
      }

      // Validate message fields
      const { transactionHash, status, auth_token } = messageBody;

      // Log processing attempt
      console.log(`Processing message: ${record.messageId}`, {
        transactionHash: transactionHash || "missing",
        status: typeof status === "boolean" ? status : "invalid type",
        authenticated: auth_token === SECRET_KEY,
      });

      // Authentication check
      if (!auth_token || auth_token !== SECRET_KEY) {
        console.warn("🚨 Unauthorized SQS message detected!", { auth_token });
        failedRecords.push({
          messageId: record.messageId,
          error: "Unauthorized",
        });
        continue;
      }

      // Message validation
      if (!transactionHash || typeof status !== "boolean") {
        console.error("❌ Invalid message structure:", messageBody);
        failedRecords.push({
          messageId: record.messageId,
          error: "Invalid structure",
        });
        continue;
      }

      // Acquire a client from the pool
      client = await pool.connect();

      // Check if transaction exists in database
      const query = "SELECT * FROM transaction WHERE transaction_hash = $1";
      const { rows } = await client.query(query, [transactionHash]);

      if (rows.length === 0) {
        console.log(`Transaction not found for hash: ${transactionHash}`);
        failedRecords.push({
          messageId: record.messageId,
          error: "Transaction not found",
        });
        continue;
      }

      // Skip if already processed successfully
      if (rows[0].status === "success") {
        console.log(
          `Transaction ${transactionHash} is already successful. Skipping.`
        );
        successfulRecords.push({
          messageId: record.messageId,
          status: "already processed",
        });
        continue;
      }

      // Extract transaction data
      const transactionData = rows[0];
      const {
        org_id,
        from,
        to,
        value,
        invoice_id,
        transaction_id,
        user_id,
        jwt_token,
      } = transactionData;

      // Set transaction status based on input
      const transactionStatus = status ? "success" : "failed";

      // Get new balance if transaction is successful
      let newBalance = "0";
      if (status) {
        try {
          // Validate blockchain address format
          if (!to || !to.startsWith("0x") || to.length !== 42) {
            throw new Error(`Invalid recipient blockchain address: ${to}`);
          }

          // Validate org_id is a number
          if (isNaN(Number(org_id))) {
            throw new Error(`Invalid org_id: ${org_id}`);
          }

          newBalance = await getUserBalance(to, org_id);
        } catch (error) {
          console.error("Error querying blockchain:", error);
          failedRecords.push({
            messageId: record.messageId,
            error: `Blockchain query error: ${error.message}`,
          });
          continue;
        }
      }

      // Begin transaction
      await client.query("BEGIN");

      try {
        // Update transaction status
        const updateQuery = `
          UPDATE transaction
          SET status = $1
          WHERE transaction_hash = $2
          RETURNING *;
        `;

        const updateValues = [transactionStatus, transactionHash];
        const { rows: updatedRows } = await client.query(
          updateQuery,
          updateValues
        );

        if (updatedRows.length === 0) {
          await client.query("ROLLBACK");
          console.log(
            `Failed to update transaction with hash: ${transactionHash}`
          );
          failedRecords.push({
            messageId: record.messageId,
            error: "Transaction update failed",
          });
          continue;
        }

        console.log(
          `Transaction updated to status: ${transactionStatus}`,
          updatedRows[0]
        );

        // Handle failed transaction updates
        if (!status) {
          try {
            await Promise.all([
              client.query(
                "UPDATE pos_invoice SET status = 'failed' WHERE invoice_id = $1",
                [invoice_id]
              ),
              client.query(
                "UPDATE pos_transaction SET status = 'failed' WHERE transaction_id = $1",
                [transaction_id]
              ),
              client.query(
                "UPDATE transaction SET status = 'failed' WHERE transaction_hash = $1",
                [transactionHash]
              ),
            ]);
            console.log(
              `Updated pos_invoice and pos_transaction to 'failed' for invoice_id: ${invoice_id} and transaction_id: ${transaction_id}`
            );
          } catch (updateError) {
            await client.query("ROLLBACK");
            console.error("Error updating related records:", updateError);
            failedRecords.push({
              messageId: record.messageId,
              error: `Related updates failed: ${updateError.message}`,
            });
            continue;
          }
        }

        // For successful transactions, call wallet API
        if (status) {
          console.log(`New balance for ${to} in org ${org_id}: ${newBalance}`);

          try {
            await sendWalletTopupApi(
              user_id,
              value,
              newBalance,
              org_id,
              jwt_token
            );
            console.log("API call to update wallet balance successful");
          } catch (apiError) {
            await client.query("ROLLBACK");
            console.error("Failed to update wallet balance:", apiError);
            failedRecords.push({
              messageId: record.messageId,
              error: `Wallet API error: ${apiError.message}`,
            });
            continue;
          }
        }

        // Commit transaction if everything succeeded
        await client.query("COMMIT");
        successfulRecords.push({
          messageId: record.messageId,
          transactionHash,
          status: transactionStatus,
        });
      } catch (error) {
        // Rollback transaction on error
        await client.query("ROLLBACK");
        console.error("Error processing transaction:", error.stack);
        failedRecords.push({
          messageId: record.messageId,
          error: `Database transaction error: ${error.message}`,
        });
      }
    } catch (err) {
      console.error("⚠️ Error processing message:", err.message);
      failedRecords.push({
        messageId: record.messageId || "unknown",
        error: err.message,
      });
    } finally {
      // Make sure to release the client back to the pool
      if (client) {
        client.release();
      }
    }
  }

  // Summary of processing results
  console.log(
    `Processing completed. Success: ${successfulRecords.length}, Failed: ${failedRecords.length}`
  );

  return {
    statusCode: failedRecords.length > 0 ? 207 : 200, // 207 Multi-Status if partial failures
    body: JSON.stringify({
      message: "SQS processing completed",
      processed: event.Records.length,
      successful: successfulRecords.length,
      failed: failedRecords.length,
    }),
  };
};

process.on("uncaughtException", async (error) => {
  console.error("Uncaught exception:", error);
  await pool.end();
  process.exit(1);
});
