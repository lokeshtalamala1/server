from typing import Any
import asyncpg
import os
from datetime import datetime, timedelta, date
from mcp.server.fastmcp import FastMCP
from dotenv import load_dotenv

load_dotenv()

# Initialize FastMCP instance
mcp = FastMCP(
    title="Remote MCP Server",
    version="1.0",
)

# Load environment variables
DB_HOST = os.getenv("DB_HOST", "localhost")
DB_PORT = int(os.getenv("DB_PORT", 5432))
DB_NAME = os.getenv("DB_NAME", "postgres")
DB_USER = os.getenv("DB_USER", "postgres")
DB_PASS = os.getenv("DB_PASS", "password")

# Global DB connection pool
_db_pool = None

async def get_db_pool():
    global _db_pool
    if _db_pool is None or _db_pool._closed:
        _db_pool = await asyncpg.create_pool(
            user=DB_USER,
            password=DB_PASS,
            database=DB_NAME,
            host=DB_HOST,
            port=DB_PORT,
            min_size=1,
            max_size=10,
            command_timeout=60,
            server_settings={
                'application_name': 'mcp_banking_app',
            }
        )
    return _db_pool

@mcp.tool()
async def get_customer_transactions(
    customer_id: str,
    months_back: int = 6
) -> str:
    """Get recent transactions (CASA + CARD) for a customer by ID."""
    pool = await get_db_pool()

    async with pool.acquire() as conn:
        # Validate customer
        accounts = await conn.fetch("SELECT account_id FROM accounts WHERE customer_id = $1", customer_id)
        if not accounts:
            raise ValueError(f"Customer {customer_id} not found.")

        # Get latest txn_date across tables
        latest_casa = await conn.fetchval("SELECT MAX(txn_date) FROM casa_transactions")
        latest_card = await conn.fetchval("SELECT MAX(txn_date) FROM card_transactions")
        reference_date = max([d for d in [latest_casa, latest_card] if d])

        if not reference_date:
            raise ValueError("No transactions found in database.")

        from_date = reference_date - timedelta(days=30 * months_back)
        to_date = reference_date

        # Check if any transactions exist
        has_txns = await conn.fetchval("""
            SELECT COUNT(*) FROM (
                SELECT 1 FROM casa_transactions c
                JOIN accounts a ON c.account_id = a.account_id
                WHERE a.customer_id = $1 AND c.txn_date BETWEEN $2 AND $3
                UNION ALL
                SELECT 1 FROM card_transactions ct
                WHERE ct.customer_id = $1 AND ct.txn_date BETWEEN $2 AND $3
            ) combined
        """, customer_id, from_date, to_date)

        if has_txns == 0:
            raise ValueError(f"No transactions found for {customer_id} between {from_date} and {to_date}.")

        # Fetch and return transactions
        query = """
            SELECT * FROM (
                SELECT 'CASA' AS txn_type, c.txn_id, c.account_id, c.txn_date, 
                       c.txn_time, c.amount, c.dr_cr_flag, c.txn_description,
                       c.currency, c.txn_code, NULL AS merchant_name, NULL AS mcc_code
                FROM casa_transactions c
                JOIN accounts a ON c.account_id = a.account_id
                WHERE a.customer_id = $1 AND c.txn_date BETWEEN $2 AND $3
                UNION ALL
                SELECT 'CARD', ct.txn_id, ct.account_id, ct.txn_date, ct.txn_time,
                       ct.amount, ct.dr_cr_flag, ct.merchant_name,
                       NULL, NULL, ct.merchant_name, ct.mcc_code
                FROM card_transactions ct
                WHERE ct.customer_id = $1 AND ct.txn_date BETWEEN $2 AND $3
            ) combined
            ORDER BY txn_date DESC, txn_time DESC
            LIMIT 100
        """

        rows = await conn.fetch(query, customer_id, from_date, to_date)

        lines = [f"Transactions for {customer_id} from {from_date} to {to_date}", "-"*75]
        for i, row in enumerate(rows, 1):
            amount = float(row["amount"]) if row["amount"] else 0
            symbol = "+" if row["dr_cr_flag"] == "CR" else "-"
            lines.append(f"{i:2d}. {row['txn_date']} {row['txn_time']} | {row['txn_type']} | {row['account_id']} | {symbol}{amount:,.2f} | {row['txn_description'][:40]}")

        lines.append("-" * 75)
        return "\n".join(lines[:50]) + ("\n...more..." if len(rows) > 50 else "")

mcp.register_tool(get_customer_transactions)

# For Render deployment
if __name__ == "__main__":
    mcp.run()

# for Render deployment
mcp.build()        # Build tool registry
app = mcp.sse_app  # SSE app for Render
