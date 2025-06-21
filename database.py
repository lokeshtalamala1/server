from typing import Any, Optional, Dict, List
import asyncpg
import json
from datetime import datetime, timedelta, date
from mcp.server.fastmcp import FastMCP
from openai import OpenAI
import os

mcp = FastMCP("database")

from dotenv import load_dotenv
load_dotenv()

# Database configuration
DB_HOST = os.getenv("DB_HOST", "localhost")
DB_PORT = int(os.getenv("DB_PORT", 5432))
DB_NAME = os.getenv("DB_NAME", "mcp_db")
DB_USER = os.getenv("DB_USER", "postgres")
DB_PASS = os.getenv("DB_PASS", "password")
_db_pool = None

async def get_db_pool():
    global _db_pool
    if _db_pool is None or _db_pool._closed:
        try:
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
            print("DEBUG: Database connection pool created successfully")
        except Exception as e:
            print(f"CRITICAL: Failed to create connection pool: {e}")
            raise
    return _db_pool

def validate_and_format_date(date_input) -> str:
    if isinstance(date_input, datetime):
        return date_input.date().strftime('%Y-%m-%d')
    elif isinstance(date_input, date):
        return date_input.strftime('%Y-%m-%d')
    elif isinstance(date_input, str):
        try:
            datetime.strptime(date_input, '%Y-%m-%d')
            return date_input
        except ValueError:
            raise ValueError(f"Invalid date format: {date_input}. Expected YYYY-MM-DD")
    else:
        raise ValueError(f"Invalid date type: {type(date_input)}")

@mcp.tool()
async def get_customer_transactions(
    customer_id: str,
    months_back: int = 6
) -> str:
    print(f"DEBUG: === Starting transaction query for customer {customer_id} ===")

    try:
        pool = await get_db_pool()
    except Exception as e:
        raise RuntimeError(f"Database connection failed: {str(e)}")

    async with pool.acquire() as conn:
        try:
            # Step 1: Check customer existence
            customer_check = await conn.fetch(
                "SELECT account_id FROM accounts WHERE customer_id = $1", customer_id
            )

            if not customer_check:
                similar_customers = await conn.fetch(
                    "SELECT DISTINCT customer_id FROM accounts WHERE customer_id ILIKE $1 LIMIT 5",
                    f"%{customer_id[-6:]}"
                )
                if similar_customers:
                    similar_ids = [row['customer_id'] for row in similar_customers]
                    raise ValueError(
                        f"Customer {customer_id} not found. Similar customers: {', '.join(similar_ids)}"
                    )
                else:
                    raise ValueError(f"Customer {customer_id} not found in database.")

            # Step 2: Determine latest txn_date across all data
            latest_casa = await conn.fetchval("SELECT MAX(txn_date) FROM casa_transactions")
            latest_card = await conn.fetchval("SELECT MAX(txn_date) FROM card_transactions")

            reference_date = max([d for d in [latest_casa, latest_card] if d is not None], default=None)
            if reference_date is None:
                raise ValueError("No transactions found in the database.")

            from_date_obj = reference_date - timedelta(days=30 * months_back)
            to_date_obj = reference_date

            # Step 3: Check if any transactions exist for customer in date range
            data_check = await conn.fetchval("""
                SELECT COUNT(*) FROM (
                    SELECT 1 FROM casa_transactions c
                    JOIN accounts a ON c.account_id = a.account_id
                    WHERE a.customer_id = $1 AND c.txn_date BETWEEN $2 AND $3
                    UNION ALL
                    SELECT 1 FROM card_transactions ct
                    WHERE ct.customer_id = $1 AND ct.txn_date BETWEEN $2 AND $3
                ) combined
            """, customer_id, from_date_obj, to_date_obj)

            if data_check == 0:
                raise ValueError(
                    f"No transactions found for customer {customer_id} in the last {months_back} months "
                    f"(from {from_date_obj} to {to_date_obj})."
                )

            # Step 4: Fetch transactions
            query = """
                SELECT * FROM (
                    SELECT 'CASA' AS txn_type, c.txn_id, c.account_id, c.txn_date, 
                           c.txn_time, c.amount, c.dr_cr_flag, c.txn_description,
                           c.currency, c.txn_code, NULL AS merchant_name, NULL AS mcc_code
                    FROM casa_transactions c
                    JOIN accounts a ON c.account_id = a.account_id
                    WHERE a.customer_id = $1 AND c.txn_date BETWEEN $2 AND $3
                    UNION ALL
                    SELECT 'CARD' AS txn_type, ct.txn_id, ct.account_id, ct.txn_date,
                           ct.txn_time, ct.amount, ct.dr_cr_flag, ct.merchant_name AS txn_description,
                           NULL AS currency, NULL AS txn_code, ct.merchant_name, ct.mcc_code
                    FROM card_transactions ct
                    WHERE ct.customer_id = $1 AND ct.txn_date BETWEEN $2 AND $3
                ) combined_transactions
                ORDER BY txn_date DESC, txn_time DESC
                LIMIT 100
            """

            rows = await conn.fetch(query, customer_id, from_date_obj, to_date_obj)

            result_lines = []
            result_lines.append(f"Transactions for Customer {customer_id}")
            result_lines.append(f"Date Range: {from_date_obj} to {to_date_obj}")
            result_lines.append(f"Total Found: {len(rows)} transactions")
            result_lines.append("=" * 80)

            for i, row in enumerate(rows, 1):
                txn_type = row['txn_type']
                account_id = row['account_id']
                txn_date = row['txn_date']
                txn_time = row['txn_time'] if row['txn_time'] else '00:00:00'
                amount = float(row['amount']) if row['amount'] else 0.0
                dr_cr_flag = row['dr_cr_flag']
                description = (row['txn_description'] or 'N/A')[:45]

                amount_display = f"+{amount:,.2f}" if dr_cr_flag == 'CR' else f"-{amount:,.2f}"

                result_lines.append(
                    f"{i:2d}. {txn_date} {txn_time} | {txn_type} | {account_id} | "
                    f"{amount_display:>12} | {description}"
                )

            result_lines.append("=" * 80)

            MAX_LINES = 50
            if len(result_lines) > MAX_LINES:
                return "\n".join(result_lines[:MAX_LINES]) + f"\n\n...and {len(result_lines)-MAX_LINES} more transactions. Use 'show all' to view."
            else:
                return "\n".join(result_lines)

        except Exception as e:
            raise RuntimeError(f"Failed to fetch transactions: {str(e)}")


if __name__ == "__main__":
    mcp.run()

app = mcp.asgi_app  # Expose FastAPI app for Render to find
