from typing import Any, Optional, Dict, List
import asyncpg
import json
from datetime import datetime, timedelta, date
from mcp.server.fastmcp import FastMCP
from openai import OpenAI
import os
import uvicorn
from typing import cast
from fastapi import FastAPI

mcp = FastMCP("database")
app = cast(FastAPI, mcp.app)        #type: ignore

# Database configuration
DB_HOST = "localhost"
DB_PORT = 5432
DB_NAME = "mcp_db"
DB_USER = "postgres"
DB_PASS = "lokit@181903"

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

@mcp.tool()
async def classify_and_store_transactions(customer_id: str, months_back: int = 6) -> str:
    """Classifies and stores last N months' transactions into 'classified_transactions' table."""
    pool = await get_db_pool()
    async with pool.acquire() as conn:
        try:
            # Load valid categories from DB
            expense_rows = await conn.fetch("SELECT category_name FROM expense_categories")
            income_rows = await conn.fetch("SELECT category_name FROM income_categories")

            valid_categories = set(row['category_name'].lower() for row in expense_rows + income_rows)

            # Step 1: Fetch recent transactions
            latest_casa = await conn.fetchval("SELECT MAX(txn_date) FROM casa_transactions")
            latest_card = await conn.fetchval("SELECT MAX(txn_date) FROM card_transactions")
            reference_date = max(filter(None, [latest_casa, latest_card]))
            from_date_obj = reference_date - timedelta(days=30 * months_back)
            to_date_obj = reference_date

            query = """
                SELECT txn_id, txn_description, amount, dr_cr_flag, merchant_name, mcc_code
                FROM (
                    SELECT c.txn_id, c.txn_description, c.amount, c.dr_cr_flag,
                           NULL AS merchant_name, NULL AS mcc_code
                    FROM casa_transactions c
                    JOIN accounts a ON c.account_id = a.account_id
                    WHERE a.customer_id = $1 AND c.txn_date BETWEEN $2 AND $3
                    UNION ALL
                    SELECT ct.txn_id, ct.merchant_name AS txn_description, ct.amount, ct.dr_cr_flag,
                           ct.merchant_name, ct.mcc_code
                    FROM card_transactions ct
                    WHERE ct.customer_id = $1 AND ct.txn_date BETWEEN $2 AND $3
                ) combined
                LIMIT 200
            """
            rows = await conn.fetch(query, customer_id, from_date_obj, to_date_obj)

            if not rows:
                return f"No transactions found for customer {customer_id} in the last {months_back} months."

            # Step 2: Classify
            classified = []
            for row in rows:
                txn_id = row['txn_id']
                description = row['txn_description'] or ""
                mcc = row['mcc_code']
                desc = description.lower()

                # Defaults
                category = "others"
                reason = "No matching keywords"
                confidence = 0.5

                if "amazon" in desc or "flipkart" in desc:
                    cat = "shopping"
                    if cat in valid_categories:
                        category = cat
                        reason = "Matched keyword: amazon/flipkart"
                        confidence = 0.9
                elif "uber" in desc or "ola" in desc:
                    cat = "transport"
                    if cat in valid_categories:
                        category = cat
                        reason = "Matched keyword: uber/ola"
                        confidence = 0.85
                elif "swiggy" in desc or "zomato" in desc:
                    cat = "food"
                    if cat in valid_categories:
                        category = cat
                        reason = "Matched keyword: swiggy/zomato"
                        confidence = 0.88
                elif mcc in ("5411", "5499"):  # Grocery MCCs
                    cat = "groceries"
                    if cat in valid_categories:
                        category = cat
                        reason = f"Matched MCC: {mcc}"
                        confidence = 0.92

                # Final fallback to "others" if not a valid category
                if category not in valid_categories:
                    category = "others"
                    reason = "Not in valid income/expense categories"
                    confidence = 0.5

                classified.append((txn_id, category, reason, confidence))

            # Step 3: Insert into DB
            await conn.executemany("""
                INSERT INTO classified_transactions (txn_id, classified_category, reason, confidence_score)
                VALUES ($1, $2, $3, $4)
                ON CONFLICT (txn_id) DO NOTHING
            """, classified)

            return f"{len(classified)} transactions classified and stored for customer {customer_id}."

        except Exception as e:
            raise RuntimeError(f"Classification failed: {str(e)}")

@mcp.tool()
async def get_mcc_details(mcc_code: str) -> str:
    """Fetches description and category for a given MCC code from the mcc_codes table."""
    pool = await get_db_pool()
    async with pool.acquire() as conn:
        try:
            row = await conn.fetchrow(
                "SELECT * FROM mcc_codes WHERE mcc_code = $1", mcc_code
            )

            if not row:
                raise ValueError(f"MCC code '{mcc_code}' not found in the database.")

            return (
                f"MCC Code: {row['mcc_code']}\n"
                f"Description: {row['description']}\n"
                f"Category: {row['category']}"
            )
        except Exception as e:
            raise RuntimeError(f"Failed to fetch MCC details: {str(e)}")
        
@mcp.tool()
async def get_tc_details(tc_code: str) -> str:
    """Fetches details for a given TC code from the tc_codes table."""
    pool = await get_db_pool()
    async with pool.acquire() as conn:
        try:
            row = await conn.fetchrow(
                "SELECT * FROM tc_codes WHERE tc_code = $1", tc_code
            )

            if not row:
                raise ValueError(f"TC code '{tc_code}' not found in the database.")

            details = [f"{key}: {value}" for key, value in row.items()]
            return "\n".join(details)

        except Exception as e:
            raise RuntimeError(f"Failed to fetch TC details: {str(e)}")

# @mcp.tool()
# async def classify_and_store_transactions(customer_id: str, months_back: int = 6) -> str:
#     """Classifies and stores last N months' transactions into 'classified_transactions' table."""

#     pool = await get_db_pool()
#     async with pool.acquire() as conn:
#         try:
#             print(f"DEBUG: Classifying transactions for {customer_id}...")

#             # Step 1: Fetch transactions (reuse logic from get_customer_transactions)
#             latest_casa = await conn.fetchval("SELECT MAX(txn_date) FROM casa_transactions")
#             latest_card = await conn.fetchval("SELECT MAX(txn_date) FROM card_transactions")
#             reference_date = max(filter(None, [latest_casa, latest_card]))
#             from_date_obj = reference_date - timedelta(days=30 * months_back)
#             to_date_obj = reference_date

#             query = """
#                 SELECT txn_id, txn_description, amount, dr_cr_flag, merchant_name, mcc_code
#                 FROM (
#                     SELECT c.txn_id, c.txn_description, c.amount, c.dr_cr_flag,
#                            NULL AS merchant_name, NULL AS mcc_code
#                     FROM casa_transactions c
#                     JOIN accounts a ON c.account_id = a.account_id
#                     WHERE a.customer_id = $1 AND c.txn_date BETWEEN $2 AND $3
#                     UNION ALL
#                     SELECT ct.txn_id, ct.merchant_name AS txn_description, ct.amount, ct.dr_cr_flag,
#                            ct.merchant_name, ct.mcc_code
#                     FROM card_transactions ct
#                     WHERE ct.customer_id = $1 AND ct.txn_date BETWEEN $2 AND $3
#                 ) combined
#                 LIMIT 200
#             """
#             rows = await conn.fetch(query, customer_id, from_date_obj, to_date_obj)

#             if not rows:
#                 return f"No transactions found for customer {customer_id} in the last {months_back} months."

#             # Step 2: Classification logic (simple keyword-based rules)
#             classified = []
#             for row in rows:
#                 txn_id = row['txn_id']
#                 description = row['txn_description'] or ""
#                 mcc = row['mcc_code']
#                 category = "others"
#                 reason = "No matching keywords"
#                 confidence = 0.5

#                 desc = description.lower()

#                 if "amazon" in desc or "flipkart" in desc:
#                     category = "shopping"
#                     reason = "Matched keyword: amazon/flipkart"
#                     confidence = 0.9
#                 elif "uber" in desc or "ola" in desc:
#                     category = "transport"
#                     reason = "Matched keyword: uber/ola"
#                     confidence = 0.85
#                 elif "swiggy" in desc or "zomato" in desc:
#                     category = "food"
#                     reason = "Matched keyword: swiggy/zomato"
#                     confidence = 0.88
#                 elif mcc in ("5411", "5499"):  # example grocery MCCs
#                     category = "groceries"
#                     reason = f"Matched MCC: {mcc}"
#                     confidence = 0.92

#                 classified.append((txn_id, category, reason, confidence))

#             # Step 3: Insert into classified_transactions
#             await conn.executemany("""
#                 INSERT INTO classified_transactions (txn_id, classified_category, reason, confidence_score)
#                 VALUES ($1, $2, $3, $4)
#                 ON CONFLICT (txn_id) DO NOTHING
#             """, classified)

#             return f"{len(classified)} transactions classified and stored for customer {customer_id}."

#         except Exception as e:
#             raise RuntimeError(f"Classification failed: {str(e)}")


if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=8000)
