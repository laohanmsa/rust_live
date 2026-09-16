"""Run in the existing Django container after authorization to enable the dry-run reader.

Creates a dedicated role with SELECT on only the required public/configuration columns.
The credential is written to a private temporary file, never standard output.
"""
import json
import os
import secrets
from pathlib import Path

from django.db import connection, transaction
from psycopg2 import sql

ROLE = "rust_uma_reader"
COLUMNS = {
    "market_data_market": "id event_id question volume token_id_yes token_id_no active closed accepting_orders auto_archived min_tick_size min_order_size neg_risk fees_enabled fee_verification_status fee_schedule_rate fee_schedule_exponent fee_schedule_key fee_category",
    "market_data_event": "id volume",
    "market_data_tag": "id label",
    "market_data_event_tags": "event_id tag_id",
    "market_data_resolution": "id market_id market_id_external uma_request_id status proposed_price propose_time block_number dispute_block_number settle_block_number dispute_timestamp settle_timestamp",
    "market_data_autotradeconfig": "id manual_trade_shutdown_enabled max_ask_price max_orders_per_market ev_threshold order_size_usd low_price_order_size_usd low_depth_099_order_size_usd",
    "strategy_tradelinedefinition": "line_key valuation_key strategy_key is_active",
}
password = secrets.token_hex(32)
with transaction.atomic(), connection.cursor() as cursor:
    cursor.execute("SELECT 1 FROM pg_roles WHERE rolname=%s", [ROLE])
    if cursor.fetchone():
        raise SystemExit("Reader role already exists; reuse its existing protected credential.")
    cursor.execute(sql.SQL("CREATE ROLE {} LOGIN PASSWORD %s NOSUPERUSER NOCREATEDB NOCREATEROLE NOINHERIT CONNECTION LIMIT 10").format(sql.Identifier(ROLE)), [password])
    cursor.execute(sql.SQL("ALTER ROLE {} SET default_transaction_read_only=on").format(sql.Identifier(ROLE)))
    cursor.execute(sql.SQL("ALTER ROLE {} SET statement_timeout='750ms'").format(sql.Identifier(ROLE)))
    cursor.execute(sql.SQL("GRANT CONNECT ON DATABASE {} TO {}").format(sql.Identifier(connection.settings_dict["NAME"]), sql.Identifier(ROLE)))
    cursor.execute(sql.SQL("GRANT USAGE ON SCHEMA public TO {}").format(sql.Identifier(ROLE)))
    for table, columns in COLUMNS.items():
        cursor.execute(sql.SQL("GRANT SELECT ({}) ON {} TO {}").format(sql.SQL(",").join(map(sql.Identifier, columns.split())), sql.Identifier(table), sql.Identifier(ROLE)))
config = dict(host=connection.settings_dict["HOST"], port=int(connection.settings_dict.get("PORT") or 5432), dbname=connection.settings_dict["NAME"], user=ROLE, password=password)
path = Path("/tmp/rust-uma-database-reader.json")
fd = os.open(path, os.O_CREAT | os.O_EXCL | os.O_WRONLY, 0o600)
with os.fdopen(fd, "w") as file:
    json.dump(config, file)
print(json.dumps({"role": ROLE, "read_only": True, "tables": len(COLUMNS), "credential_file_ready": True}))
