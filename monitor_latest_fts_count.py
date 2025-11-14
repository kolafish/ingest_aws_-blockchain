#!/usr/bin/env python3
import sys
import time
import logging
from typing import Optional

from sqlalchemy import text
from sqlalchemy.engine import Engine

from eth_import_transactions import get_engine


def configure_logging() -> None:
	logging.basicConfig(
		level=logging.INFO,
		format="%(asctime)s %(levelname)s %(message)s",
		handlers=[logging.StreamHandler(sys.stdout)],
	)


def find_latest_table(engine: Engine) -> Optional[str]:
	query = text(
		"""
		SELECT table_name
		FROM information_schema.tables
		WHERE table_schema = 'test' AND table_name LIKE 'eth_transactions\\_%'
		ORDER BY table_name DESC
		LIMIT 1
		"""
	)
	with engine.connect() as conn:
		result = conn.execute(query).scalar()
		return result


def fetch_count(engine: Engine, table_name: str) -> tuple[int, float]:
    start = time.perf_counter()
    #stmt = text(f"SELECT COUNT(*) FROM test.{table_name} WHERE fts_match_word(\"2025\", date)")
    stmt = text(f"SELECT COUNT(*) FROM test.{table_name} WHERE  date=\"2025-10-25\" or  date=\"2025-10-24\" or  date=\"2025-10-23\" or  date=\"2025-10-22\" or  date=\"2025-10-21\"")
    with engine.connect() as conn:
        val = conn.execute(stmt).scalar()
    elapsed_ms = (time.perf_counter() - start) * 1000.0
    return int(val or 0), elapsed_ms


def main() -> None:
	configure_logging()
	engine = get_engine()
	logging.info("Starting 10-second polling for latest eth_transactions_* table")
	try:
		while True:
			latest = find_latest_table(engine)
			if not latest:
				logging.warning("No matching tables found in schema 'test'")
			else:
				try:
					cnt, elapsed_ms = fetch_count(engine, latest)
					print(f"{time.strftime('%Y-%m-%d %H:%M:%S')} table={latest} count={cnt} duration_ms={elapsed_ms:.1f}", flush=True)
				except Exception as e:
					logging.error("Query failed on table %s: %s", latest, str(e))
			time.sleep(10)
	except KeyboardInterrupt:
		logging.info("Interrupted, exiting")


if __name__ == "__main__":
	main()


