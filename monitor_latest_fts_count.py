#!/usr/bin/env python3
import sys
import time
import logging
from typing import Optional
from datetime import datetime, timedelta

from sqlalchemy import text
from sqlalchemy.engine import Engine

from eth_import_transactions import get_engine


# Configuration: start date and number of days to query backwards
DEFAULT_START_DATE = "2025-10-25"
DEFAULT_DAYS = 32

# FTS match term for comparison
FTS_MATCH_TERM = "2025"


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


def generate_date_conditions(start_date: str, days: int) -> str:
	"""Generate SQL date conditions for the specified date range"""
	start_dt = datetime.strptime(start_date, "%Y-%m-%d")
	date_list = []
	for i in range(days):
		date_str = (start_dt - timedelta(days=i)).strftime("%Y-%m-%d")
		date_list.append(f'date="{date_str}"')
	return " OR ".join(date_list)


def fetch_count(engine: Engine, table_name: str, start_date: str = DEFAULT_START_DATE, days: int = DEFAULT_DAYS) -> tuple[int, float]:
	"""Fetch both normal count and FTS-based count for comparison, with separate latencies."""
	date_conditions = generate_date_conditions(start_date, days)

	normal_stmt = text(f"SELECT COUNT(*) FROM test.{table_name} WHERE {date_conditions}")
	fts_stmt = text(f"SELECT COUNT(*) FROM test.{table_name} WHERE fts_match_word(:term, date)")

	with engine.connect() as conn:
		start_normal = time.perf_counter()
		normal_val = conn.execute(normal_stmt).scalar()
		elapsed_normal_ms = (time.perf_counter() - start_normal) * 1000.0

		start_fts = time.perf_counter()
		fts_val = conn.execute(fts_stmt, {"term": FTS_MATCH_TERM}).scalar()
		elapsed_fts_ms = (time.perf_counter() - start_fts) * 1000.0

	return int(normal_val or 0), elapsed_normal_ms, int(fts_val or 0), elapsed_fts_ms


def main() -> None:
	configure_logging()
	engine = get_engine()
	start_date = DEFAULT_START_DATE
	days = DEFAULT_DAYS
	logging.info("Starting 10-second polling for latest eth_transactions_* table")
	logging.info("Query configuration: start_date=%s, days=%d", start_date, days)
	try:
		while True:
			latest = find_latest_table(engine)
			if not latest:
				logging.warning("No matching tables found in schema 'test'")
			else:
				try:
					normal_cnt, normal_ms, fts_cnt, fts_ms = fetch_count(engine, latest, start_date, days)
					diff = normal_cnt - fts_cnt
					ts = time.strftime('%Y-%m-%d %H:%M:%S')
					print(f"{ts} table={latest} normal_count={normal_cnt} fts_count={fts_cnt} diff={diff}")
					print(f"{ts} table={latest} normal_latency_ms={normal_ms:.1f} fts_latency_ms={fts_ms:.1f}", flush=True)
				except Exception as e:
					logging.error("Query failed on table %s: %s", latest, str(e))
			time.sleep(10)
	except KeyboardInterrupt:
		logging.info("Interrupted, exiting")


if __name__ == "__main__":
	main()


