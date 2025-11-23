#!/usr/bin/env python3
import sys
import time
import logging
from typing import Optional, List, Tuple
from datetime import datetime

from sqlalchemy import text
from sqlalchemy.engine import Engine

from eth_import_transactions import get_engine, find_latest_table


def configure_logging() -> None:
	logging.basicConfig(
		level=logging.INFO,
		format="%(asctime)s %(levelname)s %(message)s",
		handlers=[logging.StreamHandler(sys.stdout)],
	)


def fetch_total_count(engine: Engine, table_name: str) -> int:
	"""Execute SQL: SELECT COUNT(*) FROM table_name to get total row count"""
	stmt = text(f"SELECT COUNT(*) FROM test.{table_name}")
	with engine.connect() as conn:
		result = conn.execute(stmt).scalar()
		return int(result or 0)


def fetch_distinct_days_count(engine: Engine, table_name: str) -> int:
	"""Execute SQL: SELECT COUNT(DISTINCT date) FROM table_name to get number of distinct dates"""
	stmt = text(f"SELECT COUNT(DISTINCT date) FROM test.{table_name}")
	with engine.connect() as conn:
		result = conn.execute(stmt).scalar()
		return int(result or 0)


def fetch_daily_counts_groupby(engine: Engine, table_name: str) -> List[Tuple[str, int]]:
	"""Execute first SQL: SELECT COUNT(*), date FROM table_name GROUP BY date"""
	stmt = text(f"SELECT COUNT(*), date FROM test.{table_name} GROUP BY date ORDER BY date")
	with engine.connect() as conn:
		results = conn.execute(stmt).fetchall()
		return [(row[1], int(row[0])) for row in results]  # (date, count)


def fetch_count_by_date(engine: Engine, table_name: str, date: str) -> int:
	"""Execute second SQL: SELECT COUNT(*) FROM table_name WHERE date='YYYY-MM-DD'"""
	stmt = text(f"SELECT COUNT(*) FROM test.{table_name} WHERE date=:date")
	with engine.connect() as conn:
		result = conn.execute(stmt, {"date": date}).scalar()
		return int(result or 0)


def main() -> None:
	configure_logging()
	engine = get_engine()
	
	# Find latest table
	latest_table = find_latest_table(engine)
	if not latest_table:
		logging.error("No matching tables found in schema 'test'")
		sys.exit(1)
	
	logging.info("Found latest table: %s", latest_table)
	
	# Count total rows in the table
	logging.info("Counting total rows in table %s...", latest_table)
	start_time = time.perf_counter()
	total_count = fetch_total_count(engine, latest_table)
	elapsed_ms_total = (time.perf_counter() - start_time) * 1000.0
	logging.info("Total row count: %d (took %.2f ms)", total_count, elapsed_ms_total)
	
	# Count distinct days in the table
	logging.info("Counting distinct days in table %s...", latest_table)
	start_time = time.perf_counter()
	distinct_days = fetch_distinct_days_count(engine, latest_table)
	elapsed_ms_days = (time.perf_counter() - start_time) * 1000.0
	logging.info("Distinct days count: %d (took %.2f ms)", distinct_days, elapsed_ms_days)
	
	print(f"\nTotal rows in table {latest_table}: {total_count:,}")
	print(f"Total distinct days: {distinct_days}")
	
	# Execute first SQL: GROUP BY date
	logging.info("Executing first SQL: SELECT COUNT(*), date FROM %s GROUP BY date", latest_table)
	start_time = time.perf_counter()
	daily_counts_groupby = fetch_daily_counts_groupby(engine, latest_table)
	elapsed_ms1 = (time.perf_counter() - start_time) * 1000.0
	logging.info("First SQL completed in %.2f ms, found %d dates", elapsed_ms1, len(daily_counts_groupby))
	
	# Output header
	print("\n" + "="*100)
	print(f"{'Date':<15} {'GROUP BY Count':<20} {'WHERE Count':<20} {'Match':<10}")
	print("-" * 100)
	
	# For each date, execute second SQL and compare
	total_time_ms2 = 0.0
	for date, count_groupby in daily_counts_groupby:
		start_time = time.perf_counter()
		count_where = fetch_count_by_date(engine, latest_table, date)
		elapsed_ms = (time.perf_counter() - start_time) * 1000.0
		total_time_ms2 += elapsed_ms
		
		match = "✓" if count_groupby == count_where else "✗"
		print(f"{date:<15} {count_groupby:<20} {count_where:<20} {match:<10}")
	
	print("="*100)
	print(f"\nFirst SQL (GROUP BY) duration: {elapsed_ms1:.2f} ms")
	print(f"Second SQL (WHERE per date) total duration: {total_time_ms2:.2f} ms")
	print(f"Average WHERE query duration: {total_time_ms2 / len(daily_counts_groupby):.2f} ms per date")


if __name__ == "__main__":
	main()

