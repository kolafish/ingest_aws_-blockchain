#!/usr/bin/env python3
import os
import sys
import argparse
import logging
from datetime import datetime, timedelta
import time
import shutil
from typing import Optional, List
from concurrent.futures import ThreadPoolExecutor, as_completed

import pandas as pd
import pyarrow.dataset as ds
from pyarrow import fs as pa_fs
from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine
import random


def configure_logging(verbose: bool = False) -> None:
	level = logging.DEBUG if verbose else logging.INFO
	logging.basicConfig(
		level=level,
		format="%(asctime)s %(levelname)s %(message)s",
		handlers=[logging.StreamHandler(sys.stdout)],
	)


# Module-level defaults (edit here to change behavior without CLI)
DEFAULT_START_DATE = "2025-10-25"
DEFAULT_DAYS = 32 # total 45 days
DEFAULT_CHUNKSIZE = 5000
DEFAULT_BATCH_SIZE = 5000
DEFAULT_SOURCE = "local"  # download | local | s3
DEFAULT_LOCAL_DIR = "local_data_multi"
DEFAULT_CREATE_NEW_TABLE = True 
DEFAULT_DOWNLOAD_TIMEOUT = 300
DEFAULT_DOWNLOAD_RETRIES = 3
DEFAULT_DOWNLOAD_WORKERS = 8
DEFAULT_INDEX_TYPE = "hybrid"  # fts | hybrid
DEFAULT_BLOCK_TIMESTAMP_TYPE = "bigint"  # datetime | bigint


## Removed legacy progress/memory helpers (save_progress, load_progress, cleanup_progress, get_memory_usage)


def get_engine() -> Engine:
	user = os.getenv("TIDB_USER", "root")
	password = os.getenv("TIDB_PASSWORD", "")
	host = os.getenv("TIDB_HOST", "127.0.0.1")
	port = int(os.getenv("TIDB_PORT", "4001"))
	db = os.getenv("TIDB_DB", "test")

	url = f"mysql+mysqlconnector://{user}:{password}@{host}:{port}/{db}?charset=utf8mb4"
	return create_engine(url, pool_pre_ping=True, pool_recycle=3600)


DDL_CREATE_DATABASE = """
CREATE DATABASE IF NOT EXISTS eth;
"""

def get_create_table_ddl(table_name: str, block_timestamp_type: str = "datetime") -> str:
	"""Generate CREATE TABLE DDL with configurable block_timestamp type"""
	if block_timestamp_type not in ["datetime", "bigint"]:
		raise ValueError(f"block_timestamp_type must be 'datetime' or 'bigint', got: {block_timestamp_type}")
	
	timestamp_type = "DATETIME" if block_timestamp_type == "datetime" else "BIGINT"
	
	return f"""
CREATE TABLE IF NOT EXISTS test.{table_name} (
  date VARCHAR(10) NOT NULL,
  hash VARCHAR(66) NOT NULL,
  block_timestamp {timestamp_type} NOT NULL,
  nonce BIGINT NULL,
  transaction_index BIGINT NULL,
  from_address VARCHAR(42) NULL,
  to_address VARCHAR(42) NULL,
  value DOUBLE NULL,
  gas BIGINT NULL,
  gas_price BIGINT NOT NULL,
  input TEXT NULL,
  receipt_cumulative_gas_used BIGINT NULL,
  receipt_gas_used BIGINT NULL,
  receipt_contract_address VARCHAR(42) NULL,
  receipt_status BIGINT NULL,
  block_number BIGINT NULL,
  block_hash VARCHAR(66) NULL,
  max_fee_per_gas BIGINT NULL,
  max_priority_fee_per_gas BIGINT NULL,
  transaction_type BIGINT NULL,
  receipt_effective_gas_price BIGINT NULL,
  random_flag BOOLEAN NULL,
  PRIMARY KEY (block_timestamp, gas_price,hash)
);
"""

DDL_ADD_FULLTEXT = """
ALTER TABLE test.{table_name} ADD FULLTEXT INDEX ft_index (date,from_address,to_address,input,receipt_contract_address,block_hash) WITH PARSER standard;
"""

# Columns for Hybrid index: selected VARCHAR, DATETIME and BIGINT columns (max 16 columns)
# Selected most commonly queried columns to stay within the 16-column limit
# Note: block_timestamp can be DATETIME or BIGINT type depending on configuration, but can still be included in Hybrid index
HYBRID_INDEX_VARCHAR_COLUMNS = [
	"date",
	"hash",
	"from_address",
	"to_address",
	"receipt_contract_address",
]

HYBRID_INDEX_BIGINT_COLUMNS = [
	"block_timestamp",  # Can be DATETIME or BIGINT type depending on configuration, included here for Hybrid index compatibility
	"block_number",
	"transaction_index",
	"gas",
	"gas_price",
	"receipt_status",
	"transaction_type",
]

HYBRID_INDEX_BOOLEAN_COLUMNS = [
	"random_flag",
]

def get_hybrid_index_sql(table_name: str) -> str:
	"""Generate Hybrid index SQL with inverted and sort configuration"""
	all_columns = HYBRID_INDEX_VARCHAR_COLUMNS + HYBRID_INDEX_BIGINT_COLUMNS + HYBRID_INDEX_BOOLEAN_COLUMNS
	columns_str = ", ".join(all_columns)
	columns_json = ", ".join([f'"{col}"' for col in all_columns])
	
	# Sort columns: block_timestamp, date, gas_price
	sort_columns = ["block_timestamp", "gas_price"]
	sort_columns_json = ", ".join([f'"{col}"' for col in sort_columns])
	# Sort order: desc for timestamp and date (newest first), desc for gas_price (highest first)
	sort_order = ["desc", "desc"]
	sort_order_json = ", ".join([f'"{order}"' for order in sort_order])
	
	sql = f"""CREATE HYBRID INDEX idx_{table_name}_hybrid ON test.{table_name}({columns_str}) PARAMETER '{{"inverted": {{"columns": [{columns_json}]}}, "sort": {{"columns": [{sort_columns_json}], "order": [{sort_order_json}]}}}}';"""
	return sql


PREFERRED_COLUMNS = [
	"hash",
	"nonce",
	"transaction_index",
	"from_address",
	"to_address",
	"value",
	"gas",
	"gas_price",
	"input",
	"receipt_cumulative_gas_used",
	"receipt_gas_used",
	"receipt_contract_address",
	"receipt_status",
	"block_timestamp",
	"block_number",
	"block_hash",
	"max_fee_per_gas",
	"max_priority_fee_per_gas",
	"transaction_type",
	"receipt_effective_gas_price",
	"random_flag",
]


def normalize_hex(df: pd.DataFrame) -> pd.DataFrame:
	hex_cols = [
		"hash",
		"from_address",
		"to_address",
		"block_hash",
		"receipt_contract_address",
	]
	for col in hex_cols:
		if col in df.columns:
			df[col] = df[col].astype("string").str.lower()
	return df


def validate_and_clean_data(df: pd.DataFrame) -> pd.DataFrame:
	"""Validate and clean data to prevent insertion errors"""
	# Handle input field length validation
	if "input" in df.columns:
		# Convert to string and handle NaN values
		df["input"] = df["input"].astype("string")
		
		# Truncate to MySQL TEXT max length (approx 65,535 chars)
		max_input_length = 65535
		long_inputs = df["input"].str.len() > max_input_length
		long_count = long_inputs.sum() if long_inputs.any() else 0
		df["input"] = df["input"].str.slice(0, max_input_length)
		if long_count > 0:
			logging.debug("Input truncation: %d row(s) truncated to %d chars", long_count, max_input_length)
	
	return df


def generate_table_name() -> str:
	"""Generate table name with timestamp"""
	timestamp = datetime.now().strftime("%Y%m%d%H%M%S")
	return f"eth_transactions_{timestamp}"


def find_latest_table(engine: Engine) -> Optional[str]:
	"""Find the latest table by timestamp in table name"""
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


def ensure_schema(engine: Engine, create_new_table: bool = False, table_name: str = None, index_type: str = "fts", block_timestamp_type: str = "datetime") -> str:
	"""
	Ensure schema exists. If create_new_table, create a new table with timestamp.
	Returns the table name to use.
	index_type: "fts" for FULLTEXT index, "hybrid" for Hybrid index
	block_timestamp_type: "datetime" for DATETIME type, "bigint" for BIGINT type
	"""
	if create_new_table:
		table_name = generate_table_name()
		logging.info("Creating new table with timestamp: %s (block_timestamp type: %s)", table_name, block_timestamp_type)
		with engine.begin() as conn:
			# logging.info("Creating database...")
			# conn.execute(text(DDL_CREATE_DATABASE.strip()))
			
			logging.info("Creating table %s...", table_name)
			create_sql = get_create_table_ddl(table_name, block_timestamp_type)
			conn.execute(text(create_sql.strip()))
			
			if index_type == "hybrid":
				logging.info("Adding Hybrid index to %s...", table_name)
				hybrid_sql = get_hybrid_index_sql(table_name)
				logging.info("Hybrid index SQL: %s", hybrid_sql.strip())
				conn.execute(text(hybrid_sql.strip()))
			else:
				logging.info("Adding FULLTEXT index to %s...", table_name)
				ft_sql = DDL_ADD_FULLTEXT.format(table_name=table_name)
				logging.info("FULLTEXT index SQL: %s", ft_sql.strip())
				conn.execute(text(ft_sql.strip()))
		return table_name
	else:
		# Use existing table name or find latest table by timestamp
		if table_name is None:
			latest_table = find_latest_table(engine)
			if latest_table:
				table_name = latest_table
				logging.info("Found latest table by timestamp: %s", table_name)
			else:
				table_name = "eth_transactions"
				logging.info("No timestamped table found, using default: %s", table_name)
		else:
			logging.info("Using specified table: %s", table_name)
		
		with engine.begin() as conn:
			logging.info("Creating database...")
			conn.execute(text(DDL_CREATE_DATABASE.strip()))
			# Check if table exists, if not create default one
			result = conn.execute(text("SELECT COUNT(*) FROM information_schema.tables WHERE table_schema='test' AND table_name=:tname"), {"tname": table_name})
			if result.scalar() == 0:
				logging.info("Table %s does not exist, creating it...", table_name)
				create_sql = get_create_table_ddl(table_name, block_timestamp_type)
				conn.execute(text(create_sql.strip()))
				# Add index only if table was just created
				if index_type == "hybrid":
					logging.info("Adding Hybrid index to %s...", table_name)
					hybrid_sql = get_hybrid_index_sql(table_name)
					logging.info("Hybrid index SQL: %s", hybrid_sql.strip())
					conn.execute(text(hybrid_sql.strip()))
				else:
					logging.info("Adding FULLTEXT index to %s...", table_name)
					ft_sql = DDL_ADD_FULLTEXT.format(table_name=table_name)
					logging.info("FULLTEXT index SQL: %s", ft_sql.strip())
					conn.execute(text(ft_sql.strip()))
		return table_name


def _list_parquet_files(fs: pa_fs.S3FileSystem, date_str: str) -> list:
	dir_path = f"aws-public-blockchain/v1.0/eth/transactions/date={date_str}"
	selector = pa_fs.FileSelector(dir_path, recursive=False)
	infos = fs.get_file_info(selector)
	files = [fi.path for fi in infos if fi.is_file and fi.path.endswith(".parquet")]
	return files


def download_files_to_local(fs: pa_fs.S3FileSystem, s3_files: list, date_str: str, local_dir: str = None, max_retries: int = 3, timeout_seconds: int = 300) -> list:
	"""Download S3 files to local directory with retry and resume support"""
	if local_dir is None:
		local_dir = f"local_data_{date_str}"
	
	os.makedirs(local_dir, exist_ok=True)
	local_files = []
	
	logging.info("Downloading %d files to local directory: %s", len(s3_files), local_dir)
	logging.info("Using timeout: %d seconds, max retries: %d", timeout_seconds, max_retries)
	
	for i, s3_file in enumerate(s3_files):
		filename = s3_file.split('/')[-1]
		# Prefix with date to allow multi-day single directory and later date inference
		prefixed = f"{date_str}__{filename}"
		local_file = os.path.join(local_dir, prefixed)
		
		# Check if file already exists locally and is complete
		if os.path.exists(local_file):
			try:
				# Get remote file size
				remote_info = fs.get_file_info(s3_file)
				local_size = os.path.getsize(local_file)
				
				if local_size == remote_info.size:
					logging.info("File %d/%d already exists locally: %s (%.2f MB)", 
							i+1, len(s3_files), prefixed, local_size / (1024 * 1024))
					local_files.append(local_file)
					continue
				else:
					logging.info("File %d/%d exists but incomplete (%.2f MB / %.2f MB), resuming download: %s", 
							i+1, len(s3_files), local_size / (1024 * 1024), remote_info.size / (1024 * 1024), prefixed)
			except Exception as e:
				logging.warning("Could not check remote file size for %s: %s", filename, str(e))
				# If we can't check, assume file is incomplete and retry
		
		# Download with retry
		success = False
		for attempt in range(max_retries):
			try:
				logging.info("Downloading file %d/%d (attempt %d/%d): %s", 
							i+1, len(s3_files), attempt + 1, max_retries, prefixed)
				start_time = time.time()
				
				# Configure S3 filesystem with longer timeout
				fs_with_timeout = pa_fs.S3FileSystem(
					region="us-east-2", 
					anonymous=True,
					request_timeout=timeout_seconds,
					connect_timeout=timeout_seconds
				)
				
				# Resume download if file exists
				mode = 'ab' if os.path.exists(local_file) else 'wb'
				with fs_with_timeout.open_input_file(s3_file) as source:
					with open(local_file, mode) as target:
						if mode == 'ab':
							# Skip already downloaded bytes
							target.seek(0, 2)  # Seek to end
							source.seek(target.tell())
						
						shutil.copyfileobj(source, target)
				
				download_time = time.time() - start_time
				file_size_mb = os.path.getsize(local_file) / (1024 * 1024)
				speed_mbps = file_size_mb / download_time if download_time > 0 else 0
				
				logging.info("Downloaded %s (%.2f MB) in %.2fs (%.2f MB/s)", 
							prefixed, file_size_mb, download_time, speed_mbps)
				local_files.append(local_file)
				success = True
				break
				
			except Exception as e:
				logging.warning("Attempt %d/%d failed for %s: %s", attempt + 1, max_retries, filename, str(e))
				if attempt < max_retries - 1:
					wait_time = (attempt + 1) * 10  # Exponential backoff
					logging.info("Waiting %d seconds before retry...", wait_time)
					time.sleep(wait_time)
				else:
					logging.error("All %d attempts failed for %s", max_retries, filename)
					# Clean up partial file on final failure
					if os.path.exists(local_file):
						os.remove(local_file)
					raise
		
		if not success:
			raise Exception(f"Failed to download {filename} after {max_retries} attempts")
	
	logging.info("All files downloaded successfully to %s", local_dir)
	return local_files


def bulk_insert_chunk(conn, chunk: pd.DataFrame, table_name: str) -> None:
	"""High-performance bulk insert using parameterized queries with executemany"""
	columns = list(chunk.columns)
	columns_str = ', '.join([f'`{col}`' for col in columns])
	placeholders = ', '.join(['%s'] * len(columns))
	
	# Build parameterized INSERT statement (no string concatenation needed)
	sql = f"INSERT INTO test.{table_name} ({columns_str}) VALUES ({placeholders})"
	
	# Convert DataFrame to list of tuples efficiently
	# Replace NaN with None (MySQL interprets None as NULL)
	chunk_clean = chunk.where(pd.notna(chunk), None)
	values = [tuple(row) for row in chunk_clean.values]
	
	# Use raw connection for executemany (much faster than string concatenation)
	# Get the underlying connection from SQLAlchemy
	raw_conn = conn.connection
	cursor = raw_conn.cursor()
	try:
		cursor.executemany(sql, values)
		# Note: commit is handled by the transaction context manager
	finally:
		cursor.close()


def import_one_day(date_str: str, engine: Engine, table_name: str, chunksize: int = 10000, batch_size: int = 10000, use_bulk_insert: bool = True, verbose: bool = False, download_local: bool = True, download_timeout: int = 300, download_retries: int = 3, block_timestamp_type: str = "datetime", progress: Optional[dict] = None) -> int:
	fs = pa_fs.S3FileSystem(region="us-east-2", anonymous=True)
	files = _list_parquet_files(fs, date_str)
	if not files:
		logging.error("No Parquet files found for date=%s", date_str)
		sys.exit(2)

	# Download files to local if requested
	if download_local:
		local_files = download_files_to_local(fs, files, date_str, max_retries=download_retries, timeout_seconds=download_timeout)
		files = local_files
		fs = None  # No filesystem needed for local files

	total_size = 0
	for file_path in files:
		try:
			if fs:
				total_size += fs.get_file_info(file_path).size
			else:
				total_size += os.path.getsize(file_path)
		except Exception:
			pass
	total_size_gb = total_size / (1024 * 1024 * 1024)
	logging.info("Date %s: %d files, %.2f GB", date_str, len(files), total_size_gb)

	# Create dataset from files (local or S3)
	if fs:
		dataset = ds.dataset(files, filesystem=fs, format="parquet")
	else:
		dataset = ds.dataset(files, format="parquet")

	available = set(dataset.schema.names)
	selected_columns = [c for c in PREFERRED_COLUMNS if c in available]
	scanner = dataset.scanner(columns=selected_columns, batch_size=batch_size)

	total_rows = 0
	batch_idx = 0
	for batch in scanner.to_batches():
		batch_idx += 1
		df = batch.to_pandas(types_mapper=None)

		# Print first record field information only when verbose
		if verbose and batch_idx == 1 and len(df) > 0:
			print("\n" + "="*100)
			print("原始数据第一条记录字段信息 (First Record Field Information)")
			print("="*100)
			print(f"{'字段名称':<20} {'字段值':<50} {'字段类型':<20}")
			print("-" * 100)
			first_row = df.iloc[0]
			for col in df.columns:
				value = first_row[col]
				# Show datetime type for block_timestamp
				if col == "block_timestamp":
					col_type = "datetime (Unix timestamp)"
				else:
					col_type = str(df[col].dtype)
				# Handle NaN values and truncate very long values for display
				if pd.isna(value):
					value_str = "<NULL>"
				else:
					# Convert block_timestamp to datetime format for display
					if col == "block_timestamp":
						try:
							# Try to convert Unix timestamp to datetime
							if isinstance(value, (int, float)) and value > 0:
								dt = datetime.fromtimestamp(value)
								value_str = dt.strftime("%Y-%m-%d %H:%M:%S") + f" (timestamp: {int(value)})"
							else:
								value_str = str(value)
						except (ValueError, OSError, OverflowError):
							value_str = str(value)
					else:
						value_str = str(value)
					if len(value_str) > 50:
						value_str = value_str[:47] + "..."
				print(f"{col:<20} {value_str:<50} {col_type:<20}")
			print("="*100 + "\n")

		df = normalize_hex(df)
		df = validate_and_clean_data(df)
		
		# Convert block_timestamp based on block_timestamp_type
		if "block_timestamp" in df.columns:
			if block_timestamp_type == "datetime":
				# Convert block_timestamp from Unix timestamp to datetime
				# Check if already datetime type - if so, skip conversion
				if not pd.api.types.is_datetime64_any_dtype(df["block_timestamp"]):
					# First convert to numeric to handle any string representations
					df["block_timestamp"] = pd.to_numeric(df["block_timestamp"], errors="coerce")
					
					# Check if values are in milliseconds (typical for Ethereum: > 1e12) or seconds
					sample_val = df["block_timestamp"].dropna()
					if len(sample_val) > 0:
						median_val = sample_val.median()
						if median_val > 1e12:
							# Values are in milliseconds, convert to seconds first
							if batch_idx == 1:
								logging.info(f"Converting block_timestamp: detected millisecond timestamps, converting to seconds...")
							df["block_timestamp"] = df["block_timestamp"] / 1000.0
						elif median_val <= 0:
							logging.error(f"Invalid timestamp values detected! Median: {median_val}")
					
					# Convert Unix timestamp to datetime
					df["block_timestamp"] = pd.to_datetime(df["block_timestamp"], unit='s', errors='coerce')
					
					# Check for invalid dates and log warning
					invalid_count = df["block_timestamp"].isna().sum()
					if invalid_count > 0:
						logging.warning(f"Found {invalid_count} invalid block_timestamp values that could not be converted to datetime")
			else:
				# block_timestamp_type == "bigint"
				# Keep as numeric (Unix timestamp), ensure it's integer type
				if not pd.api.types.is_integer_dtype(df["block_timestamp"]):
					# Convert to numeric first
					df["block_timestamp"] = pd.to_numeric(df["block_timestamp"], errors="coerce")
					
					# Check if values are in milliseconds (typical for Ethereum: > 1e12) or seconds
					sample_val = df["block_timestamp"].dropna()
					if len(sample_val) > 0:
						median_val = sample_val.median()
						if median_val > 1e12:
							# Values are in milliseconds, convert to seconds first
							if batch_idx == 1:
								logging.info(f"Converting block_timestamp: detected millisecond timestamps, converting to seconds...")
							df["block_timestamp"] = df["block_timestamp"] / 1000.0
					
					# Convert to integer (Unix timestamp in seconds)
					df["block_timestamp"] = df["block_timestamp"].astype('Int64')  # Nullable integer type

		df["date"] = str(date_str)
		df["random_flag"] = [random.choice([True, False]) for _ in range(len(df))]

		if len(df) == 0:
			continue

		for start in range(0, len(df), chunksize):
			end = min(start + chunksize, len(df))
			chunk = df.iloc[start:end].copy()

			# Use individual transaction for each chunk to ensure data is committed
			with engine.begin() as conn:
				if use_bulk_insert:
					try:
						bulk_insert_chunk(conn, chunk, table_name)
					except Exception as e:
						logging.warning("Bulk insert failed, falling back to to_sql: %s", str(e))
						chunk.to_sql(
							table_name,
							con=conn,
							schema="eth",
							if_exists="append",
							index=False,
							method="multi",
							chunksize=None,
						)
				else:
					chunk.to_sql(
						table_name,
						con=conn,
						schema="eth",
						if_exists="append",
						index=False,
						method="multi",
						chunksize=None,  # Disable pandas chunking since we're already chunking manually
					)
				
			total_rows += len(chunk)
			if progress is not None:
				progress["current_day_rows"] = total_rows

	logging.info("Date %s done: %d rows", date_str, total_rows)
	return total_rows


def parse_args() -> argparse.Namespace:
	parser = argparse.ArgumentParser(description="Import Ethereum transactions to TiDB")
	# New preferred inputs
	parser.add_argument("--start-date", default=DEFAULT_START_DATE, help="Start date in YYYY-MM-DD (inclusive)")
	parser.add_argument("--days", type=int, default=DEFAULT_DAYS, help="Number of days to import, counting backwards from start-date")
	parser.add_argument("--source", choices=["download", "local", "s3"], default=DEFAULT_SOURCE, help="Input source: download (S3->local then read), local (read local only), s3 (read S3 directly)")
	parser.add_argument("--local-dir", default=DEFAULT_LOCAL_DIR, help="Local directory for parquet files")
	# Backward compatibility
	parser.add_argument("--date", required=False, help="Date in YYYY-MM-DD, e.g. 2025-10-25")
	parser.add_argument("--dates", required=False, help="Comma-separated dates YYYY-MM-DD,YYYY-MM-DD")
	parser.add_argument("--chunksize", type=int, default=DEFAULT_CHUNKSIZE, help="Rows per insert batch")
	parser.add_argument("--batch-size", type=int, default=DEFAULT_BATCH_SIZE, help="PyArrow scanner batch size")
	parser.add_argument("--create-new-table", action="store_true", default=DEFAULT_CREATE_NEW_TABLE, help="Create a new table with timestamp instead of using existing table")
	parser.add_argument("--no-bulk-insert", action="store_true", help="Use pandas to_sql instead of bulk insert")
	parser.add_argument("--download-timeout", type=int, default=DEFAULT_DOWNLOAD_TIMEOUT, help="Download timeout in seconds")
	parser.add_argument("--download-retries", type=int, default=DEFAULT_DOWNLOAD_RETRIES, help="Max download retries")
	parser.add_argument("--download-workers", type=int, default=DEFAULT_DOWNLOAD_WORKERS, help="Concurrent download workers")
	parser.add_argument("--index-type", choices=["fts", "hybrid"], default=DEFAULT_INDEX_TYPE, help="Index type: fts (FULLTEXT) or hybrid (Hybrid index)")
	parser.add_argument("--block-timestamp-type", choices=["datetime", "bigint"], default=DEFAULT_BLOCK_TIMESTAMP_TYPE, help="block_timestamp column type: datetime (DATETIME) or bigint (BIGINT)")
	parser.add_argument("--verbose", "-v", action="store_true", help="Enable verbose logging")
	return parser.parse_args()


def main() -> None:
	args = parse_args()
	configure_logging(args.verbose)

	# Resolve date list: deprecated flags override, otherwise use start-date + days (defaults already applied by argparse)
	if args.dates or args.date:
		dates: List[str] = []
		if args.dates:
			dates = [d.strip() for d in args.dates.split(',') if d.strip()]
		elif args.date:
			dates = [args.date]
		for d in dates:
			try:
				datetime.strptime(d, "%Y-%m-%d")
			except ValueError:
				logging.error("Invalid date; expected YYYY-MM-DD: %s", d)
				sys.exit(1)
	else:
		try:
			start_dt = datetime.strptime(args.start_date, "%Y-%m-%d")
		except ValueError:
			logging.error("Invalid start date; expected YYYY-MM-DD: %s", args.start_date)
			sys.exit(1)
		if args.days <= 0:
			logging.error("--days must be positive")
			sys.exit(1)
		dates = [(start_dt - timedelta(days=i)).strftime("%Y-%m-%d") for i in range(args.days)]

	engine = get_engine()

	# Resolve effective settings (argparse already applied defaults from constants)
	chunksize = args.chunksize
	batch_size = args.batch_size
	source = args.source
	local_dir = args.local_dir
	download_timeout = args.download_timeout
	download_retries = args.download_retries
	download_workers = args.download_workers

	# Create new table or use existing one
	table_name = ensure_schema(engine, create_new_table=args.create_new_table, index_type=args.index_type, block_timestamp_type=args.block_timestamp_type)

	total_days = len(dates)
	total_rows_global = 0

	# If source is download, download all days concurrently into one directory
	if source == "download":
		fs = pa_fs.S3FileSystem(region="us-east-2", anonymous=True)
		os.makedirs(local_dir, exist_ok=True)
		logging.info("Starting concurrent downloads for %d day(s) into %s with %d workers", len(dates), local_dir, download_workers)
		futures = []
		with ThreadPoolExecutor(max_workers=download_workers) as executor:
			for d in dates:
				s3_files = _list_parquet_files(fs, d)
				if not s3_files:
					logging.warning("No files to download for date=%s", d)
					continue
				futures.append(executor.submit(download_files_to_local, fs, s3_files, d, local_dir, download_retries, download_timeout))
			for f in as_completed(futures):
				try:
					_ = f.result()
				except Exception as e:
					logging.error("One download task failed: %s", str(e))
		logging.info("All download tasks completed")

	# Sequentially import per day, reading local if available
	days_done = 0
	for d in dates:
		try:
			if source in ("download", "local"):
				# Build local file list by date prefix
				if not os.path.isdir(local_dir):
					logging.warning("Local directory not found: %s, skipping date=%s", local_dir, d)
					continue
				local_files = [os.path.join(local_dir, f) for f in os.listdir(local_dir) if f.startswith(f"{d}__") and f.endswith('.parquet')]
				if not local_files:
					logging.warning("No local files found for date=%s in %s, skipping this date", d, local_dir)
					continue
				logging.info("Importing from local files for date=%s (files=%d)", d, len(local_files))
				progress["current_day_rows"] = 0
				dataset = ds.dataset(local_files, format="parquet")
				available = set(dataset.schema.names)
				selected_columns = [c for c in PREFERRED_COLUMNS if c in available]
				scanner = dataset.scanner(columns=selected_columns, batch_size=batch_size)
				total_rows = 0
				batch_idx = 0
				for batch in scanner.to_batches():
					batch_idx += 1
					df = batch.to_pandas(types_mapper=None)

					if args.verbose and batch_idx == 1 and len(df) > 0 and d == dates[0]:
						print("\n" + "="*100)
						print("原始数据第一条记录字段信息 (First Record Field Information)")
						print("="*100)
						print(f"{'字段名称':<20} {'字段值':<50} {'字段类型':<20}")
						print("-" * 100)
						first_row = df.iloc[0]
						for col in df.columns:
							value = first_row[col]
							# Show datetime type for block_timestamp
							if col == "block_timestamp":
								col_type = "datetime (Unix timestamp)"
							else:
								col_type = str(df[col].dtype)
							# Handle NaN values and truncate very long values for display
							if pd.isna(value):
								value_str = "<NULL>"
							else:
								# Convert block_timestamp to datetime format for display
								if col == "block_timestamp":
									try:
										# Try to convert Unix timestamp to datetime
										if isinstance(value, (int, float)) and value > 0:
											dt = datetime.fromtimestamp(value)
											value_str = dt.strftime("%Y-%m-%d %H:%M:%S") + f" (timestamp: {int(value)})"
										else:
											value_str = str(value)
									except (ValueError, OSError, OverflowError):
										value_str = str(value)
								else:
									value_str = str(value)
								if len(value_str) > 50:
									value_str = value_str[:47] + "..."
							print(f"{col:<20} {value_str:<50} {col_type:<20}")
						print("="*100 + "\n")
					df = normalize_hex(df)
					# Ensure input fits TEXT column
					df = validate_and_clean_data(df)
					# Convert block_timestamp based on block_timestamp_type
					if "block_timestamp" in df.columns:
						if args.block_timestamp_type == "datetime":
							# Convert block_timestamp from Unix timestamp to datetime
							# Check if already datetime type - if so, skip conversion
							if not pd.api.types.is_datetime64_any_dtype(df["block_timestamp"]):
								# First convert to numeric to handle any string representations
								df["block_timestamp"] = pd.to_numeric(df["block_timestamp"], errors="coerce")
								
								# Check if values are in milliseconds (typical for Ethereum: > 1e12) or seconds
								sample_val = df["block_timestamp"].dropna()
								if len(sample_val) > 0:
									median_val = sample_val.median()
									if median_val > 1e12:
										# Values are in milliseconds, convert to seconds first
										if batch_idx == 1 and d == dates[0]:
											logging.info(f"Converting block_timestamp: detected millisecond timestamps, converting to seconds...")
										df["block_timestamp"] = df["block_timestamp"] / 1000.0
									elif median_val <= 0:
										logging.error(f"Invalid timestamp values detected! Median: {median_val}")
								
								# Convert Unix timestamp to datetime
								df["block_timestamp"] = pd.to_datetime(df["block_timestamp"], unit='s', errors='coerce')
								
								# Check for invalid dates and log warning
								invalid_count = df["block_timestamp"].isna().sum()
								if invalid_count > 0:
									logging.warning(f"Found {invalid_count} invalid block_timestamp values that could not be converted to datetime")
						else:
							# block_timestamp_type == "bigint"
							# Keep as numeric (Unix timestamp), ensure it's integer type
							if not pd.api.types.is_integer_dtype(df["block_timestamp"]):
								# Convert to numeric first
								df["block_timestamp"] = pd.to_numeric(df["block_timestamp"], errors="coerce")
								
								# Check if values are in milliseconds (typical for Ethereum: > 1e12) or seconds
								sample_val = df["block_timestamp"].dropna()
								if len(sample_val) > 0:
									median_val = sample_val.median()
									if median_val > 1e12:
										# Values are in milliseconds, convert to seconds first
										if batch_idx == 1 and d == dates[0]:
											logging.info(f"Converting block_timestamp: detected millisecond timestamps, converting to seconds...")
										df["block_timestamp"] = df["block_timestamp"] / 1000.0
								
								# Convert to integer (Unix timestamp in seconds)
								df["block_timestamp"] = df["block_timestamp"].astype('Int64')  # Nullable integer type
					df["date"] = str(d)
					# Generate random boolean values for random_flag column
					df["random_flag"] = [random.choice([True, False]) for _ in range(len(df))]
					if len(df) == 0:
						continue
					for start in range(0, len(df), chunksize):
						end = min(start + chunksize, len(df))
						chunk = df.iloc[start:end].copy()
						with engine.begin() as conn:
							try:
								bulk_insert_chunk(conn, chunk, table_name)
							except Exception:
								chunk.to_sql(table_name, con=conn, schema="eth", if_exists="append", index=False, method="multi", chunksize=None)
						total_rows += len(chunk)
				days_done += 1
				total_rows_global += total_rows
				logging.info("Progress: %d/%d days, %d rows", days_done, total_days, total_rows_global)
			elif source == "s3":
				rows = import_one_day(d, engine, table_name, chunksize=chunksize, batch_size=batch_size, use_bulk_insert=not args.no_bulk_insert, verbose=args.verbose, download_local=False, download_timeout=download_timeout, download_retries=download_retries, block_timestamp_type=args.block_timestamp_type)
				days_done += 1
				total_rows_global += rows
				logging.info("Progress: %d/%d days, %d rows", days_done, total_days, total_rows_global)
		except Exception as e:
			logging.error("Error processing date=%s: %s, skipping this date and continuing with next date", d, str(e), exc_info=args.verbose)
			continue


if __name__ == "__main__":
	main()
