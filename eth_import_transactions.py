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


def configure_logging(verbose: bool = False) -> None:
	level = logging.DEBUG if verbose else logging.INFO
	logging.basicConfig(
		level=level,
		format="%(asctime)s %(levelname)s %(message)s",
		handlers=[logging.StreamHandler(sys.stdout)],
	)


# Module-level defaults (edit here to change behavior without CLI)
DEFAULT_START_DATE = "2025-10-11"
DEFAULT_DAYS = 8
DEFAULT_CHUNKSIZE = 2000
DEFAULT_BATCH_SIZE = 2000
DEFAULT_SOURCE = "download"  # download | local | s3
DEFAULT_LOCAL_DIR = "local_data_multi"
DEFAULT_CREATE_NEW_TABLE = True
DEFAULT_DOWNLOAD_TIMEOUT = 300
DEFAULT_DOWNLOAD_RETRIES = 3
DEFAULT_DOWNLOAD_WORKERS = 8


## Removed legacy progress/memory helpers (save_progress, load_progress, cleanup_progress, get_memory_usage)


def get_engine() -> Engine:
	user = os.getenv("TIDB_USER", "root")
	password = os.getenv("TIDB_PASSWORD", "")
	host = os.getenv("TIDB_HOST", "127.0.0.1")
	port = int(os.getenv("TIDB_PORT", "4000"))
	db = os.getenv("TIDB_DB", "eth")

	url = f"mysql+mysqlconnector://{user}:{password}@{host}:{port}/{db}?charset=utf8mb4"
	return create_engine(url, pool_pre_ping=True, pool_recycle=3600)


DDL_CREATE_DATABASE = """
CREATE DATABASE IF NOT EXISTS eth;
"""

DDL_CREATE_TABLE_TEMPLATE = """
CREATE TABLE IF NOT EXISTS eth.{table_name} (
  date VARCHAR(10) NOT NULL,
  hash VARCHAR(66) NOT NULL,
  block_timestamp BIGINT NOT NULL,
  nonce BIGINT NULL,
  transaction_index BIGINT NULL,
  from_address VARCHAR(42) NULL,
  to_address VARCHAR(42) NULL,
  value DOUBLE NULL,
  gas BIGINT NULL,
  gas_price BIGINT NULL,
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
  PRIMARY KEY (block_timestamp)
);
"""

DDL_ADD_FULLTEXT = """
ALTER TABLE eth.{table_name} ADD FULLTEXT INDEX ft_index (date,hash,from_address,to_address,receipt_contract_address,block_hash) WITH PARSER standard;
"""


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
		
		# Check for long input data and log warnings near TEXT limit (~65,535 chars)
		long_inputs = df["input"].str.len() > 60000
		if long_inputs.any():
			long_count = long_inputs.sum()
			logging.warning("Found %d transactions with input data longer than 1MB", long_count)
			
			# Log some examples of long inputs
			long_examples = df[long_inputs]["input"].str.len().head(5)
			for idx, length in long_examples.items():
				logging.warning("Transaction at index %d has input length: %d characters", idx, length)
		
		# Truncate to MySQL TEXT max length (approx 65,535 chars)
		max_input_length = 65535
		df["input"] = df["input"].str.slice(0, max_input_length)
		
		# Log if any truncation occurred
		truncated = df["input"].str.len() >= max_input_length
		if truncated.any():
			truncated_count = truncated.sum()
			logging.warning("Truncated %d input fields to %d characters", truncated_count, max_input_length)
	
	return df


def generate_table_name() -> str:
	"""Generate table name with timestamp"""
	timestamp = datetime.now().strftime("%Y%m%d%H%M%S")
	return f"eth_transactions_{timestamp}"


def ensure_schema(engine: Engine, create_new_table: bool = False, table_name: str = None) -> str:
	"""
	Ensure schema exists. If create_new_table, create a new table with timestamp.
	Returns the table name to use.
	"""
	if create_new_table:
		table_name = generate_table_name()
		logging.info("Creating new table with timestamp: %s", table_name)
		with engine.begin() as conn:
			logging.info("Creating database...")
			conn.execute(text(DDL_CREATE_DATABASE.strip()))
			
			logging.info("Creating table %s...", table_name)
			create_sql = DDL_CREATE_TABLE_TEMPLATE.format(table_name=table_name)
			conn.execute(text(create_sql.strip()))
			
			logging.info("Adding FULLTEXT index to %s...", table_name)
			ft_sql = DDL_ADD_FULLTEXT.format(table_name=table_name)
			logging.info("FULLTEXT index added to: %s", ft_sql.strip())

			# conn.execute(text(ft_sql.strip()))
			# logging.info("FULLTEXT index added to %s", ft_sql.strip())
		return table_name
	else:
		# Use existing table name or default
		if table_name is None:
			table_name = "eth_transactions"
		logging.info("Using existing table: %s", table_name)
		with engine.begin() as conn:
			logging.info("Creating database...")
			conn.execute(text(DDL_CREATE_DATABASE.strip()))
			# Check if table exists, if not create default one
			result = conn.execute(text("SELECT COUNT(*) FROM information_schema.tables WHERE table_schema='eth' AND table_name=:tname"), {"tname": table_name})
			if result.scalar() == 0:
				logging.info("Table %s does not exist, creating it...", table_name)
				create_sql = DDL_CREATE_TABLE_TEMPLATE.format(table_name=table_name)
				conn.execute(text(create_sql.strip()))
				# Add FULLTEXT index only if table was just created
				# ft_sql = DDL_ADD_FULLTEXT.format(table_name=table_name)
				# conn.execute(text(ft_sql.strip()))
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
	"""High-performance bulk insert using MySQL multi-row INSERT"""
	columns = list(chunk.columns)
	columns_str = ', '.join([f'`{col}`' for col in columns])
	
	# Create VALUES clause for all rows
	values_list = []
	for _, row in chunk.iterrows():
		# Convert each value to proper format
		row_values = []
		for val in row:
			if pd.isna(val):
				row_values.append('NULL')
			elif isinstance(val, str):
				# Escape single quotes and wrap in quotes
				escaped_val = val.replace("'", "''")
				row_values.append(f"'{escaped_val}'")
			else:
				row_values.append(str(val))
		values_list.append(f"({', '.join(row_values)})")
	
	# Build the complete INSERT statement
	sql = f"""
	INSERT INTO eth.{table_name} ({columns_str})
	VALUES {', '.join(values_list)}
	"""
	
	# Execute the bulk insert
	conn.execute(text(sql))


def import_one_day(date_str: str, engine: Engine, table_name: str, chunksize: int = 10000, batch_size: int = 10000, use_bulk_insert: bool = True, verbose: bool = False, download_local: bool = True, download_timeout: int = 300, download_retries: int = 3) -> None:
	fs = pa_fs.S3FileSystem(region="us-east-2", anonymous=True)
	files = _list_parquet_files(fs, date_str)
	if not files:
		logging.error("No Parquet files found for date=%s", date_str)
		sys.exit(2)

	logging.info("Found %d parquet files for date=%s", len(files), date_str)
	
	# Download files to local if requested
	if download_local:
		local_files = download_files_to_local(fs, files, date_str, max_retries=download_retries, timeout_seconds=download_timeout)
		# Use local files instead of S3 files
		files = local_files
		fs = None  # No filesystem needed for local files
	
	# Display file sizes
	total_size = 0
	for i, file_path in enumerate(files):
		try:
			if fs:
				file_info = fs.get_file_info(file_path)
				file_size_mb = file_info.size / (1024 * 1024)
				total_size += file_info.size
			else:
			# Local file
				file_size_mb = os.path.getsize(file_path) / (1024 * 1024)
				total_size += os.path.getsize(file_path)
			logging.info("File %d: %s (%.2f MB)", i+1, os.path.basename(file_path), file_size_mb)
		except Exception as e:
			logging.warning("Could not get size for file %s: %s", file_path, str(e))
	
	total_size_gb = total_size / (1024 * 1024 * 1024)
	logging.info("Total dataset size: %.2f GB", total_size_gb)
	
	# Create dataset from files (local or S3)
	if fs:
		dataset = ds.dataset(files, filesystem=fs, format="parquet")
	else:
		dataset = ds.dataset(files, format="parquet")

	available = set(dataset.schema.names)
	selected_columns = [c for c in PREFERRED_COLUMNS if c in available]
	missing = [c for c in PREFERRED_COLUMNS if c not in available]
	not_selected = [c for c in available if c not in PREFERRED_COLUMNS]
	logging.info("Schema columns available=%d, selected=%d, missing=%d", len(available), len(selected_columns), len(missing))
	if missing:
		logging.warning("Missing columns skipped: %s", ", ".join(missing))
	if not_selected:
		logging.info("Available columns not selected: %s", ", ".join(sorted(not_selected)))

	logging.info("Creating scanner with batch_size=%d...", batch_size)
	scanner = dataset.scanner(columns=selected_columns, batch_size=batch_size)
	logging.info("Scanner created successfully, starting to process batches...")

	total_rows = 0
	batch_idx = 0
	
	logging.info("Starting batch processing...")
	for batch in scanner.to_batches():
		batch_idx += 1
		batch_start_time = time.time()
		logging.info("Processing record batch #%d with %d rows", batch_idx, batch.num_rows)
		
		# Calculate progress percentage
		if total_size > 0:
			# Estimate bytes processed based on rows processed
			estimated_bytes_processed = (total_rows + batch.num_rows) * (total_size / dataset.count_rows())
			progress_percent = (estimated_bytes_processed / total_size) * 100
			logging.info("Estimated progress: %.1f%% (%.2f GB / %.2f GB)", 
						progress_percent, estimated_bytes_processed / (1024**3), total_size_gb)
		
		logging.info("Converting batch to pandas DataFrame...")
		df = batch.to_pandas(types_mapper=None)
		logging.info("DataFrame created with shape: %s", df.shape)
		
		# Only normalize hex-like columns and set date string; keep numeric types as-is
		logging.info("Normalizing hex columns...")
		df = normalize_hex(df)
		
		# Validate and clean data to prevent insertion errors
		logging.info("Validating and cleaning data...")
		df = validate_and_clean_data(df)
		
		# Ensure block_timestamp is numeric (unix seconds) for BIGINT column
		if "block_timestamp" in df.columns:
			logging.info("Converting block_timestamp to numeric...")
			df["block_timestamp"] = pd.to_numeric(df["block_timestamp"], errors="coerce")
		
		logging.info("Setting date column to: %s", date_str)
		df["date"] = str(date_str)

		if len(df) == 0:
			logging.info("Batch #%d is empty after transforms; skipping", batch_idx)
			continue

		logging.info("Starting chunked insertion for batch #%d...")
		for start in range(0, len(df), chunksize):
			end = min(start + chunksize, len(df))
			logging.info("Inserting rows %d..%d of batch #%d", start, end - 1, batch_idx)
			
			chunk = df.iloc[start:end].copy()
			logging.info("Chunk prepared, executing insert...")
			
			# Use individual transaction for each chunk to ensure data is committed
			with engine.begin() as conn:
				start_time = time.time()
				
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
				
				insert_time = time.time() - start_time
			
			total_rows += len(chunk)
			logging.info("Chunk inserted and committed successfully. Rows in chunk: %d, Total rows: %d, Insert time: %.2fs", 
						len(chunk), total_rows, insert_time)
			
		# Log batch performance statistics
		batch_total_time = time.time() - batch_start_time
		logging.info("Batch #%d completed in %.2fs (%.2f rows/sec)", batch_idx, batch_total_time, batch.num_rows / batch_total_time)

	logging.info("Done. Total rows inserted: %d", total_rows)


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
	table_name = ensure_schema(engine, create_new_table=args.create_new_table)

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
	for d in dates:
		if source in ("download", "local"):
			# Build local file list by date prefix
			if not os.path.isdir(local_dir):
				logging.error("Local directory not found: %s", local_dir)
				sys.exit(2)
			local_files = [os.path.join(local_dir, f) for f in os.listdir(local_dir) if f.startswith(f"{d}__") and f.endswith('.parquet')]
			if not local_files:
				logging.error("No local files found for date=%s in %s", d, local_dir)
				sys.exit(2)
			# Temporarily adapt import_one_day: call with fake fs path list by using local-only branch
			# We reuse import_one_day by bypassing S3 discovery: set download_local=False and fake S3 listing via monkey list
			# Easiest: create dataset directly here and reuse inner logic? For simplicity, call import_one_day but with download_local=False after swapping _list_parquet_files? Instead, run a small local import loop here:
			logging.info("Importing from local files for date=%s (files=%d)", d, len(local_files))
			# Create dataset
			dataset = ds.dataset(local_files, format="parquet")
			available = set(dataset.schema.names)
			selected_columns = [c for c in PREFERRED_COLUMNS if c in available]
			missing = [c for c in PREFERRED_COLUMNS if c not in available]
			not_selected = [c for c in available if c not in PREFERRED_COLUMNS]
			logging.info("Schema columns available=%d, selected=%d, missing=%d", len(available), len(selected_columns), len(missing))
			if missing:
				logging.warning("Missing columns skipped: %s", ", ".join(missing))
			if not_selected:
				logging.info("Available columns not selected: %s", ", ".join(sorted(not_selected)))
			scanner = dataset.scanner(columns=selected_columns, batch_size=batch_size)
			batch_idx = 0
			total_rows = 0
			for batch in scanner.to_batches():
				batch_idx += 1
				df = batch.to_pandas(types_mapper=None)
				df = normalize_hex(df)
				# Ensure input fits TEXT column
				df = validate_and_clean_data(df)
				if "block_timestamp" in df.columns:
					df["block_timestamp"] = pd.to_numeric(df["block_timestamp"], errors="coerce")
				df["date"] = str(d)
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
					total_rows += len(df)
			logging.info("Date %s import complete, total rows: %d", d, total_rows)
		elif source == "s3":
			import_one_day(d, engine, table_name, chunksize=chunksize, batch_size=batch_size, use_bulk_insert=not args.no_bulk_insert, verbose=args.verbose, download_local=False, download_timeout=download_timeout, download_retries=download_retries)


if __name__ == "__main__":
	main()
