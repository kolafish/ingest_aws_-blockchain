#!/usr/bin/env python3
"""
Utility script to convert local Ethereum parquet files into the exact schema
expected by eth_import_transactions before writing them back as parquet.
"""

import argparse
import logging
import sys
from pathlib import Path
from typing import List, Optional

import numpy as np
import pandas as pd
from pandas.api import types as pd_types
import pyarrow as pa
import pyarrow.dataset as ds
import pyarrow.parquet as pq

CURRENT_DIR = Path(__file__).resolve().parent
if str(CURRENT_DIR) not in sys.path:
	sys.path.insert(0, str(CURRENT_DIR))

from eth_import_transactions import (  # noqa: E402
	PREFERRED_COLUMNS,
	normalize_hex,
	validate_and_clean_data,
)

DEFAULT_BATCH_SIZE = 5000
DEFAULT_OUTPUT_DIR = "converted_parquet"
DEFAULT_BLOCK_TIMESTAMP_TYPE = "bigint"
DEFAULT_COMPRESSION = "snappy"

MIN_UNIX_SECONDS = int(pd.Timestamp.min.timestamp())
MAX_UNIX_SECONDS = int(pd.Timestamp.max.timestamp())
MILLISECOND_THRESHOLD = 1e11  # seconds ~ 3,171 years, good enough to distinguish ms
MICROSECOND_THRESHOLD = 1e13
NANOSECOND_THRESHOLD = 1e15

TABLE_COLUMNS = [
	"date",
	"hash",
	"block_timestamp",
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
	"block_number",
	"block_hash",
	"max_fee_per_gas",
	"max_priority_fee_per_gas",
	"transaction_type",
	"receipt_effective_gas_price",
]


def configure_logging(verbose: bool = False) -> None:
	level = logging.DEBUG if verbose else logging.INFO
	logging.basicConfig(
		level=level,
		format="%(asctime)s %(levelname)s %(message)s",
	)


def valid_date_string(value: str) -> bool:
	if not value:
		return False
	try:
		pd.Timestamp(value)
		return True
	except (ValueError, TypeError):
		return False


def infer_date(file_path: Path, explicit_date: Optional[str]) -> str:
	if explicit_date:
		if not valid_date_string(explicit_date):
			raise ValueError(f"Invalid --date value: {explicit_date}")
		return explicit_date
	name = file_path.name
	candidates: List[str] = []
	if "__" in name:
		candidates.append(name.split("__", 1)[0])
	for part in file_path.parts:
		if part.startswith("date="):
			candidates.append(part.split("=", 1)[1])
	for candidate in candidates:
		if valid_date_string(candidate):
			return candidate
	raise ValueError(f"Unable to infer date for file: {file_path}")


def normalize_unix_seconds(series: pd.Series, context: str) -> pd.Series:
	sample_val = series.dropna()
	if len(sample_val) == 0:
		return series
	median_val = sample_val.median()
	if median_val <= 0:
		logging.error("%s invalid timestamp median value: %s", context, median_val)
		return series
	if median_val > NANOSECOND_THRESHOLD:
		logging.info("%s detected nanosecond timestamps, converting to seconds", context)
		return series / 1_000_000_000.0
	if median_val > MICROSECOND_THRESHOLD:
		logging.info("%s detected microsecond timestamps, converting to seconds", context)
		return series / 1_000_000.0
	if median_val > MILLISECOND_THRESHOLD:
		logging.info("%s detected millisecond timestamps, converting to seconds", context)
		return series / 1_000.0
	return series


def _coerce_numeric_seconds(series: pd.Series, context: str) -> Optional[pd.Series]:
	numeric = pd.to_numeric(series, errors="coerce")
	if numeric.isna().all():
		return None
	numeric = pd.Series(numeric, index=series.index, dtype="float64")
	inf_mask = np.isinf(numeric.to_numpy())
	if inf_mask.any():
		logging.warning("%s found %d infinite block_timestamp values; setting to NaN", context, inf_mask.sum())
		numeric.iloc[inf_mask] = np.nan
	numeric = normalize_unix_seconds(numeric, context)
	out_of_range_mask = numeric.notna() & ~numeric.between(MIN_UNIX_SECONDS, MAX_UNIX_SECONDS)
	if out_of_range_mask.any():
		logging.warning(
			"%s found %d block_timestamp values outside supported range; setting to NaN",
			context,
			out_of_range_mask.sum(),
		)
		numeric.loc[out_of_range_mask] = np.nan
	if numeric.notna().sum() == 0:
		return None
	return numeric


def _coerce_datetime_direct(series: pd.Series, context: str) -> Optional[pd.Series]:
	dt = pd.to_datetime(series, errors="coerce", utc=True)
	if pd_types.is_datetime64tz_dtype(dt.dtype):
		dt = dt.dt.tz_convert(None)
	if dt.isna().all():
		return None
	return dt


def convert_block_timestamp(df: pd.DataFrame, block_timestamp_type: str, context: str) -> pd.DataFrame:
	if "block_timestamp" not in df.columns:
		return df
	original_series = df["block_timestamp"]
	numeric_seconds = _coerce_numeric_seconds(original_series, context)
	fallback_used = False

	if numeric_seconds is not None:
		if block_timestamp_type == "datetime":
			with np.errstate(all="ignore"):
				converted = pd.to_datetime(numeric_seconds, unit="s", errors="coerce")
		else:
			converted = numeric_seconds
	else:
		converted = None

	if converted is None or converted.notna().sum() == 0:
		fallback_used = True
		direct_dt = _coerce_datetime_direct(original_series, context)
		if direct_dt is None:
			logging.warning("%s block_timestamp conversion failed (no numeric or datetime values); dropping batch", context)
			return df.iloc[0:0]
		if block_timestamp_type == "datetime":
			converted = direct_dt
		else:
			seconds = (direct_dt.view("int64") // 1_000_000_000).astype("float64")
			seconds[pd.isna(direct_dt)] = np.nan
			converted = pd.Series(seconds, index=direct_dt.index, dtype="float64")

	valid_mask = converted.notna()
	if not valid_mask.any():
		logging.warning("%s no valid block_timestamp values remain after %s conversion; dropping batch", context, "fallback" if fallback_used else "numeric")
		return df.iloc[0:0]

	dropped = (~valid_mask).sum()
	if dropped > 0:
		logging.warning(
			"%s dropping %d rows with invalid block_timestamp (%s path)",
			context,
			dropped,
			"fallback" if fallback_used else "numeric",
		)

	df = df.loc[valid_mask].copy()
	if block_timestamp_type == "datetime":
		if pd_types.is_datetime64tz_dtype(converted.dtype):
			converted = converted.dt.tz_convert(None)
		df["block_timestamp"] = converted.loc[valid_mask]
	else:
		df["block_timestamp"] = converted.loc[valid_mask].round().astype("Int64")
	return df


def discover_files(input_dir: Path, recursive: bool) -> List[Path]:
	pattern = "**/*.parquet" if recursive else "*.parquet"
	return sorted(input_dir.glob(pattern))


def ensure_output_dir(path: Path) -> None:
	path.mkdir(parents=True, exist_ok=True)


def ensure_all_columns(df: pd.DataFrame) -> pd.DataFrame:
	for col in TABLE_COLUMNS:
		if col not in df.columns:
			df[col] = pd.NA
	return df[TABLE_COLUMNS]


def process_file(
	file_path: Path,
	output_path: Path,
	date_str: str,
	block_timestamp_type: str,
	batch_size: int,
	compression: str,
) -> int:
	log_prefix = f"[{file_path.name}]"
	logging.info("%s reading parquet file", log_prefix)
	dataset = ds.dataset([str(file_path)], format="parquet")
	available = set(dataset.schema.names)
	selected_columns = [c for c in PREFERRED_COLUMNS if c in available]
	missing = [c for c in PREFERRED_COLUMNS if c not in available]
	not_selected = [c for c in available if c not in PREFERRED_COLUMNS]
	logging.info("%s schema: available=%d selected=%d missing=%d", log_prefix, len(available), len(selected_columns), len(missing))
	if missing:
		logging.warning("%s missing columns skipped: %s", log_prefix, ", ".join(missing))
	if not_selected:
		logging.debug("%s columns not needed: %s", log_prefix, ", ".join(sorted(not_selected)))
	scanner = dataset.scanner(columns=selected_columns, batch_size=batch_size)
	writer: Optional[pq.ParquetWriter] = None
	total_rows = 0
	for batch_idx, batch in enumerate(scanner.to_batches(), start=1):
		df = batch.to_pandas(types_mapper=None)
		if df.empty:
			continue
		df = normalize_hex(df)
		df = validate_and_clean_data(df)
		df = convert_block_timestamp(df, block_timestamp_type, f"{log_prefix} batch#{batch_idx}")
		if df.empty:
			logging.warning("%s batch #%d skipped because block_timestamp conversion removed all rows", log_prefix, batch_idx)
			continue
		df["date"] = date_str
		df = ensure_all_columns(df)
		table = pa.Table.from_pandas(df, preserve_index=False)
		if writer is None:
			writer = pq.ParquetWriter(str(output_path), table.schema, compression=compression)
		writer.write_table(table)
		total_rows += len(df)
	if writer is not None:
		writer.close()
		logging.info("%s wrote %d rows to %s", log_prefix, total_rows, output_path)
	else:
		logging.warning("%s no data written (empty source)", log_prefix)
	return total_rows


def parse_args() -> argparse.Namespace:
	parser = argparse.ArgumentParser(description="Convert local Ethereum parquet files to table-ready format.")
	parser.add_argument("--input-dir", required=True, help="Directory containing source parquet files.")
	parser.add_argument("--output-dir", default=DEFAULT_OUTPUT_DIR, help="Directory to write converted parquet files.")
	parser.add_argument("--date", help="Override date value for all files (YYYY-MM-DD).")
	parser.add_argument("--recursive", action="store_true", help="Search input directory recursively.")
	parser.add_argument("--batch-size", type=int, default=DEFAULT_BATCH_SIZE, help="PyArrow scanner batch size.")
	parser.add_argument("--block-timestamp-type", choices=["datetime", "bigint"], default=DEFAULT_BLOCK_TIMESTAMP_TYPE, help="Output block_timestamp type.")
	parser.add_argument("--compression", default=DEFAULT_COMPRESSION, help="Compression codec for output parquet files.")
	parser.add_argument("--overwrite", action="store_true", help="Overwrite existing files in output directory.")
	parser.add_argument("--limit", type=int, help="Process at most this many files.")
	parser.add_argument("--verbose", "-v", action="store_true", help="Enable verbose logging.")
	return parser.parse_args()


def main() -> None:
	args = parse_args()
	configure_logging(args.verbose)
	input_dir = Path(args.input_dir).expanduser().resolve()
	output_dir = Path(args.output_dir).expanduser().resolve()
	if not input_dir.exists():
		raise FileNotFoundError(f"Input directory not found: {input_dir}")
	ensure_output_dir(output_dir)
	files = discover_files(input_dir, args.recursive)
	if args.limit:
		files = files[: args.limit]
	if not files:
		logging.warning("No parquet files found under %s", input_dir)
		return
	logging.info("Found %d parquet file(s) to process", len(files))
	total_rows = 0
	for file_path in files:
		try:
			date_str = infer_date(file_path, args.date)
		except ValueError as exc:
			logging.error("Skipping %s: %s", file_path.name, exc)
			continue
		output_path = output_dir / file_path.name
		if output_path.exists() and not args.overwrite:
			logging.info("Skipping %s because output exists (use --overwrite to replace)", output_path)
			continue
		rows = process_file(
			file_path=file_path,
			output_path=output_path,
			date_str=date_str,
			block_timestamp_type=args.block_timestamp_type,
			batch_size=args.batch_size,
			compression=args.compression,
		)
		total_rows += rows
	logging.info("Done. Total rows written: %d", total_rows)


if __name__ == "__main__":
	main()

