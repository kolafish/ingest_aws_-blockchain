# ingest_aws_-blockchain

Import public AWS Ethereum transactions parquet into TiDB/MySQL-compatible DB.

## Features

- Read from three sources:
  - download: S3 → local directory (concurrent, resumable), then import locally
  - local: read existing local parquet files
  - s3: read directly from S3 without downloading
- Multi-day import: from a start date counting backward N days
- Robust insert: bulk multi-row INSERT with fallback to `to_sql`
- Schema: primary key `(date, hash, block_timestamp)` and `input` as `LONGTEXT`

## Requirements

Python 3.9+ recommended.

Install Python dependencies:

```bash
pip install -r requirements.txt
```

Contents of `requirements.txt`:

```txt
pyarrow==17.0.0
pandas==2.2.3
s3fs==2024.9.0
SQLAlchemy==2.0.36
mysql-connector-python==9.0.0
python-dotenv==1.0.1
```

## Environment variables (TiDB/MySQL)

Set these to point to your TiDB/MySQL-compatible endpoint (defaults shown):

```bash
export TIDB_USER=root
export TIDB_PASSWORD=""
export TIDB_HOST=127.0.0.1
export TIDB_PORT=4000
export TIDB_DB=eth
```

## Usage

Entry script: `eth_import_transactions.py`

Key arguments:

- `--start-date YYYY-MM-DD`: start date (inclusive)
- `--days N`: number of days counting backward from start date
- `--source {download,local,s3}`: input mode
- `--local-dir PATH`: where parquet files are stored/read (for download/local)
- `--drop-table`: drop and recreate table before import (default can be changed in code)
- `--chunksize`: rows per DB insert chunk (default in code)
- `--batch-size`: arrow scanner batch size (default in code)
- Download tuning: `--download-timeout`, `--download-retries`, `--download-workers`
- `-v/--verbose`: verbose logging

### Examples

Download 3 days starting from 2025-10-29 to local and import:

```bash
python eth_import_transactions.py \
  --start-date 2025-10-29 --days 3 \
  --source download \
  --chunksize 1000 --batch-size 1000
```

Import from local only (no download), using date-prefixed filenames in `local_data_multi/`:

```bash
python eth_import_transactions.py \
  --start-date 2025-10-29 --days 3 \
  --source local --local-dir local_data_multi
```

Read directly from S3 (no local files):

```bash
python eth_import_transactions.py \
  --start-date 2025-10-29 --days 3 \
  --source s3
```

Recreate table before import:

```bash
python eth_import_transactions.py --start-date 2025-10-29 --days 1 --drop-table
```

### File naming for local mode

Downloaded files are saved as:

```
<local-dir>/<YYYY-MM-DD>__<original_parquet_name>.parquet
```

The importer uses the `YYYY-MM-DD` prefix to set the `date` column and group files by day.

## Defaults in code

You can change defaults without CLI by editing constants near the top of `main()`:

```python
DEFAULT_START_DATE = "YYYY-MM-DD"
DEFAULT_DAYS = 1
DEFAULT_CHUNKSIZE = 10000
DEFAULT_BATCH_SIZE = 10000
DEFAULT_SOURCE = "download"  # download | local | s3
DEFAULT_LOCAL_DIR = "local_data_multi"
DEFAULT_DROP_TABLE = False
DEFAULT_DOWNLOAD_TIMEOUT = 300
DEFAULT_DOWNLOAD_RETRIES = 3
DEFAULT_DOWNLOAD_WORKERS = 4
```

## Schema

The script creates database `eth` and table `eth.eth_transactions` if not present:

```sql
CREATE TABLE IF NOT EXISTS eth.eth_transactions (
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
  input LONGTEXT NULL,
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
  PRIMARY KEY (date, hash, block_timestamp)
);
```

## Notes & Troubleshooting

- S3 region is `us-east-2` and public bucket path is `aws-public-blockchain/v1.0/eth/transactions/date=YYYY-MM-DD`.
- If direct S3 reads stall with very large batch sizes, reduce `--batch-size` (e.g., 1000–2000).
- Bulk insert is faster; on error it falls back to `to_sql` automatically.
- For TiDB, prefer `mysql+mysqlconnector` URI used in the script; ensure port (`4000`) is reachable.


