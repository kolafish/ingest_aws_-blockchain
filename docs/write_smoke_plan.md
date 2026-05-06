# 10GB Write Smoke Plan

This smoke plan validates the write path and metric format before any large
cluster is started. It is not a formal performance result.

## Cost Gates

1. Do not start TiCI or Elasticsearch before a 10GB local manifest and client
   dry-run pass.
2. Do not run TiCI and Elasticsearch smoke clusters at the same time.
3. Use the smallest non-HA topology for smoke. Do not provision 100GB or 1TB
   capacity before smoke is clean.
4. Stop the smoke cluster immediately after result export. Destroy it if the
   next run is not expected within 24 hours.
5. Data download, parquet conversion, schema creation, and index creation are
   not included in ingest timing.

## Smoke Scope

Dataset:

```text
AWS public blockchain Ethereum transactions
Target local parquet bytes: 10GB
```

Timed phases:

```text
client_dry_run: parquet decode + row normalization + optional JSON encode
tidb_insert: first INSERT batch sent -> last INSERT batch ack
tici_catch_up: last INSERT batch ack -> TiCI CDC/index catch-up
es_bulk: first bulk request sent -> last bulk ack
es_searchable: last bulk ack -> refresh/pending tasks/store stable
```

Only `tidb_insert`, `tici_catch_up`, `es_bulk`, and `es_searchable` are used for
database-side comparison. `client_dry_run` is a gate that checks whether the
driver can feed the database fast enough.

## Index Alignment

TiCI uses only the hybrid inverted columns and hybrid sort columns in
`schema/tidb_eth_transactions.sql`.

Elasticsearch uses only the corresponding indexed fields in
`schema/es_eth_transactions_mapping.json`.

Do not add these for smoke:

```text
text fields
analyzers
copy_to
runtime fields
extra multi-fields
extra indexed fields
extra doc_values fields
ingest pipelines
```

`input`, `value`, `nonce`, `block_hash`, and fee-related columns are stored in
the document/table but are not indexed in either system for the smoke run.

## Prepare 10GB Local Data

Download or reuse local parquet files before starting any cluster:

```bash
python eth_import_transactions.py \
  --start-date 2025-10-25 \
  --days 3 \
  --source download \
  --local-dir local_data_multi \
  --download-workers 8
```

Build the 10GB manifest:

```bash
go run ./cmd/manifest-builder \
  --input-dir local_data_multi \
  --target-size 10GB \
  --output bench/manifests/eth_transactions_10gb.json
```

Run the dry-run gate:

```bash
go run ./cmd/client-dry-run \
  --manifest bench/manifests/eth_transactions_10gb.json \
  --reader-workers 4 \
  --encode es \
  --result-jsonl results/smoke_client_dry_run.jsonl
```

If the dry-run cannot sustain enough throughput, fix the driver path first:
increase driver instance size, local disk throughput, reader workers, or use
preconverted local parquet. Do not scale the database cluster to hide a client
bottleneck.

## TiCI Smoke

Use nightly mirror:

```bash
tiup mirror set http://tiup.pingcap.net:8988
tiup update --self
tiup update --all
```

Smoke topology should be the minimum non-HA layout:

```text
driver-1: Go writer and metrics collection
tidb-1: PD + TiDB + TiCDC
tikv-1: TiKV
tici-1: TiFlash/TiCI reader + TiCI meta + TiCI worker
S3 bucket: ticidefaultbucket
```

If TiUP cluster deployment supports the nightly component version directly,
deploy with the smoke topology. Otherwise deploy the smallest cluster baseline,
then patch TiDB/TiFlash/TiCI services to the nightly artifacts from the mirror.
Record all component versions and git SHAs.

Create schema and hybrid index before timing:

```bash
mysql -h <tidb-host> -P 4000 -u <user> -p < schema/tidb_eth_transactions.sql
```

Run timed insert:

```bash
export TIDB_DSN='<user>:<password>@tcp(<tidb-host>:4000)/test?charset=utf8mb4&parseTime=true'

go run ./cmd/tidb-writer \
  --manifest bench/manifests/eth_transactions_10gb.json \
  --workers 8 \
  --reader-workers 4 \
  --batch-rows 5000 \
  --result-jsonl results/smoke_tidb_insert.jsonl
```

After the last insert batch, measure TiCI catch-up separately:

```bash
tiup cdc cli capture list --server http://<ticdc-host>:8300
tiup cdc cli changefeed list --server http://<ticdc-host>:8300
grep 'unread_files_count' /home/ubuntu/tici_worker.log | tail -n 20
```

For table-level progress:

```sql
SELECT TIDB_TABLE_ID FROM information_schema.tables
WHERE table_schema = 'test' AND table_name = 'eth_transactions';

SELECT shard_id, shard_writer, progress
FROM tici.tici_shard_meta
WHERE table_id = <tidb_table_id>
ORDER BY shard_id;
```

Export these before stopping the cluster:

```text
smoke_tidb_insert.jsonl
TiDB/TiKV/TiCDC/TiCI CPU, memory, disk, network
TiCDC checkpoint/changefeed status
TiCI worker unread_files_count history
TiCI shard progress
S3 bytes written/read for ticidefaultbucket
final disk usage
component versions
```

Stop or destroy the TiCI smoke cluster before starting Elasticsearch smoke.

## Elasticsearch Smoke

Use a single-node self-managed Elasticsearch cluster for smoke:

```text
es-1: master + data + ingest
driver-1: Go writer and metrics collection
```

Create the index before timing:

```bash
curl -X PUT "$ES_URL/eth_transactions" \
  -H 'Content-Type: application/json' \
  --data-binary @schema/es_eth_transactions_mapping.json
```

Run timed bulk ingest:

```bash
go run ./cmd/es-writer \
  --manifest bench/manifests/eth_transactions_10gb.json \
  --url "$ES_URL" \
  --index eth_transactions \
  --workers 8 \
  --reader-workers 4 \
  --batch-rows 5000 \
  --batch-bytes 10000000 \
  --result-jsonl results/smoke_es_bulk.jsonl
```

After the last bulk ack, measure searchable/store-stable time separately:

```bash
curl -X POST "$ES_URL/eth_transactions/_refresh"
curl "$ES_URL/_cluster/pending_tasks"
curl "$ES_URL/eth_transactions/_stats/store,indexing,merge,refresh,segments"
curl "$ES_URL/_cat/indices/eth_transactions?v&bytes=b"
```

Export these before stopping the node:

```text
smoke_es_bulk.jsonl
node CPU, memory, disk, network
index store size
segment count
merge time and throttle time
refresh time
Elasticsearch version and node settings
```

## Promotion Criteria

Only move to 100GB tune after all are true:

```text
client dry-run completes without decode/encode errors
TiDB insert completes without failed batches
TiCI catches up and shard progress is complete
ES bulk completes without failed items
ES index is refreshable and store stats are available
row counts match between source manifest, TiDB, and ES
final index definitions are confirmed aligned
all result JSONL and resource metrics are exported
clusters have been stopped or destroyed
```
