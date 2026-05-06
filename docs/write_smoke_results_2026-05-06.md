# 10GB Write Smoke Results - 2026-05-06

This document records the first TiCI and Elasticsearch write-path smoke
results for the Ethereum transactions 10GB manifest. It is a smoke and tuning
log, not a final 1TB benchmark result.

## Dataset

```text
dataset: AWS public blockchain Ethereum transactions
manifest: eth_transactions_10gb
manifest entries: 17
local parquet bytes: 10,086,645,566
logical rows: 27,045,923
logical bytes: 39,367,669,486
client dry-run: 129.07s, 209,539.6 rows/s, 305.0 MB/s, errors=0
```

The client dry-run throughput was much higher than database ingest throughput,
so the Go parquet reader and row normalization path was not the bottleneck for
these TiCI runs.

## TiCI Testbed

AWS region: `us-west-2`

Component sizing:

```text
center:      c5.xlarge
pd:          c5.xlarge
tidb:        c5.xlarge
tikv:        r5.xlarge
tiflash:     r5.xlarge
tici-meta:   c5.xlarge
tici-worker: r5.xlarge
ticdc:       c5.xlarge
```

TiDB/TiCI version:

```text
TiDB version: 8.0.11-TiDB-v9.0.0-feature.fts
TiUP cluster version: v9.0.0-feature.fts
TiCDC CLI: v8.5.4-nextgen.202510.5-nightly
```

Index alignment:

```text
TiCI hybrid inverted columns:
date, hash, from_address, to_address, receipt_contract_address,
block_timestamp, block_number, transaction_index, gas, gas_price,
receipt_status, transaction_type, random_flag

TiCI sort columns:
block_timestamp desc, gas_price desc

Elasticsearch target mapping:
schema/es_eth_transactions_mapping.json
```

## Elasticsearch Testbed

Elasticsearch was tested after the TiCI cluster was stopped, to avoid running
both systems at the same time during the cost-gated smoke phase.

Deployment:

```text
node: center
observed CPU/RAM: 4 vCPU, 7.5 GiB RAM
Elasticsearch: 9.3.4 tarball, bundled JDK
Lucene: 10.3.2
cluster.name: eth-es-smoke
node.name: es-1
network.host: 127.0.0.1
http.port: 9200
security: disabled
heap: -Xms4g -Xmx4g
path.data: /home/ubuntu/elasticsearch-smoke/data
path.logs: /home/ubuntu/elasticsearch-smoke/logs
vm.max_map_count: 262144
```

Index settings:

```text
index: eth_transactions
number_of_shards: 1
number_of_replicas: 0
refresh_interval: -1
index sort: block_timestamp desc, gas_price desc
dynamic mapping: strict
```

The mapping intentionally avoids analyzed text fields and tokenizers. It uses
`keyword`, `long`, `double`, and `boolean` fields, and disables `index` and/or
`doc_values` for fields that are not part of the TiCI-aligned write smoke
indexing surface.

## TiCI Insert Results

Each clean run recreated `test.eth_transactions`, created a fresh TiCDC
changefeed before insert, and measured TiDB-side `INSERT` time separately from
TiCI catch-up.

| Run | Writer Settings | Rows | Elapsed | Rows/s | Errors | Result |
| --- | --- | ---: | ---: | ---: | ---: | --- |
| `20260506T091242Z_r5_w1_b500` | `workers=1`, `batch_rows=500` | 27,045,923 | 2,813.91s | 9,611.5 | 0 | clean |
| `20260506T114039Z_r5_w2_b500` | `workers=2`, `batch_rows=500` | 27,045,923 | 1,841.52s | 14,686.7 | 0 | clean |
| `20260506T121500Z_r5_w4_b500` | `workers=4`, `batch_rows=500` | about 19,104,500 | stopped at 38,209 batches | partial | 0 before stop | invalid, TiKV disk full |

## TiCI Catch-Up

`workers=1`:

```text
last INSERT batch: 2026-05-06 10:00:48 UTC
last observed TiCI submit_frag_success: about 2026-05-06 10:00:58 UTC
catch-up after insert: about 10s
final shard progress: 8 shards at CDC00000000000000002696.json
tici-worker restarts: 0
earlyoom kills: 0
```

`workers=2`:

```text
final row count: 27,045,923
final shard progress: 8 shards at CDC00000000000000001847.json
tici-worker peak RSS: 12,381,720 KB
tici-worker minimum available memory: 20,801 MB
tici-worker restarts: 0
earlyoom kills: 0
```

`workers=4`:

```text
final recorded writer batch: 38,209
final recorded rows before stop: about 19,104,500
tici-worker peak RSS: 15,126,904 KB
tici-worker minimum available memory: 23,396 MB
ticdc peak RSS: 5,976,804 KB
tici-worker restarts: 0
tikv restarts: 1
ticdc restarts: 1
```

The `workers=4` run is not a clean ingest result. TiKV reported:

```text
2026-05-06 12:36:49 UTC
IO error: No space left on device:
While appending to file: /tidb-deploy/tikv-20160/data/db/018933.sst
```

TiKV then exited and systemd restarted `tikv-20160` at
`2026-05-06 12:37:06 UTC`. During the same pressure window, writer batch
latency had spikes up to about 2.2s, and TiKV logs showed prewrite scheduler
flow-control delays around 1.1s to 1.55s.

## Elasticsearch Write Results

The official Elasticsearch result uses the same 10GB manifest and a clean ES
data path. The earlier `workers=4` run is retained only as a pressure sample.

| Run | Writer Settings | Rows | Elapsed | Rows/s | Errors | Result |
| --- | --- | ---: | ---: | ---: | ---: | --- |
| `es_sanity_20260506T125740Z` | `workers=1`, `reader_workers=2`, `batch_rows=1000` | 1,704,159 | 145.85s | 11,684.1 | 0 | sanity only |
| `20260506T130117_es_w4_b5000` | `workers=4`, `reader_workers=4`, `batch_rows=5000` | about 9,865,000 | stopped at 1,973 batches | partial | 0 before stop | invalid, segment writer backpressure |
| `20260506T132912_es_w1_b5000` | `workers=1`, `reader_workers=2`, `batch_rows=5000` | 27,045,923 | 3,834.50s | 7,053.3 | 0 | clean |

Clean `workers=1` validation after refresh:

```text
_count: 27,045,923
store.size: 16,585,412,386 bytes
docs.deleted: 4,168
index_total: 27,060,923
write thread-pool rejected: 0
```

`index_total` includes indexing attempts, so it is higher than `_count` by
15,000 documents because the writer performed three successful retry attempts
of 5,000-row bulk requests. The final row count still matched the manifest.

Clean `workers=1` bulk latency:

```text
bulk requests: 5,415
avg batch latency: 669.027 ms
p50 batch latency: 261.583 ms
p95 batch latency: 496.313 ms
p99 batch latency: 1,397.037 ms
max batch latency: 121,296.036 ms
retry attempts: 3
max retry count on one bulk request: 1
```

## Bottleneck Interpretation

The write path did not scale linearly with client worker count:

```text
workers=1:  9,611.5 rows/s
workers=2: 14,686.7 rows/s  (1.53x over workers=1)
workers=4: invalid; hit TiKV disk capacity before completion
```

Interpretation:

1. `workers=1` was partly limited by a single in-flight batch and TiDB/TiKV
   commit latency.
2. `workers=2` improved throughput but showed that TiDB/TiKV write commit path
   was already the dominant limiter.
3. `workers=4` pushed the small TiKV node into storage pressure and eventually
   disk-full failure. This run should be used only as evidence that the current
   100GB TiKV root volume is too small for repeated 10GB smoke runs with this
   schema and hybrid index.
4. TiCI worker memory requires an r5.xlarge-class node for this smoke. The
   earlier c5.xlarge worker failed because earlyoom killed `tici-server` when
   its RSS reached about 7GB.

For Elasticsearch, `workers=4` was too aggressive for the first clean smoke on
the single small node. Even `workers=1` eventually hit Lucene segment write
backpressure:

```text
IndexingMemoryController events: 21 throttle cycles
representative log: segment writing can't keep up
indexing.throttle_time: 1,109,564 ms
merge.total_time: 1,139,302 ms
merge.total_throttled_time: 595,445 ms
flush.total_time: 3,324,179 ms
write queue: 0
write rejected: 0
```

Interpretation: the clean ES `workers=1` run was not blocked by the Go client
or write-thread rejection. The dominant limiter was single-node Lucene segment
flush/merge/indexing backpressure on the center node's storage and memory
budget. This means higher client concurrency should be tested only after this
baseline, with a clean index reset and close tracking of throttle time, merge
time, disk usage, and bulk latency spikes.

The TiCI and Elasticsearch 10GB clean results are therefore not a symmetric
cluster-size comparison yet. TiCI used a multi-node topology with TiKV, TiDB,
TiCDC, TiFlash, TiCI meta, and a TiCI worker. Elasticsearch used a single
self-managed node to keep the smoke cost minimal.

## Storage Artifacts

S3 artifacts retained after these runs:

| Table ID | Run | Prefix | Objects | Size |
| --- | --- | --- | ---: | ---: |
| 168 | w1 clean | `frags/t_168` | 4,026 | 89,262,601,428 bytes |
| 172 | w2 clean | `frags/t_172` | 2,814 | 59,352,749,653 bytes |
| 172 | w2 clean | `cdc/172` | 1,788 | 74,862,915,515 bytes |
| 176 | w4 partial | `frags/t_176` | 814 | 26,435,254,866 bytes |
| 176 | w4 partial | `cdc/176` | 14 | 714,729,552 bytes |

## Next Steps

1. Keep Elasticsearch and TiCI clusters separate to preserve the cost gate.
2. Do not rerun Elasticsearch `workers=4` on the current single small node as a
   clean benchmark. Run `workers=2` first if a second ES point is needed.
3. Do not rerun TiCI `workers=4` on the current TiKV volume. Increase TiKV disk to
   at least 300GB, or clean TiKV/S3 artifacts between runs before collecting
   another 4-worker result.
4. Avoid `COUNT(*)` during timed writes. The TiCI w4 investigation confirmed that
   count scans from the previous table can add slow TiKV scan work and pollute
   the write environment.
5. For a fairer ES-vs-TiCI write comparison, choose the next ES topology before
   scaling data: either keep a minimal single-node cost baseline, or deploy a
   small multi-node ES cluster with explicit data-node sizing and EBS volume
   throughput.
