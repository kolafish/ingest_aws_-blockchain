# 10GB Write Smoke Results - 2026-05-06

This document records the first TiCI write-path smoke results for the
Ethereum transactions 10GB manifest. It is a smoke and tuning log, not a final
1TB benchmark result.

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

1. Run Elasticsearch 10GB write smoke on a single self-managed node, using
   `schema/es_eth_transactions_mapping.json`.
2. Keep Elasticsearch and TiCI clusters separate to preserve the cost gate.
3. Do not rerun `workers=4` on the current TiKV volume. Increase TiKV disk to
   at least 300GB, or clean TiKV/S3 artifacts between runs before collecting
   another 4-worker result.
4. Avoid `COUNT(*)` during timed writes. The w4 investigation confirmed that
   count scans from the previous table can add slow TiKV scan work and pollute
   the write environment.
