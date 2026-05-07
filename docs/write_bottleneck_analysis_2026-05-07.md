# Write Bottleneck Analysis - 2026-05-07

This document separates evidence from inference for the current TiCI/TiDB and
Elasticsearch write-path smoke results.

## Evidence Available

Elasticsearch has raw local artifacts:

```text
results/es-prod10g-20260507/es_prod10g_20260507T003415_es_prod10g_w1_b5000.jsonl
results/es-prod10g-20260507/es_prod10g_20260507T010726_es_prod10g_w2_b5000.jsonl
results/es-prod10g-20260507/resource_20260507T003415_es_prod10g_w1_b5000.log
results/es-prod10g-20260507/resource_20260507T010726_es_prod10g_w2_b5000.log
```

TiCI/TiDB has summary evidence captured in:

```text
docs/write_smoke_results_2026-05-06.md
docs/write_benchmark_comparison_2026-05-07.md
```

The full TiDB/TiKV/TiCDC/TiCI worker raw logs for the 2026-05-06 AWS smoke were
not found in the local benchmark repo or `terraform-tici` workspace after the
run. The TiCI conclusions below are therefore intentionally narrower.

## Elasticsearch

Conclusion: the ES prod10g bottleneck was internal segment flush/merge/indexing
backpressure on the baseline gp3 data volumes, not the Go client, not write
thread-pool rejection, and not CPU saturation.

Evidence:

```text
w1 writer: 27,489,769 rows, 1,901.22s, 14,459.0 rows/s, 0 retries
w2 writer: 27,489,769 rows, 2,056.88s, 13,364.8 rows/s, 6 retries
client dry-run: 104,332.3 rows/s, 149.5 MB/s, errors=0
```

The client dry-run was about 7x faster than ES ingest, so parquet reading and
ES JSON encoding were not the bottleneck.

Bulk long-tail evidence:

| Run | Top long-tail batch | Retry count |
| --- | ---: | ---: |
| w1 | 111,207.747 ms | 0 |
| w2 | 333,731.927 ms | 2 |
| w2 | 246,047.200 ms | 2 |
| w2 | 190,488.878 ms | 1 |
| w2 | 137,823.034 ms | 1 |

Thread-pool evidence:

```text
write rejected: 0 throughout w1 and w2
write queue: usually 0; transient small queue only
```

That rules out write-thread rejection as the primary limiter.

Resource-log evidence from w2:

| Window UTC | Node-level index rate | Index throttle delta | Merge delta | Merge throttled delta | Refresh delta | CPU at end |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| 01:19:20 -> 01:20:20 | 15,641 rows/s | 29.218s | 380.593s | 239.944s | 237.295s | 22.3% |
| 01:20:20 -> 01:25:02 | 4,289 rows/s | 90.656s | 691.804s | 451.948s | 988.114s | 11.3% |
| 01:35:25 -> 01:39:16 | 14,362 rows/s | 28.806s | 538.177s | 381.441s | 461.746s | 1.0% |

During the worst stalls, indexed-row progress dropped sharply while merge,
merge-throttled, refresh, and indexing-throttle counters increased. CPU was low
or moderate at the end of those windows, so the observed wait was not CPU-bound.

Final index-level stats also support the same conclusion:

```text
w1 total indexing.throttle_time: 13,843 ms
w1 total merge.total_throttled_time: 728,967 ms
w1 total flush.total_time: 4,135,713 ms

w2 total indexing.throttle_time: 528,180 ms
w2 total merge.total_throttled_time: 1,505,607 ms
w2 total flush.total_time: 5,687,767 ms
```

w2 added more client concurrency but became slower. The counters above show the
extra concurrency amplified ES background write pressure and long-tail stalls.

## TiCI/TiDB

Conclusion: for the clean TiCI runs, the saved evidence points to the TiDB/TiKV
front-door write path as the limiter, not TiCI worker catch-up. For the invalid
`workers=4` pressure run, the bottleneck is explicitly TiKV storage capacity and
storage pressure.

Clean-run evidence:

```text
w1 insert: 27,045,923 rows, 2,813.91s, 9,611.5 rows/s
w2 insert: 27,045,923 rows, 1,841.52s, 14,686.7 rows/s
w1 last INSERT batch: 2026-05-06 10:00:48 UTC
w1 last observed TiCI submit_frag_success: about 2026-05-06 10:00:58 UTC
w1 catch-up after insert: about 10s
w2 final row count: 27,045,923
w2 final shard progress: 8 shards complete
tici-worker restarts: 0
earlyoom kills: 0
w2 tici-worker peak RSS: 12,381,720 KB
```

The w1 catch-up gap was only about 10 seconds after a 2,813.91-second insert
run. That makes TiCI CDC/index catch-up too small to explain overall write
throughput. w2 did not preserve an exact catch-up timestamp, but final shard
progress was complete and the worker did not restart or hit memory pressure.

Concurrency evidence:

```text
w1 -> w2 throughput: 9,611.5 rows/s -> 14,686.7 rows/s
scale factor: 1.53x, not 2x
```

This means adding client concurrency helped, but the write path was already
encountering server-side commit/storage limits before linear scaling.

Invalid `workers=4` evidence:

```text
TiKV log error:
IO error: No space left on device:
While appending to file: /tidb-deploy/tikv-20160/data/db/018933.sst

systemd restart:
tikv-20160 restarted at 2026-05-06 12:37:06 UTC

writer behavior:
batch latency spikes up to about 2.2s

TiKV logs:
prewrite scheduler flow-control delays around 1.1s to 1.55s

tici-worker:
peak RSS about 15,126,904 KB
restarts: 0
```

That evidence is strong for the pressure run: TiKV disk/storage pressure was the
first hard bottleneck. TiCI worker was not the component that failed.

## What Cannot Be Proven From Saved Artifacts

For TiCI clean w1/w2, the available saved data is not enough to split the
TiDB/TiKV bottleneck further into:

```text
TiDB SQL layer CPU
TiKV raftstore/apply/write stalls
TiKV RocksDB write stall/compaction
EBS throughput/IOPS saturation
TiCDC checkpoint lag over time
TiCI worker unread_files_count over time
```

Those require raw TiDB/TiKV/TiCDC/TiCI logs and time-series metrics from the
clean runs. The next TiCI run should preserve:

```text
tidb-writer JSONL
TiDB slow log around the run
TiKV log and RocksDB stall/compaction metrics
TiCDC checkpoint/resolved-ts history
TiCI worker unread_files_count and submit_frag_success timestamps
node CPU, memory, disk IO, disk fullness, and network every 10s
```

## Current Bottom Line

| System | Current bottleneck supported by evidence | Confidence |
| --- | --- | --- |
| ES prod10g w1/w2 | ES internal flush/merge/indexing throttle on baseline gp3 storage | High |
| TiCI clean w1/w2 | TiDB/TiKV write/commit path, not TiCI worker catch-up | Medium |
| TiCI w4 invalid | TiKV storage capacity/storage pressure | High |

