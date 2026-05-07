# TiCI vs Elasticsearch Write Comparison - 2026-05-07

This is the cross-system write-path comparison table for the current 10GB
Ethereum transactions smoke runs. It is intended for cost and sizing decisions,
not as a final 1TB result.

## Pricing Basis

Region: `us-west-2`

Prices were checked on 2026-05-07 with the AWS Price List API for EC2 and the
AWS EBS pricing pages for gp3/gp2:

- EC2 On-Demand Linux, shared tenancy, no preinstalled software:
  https://aws.amazon.com/ec2/pricing/
- gp3 baseline and extra-performance pricing:
  https://aws.amazon.com/ebs/general-purpose/
- EBS billing model:
  https://aws.amazon.com/ebs/pricing/

```text
c5.xlarge: $0.170/hour
r5.xlarge: $0.252/hour
gp3 storage: $0.080/GB-month
gp3 baseline included: 3000 IOPS, 125 MiB/s
gp3 extra IOPS: $0.005/provisioned IOPS-month over 3000
gp3 extra throughput: $0.040/provisioned MiB/s-month over 125
gp2 storage: $0.100/GB-month
```

Cost model used below:

```text
1 month = 720 hours
cluster_hourly = EC2 hourly + provisioned EBS hourly
not included: S3, network transfer, snapshots, NAT, support, discounts,
              Savings Plans, Spot, or engineering time
```

## Cluster Cost

| System | Nodes | EC2 hourly | EBS hourly | Running hourly | Provisioned storage |
| --- | --- | ---: | ---: | ---: | ---: |
| TiCI/TiDB | 5 x `c5.xlarge`, 3 x `r5.xlarge` | $1.606 | $0.127 | $1.733 | 708GB EBS |
| ES prod10g | 4 x `c5.xlarge` | $0.680 | $0.044 | $0.724 | 400GB EBS |

TiCI/TiDB EBS detail:

| Component | Instance | Volume | IOPS | Throughput |
| --- | --- | ---: | ---: | ---: |
| center | `c5.xlarge` | 100GB gp3 | 3000 | 125 MiB/s |
| pd | `c5.xlarge` | 100GB gp3 | 3000 | 125 MiB/s |
| tidb | `c5.xlarge` | 100GB gp3 | 3000 | 125 MiB/s |
| tikv | `r5.xlarge` | 100GB gp3 | 4000 | 288 MiB/s |
| tiflash | `r5.xlarge` | 100GB gp3 | 4000 | 288 MiB/s |
| tici-worker | `r5.xlarge` | 100GB gp3 | 3000 | 125 MiB/s |
| ticdc | `c5.xlarge` | 100GB gp3 | 4000 | 288 MiB/s |
| tici-meta | `c5.xlarge` | 8GB gp2 | 100 | n/a |

ES prod10g EBS detail:

| Component | Instance | Volume | IOPS | Throughput |
| --- | --- | ---: | ---: | ---: |
| es-1 | `c5.xlarge` | 100GB gp3 | 3000 | 125 MiB/s |
| es-2 | `c5.xlarge` | 100GB gp3 | 3000 | 125 MiB/s |
| es-3 | `c5.xlarge` | 100GB gp3 | 3000 | 125 MiB/s |
| driver | `c5.xlarge` | 100GB gp3 | 3000 | 125 MiB/s |

## Dataset Caveat

The TiCI and ES runs were both about 10GB, but they used slightly different
date slices:

| System | Manifest entries | Local parquet bytes | Rows | Logical bytes |
| --- | ---: | ---: | ---: | ---: |
| TiCI/TiDB smoke | 17 | 10,086,645,566 | 27,045,923 | 39,367,669,486 |
| ES prod10g | 18 | 10,013,628,561 | 27,489,769 | 39,399,039,010 |

Use the rows/s and cost ratios as directionally useful 10GB sizing data. For a
strict final comparison, rerun both systems on the same manifest.

## Write Throughput And Cost Efficiency

`rows/s per $/hour` is the observed write throughput divided by the running
cluster hourly cost. Higher is better. `$ per 1M rows` is the runtime cluster
cost for that run divided by rows written. Lower is better.

| System | Run | Writer workers | Batch rows | Rows | Write elapsed | Rows/s | MB/s | Running hourly | Rows/s per $/h | Runtime cost | $ per 1M rows |
| --- | --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| TiCI/TiDB | `20260506T091242Z_r5_w1_b500` | 1 | 500 | 27,045,923 | 2,813.91s | 9,611.5 | 13.99 | $1.733/h | 5,547 | $1.3545 | $0.0501 |
| TiCI/TiDB | `20260506T114039Z_r5_w2_b500` | 2 | 500 | 27,045,923 | 1,841.52s | 14,686.7 | 21.38 | $1.733/h | 8,475 | $0.8864 | $0.0328 |
| ES prod10g | `20260507T003415_es_prod10g_w1_b5000` | 1 | 5000 | 27,489,769 | 1,901.22s | 14,459.0 | 20.72 | $0.724/h | 19,959 | $0.3826 | $0.0139 |
| ES prod10g | `20260507T010726_es_prod10g_w2_b5000` | 2 | 5000 | 27,489,769 | 2,056.88s | 13,364.8 | 19.15 | $0.724/h | 18,448 | $0.4139 | $0.0151 |

Current best clean points:

| System | Best clean run | Rows/s | Running hourly | Rows/s per $/h | $ per 1M rows |
| --- | --- | ---: | ---: | ---: | ---: |
| TiCI/TiDB | TiCI w2 | 14,686.7 | $1.733/h | 8,475 | $0.0328 |
| ES prod10g | ES w1 | 14,459.0 | $0.724/h | 19,959 | $0.0139 |

Interpretation: at this 10GB scale and current topology, ES prod10g w1 had
similar write throughput to TiCI w2, but used a smaller and cheaper cluster.
That gives ES better write-throughput-per-dollar in this smoke. It is not yet a
1TB conclusion because the TiCI and ES storage and replication models are not
identical, and the runs used slightly different manifests.

## Index Sync And Searchability

| System | Run | Write ack scope | Index sync/searchable timing | Validation |
| --- | --- | --- | --- | --- |
| TiCI/TiDB | w1 | TiDB `INSERT` ack, TiCI indexes through TiCDC | Last insert at 2026-05-06 10:00:48 UTC; last observed TiCI `submit_frag_success` about 2026-05-06 10:00:58 UTC; catch-up about 10s; end-to-end about 2,824s | final shard progress complete, 8 shards |
| TiCI/TiDB | w2 | TiDB `INSERT` ack, TiCI indexes through TiCDC | Insert elapsed 1,841.52s; separate catch-up timestamp was not captured; final shard progress was complete | final row count 27,045,923; 8 shards |
| ES prod10g | w1 | Bulk ack includes primary indexing and one replica write | Bulk elapsed 1,901.22s; no async CDC catch-up; refresh/count verified after run | `_count` 27,489,769; health green |
| ES prod10g | w2 | Bulk ack includes primary indexing and one replica write | Bulk elapsed 2,056.88s; no async CDC catch-up; refresh/count verified after run | `_count` 27,489,769; health green |

For future runs, record TiCI catch-up with exact timestamps for every worker
setting, and record ES last bulk ack to post-refresh timing separately. The
current data is enough for write-throughput comparison, but not enough for a
fully precise searchable-latency comparison.

## Storage

| System | Run | Serving/index storage observed | Retained staging/storage observed | Notes |
| --- | --- | ---: | ---: | --- |
| TiCI/TiDB | w1 | `frags/t_168`: 89,262,601,428 bytes | not separately recorded | S3 fragment storage only recorded for this run |
| TiCI/TiDB | w2 | `frags/t_172`: 59,352,749,653 bytes | `cdc/172`: 74,862,915,515 bytes; combined retained: 134,215,665,168 bytes | CDC files are retained staging/changefeed artifacts |
| ES prod10g | w1 | total store: 36,990,943,496 bytes; primary store: 18,610,717,581 bytes | included in ES store | 3 primary shards, 1 replica |
| ES prod10g | w2 | total store: 37,383,592,759 bytes; primary store: 19,081,655,132 bytes | included in ES store | 3 primary shards, 1 replica; 16,572 deleted docs after retries |

Storage interpretation:

```text
ES physical store with one replica: about 37GB for 10GB parquet input.
ES primary-only store: about 18.6GB to 19.1GB.
TiCI S3 fragment size varied by run: 59GB to 89GB.
TiCI retained CDC plus fragments for w2: about 134GB.
```

For cost planning, separate serving storage from retained staging artifacts. ES
does not keep a separate CDC object log in this setup. TiCI currently keeps S3
fragment and CDC artifacts unless they are explicitly cleaned.

## Bottlenecks

TiCI/TiDB:

```text
w1 -> w2 scaled from 9,611.5 rows/s to 14,686.7 rows/s.
w4 was invalid because TiKV ran out of disk during the run.
The write limiter was TiDB/TiKV commit and storage pressure before TiCI worker
became the bottleneck on r5.xlarge.
```

ES prod10g:

```text
w1 was the best clean point: 14,459.0 rows/s, 0 retries.
w2 was slower despite more client concurrency: 13,364.8 rows/s, 6 retries.
The limiter was ES flush/merge/indexing throttle on 100GB gp3 baseline disks,
not Go client throughput and not write-thread rejection.
```

## Next Measurement Changes

For the next comparable run:

1. Use the same manifest for TiCI and ES.
2. Record TiCI `insert_elapsed`, `catch_up_after_insert`, and `end_to_end` for
   every worker setting.
3. Record ES `bulk_elapsed`, `refresh_elapsed`, store-stable time, retry count,
   and max bulk latency for every worker setting.
4. Add S3 cost for TiCI retained `frags/` and `cdc/` prefixes if those objects
   will remain after the run.
5. For 100GB or 1TB, do not extrapolate only from average rows/s. ES w2 already
   showed severe long-tail stalls from flush/merge throttle on baseline gp3.

