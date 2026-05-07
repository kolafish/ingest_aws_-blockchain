# Elasticsearch Production-Style 10GB Results - 2026-05-07

This records the first multi-node Elasticsearch write-path rerun for the
Ethereum transactions 10GB dataset. It replaces the earlier single-node ES smoke
as the planning baseline for larger ES write tests.

## Topology

AWS region: `us-west-2`

```text
es-1..3:
  count: 3
  instance type: c5.xlarge
  vCPU/RAM: 4 vCPU, 8 GiB RAM each
  Elasticsearch: 9.3.4 tarball, bundled JDK
  heap: -Xms4g -Xmx4g
  roles: master, data_hot, ingest
  root volume: 100GB gp3, 3000 IOPS, 125 MiB/s

driver:
  count: 1
  instance type: c5.xlarge
  role: Go writer, local parquet staging, metrics
  root volume: 100GB gp3, 3000 IOPS, 125 MiB/s
```

The cluster was single-AZ to keep cost low, but used three master-eligible data
nodes and one replica to exercise production-relevant shard distribution and
replica-write cost.

Instance launch time for this run:

```text
2026-05-07T00:19:11Z
```

The TiCI/TiDB testbed was stopped before creating this ES stack. The ES stack
used independent Terraform state under `infra/es-single-az-10gb`.

## Dataset

```text
source: AWS public blockchain Ethereum transactions parquet
dates: 2025-10-25 through 2025-11-11
manifest: /home/ubuntu/bench/manifests/eth_transactions_prod10g.json
manifest entries: 18
local parquet bytes: 10,013,628,561
logical rows: 27,489,769
logical bytes: 39,399,039,010
```

Client dry-run on the driver:

```text
reader_workers=4
encode=es
elapsed: 263.48s
rows/s: 104,332.3
MB/s: 149.5
errors: 0
```

The client reader/normalization path was not the bottleneck for either ES run.

## Index Configuration

Mapping:

```text
schema/es_eth_transactions_10gb_replicated_mapping.json
```

Key settings:

```text
number_of_shards: 3
number_of_replicas: 1
refresh_interval: -1 during timed ingest
routing allocation tier: data_hot
index sort: block_timestamp desc, gas_price desc
dynamic mapping: strict
no analyzed text fields or tokenizers
```

The indexed field surface matches the TiCI inverted-index smoke: keyword,
numeric, date-like long, and boolean fields only. It does not add FTS/analyzed
text fields on the ES side.

## Write Results

Both runs used all three ES HTTP endpoints:

```text
http://172.31.21.1:9200,http://172.31.21.2:9200,http://172.31.21.3:9200
```

| Run | Writer settings | Rows | Elapsed | Rows/s | MB/s | Errors | Retries | Result |
| --- | --- | ---: | ---: | ---: | ---: | ---: | ---: | --- |
| `20260507T003415_es_prod10g_w1_b5000` | `workers=1`, `reader_workers=4`, `batch_rows=5000` | 27,489,769 | 1,901.22s | 14,459.0 | 20.7 | 0 | 0 | clean |
| `20260507T010726_es_prod10g_w2_b5000` | `workers=2`, `reader_workers=4`, `batch_rows=5000` | 27,489,769 | 2,056.88s | 13,364.8 | 19.2 | 0 | 6 | clean, but long-tail stalls |

Bulk latency:

| Run | Bulk requests | Avg | P50 | P95 | P99 | Max |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| w1 | 5,503 | 306.029 ms | 232.145 ms | 440.968 ms | 709.836 ms | 111,207.747 ms |
| w2 | 5,503 | 705.313 ms | 268.534 ms | 530.690 ms | 1,882.155 ms | 333,731.927 ms |

The w2 retry events were concentrated in long-tail stalls:

```text
batch 2924: 333,731.927 ms, retry_count=2
batch 3110: 246,047.200 ms, retry_count=2
batch 3624: 137,823.034 ms, retry_count=1
batch 4856: 190,488.878 ms, retry_count=1
```

Final w2 validation after refresh:

```text
_count: 27,489,769
health: green
docs.deleted: 16,572
total store: 37,383,592,759 bytes
primary store: 19,081,655,132 bytes
write rejected: 0
```

`docs.deleted` is expected for this run because several bulk requests timed out
client-side and were retried after Elasticsearch had already applied at least
part of the request. The final `_count` still matched the manifest.

## Elasticsearch Internal Stats

Final w1 stats:

```text
primary store: 18,610,717,581 bytes
total store: 36,990,943,496 bytes
primaries indexing.index_total: 27,489,769
primaries indexing.index_time: 835,765 ms
primaries indexing.throttle_time: 6,923 ms
total indexing.index_total: 54,979,538
total indexing.index_time: 1,466,978 ms
total indexing.throttle_time: 13,843 ms
total merge.total_time: 1,597,086 ms
total merge.total_throttled_time: 728,967 ms
total refresh.total_time: 1,231,915 ms
total flush.total_time: 4,135,713 ms
segments: 186
```

Final w2 stats:

```text
primary store: 19,081,655,132 bytes
total store: 37,383,592,759 bytes
primaries indexing.index_total: 27,519,769
primaries indexing.index_time: 1,471,067 ms
primaries indexing.throttle_time: 175,506 ms
total indexing.index_total: 55,039,538
total indexing.index_time: 3,899,905 ms
total indexing.throttle_time: 528,180 ms
total merge.total_time: 2,217,860 ms
total merge.total_throttled_time: 1,505,607 ms
total refresh.total_time: 2,554,599 ms
total flush.total_time: 5,687,767 ms
segments: 207
```

w2 shard distribution after completion:

| Shard | Primary node | Primary docs | Primary bytes | Replica node | Replica bytes |
| --- | --- | ---: | ---: | --- | ---: |
| 0 | es-3 | 9,161,498 | 6,191,146,388 | es-2 | 5,717,202,531 |
| 1 | es-1 | 9,163,454 | 7,140,111,961 | es-2 | 5,805,110,197 |
| 2 | es-3 | 9,164,817 | 5,750,396,783 | es-1 | 6,779,624,899 |

## Optimization Reruns

Two follow-up ES write tests were run after the 3-node baseline:

```text
EBS6000:
  topology: 3 ES data/master/ingest nodes + 1 driver
  instance type: c5.xlarge
  storage: 100GB gp3 each, 6000 IOPS, 250 MiB/s
  index: 3 primary shards, 1 replica

6-node:
  topology: 6 ES data/master/ingest nodes + 1 driver
  instance type: c5.xlarge
  storage: 100GB gp3 each, 3000 IOPS, 125 MiB/s
  index: schema/es_eth_transactions_10gb_replicated_6shards_mapping.json
         6 primary shards, 1 replica
```

All four optimization runs used the same 10GB manifest and the same indexed
field surface as the 3-node baseline.

| Topology | Run | Workers | Rows | Elapsed | Rows/s | Errors | Retries | `_count` | Store | Primary store |
| --- | --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| EBS6000 | `20260507T064258Z_es_ebs6000_w1_b5000` | 1 | 27,489,769 | 1,657.12s | 16,588.9 | 0 | 1 | 27,489,769 | 34,726,546,709 | 17,359,372,888 |
| EBS6000 | `20260507T071159Z_es_ebs6000_w2_b5000` | 2 | 27,489,769 | 1,650.95s | 16,650.9 | 0 | 3 | 27,489,769 | 35,937,181,742 | 17,828,000,708 |
| 6-node | `20260507T075452Z_es_6node_gp3base_w1_b5000` | 1 | 27,489,769 | 1,346.57s | 20,414.7 | 0 | 0 | 27,489,769 | 40,348,004,963 | 19,733,296,829 |
| 6-node | `20260507T081808Z_es_6node_gp3base_w2_b5000` | 2 | 27,489,769 | 988.39s | 27,812.6 | 0 | 0 | 27,489,769 | 39,382,490,643 | 20,830,128,455 |

Optimization bulk latency:

| Topology | Workers | Bulk requests | Avg | P50 | P95 | P99 | Max | >5s batches | >60s batches |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| EBS6000 | 1 | 5,502 | 260.651 ms | 216.946 ms | 315.377 ms | 396.900 ms | 157,347.539 ms | 1 | 1 |
| EBS6000 | 2 | 5,504 | 553.669 ms | 239.682 ms | 356.547 ms | 3,094.752 ms | 252,407.016 ms | 47 | 8 |
| 6-node | 1 | 5,504 | 205.249 ms | 184.110 ms | 321.801 ms | 446.081 ms | 1,399.288 ms | 0 | 0 |
| 6-node | 2 | 5,504 | 316.074 ms | 196.197 ms | 408.481 ms | 772.294 ms | 76,754.911 ms | 39 | 2 |

Final index-level stats for the optimization reruns:

| Topology | Workers | Total indexing throttle | Total merge throttled | Total flush time | Segments |
| --- | ---: | ---: | ---: | ---: | ---: |
| EBS6000 | 1 | 63,596 ms | 686,664 ms | 4,164,158 ms | 181 |
| EBS6000 | 2 | 338,905 ms | 1,111,000 ms | 4,642,229 ms | 208 |
| 6-node | 1 | 0 ms | 582,868 ms | 3,588,225 ms | 315 |
| 6-node | 2 | 0 ms | 1,028,394 ms | 2,984,368 ms | 373 |

## Interpretation

The multi-node ES topology more than doubled single-node ES throughput from the
previous smoke, but increasing writer concurrency from one to two workers did
not help on this storage shape:

```text
single-node ES w1, no replica: 7,053.3 rows/s
3-node ES w1, one replica:    14,459.0 rows/s
3-node ES w2, one replica:    13,364.8 rows/s
```

The w2 run did not fail and did not produce write-thread rejections, but it
created substantially worse long-tail latency. The bottleneck was ES internal
flush, merge, and indexing throttle on the 100GB gp3 baseline volumes, not the
Go client and not HTTP endpoint imbalance.

Increasing gp3 performance on the 3-node cluster improved throughput from
14,459.0 to about 16,650 rows/s, but the modeled hourly cost also rose from
about $0.724/h to about $0.836/h. Throughput per dollar therefore did not
materially improve.

Rolling gp3 back to baseline and increasing data-node count to six changed the
result more materially. With `workers=2`, the 6-node cluster reached 27,812.6
rows/s, count matched exactly, and there were no client retries or ES
write-thread rejections. It still produced long-tail bulk stalls, including two
batches above 60 seconds, so larger runs should continue to track max latency,
merge throttle, flush time, and store growth.

Current ES write-test guidance:

```text
1. Use the 6-node workers=2 result as the current 10GB ES write baseline.
2. If testing larger data on the same nodes, start with workers=2 and watch
   merge.total_throttled_time, flush.total_time, and max bulk latency.
3. Prefer adding data nodes before raising gp3 performance further, based on
   the current cost-efficiency result.
4. Keep the same indexed field surface and one-replica setting when comparing
   against TiCI.
```

## Storage Scaling Estimate

Production-style 10GB primary-store ratio:

```text
w1 primary/local parquet: 18,610,717,581 / 10,013,628,561 = 1.86x
w2 primary/local parquet: 19,081,655,132 / 10,013,628,561 = 1.91x
6-node w2 primary/local parquet: 20,830,128,455 / 10,013,628,561 = 2.08x
```

For a later 1TB local parquet run, use about `1.9TB` to `2.1TB` primary store
as the planning baseline. With one replica this is about `3.8TB` to `4.2TB` ES
store before headroom. With 25% disk headroom, the raw ES data capacity target
is about `5.1TB` to `5.6TB`.
