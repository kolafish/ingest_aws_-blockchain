# Elasticsearch Production-Style Rerun Plan

This plan replaces the earlier single-node Elasticsearch smoke with a
production-style shape, but it keeps the first rerun sized for a 10GB dataset.
The goal is to validate replica cost, master quorum, shard distribution, and
writer routing without jumping directly to a 1TB cluster.

## Source Guidance

Elastic production guidance used for this plan:

- Production deployments should use multiple zones, replicas, and three
  master-eligible nodes for HA:
  https://www.elastic.co/docs/deploy-manage/deploy/elastic-cloud/elastic-cloud-hosted-planning
- A resilient cluster needs at least three master-eligible nodes and at least
  two copies of each shard:
  https://www.elastic.co/docs/deploy-manage/production-guidance/availability-and-resilience
- Dedicated master nodes are most useful once the cluster has more than a
  handful of nodes; smaller clusters may use data nodes that are also
  master-eligible:
  https://www.elastic.co/docs/deploy-manage/distributed-architecture/clusters-nodes-shards/node-roles
- Shards should generally target 10GB to 50GB and stay below about 200M
  documents per shard:
  https://www.elastic.co/docs/deploy-manage/production-guidance/optimize-performance/size-shards
- Bulk indexing should tune bulk size and worker count, watch for 429 rejects,
  disable or increase refresh during bulk load, preserve filesystem cache, and
  use fast storage:
  https://www.elastic.co/docs/deploy-manage/production-guidance/optimize-performance/indexing-speed

## Current 10GB Rerun Topology

Use a single-AZ, 3-node ES cluster plus one driver. This is not full HA because
it is in one AZ, but it keeps the production-relevant ES mechanics that the
single-node smoke missed: three master-eligible nodes, one replica, and
cross-node shard distribution.

```text
es-1..3:
  count: 3
  role: master + data_hot + ingest
  instance type: c5.xlarge
  heap: 4g
  disk: 100GB gp3
  gp3 performance: 3000 IOPS, 125 MiB/s

driver-1:
  count: 1
  role: Go writer, manifests/data staging, metrics, optional HAProxy
  instance type: c5.xlarge
  disk: 100GB gp3
  gp3 performance: 3000 IOPS, 125 MiB/s
```

Why not three dedicated masters for this run: Elastic's requirement is three
master-eligible nodes for quorum. Dedicated masters are recommended as clusters
grow, but a 3-node 10GB benchmark can use the same nodes for master, data, and
ingest to keep cost low while still testing replicas and distribution.

The ES resources must use a separate Terraform state from the TiCI/TiDB
cluster. Do not apply the existing `terraform-tici` state for ES; its current
plan would replace stopped TiCI/TiDB instances because their public IPs changed
while stopped.

## TiCI/TiDB Baseline Storage

The current TiCI/TiDB AWS testbed volume configuration is:

| Component | Volume | IOPS | Throughput |
| --- | ---: | ---: | ---: |
| `tidb` | 100GB gp3 | 3000 | 125 MiB/s |
| `pd` | 100GB gp3 | 3000 | 125 MiB/s |
| `tikv` | 100GB gp3 | 4000 | 288 MiB/s |
| `tiflash` | 100GB gp3 | 4000 | 288 MiB/s |
| `cdc` | 100GB gp3 | 4000 | 288 MiB/s |
| `tici-worker` | 100GB gp3 | 3000 | 125 MiB/s |
| `center` | 100GB gp3 | 3000 | 125 MiB/s |
| `tici-meta` | 8GB gp2 | 100 | n/a |

Use the same baseline gp3 class for the 10GB ES rerun. If ES still shows heavy
merge/indexing throttle on 3 nodes, scale instance memory or gp3 throughput only
after this point.

## Elasticsearch Node Configuration

Common settings:

```yaml
cluster.name: eth-es-prod10g-bench
network.host: <private-ip>
http.host: <private-ip>
transport.host: <private-ip>
path.data: /home/ubuntu/elasticsearch-prod10g/data
path.logs: /home/ubuntu/elasticsearch-prod10g/logs
bootstrap.memory_lock: false
discovery.seed_hosts:
  - 172.31.21.1
  - 172.31.21.2
  - 172.31.21.3
cluster.initial_master_nodes:
  - es-1
  - es-2
  - es-3
node.roles: [ master, data_hot, ingest ]
```

After the cluster has formed, do not reuse `cluster.initial_master_nodes` for
node replacement or existing-cluster restarts.

The writer should send bulk requests to all ES nodes:

```bash
ES_URL=http://172.31.21.1:9200,http://172.31.21.2:9200,http://172.31.21.3:9200
```

## 10GB Index/Data Configuration

Use:

```text
schema/es_eth_transactions_10gb_replicated_mapping.json
```

Key settings:

```text
number_of_shards: 3
number_of_replicas: 1
refresh_interval: -1 during timed ingest
index sort: block_timestamp desc, gas_price desc
dynamic mapping: strict
no analyzed text fields or tokenizers
```

Three primary shards allow all three data nodes to participate. One replica
captures production replica-write overhead and gives two shard copies.

## Later 1TB Target

The single-node 10GB result estimated:

```text
local parquet bytes: 10,086,645,566
ES primary store bytes: 16,585,412,386
primary store / local parquet: 1.64x
```

For 1TB local parquet input:

```text
estimated primary store: about 1.64TB
with 1 replica: about 3.28TB
with 25% disk headroom: about 4.4TB raw ES data capacity
estimated rows: about 2.68B
```

For that later run, use:

```text
schema/es_eth_transactions_prod_1t_mapping.json
number_of_shards: 48
number_of_replicas: 1
target primary shard size: about 35GB
```

The 2026-05-07 production-style 10GB rerun observed a higher primary-store
ratio than the earlier single-node smoke:

```text
3-node w1 primary/local parquet: 1.86x
3-node w2 primary/local parquet: 1.91x
```

Use about 1.9TB primary store for the later 1TB local parquet planning baseline.
With one replica and 25% disk headroom, plan roughly 5.1TB raw ES data capacity.

Do not provision 1TB-class gp3 volumes until 10GB and 100GB production-topology
runs are clean.

## Rerun Sequence

1. `prod10g-functional`
   - 3 ES nodes + 1 driver, single AZ.
   - 3 primary shards, 1 replica.
   - Start with writer `workers=1`, then `workers=2` only if no 429 rejects and
     merge/indexing throttle is reasonable.
2. `prod100g-calibration`
   - Same topology unless 10GB shows clear hardware bottleneck.
   - Consider 6 primary shards, 1 replica.
   - Tune writer workers and bulk size.
3. `prod1t`
   - Scale data nodes and gp3 capacity after 100GB results.
   - Use 48 primary shards, 1 replica.

Metrics to capture for every run:

```text
writer rows/s and MB/s
bulk latency p50/p95/p99/max
bulk retry count and 429 count
_count after refresh
_cat/shards bytes and distribution
store.size and docs.deleted
indexing.index_time and throttle_time
merge total_time and total_throttled_time
refresh and flush time
JVM heap pressure
filesystem cache and disk available
CPU, disk read/write throughput, network throughput
cost clock: instance start time -> stop time
```

## Cost Guardrails

Before starting:

```text
AWS SSO must be valid.
No TiCI cluster should be running.
Use the 10GB production-topology functional run first.
Use independent ES Terraform state.
Do not keep the 4-node ES cluster idle overnight.
```

After each run:

```text
export result JSONL and ES stats
stop Elasticsearch
stop all ES EC2 instances
destroy ES instances/volumes if the next run is not planned within 24 hours
```
