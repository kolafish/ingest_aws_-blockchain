# Canonical 30GB Write Dataset - 2026-05-07

This locks the next TiCI and Elasticsearch write test to the exact same S3
objects. It replaces the earlier 10GB smoke behavior where TiCI and ES used
different manifests.

## Locked Dataset

Manifest:

```text
manifests/eth_transactions_30gb_s3.lock.json
```

S3 source:

```text
s3://aws-public-blockchain/v1.0/eth/transactions/
```

Locked range and size:

```text
first date: 2025-09-19
last date: 2025-11-10
parquet objects: 53
parquet bytes: 30,486,156,099
parquet size: 30.49 GB / 28.39 GiB
```

Remote dry-run on the TiCI center node:

```text
instance: tidb-test-center only
local data path: /home/ubuntu/bench30/data/eth_transactions_30gb
local manifest: /home/ubuntu/bench30/manifests/eth_transactions_30gb_local.json
result: /home/ubuntu/bench30/results/client_dry_run_30gb_20260507T110531Z.jsonl
```

Dry-run result:

```text
rows: 82,501,974
logical bytes: 118,956,184,322
elapsed: 631.85s
rows/s: 130,572.5
MB/s: 188.3
errors: 0
```

The dry-run only read local parquet and encoded records as ES JSON on the remote
center node. It did not connect to TiDB or Elasticsearch.

## Equality Contract

For the next TiCI and ES write runs, both systems must use this S3 lock. The
required final validation is:

```text
TiCI SELECT COUNT(*) = 82,501,974
ES _count            = 82,501,974
writer logical bytes = 118,956,184,322
```

If any system reports a different row count, treat the run as invalid until the
manifest path and object list are confirmed.

## Execution Order

Cost-control sequence:

```text
1. Keep ES destroyed.
2. Keep TiFlash stopped for write-only TiCI tests.
3. Start only the TiCI/TiDB nodes needed for INSERT + TiCDC + TiCI ingestion.
4. Run TiCI workers=1 first.
5. Run TiCI workers=2 only after workers=1 count and CDC catch-up are clean.
6. Stop TiCI/TiDB cluster before provisioning ES.
7. Run ES 6-node baseline gp3 with the same S3 lock.
8. Destroy ES stack after results are copied.
```

## Expected Scale

The locked dataset is about 3.04x the prior ES 10GB parquet input:

```text
30,486,156,099 / 10,013,628,561 = 3.04x
```

The logical write volume is about 3.02x the prior ES 10GB logical bytes:

```text
118,956,184,322 / 39,399,039,010 = 3.02x
```

This is therefore the planned "about 100GB actual write" step before moving to
larger 1TB-scale tests.
