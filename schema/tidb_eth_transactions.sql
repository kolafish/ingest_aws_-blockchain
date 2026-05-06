CREATE DATABASE IF NOT EXISTS test;

CREATE TABLE IF NOT EXISTS test.eth_transactions (
  date VARCHAR(10) NOT NULL,
  hash VARCHAR(66) NOT NULL,
  block_timestamp BIGINT NOT NULL,
  nonce BIGINT NULL,
  transaction_index BIGINT NULL,
  from_address VARCHAR(42) NULL,
  to_address VARCHAR(42) NULL,
  value DOUBLE NULL,
  gas BIGINT NULL,
  gas_price BIGINT NOT NULL DEFAULT 0,
  input TEXT NULL,
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
  random_flag BOOLEAN NOT NULL,
  PRIMARY KEY (block_timestamp, gas_price, hash)
);

CREATE HYBRID INDEX idx_eth_transactions_hybrid
ON test.eth_transactions (
  date,
  hash,
  from_address,
  to_address,
  receipt_contract_address,
  block_timestamp,
  block_number,
  transaction_index,
  gas,
  gas_price,
  receipt_status,
  transaction_type,
  random_flag
)
PARAMETER '{
  "inverted": {
    "columns": [
      "date",
      "hash",
      "from_address",
      "to_address",
      "receipt_contract_address",
      "block_timestamp",
      "block_number",
      "transaction_index",
      "gas",
      "gas_price",
      "receipt_status",
      "transaction_type",
      "random_flag"
    ]
  },
  "sort": {
    "columns": ["block_timestamp", "gas_price"],
    "order": ["desc", "desc"]
  }
}';
