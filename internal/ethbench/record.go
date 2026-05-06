package ethbench

import (
	"encoding/json"
	"hash/fnv"
	"strings"
)

type EthTransaction struct {
	Date                     string   `json:"date"`
	Hash                     string   `json:"hash"`
	BlockTimestamp           int64    `json:"block_timestamp"`
	Nonce                    *int64   `json:"nonce,omitempty"`
	TransactionIndex         *int64   `json:"transaction_index,omitempty"`
	FromAddress              *string  `json:"from_address,omitempty"`
	ToAddress                *string  `json:"to_address,omitempty"`
	Value                    *float64 `json:"value,omitempty"`
	Gas                      *int64   `json:"gas,omitempty"`
	GasPrice                 int64    `json:"gas_price"`
	Input                    *string  `json:"input,omitempty"`
	ReceiptCumulativeGasUsed *int64   `json:"receipt_cumulative_gas_used,omitempty"`
	ReceiptGasUsed           *int64   `json:"receipt_gas_used,omitempty"`
	ReceiptContractAddress   *string  `json:"receipt_contract_address,omitempty"`
	ReceiptStatus            *int64   `json:"receipt_status,omitempty"`
	BlockNumber              *int64   `json:"block_number,omitempty"`
	BlockHash                *string  `json:"block_hash,omitempty"`
	MaxFeePerGas             *int64   `json:"max_fee_per_gas,omitempty"`
	MaxPriorityFeePerGas     *int64   `json:"max_priority_fee_per_gas,omitempty"`
	TransactionType          *int64   `json:"transaction_type,omitempty"`
	ReceiptEffectiveGasPrice *int64   `json:"receipt_effective_gas_price,omitempty"`
	RandomFlag               bool     `json:"random_flag"`
}

var InsertColumns = []string{
	"date",
	"hash",
	"block_timestamp",
	"nonce",
	"transaction_index",
	"from_address",
	"to_address",
	"value",
	"gas",
	"gas_price",
	"input",
	"receipt_cumulative_gas_used",
	"receipt_gas_used",
	"receipt_contract_address",
	"receipt_status",
	"block_number",
	"block_hash",
	"max_fee_per_gas",
	"max_priority_fee_per_gas",
	"transaction_type",
	"receipt_effective_gas_price",
	"random_flag",
}

func (r *EthTransaction) Normalize() {
	r.Hash = strings.ToLower(r.Hash)
	lowerStringPtr(r.FromAddress)
	lowerStringPtr(r.ToAddress)
	lowerStringPtr(r.ReceiptContractAddress)
	lowerStringPtr(r.BlockHash)
	r.RandomFlag = StableFlag(r.Hash)
}

func (r EthTransaction) Valid() bool {
	return r.Date != "" && r.Hash != "" && r.BlockTimestamp > 0
}

func (r EthTransaction) InsertValues() []any {
	return []any{
		r.Date,
		r.Hash,
		r.BlockTimestamp,
		nilableInt(r.Nonce),
		nilableInt(r.TransactionIndex),
		nilableString(r.FromAddress),
		nilableString(r.ToAddress),
		nilableFloat(r.Value),
		nilableInt(r.Gas),
		r.GasPrice,
		nilableString(r.Input),
		nilableInt(r.ReceiptCumulativeGasUsed),
		nilableInt(r.ReceiptGasUsed),
		nilableString(r.ReceiptContractAddress),
		nilableInt(r.ReceiptStatus),
		nilableInt(r.BlockNumber),
		nilableString(r.BlockHash),
		nilableInt(r.MaxFeePerGas),
		nilableInt(r.MaxPriorityFeePerGas),
		nilableInt(r.TransactionType),
		nilableInt(r.ReceiptEffectiveGasPrice),
		r.RandomFlag,
	}
}

func (r EthTransaction) DocumentID() string {
	if r.Hash != "" {
		return r.Hash
	}
	return r.Date
}

func (r EthTransaction) ApproxBytes() int64 {
	data, err := json.Marshal(r)
	if err != nil {
		return 0
	}
	return int64(len(data))
}

func StableFlag(value string) bool {
	h := fnv.New64a()
	_, _ = h.Write([]byte(value))
	return h.Sum64()%2 == 0
}

func lowerStringPtr(value *string) {
	if value != nil {
		*value = strings.ToLower(*value)
	}
}

func nilableInt(value *int64) any {
	if value == nil {
		return nil
	}
	return *value
}

func nilableFloat(value *float64) any {
	if value == nil {
		return nil
	}
	return *value
}

func nilableString(value *string) any {
	if value == nil {
		return nil
	}
	return *value
}
