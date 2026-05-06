package ethbench

import (
	"context"
	"fmt"
	"io"
	"os"
	"strconv"
	"strings"
	"sync"

	"github.com/parquet-go/parquet-go"
)

func StreamManifest(ctx context.Context, manifest *Manifest, readerWorkers int, bufferSize int) (<-chan EthTransaction, <-chan error) {
	if readerWorkers <= 0 {
		readerWorkers = 1
	}
	if bufferSize <= 0 {
		bufferSize = 4096
	}
	records := make(chan EthTransaction, bufferSize)
	errs := make(chan error, readerWorkers)
	jobs := make(chan ManifestEntry)

	var wg sync.WaitGroup
	for i := 0; i < readerWorkers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for entry := range jobs {
				if err := StreamParquetFile(ctx, entry, records); err != nil {
					errs <- err
					return
				}
			}
		}()
	}

	go func() {
		defer close(jobs)
		for _, entry := range manifest.Entries {
			select {
			case <-ctx.Done():
				return
			case jobs <- entry:
			}
		}
	}()

	go func() {
		wg.Wait()
		close(records)
		close(errs)
	}()

	return records, errs
}

func StreamParquetFile(ctx context.Context, entry ManifestEntry, out chan<- EthTransaction) error {
	if entry.Source != "local" {
		return fmt.Errorf("unsupported manifest source %q for %s", entry.Source, entry.Key)
	}
	file, err := os.Open(entry.Path)
	if err != nil {
		return err
	}
	defer file.Close()

	info, err := file.Stat()
	if err != nil {
		return err
	}
	pfile, err := parquet.OpenFile(file, info.Size())
	if err != nil {
		return err
	}
	columns := leafColumnNames(pfile.Schema())
	for _, rowGroup := range pfile.RowGroups() {
		rows := rowGroup.Rows()
		batch := make([]parquet.Row, 1024)
		for {
			n, readErr := rows.ReadRows(batch)
			for i := 0; i < n; i++ {
				record := rowToEthTransaction(batch[i], columns, entry.Date)
				record.Normalize()
				if !record.Valid() {
					continue
				}
				select {
				case <-ctx.Done():
					_ = rows.Close()
					return ctx.Err()
				case out <- record:
				}
			}
			if readErr == io.EOF {
				break
			}
			if readErr != nil {
				_ = rows.Close()
				return readErr
			}
		}
		if err := rows.Close(); err != nil {
			return err
		}
	}
	return nil
}

func leafColumnNames(schema *parquet.Schema) []string {
	paths := schema.Columns()
	columns := make([]string, len(paths))
	for i, path := range paths {
		if len(path) == 0 {
			continue
		}
		columns[i] = path[len(path)-1]
	}
	return columns
}

func rowToEthTransaction(row parquet.Row, columns []string, date string) EthTransaction {
	var record EthTransaction
	record.Date = date
	row.Range(func(columnIndex int, values []parquet.Value) bool {
		if columnIndex < 0 || columnIndex >= len(columns) || len(values) == 0 {
			return true
		}
		value, ok := firstValue(values)
		if !ok {
			return true
		}
		switch columns[columnIndex] {
		case "hash":
			record.Hash = valueToString(value)
		case "nonce":
			record.Nonce = intPtrValue(value)
		case "transaction_index":
			record.TransactionIndex = intPtrValue(value)
		case "from_address":
			record.FromAddress = stringPtrValue(value)
		case "to_address":
			record.ToAddress = stringPtrValue(value)
		case "value":
			record.Value = floatPtrValue(value)
		case "gas":
			record.Gas = intPtrValue(value)
		case "gas_price":
			if gasPrice := intPtrValue(value); gasPrice != nil {
				record.GasPrice = *gasPrice
			}
		case "input":
			record.Input = stringPtrValue(value)
		case "receipt_cumulative_gas_used":
			record.ReceiptCumulativeGasUsed = intPtrValue(value)
		case "receipt_gas_used":
			record.ReceiptGasUsed = intPtrValue(value)
		case "receipt_contract_address":
			record.ReceiptContractAddress = stringPtrValue(value)
		case "receipt_status":
			record.ReceiptStatus = intPtrValue(value)
		case "block_timestamp":
			if timestamp := intPtrValue(value); timestamp != nil {
				record.BlockTimestamp = normalizeUnixSeconds(*timestamp)
			} else if text := valueToString(value); text != "" {
				if parsed, err := strconv.ParseInt(text, 10, 64); err == nil {
					record.BlockTimestamp = normalizeUnixSeconds(parsed)
				}
			}
		case "block_number":
			record.BlockNumber = intPtrValue(value)
		case "block_hash":
			record.BlockHash = stringPtrValue(value)
		case "max_fee_per_gas":
			record.MaxFeePerGas = intPtrValue(value)
		case "max_priority_fee_per_gas":
			record.MaxPriorityFeePerGas = intPtrValue(value)
		case "transaction_type":
			record.TransactionType = intPtrValue(value)
		case "receipt_effective_gas_price":
			record.ReceiptEffectiveGasPrice = intPtrValue(value)
		}
		return true
	})
	return record
}

func firstValue(values []parquet.Value) (parquet.Value, bool) {
	for _, value := range values {
		if !value.IsNull() {
			return value, true
		}
	}
	return parquet.Value{}, false
}

func intPtrValue(value parquet.Value) *int64 {
	var out int64
	switch value.Kind() {
	case parquet.Int64:
		out = value.Int64()
	case parquet.Int32:
		out = int64(value.Int32())
	case parquet.Double:
		out = int64(value.Double())
	case parquet.Float:
		out = int64(value.Float())
	case parquet.ByteArray, parquet.FixedLenByteArray:
		parsed, err := strconv.ParseInt(strings.TrimSpace(string(value.ByteArray())), 10, 64)
		if err != nil {
			return nil
		}
		out = parsed
	default:
		return nil
	}
	return &out
}

func floatPtrValue(value parquet.Value) *float64 {
	var out float64
	switch value.Kind() {
	case parquet.Double:
		out = value.Double()
	case parquet.Float:
		out = float64(value.Float())
	case parquet.Int64:
		out = float64(value.Int64())
	case parquet.Int32:
		out = float64(value.Int32())
	case parquet.ByteArray, parquet.FixedLenByteArray:
		parsed, err := strconv.ParseFloat(strings.TrimSpace(string(value.ByteArray())), 64)
		if err != nil {
			return nil
		}
		out = parsed
	default:
		return nil
	}
	return &out
}

func stringPtrValue(value parquet.Value) *string {
	text := valueToString(value)
	if text == "" {
		return nil
	}
	return &text
}

func valueToString(value parquet.Value) string {
	switch value.Kind() {
	case parquet.ByteArray, parquet.FixedLenByteArray:
		return string(value.ByteArray())
	case parquet.Int64:
		return strconv.FormatInt(value.Int64(), 10)
	case parquet.Int32:
		return strconv.FormatInt(int64(value.Int32()), 10)
	case parquet.Double:
		return strconv.FormatFloat(value.Double(), 'f', -1, 64)
	case parquet.Float:
		return strconv.FormatFloat(float64(value.Float()), 'f', -1, 32)
	case parquet.Boolean:
		if value.Boolean() {
			return "true"
		}
		return "false"
	default:
		return ""
	}
}

func normalizeUnixSeconds(value int64) int64 {
	switch {
	case value > 1_000_000_000_000_000_000:
		return value / 1_000_000_000
	case value > 1_000_000_000_000_000:
		return value / 1_000_000
	case value > 1_000_000_000_000:
		return value / 1_000
	default:
		return value
	}
}
