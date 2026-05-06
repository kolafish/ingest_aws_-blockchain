package ethbench

import (
	"context"
	"path/filepath"
	"testing"
	"time"

	"github.com/parquet-go/parquet-go"
	"github.com/parquet-go/parquet-go/deprecated"
)

type parquetTestTx struct {
	Hash           string  `parquet:"hash"`
	BlockTimestamp int64   `parquet:"block_timestamp"`
	FromAddress    string  `parquet:"from_address"`
	ToAddress      string  `parquet:"to_address"`
	Value          float64 `parquet:"value"`
	Gas            int64   `parquet:"gas"`
	GasPrice       int64   `parquet:"gas_price"`
	Input          string  `parquet:"input"`
	ReceiptStatus  int64   `parquet:"receipt_status"`
	BlockNumber    int64   `parquet:"block_number"`
}

func TestStreamParquetFile(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "2025-10-25__part-000.parquet")
	rows := []parquetTestTx{
		{
			Hash:           "0xABC",
			BlockTimestamp: 1_761_350_400_000,
			FromAddress:    "0xFROM",
			ToAddress:      "0xTO",
			Value:          1.5,
			Gas:            21000,
			GasPrice:       7,
			Input:          "0x",
			ReceiptStatus:  1,
			BlockNumber:    123,
		},
	}
	if err := parquet.WriteFile(path, rows); err != nil {
		t.Fatalf("write parquet: %v", err)
	}

	date, err := InferDate(path)
	if err != nil {
		t.Fatalf("infer date: %v", err)
	}
	out := make(chan EthTransaction, 1)
	errs := make(chan error, 1)
	go func() {
		defer close(out)
		errs <- StreamParquetFile(context.Background(), ManifestEntry{
			Source:    "local",
			Path:      path,
			Date:      date,
			SizeBytes: 1,
		}, out)
	}()

	record := <-out
	if err := <-errs; err != nil {
		t.Fatalf("stream parquet: %v", err)
	}
	if record.Date != "2025-10-25" {
		t.Fatalf("date = %q", record.Date)
	}
	if record.Hash != "0xabc" {
		t.Fatalf("hash = %q", record.Hash)
	}
	if record.BlockTimestamp != 1_761_350_400 {
		t.Fatalf("block_timestamp = %d", record.BlockTimestamp)
	}
	if record.FromAddress == nil || *record.FromAddress != "0xfrom" {
		t.Fatalf("from_address = %#v", record.FromAddress)
	}
	if record.GasPrice != 7 {
		t.Fatalf("gas_price = %d", record.GasPrice)
	}
	if !record.Valid() {
		t.Fatalf("record should be valid: %#v", record)
	}
}

func TestInt96UnixSeconds(t *testing.T) {
	ts := time.Date(2025, 10, 1, 12, 34, 56, 789_000_000, time.UTC)
	if got := int96UnixSeconds(timeToInt96(ts)); got != ts.Unix() {
		t.Fatalf("int96UnixSeconds() = %d, want %d", got, ts.Unix())
	}
}

func timeToInt96(ts time.Time) deprecated.Int96 {
	const julianUnixEpochDays = 2_440_588
	utc := ts.UTC()
	seconds := utc.Unix()
	days := seconds / 86_400
	secondsOfDay := seconds % 86_400
	if secondsOfDay < 0 {
		days--
		secondsOfDay += 86_400
	}
	nanosOfDay := secondsOfDay*int64(time.Second) + int64(utc.Nanosecond())
	return deprecated.Int96{
		uint32(nanosOfDay),
		uint32(nanosOfDay >> 32),
		uint32(days + julianUnixEpochDays),
	}
}

func TestParseByteSize(t *testing.T) {
	tests := map[string]int64{
		"10GB":  10_000_000_000,
		"10GiB": 10 << 30,
		"0":     0,
	}
	for input, expected := range tests {
		got, err := ParseByteSize(input)
		if err != nil {
			t.Fatalf("ParseByteSize(%q): %v", input, err)
		}
		if got != expected {
			t.Fatalf("ParseByteSize(%q) = %d, want %d", input, got, expected)
		}
	}
}
