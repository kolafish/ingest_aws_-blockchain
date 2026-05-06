package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"sync/atomic"
	"time"

	"github.com/kolafish/ingest_aws_-blockchain/internal/ethbench"
)

func main() {
	var (
		manifestPath  = flag.String("manifest", "", "Local parquet manifest.")
		readerWorkers = flag.Int("reader-workers", 2, "Concurrent parquet file readers.")
		batchRows     = flag.Int("batch-rows", 5000, "Rows per result sample.")
		encode        = flag.String("encode", "es", "Encoding work to simulate: none or es.")
		resultPath    = flag.String("result-jsonl", "", "Optional JSONL result path.")
		runID         = flag.String("run-id", time.Now().UTC().Format("20060102T150405Z"), "Run identifier.")
	)
	flag.Parse()

	if *manifestPath == "" {
		fmt.Fprintln(os.Stderr, "--manifest is required")
		os.Exit(2)
	}
	manifest, err := ethbench.LoadManifest(*manifestPath)
	if err != nil {
		fmt.Fprintf(os.Stderr, "load manifest: %v\n", err)
		os.Exit(1)
	}
	if err := manifest.ValidateLocal(); err != nil {
		fmt.Fprintf(os.Stderr, "validate manifest: %v\n", err)
		os.Exit(1)
	}
	results, err := ethbench.NewResultWriter(*resultPath)
	if err != nil {
		fmt.Fprintf(os.Stderr, "open result jsonl: %v\n", err)
		os.Exit(1)
	}
	defer results.Close()

	ctx := context.Background()
	records, errs := ethbench.StreamManifest(ctx, manifest, *readerWorkers, *batchRows*2)
	started := time.Now()

	var rows, logicalBytes, errors, batchID int64
	var batchRowsSeen int
	var batchBytes int64
	batchStarted := time.Now()

	for record := range records {
		if *encode == "es" {
			if _, err := json.Marshal(record); err != nil {
				atomic.AddInt64(&errors, 1)
				continue
			}
		}
		atomic.AddInt64(&rows, 1)
		recordBytes := record.ApproxBytes()
		atomic.AddInt64(&logicalBytes, recordBytes)
		batchRowsSeen++
		batchBytes += recordBytes
		if batchRowsSeen >= *batchRows {
			batchID++
			_ = results.Write(ethbench.NewBatchResult("client", *runID, "dry_run", batchID, batchRowsSeen, batchBytes, time.Since(batchStarted), 0, nil))
			batchRowsSeen = 0
			batchBytes = 0
			batchStarted = time.Now()
		}
	}
	for err := range errs {
		if err != nil {
			atomic.AddInt64(&errors, 1)
			_ = results.Write(ethbench.NewBatchResult("client", *runID, "dry_run", batchID+1, 0, 0, 0, 0, err))
		}
	}
	if batchRowsSeen > 0 {
		batchID++
		_ = results.Write(ethbench.NewBatchResult("client", *runID, "dry_run", batchID, batchRowsSeen, batchBytes, time.Since(batchStarted), 0, nil))
	}

	summary := ethbench.NewSummary("client", *runID, "dry_run", rows, logicalBytes, errors, time.Since(started))
	_ = results.Write(summary)
	fmt.Printf("dry-run rows=%d logical_bytes=%d elapsed=%.2fs rows_per_sec=%.1f mb_per_sec=%.1f errors=%d\n",
		summary.Rows, summary.LogicalBytes, summary.ElapsedSecs, summary.RowsPerSec, summary.MBPerSec, summary.Errors)
	if summary.Errors > 0 {
		os.Exit(1)
	}
}
