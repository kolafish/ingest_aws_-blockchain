package main

import (
	"context"
	"database/sql"
	"flag"
	"fmt"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	_ "github.com/go-sql-driver/mysql"
	"github.com/kolafish/ingest_aws_-blockchain/internal/ethbench"
)

func main() {
	var (
		manifestPath  = flag.String("manifest", "", "Local parquet manifest.")
		dsn           = flag.String("dsn", envDefault("TIDB_DSN", "root:@tcp(127.0.0.1:4000)/test?charset=utf8mb4&parseTime=true"), "TiDB/MySQL DSN.")
		table         = flag.String("table", "test.eth_transactions", "Fully qualified destination table.")
		readerWorkers = flag.Int("reader-workers", 2, "Concurrent parquet file readers.")
		workers       = flag.Int("workers", 8, "Concurrent DB insert workers.")
		batchRows     = flag.Int("batch-rows", 2000, "Rows per INSERT statement.")
		retries       = flag.Int("retries", 3, "Retries per failed batch.")
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

	db, err := sql.Open("mysql", *dsn)
	if err != nil {
		fmt.Fprintf(os.Stderr, "open tidb: %v\n", err)
		os.Exit(1)
	}
	defer db.Close()
	db.SetMaxOpenConns(*workers + *readerWorkers + 2)
	db.SetMaxIdleConns(*workers)
	if err := db.Ping(); err != nil {
		fmt.Fprintf(os.Stderr, "ping tidb: %v\n", err)
		os.Exit(1)
	}

	ctx := context.Background()
	records, errs := ethbench.StreamManifest(ctx, manifest, *readerWorkers, *batchRows*(*workers+1))
	started := time.Now()

	var totalRows, totalBytes, totalErrors, batchID int64
	var ingestWindow ethbench.IngestWindow
	var wg sync.WaitGroup
	for i := 0; i < *workers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			batch := make([]ethbench.EthTransaction, 0, *batchRows)
			for record := range records {
				batch = append(batch, record)
				if len(batch) >= *batchRows {
					flushTiDBBatch(ctx, db, results, *table, *runID, batch, *retries, &ingestWindow, &batchID, &totalRows, &totalBytes, &totalErrors)
					batch = batch[:0]
				}
			}
			if len(batch) > 0 {
				flushTiDBBatch(ctx, db, results, *table, *runID, batch, *retries, &ingestWindow, &batchID, &totalRows, &totalBytes, &totalErrors)
			}
		}()
	}
	wg.Wait()

	for err := range errs {
		if err != nil {
			atomic.AddInt64(&totalErrors, 1)
			_ = results.Write(ethbench.NewBatchResult("tidb-tici", *runID, "insert", atomic.AddInt64(&batchID, 1), 0, 0, 0, 0, err))
		}
	}

	summary := ethbench.NewSummary("tidb-tici", *runID, "insert", totalRows, totalBytes, totalErrors, ingestWindow.Elapsed(started))
	_ = results.Write(summary)
	fmt.Printf("tidb insert rows=%d logical_bytes=%d elapsed=%.2fs rows_per_sec=%.1f mb_per_sec=%.1f errors=%d\n",
		summary.Rows, summary.LogicalBytes, summary.ElapsedSecs, summary.RowsPerSec, summary.MBPerSec, summary.Errors)
	if summary.Errors > 0 {
		os.Exit(1)
	}
}

func flushTiDBBatch(ctx context.Context, db *sql.DB, results *ethbench.ResultWriter, table, runID string, batch []ethbench.EthTransaction, maxRetries int, ingestWindow *ethbench.IngestWindow, batchID, totalRows, totalBytes, totalErrors *int64) {
	id := atomic.AddInt64(batchID, 1)
	logicalBytes := int64(0)
	for _, record := range batch {
		logicalBytes += record.ApproxBytes()
	}

	var err error
	started := time.Now()
	attempt := 0
	for ; attempt <= maxRetries; attempt++ {
		err = execTiDBBatch(ctx, db, table, batch)
		if err == nil {
			break
		}
		time.Sleep(time.Duration(attempt+1) * 250 * time.Millisecond)
	}
	latency := time.Since(started)
	ingestWindow.Mark(started, time.Now())
	if err != nil {
		atomic.AddInt64(totalErrors, 1)
	} else {
		atomic.AddInt64(totalRows, int64(len(batch)))
		atomic.AddInt64(totalBytes, logicalBytes)
	}
	_ = results.Write(ethbench.NewBatchResult("tidb-tici", runID, "insert", id, len(batch), logicalBytes, latency, attempt, err))
}

func execTiDBBatch(ctx context.Context, db *sql.DB, table string, batch []ethbench.EthTransaction) error {
	if len(batch) == 0 {
		return nil
	}
	columnList := "`" + strings.Join(ethbench.InsertColumns, "`, `") + "`"
	rowPlaceholders := "(" + strings.TrimRight(strings.Repeat("?,", len(ethbench.InsertColumns)), ",") + ")"
	placeholders := strings.TrimRight(strings.Repeat(rowPlaceholders+",", len(batch)), ",")
	query := fmt.Sprintf("INSERT INTO %s (%s) VALUES %s", table, columnList, placeholders)
	args := make([]any, 0, len(batch)*len(ethbench.InsertColumns))
	for _, record := range batch {
		args = append(args, record.InsertValues()...)
	}
	_, err := db.ExecContext(ctx, query, args...)
	return err
}

func envDefault(key, fallback string) string {
	if value := os.Getenv(key); value != "" {
		return value
	}
	return fallback
}
