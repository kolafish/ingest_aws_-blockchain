package main

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"net/http"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/kolafish/ingest_aws_-blockchain/internal/ethbench"
)

func main() {
	var (
		manifestPath  = flag.String("manifest", "", "Local parquet manifest.")
		esURL         = flag.String("url", envDefault("ES_URL", "http://127.0.0.1:9200"), "Elasticsearch URL. Use commas to provide multiple endpoints.")
		index         = flag.String("index", "eth_transactions", "Elasticsearch index.")
		user          = flag.String("user", os.Getenv("ES_USER"), "Elasticsearch user.")
		password      = flag.String("password", os.Getenv("ES_PASSWORD"), "Elasticsearch password.")
		readerWorkers = flag.Int("reader-workers", 2, "Concurrent parquet file readers.")
		workers       = flag.Int("workers", 8, "Concurrent bulk workers.")
		batchRows     = flag.Int("batch-rows", 5000, "Rows per bulk request.")
		batchBytes    = flag.Int("batch-bytes", 10_000_000, "Approximate max bulk request bytes.")
		retries       = flag.Int("retries", 3, "Retries per failed bulk request.")
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
	esURLs := parseURLs(*esURL)
	if len(esURLs) == 0 {
		fmt.Fprintln(os.Stderr, "--url must contain at least one Elasticsearch endpoint")
		os.Exit(2)
	}
	results, err := ethbench.NewResultWriter(*resultPath)
	if err != nil {
		fmt.Fprintf(os.Stderr, "open result jsonl: %v\n", err)
		os.Exit(1)
	}
	defer results.Close()

	client := &http.Client{Timeout: 120 * time.Second}
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
			batchSize := 0
			for record := range records {
				recordBytes := int(record.ApproxBytes())
				batch = append(batch, record)
				batchSize += recordBytes + 128
				if len(batch) >= *batchRows || batchSize >= *batchBytes {
					flushESBatch(ctx, client, results, esURLs, *index, *user, *password, *runID, batch, *retries, &ingestWindow, &batchID, &totalRows, &totalBytes, &totalErrors)
					batch = batch[:0]
					batchSize = 0
				}
			}
			if len(batch) > 0 {
				flushESBatch(ctx, client, results, esURLs, *index, *user, *password, *runID, batch, *retries, &ingestWindow, &batchID, &totalRows, &totalBytes, &totalErrors)
			}
		}()
	}
	wg.Wait()

	for err := range errs {
		if err != nil {
			atomic.AddInt64(&totalErrors, 1)
			_ = results.Write(ethbench.NewBatchResult("elasticsearch", *runID, "bulk", atomic.AddInt64(&batchID, 1), 0, 0, 0, 0, err))
		}
	}

	summary := ethbench.NewSummary("elasticsearch", *runID, "bulk", totalRows, totalBytes, totalErrors, ingestWindow.Elapsed(started))
	_ = results.Write(summary)
	fmt.Printf("es bulk rows=%d logical_bytes=%d elapsed=%.2fs rows_per_sec=%.1f mb_per_sec=%.1f errors=%d\n",
		summary.Rows, summary.LogicalBytes, summary.ElapsedSecs, summary.RowsPerSec, summary.MBPerSec, summary.Errors)
	if summary.Errors > 0 {
		os.Exit(1)
	}
}

func flushESBatch(ctx context.Context, client *http.Client, results *ethbench.ResultWriter, esURLs []string, index, user, password, runID string, batch []ethbench.EthTransaction, maxRetries int, ingestWindow *ethbench.IngestWindow, batchID, totalRows, totalBytes, totalErrors *int64) {
	id := atomic.AddInt64(batchID, 1)
	logicalBytes := int64(0)
	for _, record := range batch {
		logicalBytes += record.ApproxBytes()
	}

	var err error
	started := time.Now()
	attempt := 0
	for ; attempt <= maxRetries; attempt++ {
		esURL := esURLs[int((id+int64(attempt))%int64(len(esURLs)))]
		err = execESBulk(ctx, client, esURL, index, user, password, batch)
		if err == nil {
			break
		}
		time.Sleep(time.Duration(attempt+1) * 500 * time.Millisecond)
	}
	latency := time.Since(started)
	ingestWindow.Mark(started, time.Now())
	if err != nil {
		atomic.AddInt64(totalErrors, 1)
	} else {
		atomic.AddInt64(totalRows, int64(len(batch)))
		atomic.AddInt64(totalBytes, logicalBytes)
	}
	_ = results.Write(ethbench.NewBatchResult("elasticsearch", runID, "bulk", id, len(batch), logicalBytes, latency, attempt, err))
}

func execESBulk(ctx context.Context, client *http.Client, esURL, index, user, password string, batch []ethbench.EthTransaction) error {
	var body bytes.Buffer
	writer := bufio.NewWriterSize(&body, 1<<20)
	for _, record := range batch {
		meta := map[string]any{"index": map[string]any{"_index": index, "_id": record.DocumentID()}}
		if err := writeJSONLine(writer, meta); err != nil {
			return err
		}
		if err := writeJSONLine(writer, record); err != nil {
			return err
		}
	}
	if err := writer.Flush(); err != nil {
		return err
	}

	endpoint := strings.TrimRight(esURL, "/") + "/_bulk?refresh=false"
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, endpoint, &body)
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/x-ndjson")
	if user != "" {
		req.SetBasicAuth(user, password)
	}
	resp, err := client.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	data, readErr := io.ReadAll(resp.Body)
	if readErr != nil {
		return readErr
	}
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return fmt.Errorf("bulk status %d: %s", resp.StatusCode, string(data))
	}
	var bulk bulkResponse
	if err := json.Unmarshal(data, &bulk); err != nil {
		return err
	}
	if bulk.Errors {
		return bulk.firstError()
	}
	return nil
}

func writeJSONLine(writer *bufio.Writer, value any) error {
	data, err := json.Marshal(value)
	if err != nil {
		return err
	}
	if _, err := writer.Write(data); err != nil {
		return err
	}
	return writer.WriteByte('\n')
}

type bulkResponse struct {
	Errors bool                         `json:"errors"`
	Items  []map[string]bulkItemDetails `json:"items"`
}

type bulkItemDetails struct {
	Status int            `json:"status"`
	Error  map[string]any `json:"error,omitempty"`
}

func (r bulkResponse) firstError() error {
	for _, item := range r.Items {
		for action, details := range item {
			if details.Status >= 300 {
				errorData, _ := json.Marshal(details.Error)
				return fmt.Errorf("bulk %s status %d: %s", action, details.Status, string(errorData))
			}
		}
	}
	return fmt.Errorf("bulk response reported errors")
}

func envDefault(key, fallback string) string {
	if value := os.Getenv(key); value != "" {
		return value
	}
	return fallback
}

func parseURLs(value string) []string {
	parts := strings.Split(value, ",")
	urls := make([]string, 0, len(parts))
	for _, part := range parts {
		trimmed := strings.TrimSpace(part)
		if trimmed == "" {
			continue
		}
		urls = append(urls, trimmed)
	}
	return urls
}
