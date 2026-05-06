package ethbench

import (
	"encoding/json"
	"os"
	"path/filepath"
	"sync"
	"time"
)

type BatchResult struct {
	TS             string  `json:"ts"`
	System         string  `json:"system"`
	RunID          string  `json:"run_id"`
	Phase          string  `json:"phase"`
	BatchID        int64   `json:"batch_id"`
	Rows           int     `json:"rows"`
	LogicalBytes   int64   `json:"logical_bytes"`
	BatchLatencyMS float64 `json:"batch_latency_ms"`
	RetryCount     int     `json:"retry_count"`
	Error          string  `json:"error,omitempty"`
}

type Summary struct {
	System       string  `json:"system"`
	RunID        string  `json:"run_id"`
	Phase        string  `json:"phase"`
	Rows         int64   `json:"rows"`
	LogicalBytes int64   `json:"logical_bytes"`
	ElapsedSecs  float64 `json:"elapsed_secs"`
	RowsPerSec   float64 `json:"rows_per_sec"`
	MBPerSec     float64 `json:"mb_per_sec"`
	Errors       int64   `json:"errors"`
}

type ResultWriter struct {
	mu   sync.Mutex
	file *os.File
	enc  *json.Encoder
}

func NewResultWriter(path string) (*ResultWriter, error) {
	if path == "" {
		return &ResultWriter{}, nil
	}
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return nil, err
	}
	file, err := os.Create(path)
	if err != nil {
		return nil, err
	}
	return &ResultWriter{file: file, enc: json.NewEncoder(file)}, nil
}

func (w *ResultWriter) Close() error {
	if w == nil || w.file == nil {
		return nil
	}
	return w.file.Close()
}

func (w *ResultWriter) Write(value any) error {
	if w == nil || w.enc == nil {
		return nil
	}
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.enc.Encode(value)
}

func NewBatchResult(system, runID, phase string, batchID int64, rows int, logicalBytes int64, latency time.Duration, retries int, err error) BatchResult {
	result := BatchResult{
		TS:             time.Now().UTC().Format(time.RFC3339Nano),
		System:         system,
		RunID:          runID,
		Phase:          phase,
		BatchID:        batchID,
		Rows:           rows,
		LogicalBytes:   logicalBytes,
		BatchLatencyMS: float64(latency.Microseconds()) / 1000.0,
		RetryCount:     retries,
	}
	if err != nil {
		result.Error = err.Error()
	}
	return result
}

func NewSummary(system, runID, phase string, rows, logicalBytes, errors int64, elapsed time.Duration) Summary {
	summary := Summary{
		System:       system,
		RunID:        runID,
		Phase:        phase,
		Rows:         rows,
		LogicalBytes: logicalBytes,
		ElapsedSecs:  elapsed.Seconds(),
		Errors:       errors,
	}
	if elapsed > 0 {
		summary.RowsPerSec = float64(rows) / elapsed.Seconds()
		summary.MBPerSec = float64(logicalBytes) / 1_000_000.0 / elapsed.Seconds()
	}
	return summary
}
