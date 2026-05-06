package ethbench

import (
	"sync"
	"time"
)

type IngestWindow struct {
	mu    sync.Mutex
	start time.Time
	end   time.Time
}

func (w *IngestWindow) Mark(start, end time.Time) {
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.start.IsZero() || start.Before(w.start) {
		w.start = start
	}
	if w.end.IsZero() || end.After(w.end) {
		w.end = end
	}
}

func (w *IngestWindow) Elapsed(fallbackStart time.Time) time.Duration {
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.start.IsZero() || w.end.IsZero() {
		return time.Since(fallbackStart)
	}
	return w.end.Sub(w.start)
}
