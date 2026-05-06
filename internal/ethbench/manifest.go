package ethbench

import (
	"encoding/json"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"time"
)

type Manifest struct {
	Name        string          `json:"name"`
	Dataset     string          `json:"dataset"`
	Region      string          `json:"region,omitempty"`
	TargetBytes int64           `json:"target_bytes,omitempty"`
	CreatedAt   string          `json:"created_at"`
	Entries     []ManifestEntry `json:"entries"`
}

type ManifestEntry struct {
	Source        string `json:"source"`
	Path          string `json:"path,omitempty"`
	Bucket        string `json:"bucket,omitempty"`
	Key           string `json:"key,omitempty"`
	Date          string `json:"date"`
	SizeBytes     int64  `json:"size_bytes"`
	EstimatedRows int64  `json:"estimated_rows,omitempty"`
}

func LoadManifest(path string) (*Manifest, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	var manifest Manifest
	if err := json.Unmarshal(data, &manifest); err != nil {
		return nil, err
	}
	if len(manifest.Entries) == 0 {
		return nil, fmt.Errorf("manifest %s has no entries", path)
	}
	return &manifest, nil
}

func SaveManifest(path string, manifest *Manifest) error {
	data, err := json.MarshalIndent(manifest, "", "  ")
	if err != nil {
		return err
	}
	data = append(data, '\n')
	return os.WriteFile(path, data, 0o644)
}

func (m *Manifest) TotalSizeBytes() int64 {
	var total int64
	for _, entry := range m.Entries {
		total += entry.SizeBytes
	}
	return total
}

func (m *Manifest) ValidateLocal() error {
	for _, entry := range m.Entries {
		if entry.Source != "local" {
			return fmt.Errorf("entry source %q is not supported by local smoke writers", entry.Source)
		}
		if entry.Path == "" {
			return errors.New("local manifest entry has empty path")
		}
		if entry.Date == "" {
			return fmt.Errorf("local manifest entry %s has empty date", entry.Path)
		}
	}
	return nil
}

func BuildLocalManifest(name, dataset, root string, recursive bool, targetBytes int64) (*Manifest, error) {
	var files []ManifestEntry
	root, err := filepath.Abs(root)
	if err != nil {
		return nil, err
	}

	addFile := func(path string, info fs.FileInfo) error {
		if info.IsDir() || !strings.EqualFold(filepath.Ext(path), ".parquet") {
			return nil
		}
		date, err := InferDate(path)
		if err != nil {
			return err
		}
		files = append(files, ManifestEntry{
			Source:    "local",
			Path:      path,
			Date:      date,
			SizeBytes: info.Size(),
		})
		return nil
	}

	if recursive {
		err = filepath.WalkDir(root, func(path string, d fs.DirEntry, walkErr error) error {
			if walkErr != nil {
				return walkErr
			}
			info, err := d.Info()
			if err != nil {
				return err
			}
			return addFile(path, info)
		})
	} else {
		entries, readErr := os.ReadDir(root)
		if readErr != nil {
			return nil, readErr
		}
		for _, entry := range entries {
			info, err := entry.Info()
			if err != nil {
				return nil, err
			}
			if err := addFile(filepath.Join(root, entry.Name()), info); err != nil {
				return nil, err
			}
		}
	}
	if err != nil {
		return nil, err
	}

	sort.Slice(files, func(i, j int) bool {
		if files[i].Date == files[j].Date {
			return files[i].Path < files[j].Path
		}
		return files[i].Date < files[j].Date
	})

	if targetBytes > 0 {
		var selected []ManifestEntry
		var total int64
		for _, entry := range files {
			if total >= targetBytes {
				break
			}
			selected = append(selected, entry)
			total += entry.SizeBytes
		}
		files = selected
	}

	if len(files) == 0 {
		return nil, fmt.Errorf("no parquet files found under %s", root)
	}
	if name == "" {
		name = fmt.Sprintf("%s_%s", dataset, time.Now().UTC().Format("20060102T150405Z"))
	}
	return &Manifest{
		Name:        name,
		Dataset:     dataset,
		TargetBytes: targetBytes,
		CreatedAt:   time.Now().UTC().Format(time.RFC3339),
		Entries:     files,
	}, nil
}

var (
	datePrefixPattern = regexp.MustCompile(`(^|[/\\])(\d{4}-\d{2}-\d{2})__`)
	dateDirPattern    = regexp.MustCompile(`(^|[/\\])date=(\d{4}-\d{2}-\d{2})([/\\]|$)`)
)

func InferDate(path string) (string, error) {
	if match := datePrefixPattern.FindStringSubmatch(path); len(match) == 3 {
		return match[2], nil
	}
	if match := dateDirPattern.FindStringSubmatch(path); len(match) == 4 {
		return match[2], nil
	}
	return "", fmt.Errorf("cannot infer date from %s; expected YYYY-MM-DD__*.parquet or date=YYYY-MM-DD path", path)
}

func ParseByteSize(s string) (int64, error) {
	text := strings.TrimSpace(s)
	if text == "" || text == "0" {
		return 0, nil
	}
	units := []struct {
		suffix string
		value  int64
	}{
		{"tib", 1 << 40}, {"tb", 1_000_000_000_000},
		{"gib", 1 << 30}, {"gb", 1_000_000_000},
		{"mib", 1 << 20}, {"mb", 1_000_000},
		{"kib", 1 << 10}, {"kb", 1_000},
		{"b", 1},
	}
	lower := strings.ToLower(text)
	for _, unit := range units {
		if strings.HasSuffix(lower, unit.suffix) {
			number := strings.TrimSpace(text[:len(text)-len(unit.suffix)])
			value, err := strconv.ParseFloat(number, 64)
			if err != nil {
				return 0, err
			}
			return int64(value * float64(unit.value)), nil
		}
	}
	return strconv.ParseInt(text, 10, 64)
}
