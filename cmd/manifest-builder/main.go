package main

import (
	"flag"
	"fmt"
	"os"

	"github.com/kolafish/ingest_aws_-blockchain/internal/ethbench"
)

func main() {
	var (
		inputDir   = flag.String("input-dir", "", "Directory containing local parquet files.")
		output     = flag.String("output", "bench/manifests/eth_transactions_10gb.json", "Manifest output path.")
		name       = flag.String("name", "eth_transactions_10gb", "Manifest name.")
		dataset    = flag.String("dataset", "eth_transactions", "Dataset name.")
		targetSize = flag.String("target-size", "10GB", "Target local parquet bytes, for example 10GB or 10GiB. Use 0 to include all files.")
		recursive  = flag.Bool("recursive", true, "Search input directory recursively.")
	)
	flag.Parse()

	if *inputDir == "" {
		fmt.Fprintln(os.Stderr, "--input-dir is required")
		os.Exit(2)
	}
	targetBytes, err := ethbench.ParseByteSize(*targetSize)
	if err != nil {
		fmt.Fprintf(os.Stderr, "parse --target-size: %v\n", err)
		os.Exit(2)
	}
	manifest, err := ethbench.BuildLocalManifest(*name, *dataset, *inputDir, *recursive, targetBytes)
	if err != nil {
		fmt.Fprintf(os.Stderr, "build manifest: %v\n", err)
		os.Exit(1)
	}
	if err := os.MkdirAll(dirName(*output), 0o755); err != nil {
		fmt.Fprintf(os.Stderr, "create output dir: %v\n", err)
		os.Exit(1)
	}
	if err := ethbench.SaveManifest(*output, manifest); err != nil {
		fmt.Fprintf(os.Stderr, "write manifest: %v\n", err)
		os.Exit(1)
	}
	fmt.Printf("wrote %s entries=%d bytes=%d\n", *output, len(manifest.Entries), manifest.TotalSizeBytes())
}

func dirName(path string) string {
	for i := len(path) - 1; i >= 0; i-- {
		if path[i] == '/' {
			if i == 0 {
				return "/"
			}
			return path[:i]
		}
	}
	return "."
}
