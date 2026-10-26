// schedbench writes a privacy-safe, synthetic scheduler evaluation as JSON.
package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"os"

	"github.com/maistr0/maistr0/internal/scheduler/evaluation"
)

func main() {
	if err := run(); err != nil {
		fmt.Fprintf(os.Stderr, "schedbench: %v\n", err)
		os.Exit(1)
	}
}

func run() error {
	seed := flag.Int64("seed", 17, "synthetic trace seed")
	requests := flag.Int("requests", 256, "requests per scenario (1-100000)")
	iterations := flag.Int("iterations", 100000, "minimum decisions per CPU sample (batches grow to at least 100ms)")
	repetitions := flag.Int("repetitions", 5, "independent measurement samples per policy and scenario")
	output := flag.String("output", "", "JSON output path (empty writes stdout)")
	flag.Parse()
	scenarios, err := evaluation.Scenarios(*seed, *requests)
	if err != nil {
		return err
	}
	report, err := evaluation.Measure(scenarios, *iterations, *repetitions)
	if err != nil {
		return err
	}
	var writer io.Writer = os.Stdout
	if *output != "" {
		file, err := os.Create(*output)
		if err != nil {
			return fmt.Errorf("create evaluation output: %w", err)
		}
		encoder := json.NewEncoder(file)
		encoder.SetIndent("", "  ")
		encodeErr := encoder.Encode(report)
		closeErr := file.Close()
		if encodeErr != nil {
			return fmt.Errorf("write evaluation output: %w", encodeErr)
		}
		if closeErr != nil {
			return fmt.Errorf("close evaluation output: %w", closeErr)
		}
		return nil
	}
	encoder := json.NewEncoder(writer)
	encoder.SetIndent("", "  ")
	if err := encoder.Encode(report); err != nil {
		return fmt.Errorf("write evaluation output: %w", err)
	}
	return nil
}
