package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"net/http"
	"os"
	"os/signal"

	"github.com/maistr0/maistr0/internal/benchrun"
)

func main() {
	configPath := flag.String("config", "", "benchmark JSON configuration (required)")
	outputPath := flag.String("output", "", "new JSON results file (required; mode 0600)")
	flag.Parse()
	if err := run(*configPath, *outputPath); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

func run(configPath, outputPath string) error {
	if configPath == "" || outputPath == "" {
		return errors.New("-config and -output are required")
	}
	input, err := os.Open(configPath)
	if err != nil {
		return fmt.Errorf("open benchmark configuration: %w", err)
	}
	limited := &io.LimitedReader{R: input, N: (1 << 20) + 1}
	decoder := json.NewDecoder(limited)
	decoder.DisallowUnknownFields()
	var cfg benchrun.Config
	decodeErr := decoder.Decode(&cfg)
	var extra any
	if decodeErr == nil && decoder.Decode(&extra) != io.EOF {
		decodeErr = errors.New("configuration must contain one JSON object")
	}
	if limited.N == 0 {
		decodeErr = errors.New("configuration exceeds 1 MiB")
	}
	if err := errors.Join(decodeErr, input.Close()); err != nil {
		return fmt.Errorf("read benchmark configuration: %w", err)
	}
	if err := cfg.Validate(); err != nil {
		return err
	}
	for i := range cfg.Targets {
		if name := cfg.Targets[i].BearerTokenEnv; name != "" {
			token, ok := os.LookupEnv(name)
			if !ok || token == "" {
				return fmt.Errorf("target %d bearer-token environment variable is empty or unset", i)
			}
			cfg.Targets[i].Token = token
		}
	}
	output, err := os.OpenFile(outputPath, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0600)
	if err != nil {
		return fmt.Errorf("create benchmark report: %w", err)
	}
	client := &http.Client{CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }}
	defer client.CloseIdleConnections()
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt)
	defer stop()
	report, runErr := benchrun.Run(ctx, client, cfg)
	writeErr := json.NewEncoder(output).Encode(report)
	if err := errors.Join(runErr, writeErr, output.Close()); err != nil {
		return fmt.Errorf("benchmark failed: %w", err)
	}
	for _, result := range report.Results {
		if result.Errors > 0 {
			return errors.New("benchmark recorded failed requests; inspect the report")
		}
	}
	return nil
}
