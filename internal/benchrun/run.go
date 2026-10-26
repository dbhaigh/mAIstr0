// Package benchrun measures bounded OpenAI-compatible HTTP workloads without
// recording prompts, response content, credentials, or endpoint URLs.
package benchrun

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math"
	"net/http"
	"net/url"
	"os"
	"runtime"
	"runtime/debug"
	"sort"
	"strings"
	"sync"
	"time"

	buildversion "github.com/maistr0/maistr0/internal/version"
)

const maxResponseBytes = 8 << 20

type Target struct {
	Name           string   `json:"name"`
	URL            string   `json:"url"`
	Nodes          int      `json:"nodes"`
	Engine         string   `json:"engine"`
	Hardware       []string `json:"hardware"`
	Settings       string   `json:"settings"`
	BearerTokenEnv string   `json:"bearer_token_env,omitempty"`
	Token          string   `json:"-"`
}

type Workload struct {
	Name          string `json:"name"`
	Model         string `json:"model"`
	ModelRevision string `json:"model_revision"`
	PromptWords   int    `json:"prompt_words"`
	OutputTokens  int    `json:"output_tokens"`
	Concurrency   int    `json:"concurrency"`
	Requests      int    `json:"requests"`
	Warmup        int    `json:"warmup"`
	Stream        bool   `json:"stream"`
	LoadState     string `json:"load_state"`
}

type Config struct {
	TimeoutMS int        `json:"timeout_ms"`
	Targets   []Target   `json:"targets"`
	Workloads []Workload `json:"workloads"`
}

type Environment struct {
	At         time.Time `json:"at"`
	OS         string    `json:"os"`
	Arch       string    `json:"arch"`
	Go         string    `json:"go"`
	Version    string    `json:"version"`
	Revision   string    `json:"revision"`
	Dirty      *bool     `json:"dirty"`
	CPUs       int       `json:"logical_cpus"`
	GOMAXPROCS int       `json:"gomaxprocs"`
}

type Sample struct {
	Index        int      `json:"index"`
	Status       int      `json:"http_status"`
	Outcome      string   `json:"outcome"`
	ElapsedMS    float64  `json:"elapsed_ms"`
	FirstByteMS  *float64 `json:"first_byte_ms"`
	FirstTokenMS *float64 `json:"first_token_ms"`
	Bytes        int64    `json:"response_bytes"`
}

type Distribution struct {
	Samples int      `json:"samples"`
	MeanMS  *float64 `json:"mean_ms"`
	P50MS   *float64 `json:"p50_ms"`
	P95MS   *float64 `json:"p95_ms"`
	P99MS   *float64 `json:"p99_ms"`
}

type Result struct {
	Target            string              `json:"target"`
	Nodes             int                 `json:"nodes"`
	Engine            string              `json:"engine"`
	Hardware          []string            `json:"hardware"`
	Settings          string              `json:"settings"`
	Workload          Workload            `json:"workload"`
	DurationMS        float64             `json:"duration_ms"`
	Completed         int                 `json:"completed"`
	Errors            int                 `json:"errors"`
	CompletedPerSec   *float64            `json:"completed_per_second"`
	Completion        Distribution        `json:"completion"`
	FirstToken        Distribution        `json:"first_token"`
	ProcessAllocBytes uint64              `json:"process_alloc_bytes"`
	ProcessMallocs    uint64              `json:"process_mallocs"`
	Samples           []Sample            `json:"samples"`
	Server            *ServerObservations `json:"server_observations"`
}

type NodeObservation struct {
	Name             string  `json:"name"`
	Capacity         int     `json:"capacity"`
	PeakActive       int64   `json:"peak_active"`
	Requests         int64   `json:"requests"`
	BusySlotFraction float64 `json:"busy_slot_fraction"`
}

type ServerObservations struct {
	Scope           string            `json:"scope"`
	QueueWait       Distribution      `json:"queue_wait"`
	DispatchSetup   Distribution      `json:"dispatch_setup"`
	Service         Distribution      `json:"engine_service"`
	SchedulerWallNS *uint64           `json:"scheduler_wall_ns"`
	Assignments     *uint64           `json:"assignments"`
	Retries         *uint64           `json:"retries"`
	RecoveryMS      *float64          `json:"recovery_ms"`
	Nodes           []NodeObservation `json:"nodes"`
}

type Report struct {
	SchemaVersion int         `json:"schema_version"`
	Environment   Environment `json:"environment"`
	TimeoutMS     int         `json:"timeout_ms"`
	Scope         string      `json:"scope"`
	Results       []Result    `json:"results"`
	Unavailable   []string    `json:"unavailable_metrics"`
}

func NewReport(timeoutMS int) Report {
	env := Environment{At: time.Now().UTC(), OS: runtime.GOOS, Arch: runtime.GOARCH,
		Go: runtime.Version(), Version: buildversion.Value, CPUs: runtime.NumCPU(), GOMAXPROCS: runtime.GOMAXPROCS(0)}
	if info, ok := debug.ReadBuildInfo(); ok {
		for _, setting := range info.Settings {
			switch setting.Key {
			case "vcs.revision":
				env.Revision = setting.Value
			case "vcs.modified":
				dirty := setting.Value == "true"
				env.Dirty = &dirty
			}
		}
	}
	if revision := os.Getenv("MAISTR0_BENCH_REVISION"); env.Revision == "" && revision != "" {
		env.Revision = revision
		if dirty := os.Getenv("MAISTR0_BENCH_DIRTY"); dirty == "true" || dirty == "false" {
			value := dirty == "true"
			env.Dirty = &value
		}
	}
	return Report{SchemaVersion: 1, Environment: env, TimeoutMS: timeoutMS,
		Scope: "HTTP client wall time; allocation deltas are whole-process, not scheduler or engine allocations",
		Unavailable: []string{"hardware CPU/GPU utilization and peak RSS require external sampling",
			"token throughput and exact input tokens require engine/tokenizer instrumentation",
			"server_observations is null unless the fixture supplies scoped server-side instrumentation",
			"scheduler CPU/allocations and memory/sync costs are separate component/profile workloads"}}
}

func (c Config) Validate() error {
	if c.TimeoutMS < 1 || c.TimeoutMS > 600000 || len(c.Targets) < 1 || len(c.Targets) > 8 ||
		len(c.Workloads) < 1 || len(c.Workloads) > 128 {
		return errors.New("benchmark requires 1..8 targets, 1..128 workloads, timeout_ms 1..600000")
	}
	names := make(map[string]bool)
	for i, target := range c.Targets {
		u, err := url.Parse(target.URL)
		if err != nil || u.Host == "" || (u.Scheme != "http" && u.Scheme != "https") || u.User != nil ||
			u.RawQuery != "" || u.Fragment != "" || target.Name == "" || names[target.Name] ||
			target.Nodes < 1 || target.Engine == "" || len(target.Hardware) == 0 || target.Settings == "" {
			return fmt.Errorf("target %d requires a unique name, HTTP(S) URL without credentials/query, nodes, engine, hardware and settings", i)
		}
		names[target.Name] = true
	}
	names = make(map[string]bool)
	requests := 0
	for i, w := range c.Workloads {
		if w.Name == "" || names[w.Name] || w.Model == "" || w.ModelRevision == "" ||
			w.PromptWords < 1 || w.PromptWords > 65536 || w.OutputTokens < 1 || w.OutputTokens > 65536 ||
			w.Concurrency < 1 || w.Concurrency > 128 || w.Requests < 1 || w.Requests > 10000 ||
			w.Warmup < 0 || w.Warmup > 100 ||
			(w.LoadState != "warm" && w.LoadState != "uncontrolled" && w.LoadState != "operator-cold") ||
			(w.LoadState == "operator-cold" && (w.Warmup != 0 || w.Concurrency != 1 || w.Requests != 1)) {
			return fmt.Errorf("workload %d has invalid metadata or bounds; operator-cold requires one request, concurrency one and no warmup", i)
		}
		if w.LoadState == "operator-cold" && (len(c.Targets) != 1 || len(c.Workloads) != 1) {
			return errors.New("operator-cold requires one target and one workload; reset the engine independently before each run")
		}
		names[w.Name] = true
		requests += w.Requests + w.Warmup
	}
	if requests*len(c.Targets) > 100000 {
		return errors.New("benchmark exceeds 100000 total requests including warmup")
	}
	return nil
}

func Run(ctx context.Context, client *http.Client, cfg Config) (Report, error) {
	if err := cfg.Validate(); err != nil {
		return Report{}, err
	}
	report := NewReport(cfg.TimeoutMS)
	for _, workload := range cfg.Workloads {
		for _, target := range cfg.Targets {
			result, err := RunWorkload(ctx, client, target, workload, time.Duration(cfg.TimeoutMS)*time.Millisecond)
			if err != nil {
				return report, err
			}
			report.Results = append(report.Results, result)
		}
	}
	return report, nil
}

// RunWorkload allows test fixtures to reset synthetic cold/failure state between
// cases. Callers must validate configuration before invoking it directly.
func RunWorkload(ctx context.Context, client *http.Client, target Target, workload Workload, timeout time.Duration) (Result, error) {
	body, err := json.Marshal(map[string]any{
		"model": workload.Model, "stream": workload.Stream, "temperature": 0, "max_tokens": workload.OutputTokens,
		"messages": []map[string]string{{"role": "user", "content": strings.Repeat("benchmark ", workload.PromptWords)}},
	})
	if err != nil {
		return Result{}, fmt.Errorf("encode benchmark request: %w", err)
	}
	for range workload.Warmup {
		sample := request(ctx, client, target, body, workload.Stream, timeout)
		if sample.Outcome != "completed" {
			return Result{}, fmt.Errorf("warmup failed for target %q: %s (HTTP %d)", target.Name, sample.Outcome, sample.Status)
		}
	}
	result := Result{Target: target.Name, Nodes: target.Nodes, Engine: target.Engine, Hardware: target.Hardware,
		Settings: target.Settings, Workload: workload, Samples: make([]Sample, workload.Requests)}
	var before, after runtime.MemStats
	runtime.ReadMemStats(&before)
	started := time.Now()
	jobs := make(chan int)
	var workers sync.WaitGroup
	for range min(workload.Concurrency, workload.Requests) {
		workers.Add(1)
		go func() {
			defer workers.Done()
			for index := range jobs {
				sample := request(ctx, client, target, body, workload.Stream, timeout)
				sample.Index = index
				result.Samples[index] = sample
			}
		}()
	}
	for index := range workload.Requests {
		jobs <- index
	}
	close(jobs)
	workers.Wait()
	elapsed := time.Since(started)
	runtime.ReadMemStats(&after)
	result.DurationMS = milliseconds(elapsed)
	result.ProcessAllocBytes, result.ProcessMallocs = after.TotalAlloc-before.TotalAlloc, after.Mallocs-before.Mallocs
	var completion, firstTokens []float64
	for _, sample := range result.Samples {
		if sample.Outcome != "completed" {
			result.Errors++
			continue
		}
		result.Completed++
		completion = append(completion, sample.ElapsedMS)
		if sample.FirstTokenMS != nil {
			firstTokens = append(firstTokens, *sample.FirstTokenMS)
		}
	}
	if elapsed > 0 {
		rate := float64(result.Completed) / elapsed.Seconds()
		result.CompletedPerSec = &rate
	}
	result.Completion, result.FirstToken = Summarize(completion), Summarize(firstTokens)
	return result, nil
}

func request(parent context.Context, client *http.Client, target Target, body []byte, stream bool, timeout time.Duration) (sample Sample) {
	start := time.Now()
	defer func() { sample.ElapsedMS = milliseconds(time.Since(start)) }()
	ctx, cancel := context.WithTimeout(parent, timeout)
	defer cancel()
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, target.URL, bytes.NewReader(body))
	if err != nil {
		sample.Outcome = "invalid_request"
		return sample
	}
	req.Header.Set("Content-Type", "application/json")
	if target.Token != "" {
		req.Header.Set("Authorization", "Bearer "+target.Token)
	}
	response, err := client.Do(req)
	if err != nil {
		sample.Outcome = requestFailure(ctx)
		return sample
	}
	defer func() {
		if err := response.Body.Close(); err != nil && sample.Outcome == "completed" {
			sample.Outcome = "response_close_error"
		}
	}()
	sample.Status = response.StatusCode
	if response.StatusCode < 200 || response.StatusCode >= 300 {
		sample.Outcome = "http_error"
		return sample
	}
	reader := &measuredReader{reader: io.LimitReader(response.Body, maxResponseBytes+1), start: start}
	var valid bool
	if stream {
		valid, sample.FirstTokenMS = readStream(reader, start)
	} else {
		var payload struct {
			Choices []struct {
				Message struct {
					Content string `json:"content"`
				} `json:"message"`
			} `json:"choices"`
		}
		data, readErr := io.ReadAll(reader)
		valid = readErr == nil && json.Unmarshal(data, &payload) == nil &&
			len(payload.Choices) > 0 && payload.Choices[0].Message.Content != ""
	}
	sample.FirstByteMS, sample.Bytes = reader.first, reader.bytes
	sample.Outcome = "completed"
	if ctx.Err() != nil {
		sample.Outcome = requestFailure(ctx)
	} else if !valid || sample.Bytes > maxResponseBytes {
		sample.Outcome = "invalid_or_truncated_response"
	}
	return sample
}

func requestFailure(ctx context.Context) string {
	if errors.Is(ctx.Err(), context.Canceled) {
		return "cancelled"
	}
	if errors.Is(ctx.Err(), context.DeadlineExceeded) {
		return "timeout"
	}
	return "transport_error"
}

type measuredReader struct {
	reader io.Reader
	start  time.Time
	first  *float64
	bytes  int64
}

func (r *measuredReader) Read(p []byte) (int, error) {
	n, err := r.reader.Read(p)
	if n > 0 && r.first == nil {
		value := milliseconds(time.Since(r.start))
		r.first = &value
	}
	r.bytes += int64(n)
	return n, err
}

func readStream(reader io.Reader, started time.Time) (bool, *float64) {
	scanner := bufio.NewScanner(reader)
	scanner.Buffer(make([]byte, 4096), maxResponseBytes)
	var data strings.Builder
	var first *float64
	content := false
	validFrames := true
	parse := func() bool {
		frame := strings.TrimSpace(data.String())
		data.Reset()
		if frame == "[DONE]" {
			return true
		}
		if frame == "" {
			return false
		}
		var chunk struct {
			Error   json.RawMessage `json:"error"`
			Choices []struct {
				Delta struct {
					Content string `json:"content"`
				} `json:"delta"`
			} `json:"choices"`
		}
		if json.Unmarshal([]byte(frame), &chunk) != nil || (len(chunk.Error) > 0 && string(chunk.Error) != "null") {
			validFrames = false
		} else {
			for _, choice := range chunk.Choices {
				if choice.Delta.Content != "" {
					content = true
					if first == nil {
						value := milliseconds(time.Since(started))
						first = &value
					}
				}
			}
		}
		return false
	}
	for scanner.Scan() {
		line := scanner.Text()
		if line == "" {
			if parse() {
				return content && validFrames, first
			}
		} else if strings.HasPrefix(line, "data:") {
			data.WriteString(strings.TrimPrefix(strings.TrimPrefix(line, "data:"), " "))
			data.WriteByte('\n')
		}
	}
	return false, first
}

func milliseconds(d time.Duration) float64 { return float64(d) / float64(time.Millisecond) }

// Quantiles are nearest-rank and suppressed below conservative sample counts.
func Summarize(values []float64) Distribution {
	result := Distribution{Samples: len(values)}
	if len(values) == 0 {
		return result
	}
	sorted := append([]float64(nil), values...)
	sort.Float64s(sorted)
	mean := 0.0
	for _, value := range sorted {
		mean += value / float64(len(sorted))
	}
	result.MeanMS = &mean
	quantile := func(p float64, minimum int) *float64 {
		if len(sorted) < minimum {
			return nil
		}
		value := sorted[int(math.Ceil(float64(len(sorted))*p))-1]
		return &value
	}
	result.P50MS, result.P95MS, result.P99MS = quantile(.5, 20), quantile(.95, 100), quantile(.99, 1000)
	return result
}
