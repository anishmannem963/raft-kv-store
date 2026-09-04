package main

import (
	"encoding/json"
	"fmt"
	"os"
	"sort"
)

type latencyResult struct {
	P50 float64 `json:"p50_ms"`
	P95 float64 `json:"p95_ms"`
	P99 float64 `json:"p99_ms"`
}

type benchmarkResult struct {
	Requests   int           `json:"requests"`
	Concurrency int          `json:"concurrency"`
	Successful int           `json:"successful"`
	Failed     int           `json:"failed"`
	Throughput float64       `json:"throughput_ops_per_sec"`
	Latency    latencyResult `json:"latency_ms"`
	Converged  bool          `json:"converged"`
}

type concurrencySummary struct {
	Concurrency       int     `json:"concurrency"`
	Runs              int     `json:"runs"`
	RequestsPerRun    int     `json:"requests_per_run"`
	TotalRequests     int     `json:"total_requests"`
	TotalSuccessful   int     `json:"total_successful"`
	MeanThroughput    float64 `json:"mean_throughput_ops_per_sec"`
	MedianThroughput  float64 `json:"median_throughput_ops_per_sec"`
	MinThroughput     float64 `json:"min_throughput_ops_per_sec"`
	MaxThroughput     float64 `json:"max_throughput_ops_per_sec"`
	MeanP50LatencyMS  float64 `json:"mean_p50_latency_ms"`
	MeanP95LatencyMS  float64 `json:"mean_p95_latency_ms"`
	MeanP99LatencyMS  float64 `json:"mean_p99_latency_ms"`
	MaxP99LatencyMS   float64 `json:"max_p99_latency_ms"`
	AllRunsConverged  bool    `json:"all_runs_converged"`
}

type suiteSummary struct {
	TotalRuns         int                  `json:"total_runs"`
	TotalRequests     int                  `json:"total_requests"`
	TotalSuccessful   int                  `json:"total_successful"`
	TotalFailed       int                  `json:"total_failed"`
	AllRunsConverged  bool                 `json:"all_runs_converged"`
	ByConcurrency     []concurrencySummary `json:"by_concurrency"`
}

func main() {
	if len(os.Args) < 2 {
		fail("provide at least one benchmark JSON file")
	}
	results := make([]benchmarkResult, 0, len(os.Args)-1)
	for _, path := range os.Args[1:] {
		file, err := os.Open(path)
		if err != nil { fail(err.Error()) }
		var result benchmarkResult
		err = json.NewDecoder(file).Decode(&result)
		file.Close()
		if err != nil { fail(fmt.Sprintf("decode %s: %v", path, err)) }
		results = append(results, result)
	}
	summary := summarize(results)
	encoder := json.NewEncoder(os.Stdout)
	encoder.SetIndent("", "  ")
	if err := encoder.Encode(summary); err != nil { fail(err.Error()) }
}

func summarize(results []benchmarkResult) suiteSummary {
	groups := map[int][]benchmarkResult{}
	summary := suiteSummary{AllRunsConverged: true}
	for _, result := range results {
		groups[result.Concurrency] = append(groups[result.Concurrency], result)
		summary.TotalRuns++
		summary.TotalRequests += result.Requests
		summary.TotalSuccessful += result.Successful
		summary.TotalFailed += result.Failed
		summary.AllRunsConverged = summary.AllRunsConverged && result.Converged
	}
	levels := make([]int, 0, len(groups))
	for concurrency := range groups { levels = append(levels, concurrency) }
	sort.Ints(levels)
	for _, concurrency := range levels {
		runs := groups[concurrency]
		item := concurrencySummary{Concurrency: concurrency, Runs: len(runs), RequestsPerRun: runs[0].Requests, AllRunsConverged: true}
		throughputs := make([]float64, 0, len(runs))
		for _, run := range runs {
			item.TotalRequests += run.Requests
			item.TotalSuccessful += run.Successful
			item.MeanThroughput += run.Throughput
			item.MeanP50LatencyMS += run.Latency.P50
			item.MeanP95LatencyMS += run.Latency.P95
			item.MeanP99LatencyMS += run.Latency.P99
			item.AllRunsConverged = item.AllRunsConverged && run.Converged
			if run.Latency.P99 > item.MaxP99LatencyMS { item.MaxP99LatencyMS = run.Latency.P99 }
			throughputs = append(throughputs, run.Throughput)
		}
		divisor := float64(len(runs))
		item.MeanThroughput /= divisor
		item.MeanP50LatencyMS /= divisor
		item.MeanP95LatencyMS /= divisor
		item.MeanP99LatencyMS /= divisor
		sort.Float64s(throughputs)
		item.MinThroughput = throughputs[0]
		item.MaxThroughput = throughputs[len(throughputs)-1]
		middle := len(throughputs)/2
		if len(throughputs)%2 == 0 { item.MedianThroughput = (throughputs[middle-1]+throughputs[middle])/2 } else { item.MedianThroughput = throughputs[middle] }
		summary.ByConcurrency = append(summary.ByConcurrency, item)
	}
	return summary
}

func fail(message string) {
	fmt.Fprintln(os.Stderr, "benchmark-summary:", message)
	os.Exit(1)
}
