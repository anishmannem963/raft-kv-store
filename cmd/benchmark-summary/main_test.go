package main

import "testing"

func TestSummarizeGroupsRunsByConcurrency(t *testing.T) {
	results := []benchmarkResult{
		{Requests: 100, Concurrency: 8, Successful: 100, Throughput: 300, Latency: latencyResult{P50: 10, P95: 20, P99: 30}, Converged: true},
		{Requests: 100, Concurrency: 1, Successful: 100, Throughput: 100, Latency: latencyResult{P50: 5, P95: 8, P99: 10}, Converged: true},
		{Requests: 100, Concurrency: 8, Successful: 99, Failed: 1, Throughput: 500, Latency: latencyResult{P50: 14, P95: 24, P99: 40}, Converged: false},
	}
	summary := summarize(results)
	if summary.TotalRuns != 3 || summary.TotalRequests != 300 || summary.TotalFailed != 1 || summary.AllRunsConverged { t.Fatalf("unexpected total: %+v", summary) }
	if len(summary.ByConcurrency) != 2 || summary.ByConcurrency[0].Concurrency != 1 || summary.ByConcurrency[1].Concurrency != 8 { t.Fatalf("groups not sorted: %+v", summary.ByConcurrency) }
	eight := summary.ByConcurrency[1]
	if eight.MeanThroughput != 400 || eight.MedianThroughput != 400 || eight.MeanP95LatencyMS != 22 || eight.MaxP99LatencyMS != 40 || eight.AllRunsConverged { t.Fatalf("unexpected concurrency summary: %+v", eight) }
}
