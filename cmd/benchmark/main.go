package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"net/http"
	"os"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

type status struct {
	ID          string `json:"id"`
	State       string `json:"state"`
	StateHash   string `json:"state_hash"`
	CommitIndex int    `json:"commit_index"`
	KeyCount    int    `json:"key_count"`
}

type latencyResult struct {
	Min float64 `json:"min_ms"`
	P50 float64 `json:"p50_ms"`
	P95 float64 `json:"p95_ms"`
	P99 float64 `json:"p99_ms"`
	Max float64 `json:"max_ms"`
}

type result struct {
	DurationMS  float64       `json:"duration_ms"`
	Requests    int           `json:"requests"`
	Concurrency int           `json:"concurrency"`
	Successful  int           `json:"successful"`
	Failed      int           `json:"failed"`
	Throughput  float64       `json:"throughput_ops_per_sec"`
	Latency     latencyResult `json:"latency_ms"`
	CommitIndex int           `json:"commit_index"`
	KeyCount    int           `json:"key_count"`
	StateHash   string        `json:"state_hash"`
	Converged   bool          `json:"converged"`
}

func main() {
	endpointsFlag := flag.String("endpoints", "http://localhost:8081,http://localhost:8082,http://localhost:8083", "comma-separated node URLs")
	requests := flag.Int("requests", 10000, "number of writes")
	concurrency := flag.Int("concurrency", 16, "parallel workers")
	timeout := flag.Duration("timeout", 2*time.Minute, "workload timeout")
	flag.Parse()
	if *requests < 1 || *concurrency < 1 {
		fail("requests and concurrency must be positive")
	}

	endpoints := strings.Split(*endpointsFlag, ",")
	client := &http.Client{Timeout: 5 * time.Second, CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }}
	ctx, cancel := context.WithTimeout(context.Background(), *timeout)
	defer cancel()
	baseline, err := waitConvergence(ctx, client, endpoints, 0)
	if err != nil {
		fail("cluster was not converged before the workload: " + err.Error())
	}
	runID := fmt.Sprintf("run-%d", time.Now().UnixNano())
	latencies := make([]time.Duration, *requests)
	var next, successful atomic.Int64
	var wg sync.WaitGroup
	started := time.Now()
	for worker := 0; worker < *concurrency; worker++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				index := int(next.Add(1) - 1)
				if index >= *requests {
					return
				}
				began := time.Now()
				if write(ctx, client, endpoints, runID, index) == nil {
					successful.Add(1)
				}
				latencies[index] = time.Since(began)
			}
		}()
	}
	wg.Wait()

	duration := time.Since(started)
	success := int(successful.Load())
	if success != *requests {
		fail(fmt.Sprintf("only %d/%d writes committed", success, *requests))
	}
	statuses, err := waitConvergence(ctx, client, endpoints, baseline[0].KeyCount+*requests)
	if err != nil {
		fail(err.Error())
	}
	sort.Slice(latencies, func(i, j int) bool { return latencies[i] < latencies[j] })
	out := result{
		DurationMS: durationMS(duration), Requests: *requests, Concurrency: *concurrency,
		Successful: success, Failed: *requests - success, Throughput: float64(success) / duration.Seconds(),
		CommitIndex: statuses[0].CommitIndex, KeyCount: statuses[0].KeyCount,
		StateHash: statuses[0].StateHash, Converged: true,
	}
	out.Latency = latencyResult{Min: durationMS(latencies[0]), P50: percentile(latencies, .50), P95: percentile(latencies, .95), P99: percentile(latencies, .99), Max: durationMS(latencies[len(latencies)-1])}
	encoder := json.NewEncoder(os.Stdout)
	encoder.SetIndent("", "  ")
	if err := encoder.Encode(out); err != nil {
		fail(err.Error())
	}
}

func write(ctx context.Context, client *http.Client, endpoints []string, runID string, index int) error {
	body := []byte(fmt.Sprintf(`{"value":"value-%d"}`, index))
	key := fmt.Sprintf("bench-%s-%08d", runID, index)
	for attempt := 0; attempt < 20; attempt++ {
		for _, endpoint := range endpoints {
			req, err := http.NewRequestWithContext(ctx, http.MethodPut, endpoint+"/kv/"+key, bytes.NewReader(body))
			if err != nil {
				return err
			}
			req.Header.Set("Content-Type", "application/json")
			req.Header.Set("X-Client-ID", "benchmark-"+runID)
			req.Header.Set("X-Request-ID", fmt.Sprintf("write-%d", index))
			resp, err := client.Do(req)
			if err != nil {
				continue
			}
			_, _ = io.Copy(io.Discard, resp.Body)
			resp.Body.Close()
			if resp.StatusCode == http.StatusCreated {
				return nil
			}
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(25 * time.Millisecond):
		}
	}
	return errors.New("write retries exhausted")
}

func waitConvergence(ctx context.Context, client *http.Client, endpoints []string, requests int) ([]status, error) {
	ticker := time.NewTicker(50 * time.Millisecond)
	defer ticker.Stop()
	for {
		statuses := make([]status, 0, len(endpoints))
		valid := true
		for _, endpoint := range endpoints {
			resp, err := client.Get(endpoint + "/status")
			if err != nil {
				valid = false
				break
			}
			var item status
			err = json.NewDecoder(resp.Body).Decode(&item)
			resp.Body.Close()
			if err != nil || resp.StatusCode != http.StatusOK {
				valid = false
				break
			}
			statuses = append(statuses, item)
		}
		if valid && statusesConverged(statuses, requests) {
			return statuses, nil
		}
		select {
		case <-ctx.Done():
			return nil, fmt.Errorf("cluster did not converge: %w", ctx.Err())
		case <-ticker.C:
		}
	}
}

func statusesConverged(statuses []status, minimumKeys int) bool {
	if len(statuses) == 0 {
		return false
	}
	first := statuses[0]
	if first.KeyCount < minimumKeys || first.StateHash == "" {
		return false
	}
	leaders := 0
	for _, item := range statuses {
		if item.State == "leader" {
			leaders++
		}
		if item.CommitIndex != first.CommitIndex || item.KeyCount != first.KeyCount || item.StateHash != first.StateHash {
			return false
		}
	}
	return leaders == 1
}

func percentile(values []time.Duration, p float64) float64 {
	index := int(float64(len(values)-1) * p)
	return durationMS(values[index])
}

func durationMS(value time.Duration) float64 { return float64(value.Microseconds()) / 1000 }

func fail(message string) {
	fmt.Fprintln(os.Stderr, "benchmark:", message)
	os.Exit(1)
}
