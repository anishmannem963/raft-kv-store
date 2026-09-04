package main

import (
	"bufio"
	"encoding/json"
	"fmt"
	"os"
	"sort"
)

type scenarioResult struct {
	Scenario    string `json:"scenario"`
	Type        string `json:"type"`
	Passed      bool   `json:"passed"`
	DurationMS  int64  `json:"duration_ms"`
	ElectionMS  int64  `json:"election_ms,omitempty"`
	HealingMS   int64  `json:"healing_ms,omitempty"`
}

type typeSummary struct {
	Type              string  `json:"type"`
	Scenarios         int     `json:"scenarios"`
	Passed            int     `json:"passed"`
	Failed            int     `json:"failed"`
	MeanDurationMS    float64 `json:"mean_duration_ms"`
	MaxDurationMS     int64   `json:"max_duration_ms"`
	MeanElectionMS    float64 `json:"mean_election_ms,omitempty"`
	MaxElectionMS     int64   `json:"max_election_ms,omitempty"`
	MeanHealingMS     float64 `json:"mean_healing_ms,omitempty"`
	MaxHealingMS      int64   `json:"max_healing_ms,omitempty"`
}

type matrixSummary struct {
	TotalScenarios int           `json:"total_scenarios"`
	Passed         int           `json:"passed"`
	Failed         int           `json:"failed"`
	SuccessRate    float64       `json:"success_rate_percent"`
	ByType         []typeSummary `json:"by_type"`
}

func main() {
	if len(os.Args) != 2 { fail("provide one NDJSON results file") }
	file, err := os.Open(os.Args[1])
	if err != nil { fail(err.Error()) }
	defer file.Close()
	var results []scenarioResult
	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		var result scenarioResult
		if err := json.Unmarshal(scanner.Bytes(), &result); err != nil { fail(err.Error()) }
		results = append(results, result)
	}
	if err := scanner.Err(); err != nil { fail(err.Error()) }
	if len(results) == 0 { fail("results file is empty") }
	summary := summarize(results)
	encoder := json.NewEncoder(os.Stdout)
	encoder.SetIndent("", "  ")
	if err := encoder.Encode(summary); err != nil { fail(err.Error()) }
}

func summarize(results []scenarioResult) matrixSummary {
	groups := map[string][]scenarioResult{}
	summary := matrixSummary{TotalScenarios: len(results)}
	for _, result := range results {
		groups[result.Type] = append(groups[result.Type], result)
		if result.Passed { summary.Passed++ } else { summary.Failed++ }
	}
	summary.SuccessRate = 100 * float64(summary.Passed) / float64(summary.TotalScenarios)
	types := make([]string, 0, len(groups))
	for kind := range groups { types = append(types, kind) }
	sort.Strings(types)
	for _, kind := range types {
		item := typeSummary{Type: kind, Scenarios: len(groups[kind])}
		var elections, healings int
		for _, result := range groups[kind] {
			if result.Passed { item.Passed++ } else { item.Failed++ }
			item.MeanDurationMS += float64(result.DurationMS)
			if result.DurationMS > item.MaxDurationMS { item.MaxDurationMS = result.DurationMS }
			if result.ElectionMS > 0 { item.MeanElectionMS += float64(result.ElectionMS); elections++; if result.ElectionMS > item.MaxElectionMS { item.MaxElectionMS = result.ElectionMS } }
			if result.HealingMS > 0 { item.MeanHealingMS += float64(result.HealingMS); healings++; if result.HealingMS > item.MaxHealingMS { item.MaxHealingMS = result.HealingMS } }
		}
		item.MeanDurationMS /= float64(item.Scenarios)
		if elections > 0 { item.MeanElectionMS /= float64(elections) }
		if healings > 0 { item.MeanHealingMS /= float64(healings) }
		summary.ByType = append(summary.ByType, item)
	}
	return summary
}

func fail(message string) {
	fmt.Fprintln(os.Stderr, "fault-summary:", message)
	os.Exit(1)
}
