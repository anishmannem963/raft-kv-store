package main

import "testing"

func TestSummarizeFaultMatrix(t *testing.T) {
	results := []scenarioResult{
		{Scenario: "leader-1", Type: "leader_failure", Passed: true, DurationMS: 500, ElectionMS: 400},
		{Scenario: "leader-2", Type: "leader_failure", Passed: false, DurationMS: 700, ElectionMS: 600},
		{Scenario: "restart-1", Type: "container_restart", Passed: true, DurationMS: 200},
	}
	summary := summarize(results)
	if summary.TotalScenarios != 3 || summary.Passed != 2 || summary.Failed != 1 { t.Fatalf("unexpected total: %+v", summary) }
	if len(summary.ByType) != 2 || summary.ByType[0].Type != "container_restart" || summary.ByType[1].Type != "leader_failure" { t.Fatalf("groups not sorted: %+v", summary.ByType) }
	leader := summary.ByType[1]
	if leader.MeanDurationMS != 600 || leader.MeanElectionMS != 500 || leader.MaxElectionMS != 600 { t.Fatalf("unexpected leader summary: %+v", leader) }
}
