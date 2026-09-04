package raft

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

type fakeTransport struct {
	vote func(RequestVoteRequest) RequestVoteResponse
	append func(AppendEntriesRequest) AppendEntriesResponse
	snapshot func(InstallSnapshotRequest) InstallSnapshotResponse
}
func (f fakeTransport) RequestVote(_ context.Context,_ string,r RequestVoteRequest)(RequestVoteResponse,error){ return f.vote(r),nil }
func (f fakeTransport) AppendEntries(_ context.Context,_ string,r AppendEntriesRequest)(AppendEntriesResponse,error){ return f.append(r),nil }
func (f fakeTransport) InstallSnapshot(_ context.Context,_ string,r InstallSnapshotRequest)(InstallSnapshotResponse,error){ if f.snapshot!=nil { return f.snapshot(r),nil }; return InstallSnapshotResponse{Term:r.Term,Success:true,MatchIndex:r.Snapshot.LastIncludedIndex},nil }

func TestFollowerRejectsStaleVote(t *testing.T){
	n:=NewNode("n1",map[string]string{"n1":"one"},fakeTransport{})
	n.term=3
	got:=n.HandleRequestVote(RequestVoteRequest{Term:2,CandidateID:"n2",LastLogIndex:-1})
	if got.VoteGranted || got.Term!=3 { t.Fatalf("unexpected response: %+v",got) }
}

func TestFollowerAppendsAndAppliesCommittedEntry(t *testing.T){
	n:=NewNode("n2",map[string]string{"n1":"one","n2":"two","n3":"three"},fakeTransport{})
	got:=n.HandleAppendEntries(AppendEntriesRequest{Term:1,LeaderID:"n1",PrevLogIndex:-1,Entries:[]LogEntry{{Term:1,Command:Command{Operation:"put",Key:"language",Value:"go"}}},LeaderCommit:0})
	if !got.Success { t.Fatal("expected append success") }
	if value,ok:=n.Get("language"); !ok || value!="go" { t.Fatalf("unexpected value %q, %v",value,ok) }
}

func TestLeaderCommitsWithMajority(t *testing.T){
	var followerLogMu sync.Mutex
	var followerLog []LogEntry
	f:=fakeTransport{
		vote:func(r RequestVoteRequest)RequestVoteResponse{return RequestVoteResponse{Term:r.Term,VoteGranted:true}},
		append:func(r AppendEntriesRequest)AppendEntriesResponse{ followerLogMu.Lock(); followerLog=append(followerLog,r.Entries...); followerLogMu.Unlock(); return AppendEntriesResponse{Term:r.Term,Success:true,MatchIndex:r.PrevLogIndex+len(r.Entries)} },
	}
	n:=NewNode("n1",map[string]string{"n1":"one","n2":"two","n3":"three"},f)
	n.state=Leader; n.term=1; n.leaderID="n1"; n.nextIndex["n2"]=0; n.nextIndex["n3"]=0
	if err:=n.Put("project","raft-kv-store"); err!=nil { t.Fatal(err) }
	if value,ok:=n.Get("project"); !ok || value!="raft-kv-store" { t.Fatalf("command not applied: %q",value) }
	followerLogMu.Lock()
	replicatedEntries := len(followerLog)
	followerLogMu.Unlock()
	if replicatedEntries<1 { t.Fatal("entry was not replicated") }
}

func TestConcurrentLeaderWritesCommit(t *testing.T) {
	transport := fakeTransport{append: func(r AppendEntriesRequest) AppendEntriesResponse {
		return AppendEntriesResponse{Term: r.Term, Success: true, MatchIndex: r.PrevLogIndex + len(r.Entries)}
	}}
	n := NewNode("n1", map[string]string{"n1":"one", "n2":"two", "n3":"three"}, transport)
	n.state = Leader
	n.term = 1
	n.leaderID = "n1"
	n.nextIndex["n2"] = 0
	n.nextIndex["n3"] = 0

	const writes = 50
	errors := make(chan error, writes)
	var wg sync.WaitGroup
	for index := 0; index < writes; index++ {
		wg.Add(1)
		go func(index int) {
			defer wg.Done()
			errors <- n.PutWithRequest("concurrent-test", string(rune(index)), "shared", "value")
		}(index)
	}
	wg.Wait()
	close(errors)
	for err := range errors {
		if err != nil { t.Fatalf("concurrent write failed: %v", err) }
	}
	if status := n.Status(); status.CommitIndex != writes-1 { t.Fatalf("only committed through index %d", status.CommitIndex) }
}

func TestLeaderIgnoresElectionTimeout(t *testing.T) {
	var voteRequests atomic.Int32
	transport := fakeTransport{
		vote: func(r RequestVoteRequest) RequestVoteResponse {
			voteRequests.Add(1)
			return RequestVoteResponse{Term: r.Term, VoteGranted: true}
		},
		append: func(r AppendEntriesRequest) AppendEntriesResponse {
			return AppendEntriesResponse{Term: r.Term, Success: true, MatchIndex: r.PrevLogIndex + len(r.Entries)}
		},
	}
	n := NewNode("n1", map[string]string{"n1":"one", "n2":"two", "n3":"three"}, transport)
	n.state = Leader
	n.term = 7
	n.leaderID = "n1"
	n.electionMin = 10 * time.Millisecond
	n.electionMax = 20 * time.Millisecond
	n.heartbeat = 5 * time.Millisecond
	n.Start()
	time.Sleep(60 * time.Millisecond)
	n.Stop()
	if status := n.Status(); status.State != Leader || status.Term != 7 {
		t.Fatalf("leader started a new election: state=%s term=%d", status.State, status.Term)
	}
	if got := voteRequests.Load(); got != 0 {
		t.Fatalf("leader sent %d vote requests after its election timer fired", got)
	}
}

type unavailableTransport struct{}

func (unavailableTransport) RequestVote(context.Context, string, RequestVoteRequest) (RequestVoteResponse, error) {
	return RequestVoteResponse{}, errors.New("peer unavailable")
}

func (unavailableTransport) AppendEntries(context.Context, string, AppendEntriesRequest) (AppendEntriesResponse, error) {
	return AppendEntriesResponse{}, errors.New("peer unavailable")
}

func (unavailableTransport) InstallSnapshot(context.Context, string, InstallSnapshotRequest) (InstallSnapshotResponse, error) {
	return InstallSnapshotResponse{}, errors.New("peer unavailable")
}

func TestLinearizableReadRequiresQuorum(t *testing.T) {
	n := NewNode("n1", map[string]string{"n1":"one","n2":"two","n3":"three"}, unavailableTransport{})
	n.state = Leader
	n.term = 1
	n.leaderID = "n1"
	n.store["key"] = "possibly-stale"
	if _, _, err := n.Read("key"); !errors.Is(err, ErrQuorumUnavailable) {
		t.Fatalf("expected quorum error, got %v", err)
	}
}

func TestLinearizableReadAfterQuorumConfirmation(t *testing.T) {
	transport := fakeTransport{
		append: func(r AppendEntriesRequest) AppendEntriesResponse {
			return AppendEntriesResponse{Term:r.Term, Success:true, MatchIndex:r.PrevLogIndex+len(r.Entries)}
		},
	}
	n := NewNode("n1", map[string]string{"n1":"one","n2":"two","n3":"three"}, transport)
	n.state = Leader
	n.term = 1
	n.leaderID = "n1"
	n.store["key"] = "current"
	value, ok, err := n.Read("key")
	if err != nil || !ok || value != "current" {
		t.Fatalf("unexpected read: value=%q ok=%v err=%v", value, ok, err)
	}
}

func TestDuplicateRequestIsAppliedOnce(t *testing.T) {
	transport := fakeTransport{
		append: func(r AppendEntriesRequest) AppendEntriesResponse {
			return AppendEntriesResponse{Term:r.Term, Success:true, MatchIndex:r.PrevLogIndex+len(r.Entries)}
		},
	}
	n := NewNode("n1", map[string]string{"n1":"one","n2":"two","n3":"three"}, transport)
	n.state = Leader
	n.term = 1
	n.leaderID = "n1"
	n.nextIndex["n2"] = 0
	n.nextIndex["n3"] = 0
	if err := n.PutWithRequest("client-a", "request-1", "key", "value"); err != nil { t.Fatal(err) }
	if err := n.PutWithRequest("client-a", "request-1", "key", "value"); err != nil { t.Fatal(err) }
	if len(n.log) != 1 {
		t.Fatalf("duplicate request appended %d log entries", len(n.log))
	}
	if err := n.PutWithRequest("client-a", "request-1", "key", "different"); !errors.Is(err, ErrRequestConflict) {
		t.Fatalf("expected request conflict, got %v", err)
	}
}
