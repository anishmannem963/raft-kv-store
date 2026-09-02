package raft

import (
	"context"
	"sync"
	"testing"
)

type fakeTransport struct {
	vote func(RequestVoteRequest) RequestVoteResponse
	append func(AppendEntriesRequest) AppendEntriesResponse
}
func (f fakeTransport) RequestVote(_ context.Context,_ string,r RequestVoteRequest)(RequestVoteResponse,error){ return f.vote(r),nil }
func (f fakeTransport) AppendEntries(_ context.Context,_ string,r AppendEntriesRequest)(AppendEntriesResponse,error){ return f.append(r),nil }

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
