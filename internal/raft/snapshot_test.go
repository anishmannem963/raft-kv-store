package raft

import "testing"

func TestNodeCompactsCommittedLog(t *testing.T) {
	n := NewNode("n1", map[string]string{"n1":"one"}, fakeTransport{})
	n.state = Leader
	n.term = 1
	n.leaderID = "n1"
	n.SetSnapshotThreshold(2)

	if err := n.PutWithRequest("client", "1", "one", "first"); err != nil { t.Fatal(err) }
	if err := n.PutWithRequest("client", "2", "two", "second"); err != nil { t.Fatal(err) }
	if err := n.PutWithRequest("client", "3", "three", "third"); err != nil { t.Fatal(err) }

	status := n.Status()
	if status.SnapshotIndex != 1 || status.CommitIndex != 2 || status.LogLength != 1 {
		t.Fatalf("unexpected compacted state: %+v", status)
	}
	if value, ok := n.Get("one"); !ok || value != "first" { t.Fatalf("snapshot lost value: %q",value) }
}

func TestSnapshotSurvivesRestart(t *testing.T) {
	storage := NewFileStorage(t.TempDir())
	n, err := NewNodeWithStorage("n1",map[string]string{"n1":"one"},fakeTransport{},storage)
	if err != nil { t.Fatal(err) }
	n.state=Leader; n.term=1; n.leaderID="n1"; n.SetSnapshotThreshold(2)
	if err:=n.PutWithRequest("client","1","one","first"); err!=nil { t.Fatal(err) }
	if err:=n.PutWithRequest("client","2","two","second"); err!=nil { t.Fatal(err) }
	if err:=n.PutWithRequest("client","3","three","third"); err!=nil { t.Fatal(err) }

	restarted, err := NewNodeWithStorage("n1",map[string]string{"n1":"one"},fakeTransport{},storage)
	if err != nil { t.Fatal(err) }
	for key,want:=range map[string]string{"one":"first","two":"second","three":"third"} {
		if got,ok:=restarted.Get(key); !ok || got!=want { t.Fatalf("%s: got %q, want %q",key,got,want) }
	}
	restarted.state=Leader; restarted.leaderID="n1"
	if err:=restarted.PutWithRequest("client","1","one","first"); err!=nil { t.Fatal(err) }
	if restarted.lastLogIndexLocked()!=2 { t.Fatal("deduplication state was not restored from snapshot") }
}

func TestFollowerInstallsSnapshot(t *testing.T) {
	n:=NewNode("n2",map[string]string{"n1":"one","n2":"two","n3":"three"},fakeTransport{})
	snapshot:=Snapshot{LastIncludedIndex:9,LastIncludedTerm:2,Store:map[string]string{"durable":"yes"},AppliedRequests:map[string]Command{}}
	response:=n.HandleInstallSnapshot(InstallSnapshotRequest{Term:3,LeaderID:"n1",Snapshot:snapshot})
	if !response.Success || response.MatchIndex!=9 { t.Fatalf("unexpected response: %+v",response) }
	if value,ok:=n.Get("durable"); !ok || value!="yes" { t.Fatalf("snapshot not installed: %q",value) }
	if status:=n.Status(); status.SnapshotIndex!=9 || status.CommitIndex!=9 { t.Fatalf("unexpected status: %+v",status) }
}

func TestLeaderSendsSnapshotToLaggingFollower(t *testing.T) {
	snapshotSent:=false
	transport:=fakeTransport{
		snapshot:func(req InstallSnapshotRequest) InstallSnapshotResponse { snapshotSent=true; return InstallSnapshotResponse{Term:req.Term,Success:true,MatchIndex:req.Snapshot.LastIncludedIndex} },
		append:func(req AppendEntriesRequest) AppendEntriesResponse { return AppendEntriesResponse{Term:req.Term,Success:true,MatchIndex:req.PrevLogIndex+len(req.Entries)} },
	}
	n:=NewNode("n1",map[string]string{"n1":"one","n2":"two"},transport)
	n.state=Leader; n.term=2; n.leaderID="n1"
	n.snapshot=Snapshot{LastIncludedIndex:4,LastIncludedTerm:1,Store:map[string]string{"old":"value"},AppliedRequests:map[string]Command{}}
	n.log=[]LogEntry{{Term:2,Command:Command{Operation:"put",Key:"new",Value:"value"}}}
	n.commitIndex=5; n.lastApplied=5; n.nextIndex["n2"]=0; n.matchIndex["n2"]=-1
	if !n.replicateAll() { t.Fatal("expected snapshot replication quorum") }
	if !snapshotSent { t.Fatal("leader did not send snapshot") }
	if n.matchIndex["n2"]!=5 { t.Fatalf("follower only reached index %d",n.matchIndex["n2"]) }
}
