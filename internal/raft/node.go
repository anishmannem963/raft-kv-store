package raft

import (
	"context"
	"errors"
	"math/rand"
	"sync"
	"time"
)

var ErrNotLeader = errors.New("request must be sent to the leader")
var ErrQuorumUnavailable = errors.New("leader could not confirm a quorum")
var ErrRequestConflict = errors.New("request ID was already used with a different command")

type Node struct {
	mu sync.Mutex
	id string
	peers map[string]string
	transport Transport
	state State
	term int
	votedFor string
	leaderID string
	log []LogEntry
	snapshot Snapshot
	commitIndex int
	lastApplied int
	nextIndex map[string]int
	matchIndex map[string]int
	store map[string]string
	appliedRequests map[string]Command
	storage Storage
	storageErr error
	snapshotThreshold int
	electionMin, electionMax, heartbeat time.Duration
	resetElection, stop, stopped chan struct{}
}

func NewNode(id string, peers map[string]string, transport Transport) *Node {
	n, err := NewNodeWithStorage(id, peers, transport, &MemoryStorage{})
	if err != nil { panic(err) }
	return n
}

func NewNodeWithStorage(id string, peers map[string]string, transport Transport, storage Storage) (*Node, error) {
	n := &Node{id:id, peers:peers, transport:transport, state:Follower,
		snapshot:Snapshot{LastIncludedIndex:-1}, commitIndex:-1, lastApplied:-1,
		nextIndex:map[string]int{}, matchIndex:map[string]int{}, store:map[string]string{}, appliedRequests:map[string]Command{},
		storage:storage, snapshotThreshold:100, electionMin:300*time.Millisecond, electionMax:550*time.Millisecond, heartbeat:100*time.Millisecond,
		resetElection:make(chan struct{},1), stop:make(chan struct{}), stopped:make(chan struct{})}
	p, err := storage.Load()
	if err != nil && !errors.Is(err, ErrNoState) { return nil, err }
	if err == nil {
		n.term, n.votedFor, n.log = p.CurrentTerm, p.VotedFor, append([]LogEntry(nil),p.Log...)
		if p.Snapshot != nil {
			n.snapshot=cloneSnapshot(*p.Snapshot)
			n.store=cloneStringMap(n.snapshot.Store)
			n.appliedRequests=cloneCommandMap(n.snapshot.AppliedRequests)
		}
		n.commitIndex=min(p.CommitIndex,n.lastLogIndexLocked())
		if n.commitIndex<n.snapshot.LastIncludedIndex { n.commitIndex=n.snapshot.LastIncludedIndex }
		n.lastApplied=n.snapshot.LastIncludedIndex
		n.applyCommittedLocked()
	}
	return n,nil
}

func (n *Node) SetSnapshotThreshold(entries int) { n.mu.Lock(); n.snapshotThreshold=entries; n.mu.Unlock() }
func (n *Node) Start() { go n.run() }
func (n *Node) Stop() { select { case <-n.stop: default: close(n.stop) }; <-n.stopped }

func (n *Node) run() {
	defer close(n.stopped)
	timer:=time.NewTimer(n.randomElectionTimeout()); defer timer.Stop()
	heartbeats:=time.NewTicker(n.heartbeat); defer heartbeats.Stop()
	for { select {
	case <-n.stop: return
	case <-n.resetElection:
		if !timer.Stop() { select { case <-timer.C: default: } }
		timer.Reset(n.randomElectionTimeout())
	case <-timer.C: n.startElection(); timer.Reset(n.randomElectionTimeout())
	case <-heartbeats.C: n.mu.Lock(); leader:=n.state==Leader; n.mu.Unlock(); if leader { n.replicateAll() }
	} }
}

func (n *Node) randomElectionTimeout() time.Duration { return n.electionMin+time.Duration(rand.Int63n(int64(n.electionMax-n.electionMin))) }

func (n *Node) startElection() {
	n.mu.Lock()
	n.state=Candidate; n.term++; term:=n.term; n.votedFor=n.id; n.leaderID=""
	if n.persistLocked()!=nil { n.state=Follower; n.mu.Unlock(); return }
	lastIndex,lastTerm:=n.lastLogInfoLocked(); n.mu.Unlock()
	votes:=1; var voteMu sync.Mutex; var wg sync.WaitGroup
	ctx,cancel:=context.WithTimeout(context.Background(),n.electionMin); defer cancel()
	for id,address:=range n.peers { if id!=n.id { wg.Add(1); go func(peer string) {
		defer wg.Done()
		resp,err:=n.transport.RequestVote(ctx,peer,RequestVoteRequest{Term:term,CandidateID:n.id,LastLogIndex:lastIndex,LastLogTerm:lastTerm})
		if err!=nil { return }
		n.mu.Lock()
		if resp.Term>n.term { n.becomeFollowerLocked(resp.Term,""); n.persistLocked(); n.mu.Unlock(); return }
		valid:=n.state==Candidate && n.term==term; n.mu.Unlock()
		if resp.VoteGranted && valid { voteMu.Lock(); votes++; voteMu.Unlock() }
	}(address) } }
	wg.Wait(); voteMu.Lock(); won:=votes>=n.quorum(); voteMu.Unlock()
	n.mu.Lock()
	if won && n.state==Candidate && n.term==term {
		n.state=Leader; n.leaderID=n.id; last:=n.lastLogIndexLocked()
		for id:=range n.peers { if id!=n.id { n.nextIndex[id]=last+1; n.matchIndex[id]=-1 } }
	}
	n.mu.Unlock(); if won { n.replicateAll() }
}

func (n *Node) HandleRequestVote(req RequestVoteRequest) RequestVoteResponse {
	n.mu.Lock(); defer n.mu.Unlock()
	if req.Term<n.term { return RequestVoteResponse{Term:n.term} }
	if req.Term>n.term { n.becomeFollowerLocked(req.Term,""); if n.persistLocked()!=nil { return RequestVoteResponse{Term:n.term} } }
	lastIndex,lastTerm:=n.lastLogInfoLocked()
	upToDate:=req.LastLogTerm>lastTerm || (req.LastLogTerm==lastTerm && req.LastLogIndex>=lastIndex)
	grant:=(n.votedFor=="" || n.votedFor==req.CandidateID) && upToDate
	if grant { n.votedFor=req.CandidateID; if n.persistLocked()!=nil { return RequestVoteResponse{Term:n.term} }; n.signalElectionReset() }
	return RequestVoteResponse{Term:n.term,VoteGranted:grant}
}

func (n *Node) HandleAppendEntries(req AppendEntriesRequest) AppendEntriesResponse {
	n.mu.Lock(); defer n.mu.Unlock()
	if req.Term<n.term { return AppendEntriesResponse{Term:n.term,MatchIndex:n.lastLogIndexLocked()} }
	termChanged:=req.Term>n.term
	if termChanged || n.state!=Follower { n.becomeFollowerLocked(req.Term,req.LeaderID) }
	n.leaderID=req.LeaderID; n.signalElectionReset()
	prevTerm,found:=n.termAtLocked(req.PrevLogIndex)
	if !found || prevTerm!=req.PrevLogTerm { return AppendEntriesResponse{Term:n.term,MatchIndex:n.lastLogIndexLocked()} }
	changed:=termChanged
	for offset,entry:=range req.Entries {
		index:=req.PrevLogIndex+1+offset
		if index<=n.snapshot.LastIncludedIndex { continue }
		position:=index-n.snapshot.LastIncludedIndex-1
		if position<len(n.log) && n.log[position].Term!=entry.Term { n.log=n.log[:position]; changed=true }
		if position>=len(n.log) { n.log=append(n.log,entry); changed=true }
	}
	if changed && n.persistLocked()!=nil { return AppendEntriesResponse{Term:n.term,MatchIndex:n.lastLogIndexLocked()} }
	if req.LeaderCommit>n.commitIndex {
		old:=n.commitIndex; n.commitIndex=min(req.LeaderCommit,n.lastLogIndexLocked())
		if n.persistLocked()!=nil { n.commitIndex=old; return AppendEntriesResponse{Term:n.term,MatchIndex:n.lastLogIndexLocked()} }
		n.applyCommittedLocked(); n.compactLocked()
	}
	return AppendEntriesResponse{Term:n.term,Success:true,MatchIndex:n.lastLogIndexLocked()}
}

func (n *Node) HandleInstallSnapshot(req InstallSnapshotRequest) InstallSnapshotResponse {
	n.mu.Lock(); defer n.mu.Unlock()
	if req.Term<n.term { return InstallSnapshotResponse{Term:n.term,MatchIndex:n.snapshot.LastIncludedIndex} }
	if req.Term>n.term || n.state!=Follower { n.becomeFollowerLocked(req.Term,req.LeaderID) }
	n.leaderID=req.LeaderID; n.signalElectionReset()
	if req.Snapshot.LastIncludedIndex<=n.snapshot.LastIncludedIndex { return InstallSnapshotResponse{Term:n.term,Success:true,MatchIndex:n.snapshot.LastIncludedIndex} }
	var remaining []LogEntry
	if term,ok:=n.termAtLocked(req.Snapshot.LastIncludedIndex); ok && term==req.Snapshot.LastIncludedTerm {
		start:=req.Snapshot.LastIncludedIndex-n.snapshot.LastIncludedIndex
		if start<len(n.log) { remaining=append(remaining,n.log[start:]...) }
	}
	n.snapshot=cloneSnapshot(req.Snapshot); n.log=remaining
	n.store=cloneStringMap(n.snapshot.Store); n.appliedRequests=cloneCommandMap(n.snapshot.AppliedRequests)
	n.commitIndex=max(n.commitIndex,n.snapshot.LastIncludedIndex)
	if n.commitIndex>n.lastLogIndexLocked() { n.commitIndex=n.lastLogIndexLocked() }
	n.lastApplied=n.snapshot.LastIncludedIndex; n.applyCommittedLocked()
	if n.persistLocked()!=nil { return InstallSnapshotResponse{Term:n.term,MatchIndex:n.snapshot.LastIncludedIndex} }
	return InstallSnapshotResponse{Term:n.term,Success:true,MatchIndex:n.snapshot.LastIncludedIndex}
}

func (n *Node) Put(key,value string) error { return n.PutWithRequest("","",key,value) }
func (n *Node) PutWithRequest(clientID,requestID,key,value string) error {
	n.mu.Lock(); if n.state!=Leader { n.mu.Unlock(); return ErrNotLeader }
	command:=Command{Operation:"put",Key:key,Value:value,ClientID:clientID,RequestID:requestID}; target:=-1
	if clientID!="" && requestID!="" {
		if previous,ok:=n.appliedRequests[requestKey(clientID,requestID)]; ok { n.mu.Unlock(); if !sameWrite(previous,command) { return ErrRequestConflict }; return nil }
		for index:=n.commitIndex+1; index<=n.lastLogIndexLocked(); index++ { candidate,_:=n.entryAtLocked(index); if candidate.Command.ClientID==clientID && candidate.Command.RequestID==requestID { target=index; if !sameWrite(candidate.Command,command) { n.mu.Unlock(); return ErrRequestConflict }; break } }
	}
	if target==-1 { n.log=append(n.log,LogEntry{Term:n.term,Command:command}); target=n.lastLogIndexLocked(); if err:=n.persistLocked(); err!=nil { n.log=n.log[:len(n.log)-1]; n.mu.Unlock(); return err } }
	n.mu.Unlock(); n.replicateAll(); n.mu.Lock(); defer n.mu.Unlock()
	if n.commitIndex<target { return errors.New("entry did not reach quorum") }
	return nil
}

func (n *Node) Get(key string) (string,bool) { n.mu.Lock(); defer n.mu.Unlock(); value,ok:=n.store[key]; return value,ok }
func (n *Node) Read(key string) (string,bool,error) {
	n.mu.Lock(); if n.state!=Leader { n.mu.Unlock(); return "",false,ErrNotLeader }; term:=n.term
	commitTerm,_:=n.termAtLocked(n.commitIndex); _,lastTerm:=n.lastLogInfoLocked()
	if commitTerm!=term && lastTerm!=term { n.log=append(n.log,LogEntry{Term:term,Command:Command{Operation:"noop"}}); if err:=n.persistLocked(); err!=nil { n.log=n.log[:len(n.log)-1]; n.mu.Unlock(); return "",false,err } }
	n.mu.Unlock(); if !n.replicateAll() { return "",false,ErrQuorumUnavailable }
	n.mu.Lock(); defer n.mu.Unlock(); if n.state!=Leader || n.term!=term { return "",false,ErrNotLeader }
	commitTerm,_=n.termAtLocked(n.commitIndex); if commitTerm!=term { return "",false,ErrQuorumUnavailable }
	value,ok:=n.store[key]; return value,ok,nil
}

func (n *Node) LeaderAddress() string { n.mu.Lock(); defer n.mu.Unlock(); return n.peers[n.leaderID] }
func (n *Node) Status() Status { n.mu.Lock(); defer n.mu.Unlock(); s:=Status{ID:n.id,State:n.state,Term:n.term,LeaderID:n.leaderID,CommitIndex:n.commitIndex,LogLength:len(n.log),SnapshotIndex:n.snapshot.LastIncludedIndex,StorageOK:n.storageErr==nil}; if n.storageErr!=nil { s.StorageError=n.storageErr.Error() }; return s }

func (n *Node) replicateAll() bool {
	n.mu.Lock(); if n.state!=Leader { n.mu.Unlock(); return false }; term:=n.term; n.mu.Unlock()
	acks:=1; var ackMu sync.Mutex; var wg sync.WaitGroup
	for id,address:=range n.peers { if id!=n.id { wg.Add(1); go func(peerID,peer string) { defer wg.Done(); if n.replicatePeer(term,peerID,peer) { ackMu.Lock(); acks++; ackMu.Unlock() } }(id,address) } }
	wg.Wait(); n.advanceCommit(); ackMu.Lock(); quorum:=acks>=n.quorum(); ackMu.Unlock(); return quorum
}

func (n *Node) replicatePeer(term int,id,peer string) bool {
	for attempts:=0; attempts<5; attempts++ {
		n.mu.Lock(); if n.state!=Leader || n.term!=term { n.mu.Unlock(); return false }; next:=n.nextIndex[id]
		if next<=n.snapshot.LastIncludedIndex {
			snapshot:=cloneSnapshot(n.snapshot); n.mu.Unlock()
			ctx,cancel:=context.WithTimeout(context.Background(),n.heartbeat); resp,err:=n.transport.InstallSnapshot(ctx,peer,InstallSnapshotRequest{Term:term,LeaderID:n.id,Snapshot:snapshot}); cancel()
			if err!=nil { return false }
			n.mu.Lock(); if resp.Term>n.term { n.becomeFollowerLocked(resp.Term,""); n.persistLocked(); n.mu.Unlock(); return false }
			if resp.Success { n.matchIndex[id]=resp.MatchIndex; n.nextIndex[id]=resp.MatchIndex+1; n.mu.Unlock(); continue }; n.mu.Unlock(); return false
		}
		prev:=next-1; prevTerm,ok:=n.termAtLocked(prev); if !ok { n.nextIndex[id]=n.snapshot.LastIncludedIndex; n.mu.Unlock(); continue }
		req:=AppendEntriesRequest{Term:term,LeaderID:n.id,PrevLogIndex:prev,PrevLogTerm:prevTerm,Entries:n.entriesFromLocked(next),LeaderCommit:n.commitIndex}; n.mu.Unlock()
		ctx,cancel:=context.WithTimeout(context.Background(),n.heartbeat); resp,err:=n.transport.AppendEntries(ctx,peer,req); cancel(); if err!=nil { return false }
		n.mu.Lock(); if resp.Term>n.term { n.becomeFollowerLocked(resp.Term,""); n.persistLocked(); n.mu.Unlock(); return false }
		if resp.Success { n.matchIndex[id]=resp.MatchIndex; n.nextIndex[id]=resp.MatchIndex+1; n.mu.Unlock(); return true }
		candidate:=resp.MatchIndex+1; if candidate>=n.nextIndex[id] { candidate=n.nextIndex[id]-1 }; if candidate<0 { candidate=0 }; n.nextIndex[id]=candidate; n.mu.Unlock()
	}
	return false
}

func (n *Node) advanceCommit() {
	n.mu.Lock(); defer n.mu.Unlock()
	for index:=n.lastLogIndexLocked(); index>n.commitIndex; index-- { entry,ok:=n.entryAtLocked(index); if !ok || entry.Term!=n.term { continue }; count:=1; for id:=range n.peers { if id!=n.id && n.matchIndex[id]>=index { count++ } }; if count>=n.quorum() { old:=n.commitIndex; n.commitIndex=index; if n.persistLocked()!=nil { n.commitIndex=old; return }; n.applyCommittedLocked(); n.compactLocked(); return } }
}

func (n *Node) applyCommittedLocked() { for n.lastApplied<n.commitIndex { n.lastApplied++; entry,ok:=n.entryAtLocked(n.lastApplied); if !ok { continue }; c:=entry.Command; if c.Operation=="put" { n.store[c.Key]=c.Value; if c.ClientID!="" && c.RequestID!="" { n.appliedRequests[requestKey(c.ClientID,c.RequestID)]=c } } } }
func (n *Node) compactLocked() {
	if n.snapshotThreshold<=0 || n.commitIndex-n.snapshot.LastIncludedIndex<n.snapshotThreshold { return }
	term,ok:=n.termAtLocked(n.commitIndex); if !ok { return }; remove:=n.commitIndex-n.snapshot.LastIncludedIndex; if remove>len(n.log) { remove=len(n.log) }
	n.snapshot=Snapshot{LastIncludedIndex:n.commitIndex,LastIncludedTerm:term,Store:cloneStringMap(n.store),AppliedRequests:cloneCommandMap(n.appliedRequests)}; n.log=append([]LogEntry(nil),n.log[remove:]...); n.persistLocked()
}

func (n *Node) lastLogIndexLocked() int { return n.snapshot.LastIncludedIndex+len(n.log) }
func (n *Node) lastLogInfoLocked() (int,int) { index:=n.lastLogIndexLocked(); term,_:=n.termAtLocked(index); return index,term }
func (n *Node) termAtLocked(index int) (int,bool) { if index==n.snapshot.LastIncludedIndex { return n.snapshot.LastIncludedTerm,true }; position:=index-n.snapshot.LastIncludedIndex-1; if position<0 || position>=len(n.log) { return 0,false }; return n.log[position].Term,true }
func (n *Node) entryAtLocked(index int) (LogEntry,bool) { position:=index-n.snapshot.LastIncludedIndex-1; if position<0 || position>=len(n.log) { return LogEntry{},false }; return n.log[position],true }
func (n *Node) entriesFromLocked(index int) []LogEntry { position:=index-n.snapshot.LastIncludedIndex-1; if position<0 { position=0 }; if position>len(n.log) { position=len(n.log) }; return append([]LogEntry(nil),n.log[position:]...) }
func (n *Node) quorum() int { return len(n.peers)/2+1 }
func (n *Node) becomeFollowerLocked(term int,leader string) { n.state=Follower; if term>n.term { n.term=term; n.votedFor="" }; n.leaderID=leader; n.signalElectionReset() }
func (n *Node) signalElectionReset() { select { case n.resetElection<-struct{}{}: default: } }
func (n *Node) persistLocked() error { snapshot:=cloneSnapshot(n.snapshot); err:=n.storage.Save(PersistentState{CurrentTerm:n.term,VotedFor:n.votedFor,Log:n.log,CommitIndex:n.commitIndex,Snapshot:&snapshot}); n.storageErr=err; return err }
func requestKey(clientID,requestID string) string { return clientID+"\x00"+requestID }
func sameWrite(left,right Command) bool { return left.Operation==right.Operation && left.Key==right.Key && left.Value==right.Value }
