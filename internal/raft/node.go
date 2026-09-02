package raft

import (
	"context"
	"errors"
	"math/rand"
	"sync"
	"time"
)

var ErrNotLeader = errors.New("request must be sent to the leader")

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
	commitIndex int
	lastApplied int
	nextIndex map[string]int
	matchIndex map[string]int
	store map[string]string
	storage Storage
	storageErr error
	electionMin time.Duration
	electionMax time.Duration
	heartbeat time.Duration
	resetElection chan struct{}
	stop chan struct{}
	stopped chan struct{}
}

func NewNode(id string, peers map[string]string, transport Transport) *Node {
	node, err := NewNodeWithStorage(id, peers, transport, &MemoryStorage{})
	if err != nil {
		panic(err)
	}
	return node
}

func NewNodeWithStorage(id string, peers map[string]string, transport Transport, storage Storage) (*Node, error) {
	n := &Node{id:id, peers:peers, transport:transport, state:Follower, commitIndex:-1, lastApplied:-1,
		nextIndex:map[string]int{}, matchIndex:map[string]int{}, store:map[string]string{},
		storage:storage,
		electionMin:300*time.Millisecond, electionMax:550*time.Millisecond, heartbeat:100*time.Millisecond,
		resetElection:make(chan struct{},1), stop:make(chan struct{}), stopped:make(chan struct{})}
	state, err := storage.Load()
	if err != nil && !errors.Is(err, ErrNoState) {
		return nil, err
	}
	if err == nil {
		n.term = state.CurrentTerm
		n.votedFor = state.VotedFor
		n.log = append([]LogEntry(nil), state.Log...)
		n.commitIndex = min(state.CommitIndex, len(n.log)-1)
		n.applyCommittedLocked()
	}
	return n, nil
}

func (n *Node) Start() { go n.run() }
func (n *Node) Stop() { select { case <-n.stop: default: close(n.stop) }; <-n.stopped }

func (n *Node) run() {
	defer close(n.stopped)
	timer := time.NewTimer(n.randomElectionTimeout())
	defer timer.Stop()
	heartbeat := time.NewTicker(n.heartbeat)
	defer heartbeat.Stop()
	for {
		select {
		case <-n.stop: return
		case <-n.resetElection:
			if !timer.Stop() { select { case <-timer.C: default: } }
			timer.Reset(n.randomElectionTimeout())
		case <-timer.C:
			n.startElection()
			timer.Reset(n.randomElectionTimeout())
		case <-heartbeat.C:
			n.mu.Lock(); leader := n.state == Leader; n.mu.Unlock()
			if leader { n.replicateAll() }
		}
	}
}

func (n *Node) randomElectionTimeout() time.Duration {
	delta := n.electionMax - n.electionMin
	return n.electionMin + time.Duration(rand.Int63n(int64(delta)))
}

func (n *Node) startElection() {
	n.mu.Lock()
	n.state = Candidate; n.term++; term := n.term; n.votedFor = n.id; n.leaderID = ""
	if err := n.persistLocked(); err != nil {
		n.state = Follower
		n.mu.Unlock()
		return
	}
	lastIndex, lastTerm := n.lastLogInfoLocked()
	n.mu.Unlock()

	votes := 1
	var voteMu sync.Mutex
	ctx, cancel := context.WithTimeout(context.Background(), n.electionMin)
	defer cancel()
	var wg sync.WaitGroup
	for id, address := range n.peers {
		if id == n.id { continue }
		wg.Add(1)
		go func(peer string) {
			defer wg.Done()
			resp, err := n.transport.RequestVote(ctx, peer, RequestVoteRequest{Term:term, CandidateID:n.id, LastLogIndex:lastIndex, LastLogTerm:lastTerm})
			if err != nil { return }
			n.mu.Lock()
			if resp.Term > n.term { n.becomeFollowerLocked(resp.Term, ""); n.persistLocked(); n.mu.Unlock(); return }
			stillCandidate := n.state == Candidate && n.term == term
			n.mu.Unlock()
			if resp.VoteGranted && stillCandidate { voteMu.Lock(); votes++; voteMu.Unlock() }
		}(address)
	}
	wg.Wait()
	voteMu.Lock(); won := votes >= n.quorum(); voteMu.Unlock()
	n.mu.Lock()
	if won && n.state == Candidate && n.term == term {
		n.state = Leader; n.leaderID = n.id
		for id := range n.peers { if id != n.id { n.nextIndex[id]=len(n.log); n.matchIndex[id]=-1 } }
	}
	n.mu.Unlock()
	if won { n.replicateAll() }
}

func (n *Node) HandleRequestVote(req RequestVoteRequest) RequestVoteResponse {
	n.mu.Lock(); defer n.mu.Unlock()
	if req.Term < n.term { return RequestVoteResponse{Term:n.term} }
	if req.Term > n.term {
		n.becomeFollowerLocked(req.Term, "")
		if n.persistLocked() != nil { return RequestVoteResponse{Term:n.term} }
	}
	lastIndex, lastTerm := n.lastLogInfoLocked()
	upToDate := req.LastLogTerm > lastTerm || (req.LastLogTerm == lastTerm && req.LastLogIndex >= lastIndex)
	grant := (n.votedFor == "" || n.votedFor == req.CandidateID) && upToDate
	if grant {
		n.votedFor = req.CandidateID
		if n.persistLocked() != nil { return RequestVoteResponse{Term:n.term} }
		n.signalElectionReset()
	}
	return RequestVoteResponse{Term:n.term, VoteGranted:grant}
}

func (n *Node) HandleAppendEntries(req AppendEntriesRequest) AppendEntriesResponse {
	n.mu.Lock(); defer n.mu.Unlock()
	if req.Term < n.term { return AppendEntriesResponse{Term:n.term, Success:false, MatchIndex:len(n.log)-1} }
	termChanged := req.Term > n.term
	if termChanged || n.state != Follower { n.becomeFollowerLocked(req.Term, req.LeaderID) }
	n.leaderID = req.LeaderID; n.signalElectionReset()
	if req.PrevLogIndex >= 0 && (req.PrevLogIndex >= len(n.log) || n.log[req.PrevLogIndex].Term != req.PrevLogTerm) {
		return AppendEntriesResponse{Term:n.term, Success:false, MatchIndex:len(n.log)-1}
	}
	insert := req.PrevLogIndex + 1
	for i, entry := range req.Entries {
		at := insert+i
		if at < len(n.log) && n.log[at].Term != entry.Term { n.log=n.log[:at] }
		if at >= len(n.log) { n.log=append(n.log, entry) }
	}
	if termChanged || len(req.Entries) > 0 {
		if n.persistLocked() != nil {
			return AppendEntriesResponse{Term:n.term, Success:false, MatchIndex:len(n.log)-1}
		}
	}
	if req.LeaderCommit > n.commitIndex {
		previousCommit := n.commitIndex
		n.commitIndex=min(req.LeaderCommit, len(n.log)-1)
		if n.persistLocked() != nil {
			n.commitIndex = previousCommit
			return AppendEntriesResponse{Term:n.term, Success:false, MatchIndex:len(n.log)-1}
		}
		n.applyCommittedLocked()
	}
	return AppendEntriesResponse{Term:n.term, Success:true, MatchIndex:len(n.log)-1}
}

func (n *Node) Put(key, value string) error {
	n.mu.Lock()
	if n.state != Leader { n.mu.Unlock(); return ErrNotLeader }
	n.log=append(n.log, LogEntry{Term:n.term, Command:Command{Operation:"put", Key:key, Value:value}})
	if err := n.persistLocked(); err != nil {
		n.log = n.log[:len(n.log)-1]
		n.mu.Unlock()
		return err
	}
	target := len(n.log)-1
	n.mu.Unlock()
	n.replicateAll()
	n.mu.Lock(); defer n.mu.Unlock()
	if n.commitIndex < target { return errors.New("entry did not reach quorum") }
	return nil
}

func (n *Node) Get(key string) (string, bool) { n.mu.Lock(); defer n.mu.Unlock(); v,ok:=n.store[key]; return v,ok }
func (n *Node) LeaderAddress() string { n.mu.Lock(); defer n.mu.Unlock(); return n.peers[n.leaderID] }
func (n *Node) Status() Status { n.mu.Lock(); defer n.mu.Unlock(); status:=Status{ID:n.id,State:n.state,Term:n.term,LeaderID:n.leaderID,CommitIndex:n.commitIndex,LogLength:len(n.log),StorageOK:n.storageErr==nil}; if n.storageErr!=nil {status.StorageError=n.storageErr.Error()}; return status }

func (n *Node) replicateAll() {
	n.mu.Lock()
	if n.state != Leader { n.mu.Unlock(); return }
	term := n.term
	n.mu.Unlock()
	var wg sync.WaitGroup
	for id, address := range n.peers { if id != n.id { wg.Add(1); go func(pid, peer string){ defer wg.Done(); n.replicatePeer(term,pid,peer) }(id,address) } }
	wg.Wait(); n.advanceCommit()
}

func (n *Node) replicatePeer(term int, id, peer string) {
	for attempts:=0; attempts<3; attempts++ {
		n.mu.Lock()
		if n.state != Leader || n.term != term { n.mu.Unlock(); return }
		next:=n.nextIndex[id]; prev:=next-1; prevTerm:=0; if prev>=0 { prevTerm=n.log[prev].Term }
		entries:=append([]LogEntry(nil),n.log[next:]...)
		req:=AppendEntriesRequest{Term:term,LeaderID:n.id,PrevLogIndex:prev,PrevLogTerm:prevTerm,Entries:entries,LeaderCommit:n.commitIndex}
		n.mu.Unlock()
		ctx,cancel:=context.WithTimeout(context.Background(),n.heartbeat); resp,err:=n.transport.AppendEntries(ctx,peer,req); cancel()
		if err!=nil { return }
		n.mu.Lock()
		if resp.Term>n.term { n.becomeFollowerLocked(resp.Term,""); n.persistLocked(); n.mu.Unlock(); return }
		if resp.Success { n.matchIndex[id]=resp.MatchIndex; n.nextIndex[id]=resp.MatchIndex+1; n.mu.Unlock(); return }
		if n.nextIndex[id]>0 { n.nextIndex[id]-- }; n.mu.Unlock()
	}
}

func (n *Node) advanceCommit() {
	n.mu.Lock(); defer n.mu.Unlock()
	for idx:=len(n.log)-1; idx>n.commitIndex; idx-- {
		if n.log[idx].Term!=n.term { continue }
		count:=1; for id:=range n.peers { if id!=n.id && n.matchIndex[id]>=idx { count++ } }
		if count>=n.quorum() {
			previousCommit := n.commitIndex
			n.commitIndex=idx
			if n.persistLocked() != nil { n.commitIndex=previousCommit; return }
			n.applyCommittedLocked()
			break
		}
	}
}

func (n *Node) applyCommittedLocked() { for n.lastApplied<n.commitIndex { n.lastApplied++; c:=n.log[n.lastApplied].Command; if c.Operation=="put" { n.store[c.Key]=c.Value } } }
func (n *Node) lastLogInfoLocked()(int,int){ i:=len(n.log)-1; if i<0{return -1,0}; return i,n.log[i].Term }
func (n *Node) quorum() int { return len(n.peers)/2+1 }
func (n *Node) becomeFollowerLocked(term int, leader string){ n.state=Follower; if term>n.term { n.term=term; n.votedFor="" }; n.leaderID=leader; n.signalElectionReset() }
func (n *Node) signalElectionReset(){ select{case n.resetElection<-struct{}{}:default:} }
func (n *Node) persistLocked() error {
	err := n.storage.Save(PersistentState{CurrentTerm:n.term,VotedFor:n.votedFor,Log:n.log,CommitIndex:n.commitIndex})
	n.storageErr = err
	return err
}
