package raft

type State string

const (
	Follower  State = "follower"
	Candidate State = "candidate"
	Leader    State = "leader"
)

type LogEntry struct {
	Term    int     `json:"term"`
	Command Command `json:"command"`
}

type Command struct {
	Operation string `json:"operation"`
	Key       string `json:"key"`
	Value     string `json:"value"`
}

type RequestVoteRequest struct {
	Term         int    `json:"term"`
	CandidateID  string `json:"candidate_id"`
	LastLogIndex int    `json:"last_log_index"`
	LastLogTerm  int    `json:"last_log_term"`
}

type RequestVoteResponse struct {
	Term        int  `json:"term"`
	VoteGranted bool `json:"vote_granted"`
}

type AppendEntriesRequest struct {
	Term         int        `json:"term"`
	LeaderID     string     `json:"leader_id"`
	PrevLogIndex int        `json:"prev_log_index"`
	PrevLogTerm  int        `json:"prev_log_term"`
	Entries      []LogEntry `json:"entries"`
	LeaderCommit int        `json:"leader_commit"`
}

type AppendEntriesResponse struct {
	Term       int  `json:"term"`
	Success    bool `json:"success"`
	MatchIndex int  `json:"match_index"`
}

type Status struct {
	ID          string `json:"id"`
	State       State  `json:"state"`
	Term        int    `json:"term"`
	LeaderID    string `json:"leader_id,omitempty"`
	CommitIndex int    `json:"commit_index"`
	LogLength   int    `json:"log_length"`
	StorageOK   bool   `json:"storage_ok"`
	StorageError string `json:"storage_error,omitempty"`
}
