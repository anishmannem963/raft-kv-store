package raft

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"time"
)

type Transport interface {
	RequestVote(context.Context, string, RequestVoteRequest) (RequestVoteResponse, error)
	AppendEntries(context.Context, string, AppendEntriesRequest) (AppendEntriesResponse, error)
	InstallSnapshot(context.Context, string, InstallSnapshotRequest) (InstallSnapshotResponse, error)
}

type HTTPTransport struct{ client *http.Client }

func NewHTTPTransport(timeout time.Duration) *HTTPTransport {
	return &HTTPTransport{client: &http.Client{Timeout: timeout}}
}

func (t *HTTPTransport) RequestVote(ctx context.Context, peer string, in RequestVoteRequest) (RequestVoteResponse, error) {
	var out RequestVoteResponse
	err := t.post(ctx, peer+"/raft/vote", in, &out)
	return out, err
}

func (t *HTTPTransport) AppendEntries(ctx context.Context, peer string, in AppendEntriesRequest) (AppendEntriesResponse, error) {
	var out AppendEntriesResponse
	err := t.post(ctx, peer+"/raft/append", in, &out)
	return out, err
}

func (t *HTTPTransport) InstallSnapshot(ctx context.Context, peer string, in InstallSnapshotRequest) (InstallSnapshotResponse, error) {
	var out InstallSnapshotResponse
	err := t.post(ctx, peer+"/raft/snapshot", in, &out)
	return out, err
}

func (t *HTTPTransport) post(ctx context.Context, url string, in, out any) error {
	body, err := json.Marshal(in)
	if err != nil { return err }
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, url, bytes.NewReader(body))
	if err != nil { return err }
	req.Header.Set("Content-Type", "application/json")
	resp, err := t.client.Do(req)
	if err != nil { return err }
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK { return fmt.Errorf("peer returned %s", resp.Status) }
	return json.NewDecoder(resp.Body).Decode(out)
}
