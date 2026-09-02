package main

import (
	"encoding/json"
	"errors"
	"flag"
	"log"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/anishmannem963/raft-kv-store/internal/raft"
)

func main() {
	id := flag.String("id", env("NODE_ID", "node1"), "node ID")
	listen := flag.String("listen", env("LISTEN_ADDR", ":8080"), "HTTP listen address")
	peerList := flag.String("peers", env("PEERS", "node1=http://localhost:8080"), "comma-separated id=url peers")
	flag.Parse()
	peers := parsePeers(*peerList)
	var storage raft.Storage = &raft.MemoryStorage{}
	if dataDir := os.Getenv("DATA_DIR"); dataDir != "" {
		storage = raft.NewFileStorage(dataDir)
	}
	node, err := raft.NewNodeWithStorage(*id, peers, raft.NewHTTPTransport(250*time.Millisecond), storage)
	if err != nil {
		log.Fatalf("load raft state: %v", err)
	}
	node.Start()
	defer node.Stop()

	mux := http.NewServeMux()
	mux.HandleFunc("POST /raft/vote", jsonHandler(func(r *http.Request) (any, int) {
		var req raft.RequestVoteRequest
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil { return apiError{err.Error()}, http.StatusBadRequest }
		return node.HandleRequestVote(req), http.StatusOK
	}))
	mux.HandleFunc("POST /raft/append", jsonHandler(func(r *http.Request) (any, int) {
		var req raft.AppendEntriesRequest
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil { return apiError{err.Error()}, http.StatusBadRequest }
		return node.HandleAppendEntries(req), http.StatusOK
	}))
	mux.HandleFunc("PUT /kv/{key}", jsonHandler(func(r *http.Request) (any, int) {
		var body struct{ Value string `json:"value"` }
		if err := json.NewDecoder(r.Body).Decode(&body); err != nil { return apiError{err.Error()}, http.StatusBadRequest }
		clientID := r.Header.Get("X-Client-ID")
		requestID := r.Header.Get("X-Request-ID")
		if clientID == "" || requestID == "" { return apiError{"X-Client-ID and X-Request-ID headers are required"}, http.StatusBadRequest }
		if err := node.PutWithRequest(clientID, requestID, r.PathValue("key"), body.Value); err != nil {
			if errors.Is(err, raft.ErrNotLeader) { return map[string]string{"error":err.Error(),"leader":node.LeaderAddress()}, http.StatusTemporaryRedirect }
			if errors.Is(err, raft.ErrRequestConflict) { return apiError{err.Error()}, http.StatusConflict }
			return apiError{err.Error()}, http.StatusServiceUnavailable
		}
		return map[string]string{"status":"committed"}, http.StatusCreated
	}))
	mux.HandleFunc("GET /kv/{key}", jsonHandler(func(r *http.Request) (any, int) {
		if r.URL.Query().Get("consistency") == "stale" {
			value, ok := node.Get(r.PathValue("key")); if !ok { return apiError{"key not found"}, http.StatusNotFound }
			return map[string]string{"key":r.PathValue("key"),"value":value,"consistency":"stale"}, http.StatusOK
		}
		value, ok, err := node.Read(r.PathValue("key"))
		if errors.Is(err, raft.ErrNotLeader) { return map[string]string{"error":err.Error(),"leader":node.LeaderAddress()}, http.StatusTemporaryRedirect }
		if err != nil { return apiError{err.Error()}, http.StatusServiceUnavailable }
		if !ok { return apiError{"key not found"}, http.StatusNotFound }
		return map[string]string{"key":r.PathValue("key"),"value":value}, http.StatusOK
	}))
	mux.HandleFunc("GET /status", jsonHandler(func(_ *http.Request) (any, int) { return node.Status(), http.StatusOK }))

	server := &http.Server{Addr:*listen, Handler:mux, ReadHeaderTimeout:3*time.Second}
	go func(){ log.Printf("node %s listening on %s",*id,*listen); if err:=server.ListenAndServe(); err!=nil && err!=http.ErrServerClosed { log.Fatal(err) } }()
	ch:=make(chan os.Signal,1); signal.Notify(ch,syscall.SIGINT,syscall.SIGTERM); <-ch
}

type apiError struct{ Error string `json:"error"` }
type endpoint func(*http.Request)(any,int)
func jsonHandler(fn endpoint) http.HandlerFunc { return func(w http.ResponseWriter,r *http.Request){ out,status:=fn(r); w.Header().Set("Content-Type","application/json"); w.WriteHeader(status); _=json.NewEncoder(w).Encode(out) } }
func env(key,fallback string)string{ if value:=os.Getenv(key); value!=""{return value}; return fallback }
func parsePeers(value string)map[string]string{ out:=map[string]string{}; for _,item:=range strings.Split(value,","){ pair:=strings.SplitN(item,"=",2); if len(pair)!=2{log.Fatalf("invalid peer %q",item)}; out[pair[0]]=strings.TrimRight(pair[1],"/") }; return out }
