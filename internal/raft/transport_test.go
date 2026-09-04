package raft

import (
	"net/http"
	"testing"
	"time"
)

func TestHTTPTransportUsesCurrentNetworkPath(t *testing.T) {
	transport := NewHTTPTransport(time.Second)
	roundTripper, ok := transport.client.Transport.(*http.Transport)
	if !ok || !roundTripper.DisableKeepAlives {
		t.Fatal("Raft RPC transport must not reuse connections across network partitions")
	}
}
