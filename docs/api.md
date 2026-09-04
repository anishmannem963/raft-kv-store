# HTTP API

The examples assume node 1 is the current leader. Followers return HTTP 307 for leader-only operations and include their current leader address when known.

## Write a value

`PUT /kv/{key}`

Required headers:

- `Content-Type: application/json`
- `X-Client-ID: <stable-client-id>`
- `X-Request-ID: <unique-request-id>`

```bash
curl -i -X PUT localhost:8081/kv/message \
  -H 'Content-Type: application/json' \
  -H 'X-Client-ID: example-client' \
  -H 'X-Request-ID: request-1' \
  -d '{"value":"hello raft"}'
```

| Status | Meaning |
|---|---|
| 201 | Command committed by a majority |
| 307 | Request reached a follower |
| 400 | Invalid JSON or missing request identifiers |
| 409 | Client/request pair was already used for a different command |
| 503 | A majority could not commit the command |

Retrying the same client ID, request ID, key, and value is safe. Redirects are returned as JSON rather than automatically forwarded by the server.

## Read a value

`GET /kv/{key}` performs a leader-only quorum-confirmed read.

```bash
curl -i localhost:8081/kv/message
```

| Status | Meaning |
|---|---|
| 200 | Value returned after the read barrier |
| 307 | Request reached a follower |
| 404 | Key does not exist in committed state |
| 503 | The leader could not confirm a majority |

`GET /kv/{key}?consistency=stale` reads the local replica without contacting a majority. It is intended for diagnostics and may return older committed state.

## Inspect a node

`GET /status`

```json
{
  "id": "node1",
  "state": "leader",
  "term": 3,
  "leader_id": "node1",
  "commit_index": 120,
  "log_length": 20,
  "snapshot_index": 100,
  "key_count": 119,
  "state_hash": "<sha256>",
  "storage_ok": true
}
```

`state_hash` is a deterministic SHA-256 digest used by the integration and benchmark harnesses to verify replica convergence.

## Internal RPCs

`POST /raft/vote`, `POST /raft/append`, and `POST /raft/snapshot` are internal node-to-node endpoints. They are unauthenticated in this educational implementation and must not be exposed to an untrusted network.
