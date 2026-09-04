# Contributing

## Development setup

Install Go 1.23+, Docker with Compose, Bash, and `curl`.

```bash
go test -race ./...
docker compose up --build --detach
./scripts/smoke-test.sh
docker compose down --volumes
```

## Before opening a pull request

```bash
gofmt -w .
go vet ./...
go test -race -count=5 ./...
```

Changes to consensus, persistence, or transport behavior should include a focused regression test and, when applicable, a live three-node scenario. Changes to benchmark or fault tooling must preserve machine-readable output and convergence assertions.

Use small commits with imperative subjects. In a pull request, explain the invariant being protected, the failure mode being tested, and the exact verification performed. Do not present timeout ceilings as measured performance.

## Reporting bugs

Include the commit SHA, Go and Docker versions, the exact command, node status responses, and relevant container logs. For correctness reports, state whether the result came from a linearizable or explicit stale read.
