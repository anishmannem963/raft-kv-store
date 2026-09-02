FROM golang:1.23-alpine AS build
WORKDIR /src
COPY go.mod ./
COPY cmd ./cmd
COPY internal ./internal
RUN CGO_ENABLED=0 go build -trimpath -o /raft-node ./cmd/node

FROM alpine:3.20
RUN adduser -D -u 10001 raft && mkdir /data && chown raft:raft /data
USER raft
COPY --from=build /raft-node /usr/local/bin/raft-node
ENV DATA_DIR=/data
ENTRYPOINT ["raft-node"]
