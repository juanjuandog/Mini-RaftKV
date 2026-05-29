FROM golang:1.26-alpine AS build

WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN go build -o /out/raftkv ./cmd/raftkv
RUN go build -o /out/raftkvctl ./cmd/raftkvctl

FROM alpine:3.22

WORKDIR /app
COPY --from=build /out/raftkv /usr/local/bin/raftkv
COPY --from=build /out/raftkvctl /usr/local/bin/raftkvctl
COPY configs ./configs

ENTRYPOINT ["raftkv"]
