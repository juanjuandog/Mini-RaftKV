.PHONY: proto test build run-n1 run-n2 run-n3 run-cluster demo bench clean docker-up docker-down

proto:
	$$(go env GOPATH)/bin/buf generate api --template buf.gen.yaml

test:
	go test ./...

build:
	go build -o bin/raftkv ./cmd/raftkv
	go build -o bin/raftkvctl ./cmd/raftkvctl

run-n1:
	go run ./cmd/raftkv -config configs/n1.yaml

run-n2:
	go run ./cmd/raftkv -config configs/n2.yaml

run-n3:
	go run ./cmd/raftkv -config configs/n3.yaml

run-cluster:
	scripts/run-local-cluster.sh

demo:
	scripts/demo-flow.sh

bench:
	scripts/bench.sh

docker-up:
	docker compose up --build

docker-down:
	docker compose down -v

clean:
	rm -rf bin data
