package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"os"
	"time"

	"github.com/juanjuandog/mini-raftkv/internal/pb"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

func main() {
	addr := flag.String("addr", "127.0.0.1:7001", "raftkv node address")
	clientID := flag.String("client", "raftkvctl", "client id for idempotency")
	requestID := flag.Uint64("request", uint64(time.Now().UnixNano()), "request id for put/delete idempotency")
	flag.Parse()

	if flag.NArg() < 1 {
		usage()
		os.Exit(2)
	}

	client, closeFn, err := newClient(*addr)
	if err != nil {
		log.Fatalf("connect: %v", err)
	}
	defer closeFn()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	switch flag.Arg(0) {
	case "get":
		requireArgs(2)
		resp, err := client.Get(ctx, &pb.GetRequest{Key: flag.Arg(1)})
		if err != nil {
			log.Fatalf("get: %v", err)
		}
		if resp.Error != "" {
			log.Fatalf("get: %s leader=%s", resp.Error, resp.LeaderId)
		}
		if !resp.Found {
			fmt.Println("NOT_FOUND")
			return
		}
		fmt.Println(resp.Value)
	case "put":
		requireArgs(3)
		resp, err := client.Put(ctx, &pb.PutRequest{
			Key: flag.Arg(1), Value: flag.Arg(2), ClientId: *clientID, RequestId: *requestID,
		})
		printMutation("put", resp, err)
	case "delete":
		requireArgs(2)
		resp, err := client.Delete(ctx, &pb.DeleteRequest{
			Key: flag.Arg(1), ClientId: *clientID, RequestId: *requestID,
		})
		printMutation("delete", resp, err)
	default:
		usage()
		os.Exit(2)
	}
}

func newClient(addr string) (pb.RaftKVClient, func(), error) {
	conn, err := grpc.NewClient(addr,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	if err != nil {
		return nil, func() {}, err
	}
	return pb.NewRaftKVClient(conn), func() { _ = conn.Close() }, nil
}

func printMutation(op string, resp *pb.MutateResponse, err error) {
	if err != nil {
		log.Fatalf("%s: %v", op, err)
	}
	if resp.Error != "" {
		log.Fatalf("%s: %s leader=%s", op, resp.Error, resp.LeaderId)
	}
	fmt.Printf("OK leader=%s\n", resp.LeaderId)
}

func requireArgs(n int) {
	if flag.NArg() != n {
		usage()
		os.Exit(2)
	}
}

func usage() {
	fmt.Fprintln(os.Stderr, "usage:")
	fmt.Fprintln(os.Stderr, "  raftkvctl -addr 127.0.0.1:7001 put <key> <value>")
	fmt.Fprintln(os.Stderr, "  raftkvctl -addr 127.0.0.1:7001 get <key>")
	fmt.Fprintln(os.Stderr, "  raftkvctl -addr 127.0.0.1:7001 delete <key>")
	fmt.Fprintln(os.Stderr, "optional:")
	fmt.Fprintln(os.Stderr, "  -client <client-id> -request <request-id>")
}
