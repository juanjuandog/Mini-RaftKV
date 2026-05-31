package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"sort"
	"strings"
	"text/tabwriter"
	"time"

	"github.com/juanjuandog/mini-raftkv/internal/pb"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

type debugStatus struct {
	ID            string `json:"id"`
	State         string `json:"state"`
	LeaderID      string `json:"leaderId"`
	CurrentTerm   uint64 `json:"currentTerm"`
	CommitIndex   uint64 `json:"commitIndex"`
	LastApplied   uint64 `json:"lastApplied"`
	SnapshotIndex uint64 `json:"snapshotIndex"`
	Error         string `json:"-"`
	Addr          string `json:"-"`
}

func main() {
	addr := flag.String("addr", "127.0.0.1:7001", "raftkv node address")
	clientID := flag.String("client", "raftkvctl", "client id for idempotency")
	requestID := flag.Uint64("request", uint64(time.Now().UnixNano()), "request id for put/delete idempotency")
	flag.Parse()

	if flag.NArg() < 1 {
		usage()
		os.Exit(2)
	}

	switch flag.Arg(0) {
	case "status":
		runStatus(flag.Args()[1:], os.Stdout)
		return
	case "cluster":
		runCluster(flag.Args()[1:], os.Stdout)
		return
	case "watch":
		runWatch(flag.Args()[1:], os.Stdout)
		return
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

func runStatus(args []string, out io.Writer) {
	fs := flag.NewFlagSet("status", flag.ExitOnError)
	httpAddr := fs.String("http", "127.0.0.1:9001", "debug HTTP address")
	_ = fs.Parse(args)

	status, err := fetchStatus(http.DefaultClient, *httpAddr)
	if err != nil {
		log.Fatalf("status: %v", err)
	}
	printStatuses(out, []debugStatus{status})
}

func runCluster(args []string, out io.Writer) {
	fs := flag.NewFlagSet("cluster", flag.ExitOnError)
	httpAddrs := fs.String("http-addrs", "127.0.0.1:9001,127.0.0.1:9002,127.0.0.1:9003", "comma-separated debug HTTP addresses")
	_ = fs.Parse(args)

	statuses := collectStatuses(http.DefaultClient, splitAddrs(*httpAddrs))
	printStatuses(out, statuses)
}

func runWatch(args []string, out io.Writer) {
	fs := flag.NewFlagSet("watch", flag.ExitOnError)
	httpAddrs := fs.String("http-addrs", "127.0.0.1:9001,127.0.0.1:9002,127.0.0.1:9003", "comma-separated debug HTTP addresses")
	interval := fs.Duration("interval", time.Second, "refresh interval")
	count := fs.Int("count", 0, "refresh count, 0 means forever")
	_ = fs.Parse(args)

	addrs := splitAddrs(*httpAddrs)
	for i := 0; *count == 0 || i < *count; i++ {
		if i > 0 {
			fmt.Fprintln(out)
		}
		fmt.Fprintf(out, "time=%s\n", time.Now().Format(time.RFC3339))
		printStatuses(out, collectStatuses(http.DefaultClient, addrs))
		if *count != 0 && i == *count-1 {
			return
		}
		time.Sleep(*interval)
	}
}

func fetchStatus(client *http.Client, addr string) (debugStatus, error) {
	url := "http://" + strings.TrimPrefix(strings.TrimSpace(addr), "http://") + "/debug/status"
	resp, err := client.Get(url)
	if err != nil {
		return debugStatus{Addr: addr, Error: err.Error()}, err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return debugStatus{Addr: addr, Error: resp.Status}, fmt.Errorf("%s returned %s", addr, resp.Status)
	}
	var status debugStatus
	if err := json.NewDecoder(resp.Body).Decode(&status); err != nil {
		return debugStatus{Addr: addr, Error: err.Error()}, err
	}
	status.Addr = addr
	return status, nil
}

func collectStatuses(client *http.Client, addrs []string) []debugStatus {
	statuses := make([]debugStatus, 0, len(addrs))
	for _, addr := range addrs {
		status, err := fetchStatus(client, addr)
		if err != nil {
			statuses = append(statuses, debugStatus{Addr: addr, Error: err.Error()})
			continue
		}
		statuses = append(statuses, status)
	}
	sort.Slice(statuses, func(i, j int) bool {
		return statuses[i].Addr < statuses[j].Addr
	})
	return statuses
}

func splitAddrs(raw string) []string {
	parts := strings.Split(raw, ",")
	addrs := make([]string, 0, len(parts))
	for _, part := range parts {
		addr := strings.TrimSpace(part)
		if addr != "" {
			addrs = append(addrs, addr)
		}
	}
	return addrs
}

func printStatuses(out io.Writer, statuses []debugStatus) {
	w := tabwriter.NewWriter(out, 0, 0, 2, ' ', 0)
	fmt.Fprintln(w, "ADDR\tNODE\tSTATE\tLEADER\tTERM\tCOMMIT\tAPPLIED\tSNAPSHOT\tERROR")
	for _, status := range statuses {
		fmt.Fprintf(w, "%s\t%s\t%s\t%s\t%d\t%d\t%d\t%d\t%s\n",
			status.Addr,
			status.ID,
			status.State,
			status.LeaderID,
			status.CurrentTerm,
			status.CommitIndex,
			status.LastApplied,
			status.SnapshotIndex,
			status.Error,
		)
	}
	_ = w.Flush()
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
	fmt.Fprintln(os.Stderr, "  raftkvctl status -http 127.0.0.1:9001")
	fmt.Fprintln(os.Stderr, "  raftkvctl cluster -http-addrs 127.0.0.1:9001,127.0.0.1:9002,127.0.0.1:9003")
	fmt.Fprintln(os.Stderr, "  raftkvctl watch -http-addrs 127.0.0.1:9001,127.0.0.1:9002,127.0.0.1:9003")
	fmt.Fprintln(os.Stderr, "optional:")
	fmt.Fprintln(os.Stderr, "  -client <client-id> -request <request-id>")
}
