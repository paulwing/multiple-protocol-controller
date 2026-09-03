package store

import (
	"bufio"
	"context"
	"fmt"
	"io"
	"net"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"
)

func TestJudgePublishClientIsLazyAndUsesBoundedCommandPolicy(t *testing.T) {
	client := NewRedisPublishClient("127.0.0.1:0", "", 0, 200*time.Millisecond)
	defer client.Close()
	if got := client.rdb.PoolStats().TotalConns; got != 0 {
		t.Fatalf("constructor opened %d Redis connections", got)
	}
	options := client.rdb.Options()
	if !options.ContextTimeoutEnabled || options.MaxRetries != 0 {
		t.Fatal("publisher must honor the caller deadline and disable command retries")
	}
	if options.ReadTimeout != 200*time.Millisecond || options.WriteTimeout != 200*time.Millisecond || options.PoolTimeout != 200*time.Millisecond || options.DialTimeout != 200*time.Millisecond {
		t.Fatal("publisher did not retain its configured per-stage fallback limits")
	}
}

func TestJudgePublishNetworkPhasesShareTheCallerDeadline(t *testing.T) {
	client := NewRedisPublishClient("unused:6379", "", 0, time.Second)
	clientEnd, serverEnd := net.Pipe()
	connection := &publishDeadlineConn{Conn: clientEnd}
	done := make(chan struct{})
	go func() { defer close(done); servePublishTestConnection(serverEnd) }()
	client.rdb.AddHook(publishConnectionHook{connection: connection})
	t.Cleanup(func() { _ = client.Close(); _ = clientEnd.Close(); _ = serverEnd.Close(); <-done })
	ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
	defer cancel()
	deadline, _ := ctx.Deadline()
	count, err := client.PublishWithSubscriberCount(ctx, "iot:judge:test", []byte("event"))
	if err != nil || count != 1 {
		t.Fatalf("publish = %d, %v", count, err)
	}
	connection.mu.Lock()
	defer connection.mu.Unlock()
	if len(connection.reads) < 2 || len(connection.writes) < 2 {
		t.Fatal("did not observe connection setup and PUBLISH I/O")
	}
	for _, deadlines := range [][]time.Time{connection.reads, connection.writes} {
		for _, got := range deadlines {
			if got.IsZero() || got.After(deadline) {
				t.Fatalf("network phase deadline %v exceeded shared deadline %v", got, deadline)
			}
		}
	}
}

type publishDeadlineConn struct {
	net.Conn
	mu            sync.Mutex
	reads, writes []time.Time
}

func (conn *publishDeadlineConn) SetReadDeadline(deadline time.Time) error {
	conn.mu.Lock()
	conn.reads = append(conn.reads, deadline)
	conn.mu.Unlock()
	return conn.Conn.SetReadDeadline(deadline)
}

func (conn *publishDeadlineConn) SetWriteDeadline(deadline time.Time) error {
	conn.mu.Lock()
	conn.writes = append(conn.writes, deadline)
	conn.mu.Unlock()
	return conn.Conn.SetWriteDeadline(deadline)
}

type publishConnectionHook struct{ connection net.Conn }

func (hook publishConnectionHook) DialHook(redis.DialHook) redis.DialHook {
	return func(context.Context, string, string) (net.Conn, error) { return hook.connection, nil }
}
func (publishConnectionHook) ProcessHook(next redis.ProcessHook) redis.ProcessHook { return next }
func (publishConnectionHook) ProcessPipelineHook(next redis.ProcessPipelineHook) redis.ProcessPipelineHook {
	return next
}

// This in-memory peer implements only the setup commands and PUBLISH used by
// this client. No TCP listener, external Redis or persistent data is involved.
func servePublishTestConnection(conn net.Conn) {
	defer conn.Close()
	reader := bufio.NewReader(conn)
	for {
		line, err := reader.ReadString('\n')
		if err != nil || !strings.HasPrefix(line, "*") {
			return
		}
		count, err := strconv.Atoi(strings.TrimSpace(line[1:]))
		if err != nil || count <= 0 {
			return
		}
		args := make([]string, count)
		for index := range args {
			lengthLine, err := reader.ReadString('\n')
			if err != nil || !strings.HasPrefix(lengthLine, "$") {
				return
			}
			length, err := strconv.Atoi(strings.TrimSpace(lengthLine[1:]))
			if err != nil || length < 0 {
				return
			}
			data := make([]byte, length+2)
			if _, err := io.ReadFull(reader, data); err != nil {
				return
			}
			args[index] = string(data[:length])
		}
		reply := "+OK\r\n"
		switch strings.ToLower(args[0]) {
		case "hello":
			reply = "%1\r\n+proto\r\n:3\r\n"
		case "publish":
			reply = ":1\r\n"
		}
		if _, err := fmt.Fprint(conn, reply); err != nil {
			return
		}
	}
}
