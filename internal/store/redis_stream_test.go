package store

import (
	"bufio"
	"context"
	"fmt"
	"io"
	"net"
	"strconv"
	"strings"
	"testing"
	"time"
)

func TestSetAndXAddWritesSnapshotAndStreamInOnePipeline(t *testing.T) {
	stub := startStreamRedisStub(t, "+1720000000000-0\r\n")
	client, err := NewRedisClient(context.Background(), stub.addr, "", 0)
	if err != nil {
		t.Fatalf("NewRedisClient() error = %v", err)
	}
	t.Cleanup(func() { _ = client.Close() })

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	result := client.SetAndXAdd(
		ctx,
		"device:data:d1",
		`{"device_id":"d1"}`,
		"judge:source",
		map[string]any{"event_id": "e1"},
	)
	if result.SnapshotErr != nil || result.StreamErr != nil {
		t.Fatalf("SetAndXAdd() result = %#v", result)
	}
	if result.StreamID != "1720000000000-0" {
		t.Fatalf("StreamID = %q, want %q", result.StreamID, "1720000000000-0")
	}

	commands := receiveCommands(t, stub.commands, 2)
	if commands[0].name != "SET" || commands[0].args[1] != "device:data:d1" {
		t.Fatalf("first command = %#v", commands[0])
	}
	if commands[1].name != "XADD" || commands[1].args[1] != "judge:source" {
		t.Fatalf("second command = %#v", commands[1])
	}
}

func TestSetAndXAddReportsCommandErrorsSeparately(t *testing.T) {
	stub := startStreamRedisStub(t, "-WRONGTYPE Operation against a key holding the wrong kind of value\r\n")
	client, err := NewRedisClient(context.Background(), stub.addr, "", 0)
	if err != nil {
		t.Fatalf("NewRedisClient() error = %v", err)
	}
	t.Cleanup(func() { _ = client.Close() })

	result := client.SetAndXAdd(
		context.Background(),
		"device:data:d1",
		`{}`,
		"judge:source",
		map[string]any{"event_id": "e1"},
	)
	if result.SnapshotErr != nil {
		t.Fatalf("SnapshotErr = %v, want nil", result.SnapshotErr)
	}
	if result.StreamErr == nil {
		t.Fatal("StreamErr = nil, want WRONGTYPE")
	}
}

func TestXAddDoesNotSendTrimmingArguments(t *testing.T) {
	stub := startStreamRedisStub(t, "+1720000000001-0\r\n")
	client, err := NewRedisClient(context.Background(), stub.addr, "", 0)
	if err != nil {
		t.Fatalf("NewRedisClient() error = %v", err)
	}
	t.Cleanup(func() { _ = client.Close() })

	streamID, err := client.XAdd(context.Background(), "judge:source", map[string]any{"event_id": "e1"})
	if err != nil {
		t.Fatalf("XAdd() error = %v", err)
	}
	if streamID != "1720000000001-0" {
		t.Fatalf("streamID = %q", streamID)
	}

	command := receiveCommands(t, stub.commands, 1)[0]
	want := []string{"xadd", "judge:source", "*", "event_id", "e1"}
	if len(command.args) != len(want) {
		t.Fatalf("XADD args = %#v, want %#v", command.args, want)
	}
	for i := range want {
		if command.args[i] != want[i] {
			t.Fatalf("XADD args = %#v, want %#v", command.args, want)
		}
	}
}

type streamRedisCommand struct {
	name string
	args []string
}

type streamRedisStub struct {
	addr     string
	commands chan streamRedisCommand
}

func startStreamRedisStub(t *testing.T, xaddReply string) streamRedisStub {
	t.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("net.Listen() error = %v", err)
	}
	stub := streamRedisStub{
		addr:     listener.Addr().String(),
		commands: make(chan streamRedisCommand, 16),
	}
	t.Cleanup(func() { _ = listener.Close() })

	go func() {
		for {
			conn, err := listener.Accept()
			if err != nil {
				return
			}
			go serveStreamRedisStubConn(conn, stub.commands, xaddReply)
		}
	}()
	return stub
}

func serveStreamRedisStubConn(conn net.Conn, commands chan<- streamRedisCommand, xaddReply string) {
	defer conn.Close()
	reader := bufio.NewReader(conn)
	for {
		args, err := readStreamRESPArray(reader)
		if err != nil {
			return
		}
		if len(args) == 0 {
			_, _ = io.WriteString(conn, "-ERR empty command\r\n")
			continue
		}

		name := strings.ToUpper(args[0])
		switch name {
		case "HELLO":
			_, _ = io.WriteString(conn, "%7\r\n+server\r\n+redis\r\n+version\r\n+7.0.0\r\n+proto\r\n:3\r\n+id\r\n:1\r\n+mode\r\n+standalone\r\n+role\r\n+master\r\n+modules\r\n*0\r\n")
		case "CLIENT":
			_, _ = io.WriteString(conn, "+OK\r\n")
		case "PING":
			_, _ = io.WriteString(conn, "+PONG\r\n")
		case "SET":
			commands <- streamRedisCommand{name: name, args: args}
			_, _ = io.WriteString(conn, "+OK\r\n")
		case "XADD":
			commands <- streamRedisCommand{name: name, args: args}
			_, _ = io.WriteString(conn, xaddReply)
		case "QUIT":
			_, _ = io.WriteString(conn, "+OK\r\n")
			return
		default:
			_, _ = io.WriteString(conn, fmt.Sprintf("-ERR unsupported command %s\r\n", name))
		}
	}
}

func readStreamRESPArray(reader *bufio.Reader) ([]string, error) {
	line, err := reader.ReadString('\n')
	if err != nil {
		return nil, err
	}
	line = strings.TrimSuffix(strings.TrimSuffix(line, "\n"), "\r")
	if !strings.HasPrefix(line, "*") {
		return nil, fmt.Errorf("expected array, got %q", line)
	}
	count, err := strconv.Atoi(strings.TrimPrefix(line, "*"))
	if err != nil {
		return nil, err
	}

	args := make([]string, 0, count)
	for i := 0; i < count; i++ {
		bulkHeader, err := reader.ReadString('\n')
		if err != nil {
			return nil, err
		}
		bulkHeader = strings.TrimSuffix(strings.TrimSuffix(bulkHeader, "\n"), "\r")
		if !strings.HasPrefix(bulkHeader, "$") {
			return nil, fmt.Errorf("expected bulk string, got %q", bulkHeader)
		}
		size, err := strconv.Atoi(strings.TrimPrefix(bulkHeader, "$"))
		if err != nil {
			return nil, err
		}
		buffer := make([]byte, size+2)
		if _, err := io.ReadFull(reader, buffer); err != nil {
			return nil, err
		}
		args = append(args, string(buffer[:size]))
	}
	return args, nil
}

func receiveCommands(t *testing.T, commands <-chan streamRedisCommand, count int) []streamRedisCommand {
	t.Helper()
	result := make([]streamRedisCommand, 0, count)
	deadline := time.NewTimer(2 * time.Second)
	defer deadline.Stop()
	for len(result) < count {
		select {
		case command := <-commands:
			result = append(result, command)
		case <-deadline.C:
			t.Fatalf("timed out after %d/%d commands", len(result), count)
		}
	}
	return result
}
