package collector

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

	"multiple-protocol-controller/internal/config"
	"multiple-protocol-controller/internal/store"
)

func TestRecordWritesRealtimeDataUnderDeviceIDKey(t *testing.T) {
	redis := startRedisStub(t)

	client, err := store.NewRedisClient(context.Background(), redis.addr, "", 0)
	if err != nil {
		t.Fatalf("store.NewRedisClient() error = %v", err)
	}
	t.Cleanup(func() { _ = client.Close() })

	writer := &deviceResultWriter{
		redis:     client,
		snapshots: make(map[string]*deviceSnapshot),
	}
	device := config.DeviceRuntime{
		Config: config.DeviceConfig{
			ID:           "device-id-1",
			SerialNumber: "serial-1",
			DeviceName:   "pump",
		},
	}

	if err := writer.record(device, config.ModbusParam{Identify: "temperature"}, 23.5); err != nil {
		t.Fatalf("record() error = %v", err)
	}

	select {
	case key := <-redis.setKeys:
		if key != "device:data:device-id-1" {
			t.Fatalf("SET key = %q, want %q", key, "device:data:device-id-1")
		}
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for redis SET")
	}
}

type redisStub struct {
	addr    string
	setKeys chan string
}

func startRedisStub(t *testing.T) redisStub {
	t.Helper()

	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("net.Listen() error = %v", err)
	}

	stub := redisStub{
		addr:    ln.Addr().String(),
		setKeys: make(chan string, 8),
	}
	t.Cleanup(func() { _ = ln.Close() })

	go func() {
		for {
			conn, err := ln.Accept()
			if err != nil {
				return
			}
			go serveRedisStubConn(conn, stub.setKeys)
		}
	}()

	return stub
}

func serveRedisStubConn(conn net.Conn, setKeys chan<- string) {
	defer conn.Close()
	reader := bufio.NewReader(conn)

	for {
		args, err := readRESPArray(reader)
		if err != nil {
			return
		}
		if len(args) == 0 {
			_, _ = io.WriteString(conn, "-ERR empty command\r\n")
			continue
		}

		switch strings.ToUpper(args[0]) {
		case "HELLO":
			_, _ = io.WriteString(conn, "%7\r\n+server\r\n+redis\r\n+version\r\n+7.0.0\r\n+proto\r\n:3\r\n+id\r\n:1\r\n+mode\r\n+standalone\r\n+role\r\n+master\r\n+modules\r\n*0\r\n")
		case "CLIENT":
			_, _ = io.WriteString(conn, "+OK\r\n")
		case "PING":
			_, _ = io.WriteString(conn, "+PONG\r\n")
		case "SET":
			if len(args) >= 2 {
				setKeys <- args[1]
			}
			_, _ = io.WriteString(conn, "+OK\r\n")
		case "QUIT":
			_, _ = io.WriteString(conn, "+OK\r\n")
			return
		default:
			_, _ = io.WriteString(conn, fmt.Sprintf("-ERR unsupported command %s\r\n", args[0]))
		}
	}
}

func readRESPArray(reader *bufio.Reader) ([]string, error) {
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
		buf := make([]byte, size+2)
		if _, err := io.ReadFull(reader, buf); err != nil {
			return nil, err
		}
		args = append(args, string(buf[:size]))
	}
	return args, nil
}
