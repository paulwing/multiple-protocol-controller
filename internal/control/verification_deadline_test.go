package control

import (
	"context"
	"errors"
	"io"
	"net"
	"testing"
	"time"

	"multiple-protocol-controller/internal/config"
)

func TestExecuteModbusWriteBoundsBlockedSend(t *testing.T) {
	for _, tc := range []struct {
		name            string
		contextTimeout  time.Duration
		responseTimeout int
	}{
		{name: "remaining command timeout", contextTimeout: 50 * time.Millisecond, responseTimeout: 5000},
		{name: "device timeout without context deadline", responseTimeout: 50},
	} {
		t.Run(tc.name, func(t *testing.T) {
			client, server := net.Pipe()
			defer client.Close()
			defer server.Close()
			connection := &countingWriteConn{Conn: client}
			ctx := context.Background()
			if tc.contextTimeout > 0 {
				var cancel context.CancelFunc
				ctx, cancel = context.WithTimeout(ctx, tc.contextTimeout)
				defer cancel()
			}
			completed := make(chan commandExecutionResult, 1)
			go func() {
				completed <- executeModbusWrite(ctx, connection, []byte{1, 5, 0, 0, 255, 0, 140, 58},
					config.DeviceRuntime{SlaveID: 1, ResponseTimeoutMs: tc.responseTimeout},
					config.ModbusCommand{FunctionCode: 5}, nil, true)
			}()

			// The gateway never reads. A context alone cannot unblock net.Conn.Write.
			select {
			case result := <-completed:
				var timeout net.Error
				if result.Status != commandStatusFailed || !errors.As(result.Err, &timeout) || !timeout.Timeout() {
					t.Fatalf("blocked send = %+v, want a timeout failure", result)
				}
				if connection.writes != 1 || connection.reads != 0 {
					t.Fatalf("writes=%d reads=%d, want one send and no ACK read/retry", connection.writes, connection.reads)
				}
			case <-time.After(time.Second):
				_ = client.Close()
				<-completed
				t.Fatal("blocked send outlived the configured timeout")
			}
		})
	}
}

func TestExecuteModbusWriteKeepsSuccessfulACKAndConnectionReusable(t *testing.T) {
	client, server := net.Pipe()
	defer client.Close()
	defer server.Close()
	frame := []byte{1, 5, 0, 0, 255, 0, 140, 58}
	peerDone := make(chan error, 1)
	go func() {
		defer server.Close()
		_ = server.SetDeadline(time.Now().Add(time.Second))
		request := make([]byte, len(frame))
		if _, err := io.ReadFull(server, request); err != nil {
			peerDone <- err
			return
		}
		if _, err := server.Write(request); err != nil {
			peerDone <- err
			return
		}
		_, err := io.ReadFull(server, make([]byte, 1))
		peerDone <- err
	}()
	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()
	result := executeModbusWrite(ctx, client, frame, config.DeviceRuntime{SlaveID: 1, ResponseTimeoutMs: 5000},
		config.ModbusCommand{FunctionCode: 5}, nil, true)
	if result.Status != commandStatusUnverified || result.Err != nil {
		t.Fatalf("successful ACK without a read point = %+v, want unverified", result)
	}
	<-ctx.Done()
	if _, err := client.Write([]byte{42}); err != nil {
		t.Fatalf("command left an expired deadline on the reusable connection: %v", err)
	}
	if err := <-peerDone; err != nil {
		t.Fatal(err)
	}
}

type countingWriteConn struct {
	net.Conn
	writes int
	reads  int
}

func (c *countingWriteConn) Write(payload []byte) (int, error) {
	c.writes++
	return c.Conn.Write(payload)
}

func (c *countingWriteConn) Read(payload []byte) (int, error) {
	c.reads++
	return c.Conn.Read(payload)
}
