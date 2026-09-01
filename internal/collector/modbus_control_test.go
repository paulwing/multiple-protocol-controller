package collector

import (
	"context"
	"multiple-protocol-controller/internal/config"
	"net"
	"strings"
	"testing"
	"time"
)

func TestWaitCommandAckRejectsResponseForDifferentRegister(t *testing.T) {
	client, server := net.Pipe()
	defer client.Close()
	defer server.Close()

	go func() {
		response := appendCRC([]byte{1, 5, 0, 2, 0xff, 0})
		_, _ = server.Write(response)
	}()

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	err := WaitCommandAck(ctx, client, config.DeviceRuntime{SlaveID: 1, ResponseTimeoutMs: 100}, config.ModbusCommand{
		FunctionCode: 5,
		Address:      1,
	})

	if err == nil || !strings.Contains(err.Error(), "address mismatch") {
		t.Fatalf("WaitCommandAck() error = %v, want address mismatch", err)
	}
}
