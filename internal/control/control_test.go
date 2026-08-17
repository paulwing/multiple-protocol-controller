package control

import (
	"context"
	"testing"
	"time"
)

func TestWithCommandTimeoutCancelsContextAfterCallback(t *testing.T) {
	var captured context.Context

	withCommandTimeout(time.Minute, func(ctx context.Context) {
		captured = ctx
	})

	if captured == nil {
		t.Fatal("callback did not receive a context")
	}
	select {
	case <-captured.Done():
	case <-time.After(time.Second):
		t.Fatal("command context was not canceled after callback returned")
	}
}
