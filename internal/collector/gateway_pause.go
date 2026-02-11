package collector

import (
	"context"
	"sync"

	"multiple-protocol-controller/internal/config"
)

type gatewayPauseState struct {
	mu       sync.Mutex
	paused   bool
	refCount int
	resumeCh chan struct{}
}

type gatewayPauseManager struct {
	mu     sync.Mutex
	states map[string]*gatewayPauseState
}

var pauseManager = &gatewayPauseManager{
	states: make(map[string]*gatewayPauseState),
}

func (m *gatewayPauseManager) state(serial string) *gatewayPauseState {
	if serial == "" {
		return nil
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	if st, exists := m.states[serial]; exists {
		return st
	}
	st := &gatewayPauseState{}
	m.states[serial] = st
	return st
}

// PauseGateway signals collectors sharing the same网关 to pause after完成当前采集循环.
func PauseGateway(serial string) {
	state := pauseManager.state(serial)
	if state == nil {
		return
	}
	state.mu.Lock()
	defer state.mu.Unlock()
	state.refCount++
	if state.refCount == 1 {
		state.paused = true
		state.resumeCh = make(chan struct{})
	}
}

// ResumeGateway releases the pause signal issued via PauseGateway.
func ResumeGateway(serial string) {
	state := pauseManager.state(serial)
	if state == nil {
		return
	}
	state.mu.Lock()
	defer state.mu.Unlock()
	if state.refCount == 0 {
		return
	}
	state.refCount--
	if state.refCount == 0 && state.paused {
		state.paused = false
		ch := state.resumeCh
		state.resumeCh = nil
		if ch != nil {
			close(ch)
		}
	}
}

func waitGatewayResume(ctx context.Context, serial string) error {
	if serial == "" {
		return nil
	}
	state := pauseManager.state(serial)
	if state == nil {
		return nil
	}
	for {
		state.mu.Lock()
		if !state.paused {
			state.mu.Unlock()
			return nil
		}
		ch := state.resumeCh
		state.mu.Unlock()

		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ch:
		}
	}
}

// GatewaySerial exposes the网关序列号，供其它模块（如控制指令）复用相同的暂停策略.
func GatewaySerial(device config.DeviceRuntime) string {
	return gatewaySerial(device)
}

func isGatewayPaused(serial string) bool {
	if serial == "" {
		return false
	}
	state := pauseManager.state(serial)
	if state == nil {
		return false
	}
	state.mu.Lock()
	defer state.mu.Unlock()
	return state.paused
}
