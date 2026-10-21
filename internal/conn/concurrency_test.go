package conn

import (
	"context"
	"net"
	"strconv"
	"sync"
	"testing"
	"time"
)

// 覆盖锁顺序：ApplyConfig/Close/storeGatewayConn 走 m.mu → entry.mu，
// 业务拨号必须先取齐快照再进入 entry.mu，不能在持有 entry.mu 时反向获取 m.mu。
// 并发用例在 -race 下执行；若出现锁序死锁，等待超时会直接失败而不是挂满测试超时。
func TestConcurrentApplyConfigProbeAndExclusiveConnection(t *testing.T) {
	listener := startAcceptCounter(t)
	address := listener.addr()
	host, portText, _ := net.SplitHostPort(address)
	port, _ := strconv.Atoi(portText)
	cfg := buildGatewayRuntimeConfig(t, gatewayDevice("gw-1", host, port, 1, "modbus_rtu"))

	manager := newGatewayTestManager(t)

	stop := make(chan struct{})
	var wg sync.WaitGroup
	run := func(fn func()) {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				select {
				case <-stop:
					return
				default:
					fn()
				}
			}
		}()
	}

	for i := 0; i < 2; i++ {
		run(func() {
			if err := manager.ApplyConfig(cfg); err != nil {
				t.Errorf("ApplyConfig: %v", err)
			}
		})
	}
	run(func() {
		manager.CheckGateway(context.Background(), address)
	})
	run(func() {
		// 并发刷新会周期性关闭池内连接，拨号偶发失败是预期行为；
		// 本用例只验证锁序与数据竞争，不把瞬时失败计为缺陷。
		_, release, err := manager.ExclusiveConnection(context.Background(), "SN-gw-1")
		if err == nil {
			release()
		}
	})

	time.Sleep(400 * time.Millisecond)
	close(stop)
	waitForGoroutines(t, &wg, 5*time.Second)
}

func TestConcurrentCloseWithApplyProbeAndExclusiveConnection(t *testing.T) {
	listener := startAcceptCounter(t)
	address := listener.addr()
	host, portText, _ := net.SplitHostPort(address)
	port, _ := strconv.Atoi(portText)
	cfg := buildGatewayRuntimeConfig(t, gatewayDevice("gw-1", host, port, 1, "modbus_rtu"))

	manager := newGatewayTestManager(t)

	stop := make(chan struct{})
	var wg sync.WaitGroup
	run := func(fn func()) {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				select {
				case <-stop:
					return
				default:
					fn()
				}
			}
		}()
	}

	run(func() { _ = manager.ApplyConfig(cfg) })
	run(func() { manager.CheckGateway(context.Background(), address) })
	run(func() {
		_, release, err := manager.ExclusiveConnection(context.Background(), "SN-gw-1")
		if err == nil {
			release()
		}
	})

	time.Sleep(150 * time.Millisecond)
	if err := manager.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	close(stop)
	waitForGoroutines(t, &wg, 5*time.Second)
}

func waitForGoroutines(t *testing.T, wg *sync.WaitGroup, timeout time.Duration) {
	t.Helper()
	done := make(chan struct{})
	go func() {
		wg.Wait()
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(timeout):
		t.Fatalf("concurrent operations did not finish within %s (likely lock-order deadlock)", timeout)
	}
}
