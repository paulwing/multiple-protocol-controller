package conn

import (
	"context"
	"errors"
	"net"
	"sort"
	"strconv"
	"sync"
	"testing"
	"time"

	"go.uber.org/zap"

	"multiple-protocol-controller/internal/config"
	"multiple-protocol-controller/pkg/logger"
)

func TestBuildGatewayTargetsGroupsSharedEndpointAndFiltersOthers(t *testing.T) {
	cfg := buildGatewayRuntimeConfig(t,
		gatewayDevice("gw-1", "127.0.0.1", 1502, 1, "modbus_rtu"),
		gatewayDevice("gw-2", "127.0.0.1", 1502, 1, "MODBUS_TCP"),
		gatewayDevice("gw-3", "127.0.0.1", 1503, 1, "modbus_rtu"),
		// 自带网关、普通设备和其它协议都不在本次范围内。
		gatewayDevice("gw-type2", "127.0.0.1", 1504, 2, "modbus_rtu"),
		gatewayDevice("normal", "127.0.0.1", 1505, 0, "modbus_rtu"),
		gatewayDevice("opcua-gw", "127.0.0.1", 1506, 1, "opcua"),
	)

	targets := buildGatewayTargets(cfg)
	if len(targets) != 2 {
		t.Fatalf("targets = %#v, want 2 endpoints", targets)
	}

	byKey := make(map[string][]string, len(targets))
	for _, target := range targets {
		byKey[target.Key] = append([]string{}, target.DeviceIDs...)
		sort.Strings(byKey[target.Key])
	}
	if got := byKey["127.0.0.1:1502"]; len(got) != 2 || got[0] != "gw-1" || got[1] != "gw-2" {
		t.Fatalf("shared endpoint IDs = %#v, want [gw-1 gw-2]", got)
	}
	if got := byKey["127.0.0.1:1503"]; len(got) != 1 || got[0] != "gw-3" {
		t.Fatalf("single endpoint IDs = %#v, want [gw-3]", got)
	}
}

func TestCheckGatewayDialsOnceAndReusesPooledConnection(t *testing.T) {
	listener := startAcceptCounter(t)
	manager := newGatewayTestManager(t)
	setGatewayTestTarget(manager, listener.addr())

	inspects := 0
	manager.inspectSocket = func(net.Conn) (socketVerdict, error) {
		inspects++
		return socketHealthy, nil
	}

	first := manager.CheckGateway(context.Background(), listener.addr())
	if first.State != GatewayProbeHealthy {
		t.Fatalf("first state = %v, want healthy", first.State)
	}
	listener.waitFor(t, 1)

	second := manager.CheckGateway(context.Background(), listener.addr())
	if second.State != GatewayProbeHealthy {
		t.Fatalf("second state = %v, want healthy", second.State)
	}
	listener.assertNoMore(t, 1)
	if inspects != 1 {
		t.Fatalf("socket inspections = %d, want 1", inspects)
	}
}

func TestCheckGatewayBusyWithoutConnectionSkipsDialingAndInspecting(t *testing.T) {
	listener := startAcceptCounter(t)
	manager := newGatewayTestManager(t)
	setGatewayTestTarget(manager, listener.addr())
	manager.getGatewayGuard(listener.addr()).markBusy()
	defer manager.getGatewayGuard(listener.addr()).clearBusy()

	inspected := false
	manager.inspectSocket = func(net.Conn) (socketVerdict, error) {
		inspected = true
		return socketHealthy, nil
	}

	result := manager.CheckGateway(context.Background(), listener.addr())
	if result.State != GatewayProbeBusy {
		t.Fatalf("state = %v, want busy", result.State)
	}
	listener.assertNoMore(t, 0)
	if inspected {
		t.Fatalf("busy gateway without connection must not be inspected")
	}
}

func TestCheckGatewayBusyWithConnectionStillUsesKernelState(t *testing.T) {
	listener := startAcceptCounter(t)
	manager := newGatewayTestManager(t)
	setGatewayTestTarget(manager, listener.addr())

	if result := manager.CheckGateway(context.Background(), listener.addr()); result.State != GatewayProbeHealthy {
		t.Fatalf("initial state = %v, want healthy", result.State)
	}
	listener.waitFor(t, 1)

	guard := manager.getGatewayGuard(listener.addr())
	guard.markBusy()
	defer guard.clearBusy()

	inspected := 0
	manager.inspectSocket = func(net.Conn) (socketVerdict, error) {
		inspected++
		return socketHealthy, nil
	}
	result := manager.CheckGateway(context.Background(), listener.addr())
	if result.State != GatewayProbeHealthy {
		t.Fatalf("busy-with-connection state = %v, want healthy from kernel state", result.State)
	}
	if inspected != 1 {
		t.Fatalf("busy-with-connection inspections = %d, want 1", inspected)
	}
	listener.assertNoMore(t, 1)
	if conn, _ := manager.pooledGatewayConn(listener.addr()); conn == nil {
		t.Fatalf("busy-with-connection check must not drop the pooled connection")
	}

	// 业务持锁时即使判定断开，也不得在健康检查里关闭/重建连接。
	manager.inspectSocket = func(net.Conn) (socketVerdict, error) {
		return socketDead, nil
	}
	result = manager.CheckGateway(context.Background(), listener.addr())
	if result.State != GatewayProbeUnhealthy {
		t.Fatalf("busy-with-dead-connection state = %v, want unhealthy", result.State)
	}
	listener.assertNoMore(t, 1)
	if conn, _ := manager.pooledGatewayConn(listener.addr()); conn == nil {
		t.Fatalf("busy-with-dead-connection check must leave the connection to the business path")
	}
}

func TestCheckGatewayUnknownSocketDoesNotRenewHealth(t *testing.T) {
	listener := startAcceptCounter(t)
	manager := newGatewayTestManager(t)
	setGatewayTestTarget(manager, listener.addr())

	if result := manager.CheckGateway(context.Background(), listener.addr()); result.State != GatewayProbeHealthy {
		t.Fatalf("initial state = %v, want healthy", result.State)
	}

	manager.inspectSocket = func(net.Conn) (socketVerdict, error) {
		return socketUnknown, nil
	}
	result := manager.CheckGateway(context.Background(), listener.addr())
	if result.State != GatewayProbeUnknown {
		t.Fatalf("state = %v, want unknown (cached conn must not prove health)", result.State)
	}
	listener.assertNoMore(t, 1)
}

func TestCheckGatewayReplacesDeadConnectionAfterRedial(t *testing.T) {
	listener := startAcceptCounter(t)
	manager := newGatewayTestManager(t)
	setGatewayTestTarget(manager, listener.addr())

	if result := manager.CheckGateway(context.Background(), listener.addr()); result.State != GatewayProbeHealthy {
		t.Fatalf("initial state = %v, want healthy", result.State)
	}
	listener.waitFor(t, 1)

	manager.inspectSocket = func(net.Conn) (socketVerdict, error) {
		return socketDead, nil
	}
	result := manager.CheckGateway(context.Background(), listener.addr())
	if result.State != GatewayProbeHealthy {
		t.Fatalf("state = %v, want healthy after replacing dead connection", result.State)
	}
	listener.waitFor(t, 2)
}

func TestCheckGatewayRequiresTwoConsecutiveDialFailures(t *testing.T) {
	_, address := startAndCloseListener(t)
	manager := newGatewayTestManager(t)
	setGatewayTestTarget(manager, address)

	first := manager.CheckGateway(context.Background(), address)
	if first.State != GatewayProbeUnknown {
		t.Fatalf("first dial failure state = %v, want unknown before debounce", first.State)
	}
	second := manager.CheckGateway(context.Background(), address)
	if second.State != GatewayProbeUnhealthy {
		t.Fatalf("second dial failure state = %v, want unhealthy", second.State)
	}

	// 端口恢复后下一轮重新拨号并回到在线。
	rebound, err := net.Listen("tcp", address)
	if err != nil {
		t.Skipf("cannot rebind %s in this environment: %v", address, err)
	}
	defer rebound.Close()
	go func() {
		for {
			conn, acceptErr := rebound.Accept()
			if acceptErr != nil {
				return
			}
			defer conn.Close()
		}
	}()

	recovered := manager.CheckGateway(context.Background(), address)
	if recovered.State != GatewayProbeHealthy {
		t.Fatalf("recovered state = %v, want healthy", recovered.State)
	}
}

func TestCheckGatewayStaleTargetIsNotCheckedOrWritten(t *testing.T) {
	listener := startAcceptCounter(t)
	manager := newGatewayTestManager(t)
	setGatewayTestTarget(manager, listener.addr())
	manager.gatewayTargets = nil

	result := manager.CheckGateway(context.Background(), listener.addr())
	if result.State != GatewayProbeStale {
		t.Fatalf("state = %v, want stale", result.State)
	}
	listener.assertNoMore(t, 0)
}

func TestApplyConfigSkipsPreDialForPureGatewayEndpoints(t *testing.T) {
	listener := startAcceptCounter(t)
	host, portText, _ := net.SplitHostPort(listener.addr())
	port, _ := strconv.Atoi(portText)
	manager := newGatewayTestManager(t)
	cfg := buildGatewayRuntimeConfig(t, gatewayDevice("gw-1", host, port, 1, "modbus_rtu"))

	if err := manager.ApplyConfig(cfg); err != nil {
		t.Fatalf("ApplyConfig: %v", err)
	}

	// 纯网关端点由健康检查独占拨号，配置刷新不得额外探测连接。
	listener.assertNoMore(t, 0)
}

func TestKeepAliveOnlyAppliedToPureGatewayEndpoints(t *testing.T) {
	pureListener := startAcceptCounter(t)
	normalListener := startAcceptCounter(t)
	pureHost, purePortText, _ := net.SplitHostPort(pureListener.addr())
	purePort, _ := strconv.Atoi(purePortText)
	normalHost, normalPortText, _ := net.SplitHostPort(normalListener.addr())
	normalPort, _ := strconv.Atoi(normalPortText)

	cfg := buildGatewayRuntimeConfig(t,
		gatewayDevice("gw-1", pureHost, purePort, 1, "modbus_rtu"),
		gatewayDevice("dev-1", normalHost, normalPort, 0, "modbus_rtu"),
	)
	manager := newGatewayTestManager(t)

	var mu sync.Mutex
	keepAliveCalls := 0
	manager.setGatewayKeepAlive = func(*net.TCPConn) error {
		mu.Lock()
		keepAliveCalls++
		mu.Unlock()
		return nil
	}
	if err := manager.ApplyConfig(cfg); err != nil {
		t.Fatalf("ApplyConfig: %v", err)
	}

	_, release, err := manager.ExclusiveConnection(context.Background(), "SN-gw-1")
	if err != nil {
		t.Fatalf("pure gateway connection: %v", err)
	}
	release()
	mu.Lock()
	if keepAliveCalls != 1 {
		t.Fatalf("keepalive calls after pure gateway dial = %d, want 1", keepAliveCalls)
	}
	mu.Unlock()

	_, release, err = manager.ExclusiveConnection(context.Background(), "SN-dev-1")
	if err != nil {
		t.Fatalf("normal device connection: %v", err)
	}
	release()
	mu.Lock()
	defer mu.Unlock()
	if keepAliveCalls != 1 {
		t.Fatalf("keepalive must not be applied to type0/type2 endpoints, calls = %d", keepAliveCalls)
	}
}

func TestCheckGatewayKeepAliveFailureNeverReportsOnline(t *testing.T) {
	listener := startAcceptCounter(t)
	manager := newGatewayTestManager(t)
	setGatewayTestTarget(manager, listener.addr())
	manager.setGatewayKeepAlive = func(*net.TCPConn) error {
		return errors.New("keepalive not supported")
	}

	result := manager.CheckGateway(context.Background(), listener.addr())
	if result.State != GatewayProbeUnknown {
		t.Fatalf("state = %v, want unknown when keepalive cannot be configured", result.State)
	}
	listener.waitFor(t, 1)
	if conn, _ := manager.pooledGatewayConn(listener.addr()); conn != nil {
		t.Fatalf("connection without keepalive must not be left in the pool")
	}

	// 下一轮重新拨号并再次失败，仍不得报在线。
	result = manager.CheckGateway(context.Background(), listener.addr())
	if result.State != GatewayProbeUnknown {
		t.Fatalf("second state = %v, want unknown", result.State)
	}
	listener.waitFor(t, 2)
}

func TestBusinessConnectionWithoutKeepAliveIsRebuiltByHealthCheck(t *testing.T) {
	listener := startAcceptCounter(t)
	manager := newGatewayTestManager(t)
	setGatewayTestTarget(manager, listener.addr())
	manager.setGatewayKeepAlive = func(*net.TCPConn) error {
		return errors.New("keepalive not supported")
	}

	_, release, err := manager.ExclusiveConnection(context.Background(), "SN-gw-1")
	if err != nil {
		t.Fatalf("ExclusiveConnection: %v", err)
	}
	release()

	result := manager.CheckGateway(context.Background(), listener.addr())
	if result.State != GatewayProbeUnknown {
		t.Fatalf("state = %v, want unknown for business conn without keepalive", result.State)
	}
	listener.waitFor(t, 2)
	if conn, _ := manager.pooledGatewayConn(listener.addr()); conn != nil {
		t.Fatalf("health check must not retain a connection whose keepalive failed")
	}
}

func TestStoreGatewayConnRejectsStaleGeneration(t *testing.T) {
	manager := newGatewayTestManager(t)
	setGatewayTestTarget(manager, "127.0.0.1:1502")
	staleGeneration := manager.ConfigGeneration()

	manager.mu.Lock()
	manager.configGeneration++
	manager.mu.Unlock()

	client, server := net.Pipe()
	defer client.Close()
	defer server.Close()

	if manager.storeGatewayConn("127.0.0.1:1502", client, staleGeneration, true) {
		t.Fatalf("stale generation must not be published into the pool")
	}
	if conn, _ := manager.pooledGatewayConn("127.0.0.1:1502"); conn != nil {
		t.Fatalf("stale connection must not be stored")
	}
}

func TestCheckGatewayResultCarriesGeneration(t *testing.T) {
	listener := startAcceptCounter(t)
	manager := newGatewayTestManager(t)
	setGatewayTestTarget(manager, listener.addr())

	manager.mu.Lock()
	manager.configGeneration++
	want := manager.configGeneration
	manager.mu.Unlock()

	result := manager.CheckGateway(context.Background(), listener.addr())
	if result.Generation != want {
		t.Fatalf("generation = %d, want %d", result.Generation, want)
	}
}

func newGatewayTestManager(t *testing.T) *Manager {
	t.Helper()
	manager := NewManager(context.Background())
	t.Cleanup(func() { _ = manager.Close() })
	manager.inspectSocket = func(net.Conn) (socketVerdict, error) {
		return socketHealthy, nil
	}
	return manager
}

func setGatewayTestTarget(manager *Manager, address string) {
	host, portText, _ := net.SplitHostPort(address)
	port, _ := strconv.Atoi(portText)
	target := GatewayTarget{
		DeviceIDs: []string{"gw-1"},
		Key:       address,
		IP:        host,
		Port:      uint16(port),
	}
	manager.gatewayTargets = []GatewayTarget{target}
	manager.mu.Lock()
	manager.gateways = map[string]gatewayMeta{
		address: {Key: address, Serial: "GW-1", DeviceID: "gw-1", IP: host, Port: uint16(port)},
	}
	manager.devices = map[string]deviceMeta{
		"SN-gw-1": {Serial: "SN-gw-1", DeviceID: "gw-1", IP: host, Port: uint16(port), GatewayKey: address},
	}
	manager.mu.Unlock()
}

type acceptCounter struct {
	listener   net.Listener
	mu         sync.Mutex
	accepted   int
	conns      []net.Conn
	acceptedCh chan struct{}
}

func startAcceptCounter(t *testing.T) *acceptCounter {
	t.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	counter := &acceptCounter{listener: listener, acceptedCh: make(chan struct{}, 16)}
	go func() {
		for {
			conn, acceptErr := listener.Accept()
			if acceptErr != nil {
				return
			}
			counter.mu.Lock()
			counter.accepted++
			counter.conns = append(counter.conns, conn)
			counter.mu.Unlock()
			counter.acceptedCh <- struct{}{}
		}
	}()
	t.Cleanup(func() {
		_ = listener.Close()
		counter.mu.Lock()
		for _, conn := range counter.conns {
			_ = conn.Close()
		}
		counter.mu.Unlock()
	})
	return counter
}

func (c *acceptCounter) addr() string { return c.listener.Addr().String() }

func (c *acceptCounter) count() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.accepted
}

// waitFor 等到指定数量的 Accept 真正返回，不依赖 goroutine 调度时序。
func (c *acceptCounter) waitFor(t *testing.T, want int) {
	t.Helper()
	deadline := time.NewTimer(2 * time.Second)
	defer deadline.Stop()
	for c.count() < want {
		select {
		case <-c.acceptedCh:
		case <-deadline.C:
			t.Fatalf("timed out waiting for %d accepts, got %d", want, c.count())
		}
	}
}

// assertNoMore 在短促的稳定窗口后确认没有额外连接被接受。
// 正向等待用 waitFor；这里只用于“没有发生”的断言。
func (c *acceptCounter) assertNoMore(t *testing.T, want int) {
	t.Helper()
	time.Sleep(150 * time.Millisecond)
	if got := c.count(); got != want {
		t.Fatalf("accepted connections = %d, want %d", got, want)
	}
}

func startAndCloseListener(t *testing.T) (net.Listener, string) {
	t.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	address := listener.Addr().String()
	_ = listener.Close()
	// 等端口释放，避免间歇性 bind 成功。
	time.Sleep(20 * time.Millisecond)
	return listener, address
}

func buildGatewayRuntimeConfig(t *testing.T, devices ...config.DeviceConfig) config.IotCfgType {
	t.Helper()
	// 配置解析在未知协议等分支会写日志；单元测试不依赖全局日志初始化。
	previous := logger.Log
	logger.Log = zap.NewNop()
	t.Cleanup(func() { logger.Log = previous })
	runtime, err := config.BuildRuntimeConfig(devices)
	if err != nil {
		t.Fatalf("BuildRuntimeConfig: %v", err)
	}
	return runtime
}

func gatewayDevice(id string, ip string, port int, deviceType int, protocol string) config.DeviceConfig {
	device := config.DeviceConfig{
		ID:           id,
		DeviceType:   deviceType,
		SerialNumber: "SN-" + id,
		DeviceName:   id,
		Protocol:     protocol,
		AcqFreq:      1000,
	}
	device.GatewayInfo.ID = id
	device.GatewayInfo.IP = ip
	device.GatewayInfo.Port = port
	device.GatewayInfo.DeviceName = id
	return device
}
