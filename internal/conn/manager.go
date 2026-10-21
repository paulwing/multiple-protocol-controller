// Package conn exposes the TCP connection pool used by device control and collection routines.
package conn

import (
	"context"
	"errors"
	"fmt"
	"net"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"multiple-protocol-controller/internal/config"
	"multiple-protocol-controller/internal/protocol"
	"multiple-protocol-controller/pkg/logger"

	"go.uber.org/zap"
)

const (
	defaultDialTimeout = 5 * time.Second
	defaultKeepAlive   = 2 * time.Minute
)

type deviceMeta struct {
	Serial          string
	DeviceID        string
	IP              string
	Port            uint16
	GatewayKey      string
	GatewaySerial   string
	GatewayDeviceID string
}

type gatewayMeta struct {
	Key      string
	Serial   string
	DeviceID string
	IP       string
	Port     uint16
}

type gatewayConn struct {
	mu          sync.Mutex
	meta        gatewayMeta
	conn        net.Conn
	lastUsed    time.Time
	lastError   error
	keepAliveOK bool
}

// Manager maintains TCP connections keyed by gateway endpoint while devices map into it.
type Manager struct {
	ctx    context.Context
	cancel context.CancelFunc

	mu       sync.RWMutex
	dialer   net.Dialer
	devices  map[string]deviceMeta
	gateways map[string]gatewayMeta
	conns    map[string]*gatewayConn

	dialTimeout time.Duration

	closeOnce sync.Once
	closeErr  error

	guardMu       sync.Mutex
	gatewayGuards map[string]*gatewayGuard

	// 纯网关（deviceType=1）独立健康检查。网关状态与子设备采集结果无关，
	// 只依据端点连接本身；targets 按 IP:port 分组，同端点的多个设备 ID 共享一次检查。
	gatewayTargets      []GatewayTarget
	configGeneration    uint64
	healthMu            sync.Mutex
	dialFailures        map[string]int
	keepAliveLogAt      map[string]time.Time
	inspectSocket       func(net.Conn) (socketVerdict, error)
	setGatewayKeepAlive func(*net.TCPConn) error
}

// Option customises the Manager instance during construction.
type Option func(*Manager)

// WithDialTimeout overrides the default TCP dial timeout.
func WithDialTimeout(timeout time.Duration) Option {
	return func(m *Manager) {
		if timeout > 0 {
			m.dialTimeout = timeout
		}
	}
}

// WithKeepAlive configures the SO_KEEPALIVE interval for outbound dials.
func WithKeepAlive(interval time.Duration) Option {
	return func(m *Manager) {
		if interval > 0 {
			m.dialer.KeepAlive = interval
		}
	}
}

var defaultHolder struct {
	mu  sync.RWMutex
	mgr *Manager
}

// InitDefault constructs a Manager, applies the provided IoT config snapshot and
// installs it as the process-wide default connection pool.
func InitDefault(ctx context.Context, cfg config.IotCfgType, opts ...Option) (*Manager, error) {
	m := NewManager(ctx, opts...)
	if err := m.ApplyConfig(cfg); err != nil {
		return nil, err
	}

	defaultHolder.mu.Lock()
	if defaultHolder.mgr != nil {
		_ = defaultHolder.mgr.Close()
	}
	defaultHolder.mgr = m
	defaultHolder.mu.Unlock()

	return m, nil
}

// InitDefaultFromStore builds the default Manager using the latest snapshot from IotCfgStore.
func InitDefaultFromStore(ctx context.Context, opts ...Option) (*Manager, error) {
	val := config.IotCfgStore.Load()
	if val == nil {
		return nil, errors.New("iot config store is empty")
	}
	cfg, ok := val.(config.IotCfgType)
	if !ok {
		return nil, errors.New("iot config store contains unexpected data")
	}
	return InitDefault(ctx, cfg, opts...)
}

// Default returns the current default Manager if it has been initialised.
func Default() (*Manager, bool) {
	defaultHolder.mu.RLock()
	m := defaultHolder.mgr
	defaultHolder.mu.RUnlock()
	return m, m != nil
}

// ShutdownDefault closes the default Manager and removes it from the global holder.
func ShutdownDefault() error {
	defaultHolder.mu.Lock()
	defer defaultHolder.mu.Unlock()

	if defaultHolder.mgr == nil {
		return nil
	}
	err := defaultHolder.mgr.Close()
	defaultHolder.mgr = nil
	return err
}

// RefreshDefault applies an updated IoT config snapshot to the default Manager.
func RefreshDefault(cfg config.IotCfgType) error {
	defaultHolder.mu.RLock()
	m := defaultHolder.mgr
	defaultHolder.mu.RUnlock()

	if m == nil {
		return errors.New("default manager not initialised")
	}
	return m.ApplyConfig(cfg)
}

// RefreshDefaultFromStore reloads device definitions from IotCfgStore.
func RefreshDefaultFromStore() error {
	defaultHolder.mu.RLock()
	m := defaultHolder.mgr
	defaultHolder.mu.RUnlock()

	if m == nil {
		return errors.New("default manager not initialised")
	}
	return m.RefreshFromStore()
}

// NewManager creates an empty Manager instance.
func NewManager(ctx context.Context, opts ...Option) *Manager {
	if ctx == nil {
		ctx = context.Background()
	}
	mCtx, cancel := context.WithCancel(ctx)

	m := &Manager{
		ctx:                 mCtx,
		cancel:              cancel,
		dialer:              net.Dialer{KeepAlive: defaultKeepAlive},
		dialTimeout:         defaultDialTimeout,
		devices:             make(map[string]deviceMeta),
		gateways:            make(map[string]gatewayMeta),
		conns:               make(map[string]*gatewayConn),
		gatewayGuards:       make(map[string]*gatewayGuard),
		dialFailures:        make(map[string]int),
		keepAliveLogAt:      make(map[string]time.Time),
		inspectSocket:       inspectSocketPlatform,
		setGatewayKeepAlive: setGatewayKeepAliveConfig,
	}

	for _, opt := range opts {
		opt(m)
	}

	go m.waitForShutdown()
	return m
}

// RefreshFromStore pulls the latest parsed devices from IotCfgStore and applies them.
func (m *Manager) RefreshFromStore() error {
	val := config.IotCfgStore.Load()
	if val == nil {
		return errors.New("iot config store is empty")
	}
	cfg, ok := val.(config.IotCfgType)
	if !ok {
		return errors.New("iot config store contains unexpected data")
	}
	return m.ApplyConfig(cfg)
}

// ApplyConfig syncs the connection pool with the provided IoT configuration snapshot.
func (m *Manager) ApplyConfig(cfg config.IotCfgType) error {
	newDevices, newGateways := collectDeviceMeta(cfg)

	m.mu.Lock()
	// 代次用于拒绝旧配置下发起的拨号/检查结果覆盖新配置的连接与状态。
	m.configGeneration++

	for key, entry := range m.conns {
		meta, ok := newGateways[key]
		if !ok {
			logger.Log.Info("gateway removed from tcp pool", zap.String("address", entry.meta.address()))
			_ = entry.close()
			delete(m.conns, key)
			continue
		}
		entry.updateMeta(meta)
		_ = entry.close() // force reconnect on config refresh
	}

	added := 0
	for key := range newGateways {
		if _, exists := m.gateways[key]; !exists {
			added++
		}
	}
	m.devices = newDevices
	m.gateways = newGateways
	m.gatewayTargets = buildGatewayTargets(cfg)
	targetKeys := make(map[string]struct{}, len(m.gatewayTargets))
	for _, target := range m.gatewayTargets {
		targetKeys[target.Key] = struct{}{}
	}

	pendingDial := make([]gatewayMeta, 0, len(newGateways))
	for _, meta := range newGateways {
		// 纯网关端点由健康检查独占拨号；避免 preDial 与健康检查/业务拨号并发建立额外连接。
		if _, isPureGatewayEndpoint := targetKeys[meta.Key]; isPureGatewayEndpoint {
			continue
		}
		pendingDial = append(pendingDial, meta)
	}
	m.mu.Unlock()

	m.healthMu.Lock()
	for key := range m.dialFailures {
		if _, ok := targetKeys[key]; !ok {
			delete(m.dialFailures, key)
		}
	}
	m.healthMu.Unlock()

	if added == 0 || len(pendingDial) == 0 {
		return nil
	}

	go m.preDialGateways(pendingDial)
	return nil
}

func (m *Manager) waitForShutdown() {
	<-m.ctx.Done()
	_ = m.Close()
}

// Close tears down managed connections and stops background tasks.
func (m *Manager) Close() error {
	m.closeOnce.Do(func() {
		m.mu.Lock()
		for _, conn := range m.conns {
			_ = conn.close()
		}
		m.conns = nil
		m.devices = nil
		m.gateways = nil
		m.gatewayTargets = nil
		m.mu.Unlock()

		m.guardMu.Lock()
		m.gatewayGuards = nil
		m.guardMu.Unlock()

		m.cancel()
	})
	return m.closeErr
}

// ExclusiveConnection returns a TCP connection to the网关 corresponding to the given device serial.
// The caller must call the release function once finished (similar to sync.Mutex semantics).
func (m *Manager) ExclusiveConnection(ctx context.Context, deviceSerial string) (net.Conn, func(), error) {
	meta, gateway, err := m.resolveDevice(deviceSerial)
	if err != nil {
		return nil, nil, err
	}
	conn, err := m.exclusiveGatewayConn(ctx, gateway.Key)
	if err != nil {
		return nil, nil, err
	}

	// For convenience, set an alias name on the connection to ease debugging.
	if nc, ok := conn.(interface{ SetDeadline(time.Time) error }); ok && meta.Serial != "" {
		_ = nc.SetDeadline(time.Time{})
	}

	release := func() {
		m.releaseGatewayConn(gateway.Key)
	}
	return conn, release, nil
}

// ResolveGateway returns the网关连接信息 used by the given device serial.
func (m *Manager) ResolveGateway(deviceSerial string) (string, uint16, error) {
	_, gateway, err := m.resolveDevice(deviceSerial)
	if err != nil {
		return "", 0, err
	}
	return gateway.IP, gateway.Port, nil
}

func (m *Manager) resolveDevice(deviceSerial string) (deviceMeta, gatewayMeta, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	meta, ok := m.devices[deviceSerial]
	if !ok {
		return deviceMeta{}, gatewayMeta{}, fmt.Errorf("device serial %s not registered in tcp pool", deviceSerial)
	}
	if meta.GatewayKey == "" {
		return deviceMeta{}, gatewayMeta{}, fmt.Errorf("device %s missing gateway config", deviceSerial)
	}

	gateway, ok := m.gateways[meta.GatewayKey]
	if !ok {
		return deviceMeta{}, gatewayMeta{}, fmt.Errorf("gateway %s not found for device %s", meta.GatewayKey, deviceSerial)
	}
	return meta, gateway, nil
}

// ResetConnection closes and clears the cached connection for the gateway used by the given device serial.
// Next acquisition will trigger a reconnect.
func (m *Manager) ResetConnection(deviceSerial string) {
	meta, gateway, err := m.resolveDevice(deviceSerial)
	if err != nil {
		return
	}
	_ = meta

	m.mu.Lock()
	defer m.mu.Unlock()
	if entry, ok := m.conns[gateway.Key]; ok {
		_ = entry.close()
		entry.conn = nil
		entry.keepAliveOK = false
	}
}

func (m *Manager) exclusiveGatewayConn(ctx context.Context, gatewayKey string) (net.Conn, error) {
	guard := m.getGatewayGuard(gatewayKey)
	if !guard.markBusy() {
		return nil, ErrGatewayBusy
	}

	conn, err := m.ensureGatewayConn(ctx, gatewayKey)
	if err != nil {
		guard.clearBusy()
		return nil, err
	}
	return conn, nil
}

func (m *Manager) releaseGatewayConn(gatewayKey string) {
	guard := m.getGatewayGuard(gatewayKey)
	guard.clearBusy()
}

func (m *Manager) ensureGatewayConn(ctx context.Context, key string) (net.Conn, error) {
	// 统一锁顺序：m.mu → entry.mu。
	// 这里在进入 entry.mu 前一次性取齐 entry/meta/纯网关标记，
	// 避免持有 entry.mu 时再取 m.mu（ApplyConfig/Close/storeGatewayConn 是反向顺序）。
	entry, pureGateway, ok := m.gatewayPoolEntry(key)
	if !ok {
		return nil, fmt.Errorf("gateway %s not registered in tcp pool", key)
	}

	entry.mu.Lock()
	defer entry.mu.Unlock()

	if entry.conn != nil {
		return entry.conn, nil
	}

	dialCtx := ctx
	if dialCtx == nil {
		dialCtx = context.Background()
	}
	if _, ok := dialCtx.Deadline(); !ok && m.dialTimeout > 0 {
		var cancel context.CancelFunc
		dialCtx, cancel = context.WithTimeout(dialCtx, m.dialTimeout)
		defer cancel()
	}

	conn, err := m.dialer.DialContext(dialCtx, "tcp", entry.meta.address())
	if err != nil {
		entry.lastError = err
		return nil, err
	}
	// 受控 keepalive 只作用于本轮纯网关端点；type0/type2 及其它协议保持旧行为。
	entry.keepAliveOK = false
	if pureGateway {
		keepAliveOK, keepAliveErr := m.applyGatewayKeepAlive(conn)
		if keepAliveErr != nil {
			// 不阻断采集，但记录并标记该连接不能作为在线证据；健康检查会重建它。
			m.logGatewayKeepAliveFailure(key, keepAliveErr)
		} else {
			entry.keepAliveOK = keepAliveOK
		}
	}
	entry.conn = conn
	entry.lastUsed = time.Now()
	entry.lastError = nil
	return conn, nil
}

// gatewayPoolEntry 在 m.mu 保护下取齐连接条目、纯网关标记与端点元数据。
// 返回值中的 entry 可能为 nil 且 ok 为 true（端点存在但尚未建条目），此时由调用方创建。
func (m *Manager) gatewayPoolEntry(key string) (*gatewayConn, bool, bool) {
	m.mu.RLock()
	entry := m.conns[key]
	meta, hasMeta := m.gateways[key]
	pure := m.isPureGatewayEndpointLocked(key)
	m.mu.RUnlock()

	if !hasMeta {
		return nil, false, false
	}
	if entry != nil {
		return entry, pure, true
	}

	m.mu.Lock()
	defer m.mu.Unlock()
	// 配置可能在两次取锁之间刷新；必须在写锁内重新校验端点与纯网关标记，
	// 避免为已移除的端点创建连接条目，或把 keepalive 误用到已变成 type0/2 的端点。
	meta, hasMeta = m.gateways[key]
	if !hasMeta || m.conns == nil {
		return nil, false, false
	}
	pure = m.isPureGatewayEndpointLocked(key)
	if existing := m.conns[key]; existing != nil {
		return existing, pure, true
	}
	entry = &gatewayConn{meta: meta}
	m.conns[key] = entry
	return entry, pure, true
}

func (m *Manager) preDialGateways(gateways []gatewayMeta) {
	for _, meta := range gateways {
		conn, err := m.dialer.DialContext(m.ctx, "tcp", meta.address())
		if err != nil {
			logger.Log.Warn("pre-dial gateway failed", zap.String("address", meta.address()), zap.Error(err))
			continue
		}
		_ = conn.Close()
	}
}

// gatewayConn helpers
func (gm *gatewayMeta) address() string {
	if gm == nil {
		return ""
	}
	return net.JoinHostPort(gm.IP, strconv.Itoa(int(gm.Port)))
}

func (dc *gatewayConn) updateMeta(meta gatewayMeta) {
	if dc == nil {
		return
	}
	dc.mu.Lock()
	dc.meta = meta
	dc.mu.Unlock()
}

func (dc *gatewayConn) close() error {
	if dc == nil {
		return nil
	}
	dc.mu.Lock()
	defer dc.mu.Unlock()
	if dc.conn == nil {
		return nil
	}
	err := dc.conn.Close()
	dc.conn = nil
	dc.keepAliveOK = false
	return err
}

func collectDeviceMeta(cfg config.IotCfgType) (map[string]deviceMeta, map[string]gatewayMeta) {
	devices := make(map[string]deviceMeta, len(cfg.Devices))
	gateways := make(map[string]gatewayMeta)
	for _, dev := range cfg.Devices {
		if !isTCPManagedProtocol(dev.Config.Protocol) {
			continue
		}
		serial := strings.TrimSpace(dev.Config.SerialNumber)
		if serial == "" {
			continue
		}
		ip := strings.TrimSpace(dev.GatewayIP)
		port := dev.GatewayPort
		if ip == "" || port == 0 {
			logger.Log.Warn("skip device without reachable tcp endpoint",
				zap.String("serial", serial),
				zap.String("deviceId", dev.Config.ID),
				zap.String("ip", ip),
				zap.Uint16("port", port),
			)
			continue
		}
		addr := net.JoinHostPort(ip, strconv.Itoa(int(port)))

		gatewaySerial := dev.GatewaySerial
		if gatewaySerial == "" {
			gatewaySerial = serial
		}
		gatewayDeviceID := dev.GatewayID
		if gatewayDeviceID == "" {
			gatewayDeviceID = dev.Config.ID
		}

		devices[serial] = deviceMeta{
			Serial:          serial,
			DeviceID:        dev.Config.ID,
			IP:              ip,
			Port:            port,
			GatewayKey:      addr,
			GatewaySerial:   gatewaySerial,
			GatewayDeviceID: gatewayDeviceID,
		}

		if _, exists := gateways[addr]; !exists {
			gateways[addr] = gatewayMeta{
				Key:      addr,
				Serial:   gatewaySerial,
				DeviceID: gatewayDeviceID,
				IP:       ip,
				Port:     port,
			}
		}
	}
	return devices, gateways
}

func isTCPManagedProtocol(proto string) bool {
	switch protocol.NormalizeName(proto) {
	case "MODBUS", "MODBUSRTU", "MODBUS_RTU", "MODBUSTCP", "MODBUS_TCP":
		return true
	default:
		return false
	}
}

var ErrGatewayBusy = errors.New("gateway is executing an exclusive operation")

type gatewayGuard struct {
	busy uint32
}

func (g *gatewayGuard) markBusy() bool {
	return atomic.CompareAndSwapUint32(&g.busy, 0, 1)
}

func (g *gatewayGuard) clearBusy() {
	atomic.StoreUint32(&g.busy, 0)
}

func (g *gatewayGuard) isBusy() bool {
	return atomic.LoadUint32(&g.busy) == 1
}

func (m *Manager) getGatewayGuard(key string) *gatewayGuard {
	m.guardMu.Lock()
	defer m.guardMu.Unlock()
	if guard, ok := m.gatewayGuards[key]; ok {
		return guard
	}
	if m.gatewayGuards == nil {
		m.gatewayGuards = make(map[string]*gatewayGuard)
	}
	guard := &gatewayGuard{}
	m.gatewayGuards[key] = guard
	return guard
}
