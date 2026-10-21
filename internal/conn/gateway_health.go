package conn

import (
	"context"
	"net"
	"strconv"
	"strings"
	"time"

	"go.uber.org/zap"

	"multiple-protocol-controller/internal/config"
	"multiple-protocol-controller/pkg/logger"
)

// GatewayTarget 描述一个需要独立健康判定的纯 Modbus 网关端点。
// 同一 IP:port 可能对应多个 deviceType=1 的设备 ID，检查一次端点后应为每个 ID 写入状态。
type GatewayTarget struct {
	DeviceIDs []string
	Key       string
	IP        string
	Port      uint16
}

// GatewayProbeState 是单轮健康判定的结果。
type GatewayProbeState int

const (
	// GatewayProbeUnknown 表示本轮没有拿到可用的健康证据（检查不受支持、首次拨号失败尚未确认等）。
	// 调用方不得据此续期在线状态，也不得判离线。
	GatewayProbeUnknown GatewayProbeState = iota
	GatewayProbeHealthy
	GatewayProbeUnhealthy
	// GatewayProbeBusy 表示业务采集/控制正持有该网关的互斥锁，本轮不检查。
	GatewayProbeBusy
	// GatewayProbeStale 表示该端点已不在当前配置中，不得写入状态。
	GatewayProbeStale
)

type GatewayProbeResult struct {
	State  GatewayProbeState
	Target GatewayTarget
	// Generation 是本次判定基于的配置代次；发布端必须与当前代次一致才允许写 Redis。
	Generation uint64
}

type socketVerdict int

const (
	socketUnknown socketVerdict = iota
	socketHealthy
	socketDead
)

// 标准 TCP 状态值（Linux UAPI）。
const (
	tcpStateClosed      = 0
	tcpStateEstablished = 1
	tcpStateSynSent     = 2
	tcpStateSynRecv     = 3
	tcpStateTimeWait    = 6
	tcpStateClose       = 7
	tcpStateCloseWait   = 8
	tcpStateLastAck     = 9
	tcpStateClosing     = 11
)

// classifyTCPState 把内核连接状态归类为健康/断开/未知。
func classifyTCPState(state uint8) socketVerdict {
	switch state {
	case tcpStateEstablished:
		return socketHealthy
	case tcpStateClosed, tcpStateTimeWait, tcpStateClose,
		tcpStateCloseWait, tcpStateLastAck, tcpStateClosing:
		return socketDead
	case tcpStateSynSent, tcpStateSynRecv:
		// 握手尚未完成，不作为在线证据，也不当作已断开。
		return socketUnknown
	default:
		return socketUnknown
	}
}

// 健康检查默认参数；最终契约以跨仓文档为准。
const (
	defaultGatewayProbeInterval = 10 * time.Second
	defaultGatewayStatusTTL     = 30 * time.Second
	defaultGatewayProbeTimeout  = 2 * time.Second
	gatewayKeepAliveIdle        = 10 * time.Second
	gatewayKeepAliveInterval    = 5 * time.Second
	gatewayKeepAliveCount       = 3
	gatewayDialFailureThreshold = 2
	gatewayKeepAliveLogCooldown = time.Minute
)

// ConfigGeneration 返回当前配置代次。发布端写状态前必须复核。
func (m *Manager) ConfigGeneration() uint64 {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.configGeneration
}

// GatewayTargets 返回当前配置中的纯 Modbus 网关端点快照。
func (m *Manager) GatewayTargets() []GatewayTarget {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return cloneGatewayTargets(m.gatewayTargets)
}

// buildGatewayTargets 只收集 deviceType=1 且属于现有 Modbus TCP 管理通道的网关。
// 同 IP:port 的多个网关 ID 合并为一个端点目标，一次检查覆盖全部 ID。
func buildGatewayTargets(cfg config.IotCfgType) []GatewayTarget {
	grouped := make(map[string]*GatewayTarget)
	order := make([]string, 0)
	seenIDs := make(map[string]map[string]struct{})

	for _, dev := range cfg.Devices {
		if dev.Config.DeviceType != 1 {
			continue
		}
		if !isTCPManagedProtocol(dev.Config.Protocol) {
			continue
		}
		ip := strings.TrimSpace(dev.GatewayIP)
		port := dev.GatewayPort
		if ip == "" || port == 0 {
			continue
		}
		key := net.JoinHostPort(ip, strconv.Itoa(int(port)))
		target, ok := grouped[key]
		if !ok {
			target = &GatewayTarget{Key: key, IP: ip, Port: port}
			grouped[key] = target
			order = append(order, key)
			seenIDs[key] = make(map[string]struct{})
		}
		id := strings.TrimSpace(dev.Config.ID)
		if id == "" {
			continue
		}
		if _, exists := seenIDs[key][id]; exists {
			continue
		}
		seenIDs[key][id] = struct{}{}
		target.DeviceIDs = append(target.DeviceIDs, id)
	}

	result := make([]GatewayTarget, 0, len(order))
	for _, key := range order {
		target := *grouped[key]
		target.DeviceIDs = append([]string{}, target.DeviceIDs...)
		if len(target.DeviceIDs) == 0 {
			continue
		}
		result = append(result, target)
	}
	return result
}

func cloneGatewayTargets(targets []GatewayTarget) []GatewayTarget {
	if len(targets) == 0 {
		return nil
	}
	cloned := make([]GatewayTarget, 0, len(targets))
	for _, target := range targets {
		target.DeviceIDs = append([]string{}, target.DeviceIDs...)
		cloned = append(cloned, target)
	}
	return cloned
}

// CheckGateway 对指定端点做一轮健康判定。
// 判定顺序：业务互斥保护 → 复用池内连接做不消费数据的 socket 检查 → 仅在没有可用连接时拨号一次。
func (m *Manager) CheckGateway(ctx context.Context, key string) GatewayProbeResult {
	if ctx == nil {
		ctx = context.Background()
	}
	target, generation, ok := m.gatewayTarget(key)
	if !ok {
		return GatewayProbeResult{State: GatewayProbeStale}
	}
	if err := ctx.Err(); err != nil {
		return GatewayProbeResult{State: GatewayProbeUnknown, Target: target, Generation: generation}
	}

	// 与业务采集/控制共用同一个按端点互斥。
	guard := m.getGatewayGuard(key)
	if guard == nil {
		return GatewayProbeResult{State: GatewayProbeUnknown, Target: target, Generation: generation}
	}
	pooled, keepAliveOK := m.pooledGatewayConn(key)
	if !guard.markBusy() {
		// 业务事务进行中。已有连接时仍可做只读的内核状态检查：
		// 它不读取/消费业务数据，也不依赖子设备事务是否成功，避免健康续期被业务锁长期挡住。
		// 没有连接时不拨号（避免并发第二连接），本轮跳过。
		if pooled == nil {
			return GatewayProbeResult{State: GatewayProbeBusy, Target: target, Generation: generation}
		}
		result, _ := m.inspectPooledGateway(pooled, key, target, generation, keepAliveOK, false)
		return result
	}
	defer guard.clearBusy()

	// 拿到互斥后重新读取连接：业务可能已在两者之间完成一次重置。
	pooled, keepAliveOK = m.pooledGatewayConn(key)
	if pooled != nil {
		result, needsDial := m.inspectPooledGateway(pooled, key, target, generation, keepAliveOK, true)
		if !needsDial {
			return result
		}
		// 对端已关闭/RST/半关闭且当前没有业务在用它：移除后拨号重建。
		m.dropGatewayConn(key, pooled)
	}

	return m.dialGatewayForHealth(ctx, key, target, generation)
}

// inspectPooledGateway 只读取内核 socket 元数据，不读取、不消费业务数据。
// allowDrop 为 false 时（业务正在使用连接）即使判定已断开也不关闭连接，交由业务路径处理。
func (m *Manager) inspectPooledGateway(conn net.Conn, key string, target GatewayTarget, generation uint64, keepAliveOK bool, allowDrop bool) (GatewayProbeResult, bool) {
	verdict, err := m.inspectSocket(conn)
	if err != nil {
		// 本进程刚关闭连接等本地错误不能当作网关离线证据。
		return GatewayProbeResult{State: GatewayProbeUnknown, Target: target, Generation: generation}, false
	}
	switch verdict {
	case socketHealthy:
		if !keepAliveOK {
			// 未成功启用 keepalive 的连接无法发现静默半开，不能作为在线证据。
			// 允许重建时丢弃并重新拨号，由新连接重新尝试配置 keepalive；
			// 业务持锁时不动连接，交给业务路径。
			return GatewayProbeResult{State: GatewayProbeUnknown, Target: target, Generation: generation}, allowDrop
		}
		m.resetGatewayDialFailures(key)
		return GatewayProbeResult{State: GatewayProbeHealthy, Target: target, Generation: generation}, false
	case socketDead:
		if !allowDrop {
			// 业务持有连接时只报告判定结果，不关闭、不重建。
			return GatewayProbeResult{State: GatewayProbeUnhealthy, Target: target, Generation: generation}, false
		}
		return GatewayProbeResult{State: GatewayProbeUnhealthy, Target: target, Generation: generation}, true
	default:
		// 平台不支持检查或内核状态无法判定：不续期、不判离线，交给 TTL 过期。
		return GatewayProbeResult{State: GatewayProbeUnknown, Target: target, Generation: generation}, false
	}
}

func (m *Manager) gatewayTarget(key string) (GatewayTarget, uint64, bool) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	for _, target := range m.gatewayTargets {
		if target.Key == key {
			cloned := target
			cloned.DeviceIDs = append([]string{}, target.DeviceIDs...)
			return cloned, m.configGeneration, true
		}
	}
	return GatewayTarget{}, 0, false
}

func (m *Manager) isPureGatewayEndpoint(key string) bool {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.isPureGatewayEndpointLocked(key)
}

// isPureGatewayEndpointLocked 调用方必须持有 m.mu（读或写）。
func (m *Manager) isPureGatewayEndpointLocked(key string) bool {
	for _, target := range m.gatewayTargets {
		if target.Key == key {
			return true
		}
	}
	return false
}

func (m *Manager) pooledGatewayConn(key string) (net.Conn, bool) {
	m.mu.RLock()
	entry := m.conns[key]
	m.mu.RUnlock()
	if entry == nil {
		return nil, false
	}
	entry.mu.Lock()
	defer entry.mu.Unlock()
	return entry.conn, entry.keepAliveOK
}

func (m *Manager) dropGatewayConn(key string, expected net.Conn) {
	m.mu.RLock()
	entry := m.conns[key]
	m.mu.RUnlock()
	if entry == nil {
		return
	}
	entry.mu.Lock()
	defer entry.mu.Unlock()
	if entry.conn != expected {
		return
	}
	if entry.conn != nil {
		_ = entry.conn.Close()
		entry.conn = nil
		entry.keepAliveOK = false
	}
}

func (m *Manager) dialGatewayForHealth(ctx context.Context, key string, target GatewayTarget, generation uint64) GatewayProbeResult {
	dialCtx := ctx
	if _, hasDeadline := dialCtx.Deadline(); !hasDeadline {
		var cancel context.CancelFunc
		dialCtx, cancel = context.WithTimeout(dialCtx, defaultGatewayProbeTimeout)
		defer cancel()
	}

	conn, err := m.dialer.DialContext(dialCtx, "tcp", key)
	if err != nil {
		if m.noteGatewayDialFailure(key) >= gatewayDialFailureThreshold {
			return GatewayProbeResult{State: GatewayProbeUnhealthy, Target: target, Generation: generation}
		}
		return GatewayProbeResult{State: GatewayProbeUnknown, Target: target, Generation: generation}
	}

	keepAliveOK, keepAliveErr := m.applyGatewayKeepAlive(conn)
	if keepAliveErr != nil {
		// 没有 keepalive 就无法发现静默半开。保守处理：不为本轮续期，并关闭这条不完整的连接。
		m.logGatewayKeepAliveFailure(key, keepAliveErr)
		_ = conn.Close()
		return GatewayProbeResult{State: GatewayProbeUnknown, Target: target, Generation: generation}
	}

	if !m.storeGatewayConn(key, conn, generation, keepAliveOK) {
		_ = conn.Close()
		return GatewayProbeResult{State: GatewayProbeStale, Target: target, Generation: generation}
	}
	m.resetGatewayDialFailures(key)
	return GatewayProbeResult{State: GatewayProbeHealthy, Target: target, Generation: generation}
}

// storeGatewayConn 把健康检查建立的连接留给业务池复用。
// 必须同时满足配置代次未变且端点仍存在，防止旧拨号落入新配置并覆盖新连接。
func (m *Manager) storeGatewayConn(key string, conn net.Conn, generation uint64, keepAliveOK bool) bool {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.configGeneration != generation {
		return false
	}
	if m.gateways == nil || m.conns == nil {
		return false
	}
	meta, ok := m.gateways[key]
	if !ok {
		return false
	}
	entry := m.conns[key]
	if entry == nil {
		entry = &gatewayConn{meta: meta}
		m.conns[key] = entry
	}
	entry.mu.Lock()
	defer entry.mu.Unlock()
	entry.meta = meta
	if entry.conn != nil {
		_ = entry.conn.Close()
	}
	entry.conn = conn
	entry.lastUsed = time.Now()
	entry.lastError = nil
	entry.keepAliveOK = keepAliveOK
	return true
}

func (m *Manager) noteGatewayDialFailure(key string) int {
	m.healthMu.Lock()
	defer m.healthMu.Unlock()
	if m.dialFailures == nil {
		m.dialFailures = make(map[string]int)
	}
	m.dialFailures[key]++
	return m.dialFailures[key]
}

func (m *Manager) resetGatewayDialFailures(key string) {
	m.healthMu.Lock()
	defer m.healthMu.Unlock()
	delete(m.dialFailures, key)
}

func (m *Manager) applyGatewayKeepAlive(conn net.Conn) (bool, error) {
	tcp, ok := conn.(*net.TCPConn)
	if !ok {
		return false, nil
	}
	if err := m.setGatewayKeepAlive(tcp); err != nil {
		return false, err
	}
	return true, nil
}

func (m *Manager) logGatewayKeepAliveFailure(key string, err error) {
	m.healthMu.Lock()
	if m.keepAliveLogAt == nil {
		m.keepAliveLogAt = make(map[string]time.Time)
	}
	now := time.Now()
	if last, ok := m.keepAliveLogAt[key]; ok && now.Sub(last) < gatewayKeepAliveLogCooldown {
		m.healthMu.Unlock()
		return
	}
	m.keepAliveLogAt[key] = now
	m.healthMu.Unlock()

	if logger.Log != nil {
		logger.Log.Warn("configure gateway keepalive failed",
			zap.String("gateway", key),
			zap.Error(err))
	}
}

func setGatewayKeepAliveConfig(tcp *net.TCPConn) error {
	return tcp.SetKeepAliveConfig(net.KeepAliveConfig{
		Enable:   true,
		Idle:     gatewayKeepAliveIdle,
		Interval: gatewayKeepAliveInterval,
		Count:    gatewayKeepAliveCount,
	})
}
