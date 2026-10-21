package collector

import (
	"context"
	"encoding/json"
	"slices"
	"sync"
	"time"

	"go.uber.org/zap"

	"multiple-protocol-controller/internal/conn"
	"multiple-protocol-controller/pkg/logger"
)

// 纯网关（deviceType=1）在线状态发布契约：
//   - key:   gateway:status:<网关设备ID>
//   - value: {"status":"0|1","timestamp":<Unix 毫秒>}，沿用平台既有 "0=在线" 约定
//   - TTL:   30s；MPC 每 10s 刷新一次，MPC 停止后状态自动过期
//
// 该状态只来自网关端点连接健康，与下属设备采集结果无关，也不写 device:data 实时属性快照。
const (
	gatewayStatusKeyPrefix = "gateway:status:"
	gatewayStatusOnline    = "0"
	gatewayStatusOffline   = "1"

	gatewayStatusInterval     = 10 * time.Second
	gatewayStatusTTL          = 30 * time.Second
	gatewayStatusCheckTimeout = 5 * time.Second
	gatewayStatusConcurrency  = 4
)

// GatewayStatusWriter 是发布器依赖的最小 Redis 写接口。
type GatewayStatusWriter interface {
	Set(ctx context.Context, key string, value interface{}, expiration time.Duration) error
}

type gatewayStatusPayload struct {
	Status    string `json:"status"`
	Timestamp int64  `json:"timestamp"`
}

// GatewayStatusPublisher 周期性检查纯网关端点并刷新 Redis 状态。
// 单轮检查有界、非重叠；单个端点失败不影响其它端点。
type GatewayStatusPublisher struct {
	managerFn   func() (*conn.Manager, bool)
	writer      GatewayStatusWriter
	probe       func(context.Context, *conn.Manager, string) conn.GatewayProbeResult
	now         func() time.Time
	interval    time.Duration
	ttl         time.Duration
	timeout     time.Duration
	concurrency int
}

// NewGatewayStatusPublisher 使用生产默认参数构造发布器。
func NewGatewayStatusPublisher(managerFn func() (*conn.Manager, bool), writer GatewayStatusWriter) *GatewayStatusPublisher {
	if managerFn == nil {
		managerFn = func() (*conn.Manager, bool) { return conn.Default() }
	}
	return &GatewayStatusPublisher{
		managerFn: managerFn,
		writer:    writer,
		probe: func(ctx context.Context, manager *conn.Manager, key string) conn.GatewayProbeResult {
			return manager.CheckGateway(ctx, key)
		},
		now:         time.Now,
		interval:    gatewayStatusInterval,
		ttl:         gatewayStatusTTL,
		timeout:     gatewayStatusCheckTimeout,
		concurrency: gatewayStatusConcurrency,
	}
}

// StartGatewayStatusPublisher 阻塞运行发布循环，直到 ctx 结束。
func StartGatewayStatusPublisher(ctx context.Context, writer GatewayStatusWriter) {
	NewGatewayStatusPublisher(nil, writer).Run(ctx)
}

// Run 先执行一轮，再按固定间隔执行；单轮结束前不会开始下一轮。
func (p *GatewayStatusPublisher) Run(ctx context.Context) {
	if ctx == nil {
		ctx = context.Background()
	}
	p.runOnce(ctx)
	ticker := time.NewTicker(p.interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			p.runOnce(ctx)
		}
	}
}

func (p *GatewayStatusPublisher) runOnce(ctx context.Context) {
	manager, ok := p.managerFn()
	if !ok || manager == nil {
		return
	}
	targets := manager.GatewayTargets()
	if len(targets) == 0 {
		return
	}

	limit := p.concurrency
	if limit <= 0 {
		limit = 1
	}
	sem := make(chan struct{}, limit)
	var wg sync.WaitGroup
	for _, target := range targets {
		select {
		case <-ctx.Done():
			wg.Wait()
			return
		case sem <- struct{}{}:
		}
		wg.Add(1)
		go func(target conn.GatewayTarget) {
			defer wg.Done()
			defer func() { <-sem }()
			p.checkTarget(ctx, manager, target)
		}(target)
	}
	wg.Wait()
}

func (p *GatewayStatusPublisher) checkTarget(ctx context.Context, manager *conn.Manager, target conn.GatewayTarget) {
	probeCtx, cancel := context.WithTimeout(ctx, p.timeout)
	result := p.probe(probeCtx, manager, target.Key)
	cancel()

	status, ok := gatewayStatusFromProbe(result.State)
	if !ok {
		// Unknown/Busy/Stale 不刷新也不覆盖：交给已有 TTL 自然过期。
		return
	}
	// 配置已刷新时，本轮结果可能来自旧端点，禁止写入。
	if manager.ConfigGeneration() != result.Generation {
		return
	}
	// The queued snapshot may predate the probe's generation even when the
	// endpoint key is unchanged. Never apply a new binding's health to old IDs.
	if result.Target.Key != target.Key || !slices.Equal(result.Target.DeviceIDs, target.DeviceIDs) {
		return
	}

	payload, err := json.Marshal(gatewayStatusPayload{
		Status:    status,
		Timestamp: p.now().UnixMilli(),
	})
	if err != nil {
		logGatewayStatusWarn("marshal gateway status failed",
			zap.String("gateway", target.Key), zap.Error(err))
		return
	}

	for _, deviceID := range target.DeviceIDs {
		// 逐 ID 写前复核代次，避免多 ID 循环期间配置刷新后继续写旧目标。
		if manager.ConfigGeneration() != result.Generation {
			return
		}
		writeCtx, cancel := context.WithTimeout(ctx, p.timeout)
		err := p.writer.Set(writeCtx, gatewayStatusKey(deviceID), string(payload), p.ttl)
		cancel()
		if err != nil {
			// Redis 写失败只影响本轮状态刷新，不得阻塞采集或其它网关。
			logGatewayStatusWarn("write gateway status failed",
				zap.String("gateway", target.Key),
				zap.String("deviceId", deviceID),
				zap.Error(err))
		}
	}
}

func gatewayStatusFromProbe(state conn.GatewayProbeState) (string, bool) {
	switch state {
	case conn.GatewayProbeHealthy:
		return gatewayStatusOnline, true
	case conn.GatewayProbeUnhealthy:
		return gatewayStatusOffline, true
	default:
		return "", false
	}
}

func gatewayStatusKey(deviceID string) string {
	return gatewayStatusKeyPrefix + deviceID
}

func logGatewayStatusWarn(msg string, fields ...zap.Field) {
	if logger.Log == nil {
		return
	}
	logger.Log.Warn(msg, fields...)
}
