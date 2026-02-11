package collector

import (
	"context"
	"time"

	"multiple-protocol-controller/internal/config"
	opcuaClient "multiple-protocol-controller/internal/opcua"

	"github.com/gopcua/opcua/ua"
	"go.uber.org/zap"
)

type opcuaSpec struct {
	Device config.DeviceRuntime
	Params []config.OpcuaParam
}

func buildOpcuaDeviceSpecs(cfg config.IotCfgType) map[string]opcuaSpec {
	specs := make(map[string]opcuaSpec)
	for _, dev := range cfg.Devices {
		if !isOpcuaDevice(dev.Config.Protocol) {
			continue
		}
		if dev.Config.SerialNumber == "" {
			continue
		}
		params := dev.OpcuaReadPoints
		if len(params) == 0 {
			continue
		}
		specs[dev.Config.SerialNumber] = opcuaSpec{
			Device: dev,
			Params: params,
		}
	}
	return specs
}

type opcuaWorker struct {
	spec   opcuaSpec
	ctx    context.Context
	cancel context.CancelFunc
	done   chan struct{}
}

func newOpcuaWorker(parent context.Context, spec opcuaSpec) *opcuaWorker {
	ctx, cancel := context.WithCancel(parent)
	return &opcuaWorker{
		spec:   spec,
		ctx:    ctx,
		cancel: cancel,
		done:   make(chan struct{}),
	}
}

func (w *opcuaWorker) start() {
	go w.run()
}

func (w *opcuaWorker) stop() {
	w.cancel()
	<-w.done
}

func (w *opcuaWorker) run() {
	defer close(w.done)

	interval := time.Duration(w.spec.Device.Config.AcqFreq) * time.Millisecond
	if interval <= 0 {
		interval = time.Second
	}
	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			w.collectOnce()
		case <-w.ctx.Done():
			return
		}
	}
}

func (w *opcuaWorker) collectOnce() {
	nodeIDs := make([]string, 0, len(w.spec.Params))
	activeParams := make([]config.OpcuaParam, 0, len(w.spec.Params))
	for _, param := range w.spec.Params {
		if param.Passive {
			continue
		}
		nodeIDs = append(nodeIDs, param.NodeID)
		activeParams = append(activeParams, param)
	}
	if len(nodeIDs) == 0 {
		return
	}

	timeout := time.Duration(w.spec.Device.ResponseTimeoutMs) * time.Millisecond
	if timeout <= 0 {
		timeout = 5 * time.Second
	}
	ctx, cancel := context.WithTimeout(w.ctx, timeout)
	defer cancel()

	cfg := opcuaClient.Config{
		Endpoint:       w.spec.Device.OpcuaEndpoint,
		SecurityPolicy: w.spec.Device.OpcuaSecurityPolicy,
		SecurityMode:   w.spec.Device.OpcuaSecurityMode,
		Username:       w.spec.Device.Config.Username,
		Password:       w.spec.Device.Config.Password,
	}
	if cfg.Endpoint == "" {
		logInfo("opcua endpoint missing",
			zap.String("deviceSerial", w.spec.Device.Config.SerialNumber))
		return
	}

	results, err := opcuaClient.Default().ReadNodes(ctx, cfg, nodeIDs)
	if err != nil {
		logInfo("opcua read failed",
			zap.String("deviceSerial", w.spec.Device.Config.SerialNumber),
			zap.Error(err))
		return
	}

	for i, param := range activeParams {
		if i >= len(results) {
			break
		}
		val := results[i]
		if val == nil || val.Status != ua.StatusOK || val.Value == nil {
			status := ua.StatusBadUnexpectedError
			if val != nil {
				status = val.Status
			}
			logInfo("opcua read bad status",
				zap.String("deviceSerial", w.spec.Device.Config.SerialNumber),
				zap.String("param", param.Identify),
				zap.Any("status", status))
			continue
		}
		raw := val.Value.Value()
		converted, err := opcuaClient.NormalizeValue(raw, param.DataType)
		if err != nil {
			logInfo("opcua value normalize failed",
				zap.String("deviceSerial", w.spec.Device.Config.SerialNumber),
				zap.String("param", param.Identify),
				zap.Error(err))
			continue
		}
		recordCollectedOpcuaValue(w.spec.Device, param, converted)
	}
}
