package collector

import (
	"context"
	"time"

	"multiple-protocol-controller/internal/config"
	"multiple-protocol-controller/internal/conn"
	"multiple-protocol-controller/pkg/utils"

	"go.uber.org/zap"
)

type bacnetSpec struct {
	Device config.DeviceRuntime
	Params []config.BacnetParam
}

func buildBacnetDeviceSpecs(cfg config.IotCfgType) map[string]bacnetSpec {
	specs := make(map[string]bacnetSpec)
	for _, dev := range cfg.Devices {
		if !isBacnetDevice(dev.Config.Protocol) {
			continue
		}
		if dev.Config.SerialNumber == "" {
			continue
		}
		params := dev.BacnetReadPoints
		if len(params) == 0 {
			continue
		}
		specs[dev.Config.SerialNumber] = bacnetSpec{
			Device: dev,
			Params: params,
		}
	}
	return specs
}

type bacnetWorker struct {
	spec   bacnetSpec
	ctx    context.Context
	cancel context.CancelFunc
	done   chan struct{}
}

func newBacnetWorker(parent context.Context, spec bacnetSpec) *bacnetWorker {
	ctx, cancel := context.WithCancel(parent)
	return &bacnetWorker{
		spec:   spec,
		ctx:    ctx,
		cancel: cancel,
		done:   make(chan struct{}),
	}
}

func (w *bacnetWorker) start() {
	go w.run()
}

func (w *bacnetWorker) stop() {
	w.cancel()
	<-w.done
}

func (w *bacnetWorker) run() {
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

func (w *bacnetWorker) collectOnce() {
	cfg := conn.BacnetConfig{
		IP:             w.spec.Device.GatewayIP,
		Port:           w.spec.Device.GatewayPort,
		DeviceInstance: w.spec.Device.BacnetDeviceInstance,
	}
	timeout := time.Duration(w.spec.Device.ResponseTimeoutMs) * time.Millisecond
	if timeout <= 0 {
		timeout = 5 * time.Second
	}

	for _, param := range w.spec.Params {
		if param.ReadDisabled {
			continue
		}
		ctx, cancel := context.WithTimeout(w.ctx, timeout)
		value, err := conn.DefaultBacnetManager().ReadPresentValue(ctx, cfg, param.ObjectType, param.ObjectInstance)
		cancel()
		if err != nil {
			logInfo("bacnet collect failed",
				zap.String("deviceSerial", w.spec.Device.Config.SerialNumber),
				zap.String("param", param.Identify),
				zap.Error(err))
			continue
		}

		normalized, err := normalizeBacnetValue(value, param.DataType)
		if err != nil {
			logInfo("bacnet value normalize failed",
				zap.String("deviceSerial", w.spec.Device.Config.SerialNumber),
				zap.String("param", param.Identify),
				zap.Error(err))
			continue
		}
		recordCollectedBacnetValue(w.spec.Device, param, normalized)
	}
}

func normalizeBacnetValue(value interface{}, dataType string) (interface{}, error) {
	switch dataType {
	case "float":
		f, err := utils.CoerceFloat32(value)
		if err != nil {
			return nil, err
		}
		return f, nil
	case "int", "enum":
		i, err := utils.CoerceInt32(value)
		if err != nil {
			return nil, err
		}
		return i, nil
	case "bool":
		b, err := utils.CoerceBool(value)
		if err != nil {
			return nil, err
		}
		return b, nil
	case "string":
		return utils.CoerceString(value), nil
	default:
		return value, nil
	}
}
