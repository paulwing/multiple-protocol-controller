package collector

import (
	"context"
	"errors"
	"strconv"
	"strings"
	"sync"
	"time"

	"multiple-protocol-controller/internal/config"
	"multiple-protocol-controller/internal/conn"
	"multiple-protocol-controller/pkg/logger"

	"go.uber.org/zap"
)

var (
	managerOnce sync.Once
	mgr         *Manager
)

// Start initialises the global collector manager.
func Start(ctx context.Context) {
	managerOnce.Do(func() {
		mgr = newManager(ctx)
	})
}

// UpdateSnapshot refreshes the device list used by the collector.
func UpdateSnapshot(cfg config.IotCfgType) {
	if mgr == nil {
		return
	}
	mgr.update(cfg)
}

type Manager struct {
	ctx    context.Context
	cancel context.CancelFunc

	mu            sync.Mutex
	modbusWorkers map[string]*deviceWorker
	bacnetWorkers map[string]*bacnetWorker
	opcuaWorkers  map[string]*opcuaWorker
	mqttWorkers   map[string]*mqttWorker
}

func newManager(ctx context.Context) *Manager {
	if ctx == nil {
		ctx = context.Background()
	}
	c, cancel := context.WithCancel(ctx)
	return &Manager{
		ctx:           c,
		cancel:        cancel,
		modbusWorkers: make(map[string]*deviceWorker),
		bacnetWorkers: make(map[string]*bacnetWorker),
		opcuaWorkers:  make(map[string]*opcuaWorker),
		mqttWorkers:   make(map[string]*mqttWorker),
	}
}

func (m *Manager) update(cfg config.IotCfgType) {
	modbusSpecs := buildModbusDeviceSpecs(cfg)
	bacnetSpecs := buildBacnetDeviceSpecs(cfg)
	opcuaSpecs := buildOpcuaDeviceSpecs(cfg)
	mqttSpecs := buildMqttDeviceSpecs(cfg)

	m.mu.Lock()
	defer m.mu.Unlock()

	// stop workers that are no longer needed
	for serial, worker := range m.modbusWorkers {
		if _, exists := modbusSpecs[serial]; !exists {
			worker.stop()
			delete(m.modbusWorkers, serial)
			removeDeviceSnapshot(serial)
		}
	}
	for serial, worker := range m.bacnetWorkers {
		if _, exists := bacnetSpecs[serial]; !exists {
			worker.stop()
			delete(m.bacnetWorkers, serial)
			removeDeviceSnapshot(serial)
		}
	}
	for serial, worker := range m.opcuaWorkers {
		if _, exists := opcuaSpecs[serial]; !exists {
			worker.stop()
			delete(m.opcuaWorkers, serial)
			removeDeviceSnapshot(serial)
		}
	}
	for serial, worker := range m.mqttWorkers {
		if _, exists := mqttSpecs[serial]; !exists {
			worker.stop()
			delete(m.mqttWorkers, serial)
			removeDeviceSnapshot(serial)
		}
	}

	// start new workers or refresh existing ones by restarting
	for serial, spec := range modbusSpecs {
		if worker, exists := m.modbusWorkers[serial]; exists {
			worker.stop()
			delete(m.modbusWorkers, serial)
			logInfo("collector restarting worker", zap.String("deviceSerial", serial))
		}

		worker := newDeviceWorker(m.ctx, spec)
		m.modbusWorkers[serial] = worker
		worker.start()
	}
	for serial, spec := range bacnetSpecs {
		if worker, exists := m.bacnetWorkers[serial]; exists {
			worker.stop()
			delete(m.bacnetWorkers, serial)
			logInfo("collector restarting BACnet worker", zap.String("deviceSerial", serial))
		}

		worker := newBacnetWorker(m.ctx, spec)
		m.bacnetWorkers[serial] = worker
		worker.start()
	}
	for serial, spec := range opcuaSpecs {
		if worker, exists := m.opcuaWorkers[serial]; exists {
			worker.stop()
			delete(m.opcuaWorkers, serial)
			logInfo("collector restarting OPC UA worker", zap.String("deviceSerial", serial))
		}

		worker := newOpcuaWorker(m.ctx, spec)
		m.opcuaWorkers[serial] = worker
		worker.start()
	}
	for serial, spec := range mqttSpecs {
		if worker, exists := m.mqttWorkers[serial]; exists {
			worker.stop()
			delete(m.mqttWorkers, serial)
			logInfo("collector restarting MQTT worker", zap.String("deviceSerial", serial))
		}

		worker := newMqttWorker(m.ctx, spec)
		m.mqttWorkers[serial] = worker
		worker.start()
	}
}

type deviceSpec struct {
	Device       config.DeviceRuntime
	Params       []config.ModbusParam
	Batches      []batchQuery
	batchedParam map[string]struct{}
}

func buildModbusDeviceSpecs(cfg config.IotCfgType) map[string]deviceSpec {
	specs := make(map[string]deviceSpec)
	for _, dev := range cfg.Devices {
		if !isModbusDevice(dev.Config.Protocol) {
			continue
		}
		if dev.Config.SerialNumber == "" {
			continue
		}
		params := dev.ReadPoints
		if len(params) == 0 {
			continue
		}
		batches, covered := buildBatchQueries(dev, params)

		specs[dev.Config.SerialNumber] = deviceSpec{
			Device:       dev,
			Params:       params,
			Batches:      batches,
			batchedParam: covered,
		}
	}
	return specs
}

func isModbusDevice(protocol string) bool {
	p := strings.ToLower(strings.TrimSpace(protocol))
	return p == "modbusrtu" || p == "modbus" || p == "modbustcp"
}

func isBacnetDevice(protocol string) bool {
	return strings.EqualFold(strings.TrimSpace(protocol), "bacnet")
}

func isOpcuaDevice(protocol string) bool {
	return strings.EqualFold(strings.TrimSpace(protocol), "opcua")
}

type deviceWorker struct {
	spec          deviceSpec
	ctx           context.Context
	cancel        context.CancelFunc
	done          chan struct{}
	gatewaySerial string
}

func newDeviceWorker(parent context.Context, spec deviceSpec) *deviceWorker {
	ctx, cancel := context.WithCancel(parent)
	return &deviceWorker{
		spec:          spec,
		ctx:           ctx,
		cancel:        cancel,
		done:          make(chan struct{}),
		gatewaySerial: gatewaySerial(spec.Device),
	}
}

func (w *deviceWorker) start() {
	go w.run()
}

func (w *deviceWorker) stop() {
	w.cancel()
	<-w.done
}

func (w *deviceWorker) run() {
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
			if err := waitGatewayResume(w.ctx, w.gatewaySerial); err != nil {
				return
			}
			w.collectOnce()
		case <-w.ctx.Done():
			return
		}
	}
}

func (w *deviceWorker) collectOnce() {
	for _, batch := range w.spec.Batches {
		if err := collectModbusBatch(w.ctx, w.spec.Device, batch); err != nil {
			if errors.Is(err, conn.ErrGatewayBusy) {
				logInfo("collector paused: gateway busy",
					zap.String("deviceSerial", w.spec.Device.Config.SerialNumber))
				return
			}
			logInfo("modbus batch collect failed",
				zap.String("deviceSerial", w.spec.Device.Config.SerialNumber),
				zap.Int("functionCode", batch.functionCode),
				zap.Uint16("startAddr", batch.startAddr),
				zap.Uint16("quantity", batch.quantity),
				zap.Error(err))
		}
	}

	for _, param := range w.spec.Params {
		if param.ReadDisabled {
			continue
		}
		if !isReadFunctionCode(param.FunctionCode) {
			continue
		}
		if _, ok := w.spec.batchedParam[paramKey(&param)]; ok {
			continue
		}
		if err := collectModbusParam(w.ctx, w.spec.Device, param); err != nil {
			logInfo("modbus collect failed",
				zap.String("deviceSerial", w.spec.Device.Config.SerialNumber),
				zap.String("param", param.Identify),
				zap.Error(err))
		}
	}
}

func isReadFunctionCode(functionCode int) bool {
	switch functionCode {
	case 1, 2, 3, 4:
		return true
	default:
		return false
	}
}

func logInfo(msg string, fields ...zap.Field) {
	if logger.Log == nil {
		return
	}
	logger.Log.Info(msg, fields...)
}

func logDebug(msg string, fields ...zap.Field) {
	if logger.Log == nil {
		return
	}
	logger.Log.Debug(msg, fields...)
}

func paramKey(param *config.ModbusParam) string {
	if param == nil {
		return ""
	}
	return strings.ToLower(param.Identify) + "|" +
		strconv.FormatUint(param.Address, 10) + "|" +
		strconv.Itoa(param.FunctionCode)
}
