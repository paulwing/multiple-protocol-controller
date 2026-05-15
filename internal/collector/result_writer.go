package collector

import (
	"context"
	"encoding/json"
	"fmt"
	"strconv"
	"strings"
	"sync"
	"time"

	"multiple-protocol-controller/internal/config"
	"multiple-protocol-controller/internal/store"
	"multiple-protocol-controller/pkg/logger"

	"go.uber.org/zap"
)

const (
	deviceDataKeyPrefix = "device:data:"
	defaultDeviceStatus = "0"
	defaultStatusDesc   = "设备正常"
)

var (
	resultWriterMu sync.RWMutex
	resultWriter   *deviceResultWriter
)

// InitResultWriter prepares the redis writer responsible for persisting realtime device data.
func InitResultWriter(ctx context.Context, cfg *config.Config) error {
	resultWriterMu.Lock()
	defer resultWriterMu.Unlock()

	if resultWriter != nil {
		return nil
	}

	client, err := store.NewRedisClient(ctx, cfg.Redis.Address, cfg.Redis.Pwd, 0)
	if err != nil {
		return fmt.Errorf("init collector redis client failed: %w", err)
	}

	resultWriter = &deviceResultWriter{
		redis:     client,
		snapshots: make(map[string]*deviceSnapshot),
	}

	go func() {
		<-ctx.Done()
		resultWriterMu.Lock()
		defer resultWriterMu.Unlock()
		_ = client.Close()
		resultWriter = nil
	}()

	return nil
}

// RefreshResultWriter rebuilds the in-memory snapshot cache using the latest IoT configuration.
func RefreshResultWriter(cfg config.IotCfgType) {
	writer := currentResultWriter()
	if writer == nil {
		return
	}
	if err := writer.applyConfig(cfg); err != nil {
		logger.Log.Warn("refresh result writer failed", zap.Error(err))
	}
}

// UpdateDeviceStatus writes the latest device status (and g_sys_status) to the realtime snapshot.
func UpdateDeviceStatus(serial string, status int, desc string) {
	writer := currentResultWriter()
	if writer == nil {
		return
	}
	writer.updateStatus(serial, status, desc)
}

func recordCollectedValue(device config.DeviceRuntime, param config.ModbusParam, value interface{}) {
	writer := currentResultWriter()
	if writer == nil {
		return
	}
	if err := writer.record(device, param, value); err != nil {
		logger.Log.Warn("write realtime data failed",
			zap.String("deviceSerial", device.Config.SerialNumber),
			zap.String("param", param.Identify),
			zap.Error(err))
	}
}

func recordCollectedOpcuaValue(device config.DeviceRuntime, param config.OpcuaParam, value interface{}) {
	writer := currentResultWriter()
	if writer == nil {
		return
	}
	if err := writer.recordOpcua(device, param, value); err != nil {
		logger.Log.Warn("write realtime data failed",
			zap.String("deviceSerial", device.Config.SerialNumber),
			zap.String("param", param.Identify),
			zap.Error(err))
	}
}

func recordCollectedBacnetValue(device config.DeviceRuntime, param config.BacnetParam, value interface{}) {
	writer := currentResultWriter()
	if writer == nil {
		return
	}
	if err := writer.recordBacnet(device, param, value); err != nil {
		logger.Log.Warn("write realtime data failed",
			zap.String("deviceSerial", device.Config.SerialNumber),
			zap.String("param", param.Identify),
			zap.Error(err))
	}
}

func removeDeviceSnapshot(serial string) {
	writer := currentResultWriter()
	if writer == nil {
		return
	}
	writer.remove(serial)
}

func currentResultWriter() *deviceResultWriter {
	resultWriterMu.RLock()
	defer resultWriterMu.RUnlock()
	return resultWriter
}

type deviceResultWriter struct {
	redis     *store.RedisClient
	mu        sync.Mutex
	snapshots map[string]*deviceSnapshot
}

type deviceSnapshot struct {
	meta       deviceMeta
	values     map[string]interface{}
	pointNames map[string]string
	status     string
	statusDesc string
}

type deviceMeta struct {
	DeviceID      string
	DeviceName    string
	DeviceType    string
	ProductType   string
	AddressID     string
	Factory       string
	DevicePicture string
	DeviceModel   string
}

type deviceResult struct {
	DeviceID          string                 `json:"device_id"`
	DeviceType        string                 `json:"device_type"`
	DeviceName        string                 `json:"device_name"`
	ProductType       string                 `json:"product_type"`
	CurrentValue      map[string]interface{} `json:"current_value"`
	PointName         map[string]string      `json:"point_name"`
	AddressID         string                 `json:"address_id"`
	Factory           string                 `json:"factory"`
	DevicePicture     string                 `json:"device_picture"`
	DeviceModel       string                 `json:"device_model"`
	Status            string                 `json:"status"`
	StatusDescription string                 `json:"status_description"`
	Timestamp         int64                  `json:"timestamp"`
}

func (w *deviceResultWriter) applyConfig(cfg config.IotCfgType) error {
	snapshots := make(map[string]*deviceSnapshot, len(cfg.Devices))

	for _, dev := range cfg.Devices {
		if dev.Config.SerialNumber == "" {
			continue
		}

		pointNames := make(map[string]string)
		for _, p := range dev.ReadPoints {
			pointNames[p.Identify] = p.Name
		}
		for _, p := range dev.BacnetReadPoints {
			pointNames[p.Identify] = p.Name
		}
		for _, p := range dev.OpcuaReadPoints {
			pointNames[p.Identify] = p.Name
		}
		for _, p := range dev.MqttReadPoints {
			pointNames[p.Identify] = p.Name
		}

		snapshots[dev.Config.SerialNumber] = &deviceSnapshot{
			meta: deviceMeta{
				DeviceID:      dev.Config.ID,
				DeviceName:    dev.Config.DeviceName,
				DeviceType:    strconv.Itoa(dev.Config.DeviceType),
				ProductType:   "",
				AddressID:     dev.Config.SerialNumber,
				Factory:       "",
				DevicePicture: "",
				DeviceModel:   "",
			},
			values:     make(map[string]interface{}),
			pointNames: pointNames,
			status:     defaultDeviceStatus,
			statusDesc: defaultStatusDesc,
		}
	}

	w.mu.Lock()
	w.snapshots = snapshots
	w.mu.Unlock()

	return nil
}

func (w *deviceResultWriter) record(device config.DeviceRuntime, param config.ModbusParam, value interface{}) error {
	if device.Config.SerialNumber == "" || device.Config.ID == "" || param.Identify == "" {
		return fmt.Errorf("device serial, device id or parameter identify missing")
	}

	w.mu.Lock()
	snapshot := w.ensureSnapshot(device)
	snapshot.values[param.Identify] = value
	result := snapshot.cloneResult()
	w.mu.Unlock()

	payload, err := json.Marshal(result)
	if err != nil {
		return err
	}
	key, err := deviceDataKey(device)
	if err != nil {
		return err
	}
	return w.redis.Set(context.Background(), key, string(payload), 0)
}

func (w *deviceResultWriter) recordOpcua(device config.DeviceRuntime, param config.OpcuaParam, value interface{}) error {
	if device.Config.SerialNumber == "" || device.Config.ID == "" || param.Identify == "" {
		return fmt.Errorf("device serial, device id or parameter identify missing")
	}

	w.mu.Lock()
	snapshot := w.ensureSnapshot(device)
	snapshot.values[param.Identify] = value
	result := snapshot.cloneResult()
	w.mu.Unlock()

	payload, err := json.Marshal(result)
	if err != nil {
		return err
	}
	key, err := deviceDataKey(device)
	if err != nil {
		return err
	}
	return w.redis.Set(context.Background(), key, string(payload), 0)
}

func (w *deviceResultWriter) recordBacnet(device config.DeviceRuntime, param config.BacnetParam, value interface{}) error {
	if device.Config.SerialNumber == "" || device.Config.ID == "" || param.Identify == "" {
		return fmt.Errorf("device serial, device id or parameter identify missing")
	}

	w.mu.Lock()
	snapshot := w.ensureSnapshot(device)
	snapshot.values[param.Identify] = value
	result := snapshot.cloneResult()
	w.mu.Unlock()

	payload, err := json.Marshal(result)
	if err != nil {
		return err
	}
	key, err := deviceDataKey(device)
	if err != nil {
		return err
	}
	return w.redis.Set(context.Background(), key, string(payload), 0)
}

func (w *deviceResultWriter) recordMqtt(device config.DeviceRuntime, param config.MqttParam, value interface{}) error {
	if device.Config.SerialNumber == "" || device.Config.ID == "" || param.Identify == "" {
		return fmt.Errorf("device serial, device id or parameter identify missing")
	}

	w.mu.Lock()
	snapshot := w.ensureSnapshot(device)
	snapshot.values[param.Identify] = value
	result := snapshot.cloneResult()
	w.mu.Unlock()

	payload, err := json.Marshal(result)
	if err != nil {
		return err
	}
	key, err := deviceDataKey(device)
	if err != nil {
		return err
	}
	return w.redis.Set(context.Background(), key, string(payload), 0)
}

func deviceDataKey(device config.DeviceRuntime) (string, error) {
	id := strings.TrimSpace(device.Config.ID)
	if id == "" {
		return "", fmt.Errorf("device id missing")
	}
	return deviceDataKeyPrefix + id, nil
}

func (w *deviceResultWriter) ensureSnapshot(device config.DeviceRuntime) *deviceSnapshot {
	if snap, exists := w.snapshots[device.Config.SerialNumber]; exists {
		return snap
	}
	snap := &deviceSnapshot{
		meta: deviceMeta{
			DeviceID:   device.Config.ID,
			DeviceName: device.Config.DeviceName,
			AddressID:  device.Config.SerialNumber,
		},
		values:     make(map[string]interface{}),
		pointNames: make(map[string]string),
		status:     defaultDeviceStatus,
		statusDesc: defaultStatusDesc,
	}
	w.snapshots[device.Config.SerialNumber] = snap
	return snap
}

func (w *deviceResultWriter) remove(serial string) {
	w.mu.Lock()
	defer w.mu.Unlock()
	delete(w.snapshots, serial)
}

func (w *deviceResultWriter) updateStatus(serial string, status int, desc string) {
	w.mu.Lock()
	defer w.mu.Unlock()
	snap := w.snapshots[serial]
	if snap == nil {
		snap = &deviceSnapshot{
			meta:       deviceMeta{DeviceID: serial, AddressID: serial},
			values:     make(map[string]interface{}),
			pointNames: make(map[string]string),
		}
		w.snapshots[serial] = snap
	}
	snap.status = strconv.Itoa(status)
	if desc != "" {
		snap.statusDesc = desc
	}
}

func (d *deviceSnapshot) cloneResult() deviceResult {
	res := deviceResult{
		DeviceID:          d.meta.DeviceID,
		DeviceType:        d.meta.DeviceType,
		DeviceName:        d.meta.DeviceName,
		ProductType:       d.meta.ProductType,
		CurrentValue:      make(map[string]interface{}, len(d.values)),
		PointName:         make(map[string]string, len(d.pointNames)),
		AddressID:         d.meta.AddressID,
		Factory:           d.meta.Factory,
		DevicePicture:     d.meta.DevicePicture,
		DeviceModel:       d.meta.DeviceModel,
		Status:            d.status,
		StatusDescription: d.statusDesc,
		Timestamp:         time.Now().UnixMilli(),
	}
	for k, v := range d.values {
		res.CurrentValue[k] = v
	}
	for k, v := range d.pointNames {
		res.PointName[k] = v
	}
	return res
}
