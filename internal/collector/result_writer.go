package collector

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net"
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
	judgeSource := normalizedJudgeSourceConfig(cfg.JudgeSource)
	if ctx == nil {
		ctx = context.Background()
	}

	resultWriter = &deviceResultWriter{
		ctx:         ctx,
		redis:       client,
		history:     newHistoryWriter(ctx, cfg.Influx),
		snapshots:   make(map[string]*deviceSnapshot),
		judgeSource: judgeSource,
		newEventID:  newJudgeEventID,
	}

	go func() {
		<-ctx.Done()
		resultWriterMu.RLock()
		writer := resultWriter
		resultWriterMu.RUnlock()
		if writer != nil && writer.history != nil {
			writer.history.stop()
		}
		_ = client.Close()

		resultWriterMu.Lock()
		defer resultWriterMu.Unlock()
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
	ctx         context.Context
	redis       resultWriterRedis
	history     *historyWriter
	mu          sync.RWMutex
	snapshots   map[string]*deviceSnapshot
	judgeSource judgeSourceConfig
	newEventID  func() (string, error)
}

type deviceSnapshot struct {
	mu         sync.Mutex
	meta       deviceMeta
	values     map[string]interface{}
	pointNames map[string]string
	status     string
	statusDesc string
}

type resultWriterRedis interface {
	Set(context.Context, string, interface{}, time.Duration) error
	SetAndXAdd(context.Context, string, any, string, map[string]any) store.SnapshotStreamWriteResult
	XAdd(context.Context, string, map[string]any) (string, error)
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
	w.mu.RLock()
	existing := make(map[string]*deviceSnapshot, len(w.snapshots))
	for serial, snapshot := range w.snapshots {
		existing[serial] = snapshot
	}
	w.mu.RUnlock()

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
		meta := deviceMeta{
			DeviceID:      dev.Config.ID,
			DeviceName:    dev.Config.DeviceName,
			DeviceType:    strconv.Itoa(dev.Config.DeviceType),
			ProductType:   "",
			AddressID:     dev.Config.SerialNumber,
			Factory:       "",
			DevicePicture: "",
			DeviceModel:   "",
		}

		var snapshot *deviceSnapshot
		if existingSnapshot := existing[dev.Config.SerialNumber]; existingSnapshot != nil {
			existingSnapshot.mu.Lock()
			if existingSnapshot.meta.DeviceID == dev.Config.ID {
				existingSnapshot.meta = meta
				existingSnapshot.values = make(map[string]interface{})
				existingSnapshot.pointNames = pointNames
				existingSnapshot.status = defaultDeviceStatus
				existingSnapshot.statusDesc = defaultStatusDesc
				snapshot = existingSnapshot
			}
			existingSnapshot.mu.Unlock()
		}
		if snapshot == nil {
			snapshot = &deviceSnapshot{
				meta:       meta,
				values:     make(map[string]interface{}),
				pointNames: pointNames,
				status:     defaultDeviceStatus,
				statusDesc: defaultStatusDesc,
			}
		}
		snapshots[dev.Config.SerialNumber] = snapshot
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
	return w.recordValue(device, param.Identify, value, historyPointFromModbus(device, param, value))
}

func (w *deviceResultWriter) recordOpcua(device config.DeviceRuntime, param config.OpcuaParam, value interface{}) error {
	if device.Config.SerialNumber == "" || device.Config.ID == "" || param.Identify == "" {
		return fmt.Errorf("device serial, device id or parameter identify missing")
	}

	return w.recordValue(device, param.Identify, value, historyPointFromOpcua(device, param, value))
}

func (w *deviceResultWriter) recordBacnet(device config.DeviceRuntime, param config.BacnetParam, value interface{}) error {
	if device.Config.SerialNumber == "" || device.Config.ID == "" || param.Identify == "" {
		return fmt.Errorf("device serial, device id or parameter identify missing")
	}

	return w.recordValue(device, param.Identify, value, historyPointFromBacnet(device, param, value))
}

func (w *deviceResultWriter) recordMqtt(device config.DeviceRuntime, param config.MqttParam, value interface{}) error {
	if device.Config.SerialNumber == "" || device.Config.ID == "" || param.Identify == "" {
		return fmt.Errorf("device serial, device id or parameter identify missing")
	}

	return w.recordValue(device, param.Identify, value, historyPointFromMqtt(device, param, value))
}

func (w *deviceResultWriter) recordValue(
	device config.DeviceRuntime,
	updatedPoint string,
	value any,
	history historyPoint,
) error {
	snapshot := w.ensureSnapshot(device)
	snapshot.mu.Lock()
	defer snapshot.mu.Unlock()

	snapshot.values[updatedPoint] = value
	result := snapshot.cloneResult()
	payload, err := json.Marshal(result)
	if err != nil {
		return err
	}
	key, err := deviceDataKey(device)
	if err != nil {
		return err
	}

	baseContext := w.ctx
	if baseContext == nil {
		baseContext = context.Background()
	}
	if !w.judgeSource.enabled {
		if err := w.redis.Set(baseContext, key, string(payload), 0); err != nil {
			return err
		}
		w.history.writeAsync(history)
		return nil
	}

	newEventID := w.newEventID
	if newEventID == nil {
		newEventID = newJudgeEventID
	}
	eventID, sourceErr := newEventID()
	var event judgeSourceEvent
	if sourceErr == nil {
		event, sourceErr = buildJudgeSourceEvent(
			eventID, device.Config.ID, updatedPoint, time.UnixMilli(result.Timestamp),
			snapshot.cloneValues(), w.judgeSource.maximumEventBytes,
		)
	}
	if sourceErr != nil {
		realtimeErr := w.redis.Set(baseContext, key, string(payload), 0)
		if realtimeErr == nil {
			w.history.writeAsync(history)
		}
		w.logJudgeSourceFailure(device.Config.ID, updatedPoint, "SOURCE_EVENT_INVALID", sourceErr)
		return realtimeErr
	}

	streamValues := event.redisValues()
	writeContext, cancel := context.WithTimeout(baseContext, w.judgeSource.writeTimeout)
	writeResult := w.redis.SetAndXAdd(writeContext, key, string(payload), w.judgeSource.stream, streamValues)
	cancel()

	if writeResult.SnapshotErr == nil {
		w.history.writeAsync(history)
	}
	if writeResult.StreamErr != nil {
		sourceErr = w.retryJudgeSource(baseContext, streamValues, writeResult.StreamErr)
		if sourceErr != nil {
			w.logJudgeSourceFailure(
				device.Config.ID,
				updatedPoint,
				classifyJudgeSourceError(sourceErr),
				sourceErr,
			)
		}
	}
	return writeResult.SnapshotErr
}

func deviceDataKey(device config.DeviceRuntime) (string, error) {
	id := strings.TrimSpace(device.Config.ID)
	if id == "" {
		return "", fmt.Errorf("device id missing")
	}
	return deviceDataKeyPrefix + id, nil
}

func (w *deviceResultWriter) ensureSnapshot(device config.DeviceRuntime) *deviceSnapshot {
	w.mu.RLock()
	snapshot := w.snapshots[device.Config.SerialNumber]
	w.mu.RUnlock()
	if snapshot != nil {
		return snapshot
	}

	w.mu.Lock()
	defer w.mu.Unlock()
	if snap, exists := w.snapshots[device.Config.SerialNumber]; exists {
		return snap
	}
	snapshot = &deviceSnapshot{
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
	w.snapshots[device.Config.SerialNumber] = snapshot
	return snapshot
}

func (w *deviceResultWriter) remove(serial string) {
	w.mu.Lock()
	defer w.mu.Unlock()
	delete(w.snapshots, serial)
}

func (w *deviceResultWriter) updateStatus(serial string, status int, desc string) {
	w.mu.RLock()
	snap := w.snapshots[serial]
	w.mu.RUnlock()
	if snap == nil {
		candidate := &deviceSnapshot{
			meta:       deviceMeta{DeviceID: serial, AddressID: serial},
			values:     make(map[string]interface{}),
			pointNames: make(map[string]string),
			status:     defaultDeviceStatus,
			statusDesc: defaultStatusDesc,
		}
		w.mu.Lock()
		if snap = w.snapshots[serial]; snap == nil {
			snap = candidate
			w.snapshots[serial] = snap
		}
		w.mu.Unlock()
	}
	snap.mu.Lock()
	defer snap.mu.Unlock()
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

func (d *deviceSnapshot) cloneValues() map[string]any {
	result := make(map[string]any, len(d.values))
	for key, value := range d.values {
		result[key] = value
	}
	return result
}

func (w *deviceResultWriter) retryJudgeSource(
	ctx context.Context,
	streamValues map[string]any,
	initialErr error,
) error {
	lastErr := initialErr
	for attempt := 0; attempt < w.judgeSource.retryCount; attempt++ {
		if err := waitForJudgeRetry(ctx, w.judgeSource.retryInterval); err != nil {
			return err
		}
		attemptContext, cancel := context.WithTimeout(ctx, w.judgeSource.writeTimeout)
		_, err := w.redis.XAdd(attemptContext, w.judgeSource.stream, streamValues)
		cancel()
		if err == nil {
			return nil
		}
		lastErr = err
	}
	return lastErr
}

func waitForJudgeRetry(ctx context.Context, delay time.Duration) error {
	timer := time.NewTimer(delay)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return nil
	}
}

func (w *deviceResultWriter) logJudgeSourceFailure(
	deviceID string,
	updatedPoint string,
	reasonCode string,
	err error,
) {
	if logger.Log == nil {
		return
	}
	logger.Log.Warn(
		"judge source event dropped",
		zap.String("reason_code", reasonCode),
		zap.String("deviceID", deviceID),
		zap.String("updatedPoint", updatedPoint),
		zap.Error(err),
	)
}

func classifyJudgeSourceError(err error) string {
	if err == nil {
		return ""
	}
	if errors.Is(err, context.DeadlineExceeded) {
		return "REDIS_TIMEOUT"
	}
	var networkError net.Error
	if errors.As(err, &networkError) && networkError.Timeout() {
		return "REDIS_TIMEOUT"
	}

	message := strings.ToUpper(err.Error())
	switch {
	case strings.Contains(message, "OOM"), strings.Contains(message, "MAXMEMORY"):
		return "REDIS_MEMORY_EXHAUSTED"
	case strings.Contains(message, "READONLY"):
		return "REDIS_READONLY"
	case strings.Contains(message, "MISCONF"):
		return "REDIS_PERSISTENCE_ERROR"
	case strings.Contains(message, "POOL") && strings.Contains(message, "TIMEOUT"):
		return "REDIS_POOL_EXHAUSTED"
	case strings.Contains(message, "NOAUTH"), strings.Contains(message, "WRONGPASS"), strings.Contains(message, "NOPERM"):
		return "REDIS_AUTH_FAILED"
	case strings.Contains(message, "WRONGTYPE"):
		return "REDIS_KEY_TYPE_INVALID"
	case errors.Is(err, context.Canceled),
		strings.Contains(message, "CONNECTION REFUSED"),
		strings.Contains(message, "CONNECTION RESET"),
		strings.Contains(message, "NO ROUTE TO HOST"),
		strings.Contains(message, "BROKEN PIPE"),
		message == "EOF":
		return "REDIS_UNAVAILABLE"
	default:
		return "REDIS_WRITE_FAILED"
	}
}
