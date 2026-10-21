package service

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"multiple-protocol-controller/internal/config"
	"multiple-protocol-controller/internal/control"
	"multiple-protocol-controller/internal/store"
	"multiple-protocol-controller/pkg/logger"

	"github.com/redis/go-redis/v9"
	"go.uber.org/zap"
)

func SubRedisChannel(ctx context.Context, redisClient *store.RedisClient, cfg *config.Config) error {
	handlers := map[string]func(msg *redis.Message) error{
		config.CfgChangeCh: newCfgChangeHandler(func() error {
			return GetIoTCfg(ctx, &config.IotCfgStore, cfg)
		}),
		config.SetDeviceProperty: func(msg *redis.Message) error {
			// 参考mpc iot-system-base this.business!.processCommandData(data);
			fmt.Println("Recv original command data:", msg.Payload)
			cmdParams, err := decodeCommandPayload([]byte(msg.Payload))
			if err != nil {
				logger.Log.Error("device command parse error:", zap.Error(err))
				return nil
			}
			sendErr := control.ProcessCommand(redisClient, cmdParams)
			if sendErr != nil {
				logger.Log.Error("send command error:", zap.Error(sendErr))
			}
			return nil
		},
	}
	// 带自动重连机制的订阅
	errCh := redisClient.SubscribeWithRetry(ctx, []string{config.CfgChangeCh, config.SetDeviceProperty}, handlers)

	select {
	case <-ctx.Done():
		_ = redisClient.Close()
		logger.Log.Warn("SubRedisChannel closed by context")
		return nil
	case err := <-errCh:
		_ = redisClient.Close() // ✅ 同样关闭，释放连接池资源
		logger.Log.Error("SubRedisChannel exited with error", zap.Error(err))
		return err
	}
}

// decodeCommandPayload keeps numbers as json.Number so an int64/uint64 point is
// not rounded through float64 before it reaches the protocol encoder. The
// trailing-content rejection matches the json.Unmarshal this replaced.
func decodeCommandPayload(payload []byte) (config.Command4MPC, error) {
	var command config.Command4MPC
	decoder := json.NewDecoder(bytes.NewReader(payload))
	decoder.UseNumber()
	if err := decoder.Decode(&command); err != nil {
		return config.Command4MPC{}, err
	}
	if err := decoder.Decode(&struct{}{}); err != io.EOF {
		return config.Command4MPC{}, fmt.Errorf("unexpected trailing content")
	}
	return command, nil
}

func newCfgChangeHandler(reload func() error) func(msg *redis.Message) error {
	return func(msg *redis.Message) error {
		fmt.Println("配置变更:", msg.Payload)
		if msg.Payload != config.DeviceCfg {
			return nil
		}
		if err := reload(); err != nil {
			logger.Log.Error("get cfg error:", zap.Error(err))
			return nil
		}
		fmt.Println("重新获取配置")
		return nil
	}
}
