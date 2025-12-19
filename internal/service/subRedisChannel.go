package service

import (
	"context"
	"encoding/json"
	"fmt"
	"multiple-protocol-controller/internal/config"
	"multiple-protocol-controller/internal/store"
	"multiple-protocol-controller/pkg/logger"

	"github.com/redis/go-redis/v9"
	"go.uber.org/zap"
)

func SubRedisChannel(ctx context.Context, redisClient *store.RedisClient, cfg *config.Config) error {
	cfgChangeArr := []string{}

	handlers := map[string]func(msg *redis.Message) error{
		config.CfgChangeCh: func(msg *redis.Message) error {
			fmt.Println("配置变更:", msg.Payload)
			itemKey := msg.Payload
			// todo: 跟学开重新设计该频道消息内容及此处逻辑（树仁已将新版配置下发逻辑改为单个下发）
			var getCfgErr error
			if itemKey == "END" {
				// kill goroutine...
				getCfgErr = GetIoTCfg(ctx, &config.IotCfgStore, cfg)
			} else {
				cfgChangeArr = append(cfgChangeArr, itemKey) // 目前考虑到配置变更频道消息是单线程分发，先简单处理，如果将来出现多个消息并发触发，考虑加锁或用channel模式
				if itemKey != "BEGIN" && len(cfgChangeArr) == 1 {
					// kill goroutine...
					getCfgErr = GetIoTCfg(ctx, &config.IotCfgStore, cfg)
				}
			}
			if getCfgErr != nil {
				logger.Log.Error("get cfg error:", zap.Error(getCfgErr))
				// return getCfgErr
			}
			fmt.Println("重新获取配置")
			return nil
		},
		config.SetDeviceProperty: func(msg *redis.Message) error {
			// 参考mpc iot-system-base this.business!.processCommandData(data);
			fmt.Println("Recv original command data:", msg.Payload)
			cmdParams := config.Command4MPC{}
			err := json.Unmarshal([]byte(msg.Payload), &cmdParams)
			if err != nil {
				logger.Log.Error("device command parse error:", zap.Error(err))
				return nil
			}
			// sendErr := control.ProcessCommand(cmdParams)
			// if sendErr != nil {
			// 	logger.Log.Error("send command error:", zap.Error(sendErr))
			// }
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
