package service

import (
	"context"
	"encoding/json"
	"multiple-protocol-controller/internal/config"
	"multiple-protocol-controller/internal/store"
	"multiple-protocol-controller/pkg/logger"
	"time"

	"go.uber.org/zap"
)

func SetServiceStatus(ctx context.Context, redisClient *store.RedisClient) error {
	go func() {
		ticker := time.NewTicker(60 * time.Second)
		defer ticker.Stop() // 确保退出时释放定时器
		for {
			select {
			case <-ticker.C:
				currentTime := time.Now().UnixMilli()
				statusInfo := map[string]interface{}{
					"time":           currentTime,
					"status":         0,
					"currentVersion": "1.0",
				}
				infoStr, _ := json.Marshal(statusInfo)
				setErr := redisClient.SetWithTimeout(config.ServerStatus, string(infoStr), 0, 2*time.Second)
				if setErr != nil {
					logger.Log.Warn("设置心跳信息失败", zap.Error(setErr))
				}
			case <-ctx.Done():
				_ = redisClient.Close()
				logger.Log.Warn("SetServiceStatus closed by context")
				return
			}
		}
	}()
	return nil
}
