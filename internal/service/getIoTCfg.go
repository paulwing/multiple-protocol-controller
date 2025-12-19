package service

import (
	"context"
	"encoding/json"
	"fmt"
	"multiple-protocol-controller/internal/config"
	"multiple-protocol-controller/internal/store"
	"multiple-protocol-controller/pkg/logger"
	"sync/atomic"
	"time"

	"go.uber.org/zap"
	"golang.org/x/sync/errgroup"
)

func GetIoTCfg(ctx context.Context, iotCfgStore *atomic.Value, cfg *config.Config) error {
	rootCtx := ctx

	redisClient, err := store.NewRedisClient(rootCtx, cfg.Redis.Address, cfg.Redis.Pwd, 0)
	if err != nil {
		return fmt.Errorf("getdataclient init failed: %w", err)
	}
	defer redisClient.Close()

	var iotDevices []config.DeviceConfig
	// 并发获取
	g, _ := errgroup.WithContext(rootCtx)
	g.Go(func() error {
		start := time.Now()
		val, err := redisClient.GetWithTimeout(config.DeviceCfg, 10*time.Second)
		elapsed := time.Since(start)
		logger.Log.Info("get cfg data cost: ", zap.String("time", elapsed.String()), zap.String("key", config.DeviceCfg))
		if err != nil {
			return fmt.Errorf("get %s failed: %w", config.DeviceCfg, err)
		}
		var wrapper config.RedisWrapper
		if err := json.Unmarshal([]byte(val), &wrapper); err != nil {
			return fmt.Errorf("unmarshal wrapper for %s failed: %w", config.DeviceCfg, err)
		}

		if err := json.Unmarshal(wrapper.Data, &iotDevices); err != nil {
			return fmt.Errorf("unmarshal data for %s failed: %w", config.DeviceCfg, err)
		}
		return nil
	})

	if err := g.Wait(); err != nil {
		return err
	}
	fmt.Println(iotDevices)
	// todo: to be continued...
	// fmtErr := validateIoTCfg(&iotCfg)
	// if fmtErr != nil {
	// 	return fmtErr
	// }
	// fmt.Println(len(iotCfg.IoTProperties), len(iotCfg.IoTProducts))

	// collector.RefreshResultWriter(iotCfg)
	// iotCfgStore.Store(iotCfg)

	// if err := syncConnectionManager(rootCtx, iotCfg); err != nil {
	// 	return err
	// }

	// collector.UpdateSnapshot(iotCfg)

	return nil
}

// func syncConnectionManager(ctx context.Context, iotCfg config.IotCfgType) error {
// 	if _, exists := conn.Default(); !exists {
// 		if _, err := conn.InitDefault(ctx, iotCfg); err != nil {
// 			return fmt.Errorf("init tcp manager failed: %w", err)
// 		}
// 		return nil
// 	}
// 	if err := conn.RefreshDefault(iotCfg); err != nil {
// 		return fmt.Errorf("refresh tcp manager failed: %w", err)
// 	}
// 	return nil
// }
