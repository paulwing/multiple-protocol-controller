package service

import (
	"context"
	"encoding/json"
	"fmt"
	"multiple-protocol-controller/internal/collector"
	"multiple-protocol-controller/internal/config"
	"multiple-protocol-controller/internal/conn"
	"multiple-protocol-controller/internal/store"
	"multiple-protocol-controller/pkg/logger"
	"slices"
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

	var iotCfg []config.DeviceConfig
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

		if err := json.Unmarshal(wrapper.Data, &iotCfg); err != nil {
			return fmt.Errorf("unmarshal data for %s failed: %w", config.DeviceCfg, err)
		}
		return nil
	})

	if err := g.Wait(); err != nil {
		return err
	}

	iotCfg = slices.DeleteFunc(iotCfg, func(v config.DeviceConfig) bool {
		return v.Disabled
	})

	runtimeCfg, buildErr := config.BuildRuntimeConfig(iotCfg)
	if buildErr != nil {
		return buildErr
	}

	collector.RefreshResultWriter(runtimeCfg)
	iotCfgStore.Store(runtimeCfg)

	if err := syncConnectionManager(rootCtx, runtimeCfg); err != nil {
		return err
	}

	collector.UpdateSnapshot(runtimeCfg)

	return nil
}

func syncConnectionManager(ctx context.Context, iotCfg config.IotCfgType) error {
	if _, exists := conn.Default(); !exists {
		if _, err := conn.InitDefault(ctx, iotCfg); err != nil {
			return fmt.Errorf("init tcp manager failed: %w", err)
		}
		return nil
	}
	if err := conn.RefreshDefault(iotCfg); err != nil {
		return fmt.Errorf("refresh tcp manager failed: %w", err)
	}
	return nil
}
