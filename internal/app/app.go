package app

import (
	"context"
	"fmt"
	"multiple-protocol-controller/internal/config"
	"multiple-protocol-controller/internal/service"
	"multiple-protocol-controller/internal/store"
	"multiple-protocol-controller/pkg/logger"
	"os"
	"os/signal"
	"syscall"

	"go.uber.org/zap"
	"golang.org/x/sync/errgroup"
)

func Run(ctx context.Context) error {
	// 1. 加载配置
	cfg, err := config.Load("configs/config.toml")
	if err != nil {
		return err
	}

	// if err := control.Init(ctx, cfg); err != nil {
	// 	return fmt.Errorf("control init failed: %w", err)
	// }

	// if err := collector.InitResultWriter(ctx, cfg); err != nil {
	// 	return fmt.Errorf("collector writer init failed: %w", err)
	// }

	g, ctx := errgroup.WithContext(ctx)
	// collector.Start(ctx)
	// 1. 捕获系统信号
	g.Go(func() error {
		sigCh := make(chan os.Signal, 1)
		signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
		select {
		case <-ctx.Done():
			return nil // 上下文取消，退出
		case sig := <-sigCh:
			logger.Log.Info("收到系统信号:", zap.String("sig", sig.String()))
			err = fmt.Errorf("by signal %v", sig)
			return err
		}
	})
	// 2. 初始化日志
	var isDev bool
	if cfg.RunMode == "dev" {
		isDev = true
	} else {
		isDev = false
	}
	g.Go(func() error {
		if err := logger.InitLogger("logs/app.log", "info", isDev); err != nil {
			return fmt.Errorf("logger init failed: %w", err)
		}
		logger.Log.Info("logger initialized")

		<-ctx.Done() // 等待context被取消或超时关闭
		logger.Log.Info("logger shutting down")
		_ = logger.Log.Sync() // flush 缓存，防止丢日志
		return nil
	})
	// 3. 订阅频道
	g.Go(func() error {
		redisClient, err := store.NewRedisClient(ctx, cfg.Redis.Address, cfg.Redis.Pwd, 0)
		if err != nil {
			return fmt.Errorf("subclient init failed: %w", err)
		}
		fmt.Println("redis subclient connect success")
		return service.SubRedisChannel(ctx, redisClient, cfg)
	})
	// 4.发送服务心跳
	g.Go(func() error {
		redisClient, err := store.NewRedisClient(ctx, cfg.Redis.Address, cfg.Redis.Pwd, 0)
		if err != nil {
			return fmt.Errorf("dataclient init failed: %w", err)
		}
		fmt.Println("redis dataclient connect success")
		return service.SetServiceStatus(ctx, redisClient)
	})
	// 5.获取iot配置
	g.Go(func() error {
		fmt.Println("redis getdataclient connect success")
		err := service.GetIoTCfg(ctx, &config.IotCfgStore, cfg)
		if err != nil {
			fmt.Println("get config info err:", err.Error())
		}
		return nil
	})

	err = g.Wait()

	// 优雅退出逻辑
	return err
}
