package main

import (
	"context"
	"multiple-protocol-controller/internal/app"
	"multiple-protocol-controller/pkg/logger"

	// 引入协议包以注册协议实现
	_ "multiple-protocol-controller/internal/protocol/mqtt"
	_ "multiple-protocol-controller/internal/protocol/modbusRtu"
	_ "multiple-protocol-controller/internal/protocol/opcua"

	"go.uber.org/zap"
)

func main() {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	if err := app.Run(ctx); err != nil {
		logger.Log.Info("application exit with: ", zap.String("sig", err.Error()))
	}
}
