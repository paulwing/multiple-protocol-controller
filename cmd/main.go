package main

import (
	"context"
	"multiple-protocol-controller/internal/app"
	"multiple-protocol-controller/pkg/logger"

	// _ "multiple-protocol-controller/internal/protocol/modbusRtu"

	"go.uber.org/zap"
)

func main() {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	if err := app.Run(ctx); err != nil {
		logger.Log.Info("application exit with: ", zap.String("sig", err.Error()))
	}
}
