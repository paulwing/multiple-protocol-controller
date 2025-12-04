package logger

import (
	"fmt"
	"os"
	"time"

	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
	"gopkg.in/natefinch/lumberjack.v2"
)

var Log *zap.Logger

// InitLogger 初始化日志
// logFile: 日志文件路径，例如 "logs/app.log"
// level: 日志级别（debug/info/warn/error）
// isDev: 是否开发模式（true -> 控制台彩色输出）
func InitLogger(logFile string, level string, isDev bool) error {
	// 确保日志目录存在
	if err := os.MkdirAll(getDir(logFile), 0755); err != nil {
		return fmt.Errorf("failed to create log dir: %w", err)
	}

	// 日志级别
	var logLevel zapcore.Level
	switch level {
	case "debug":
		logLevel = zap.DebugLevel
	case "info":
		logLevel = zap.InfoLevel
	case "warn":
		logLevel = zap.WarnLevel
	case "error":
		logLevel = zap.ErrorLevel
	default:
		logLevel = zap.InfoLevel
	}

	// 日志轮转配置
	writer := zapcore.AddSync(&lumberjack.Logger{
		Filename:   logFile, // 日志文件路径
		MaxSize:    100,     // 每个日志文件最大 100MB
		MaxBackups: 7,       // 最多保留 7 个备份
		MaxAge:     30,      // 最多保留 30 天
		Compress:   true,    // 是否压缩旧日志
	})

	// 日志格式
	encoderConfig := zap.NewProductionEncoderConfig()
	encoderConfig.TimeKey = "time"
	encoderConfig.LevelKey = "level"
	encoderConfig.NameKey = "logger"
	encoderConfig.CallerKey = "caller"
	encoderConfig.MessageKey = "msg"
	encoderConfig.StacktraceKey = "stacktrace"
	encoderConfig.EncodeLevel = zapcore.CapitalColorLevelEncoder
	encoderConfig.EncodeTime = func(t time.Time, enc zapcore.PrimitiveArrayEncoder) {
		enc.AppendString(t.Format("2006-01-02 15:04:05"))
	}
	encoderConfig.EncodeCaller = zapcore.ShortCallerEncoder

	// var encoder zapcore.Encoder
	stdEncoder := zapcore.NewConsoleEncoder(encoderConfig) // 控制台输出
	encoderConfig.EncodeLevel = zapcore.CapitalLevelEncoder
	fsEncoder := zapcore.NewJSONEncoder(encoderConfig) // 日志文件JSON 格式

	var core zapcore.Core

	if isDev {
		// 开发环境：同时输出到文件和控制台
		core = zapcore.NewTee(
			zapcore.NewCore(fsEncoder, writer, logLevel),
			zapcore.NewCore(stdEncoder, zapcore.AddSync(os.Stdout), logLevel),
		)
	} else {
		// 生产环境
		core = zapcore.NewCore(fsEncoder, writer, logLevel)
	}

	Log = zap.New(core, zap.AddCaller(), zap.AddCallerSkip(0))

	return nil
}

// getDir 提取文件路径中的目录部分
func getDir(path string) string {
	if idx := len(path) - 1; idx >= 0 {
		for i := idx; i >= 0; i-- {
			if path[i] == '/' {
				return path[:i]
			}
		}
	}
	return "."
}
