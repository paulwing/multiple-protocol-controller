package app

import (
	"testing"

	"multiple-protocol-controller/internal/config"
)

func TestJudgeDeviceLimitEnvOverridesStayInsideJudgeConfiguration(t *testing.T) {
	for _, test := range []struct {
		name, count, bytes   string
		wantCount, wantBytes int
	}{
		{"valid", "12", "8192", 12, 8192},
		{"invalid preserves config", "invalid", "invalid", 4, 4096},
		{"unset preserves config", "", "", 4, 4096},
	} {
		t.Run(test.name, func(t *testing.T) {
			t.Setenv("JUDGE_SOURCE_DEVICE_QUEUE_SIZE", test.count)
			t.Setenv("JUDGE_SOURCE_DEVICE_QUEUE_MAX_BYTES", test.bytes)
			cfg := &config.Config{
				Redis:       config.RedisCfg{Address: "snapshot:6379", DB: 3, Timeout: "5s"},
				Influx:      config.InfluxCfg{Enabled: true, QueueSize: 321, RetryCount: 2},
				JudgeSource: config.JudgeSourceCfg{DeviceQueueSize: 4, DeviceQueueMaxBytes: 4096},
			}
			originalRedis, originalInflux := cfg.Redis, cfg.Influx
			applyJudgeSourceEnvOverrides(cfg)
			if cfg.JudgeSource.DeviceQueueSize != test.wantCount || cfg.JudgeSource.DeviceQueueMaxBytes != test.wantBytes {
				t.Fatalf("device budgets = %d/%d, want %d/%d", cfg.JudgeSource.DeviceQueueSize, cfg.JudgeSource.DeviceQueueMaxBytes, test.wantCount, test.wantBytes)
			}
			if cfg.Redis != originalRedis || cfg.Influx != originalInflux {
				t.Fatal("Judge environment overrides changed snapshot or history configuration")
			}
		})
	}
}
