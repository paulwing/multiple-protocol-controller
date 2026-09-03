package app

import (
	"testing"

	"multiple-protocol-controller/internal/config"
)

func TestApplyRedisEnvOverrides(t *testing.T) {
	t.Setenv("REDIS_HOST", "redis.internal")
	t.Setenv("REDIS_PORT", "6380")
	t.Setenv("REDIS_PWD", "secret")
	t.Setenv("REDIS_DB", "4")

	cfg := &config.Config{}
	applyRedisEnvOverrides(cfg)

	if cfg.Redis.Address != "redis.internal:6380" || cfg.Redis.Pwd != "secret" || cfg.Redis.DB != 4 {
		t.Fatalf("Redis = %#v", cfg.Redis)
	}
}

func TestApplyRedisEnvOverridesKeepsDBForInvalidValue(t *testing.T) {
	for _, value := range []string{"invalid", "-1"} {
		t.Run(value, func(t *testing.T) {
			t.Setenv("REDIS_DB", value)
			cfg := &config.Config{Redis: config.RedisCfg{DB: 3}}

			applyRedisEnvOverrides(cfg)

			if cfg.Redis.DB != 3 {
				t.Fatalf("Redis.DB = %d, want 3", cfg.Redis.DB)
			}
		})
	}
}

func TestApplyJudgeSourceEnvOverrides(t *testing.T) {
	t.Setenv("JUDGE_SOURCE_ENABLED", "true")
	t.Setenv("JUDGE_SOURCE_CHANNEL", "iot:judge:test")
	t.Setenv("JUDGE_SOURCE_WRITE_TIMEOUT_MS", "120")
	t.Setenv("JUDGE_SOURCE_WORKER_COUNT", "4")
	t.Setenv("JUDGE_SOURCE_QUEUE_SIZE", "2048")
	t.Setenv("JUDGE_SOURCE_QUEUE_MAX_BYTES", "16777216")
	t.Setenv("JUDGE_SOURCE_MAX_EVENT_BYTES", "32768")

	cfg := &config.Config{}
	applyJudgeSourceEnvOverrides(cfg)

	if !cfg.JudgeSource.Enabled || cfg.JudgeSource.Channel != "iot:judge:test" {
		t.Fatalf("JudgeSource = %#v", cfg.JudgeSource)
	}
	if cfg.JudgeSource.WriteTimeoutMS != 120 ||
		cfg.JudgeSource.WorkerCount != 4 || cfg.JudgeSource.QueueSize != 2048 ||
		cfg.JudgeSource.QueueMaxBytes != 16777216 ||
		cfg.JudgeSource.MaxEventBytes != 32768 {
		t.Fatalf("JudgeSource = %#v", cfg.JudgeSource)
	}
}

func TestApplyJudgeSourceEnvOverridesKeepsValuesForInvalidIntegers(t *testing.T) {
	t.Setenv("JUDGE_SOURCE_WRITE_TIMEOUT_MS", "invalid")
	t.Setenv("JUDGE_SOURCE_WORKER_COUNT", "invalid")
	t.Setenv("JUDGE_SOURCE_QUEUE_SIZE", "invalid")
	t.Setenv("JUDGE_SOURCE_QUEUE_MAX_BYTES", "invalid")
	t.Setenv("JUDGE_SOURCE_MAX_EVENT_BYTES", "invalid")

	cfg := &config.Config{JudgeSource: config.JudgeSourceCfg{
		WriteTimeoutMS: 100, WorkerCount: 4, QueueSize: 2048,
		QueueMaxBytes: 16777216, MaxEventBytes: 65536,
	}}
	applyJudgeSourceEnvOverrides(cfg)

	if cfg.JudgeSource.WriteTimeoutMS != 100 ||
		cfg.JudgeSource.WorkerCount != 4 || cfg.JudgeSource.QueueSize != 2048 ||
		cfg.JudgeSource.QueueMaxBytes != 16777216 ||
		cfg.JudgeSource.MaxEventBytes != 65536 {
		t.Fatalf("JudgeSource = %#v", cfg.JudgeSource)
	}
}
