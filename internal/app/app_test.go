package app

import (
	"testing"

	"multiple-protocol-controller/internal/config"
)

func TestApplyJudgeSourceEnvOverrides(t *testing.T) {
	t.Setenv("JUDGE_SOURCE_ENABLED", "true")
	t.Setenv("JUDGE_SOURCE_STREAM", "judge:source:test")
	t.Setenv("JUDGE_SOURCE_WRITE_TIMEOUT_MS", "120")
	t.Setenv("JUDGE_SOURCE_RETRY_COUNT", "2")
	t.Setenv("JUDGE_SOURCE_RETRY_INTERVAL_MS", "25")
	t.Setenv("JUDGE_SOURCE_MAX_EVENT_BYTES", "32768")

	cfg := &config.Config{}
	applyJudgeSourceEnvOverrides(cfg)

	if !cfg.JudgeSource.Enabled || cfg.JudgeSource.Stream != "judge:source:test" {
		t.Fatalf("JudgeSource = %#v", cfg.JudgeSource)
	}
	if cfg.JudgeSource.WriteTimeoutMS != 120 ||
		cfg.JudgeSource.RetryCount != 2 ||
		cfg.JudgeSource.RetryIntervalMS != 25 ||
		cfg.JudgeSource.MaxEventBytes != 32768 {
		t.Fatalf("JudgeSource = %#v", cfg.JudgeSource)
	}
}

func TestApplyJudgeSourceEnvOverridesKeepsValuesForInvalidIntegers(t *testing.T) {
	t.Setenv("JUDGE_SOURCE_WRITE_TIMEOUT_MS", "invalid")
	t.Setenv("JUDGE_SOURCE_RETRY_COUNT", "invalid")
	t.Setenv("JUDGE_SOURCE_RETRY_INTERVAL_MS", "invalid")
	t.Setenv("JUDGE_SOURCE_MAX_EVENT_BYTES", "invalid")

	cfg := &config.Config{JudgeSource: config.JudgeSourceCfg{
		WriteTimeoutMS:  100,
		RetryCount:      1,
		RetryIntervalMS: 20,
		MaxEventBytes:   65536,
	}}
	applyJudgeSourceEnvOverrides(cfg)

	if cfg.JudgeSource.WriteTimeoutMS != 100 ||
		cfg.JudgeSource.RetryCount != 1 ||
		cfg.JudgeSource.RetryIntervalMS != 20 ||
		cfg.JudgeSource.MaxEventBytes != 65536 {
		t.Fatalf("JudgeSource = %#v", cfg.JudgeSource)
	}
}
