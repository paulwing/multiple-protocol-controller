package config

import (
	"os"
	"path/filepath"
	"testing"
)

func TestLoadJudgeDeviceBudgetsFromTOML(t *testing.T) {
	path := filepath.Join(t.TempDir(), "config.toml")
	if err := os.WriteFile(path, []byte("[judge_source]\ndevice_queue_size = 12\ndevice_queue_max_bytes = 8192\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	cfg, err := Load(path)
	if err != nil {
		t.Fatal(err)
	}
	if cfg.JudgeSource.DeviceQueueSize != 12 || cfg.JudgeSource.DeviceQueueMaxBytes != 8192 {
		t.Fatalf("device budgets = %d/%d, want 12/8192", cfg.JudgeSource.DeviceQueueSize, cfg.JudgeSource.DeviceQueueMaxBytes)
	}
}
