package config

import (
	"os"
	"path/filepath"
	"testing"
)

func TestLoadJudgeSourceConfig(t *testing.T) {
	path := filepath.Join(t.TempDir(), "config.toml")
	content := []byte(`
runmode = "test"

[redis]
address = "127.0.0.1:6379"
db = 4

[judge_source]
enabled = true
channel = "iot:judge:test"
write_timeout_ms = 120
worker_count = 4
queue_size = 2048
queue_max_bytes = 16777216
max_event_bytes = 65536
`)
	if err := os.WriteFile(path, content, 0o600); err != nil {
		t.Fatalf("os.WriteFile() error = %v", err)
	}

	cfg, err := Load(path)
	if err != nil {
		t.Fatalf("Load() error = %v", err)
	}
	if cfg.Redis.DB != 4 {
		t.Fatalf("Redis.DB = %d, want 4", cfg.Redis.DB)
	}
	if !cfg.JudgeSource.Enabled {
		t.Fatal("JudgeSource.Enabled = false, want true")
	}
	if cfg.JudgeSource.Channel != "iot:judge:test" {
		t.Fatalf("JudgeSource.Channel = %q, want %q", cfg.JudgeSource.Channel, "iot:judge:test")
	}
	if cfg.JudgeSource.WriteTimeoutMS != 120 ||
		cfg.JudgeSource.WorkerCount != 4 || cfg.JudgeSource.QueueSize != 2048 ||
		cfg.JudgeSource.QueueMaxBytes != 16777216 ||
		cfg.JudgeSource.MaxEventBytes != 65536 {
		t.Fatalf("JudgeSource = %#v", cfg.JudgeSource)
	}
}
