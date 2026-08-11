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

[judge_source]
enabled = true
stream = "judge:source:test"
write_timeout_ms = 120
retry_count = 1
retry_interval_ms = 20
max_event_bytes = 65536
`)
	if err := os.WriteFile(path, content, 0o600); err != nil {
		t.Fatalf("os.WriteFile() error = %v", err)
	}

	cfg, err := Load(path)
	if err != nil {
		t.Fatalf("Load() error = %v", err)
	}
	if !cfg.JudgeSource.Enabled {
		t.Fatal("JudgeSource.Enabled = false, want true")
	}
	if cfg.JudgeSource.Stream != "judge:source:test" {
		t.Fatalf("JudgeSource.Stream = %q, want %q", cfg.JudgeSource.Stream, "judge:source:test")
	}
	if cfg.JudgeSource.WriteTimeoutMS != 120 ||
		cfg.JudgeSource.RetryCount != 1 ||
		cfg.JudgeSource.RetryIntervalMS != 20 ||
		cfg.JudgeSource.MaxEventBytes != 65536 {
		t.Fatalf("JudgeSource = %#v", cfg.JudgeSource)
	}
}
