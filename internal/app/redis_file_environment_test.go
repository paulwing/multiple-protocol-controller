package app

import (
	"os"
	"path/filepath"
	"testing"

	"multiple-protocol-controller/internal/config"
)

// Match the two existing configuration steps in Run without starting Redis,
// collectors, protocol connections or background goroutines.
func TestRedisEnvironmentPreservesLoadedTOMLWhenOverridesAreIncomplete(t *testing.T) {
	for _, test := range []struct {
		name            string
		env             map[string]string
		address, secret string
		db              int
	}{
		{name: "empty environment retains file", address: "file-redis:6388", secret: "file-fixture-secret", db: 5},
		{name: "host alone cannot replace file endpoint", env: map[string]string{"REDIS_HOST": "host-redis", "REDIS_PASSWORD": "unsupported-alias"}, address: "file-redis:6388", secret: "file-fixture-secret", db: 5},
		{name: "port alone cannot replace file endpoint", env: map[string]string{"REDIS_PORT": "6382"}, address: "file-redis:6388", secret: "file-fixture-secret", db: 5},
		{name: "complete endpoint and explicit zero database override file", env: map[string]string{"REDIS_HOST": "host-redis", "REDIS_PORT": "6382", "REDIS_PWD": "env-fixture-secret", "REDIS_DB": "0"}, address: "host-redis:6382", secret: "env-fixture-secret", db: 0},
		{name: "new endpoint does not erase file password or database", env: map[string]string{"REDIS_HOST": "host-redis", "REDIS_PORT": "6382", "REDIS_DB": "not-a-number"}, address: "host-redis:6382", secret: "file-fixture-secret", db: 5},
	} {
		t.Run(test.name, func(t *testing.T) {
			for _, name := range []string{"REDIS_ADDR", "REDIS_HOST", "REDIS_PORT", "REDIS_PASSWORD", "REDIS_PWD", "REDIS_DB"} {
				t.Setenv(name, test.env[name])
			}
			path := filepath.Join(t.TempDir(), "config.toml")
			if err := os.WriteFile(path, []byte("[redis]\naddress = 'file-redis:6388'\npwd = 'file-fixture-secret'\ndb = 5\ntimeout = '3s'\n"), 0600); err != nil {
				t.Fatal(err)
			}
			cfg, err := config.Load(path)
			if err != nil {
				t.Fatal(err)
			}
			applyRedisEnvOverrides(cfg)
			if cfg.Redis.Address != test.address || cfg.Redis.Pwd != test.secret || cfg.Redis.DB != test.db || cfg.Redis.Timeout != "3s" {
				t.Fatalf("Redis source precedence changed: address=%q DB=%d timeout=%q passwordMatches=%t", cfg.Redis.Address, cfg.Redis.DB, cfg.Redis.Timeout, cfg.Redis.Pwd == test.secret)
			}
		})
	}
}

func TestRequiredTOMLFailureIsNotReplacedByEnvironment(t *testing.T) {
	t.Setenv("REDIS_HOST", "env-redis")
	t.Setenv("REDIS_PORT", "6379")
	t.Setenv("REDIS_PWD", "env-fixture-secret")
	t.Setenv("REDIS_DB", "0")
	for _, test := range []struct{ name, content string }{
		{name: "missing file"},
		{name: "malformed file", content: "[redis\n"},
	} {
		t.Run(test.name, func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "config.toml")
			if test.content != "" {
				if err := os.WriteFile(path, []byte(test.content), 0600); err != nil {
					t.Fatal(err)
				}
			}
			if cfg, err := config.Load(path); err == nil || cfg != nil {
				t.Fatalf("required file error was hidden: configPresent=%t error=%v", cfg != nil, err)
			}
		})
	}
}
