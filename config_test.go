package scheduler

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestDefaultConfig(t *testing.T) {
	cfg := DefaultConfig()

	assert.True(t, cfg.Enabled, "DefaultConfig.Enabled should be true")
	assert.Equal(t, 10, cfg.WorkerPoolSize, "DefaultConfig.WorkerPoolSize should be 10")
	assert.Equal(t, 10*time.Minute, cfg.LockTTL, "DefaultConfig.LockTTL should be 10 minutes")
	assert.True(t, cfg.MetricsEnabled, "DefaultConfig.MetricsEnabled should be true")
}

func TestConfigFields(t *testing.T) {
	cfg := Config{
		Enabled:        false,
		WorkerPoolSize: 20,
		LockTTL:        30 * time.Second,
		MetricsEnabled: false,
	}

	assert.False(t, cfg.Enabled)
	assert.Equal(t, 20, cfg.WorkerPoolSize)
	assert.Equal(t, 30*time.Second, cfg.LockTTL)
	assert.False(t, cfg.MetricsEnabled)
}

func TestTaskConfigFields(t *testing.T) {
	tc := TaskConfig{
		Enabled:     true,
		Schedule:    "*/5 * * * *",
		Timeout:     2 * time.Minute,
		MaxRetries:  5,
		RequireLock: true,
	}

	assert.True(t, tc.Enabled)
	assert.Equal(t, "*/5 * * * *", tc.Schedule)
	assert.Equal(t, 2*time.Minute, tc.Timeout)
	assert.Equal(t, 5, tc.MaxRetries)
	assert.True(t, tc.RequireLock)
}
