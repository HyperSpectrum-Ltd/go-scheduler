package scheduler

import "time"

// Config 调度器配置
type Config struct {
	Enabled        bool          `yaml:"enabled" mapstructure:"enabled"`
	WorkerPoolSize int           `yaml:"worker_pool_size" mapstructure:"worker_pool_size"`
	LockTTL        time.Duration `yaml:"lock_ttl" mapstructure:"lock_ttl"`
	MetricsEnabled bool          `yaml:"metrics_enabled" mapstructure:"metrics_enabled"`
}

// TaskConfig 任务配置
type TaskConfig struct {
	Enabled     bool          `yaml:"enabled" mapstructure:"enabled"`
	Schedule    string        `yaml:"schedule" mapstructure:"schedule"`
	Timeout     time.Duration `yaml:"timeout" mapstructure:"timeout"`
	MaxRetries  int           `yaml:"max_retries" mapstructure:"max_retries"`
	RequireLock bool          `yaml:"require_lock" mapstructure:"require_lock"`
}

// DefaultConfig 返回默认配置
func DefaultConfig() Config {
	return Config{
		Enabled:        true,
		WorkerPoolSize: 10,
		LockTTL:        10 * time.Minute,
		MetricsEnabled: true,
	}
}
