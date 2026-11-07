package scheduler

import (
	"context"

	dkport "github.com/HyperSpectrum-Ltd/go-domainkit/port"
	"time"
)

// Task 定义调度任务接口
type Task interface {
	// Name 返回任务名称（唯一标识）
	Name() string
	// Description 返回任务描述
	Description() string
	// Schedule() 返回调度配置
	Schedule() ScheduleConfig
	// Execute 执行任务逻辑
	Execute(ctx context.Context) error
	// Timeout 返回任务执行超时时间
	Timeout() time.Duration
	// RetryPolicy 返回重试策略
	RetryPolicy() RetryPolicy
	// RequireLock 是否需要分布式锁
	RequireLock() bool
	// LockTTL 返回锁的 TTL
	LockTTL() time.Duration
}

// ScheduleType 调度类型
type ScheduleType string

const (
	ScheduleTypeCron     ScheduleType = "cron"
	ScheduleTypeInterval ScheduleType = "interval"
)

// ScheduleConfig 调度配置
type ScheduleConfig struct {
	Type       ScheduleType
	CronExpr   string
	Interval   time.Duration
	Enabled    bool
	RunOnStart bool
}

// RetryPolicy 重试策略
type RetryPolicy struct {
	MaxRetries      int
	InitialInterval time.Duration
	MaxInterval     time.Duration
	Multiplier      float64
}

// BackoffPolicy 把调度器的重试策略映射为 domainkit 统一退避策略。
// MaxRetries 是"首次之外的重试次数"，因此 MaxAttempts = MaxRetries + 1。
func (p RetryPolicy) BackoffPolicy() dkport.BackoffPolicy {
	return dkport.BackoffPolicy{
		MaxAttempts:  p.MaxRetries + 1,
		InitialDelay: p.InitialInterval,
		MaxDelay:     p.MaxInterval,
		Multiplier:   p.Multiplier,
	}
}

// DefaultRetryPolicy 返回默认重试策略
func DefaultRetryPolicy() RetryPolicy {
	return RetryPolicy{
		MaxRetries:      3,
		InitialInterval: 1 * time.Second,
		MaxInterval:     30 * time.Second,
		Multiplier:      2.0,
	}
}

// BaseTask 提供 Task 接口的基础实现
// 嵌入 BaseTask 的自定义任务只需实现 Execute 方法
type BaseTask struct {
	name        string
	description string
	schedule    ScheduleConfig
	timeout     time.Duration
	retryPolicy RetryPolicy
	requireLock bool
	lockTTL     time.Duration
}

// NewBaseTask 创建基础任务
func NewBaseTask(name, description string, schedule ScheduleConfig) *BaseTask {
	return &BaseTask{
		name:        name,
		description: description,
		schedule:    schedule,
		timeout:     5 * time.Minute,
		retryPolicy: DefaultRetryPolicy(),
		requireLock: true,
		lockTTL:     10 * time.Minute,
	}
}

func (t *BaseTask) Name() string               { return t.name }
func (t *BaseTask) Description() string        { return t.description }
func (t *BaseTask) Schedule() ScheduleConfig   { return t.schedule }
func (t *BaseTask) Timeout() time.Duration     { return t.timeout }
func (t *BaseTask) RetryPolicy() RetryPolicy   { return t.retryPolicy }
func (t *BaseTask) RequireLock() bool          { return t.requireLock }
func (t *BaseTask) LockTTL() time.Duration     { return t.lockTTL }
func (t *BaseTask) SetTimeout(d time.Duration) { t.timeout = d }
func (t *BaseTask) SetRequireLock(v bool)      { t.requireLock = v }
