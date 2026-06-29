package scheduler

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestDefaultRetryPolicy(t *testing.T) {
	rp := DefaultRetryPolicy()

	assert.Equal(t, 3, rp.MaxRetries)
	assert.Equal(t, 1*time.Second, rp.InitialInterval)
	assert.Equal(t, 30*time.Second, rp.MaxInterval)
	assert.Equal(t, 2.0, rp.Multiplier)
}

func TestNewBaseTask(t *testing.T) {
	schedule := ScheduleConfig{
		Type:       ScheduleTypeCron,
		CronExpr:   "*/5 * * * *",
		Enabled:    true,
		RunOnStart:  false,
	}
	bt := NewBaseTask("test-task", "a test task", schedule)

	assert.Equal(t, "test-task", bt.Name())
	assert.Equal(t, "a test task", bt.Description())
	assert.Equal(t, schedule, bt.Schedule())
	assert.Equal(t, 5*time.Minute, bt.Timeout(), "default timeout should be 5 minutes")
	assert.Equal(t, DefaultRetryPolicy(), bt.RetryPolicy())
	assert.True(t, bt.RequireLock(), "default requireLock should be true")
	assert.Equal(t, 10*time.Minute, bt.LockTTL(), "default lockTTL should be 10 minutes")
}

func TestBaseTaskSetTimeout(t *testing.T) {
	bt := NewBaseTask("t", "d", ScheduleConfig{Enabled: true})
	assert.Equal(t, 5*time.Minute, bt.Timeout())

	bt.SetTimeout(30 * time.Second)
	assert.Equal(t, 30*time.Second, bt.Timeout())
}

func TestBaseTaskSetRequireLock(t *testing.T) {
	bt := NewBaseTask("t", "d", ScheduleConfig{Enabled: true})
	assert.True(t, bt.RequireLock())

	bt.SetRequireLock(false)
	assert.False(t, bt.RequireLock())

	bt.SetRequireLock(true)
	assert.True(t, bt.RequireLock())
}

// customTask 嵌入 BaseTask 并只实现 Execute 方法
type customTask struct {
	*BaseTask
}

func (c *customTask) Execute(ctx context.Context) error {
	return nil
}

func TestEmbeddedBaseTaskSatisfiesTaskInterface(t *testing.T) {
	bt := NewBaseTask("custom", "custom task", ScheduleConfig{Enabled: true})
	ct := &customTask{BaseTask: bt}

	var _ Task = ct

	assert.Equal(t, "custom", ct.Name())
	assert.Equal(t, "custom task", ct.Description())
	assert.True(t, ct.Schedule().Enabled)
	assert.NoError(t, ct.Execute(context.Background()))
}

func TestScheduleConfigFields(t *testing.T) {
	sc := ScheduleConfig{
		Type:       ScheduleTypeInterval,
		CronExpr:   "*/5 * * * *",
		Interval:   10 * time.Second,
		Enabled:    true,
		RunOnStart: true,
	}

	assert.Equal(t, ScheduleTypeInterval, sc.Type)
	assert.Equal(t, "*/5 * * * *", sc.CronExpr)
	assert.Equal(t, 10*time.Second, sc.Interval)
	assert.True(t, sc.Enabled)
	assert.True(t, sc.RunOnStart)
}

func TestScheduleTypeConstants(t *testing.T) {
	assert.Equal(t, ScheduleType("cron"), ScheduleTypeCron)
	assert.Equal(t, ScheduleType("interval"), ScheduleTypeInterval)
	assert.NotEqual(t, ScheduleTypeCron, ScheduleTypeInterval)
}

func TestRetryPolicyFields(t *testing.T) {
	rp := RetryPolicy{
		MaxRetries:      5,
		InitialInterval: 500 * time.Millisecond,
		MaxInterval:     60 * time.Second,
		Multiplier:      3.0,
	}

	assert.Equal(t, 5, rp.MaxRetries)
	assert.Equal(t, 500*time.Millisecond, rp.InitialInterval)
	assert.Equal(t, 60*time.Second, rp.MaxInterval)
	assert.Equal(t, 3.0, rp.Multiplier)
}

func TestBaseTaskRetryPolicyCustomization(t *testing.T) {
	bt := NewBaseTask("t", "d", ScheduleConfig{Enabled: true})
	assert.Equal(t, DefaultRetryPolicy(), bt.RetryPolicy())

	bt.retryPolicy = RetryPolicy{
		MaxRetries:      0,
		InitialInterval: 100 * time.Millisecond,
		MaxInterval:     5 * time.Second,
		Multiplier:      1.5,
	}
	assert.Equal(t, 0, bt.RetryPolicy().MaxRetries)
	assert.Equal(t, 100*time.Millisecond, bt.RetryPolicy().InitialInterval)
}

func TestBaseTaskLockTTLDefault(t *testing.T) {
	bt := NewBaseTask("t", "d", ScheduleConfig{Enabled: true})
	require.Equal(t, 10*time.Minute, bt.LockTTL())

	bt.lockTTL = 30 * time.Second
	assert.Equal(t, 30*time.Second, bt.LockTTL())
}
