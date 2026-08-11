package scheduler

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
)

// TestStartWithInvalidCronExpr covers Start's handling of a task whose cron
// expression fails to parse: the failure is logged and the scheduler continues.
func TestStartWithInvalidCronExpr(t *testing.T) {
	m := newSchedulerManagerForTest()
	defer stopManager(t, m)

	task := newMockTask("bad-cron")
	task.schedule = ScheduleConfig{
		Type:     ScheduleTypeCron,
		CronExpr: "not a valid cron",
		Enabled:  true,
	}
	task.requireLock = false
	require.NoError(t, m.RegisterTask(task))

	err := m.Start(context.Background())
	assert.NoError(t, err, "a single bad schedule must not prevent Start")
	assert.True(t, m.IsRunning())
}

// TestStartWithUnsupportedScheduleType covers scheduleTask's default branch.
func TestStartWithUnsupportedScheduleType(t *testing.T) {
	m := newSchedulerManagerForTest()
	defer stopManager(t, m)

	task := newMockTask("bad-type")
	task.schedule = ScheduleConfig{
		Type:    ScheduleType("bogus"),
		Enabled: true,
	}
	task.requireLock = false
	require.NoError(t, m.RegisterTask(task))

	err := m.Start(context.Background())
	assert.NoError(t, err, "an unsupported schedule type must not prevent Start")
	assert.True(t, m.IsRunning())
}

// TestScheduleTaskUnsupportedType calls scheduleTask directly to pin the exact
// error surfaced for an unknown schedule type.
func TestScheduleTaskUnsupportedType(t *testing.T) {
	m := newSchedulerManagerForTest()

	task := newMockTask("direct-bad-type")
	task.schedule = ScheduleConfig{Type: ScheduleType("bogus"), Enabled: true}

	err := m.scheduleTask(context.Background(), task)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "unsupported schedule type")
}

// TestRunTaskAfterStop covers the stop-channel guard inside runTask: a task
// triggered after the scheduler has stopped must exit without executing.
func TestRunTaskAfterStop(t *testing.T) {
	m := newSchedulerManagerForTest()

	task := newMockTask("post-stop")
	task.schedule.Enabled = true
	task.schedule.Interval = time.Hour
	task.requireLock = false
	task.execFn = func(ctx context.Context) error { return nil }
	require.NoError(t, m.RegisterTask(task))

	require.NoError(t, m.Start(context.Background()))
	require.NoError(t, m.Stop(context.Background()))

	require.NoError(t, m.TriggerTask(context.Background(), "post-stop"))
	time.Sleep(100 * time.Millisecond)
	assert.Equal(t, int64(0), task.getExecCount(), "task must not execute after stop")
}

// TestCronLoggerError covers cronLogger.Error used by the cron engine.
func TestCronLoggerError(t *testing.T) {
	logger := cronLogger{logger: zap.NewNop()}
	logger.Error(errors.New("cron engine failure"), "cron entry failed", "k", "v")
}
