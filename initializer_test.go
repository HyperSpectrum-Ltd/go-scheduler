package scheduler

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
)

func newTestManager() *SchedulerManager {
	return NewSchedulerManager(DefaultConfig(), newMockLock(), NewEmptyMetrics(), zap.NewNop())
}

func newTestRegistry(m *SchedulerManager) *TaskRegistry {
	return NewTaskRegistry(m, zap.NewNop())
}

// --- RegisterTask 测试 ---

func TestRegistryRegisterTaskSuccess(t *testing.T) {
	m := newTestManager()
	r := newTestRegistry(m)

	task := newMockTask("test-register")
	task.schedule.Enabled = true

	err := r.RegisterTask(task)
	assert.NoError(t, err)

	status, err := m.GetTaskStatus("test-register")
	assert.NoError(t, err)
	assert.Equal(t, "test-register", status.Name)
}

func TestRegistryRegisterTaskDuplicate(t *testing.T) {
	m := newTestManager()
	r := newTestRegistry(m)

	task := newMockTask("test-dup")
	task.schedule.Enabled = true

	err := r.RegisterTask(task)
	require.NoError(t, err)

	err = r.RegisterTask(task)
	assert.Error(t, err)
	assert.ErrorIs(t, err, ErrTaskAlreadyExists)
}

func TestRegistryRegisterTaskDisabled(t *testing.T) {
	m := newTestManager()
	r := newTestRegistry(m)

	task := newMockTask("test-disabled")
	task.schedule.Enabled = false

	err := r.RegisterTask(task)
	assert.NoError(t, err, "disabled task should register without error")

	// 但不应出现在 taskStatus 中
	_, err = m.GetTaskStatus("test-disabled")
	assert.Error(t, err)
	assert.ErrorIs(t, err, ErrTaskNotFound)
}

// --- RegisterAll 测试 ---

func TestRegistryRegisterAllSuccess(t *testing.T) {
	m := newTestManager()
	r := newTestRegistry(m)

	tasks := []Task{
		newMockTask("batch-1"),
		newMockTask("batch-2"),
		newMockTask("batch-3"),
	}
	for _, tk := range tasks {
		tk.(*mockTask).schedule.Enabled = true
	}

	err := r.RegisterAll(tasks...)
	assert.NoError(t, err)

	all := m.GetAllTaskStatus()
	assert.Len(t, all, 3)
}

func TestRegistryRegisterAllMidFailure(t *testing.T) {
	m := newTestManager()
	r := newTestRegistry(m)

	// 先注册一个
	task1 := newMockTask("batch-fail-1")
	task1.schedule.Enabled = true
	require.NoError(t, r.RegisterTask(task1))

	// RegisterAll 中第二个会重复
	task2 := newMockTask("batch-fail-2")
	task2.schedule.Enabled = true
	task3 := newMockTask("batch-fail-1") // 重复
	task3.schedule.Enabled = true
	task4 := newMockTask("batch-fail-3")
	task4.schedule.Enabled = true

	err := r.RegisterAll(task2, task3, task4)
	assert.Error(t, err)
	assert.ErrorIs(t, err, ErrTaskAlreadyExists)

	// task2 已注册成功，task4 未注册
	all := m.GetAllTaskStatus()
	assert.Contains(t, all, "batch-fail-1")
	assert.Contains(t, all, "batch-fail-2")
	assert.NotContains(t, all, "batch-fail-3")
}

// --- InitializeScheduler 测试 ---

func TestInitializeSchedulerSuccess(t *testing.T) {
	m := newTestManager()

	err := InitializeScheduler(context.Background(), m, zap.NewNop())
	assert.NoError(t, err)
	assert.True(t, m.IsRunning())

	// 清理
	_ = m.Stop(context.Background())
}

func TestInitializeSchedulerDisabled(t *testing.T) {
	cfg := DefaultConfig()
	cfg.Enabled = false
	m := NewSchedulerManager(cfg, newMockLock(), NewEmptyMetrics(), zap.NewNop())

	err := InitializeScheduler(context.Background(), m, zap.NewNop())
	assert.NoError(t, err)
	assert.False(t, m.IsRunning(), "disabled scheduler should not be running")
}

func TestInitializeSchedulerWithRunOnStart(t *testing.T) {
	cfg := DefaultConfig()
	m := NewSchedulerManager(cfg, newMockLock(), NewEmptyMetrics(), zap.NewNop())

	task := newMockTask("init-runonstart")
	task.schedule.Enabled = true
	task.schedule.RunOnStart = true
	task.schedule.Interval = time.Hour
	task.requireLock = false
	task.execFn = func(ctx context.Context) error { return nil }

	require.NoError(t, m.RegisterTask(task))

	err := InitializeScheduler(context.Background(), m, zap.NewNop())
	assert.NoError(t, err)

	// 等待 RunOnStart goroutine 执行
	time.Sleep(100 * time.Millisecond)

	status, err := m.GetTaskStatus("init-runonstart")
	require.NoError(t, err)
	assert.Equal(t, int64(1), status.SuccessCount)

	// 清理
	stopCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	_ = m.Stop(stopCtx)
}
