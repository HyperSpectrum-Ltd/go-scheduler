package scheduler

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
)

func newSchedulerManagerForTest() *SchedulerManager {
	return NewSchedulerManager(DefaultConfig(), newMockLock(), NewEmptyMetrics(), zap.NewNop())
}

func stopManager(t *testing.T, m *SchedulerManager) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := m.Stop(ctx); err != nil {
		t.Logf("stop error: %v", err)
	}
}

// --- NewSchedulerManager 测试 ---

func TestNewSchedulerManager(t *testing.T) {
	m := NewSchedulerManager(DefaultConfig(), newMockLock(), NewEmptyMetrics(), zap.NewNop())
	assert.NotNil(t, m)
	assert.False(t, m.IsRunning())
}

func TestNewSchedulerManagerWorkerPoolSizeDefault(t *testing.T) {
	cfg := Config{Enabled: true, WorkerPoolSize: 0}
	m := NewSchedulerManager(cfg, newMockLock(), NewEmptyMetrics(), zap.NewNop())
	assert.Equal(t, 10, m.config.WorkerPoolSize, "WorkerPoolSize <= 0 should default to 10")
}

func TestNewSchedulerManagerNegativeWorkerPoolSize(t *testing.T) {
	cfg := Config{Enabled: true, WorkerPoolSize: -5}
	m := NewSchedulerManager(cfg, newMockLock(), NewEmptyMetrics(), zap.NewNop())
	assert.Equal(t, 10, m.config.WorkerPoolSize)
}

func TestNewSchedulerManagerCustomWorkerPoolSize(t *testing.T) {
	cfg := Config{Enabled: true, WorkerPoolSize: 50}
	m := NewSchedulerManager(cfg, newMockLock(), NewEmptyMetrics(), zap.NewNop())
	assert.Equal(t, 50, m.config.WorkerPoolSize)
}

// --- RegisterTask 测试 ---

func TestManagerRegisterTaskSuccess(t *testing.T) {
	m := newSchedulerManagerForTest()

	task := newMockTask("reg-success")
	task.schedule.Enabled = true

	err := m.RegisterTask(task)
	assert.NoError(t, err)

	status, err := m.GetTaskStatus("reg-success")
	assert.NoError(t, err)
	assert.Equal(t, "reg-success", status.Name)
}

func TestManagerRegisterTaskDuplicate(t *testing.T) {
	m := newSchedulerManagerForTest()

	task := newMockTask("reg-dup")
	task.schedule.Enabled = true

	err := m.RegisterTask(task)
	require.NoError(t, err)

	err = m.RegisterTask(task)
	assert.Error(t, err)
	assert.ErrorIs(t, err, ErrTaskAlreadyExists)
}

func TestManagerRegisterTaskDisabled(t *testing.T) {
	m := newSchedulerManagerForTest()

	task := newMockTask("reg-disabled")
	task.schedule.Enabled = false

	err := m.RegisterTask(task)
	assert.NoError(t, err)

	_, err = m.GetTaskStatus("reg-disabled")
	assert.Error(t, err)
	assert.ErrorIs(t, err, ErrTaskNotFound)
}

// --- Start 测试 ---

func TestStartSuccess(t *testing.T) {
	m := newSchedulerManagerForTest()
	defer stopManager(t, m)

	task := newMockTask("start-success")
	task.schedule.Enabled = true
	task.schedule.Interval = time.Hour
	task.requireLock = false
	require.NoError(t, m.RegisterTask(task))

	err := m.Start(context.Background())
	assert.NoError(t, err)
	assert.True(t, m.IsRunning())
}

func TestStartAlreadyRunning(t *testing.T) {
	m := newSchedulerManagerForTest()
	defer stopManager(t, m)

	require.NoError(t, m.Start(context.Background()))

	// 重复 Start 应该返回 nil（幂等）
	err := m.Start(context.Background())
	assert.NoError(t, err)
}

func TestStartDisabled(t *testing.T) {
	cfg := DefaultConfig()
	cfg.Enabled = false
	m := NewSchedulerManager(cfg, newMockLock(), NewEmptyMetrics(), zap.NewNop())
	defer stopManager(t, m)

	err := m.Start(context.Background())
	assert.NoError(t, err)
	assert.False(t, m.IsRunning())
}

func TestStartWithRunOnStart(t *testing.T) {
	m := newSchedulerManagerForTest()
	defer stopManager(t, m)

	task := newMockTask("start-runonstart")
	task.schedule.Enabled = true
	task.schedule.RunOnStart = true
	task.schedule.Interval = time.Hour
	task.requireLock = false
	task.execFn = func(ctx context.Context) error { return nil }
	require.NoError(t, m.RegisterTask(task))

	err := m.Start(context.Background())
	require.NoError(t, err)

	// 等待 RunOnStart 执行完成
	require.Eventually(t, func() bool {
		status, _ := m.GetTaskStatus("start-runonstart")
		return status != nil && status.SuccessCount == 1
	}, 2*time.Second, 10*time.Millisecond)
}

func TestStartCronSchedule(t *testing.T) {
	m := newSchedulerManagerForTest()
	defer stopManager(t, m)

	task := newMockTask("start-cron")
	task.schedule = ScheduleConfig{
		Type:     ScheduleTypeInterval,
		Interval: 50 * time.Millisecond,
		Enabled:  true,
	}
	task.requireLock = false
	task.execFn = func(ctx context.Context) error { return nil }
	require.NoError(t, m.RegisterTask(task))

	require.NoError(t, m.Start(context.Background()))

	require.Eventually(t, func() bool {
		status, _ := m.GetTaskStatus("start-cron")
		return status != nil && status.SuccessCount >= 2
	}, 3*time.Second, 20*time.Millisecond, "task should have executed at least twice")
}

// --- Stop 测试 ---

func TestStopNotRunning(t *testing.T) {
	m := newSchedulerManagerForTest()

	err := m.Stop(context.Background())
	assert.NoError(t, err, "stopping a non-running scheduler should be a no-op")
}

func TestStopRunning(t *testing.T) {
	m := newSchedulerManagerForTest()
	require.NoError(t, m.Start(context.Background()))

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	err := m.Stop(ctx)
	assert.NoError(t, err)
	assert.False(t, m.IsRunning())
}

func TestStopIdempotent(t *testing.T) {
	m := newSchedulerManagerForTest()
	defer stopManager(t, m)

	require.NoError(t, m.Start(context.Background()))

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	require.NoError(t, m.Stop(ctx))

	// 再次 Stop 应该安全返回
	err := m.Stop(ctx)
	assert.NoError(t, err)
}

func TestStopWaitsForGoroutines(t *testing.T) {
	m := newSchedulerManagerForTest()

	task := newMockTask("stop-goroutines")
	task.schedule.Enabled = true
	task.schedule.RunOnStart = true
	task.schedule.Interval = time.Hour
	task.requireLock = false

	executed := make(chan struct{})
	task.execFn = func(ctx context.Context) error {
		time.Sleep(50 * time.Millisecond)
		close(executed)
		return nil
	}
	require.NoError(t, m.RegisterTask(task))

	require.NoError(t, m.Start(context.Background()))

	// 轮询等待任务真正进入运行状态，避免 Stop 在 goroutine 启动前关闭 stopCh 导致提前返回
	require.Eventually(t, func() bool {
		status, _ := m.GetTaskStatus("stop-goroutines")
		return status != nil && status.IsRunning
	}, 2*time.Second, 5*time.Millisecond)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	err := m.Stop(ctx)
	assert.NoError(t, err, "Stop should wait for goroutines to complete (Bug 2 fix)")

	select {
	case <-executed:
		// 任务完成
	default:
		t.Fatal("task should have completed before Stop returned")
	}
}

func TestStopWithTimeout(t *testing.T) {
	m := newSchedulerManagerForTest()

	task := newMockTask("stop-timeout")
	task.schedule.Enabled = true
	task.schedule.RunOnStart = true
	task.schedule.Interval = time.Hour
	task.requireLock = false

	// 一个长时间运行的任务
	block := make(chan struct{})
	task.execFn = func(ctx context.Context) error {
		select {
		case <-block:
			return nil
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	require.NoError(t, m.RegisterTask(task))

	require.NoError(t, m.Start(context.Background()))

	// 轮询等待任务进入运行状态，确保 Stop 时 goroutine 正阻塞在 execFn 中
	require.Eventually(t, func() bool {
		status, _ := m.GetTaskStatus("stop-timeout")
		return status != nil && status.IsRunning
	}, 2*time.Second, 5*time.Millisecond)

	// 用非常短的超时 Stop，应该超时返回（goroutine 正阻塞在 block 上，wg.Wait 不会立即完成）
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Millisecond)
	err := m.Stop(ctx)
	cancel()
	assert.Error(t, err)
	assert.ErrorIs(t, err, context.DeadlineExceeded)

	// 释放任务让残留 goroutine 完成，避免 race 检测误报
	close(block)
	require.Eventually(t, func() bool {
		status, _ := m.GetTaskStatus("stop-timeout")
		return status != nil && status.SuccessCount == 1
	}, 2*time.Second, 5*time.Millisecond)
}

// --- TriggerTask 测试 ---

func TestTriggerTaskNotFound(t *testing.T) {
	m := newSchedulerManagerForTest()

	err := m.TriggerTask(context.Background(), "nonexistent")
	assert.Error(t, err)
	assert.ErrorIs(t, err, ErrTaskNotFound)
}

func TestTriggerTaskAsyncExecution(t *testing.T) {
	m := newSchedulerManagerForTest()
	defer stopManager(t, m)

	task := newMockTask("trigger-async")
	task.schedule.Enabled = true
	task.schedule.Interval = time.Hour
	task.requireLock = false
	task.execFn = func(ctx context.Context) error { return nil }
	require.NoError(t, m.RegisterTask(task))

	// 不需要 Start 也可以 Trigger
	err := m.TriggerTask(context.Background(), "trigger-async")
	assert.NoError(t, err)

	// 轮询等待异步执行完成
	require.Eventually(t, func() bool {
		status, _ := m.GetTaskStatus("trigger-async")
		return status != nil && status.SuccessCount == 1
	}, 2*time.Second, 5*time.Millisecond)

	status, err := m.GetTaskStatus("trigger-async")
	require.NoError(t, err)
	assert.Equal(t, int64(1), status.SuccessCount)
}

func TestTriggerTaskFailure(t *testing.T) {
	m := newSchedulerManagerForTest()
	defer stopManager(t, m)

	task := newMockTask("trigger-fail")
	task.schedule.Enabled = true
	task.schedule.Interval = time.Hour
	task.requireLock = false
	task.retryPolicy = fastRetryPolicy(0)
	task.execFn = func(ctx context.Context) error {
		return errors.New("execution failed")
	}
	require.NoError(t, m.RegisterTask(task))

	err := m.TriggerTask(context.Background(), "trigger-fail")
	assert.NoError(t, err)

	// 轮询等待异步执行完成并记录失败
	require.Eventually(t, func() bool {
		status, _ := m.GetTaskStatus("trigger-fail")
		return status != nil && status.FailedCount == 1
	}, 2*time.Second, 5*time.Millisecond)

	status, err := m.GetTaskStatus("trigger-fail")
	require.NoError(t, err)
	assert.Equal(t, int64(1), status.FailedCount)
	assert.NotNil(t, status.LastErr)
}

// --- runTask 测试 ---

func TestRunTaskAlreadyRunningSkip(t *testing.T) {
	m := newSchedulerManagerForTest()
	defer stopManager(t, m)

	task := newMockTask("already-running")
	task.schedule.Enabled = true
	task.schedule.Interval = time.Hour
	task.requireLock = true
	task.lockTTL = 30 * time.Second

	block := make(chan struct{})
	task.execFn = func(ctx context.Context) error {
		<-block
		return nil
	}
	require.NoError(t, m.RegisterTask(task))

	// 触发第一次（开始执行，阻塞）
	require.NoError(t, m.TriggerTask(context.Background(), "already-running"))

	// 轮询等待任务进入运行状态，确保第二次触发时 IsRunning=true 从而被跳过
	require.Eventually(t, func() bool {
		status, _ := m.GetTaskStatus("already-running")
		return status != nil && status.IsRunning
	}, 2*time.Second, 5*time.Millisecond)

	// 触发第二次（应该跳过，因为已在运行且 RequireLock=true）
	require.NoError(t, m.TriggerTask(context.Background(), "already-running"))

	// 释放任务
	close(block)

	// 轮询等待任务完成，验证只执行了一次
	require.Eventually(t, func() bool {
		status, _ := m.GetTaskStatus("already-running")
		return status != nil && status.SuccessCount == 1
	}, 2*time.Second, 5*time.Millisecond)

	status, err := m.GetTaskStatus("already-running")
	require.NoError(t, err)
	assert.Equal(t, int64(1), task.getExecCount(), "second trigger should have been skipped")
	assert.Equal(t, int64(1), status.SuccessCount)
}

// --- 并发 race 验证 (Bug 1 修复) ---

func TestConcurrentTriggerNoRace(t *testing.T) {
	m := newSchedulerManagerForTest()

	const N = 50
	var execDone sync.WaitGroup
	execDone.Add(N)

	task := newMockTask("concurrent-norace")
	task.schedule.Enabled = true
	task.schedule.Interval = time.Hour
	task.requireLock = false // RequireLock=false 使得并发触发都能执行
	task.execFn = func(ctx context.Context) error {
		execDone.Done()
		return nil
	}
	require.NoError(t, m.RegisterTask(task))

	var wg sync.WaitGroup
	for i := 0; i < N; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			_ = m.TriggerTask(context.Background(), "concurrent-norace")
		}()
	}
	wg.Wait()

	// 等待所有任务执行完成
	execDone.Wait()

	// 轮询等待状态更新完成（runTask 在 Execute 后更新 SuccessCount）
	require.Eventually(t, func() bool {
		status, _ := m.GetTaskStatus("concurrent-norace")
		return status != nil && status.SuccessCount == int64(N)
	}, 3*time.Second, 10*time.Millisecond, "all concurrent triggers should be counted (no lost increments)")

	// 清理：Stop 会等待所有 goroutine 完成（Bug 2 修复）
	stopCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	_ = m.Stop(stopCtx)
}

func TestConcurrentTriggerWithLockNoRace(t *testing.T) {
	m := newSchedulerManagerForTest()
	defer stopManager(t, m)

	task := newMockTask("concurrent-lock")
	task.schedule.Enabled = true
	task.schedule.Interval = time.Hour
	task.requireLock = true
	task.lockTTL = 30 * time.Second
	task.execFn = func(ctx context.Context) error {
		time.Sleep(1 * time.Millisecond)
		return nil
	}
	require.NoError(t, m.RegisterTask(task))

	const N = 20
	var wg sync.WaitGroup
	for i := 0; i < N; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			_ = m.TriggerTask(context.Background(), "concurrent-lock")
		}()
	}
	wg.Wait()

	// 由于 RequireLock=true，并发触发时已在运行的任务会被跳过
	// 轮询等待至少一个任务完成执行
	require.Eventually(t, func() bool {
		status, _ := m.GetTaskStatus("concurrent-lock")
		return status != nil && status.SuccessCount >= 1
	}, 3*time.Second, 10*time.Millisecond)

	status, err := m.GetTaskStatus("concurrent-lock")
	require.NoError(t, err)
	assert.GreaterOrEqual(t, status.SuccessCount, int64(1))
}

// TestConcurrentGetStatusNoRace 验证并发读取状态不会 race
func TestConcurrentGetStatusNoRace(t *testing.T) {
	m := newSchedulerManagerForTest()
	defer stopManager(t, m)

	task := newMockTask("concurrent-getstatus")
	task.schedule.Enabled = true
	task.schedule.Interval = 50 * time.Millisecond
	task.requireLock = false
	task.execFn = func(ctx context.Context) error { return nil }
	require.NoError(t, m.RegisterTask(task))
	require.NoError(t, m.Start(context.Background()))

	var wg sync.WaitGroup
	// 并发读取状态
	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < 10; j++ {
				_, _ = m.GetTaskStatus("concurrent-getstatus")
				_ = m.GetAllTaskStatus()
			}
		}()
	}
	// 同时并发触发
	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			_ = m.TriggerTask(context.Background(), "concurrent-getstatus")
		}()
	}
	wg.Wait()

	// 轮询等待至少一次执行完成，确保并发读/触发/调度路径都被覆盖后再由 defer stopManager 清理
	require.Eventually(t, func() bool {
		status, _ := m.GetTaskStatus("concurrent-getstatus")
		return status != nil && status.SuccessCount >= 1
	}, 2*time.Second, 5*time.Millisecond)
}

// --- GetTaskStatus / GetAllTaskStatus / IsRunning 测试 ---

func TestGetTaskStatusNotFound(t *testing.T) {
	m := newSchedulerManagerForTest()

	_, err := m.GetTaskStatus("nonexistent")
	assert.Error(t, err)
	assert.ErrorIs(t, err, ErrTaskNotFound)
}

func TestGetTaskStatusReturnsSnapshot(t *testing.T) {
	m := newSchedulerManagerForTest()
	defer stopManager(t, m)

	task := newMockTask("snapshot-test")
	task.schedule.Enabled = true
	task.schedule.Interval = time.Hour
	task.requireLock = false
	task.execFn = func(ctx context.Context) error { return nil }
	require.NoError(t, m.RegisterTask(task))

	require.NoError(t, m.TriggerTask(context.Background(), "snapshot-test"))

	// 轮询等待第一次执行完成
	require.Eventually(t, func() bool {
		status, _ := m.GetTaskStatus("snapshot-test")
		return status != nil && status.SuccessCount == 1
	}, 2*time.Second, 5*time.Millisecond)

	s1, err := m.GetTaskStatus("snapshot-test")
	require.NoError(t, err)
	assert.Equal(t, int64(1), s1.SuccessCount)

	// 再次触发
	require.NoError(t, m.TriggerTask(context.Background(), "snapshot-test"))

	// 轮询等待第二次执行完成
	require.Eventually(t, func() bool {
		status, _ := m.GetTaskStatus("snapshot-test")
		return status != nil && status.SuccessCount == 2
	}, 2*time.Second, 5*time.Millisecond)

	s2, err := m.GetTaskStatus("snapshot-test")
	require.NoError(t, err)
	assert.Equal(t, int64(2), s2.SuccessCount)

	// s1 是快照，不应改变
	assert.Equal(t, int64(1), s1.SuccessCount, "returned snapshot should not change")
}

func TestGetAllTaskStatus(t *testing.T) {
	m := newSchedulerManagerForTest()

	for _, name := range []string{"task-a", "task-b", "task-c"} {
		task := newMockTask(name)
		task.schedule.Enabled = true
		require.NoError(t, m.RegisterTask(task))
	}

	all := m.GetAllTaskStatus()
	assert.Len(t, all, 3)
	assert.Contains(t, all, "task-a")
	assert.Contains(t, all, "task-b")
	assert.Contains(t, all, "task-c")
}

func TestGetAllTaskStatusEmpty(t *testing.T) {
	m := newSchedulerManagerForTest()
	all := m.GetAllTaskStatus()
	assert.Empty(t, all)
}

func TestIsRunning(t *testing.T) {
	m := newSchedulerManagerForTest()
	defer stopManager(t, m)

	assert.False(t, m.IsRunning())

	require.NoError(t, m.Start(context.Background()))
	assert.True(t, m.IsRunning())

	stopCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	require.NoError(t, m.Stop(stopCtx))
	assert.False(t, m.IsRunning())
}

// --- 综合场景测试 ---

func TestFullLifecycle(t *testing.T) {
	m := newSchedulerManagerForTest()

	var execCount int64
	task := newMockTask("lifecycle")
	task.schedule.Enabled = true
	task.schedule.Interval = 50 * time.Millisecond
	task.requireLock = false
	task.execFn = func(ctx context.Context) error {
		atomic.AddInt64(&execCount, 1)
		return nil
	}
	require.NoError(t, m.RegisterTask(task))

	require.NoError(t, m.Start(context.Background()))

	// 等待至少 2 次执行
	require.Eventually(t, func() bool {
		return atomic.LoadInt64(&execCount) >= 2
	}, 3*time.Second, 20*time.Millisecond)

	stopCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	require.NoError(t, m.Stop(stopCtx))

	assert.False(t, m.IsRunning())
	assert.GreaterOrEqual(t, atomic.LoadInt64(&execCount), int64(2))

	status, err := m.GetTaskStatus("lifecycle")
	require.NoError(t, err)
	assert.Equal(t, atomic.LoadInt64(&execCount), status.SuccessCount)
}

func TestMultipleTasksIndependent(t *testing.T) {
	m := newSchedulerManagerForTest()
	defer stopManager(t, m)

	task1 := newMockTask("multi-1")
	task1.schedule.Enabled = true
	task1.schedule.Interval = time.Hour
	task1.requireLock = false
	task1.execFn = func(ctx context.Context) error { return nil }

	task2 := newMockTask("multi-2")
	task2.schedule.Enabled = true
	task2.schedule.Interval = time.Hour
	task2.requireLock = false
	task2.execFn = func(ctx context.Context) error { return nil }

	require.NoError(t, m.RegisterTask(task1))
	require.NoError(t, m.RegisterTask(task2))

	require.NoError(t, m.TriggerTask(context.Background(), "multi-1"))
	require.NoError(t, m.TriggerTask(context.Background(), "multi-2"))

	// 轮询等待两个任务都执行完成
	require.Eventually(t, func() bool {
		s1, _ := m.GetTaskStatus("multi-1")
		s2, _ := m.GetTaskStatus("multi-2")
		return s1 != nil && s2 != nil && s1.SuccessCount == 1 && s2.SuccessCount == 1
	}, 2*time.Second, 5*time.Millisecond)

	s1, _ := m.GetTaskStatus("multi-1")
	s2, _ := m.GetTaskStatus("multi-2")
	assert.Equal(t, int64(1), s1.SuccessCount)
	assert.Equal(t, int64(1), s2.SuccessCount)
}
