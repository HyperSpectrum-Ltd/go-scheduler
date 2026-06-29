package scheduler

import (
	"context"
	"errors"
	"fmt"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
)

func newTestExecutor() *TaskExecutor {
	return NewTaskExecutor(newMockLock(), NewEmptyMetrics(), zap.NewNop())
}

func newTestExecutorWithLock(lock DistributedLock) *TaskExecutor {
	return NewTaskExecutor(lock, NewEmptyMetrics(), zap.NewNop())
}

func fastRetryPolicy(maxRetries int) RetryPolicy {
	return RetryPolicy{
		MaxRetries:      maxRetries,
		InitialInterval: 1 * time.Millisecond,
		MaxInterval:     10 * time.Millisecond,
		Multiplier:      2.0,
	}
}

// --- 成功执行测试 ---

func TestExecuteSuccessNoLock(t *testing.T) {
	executor := newTestExecutor()

	task := newMockTask("test-success")
	task.requireLock = false
	task.execFn = func(ctx context.Context) error { return nil }

	err := executor.Execute(context.Background(), task)
	assert.NoError(t, err)
	assert.Equal(t, int64(1), task.getExecCount())
}

func TestExecuteSuccessWithLock(t *testing.T) {
	lock := newMockLock()
	executor := newTestExecutorWithLock(lock)

	task := newMockTask("test-lock-success")
	task.requireLock = true
	task.lockTTL = 5 * time.Second
	task.execFn = func(ctx context.Context) error { return nil }

	err := executor.Execute(context.Background(), task)
	assert.NoError(t, err)
	assert.Equal(t, int64(1), task.getExecCount())

	// 锁应该已被释放
	held, err := lock.IsHeld(context.Background(), "task:"+task.Name())
	assert.NoError(t, err)
	assert.False(t, held, "lock should be released after execution")
}

func TestExecuteRequireLockFalseNoLockCall(t *testing.T) {
	lock := newMockLock()
	executor := newTestExecutorWithLock(lock)

	task := newMockTask("test-no-lock")
	task.requireLock = false
	task.execFn = func(ctx context.Context) error { return nil }

	err := executor.Execute(context.Background(), task)
	assert.NoError(t, err)

	// 没有获取锁
	held, err := lock.IsHeld(context.Background(), "task:"+task.Name())
	assert.NoError(t, err)
	assert.False(t, held)
}

// --- 锁获取失败测试 ---

func TestExecuteLockAcquireError(t *testing.T) {
	lock := newMockLock()
	lock.acquireErr = errors.New("redis error")
	executor := newTestExecutorWithLock(lock)

	task := newMockTask("test-lock-error")
	task.requireLock = true
	task.execFn = func(ctx context.Context) error { return nil }

	err := executor.Execute(context.Background(), task)
	assert.Error(t, err)
	assert.Equal(t, int64(0), task.getExecCount(), "task should not execute when lock fails")
}

func TestExecuteLockAlreadyHeld(t *testing.T) {
	lock := newMockLock()
	executor := newTestExecutorWithLock(lock)

	// 先获取锁
	_, _, err := lock.Acquire(context.Background(), "task:test-already-held", 10*time.Second)
	require.NoError(t, err)

	task := newMockTask("test-already-held")
	task.requireLock = true
	task.execFn = func(ctx context.Context) error { return nil }

	err = executor.Execute(context.Background(), task)
	assert.Error(t, err)
	assert.ErrorIs(t, err, ErrLockAcquireFailed)
	assert.Equal(t, int64(0), task.getExecCount())
}

// --- 重试测试 ---

func TestExecuteFirstSuccessNoRetry(t *testing.T) {
	executor := newTestExecutor()

	var callCount int64
	task := newMockTask("test-first-success")
	task.requireLock = false
	task.retryPolicy = fastRetryPolicy(3)
	task.execFn = func(ctx context.Context) error {
		atomic.AddInt64(&callCount, 1)
		return nil
	}

	err := executor.Execute(context.Background(), task)
	assert.NoError(t, err)
	assert.Equal(t, int64(1), atomic.LoadInt64(&callCount))
	assert.Equal(t, int64(1), task.getExecCount())
}

func TestExecuteRetryUntilSuccess(t *testing.T) {
	executor := newTestExecutor()

	var callCount int64
	task := newMockTask("test-retry-success")
	task.requireLock = false
	task.retryPolicy = fastRetryPolicy(3)
	task.execFn = func(ctx context.Context) error {
		c := atomic.AddInt64(&callCount, 1)
		if c < 3 {
			return errors.New("transient error")
		}
		return nil
	}

	err := executor.Execute(context.Background(), task)
	assert.NoError(t, err)
	assert.Equal(t, int64(3), atomic.LoadInt64(&callCount))
}

func TestExecuteMaxRetriesReached(t *testing.T) {
	executor := newTestExecutor()

	taskErr := errors.New("persistent error")
	var callCount int64
	task := newMockTask("test-max-retries")
	task.requireLock = false
	task.retryPolicy = fastRetryPolicy(2) // 3 total attempts (0,1,2)
	task.execFn = func(ctx context.Context) error {
		atomic.AddInt64(&callCount, 1)
		return taskErr
	}

	err := executor.Execute(context.Background(), task)
	assert.Error(t, err)
	assert.Equal(t, taskErr, err)
	// attempt 0, 1, 2 = 3 calls
	assert.Equal(t, int64(3), atomic.LoadInt64(&callCount))
}

func TestExecuteNonRetryableError(t *testing.T) {
	executor := newTestExecutor()

	var callCount int64
	task := newMockTask("test-non-retryable")
	task.requireLock = false
	task.retryPolicy = fastRetryPolicy(3)
	task.execFn = func(ctx context.Context) error {
		atomic.AddInt64(&callCount, 1)
		return context.Canceled
	}

	err := executor.Execute(context.Background(), task)
	assert.Error(t, err)
	assert.ErrorIs(t, err, context.Canceled)
	assert.Equal(t, int64(1), atomic.LoadInt64(&callCount), "non-retryable error should not retry")
}

// --- context 取消测试 ---

func TestExecuteContextCanceled(t *testing.T) {
	executor := newTestExecutor()

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	task := newMockTask("test-ctx-cancel")
	task.requireLock = false
	task.retryPolicy = fastRetryPolicy(3)
	task.execFn = func(ctx context.Context) error {
		return ctx.Err()
	}

	err := executor.Execute(ctx, task)
	assert.Error(t, err)
	assert.ErrorIs(t, err, context.Canceled)
	assert.Equal(t, int64(1), task.getExecCount(), "should not retry on context.Canceled")
}

func TestExecuteContextDeadlineExceeded(t *testing.T) {
	executor := newTestExecutor()

	ctx, cancel := context.WithTimeout(context.Background(), 1*time.Millisecond)
	defer cancel()
	time.Sleep(5 * time.Millisecond)

	task := newMockTask("test-ctx-deadline")
	task.requireLock = false
	task.retryPolicy = fastRetryPolicy(3)
	task.execFn = func(ctx context.Context) error {
		return ctx.Err()
	}

	err := executor.Execute(ctx, task)
	assert.Error(t, err)
	assert.ErrorIs(t, err, context.DeadlineExceeded)
}

// --- panic 恢复测试 (Bug 5) ---

func TestExecutePanicRecovery(t *testing.T) {
	executor := newTestExecutor()

	task := newMockTask("test-panic")
	task.requireLock = false
	task.retryPolicy = fastRetryPolicy(0) // no retries
	task.execFn = func(ctx context.Context) error {
		panic("boom!")
	}

	err := executor.Execute(context.Background(), task)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "task panicked")
	assert.Contains(t, err.Error(), "boom!")
	assert.Equal(t, int64(1), task.getExecCount())
}

func TestExecutePanicRecoveryWithRetry(t *testing.T) {
	executor := newTestExecutor()

	var callCount int64
	task := newMockTask("test-panic-retry")
	task.requireLock = false
	task.retryPolicy = fastRetryPolicy(3)
	task.execFn = func(ctx context.Context) error {
		c := atomic.AddInt64(&callCount, 1)
		if c < 3 {
			panic("transient panic")
		}
		return nil
	}

	err := executor.Execute(context.Background(), task)
	assert.NoError(t, err)
	assert.Equal(t, int64(3), atomic.LoadInt64(&callCount))
}

func TestExecutePanicWithLockReleased(t *testing.T) {
	lock := newMockLock()
	executor := newTestExecutorWithLock(lock)

	task := newMockTask("test-panic-lock")
	task.requireLock = true
	task.lockTTL = 5 * time.Second
	task.retryPolicy = fastRetryPolicy(0)
	task.execFn = func(ctx context.Context) error {
		panic("crash")
	}

	err := executor.Execute(context.Background(), task)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "task panicked")

	// 锁应该已被释放（defer 在 panic 恢复后执行）
	held, err := lock.IsHeld(context.Background(), "task:"+task.Name())
	assert.NoError(t, err)
	assert.False(t, held, "lock should be released even after panic")
}

// --- calculateBackoff 测试 ---

func TestCalculateBackoff(t *testing.T) {
	executor := newTestExecutor()
	policy := RetryPolicy{
		InitialInterval: 1 * time.Second,
		MaxInterval:     30 * time.Second,
		Multiplier:      2.0,
	}

	tests := []struct {
		attempt  int
		expected time.Duration
	}{
		{1, 1 * time.Second},
		{2, 2 * time.Second},
		{3, 4 * time.Second},
		{4, 8 * time.Second},
		{5, 16 * time.Second},
	}

	for _, tc := range tests {
		t.Run("", func(t *testing.T) {
			result := executor.calculateBackoff(tc.attempt, policy)
			assert.Equal(t, tc.expected, result)
		})
	}
}

func TestCalculateBackoffMaxIntervalCap(t *testing.T) {
	executor := newTestExecutor()
	policy := RetryPolicy{
		InitialInterval: 1 * time.Second,
		MaxInterval:     5 * time.Second,
		Multiplier:      2.0,
	}

	// attempt 1: 1s, 2: 2s, 3: 4s, 4: 8s -> capped at 5s
	result := executor.calculateBackoff(4, policy)
	assert.Equal(t, 5*time.Second, result)
}

func TestCalculateBackoffAttempt1(t *testing.T) {
	executor := newTestExecutor()
	policy := RetryPolicy{
		InitialInterval: 500 * time.Millisecond,
		MaxInterval:     10 * time.Second,
		Multiplier:      2.0,
	}

	result := executor.calculateBackoff(1, policy)
	assert.Equal(t, 500*time.Millisecond, result)
}

// --- isRetryableError 测试 ---

func TestIsRetryableError(t *testing.T) {
	executor := newTestExecutor()

	tests := []struct {
		name     string
		err      error
		expected bool
	}{
		{"nil error", nil, false},
		{"generic error", errors.New("some error"), true},
		{"context.Canceled", context.Canceled, false},
		{"context.DeadlineExceeded", context.DeadlineExceeded, false},
		{"wrapped context.Canceled", fmt.Errorf("wrapped: %w", context.Canceled), false},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			result := executor.isRetryableError(tc.err)
			assert.Equal(t, tc.expected, result)
		})
	}
}

func TestIsRetryableErrorWrappedCanceled(t *testing.T) {
	executor := newTestExecutor()

	wrapped := context.Canceled
	result := executor.isRetryableError(wrapped)
	assert.False(t, result)
}

// --- 执行器集成测试 ---

func TestExecuteWithCustomTimeout(t *testing.T) {
	executor := newTestExecutor()

	task := newMockTask("test-timeout")
	task.requireLock = false
	task.timeout = 100 * time.Millisecond
	task.retryPolicy = fastRetryPolicy(0)
	task.execFn = func(ctx context.Context) error {
		select {
		case <-time.After(200 * time.Millisecond):
			return nil
		case <-ctx.Done():
			return ctx.Err()
		}
	}

	err := executor.Execute(context.Background(), task)
	assert.Error(t, err)
	assert.ErrorIs(t, err, context.DeadlineExceeded)
}

func TestExecuteMultipleTasksIndependent(t *testing.T) {
	executor := newTestExecutor()

	task1 := newMockTask("task-1")
	task1.requireLock = false
	task1.execFn = func(ctx context.Context) error { return nil }

	task2 := newMockTask("task-2")
	task2.requireLock = false
	task2.execFn = func(ctx context.Context) error { return nil }

	require.NoError(t, executor.Execute(context.Background(), task1))
	require.NoError(t, executor.Execute(context.Background(), task2))

	assert.Equal(t, int64(1), task1.getExecCount())
	assert.Equal(t, int64(1), task2.getExecCount())
}
