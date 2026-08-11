package scheduler

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"go.uber.org/zap"
)

// TestExecuteLockReleaseError covers the deferred lock-release error branch:
// a failing Release must be logged but must not change the execution result.
func TestExecuteLockReleaseError(t *testing.T) {
	lock := newMockLock()
	lock.releaseErr = errors.New("redis release failed")
	executor := NewTaskExecutor(lock, NewEmptyMetrics(), zap.NewNop())

	task := newMockTask("release-error")
	task.requireLock = true
	task.lockTTL = 5 * time.Second
	task.execFn = func(ctx context.Context) error { return nil }

	err := executor.Execute(context.Background(), task)
	assert.NoError(t, err, "task result should not be affected by release failure")
	assert.Equal(t, int64(1), task.getExecCount())
}

// TestExecuteRetryContextCanceledDuringBackoff covers the ctx.Done branch of
// the retry backoff wait: cancelling the context mid-wait must abort Execute.
func TestExecuteRetryContextCanceledDuringBackoff(t *testing.T) {
	executor := newTestExecutor()

	var callCount int64
	task := newMockTask("ctx-during-backoff")
	task.requireLock = false
	task.retryPolicy = RetryPolicy{
		MaxRetries:      2,
		InitialInterval: 2 * time.Second,
		MaxInterval:     4 * time.Second,
		Multiplier:      2.0,
	}
	task.execFn = func(ctx context.Context) error {
		atomic.AddInt64(&callCount, 1)
		return errors.New("transient error")
	}

	ctx, cancel := context.WithCancel(context.Background())
	go func() {
		time.Sleep(100 * time.Millisecond)
		cancel()
	}()

	err := executor.Execute(ctx, task)
	assert.ErrorIs(t, err, context.Canceled)
	assert.Equal(t, int64(1), atomic.LoadInt64(&callCount), "only the first attempt should run")
}
