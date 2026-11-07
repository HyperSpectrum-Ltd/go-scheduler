package scheduler

import (
	"context"
	"errors"
	"fmt"
	"time"

	"go.uber.org/zap"
)

// TaskExecutor 任务执行器
type TaskExecutor struct {
	lock    DistributedLock
	metrics MetricsRecorder
	logger  *zap.Logger
}

// NewTaskExecutor 创建任务执行器
func NewTaskExecutor(lock DistributedLock, metrics MetricsRecorder, logger *zap.Logger) *TaskExecutor {
	return &TaskExecutor{
		lock:    lock,
		metrics: metrics,
		logger:  logger,
	}
}

// Execute 执行任务（支持重试和分布式锁）
func (e *TaskExecutor) Execute(ctx context.Context, task Task) error {
	var lockKey string

	// 尝试获取分布式锁
	if task.RequireLock() {
		lockKey = "task:" + task.Name()
		lockValue, acquired, err := e.lock.Acquire(ctx, lockKey, task.LockTTL())
		if err != nil {
			e.logger.Error("failed to acquire lock",
				zap.String("task", task.Name()),
				zap.Error(err),
			)
			return err
		}
		if !acquired {
			e.logger.Warn("lock already held by another instance",
				zap.String("task", task.Name()),
			)
			return ErrLockAcquireFailed
		}
		e.metrics.RecordLockAcquisition(task.Name(), true)

		defer func() {
			if err := e.lock.Release(ctx, lockKey, lockValue); err != nil {
				e.logger.Error("failed to release lock",
					zap.String("task", task.Name()),
					zap.Error(err),
				)
			}
		}()
	}

	// 执行任务（支持重试）
	var lastErr error
	retryPolicy := task.RetryPolicy()

	for attempt := 0; attempt <= retryPolicy.MaxRetries; attempt++ {
		if attempt > 0 {
			waitTime := e.calculateBackoff(attempt, retryPolicy)
			e.logger.Info("retrying task",
				zap.String("task", task.Name()),
				zap.Int("attempt", attempt),
				zap.Duration("wait", waitTime),
			)

			select {
			case <-time.After(waitTime):
			case <-ctx.Done():
				return ctx.Err()
			}
		}

		// 创建带超时的上下文
		execCtx, cancel := context.WithTimeout(ctx, task.Timeout())

		// Bug 5: 添加 panic 恢复，使 panic 转为错误计入 FailedCount 和重试
		err := func() (e error) {
			defer func() {
				if r := recover(); r != nil {
					e = fmt.Errorf("task panicked: %v", r)
				}
			}()
			return task.Execute(execCtx)
		}()
		cancel() // 立即释放资源，避免在循环中泄漏

		if err == nil {
			return nil
		}

		lastErr = err
		e.logger.Warn("task execution attempt failed",
			zap.String("task", task.Name()),
			zap.Int("attempt", attempt),
			zap.Error(err),
		)

		// 检查是否为可重试错误
		if !e.isRetryableError(err) {
			e.logger.Error("non-retryable error encountered",
				zap.String("task", task.Name()),
				zap.Error(err),
			)
			return err
		}
	}

	return lastErr
}

// calculateBackoff 计算退避时间：委托 domainkit 统一策略，
// attempt 是 1-based 重试序号（调用方在 attempt > 0 时才调用）。
func (e *TaskExecutor) calculateBackoff(attempt int, policy RetryPolicy) time.Duration {
	return policy.BackoffPolicy().Delay(attempt)
}

// isRetryableError 判断错误是否可重试
// context.Canceled 和 context.DeadlineExceeded 不可恢复，不应重试
func (e *TaskExecutor) isRetryableError(err error) bool {
	if err == nil {
		return false
	}

	if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
		return false
	}

	return true
}
