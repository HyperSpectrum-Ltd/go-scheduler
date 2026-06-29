package scheduler

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/google/uuid"
)

// --- mockLock: 测试用 DistributedLock 实现 ---

type mockLock struct {
	mu         sync.Mutex
	held       map[string]string // key -> lockValue
	acquireErr error
	releaseErr error
	extendErr  error
	isHeldErr  error
}

func newMockLock() *mockLock {
	return &mockLock{held: make(map[string]string)}
}

func (l *mockLock) Acquire(ctx context.Context, key string, ttl time.Duration) (string, bool, error) {
	if l.acquireErr != nil {
		return "", false, l.acquireErr
	}
	l.mu.Lock()
	defer l.mu.Unlock()
	if _, exists := l.held[key]; exists {
		return "", false, nil
	}
	lockValue := uuid.New().String()
	l.held[key] = lockValue
	return lockValue, true, nil
}

func (l *mockLock) Release(ctx context.Context, key string, lockValue string) error {
	if l.releaseErr != nil {
		return l.releaseErr
	}
	l.mu.Lock()
	defer l.mu.Unlock()
	if v, exists := l.held[key]; exists && v == lockValue {
		delete(l.held, key)
		return nil
	}
	return fmt.Errorf("lock not held by this owner: %s", key)
}

func (l *mockLock) Extend(ctx context.Context, key string, lockValue string, ttl time.Duration) error {
	if l.extendErr != nil {
		return l.extendErr
	}
	l.mu.Lock()
	defer l.mu.Unlock()
	if v, exists := l.held[key]; exists && v == lockValue {
		return nil
	}
	return fmt.Errorf("lock not held by this owner: %s", key)
}

func (l *mockLock) IsHeld(ctx context.Context, key string) (bool, error) {
	if l.isHeldErr != nil {
		return false, l.isHeldErr
	}
	l.mu.Lock()
	defer l.mu.Unlock()
	_, exists := l.held[key]
	return exists, nil
}

// --- mockTask: 测试用 Task 实现 ---

type mockTask struct {
	name        string
	description string
	schedule    ScheduleConfig
	timeout     time.Duration
	retryPolicy RetryPolicy
	requireLock bool
	lockTTL     time.Duration

	execFn    func(ctx context.Context) error
	execCount atomic.Int64
}

func newMockTask(name string) *mockTask {
	return &mockTask{
		name:        name,
		description: "mock task for testing: " + name,
		schedule: ScheduleConfig{
			Type:       ScheduleTypeInterval,
			Interval:   time.Hour,
			Enabled:    true,
			RunOnStart: false,
		},
		timeout:     5 * time.Second,
		retryPolicy: DefaultRetryPolicy(),
		requireLock: false,
		lockTTL:     10 * time.Second,
	}
}

func (t *mockTask) Name() string             { return t.name }
func (t *mockTask) Description() string       { return t.description }
func (t *mockTask) Schedule() ScheduleConfig  { return t.schedule }
func (t *mockTask) Timeout() time.Duration    { return t.timeout }
func (t *mockTask) RetryPolicy() RetryPolicy  { return t.retryPolicy }
func (t *mockTask) RequireLock() bool         { return t.requireLock }
func (t *mockTask) LockTTL() time.Duration    { return t.lockTTL }

func (t *mockTask) Execute(ctx context.Context) error {
	t.execCount.Add(1)
	if t.execFn != nil {
		return t.execFn(ctx)
	}
	return nil
}

func (t *mockTask) getExecCount() int64 {
	return t.execCount.Load()
}
