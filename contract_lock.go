package scheduler

import (
	"context"
	"time"

	dkport "github.com/HyperSpectrum-Ltd/go-domainkit/port"
)

// ContractLock 把调度器的 DistributedLock（Acquire/Release/Extend）
// 适配为 domainkit 的 ExtendableLock（Lock/Unlock/Extend）。
//
// 调度器保留既有命名以避免破坏现有调用方；新代码应依赖 domainkit 契约。
type ContractLock struct {
	lock DistributedLock
}

var _ dkport.ExtendableLock = (*ContractLock)(nil)

// NewContractLock 包装调度器锁实现。
func NewContractLock(lock DistributedLock) *ContractLock {
	return &ContractLock{lock: lock}
}

func (c *ContractLock) Lock(ctx context.Context, key string, ttl time.Duration) (string, bool, error) {
	if c.lock == nil {
		return "", false, ErrLockAcquireFailed
	}
	return c.lock.Acquire(ctx, key, ttl)
}

func (c *ContractLock) Unlock(ctx context.Context, key string, lockValue string) error {
	if c.lock == nil {
		return nil
	}
	return c.lock.Release(ctx, key, lockValue)
}

func (c *ContractLock) Extend(ctx context.Context, key string, lockValue string, ttl time.Duration) error {
	if c.lock == nil {
		return ErrLockAcquireFailed
	}
	return c.lock.Extend(ctx, key, lockValue, ttl)
}
