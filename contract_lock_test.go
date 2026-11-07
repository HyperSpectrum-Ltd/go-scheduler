package scheduler

import (
	"context"
	"errors"
	"testing"
	"time"

	dkport "github.com/HyperSpectrum-Ltd/go-domainkit/port"
)

func TestContractLock_DelegatesToSchedulerLock(t *testing.T) {
	mock := newMockLock()
	lock := NewContractLock(mock)
	ctx := context.Background()

	value, acquired, err := lock.Lock(ctx, "job", time.Minute)
	if err != nil || !acquired {
		t.Fatalf("Lock: acquired=%v err=%v", acquired, err)
	}
	if err := lock.Extend(ctx, "job", value, 2*time.Minute); err != nil {
		t.Fatalf("Extend: %v", err)
	}
	if err := lock.Unlock(ctx, "job", value); err != nil {
		t.Fatalf("Unlock: %v", err)
	}
	if held, _ := mock.IsHeld(ctx, "job"); held {
		t.Fatal("underlying lock must be released")
	}
}

func TestContractLock_UsableWithDomainkitWithLock(t *testing.T) {
	lock := NewContractLock(newMockLock())
	ctx := context.Background()

	if err := dkport.WithLock(ctx, lock, "job", time.Minute, func() error { return nil }); err != nil {
		t.Fatalf("WithLock: %v", err)
	}

	// 第二次获取应成功，证明第一次已释放。
	_, acquired, err := lock.Lock(ctx, "job", time.Minute)
	if err != nil || !acquired {
		t.Fatalf("re-acquire: acquired=%v err=%v", acquired, err)
	}
}

func TestContractLock_Nil(t *testing.T) {
	lock := NewContractLock(nil)
	ctx := context.Background()

	if _, _, err := lock.Lock(ctx, "job", time.Minute); !errors.Is(err, ErrLockAcquireFailed) {
		t.Fatalf("Lock err = %v", err)
	}
	if err := lock.Extend(ctx, "job", "v", time.Minute); !errors.Is(err, ErrLockAcquireFailed) {
		t.Fatalf("Extend err = %v", err)
	}
	if err := lock.Unlock(ctx, "job", "v"); err != nil {
		t.Fatalf("Unlock: %v", err)
	}
}
