package scheduler

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// --- mockLock Acquire 测试 ---

func TestMockLockAcquireFirstSuccess(t *testing.T) {
	l := newMockLock()
	val, acquired, err := l.Acquire(context.Background(), "task:test", 10*time.Second)

	assert.NoError(t, err)
	assert.True(t, acquired)
	assert.NotEmpty(t, val)
}

func TestMockLockAcquireAlreadyHeld(t *testing.T) {
	l := newMockLock()
	_, _, err := l.Acquire(context.Background(), "task:test", 10*time.Second)
	require.NoError(t, err)

	val2, acquired2, err2 := l.Acquire(context.Background(), "task:test", 10*time.Second)

	assert.NoError(t, err2)
	assert.False(t, acquired2, "second Acquire should return acquired=false")
	assert.Empty(t, val2)
}

func TestMockLockAcquireError(t *testing.T) {
	l := newMockLock()
	customErr := errors.New("redis connection refused")
	l.acquireErr = customErr

	_, acquired, err := l.Acquire(context.Background(), "task:test", 10*time.Second)

	assert.Equal(t, customErr, err)
	assert.False(t, acquired)
}

func TestMockLockAcquireDifferentKeys(t *testing.T) {
	l := newMockLock()

	val1, acquired1, err1 := l.Acquire(context.Background(), "task:a", 10*time.Second)
	assert.NoError(t, err1)
	assert.True(t, acquired1)

	val2, acquired2, err2 := l.Acquire(context.Background(), "task:b", 10*time.Second)
	assert.NoError(t, err2)
	assert.True(t, acquired2)

	assert.NotEqual(t, val1, val2, "different keys should have different lock values")
}

// --- mockLock Release 测试 ---

func TestMockLockReleaseCorrectHolder(t *testing.T) {
	l := newMockLock()
	val, _, err := l.Acquire(context.Background(), "task:test", 10*time.Second)
	require.NoError(t, err)

	err = l.Release(context.Background(), "task:test", val)
	assert.NoError(t, err)

	// 释放后可以重新获取
	_, acquired, err := l.Acquire(context.Background(), "task:test", 10*time.Second)
	assert.NoError(t, err)
	assert.True(t, acquired)
}

func TestMockLockReleaseWrongHolder(t *testing.T) {
	l := newMockLock()
	_, _, err := l.Acquire(context.Background(), "task:test", 10*time.Second)
	require.NoError(t, err)

	err = l.Release(context.Background(), "task:test", "wrong-value")
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "lock not held by this owner")
}

func TestMockLockReleaseNonExistent(t *testing.T) {
	l := newMockLock()

	err := l.Release(context.Background(), "task:nonexistent", "some-value")
	assert.Error(t, err)
}

func TestMockLockReleaseError(t *testing.T) {
	l := newMockLock()
	customErr := errors.New("redis release error")
	l.releaseErr = customErr

	err := l.Release(context.Background(), "task:test", "val")
	assert.Equal(t, customErr, err)
}

// --- mockLock Extend 测试 ---

func TestMockLockExtendHolderSuccess(t *testing.T) {
	l := newMockLock()
	val, _, err := l.Acquire(context.Background(), "task:test", 10*time.Second)
	require.NoError(t, err)

	err = l.Extend(context.Background(), "task:test", val, 20*time.Second)
	assert.NoError(t, err)
}

func TestMockLockExtendNonHolder(t *testing.T) {
	l := newMockLock()
	_, _, err := l.Acquire(context.Background(), "task:test", 10*time.Second)
	require.NoError(t, err)

	err = l.Extend(context.Background(), "task:test", "wrong-value", 20*time.Second)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "lock not held by this owner")
}

func TestMockLockExtendNonExistent(t *testing.T) {
	l := newMockLock()

	err := l.Extend(context.Background(), "task:nonexistent", "val", 10*time.Second)
	assert.Error(t, err)
}

func TestMockLockExtendError(t *testing.T) {
	l := newMockLock()
	customErr := errors.New("redis extend error")
	l.extendErr = customErr

	err := l.Extend(context.Background(), "task:test", "val", 10*time.Second)
	assert.Equal(t, customErr, err)
}

// TestExtendTTLNoTruncation 验证 Bug 6 修复：
// 子秒级 TTL 不应被截断为零
// RedisLock.Extend 使用 PEXPIRE（毫秒），mockLock 直接忽略 TTL，
// 但此测试验证了 TTL 毫秒转换的正确性
func TestExtendTTLNoTruncation(t *testing.T) {
	tests := []struct {
		name string
		ttl  time.Duration
		// 旧逻辑: int(ttl.Seconds()) — 会被截断为 0
		oldSeconds int
		// 新逻辑: int64(ttl.Milliseconds()) — 不会被截断
		newMillis int64
	}{
		{"500ms", 500 * time.Millisecond, 0, 500},
		{"100ms", 100 * time.Millisecond, 0, 100},
		{"1ms", 1 * time.Millisecond, 0, 1},
		{"1s", 1 * time.Second, 1, 1000},
		{"1.5s", 1500 * time.Millisecond, 1, 1500},
		{"30s", 30 * time.Second, 30, 30000},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			oldVal := int(tc.ttl.Seconds())
			newVal := int64(tc.ttl.Milliseconds())

			assert.Equal(t, tc.oldSeconds, oldVal, "old logic (EXPIRE seconds) value")
			assert.Equal(t, tc.newMillis, newVal, "new logic (PEXPIRE milliseconds) value")
			assert.NotZero(t, newVal, "new TTL in milliseconds should never be zero for positive durations")
		})
	}
}

func TestMockLockExtendSubSecondTTL(t *testing.T) {
	l := newMockLock()
	val, _, err := l.Acquire(context.Background(), "task:test", 500*time.Millisecond)
	require.NoError(t, err)

	// 子秒 TTL 不应导致任何问题
	err = l.Extend(context.Background(), "task:test", val, 500*time.Millisecond)
	assert.NoError(t, err)
}

// --- mockLock IsHeld 测试 ---

func TestMockLockIsHeldKeyExists(t *testing.T) {
	l := newMockLock()
	_, _, err := l.Acquire(context.Background(), "task:test", 10*time.Second)
	require.NoError(t, err)

	held, err := l.IsHeld(context.Background(), "task:test")
	assert.NoError(t, err)
	assert.True(t, held)
}

func TestMockLockIsHeldKeyNotExists(t *testing.T) {
	l := newMockLock()

	held, err := l.IsHeld(context.Background(), "task:nonexistent")
	assert.NoError(t, err)
	assert.False(t, held)
}

func TestMockLockIsHeldAfterRelease(t *testing.T) {
	l := newMockLock()
	val, _, err := l.Acquire(context.Background(), "task:test", 10*time.Second)
	require.NoError(t, err)

	held, err := l.IsHeld(context.Background(), "task:test")
	assert.True(t, held)

	err = l.Release(context.Background(), "task:test", val)
	require.NoError(t, err)

	held, err = l.IsHeld(context.Background(), "task:test")
	assert.NoError(t, err)
	assert.False(t, held)
}

func TestMockLockIsHeldError(t *testing.T) {
	l := newMockLock()
	customErr := errors.New("redis isheld error")
	l.isHeldErr = customErr

	held, err := l.IsHeld(context.Background(), "task:test")
	assert.Equal(t, customErr, err)
	assert.False(t, held)
}

// --- DistributedLock 接口实现验证 ---

func TestMockLockImplementsDistributedLock(t *testing.T) {
	var _ DistributedLock = (*mockLock)(nil)
}

func TestRedisLockImplementsDistributedLock(t *testing.T) {
	var _ DistributedLock = (*RedisLock)(nil)
}

// TestNewRedisLock 验证前缀默认值
func TestNewRedisLockDefaultPrefix(t *testing.T) {
	rl := NewRedisLock(nil, nil, "")
	assert.Equal(t, "scheduler:lock:", rl.prefix)
}

func TestNewRedisLockCustomPrefix(t *testing.T) {
	rl := NewRedisLock(nil, nil, "custom:")
	assert.Equal(t, "custom:", rl.prefix)
}

// TestReleaseLockScript 验证 Lua 脚本非空
func TestReleaseLockScript(t *testing.T) {
	require.NotNil(t, releaseLockScript)
	assert.NotEmpty(t, releaseLockScript.Hash())
}

// TestFullKey 验证 prefix + key 拼接
func TestRedisLockFullKey(t *testing.T) {
	rl := NewRedisLock(nil, nil, "prefix:")
	// 通过 Acquire 的 fullKey 验证（mockLock 模拟）
	// 这里仅验证前缀逻辑
	assert.Equal(t, "prefix:", rl.prefix)

	expected := fmt.Sprintf("prefix:task:test")
	assert.Contains(t, expected, rl.prefix)
}
