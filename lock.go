package scheduler

import (
	"context"
	"fmt"
	"time"

	"github.com/google/uuid"
	"github.com/redis/go-redis/v9"
	"go.uber.org/zap"
)

// releaseLockScript 释放锁的 Lua 脚本：只有当锁的值等于传入值时才删除
var releaseLockScript = redis.NewScript(`
if redis.call("get", KEYS[1]) == ARGV[1] then
    return redis.call("del", KEYS[1])
else
    return 0
end
`)

// DistributedLock 分布式锁接口
type DistributedLock interface {
	Acquire(ctx context.Context, key string, ttl time.Duration) (string, bool, error)
	Release(ctx context.Context, key string, lockValue string) error
	Extend(ctx context.Context, key string, lockValue string, ttl time.Duration) error
	IsHeld(ctx context.Context, key string) (bool, error)
}

// RedisLock 基于 Redis 的分布式锁实现
type RedisLock struct {
	client *redis.Client
	prefix string
	logger *zap.Logger
}

// NewRedisLock 创建 Redis 分布式锁
// prefix 为锁 key 的前缀，默认为 "scheduler:lock:"
func NewRedisLock(client *redis.Client, logger *zap.Logger, prefix string) *RedisLock {
	if prefix == "" {
		prefix = "scheduler:lock:"
	}
	return &RedisLock{
		client: client,
		prefix: prefix,
		logger: logger,
	}
}

// Acquire 获取分布式锁
// 返回值：lockValue 为锁的唯一标识，用于安全释放锁；acquired 表示是否获取成功
func (l *RedisLock) Acquire(ctx context.Context, key string, ttl time.Duration) (string, bool, error) {
	fullKey := l.prefix + key
	lockValue := uuid.New().String()

	result, err := l.client.SetNX(ctx, fullKey, lockValue, ttl).Result()
	if err != nil {
		l.logger.Error("failed to acquire lock",
			zap.String("key", key),
			zap.Error(err),
		)
		return "", false, err
	}
	if result {
		l.logger.Debug("lock acquired",
			zap.String("key", key),
			zap.Duration("ttl", ttl),
		)
		return lockValue, true, nil
	}
	return "", false, nil
}

// Release 释放分布式锁
// 使用 Lua 脚本验证锁值后再删除，确保只有锁的持有者才能释放锁
func (l *RedisLock) Release(ctx context.Context, key string, lockValue string) error {
	fullKey := l.prefix + key

	result, err := releaseLockScript.Run(ctx, l.client, []string{fullKey}, lockValue).Int64()
	if err != nil {
		l.logger.Error("failed to release lock",
			zap.String("key", key),
			zap.Error(err),
		)
		return fmt.Errorf("failed to release lock: %w", err)
	}
	if result == 0 {
		l.logger.Warn("lock not held by this owner, cannot release",
			zap.String("key", key),
		)
		return fmt.Errorf("lock not held by this owner: %s", key)
	}
	l.logger.Debug("lock released", zap.String("key", key))
	return nil
}

// Extend 延长锁的过期时间
// lockValue 为获取锁时返回的唯一标识，用于验证锁的持有者身份
func (l *RedisLock) Extend(ctx context.Context, key string, lockValue string, ttl time.Duration) error {
	fullKey := l.prefix + key

	// 使用 Lua 脚本验证锁值后再延长
	script := `
	if redis.call("get", KEYS[1]) == ARGV[1] then
	    return redis.call("expire", KEYS[1], ARGV[2])
	else
	    return 0
	end
	`
	result, err := l.client.Eval(ctx, script, []string{fullKey}, lockValue, int(ttl.Seconds())).Int64()
	if err != nil {
		l.logger.Error("failed to extend lock",
			zap.String("key", key),
			zap.Error(err),
		)
		return fmt.Errorf("failed to extend lock: %w", err)
	}
	if result == 0 {
		return fmt.Errorf("lock not held by this owner: %s", key)
	}
	return nil
}

// IsHeld 检查锁是否被持有
func (l *RedisLock) IsHeld(ctx context.Context, key string) (bool, error) {
	fullKey := l.prefix + key
	exists, err := l.client.Exists(ctx, fullKey).Result()
	if err != nil {
		return false, err
	}
	return exists > 0, nil
}
