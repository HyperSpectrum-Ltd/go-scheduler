package scheduler

import (
	"context"
	"errors"
	"net"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/assert"
	"go.uber.org/zap"
)

// newFailingRedisClient returns a redis.Client whose connections always fail,
// forcing every command through the error branch without touching the network.
func newFailingRedisClient(dialErr error) *redis.Client {
	return redis.NewClient(&redis.Options{
		Addr: "127.0.0.1:6379",
		Dialer: func(ctx context.Context, network, addr string) (net.Conn, error) {
			return nil, dialErr
		},
	})
}

func TestRedisLock_AcquireError(t *testing.T) {
	simErr := errors.New("simulated dial failure")
	lock := NewRedisLock(newFailingRedisClient(simErr), zap.NewNop(), "prefix:")

	lockValue, acquired, err := lock.Acquire(context.Background(), "task:test", 10*time.Second)
	assert.Error(t, err)
	assert.False(t, acquired)
	assert.Empty(t, lockValue)
}

func TestRedisLock_ReleaseError(t *testing.T) {
	simErr := errors.New("simulated dial failure")
	lock := NewRedisLock(newFailingRedisClient(simErr), zap.NewNop(), "prefix:")

	err := lock.Release(context.Background(), "task:test", "some-value")
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "failed to release lock")
}

func TestRedisLock_ExtendError(t *testing.T) {
	simErr := errors.New("simulated dial failure")
	lock := NewRedisLock(newFailingRedisClient(simErr), zap.NewNop(), "prefix:")

	err := lock.Extend(context.Background(), "task:test", "some-value", 10*time.Second)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "failed to extend lock")
}

func TestRedisLock_IsHeldError(t *testing.T) {
	simErr := errors.New("simulated dial failure")
	lock := NewRedisLock(newFailingRedisClient(simErr), zap.NewNop(), "prefix:")

	held, err := lock.IsHeld(context.Background(), "task:test")
	assert.Error(t, err)
	assert.False(t, held)
}
