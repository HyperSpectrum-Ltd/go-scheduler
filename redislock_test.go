package scheduler

import (
	"bufio"
	"context"
	"fmt"
	"io"
	"net"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
)

// fakeRedis is a minimal in-memory RESP2 server that implements just enough of
// the Redis protocol for the RedisLock success paths: HELLO, SET NX, GET,
// EXISTS and the two EVAL lock scripts.
type fakeRedis struct {
	mu    sync.Mutex
	store map[string]string
	ln    net.Listener
}

func newFakeRedis(t *testing.T) *fakeRedis {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)

	fr := &fakeRedis{
		store: make(map[string]string),
		ln:    ln,
	}
	go fr.serve()

	t.Cleanup(func() { _ = ln.Close() })
	return fr
}

func (fr *fakeRedis) addr() string { return fr.ln.Addr().String() }

func (fr *fakeRedis) newClient() *redis.Client {
	return redis.NewClient(&redis.Options{Addr: fr.addr(), Protocol: 2})
}

func (fr *fakeRedis) serve() {
	for {
		conn, err := fr.ln.Accept()
		if err != nil {
			return
		}
		go fr.handleConn(conn)
	}
}

func (fr *fakeRedis) handleConn(conn net.Conn) {
	defer conn.Close()
	reader := bufio.NewReader(conn)
	writer := bufio.NewWriter(conn)

	for {
		args, err := readCommand(reader)
		if err != nil {
			return
		}
		writeReply(writer, fr.dispatch(args))
		if err := writer.Flush(); err != nil {
			return
		}
	}
}

func readCommand(r *bufio.Reader) ([]string, error) {
	line, err := r.ReadString('\n')
	if err != nil {
		return nil, err
	}
	line = strings.TrimRight(line, "\r\n")
	if len(line) == 0 || line[0] != '*' {
		return nil, fmt.Errorf("expected array, got %q", line)
	}
	n, err := strconv.Atoi(line[1:])
	if err != nil {
		return nil, err
	}
	args := make([]string, 0, n)
	for i := 0; i < n; i++ {
		bl, err := r.ReadString('\n')
		if err != nil {
			return nil, err
		}
		bl = strings.TrimRight(bl, "\r\n")
		if len(bl) == 0 || bl[0] != '$' {
			return nil, fmt.Errorf("expected bulk string, got %q", bl)
		}
		size, err := strconv.Atoi(bl[1:])
		if err != nil {
			return nil, err
		}
		buf := make([]byte, size+2)
		if _, err := io.ReadFull(r, buf); err != nil {
			return nil, err
		}
		args = append(args, string(buf[:size]))
	}
	return args, nil
}

type redisError string

func (e redisError) Error() string { return string(e) }

func (fr *fakeRedis) dispatch(args []string) interface{} {
	if len(args) == 0 {
		return redisError("empty command")
	}
	switch strings.ToUpper(args[0]) {
	case "HELLO":
		return []interface{}{
			"server", "redis",
			"version", "7.0.0",
			"proto", "2",
			"role", "master",
		}
	case "SET":
		key, value := args[1], args[2]
		fr.mu.Lock()
		defer fr.mu.Unlock()
		if _, exists := fr.store[key]; exists {
			return nil
		}
		fr.store[key] = value
		return "OK"
	case "GET":
		fr.mu.Lock()
		v, exists := fr.store[args[1]]
		fr.mu.Unlock()
		if !exists {
			return nil
		}
		return v
	case "EXISTS":
		fr.mu.Lock()
		count := 0
		for _, k := range args[1:] {
			if _, ok := fr.store[k]; ok {
				count++
			}
		}
		fr.mu.Unlock()
		return count
	case "EVAL":
		return fr.eval(args[1:])
	case "EVALSHA":
		// The real server knows the script hash; we do not, so always reply
		// NOSCRIPT which makes go-redis fall back to EVAL.
		return redisError("NOSCRIPT No matching script. Please use EVAL.")
	default:
		return redisError("unknown command " + strings.ToUpper(args[0]))
	}
}

func (fr *fakeRedis) eval(args []string) interface{} {
	// args: script numkeys key... argv...
	if len(args) < 3 {
		return redisError("EVAL requires script and numkeys")
	}
	numKeys, err := strconv.Atoi(args[1])
	if err != nil || numKeys < 0 || len(args) < 2+numKeys {
		return redisError("bad numkeys")
	}
	keys := args[2 : 2+numKeys]
	argv := args[2+numKeys:]

	// The lock scripts only ever guard a single key by its lockValue.
	if numKeys != 1 || len(argv) < 1 {
		return redisError("unsupported script")
	}
	key, lockValue := keys[0], argv[0]

	fr.mu.Lock()
	defer fr.mu.Unlock()
	if fr.store[key] != lockValue {
		return 0
	}
	if len(argv) >= 2 {
		// Extend script: PEXPIRE -> 1
		return 1
	}
	// Release script: DEL -> 1
	delete(fr.store, key)
	return 1
}

func writeReply(w *bufio.Writer, reply interface{}) {
	switch v := reply.(type) {
	case string:
		fmt.Fprintf(w, "+%s\r\n", v)
	case int:
		fmt.Fprintf(w, ":%d\r\n", v)
	case nil:
		fmt.Fprint(w, "$-1\r\n")
	case []interface{}:
		fmt.Fprintf(w, "*%d\r\n", len(v))
		for _, item := range v {
			switch it := item.(type) {
			case string:
				fmt.Fprintf(w, "$%d\r\n%s\r\n", len(it), it)
			case int:
				fmt.Fprintf(w, ":%d\r\n", it)
			default:
				panic(fmt.Sprintf("unsupported element type %T", item))
			}
		}
	case error:
		fmt.Fprintf(w, "-%s\r\n", v.Error())
	default:
		panic(fmt.Sprintf("unsupported reply type %T", reply))
	}
}

func newRedisLockClient(fr *fakeRedis) *RedisLock {
	return NewRedisLock(fr.newClient(), zap.NewNop(), "prefix:")
}

func TestRedisLock_AcquireReleaseLifecycle(t *testing.T) {
	fr := newFakeRedis(t)
	lock := newRedisLockClient(fr)
	defer lock.client.Close()

	ctx := context.Background()

	lockValue, acquired, err := lock.Acquire(ctx, "task:lifecycle", 10*time.Second)
	require.NoError(t, err)
	assert.True(t, acquired)
	assert.NotEmpty(t, lockValue)

	held, err := lock.IsHeld(ctx, "task:lifecycle")
	require.NoError(t, err)
	assert.True(t, held)

	// A second acquire must fail while the key is held.
	_, acquired2, err := lock.Acquire(ctx, "task:lifecycle", 10*time.Second)
	require.NoError(t, err)
	assert.False(t, acquired2)

	// A wrong lock value must not release the lock.
	err = lock.Release(ctx, "task:lifecycle", "wrong-value")
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "lock not held by this owner")

	held, err = lock.IsHeld(ctx, "task:lifecycle")
	require.NoError(t, err)
	assert.True(t, held, "wrong-value release must not delete the lock")

	// The correct lock value releases the lock.
	err = lock.Release(ctx, "task:lifecycle", lockValue)
	require.NoError(t, err)

	held, err = lock.IsHeld(ctx, "task:lifecycle")
	require.NoError(t, err)
	assert.False(t, held)
}

func TestRedisLock_ExtendLifecycle(t *testing.T) {
	fr := newFakeRedis(t)
	lock := newRedisLockClient(fr)
	defer lock.client.Close()

	ctx := context.Background()

	lockValue, acquired, err := lock.Acquire(ctx, "task:extend", 10*time.Second)
	require.NoError(t, err)
	assert.True(t, acquired)

	err = lock.Extend(ctx, "task:extend", lockValue, 20*time.Second)
	require.NoError(t, err)

	err = lock.Extend(ctx, "task:extend", "wrong-value", 20*time.Second)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "lock not held by this owner")
}

func TestRedisLock_IsHeldFalseForMissingKey(t *testing.T) {
	fr := newFakeRedis(t)
	lock := newRedisLockClient(fr)
	defer lock.client.Close()

	held, err := lock.IsHeld(context.Background(), "task:missing")
	require.NoError(t, err)
	assert.False(t, held)
}
