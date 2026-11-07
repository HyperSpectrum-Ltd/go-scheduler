package scheduler

import (
	"testing"
	"time"
)

func TestRetryPolicy_MapsToDomainkitBackoff(t *testing.T) {
	p := RetryPolicy{
		MaxRetries:      3,
		InitialInterval: 500 * time.Millisecond,
		MaxInterval:     2 * time.Second,
		Multiplier:      3,
	}
	bp := p.BackoffPolicy()

	if bp.MaxAttempts != 4 {
		t.Fatalf("MaxAttempts = %d, want 4 (MaxRetries+1)", bp.MaxAttempts)
	}
	if bp.InitialDelay != p.InitialInterval || bp.MaxDelay != p.MaxInterval || bp.Multiplier != p.Multiplier {
		t.Fatalf("mapping mismatch: %+v", bp)
	}

	// 调度器退避计算必须与内核等价：500ms, 1.5s, 2s(封顶)。
	want := []time.Duration{500 * time.Millisecond, 1500 * time.Millisecond, 2 * time.Second}
	for i, w := range want {
		if got := bp.Delay(i + 1); got != w {
			t.Fatalf("Delay(%d) = %v, want %v", i+1, got, w)
		}
	}
}
