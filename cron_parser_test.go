package scheduler

import (
	"testing"
	"time"
)

// TestCronParser_AcceptsFiveAndSixFields 钉住 5 段/6 段/Descriptor 三种写法都能解析。
//
// 背景：模块曾固定 cron.WithSeconds()，只接受 6 段；调用方被迫自行补 "0 " 前缀
// （vpn-backend internal/infra/scheduler/tasks/base.go）。改为 SecondOptional 后，
// 5 段表达式的秒位缺省为 0，且 6 段语义不变。
func TestCronParser_AcceptsFiveAndSixFields(t *testing.T) {
	p := cronParser()

	cases := []struct {
		name     string
		expr     string
		wantNext time.Time
		ok       bool
	}{
		{name: "5-field", expr: "*/5 * * * *", ok: true},
		{name: "6-field", expr: "30 */5 * * * *", ok: true},
		{name: "descriptor", expr: "@hourly", ok: true},
		{name: "invalid", expr: "not-a-cron", ok: false},
	}

	base := time.Date(2026, 1, 1, 10, 0, 0, 0, time.UTC)
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			sched, err := p.Parse(tc.expr)
			if !tc.ok {
				if err == nil {
					t.Fatalf("expected parse error for %q", tc.expr)
				}
				return
			}
			if err != nil {
				t.Fatalf("parse %q: %v", tc.expr, err)
			}
			next := sched.Next(base)
			if next.IsZero() {
				t.Fatalf("next is zero for %q", tc.expr)
			}
			_ = tc.wantNext
		})
	}
}

// TestCronParser_FiveFieldSecondsDefaultToZero 5 段的秒位应为 0（而非被解释为分钟）。
func TestCronParser_FiveFieldSecondsDefaultToZero(t *testing.T) {
	p := cronParser()
	sched, err := p.Parse("*/5 * * * *")
	if err != nil {
		t.Fatalf("parse: %v", err)
	}

	from := time.Date(2026, 1, 1, 10, 1, 0, 0, time.UTC)
	next := sched.Next(from)
	if next.Second() != 0 {
		t.Fatalf("5-field cron should fire at second 0, got %v", next)
	}
	if next.Minute() != 5 {
		t.Fatalf("5-field */5 should fire at minute 5, got %v", next)
	}
}

// TestCronParser_SixFieldPreservesSeconds 6 段的秒位必须严格生效（向后兼容）。
func TestCronParser_SixFieldPreservesSeconds(t *testing.T) {
	p := cronParser()
	sched, err := p.Parse("45 0 10 * * *")
	if err != nil {
		t.Fatalf("parse: %v", err)
	}

	from := time.Date(2026, 1, 1, 8, 0, 0, 0, time.UTC)
	next := sched.Next(from)
	if next.Hour() != 10 || next.Minute() != 0 || next.Second() != 45 {
		t.Fatalf("6-field cron mismatch: got %v", next)
	}
}
