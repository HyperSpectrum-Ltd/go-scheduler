package scheduler

import (
	"errors"
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestSentinelErrors(t *testing.T) {
	errs := []struct {
		name string
		err  error
	}{
		{"ErrTaskNotFound", ErrTaskNotFound},
		{"ErrTaskAlreadyExists", ErrTaskAlreadyExists},
		{"ErrTaskAlreadyRunning", ErrTaskAlreadyRunning},
		{"ErrLockAcquireFailed", ErrLockAcquireFailed},
		{"ErrTaskTimeout", ErrTaskTimeout},
		{"ErrSchedulerStopped", ErrSchedulerStopped},
	}

	for _, tc := range errs {
		t.Run(tc.name, func(t *testing.T) {
			assert.Error(t, tc.err)
			assert.True(t, errors.Is(tc.err, tc.err), "errors.Is should match itself")
		})
	}
}

func TestSentinelErrorsDistinct(t *testing.T) {
	allErrs := []error{
		ErrTaskNotFound,
		ErrTaskAlreadyExists,
		ErrTaskAlreadyRunning,
		ErrLockAcquireFailed,
		ErrTaskTimeout,
		ErrSchedulerStopped,
	}

	for i, a := range allErrs {
		for j, b := range allErrs {
			if i == j {
				continue
			}
			assert.Falsef(t, errors.Is(a, b), "error %v should not match %v", a, b)
		}
	}
}

func TestWrappedErrorsIdentifiable(t *testing.T) {
	tests := []struct {
		name   string
		base   error
		wrapFn func(error) error
	}{
		{
			name:   "ErrTaskNotFound wrapped with fmt.Errorf",
			base:   ErrTaskNotFound,
			wrapFn: func(e error) error { return fmt.Errorf("%w: task-abc", e) },
		},
		{
			name:   "ErrTaskAlreadyExists wrapped with fmt.Errorf",
			base:   ErrTaskAlreadyExists,
			wrapFn: func(e error) error { return fmt.Errorf("%w: task-xyz", e) },
		},
		{
			name:   "ErrLockAcquireFailed wrapped with fmt.Errorf",
			base:   ErrLockAcquireFailed,
			wrapFn: func(e error) error { return fmt.Errorf("custom: %w", e) },
		},
		{
			name:   "ErrTaskTimeout wrapped with fmt.Errorf",
			base:   ErrTaskTimeout,
			wrapFn: func(e error) error { return fmt.Errorf("outer: %w", e) },
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			wrapped := tc.wrapFn(tc.base)
			assert.True(t, errors.Is(wrapped, tc.base), "wrapped error should be identifiable via errors.Is")
		})
	}
}
