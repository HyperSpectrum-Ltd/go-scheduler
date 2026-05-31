package scheduler

import "errors"

var (
	ErrTaskNotFound       = errors.New("task not found")
	ErrTaskAlreadyExists  = errors.New("task already exists")
	ErrTaskAlreadyRunning = errors.New("task is already running")
	ErrLockAcquireFailed  = errors.New("failed to acquire lock")
	ErrTaskTimeout        = errors.New("task execution timeout")
	ErrSchedulerStopped   = errors.New("scheduler is stopped")
)
