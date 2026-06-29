package scheduler

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestEmptyMetricsImplementsMetricsRecorder(t *testing.T) {
	var _ MetricsRecorder = (*EmptyMetrics)(nil)
	var _ MetricsRecorder = NewEmptyMetrics()
}

func TestNewEmptyMetrics(t *testing.T) {
	m := NewEmptyMetrics()
	assert.NotNil(t, m)
}

func TestEmptyMetricsNoOp(t *testing.T) {
	m := NewEmptyMetrics()

	assert.NotPanics(t, func() {
		m.RecordTaskStart("task-1")
		m.RecordTaskSuccess("task-1", 1.5)
		m.RecordTaskFailure("task-1", 2.5)
		m.RecordLockAcquisition("task-1", true)
		m.RecordLockAcquisition("task-1", false)
		m.SetQueueSize("pending", 10)
		m.SetActiveTasks(5)
	})
}
