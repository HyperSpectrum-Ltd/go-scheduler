package scheduler

// EmptyMetrics 空指标实现，用于不需要 Prometheus 指标的场景
type EmptyMetrics struct{}

// NewEmptyMetrics 创建空指标实现
func NewEmptyMetrics() *EmptyMetrics {
	return &EmptyMetrics{}
}

func (m *EmptyMetrics) RecordTaskStart(taskName string)                     {}
func (m *EmptyMetrics) RecordTaskSuccess(taskName string, duration float64) {}
func (m *EmptyMetrics) RecordTaskFailure(taskName string, duration float64) {}
func (m *EmptyMetrics) RecordLockAcquisition(taskName string, success bool) {}
func (m *EmptyMetrics) SetQueueSize(status string, size float64)            {}
func (m *EmptyMetrics) SetActiveTasks(count float64)                        {}
