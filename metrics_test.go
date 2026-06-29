package scheduler

import (
	"fmt"
	"sync"
	"testing"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
)

// TestNewPrometheusMetricsWithRegistry 验证 Bug 4 修复：
// 使用独立的 Registry 创建多个 PrometheusMetrics 不会 panic
func TestNewPrometheusMetricsWithRegistry(t *testing.T) {
	reg1 := prometheus.NewRegistry()
	m1 := NewPrometheusMetrics(reg1, "ns1")
	assert.NotNil(t, m1)

	// 第二个实例使用不同的 registry，不应 panic
	reg2 := prometheus.NewRegistry()
	m2 := NewPrometheusMetrics(reg2, "ns2")
	assert.NotNil(t, m2)
}

// TestNewPrometheusMetricsNoDuplicatePanic 验证 Bug 4 修复：
// 同一进程中用不同 registry 创建多次不 panic
func TestNewPrometheusMetricsNoDuplicatePanic(t *testing.T) {
	assert.NotPanics(t, func() {
		for i := 0; i < 5; i++ {
			reg := prometheus.NewRegistry()
			m := NewPrometheusMetrics(reg, fmt.Sprintf("ns%d", i))
			assert.NotNil(t, m)
		}
	})
}

// TestNewPrometheusMetricsNilRegistererUsesDefault
func TestNewPrometheusMetricsNilRegistererUsesDefault(t *testing.T) {
	assert.NotPanics(t, func() {
		m := NewPrometheusMetrics(nil, "test_nil_reg")
		assert.NotNil(t, m)
	})
}

// TestNewPrometheusMetricsEmptyNamespace
func TestNewPrometheusMetricsEmptyNamespace(t *testing.T) {
	reg := prometheus.NewRegistry()
	m := NewPrometheusMetrics(reg, "")
	assert.Equal(t, defaultNamespace, m.GetNamespace())
}

// TestNewPrometheusMetricsWithDefaults 验证向后兼容构造器
func TestNewPrometheusMetricsWithDefaults(t *testing.T) {
	assert.NotPanics(t, func() {
		m := NewPrometheusMetricsWithDefaults("test_defaults")
		assert.NotNil(t, m)
		assert.Equal(t, "test_defaults", m.GetNamespace())
	})
}

// TestPrometheusMetricsRecordMethods
func TestPrometheusMetricsRecordMethods(t *testing.T) {
	reg := prometheus.NewRegistry()
	m := NewPrometheusMetrics(reg, "test_record")

	assert.NotPanics(t, func() {
		m.RecordTaskStart("task-a")
		m.RecordTaskSuccess("task-a", 0.5)
		m.RecordTaskFailure("task-a", 1.2)
		m.RecordLockAcquisition("task-a", true)
		m.RecordLockAcquisition("task-a", false)
		m.SetQueueSize("pending", 10)
		m.SetQueueSize("running", 3)
		m.SetActiveTasks(5)
	})
}

// TestPrometheusMetricsRecordMultipleTasks
func TestPrometheusMetricsRecordMultipleTasks(t *testing.T) {
	reg := prometheus.NewRegistry()
	m := NewPrometheusMetrics(reg, "test_multi")

	assert.NotPanics(t, func() {
		for _, name := range []string{"task-1", "task-2", "task-3"} {
			m.RecordTaskStart(name)
			m.RecordTaskSuccess(name, 0.1)
			m.RecordTaskFailure(name, 0.2)
			m.RecordLockAcquisition(name, true)
			m.RecordLockAcquisition(name, false)
		}
		m.SetQueueSize("pending", 5)
		m.SetActiveTasks(3)
	})
}

// TestPrometheusMetricsSetGetNamespace
func TestPrometheusMetricsSetGetNamespace(t *testing.T) {
	reg := prometheus.NewRegistry()
	m := NewPrometheusMetrics(reg, "initial_ns")

	assert.Equal(t, "initial_ns", m.GetNamespace())

	m.SetNamespace("updated_ns")
	assert.Equal(t, "updated_ns", m.GetNamespace())

	m.SetNamespace("")
	assert.Equal(t, "", m.GetNamespace())
}

// TestPrometheusMetricsImplementsMetricsRecorder
func TestPrometheusMetricsImplementsMetricsRecorder(t *testing.T) {
	reg := prometheus.NewRegistry()
	m := NewPrometheusMetrics(reg, "test_iface")
	var _ MetricsRecorder = m
}

// TestPrometheusMetricsConcurrent 验证 -race 下并发访问安全
func TestPrometheusMetricsConcurrent(t *testing.T) {
	reg := prometheus.NewRegistry()
	m := NewPrometheusMetrics(reg, "test_concurrent")

	var wg sync.WaitGroup
	for i := 0; i < 20; i++ {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			name := fmt.Sprintf("task-%d", idx%3)
			m.RecordTaskStart(name)
			m.RecordTaskSuccess(name, 0.1)
			m.RecordTaskFailure(name, 0.2)
			m.RecordLockAcquisition(name, true)
			m.SetQueueSize("pending", float64(idx))
			m.SetActiveTasks(float64(idx))
		}(i)
	}

	// 并发读写 namespace
	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			m.SetNamespace(fmt.Sprintf("ns-%d", idx))
			_ = m.GetNamespace()
		}(i)
	}

	wg.Wait()
}

// TestNewTaskExecutorWithPrometheusMetrics 确保 executor 与 PrometheusMetrics 协同工作
func TestNewTaskExecutorWithPrometheusMetrics(t *testing.T) {
	reg := prometheus.NewRegistry()
	metrics := NewPrometheusMetrics(reg, "test_exec")
	executor := NewTaskExecutor(newMockLock(), metrics, zap.NewNop())
	require.NotNil(t, executor)
}
