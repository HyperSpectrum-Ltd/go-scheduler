package scheduler

import (
	"sync"

	"github.com/prometheus/client_golang/prometheus"
)

// 默认 namespace
const defaultNamespace = "go_scheduler"

// MetricsRecorder 指标记录器接口
type MetricsRecorder interface {
	RecordTaskStart(taskName string)
	RecordTaskSuccess(taskName string, duration float64)
	RecordTaskFailure(taskName string, duration float64)
	RecordLockAcquisition(taskName string, success bool)
	SetQueueSize(status string, size float64)
	SetActiveTasks(count float64)
}

// PrometheusMetrics 基于 Prometheus 的指标实现
type PrometheusMetrics struct {
	mu sync.RWMutex

	namespace             string
	taskExecutionTotal    *prometheus.CounterVec
	taskExecutionDuration *prometheus.HistogramVec
	taskLastExecutionTime *prometheus.GaugeVec
	taskQueueSize         *prometheus.GaugeVec
	lockAcquisitionTotal  *prometheus.CounterVec
	activeTasksGauge      prometheus.Gauge
}

// registerOrGet 尝试注册 collector；若已有相同描述的 collector，则返回已注册的实例
// 相比 promauto.MustRegister，在重复注册场景（测试 -count>1、生产多次初始化）下不会 panic
func registerOrGet[T prometheus.Collector](r prometheus.Registerer, c T) T {
	if err := r.Register(c); err != nil {
		if are, ok := err.(prometheus.AlreadyRegisteredError); ok {
			if existing, ok := are.ExistingCollector.(T); ok {
				return existing
			}
		}
		panic(err)
	}
	return c
}

// NewPrometheusMetrics 创建 Prometheus 指标记录器
// registerer 用于注册指标，测试时可传入 prometheus.NewRegistry() 避免重复注册 panic
// namespace 为指标命名空间，为空时使用默认值 "go_scheduler"
// 重复注册同一指标时不会 panic，而是复用已注册的 collector
func NewPrometheusMetrics(registerer prometheus.Registerer, namespace string) *PrometheusMetrics {
	if registerer == nil {
		registerer = prometheus.DefaultRegisterer
	}
	if namespace == "" {
		namespace = defaultNamespace
	}

	return &PrometheusMetrics{
		namespace: namespace,
		taskExecutionTotal: registerOrGet(registerer, prometheus.NewCounterVec(
			prometheus.CounterOpts{
				Namespace: namespace,
				Subsystem: "scheduler",
				Name:      "task_execution_total",
				Help:      "Total number of task executions",
			},
			[]string{"task", "status"},
		)),
		taskExecutionDuration: registerOrGet(registerer, prometheus.NewHistogramVec(
			prometheus.HistogramOpts{
				Namespace: namespace,
				Subsystem: "scheduler",
				Name:      "task_execution_duration_seconds",
				Help:      "Task execution duration in seconds",
				Buckets:   prometheus.ExponentialBuckets(0.1, 2, 10),
			},
			[]string{"task"},
		)),
		taskLastExecutionTime: registerOrGet(registerer, prometheus.NewGaugeVec(
			prometheus.GaugeOpts{
				Namespace: namespace,
				Subsystem: "scheduler",
				Name:      "task_last_execution_timestamp",
				Help:      "Unix timestamp of last task execution",
			},
			[]string{"task"},
		)),
		taskQueueSize: registerOrGet(registerer, prometheus.NewGaugeVec(
			prometheus.GaugeOpts{
				Namespace: namespace,
				Subsystem: "scheduler",
				Name:      "task_queue_size",
				Help:      "Number of tasks in queue",
			},
			[]string{"status"},
		)),
		lockAcquisitionTotal: registerOrGet(registerer, prometheus.NewCounterVec(
			prometheus.CounterOpts{
				Namespace: namespace,
				Subsystem: "scheduler",
				Name:      "lock_acquisition_total",
				Help:      "Total number of lock acquisitions",
			},
			[]string{"task", "result"},
		)),
		activeTasksGauge: registerOrGet(registerer, prometheus.NewGauge(
			prometheus.GaugeOpts{
				Namespace: namespace,
				Subsystem: "scheduler",
				Name:      "active_tasks",
				Help:      "Number of currently active tasks",
			},
		)),
	}
}

// NewPrometheusMetricsWithDefaults 使用默认 Registerer 创建 Prometheus 指标记录器
// 保持向后兼容，适合生产环境使用
func NewPrometheusMetricsWithDefaults(namespace string) *PrometheusMetrics {
	return NewPrometheusMetrics(prometheus.DefaultRegisterer, namespace)
}

// SetNamespace 设置 Prometheus 指标的 namespace
func (m *PrometheusMetrics) SetNamespace(ns string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.namespace = ns
}

// GetNamespace 返回当前设置的 namespace
func (m *PrometheusMetrics) GetNamespace() string {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.namespace
}

func (m *PrometheusMetrics) RecordTaskStart(taskName string) {
	m.taskLastExecutionTime.WithLabelValues(taskName).SetToCurrentTime()
}

func (m *PrometheusMetrics) RecordTaskSuccess(taskName string, duration float64) {
	m.taskExecutionTotal.WithLabelValues(taskName, "success").Inc()
	m.taskExecutionDuration.WithLabelValues(taskName).Observe(duration)
	m.taskLastExecutionTime.WithLabelValues(taskName).SetToCurrentTime()
}

func (m *PrometheusMetrics) RecordTaskFailure(taskName string, duration float64) {
	m.taskExecutionTotal.WithLabelValues(taskName, "failed").Inc()
	m.taskExecutionDuration.WithLabelValues(taskName).Observe(duration)
	m.taskLastExecutionTime.WithLabelValues(taskName).SetToCurrentTime()
}

func (m *PrometheusMetrics) RecordLockAcquisition(taskName string, success bool) {
	result := "success"
	if !success {
		result = "failed"
	}
	m.lockAcquisitionTotal.WithLabelValues(taskName, result).Inc()
}

func (m *PrometheusMetrics) SetQueueSize(status string, size float64) {
	m.taskQueueSize.WithLabelValues(status).Set(size)
}

func (m *PrometheusMetrics) SetActiveTasks(count float64) {
	m.activeTasksGauge.Set(count)
}
