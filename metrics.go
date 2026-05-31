package scheduler

import (
	"sync"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

// 默认 namespace
var defaultNamespace = "go_scheduler"
var namespaceMu sync.RWMutex

// SetNamespace 设置 Prometheus 指标的 namespace
// 必须在创建 PrometheusMetrics 之前调用
func SetNamespace(ns string) {
	namespaceMu.Lock()
	defer namespaceMu.Unlock()
	defaultNamespace = ns
}

// GetNamespace 返回当前设置的 namespace
func GetNamespace() string {
	namespaceMu.RLock()
	defer namespaceMu.RUnlock()
	return defaultNamespace
}

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
	namespace          string
	taskExecutionTotal *prometheus.CounterVec
	taskExecutionDuration *prometheus.HistogramVec
	taskLastExecutionTime *prometheus.GaugeVec
	taskQueueSize      *prometheus.GaugeVec
	lockAcquisitionTotal *prometheus.CounterVec
	activeTasksGauge   prometheus.Gauge
}

// NewPrometheusMetrics 创建 Prometheus 指标记录器
// 如果 namespace 为空，则使用默认值
func NewPrometheusMetrics(namespace string) *PrometheusMetrics {
	if namespace == "" {
		namespace = GetNamespace()
	}

	return &PrometheusMetrics{
		namespace: namespace,
		taskExecutionTotal: promauto.NewCounterVec(
			prometheus.CounterOpts{
				Namespace: namespace,
				Subsystem: "scheduler",
				Name:      "task_execution_total",
				Help:      "Total number of task executions",
			},
			[]string{"task", "status"},
		),
		taskExecutionDuration: promauto.NewHistogramVec(
			prometheus.HistogramOpts{
				Namespace: namespace,
				Subsystem: "scheduler",
				Name:      "task_execution_duration_seconds",
				Help:      "Task execution duration in seconds",
				Buckets:   prometheus.ExponentialBuckets(0.1, 2, 10),
			},
			[]string{"task"},
		),
		taskLastExecutionTime: promauto.NewGaugeVec(
			prometheus.GaugeOpts{
				Namespace: namespace,
				Subsystem: "scheduler",
				Name:      "task_last_execution_timestamp",
				Help:      "Unix timestamp of last task execution",
			},
			[]string{"task"},
		),
		taskQueueSize: promauto.NewGaugeVec(
			prometheus.GaugeOpts{
				Namespace: namespace,
				Subsystem: "scheduler",
				Name:      "task_queue_size",
				Help:      "Number of tasks in queue",
			},
			[]string{"status"},
		),
		lockAcquisitionTotal: promauto.NewCounterVec(
			prometheus.CounterOpts{
				Namespace: namespace,
				Subsystem: "scheduler",
				Name:      "lock_acquisition_total",
				Help:      "Total number of lock acquisitions",
			},
			[]string{"task", "result"},
		),
		activeTasksGauge: promauto.NewGauge(
			prometheus.GaugeOpts{
				Namespace: namespace,
				Subsystem: "scheduler",
				Name:      "active_tasks",
				Help:      "Number of currently active tasks",
			},
		),
	}
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
