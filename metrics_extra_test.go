package scheduler

import (
	"errors"
	"testing"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestPrometheusMetricsReusesCollectorsOnDuplicateRegistration covers the
// AlreadyRegisteredError branch of registerOrGet: re-registering the same
// metrics against the same registry must reuse the existing collectors instead
// of panicking.
func TestPrometheusMetricsReusesCollectorsOnDuplicateRegistration(t *testing.T) {
	reg := prometheus.NewRegistry()

	m1 := NewPrometheusMetrics(reg, "reuse_ns")
	require.NotNil(t, m1)

	m2 := NewPrometheusMetrics(reg, "reuse_ns")
	require.NotNil(t, m2)

	// Both instances must point at the same underlying collectors.
	assert.Same(t, m1.taskExecutionTotal, m2.taskExecutionTotal)
	assert.Same(t, m1.taskExecutionDuration, m2.taskExecutionDuration)
	assert.Same(t, m1.activeTasksGauge, m2.activeTasksGauge)

	// The reused instance must still record without errors.
	assert.NotPanics(t, func() {
		m2.RecordTaskStart("task-a")
		m2.RecordTaskSuccess("task-a", 0.5)
	})
}

// failingRegisterer returns an error from Register that is not an
// AlreadyRegisteredError, forcing registerOrGet onto its panic branch.
type failingRegisterer struct{}

func (failingRegisterer) Register(prometheus.Collector) error { return errors.New("register failed") }
func (failingRegisterer) MustRegister(...prometheus.Collector) {}
func (failingRegisterer) Unregister(prometheus.Collector) bool { return true }

// TestPrometheusMetricsPanicsOnOtherRegisterError covers the panic branch of
// registerOrGet: an unexpected registration error must surface loudly.
func TestPrometheusMetricsPanicsOnOtherRegisterError(t *testing.T) {
	assert.Panics(t, func() {
		NewPrometheusMetrics(failingRegisterer{}, "panic_ns")
	})
}
