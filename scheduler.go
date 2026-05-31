package scheduler

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/robfig/cron/v3"
	"go.uber.org/zap"
)

// SchedulerManager 调度管理器
type SchedulerManager struct {
	config   Config
	cron     *cron.Cron
	lock     DistributedLock
	executor *TaskExecutor
	metrics  MetricsRecorder
	logger   *zap.Logger

	tasks      map[string]Task
	taskStatus map[string]*TaskStatus
	mu         sync.RWMutex

	running bool
	stopCh  chan struct{}
	wg      sync.WaitGroup
}

// TaskStatus 任务状态
type TaskStatus struct {
	Name         string
	LastRunTime  *time.Time
	LastDuration time.Duration
	SuccessCount int64
	FailedCount  int64
	IsRunning    bool
	LastErr      error
}

// NewSchedulerManager 创建调度管理器
func NewSchedulerManager(
	config Config,
	lock DistributedLock,
	metrics MetricsRecorder,
	logger *zap.Logger,
) *SchedulerManager {
	if config.WorkerPoolSize <= 0 {
		config.WorkerPoolSize = 10
	}

	return &SchedulerManager{
		config:     config,
		cron:       cron.New(cron.WithSeconds(), cron.WithLogger(cronLogger{logger: logger})),
		lock:       lock,
		executor:   NewTaskExecutor(lock, metrics, logger),
		metrics:    metrics,
		logger:     logger,
		tasks:      make(map[string]Task),
		taskStatus: make(map[string]*TaskStatus),
		stopCh:     make(chan struct{}),
	}
}

// RegisterTask 注册任务
func (m *SchedulerManager) RegisterTask(task Task) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	if _, exists := m.tasks[task.Name()]; exists {
		return fmt.Errorf("%w: %s", ErrTaskAlreadyExists, task.Name())
	}

	if !task.Schedule().Enabled {
		m.logger.Info("task is disabled, skipping registration",
			zap.String("task", task.Name()),
		)
		return nil
	}

	m.tasks[task.Name()] = task
	m.taskStatus[task.Name()] = &TaskStatus{
		Name: task.Name(),
	}

	return nil
}

// Start 启动调度器
func (m *SchedulerManager) Start(ctx context.Context) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	if m.running {
		return nil
	}

	if !m.config.Enabled {
		m.logger.Info("scheduler is disabled")
		return nil
	}

	m.running = true

	for name, task := range m.tasks {
		if err := m.scheduleTask(ctx, task); err != nil {
			m.logger.Error("failed to schedule task",
				zap.String("task", name),
				zap.Error(err),
			)
			continue
		}

		if task.Schedule().RunOnStart {
			go m.runTask(ctx, task)
		}
	}

	m.cron.Start()

	m.logger.Info("scheduler started",
		zap.Int("task_count", len(m.tasks)),
	)

	return nil
}

// scheduleTask 调度任务
func (m *SchedulerManager) scheduleTask(ctx context.Context, task Task) error {
	schedule := task.Schedule()

	jobFunc := func() {
		m.runTask(ctx, task)
	}

	switch schedule.Type {
	case ScheduleTypeCron:
		_, err := m.cron.AddFunc(schedule.CronExpr, jobFunc)
		return err
	case ScheduleTypeInterval:
		_, err := m.cron.AddFunc(fmt.Sprintf("@every %s", schedule.Interval), jobFunc)
		return err
	default:
		return fmt.Errorf("unsupported schedule type: %s", schedule.Type)
	}
}

// runTask 运行任务
func (m *SchedulerManager) runTask(ctx context.Context, task Task) {
	status := m.taskStatus[task.Name()]
	if status.IsRunning && task.RequireLock() {
		m.logger.Warn("task is already running, skipping",
			zap.String("task", task.Name()),
		)
		return
	}

	status.IsRunning = true
	defer func() { status.IsRunning = false }()

	startTime := time.Now()
	m.metrics.RecordTaskStart(task.Name())

	err := m.executor.Execute(ctx, task)

	duration := time.Since(startTime)
	status.LastRunTime = &startTime
	status.LastDuration = duration

	if err != nil {
		status.FailedCount++
		status.LastErr = err
		m.metrics.RecordTaskFailure(task.Name(), duration.Seconds())
		m.logger.Error("task execution failed",
			zap.String("task", task.Name()),
			zap.Duration("duration", duration),
			zap.Error(err),
		)
	} else {
		status.SuccessCount++
		m.metrics.RecordTaskSuccess(task.Name(), duration.Seconds())
		m.logger.Info("task execution completed",
			zap.String("task", task.Name()),
			zap.Duration("duration", duration),
		)
	}
}

// Stop 停止调度器
func (m *SchedulerManager) Stop(ctx context.Context) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	if !m.running {
		return nil
	}

	m.running = false
	close(m.stopCh)

	cronCtx := m.cron.Stop()
	<-cronCtx.Done()

	m.wg.Wait()

	m.logger.Info("scheduler stopped")
	return nil
}

// TriggerTask 手动触发任务
func (m *SchedulerManager) TriggerTask(ctx context.Context, taskName string) error {
	m.mu.RLock()
	task, exists := m.tasks[taskName]
	m.mu.RUnlock()

	if !exists {
		return fmt.Errorf("%w: %s", ErrTaskNotFound, taskName)
	}

	go m.runTask(ctx, task)
	return nil
}

// GetTaskStatus 获取任务状态
func (m *SchedulerManager) GetTaskStatus(taskName string) (*TaskStatus, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	status, exists := m.taskStatus[taskName]
	if !exists {
		return nil, fmt.Errorf("%w: %s", ErrTaskNotFound, taskName)
	}
	return status, nil
}

// GetAllTaskStatus 获取所有任务状态
func (m *SchedulerManager) GetAllTaskStatus() map[string]*TaskStatus {
	m.mu.RLock()
	defer m.mu.RUnlock()

	result := make(map[string]*TaskStatus, len(m.taskStatus))
	for k, v := range m.taskStatus {
		result[k] = v
	}
	return result
}

// IsRunning 检查调度器是否正在运行
func (m *SchedulerManager) IsRunning() bool {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.running
}

// cronLogger 适配 cron 日志到 zap
type cronLogger struct {
	logger *zap.Logger
}

func (l cronLogger) Info(msg string, keysAndValues ...interface{}) {
	l.logger.Sugar().Infow(msg, keysAndValues...)
}

func (l cronLogger) Error(err error, msg string, keysAndValues ...interface{}) {
	l.logger.Sugar().Errorw(msg, append([]interface{}{"error", err}, keysAndValues...)...)
}
