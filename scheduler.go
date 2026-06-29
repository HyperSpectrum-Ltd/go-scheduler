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
	mu sync.Mutex

	Name         string
	LastRunTime  *time.Time
	LastDuration time.Duration
	SuccessCount int64
	FailedCount  int64
	IsRunning    bool
	LastErr      error
}

// snapshot 在持锁状态下返回 TaskStatus 的值拷贝，用于安全地对外暴露状态
func (s *TaskStatus) snapshot() TaskStatus {
	s.mu.Lock()
	defer s.mu.Unlock()
	return TaskStatus{
		Name:         s.Name,
		LastRunTime:  s.LastRunTime,
		LastDuration: s.LastDuration,
		SuccessCount: s.SuccessCount,
		FailedCount:  s.FailedCount,
		IsRunning:    s.IsRunning,
		LastErr:      s.LastErr,
	}
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
			m.wg.Add(1)
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
		m.wg.Add(1)
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
	defer m.wg.Done()

	// Bug 3: 检查调度器是否已停止
	select {
	case <-m.stopCh:
		return
	default:
	}

	status := m.taskStatus[task.Name()]

	// Bug 1: 使用独立的状态锁保护 TaskStatus 字段的读写（不与 map 锁混用以避免死锁）
	status.mu.Lock()
	if status.IsRunning && task.RequireLock() {
		status.mu.Unlock()
		m.logger.Warn("task is already running, skipping",
			zap.String("task", task.Name()),
		)
		return
	}
	status.IsRunning = true
	status.mu.Unlock()

	defer func() {
		status.mu.Lock()
		status.IsRunning = false
		status.mu.Unlock()
	}()

	startTime := time.Now()
	m.metrics.RecordTaskStart(task.Name())

	err := m.executor.Execute(ctx, task)

	duration := time.Since(startTime)

	status.mu.Lock()
	status.LastRunTime = &startTime
	status.LastDuration = duration
	if err != nil {
		status.FailedCount++
		status.LastErr = err
	} else {
		status.SuccessCount++
	}
	status.mu.Unlock()

	if err != nil {
		m.metrics.RecordTaskFailure(task.Name(), duration.Seconds())
		m.logger.Error("task execution failed",
			zap.String("task", task.Name()),
			zap.Duration("duration", duration),
			zap.Error(err),
		)
	} else {
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
	if !m.running {
		m.mu.Unlock()
		return nil
	}
	m.running = false
	close(m.stopCh)
	m.mu.Unlock()

	cronCtx := m.cron.Stop()
	<-cronCtx.Done()

	// Bug 2/3: 等待所有 runTask goroutine 完成，同时尊重 ctx 超时
	done := make(chan struct{})
	go func() {
		m.wg.Wait()
		close(done)
	}()

	select {
	case <-done:
		m.logger.Info("scheduler stopped")
		return nil
	case <-ctx.Done():
		m.logger.Warn("scheduler stop timed out, some goroutines may still be running")
		return ctx.Err()
	}
}

// TriggerTask 手动触发任务
func (m *SchedulerManager) TriggerTask(ctx context.Context, taskName string) error {
	m.mu.RLock()
	task, exists := m.tasks[taskName]
	m.mu.RUnlock()

	if !exists {
		return fmt.Errorf("%w: %s", ErrTaskNotFound, taskName)
	}

	m.wg.Add(1)
	go m.runTask(ctx, task)
	return nil
}

// GetTaskStatus 获取任务状态
func (m *SchedulerManager) GetTaskStatus(taskName string) (*TaskStatus, error) {
	m.mu.RLock()
	status, exists := m.taskStatus[taskName]
	m.mu.RUnlock()

	if !exists {
		return nil, fmt.Errorf("%w: %s", ErrTaskNotFound, taskName)
	}
	snap := status.snapshot()
	return &snap, nil
}

// GetAllTaskStatus 获取所有任务状态
func (m *SchedulerManager) GetAllTaskStatus() map[string]*TaskStatus {
	m.mu.RLock()
	defer m.mu.RUnlock()

	result := make(map[string]*TaskStatus, len(m.taskStatus))
	for k, v := range m.taskStatus {
		snap := v.snapshot()
		result[k] = &snap
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
