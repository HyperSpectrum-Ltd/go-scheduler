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
		config: config,
		// cronParser 同时接受 5 段与 6 段表达式。
		//
		// 历史行为：固定 cron.WithSeconds() 只认 6 段，导致调用方（如 vpn-backend
		// 的 37 个任务与既有 5 段配置）必须自行补 "0 " 前缀。SecondOptional 让
		// 5 段表达式的秒位缺省为 0；6 段行为完全不变；Descriptor（@hourly 等）保持兼容。
		cron:       cron.New(cron.WithParser(cronParser()), cron.WithLogger(cronLogger{logger: logger})),
		lock:       lock,
		executor:   NewTaskExecutor(lock, metrics, logger),
		metrics:    metrics,
		logger:     logger,
		tasks:      make(map[string]Task),
		taskStatus: make(map[string]*TaskStatus),
		stopCh:     make(chan struct{}),
	}
}

// cronParser 返回同时支持 5 段与 6 段 cron 的解析器。
func cronParser() cron.Parser {
	return cron.NewParser(
		cron.SecondOptional | cron.Minute | cron.Hour |
			cron.Dom | cron.Month | cron.Dow | cron.Descriptor,
	)
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
			// 同步认领后再起 goroutine（见 beginRun 注释：判定必须与触发同序）
			if m.beginRun(task) {
				m.wg.Add(1)
				go m.runTask(ctx, task)
			}
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
		// 同步认领：cron 触发同样必须"运行中即跳过"，不能把判定留给异步执行体
		if !m.beginRun(task) {
			m.logger.Warn("task is already running, skipping",
				zap.String("task", task.Name()),
			)
			return
		}
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

// beginRun 原子地"认领"一次任务执行。
//
// 返回 false 表示本次触发应被**完整跳过**：调度器已停止，或在 RequireLock 下任务已在运行。
// 返回 true 时 IsRunning 已被置为 true（调用方随后必须调用 runTask 执行并在结束时释放）。
//
// ⚠️ 为什么必须在**调用点同步**认领（2026-09-28 修复，由新接入的嵌套 module 测试门禁暴露）：
// 此前"是否已在运行"的判断写在 runTask 内部，而 TriggerTask 的实现是 `go m.runTask(...)`
// 后立即返回——于是"运行中触发即跳过"这条约定**从来没有被保证**：只要两个被触发的
// goroutine 之间存在调度间隔（前一个在第二个拿到 CPU 之前就已结束），第二个就会看到
// IsRunning=false 并**真的再执行一次**。实测在 `-count=N` 高重复下约 10% 触发，
// 且失败现场明确显示 `success=2 execCount=2`（任务执行了两次）。
// 认领移到调用点后，判定与触发同序，该约定成为**确定性**行为。
//
// 注意：这是模块级行为修正，不是测试放宽——原测试断言（触发两次只执行一次）是对的，
// 是实现的保证缺失。
func (m *SchedulerManager) beginRun(task Task) bool {
	// 调度器已停止：不再认领任何新运行
	select {
	case <-m.stopCh:
		return false
	default:
	}

	status := m.taskStatus[task.Name()]

	// Bug 1: 使用独立的状态锁保护 TaskStatus 字段的读写（不与 map 锁混用以避免死锁）
	status.mu.Lock()
	defer status.mu.Unlock()
	if status.IsRunning && task.RequireLock() {
		return false
	}
	status.IsRunning = true
	return true
}

// runTask 执行一次**已由 beginRun 认领**的任务运行。
//
// 前提：调用方已调用 beginRun 并得到 true；本方法只负责执行与在结束时清除 IsRunning。
func (m *SchedulerManager) runTask(ctx context.Context, task Task) {
	defer m.wg.Done()

	status := m.taskStatus[task.Name()]

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

	// 同步认领：保证"运行中触发即跳过"是确定性行为（见 beginRun 注释）。
	// 被跳过时返回 nil —— 与既有约定一致（调用方无法区分"已触发"与"因在运行而跳过"，
	// 二者都不是错误）。跳过原因记入日志。
	if !m.beginRun(task) {
		m.logger.Warn("task is already running, skipping",
			zap.String("task", taskName),
		)
		return nil
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
