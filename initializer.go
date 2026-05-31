package scheduler

import (
	"context"

	"go.uber.org/zap"
)

// TaskRegistry 任务注册器
type TaskRegistry struct {
	manager *SchedulerManager
	logger  *zap.Logger
}

// NewTaskRegistry 创建任务注册器
func NewTaskRegistry(manager *SchedulerManager, logger *zap.Logger) *TaskRegistry {
	return &TaskRegistry{
		manager: manager,
		logger:  logger,
	}
}

// RegisterTask 注册单个任务
func (r *TaskRegistry) RegisterTask(task Task) error {
	if err := r.manager.RegisterTask(task); err != nil {
		r.logger.Error("failed to register task",
			zap.String("task", task.Name()),
			zap.Error(err),
		)
		return err
	}
	r.logger.Info("task registered", zap.String("task", task.Name()))
	return nil
}

// RegisterAll 批量注册任务
func (r *TaskRegistry) RegisterAll(tasks ...Task) error {
	for _, task := range tasks {
		if err := r.manager.RegisterTask(task); err != nil {
			r.logger.Error("failed to register task",
				zap.String("task", task.Name()),
				zap.Error(err),
			)
			return err
		}
		r.logger.Info("task registered", zap.String("task", task.Name()))
	}

	return nil
}

// InitializeScheduler 初始化并启动调度器
func InitializeScheduler(
	ctx context.Context,
	manager *SchedulerManager,
	logger *zap.Logger,
) error {
	if err := manager.Start(ctx); err != nil {
		return err
	}

	logger.Info("scheduler initialized successfully")
	return nil
}
