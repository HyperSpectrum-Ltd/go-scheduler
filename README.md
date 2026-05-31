# go-scheduler

[![Go Reference](https://pkg.go.dev/badge/github.com/HyperSpectrum-Ltd/go-scheduler.svg)](https://pkg.go.dev/github.com/HyperSpectrum-Ltd/go-scheduler)
[![Go Report Card](https://goreportcard.com/badge/github.com/HyperSpectrum-Ltd/go-scheduler)](https://goreportcard.com/report/github.com/HyperSpectrum-Ltd/go-scheduler)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)

一个通用的分布式任务调度框架，支持 Cron 表达式、固定间隔调度、分布式锁、重试机制和 Prometheus 指标监控。专为云原生环境设计，可与 Kubernetes、Docker 等容器化部署无缝集成。

## ✨ 特性

- **⏰ 灵活调度**：支持 Cron 表达式和固定间隔两种调度方式
- **🔒 分布式锁**：基于 Redis 的分布式锁，确保任务在集群中只执行一次
- **🔄 自动重试**：支持指数退避的重试策略，可配置重试次数和间隔
- **📊 可观测性**：集成 Prometheus 指标，实时监控任务执行情况
- **🎯 超时控制**：每个任务可配置独立的超时时间
- **🚀 高性能**：基于 robfig/cron 调度引擎，轻量高效
- **🔧 易于扩展**：通过 Task 接口轻松定义自定义任务

## 📋 目录

- [安装](#安装)
- [快速开始](#快速开始)
- [使用指南](#使用指南)
  - [定义任务](#定义任务)
  - [配置调度器](#配置调度器)
  - [分布式锁配置](#分布式锁配置)
  - [Prometheus 指标](#prometheus-指标)
  - [任务注册与初始化](#任务注册与初始化)
- [配置说明](#配置说明)
- [API 文档](#api-文档)
- [架构设计](#架构设计)
- [许可证](#许可证)

## 安装

```bash
go get github.com/HyperSpectrum-Ltd/go-scheduler
```

**要求**：
- Go >= 1.24.0

## 快速开始

### 基本使用

```go
package main

import (
    "context"
    "fmt"
    "time"

    "github.com/HyperSpectrum-Ltd/go-scheduler"
    "github.com/redis/go-redis/v9"
    "go.uber.org/zap"
)

// 定义自定义任务
type MyTask struct {
    scheduler.BaseTask
}

func NewMyTask() *MyTask {
    return &MyTask{
        BaseTask: *scheduler.NewBaseTask(
            "my-task",
            "我的自定义任务",
            scheduler.ScheduleConfig{
                Type:     scheduler.ScheduleTypeInterval,
                Interval: 1 * time.Minute,
                Enabled:  true,
            },
        ),
    }
}

func (t *MyTask) Execute(ctx context.Context) error {
    fmt.Println("任务执行中...")
    return nil
}

func main() {
    // 创建 logger
    logger, _ := zap.NewDevelopment()

    // 创建 Redis 客户端
    redisClient := redis.NewClient(&redis.Options{
        Addr: "localhost:6379",
    })

    // 创建分布式锁
    lock := scheduler.NewRedisLock(redisClient, logger, "my-app:lock:")

    // 创建指标记录器（使用空实现，生产环境建议使用 PrometheusMetrics）
    metrics := scheduler.NewEmptyMetrics()

    // 创建调度管理器
    config := scheduler.DefaultConfig()
    manager := scheduler.NewSchedulerManager(config, lock, metrics, logger)

    // 注册任务
    task := NewMyTask()
    if err := manager.RegisterTask(task); err != nil {
        logger.Fatal("注册任务失败", zap.Error(err))
    }

    // 启动调度器
    ctx := context.Background()
    if err := manager.Start(ctx); err != nil {
        logger.Fatal("启动调度器失败", zap.Error(err))
    }

    // 保持运行
    select {}
}
```

### 使用 Cron 表达式

```go
type CronTask struct {
    scheduler.BaseTask
}

func NewCronTask() *CronTask {
    return &CronTask{
        BaseTask: *scheduler.NewBaseTask(
            "cron-task",
            "每小时执行的任务",
            scheduler.ScheduleConfig{
                Type:     scheduler.ScheduleTypeCron,
                CronExpr: "0 * * * * *", // 每小时执行
                Enabled:  true,
            },
        ),
    }
}

func (t *CronTask) Execute(ctx context.Context) error {
    fmt.Println("Cron 任务执行中...")
    return nil
}
```

## 使用指南

### 定义任务

通过嵌入 `BaseTask` 并实现 `Execute` 方法来自定义任务：

```go
type DataCleanupTask struct {
    scheduler.BaseTask
    db *sql.DB
}

func NewDataCleanupTask(db *sql.DB) *DataCleanupTask {
    task := &DataCleanupTask{
        BaseTask: *scheduler.NewBaseTask(
            "data-cleanup",
            "清理过期数据",
            scheduler.ScheduleConfig{
                Type:     scheduler.ScheduleTypeCron,
                CronExpr: "0 0 2 * * *", // 每天凌晨2点执行
                Enabled:  true,
            },
        ),
        db: db,
    }
    
    // 自定义超时时间
    task.SetTimeout(30 * time.Minute)
    
    // 不需要分布式锁
    task.SetRequireLock(false)
    
    return task
}

func (t *DataCleanupTask) Execute(ctx context.Context) error {
    // 实现清理逻辑
    _, err := t.db.ExecContext(ctx, "DELETE FROM logs WHERE created_at < NOW() - INTERVAL '30 days'")
    return err
}
```

### 配置调度器

```go
// 使用默认配置
config := scheduler.DefaultConfig()

// 自定义配置
config := scheduler.Config{
    Enabled:        true,
    WorkerPoolSize: 20,
    LockTTL:        15 * time.Minute,
    MetricsEnabled: true,
}

// 创建调度管理器
manager := scheduler.NewSchedulerManager(config, lock, metrics, logger)
```

### 分布式锁配置

```go
// 使用自定义前缀创建分布式锁
lock := scheduler.NewRedisLock(redisClient, logger, "my-app:scheduler:lock:")

// 使用默认前缀（scheduler:lock:）
lock := scheduler.NewRedisLock(redisClient, logger, "")
```

### Prometheus 指标

```go
// 设置全局 namespace（必须在创建 PrometheusMetrics 之前调用）
scheduler.SetNamespace("my_app")

// 创建 Prometheus 指标记录器
metrics := scheduler.NewPrometheusMetrics("")

// 或者直接指定 namespace
metrics := scheduler.NewPrometheusMetrics("my_app")
```

#### 可用指标

| 指标名称 | 类型 | 说明 |
|---------|------|------|
| `{namespace}_scheduler_task_execution_total` | Counter | 任务执行总数（按任务和状态分组） |
| `{namespace}_scheduler_task_execution_duration_seconds` | Histogram | 任务执行耗时（秒） |
| `{namespace}_scheduler_task_last_execution_timestamp` | Gauge | 上次任务执行时间戳 |
| `{namespace}_scheduler_task_queue_size` | Gauge | 任务队列大小 |
| `{namespace}_scheduler_lock_acquisition_total` | Counter | 锁获取次数（按任务和结果分组） |
| `{namespace}_scheduler_active_tasks` | Gauge | 当前活跃任务数 |

### 任务注册与初始化

```go
// 方式1：直接注册
manager := scheduler.NewSchedulerManager(config, lock, metrics, logger)
manager.RegisterTask(task1)
manager.RegisterTask(task2)

// 方式2：使用 TaskRegistry
registry := scheduler.NewTaskRegistry(manager, logger)
registry.RegisterTask(task1)
registry.RegisterAll(task2, task3, task4)

// 方式3：使用初始化函数（推荐）
scheduler.InitializeScheduler(ctx, manager, logger)
```

### 手动触发任务

```go
// 手动触发任务执行
if err := manager.TriggerTask(ctx, "my-task"); err != nil {
    logger.Error("触发任务失败", zap.Error(err))
}
```

### 查询任务状态

```go
// 获取单个任务状态
status, err := manager.GetTaskStatus("my-task")
if err != nil {
    logger.Error("获取任务状态失败", zap.Error(err))
} else {
    fmt.Printf("成功次数: %d, 失败次数: %d\n", status.SuccessCount, status.FailedCount)
}

// 获取所有任务状态
allStatus := manager.GetAllTaskStatus()
for name, status := range allStatus {
    fmt.Printf("任务 %s: 成功=%d, 失败=%d\n", name, status.SuccessCount, status.FailedCount)
}
```

## 配置说明

### 调度器配置

```go
type Config struct {
    Enabled        bool          // 是否启用调度器
    WorkerPoolSize int           // 工作池大小（当前版本保留字段）
    LockTTL        time.Duration // 默认锁 TTL
    MetricsEnabled bool          // 是否启用指标
}
```

### 任务配置

```go
type ScheduleConfig struct {
    Type       ScheduleType   // 调度类型：cron 或 interval
    CronExpr   string         // Cron 表达式（Type=cron 时使用）
    Interval   time.Duration  // 固定间隔（Type=interval 时使用）
    Enabled    bool           // 是否启用
    RunOnStart bool           // 启动时是否立即执行一次
}

type RetryPolicy struct {
    MaxRetries      int           // 最大重试次数
    InitialInterval time.Duration // 初始重试间隔
    MaxInterval     time.Duration // 最大重试间隔
    Multiplier      float64       // 退避乘数
}
```

### 重试策略

默认重试策略：
- 最大重试次数：3
- 初始间隔：1 秒
- 最大间隔：30 秒
- 退避乘数：2.0（指数退避）

```go
// 自定义重试策略
task := NewMyTask()
task.RetryPolicy = scheduler.RetryPolicy{
    MaxRetries:      5,
    InitialInterval: 2 * time.Second,
    MaxInterval:     1 * time.Minute,
    Multiplier:      1.5,
}
```

## API 文档

### 核心接口

```go
// Task 任务接口
type Task interface {
    Name() string
    Description() string
    Schedule() ScheduleConfig
    Execute(ctx context.Context) error
    Timeout() time.Duration
    RetryPolicy() RetryPolicy
    RequireLock() bool
    LockTTL() time.Duration
}

// DistributedLock 分布式锁接口
type DistributedLock interface {
    Acquire(ctx context.Context, key string, ttl time.Duration) (string, bool, error)
    Release(ctx context.Context, key string, lockValue string) error
    Extend(ctx context.Context, key string, lockValue string, ttl time.Duration) error
    IsHeld(ctx context.Context, key string) (bool, error)
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
```

### SchedulerManager

```go
// 创建调度管理器
func NewSchedulerManager(config Config, lock DistributedLock, metrics MetricsRecorder, logger *zap.Logger) *SchedulerManager

// 注册任务
func (m *SchedulerManager) RegisterTask(task Task) error

// 启动调度器
func (m *SchedulerManager) Start(ctx context.Context) error

// 停止调度器
func (m *SchedulerManager) Stop(ctx context.Context) error

// 手动触发任务
func (m *SchedulerManager) TriggerTask(ctx context.Context, taskName string) error

// 获取任务状态
func (m *SchedulerManager) GetTaskStatus(taskName string) (*TaskStatus, error)
func (m *SchedulerManager) GetAllTaskStatus() map[string]*TaskStatus

// 检查调度器是否运行中
func (m *SchedulerManager) IsRunning() bool
```

### TaskStatus

```go
type TaskStatus struct {
    Name         string
    LastRunTime  *time.Time
    LastDuration time.Duration
    SuccessCount int64
    FailedCount  int64
    IsRunning    bool
    LastErr      error
}
```

## 架构设计

### 设计原则

1. **接口驱动**：通过 Task、DistributedLock、MetricsRecorder 接口实现松耦合
2. **可替换性**：所有核心组件都可以通过接口替换实现
3. **可观测性**：内置 Prometheus 指标，便于监控告警
4. **分布式友好**：通过 Redis 分布式锁支持多实例部署

### 组件架构

```
┌─────────────────────────────────────────────────────────┐
│                   go-scheduler 架构                      │
├─────────────────────────────────────────────────────────┤
│                                                         │
│  ┌─────────────────┐                                    │
│  │  TaskRegistry   │                                    │
│  │  (任务注册)     │                                    │
│  └────────┬────────┘                                    │
│           │                                             │
│           ▼                                             │
│  ┌─────────────────┐     ┌──────────────────┐         │
│  │SchedulerManager │────►│  TaskExecutor    │         │
│  │  (调度管理)     │     │  (任务执行)      │         │
│  └────────┬────────┘     └────────┬─────────┘         │
│           │                       │                     │
│           ▼                       ▼                     │
│  ┌─────────────────┐     ┌──────────────────┐         │
│  │   cron.Cron     │     │ DistributedLock  │         │
│  │  (Cron 调度)    │     │  (分布式锁)      │         │
│  └─────────────────┘     └──────────────────┘         │
│                                                         │
│  ┌─────────────────┐     ┌──────────────────┐         │
│  │ MetricsRecorder │     │   BaseTask       │         │
│  │  (指标记录)     │     │  (任务基类)      │         │
│  └─────────────────┘     └──────────────────┘         │
│                                                         │
└─────────────────────────────────────────────────────────┘
```

### 依赖关系

```
go-scheduler
├── github.com/robfig/cron/v3          # Cron 调度引擎
├── github.com/redis/go-redis/v9        # Redis 客户端（分布式锁）
├── github.com/google/uuid             # UUID 生成（锁标识）
├── github.com/prometheus/client_golang # Prometheus 指标
└── go.uber.org/zap                    # 结构化日志
```

## 常见问题

### Q: 如何确保任务在集群中只执行一次？

A: 默认情况下，所有任务都会使用分布式锁。确保所有实例使用同一个 Redis 实例，并且任务名称相同。锁的 key 格式为 `{prefix}task:{task-name}`。

### Q: 如何处理长时间运行的任务？

A: 可以为任务配置更长的超时时间和锁 TTL：

```go
task := NewMyTask()
task.SetTimeout(1 * time.Hour)
task.LockTTL = 2 * time.Hour
```

### Q: 如何禁用某个任务？

A: 在 ScheduleConfig 中设置 Enabled 为 false：

```go
scheduler.ScheduleConfig{
    Type:    scheduler.ScheduleTypeCron,
    CronExpr: "0 * * * * *",
    Enabled: false, // 禁用任务
}
```

### Q: 如何测试任务逻辑？

A: 直接调用任务的 Execute 方法进行测试：

```go
func TestMyTask(t *testing.T) {
    task := NewMyTask()
    ctx := context.Background()
    
    err := task.Execute(ctx)
    assert.NoError(t, err)
}
```

### Q: 如何自定义分布式锁实现？

A: 实现 DistributedLock 接口：

```go
type MyLock struct{}

func (l *MyLock) Acquire(ctx context.Context, key string, ttl time.Duration) (string, bool, error) {
    // 自定义获取锁逻辑
}

func (l *MyLock) Release(ctx context.Context, key string, lockValue string) error {
    // 自定义释放锁逻辑
}

// 其他方法...
```

## 贡献

欢迎贡献！请查看 [CONTRIBUTING.md](CONTRIBUTING.md) 了解详情。

## 变更日志

查看 [CHANGELOG.md](CHANGELOG.md) 了解版本变更历史。

## 许可证

本项目采用 MIT 许可证，详见 [LICENSE](LICENSE) 文件。

## 致谢

- [robfig/cron](https://github.com/robfig/cron) - 强大的 Go Cron 调度库
- [go-redis](https://github.com/redis/go-redis) - Redis 客户端
- [Prometheus](https://prometheus.io/) - 强大的监控和告警系统
