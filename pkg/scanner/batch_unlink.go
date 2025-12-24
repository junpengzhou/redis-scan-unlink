package scanner

import (
	"context"
	"fmt"
	"log"
	"runtime"
	"sync"
	"sync/atomic"
	"time"

	"github.com/go-redis/redis/v8"
)

// 默认配置
const (
	DefaultBatchSize = 1000
	DefaultScanCount = 1000
	DefaultQueueSize = 10000
	DefaultTimeout   = 30 * time.Minute
	MaxBatchSize     = 10000
	ProgressInterval = 5 * time.Second
)

// Config 扫描器配置
type Config struct {
	RedisClient  *redis.Client
	Pattern      string
	BatchSize    int
	ScanCount    int
	MaxWorkers   int
	QueueSize    int
	Timeout      time.Duration
	ShowProgress bool
	Async        bool
	Database     int
}

// BatchUnlinkResult 批量删除结果
type BatchUnlinkResult struct {
	Pattern      string
	TotalDeleted int64
	TotalKeys    int64
	Duration     time.Duration
	StartTime    time.Time
	EndTime      time.Time
	Errors       []error
	Success      bool
}

// RedisBatchScanner Redis 批量扫描器
type RedisBatchScanner struct {
	config         Config
	ctx            context.Context
	cancelFunc     context.CancelFunc
	resultChan     chan *BatchUnlinkResult
	errorChan      chan error
	wg             sync.WaitGroup
	isRunning      atomic.Bool
	totalDeleted   atomic.Int64
	totalKeys      atomic.Int64
	startTime      time.Time
	progressTicker *time.Ticker
}

// NewRedisBatchScanner 创建新的 Redis 批量扫描器
func NewRedisBatchScanner(config Config) *RedisBatchScanner {
	// 设置默认值
	if config.BatchSize <= 0 || config.BatchSize > MaxBatchSize {
		config.BatchSize = DefaultBatchSize
	}
	if config.ScanCount <= 0 {
		config.ScanCount = DefaultScanCount
	}
	if config.MaxWorkers <= 0 {
		config.MaxWorkers = runtime.NumCPU()
		if config.MaxWorkers < 2 {
			config.MaxWorkers = 2
		}
	}
	if config.QueueSize <= 0 {
		config.QueueSize = DefaultQueueSize
	}
	if config.Timeout <= 0 {
		config.Timeout = DefaultTimeout
	}

	ctx, cancel := context.WithTimeout(context.Background(), config.Timeout)

	return &RedisBatchScanner{
		config:     config,
		ctx:        ctx,
		cancelFunc: cancel,
		resultChan: make(chan *BatchUnlinkResult, 1),
		errorChan:  make(chan error, 100),
		startTime:  time.Now(),
	}
}

// BatchUnlink 执行批量的 Unlink 动作
func (s *RedisBatchScanner) BatchUnlink() (*BatchUnlinkResult, error) {
	if s.isRunning.Load() {
		return nil, fmt.Errorf("scanner is already running")
	}

	s.isRunning.Store(true)
	defer s.isRunning.Store(false)

	// 重置统计
	s.totalDeleted.Store(0)
	s.totalKeys.Store(0)
	s.startTime = time.Now()

	log.Printf("开始 Scan 并 Unlink: 模式=%s, 批次大小=%d, 工作线程=%d",
		s.config.Pattern, s.config.BatchSize, s.config.MaxWorkers)

	// 启动进度监控
	if s.config.ShowProgress {
		s.startProgressMonitor()
	}

	// 选择数据库
	if s.config.Database >= 0 {
		s.config.RedisClient = s.config.RedisClient.WithContext(s.ctx)
		// 注意：go-redis 不支持动态切换数据库，通常需要在连接时指定
		// 这里假设连接时已经指定了数据库
	}

	// 根据是否异步执行
	if s.config.Async {
		go s.DoBatchUnlink()
		return &BatchUnlinkResult{
			Pattern:   s.config.Pattern,
			StartTime: s.startTime,
			Success:   true,
		}, nil
	}

	return s.DoBatchUnlink(), nil
}

// DoBatchUnlink 执行实际的批量删除
func (s *RedisBatchScanner) DoBatchUnlink() *BatchUnlinkResult {
	defer func() {
		if s.progressTicker != nil {
			s.progressTicker.Stop()
		}
		s.cancelFunc()
	}()

	var resultErrors []error

	// 创建工作池
	jobChan := make(chan []string, s.config.QueueSize)
	errorChan := make(chan error, 100)

	// 启动工作线程
	for i := 0; i < s.config.MaxWorkers; i++ {
		s.wg.Add(1)
		go s.worker(i, jobChan, errorChan)
	}

	// 启动错误收集器
	go s.errorCollector(errorChan, &resultErrors)

	// 执行扫描
	if err := s.scanAndProcess(jobChan); err != nil {
		resultErrors = append(resultErrors, err)
	}

	// 等待所有工作完成
	close(jobChan)
	s.wg.Wait()
	close(errorChan)

	// 收集剩余的错误
	for err := range errorChan {
		resultErrors = append(resultErrors, err)
	}

	endTime := time.Now()
	duration := endTime.Sub(s.startTime)

	result := &BatchUnlinkResult{
		Pattern:      s.config.Pattern,
		TotalDeleted: s.totalDeleted.Load(),
		TotalKeys:    s.totalKeys.Load(),
		Duration:     duration,
		StartTime:    s.startTime,
		EndTime:      endTime,
		Errors:       resultErrors,
		Success:      len(resultErrors) == 0,
	}

	log.Printf("Scan 删除完成: 模式=%s, 删除数量=%d, 耗时=%v",
		s.config.Pattern, s.totalDeleted.Load(), duration)

	return result
}

// scanAndProcess 扫描并处理键
func (s *RedisBatchScanner) scanAndProcess(jobChan chan<- []string) error {
	var cursor uint64
	var batch []string

	for {
		select {
		case <-s.ctx.Done():
			return s.ctx.Err()
		default:
			// 执行 SCAN
			var keys []string
			var err error
			keys, cursor, err = s.config.RedisClient.Scan(s.ctx, cursor, s.config.Pattern, int64(s.config.ScanCount)).Result()

			if err != nil {
				return fmt.Errorf("scan 失败: %w", err)
			}

			// 添加到批次
			for _, key := range keys {
				batch = append(batch, key)
				s.totalKeys.Add(1)

				// 批次达到大小时发送到工作队列
				if len(batch) >= s.config.BatchSize {
					select {
					case jobChan <- batch:
						batch = make([]string, 0, s.config.BatchSize)
					case <-s.ctx.Done():
						return s.ctx.Err()
					}
				}
			}

			// 扫描完成
			if cursor == 0 {
				// 发送剩余的批次
				if len(batch) > 0 {
					select {
					case jobChan <- batch:
					case <-s.ctx.Done():
						return s.ctx.Err()
					}
				}
				return nil
			}
		}
	}
}

// worker 工作线程
func (s *RedisBatchScanner) worker(id int, jobChan <-chan []string, errorChan chan<- error) {
	defer s.wg.Done()

	for batch := range jobChan {
		select {
		case <-s.ctx.Done():
			return
		default:
			if err := s.processBatch(batch); err != nil {
				select {
				case errorChan <- fmt.Errorf("worker %d: %w", id, err):
				case <-s.ctx.Done():
					return
				}
			}
		}
	}
}

// processBatch 处理批次删除
func (s *RedisBatchScanner) processBatch(keys []string) error {
	if len(keys) == 0 {
		return nil
	}

	// 执行 UNLINK
	result, _ := s.config.RedisClient.Unlink(s.ctx, keys...).Result()

	deleted := result
	s.totalDeleted.Add(deleted)

	return nil
}

// errorCollector 错误收集器
func (s *RedisBatchScanner) errorCollector(errorChan <-chan error, errorList *[]error) {
	for err := range errorChan {
		*errorList = append(*errorList, err)
		log.Printf("错误: %v", err)
	}
}

// startProgressMonitor 启动进度监控
func (s *RedisBatchScanner) startProgressMonitor() {
	s.progressTicker = time.NewTicker(ProgressInterval)

	go func() {
		for {
			select {
			case <-s.progressTicker.C:
				s.logProgress()
			case <-s.ctx.Done():
				return
			}
		}
	}()
}

// logProgress 记录进度
func (s *RedisBatchScanner) logProgress() {
	deleted := s.totalDeleted.Load()
	keys := s.totalKeys.Load()
	duration := time.Since(s.startTime)

	if keys > 0 {
		progress := float64(deleted) / float64(keys) * 100
		log.Printf("进度: 已扫描 %d, 已删除 %d (%.1f%%), 耗时: %v",
			keys, deleted, progress, duration)
	} else {
		log.Printf("进度: 已删除 %d, 耗时: %v", deleted, duration)
	}
}

// Stop 停止扫描器
func (s *RedisBatchScanner) Stop() {
	s.cancelFunc()
	s.wg.Wait()

	if s.progressTicker != nil {
		s.progressTicker.Stop()
	}

	close(s.resultChan)
	close(s.errorChan)
}

// GetResultChannel 获取结果通道
func (s *RedisBatchScanner) GetResultChannel() <-chan *BatchUnlinkResult {
	return s.resultChan
}

// GetErrorChannel 获取错误通道
func (s *RedisBatchScanner) GetErrorChannel() <-chan error {
	return s.errorChan
}

// IsRunning 检查是否在运行
func (s *RedisBatchScanner) IsRunning() bool {
	return s.isRunning.Load()
}
