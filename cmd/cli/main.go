package main

import (
	"flag"
	"fmt"
	"log"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"redis-scan-unlink/pkg/redisclient"
	"redis-scan-unlink/pkg/scanner"
)

func main() {
	// 解析命令行参数
	pattern := flag.String("pattern", "", "Redis 键模式 (必填)")
	batchSize := flag.Int("batch", 1000, "批次大小")
	scanCount := flag.Int("scan", 1000, "每次扫描数量")
	workers := flag.Int("workers", 4, "工作线程数")
	timeout := flag.String("timeout", "30m", "超时时间")
	async := flag.Bool("async", false, "是否异步执行")
	showProgress := flag.Bool("progress", true, "显示进度")
	db := flag.Int("db", 0, "Redis 数据库")
	addr := flag.String("addr", "localhost:6379", "Redis 地址")
	password := flag.String("password", "", "Redis 密码")

	flag.Parse()

	if *pattern == "" {
		log.Fatal("请使用 -pattern 参数指定键模式")
	}

	// 解析超时时间
	timeoutDuration, err := time.ParseDuration(*timeout)
	if err != nil {
		log.Fatalf("无效的超时时间格式: %v", err)
	}

	// 创建 Redis 客户端
	redisConfig := &redisclient.RedisConfig{
		Addr:     *addr,
		Password: *password,
		DB:       *db,
	}

	client, err := redisclient.NewRedisClient(redisConfig)
	if err != nil {
		log.Fatalf("创建 Redis 客户端失败: %v", err)
	}

	// 先定义一下等等 finally 要回收的资源
	defer func() {
		if closeErr := client.Close(); closeErr != nil {
			log.Printf("关闭客户端时发生错误: %v", closeErr)
		}
	}()

	// 创建扫描器
	scannerConfig := scanner.Config{
		RedisClient:  client,
		Pattern:      *pattern,
		BatchSize:    *batchSize,
		ScanCount:    *scanCount,
		MaxWorkers:   *workers,
		Timeout:      timeoutDuration,
		ShowProgress: *showProgress,
		Async:        *async,
		Database:     *db,
	}

	redisScanner := scanner.NewRedisBatchScanner(scannerConfig)

	// 设置信号处理
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)

	go func() {
		<-sigChan
		log.Println("收到停止信号，正在停止...")
		redisScanner.Stop()
	}()

	// 执行批量删除
	if *async {
		log.Println("开始异步批量删除...")
		result, err := redisScanner.BatchUnlink()
		if err != nil {
			log.Fatalf("启动异步删除失败: %v", err)
		}

		log.Printf("异步任务已启动，开始时间: %v", result.StartTime.Format("2006-01-02 15:04:05"))

		// 等待结果
		select {
		case result := <-redisScanner.GetResultChannel():
			printResult(result)
		case <-time.After(timeoutDuration):
			log.Println("操作超时")
		}
	} else {
		log.Println("开始同步批量删除...")
		result := redisScanner.DoBatchUnlink()
		printResult(result)
	}

	log.Println("程序结束")
}

// printResult 打印结果
func printResult(result *scanner.BatchUnlinkResult) {
	// 创建分隔符
	separator := strings.Repeat("=", 50)
	fmt.Println("\n" + separator)
	fmt.Println("批量删除结果:")
	fmt.Println(separator)
	fmt.Printf("模式: %s\n", result.Pattern)
	fmt.Printf("开始时间: %s\n", result.StartTime.Format("2006-01-02 15:04:05"))
	fmt.Printf("结束时间: %s\n", result.EndTime.Format("2006-01-02 15:04:05"))
	fmt.Printf("耗时: %v\n", result.Duration)
	fmt.Printf("扫描键数: %d\n", result.TotalKeys)
	fmt.Printf("删除键数: %d\n", result.TotalDeleted)
	fmt.Printf("成功: %v\n", result.Success)

	if len(result.Errors) > 0 {
		fmt.Printf("错误数: %d\n", len(result.Errors))
		for i, err := range result.Errors {
			fmt.Printf("  错误 %d: %v\n", i+1, err)
		}
	}
	fmt.Println(separator)
}
