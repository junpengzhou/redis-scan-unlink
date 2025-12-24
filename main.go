package main

import (
	"context"
	"fmt"
	"log"
	"time"

	"redis-scan-unlink/pkg/redisclient"
	"redis-scan-unlink/pkg/scanner"
)

func main() {
	// 创建 Redis 客户端
	client, err := redisclient.NewRedisClient(&redisclient.RedisConfig{
		Addr:     "localhost:6379",
		Password: "",
		DB:       0,
	})

	if err != nil {
		log.Fatal(err)
	}

	defer func() {
		if client != nil {
			_ = client.Close()
		}
	}()

	// 测试连接
	ctx := context.Background()
	if err := client.Ping(ctx).Err(); err != nil {
		log.Fatal(err)
	}
	fmt.Println("Redis 连接成功")

	// 创建扫描器
	scannerConfig := scanner.Config{
		RedisClient:  client,
		Pattern:      "user:session:*",
		BatchSize:    1000,
		ScanCount:    1000,
		MaxWorkers:   4,
		Timeout:      30 * time.Minute,
		ShowProgress: true,
		Async:        false,
		Database:     0,
	}

	redisScanner := scanner.NewRedisBatchScanner(scannerConfig)

	// 执行批量删除
	fmt.Println("开始批量删除...")
	result := redisScanner.DoBatchUnlink()

	// 打印结果
	fmt.Printf("删除完成: 模式=%s, 删除数量=%d, 耗时=%v\n",
		result.Pattern, result.TotalDeleted, result.Duration)
}
