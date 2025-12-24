package main

import (
	"fmt"
	"log"
	"redis-scan-unlink/pkg/redisclient"
)

func main() {
	fmt.Println("开始测试 Redis 连接...")

	// 测试不同的连接方式
	testCases := []struct {
		name     string
		addr     string
		password string
	}{
		{"8.217.97.105:6379", "8.217.97.105:6379", "Wejoinfx!@#135246"},
	}

	for _, tc := range testCases {
		fmt.Printf("\n测试: %s\n", tc.name)
		if err := testConnection(tc.addr, tc.password); err != nil {
			fmt.Printf("失败: %v\n", err)
		} else {
			fmt.Printf("成功\n")
		}
	}
}

func testConnection(addr, password string) error {
	// 创建 Redis 客户端
	redisConfig := &redisclient.RedisConfig{
		Addr:     addr,
		Password: password,
		DB:       0,
	}

	client, err := redisclient.NewRedisClient(redisConfig)
	if err != nil {
		log.Fatalf("创建 Redis 客户端失败: %v", err)
	}

	defer func() {
		if client != nil {
			_ = client.Close()
		}
	}()
	return err
}
