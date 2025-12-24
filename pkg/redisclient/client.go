package redisclient

import (
	"context"
	"fmt"
	"log"
	"time"

	"github.com/go-redis/redis/v8"
)

// RedisConfig Redis 配置
type RedisConfig struct {
	Addr               string
	Password           string
	DB                 int
	PoolSize           int
	MinIdleConnections int
	MaxRetries         int
	DialTimeout        time.Duration
	ReadTimeout        time.Duration
	WriteTimeout       time.Duration
	PoolTimeout        time.Duration
	IdleTimeout        time.Duration
}

// DefaultConfig 默认配置
func DefaultConfig() *RedisConfig {
	return &RedisConfig{
		Addr:               "localhost:6379",
		Password:           "",
		DB:                 0,
		PoolSize:           100,
		MinIdleConnections: 10,
		MaxRetries:         3,
		DialTimeout:        5 * time.Second,
		ReadTimeout:        3 * time.Second,
		WriteTimeout:       3 * time.Second,
		PoolTimeout:        4 * time.Second,
		IdleTimeout:        5 * time.Minute,
	}
}

// NewRedisClient 创建新的 Redis 客户端
func NewRedisClient(config *RedisConfig) (*redis.Client, error) {
	if config == nil {
		config = DefaultConfig()
	}

	client := redis.NewClient(&redis.Options{
		Addr:         config.Addr,
		Password:     config.Password,
		DB:           config.DB,
		PoolSize:     config.PoolSize,
		MinIdleConns: config.MinIdleConnections,
		MaxRetries:   config.MaxRetries,
		DialTimeout:  config.DialTimeout,
		ReadTimeout:  config.ReadTimeout,
		WriteTimeout: config.WriteTimeout,
		PoolTimeout:  config.PoolTimeout,
		IdleTimeout:  config.IdleTimeout,
	})

	// 确保在出错时关闭客户端
	defer func() {
		if client != nil {
			_ = client.Close()
		}
	}()

	// 测试连接
	ctx, cancel := context.WithTimeout(context.Background(), config.DialTimeout)
	defer cancel()

	if err := client.Ping(ctx).Err(); err != nil {
		return nil, fmt.Errorf("failed to connect to Redis: %w", err)
	}

	log.Printf("Redis 连接成功: %s, DB: %d", config.Addr, config.DB)
	return client, nil
}

// 创建 Redis 集群客户端
func _(addressArray []string, password string) (*redis.ClusterClient, error) {
	client := redis.NewClusterClient(&redis.ClusterOptions{
		Addrs:    addressArray,
		Password: password,
		PoolSize: 100,
	})

	// 确保在出错时关闭客户端
	defer func() {
		if client != nil {
			// 如果需要，可以记录关闭操作
			_ = client.Close()
		}
	}()

	// 测试连接
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	if err := client.Ping(ctx).Err(); err != nil {
		return nil, fmt.Errorf("failed to connect to Redis Cluster: %w", err)
	}

	log.Printf("Redis 集群连接成功: %v", addressArray)
	return client, nil
}
