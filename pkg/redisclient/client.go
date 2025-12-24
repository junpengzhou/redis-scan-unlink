package redisclient

import (
	"context"
	"crypto/tls"
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
	MaxConnAge         time.Duration
	Username           string
	TLSConfig          *tls.Config
	Network            string
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
		MaxConnAge:         0,
		Network:            "tcp",
		TLSConfig:          nil,
	}
}

// NewRedisClient 创建新的 Redis 客户端
func NewRedisClient(config *RedisConfig) (*redis.Client, error) {
	if config == nil {
		config = DefaultConfig()
	}
	log.Printf("尝试连接 Redis: %s, DB: %d", config.Addr, config.DB)

	opts := &redis.Options{
		Addr:         config.Addr,
		Password:     config.Password,
		DB:           config.DB,
		DialTimeout:  5 * time.Second,
		ReadTimeout:  5 * time.Second,
		WriteTimeout: 5 * time.Second,
		PoolSize:     50,
		OnConnect: func(ctx context.Context, cn *redis.Conn) error {
			log.Printf("Redis 连接已建立")
			return nil
		},
	}

	client := redis.NewClient(opts)

	// 测试连接
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	if err := client.Ping(ctx).Err(); err != nil {
		// 连接测试失败时关闭客户端
		_ = client.Close()
		return nil, fmt.Errorf("failed to connect to Redis: %w", err)
	}

	log.Printf("Redis 连接成功: %s, DB: %d", config.Addr, config.DB)
	return client, nil
}
