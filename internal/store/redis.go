package store

import (
	"context"
	"fmt"
	"multiple-protocol-controller/pkg/logger"
	"time"

	"github.com/redis/go-redis/v9"
	"go.uber.org/zap"
)

type RedisClient struct {
	rdb *redis.Client
}

// 初始化 Redis 客户端
func NewRedisClient(ctx context.Context, addr, password string, db int) (*RedisClient, error) {
	rdb := redis.NewClient(&redis.Options{
		Addr:     addr,
		Password: password,
		DB:       db,
	})
	redisClient := &RedisClient{rdb: rdb}

	if err := redisClient.TestConnection(ctx); err != nil {
		return nil, err
	}
	return redisClient, nil
}

// 封装带超时的 GET
func (c *RedisClient) GetWithTimeout(key string, timeout time.Duration) (string, error) {
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()

	return c.rdb.Get(ctx, key).Result()
}

// 封装带超时的 SET
func (c *RedisClient) SetWithTimeout(key string, value interface{}, expiration time.Duration, timeout time.Duration) error {
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()

	return c.rdb.Set(ctx, key, value, expiration).Err()
}

// Set writes a value with optional expiration using the provided context (or background if nil).
func (c *RedisClient) Set(ctx context.Context, key string, value interface{}, expiration time.Duration) error {
	if ctx == nil {
		ctx = context.Background()
	}
	return c.rdb.Set(ctx, key, value, expiration).Err()
}

// Delete removes the specified key using the provided context (or background if nil).
func (c *RedisClient) Delete(ctx context.Context, key string) error {
	if ctx == nil {
		ctx = context.Background()
	}
	return c.rdb.Del(ctx, key).Err()
}

// Publish 将消息写入指定频道，可选超时控制
func (c *RedisClient) Publish(ctx context.Context, channel string, payload interface{}) error {
	if ctx == nil {
		ctx = context.Background()
	}
	return c.rdb.Publish(ctx, channel, payload).Err()
}

// Close shuts down the underlying Redis client to release resources.
func (c *RedisClient) TestConnection(ctx context.Context) error {
	if err := c.rdb.Ping(ctx).Err(); err != nil {
		return fmt.Errorf("redis init failed: %w", err)
	}
	return nil
}

// Close shuts down the underlying Redis client to release resources.
func (c *RedisClient) Close() error {
	if c == nil || c.rdb == nil {
		return nil
	}
	return c.rdb.Close()
}

func (c *RedisClient) SubscribeWithRetry(ctx context.Context, channels []string, handlers map[string]func(msg *redis.Message) error) <-chan error {
	errCh := make(chan error, 1)

	go func() {
		defer close(errCh)
		backoff := 5 * time.Second
		const maxBackoff = time.Minute

		for {
			pubsub := c.rdb.Subscribe(ctx, channels...)

			if _, err := pubsub.Receive(ctx); err != nil {
				logger.Log.Error("subscribe failed", zap.Error(err))
				_ = pubsub.Close()
				continue
			}
			ch := pubsub.Channel()

			logger.Log.Info("subscribed to", zap.Strings("channels", channels))

			// 订阅成功后重置退避时间
			backoff = 5 * time.Second

		consumeLoop:
			for {
				select {
				case <-ctx.Done():
					_ = pubsub.Close()
					logger.Log.Warn("SubscribeWithRetry closed by context")
					return
				case msg, ok := <-ch:
					if !ok {
						_ = pubsub.Close()
						logger.Log.Warn("subscription lost, retrying...", zap.Duration("backoff", backoff))
						break consumeLoop
					}
					// handler(msg)
					if handler, exists := handlers[msg.Channel]; exists {
						err := handler(msg)
						if err != nil {
							errCh <- err
							return
						}
					} else {
						logger.Log.Warn("no handler for channel", zap.String("channel", msg.Channel))
					}
				}
			}

			// 3) 重试前的可取消休眠 + 指数退避
			if !sleepWithContext(ctx, backoff) {
				return
			}
			if backoff < maxBackoff {
				backoff *= 2
			}
		}
	}()

	return errCh
}

// 带 ctx 的可取消 sleep，返回 false 表示 ctx 已取消
func sleepWithContext(ctx context.Context, d time.Duration) bool {
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-ctx.Done():
		return false
	case <-t.C:
		return true
	}
}
