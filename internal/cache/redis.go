package cache

import (
	"context"
	"time"

	"github.com/go-redis/redis/v8"
)

type Cache struct {
	client *redis.Client
	ttl    time.Duration
	ctx    context.Context
}

func New(addr, pass string, db int, ttlSeconds int) *Cache {
	rdb := redis.NewClient(&redis.Options{
		Addr:     addr,
		Password: pass,
		DB:       db,
	})

	return &Cache{
		client: rdb,
		ttl:    time.Duration(ttlSeconds) * time.Second,
		ctx:    context.Background(),
	}
}

func (c *Cache) Get(key string) (string, error) {
	return c.client.Get(c.ctx, key).Result()
}

func (c *Cache) Set(key string, value string) error {
	return c.client.SetEX(c.ctx, key, value, c.ttl).Err()
}

func (c *Cache) Close() error {
	return c.client.Close()
}
