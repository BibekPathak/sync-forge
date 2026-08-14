package cache

import (
	"context"
	"time"

	"github.com/redis/go-redis/v9"
)

// Cache wraps a Redis client. Used for distributed rate limiting and metadata
// caching. Critical event durability lives in PostgreSQL, not here.
type Cache struct {
	Client *redis.Client
}

func New(addr string) *Cache {
	return &Cache{
		Client: redis.NewClient(&redis.Options{
			Addr:         addr,
			DialTimeout:  5 * time.Second,
			ReadTimeout:  3 * time.Second,
			WriteTimeout: 3 * time.Second,
		}),
	}
}

func (c *Cache) Ping(ctx context.Context) error {
	return c.Client.Ping(ctx).Err()
}

func (c *Cache) Close() error {
	return c.Client.Close()
}
