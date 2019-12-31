// 记录器

package redismq

import (
	"sync/atomic"

	"github.com/go-redis/redis"
)

type Logger struct {
	// redis client
	c *redis.Client
	//
	key string
	//
	closed int32
}

func NewLogger(c *redis.Client, key string) (l *Logger) {
	return &Logger{c: c, key: key}
}

// get underlying redis client
func (l *Logger) Client() *redis.Client {
	return l.c
}

func (l *Logger) Write(b []byte) (n int, err error) {
	if atomic.LoadInt32(&l.closed) != 0 {
		return 0, ErrClosed
	}
	if _, err = l.c.LPush(l.key, b).Result(); err != nil {
		return
	}
	return len(b), nil
}

func (l *Logger) Sync() error {
	return nil
}

func (l *Logger) Close() error {
	if !atomic.CompareAndSwapInt32(&l.closed, 0, 1) {
		return nil
	}
	return nil
}
