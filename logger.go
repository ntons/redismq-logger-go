// 记录器

package redismq

import (
	"sync"

	"github.com/go-redis/redis"
)

type Logger struct {
	Key string

	cli *redis.Client
	mu  sync.RWMutex
}

func NewLogger(key string) *Logger {
	return &Logger{Key: key}
}

func (l *Logger) Dial(options *redis.Options) (err error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.cli != nil {
		return ErrDialed
	}
	cli := redis.NewClient(options)
	if _, err = cli.Ping().Result(); err != nil {
		return
	}
	if l.cli != nil {
		l.cli.Close()
	}
	l.cli = cli
	return
}

func (l *Logger) Write(p []byte) (n int, err error) {
	l.mu.RLock()
	defer l.mu.RUnlock()
	if l.cli == nil {
		return 0, ErrClosed
	}
	if _, err = l.cli.LPush(l.Key, p).Result(); err != nil {
		return
	}
	return len(p), nil
}

func (l *Logger) Sync() error {
	return nil
}

func (l *Logger) Close() error {
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.cli == nil {
		return ErrClosed
	}
	cli := l.cli
	l.cli = nil
	if cli != nil {
		return cli.Close()
	}
	return nil
}
