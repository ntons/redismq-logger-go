// 记录器

package redismq

import (
	"time"

	"github.com/go-redis/redis"
)

type Logger struct {
	// redis client
	Key string
	// redis client
	atom atom
}

// equivalence to new(Logger) and Logger.Dial
func NewLogger(key string, c *redis.Client) (l *Logger) {
	l = &Logger{Key: key}
	l.atom.swap(c)
	return
}

// dial to redis server
// multiple dialing will close previous dialed connections
func (l *Logger) Dial(options *redis.Options) (err error) {
	c := redis.NewClient(options)
	if _, err = c.Ping().Result(); err != nil {
		return
	}
	l.dispose(l.atom.swap(c))
	return
}

// replace underlying redis connections, give previous client back
func (l *Logger) Redirect(c *redis.Client) (err error) {
	l.dispose(l.atom.swap(c))
	return
}

func (l *Logger) Write(b []byte) (n int, err error) {
	c := l.atom.get()
	if c == nil {
		return 0, ErrClosed
	}
	if _, err = c.LPush(l.Key, b).Result(); err != nil {
		return
	}
	return len(b), nil
}

func (l *Logger) Sync() error {
	return nil
}

func (l *Logger) Close() error {
	l.dispose(l.atom.swap(nil))
	return nil
}

// delay closing redis client after a reasonable duration
func (l *Logger) dispose(c *redis.Client) {
	if c == nil {
		return
	}
	if d := c.Options().WriteTimeout; d > 0 {
		time.AfterFunc(d+time.Second, func() { c.Close() })
	} else {
		time.AfterFunc(time.Minute, func() { c.Close() })
	}
}
