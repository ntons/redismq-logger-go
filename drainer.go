// 汲取器

package redismq

import (
	"io"
	"sync"
	"sync/atomic"
	"time"

	"github.com/go-redis/redis"
)

type DrainWork struct {
	Key     string
	Writer  io.Writer
	OnError func(error) error
	//
	cli    *redis.Client
	closed int32
}

func (w *DrainWork) serve() (err error) {
	for {
		if atomic.LoadInt32(&w.closed) != 0 {
			return nil
		}
		var data []string
		if data, err = w.cli.BRPop(time.Second, w.Key).Result(); err != nil {
			if w.OnError == nil {
				return
			}
			if err = w.OnError(err); err != nil {
				return
			}
		}
		for i := 1; i < len(data); i += 2 {
			if _, err = w.Writer.Write([]byte(data[i])); err != nil {
				if w.OnError == nil {
					return
				}
				if err = w.OnError(err); err != nil {
					return
				}
			}
		}
	}
}

func (w *DrainWork) close() {
	atomic.StoreInt32(&w.closed, 1)
}

type Drainer struct {
	// redis client
	cli *redis.Client
	// protect following
	mu sync.Mutex
	// key-entry map
	m  map[string]*DrainWork
	wg sync.WaitGroup
}

func NewDrainer() *Drainer {
	return &Drainer{
		m: make(map[string]*DrainWork),
	}
}

func (d *Drainer) Dial(options *redis.Options) (err error) {
	d.mu.Lock()
	defer d.mu.Unlock()
	if d.cli == nil {
		return ErrClosed
	}
	cli := redis.NewClient(options)
	if _, err = cli.Ping().Result(); err != nil {
		return
	}
	d.cli = cli
	return
}

func (d *Drainer) Drain(w *DrainWork) (err error) {
	d.mu.Lock()
	defer d.mu.Unlock()
	if d.cli == nil {
		return ErrClosed
	}
	if _, ok := d.m[w.Key]; ok {
		return ErrRedupKey
	}
	w.cli = d.cli
	d.m[w.Key] = w
	d.wg.Add(1)
	go func() {
		w.serve()
		d.mu.Lock()
		delete(d.m, w.Key)
		d.mu.Unlock()
		d.wg.Done()
	}()
	return
}

func (d *Drainer) Close() error {
	d.mu.Lock()
	if d.cli == nil {
		d.mu.Unlock()
		return ErrClosed
	}
	cli := d.cli
	d.cli = nil
	for _, w := range d.m {
		w.close()
	}
	d.mu.Unlock()
	// waiting for all goroutine exit
	d.wg.Wait()
	// close redis
	return cli.Close()
}
