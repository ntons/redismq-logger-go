// 汲取器

package redismq

import (
	"sync"
	"sync/atomic"

	"github.com/go-redis/redis"
)

type Drainer struct {
	c *redis.Client
	// closed flag
	closed int32
	// works
	mu sync.Mutex
	m  map[string]*Work
	wg sync.WaitGroup
}

// equivalence to new(Drainer) and Logger.Dial
func NewDrainer(c *redis.Client) *Drainer {
	return &Drainer{
		c: c,
		m: make(map[string]*Work),
	}
}

func (d *Drainer) Drain(w *Work) (err error) {
	if atomic.LoadInt32(&d.closed) != 0 {
		return ErrClosed
	}
	d.mu.Lock()
	defer d.mu.Unlock()
	if _, ok := d.m[w.Key]; ok {
		return ErrRedupKey
	}
	d.m[w.Key] = w
	d.wg.Add(1)
	go func() {
		defer d.wg.Done()
		w.serve(context{d.c, &d.closed})
		d.mu.Lock()
		defer d.mu.Unlock()
		delete(d.m, w.Key)
	}()
	return
}

func (d *Drainer) Close() (err error) {
	if !atomic.CompareAndSwapInt32(&d.closed, 0, 1) {
		return
	}
	d.wg.Wait()
	return
}
