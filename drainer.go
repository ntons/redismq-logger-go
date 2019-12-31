// 汲取器

package redismq

import (
	"sync"
	"time"

	"github.com/go-redis/redis"
)

type Drainer struct {
	// redis client
	atom atom
	// works
	mu sync.Mutex
	m  map[string]*Work
	wg sync.WaitGroup
}

// equivalence to new(Drainer) and Logger.Dial
func NewDrainer(c *redis.Client) *Drainer {
	return &Drainer{m: make(map[string]*Work)}
}

func (d *Drainer) Dial(options *redis.Options) (err error) {
	c := redis.NewClient(options)
	if _, err = c.Ping().Result(); err != nil {
		return
	}
	d.dispose(d.atom.swap(c))
	return
}

func (d *Drainer) Redirect(c *redis.Client) (err error) {
	d.dispose(d.atom.swap(c))
	return
}

func (d *Drainer) Drain(w *Work) (err error) {
	d.mu.Lock()
	defer d.mu.Unlock()
	if d.atom.closed() {
		return ErrClosed
	}
	if d.m == nil {
		d.m = make(map[string]*Work)
	}
	if _, ok := d.m[w.Key]; ok {
		return ErrRedupKey
	}
	w.atom = &d.atom
	d.m[w.Key] = w
	d.wg.Add(1)
	go func() {
		defer d.wg.Done()
		w.serve()
		d.mu.Lock()
		defer d.mu.Unlock()
		delete(d.m, w.Key)
	}()
	return
}

func (d *Drainer) Close() (err error) {
	c := d.atom.swap(nil)
	d.wg.Wait()
	if c != nil {
		c.Close()
	}
	return
}

func (d *Drainer) dispose(c *redis.Client) {
	if c == nil {
		return
	}
	if d := c.Options().ReadTimeout; d > 0 {
		time.AfterFunc(d+time.Second, func() { c.Close() })
	} else {
		time.AfterFunc(time.Minute, func() { c.Close() })
	}
}
