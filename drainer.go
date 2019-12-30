// 汲取器

package redismq

import (
	"fmt"
	"io"
	"sync"
	"sync/atomic"
	"time"

	"github.com/go-redis/redis"
	"github.com/ntons/log-go"
)

const (
	defaultErrorRecoveryDuration = time.Second / 4
)

type SleepForRecovery struct {
	Duration time.Duration
}

func (x SleepForRecovery) Error() string {
	return fmt.Sprintf("sleep %v for recovery", x.Duration)
}

var DonotSleepForRecovery = SleepForRecovery{Duration: 0}

type DrainWork struct {
	Key    string
	Writer io.Writer

	// before write hook, return false skip this data
	BeforeWrite func([]byte) bool
	// after write hook, triggered when success
	AfterWrite func([]byte)

	OnError func(error) error
	OnClose func(error)
	// sleep duration for recovery, a qualter of one second by default
	ErrorRecoveryDuration time.Duration
	//
	cli    *redis.Client
	closed int32
}

func (w *DrainWork) handleSleepForRecovery(err error) (time.Duration, error) {
	if err != nil {
		switch x := err.(type) {
		case SleepForRecovery:
			return x.Duration, nil
		case *SleepForRecovery:
			return x.Duration, nil
		default:
			return 0, err
		}
	} else if w.ErrorRecoveryDuration > 0 {
		return w.ErrorRecoveryDuration, nil
	} else {
		return defaultErrorRecoveryDuration, nil
	}
}

func (x *DrainWork) getErrorRecoveryDuration() time.Duration {
	if x.ErrorRecoveryDuration > 0 {
		return x.ErrorRecoveryDuration
	}
	return defaultErrorRecoveryDuration
}

func (w *DrainWork) handleError(err error) (time.Duration, error) {
	if w.OnError == nil {
		// by default, sleep and retry
		log.Warnf("DrainWork[%v]: %v", w.Key, err)
		return w.getErrorRecoveryDuration(), nil
	}
	// handle error by user
	if err = w.OnError(err); err == nil {
		// anyhow, sleep for error
		return w.getErrorRecoveryDuration(), nil
	}
	switch x := err.(type) {
	case SleepForRecovery:
		return x.Duration, nil
	case *SleepForRecovery:
		return x.Duration, nil
	}
	return 0, err
}

func (w *DrainWork) serve() (err error) {
	if err = w.doServe(); err != nil {
		log.Warnf("DrainWork %q exit with error: %v", w.Key, err)
	}
	if w.OnClose != nil {
		w.OnClose(err)
	}
	return
}
func (w *DrainWork) doServe() (err error) {
	for {
		if atomic.LoadInt32(&w.closed) != 0 {
			return nil
		}
		var data []string
		if data, err = w.cli.BRPop(
			time.Second, w.Key).Result(); err != nil {
			if err == redis.Nil {
				err = nil
				continue // no data to read
			}
			var sleep time.Duration // sleep duration
			if sleep, err = w.handleError(err); err != nil {
				return
			}
			if sleep > 0 {
				time.Sleep(sleep)
			}
			continue
		}
		for i := 1; i < len(data); i += 2 {
			p := []byte(data[i])
			if w.BeforeWrite != nil && !w.BeforeWrite(p) {
				continue //skip
			}
			for writen, failure := 0, 1; ; failure++ {
				var n int
				if n, err = w.Writer.Write(p[writen:]); err != nil {
					writen += n
					// retry 3 times at most while closing
					if failure >= 3 && atomic.LoadInt32(&w.closed) != 0 {
						return // with write error
					}
					var sleep time.Duration
					if sleep, err = w.handleError(err); err != nil {
						return
					}
					if sleep > 0 {
						time.Sleep(sleep)
					}
					continue
				}
				break
			}
			if w.AfterWrite != nil {
				w.AfterWrite(p)
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
	if d.cli != nil {
		return ErrDialed
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
