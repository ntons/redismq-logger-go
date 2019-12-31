package redismq

import (
	"fmt"
	"io"
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

type Work struct {
	// redis list key
	Key string
	// downstream writer
	Writer io.Writer

	// before write hook, return false skip this data
	BeforeWrite func([]byte) bool
	// after write hook, triggered when success
	AfterWrite func([]byte)
	// error hook
	OnError func(error) error
	// exit hook
	OnExit func(error)
	// sleep duration for recovery, a qualter of one second by default
	ErrorRecoveryDuration time.Duration
	// redis client
	atom *atom
}

func (x *Work) getErrorRecoveryDuration() time.Duration {
	if x.ErrorRecoveryDuration > 0 {
		return x.ErrorRecoveryDuration
	}
	return defaultErrorRecoveryDuration
}

func (w *Work) handleError(err error) (time.Duration, error) {
	if w.OnError == nil {
		// by default, sleep and retry
		log.Warnf("Work[%v]: %v", w.Key, err)
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

func (w *Work) serve() (err error) {
	if err = w.doServe(); err != nil {
		log.Warnf("Work %q exit with error: %v", w.Key, err)
	}
	if w.OnExit != nil {
		w.OnExit(err)
	}
	return
}
func (w *Work) doServe() (err error) {
	for c := w.atom.get(); c != nil; c, err = w.atom.get(), nil {
		var data []string
		if data, err = c.BRPop(time.Second, w.Key).Result(); err != nil {
			if err == redis.Nil {
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
			b := []byte(data[i])
			if w.BeforeWrite != nil && !w.BeforeWrite(b) {
				continue //skip
			}
			for writen, failure := 0, 1; ; failure++ {
				var n int
				if n, err = w.Writer.Write(b[writen:]); err != nil {
					writen += n
					// retry 3 times at most while closing
					if failure >= 3 && w.atom.closed() {
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
				w.AfterWrite(b)
			}
		}
	}
	return
}
