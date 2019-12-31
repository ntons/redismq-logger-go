package redismq

import (
	"sync/atomic"
	"unsafe"

	"github.com/go-redis/redis"
)

// for replace redis client atomically
type atom struct {
	p unsafe.Pointer // epoch pointer
}

func (a *atom) closed() bool {
	return atomic.LoadPointer(&a.p) == nil
}
func (a *atom) get() *redis.Client {
	return (*redis.Client)(atomic.LoadPointer(&a.p))
}
func (a *atom) swap(r *redis.Client) *redis.Client {
	return (*redis.Client)(atomic.SwapPointer(&a.p, unsafe.Pointer(r)))
}
