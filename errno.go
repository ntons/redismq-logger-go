package redismq

import (
	"fmt"
)

type Errno int

const (
	ErrClosed Errno = iota + 1
	ErrRedupKey
	ErrDialed
)

func (e Errno) Error() string {
	switch e {
	case ErrClosed:
		return "closed"
	case ErrDialed:
		return "dialed"
	case ErrRedupKey:
		return "redup key"
	default:
		return fmt.Sprintf("errno:%d", e)
	}
}
