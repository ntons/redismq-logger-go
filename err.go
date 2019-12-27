package redismq

import (
	"errors"
)

var (
	ErrClosed   = errors.New("closed")
	ErrRedupKey = errors.New("redup key")
	ErrDialed   = errors.New("dailed")
)
