// 单元测试

package redismq

import (
	"bytes"
	"runtime"
	"testing"

	"github.com/go-redis/redis"
)

func TestLoggerDrainer(t *testing.T) {
	const Key = "TestLoggerDrainer"

	c := redis.NewClient(&redis.Options{Addr: "127.0.0.1:6379"})
	if _, err := c.Ping().Result(); err != nil {
		t.Fatal("redis dial: ", err)
	}

	// drainer
	d := NewDrainer(c)
	defer d.Close()
	drained := 0
	buf := bytes.NewBuffer(nil)
	if err := d.Drain(&Work{
		Key:        Key,
		Writer:     buf,
		AfterWrite: func([]byte) { drained++ },
	}); err != nil {
		t.Fatal("drainer drain: ", err)
	}
	// logger
	l := NewLogger(c, Key)
	defer l.Close()
	data := [][]byte{
		[]byte("Long live the People's Republic of China\n"),
		[]byte("Long live the great unity of the peoples of the world\n"),
	}
	for _, p := range data {
		l.Write(p)
	}
	// check
	for {
		runtime.Gosched()
		if drained == 2 {
			break
		}
	}
	var b = make([]byte, 0)
	for _, p := range data {
		b = append(b, p...)
	}
	if !bytes.Equal(b, buf.Bytes()) {
		t.Fatal("sent and received data mismatch")
	}
}
