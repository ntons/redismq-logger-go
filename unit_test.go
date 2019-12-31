// 单元测试

package redismq

import (
	"bytes"
	"runtime"
	"testing"
	"time"

	"github.com/go-redis/redis"
)

func TestLoggerDrainer(t *testing.T) {
	const Key = "TestLoggerDrainer"
	opts := &redis.Options{Addr: "127.0.0.1:6379"}

	// drainer
	buf := bytes.NewBuffer(nil)
	d := NewDrainer(nil)
	if err := d.Dial(opts); err != nil {
		t.Fatal("drainer dail: ", err)
	}
	defer d.Close()
	drained := 0
	if err := d.Drain(&Work{
		Key:        Key,
		Writer:     buf,
		AfterWrite: func([]byte) { drained++ },
	}); err != nil {
		t.Fatal("drainer drain: ", err)
	}
	// logger
	l := NewLogger(Key, nil)
	if err := l.Dial(opts); err != nil {
		t.Fatal("logger dail: ", err)
	}
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
		time.Sleep(100 * time.Millisecond)
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
