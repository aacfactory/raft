package wals_test

import (
	"os"
	"strconv"
	"testing"
	"time"
)

func TestWAL(t *testing.T) {
	file, _ := os.OpenFile(`G:\tmp\raft\wal\1.txt`, os.O_CREATE|os.O_RDWR|os.O_SYNC, 0640)
	for i := 0; i < 10; i++ {
		_, _ = file.Write([]byte(strconv.Itoa(i) + ":" + time.Now().Format(time.RFC3339) + "\n"))
	}

}
