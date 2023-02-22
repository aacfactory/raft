package caches_test

import (
	"fmt"
	"github.com/aacfactory/raft/caches"
	"testing"
)

func TestNewLRU(t *testing.T) {
	cache, err := caches.NewLRU(10, func(key interface{}, value interface{}) {
		fmt.Println("out", key, value)
	})
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 20; i++ {
		cache.Add(i, i)
	}
	for i := 0; i < 20; i++ {
		fmt.Println(cache.Get(i))
	}
}
