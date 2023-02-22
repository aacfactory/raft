package wals_test

import (
	"bytes"
	"fmt"
	"github.com/aacfactory/raft/wals"
	"testing"
)

func TestEntry_Data(t *testing.T) {
	p := make([]byte, 0, 1)
	for i := 0; i < 10; i++ {
		p = append(p, []byte("0123456789")...)
		p = append(p, '|')
	}
	fmt.Println(string(p))
	entry := wals.NewEntry(wals.Key(1), p)
	fmt.Println(entry.Key(), entry.Blocks(), bytes.Equal(p, entry.Data()))

}

func TestEntryTree(t *testing.T) {
	tree := wals.NewEntryTree()
	e := wals.NewEntry(wals.Key(1), []byte("1"))
	fmt.Println(e.Committed())
	tree.Set(wals.Key(1), e, 1)
	tree.Set(wals.Key(2), []byte("2"), 2)
	tree.Set(wals.Key(3), []byte("3"), 3)
	fmt.Println(tree.Len())
	e.Commit()
	e1, pos, has := tree.Get(wals.Key(1))
	fmt.Println(e1.Committed(), pos, has)
	tree.Remove(wals.Key(2))
	fmt.Println(tree.Get(wals.Key(2)))
	fmt.Println(tree.Get(wals.Key(3)))
	fmt.Println(tree.Len())
}
