package caches

import (
	"container/list"
	"errors"
)

type EvictCallback func(key interface{}, value interface{})

type LRU struct {
	size      int64
	evictList *list.List
	items     map[interface{}]*list.Element
	onEvict   EvictCallback
}

type entry struct {
	key   interface{}
	value interface{}
}

func NewLRU(size int64, onEvict EvictCallback) (lru *LRU, err error) {
	if size <= 0 {
		return nil, errors.New("must provide a positive size")
	}
	lru = &LRU{
		size:      size,
		evictList: list.New(),
		items:     make(map[interface{}]*list.Element),
		onEvict:   onEvict,
	}
	return
}

func (c *LRU) Purge() {
	for k, v := range c.items {
		if c.onEvict != nil {
			c.onEvict(k, v.Value.(*entry).value)
		}
		delete(c.items, k)
	}
	c.evictList.Init()
}

func (c *LRU) Add(key, value interface{}) (evicted bool) {
	if ent, ok := c.items[key]; ok {
		c.evictList.MoveToFront(ent)
		ent.Value.(*entry).value = value
		return
	}

	ent := &entry{key, value}
	c.items[key] = c.evictList.PushFront(ent)

	evicted = int64(c.evictList.Len()) > c.size
	if evicted {
		c.removeOldest()
	}
	return
}

func (c *LRU) Get(key interface{}) (value interface{}, ok bool) {
	if ent, has := c.items[key]; has {
		c.evictList.MoveToFront(ent)
		if ent.Value.(*entry) == nil {
			return
		}
		ok = true
		value = ent.Value.(*entry).value
		return
	}
	return
}

func (c *LRU) Contains(key interface{}) (ok bool) {
	_, ok = c.items[key]
	return
}

func (c *LRU) Peek(key interface{}) (value interface{}, ok bool) {
	var ent *list.Element
	if ent, ok = c.items[key]; ok {
		value = ent.Value.(*entry).value
		return
	}
	return
}

func (c *LRU) Remove(key interface{}) (present bool) {
	if ent, ok := c.items[key]; ok {
		c.removeElement(ent)
		present = true
		return
	}
	return
}

func (c *LRU) RemoveOldest() (key, value interface{}, ok bool) {
	ent := c.evictList.Back()
	if ent != nil {
		c.removeElement(ent)
		kv := ent.Value.(*entry)
		key = kv.key
		value = kv.value
		ok = true
		return
	}
	return
}

func (c *LRU) GetOldest() (key, value interface{}, ok bool) {
	ent := c.evictList.Back()
	if ent != nil {
		kv := ent.Value.(*entry)
		key = kv.key
		value = kv.value
		ok = true
		return
	}
	return
}

func (c *LRU) Keys() []interface{} {
	keys := make([]interface{}, len(c.items))
	i := 0
	for ent := c.evictList.Back(); ent != nil; ent = ent.Prev() {
		keys[i] = ent.Value.(*entry).key
		i++
	}
	return keys
}

func (c *LRU) Len() int {
	return c.evictList.Len()
}

func (c *LRU) Resize(size int64) (evicted int64) {
	evicted = int64(c.Len()) - size
	if evicted < 0 {
		evicted = 0
	}
	for i := int64(0); i < evicted; i++ {
		c.removeOldest()
	}
	c.size = size
	return
}

func (c *LRU) removeOldest() {
	ent := c.evictList.Back()
	if ent != nil {
		c.removeElement(ent)
	}
}

func (c *LRU) removeElement(e *list.Element) {
	c.evictList.Remove(e)
	kv := e.Value.(*entry)
	delete(c.items, kv.key)
	if c.onEvict != nil {
		c.onEvict(kv.key, kv.value)
	}
}
