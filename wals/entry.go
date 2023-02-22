package wals

import (
	"encoding/binary"
	"math"
)

const (
	blockSize = 256
)

func NewEntry(key Key, p []byte) (entry Entry) {
	pLen := uint16(len(p))
	size := uint16(math.Ceil(float64(pLen+8) / float64(blockSize-8)))
	entry = make([]byte, blockSize*size)

	sLow := uint16(0)
	sHigh := uint16(blockSize - 8 - 8 - 2)
	for i := uint16(0); i < size; i++ {
		bLow := i * blockSize
		bHigh := bLow + blockSize
		block := entry[bLow:bHigh]
		if size == 1 || sHigh > pLen {
			sHigh = pLen
		}
		segment := p[sLow:sHigh]
		binary.BigEndian.PutUint16(block[0:2], i)
		binary.BigEndian.PutUint16(block[2:4], size)
		if i == 0 {
			binary.BigEndian.PutUint32(block[4:8], uint32(sHigh-sLow+8+2))
			binary.BigEndian.PutUint64(block[8:16], uint64(key))
			binary.BigEndian.PutUint16(block[16:18], 0)
			copy(block[18:], segment)
		} else {
			binary.BigEndian.PutUint32(block[4:8], uint32(sHigh-sLow))
			copy(block[8:], segment)
		}
		sLow = sHigh
		sHigh = sLow + blockSize - 8 - 2
	}
	return
}

func DecodeEntries(p []byte) (entries []Entry) {
	entries = make([]Entry, 0, 1)
	for {
		if len(p) == 0 {
			break
		}
		span := binary.BigEndian.Uint16(p[2:4])
		end := span * blockSize
		entries = append(entries, Entry(p[0:end]))
		p = p[end:]
	}
	return
}

type Key uint64

type Entry []byte

func (entry Entry) Blocks() (n uint16) {
	_, n = Block(entry[0:blockSize]).Header()
	return
}

func (entry Entry) Key() (key Key) {
	key = Key(binary.BigEndian.Uint64(entry[8:16]))
	return
}

func (entry Entry) Commit() {
	binary.BigEndian.PutUint16(entry[16:18], 1)
	return
}

func (entry Entry) Committed() (ok bool) {
	ok = binary.BigEndian.Uint16(entry[16:18]) == 1
	return
}

func (entry Entry) Data() (p []byte) {
	p = make([]byte, 0, len(entry))
	n := entry.Blocks()
	for i := uint16(0); i < n; i++ {
		block := Block(entry[i*blockSize : i*blockSize+blockSize])
		data := block.Data()
		if i == 0 {
			p = append(p, data[10:]...)
		} else {
			p = append(p, data...)
		}
	}
	return
}

func NewBlock() Block {
	return make([]byte, blockSize)
}

type Block []byte

func (block Block) Header() (idx uint16, span uint16) {
	idx = binary.BigEndian.Uint16(block[0:2])
	span = binary.BigEndian.Uint16(block[2:4])
	return
}

func (block Block) Data() (p []byte) {
	size := binary.BigEndian.Uint32(block[4:8])
	p = block[8 : 8+size]
	return
}

const (
	degree   = 128
	maxItems = degree*2 - 1 // max items per node. max children is +1
	minItems = maxItems / 2
)

type cow struct {
	_ int
}

type entryTreeItems []*entryTreeItem

func (items entryTreeItems) Len() int {
	return len(items)
}

func (items entryTreeItems) Less(i, j int) bool {
	return items[i].pos < items[j].pos
}

func (items entryTreeItems) Swap(i, j int) {
	items[i], items[j] = items[j], items[i]
	return
}

type entryTreeItem struct {
	key   Key
	value Entry
	pos   uint64
}

type entryTreeNode struct {
	cow      *cow
	count    int
	items    []entryTreeItem
	children *[]*entryTreeNode
}

func NewEntryTree() (tree *EntryTree) {
	tree = &EntryTree{
		cow:   new(cow),
		root:  nil,
		count: 0,
		empty: entryTreeItem{},
	}
	return
}

type EntryTree struct {
	cow   *cow
	root  *entryTreeNode
	count int
	empty entryTreeItem
}

func (tr *EntryTree) copy(n *entryTreeNode) *entryTreeNode {
	n2 := new(entryTreeNode)
	n2.cow = tr.cow
	n2.count = n.count
	n2.items = make([]entryTreeItem, len(n.items), cap(n.items))
	copy(n2.items, n.items)
	if !n.leaf() {
		n2.children = new([]*entryTreeNode)
		*n2.children = make([]*entryTreeNode, len(*n.children), maxItems+1)
		copy(*n2.children, *n.children)
	}
	return n2
}

func (tr *EntryTree) cowLoad(cn **entryTreeNode) *entryTreeNode {
	if (*cn).cow != tr.cow {
		*cn = tr.copy(*cn)
	}
	return *cn
}

func (tr *EntryTree) less(a, b Key) bool {
	return a < b
}

func (tr *EntryTree) newNode(leaf bool) *entryTreeNode {
	n := new(entryTreeNode)
	n.cow = tr.cow
	if !leaf {
		n.children = new([]*entryTreeNode)
	}
	return n
}

func (n *entryTreeNode) leaf() bool {
	return n.children == nil
}

func (tr *EntryTree) find(n *entryTreeNode, key Key) (index int, found bool) {
	low := 0
	high := len(n.items)
	for low < high {
		mid := (low + high) / 2
		if !tr.less(key, n.items[mid].key) {
			low = mid + 1
		} else {
			high = mid
		}
	}
	if low > 0 && !tr.less(n.items[low-1].key, key) {
		return low - 1, true
	}
	return low, false
}

func (tr *EntryTree) Set(key Key, value Entry, pos uint64) bool {
	item := entryTreeItem{key: key, value: value, pos: pos}
	if tr.root == nil {
		tr.root = tr.newNode(true)
		tr.root.items = append([]entryTreeItem{}, item)
		tr.root.count = 1
		tr.count = 1
		return false
	}
	replaced, split := tr.nodeSet(&tr.root, item)
	if split {
		left := tr.root
		right, median := tr.nodeSplit(left)
		tr.root = tr.newNode(false)
		*tr.root.children = make([]*entryTreeNode, 0, maxItems+1)
		*tr.root.children = append([]*entryTreeNode{}, left, right)
		tr.root.items = append([]entryTreeItem{}, median)
		tr.root.updateCount()
		return tr.Set(item.key, item.value, item.pos)
	}
	if replaced {
		return true
	}
	tr.count++
	return false
}

func (tr *EntryTree) nodeSplit(n *entryTreeNode) (right *entryTreeNode, median entryTreeItem) {
	i := maxItems / 2
	median = n.items[i]
	left := tr.newNode(n.leaf())
	left.items = make([]entryTreeItem, len(n.items[:i]), maxItems/2)
	copy(left.items, n.items[:i])
	if !n.leaf() {
		*left.children = make([]*entryTreeNode,
			len((*n.children)[:i+1]), maxItems+1)
		copy(*left.children, (*n.children)[:i+1])
	}
	left.updateCount()
	right = tr.newNode(n.leaf())
	right.items = make([]entryTreeItem, len(n.items[i+1:]), maxItems/2)
	copy(right.items, n.items[i+1:])
	if !n.leaf() {
		*right.children = make([]*entryTreeNode,
			len((*n.children)[i+1:]), maxItems+1)
		copy(*right.children, (*n.children)[i+1:])
	}
	right.updateCount()
	*n = *left
	return right, median
}

func (n *entryTreeNode) updateCount() {
	n.count = len(n.items)
	if !n.leaf() {
		for i := 0; i < len(*n.children); i++ {
			n.count += (*n.children)[i].count
		}
	}
}

func (tr *EntryTree) nodeSet(pn **entryTreeNode, item entryTreeItem) (replaced bool, split bool) {
	n := tr.cowLoad(pn)
	i, found := tr.find(n, item.key)
	if found {
		n.items[i].value = item.value
		return true, false
	}
	if n.leaf() {
		if len(n.items) == maxItems {
			return false, true
		}
		n.items = append(n.items, tr.empty)
		copy(n.items[i+1:], n.items[i:])
		n.items[i] = item
		n.count++
		return false, false
	}
	replaced, split = tr.nodeSet(&(*n.children)[i], item)
	if split {
		if len(n.items) == maxItems {
			return false, true
		}
		right, median := tr.nodeSplit((*n.children)[i])
		*n.children = append(*n.children, nil)
		copy((*n.children)[i+1:], (*n.children)[i:])
		(*n.children)[i+1] = right
		n.items = append(n.items, tr.empty)
		copy(n.items[i+1:], n.items[i:])
		n.items[i] = median
		return tr.nodeSet(&n, item)
	}
	if !replaced {
		n.count++
	}
	return replaced, false
}

func (tr *EntryTree) Get(key Key) (Entry, uint64, bool) {
	if tr.root == nil {
		return tr.empty.value, 0, false
	}
	n := tr.root
	for {
		i, found := tr.find(n, key)
		if found {
			return n.items[i].value, n.items[i].pos, true
		}
		if n.leaf() {
			return tr.empty.value, 0, false
		}
		n = (*n.children)[i]
	}
}

func (tr *EntryTree) Len() int {
	return tr.count
}

func (tr *EntryTree) Remove(key Key) bool {
	if tr.root == nil {
		return false
	}
	_, deleted := tr.remove(&tr.root, false, key)
	if !deleted {
		return false
	}
	if len(tr.root.items) == 0 && !tr.root.leaf() {
		tr.root = (*tr.root.children)[0]
	}
	tr.count--
	if tr.count == 0 {
		tr.root = nil
	}
	return true
}

func (tr *EntryTree) remove(pn **entryTreeNode, max bool, key Key) (entryTreeItem, bool) {
	n := tr.cowLoad(pn)
	var i int
	var found bool
	if max {
		i, found = len(n.items)-1, true
	} else {
		i, found = tr.find(n, key)
	}
	if n.leaf() {
		if found {
			prev := n.items[i]
			copy(n.items[i:], n.items[i+1:])
			n.items[len(n.items)-1] = tr.empty
			n.items = n.items[:len(n.items)-1]
			n.count--
			return prev, true
		}
		return tr.empty, false
	}
	var prev entryTreeItem
	var deleted bool
	if found {
		if max {
			i++
			prev, deleted = tr.remove(&(*n.children)[i], true, tr.empty.key)
		} else {
			prev = n.items[i]
			maxItem, _ := tr.remove(&(*n.children)[i], true, tr.empty.key)
			deleted = true
			n.items[i] = maxItem
		}
	} else {
		prev, deleted = tr.remove(&(*n.children)[i], max, key)
	}
	if !deleted {
		return tr.empty, false
	}
	n.count--
	if len((*n.children)[i].items) < minItems {
		tr.rebalanced(n, i)
	}
	return prev, true
}

func (tr *EntryTree) rebalanced(n *entryTreeNode, i int) {
	if i == len(n.items) {
		i--
	}
	left := tr.cowLoad(&(*n.children)[i])
	right := tr.cowLoad(&(*n.children)[i+1])
	if len(left.items)+len(right.items) < maxItems {
		left.items = append(left.items, n.items[i])
		left.items = append(left.items, right.items...)
		if !left.leaf() {
			*left.children = append(*left.children, *right.children...)
		}
		left.count += right.count + 1
		copy(n.items[i:], n.items[i+1:])
		n.items[len(n.items)-1] = tr.empty
		n.items = n.items[:len(n.items)-1]
		copy((*n.children)[i+1:], (*n.children)[i+2:])
		(*n.children)[len(*n.children)-1] = nil
		*n.children = (*n.children)[:len(*n.children)-1]
	} else if len(left.items) > len(right.items) {
		right.items = append(right.items, tr.empty)
		copy(right.items[1:], right.items)
		right.items[0] = n.items[i]
		right.count++
		n.items[i] = left.items[len(left.items)-1]
		left.items[len(left.items)-1] = tr.empty
		left.items = left.items[:len(left.items)-1]
		left.count--

		if !left.leaf() {
			*right.children = append(*right.children, nil)
			copy((*right.children)[1:], *right.children)
			(*right.children)[0] = (*left.children)[len(*left.children)-1]
			(*left.children)[len(*left.children)-1] = nil
			*left.children = (*left.children)[:len(*left.children)-1]
			left.count -= (*right.children)[0].count
			right.count += (*right.children)[0].count
		}
	} else {
		left.items = append(left.items, n.items[i])
		left.count++
		n.items[i] = right.items[0]
		copy(right.items, right.items[1:])
		right.items[len(right.items)-1] = tr.empty
		right.items = right.items[:len(right.items)-1]
		right.count--

		if !left.leaf() {
			*left.children = append(*left.children, (*right.children)[0])
			copy(*right.children, (*right.children)[1:])
			(*right.children)[len(*right.children)-1] = nil
			*right.children = (*right.children)[:len(*right.children)-1]
			left.count += (*left.children)[len(*left.children)-1].count
			right.count -= (*left.children)[len(*left.children)-1].count
		}
	}
}

func (tr *EntryTree) Height() int {
	var height int
	if tr.root != nil {
		n := tr.root
		for {
			height++
			if n.leaf() {
				break
			}
			n = (*n.children)[0]
		}
	}
	return height
}
