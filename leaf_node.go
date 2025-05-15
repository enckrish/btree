package btree

import (
	"bytes"
	"slices"
)

type LeafNode[V any] struct {
	keys     []Bytes
	values   []*V
	next     *LeafNode[V] // points to the leaf to its right
	minCount int
}

func newLeafNode[V any](nKeys int) *LeafNode[V] {
	return &LeafNode[V]{
		keys:     make([]Bytes, 0, nKeys),
		values:   make([]*V, 0, nKeys),
		next:     nil,
		minCount: ceilDiv(nKeys, 2),
	}
}

func (l *LeafNode[V]) Next() *LeafNode[V] {
	return l.next
}

func (l *LeafNode[V]) len() int {
	return len(l.keys)
}

func (l *LeafNode[V]) isLeaf() bool {
	return true
}

func (l *LeafNode[V]) needsRebalance() bool {
	return l.len() < l.minCount
}

func (l *LeafNode[V]) isHealthy() bool {
	rebalNeeded := l.needsRebalance()
	keyValLenMatch := l.len() == len(l.values)
	keysSorted := slices.IsSortedFunc(l.keys, func(a, b Bytes) int {
		return bytes.Compare(a, b)
	})
	keysUnique := !hasRepeatsFn(l.keys, bytes.Equal)
	nextIsCorrect := l.next == nil || bytes.Compare(l.keys[l.len()-1], l.next.keys[0]) == -1

	healthy := !rebalNeeded && keyValLenMatch && keysSorted && keysUnique && nextIsCorrect
	return healthy
}

func (l *LeafNode[V]) numUnhealthyChildren() (unhealthy int, total int) {
	return 0, 0
}

func (l *LeafNode[V]) pairAt(idx int) (Bytes, *V) {
	if idx >= l.len() {
		return nil, nil
	}
	return l.keys[idx], l.values[idx]
}

func (l *LeafNode[V]) setOrInsert(key Bytes, value *V) (Bytes, Node[V]) {
	idx, _ := lowerBoundBytesArr(l.keys, key)

	// Key already exists in tree
	if idx < l.len() && bytes.Equal(l.keys[idx], key) {
		l.values[idx] = value
		return nil, nil
	}

	// Key doesn't exist but leaf has available space
	if l.len() < cap(l.keys) {
		l.insertAtIndex(idx, key, value)
		return nil, nil
	}

	// Leaf needs to be split for insertion; insertWithSplit doesn't return nil in any case
	node := l.insertWithSplit(idx, key, value)
	return node.keys[0], node
}

func (l *LeafNode[V]) insertAtIndex(idx int, key Bytes, value *V) {
	sz := l.len()
	l.keys = l.keys[:sz+1]
	l.values = l.values[:sz+1]
	shrArr(l.keys[idx:], 1)
	shrArr(l.values[idx:], 1)

	l.keys[idx], l.values[idx] = key, value
}

func (l *LeafNode[V]) insertWithSplit(idx int, key Bytes, value *V) *LeafNode[V] {
	size := l.minCount               // number of keys to keep in the old node
	r := newLeafNode[V](cap(l.keys)) // new right node
	r.next = l.next
	l.next = r

	// determine if key would be in the old or new node, and its index in that node
	c := 1
	keyLeaf := l
	if idx >= size {
		c = 0
		keyLeaf = r
		idx -= size
	}

	// copy to new node
	r.keys = append(r.keys, l.keys[size-c:]...)
	r.values = append(r.values, l.values[size-c:]...)
	// trimming original node
	l.keys = l.keys[:size-c]
	l.values = l.values[:size-c]

	// insert new key and value in the correct node
	keyLeaf.insertAtIndex(idx, key, value)
	return r
}

func (l *LeafNode[V]) delete(key Bytes, _ bool) bool {
	i, _ := lowerBoundBytesArr(l.keys, key)

	// key found
	if i < l.len() && bytes.Equal(l.keys[i], key) {
		sz := l.len()
		shlArr(l.keys[i:], 1)
		shlArr(l.values[i:], 1)
		l.keys = l.keys[:sz-1]
		l.values = l.values[:sz-1]
		return true
	}
	// key not found
	return false
}

func (l *LeafNode[V]) rebalanceWith(rightNode Node[V], _ Bytes) Bytes {
	// cast sibling as leaf node type, will panic if it isn't
	rLeaf := rightNode.(*LeafNode[V])

	// if a single node can contain all the data
	merge := cap(l.keys) >= l.len()+rLeaf.len()
	if merge {
		l.keys = append(l.keys, rLeaf.keys...)
		l.values = append(l.values, rLeaf.values...)
		l.next = rLeaf.next
		return nil
	}

	redistributeLeafUnoptimized(l, rLeaf)
	return rLeaf.keys[0]
}

func redistributeLeafUnoptimized[V any](l *LeafNode[V], r *LeafNode[V]) {
	totalLen := l.len() + r.len()
	temp := newLeafNode[V](totalLen)
	temp.keys = append(temp.keys, l.keys...)
	temp.keys = append(temp.keys, r.keys...)
	temp.values = append(temp.values, l.values...)
	temp.values = append(temp.values, r.values...)

	lsz := l.minCount
	rsz := totalLen - lsz

	l.keys = l.keys[:lsz]
	l.values = l.values[:lsz]
	copy(l.keys, temp.keys)
	copy(l.values, temp.values)

	r.keys = r.keys[:rsz]
	r.values = r.values[:rsz]
	copy(r.keys, temp.keys[lsz:])
	copy(r.values, temp.values[lsz:])
}
