package btree

import (
	"bytes"
	"slices"
	"sort"
)

type LeafNode[V any] struct {
	keys   []Bytes
	values []V
	next   *LeafNode[V] // points to the leaf to its right
}

func newLeafNode[V any](nKeys int) *LeafNode[V] {
	return &LeafNode[V]{
		keys:   make([]Bytes, 0, nKeys),
		values: make([]V, 0, nKeys),
		next:   nil,
	}
}

func (l *LeafNode[V]) Next() *LeafNode[V] {
	return l.next
}

func (l *LeafNode[V]) isHealthy() bool {
	minKeysExists := len(l.keys) >= ceilDiv(cap(l.keys), 2)
	keyValLenMatch := len(l.keys) == len(l.values)
	keysSorted := slices.IsSortedFunc(l.keys, func(a, b Bytes) int {
		return bytes.Compare(a, b)
	})
	keysUnique := !hasRepeatsFn(l.keys, bytes.Equal)
	nextIsCorrect := l.next == nil || bytes.Compare(l.keys[len(l.keys)-1], l.next.keys[0]) == -1

	healthy := minKeysExists && keyValLenMatch && keysSorted && keysUnique && nextIsCorrect
	return healthy
}

func (l *LeafNode[V]) numUnhealthyChildren() (unhealthy int, total int) {
	return 0, 0
}

func (l *LeafNode[V]) pairAt(idx int) (Bytes, *V) {
	if idx >= len(l.keys) {
		return nil, nil
	}
	return l.keys[idx], &l.values[idx]
}

func (l *LeafNode[V]) lbPositionedRef(key Bytes) (*LeafNode[V], int) {
	i := sort.Search(len(l.keys), func(i int) bool {
		return bytes.Compare(key, l.keys[i]) <= 0
	})
	return l, i
}

func (l *LeafNode[V]) valueRef(key Bytes) *V {
	l, i := l.lbPositionedRef(key)
	if k, v := l.pairAt(i); k != nil && bytes.Equal(k, key) {
		return v
	}
	return nil
}

func (l *LeafNode[V]) setOrInsert(key Bytes, value V) (Bytes, Node[V]) {
	_, idx := l.lbPositionedRef(key)

	// Key already exists in tree
	if idx < len(l.keys) && bytes.Equal(l.keys[idx], key) {
		l.values[idx] = value
		return nil, nil
	}

	// Key doesn't exist but leaf has available space
	if len(l.keys) < cap(l.keys) {
		l.insertAtIndex(idx, key, value)
		return nil, nil
	}

	// Leaf needs to be split for insertion; insertWithSplit doesn't return nil in any case
	node := l.insertWithSplit(idx, key, value)
	return node.keys[0], node
}

func (l *LeafNode[V]) insertAtIndex(idx int, key Bytes, value V) {
	// Expand slices to add new values
	size := len(l.keys)
	l.keys = l.keys[:size+1]
	l.values = l.values[:size+1]

	copy(l.keys[idx+1:], l.keys[idx:])
	copy(l.values[idx+1:], l.values[idx:])
	l.keys[idx] = key
	l.values[idx] = value
}

func (l *LeafNode[V]) insertWithSplit(idx int, key Bytes, value V) *LeafNode[V] {
	size := ceilDiv(cap(l.keys), 2)  // number of keys to keep in the old node
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
