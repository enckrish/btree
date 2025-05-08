package btree

import (
	"bytes"
	"slices"
	"sort"
)

type InternalNode[V any] struct {
	keys     []Bytes
	pointers []Node[V]
}

func newInternalNode[V any](degree int) *InternalNode[V] {
	return &InternalNode[V]{
		keys:     make([]Bytes, 0, degree-1),
		pointers: make([]Node[V], 0, degree),
	}
}

func (t *InternalNode[V]) isHealthy() bool {
	minPtrsExists := len(t.pointers) >= ceilDiv(cap(t.pointers), 2)
	keyPtrLenCheck := len(t.keys) == len(t.pointers)-1
	keysSorted := slices.IsSortedFunc(t.keys, func(a, b Bytes) int {
		return bytes.Compare(a, b)
	})
	keysUnique := !hasRepeatsFn(t.keys, bytes.Equal)
	ptrsUnique := true
	s := NewSet[Node[V]]()
	for _, p := range t.pointers {
		if s.Add(p) {
			ptrsUnique = false
			break
		}
	}

	healthy := minPtrsExists && keyPtrLenCheck && keysSorted && keysUnique && ptrsUnique
	return healthy
}

func (t *InternalNode[V]) numUnhealthyChildren() (unhealthy int, total int) {
	for _, ptr := range t.pointers {
		// check direct children
		total++
		if !ptr.isHealthy() {
			unhealthy++
		}

		// get data of indirect children
		u, to := ptr.numUnhealthyChildren()
		unhealthy += u
		total += to
	}
	return
}

func (t *InternalNode[V]) leftPtr(pos int) Node[V] {
	return t.pointers[pos]
}

func (t *InternalNode[V]) rightPtr(pos int) Node[V] {
	return t.pointers[pos+1]
}

func (t *InternalNode[V]) largestSibling(idx int) Node[V] {
	// TODO unimplemented
	// for getting best local sibling for rebalancing during deletion
	return nil
}

func (t *InternalNode[V]) insertIndex(key Bytes) (pos int, exists bool) {
	return sort.Find(len(t.keys), func(i int) int {
		return bytes.Compare(key, t.keys[i])
	})
}

func (t *InternalNode[V]) childForKey(key Bytes) (ptr Node[V]) {
	pos, exists := t.insertIndex(key)
	if exists {
		ptr = t.rightPtr(pos)
	} else {
		ptr = t.leftPtr(pos)
	}
	return
}

func (t *InternalNode[V]) lbPositionedRef(key Bytes) (*LeafNode[V], int) {
	child := t.childForKey(key)
	l, i := child.lbPositionedRef(key)
	return l, i
}

func (t *InternalNode[V]) valueRef(key Bytes) *V {
	l, i := t.lbPositionedRef(key)
	if k, v := l.pairAt(i); k != nil && bytes.Equal(k, key) {
		return v
	}
	return nil
}

func (t *InternalNode[V]) setOrInsert(key Bytes, value *V) (Bytes, Node[V]) {
	c := t.childForKey(key)
	key, ptr := c.setOrInsert(key, value)
	if ptr == nil {
		// No new child formed
		return nil, nil
	}
	up, newNode := t.insertNode(key, ptr)
	// In case, we directly return newNode, it won't return true for (newNode == nil) in the calling function
	// [See https://go.dev/doc/faq#nil_error]
	if newNode != nil {
		return up, newNode
	}
	return up, nil
}

func (t *InternalNode[V]) insertNode(key Bytes, ptr Node[V]) (upKey Bytes, newNode *InternalNode[V]) {
	idx, _ := t.insertIndex(key)

	// space available in node
	if len(t.keys) < cap(t.keys) {
		t.insertAtIndex(idx, key, ptr)
		return nil, nil
	}

	// needs splitting
	return t.insertWithSplit(idx, key, ptr)
}

func (t *InternalNode[V]) insertAtIndex(idx int, key Bytes, ptr Node[V]) {
	// Expand slices to add new values
	size := len(t.keys)
	t.keys = t.keys[:size+1]
	t.pointers = t.pointers[:size+2] // pointers are always one longer than keys

	copy(t.keys[idx+1:], t.keys[idx:])
	copy(t.pointers[idx+2:], t.pointers[idx+1:])
	t.keys[idx] = key
	t.pointers[idx+1] = ptr
}

func (t *InternalNode[V]) insertWithSplit(pos int, key Bytes, ptr Node[V]) (upKey Bytes, newNode *InternalNode[V]) {
	// maximum number of keys in old node after a split
	// in case of unequal distribution, it gives the new node more keys (by 1), this is a non-issue
	size := ceilDiv(cap(t.pointers), 2)

	// construct a temporary node with usual capacity + 1 for easier operations possibly at the cost of performance
	temp := newInternalNode[V](cap(t.pointers) + 1)
	temp.keys = append(temp.keys, t.keys...)
	temp.pointers = append(temp.pointers, t.pointers...)

	temp.insertAtIndex(pos, key, ptr)
	upKeyIdx := size - 1
	upKey = temp.keys[upKeyIdx]

	copy(t.keys[:upKeyIdx], temp.keys[:upKeyIdx])
	t.keys = t.keys[:upKeyIdx]
	copy(t.pointers[:size], temp.pointers[:size])
	t.pointers = t.pointers[:size]

	r := newInternalNode[V](cap(t.pointers))
	r.keys = append(r.keys, temp.keys[upKeyIdx+1:]...)
	r.pointers = append(r.pointers, temp.pointers[size:]...)

	return upKey, r
}
