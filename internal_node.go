package btree

import (
	"bytes"
	"slices"
)

type InternalNode[V any] struct {
	keys     []Bytes
	pointers []Node[V]
	minCount int
}

func newInternalNode[V any](degree int) *InternalNode[V] {
	return &InternalNode[V]{
		keys:     make([]Bytes, 0, degree-1),
		pointers: make([]Node[V], 0, degree),
		minCount: ceilDiv(degree, 2),
	}
}

func (t *InternalNode[V]) len() int {
	return len(t.pointers)
}

func (t *InternalNode[V]) isLeaf() bool {
	return false
}

func (t *InternalNode[V]) needsRebalance() bool {
	return t.len() < t.minCount
}

func (t *InternalNode[V]) isHealthy() bool {
	rebalNeeded := t.needsRebalance()
	keyPtrLenCheck := len(t.keys) == t.len()-1
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

	healthy := !rebalNeeded && keyPtrLenCheck && keysSorted && keysUnique && ptrsUnique
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

// for getting best node-local sibling for rebalancing during deletion
func (t *InternalNode[V]) siblingPair(i int) (left Node[V], right Node[V], downKeyIdx int) {
	og := t.pointers[i]
	if i == 0 {
		return og, t.pointers[1], 0
	}
	if i == t.len()-1 {
		return t.pointers[t.len()-2], og, t.len() - 2
	}

	lSib := t.pointers[i-1]
	rSib := t.pointers[i+1]

	// chooses the sibling with maximum number of pointers, so that node deletion ops are minimum
	if lSib.len() >= rSib.len() {
		return lSib, og, i - 1
	}
	return og, rSib, i
}

// Returns the index to t.pointers for the given key
func (t *InternalNode[V]) childIndexForKey(key Bytes) int {
	pos, exists := lowerBoundBytesArr(t.keys, key)
	if exists {
		return pos + 1
	}
	return pos
}

func (t *InternalNode[V]) handleInsert(pos int, key Bytes, ptr Node[V]) (Bytes, Node[V]) {
	if ptr == nil {
		// No new child formed
		return nil, nil
	}

	// space available in node
	if len(t.keys) < cap(t.keys) {
		t.insertAtIndex(pos, key, ptr)
		return nil, nil
	}

	// needs splitting
	up, newNode := t.insertWithSplit(pos, key, ptr)

	// In case, we directly return newNode, it won't return true for (newNode == nil) in the calling function
	// [See https://go.dev/doc/faq#nil_error]
	if newNode != nil {
		return up, newNode
	}
	return up, nil
}

func (t *InternalNode[V]) insertAtIndex(idx int, key Bytes, ptr Node[V]) {
	sz := t.len()
	t.keys = t.keys[:sz]
	t.pointers = t.pointers[:sz+1]
	shrArr(t.keys[idx:], 1)
	shrArr(t.pointers[idx+1:], 1)
	t.keys[idx] = key
	t.pointers[idx+1] = ptr
}

func (t *InternalNode[V]) insertWithSplit(pos int, key Bytes, ptr Node[V]) (upKey Bytes, newNode *InternalNode[V]) {
	// maximum number of keys in old node after a split
	// in case of unequal distribution, it gives the new node more keys (by 1), this is a non-issue
	size := t.minCount

	// construct a temporary node with usual capacity + 1 for easier operations possibly at the cost of performance
	temp := newInternalNode[V](t.len() + 1)
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

func (t *InternalNode[V]) delete(key Bytes, lazy bool) bool {
	assert(!lazy, "lazy delete unimplemented")

	ci := t.childIndexForKey(key)
	child := t.pointers[ci]
	del := child.delete(key, lazy)

	if !del || lazy {
		return del
	}

	if child.needsRebalance() {
		left, right, dkIdx := t.siblingPair(ci)
		upKey := left.rebalanceWith(right, t.keys[dkIdx])

		if upKey != nil { // no nodes deleted, only strictly rebalanced
			t.keys[dkIdx] = upKey
		} else { // right node deleted
			sz := t.len()
			shlArr(t.keys[dkIdx:], 1)
			shlArr(t.pointers[dkIdx+1:], 1)
			t.pointers = t.pointers[:sz-1]
			t.keys = t.keys[:sz-2]
		}

		lnr := left.needsRebalance()
		rnr := right.needsRebalance()
		assert(!lnr, "left needs rebalance")
		assert(upKey == nil || !rnr, "right needs rebalance")
	}
	return del
}

func (t *InternalNode[V]) rebalanceWith(rightNode Node[V], downKey Bytes) Bytes {
	// cast sibling as internal node type, will panic if it isn't
	rNode := rightNode.(*InternalNode[V])

	// if a single node can contain all the data
	merge := t.len()+rNode.len() <= cap(t.pointers)
	if merge {
		t.keys = append(t.keys, downKey)
		t.keys = append(t.keys, rNode.keys...)
		t.pointers = append(t.pointers, rNode.pointers...)
		return nil
	}

	upKey := redistributeInternalUnoptimized(t, rNode, downKey)
	return upKey
}

func redistributeInternalUnoptimized[V any](l *InternalNode[V], r *InternalNode[V], downKey Bytes) (upKey Bytes) {
	totalLen := l.len() + r.len() // total number of pointers
	temp := newInternalNode[V](totalLen)

	temp.keys = append(temp.keys, l.keys...)
	temp.keys = append(temp.keys, downKey)
	temp.keys = append(temp.keys, r.keys...)
	temp.pointers = append(temp.pointers, l.pointers...)
	temp.pointers = append(temp.pointers, r.pointers...)

	lsz := l.minCount // num pointers in l
	rsz := totalLen - lsz

	l.keys = l.keys[:lsz-1]
	l.pointers = l.pointers[:lsz]
	r.keys = r.keys[:rsz-1]
	r.pointers = r.pointers[:rsz]

	copy(l.pointers, temp.pointers[:lsz])
	copy(r.pointers, temp.pointers[lsz:])

	copy(l.keys, temp.keys[:lsz-1]) // keys always one less than pointers
	upKey = temp.keys[lsz-1]
	copy(r.keys, temp.keys[lsz:])

	return upKey
}
