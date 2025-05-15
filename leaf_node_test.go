package btree

import (
	"bytes"
	"math/rand"
	"slices"
	"testing"
)

func isSorted[V any](ln *LeafNode[V]) bool {
	return slices.IsSortedFunc(ln.keys, func(a, b Bytes) int {
		return slices.Compare(a, b)
	})
}

func TestLeafSorted(t *testing.T) {
	ln := newLeafNode[int](5)
	keys, values := GetData(5)

	for i := 0; i < 5; i++ {
		ln.setOrInsert(keys[i][:], &values[i])
	}

	if !isSorted(ln) {
		t.Error("keys not sorted")
	}
}

func valueRefLeaf(l *LeafNode[int], key Bytes) *int {
	i, _ := lowerBoundBytesArr(l.keys, key)
	if i < l.len() && bytes.Equal(l.keys[i], key) {
		return l.values[i]
	}
	return nil
}

func TestLeafValues(t *testing.T) {
	n := 5
	ln := newLeafNode[int](n)
	keys, values := GetData(n)
	for i := 0; i < 5; i++ {
		_, p := ln.setOrInsert(keys[i][:], &values[i])
		if p != nil {
			t.Error("undesired splitting")
		}
	}

	for i := 0; i < 5; i++ {
		if v := valueRefLeaf(ln, keys[i][:]); v == nil || *v != values[i] {
			t.Error("value not correct")
		}
	}
}

func TestLeafSplit(t *testing.T) {
	n := 5
	ln := newLeafNode[int](n)
	keys, values := GetData(n + 1)

	var node Node[int]
	for i := 0; i < n+1; i++ {
		_, node = ln.setOrInsert(keys[i][:], &values[i])
		if node != nil && i != n {
			t.Error("split at incorrect position")
		}
	}

	// TODO undetected error here, only showed up few times, error:
	// panic: interface conversion: btree.Node[int] is nil, not *btree.LeafNode[int] [recovered]
	//	panic: interface conversion: btree.Node[int] is nil, not *btree.LeafNode[int]
	rnode := node.(*LeafNode[int])

	if !isSorted(ln) {
		t.Error("left split keys not sorted")
	}
	if !isSorted(rnode) {
		t.Error("right split keys not sorted")
	}
	if len(ln.keys)+len(rnode.keys) != 6 {
		t.Error("wrong number of keys")
	}
	if slices.Compare(ln.keys[len(ln.keys)-1][:], rnode.keys[0][:]) >= 0 {
		t.Error("split unordered")
	}
	if len(ln.keys) != len(rnode.keys) && len(ln.values) != len(rnode.values) {
		t.Error("split non-uniform for even number of elements")
	}

	if ln.next != rnode {
		t.Error("split doesn't update `next`")
	}
}

func TestLeafUpdate(t *testing.T) {
	ln := newLeafNode[int](5)
	keys, values := GetData(5)
	for i := 0; i < 5; i++ {
		ln.setOrInsert(keys[i][:], &values[i])
	}

	idx := 3
	key := keys[idx]
	oldVal := values[idx]
	newVal := rand.Int()

	if r := valueRefLeaf(ln, key[:]); r == nil || *r != oldVal {
		t.Error("old value is incorrect")
	}

	ln.setOrInsert(keys[idx][:], &newVal)
	if r := valueRefLeaf(ln, key[:]); r == nil || *r != newVal {
		t.Error("new value is incorrect")
	}
}
