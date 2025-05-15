package btree

import (
	"bytes"
	"iter"
)

// BTree is a general-propose B+ tree that takes byte arrays as key and supports arbitrary value types.
// This is in contrast to Map, which hashes all keys before inserting them in the tree.
type BTree[V any] struct {
	root   Node[V] // root starts from being a *LeafNode[V] then changes to *InternalNode[V] after first split
	deg    int     // defined as the number of pointers from each node
	height int
}

func NewBTree[V any](degree int) *BTree[V] {
	return &BTree[V]{
		root:   newLeafNode[V](degree - 1),
		deg:    degree,
		height: 0,
	}
}

func (b *BTree[V]) Degree() int {
	return b.deg
}

func (b *BTree[V]) GetOp(key Bytes) *V {
	return valueRef(b.root, key)
}

// SetOp sets/inserts the given key-value pair in the map, and handles root node split if needed
func (b *BTree[V]) SetOp(key Bytes, value *V) {
	key, newNode := setOrInsert(b.root, key, value)
	if newNode != nil {
		b.newRoot(key, newNode)
	}
}

func (b *BTree[V]) DelOp(key Bytes, lazy bool) bool {
	assert(!lazy, "lazy delete unimplemented")
	del := b.root.delete(key, lazy)
	if !lazy && del && !b.root.isLeaf() {
		ri := b.root.(*InternalNode[V])
		if ri.len() == 1 {
			b.root = ri.pointers[0]
			b.height--
		}
	}
	return del
}

func (b *BTree[V]) newRoot(up Bytes, node Node[V]) {
	root := newInternalNode[V](b.deg)
	root.keys = append(root.keys, up)
	root.pointers = append(root.pointers, b.root, node)
	b.root = root
	b.height++
}

func (b *BTree[V]) baseIterator(low, high Bytes) iter.Seq2[Bytes, *V] {
	return func(yield func(Bytes, *V) bool) {
		// get reference to key that is equal to `low` or minimally larger than it
		leaf, idx := lbPositionedRef(b.root, low)
		if leaf == nil {
			panic("leaf node not found")
		}

		for idx < len(leaf.keys) && (high == nil || bytes.Compare(leaf.keys[idx], high) < 0) {
			k, v := leaf.pairAt(idx)
			if !yield(k, v) {
				break
			}
			idx++
			if idx >= len(leaf.keys) {
				leaf = leaf.next
				if leaf == nil {
					break
				}
				idx = 0
			}
		}
	}
}

func (b *BTree[V]) All() iter.Seq2[Bytes, *V] {
	return b.baseIterator(nil, nil)
}

func (b *BTree[V]) Range(low, high Bytes) iter.Seq2[Bytes, *V] {
	return b.baseIterator(low, high)
}
