package btree

import (
	"bytes"
	"iter"
)

type Node[V any] interface {
	// setOrInsert inserts or updates the value of a key in the tree
	// In case, it creates a new node, it returns the key and the node
	setOrInsert(Bytes, V) (Bytes, Node[V])
	// lbPositionedRef fetches the leaf node and index to the key/value
	// corresponding to the lower bound of the supplied key
	// The key and value can then be fetched using leafNode.pairAt
	lbPositionedRef(Bytes) (*LeafNode[V], int)
	// valueRef returns the reference to the stored value, and is
	// implemented as a wrapper on top of lbPositionedRef
	valueRef(key Bytes) *V
	// isHealthy checks if node properly follows all the restrictions
	// used primarily for tests, but can also be used for balancing of tree when
	// deletion operations postpone balancing
	isHealthy() bool
	// numUnhealthyChildren returns number of nodes in a subtree (excluding itself) which return isHealthy as false
	numUnhealthyChildren() (unhealthy int, total int)
}

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

func (b *BTree[V]) ValueRef(key Bytes) *V {
	return b.root.valueRef(key)
}

// SetOp sets/inserts the given key-value pair in the map, and handles root node split if needed
func (b *BTree[V]) SetOp(key Bytes, value V) {
	up, newNode := b.root.setOrInsert(key, value)

	// TODO possibility of nil error mistake, please verify
	if newNode == nil {
		return
	}
	b.newRoot(up, newNode)
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
		leaf, idx := b.root.lbPositionedRef(low)
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
