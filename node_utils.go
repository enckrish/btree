package btree

import (
	"bytes"
)

type SetStackEntry[V any] struct {
	node *InternalNode[V]
	pos  int
}

func setOrInsert[V any](n Node[V], key Bytes, value *V, st Stack[SetStackEntry[V]]) (Bytes, Node[V]) {
	defer st.Clear()
	for !n.isLeaf() {
		ni := n.(*InternalNode[V])
		st.Push(SetStackEntry[V]{node: ni})
		ci := ni.childIndexForKey(key)
		st.Top().pos = ci
		n = ni.pointers[ci]
	}

	key, newNode := n.(*LeafNode[V]).setOrInsert(key, value)
	for newNode != nil && !st.Empty() {
		p, _ := st.Pop()
		key, newNode = p.node.handleInsert(p.pos, key, newNode)
	}

	return key, newNode
}

func lbPositionedRef[V any](n Node[V], key Bytes) (*LeafNode[V], int) {
	for !n.isLeaf() {
		ni := n.(*InternalNode[V])
		ci := ni.childIndexForKey(key)
		n = ni.pointers[ci]
	}

	l := n.(*LeafNode[V])
	i, _ := lowerBoundBytesArr(l.keys, key)
	return l, i
}

func valueRef[V any](n Node[V], key Bytes) *V {
	l, i := lbPositionedRef(n, key)
	if i < l.len() && bytes.Equal(l.keys[i], key) {
		return l.values[i]
	}
	return nil
}
