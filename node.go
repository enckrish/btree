package btree

import "bytes"

type Node[V any] interface {
	// Returns true if the node needs to be rebalanced. Used for rebalancing
	// while deletion or later, if we are being lazy.
	needsRebalance() bool
	// isHealthy checks if node properly follows all the restrictions.
	// Used primarily for tests.
	isHealthy() bool
	// numUnhealthyChildren returns number of nodes in a subtree (excluding itself) which return isHealthy as false.
	numUnhealthyChildren() (unhealthy int, total int)
	// rebalanceWith rebalances a node with another of the same type.
	// Must be always called using the leftmost node in the pair.
	// upkey is the new key fpr the rightmost node in node-sibling pair, if nil, it means node is right node is deleted
	rebalanceWith(sibling Node[V], downKey Bytes) (upKey Bytes)
	// len returns the number of keys or pointers in LeafNode or InternalNode respectively.
	// It is used to choose which sibling to rebalance a node with
	len() int
	isLeaf() bool
}

type TraversalPositions[V any] struct {
	node *InternalNode[V]
	pos  int
}

func leafAndPathForKey[V any](n Node[V], key Bytes, st Stack[TraversalPositions[V]]) (*LeafNode[V], Stack[TraversalPositions[V]]) {
	for !n.isLeaf() {
		ni := n.(*InternalNode[V])
		ci := ni.childIndexForKey(key)
		n = ni.pointers[ci]
		if st != nil {
			st.Push(TraversalPositions[V]{node: ni, pos: ci})
		}
	}
	return n.(*LeafNode[V]), st
}

func setOrInsert[V any](n Node[V], key Bytes, value *V, st Stack[TraversalPositions[V]]) (Bytes, Node[V]) {
	defer st.Clear()
	l, st := leafAndPathForKey(n, key, st)
	key, newNode := l.setOrInsert(key, value)
	for newNode != nil && !st.Empty() {
		p, _ := st.Pop()
		key, newNode = p.node.handleInsert(p.pos, key, newNode)
	}

	return key, newNode
}

func deleteFromNode[V any](n Node[V], key Bytes, st Stack[TraversalPositions[V]]) bool {
	defer st.Clear()
	l, st := leafAndPathForKey(n, key, st)
	del := l.delete(key)
	for !st.Empty() {
		p, _ := st.Pop()
		del = p.node.handleDelete(p.pos, del)
	}
	return del
}

func valueRef[V any](n Node[V], key Bytes) *V {
	l, _ := leafAndPathForKey(n, key, nil)
	i, _ := lowerBoundBytesArr(l.keys, key)
	if i < l.len() && bytes.Equal(l.keys[i], key) {
		return l.values[i]
	}
	return nil
}
