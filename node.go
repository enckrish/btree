package btree

type Node[V any] interface {
	// setOrInsert inserts or updates the value of a key in the tree.
	// In case, it creates a new node, it returns the key and the node.
	setOrInsert(Bytes, *V) (Bytes, Node[V])
	// lbPositionedRef fetches the leaf node and index to the key/value
	// corresponding to the lower bound of the supplied key, where index in [0, n].
	// Index of n is returned when supplied key is larger than existing keys in that leaf,
	// but lesser than right sibling's minimum key.
	// The key and value can then be fetched using leafNode.pairAt.
	lbPositionedRef(Bytes) (*LeafNode[V], int)
	// valueRef returns the reference to the stored value, and is
	// implemented as a wrapper on top of lbPositionedRef.
	valueRef(key Bytes) *V
	// Returns true if the node needs to be rebalanced. Used for rebalancing
	// while deletion or later, if we are being lazy.
	needsRebalance() bool
	// isHealthy checks if node properly follows all the restrictions.
	// Used primarily for tests.
	isHealthy() bool
	// numUnhealthyChildren returns number of nodes in a subtree (excluding itself) which return isHealthy as false.
	numUnhealthyChildren() (unhealthy int, total int)
	// delete deletes the pair corresponding to the supplied key and returns true on success.
	// It returns false if key doesn't exist in the tree. lazy=False instructs a parent node
	// to rebalance if its concerned child needs rebalancing after deletion.
	delete(key Bytes, lazy bool) bool
	// rebalanceWith rebalances a node with another of the same type.
	// Must be always called using the leftmost node in the pair.
	// upkey is the new key fpr the rightmost node in node-sibling pair, if nil, it means node is right node is deleted
	rebalanceWith(sibling Node[V], downKey Bytes) (upKey Bytes)
	// len returns the number of keys or pointers in LeafNode or InternalNode respectively.
	// It is used to choose which sibling to rebalance a node with
	len() int
}
