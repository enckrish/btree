package btree

type LeafNode struct {
	Node
}

func AsLeafNode(node Node) LeafNode {
	return LeafNode{
		node,
	}
}

func NewLeafNode() LeafNode {
	return AsLeafNode(AllocNode(LeafNodeTyp))
}

func (l LeafNode) Delete(key Bytes) {
	panic(ErrUnimplemented.Error())
}
