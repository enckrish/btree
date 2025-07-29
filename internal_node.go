package btree

type InternalNode struct {
	Node
}

func AsInternalNode(node Node) InternalNode {
	return InternalNode{
		node,
	}
}

func NewInternalNode() InternalNode {
	return AsInternalNode(AllocNode(InternalNodeTyp))
}
