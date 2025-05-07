package btree

import (
	"math/rand"
	"time"
)

var letterBytes = []byte("abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ")

func RandASCIIByte32(k int) Hash {
	pr := 5080675427 //9017351767
	var b Hash
	for i := range len(b) {
		x := (k*pr + int(time.Now().UnixNano())) % len(letterBytes)
		b[i] = letterBytes[x]
	}
	return b
}

// GetData returns `nkeys` number of Hash, int pairs; each Hash is unique
func GetData(nkeys int) ([]Hash, []int) {
	keys := make([]Hash, nkeys)
	values := make([]int, nkeys)

	// non-unique values
	for i := range values {
		values[i] = rand.Int()
	}

	set := NewSet[Hash]()
	for i := 0; i < nkeys; i++ {
		var key Hash
		key = RandASCIIByte32(i)
		for set.Contains(key) {
			key = RandASCIIByte32(i)
		}
		keys[i] = key
		set.Add(key)
	}
	return keys, values
}

func computeMapHeight[K, V any](m *Map[K, V]) int {
	node := m.root
	height := 0
	for {
		switch node.(type) {
		case *LeafNode[V]:
			return height
		case *InternalNode[V]:
			height++
			node = node.(*InternalNode[V]).pointers[0]
		}
	}
}
