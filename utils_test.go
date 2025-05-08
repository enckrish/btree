package btree

import (
	"encoding/binary"
	"math/rand"
	"time"
)

var letterBytes = []byte("abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ")

func RandASCIIByte32(_ int) Hash {
	var b Hash
	t := time.Now().UnixNano()
	for i := range 4 {
		binary.BigEndian.PutUint64(b[8*i:8*(i+1)], uint64(t*(int64(i)+1)))
	}
	for i := 0; i < 32; i++ {
		b[i] = letterBytes[int(b[i])%len(letterBytes)]
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
