package btree

import (
	crnd "crypto/rand"
	"math/rand"
)

var letterBytes = []byte("abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ")

func RandASCIIByte32(_ int) Hash {
	var b Hash
	_, _ = crnd.Read(b[:])
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
